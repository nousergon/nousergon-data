"""collectors/security_master_feed.py — the event feed for the security master (pure).

`alpha-engine-config-I12568` (audit DATA-02), second slice. The first slice
(`collectors/security_master.py`) builds the effective-dated alias table from
:class:`~collectors.security_master.AliasEvent` values. This module derives
those events from two sources the fleet already holds, and reports every
security or event it could NOT place:

* **the roster**: SEC ``company_tickers.json`` (``{"0": {"cik_str", "ticker",
  "title"}, ...}``), the ticker→CIK map `edgar_pit_fundamentals.ticker_cik_map`
  reads today. It says which securities trade NOW and under which ticker. It
  says nothing about when they started.
* **the renames**: `collectors/data/sp500_known_retickers.json` (``old``,
  ``new``, ``effective``, ``evidence``), the committed reticker list
  `historical_constituents` already trusts.

**How a security's history is reconstructed.** Each roster ticker is walked
BACKWARD through the renames (latest rename onto it first, each step strictly
earlier), giving its earliest known ticker and its rename chain. The security
is ``listed`` under that earliest ticker on its LISTING EVIDENCE date, then
``renamed`` along the chain.

**Listing evidence is supplied, never invented.** ``first_seen`` maps a ticker
to the first session it is observed trading for this security (the caller's
evidence, for example a price history's first close). When it is absent:

* a security with no renames is listed on the roster's ``snapshot_date``.
  That is the only date the roster proves, so ``resolve`` refuses every
  earlier date for it rather than asserting a past the source never showed;
* a security WITH renames is not placed at all and is reported under
  ``no_listing_evidence``. Listing it on the snapshot date would put its old
  ticker after its own rename, and listing it on an invented date would claim
  a history nobody observed.

**A shared CIK needs a share class.** Distinct classes are distinct securities
(`BRK-A`, `BRK-B`). The class is read from the fleet's class-suffix form (a
single letter after ``-``). A CIK held by several roster tickers that the
suffix cannot tell apart (`GOOG` and `GOOGL`) is WITHHELD from those
securities and reported under ``cik_withheld``. ``resolve`` by that CIK then
returns :class:`~collectors.security_master.UnresolvedIdentity`, which is
explicit, instead of an arbitrary pick.

**Ticker reuse is the case this exists for.** `ECHO` was Echo Global Logistics
until 2021 and has been EchoStar's ticker since 2026-06-25 (`SATS` → `ECHO`).
Built from these sources, ``resolve("ECHO", <before 2026-06-25>)`` is
unresolved, never EchoStar: the master asserts EchoStar under `SATS` there and
nothing under `ECHO`.

Nothing here fetches or writes. (Lists are built with ``+=``, never
``list.append``: the writer inventory (`data_gate/inventory.py`) matches write
verbs by NAME.) **Not in this slice** (tracked on I12568): delisting detection
(roster snapshot diffs), the EDGAR submissions and price-cache evidence that
fill ``first_seen``, the S3 product and its unit descriptor, and every
consumer join.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from collectors.security_master import (
    AliasEvent,
    IdentityConflict,
    build_master,
    normalize_ticker,
)

__all__ = [
    "SOURCE_RETICKERS",
    "SOURCE_ROSTER",
    "FeedReport",
    "Rename",
    "build_from_sources",
    "feed_events",
    "renames_from_retickers",
    "roster_from_company_tickers",
    "share_class_of",
]

SOURCE_ROSTER = "sec_company_tickers"
SOURCE_RETICKERS = "sp500_known_retickers"
_CLASS_SUFFIX = re.compile(r"^[A-Z0-9]+-([A-Z])$")


@dataclass(frozen=True)
class Rename:
    old: str
    new: str
    effective: str


@dataclass(frozen=True)
class FeedReport:
    """What the feed placed, and every security or rename it could not place.

    The unplaced lists are the denominator I12568 says must be explicit.
    """

    securities: int
    renames: int
    listed_on_snapshot: tuple[str, ...]
    no_listing_evidence: tuple[str, ...]
    cik_withheld: tuple[str, ...]
    unplaced_renames: tuple[str, ...]


def share_class_of(ticker: str) -> str | None:
    """The class letter of a class-suffix ticker (`BRK-B` → `B`), else ``None``."""
    m = _CLASS_SUFFIX.match(normalize_ticker(ticker))
    return m.group(1) if m else None


def roster_from_company_tickers(doc: Mapping[str, Any]) -> dict[str, int]:
    """``{ticker: cik}`` from SEC ``company_tickers.json``.

    Unlike `edgar_pit_fundamentals.ticker_cik_map` (first row wins), a ticker
    listed under two CIKs raises :class:`IdentityConflict`: the roster
    contradicts itself, and picking one is the guess this product removes.
    """
    out: dict[str, int] = {}
    for entry in doc.values():
        if not isinstance(entry, Mapping) or not entry.get("ticker"):
            continue
        ticker = normalize_ticker(str(entry["ticker"]))
        cik = int(entry["cik_str"])
        if out.get(ticker, cik) != cik:
            raise IdentityConflict(f"roster lists {ticker!r} under CIK {out[ticker]} and {cik}")
        out[ticker] = cik
    return out


def renames_from_retickers(doc: Mapping[str, Any]) -> list[Rename]:
    """The renames in `sp500_known_retickers.json`, validated, in date order."""
    out: list[Rename] = []
    for entry in doc.get("retickers") or []:
        old, new, effective = (str(entry.get(k) or "") for k in ("old", "new", "effective"))
        if not (old and new and effective):
            raise IdentityConflict(f"reticker entry {entry!r} lacks old, new or effective")
        try:
            dt.date.fromisoformat(effective)
        except ValueError as exc:
            raise IdentityConflict(f"reticker {old}->{new}: effective {effective!r} is not an ISO date") from exc
        out += [Rename(normalize_ticker(old), normalize_ticker(new), effective)]
    return sorted(out, key=lambda r: (r.effective, r.old))


def _chain(ticker: str, renames: list[Rename]) -> list[Rename]:
    """The renames that led to ``ticker``, oldest first (each step strictly earlier)."""
    chain: list[Rename] = []
    cur, before = ticker, None
    while True:
        hits = [r for r in renames if r.new == cur and (before is None or r.effective < before)]
        if not hits:
            return chain[::-1]
        step = max(hits, key=lambda r: r.effective)
        chain += [step]
        cur, before = step.old, step.effective


def feed_events(
    roster: Mapping[str, int],
    renames: list[Rename],
    *,
    snapshot_date: dt.date,
    first_seen: Mapping[str, str] | None = None,
) -> tuple[list[AliasEvent], FeedReport]:
    """The alias events the roster and renames describe, plus what could not be placed."""
    first_seen = {normalize_ticker(k): v for k, v in (first_seen or {}).items()}
    snap = snapshot_date.isoformat()
    by_cik: dict[int, list[str]] = {}
    for ticker, cik in roster.items():
        by_cik.setdefault(cik, [])
        by_cik[cik] += [ticker]
    withheld = sorted(
        t for tickers in by_cik.values() if len(tickers) > 1 for t in tickers if share_class_of(t) is None
    )

    events: list[AliasEvent] = []
    used: set[Rename] = set()
    on_snapshot: list[str] = []
    no_evidence: list[str] = []
    for ticker in sorted(roster):
        chain = _chain(ticker, renames)
        first = chain[0].old if chain else ticker
        listed = first_seen.get(first)
        if listed is None:
            if chain:
                no_evidence += [ticker]
                continue
            listed, source = snap, f"{SOURCE_ROSTER}@{snap} (listing not observed; floor = snapshot)"
            on_snapshot += [ticker]
        else:
            source = f"{SOURCE_ROSTER}@{snap}+first_seen"
        if chain and not listed < chain[0].effective:
            raise IdentityConflict(
                f"{first!r} is first seen {listed}, not before its rename to {chain[0].new!r} on {chain[0].effective}"
            )
        cik = None if ticker in withheld else roster[ticker]
        events += [AliasEvent(listed, "listed", first, cik=cik, share_class=share_class_of(ticker), source=source)]
        events += [AliasEvent(r.effective, "renamed", r.old, new_ticker=r.new, source=SOURCE_RETICKERS) for r in chain]
        used |= set(chain)

    report = FeedReport(
        securities=len(roster) - len(no_evidence),
        renames=len(used),
        listed_on_snapshot=tuple(on_snapshot),
        no_listing_evidence=tuple(no_evidence),
        cik_withheld=tuple(withheld),
        unplaced_renames=tuple(f"{r.effective} {r.old}->{r.new}" for r in renames if r not in used),
    )
    return events, report


def build_from_sources(
    company_tickers: Mapping[str, Any],
    retickers: Mapping[str, Any],
    *,
    snapshot_date: dt.date,
    master_version: str,
    available_at: dt.datetime,
    first_seen: Mapping[str, str] | None = None,
) -> tuple[list[dict[str, Any]], FeedReport]:
    """The security master the two sources describe, and the feed's report."""
    events, report = feed_events(
        roster_from_company_tickers(company_tickers),
        renames_from_retickers(retickers),
        snapshot_date=snapshot_date,
        first_seen=first_seen,
    )
    rows = build_master(events, master_version=master_version, as_of=snapshot_date, available_at=available_at)
    return rows, report
