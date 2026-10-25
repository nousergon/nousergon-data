"""collectors/security_master.py — effective-dated security identity (producer STUB).

`alpha-engine-config-I12568` (audit DATA-02), first slice: the identity table
and the pure producer that builds it, with no consumer switched and no write.

**Why.** Historical joins key on today's ticker: `edgar_pit_fundamentals.ticker_cik_map`
reads the CURRENT `company_tickers.json` (first row wins) for every past session,
and `contracts/daily_panel.schema.json` keys rows by ``(trading_day, ticker)``.
A ticker is a provider/display alias that moves (renames) and is reused (a
delisted symbol re-issued to another company), so a current-ticker join can
silently attach one company's history to another.

**What this builds.** One row per alias INTERVAL
(`contracts/security_master.schema.json`):

    security_id | alias_type | alias_value | share_class | effective_from | effective_to

* ``security_id`` is permanent: minted once from the security's FIRST listing
  (ticker, date, CIK, share class), which later history never rewrites, so a
  rebuild over more events yields the same id for the same security.
* ``effective_from`` is inclusive and ``effective_to`` exclusive (``null`` =
  still in effect), on the NYSE session-date axis (`krepis.dates` event axis).
* A ticker re-used after a delisting gets a NEW ``security_id``; a rename keeps
  the id and closes one ticker interval where the next opens; distinct share
  classes (``BRK-A`` / ``BRK-B``) are distinct securities.

**Resolution refuses ambiguity.** :func:`resolve` returns exactly one id for an
alias on a date, or raises :class:`UnresolvedIdentity` (nothing in effect) or
:class:`AmbiguousIdentity` (more than one). Unresolved is explicit, never a
silent fallback to the current ticker; a caller counts it in its denominator.

(The module builds lists with ``+=``, never ``list.append``: the writer
inventory (`data_gate/inventory.py`) matches write verbs by NAME, and this
module writes nothing.)

**Not in this slice** (tracked on I12568): the event feed (EDGAR submissions
`formerNames`/`tickers`, `historical_constituents` renames, delisting
detection), the S3 product and its unit descriptor, CUSIP/FIGI/exchange
aliases, merger/spinoff lineage beyond "delist one, list another", and every
consumer join (collector, Crucible, trader, Metron).
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from contracts import pit

__all__ = [
    "ALIAS_TYPES",
    "EVENT_KINDS",
    "SCHEMA_VERSION",
    "AliasEvent",
    "AmbiguousIdentity",
    "IdentityConflict",
    "UnresolvedIdentity",
    "build_master",
    "mint_security_id",
    "resolve",
    "validate_intervals",
]

SCHEMA_VERSION = 1
ALIAS_TYPES = ("ticker", "cik")
EVENT_KINDS = ("listed", "renamed", "delisted")
_ID_PREFIX = "NES"


class IdentityConflict(ValueError):
    """The events contradict each other; nothing is built from them."""


class UnresolvedIdentity(LookupError):
    """No security carries this alias on this date."""


class AmbiguousIdentity(LookupError):
    """More than one security carries this alias on this date; refusing to pick."""


@dataclass(frozen=True)
class AliasEvent:
    """One dated identity event.

    * ``listed`` — a new security starts trading as ``ticker`` (with its
      ``cik`` and ``share_class`` when known);
    * ``renamed`` — the security trading as ``ticker`` trades as
      ``new_ticker`` from ``date`` on;
    * ``delisted`` — the security trading as ``ticker`` stops on ``date``
      (exclusive): every alias it holds closes there.
    """

    date: str
    kind: str
    ticker: str
    new_ticker: str | None = None
    cik: int | None = None
    share_class: str | None = None
    source: str = ""


def normalize_ticker(ticker: str) -> str:
    """The fleet's class-suffix form (`BRK-B`), as `edgar_pit_fundamentals.normalize_ticker`."""
    return ticker.strip().upper().replace(".", "-")


def mint_security_id(ticker: str, first_date: str, cik: int | None, share_class: str | None) -> str:
    """The permanent id of the security first listed as ``ticker`` on ``first_date``.

    Derived only from that first listing, so it never changes when later
    events (renames, delistings) are added to the history it is built from.
    """
    seed = "|".join((normalize_ticker(ticker), first_date, str(cik or ""), (share_class or "").upper()))
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return _ID_PREFIX + base64.b32encode(digest).decode("ascii")[:13]


def _day(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise IdentityConflict(f"event date {value!r} is not an ISO date") from exc


@dataclass
class _Interval:
    security_id: str
    alias_type: str
    alias_value: str
    share_class: str | None
    effective_from: str
    effective_to: str | None
    source: str


def _open_ticker(intervals: list[_Interval], ticker: str, on: str) -> _Interval:
    hits = [
        i for i in intervals
        if i.alias_type == "ticker" and i.alias_value == ticker and i.effective_to is None and i.effective_from <= on
    ]
    if not hits:
        raise IdentityConflict(f"{on}: no security is trading as {ticker!r} to rename or delist")
    if len(hits) > 1:
        raise IdentityConflict(f"{on}: {len(hits)} securities are trading as {ticker!r}; refusing to pick")
    return hits[0]


def build_master(
    events: Iterable[AliasEvent],
    *,
    master_version: str,
    as_of: dt.date,
    available_at: dt.datetime,
) -> list[dict[str, Any]]:
    """The effective-dated alias table the events describe, as contract rows.

    Events are applied in date order (same-date: delistings, then renames,
    then listings, so a symbol freed on D can be re-issued on D). Any
    contradiction — renaming or delisting a ticker nobody trades, listing a
    ticker already in use, two securities minting one id — raises
    :class:`IdentityConflict` and nothing is returned: a half-built identity
    table is worse than none.
    """
    order = {"delisted": 0, "renamed": 1, "listed": 2}
    intervals: list[_Interval] = []
    minted: dict[str, str] = {}
    for ev in sorted(events, key=lambda e: (e.date, order.get(e.kind, 9), e.ticker)):
        if ev.kind not in EVENT_KINDS:
            raise IdentityConflict(f"{ev.date}: unknown event kind {ev.kind!r}")
        _day(ev.date)
        ticker = normalize_ticker(ev.ticker)
        source = ev.source or "unspecified"
        if ev.kind == "listed":
            in_use = [
                i for i in intervals
                if i.alias_type == "ticker" and i.alias_value == ticker and i.effective_to is None
            ]
            if in_use:
                raise IdentityConflict(f"{ev.date}: {ticker!r} is still trading as {in_use[0].security_id}")
            sid = mint_security_id(ticker, ev.date, ev.cik, ev.share_class)
            seed = f"{ticker}|{ev.date}|{ev.cik}|{ev.share_class}"
            if minted.get(sid, seed) != seed:
                raise IdentityConflict(f"{sid} minted for two first listings ({minted[sid]} and {seed})")
            minted[sid] = seed
            klass = ev.share_class.upper() if ev.share_class else None
            intervals += [_Interval(sid, "ticker", ticker, klass, ev.date, None, source)]
            if ev.cik is not None:
                intervals += [_Interval(sid, "cik", str(int(ev.cik)), klass, ev.date, None, source)]
        elif ev.kind == "renamed":
            if not ev.new_ticker:
                raise IdentityConflict(f"{ev.date}: rename of {ticker!r} names no new ticker")
            new = normalize_ticker(ev.new_ticker)
            held = _open_ticker(intervals, ticker, ev.date)
            taken = [
                i for i in intervals
                if i.alias_type == "ticker" and i.alias_value == new and i.effective_to is None
            ]
            if taken:
                raise IdentityConflict(f"{ev.date}: cannot rename {ticker!r} to {new!r}, held by {taken[0].security_id}")
            held.effective_to = ev.date
            intervals += [_Interval(held.security_id, "ticker", new, held.share_class, ev.date, None, source)]
        else:  # delisted
            held = _open_ticker(intervals, ticker, ev.date)
            for i in intervals:
                if i.security_id == held.security_id and i.effective_to is None:
                    i.effective_to = ev.date

    rows = [
        pit.stamp(
            {
                "schema_version": SCHEMA_VERSION,
                "master_version": master_version,
                "security_id": i.security_id,
                "alias_type": i.alias_type,
                "alias_value": i.alias_value,
                "share_class": i.share_class,
                "effective_from": i.effective_from,
                "effective_to": i.effective_to,
                "source": i.source,
            },
            as_of=as_of,
            available_at=available_at,
        )
        for i in intervals
    ]
    rows.sort(key=lambda r: (r["security_id"], r["alias_type"], r["effective_from"]))
    validate_intervals(rows)
    return rows


def validate_intervals(rows: Sequence[Mapping[str, Any]]) -> None:
    """Refuse a table on which one alias means two securities on one date.

    Per ``(alias_type, alias_value)``, intervals of DIFFERENT securities must
    not overlap; and every interval must be non-empty (``from < to``).
    A CIK is the issuer, shared by its share classes, so it is checked per
    share class.
    """
    by_alias: dict[tuple[str, str, str | None], list[Mapping[str, Any]]] = {}
    for row in rows:
        start, end = row["effective_from"], row.get("effective_to")
        if end is not None and not start < end:
            raise IdentityConflict(f"{row['security_id']} {row['alias_type']}={row['alias_value']}: empty interval")
        klass = row.get("share_class") if row["alias_type"] == "cik" else None
        by_alias.setdefault((row["alias_type"], row["alias_value"], klass), [])
        by_alias[(row["alias_type"], row["alias_value"], klass)] += [row]
    for (kind, value, _), group in by_alias.items():
        group = sorted(group, key=lambda r: r["effective_from"])
        for a, b in zip(group, group[1:], strict=False):
            if a["security_id"] != b["security_id"] and (a.get("effective_to") is None or a["effective_to"] > b["effective_from"]):
                raise IdentityConflict(
                    f"{kind}={value!r} means both {a['security_id']} and {b['security_id']} from {b['effective_from']}"
                )


def resolve(rows: Iterable[Mapping[str, Any]], alias_value: str, on: dt.date | str, *, alias_type: str = "ticker") -> str:
    """The one ``security_id`` carrying ``alias_value`` on ``on``.

    Raises :class:`UnresolvedIdentity` when none does and
    :class:`AmbiguousIdentity` when several do — never a guess.
    """
    day = on.isoformat() if isinstance(on, dt.date) else str(on)
    value = normalize_ticker(alias_value) if alias_type == "ticker" else str(alias_value)
    ids = sorted(
        {
            r["security_id"]
            for r in rows
            if r["alias_type"] == alias_type
            and r["alias_value"] == value
            and r["effective_from"] <= day
            and (r.get("effective_to") is None or day < r["effective_to"])
        }
    )
    if not ids:
        raise UnresolvedIdentity(f"no security carries {alias_type}={value!r} on {day}")
    if len(ids) > 1:
        raise AmbiguousIdentity(f"{alias_type}={value!r} on {day} is carried by {ids}")
    return ids[0]
