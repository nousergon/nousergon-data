"""
historical_constituents.py — Point-in-time S&P 500+400 index membership (G12).

Survivorship-bias mitigation, Phase 1 (research memo
``nousergon-docs/survivorship-bias-research.md``): the backtester/predictor
today see only *currently-listed* constituents, so 10y synthetic backtests
silently exclude every name that was delisted, acquired, or index-dropped —
an upward (survivor) bias on backtest credibility (~1-4%/yr overstatement
class).

This module reconstructs as-of-date membership by **replaying index changes
backward from today's roster**: each change (ticker *added* on date D, ticker
*removed* on date D) is undone walking from newest to oldest, so the membership
set immediately *before* date D is recovered. The output is a
``{date: [tickers]}`` map the backtester reads to define the point-in-time
universe for each backtest date.

WHERE THOSE CHANGES COME FROM (alpha-engine-config-I6946). Originally, all of
them were scraped from Wikipedia's "Selected changes to the list" table on
every run. On 2026-08-11 an editor moved that table to a different article and
the weekly pipeline failed three hours later — a load-bearing backtest input
taken down by a formatting edit. Two producers replaced it, split at
``SNAPSHOT_CUTOVER``:

  * **Before the cutover** — ``collectors/data/sp500_changes_frozen_*.json``,
    a committed, content-hashed reconstruction. 1976-2026 index changes are
    settled; re-deriving them weekly from an editable page re-ran the risk to
    re-learn facts that had not moved.
  * **On and after** — a diff of our own dated roster snapshots at
    ``market_data/weekly/{date}/constituents.json``, themselves sourced from
    SSGA's SPY holdings (config#2812). Membership is OBSERVED, not replayed.

Wikipedia is still fetched, but only to ATTEST the post-cutover window — never
to produce it. A page that moves, 404s or is vandalised now costs an
attestation and a WARNING, not the pipeline. Disagreement between the two
derivations is itself the signal worth having: it means a bad upstream edit or
a gap in our own collection.

Layers, deliberately separated so the risky parsing is unit-tested with no
network:
  * ``parse_changes_table(df)`` — Wikipedia changes DataFrame -> list of
    structured ``ConstituentChange`` events (pure). Attestation only.
  * ``changes_from_snapshots(snapshots)`` -> the same list type, from observed
    dated rosters (pure).
  * ``load_frozen_changes()`` — the hash-verified pre-cutover history.
  * ``build_pit_membership(current_tickers, changes)`` -> ``{date: [tickers]}``
    point-in-time map (pure).
  * ``divergences(observed, reference, since=...)`` — the attestation (pure).
  * ``collect(...)`` — read + build + write to S3 (the I/O shell).

SCOPE: S&P 500+400 (alpha-engine-config-I11525, Brian's ruling 2026-09-24),
the same universe as the live 903-name roster the map is replayed from. It used
to replay S&P 500 changes only against that combined roster, so a name that
moved from the S&P 400 into the S&P 500 dropped out of the universe before its
move, and a name that left the S&P 400 was never restored. The two windows
reach that scope differently, and the artifact's ``scope`` block says which:

  * **On and after the cutover** the membership changes are the diff of each
    snapshot's COMBINED roster (``sp500_tickers`` + ``sp400_tickers``, or the
    combined ``tickers`` list on older snapshots). A 400->500 move is not a
    universe change at all; a name joining or leaving the S&P 400 is. Every
    run replays the finished map against every roster snapshot and reports
    any date where they disagree (:func:`replay_mismatches`). The S&P 500
    slice is still diffed separately, because the Wikipedia reference it is
    attested against is an S&P 500 table.
  * **Before the cutover** only S&P 500 changes exist: the frozen artifact
    is S&P 500 history, and no S&P 400 change history is fetched or
    committed anywhere in this repo. There, a name that joined the S&P 500
    from the S&P 400 is still taken out at its S&P 500 add date, and S&P 400
    departures are not restored. That gap is declared in the artifact, not
    papered over.

Delisted-ticker *prices* are memo Phase 2 — out of scope here; this ships the
membership list.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

#: First date with a dated roster snapshot at
#: ``market_data/weekly/{date}/constituents.json``. On and after this date
#: membership is DIRECTLY OBSERVED and no wiki is consulted to produce it;
#: before it, history comes from the frozen artifact below.
SNAPSHOT_CUTOVER = "2026-04-04"

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: A ticker that leaves the observed roster and comes back within this many
#: calendar days was never out of the index (alpha-engine-config-I11470).
#: Measured: SPY's 2026-09-10 holdings file listed 502 names with OKE missing,
#: and OKE was back on 2026-09-11. Diffed as-is, that is a REMOVED and an ADDED
#: the reference has never heard of, and a PIT universe that drops OKE for one
#: day. S&P does not remove a constituent and re-add it inside a week, so a
#: round trip this short is a gap in the holdings file, not index churn. A
#: genuine change of that shape would still surface, because the reference
#: would list it and the attestation would then report it as not observed.
FLICKER_MAX_DAYS = 7

#: A skipped roster snapshot is DEGRADED, not just logged, when it falls
#: within this many days of the newest snapshot on S3 (alpha-engine-config-
#: I11470). A skipped recent snapshot moves the date this week's changes are
#: attributed to and, when it is the newest one, diffs the week against an
#: older roster. An old skip is still named in the artifact every run.
RECENT_SKIP_WINDOW_DAYS = 14

#: The most SPY/MDY overlap a legacy snapshot may carry and still have its
#: S&P 500 slice recovered from the prefix. See :func:`_sp500_roster`. The
#: overlap is the names moving between the two indices on a rebalance, which
#: both funds hold for a day: measured 1 (2026-04-11, 2026-04-12) and 2
#: (2026-09-18: ILMN and P, joining the S&P 500 at the 2026-09-21 open).
MAX_RECOVERABLE_INDEX_OVERLAP = 10

#: Index changes from 1976 to the cutover are settled history — they do not
#: change, so re-deriving them from an editable wiki on every weekly run buys
#: nothing and costs an outage when the page moves (alpha-engine-config-I6944,
#: 2026-08-11). Reconstructed once, hashed, and committed.
_FROZEN_CHANGES_PATH = (
    Path(__file__).resolve().parent / "data" / "sp500_changes_frozen_pre_2026_04_04.json"
)

#: Ticker changes a holdings diff cannot tell apart from index churn, and
#: which Polygon does not report as ``ticker_change``. Same reasoning as the
#: frozen history: a settled fact, written down once.
_KNOWN_RETICKERS_PATH = (
    Path(__file__).resolve().parent / "data" / "sp500_known_retickers.json"
)

# The changes table is not pinned to one page: on 2026-08-11 a Wikipedia editor
# split it out of "List of S&P 500 companies" into its own article
# ("move to [[Historical components of the S&P 500]], format"), which failed
# this collector 3h later. Both candidates are tried in order and the first
# carrying a date+added+removed table wins, so a future move back — or to a
# third title added here — degrades to a slower fetch rather than a hard stop.
_SP500_CHANGES_URLS = (
    "https://en.wikipedia.org/wiki/Historical_components_of_the_S%26P_500",
    "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
)
_HEADERS = {"User-Agent": "alpha-engine-data/1.0 (historical-constituents)"}

ADDED = "added"
REMOVED = "removed"


@dataclass(frozen=True)
class ConstituentChange:
    """One index-membership change event from the Wikipedia changes table."""

    date: str  # ISO YYYY-MM-DD
    ticker: str
    action: str  # ADDED or REMOVED


def _normalize_ticker(raw: object) -> str | None:
    """Wikipedia uses BRK.B etc.; strip footnote markers + whitespace.

    Returns ``None`` for empty / placeholder cells (the changes table leaves
    the added or removed cell blank when only one side changed)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "—", "-", "none"}:
        return None
    # Drop bracketed footnote refs like "ABC[1]" and trailing notes.
    s = re.sub(r"\[.*?\]", "", s).strip()
    # Keep the symbol token only (uppercase letters, digits, dot, dash).
    m = re.match(r"[A-Z0-9.\-]+", s.upper())
    return m.group(0) if m else None


def _parse_date(raw: object) -> str | None:
    """Parse a changes-table date cell to ISO ``YYYY-MM-DD`` (or None)."""
    if raw is None:
        return None
    s = re.sub(r"\[.*?\]", "", str(raw)).strip()
    if not s or s.lower() == "nan":
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]
    return df


def select_changes_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    """Pick the "Selected changes to the list" table from read_html output.

    Identified by columns: a Date column plus *Added* and *Removed* groups
    (each typically a Ticker sub-column). Mirrors the column-based selection
    in ``constituents._select_constituents_table`` (position is unstable —
    Wikipedia inserts banner tables without notice)."""
    for df in tables:
        flat = _flatten_columns(df)
        cols = [str(c).lower() for c in flat.columns]
        has_date = any("date" in c for c in cols)
        has_added = any("added" in c for c in cols)
        has_removed = any("removed" in c for c in cols)
        if has_date and has_added and has_removed:
            return flat
    raise RuntimeError(
        "No 'Selected changes to the list' table found on the S&P 500 "
        "Wikipedia page (need columns matching date + added + removed). "
        "Wikipedia layout drift — extractor needs update."
    )


def _pick_col(cols: list[str], *, contains: str, prefer: str) -> str | None:
    """Find the column whose lowercased name contains ``contains`` (and,
    when several match, the one also containing ``prefer`` — e.g. the
    'Added Ticker' rather than 'Added Security')."""
    matches = [c for c in cols if contains in c.lower()]
    if not matches:
        return None
    preferred = [c for c in matches if prefer in c.lower()]
    return (preferred or matches)[0]


def parse_changes_table(df: pd.DataFrame) -> list[ConstituentChange]:
    """Wikipedia changes DataFrame -> ordered list of ``ConstituentChange``.

    Each row may carry an addition, a removal, or both. Rows with an
    unparseable date or no valid ticker on either side are skipped. The
    returned list is sorted oldest-first (deterministic replay order)."""
    df = _flatten_columns(df)
    cols = list(df.columns)
    date_col = _pick_col([str(c) for c in cols], contains="date", prefer="date")
    added_ticker_col = _pick_col(
        [str(c) for c in cols], contains="added", prefer="ticker"
    )
    removed_ticker_col = _pick_col(
        [str(c) for c in cols], contains="removed", prefer="ticker"
    )
    if not date_col or not (added_ticker_col or removed_ticker_col):
        raise RuntimeError(
            "Changes table missing a usable date/added/removed column set; "
            f"saw columns {cols}."
        )

    changes: list[ConstituentChange] = []
    for _, row in df.iterrows():
        iso = _parse_date(row.get(date_col))
        if iso is None:
            continue
        if added_ticker_col:
            t = _normalize_ticker(row.get(added_ticker_col))
            if t:
                changes.append(ConstituentChange(iso, t, ADDED))
        if removed_ticker_col:
            t = _normalize_ticker(row.get(removed_ticker_col))
            if t:
                changes.append(ConstituentChange(iso, t, REMOVED))

    changes.sort(key=lambda c: (c.date, c.ticker, c.action))
    return changes


def load_frozen_changes(path: Path = _FROZEN_CHANGES_PATH) -> list[ConstituentChange]:
    """Pre-cutover index changes from the committed, content-hashed artifact.

    The hash is verified, and a mismatch RAISES. This artifact defines the
    universe every 10y backtest runs against; silently accepting an altered
    copy would change what the whole system believes about the past without
    anything failing. Regenerating it legitimately means regenerating the
    hash in the same commit, where a reviewer sees both.
    """
    body = json.loads(path.read_text())
    payload = json.dumps(body["changes"], sort_keys=True, separators=(",", ":")).encode()
    actual = hashlib.sha256(payload).hexdigest()
    if actual != body["content_sha256"]:
        raise RuntimeError(
            f"{path.name} content hash mismatch: recorded "
            f"{body['content_sha256'][:16]}…, computed {actual[:16]}… — the "
            "frozen index history has been altered without its hash being "
            "regenerated. Refusing to build a backtest universe from it."
        )
    return [
        ConstituentChange(c["date"], c["ticker"], c["action"]) for c in body["changes"]
    ]


def same_date_swaps(snapshots: dict[str, list[str]]) -> dict[str, list[str]]:
    """``{date: [tickers that left on that date]}`` where something also joined.

    A ticker RENAME shows up in a holdings diff exactly like an index change:
    the old symbol disappears and the new one appears on the same date. Both
    the 2026-05-22 ``BK`` -> ``BNY`` and the 2026-06-25 ``SATS`` -> ``ECHO``
    reticker did, and treating them as membership events would tell the
    backtester that a company left the index and a different one joined —
    a fabricated churn event in the exact dataset built to make the universe
    honest. This narrows the set worth asking Polygon about; it does not
    decide anything.
    """
    out: dict[str, list[str]] = {}
    dates = sorted(snapshots)
    for prev_date, date in zip(dates, dates[1:]):
        before, after = set(snapshots[prev_date]), set(snapshots[date])
        left, joined = sorted(before - after), sorted(after - before)
        if left and joined:
            out[date] = left
    return out


def changes_from_snapshots(
    snapshots: dict[str, list[str]],
    renames: dict[str, str] | None = None,
) -> tuple[list[ConstituentChange], list[str]]:
    """Dated rosters -> membership changes, by diffing consecutive snapshots.

    ``snapshots`` maps ``YYYY-MM-DD`` to that date's roster (S&P 500+400 for
    membership, the S&P 500 slice for the attestation). A ticker
    present on date D+1 and absent on the preceding snapshot D is ADDED on
    D+1; present on D and absent on D+1 is REMOVED on D+1. Pure — the caller
    does the S3 reads.

    This is the whole point of the collector's rewrite: after the cutover,
    membership is something we OBSERVED rather than something we replayed
    from a third party's prose. The earliest snapshot yields no changes —
    it is the baseline the rest are diffed against, not an event.

    The change is dated to the LATER snapshot because that is the first date
    we can attest to it. A weekly cadence therefore dates a change up to a
    week late; that is a known and bounded imprecision, and it is the honest
    one — claiming the exact effective date would assert something the
    snapshots do not contain.

    ``renames`` maps an old ticker to its new one (the caller resolves these
    via :func:`corporate_actions.detect_renames`); a matching same-date
    disappear/appear pair is dropped, because the company never left the
    index. Returns ``(changes, unresolved)`` where ``unresolved`` names the
    same-date swaps no rename explains — those changes ARE emitted, since a
    swap is far more often real index churn than a rename, but they are
    reported so a run cannot quietly decide either way. See
    :func:`same_date_swaps`.
    """
    renames = renames or {}
    changes: list[ConstituentChange] = []
    unresolved: list[str] = []
    dates = sorted(snapshots)
    for prev_date, date in zip(dates, dates[1:]):
        before = set(snapshots[prev_date])
        after = set(snapshots[date])
        left, joined = before - after, after - before
        renamed_away = {t for t in left if renames.get(t) in joined}
        renamed_into = {renames[t] for t in renamed_away}
        for ticker in sorted(joined - renamed_into):
            changes.append(ConstituentChange(date, ticker, ADDED))
        for ticker in sorted(left - renamed_away):
            changes.append(ConstituentChange(date, ticker, REMOVED))
        still_left, still_joined = left - renamed_away, joined - renamed_into
        if still_left and still_joined:
            unresolved.append(
                f"{date}: out={sorted(still_left)} in={sorted(still_joined)}"
            )
    changes.sort(key=lambda c: (c.date, c.ticker, c.action))
    return changes, unresolved


def suppress_flickers(
    changes: list[ConstituentChange],
    *,
    max_days: int = FLICKER_MAX_DAYS,
) -> tuple[list[ConstituentChange], list[str]]:
    """Drop round trips shorter than ``max_days``; name each one dropped.

    A REMOVED followed by an ADDED of the same ticker (a name missing from a
    holdings file for a day) or an ADDED followed by a REMOVED (a name that
    appeared in one file and was gone the next) cancels out: neither event
    happened to the index. Returns ``(kept, flickers)``, where each flicker
    reads ``"OKE absent 2026-09-10 -> 2026-09-11 (1d)"`` so the artifact says
    which names were affected and when, rather than making them disappear.
    """
    by_ticker: dict[str, list[ConstituentChange]] = {}
    for c in sorted(changes, key=lambda c: (c.date, c.action)):
        by_ticker.setdefault(c.ticker, []).append(c)

    dropped: set[ConstituentChange] = set()
    flickers: list[str] = []
    for ticker, events in sorted(by_ticker.items()):
        i = 0
        while i < len(events) - 1:
            first, second = events[i], events[i + 1]
            gap = (
                datetime.strptime(second.date, "%Y-%m-%d")
                - datetime.strptime(first.date, "%Y-%m-%d")
            ).days
            if first.action != second.action and gap <= max_days:
                dropped.update((first, second))
                shape = "absent" if first.action == REMOVED else "present"
                flickers.append(
                    f"{ticker} {shape} {first.date} -> {second.date} ({gap}d)"
                )
                i += 2
                continue
            i += 1
    kept = [c for c in changes if c not in dropped]
    return kept, flickers


def divergences(
    observed: list[ConstituentChange],
    reference: list[ConstituentChange],
    *,
    since: str,
    until: str | None = None,
) -> list[str]:
    """Human-readable disagreements between two derivations, on/after ``since``.

    The frozen artifact is only as good as the wiki was on the day it was
    frozen, and nothing about a committed file makes it true. Comparing the
    observed post-cutover changes against the same window of the live wiki
    every run is what turns "we froze it and hope" into a claim under test:
    a disagreement means either a bad wiki edit or a gap in our own
    collection, and both are worth knowing about.

    Dates are NOT compared — the snapshot derivation dates a change to the
    first snapshot that shows it, which is up to a cadence-interval later
    than the wiki's effective date. Comparing them would report a
    disagreement on every single change. Membership of the (ticker, action)
    set is the claim being tested.

    ``until`` (the newest roster snapshot) bounds the REFERENCE side: the
    changes table lists a change when it is announced, usually a week or more
    before it takes effect, and no snapshot can have observed a change dated
    after the newest one (alpha-engine-config-I11470). Those are pending, not
    disagreements; see :func:`pending_reference_changes`.
    """
    obs = {(c.ticker, c.action) for c in observed if c.date >= since}
    ref_all = {(c.ticker, c.action) for c in reference if c.date >= since}
    # Only the observable window is owed an observation. An observed change
    # is still matched against the whole table: index funds buy an addition
    # at the close BEFORE its effective date (the 2026-09-18 holdings already
    # carried ILMN and P, effective 2026-09-21), so the snapshot can lead.
    ref = {
        (c.ticker, c.action) for c in reference
        if c.date >= since and (until is None or c.date <= until)
    }
    out = []
    for ticker, action in sorted(obs - ref_all):
        out.append(f"observed {action} {ticker} not in reference")
    for ticker, action in sorted(ref - obs):
        out.append(f"reference {action} {ticker} not observed")
    return out


def pending_reference_changes(
    reference: list[ConstituentChange], *, until: str
) -> list[str]:
    """Reference changes dated after ``until``: announced, not yet owed an
    observation. Named in the artifact so a pending change is visible, and so
    the week it becomes observable is the week it starts being checked."""
    return [
        f"{c.date} {c.action} {c.ticker}"
        for c in sorted(reference, key=lambda c: (c.date, c.ticker, c.action))
        if c.date > until
    ]


def build_pit_membership(
    current_tickers: list[str],
    changes: list[ConstituentChange],
) -> dict[str, list[str]]:
    """Replay ``changes`` backward from ``current_tickers`` to a PIT map.

    Returns ``{change_date: sorted_tickers_immediately_before_that_date}``.
    The membership *after* the most recent change equals the current roster;
    walking each change date from newest to oldest, undo it to recover the
    set that held just before that date:
      * undo an ADDED ticker -> it was NOT a member before that date -> remove
      * undo a REMOVED ticker -> it WAS a member before that date -> add

    Same-date changes are applied as a group so the snapshot for date D is the
    membership the instant before D's changes took effect.
    """
    members = set(current_tickers)
    # Group changes by date, newest first.
    by_date: dict[str, list[ConstituentChange]] = {}
    for c in changes:
        by_date.setdefault(c.date, []).append(c)

    pit: dict[str, list[str]] = {}
    for date in sorted(by_date, reverse=True):
        for c in by_date[date]:
            if c.action == ADDED:
                members.discard(c.ticker)  # wasn't a member before D
            elif c.action == REMOVED:
                members.add(c.ticker)  # was a member before D
        pit[date] = sorted(members)
    return pit


def membership_as_of(
    pit: dict[str, list[str]], current_tickers: list[str], date: str
) -> set[str]:
    """Membership in effect ON ``date``, read the way consumers read the map.

    Same semantics as ``nousergon_lib.arcticdb.pit_membership_as_of``: each
    key holds the roster immediately BEFORE that change date, so the roster
    on ``date`` is the entry for the earliest change date strictly after it.
    On or after the newest change date it is the current roster."""
    later = [d for d in pit if d > date]
    return set(pit[min(later)]) if later else set(current_tickers)


def _round_trip_windows(dropped: list[ConstituentChange]) -> dict[str, list[tuple[str, str]]]:
    """``{ticker: [(from, to)]}`` for each round trip :func:`suppress_flickers`
    dropped. On a date inside ``[from, to)`` the snapshot and the map are
    MEANT to disagree about that ticker: the map says the index never moved."""
    by_ticker: dict[str, list[ConstituentChange]] = {}
    for c in sorted(dropped, key=lambda c: (c.ticker, c.date, c.action)):
        by_ticker.setdefault(c.ticker, []).append(c)
    return {
        t: [(a.date, b.date) for a, b in zip(evs[::2], evs[1::2])]
        for t, evs in by_ticker.items()
    }


def replay_mismatches(
    pit: dict[str, list[str]],
    current_tickers: list[str],
    universe: dict[str, list[str]],
    *,
    renames: dict[str, str] | None = None,
    dropped: list[ConstituentChange] | None = None,
) -> list[str]:
    """Replay the finished map against every observed S&P 500+400 roster.

    For each dated roster snapshot, the membership the map gives for that
    date must be the roster the snapshot recorded (alpha-engine-config-I11525:
    this is the check the map is closed against, run on every build rather
    than once). Two differences are expected and are not reported:

      * a reticker (``renames``, old -> new): the map carries the NEW symbol
        through the old one's dates, because the company never left;
      * a holdings-file round trip :func:`suppress_flickers` dropped: the map
        keeps the name, because the index never moved.

    Anything else is named, per date, as the names the map is missing and the
    names it has that the snapshot did not.
    """
    renames = renames or {}
    windows = _round_trip_windows(list(dropped or []))
    out: list[str] = []
    for date in sorted(universe):
        expected = {renames.get(t, t) for t in universe[date]}
        actual = membership_as_of(pit, current_tickers, date)
        exempt = {t for t, ws in windows.items() if any(a <= date < b for a, b in ws)}
        missing = sorted(expected - actual - exempt)
        extra = sorted(actual - expected - exempt)
        if missing or extra:
            out.append(f"{date}: missing={missing} extra={extra}")
    return out


def _fetch_changes_table(
    urls: tuple[str, ...] = _SP500_CHANGES_URLS,
) -> tuple[pd.DataFrame, str]:
    """Return the changes table and the URL it actually came from.

    Tries each candidate in order; a fetch error or a page with no matching
    table falls through to the next. Raises only when every candidate fails,
    naming what was tried and why each one did not serve."""
    failures = []
    for url in urls:
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            df = select_changes_table(pd.read_html(StringIO(resp.text)))
        except Exception as exc:  # noqa: BLE001 — recorded and re-raised below
            failures.append(f"{url}: {type(exc).__name__}: {exc}")
            continue
        if failures:
            logger.warning(
                "historical_constituents: changes table served by fallback %s "
                "(earlier candidates failed: %s)", url, "; ".join(failures),
            )
        return df, url
    raise RuntimeError(
        "No S&P 500 'Selected changes to the list' table found on any known "
        "Wikipedia page (need columns matching date + added + removed). "
        "Wikipedia layout drift — add the new article to _SP500_CHANGES_URLS. "
        "Tried: " + " | ".join(failures)
    )


def sp500_roster_with_provenance(snapshot: dict) -> tuple[list[str] | None, str]:
    """The S&P 500 slice of one dated ``constituents.json``, and how it was read.

    ``constituents.collect`` writes a single combined ``tickers`` list: the SPY
    holdings followed by the MDY holdings, deduped keeping the FIRST
    occurrence, plus ``sp500_count`` / ``sp400_count``. Newer snapshots carry
    explicit ``sp500_tickers`` and are read directly (provenance
    ``"explicit"``). Older ones are read from the prefix, and only when the
    counts say the prefix is the SPY batch:

    * ``sp500_count + sp400_count == len(tickers)``: nothing was deduped, the
      prefix is exactly the SPY batch (``"prefix"``).
    * The list is SHORTER than the counts by a small overlap (at most
      :data:`MAX_RECOVERABLE_INDEX_OVERLAP`): a name held by both funds on a
      rebalance day. The dedupe keeps the first occurrence and SPY comes
      first, so the overlap is removed from the MDY tail and the first
      ``sp500_count`` entries are still the SPY batch
      (``"prefix_recovered_overlap_<n>"``). alpha-engine-config-I11470: this
      shape was refused before, which skipped 2026-04-11, 2026-04-12 and
      2026-09-18 (the last snapshot before that week's run, with ILMN and P
      held by both funds ahead of their 2026-09-21 move into the S&P 500).

    Anything else returns ``(None, <reason>)`` and the caller names the
    snapshot as skipped: a list LONGER than the counts, one shorter than the
    S&P 500 slice itself, a prefix with repeated names, or an overlap too
    large to be a rebalance. Slicing those would put names on the roster or
    take them off it, which the diff would emit as index churn that did not
    happen.
    """
    explicit = snapshot.get("sp500_tickers")
    if explicit:
        return list(explicit), "explicit"
    tickers = snapshot.get("tickers") or []
    n500 = snapshot.get("sp500_count") or 0
    n400 = snapshot.get("sp400_count") or 0
    if not tickers or not n500:
        return None, (
            "no sp500_tickers and no sp500_count (a cache-served roster "
            "carries no per-index split)"
        )
    overlap = n500 + n400 - len(tickers)
    if overlap == 0:
        return list(tickers[:n500]), "prefix"
    if overlap < 0:
        return None, (
            f"tickers ({len(tickers)}) exceeds sp500_count + sp400_count "
            f"({n500 + n400})"
        )
    prefix = tickers[:n500]
    if len(prefix) < n500 or len(set(prefix)) != n500:
        return None, f"prefix of {n500} is not {n500} distinct names"
    if overlap > MAX_RECOVERABLE_INDEX_OVERLAP:
        return None, (
            f"overlap of {overlap} between the two funds exceeds "
            f"{MAX_RECOVERABLE_INDEX_OVERLAP}, larger than any rebalance"
        )
    return list(prefix), f"prefix_recovered_overlap_{overlap}"


def _sp500_roster(snapshot: dict) -> list[str] | None:
    """The S&P 500 slice of one snapshot, or None when it cannot be read.

    See :func:`sp500_roster_with_provenance`."""
    return sp500_roster_with_provenance(snapshot)[0]


def universe_roster_with_provenance(snapshot: dict) -> tuple[list[str] | None, str]:
    """The S&P 500+400 roster of one dated ``constituents.json``.

    This is the scope the point-in-time map is built in
    (alpha-engine-config-I11525). Unlike the S&P 500 slice it needs no
    ordering contract: newer snapshots carry both explicit per-index lists
    (``"explicit"``), and on older ones the combined ``tickers`` list IS the
    union of the two funds' holdings (``"tickers"``). A rebalance-day overlap
    shortens that list without making it wrong, since a name both funds hold
    is one member of the universe.

    A snapshot with no per-index counts at all is a cache-served roster: a
    copy of an older day's list, not an observation of this one. Diffing it
    would date whatever changed since the cache was written to the wrong day,
    so it returns ``(None, <reason>)`` and the caller names it as skipped.
    """
    s500 = snapshot.get("sp500_tickers")
    s400 = snapshot.get("sp400_tickers")
    if s500 and s400:
        return list(dict.fromkeys([*s500, *s400])), "explicit"
    tickers = snapshot.get("tickers") or []
    if not tickers:
        return None, "no tickers"
    if not (s500 or s400 or snapshot.get("sp500_count") or snapshot.get("sp400_count")):
        return None, (
            "no sp500_count/sp400_count (a cache-served roster is a copy of an "
            "earlier day's list, not an observation of this one)"
        )
    return list(dict.fromkeys(tickers)), "tickers"


@dataclass
class RosterSnapshots:
    """Every dated roster on S3, and what happened to each one that did not
    go in as-is. ``skipped`` and ``recovered`` are ``{date: reason}``.

    Two views of the same files (alpha-engine-config-I11525): ``universe``
    (S&P 500+400) is what membership is built from, and ``snapshots`` (the
    S&P 500 slice) is what the S&P 500 reference is attested against.
    ``skipped`` names snapshots whose S&P 500 slice could not be read;
    ``universe_skipped`` names those whose combined roster could not be.
    """

    snapshots: dict[str, list[str]] = field(default_factory=dict)
    skipped: dict[str, str] = field(default_factory=dict)
    recovered: dict[str, str] = field(default_factory=dict)
    universe: dict[str, list[str]] = field(default_factory=dict)
    universe_skipped: dict[str, str] = field(default_factory=dict)

    def newest_date(self) -> str | None:
        dates = [*self.snapshots, *self.skipped, *self.universe, *self.universe_skipped]
        return max(dates) if dates else None

    def recent_skips(self, window_days: int = RECENT_SKIP_WINDOW_DAYS) -> list[str]:
        """Skipped dates (either view) within ``window_days`` of the newest snapshot."""
        newest = self.newest_date()
        if newest is None:
            return []
        anchor = datetime.strptime(newest, "%Y-%m-%d")
        return sorted(
            d for d in {*self.skipped, *self.universe_skipped}
            if (anchor - datetime.strptime(d, "%Y-%m-%d")).days <= window_days
        )

    def skip_reason(self, date: str) -> str:
        """Why ``date`` was skipped, naming which view(s) could not be read."""
        parts = []
        if date in self.universe_skipped:
            parts.append(f"S&P 500+400 roster: {self.universe_skipped[date]}")
        if date in self.skipped:
            parts.append(
                self.skipped[date] if not parts
                else f"S&P 500 slice: {self.skipped[date]}"
            )
        return "; ".join(parts)


def load_roster_snapshots(
    bucket: str,
    s3=None,
    prefix: str = "market_data/weekly/",
) -> RosterSnapshots:
    """Read every dated ``constituents.json`` under ``prefix`` from S3.

    A snapshot whose S&P 500 slice cannot be established is skipped, and
    NAMED with its reason in the result — never only in a log line. See
    :func:`sp500_roster_with_provenance`.
    """
    s3 = s3 or boto3.client("s3")
    out = RosterSnapshots()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/constituents.json"):
                continue
            date = key[len(prefix):].split("/", 1)[0]
            if not _ISO_DATE_RE.match(date):
                continue
            body = json.loads(
                s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            )
            universe, why = universe_roster_with_provenance(body)
            if universe is None:
                out.universe_skipped[date] = why
            else:
                out.universe[date] = universe
            roster, how = sp500_roster_with_provenance(body)
            if roster is None:
                out.skipped[date] = how
                continue
            out.snapshots[date] = roster
            if how.startswith("prefix_recovered"):
                out.recovered[date] = how
    if out.recovered:
        logger.info(
            "historical_constituents: %d legacy roster snapshot(s) read from the "
            "SPY prefix despite a SPY/MDY overlap: %s",
            len(out.recovered), out.recovered,
        )
    if out.skipped:
        logger.warning(
            "historical_constituents: %d roster snapshot(s) skipped (recent: %s): %s",
            len(out.skipped), out.recent_skips(), out.skipped,
        )
    if out.universe_skipped:
        logger.warning(
            "historical_constituents: %d S&P 500+400 roster snapshot(s) skipped: %s",
            len(out.universe_skipped), out.universe_skipped,
        )
    return out


@dataclass
class RenameResolution:
    """What the rename pass decided, and what it could not.

    ``deferred`` names every candidate whose Polygon query failed (e.g. still
    rate-limited after the client's backoff). It is carried into the artifact
    and the stage result by name rather than only logged
    (alpha-engine-config-I11470): a check that did not run is a fact about
    this run, and the next run asks again. ``confirmed_by_reference`` names
    candidates not asked about because the reference already lists them as
    index removals.
    """

    renames: dict[str, str] = field(default_factory=dict)
    deferred: list[str] = field(default_factory=list)
    confirmed_by_reference: list[str] = field(default_factory=list)
    #: Candidates Polygon ANSWERED with no ticker change. Published, with the
    #: renames Polygon found, as the settled verdicts the next run reuses.
    not_renamed: list[str] = field(default_factory=list)
    #: Candidates not asked about because an earlier run's Polygon answer
    #: for them is carried in ``prior`` (alpha-engine-config-I11525).
    settled_by_prior_run: list[str] = field(default_factory=list)

    def settled(self) -> dict:
        """The verdicts Polygon gave, in the shape :func:`resolve_renames`
        accepts back as ``prior``. Committed retickers are not repeated here:
        they live in the committed file and are read from it every run."""
        known = {
            r["old"] for r in json.loads(_KNOWN_RETICKERS_PATH.read_text())["retickers"]
        }
        return {
            "renamed": {o: n for o, n in sorted(self.renames.items()) if o not in known},
            "not_renamed": sorted(self.not_renamed),
        }


def resolve_renames(
    swaps: dict[str, list[str]],
    *,
    reference_removed: set[str] | None = None,
    prior: dict | None = None,
) -> RenameResolution:
    """Ask Polygon which of the disappearing tickers were retickers.

    Reuses ``corporate_actions.detect_renames`` — the same detection the
    prune path runs, so a reticker is classified once in this repo rather
    than twice with two answers. Imported lazily: this module's pure layer
    is unit-tested without the polygon client or its config.

    ``reference_removed`` is the set of tickers the reference changes table
    lists as REMOVED since the cutover. A candidate in it is a confirmed index
    removal and is not queried (alpha-engine-config-I11470). Before this, every
    same-date swap since the cutover was re-queried every week, one call per
    ticker against a 5-calls-a-minute key the rest of the fleet shares: 12
    candidates on 2026-09-23, a number that only grows, and POOL ran out of
    429 retries. Only the swaps the reference does NOT explain need Polygon.
    ``None`` (reference unavailable) queries every candidate, as before.

    ``prior`` is the previous run's published ``rename_checks`` block
    (``{"renamed": {old: new}, "not_renamed": [...]}``): a candidate Polygon
    has already ANSWERED for is not asked again (alpha-engine-config-I11525).
    The S&P 500+400 diff meets every S&P 400 add/remove pair as a same-date
    swap, and the S&P 500 reference explains none of them, so without this
    each weekly run would re-query every S&P 400 departure since the cutover,
    a list that only grows, against the shared rate-limited key. A deferred
    candidate is never carried: it has no answer, and it is asked again.

    A detection failure returns no rename for that candidate, which lands
    the pair in ``unresolved`` and emits it as index churn, and the candidate
    is named in ``deferred``. That is the opposite of the prune path's
    history-safety default, and deliberately so: prune DELETES history on a
    wrong answer, where this only mis-dates one membership event that the
    attestation then flags.
    """
    out = RenameResolution()
    if not swaps:
        return out
    candidates = sorted({t for tickers in swaps.values() for t in tickers})

    # Committed retickers first. Polygon does NOT report these as
    # ticker_change — verified 2026-08-12, `get_ticker_events("BK")` returns
    # `[]` for the BNY Mellon rebrand — so detection alone would emit them as
    # index churn. A reticker is settled history exactly like a pre-cutover
    # index change, so it gets the same treatment: written down once, reviewed
    # in a diff, and not re-derived weekly from a source that does not have it.
    known = json.loads(_KNOWN_RETICKERS_PATH.read_text())
    out.renames = {
        r["old"]: r["new"]
        for r in known["retickers"]
        if r["old"] in candidates
    }
    candidates = [t for t in candidates if t not in out.renames]
    if prior:
        prior_renamed = dict(prior.get("renamed") or {})
        prior_not = set(prior.get("not_renamed") or [])
        out.settled_by_prior_run = [
            t for t in candidates if t in prior_renamed or t in prior_not
        ]
        out.renames.update({t: prior_renamed[t] for t in candidates if t in prior_renamed})
        out.not_renamed = [t for t in candidates if t in prior_not and t not in prior_renamed]
        candidates = [t for t in candidates if t not in out.settled_by_prior_run]
    if reference_removed is not None:
        out.confirmed_by_reference = [t for t in candidates if t in reference_removed]
        candidates = [t for t in candidates if t not in reference_removed]
    if candidates:
        try:
            from builders.prune_delisted_tickers import _build_rename_client

            import corporate_actions as ca

            client = _build_rename_client()
            if client is None:
                raise RuntimeError("no polygon client for rename detection")
            detection = ca.detect_renames(candidates, client=client)
        except Exception as exc:  # noqa: BLE001 — recorded as `deferred`, and callers warn
            logger.warning(
                "historical_constituents: rename detection unavailable (%s) — %d "
                "same-date swap(s) will be emitted as index changes unclassified: %s",
                exc, len(candidates), candidates,
            )
            out.deferred = list(candidates)
            return out
        found = {a.ticker: a.new_ticker for a in detection.renames}
        out.renames.update(found)
        out.deferred = sorted(detection.failed_candidates)
        out.not_renamed = sorted({
            *out.not_renamed,
            *(t for t in candidates if t not in found and t not in out.deferred),
        })
    if out.renames:
        logger.info(
            "historical_constituents: %d reticker(s) resolved and excluded "
            "from membership changes: %s", len(out.renames), out.renames,
        )
    if out.deferred:
        logger.warning(
            "historical_constituents: rename check DEFERRED for %s (query failed "
            "after the client's backoff); asked again next run",
            out.deferred,
        )
    return out


def _verdict(
    *,
    unexplained: list[str],
    recent_skips: dict[str, str],
    unresolved: list[str],
    deferred: list[str],
    replay: list[str] | tuple = (),
) -> tuple[str, str | None]:
    """The stage's own status and, when DEGRADED, the defect named in full.

    DEGRADED means the artifact was published and is known to be wrong
    somewhere (`_DegradedRun`, alpha-engine-config-I10784). Two conditions
    earn it (alpha-engine-config-I11470): a reference disagreement nothing
    explains, and a recent roster snapshot that could not be read. An
    unexplained disagreement on a swap whose rename check was deferred names
    the deferral too, since the check that could have explained it did not
    run. A third (alpha-engine-config-I11525): a roster snapshot the finished
    map does not reproduce (:func:`replay_mismatches`).
    """
    parts = []
    if replay:
        parts.append(
            f"{len(replay)} roster snapshot(s) the S&P 500+400 map does not "
            f"reproduce: {list(replay)[:10]}"
        )
    if unexplained:
        parts.append(
            f"{len(unexplained)} unexplained reference disagreement(s): {unexplained}"
            + (f" (unresolved swaps: {unresolved})" if unresolved else "")
            + (f" (rename check deferred for: {deferred})" if deferred else "")
        )
    if recent_skips:
        parts.append(
            f"{len(recent_skips)} recent roster snapshot(s) skipped: {recent_skips}"
        )
    if not parts:
        return "ok", None
    return "degraded", "historical_constituents: " + "; ".join(parts)


def load_prior_rename_checks(
    bucket: str, s3_prefix: str = "market_data/", s3=None,
) -> tuple[dict | None, str | None]:
    """The ``rename_checks`` block the previous build published, or ``None``.

    Returns ``(prior, error)``. A missing or unreadable previous artifact is
    not a failure of this build: it only means every candidate is asked
    about again, which is what every run did before
    alpha-engine-config-I11525. The error is returned so the artifact records
    that the carry-forward did not happen.
    """
    s3 = s3 or boto3.client("s3")
    key = f"{s3_prefix}historical_constituents.json"
    try:
        body = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        prior = body.get("rename_checks")
        if prior is not None and not isinstance(prior, dict):
            raise TypeError(f"rename_checks is {type(prior).__name__}, not an object")
    except Exception as exc:  # noqa: BLE001 — returned, logged and recorded by the caller
        return None, f"{type(exc).__name__}: {exc}"
    return prior, None


def collect(
    bucket: str,
    current_tickers: list[str],
    s3_prefix: str = "market_data/",
    dry_run: bool = False,
) -> dict:
    """Build the point-in-time S&P 500+400 membership map and write to S3.

    ``current_tickers`` is today's roster (the caller already has it from
    ``constituents.collect``); this avoids a second live fetch of the live
    roster and keeps the two collectors' rosters consistent. Writes
    ``{s3_prefix}historical_constituents.json`` per the memo's recommended
    path.

    Two producers, joined at ``SNAPSHOT_CUTOVER``: the committed frozen
    artifact for settled pre-cutover history, and a diff of our own dated
    roster snapshots for everything after. Wikipedia is fetched only to
    ATTEST the post-cutover window and never to produce it, so a page that
    moves or 404s costs an attestation, not the pipeline
    (alpha-engine-config-I6946).

    Returns ``status="degraded"`` with a ``detail`` naming every defect when
    the attestation finds a disagreement nothing explains or a recent roster
    snapshot was skipped (alpha-engine-config-I11470), or when the finished
    map does not reproduce a roster snapshot (alpha-engine-config-I11525).
    The counts and names ride the returned dict as well as the artifact, so
    the run manifest and the DEGRADED alert carry them.

    Membership is built in S&P 500+400 scope from the COMBINED roster
    snapshots; the S&P 500 slice is diffed separately, only to attest it
    against the S&P 500 reference (see the module docstring's SCOPE).
    """
    frozen = load_frozen_changes()
    rosters = load_roster_snapshots(bucket)
    snapshots = rosters.snapshots
    universe = rosters.universe
    newest = max(snapshots) if snapshots else None

    # The reference is fetched FIRST so it can narrow which swaps need a
    # Polygon rename query (see resolve_renames). Non-fatal by deliberate
    # carve-out from fail-loud: (a) the failure swallowed is an unreachable
    # or restructured wiki page, which says nothing about the membership map
    # built from two sources that do not involve it; (b) it is recorded as a
    # WARNING on this phase's log and as `attestation` in the written
    # artifact, so a run that could not attest is distinguishable from one
    # that attested cleanly — `divergences` null is not the same value as `[]`.
    reference: list[ConstituentChange] | None = None
    attestation: dict = {"status": "skipped", "divergences": None}
    try:
        reference = parse_changes_table(_fetch_changes_table()[0])
    except Exception as exc:  # noqa: BLE001 — recorded above and below
        attestation["status"] = "unavailable"
        attestation["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "historical_constituents: post-cutover attestation UNAVAILABLE "
            "(%s) — membership was still built from the frozen artifact + %d "
            "observed snapshots; nothing cross-checked it this run",
            exc, len(snapshots),
        )

    reference_removed = (
        {c.ticker for c in reference if c.action == REMOVED and c.date >= SNAPSHOT_CUTOVER}
        if reference is not None else None
    )
    # Rename candidates come from the S&P 500+400 diff. Every reticker of an
    # S&P 500 member is a swap there too, and a name that merely moved
    # between the two indices is not a swap at all, so it is never asked
    # about. Verdicts Polygon already gave are carried from the previous
    # build (alpha-engine-config-I11525; see resolve_renames).
    prior, prior_error = load_prior_rename_checks(bucket, s3_prefix)
    if prior_error:
        logger.warning(
            "historical_constituents: previous rename verdicts unreadable (%s) — "
            "every rename candidate is asked about this run", prior_error,
        )
    resolution = resolve_renames(
        same_date_swaps(universe), reference_removed=reference_removed, prior=prior,
    )
    renames = resolution.renames

    # Membership: the S&P 500+400 diff.
    raw_observed, unresolved = changes_from_snapshots(universe, renames)
    observed, flickers = suppress_flickers(raw_observed)
    kept = set(observed)
    dropped = [c for c in raw_observed if c not in kept]
    # Attestation only: the S&P 500 slice, diffed the same way.
    observed_sp500, unresolved_sp500 = changes_from_snapshots(snapshots, renames)
    observed_sp500, flickers_sp500 = suppress_flickers(observed_sp500)
    flickers = sorted({*flickers, *flickers_sp500})
    if flickers:
        logger.info(
            "historical_constituents: %d holdings-file round trip(s) shorter than "
            "%dd dropped (not index changes): %s",
            len(flickers), FLICKER_MAX_DAYS, flickers,
        )
    changes = sorted(
        frozen + observed, key=lambda c: (c.date, c.ticker, c.action)
    )
    pit = build_pit_membership(current_tickers, changes)
    replay = replay_mismatches(
        pit, current_tickers, universe, renames=renames, dropped=dropped,
    )
    if replay:
        logger.warning(
            "historical_constituents: the S&P 500+400 map does NOT reproduce %d "
            "roster snapshot(s): %s", len(replay), replay[:10],
        )

    found: list[str] = []
    if reference is not None:
        found = divergences(
            observed_sp500, reference, since=SNAPSHOT_CUTOVER, until=newest,
        )
        attestation = {
            "status": "diverged" if found else "agreed",
            "divergences": found,
            "since": SNAPSHOT_CUTOVER,
            "until": newest,
            "pending_reference_changes": (
                pending_reference_changes(reference, until=newest) if newest else []
            ),
        }
        if found:
            # An unresolved same-date swap is NOT reported on its own: a real
            # index change is a swap most weeks (one name in, one out), so
            # warning on every one of them would fire on healthy runs and be
            # tuned out long before the week it meant something. It becomes
            # interesting only where it ALSO diverges from the reference —
            # which is exactly how the BK->BNY and SATS->ECHO retickers were
            # caught on this collector's first observed run.
            logger.warning(
                "historical_constituents: observed membership DISAGREES with "
                "the reference on %d change(s) since %s — a bad upstream edit, "
                "a gap in our snapshots, or a reticker missing from %s: %s "
                "(unresolved same-date swaps this run: %s)",
                len(found), SNAPSHOT_CUTOVER, _KNOWN_RETICKERS_PATH.name,
                found[:20], unresolved_sp500[:20],
            )

    recent_skips = {d: rosters.skip_reason(d) for d in rosters.recent_skips()}
    status, detail = _verdict(
        unexplained=found, recent_skips=recent_skips,
        unresolved=unresolved_sp500, deferred=resolution.deferred,
        replay=replay,
    )
    # One block, written into the artifact AND returned, so the published
    # file, the run manifest and the DEGRADED alert read the same numbers.
    quality = {
        "n_reference_disagreements": len(found),
        "reference_disagreements": found,
        "n_skipped_snapshots": len(rosters.skipped),
        "skipped_snapshots": dict(sorted(rosters.skipped.items())),
        "n_skipped_universe_snapshots": len(rosters.universe_skipped),
        "skipped_universe_snapshots": dict(sorted(rosters.universe_skipped.items())),
        "n_replayed_snapshots": len(universe),
        "n_replay_mismatches": len(replay),
        "replay_mismatches": replay,
        "n_recent_skipped_snapshots": len(recent_skips),
        "recent_skipped_snapshots": recent_skips,
        "recovered_snapshots": dict(sorted(rosters.recovered.items())),
        "snapshot_flickers": flickers,
        "rename_checks_deferred": resolution.deferred,
        "rename_checks_confirmed_by_reference": resolution.confirmed_by_reference,
        "rename_checks_settled_by_prior_run": resolution.settled_by_prior_run,
        "rename_checks_prior_error": prior_error,
        "attestation_status": attestation["status"],
    }

    result = {
        "schema_version": 2,
        "source": {
            "frozen": _FROZEN_CHANGES_PATH.name,
            "observed": f"s3://{bucket}/market_data/weekly/*/constituents.json",
            "cutover": SNAPSHOT_CUTOVER,
        },
        "index": "S&P 500+400",
        # Which index changes each window is built from. Read this before
        # trusting a pre-cutover date as S&P 500+400 membership: it is not.
        "scope": {
            "universe": "S&P 500+400",
            "observed": {
                "from": SNAPSHOT_CUTOVER,
                "changes": "S&P 500+400 (diff of the combined roster snapshots)",
            },
            "frozen": {
                "before": SNAPSHOT_CUTOVER,
                "changes": "S&P 500 only",
                "gap": (
                    "no S&P 400 change history is fetched or committed: a name "
                    "that joined the S&P 500 from the S&P 400 is absent before "
                    "its S&P 500 add date, and S&P 400 departures are not "
                    "restored (alpha-engine-config-I11525)"
                ),
            },
        },
        "current_count": len(current_tickers),
        "n_changes": len(changes),
        "n_changes_frozen": len(frozen),
        "n_changes_observed": len(observed),
        "n_changes_observed_sp500": len(observed_sp500),
        "n_roster_snapshots": len(universe),
        "renames_excluded": renames,
        # Polygon's answers, reused by the next build (see resolve_renames).
        "rename_checks": resolution.settled(),
        "unresolved_swaps": unresolved,
        "unresolved_swaps_sp500": unresolved_sp500,
        "n_snapshots": len(pit),
        "attestation": attestation,
        "quality": {"status": status, "detail": detail, **quality},
        "membership": pit,  # {date: [tickers as-of just before that date]}
        "built_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        logger.info(
            "[dry-run] historical_constituents: %d changes (%d frozen + %d "
            "observed from %d roster snapshots) -> %d PIT snapshots "
            "(current roster %d); attestation=%s; verdict=%s %s",
            len(changes), len(frozen), len(observed), len(universe),
            len(pit), len(current_tickers), attestation["status"], status,
            detail or "",
        )
        return {
            "status": "ok_dry_run", "n_changes": len(changes),
            "n_snapshots": len(pit), "verdict": status, **quality,
        }

    s3 = boto3.client("s3")
    key = f"{s3_prefix}historical_constituents.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(result, indent=2),
        ContentType="application/json",
    )
    logger.info(
        "Wrote historical_constituents.json to s3://%s/%s (%d changes, %d snapshots)",
        bucket, key, len(changes), len(pit),
    )
    if detail:
        logger.warning("%s", detail)
    out = {
        "status": status, "n_changes": len(changes), "n_snapshots": len(pit),
        **quality,
    }
    if detail:
        out["detail"] = detail
    return out
