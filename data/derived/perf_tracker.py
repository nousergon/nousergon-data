"""Performance tracker producer: window x comparison returns from the ledger NAV.

alpha-engine-config-I12798 (epic), slice C11. Brian, 2026-10-28: a console
performance tracker with windows Day, WTD, MTD, YTD, YoY, LTM and since
inception; for each, the absolute return, the return vs the S&P 500 (SPY total
return), vs the Nasdaq-100 (QQQ total return), and alpha. YoY and LTM read
"insufficient history" until 365 days exist. The console page and the Sunday
report RENDER this artifact; neither computes a return
(`portfolio-accounting-policy` Rule 10).

Output
------
``s3://<bucket>/ops/perf/dated/{as_of}.json`` first, then
``s3://<bucket>/ops/perf/latest.json`` (body first, pointer second: a crash
between the two leaves ``latest.json`` on the previous coherent day). Nothing
else is written, and nothing the trader or executor reads is touched.

Inputs and their basis (named, never silently swapped; Rule 5)
---------------------------------------------------------------
* **NAV**: ``trades/eod_pnl.csv``, the EOD export of ``trades.db::eod_pnl``,
  which the money ledger (crucible-executor ``executor/money_ledger.py``)
  journals corrections against. Its ``nav_basis`` column is carried through;
  a basis change inside a window is reported on that window. The ground-truth
  basis is ``settled_close`` (IB shares and cash, Polygon settled closes;
  ruling 2026-10-22): a window whose base or any later row is on another
  basis (``ib_netliq``, or ``unrecorded`` before the column existed) is
  estimated and names the first and last such row
  (``nav_basis_not_settled_close:<first>..<last>``). The number is still
  published; it is never re-based onto a series the export did not publish.
* **External flows**: transfer entries in the ledger journal copies under
  ``trades/ledger/journal/``. The ledger has no transfer builder yet
  (alpha-engine-config-I12557 deliverable 2), so :data:`FLOWS_JOURNALED_FROM`
  is ``None`` and every cell carries the ``external_flows_not_journaled``
  estimate reason. When the builder lands, that constant moves to its first
  date in the same change and the reason drops for later windows.
  Until then (alpha-engine-config-I12806) an interval ``(d_{t-1}, d_t]`` is
  PROVEN flow-free by the v2 book of record (crucible-trader ``ledger.py``)
  when, cents exact: both rows' ``total_cash`` and share counts equal IB's in
  that day's ledger reconciliation, the ledger equals IB on both days, day
  ``t``'s roll-forward ties with no break and no missing journal, its journal
  holds only trading and income kinds (:data:`V2_NON_FLOW_KINDS`), and no
  trading day lies between the rows. A window whose every interval is proven
  drops ``external_flows_not_journaled``; any other window keeps it. Absent or
  unreadable records prove nothing and change nothing else.
* **SPY total return**: each ``eod_pnl`` row's persisted ``spy_return_pct``,
  the EOD reconcile's total-return leg on Polygon settled closes and
  ex-dividends (the same figure ``daily_alpha_pct`` is struck against, and
  restated by the T+1 audit). A row without it is rebuilt from that row's
  ``spy_close`` + ``spy_dividend_per_share`` and, failing that, from
  ``market_data/close_history/SPY.json`` (dividend-adjusted); either fallback
  marks the cell estimated and names the row.
* **QQQ total return**: each ``eod_pnl`` row's persisted ``qqq_close`` +
  ``qqq_dividend_per_share`` (alpha-engine-config-I13096), which the EOD
  reconcile reads from the same ArcticDB macro close and the same Polygon
  ex-dividend query as ``spy_close`` / ``spy_dividend_per_share``. An interval
  whose either row lacks ``qqq_close`` (every row before the columns existed)
  falls back to ``market_data/close_history/QQQ.json`` (dividend-adjusted),
  and its window carries ``qqq_not_polygon_settled``.
* **Nasdaq Composite and Russell 2000** (Brian, 2026-11-03): ONEQ and IWM total
  return from ``market_data/close_history/{ONEQ,IWM}.json`` (dividend-adjusted),
  the same index-proxy ETFs Metron's markets strip uses. The index VALUES
  (^IXIC, ^RUT, like ^GSPC and ^NDX) carry a separate licence, so the fleet
  carries tradeable proxies (``collectors/metron_market_data.py``
  ``INDEX_PROXY_SYMBOLS``). The EOD reconcile persists no settled close for
  either yet, so each cell carries ``<sym>_not_polygon_settled``; an
  unreadable history leaves the cell null and names it, never a fabricated 0.
* **Personal portfolio** (Brian, 2026-11-03): Brian's own household, from
  ``metron/perf/personal_returns.json``, which Metron's daily refresh
  publishes: one flow-neutralised daily return per consecutive pair of NAV
  snapshots (Metron's own TWR convention, the same as this file's), and no
  dollar figure. Chained over the same window dates. A series that starts
  after the window's base leaves the cell null (``personal_series_starts``);
  a missing base row, a gap, a late end or an estimated Metron snapshot marks
  it estimated and names the dates. Optional: absent or unreadable, the cell
  says ``personal_series_unavailable`` and every other cell is unaffected.

Return definitions
------------------
Time-weighted, chain-linked over consecutive NAV rows. A row's factor is
``(NAV_t - F_t) / NAV_{t-1}``, ``F_t`` the net external flow (deposit +) in
``(d_{t-1}, d_t]``, taken to land after the close; so a deposit is never
performance. Benchmarks are chained over exactly the same intervals. Alpha is
the excess over SPY (``R_p - R_spy``); the beta-adjusted alpha is
``R_p - beta * R_spy`` with beta the OLS slope of daily portfolio returns on
daily SPY returns over the window, published only with at least
:data:`MIN_BETA_OBSERVATIONS` days. No risk-free leg.

Risk (alpha-engine-config-I12784): each window also carries a ``risk`` block
over the same daily portfolio returns (volatility, Sharpe, Sortino, max
drawdown, tracking error, information ratio, up days, days beating SPY), and
the document carries ``underwater_pct``, the since-inception drawdown series
behind the max-drawdown figure. Definitions travel in ``definitions.risk``.
The block inherits the window's estimate reasons; turnover stays null until a
trade-level input is read.

Growth (alpha-engine-config-I12798): ``growth_pct`` holds, per series
(``portfolio``, ``spy``, ``qqq``, ``oneq``, ``iwm``), the cumulative return at
every NAV row since inception, chained over exactly the inception window's
intervals, so its last point is that window's cell; ``growth_reasons`` carries
each series' estimate reasons. The console overlays them as lines.

Determinism: the same inputs and ``now`` give the same bytes. The CLI logs
counts, dates and keys only, never a value (this repository and its run logs
are public).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import math
import statistics
import sys
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from collections.abc import Iterable, Mapping, Sequence
import dataclasses
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "perf_tracker.v1"

NAV_KEY = "trades/eod_pnl.csv"
JOURNAL_PREFIX = "trades/ledger/journal/"
CLOSE_HISTORY_KEY = "market_data/close_history/{symbol}.json"
#: Brian's personal household returns, published by Metron's daily refresh.
PERSONAL_KEY = "metron/perf/personal_returns.json"
PERSONAL_SCHEMA_VERSION = "metron_personal_returns.v1"
OUTPUT_PREFIX = "ops/perf/"
LATEST_KEY = f"{OUTPUT_PREFIX}latest.json"
DATED_KEY = f"{OUTPUT_PREFIX}dated/{{as_of}}.json"

STATUS_OK = "ok"
STATUS_INSUFFICIENT = "insufficient_history"
STATUS_ESTIMATED = "estimated"
STATUSES = (STATUS_OK, STATUS_INSUFFICIENT, STATUS_ESTIMATED)

#: YoY and LTM need a full year of history (Brian, 2026-10-28).
YEAR_WINDOW_MIN_DAYS = 365
#: Fewer daily observations than this and beta is not published.
MIN_BETA_OBSERVATIONS = 60
#: Fewer daily observations than this and the ANNUALISED risk metrics
#: (volatility, Sharpe, Sortino, tracking error, information ratio) are not
#: published: the same bar as beta, so a window's risk block and its beta
#: become publishable together.
MIN_RISK_OBSERVATIONS = MIN_BETA_OBSERVATIONS
#: Trading days per year, for annualising daily moments.
TRADING_DAYS_PER_YEAR = 252
#: The ledger source that journals deposits and withdrawals
#: (alpha-engine-config-I12557 deliverable 2, "Transfers").
TRANSFER_SOURCE = "transfer"
#: First date from which the ledger journals every external flow. ``None``
#: until the transfer builder lands; a window starting before it is estimated.
FLOWS_JOURNALED_FROM: date | None = None
#: The v2 book of record (crucible-trader ``ledger.py``): its daily
#: reconciliation ties the ledger's cash and positions to IB's, cents exact
#: (alpha-engine-config-I12806). Read-only, optional.
V2_LEDGER_BUCKET = "alpha-engine-crucible-v2"
V2_LEDGER_TENANT = "nous-ergon-paper"
V2_RECONCILIATION_KEY = "crucible/trader/ledger/reconciliation/{tenant}/{day}.json"
V2_JOURNAL_KEY = "crucible/trader/ledger/journal/{tenant}/{day}.json"
V2_RECONCILIATION_SCHEMA = "ledger_reconciliation.v1"
V2_JOURNAL_SCHEMA = "ledger_journal.v1"
#: The v2 ledger's opening day; no record exists before it.
V2_LEDGER_FIRST_DAY = date(2026, 11, 3)
#: Journal kinds that move cash or shares only by trading or by income the
#: account earns. Anything else (opening balance, declared cash flow, a gap or
#: an adjustment into or out of suspense, a split, a kind added later) can
#: carry or hide an external flow, so a day journaling one proves nothing.
V2_NON_FLOW_KINDS = frozenset({
    "fill", "commission", "interest_accrual", "interest_settlement",
    "dividend_receivable", "dividend_settlement", "cash_rounding",
})
#: How far back a close lookup may reach for a missing date before giving up.
CLOSE_LOOKBACK_DAYS = 7

REASON_FLOWS = "external_flows_not_journaled"
REASON_QQQ_BASIS = "qqq_not_polygon_settled"
#: A trading day (a date in the SPY close history) with no NAV row inside a
#: window: the chain links across it, so the return holds but the day's own
#: factor and its beta observation are lost (S1 backfill, 2026-10-29).
REASON_NAV_ROW_MISSING = "nav_row_missing"
#: A NAV row on a date the SPY close history has no close for, inside that
#: history's span: a market-closed day the export recorded anyway.
REASON_NAV_ROW_CLOSED_DAY = "nav_row_on_market_closed_day"
#: The ground-truth NAV basis: IB shares and cash marked at Polygon settled
#: closes (ruling 2026-10-22; crucible-executor ``NAV_BASIS_SETTLED_CLOSE``).
NAV_BASIS_SETTLED_CLOSE = "settled_close"
#: A NAV row inside a window (its base included: it is the first factor's
#: denominator) on any other basis. Dated by the first and last such row.
REASON_NAV_BASIS = "nav_basis_not_settled_close"

WINDOWS: tuple[tuple[str, str], ...] = (
    ("day", "Day"),
    ("wtd", "WTD"),
    ("mtd", "MTD"),
    ("ytd", "YTD"),
    ("yoy", "YoY"),
    ("ltm", "LTM"),
    ("inception", "Since inception"),
)
#: Index proxies read from close_history only: (cell, symbol, index tracked).
#: Brian, 2026-11-03: S&P 500, Nasdaq Composite, Nasdaq 100 and Russell 2000.
#: SPY (S&P 500) and QQQ (Nasdaq 100) have their settled legs above.
CLOSE_HISTORY_BENCHMARKS: tuple[tuple[str, str, str], ...] = (
    ("vs_oneq", "ONEQ", "Nasdaq Composite"),
    ("vs_iwm", "IWM", "Russell 2000"),
)
#: What each comparison cell is measured against, for the console's labels.
BENCHMARK_LABELS = {
    "vs_spy": "S&P 500 (SPY)",
    "vs_qqq": "Nasdaq 100 (QQQ)",
    **{cell: f"{label} ({sym})" for cell, sym, label in CLOSE_HISTORY_BENCHMARKS},
    "vs_personal": "Personal portfolio (Metron)",
}
COMPARISONS = (
    "absolute", "vs_spy", "vs_qqq",
    *(cell for cell, _, _ in CLOSE_HISTORY_BENCHMARKS),
    "vs_personal", "alpha",
)
REASON_PERSONAL_UNAVAILABLE = "personal_series_unavailable"

DEFINITIONS = {
    "return": "time-weighted, chain-linked over consecutive ledger NAV rows; "
    "factor (NAV_t - flow_t) / NAV_t-1, flows after the close",
    "day": "previous NAV row to as_of",
    "wtd": "last NAV row before the Monday of as_of's week to as_of",
    "mtd": "last NAV row before the 1st of the month to as_of",
    "ytd": "last NAV row before Jan 1 to as_of",
    "yoy": "last year's same-to-date window: last NAV row before Jan 1 of the prior "
    "year to the last NAV row on or before as_of minus one year",
    "ltm": "last NAV row on or before as_of minus one year to as_of",
    "inception": "first NAV row to as_of",
    "partial_from_inception": "the window's natural base precedes inception, so it runs from inception",
    "benchmarks": "SPY (S&P 500), QQQ (Nasdaq 100), ONEQ (Nasdaq Composite) and IWM "
    "(Russell 2000) total return over the same intervals as the portfolio; ETF proxies, "
    "because the index values carry a separate licence",
    "vs_personal": "Brian's personal household (Metron), flow-neutralised daily returns "
    "chained over the same window dates; benchmark_pct is the personal return, excess_pct "
    "is Crucible minus personal",
    "alpha": "excess over SPY: R_p - R_spy",
    "beta_adjusted_alpha": "R_p - beta * R_spy, beta the OLS slope of daily R_p on daily "
    f"R_spy over the window, published with >= {MIN_BETA_OBSERVATIONS} observations; no risk-free leg",
    "risk": "per window, over the same daily portfolio returns the TWR chains: "
    f"volatility = sample stdev x sqrt({TRADING_DAYS_PER_YEAR}); Sharpe = annualised mean / "
    "volatility; Sortino = annualised mean / annualised downside deviation (root mean square "
    "of min(r, 0) over every day); tracking error = annualised stdev of (R_p - R_spy) daily; "
    "information ratio = annualised mean of (R_p - R_spy) / tracking error; risk-free = 0 "
    "(no risk-free source named yet, as for alpha); annualised metrics need >= "
    f"{MIN_RISK_OBSERVATIONS} observations. Max drawdown = deepest fall of the window's "
    "flow-adjusted wealth index from its running peak (base row included). Up days = share "
    "of days with R_p > 0; days beating SPY = share with R_p > R_spy. Turnover is not "
    "published yet (no trade-level input).",
    "underwater_pct": "since inception, each NAV row's flow-adjusted wealth index as % below "
    "its running peak (0 at a new high)",
    "growth_pct": "since inception, the cumulative return at each NAV row in %: portfolio "
    "(time-weighted, flow-adjusted) and each benchmark's total return, chained over the same "
    "intervals as the inception window; 0 at the first row; an interval that could not be "
    "measured makes that series null from there on. growth_reasons names each series' "
    "estimate reasons (the inception window's)",
    "status": "ok | insufficient_history | estimated (a missing or non-authoritative input; "
    "reasons name it)",
}


class PerfInputError(ValueError):
    """An input is unusable as a whole (fail loud; this is a producer repo)."""


@dataclass(frozen=True)
class NavRow:
    day: date
    nav: float
    spy_close: float | None
    spy_dividend: float
    spy_return_pct: float | None
    nav_basis: str
    qqq_close: float | None = None
    qqq_dividend: float = 0.0
    #: IB cash in whole cents and the share counts, from ``total_cash`` and
    #: ``positions_snapshot``; ``None`` when absent or not exact.
    cash_cents: int | None = None
    shares: tuple[tuple[str, int], ...] | None = None


@dataclass(frozen=True)
class LedgerDay:
    """One v2 ledger reconciliation day, reduced to what proves a flow-free day."""

    day: date
    broker_cash_cents: int
    broker_positions: tuple[tuple[str, int], ...]
    #: The ledger's cash and positions equal IB's at this close.
    ledger_ties_broker: bool
    #: This day's activity is fully journaled and holds no flow-capable kind.
    activity_flow_free: bool


@dataclass(frozen=True)
class PersonalRow:
    """One flow-neutralised return over ``(start, end]`` from Metron."""

    start: date
    end: date
    ret: float
    estimated: bool


@dataclass(frozen=True)
class Flow:
    day: date
    amount: float


# ── parsing ──────────────────────────────────────────────────────────────────


def _float_or_none(raw: str | None) -> float | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _cents(raw: str | None) -> int | None:
    try:
        return int((Decimal((raw or "").strip()) * 100).to_integral_value(ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return None


def _share_counts(raw: Any) -> tuple[tuple[str, int], ...] | None:
    """Non-zero integer share counts by ticker, or ``None`` when not exact."""
    if not isinstance(raw, Mapping):
        return None
    out = []
    for ticker, qty in raw.items():
        qty = qty.get("shares") if isinstance(qty, Mapping) else qty
        if isinstance(qty, bool) or not isinstance(qty, (int, float)) or qty != int(qty):
            return None
        if qty:
            out.append((str(ticker), int(qty)))
    return tuple(sorted(out))


def _snapshot_shares(raw: str | None) -> tuple[tuple[str, int], ...] | None:
    try:
        return _share_counts(json.loads(raw or ""))
    except ValueError:
        return None


def parse_v2_ledger_day(
    day: date, recon: Mapping[str, Any] | None, journal: Mapping[str, Any] | None
) -> LedgerDay | None:
    """A v2 reconciliation day, or ``None`` when the record is unusable."""
    if not isinstance(recon, Mapping) or recon.get("schema_version") != V2_RECONCILIATION_SCHEMA:
        return None
    cash, positions = recon.get("broker_cash_cents"), _share_counts(recon.get("broker_positions"))
    if recon.get("trading_day") != day.isoformat() or type(cash) is not int or positions is None:
        return None
    checks = recon.get("checks") if isinstance(recon.get("checks"), Mapping) else {}
    rolled = checks.get("roll_forward") if isinstance(checks.get("roll_forward"), Mapping) else {}
    complete = checks.get("completeness") if isinstance(checks.get("completeness"), Mapping) else {}
    ties = recon.get("ledger_cash_cents") == cash and _share_counts(recon.get("ledger_positions")) == positions
    entries = journal.get("entries") if isinstance(journal, Mapping) else None
    kinds_ok = (
        isinstance(journal, Mapping)
        and journal.get("schema_version") == V2_JOURNAL_SCHEMA
        and journal.get("trading_day") == day.isoformat()
        and isinstance(entries, list)
        and all(isinstance(e, Mapping) and e.get("kind") in V2_NON_FLOW_KINDS for e in entries)
    )
    flow_free = (
        ties
        and kinds_ok
        and rolled.get("ties") is True
        and recon.get("breaks") == []
        and complete.get("missing") == []
    )
    return LedgerDay(day, cash, positions, ties, flow_free)


def parse_nav_csv(text: str) -> list[NavRow]:
    """Rows of the ``eod_pnl`` export, ascending, one per date, NAV > 0."""
    reader = csv.DictReader(io.StringIO(text))
    missing = {"date", "portfolio_nav"} - set(reader.fieldnames or ())
    if missing:
        raise PerfInputError(f"{NAV_KEY} lacks column(s) {sorted(missing)}")
    by_day: dict[date, NavRow] = {}
    # Ex-dividends on rows dropped for want of a NAV: the next surviving row's
    # interval spans that day, so its total return must carry them
    # (codex f454f5ba2e5f7f52; $500 -> $5 div -> $495 read -1%, not 0%).
    dropped_divs: dict[date, tuple[float, float]] = {}
    for raw in reader:
        try:
            day = date.fromisoformat((raw.get("date") or "").strip()[:10])
        except ValueError:
            continue
        nav = _float_or_none(raw.get("portfolio_nav"))
        if nav is None or nav <= 0:
            dropped_divs[day] = (
                _float_or_none(raw.get("spy_dividend_per_share")) or 0.0,
                _float_or_none(raw.get("qqq_dividend_per_share")) or 0.0,
            )
            continue
        spy = _float_or_none(raw.get("spy_close"))
        qqq = _float_or_none(raw.get("qqq_close"))
        by_day[day] = NavRow(
            day=day,
            nav=nav,
            spy_close=spy if spy and spy > 0 else None,
            spy_dividend=_float_or_none(raw.get("spy_dividend_per_share")) or 0.0,
            spy_return_pct=_float_or_none(raw.get("spy_return_pct")),
            nav_basis=(raw.get("nav_basis") or "").strip() or "unrecorded",
            qqq_close=qqq if qqq and qqq > 0 else None,
            qqq_dividend=_float_or_none(raw.get("qqq_dividend_per_share")) or 0.0,
            cash_cents=_cents(raw.get("total_cash")),
            shares=_snapshot_shares(raw.get("positions_snapshot")),
        )
    if not by_day:
        raise PerfInputError(f"{NAV_KEY} has no row with a positive NAV")
    kept = sorted(by_day)
    for day, (spy_div, qqq_div) in dropped_divs.items():
        if day in by_day or not (spy_div or qqq_div):
            continue
        nxt = next((d for d in kept if d > day), None)
        if nxt is None or day < kept[0]:
            continue  # before the first or after the last surviving row: no interval
        row = by_day[nxt]
        by_day[nxt] = dataclasses.replace(
            row, spy_dividend=row.spy_dividend + spy_div, qqq_dividend=row.qqq_dividend + qqq_div,
        )
    return [by_day[d] for d in kept]


def parse_close_history(doc: Mapping[str, Any]) -> dict[date, float]:
    """``close_history`` JSON (``closes: [[YYYY-MM-DD, close], ...]``).

    Raises :class:`PerfInputError` when the document is not that shape at all
    (not an object, or ``closes`` not a list); a bad row is skipped."""
    if not isinstance(doc, Mapping) or not isinstance(doc.get("closes") or [], list):
        raise PerfInputError(f"close_history is not {{closes: [...]}}: {type(doc).__name__}")
    out: dict[date, float] = {}
    for pair in doc.get("closes") or ():
        try:
            day = date.fromisoformat(str(pair[0])[:10])
            close = float(pair[1])
        except (TypeError, ValueError, IndexError, KeyError):
            continue  # a mapping row raises KeyError on pair[0] (codex 385d8cccc8e6d68a)
        if math.isfinite(close) and close > 0:
            out[day] = close
    return out


def parse_personal_returns(doc: Mapping[str, Any]) -> list[PersonalRow]:
    """``metron/perf/personal_returns.json`` rows, ascending by end date.

    Raises :class:`PerfInputError` on a schema it does not know (the caller
    treats the series as unavailable, never as zero)."""
    if not isinstance(doc, Mapping) or not isinstance(doc.get("returns") or [], list):
        raise PerfInputError(f"{PERSONAL_KEY} is not an object with a returns list")
    if doc.get("schema_version") != PERSONAL_SCHEMA_VERSION:
        raise PerfInputError(f"{PERSONAL_KEY} schema {doc.get('schema_version')!r}")
    out: dict[date, PersonalRow] = {}
    for raw in doc.get("returns") or ():
        try:
            start = date.fromisoformat(str(raw["start"])[:10])
            end = date.fromisoformat(str(raw["end"])[:10])
            ret = float(raw["return"])
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(ret) or ret <= -1.0 or end <= start:
            continue
        out[end] = PersonalRow(start, end, ret, bool(raw.get("estimated")))
    return [out[d] for d in sorted(out)]


def personal_leg(series: Sequence[PersonalRow] | None, base: date, end: date) -> tuple[float | None, tuple[str, ...]]:
    """The personal portfolio's return over ``(base, end]`` and its reasons.

    Pure. ``None`` series = unpublished or unreadable."""
    if series is None:
        return None, (REASON_PERSONAL_UNAVAILABLE,)
    if not series or series[0].start > base:
        first = series[0].start.isoformat() if series else "none"
        return None, (f"personal_series_starts:{first}",)
    picked = [r for r in series if r.start >= base and r.end <= end]
    if not picked:
        return None, (f"personal_nav_missing:{base.isoformat()}",)
    reasons: list[str] = []
    if picked[0].start != base:
        reasons.append(f"personal_nav_missing:{base.isoformat()}")
    gaps = [b.start for a, b in pairwise(picked) if b.start != a.end]
    if gaps:
        reasons.append(_dated("personal_series_gap", gaps))
    if picked[-1].end < end:
        reasons.append(f"personal_series_ends:{picked[-1].end.isoformat()}")
    estimated = [r.end for r in picked if r.estimated]
    if estimated:
        reasons.append(_dated("personal_nav_estimated", estimated))
    total = 1.0
    for r in picked:
        total *= 1.0 + r.ret
    return total - 1.0, tuple(reasons)


def flows_from_journal(entries: Iterable[Mapping[str, Any]]) -> tuple[list[Flow], dict[str, Any]]:
    """External flows from ledger journal entries, plus a summary.

    A transfer entry's flow is the sum of its ``assets:*`` postings: a deposit
    raises assets, a withdrawal lowers them, an in-kind transfer counts at the
    value it was journaled at.
    """
    flows: dict[date, float] = {}
    n_entries = n_transfers = 0
    anchor: str | None = None
    for doc in entries:
        n_entries += 1
        source = doc.get("source")
        if source == "opening_balance" and doc.get("effective_date"):
            anchor = min(anchor or str(doc["effective_date"]), str(doc["effective_date"]))
        if source != TRANSFER_SOURCE:
            continue
        n_transfers += 1
        day = date.fromisoformat(str(doc["effective_date"])[:10])
        amount = sum(
            float(p.get("amount") or 0)
            for p in doc.get("postings") or ()
            if str(p.get("account", "")).startswith("assets:")
        )
        flows[day] = flows.get(day, 0.0) + amount
    summary = {
        "entries_read": n_entries,
        "transfer_entries": n_transfers,
        "ledger_anchor_date": anchor,
        "journaled_from": FLOWS_JOURNALED_FROM.isoformat() if FLOWS_JOURNALED_FROM else None,
    }
    return [Flow(d, flows[d]) for d in sorted(flows)], summary


# ── window arithmetic ────────────────────────────────────────────────────────


def _one_year_before(day: date) -> date:
    try:
        return day.replace(year=day.year - 1)
    except ValueError:  # Feb 29
        return day.replace(year=day.year - 1, day=28)


def _last_on_or_before(days: Sequence[date], target: date) -> date | None:
    best = None
    for d in days:
        if d <= target:
            best = d
        else:
            break
    return best


def _last_before(days: Sequence[date], target: date) -> date | None:
    return _last_on_or_before(days, target - timedelta(days=1))


def window_bounds(
    window: str, days: Sequence[date]
) -> tuple[date | None, date | None, bool, str | None]:
    """``(base, end, partial_from_inception, insufficient_reason)``."""
    as_of, inception = days[-1], days[0]
    history = (as_of - inception).days
    if len(days) < 2:
        return None, None, False, "one NAV row"

    def from_natural(natural: date | None, end: date) -> tuple[date | None, date | None, bool, str | None]:
        if natural is not None:
            return natural, end, False, None
        if end <= inception:
            return None, None, False, "window ends at inception"
        return inception, end, True, None

    if window == "day":
        return days[-2], as_of, False, None
    if window == "wtd":
        return from_natural(_last_before(days, as_of - timedelta(days=as_of.weekday())), as_of)
    if window == "mtd":
        return from_natural(_last_before(days, as_of.replace(day=1)), as_of)
    if window == "ytd":
        return from_natural(_last_before(days, date(as_of.year, 1, 1)), as_of)
    if window in ("yoy", "ltm"):
        if history < YEAR_WINDOW_MIN_DAYS:
            return None, None, False, f"{history} of {YEAR_WINDOW_MIN_DAYS} days"
        year_ago = _last_on_or_before(days, _one_year_before(as_of))
        if window == "ltm":
            return year_ago, as_of, False, None
        if year_ago is None:
            return None, None, False, "no NAV row one year back"
        return from_natural(_last_before(days, date(as_of.year - 1, 1, 1)), year_ago)
    if window == "inception":
        return inception, as_of, False, None
    raise ValueError(f"unknown window {window!r}")


def _close_on_or_before(series: Mapping[date, float], day: date) -> tuple[float | None, bool]:
    """``(close, exact)`` for ``day``, reaching back at most CLOSE_LOOKBACK_DAYS."""
    for back in range(CLOSE_LOOKBACK_DAYS + 1):
        close = series.get(day - timedelta(days=back))
        if close is not None:
            return close, back == 0
    return None, False


def _dated(reason: str, days: Sequence[date]) -> str:
    """``reason:<day>`` for one day, ``reason:<first>..<last>`` for several."""
    first, last = min(days), max(days)
    return f"{reason}:{first.isoformat()}" + ("" if first == last else f"..{last.isoformat()}")


def _pct(x: float | None) -> float | None:
    return None if x is None else round(x * 100.0, 4)


def _beta(port: Sequence[float], bench: Sequence[float]) -> float | None:
    n = len(port)
    if n < MIN_BETA_OBSERVATIONS or n != len(bench):
        return None
    mp, mb = sum(port) / n, sum(bench) / n
    var = sum((b - mb) ** 2 for b in bench)
    if var <= 0:
        return None
    return sum((p - mp) * (b - mb) for p, b in zip(port, bench, strict=True)) / var


def _mean(xs: Sequence[float]) -> float:
    return sum(xs) / len(xs)


def _stdev(xs: Sequence[float]) -> float | None:
    """Sample standard deviation; None under two observations.

    ``statistics.stdev`` sums exactly, so a constant series is exactly 0 (and
    its ratio null) rather than a rounding residue like 1.7e-18 that divided
    into a ~1e16 Sharpe (codex 885d390ad694c9c1)."""
    if len(xs) < 2:
        return None
    return statistics.stdev(xs)


def _ratio(num: float, den: float | None) -> float | None:
    return None if den is None or den <= 0 else num / den


def _r4(x: float | None) -> float | None:
    return None if x is None else round(x, 4)


def _drawdowns(daily: Sequence[float]) -> list[float]:
    """Fall below the running peak after each day, the base (wealth 1.0)
    counted as the first peak; 0.0 at a new high, negative below one."""
    wealth = peak = 1.0
    out = []
    for r in daily:
        wealth *= 1.0 + r
        peak = max(peak, wealth)
        out.append(wealth / peak - 1.0)
    return out


def risk_metrics(
    port: Sequence[float],
    spy: Sequence[float] | None,
    reasons: Sequence[str],
) -> dict[str, Any]:
    """The window's risk block over its daily portfolio returns.

    ``spy`` is the SPY leg's daily returns over the same intervals, or None
    when that leg could not be aligned (then tracking error, the information
    ratio and days-beating-SPY are null). ``reasons`` are the window's estimate
    reasons (portfolio plus SPY); the block is ``estimated`` whenever any is
    present, exactly as the return cells are. Pure; no I/O.
    """
    n = len(port)
    out: dict[str, Any] = {
        "observations": n,
        "volatility_ann_pct": None,
        "sharpe": None,
        "sortino": None,
        "max_drawdown_pct": None,
        "tracking_error_ann_pct": None,
        "information_ratio": None,
        "up_days_pct": None,
        "days_beating_spy_pct": None,
        "turnover": None,
    }
    if n == 0:
        out.update(status=STATUS_INSUFFICIENT, annualised_status=STATUS_INSUFFICIENT, reasons=[])
        return out
    aligned = spy is not None and len(spy) == n
    dd = _drawdowns(port)
    out["max_drawdown_pct"] = _pct(min(0.0, *dd))
    out["up_days_pct"] = _pct(sum(1 for r in port if r > 0) / n)
    if aligned:
        out["days_beating_spy_pct"] = _pct(sum(1 for p, s in zip(port, spy, strict=True) if p > s) / n)
    rs = list(dict.fromkeys(reasons))
    if not aligned:
        rs.append("spy_daily_not_aligned")
    status = STATUS_ESTIMATED if rs else STATUS_OK
    annualised = STATUS_INSUFFICIENT
    if n >= MIN_RISK_OBSERVATIONS:
        annualised = status
        root = math.sqrt(TRADING_DAYS_PER_YEAR)
        sd = _stdev(port)
        mean_ann = _mean(port) * TRADING_DAYS_PER_YEAR
        vol = None if sd is None else sd * root
        downside = math.sqrt(_mean([min(r, 0.0) ** 2 for r in port])) * root
        out["volatility_ann_pct"] = _pct(vol)
        out["sharpe"] = _r4(_ratio(mean_ann, vol))
        out["sortino"] = _r4(_ratio(mean_ann, downside))
        if aligned:
            active = [p - s for p, s in zip(port, spy, strict=True)]
            asd = _stdev(active)
            te = None if asd is None else asd * root
            out["tracking_error_ann_pct"] = _pct(te)
            out["information_ratio"] = _r4(_ratio(_mean(active) * TRADING_DAYS_PER_YEAR, te))
    out.update(status=status, annualised_status=annualised, reasons=rs)
    return out


# ── the build ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Leg:
    value: float | None
    reasons: tuple[str, ...]
    daily: tuple[float, ...]
    #: One growth factor per interval of the window, aligned with its NAV
    #: rows; ``None`` where the interval could not be measured.
    factors: tuple[float | None, ...] = ()


def _growth(days: Sequence[date], factors: Sequence[float | None]) -> dict[str, float | None]:
    """The cumulative return at each NAV row since the first, in percent.

    ``days`` has one more entry than ``factors`` (the base row reads 0). An
    unmeasured interval makes every later point ``None``: a chain with a hole
    in it is not a return, and the gap is never bridged."""
    out: dict[str, float | None] = {days[0].isoformat(): 0.0} if days else {}
    total: float | None = 1.0
    for day, f in zip(days[1:], factors, strict=True):
        total = None if total is None or f is None else total * f
        out[day.isoformat()] = None if total is None else _pct(total - 1.0)
    return out


def _chain(factors: Iterable[float | None]) -> float | None:
    total = 1.0
    for f in factors:
        if f is None:
            return None
        total *= f
    return total - 1.0


def build(
    nav_rows: Sequence[NavRow],
    flows: Sequence[Flow],
    flows_summary: Mapping[str, Any],
    spy_history: Mapping[date, float],
    qqq_history: Mapping[date, float],
    *,
    now: datetime,
    as_of: date | None = None,
    extra_histories: Mapping[str, Mapping[date, float] | None] | None = None,
    personal: Sequence[PersonalRow] | None = None,
    ledger_days: Mapping[date, LedgerDay] | None = None,
) -> dict[str, Any]:
    """The whole ``perf_tracker.v1`` document. Pure; no I/O.

    ``extra_histories`` maps each :data:`CLOSE_HISTORY_BENCHMARKS` symbol to
    its close history (``None`` or absent = unreadable). ``personal`` is the
    Metron series, ``None`` when unpublished or unreadable."""
    extra_histories = extra_histories or {}
    rows = [r for r in nav_rows if as_of is None or r.day <= as_of]
    if not rows:
        raise PerfInputError("no NAV row on or before as_of")
    days = [r.day for r in rows]
    index = {r.day: i for i, r in enumerate(rows)}
    flow_by_day = {f.day: f.amount for f in flows}
    # The SPY close history doubles as the trading calendar (stdlib only; the
    # workflow installs boto3 alone). Outside its span nothing is inferred.
    calendar = sorted(spy_history)
    cal_lo, cal_hi = (calendar[0], calendar[-1]) if calendar else (None, None)
    cal_set = set(calendar)

    def calendar_gaps(prev_day: date, cur_day: date) -> list[str]:
        if cal_lo is None:
            return []
        out = [
            f"{REASON_NAV_ROW_MISSING}:{d.isoformat()}"
            for d in calendar
            if prev_day < d < cur_day
        ]
        if cal_lo <= cur_day <= cal_hi and cur_day not in cal_set:
            out.append(f"{REASON_NAV_ROW_CLOSED_DAY}:{cur_day.isoformat()}")
        return out

    def interval_flow(i: int) -> float:
        lo, hi = rows[i - 1].day, rows[i].day
        return sum(a for d, a in flow_by_day.items() if lo < d <= hi)

    ledger = ledger_days or {}

    def ties_ib(row: NavRow, led: LedgerDay | None) -> bool:
        return (
            led is not None
            and led.ledger_ties_broker
            and row.cash_cents == led.broker_cash_cents
            and row.shares == led.broker_positions
        )

    def proven_flow_free(i: int) -> bool:
        """The v2 ledger proves no external flow in ``(d_{i-1}, d_i]``."""
        prev, cur = rows[i - 1], rows[i]
        end = ledger.get(cur.day)
        return (
            ties_ib(prev, ledger.get(prev.day))
            and ties_ib(cur, end)
            and end is not None
            and end.activity_flow_free
            and not calendar_gaps(prev.day, cur.day)
            and interval_flow(i) == 0
        )

    def legs(base: date, end: date) -> tuple[_Leg, _Leg, _Leg]:
        lo, hi = index[base], index[end]
        p_f, s_f, q_f = [], [], []
        p_reasons: list[str] = []
        s_reasons: list[str] = []
        q_reasons: list[str] = []
        if (FLOWS_JOURNALED_FROM is None or base < FLOWS_JOURNALED_FROM) and not all(
            proven_flow_free(i) for i in range(lo + 1, hi + 1)
        ):
            p_reasons.append(REASON_FLOWS)
        off_basis = [r.day for r in rows[lo:hi + 1] if r.nav_basis != NAV_BASIS_SETTLED_CLOSE]
        if off_basis:
            p_reasons.append(_dated(REASON_NAV_BASIS, off_basis))
        for i in range(lo + 1, hi + 1):
            prev, cur = rows[i - 1], rows[i]
            p_f.append((cur.nav - interval_flow(i)) / prev.nav)
            p_reasons.extend(calendar_gaps(prev.day, cur.day))
            if cur.spy_return_pct is not None:
                s_f.append(1.0 + cur.spy_return_pct / 100.0)
            elif cur.spy_close and prev.spy_close:
                s_f.append((cur.spy_close + cur.spy_dividend) / prev.spy_close)
                s_reasons.append(f"spy_return_pct_missing:{cur.day.isoformat()}")
            else:
                a, _ = _close_on_or_before(spy_history, prev.day)
                b, _ = _close_on_or_before(spy_history, cur.day)
                s_f.append(b / a if a and b else None)
                s_reasons.extend(
                    f"spy_settled_close_missing:{r.day.isoformat()}"
                    for r in (prev, cur)
                    if not r.spy_close
                )
            if cur.qqq_close and prev.qqq_close:
                # Settled leg (alpha-engine-config-I13096): both rows carry the
                # EOD reconcile's persisted close; the ending row's ex-dividend
                # makes it a total return, the same basis as the SPY leg.
                q_f.append((cur.qqq_close + cur.qqq_dividend) / prev.qqq_close)
            else:
                q_reasons.append(REASON_QQQ_BASIS)
                a, a_exact = _close_on_or_before(qqq_history, prev.day)
                b, b_exact = _close_on_or_before(qqq_history, cur.day)
                q_f.append(b / a if a and b else None)
                # Name the day whose close is absent, not the interval's end: a
                # market-closed base row is missing, the next session is not.
                q_reasons.extend(
                    f"qqq_close_missing:{r.day.isoformat()}"
                    for r, exact in ((prev, a_exact), (cur, b_exact))
                    if not exact
                )
        return (
            _Leg(_chain(p_f), tuple(p_reasons), tuple(f - 1.0 for f in p_f), tuple(p_f)),
            _Leg(_chain(s_f), tuple(dict.fromkeys(s_reasons)),
                 tuple(f - 1.0 for f in s_f if f is not None), tuple(s_f)),
            _Leg(_chain(q_f), tuple(dict.fromkeys(q_reasons)), (), tuple(q_f)),
        )

    def history_leg(sym: str, base: date, end: date) -> _Leg:
        """A close_history-only total-return leg over the window's intervals."""
        hist = extra_histories.get(sym)
        tag = sym.lower()
        if not hist:
            return _Leg(None, (f"{tag}_close_history_unreadable",), ())
        lo, hi = index[base], index[end]
        factors: list[float | None] = []
        reasons = [f"{tag}_not_polygon_settled"]
        for i in range(lo + 1, hi + 1):
            prev, cur = rows[i - 1], rows[i]
            a, a_exact = _close_on_or_before(hist, prev.day)
            b, b_exact = _close_on_or_before(hist, cur.day)
            factors.append(b / a if a and b else None)
            reasons.extend(
                f"{tag}_close_missing:{r.day.isoformat()}"
                for r, exact in ((prev, a_exact), (cur, b_exact))
                if not exact
            )
        return _Leg(_chain(factors), tuple(dict.fromkeys(reasons)), (), tuple(factors))

    def status(value: float | None, reasons: Sequence[str]) -> str:
        return STATUS_ESTIMATED if reasons or value is None else STATUS_OK

    windows_out = []
    underwater: dict[str, float | None] = {}
    growth: dict[str, dict[str, float | None]] = {}
    growth_reasons: dict[str, list[str]] = {}
    counts = dict.fromkeys(STATUSES, 0)
    for wid, label in WINDOWS:
        base, end, partial, insufficient = window_bounds(wid, days)
        win: dict[str, Any] = {
            "id": wid,
            "label": label,
            "base_date": base.isoformat() if base else None,
            "end_date": end.isoformat() if end else None,
            "partial_from_inception": partial,
        }
        if insufficient is not None or base is None or end is None:
            win.update(trading_days=0, nav_basis_changes=[], insufficient_reason=insufficient)
            win["cells"] = {
                c: {"status": STATUS_INSUFFICIENT, "reasons": []} for c in COMPARISONS
            }
            counts[STATUS_INSUFFICIENT] += len(COMPARISONS)
            win["risk"] = risk_metrics((), None, ())
            windows_out.append(win)
            continue
        lo, hi = index[base], index[end]
        win["trading_days"] = hi - lo
        win["nav_basis_changes"] = [
            {"date": rows[i].day.isoformat(), "from": rows[i - 1].nav_basis, "to": rows[i].nav_basis}
            for i in range(lo + 1, hi + 1)
            if rows[i].nav_basis != rows[i - 1].nav_basis
        ]
        port, spy, qqq = legs(base, end)

        def versus(bench: _Leg) -> dict[str, Any]:
            reasons = list(dict.fromkeys(port.reasons + bench.reasons))
            excess = None if port.value is None or bench.value is None else port.value - bench.value
            return {
                "portfolio_pct": _pct(port.value),
                "benchmark_pct": _pct(bench.value),
                "excess_pct": _pct(excess),
                "status": status(excess, reasons),
                "reasons": reasons,
            }

        vs_spy, vs_qqq = versus(spy), versus(qqq)
        extra_legs = {sym: history_leg(sym, base, end) for _, sym, _ in CLOSE_HISTORY_BENCHMARKS}
        vs_extra = {cell: versus(extra_legs[sym]) for cell, sym, _ in CLOSE_HISTORY_BENCHMARKS}
        p_value, p_reasons = personal_leg(personal, base, end)
        vs_personal = versus(_Leg(p_value, p_reasons, ()))
        beta = _beta(port.daily, spy.daily) if len(spy.daily) == len(port.daily) else None
        beta_adj = (
            None if beta is None or port.value is None or spy.value is None
            else port.value - beta * spy.value
        )
        cells = {
            "absolute": {
                "return_pct": _pct(port.value),
                "status": status(port.value, port.reasons),
                "reasons": list(port.reasons),
            },
            "vs_spy": vs_spy,
            "vs_qqq": vs_qqq,
            **vs_extra,
            "vs_personal": vs_personal,
            "alpha": {
                "excess_vs_spy_pct": vs_spy["excess_pct"],
                "beta": None if beta is None else round(beta, 4),
                "beta_observations": len(port.daily),
                "beta_adjusted_pct": _pct(beta_adj),
                "beta_status": (
                    STATUS_INSUFFICIENT if beta is None else status(beta_adj, vs_spy["reasons"])
                ),
                "status": vs_spy["status"],
                "reasons": vs_spy["reasons"],
            },
        }
        for cell in cells.values():
            counts[cell["status"]] += 1
        win["cells"] = cells
        win["risk"] = risk_metrics(
            port.daily,
            spy.daily if len(spy.daily) == len(port.daily) else None,
            vs_spy["reasons"],
        )
        windows_out.append(win)

        if wid == "inception":
            # The underwater series behind the max-drawdown figure, one point
            # per NAV row since inception, for the console's chart
            # (alpha-engine-config-I12784).
            underwater[rows[lo].day.isoformat()] = 0.0
            for i, dd in enumerate(_drawdowns(port.daily), start=lo + 1):
                underwater[rows[i].day.isoformat()] = _pct(dd)
            # Growth since inception, Crucible beside each index, over the
            # same intervals the inception cells chain (alpha-engine-config-
            # I12798): the console overlays them as lines. Each series keeps
            # its leg's reasons, so an estimated line says so.
            span = days[lo:hi + 1]
            legs_by_series = {"portfolio": port, "spy": spy, "qqq": qqq,
                              **{sym.lower(): extra_legs[sym] for _, sym, _ in CLOSE_HISTORY_BENCHMARKS}}
            growth = {
                # An unreadable index has no factors at all: every point is
                # null, never a flat zero line.
                name: (_growth(span, leg.factors) if len(leg.factors) == len(span) - 1
                       else dict.fromkeys((d.isoformat() for d in span), None))
                for name, leg in legs_by_series.items()
            }
            growth_reasons = {
                name: list(dict.fromkeys(leg.reasons)) for name, leg in legs_by_series.items()
            }

    inception, last = days[0], days[-1]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of": last.isoformat(),
        "inception_date": inception.isoformat(),
        "days_of_history": (last - inception).days,
        "trading_days_of_history": len(rows) - 1,
        "year_window_min_days": YEAR_WINDOW_MIN_DAYS,
        "definitions": DEFINITIONS,
        "benchmark_labels": BENCHMARK_LABELS,
        "inputs": {
            "nav": {"key": NAV_KEY, "rows": len(rows), "nav_basis_latest": rows[-1].nav_basis},
            "flows": {
                "prefix": JOURNAL_PREFIX,
                "source": TRANSFER_SOURCE,
                **flows_summary,
                "v2_ledger": {
                    "reconciliation": f"s3://{V2_LEDGER_BUCKET}/"
                    + V2_RECONCILIATION_KEY.format(tenant=V2_LEDGER_TENANT, day="{day}"),
                    "basis": "an interval is flow-free when both rows' IB cash and shares equal "
                    "the reconciled broker figures to the cent and the end day's journal ties "
                    "with trading and income entries only",
                    "days_read": sum(1 for r in rows if r.day in ledger),
                    "flow_free_intervals": sum(
                        1 for i in range(1, len(rows)) if proven_flow_free(i)
                    ),
                },
            },
            "spy": {
                "primary": f"{NAV_KEY} spy_return_pct (Polygon settled total return)",
                "fallback": [
                    f"{NAV_KEY} spy_close + spy_dividend_per_share",
                    CLOSE_HISTORY_KEY.format(symbol="SPY") + " (dividend-adjusted)",
                ],
            },
            "qqq": {
                "primary": f"{NAV_KEY} qqq_close + qqq_dividend_per_share (settled close + Polygon ex-dividends)",
                "fallback": [CLOSE_HISTORY_KEY.format(symbol="QQQ") + " (dividend-adjusted)"],
                "settled_series": f"{NAV_KEY} qqq_close",
            },
            **{
                sym.lower(): {
                    "index": label,
                    "primary": CLOSE_HISTORY_KEY.format(symbol=sym) + " (dividend-adjusted)",
                    "readable": bool(extra_histories.get(sym)),
                }
                for _, sym, label in CLOSE_HISTORY_BENCHMARKS
            },
            "personal": {
                "key": PERSONAL_KEY,
                "readable": personal is not None,
                "rows": len(personal) if personal is not None else 0,
                "first_date": personal[0].start.isoformat() if personal else None,
                "last_date": personal[-1].end.isoformat() if personal else None,
            },
        },
        "windows": windows_out,
        "underwater_pct": underwater,
        "growth_pct": growth,
        "growth_reasons": growth_reasons,
        "status_counts": counts,
    }


# ── I/O ──────────────────────────────────────────────────────────────────────


def _get(s3: Any, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def _journal_entries(s3: Any, bucket: str) -> list[dict[str, Any]]:
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=JOURNAL_PREFIX):
        for obj in page.get("Contents") or ():
            if obj["Key"].endswith(".json"):
                out.append(json.loads(_get(s3, bucket, obj["Key"])))
    out.sort(key=lambda d: (str(d.get("effective_date")), str(d.get("entry_id"))))
    return out


def _get_optional_json(s3: Any, bucket: str, key: str) -> Mapping[str, Any] | None:
    """An OPTIONAL input: ``None`` when absent, unreadable or not JSON.

    The core inputs above fail loud; these only feed their own cells, which
    then name the gap. Logs the key and the error type, never a value."""
    try:
        return json.loads(_get(s3, bucket, key))
    except Exception as e:  # noqa: BLE001 - every failure means "unavailable" here
        response = getattr(e, "response", None)
        code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
        logger.warning("optional input %s unavailable: %s%s", key, type(e).__name__, f" {code}" if code else "")
        return None


def _optional_inputs(s3: Any, bucket: str) -> tuple[dict[str, dict[date, float] | None], list[PersonalRow] | None]:
    extra: dict[str, dict[date, float] | None] = {}
    for _, sym, _ in CLOSE_HISTORY_BENCHMARKS:
        doc = _get_optional_json(s3, bucket, CLOSE_HISTORY_KEY.format(symbol=sym))
        extra[sym] = None
        if doc is not None:
            # An optional input never aborts the publish (codex 385d8cccc8e6d68a).
            try:
                extra[sym] = parse_close_history(doc)
            except PerfInputError as e:
                logger.warning("optional input %s unusable: %s", CLOSE_HISTORY_KEY.format(symbol=sym), e)
    personal: list[PersonalRow] | None = None
    doc = _get_optional_json(s3, bucket, PERSONAL_KEY)
    if doc is not None:
        try:
            personal = parse_personal_returns(doc)
        except PerfInputError as e:
            logger.warning("optional input %s unusable: %s", PERSONAL_KEY, e)
    return extra, personal


def _v2_ledger_days(s3: Any, nav_rows: Sequence[NavRow]) -> dict[date, LedgerDay]:
    """The v2 reconciliation day for each NAV row since the ledger opened.

    Optional: a record that is absent or unreadable proves nothing."""
    out: dict[date, LedgerDay] = {}
    for row in nav_rows:
        if row.day < V2_LEDGER_FIRST_DAY:
            continue
        kw = {"tenant": V2_LEDGER_TENANT, "day": row.day.isoformat()}
        recon = _get_optional_json(s3, V2_LEDGER_BUCKET, V2_RECONCILIATION_KEY.format(**kw))
        if recon is None:
            continue
        journal = _get_optional_json(s3, V2_LEDGER_BUCKET, V2_JOURNAL_KEY.format(**kw))
        led = parse_v2_ledger_day(row.day, recon, journal)
        if led is not None:
            out[row.day] = led
    return out


def load_and_build(s3: Any, bucket: str, *, now: datetime, as_of: date | None = None) -> dict[str, Any]:
    nav_rows = parse_nav_csv(_get(s3, bucket, NAV_KEY).decode())
    flows, summary = flows_from_journal(_journal_entries(s3, bucket))
    spy = parse_close_history(json.loads(_get(s3, bucket, CLOSE_HISTORY_KEY.format(symbol="SPY"))))
    qqq = parse_close_history(json.loads(_get(s3, bucket, CLOSE_HISTORY_KEY.format(symbol="QQQ"))))
    extra, personal = _optional_inputs(s3, bucket)
    return build(
        nav_rows, flows, summary, spy, qqq, now=now, as_of=as_of,
        extra_histories=extra, personal=personal, ledger_days=_v2_ledger_days(s3, nav_rows),
    )


def encode(doc: Mapping[str, Any]) -> bytes:
    return (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode()


def publish(
    s3: Any, bucket: str, doc: Mapping[str, Any], *, include_latest: bool = True
) -> list[str]:
    """Dated copy first, then ``latest.json``. Returns the keys written.

    ``include_latest=False`` writes the dated copy only (the backfill)."""
    body = encode(doc)
    keys = [DATED_KEY.format(as_of=doc["as_of"])] + ([LATEST_KEY] if include_latest else [])
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return keys


def load_and_backfill(s3: Any, bucket: str, *, now: datetime) -> list[dict[str, Any]]:
    """One document per NAV row from the second on, each as of that row.

    The S1 backfill (alpha-engine-config-I12798): every window since inception,
    recomputed from TODAY's inputs. A NAV row restated since its own day is
    read as restated, so a backfilled document is the current view of that
    day, not a replay of what was published then; it says so in
    ``backfill``."""
    nav_rows = parse_nav_csv(_get(s3, bucket, NAV_KEY).decode())
    flows, summary = flows_from_journal(_journal_entries(s3, bucket))
    spy = parse_close_history(json.loads(_get(s3, bucket, CLOSE_HISTORY_KEY.format(symbol="SPY"))))
    qqq = parse_close_history(json.loads(_get(s3, bucket, CLOSE_HISTORY_KEY.format(symbol="QQQ"))))
    extra, personal = _optional_inputs(s3, bucket)
    ledger_days = _v2_ledger_days(s3, nav_rows)
    latest = nav_rows[-1].day.isoformat()
    docs = []
    for row in nav_rows[1:]:
        doc = build(
            nav_rows, flows, summary, spy, qqq, now=now, as_of=row.day,
            extra_histories=extra, personal=personal, ledger_days=ledger_days,
        )
        doc["backfill"] = {"recomputed_from_inputs_as_of": latest}
        docs.append(doc)
    return docs


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, default=None)
    parser.add_argument("--dry-run", action="store_true", help="build and validate, write nothing")
    parser.add_argument("--out", default=None, help="also write the document to this local path")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="write ops/perf/dated/{as_of}.json for EVERY NAV row (never latest.json)",
    )
    parser.add_argument("--out-dir", default=None, help="with --backfill: also write each document here")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    import boto3

    s3 = boto3.client("s3")
    if args.backfill:
        docs = load_and_backfill(s3, args.bucket, now=datetime.now(UTC))
        estimated = sum(d["status_counts"][STATUS_ESTIMATED] for d in docs)
        logger.info(
            "perf_tracker backfill documents=%d first_as_of=%s last_as_of=%s estimated_cells=%d",
            len(docs), docs[0]["as_of"] if docs else None, docs[-1]["as_of"] if docs else None, estimated,
        )
        for doc in docs:
            if args.out_dir:
                with open(f"{args.out_dir}/{doc['as_of']}.json", "wb") as fh:
                    fh.write(encode(doc))
            if not args.dry_run:
                publish(s3, args.bucket, doc, include_latest=False)
        logger.info("backfill: %s", "dry run, nothing written" if args.dry_run else f"wrote {len(docs)} dated keys")
        return 0
    doc = load_and_build(s3, args.bucket, now=datetime.now(UTC), as_of=args.as_of)
    if args.out:
        with open(args.out, "wb") as fh:
            fh.write(encode(doc))
    # Counts, dates and keys only: this repository's run logs are public.
    logger.info(
        "perf_tracker as_of=%s inception=%s days_of_history=%d status_counts=%s",
        doc["as_of"], doc["inception_date"], doc["days_of_history"], doc["status_counts"],
    )
    if args.dry_run:
        logger.info("dry run: nothing written")
        return 0
    for key in publish(s3, args.bucket, doc):
        logger.info("wrote %s", key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
