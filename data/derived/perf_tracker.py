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
  a basis change inside a window is reported on that window.
* **External flows**: transfer entries in the ledger journal copies under
  ``trades/ledger/journal/``. The ledger has no transfer builder yet
  (alpha-engine-config-I12557 deliverable 2), so :data:`FLOWS_JOURNALED_FROM`
  is ``None`` and every cell carries the ``external_flows_not_journaled``
  estimate reason. When the builder lands, that constant moves to its first
  date in the same change and the reason drops for later windows.
* **SPY total return**: each ``eod_pnl`` row's persisted ``spy_return_pct``,
  the EOD reconcile's total-return leg on Polygon settled closes and
  ex-dividends (the same figure ``daily_alpha_pct`` is struck against, and
  restated by the T+1 audit). A row without it is rebuilt from that row's
  ``spy_close`` + ``spy_dividend_per_share`` and, failing that, from
  ``market_data/close_history/SPY.json`` (dividend-adjusted); either fallback
  marks the cell estimated and names the row.
* **QQQ total return**: no Polygon settled total-return series for QQQ is
  published yet, so ``market_data/close_history/QQQ.json`` (dividend-adjusted)
  is used and every QQQ cell carries ``qqq_not_polygon_settled``.

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
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "perf_tracker.v1"

NAV_KEY = "trades/eod_pnl.csv"
JOURNAL_PREFIX = "trades/ledger/journal/"
CLOSE_HISTORY_KEY = "market_data/close_history/{symbol}.json"
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
#: The ledger source that journals deposits and withdrawals
#: (alpha-engine-config-I12557 deliverable 2, "Transfers").
TRANSFER_SOURCE = "transfer"
#: First date from which the ledger journals every external flow. ``None``
#: until the transfer builder lands; a window starting before it is estimated.
FLOWS_JOURNALED_FROM: date | None = None
#: How far back a close lookup may reach for a missing date before giving up.
CLOSE_LOOKBACK_DAYS = 7

REASON_FLOWS = "external_flows_not_journaled"
REASON_QQQ_BASIS = "qqq_not_polygon_settled"

WINDOWS: tuple[tuple[str, str], ...] = (
    ("day", "Day"),
    ("wtd", "WTD"),
    ("mtd", "MTD"),
    ("ytd", "YTD"),
    ("yoy", "YoY"),
    ("ltm", "LTM"),
    ("inception", "Since inception"),
)
COMPARISONS = ("absolute", "vs_spy", "vs_qqq", "alpha")

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
    "benchmarks": "SPY and QQQ total return over the same intervals as the portfolio",
    "alpha": "excess over SPY: R_p - R_spy",
    "beta_adjusted_alpha": "R_p - beta * R_spy, beta the OLS slope of daily R_p on daily "
    f"R_spy over the window, published with >= {MIN_BETA_OBSERVATIONS} observations; no risk-free leg",
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


def parse_nav_csv(text: str) -> list[NavRow]:
    """Rows of the ``eod_pnl`` export, ascending, one per date, NAV > 0."""
    reader = csv.DictReader(io.StringIO(text))
    missing = {"date", "portfolio_nav"} - set(reader.fieldnames or ())
    if missing:
        raise PerfInputError(f"{NAV_KEY} lacks column(s) {sorted(missing)}")
    by_day: dict[date, NavRow] = {}
    for raw in reader:
        try:
            day = date.fromisoformat((raw.get("date") or "").strip()[:10])
        except ValueError:
            continue
        nav = _float_or_none(raw.get("portfolio_nav"))
        if nav is None or nav <= 0:
            continue
        spy = _float_or_none(raw.get("spy_close"))
        by_day[day] = NavRow(
            day=day,
            nav=nav,
            spy_close=spy if spy and spy > 0 else None,
            spy_dividend=_float_or_none(raw.get("spy_dividend_per_share")) or 0.0,
            spy_return_pct=_float_or_none(raw.get("spy_return_pct")),
            nav_basis=(raw.get("nav_basis") or "").strip() or "unrecorded",
        )
    if not by_day:
        raise PerfInputError(f"{NAV_KEY} has no row with a positive NAV")
    return [by_day[d] for d in sorted(by_day)]


def parse_close_history(doc: Mapping[str, Any]) -> dict[date, float]:
    """``close_history`` JSON (``closes: [[YYYY-MM-DD, close], ...]``)."""
    out: dict[date, float] = {}
    for pair in doc.get("closes") or ():
        try:
            day = date.fromisoformat(str(pair[0])[:10])
            close = float(pair[1])
        except (TypeError, ValueError, IndexError):
            continue
        if math.isfinite(close) and close > 0:
            out[day] = close
    return out


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


# ── the build ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Leg:
    value: float | None
    reasons: tuple[str, ...]
    daily: tuple[float, ...]


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
) -> dict[str, Any]:
    """The whole ``perf_tracker.v1`` document. Pure; no I/O."""
    rows = [r for r in nav_rows if as_of is None or r.day <= as_of]
    if not rows:
        raise PerfInputError("no NAV row on or before as_of")
    days = [r.day for r in rows]
    index = {r.day: i for i, r in enumerate(rows)}
    flow_by_day = {f.day: f.amount for f in flows}

    def interval_flow(i: int) -> float:
        lo, hi = rows[i - 1].day, rows[i].day
        return sum(a for d, a in flow_by_day.items() if lo < d <= hi)

    def legs(base: date, end: date) -> tuple[_Leg, _Leg, _Leg]:
        lo, hi = index[base], index[end]
        p_f, s_f, q_f = [], [], []
        p_reasons: list[str] = []
        s_reasons: list[str] = []
        q_reasons: list[str] = [REASON_QQQ_BASIS]
        if FLOWS_JOURNALED_FROM is None or base < FLOWS_JOURNALED_FROM:
            p_reasons.append(REASON_FLOWS)
        for i in range(lo + 1, hi + 1):
            prev, cur = rows[i - 1], rows[i]
            p_f.append((cur.nav - interval_flow(i)) / prev.nav)
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
            a, a_exact = _close_on_or_before(qqq_history, prev.day)
            b, b_exact = _close_on_or_before(qqq_history, cur.day)
            q_f.append(b / a if a and b else None)
            if not (a_exact and b_exact):
                q_reasons.append(f"qqq_close_missing:{cur.day.isoformat()}")
        return (
            _Leg(_chain(p_f), tuple(p_reasons), tuple(f - 1.0 for f in p_f)),
            _Leg(_chain(s_f), tuple(dict.fromkeys(s_reasons)),
                 tuple(f - 1.0 for f in s_f if f is not None)),
            _Leg(_chain(q_f), tuple(dict.fromkeys(q_reasons)), ()),
        )

    def status(value: float | None, reasons: Sequence[str]) -> str:
        return STATUS_ESTIMATED if reasons or value is None else STATUS_OK

    windows_out = []
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
        windows_out.append(win)

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
        "inputs": {
            "nav": {"key": NAV_KEY, "rows": len(rows), "nav_basis_latest": rows[-1].nav_basis},
            "flows": {"prefix": JOURNAL_PREFIX, "source": TRANSFER_SOURCE, **flows_summary},
            "spy": {
                "primary": f"{NAV_KEY} spy_return_pct (Polygon settled total return)",
                "fallback": [
                    f"{NAV_KEY} spy_close + spy_dividend_per_share",
                    CLOSE_HISTORY_KEY.format(symbol="SPY") + " (dividend-adjusted)",
                ],
            },
            "qqq": {
                "primary": CLOSE_HISTORY_KEY.format(symbol="QQQ") + " (dividend-adjusted)",
                "settled_series": None,
            },
        },
        "windows": windows_out,
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


def load_and_build(s3: Any, bucket: str, *, now: datetime, as_of: date | None = None) -> dict[str, Any]:
    nav_rows = parse_nav_csv(_get(s3, bucket, NAV_KEY).decode())
    flows, summary = flows_from_journal(_journal_entries(s3, bucket))
    spy = parse_close_history(json.loads(_get(s3, bucket, CLOSE_HISTORY_KEY.format(symbol="SPY"))))
    qqq = parse_close_history(json.loads(_get(s3, bucket, CLOSE_HISTORY_KEY.format(symbol="QQQ"))))
    return build(nav_rows, flows, summary, spy, qqq, now=now, as_of=as_of)


def encode(doc: Mapping[str, Any]) -> bytes:
    return (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode()


def publish(s3: Any, bucket: str, doc: Mapping[str, Any]) -> list[str]:
    """Dated copy first, then ``latest.json``. Returns the keys written."""
    body = encode(doc)
    keys = [DATED_KEY.format(as_of=doc["as_of"]), LATEST_KEY]
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return keys


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, default=None)
    parser.add_argument("--dry-run", action="store_true", help="build and validate, write nothing")
    parser.add_argument("--out", default=None, help="also write the document to this local path")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    import boto3

    s3 = boto3.client("s3")
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
