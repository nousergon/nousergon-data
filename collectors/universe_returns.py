"""
collectors/universe_returns.py — Full-population forward-return tracking.

Uses polygon.io grouped-daily endpoint to fetch OHLCV for the entire US market
in a single API call per date. Computes 1d/3d/5d/10d/15d/21d/30d/60d/90d forward
returns for every ticker, SPY benchmark returns, and sector ETF returns for
sector-relative analysis. 21d arithmetic + log-domain columns added 2026-05-09
to align the measurement substrate with the predictor's canonical 21d
log-domain training target (see docs/private/predictor-21d-migration-260509.md).
60d/90d (return + SPY-relative + beat + log-domain) added 2026-06-01 (W3.1,
L4469) for the predictor horizon study + backtester 60/90d signal-quality.
1d/3d/15d (return + SPY-relative + beat + log-domain) added for config#1981 —
the alpha-decay-curve intermediate-horizon ladder (operator ruling "Option A",
2026-07-16): a real fade-over-time curve needs points BETWEEN the 5d
diagnostic and 21d primary canonical horizons, not just the two endpoints.
Combined with the pre-existing 10d columns, the producer now emits a full
1/3/5/10/15/21d ladder for score_performance_outcomes to consume.

This is the denominator for all lift calculations in the backtester evaluation
framework: scanner filter lift, sector team lift, CIO lift, predictor lift,
execution lift.

``return_30d``, ``return_60d``, ``return_90d``, ``log_return_60d`` and
``log_return_90d`` were DROPPED 2026-08-22 (alpha-engine-config-I8185):
measured 0-of-2.14M non-null (``return_30d`` frozen at eval_date 2026-03-30),
grep-verified with no consumer across `crucible-*` / `nousergon-data`. Root
cause was `_get_existing_dates` gating re-processing on 5d/21d completeness
only, so a date already 21d-complete was never revisited when its later
30d/60d/90d windows closed — see that function's docstring for the full
diagnosis (config#2972). The sibling `spy_return_{30,60,90}d`,
`beat_spy_{30,60,90}d` and `log_spy_return_{60,90}d` columns are unaffected —
`beat_spy_{30,60,90}d` is still derived from the underlying (unstored)
return, and were not in scope for this fix.

Target table: universe_returns in research.db (~900 rows/date, ~47K rows/year).

UNITS -- every ``return_{h}d`` / ``spy_return_{h}d`` / ``sector_etf_return_5d``
column this collector writes is a **DECIMAL FRACTION** (0.05 = +5%), produced by
``_pct_return`` = ``close_end / close_start - 1.0`` and rounded to 4dp. The
``log_*`` columns are natural-log returns.

That matters because a column named ``return_5d`` ALSO exists in
``score_performance`` in the SAME database, in **2dp PERCENT POINTS** -- measured
range [-20.42, 22.70] against [-0.2042, 0.2270] for the same quantity in the
long-format ``score_performance_outcomes`` store, i.e. exactly 100x. Two columns,
one bare name, opposite conventions (alpha-engine-config-I7936). A consumer that
cannot tell which one it received must raise rather than coerce; see
``crucible-backtester/analysis/shadow_book.py``.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path

import boto3
import pandas as pd
from nousergon_lib.trading_calendar import add_trading_days as _add_trading_days
from polygon_client import PolygonForbiddenError

from collectors.research_db_upload import upload_research_db
from dates import default_run_date

logger = logging.getLogger(__name__)

# -- Sector ETF mapping ------------------------------------------------------

_SECTOR_TO_ETF = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
    "Basic Materials": "XLB",
}

_ETF_TO_SECTOR = {v: k for k, v in _SECTOR_TO_ETF.items()}
_SECTOR_ETFS = set(_SECTOR_TO_ETF.values())

# Nasdaq / NYSE TEST SECURITIES. These are not companies: the exchanges publish
# them so market participants can exercise their feeds, and their quoted prices
# are arbitrary. Polygon's grouped-daily endpoint returns them alongside real
# listings, so they have been in this table since its first row.
#
# Measured on live research.db (2026-08-21, alpha-engine-config-I7936):
# 916 rows across ten of these symbols. ZWZZT closed at 19.44 on 2026-03-30 and
# 129,998.70 five sessions later, so its return_5d is
# 129998.70 / 19.44 - 1 = 6686.1759 -- the maximum of the whole column, over
# 2,012,661 rows, and the anchor observation on I7936. It is arithmetically
# correct and completely meaningless.
#
# Cost of leaving them in: the universe-wide mean 5-day return is 0.004758 with
# them and 0.001418 without, so ~70% of the reported mean of the denominator
# for every lift calculation in the backtester came from test securities.
_TEST_SECURITIES = {
    "ZAZZT", "ZBZX", "ZBZZT", "ZCZZT", "ZEXIT", "ZIEXT", "ZJZZT",
    "ZTEST", "ZVV", "ZVZZT", "ZWZZT", "ZXIET", "ZXZZT",
    "CBO", "CBX", "IBM6", "LOPP", "NTEST",
}

_SKIP_TICKERS = (
    _SECTOR_ETFS | {"SPY", "VIX", "^VIX", "^TNX", "^IRX"} | _TEST_SECURITIES
)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS universe_returns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    eval_date TEXT NOT NULL,
    sector TEXT,
    close_price REAL,
    return_5d REAL,
    return_10d REAL,
    return_21d REAL,
    spy_return_5d REAL,
    spy_return_10d REAL,
    spy_return_21d REAL,
    spy_return_30d REAL,
    spy_return_60d REAL,
    spy_return_90d REAL,
    beat_spy_5d INTEGER,
    beat_spy_10d INTEGER,
    beat_spy_21d INTEGER,
    beat_spy_30d INTEGER,
    beat_spy_60d INTEGER,
    beat_spy_90d INTEGER,
    log_return_21d REAL,
    log_spy_return_21d REAL,
    log_spy_return_60d REAL,
    log_spy_return_90d REAL,
    sector_etf TEXT,
    sector_etf_return_5d REAL,
    beat_sector_5d INTEGER,
    return_1d REAL,
    return_3d REAL,
    return_15d REAL,
    spy_return_1d REAL,
    spy_return_3d REAL,
    spy_return_15d REAL,
    beat_spy_1d INTEGER,
    beat_spy_3d INTEGER,
    beat_spy_15d INTEGER,
    log_return_1d REAL,
    log_return_3d REAL,
    log_return_15d REAL,
    log_spy_return_1d REAL,
    log_spy_return_3d REAL,
    log_spy_return_15d REAL,
    UNIQUE(ticker, eval_date)
)
"""

_NEW_COLUMNS_21D = [
    ("return_21d", "REAL"),
    ("spy_return_21d", "REAL"),
    ("beat_spy_21d", "INTEGER"),
    ("log_return_21d", "REAL"),
    ("log_spy_return_21d", "REAL"),
]

# W3.1 (L4469): 60d/90d horizon columns (SPY-relative + beat + log-spy).
# return_60d/return_90d/log_return_60d/log_return_90d DROPPED 2026-08-22
# (alpha-engine-config-I8185) — see module docstring.
_NEW_COLUMNS_60_90D = [
    ("spy_return_60d", "REAL"),
    ("spy_return_90d", "REAL"),
    ("beat_spy_60d", "INTEGER"),
    ("beat_spy_90d", "INTEGER"),
    ("log_spy_return_60d", "REAL"),
    ("log_spy_return_90d", "REAL"),
]

# config#1981: 1d/3d/15d intermediate-horizon columns (return + SPY-relative +
# beat + log) — fill the gap between the 5d diagnostic and 21d primary
# canonical horizons so the alpha-decay-curve consumer has points to plot
# between the two existing endpoints (10d already existed; 1d/3d/15d are the
# genuinely new columns this ladder needed).
_NEW_COLUMNS_DECAY_LADDER = [
    ("return_1d", "REAL"),
    ("return_3d", "REAL"),
    ("return_15d", "REAL"),
    ("spy_return_1d", "REAL"),
    ("spy_return_3d", "REAL"),
    ("spy_return_15d", "REAL"),
    ("beat_spy_1d", "INTEGER"),
    ("beat_spy_3d", "INTEGER"),
    ("beat_spy_15d", "INTEGER"),
    ("log_return_1d", "REAL"),
    ("log_return_3d", "REAL"),
    ("log_return_15d", "REAL"),
    ("log_spy_return_1d", "REAL"),
    ("log_spy_return_3d", "REAL"),
    ("log_spy_return_15d", "REAL"),
]


def collect(
    bucket: str,
    db_path: str,
    signals_prefix: str = "signals",
    sector_map_key: str = "data/sector_map.json",
    max_lookback_trading_days: int = 90,
    dry_run: bool = False,
    run_date: str | None = None,
) -> dict:
    """
    Populate universe_returns table with forward returns for every trading day.

    Enumerates NYSE trading days directly from the trading calendar (not from
    signal folders in S3). For each trading day whose 5d forward window has
    closed, fetches polygon.io grouped-daily prices at t0 + t+5d, computes
    return_5d per ticker and writes rows keyed by the trading day.

    This decouples universe_returns from research's signal cadence. The table
    is now "5d forward returns, one row per ticker per trading day," which
    is the natural grain for evaluation downstream — the backtester's
    _scanner_lift / _team_lift / _cio_lift joins on eval_date and the
    scanner/team/cio eval rows will always find a matching trading-day row
    here regardless of whether research happened to run that week.

    Args:
        bucket: S3 bucket name (research.db location)
        db_path: path to local research.db
        signals_prefix: deprecated, kept for API compatibility with the
            previous signal-folder-driven enumeration. Unused.
        sector_map_key: S3 key for sector map JSON
        max_lookback_trading_days: how far back to walk. Default 90 trading
            days (~18 calendar weeks) which is enough for rolling evaluator
            windows and well past any single weekly run's catch-up needs.
        dry_run: if True, compute but don't write to DB

    Returns:
        dict with status, dates_processed, rows_inserted, errors
    """
    del signals_prefix  # deprecated; kept in the signature for call-site compat
    from polygon_client import polygon_client

    try:
        client = polygon_client()
    except ValueError as e:
        logger.warning("Polygon client init failed: %s", e)
        return {"status": "error", "error": str(e)}

    s3 = boto3.client("s3")
    sector_map = _load_sector_map(s3, bucket, sector_map_key)

    _ensure_table(db_path)
    today = date.today()
    existing = _get_existing_dates(db_path, today=today)

    dates_to_process = _trading_days_to_process(
        today, max_lookback_trading_days, existing
    )

    if not dates_to_process:
        logger.info(
            "All trading days in the last %d lookback already have return_5d populated",
            max_lookback_trading_days,
        )
        return {
            "status": "ok" if not dry_run else "ok_dry_run",
            "dates_processed": 0,
            "rows_inserted": 0,
            "skipped": len(existing),
        }

    logger.info(
        "Processing %d trading days (lookback=%d, %d already populated)",
        len(dates_to_process), max_lookback_trading_days, len(existing),
    )

    total_inserted = 0
    errors = []

    for eval_date in dates_to_process:
        try:
            rows = _build_rows_for_date(eval_date, client, sector_map)
            if not rows:
                errors.append({"date": eval_date, "error": "no rows computed"})
                continue

            if not dry_run:
                inserted = _insert_rows(db_path, rows)
                total_inserted += inserted
                logger.info("universe_returns: %s -> %d rows inserted", eval_date, inserted)
            else:
                total_inserted += len(rows)
                logger.info("universe_returns (dry-run): %s -> %d rows computed", eval_date, len(rows))
        except Exception as e:
            logger.warning("universe_returns: failed for %s: %s", eval_date, e)
            errors.append({"date": eval_date, "error": str(e)})

    # Upload updated research.db back to S3 — BOTH the live pointer and the
    # dated backup, through the one writer that owns them (alpha-engine-config-
    # I10202). Writing the pointer alone here is what let the dated series die
    # unnoticed for 59 days while its freshness row read healthy.
    #
    # No try/except: a failed producer write must fail the run. The previous
    # WARNING-and-continue is why nothing went red.
    #
    # `db_upload` names the two keys ACTUALLY written this run (empty when
    # total_inserted == 0 — a run that inserted nothing legitimately uploads
    # nothing) — alpha-engine-config-I10855 deliverable 2: the run manifest
    # must record what a run wrote, never copy the two keys from the
    # descriptor unconditionally.
    db_upload: dict = {}
    if not dry_run and total_inserted > 0:
        db_upload = upload_research_db(s3, db_path, bucket, run_date or default_run_date())

    # Any real error (exception or "no rows computed" after pre-filter) is a
    # hard failure under the no-silent-fails rule. The old `partial` path was
    # being dropped by the Step Function. The trading-day enumerator only
    # yields dates whose 5d forward window has closed, so there is no
    # "deferred" concept — every enqueued date is expected to succeed.
    if errors:
        status = "error"
    elif dry_run:
        status = "ok_dry_run"
    else:
        status = "ok"

    return {
        "status": status,
        "dates_processed": len(dates_to_process),
        "rows_inserted": total_inserted,
        "errors": errors[:20],
        "db_upload": db_upload,
    }


# -- Trading-day enumeration -------------------------------------------------

def _trading_days_to_process(
    today: date,
    max_lookback: int,
    existing: set[str],
) -> list[str]:
    """Enumerate NYSE trading days whose 5d forward window has closed.

    Walks backwards from `today` across up to `max_lookback` trading days
    (skipping weekends and NYSE holidays). For each, includes the date in
    the result when:
      - the 5d forward window has closed (so return_5d is computable), AND
      - the date is not already in `existing` (the set of dates that have
        return_5d populated in the DB).

    Returns ISO dates sorted chronologically. The trading-calendar module
    at the repo root handles holiday awareness (market closures through 2030).
    """
    from nousergon_lib.trading_calendar import is_trading_day as nyse_is_trading_day

    out: list[str] = []
    d = today
    trading_days_seen = 0
    # Sanity fence — cap walk at roughly max_lookback * 1.5 in calendar days
    # to protect against a broken is_trading_day implementation looping forever.
    calendar_budget = max_lookback * 3 + 30
    while trading_days_seen < max_lookback and calendar_budget > 0:
        if nyse_is_trading_day(d):
            trading_days_seen += 1
            iso = d.isoformat()
            fwd_5d = _add_trading_days(d, 5)
            if fwd_5d < today and iso not in existing:
                out.append(iso)
        d -= timedelta(days=1)
        calendar_budget -= 1
    out.sort()
    return out


# -- Sector map loading ------------------------------------------------------

def _load_sector_map(s3, bucket: str, key: str) -> dict[str, str] | None:
    """Load ticker -> sector ETF mapping from S3."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        logger.warning("Could not load sector_map from s3://%s/%s: %s", bucket, key, e)
        return None


# -- DB helpers ---------------------------------------------------------------

def _ensure_table(db_path: str) -> None:
    """Create universe_returns table if it doesn't exist, and add new columns.

    Idempotent migrations:
      - 30d columns (older migration; preserved for back-compat with DBs created
        before 30d was a first-class horizon)
      - 21d arithmetic + log-domain columns (added 2026-05-09 to align the
        measurement substrate with the predictor's canonical 21d log target)
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(_CREATE_TABLE_SQL)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
        # return_30d DROPPED 2026-08-22 (alpha-engine-config-I8185) — see
        # module docstring. spy_return_30d/beat_spy_30d are unaffected.
        for col, col_type in [("spy_return_30d", "REAL"), ("beat_spy_30d", "INTEGER")]:
            if col not in cols:
                conn.execute(f"ALTER TABLE universe_returns ADD COLUMN {col} {col_type}")
        for col, col_type in _NEW_COLUMNS_21D:
            if col not in cols:
                conn.execute(f"ALTER TABLE universe_returns ADD COLUMN {col} {col_type}")
        # W3.1 (L4469): 60d/90d horizon columns — additive migration for the
        # predictor horizon study + backtester 60/90d signal quality.
        for col, col_type in _NEW_COLUMNS_60_90D:
            if col not in cols:
                conn.execute(f"ALTER TABLE universe_returns ADD COLUMN {col} {col_type}")
        # config#1981: 1d/3d/15d decay-ladder columns — additive migration for
        # the alpha-decay-curve intermediate-horizon backfill.
        for col, col_type in _NEW_COLUMNS_DECAY_LADDER:
            if col not in cols:
                conn.execute(f"ALTER TABLE universe_returns ADD COLUMN {col} {col_type}")
        # alpha-engine-config-I7936 — exchange test securities are not
        # companies, and this table is the DENOMINATOR for every lift
        # calculation in the backtester evaluation framework. They entered via
        # polygon's grouped-daily endpoint, which returns the whole tape; the
        # producer now skips them (_TEST_SECURITIES), and this removes the rows
        # already stored.
        #
        # Deliberately a STANDING INVARIANT enforced on every run rather than a
        # one-shot migration: if a new test symbol is ever listed, or a
        # partially-migrated copy of research.db is restored from a backup, the
        # next collection heals it. Runs in-region as part of the weekly
        # collector, so no operator step and no laptop write.
        #
        # Measured before this landed (2026-08-21): 1,397 rows. ZWZZT's
        # 6686.1759 was the maximum of return_5d over 2,012,661 rows
        # (19.44 -> 129,998.70 in five sessions); the universe-wide mean 5-day
        # return was 0.004758 with these rows and 0.001418 without.
        placeholders = ", ".join("?" for _ in _TEST_SECURITIES)
        purged = conn.execute(
            f"DELETE FROM universe_returns WHERE ticker IN ({placeholders})",
            tuple(sorted(_TEST_SECURITIES)),
        ).rowcount
        if purged:
            logger.warning(
                "universe_returns: purged %d exchange-test-security row(s) "
                "(alpha-engine-config-I7936)", purged,
            )
        conn.commit()
    finally:
        conn.close()


def _get_existing_dates(db_path: str, today: date | None = None) -> set[str]:
    """Return set of eval_dates with all already-closed forward windows populated.

    A date is "complete" (and therefore safely skippable) when:
      - return_5d is non-NULL, AND
      - return_21d is non-NULL OR the 21d forward window has not yet closed.

    Rows where 21d is stale-NULL (window has closed but column was written
    before this PR landed, or written before the 21d window closed) get
    re-enqueued so the new 21d arithmetic + log columns can be backfilled
    on the next run. INSERT OR REPLACE in `_insert_rows` overwrites the
    full row idempotently — re-fetching polygon prices for the same eval_date
    yields the same historical close, so the 5d/10d/30d cells are unchanged.

    RESOLVED (config#2972 / alpha-engine-config-I8185): this gating only ever
    checked return_5d/return_21d, so a date already 21d-complete was never
    revisited when its later 30d/60d/90d windows closed. Combined with
    `_trading_days_to_process`'s bounded max_lookback walk (default 90
    trading days from `today`), any date whose 21d window closed outside
    that walk depth never got reconsidered for 60d/90d backfill at all —
    the confirmed, sole reason `return_30d`/`return_60d`/`return_90d`/
    `log_return_60d`/`log_return_90d` were 0-of-2.14M non-null fleet-wide.
    Rather than add a per-horizon gating column to populate columns nothing
    consumes, alpha-engine-config-I8185 DROPPED those five columns instead
    (grep-verified no consumer across `crucible-*`/`nousergon-data`) — see
    the module docstring. This function's 5d/21d gating is therefore
    correct as written: nothing beyond 21d is stored anymore, so nothing
    beyond 21d needs its own completeness gate.
    """
    today = today or date.today()
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT eval_date, "
            "MAX(CASE WHEN return_5d IS NOT NULL THEN 1 ELSE 0 END), "
            "MAX(CASE WHEN return_21d IS NOT NULL THEN 1 ELSE 0 END) "
            "FROM universe_returns GROUP BY eval_date"
        ).fetchall()
    finally:
        conn.close()

    out: set[str] = set()
    for eval_date_str, has_5d, has_21d in rows:
        if not has_5d:
            continue
        eval_dt = date.fromisoformat(eval_date_str)
        fwd_21d_closed = _add_trading_days(eval_dt, 21) < today
        if fwd_21d_closed and not has_21d:
            continue
        out.add(eval_date_str)
    return out


def _insert_rows(db_path: str, rows: list[dict]) -> int:
    """Insert rows into universe_returns; reprocessed dates overwrite stale rows.

    Uses INSERT OR REPLACE so a date that was previously inserted with NULL
    forward-return columns (because the 5d window hadn't closed yet) gets
    its returns filled in on reprocessing. The previous INSERT OR IGNORE
    behaviour left those NULL rows stuck forever.
    """
    conn = sqlite3.connect(db_path)
    try:
        inserted = 0
        for row in rows:
            try:
                # return_30d/return_60d/return_90d/log_return_60d/log_return_90d
                # DROPPED 2026-08-22 (alpha-engine-config-I8185) — no longer
                # written; see module docstring.
                conn.execute(
                    "INSERT OR REPLACE INTO universe_returns "
                    "(ticker, eval_date, sector, close_price, "
                    "return_5d, return_10d, return_21d, "
                    "spy_return_5d, spy_return_10d, spy_return_21d, spy_return_30d, "
                    "beat_spy_5d, beat_spy_10d, beat_spy_21d, beat_spy_30d, "
                    "log_return_21d, log_spy_return_21d, "
                    "spy_return_60d, spy_return_90d, "
                    "beat_spy_60d, beat_spy_90d, "
                    "log_spy_return_60d, log_spy_return_90d, "
                    "sector_etf, sector_etf_return_5d, beat_sector_5d, "
                    "return_1d, return_3d, return_15d, "
                    "spy_return_1d, spy_return_3d, spy_return_15d, "
                    "beat_spy_1d, beat_spy_3d, beat_spy_15d, "
                    "log_return_1d, log_return_3d, log_return_15d, "
                    "log_spy_return_1d, log_spy_return_3d, log_spy_return_15d) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        row["ticker"], row["eval_date"], row["sector"], row["close_price"],
                        row["return_5d"], row["return_10d"], row["return_21d"],
                        row["spy_return_5d"], row["spy_return_10d"], row["spy_return_21d"], row["spy_return_30d"],
                        row["beat_spy_5d"], row["beat_spy_10d"], row["beat_spy_21d"], row["beat_spy_30d"],
                        row["log_return_21d"], row["log_spy_return_21d"],
                        row.get("spy_return_60d"), row.get("spy_return_90d"),
                        row.get("beat_spy_60d"), row.get("beat_spy_90d"),
                        row.get("log_spy_return_60d"), row.get("log_spy_return_90d"),
                        row["sector_etf"], row["sector_etf_return_5d"], row["beat_sector_5d"],
                        row.get("return_1d"), row.get("return_3d"), row.get("return_15d"),
                        row.get("spy_return_1d"), row.get("spy_return_3d"), row.get("spy_return_15d"),
                        row.get("beat_spy_1d"), row.get("beat_spy_3d"), row.get("beat_spy_15d"),
                        row.get("log_return_1d"), row.get("log_return_3d"), row.get("log_return_15d"),
                        row.get("log_spy_return_1d"), row.get("log_spy_return_3d"), row.get("log_spy_return_15d"),
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        return inserted
    finally:
        conn.close()


# -- Row building (polygon.io) -----------------------------------------------

# -- "not yet published" is not a collector failure (config-I7714) ------------
#
# Polygon returns 403 `"Attempted to request today's data before end of day"`
# for a grouped-daily bar on the CURRENT session before its plan-tier EOD
# publication. `polygon_client._get` raises `PolygonForbiddenError` on every 403
# — deliberately, since data#4496: swallowing 403s once masked the 2026-04-17 to
# 2026-04-23 VWAP outage by letting `daily_closes.collect` fall through to
# yfinance and write VWAP=None for every stock. That raise stays.
#
# But this collector walks FORWARD dates from each eval_date, so it asks about
# the current session by construction, and the request is simply early. Measured
# 2026-08-18 (`watch-rerun-2026-08-18-3`): DataPhase1 ran to completion in 2723s
# and then exited 1 because this collector raised, `weekly_collector.py` marked
# the phase `partial`, and the weekly pipeline halted. It had not succeeded
# since 2026-08-15 as a result, which is why every artifact downstream of the
# weekly graph was four days old.
#
# So: exactly one 403 shape, for exactly one date range, is treated as "not yet
# available" and yields no bars for that date. Every other 403 — an invalid key,
# a revoked plan, an entitlement the account does not hold — still raises, which
# is the failure mode the raise exists for. Narrowing on BOTH the message and
# the date is what keeps this from becoming the swallow it replaces.

_BEFORE_EOD_MARKER = "before end of day"


def _grouped_daily_or_empty(polygon_client, date_str: str, *, today: date) -> dict:
    """`get_grouped_daily`, tolerating only the before-EOD 403 for a session
    that has not closed yet.

    A 403 on a PAST date is re-raised: that session is over, so "before end of
    day" cannot be the true explanation, and reading it as absent data would
    silently drop returns the run needs.
    """
    try:
        return polygon_client.get_grouped_daily(date_str)
    except PolygonForbiddenError as exc:
        if _BEFORE_EOD_MARKER not in str(exc).lower():
            raise
        try:
            requested = date.fromisoformat(date_str)
        except ValueError:
            raise exc from None
        if requested < today:
            raise
        logger.info(
            "universe_returns: %s has not published yet (polygon 403 "
            "'%s') — no bars for that date this run, not a collector failure "
            "(config-I7714)",
            date_str, _BEFORE_EOD_MARKER,
        )
        return {}


def _build_rows_for_date(
    eval_date: str,
    polygon_client,
    sector_map: dict[str, str] | None,
) -> list[dict]:
    """Build universe_returns rows for a single eval_date."""
    eval_dt = date.fromisoformat(eval_date)
    fwd_1d = _add_trading_days(eval_dt, 1)
    fwd_3d = _add_trading_days(eval_dt, 3)
    fwd_5d = _add_trading_days(eval_dt, 5)
    fwd_10d = _add_trading_days(eval_dt, 10)
    fwd_15d = _add_trading_days(eval_dt, 15)
    fwd_21d = _add_trading_days(eval_dt, 21)
    fwd_30d = _add_trading_days(eval_dt, 30)
    fwd_60d = _add_trading_days(eval_dt, 60)
    fwd_90d = _add_trading_days(eval_dt, 90)

    # Check that forward dates are in the past (returns can be computed)
    today = date.today()
    if fwd_5d >= today:
        logger.debug("Skipping %s: 5d forward date %s is in the future", eval_date, fwd_5d)
        return []

    has_1d = fwd_1d < today
    has_3d = fwd_3d < today
    has_10d = fwd_10d < today
    has_15d = fwd_15d < today
    has_21d = fwd_21d < today
    has_30d = fwd_30d < today
    has_60d = fwd_60d < today
    has_90d = fwd_90d < today

    # Fetch grouped-daily prices for eval_date and forward dates
    prices_t0 = _grouped_daily_or_empty(polygon_client, eval_date, today=today)
    prices_1d = _grouped_daily_or_empty(polygon_client, str(fwd_1d), today=today) if has_1d else {}
    prices_3d = _grouped_daily_or_empty(polygon_client, str(fwd_3d), today=today) if has_3d else {}
    prices_5d = _grouped_daily_or_empty(polygon_client, str(fwd_5d), today=today)
    prices_10d = _grouped_daily_or_empty(polygon_client, str(fwd_10d), today=today) if has_10d else {}
    prices_15d = _grouped_daily_or_empty(polygon_client, str(fwd_15d), today=today) if has_15d else {}
    prices_21d = _grouped_daily_or_empty(polygon_client, str(fwd_21d), today=today) if has_21d else {}
    prices_30d = _grouped_daily_or_empty(polygon_client, str(fwd_30d), today=today) if has_30d else {}
    prices_60d = _grouped_daily_or_empty(polygon_client, str(fwd_60d), today=today) if has_60d else {}
    prices_90d = _grouped_daily_or_empty(polygon_client, str(fwd_90d), today=today) if has_90d else {}

    if not prices_t0:
        logger.warning("No prices for eval_date %s — may be a non-trading day", eval_date)
        # Try next business day
        next_day = _add_trading_days(eval_dt, 1)
        # config-I7714 — this fallback was the one grouped-daily call with no
        # `< today` guard at all, so on a run whose eval_date is the last closed
        # session it asked polygon for the CURRENT one.
        if next_day >= today:
            logger.debug(
                "Skipping %s: next trading day %s has not closed yet",
                eval_date, next_day,
            )
            return []
        prices_t0 = _grouped_daily_or_empty(polygon_client, str(next_day), today=today)
        if not prices_t0:
            return []

    # SPY benchmark
    spy_t0 = prices_t0.get("SPY", {}).get("close")
    spy_1d = prices_1d.get("SPY", {}).get("close") if has_1d else None
    spy_3d = prices_3d.get("SPY", {}).get("close") if has_3d else None
    spy_5d = prices_5d.get("SPY", {}).get("close")
    spy_10d = prices_10d.get("SPY", {}).get("close") if has_10d else None
    spy_15d = prices_15d.get("SPY", {}).get("close") if has_15d else None
    spy_21d = prices_21d.get("SPY", {}).get("close") if has_21d else None
    spy_30d = prices_30d.get("SPY", {}).get("close") if has_30d else None
    spy_60d = prices_60d.get("SPY", {}).get("close") if has_60d else None
    spy_90d = prices_90d.get("SPY", {}).get("close") if has_90d else None

    spy_ret_1d = _pct_return(spy_t0, spy_1d) if has_1d else None
    spy_ret_3d = _pct_return(spy_t0, spy_3d) if has_3d else None
    spy_ret_5d = _pct_return(spy_t0, spy_5d)
    spy_ret_10d = _pct_return(spy_t0, spy_10d) if has_10d else None
    spy_ret_15d = _pct_return(spy_t0, spy_15d) if has_15d else None
    spy_ret_21d = _pct_return(spy_t0, spy_21d) if has_21d else None
    spy_ret_30d = _pct_return(spy_t0, spy_30d) if has_30d else None
    spy_ret_60d = _pct_return(spy_t0, spy_60d) if has_60d else None
    spy_ret_90d = _pct_return(spy_t0, spy_90d) if has_90d else None
    log_spy_ret_1d = _log_return(spy_t0, spy_1d) if has_1d else None
    log_spy_ret_3d = _log_return(spy_t0, spy_3d) if has_3d else None
    log_spy_ret_15d = _log_return(spy_t0, spy_15d) if has_15d else None
    log_spy_ret_21d = _log_return(spy_t0, spy_21d) if has_21d else None
    log_spy_ret_60d = _log_return(spy_t0, spy_60d) if has_60d else None
    log_spy_ret_90d = _log_return(spy_t0, spy_90d) if has_90d else None

    # Sector ETF returns
    sector_etf_returns_5d: dict[str, float | None] = {}
    for etf in _SECTOR_ETFS:
        etf_t0 = prices_t0.get(etf, {}).get("close")
        etf_5d = prices_5d.get(etf, {}).get("close")
        sector_etf_returns_5d[etf] = _pct_return(etf_t0, etf_5d)

    # Build rows for all tickers
    rows = []
    for ticker, bar in prices_t0.items():
        if ticker in _SKIP_TICKERS:
            continue

        close_t0 = bar.get("close")
        if close_t0 is None or close_t0 <= 0:
            continue

        close_1d = prices_1d.get(ticker, {}).get("close") if has_1d else None
        close_3d = prices_3d.get(ticker, {}).get("close") if has_3d else None
        close_5d = prices_5d.get(ticker, {}).get("close")
        close_10d = prices_10d.get(ticker, {}).get("close") if has_10d else None
        close_15d = prices_15d.get(ticker, {}).get("close") if has_15d else None
        close_21d = prices_21d.get(ticker, {}).get("close") if has_21d else None
        close_30d = prices_30d.get(ticker, {}).get("close") if has_30d else None
        close_60d = prices_60d.get(ticker, {}).get("close") if has_60d else None
        close_90d = prices_90d.get(ticker, {}).get("close") if has_90d else None

        ret_1d = _pct_return(close_t0, close_1d) if has_1d else None
        ret_3d = _pct_return(close_t0, close_3d) if has_3d else None
        ret_5d = _pct_return(close_t0, close_5d)
        ret_10d = _pct_return(close_t0, close_10d) if has_10d else None
        ret_15d = _pct_return(close_t0, close_15d) if has_15d else None
        ret_21d = _pct_return(close_t0, close_21d) if has_21d else None
        # ret_30d/ret_60d/ret_90d are kept as locals (not persisted as
        # return_30d/return_60d/return_90d — DROPPED 2026-08-22,
        # alpha-engine-config-I8185) because beat_spy_30d/60d/90d still
        # derive from them below.
        ret_30d = _pct_return(close_t0, close_30d) if has_30d else None
        ret_60d = _pct_return(close_t0, close_60d) if has_60d else None
        ret_90d = _pct_return(close_t0, close_90d) if has_90d else None
        log_ret_1d = _log_return(close_t0, close_1d) if has_1d else None
        log_ret_3d = _log_return(close_t0, close_3d) if has_3d else None
        log_ret_15d = _log_return(close_t0, close_15d) if has_15d else None
        log_ret_21d = _log_return(close_t0, close_21d) if has_21d else None

        # Sector classification
        sector_etf = sector_map.get(ticker) if sector_map else None
        sector = _ETF_TO_SECTOR.get(sector_etf, "") if sector_etf else ""
        etf_ret_5d = sector_etf_returns_5d.get(sector_etf) if sector_etf else None

        rows.append({
            "ticker": ticker,
            "eval_date": eval_date,
            "sector": sector,
            "close_price": round(close_t0, 2),
            "return_5d": round(ret_5d, 4) if ret_5d is not None else None,
            "return_10d": round(ret_10d, 4) if ret_10d is not None else None,
            "return_21d": round(ret_21d, 4) if ret_21d is not None else None,
            "spy_return_5d": round(spy_ret_5d, 4) if spy_ret_5d is not None else None,
            "spy_return_10d": round(spy_ret_10d, 4) if spy_ret_10d is not None else None,
            "spy_return_21d": round(spy_ret_21d, 4) if spy_ret_21d is not None else None,
            "spy_return_30d": round(spy_ret_30d, 4) if spy_ret_30d is not None else None,
            "beat_spy_5d": int(ret_5d > spy_ret_5d) if ret_5d is not None and spy_ret_5d is not None else None,
            "beat_spy_10d": int(ret_10d > spy_ret_10d) if ret_10d is not None and spy_ret_10d is not None else None,
            "beat_spy_21d": int(ret_21d > spy_ret_21d) if ret_21d is not None and spy_ret_21d is not None else None,
            "beat_spy_30d": int(ret_30d > spy_ret_30d) if ret_30d is not None and spy_ret_30d is not None else None,
            "spy_return_60d": round(spy_ret_60d, 4) if spy_ret_60d is not None else None,
            "spy_return_90d": round(spy_ret_90d, 4) if spy_ret_90d is not None else None,
            "beat_spy_60d": int(ret_60d > spy_ret_60d) if ret_60d is not None and spy_ret_60d is not None else None,
            "beat_spy_90d": int(ret_90d > spy_ret_90d) if ret_90d is not None and spy_ret_90d is not None else None,
            "log_return_21d": round(log_ret_21d, 6) if log_ret_21d is not None else None,
            "log_spy_return_21d": round(log_spy_ret_21d, 6) if log_spy_ret_21d is not None else None,
            "log_spy_return_60d": round(log_spy_ret_60d, 6) if log_spy_ret_60d is not None else None,
            "log_spy_return_90d": round(log_spy_ret_90d, 6) if log_spy_ret_90d is not None else None,
            "sector_etf": sector_etf,
            "sector_etf_return_5d": round(etf_ret_5d, 4) if etf_ret_5d is not None else None,
            "beat_sector_5d": int(ret_5d > etf_ret_5d) if ret_5d is not None and etf_ret_5d is not None else None,
            "return_1d": round(ret_1d, 4) if ret_1d is not None else None,
            "return_3d": round(ret_3d, 4) if ret_3d is not None else None,
            "return_15d": round(ret_15d, 4) if ret_15d is not None else None,
            "spy_return_1d": round(spy_ret_1d, 4) if spy_ret_1d is not None else None,
            "spy_return_3d": round(spy_ret_3d, 4) if spy_ret_3d is not None else None,
            "spy_return_15d": round(spy_ret_15d, 4) if spy_ret_15d is not None else None,
            "beat_spy_1d": int(ret_1d > spy_ret_1d) if ret_1d is not None and spy_ret_1d is not None else None,
            "beat_spy_3d": int(ret_3d > spy_ret_3d) if ret_3d is not None and spy_ret_3d is not None else None,
            "beat_spy_15d": int(ret_15d > spy_ret_15d) if ret_15d is not None and spy_ret_15d is not None else None,
            "log_return_1d": round(log_ret_1d, 6) if log_ret_1d is not None else None,
            "log_return_3d": round(log_ret_3d, 6) if log_ret_3d is not None else None,
            "log_return_15d": round(log_ret_15d, 6) if log_ret_15d is not None else None,
            "log_spy_return_1d": round(log_spy_ret_1d, 6) if log_spy_ret_1d is not None else None,
            "log_spy_return_3d": round(log_spy_ret_3d, 6) if log_spy_ret_3d is not None else None,
            "log_spy_return_15d": round(log_spy_ret_15d, 6) if log_spy_ret_15d is not None else None,
        })

    return rows


# -- Helpers ------------------------------------------------------------------

def _pct_return(price_start: float | None, price_end: float | None) -> float | None:
    """Compute percentage return (as decimal, e.g. 0.05 = 5%)."""
    if price_start is None or price_end is None or price_start <= 0:
        return None
    return (price_end / price_start) - 1.0


def _log_return(price_start: float | None, price_end: float | None) -> float | None:
    """Compute log return ln(price_end / price_start). Decimal log-units."""
    if price_start is None or price_end is None or price_start <= 0 or price_end <= 0:
        return None
    return math.log(price_end / price_start)
