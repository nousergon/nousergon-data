"""
builders/backfill.py — Historical backfill of ArcticDB universe from S3 price cache.

Loads the full 10-year price cache from S3, computes all 53 features for every
ticker's full history, and writes each ticker as a symbol in the ArcticDB
universe library. Also writes macro features to the macro library.

This is a one-time migration script (Phase 1 of the unified data layer plan).
After initial backfill, the weekly Saturday pipeline rebuilds from fresh data,
and the daily weekday pipeline appends new rows.

Usage:
    python -m builders.backfill                          # full backfill
    python -m builders.backfill --dry-run                # compute but skip ArcticDB write
    python -m builders.backfill --ticker AAPL            # single ticker (for testing)
    python -m builders.backfill --validate               # backfill + spot-check validation
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

from features.feature_engineer import (
    FEATURES,
    MIN_ROWS_FOR_FEATURES,
    compute_features,
)
from features.factor_momentum import materialize_factor_momentum
from features.cross_sectional import materialize_factor_loading_zscores
from features.compute import (
    DEFAULT_BUCKET,
    SOURCE_CATEGORIES,
    UNIVERSE_BENCHMARK_PROXIES,
    _SKIP_TICKERS,
    _UNIVERSE_EXTRA,
    admits_universe_write,
    _apply_daily_delta,
    _build_registry,
    audit_action_jumps,
    _is_sector_etf,
    _load_parquet_from_s3,
    _load_sector_map,
    _load_cached_fundamentals,
    _load_cached_alternative,
    make_source_series,
)
from corporate_actions import CorporateActionAuditError
from store.arctic_store import (
    OHLCV_COLS as _CANONICAL_OHLCV_COLS,
    PROVENANCE_COL as _CANONICAL_PROVENANCE_COL,
    get_universe_lib,
    get_macro_lib,
    to_arctic_canonical,
    to_arctic_safe,
)
from builders._price_cache_writeboth import (
    PRICE_CACHE_LEGACY_PREFIX,
    list_price_cache_keys,
)
from builders._constituents_loader import load_constituents_for_run_date
from builders.daily_append import _scan_universe_and_emit_freshness_receipt
from collectors.prices import _ALWAYS_DOWNLOAD, _SUB_SECTOR_ETFS

log = logging.getLogger(__name__)

# Canonical universe-library schema — single source of truth in
# ``store.arctic_store``. Re-exported here for the local float32-cast
# loop + partial-features observability log, both of which iterate
# FEATURES / OHLCV_COLS directly. VWAP centralization rationale
# (2026-04-17 Phase 7): historical price_cache parquets predate polygon,
# carry no source for true volume-weighted VWAP, and we do NOT
# synthesize a ``(H+L+C)/3`` proxy — historical rows get NaN VWAP and
# the column populates from the first daily_append run against a
# polygon-sourced daily_closes parquet.
OHLCV_COLS = _CANONICAL_OHLCV_COLS
PROVENANCE_COL = _CANONICAL_PROVENANCE_COL

#: The price-cache stems a single-ticker (``ticker_filter``) write reads
#: besides the ticker itself: every macro / benchmark / sector-ETF series
#: ``_extract_macro_series`` and ``compute_features`` consume. A per-ticker
#: write never needs another STOCK's parquet — the cross-sectional second
#: passes (factor momentum, loading z-scores) are full-universe only — so
#: loading all ~930 parquets to write one symbol was pure cost, and it was
#: also a hazard: ``_apply_daily_delta`` restates and marks-applied every
#: registered split across whatever it was handed, while only the filtered
#: ticker is written (alpha-engine-config-I11444).
#: Every non-sector macro series ``_extract_macro_series`` pulls out of the
#: price cache AND ``backfill()`` writes to the ArcticDB macro library as a raw
#: ``Close`` series. ONE list for both sides, because two literals is how
#: ``TWO`` and ``BAA10Y`` went missing: ``collectors/fred_history.py`` has
#: written their 10y parquets every Saturday since alpha-engine-config-I9287,
#: ``CUMULATIVE_MACRO_SYMBOLS`` declares both, and yet neither symbol existed in
#: ArcticDB ``macro`` (measured 2026-09-24), so crucible-predictor's Stage-1b
#: macro block read them as absent and graded ``finite_pct=0.00``
#: (alpha-engine-config-I11471 / I10068). ``tests/test_macro_fred_series_reach_arctic.py``
#: asserts every ``FRED_HISTORY_MAP`` key is here.
_RAW_MACRO_SERIES: tuple[str, ...] = (
    "SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
    # config#939 — credit spreads. FRED-only (BAMLH0A0HYM2), licence-gated to
    # 2023+ on FRED; absent from price_data (e.g. pre-Stage-2.5 cache) →
    # macro.get("HYOAS") returns None downstream, which compute_features
    # already neutral-defaults rather than crashing.
    "HYOAS",
    # FRED-only (DGS2 / BAA10Y). Regime-substrate inputs for crucible-predictor
    # (yield_curve_10y_2y, baa10y_level, baa10y_change_21d); BAA10Y is the
    # full-history credit-regime series HYOAS cannot be.
    "TWO", "BAA10Y",
)

_PER_TICKER_CACHE_CONTEXT = frozenset({*_ALWAYS_DOWNLOAD, *_RAW_MACRO_SERIES})


def _load_current_constituents(s3, bucket: str, run_date: str | None = None) -> set[str]:
    """Load the current S&P 500 / 400 constituents set.

    Thin wrapper around :func:`load_constituents_for_run_date` —
    preserved for backwards compatibility with backfill's caller sites
    that consume just the ticker set (not the weekly_date tuple). New
    code should call :func:`load_constituents_for_run_date` directly.

    Lifted 2026-05-24 (ROADMAP L1397): the run_date-aware constituents
    read is now a single chokepoint at
    :func:`builders._constituents_loader.load_constituents_for_run_date`,
    shared with ``prune_delisted_tickers``. See module docstring there
    for the TOCTOU defect class this closes (see 2026-05-23 SF failure).
    """
    tickers, _ = load_constituents_for_run_date(s3, bucket, run_date=run_date)
    return tickers


def _cache_key_stem(key: str) -> str:
    return key.split("/")[-1].replace(".parquet", "")


def _load_full_cache(
    s3,
    bucket: str,
    prefix: str = PRICE_CACHE_LEGACY_PREFIX,
    only: frozenset[str] | set[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Load all 10-year price cache parquets from S3 (concurrent).

    ``only``, when given, restricts the download to those ticker stems — the
    per-ticker (``ticker_filter``) path passes the ticker plus
    ``_PER_TICKER_CACHE_CONTEXT``. ``None`` (the weekly full backfill) loads
    everything, unchanged.

    Wave-3 reader migration (ROADMAP L1401): when ``prefix`` is the
    production default the listing iterates both
    ``reference/price_cache/`` (new) and ``predictor/price_cache/``
    (legacy) via :func:`list_price_cache_keys`, deduping by
    ``{ticker}.parquet`` basename so each ticker is fetched once.
    Custom prefixes (tests, ad-hoc invocations) opt out of the
    fallback chain.
    """
    keys = list_price_cache_keys(s3, bucket, prefix)
    if only is not None:
        keys = [k for k in keys if _cache_key_stem(k) in only]

    if not keys:
        log.error("No parquets found in s3://%s/%s (read-prefix chain)", bucket, prefix)
        return {}

    log.info(
        "Downloading %d full cache parquets from s3://%s/ (read-prefix chain anchored on %s) ...",
        len(keys), bucket, prefix,
    )

    price_data: dict[str, pd.DataFrame] = {}
    errors = 0

    def _download(key: str) -> tuple[str, pd.DataFrame | None]:
        ticker = _cache_key_stem(key)
        try:
            df = _load_parquet_from_s3(s3, bucket, key)
            if df.empty:
                return ticker, None
            return ticker, df
        except Exception:
            return ticker, None

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_download, k): k for k in keys}
        for fut in as_completed(futures):
            ticker, df = fut.result()
            if df is not None:
                price_data[ticker] = df
            else:
                errors += 1

    log.info("Full cache loaded: %d tickers OK, %d errors", len(price_data), errors)
    return price_data


def _extract_macro_series(price_data: dict[str, pd.DataFrame]) -> dict[str, pd.Series]:
    """Extract macro/ETF Close series from price data."""
    macro: dict[str, pd.Series] = {}
    for key in _RAW_MACRO_SERIES:
        df = price_data.get(key)
        if df is not None and "Close" in df.columns:
            macro[key] = df["Close"].dropna()

    # Sector ETFs
    for stem, df in price_data.items():
        if stem.startswith("XL") and len(stem) <= 4 and "Close" in df.columns:
            macro[stem] = df["Close"].dropna()

    # alpha-engine-config-I9289 — the 8 sub-sector benchmark ETFs
    # (collectors/prices.py::_SUB_SECTOR_ETFS) were added to the price
    # cache's ``_ALWAYS_DOWNLOAD`` list (config#934) but never added HERE,
    # so ``_write_macro_series_no_shrink``'s raw-series loop below never
    # wrote their full price-cache history to ArcticDB — only
    # ``builders/daily_append.py``'s per-day incremental append touched
    # them. Result: every one of the eight was stuck at exactly one row per
    # weekday since their 2026-07-23 birth (26 rows measured 2026-08-29)
    # while every sibling macro symbol carried ~2514 rows from 2016. Adding
    # them here means the NEXT weekly backfill writes whatever the price
    # cache actually holds — closing the loop once the parquet itself is
    # repaired (``builders/repair_macro_series.py --symbols
    # SMH,IGV,XBI,PPH,XOP,KRE,ITA,GDX``, run in-region per AGENTS.md).
    for stem in _SUB_SECTOR_ETFS:
        df = price_data.get(stem)
        if df is not None and "Close" in df.columns:
            macro[stem] = df["Close"].dropna()

    return macro


def _build_macro_features_df(macro: dict[str, pd.Series]) -> pd.DataFrame:
    """Build a DataFrame of macro features (one row per date) for the macro library."""
    vix = macro.get("VIX")
    tnx = macro.get("TNX")
    irx = macro.get("IRX")
    gld = macro.get("GLD")
    uso = macro.get("USO")
    vix3m = macro.get("VIX3M")
    spy = macro.get("SPY")

    if vix is None or spy is None:
        log.warning("Missing VIX or SPY — macro features will be incomplete")
        return pd.DataFrame()

    # Build on the VIX index (available for all trading dates)
    idx = vix.index
    df = pd.DataFrame(index=idx)

    df["vix_level"] = (vix.reindex(idx) / 20.0).astype("float32")
    if tnx is not None:
        df["yield_10y"] = (tnx.reindex(idx) / 10.0).astype("float32")
    if tnx is not None and irx is not None:
        df["yield_curve_slope"] = ((tnx.reindex(idx) - irx.reindex(idx)) / 10.0).astype("float32")
    if gld is not None:
        df["gold_mom_5d"] = gld.reindex(idx).pct_change(5).astype("float32")
    if uso is not None:
        df["oil_mom_5d"] = uso.reindex(idx).pct_change(5).astype("float32")
    if vix3m is not None:
        vix_r = vix.reindex(idx)
        vix3m_r = vix3m.reindex(idx)
        df["vix_term_slope"] = ((vix3m_r - vix_r) / vix_r.clip(lower=1.0)).astype("float32")

    # Cross-sectional dispersion placeholder (requires per-ticker returns, set to 0)
    df["xsect_dispersion"] = np.float32(0.0)

    df = df.dropna(subset=["vix_level"])
    df.index.name = "date"
    return df


# Universe sample size for the regression preflight. Matches postflight's
# _UNIVERSE_SAMPLE_SIZE so the same set of tickers gates both ends of the
# pipeline. 20 tickers catches any systematic regression with ~certainty
# (a one-day clobber across the whole universe would land in 100% of
# samples) while keeping the preflight ArcticDB-read budget tiny.
_REGRESSION_PREFLIGHT_SAMPLE_SIZE = 20


def _planned_last_date(series_or_df) -> "pd.Timestamp | None":
    """Last index date of a Series or DataFrame, normalized to midnight UTC."""
    if series_or_df is None:
        return None
    idx = series_or_df.index
    if idx is None or len(idx) == 0:
        return None
    last = pd.Timestamp(idx[-1])
    if last.tzinfo is not None:
        last = last.tz_convert("UTC").tz_localize(None)
    return last.normalize()


def _existing_last_date(lib, symbol: str) -> "pd.Timestamp | None":
    """Last existing date in ArcticDB for ``symbol``, or None if not present."""
    try:
        df = lib.tail(symbol, n=1).data
    except Exception:
        return None
    if df is None or df.empty:
        return None
    last = pd.Timestamp(df.index[-1])
    if last.tzinfo is not None:
        last = last.tz_convert("UTC").tz_localize(None)
    return last.normalize()


# History-truncation tolerance for the macro/sector regression preflight.
# A legitimate source restatement can drop a handful of rows (a vendor
# correcting bad prints, a holiday reclassification). Losing more than this
# many rows, or moving the series' FIRST date forward at all, is a producer
# defect — see alpha-engine-config-I9256: ``VIX3M`` went 2509 rows ->
# 16 rows across two consecutive Saturday backfills (2026-08-22 10:42 UTC
# and 2026-08-29 10:48 UTC) because a 1-row ``reference/price_cache/
# VIX3M.parquet`` was written wholesale over a 10y ArcticDB series. The
# pre-existing preflight compared LAST DATE only, so a series that keeps
# today's date while losing 2493 rows of history passed clean.
#
# This row-floor-against-the-reference-series guard is also the producer-side
# deliverable of alpha-engine-config-I9324: four champion predictor vintages
# (2026-08-12/21/28) trained with all seven macro coefficients at exactly
# zero because ``macro/VIX3M`` lost coverage intermittently between two
# otherwise-healthy Saturday runs (08-07 healthy, 08-12 dead, 08-14 healthy).
# The intermittency traces to yfinance answering ``^VIX3M`` with 1 row on the
# EC2 host that runs the weekly collector some weeks and a full 2484-row
# answer other weeks (alpha-engine-config-I9286 routes the four caret index
# tickers through FRED instead). This guard is the backstop regardless of
# which upstream call answers short on a given run.
_MACRO_HISTORY_SHRINK_TOLERANCE_ROWS = 5

# Rolling-window slide allowance for the first-date / row-count checks above.
# The price cache is BY DESIGN a rolling 10-year, fully re-adjusted window
# (``collectors/prices.py``: "full replace, not append" because yfinance
# ``auto_adjust`` retroactively re-adjusts the whole series), and the FRED
# history window is a trailing N years the same way. So a macro series' FIRST
# date advances at every source refresh — ~5 NYSE sessions per weekly rebuild,
# more after a missed Saturday or a freeze that released (``macro/HYOAS``
# advanced ~85 sessions on 2026-09-05 once alpha-engine-config-I9287 unstuck
# it). Between rebuilds ``daily_append`` extends the ArcticDB series at the
# tail, so the rows a rebuild "loses" against ArcticDB equal exactly the
# sessions the window's start slid. That is a slide, not truncation.
#
# Measured origin: the first live Saturday after the I9256 guard landed
# (2026-09-05 10:48 UTC, weekly SF execution 49a1f4e0-…) it refused all 17
# macro/sector symbols on ``planned_first > existing_first`` by ONE WEEK
# (``macro.SPY: planned_first=2016-09-06 > existing_first=2016-08-29``) and
# DataPhase1 failed the pipeline.
#
# Truncation (the I9256 class — VIX3M 2509 -> 16 rows) is a start that jumps
# forward by more than any rebuild cadence can explain, or a row loss the
# slide does not account for. Above WARN sessions the slide is logged as a
# finding (a freeze released, or rebuilds were missed — history really was
# lost); above MAX it is refused outright.
_MACRO_WINDOW_SLIDE_WARN_TRADING_DAYS = 30
_MACRO_WINDOW_SLIDE_MAX_TRADING_DAYS = 252

# Max-date staleness tolerance for the macro write boundary, in NYSE trading
# days behind the reference date (alpha-engine-config-I9287). A shrink/
# first-date check is blind to a series that keeps writing NEW versions
# without the PAYLOAD ever advancing: ``macro/HYOAS`` recorded continuous
# weekly + daily ArcticDB versions right through 2026-08-29 while every
# version's last row stayed 2026-05-07 — 3.5 months stale, 786 rows, never
# shrinking, so the row-count and first-date guards both passed it clean.
# 10 trading days (~2 calendar weeks) tolerates a FRED publication lag
# without flagging routine latency; HYOAS was ~75 trading days stale.
_MACRO_STALENESS_TOLERANCE_TRADING_DAYS = 10

# Row-count floor (as a fraction of the largest established macro sibling)
# below which a symbol's FIRST write is logged as a finding rather than
# silently treated as a healthy new listing (alpha-engine-config-I9289: all
# eight sub-sector benchmark ETFs were born 2026-07-23 at 26 rows next to
# ~2514-row siblings, and nothing surfaced it for a month). This never
# blocks the write — a genuinely short-lived listing (a recent IPO ETF) is
# legitimate — it only makes the shortfall VISIBLE at birth via a loud log
# line a health sweep can grep for, rather than a month later.
_MACRO_FIRST_WRITE_SIBLING_FRACTION = 0.5

# ── Cumulative-retention registry (alpha-engine-config-I10054, option (a)) ────
#
# Every macro source is a ROLLING window: ``collectors/prices.py`` refreshes
# the price cache as a 10-year yfinance ``auto_adjust=True`` FULL REPLACE, and
# ``collectors/fred_history.py`` fetches a trailing ``period_years`` window the
# same way. ``builders/backfill.py`` then writes each ArcticDB symbol wholesale
# (``lib.write``), so the head version loses its oldest week every Saturday.
# ArcticDB keeps prior versions, so nothing is destroyed — but no reader sees
# anything older than the window, which silently contradicts the standing
# "retain all archives, data is an asset" preference for exactly the series a
# regime model trains on.
#
# The reason this cannot simply be fixed for every symbol is the ADJUSTMENT
# BASIS. ``auto_adjust=True`` retroactively re-adjusts an ETF/equity series'
# whole history on every split and dividend, so rows written last year are on a
# different scale from rows written today; splicing them would create a silent
# seam mid-series. That is why ``collectors/prices.py`` full-replaces rather
# than appends in the first place, and it is the contract data#1298 established.
#
# FRED-sourced INDICES carry no split/dividend adjustment at all — a VIX close
# from 2016 means today exactly what it meant in 2016 — so for those, and ONLY
# those, prepending the rows we already hold is EXACT and seamless. This set is
# therefore the declared cumulative-retention registry: a symbol is cumulative
# because it is DECLARED here, never because of anything inferable from its
# name at write time.
#
# It is asserted equal to ``collectors/fred_history.FRED_HISTORY_MAP``'s keys
# by ``tests/test_macro_cumulative_history.py`` — the single declared FRED
# source map since alpha-engine-config-I9286 — so adding a FRED series without
# ruling on its retention fails CI rather than silently landing on the rolling
# default. Do NOT derive this set from that map at import time: a derived set
# can never fail that test, and the ruling that FRED == un-adjusted is a
# JUDGEMENT about the data, not a fact about the map.
#
# Ruling: alpha-engine-config-I10054 option (a). Option (b) — cumulative for
# everything, re-stating old rows onto the current basis from the corporate-
# actions registry — is correct in principle and is the Crucible v2 data-plane
# shape (raw series + a factor table, adjustment applied at READ time), but it
# needs dividend factors the registry does not currently carry. Adjusted
# ETF/equity series stay rolling until v2 owns the data plane.
CUMULATIVE_MACRO_SYMBOLS: frozenset[str] = frozenset({
    "VIX",     # VIXCLS      — CBOE volatility index level
    "VIX3M",   # VXVCLS      — CBOE 3-month volatility index level
    "TNX",     # DGS10       — 10y Treasury constant-maturity yield, percent
    "IRX",     # DTB3        — 3-month T-bill rate, percent
    "TWO",     # DGS2        — 2y Treasury constant-maturity yield, percent
    "HYOAS",   # BAMLH0A0HYM2 — ICE BofA US HY index OAS, percent
    "BAA10Y",  # BAA10Y      — Moody's BAA yield less 10y Treasury, percent
})


def _series_row_span(series_or_df) -> "tuple[int, pd.Timestamp | None]":
    """``(n_rows, first_date)`` for a planned Series/DataFrame."""
    if series_or_df is None:
        return 0, None
    idx = series_or_df.index
    if idx is None or len(idx) == 0:
        return 0, None
    first = pd.Timestamp(idx[0])
    if first.tzinfo is not None:
        first = first.tz_convert("UTC").tz_localize(None)
    return len(idx), first.normalize()


def _read_existing_macro(lib, symbol: str):
    """The existing ArcticDB frame for ``symbol``, or None when it has none.

    Returns None when the symbol does not exist yet, or exists but is empty —
    a first write is never a regression. Any OTHER read failure RAISES:
    silently treating an unreadable symbol as absent is exactly how a
    truncating write gets waved through (fail-loud rule, AGENTS.md "no silent
    degrade on a producer").
    """
    try:
        df = lib.read(symbol).data
    except Exception as exc:
        if _symbol_absent(lib, symbol):
            return None
        raise RuntimeError(
            f"Macro history guard: could not read existing ArcticDB symbol "
            f"{symbol!r} to check for truncation: {exc}"
        ) from exc
    if df is None or df.empty:
        return None
    return df


def _existing_row_span(lib, symbol: str) -> "tuple[int, pd.Timestamp | None]":
    """``(n_rows, first_date)`` for an existing ArcticDB symbol.

    Returns ``(0, None)`` when the symbol does not exist yet — see
    ``_read_existing_macro`` for the fail-loud read contract.
    """
    return _existing_span_of(_read_existing_macro(lib, symbol))


def _existing_span_of(df) -> "tuple[int, pd.Timestamp | None]":
    """``(n_rows, first_date)`` for an already-read existing frame."""
    if df is None or len(df) == 0:
        return 0, None
    first = pd.Timestamp(df.index[0])
    if first.tzinfo is not None:
        first = first.tz_convert("UTC").tz_localize(None)
    return len(df), first.normalize()


def _symbol_absent(lib, symbol: str) -> bool:
    """True when ``symbol`` genuinely does not exist in ``lib``."""
    try:
        return not lib.has_symbol(symbol)
    except Exception:
        try:
            return symbol not in set(lib.list_symbols())
        except Exception:
            return False


def _history_regression(
    label: str,
    planned,
    existing_rows: int,
    existing_first: "pd.Timestamp | None",
    cumulative: bool = False,
) -> "str | None":
    """Return a human-readable regression string, or None when the write is safe.

    A rolling-window source legitimately moves the series start forward at
    every refresh (see ``_MACRO_WINDOW_SLIDE_*``). The slide, in NYSE
    sessions, is credited against the row-count floor — a window that lost
    N rows at the front because it advanced N sessions has not shrunk — and
    is itself bounded: a start that jumps more than the MAX allowance is a
    coverage change, not a slide, and is refused.

    This judges the PLANNED frame — what the SOURCE produced — and it does so
    identically for cumulative and rolling symbols. ``cumulative`` only
    corrects the *wording* of the slide finding: for a declared-cumulative
    symbol (``CUMULATIVE_MACRO_SYMBOLS``) the rows the window slid past are
    restored by ``_cumulative_prepend`` before the write, so they are NOT
    gone from ArcticDB's head version. Keeping the detector itself blind to
    that distinction is deliberate: the prepend closes the LOSS, and a source
    that answers 16 rows where it answered 2509 last week is still a producer
    defect that must page (alpha-engine-config-I9256), not a defect the repair
    is allowed to hide.
    """
    planned_rows, planned_first = _series_row_span(planned)
    if existing_rows == 0:
        return None
    slide = 0
    if (
        planned_first is not None
        and existing_first is not None
        and planned_first > existing_first
    ):
        from nousergon_lib.dates import trading_days_stale

        try:
            from krepis.trading_calendar import (
                TradingCalendarRangeError as _CalendarRangeError,
            )
        except ImportError:
            # Pinned krepis predates the I10127 coverage guard entirely (it
            # never raises this) — nothing to catch, and the except below is
            # unreachable, matching today's (pre-fix) behaviour exactly.
            _CalendarRangeError = ()

        try:
            slide = trading_days_stale(existing_first.date(), planned_first.date())
        except _CalendarRangeError as exc:
            # alpha-engine-config-I10267: krepis>=0.59.52 (I10127) correctly
            # refuses a trading-day-precise answer for any date before
            # NYSE_CALENDAR_COVERS_FROM (2016-01-01) — the holiday table has
            # no data at all that far back. A declared-CUMULATIVE FRED symbol
            # (CUMULATIVE_MACRO_SYMBOLS) can legitimately span dates that old
            # (BAA10Y/TWO/VIX etc. go back to the 1980s), so this is not an
            # error condition, just a range the live calendar cannot resolve.
            # Catching the specific krepis range-error class (not bare
            # Exception) keeps an unrelated bug in `trading_days_stale` fail
            # loud as before. Fall back to a business-day count — an
            # always-available, calendar-independent, slightly conservative
            # (holiday-blind, so marginally over-generous) estimate — logged
            # loudly so the approximation is greppable. This does not weaken
            # the integrity check for the case that can actually reach it: a
            # declared-cumulative symbol's WRITTEN result is independently
            # verified by `_cumulative_invariant_violation` after
            # `_cumulative_prepend`, using plain date/row comparisons with no
            # calendar dependency, so a wrong slide credit here cannot mask a
            # real loss. A non-cumulative (rolling yfinance) symbol never
            # reaches this branch in practice — its window is always within
            # the last ~10 years.
            slide = int(np.busday_count(existing_first.date(), planned_first.date()))
            log.info(
                "MACRO_SLIDE_PRECALENDAR_FALLBACK %s existing_first=%s "
                "planned_first=%s busday_slide=%d reason=%s — see "
                "alpha-engine-config-I10267.",
                label, existing_first.date(), planned_first.date(), slide, exc,
            )
    allowed_loss = _MACRO_HISTORY_SHRINK_TOLERANCE_ROWS + min(
        slide, _MACRO_WINDOW_SLIDE_MAX_TRADING_DAYS
    )
    if planned_rows < existing_rows - allowed_loss:
        return (
            f"{label}: planned_rows={planned_rows} < existing_rows={existing_rows} "
            f"(tolerance {_MACRO_HISTORY_SHRINK_TOLERANCE_ROWS} + window slide "
            f"{min(slide, _MACRO_WINDOW_SLIDE_MAX_TRADING_DAYS)} sessions)"
        )
    if slide > _MACRO_WINDOW_SLIDE_MAX_TRADING_DAYS:
        return (
            f"{label}: planned_first={planned_first.date()} > "
            f"existing_first={existing_first.date()} by {slide} trading days "
            f"(history start moved forward past the "
            f"{_MACRO_WINDOW_SLIDE_MAX_TRADING_DAYS}-session rolling-window "
            f"allowance — a coverage change, not a slide)"
        )
    if slide > _MACRO_WINDOW_SLIDE_WARN_TRADING_DAYS:
        log.warning(
            "MACRO_WINDOW_SLIDE %s: history start advanced %d trading days "
            "(%s -> %s) in one rebuild — more than a weekly cadence explains "
            "(a frozen source released, or rebuilds were missed); %d rows of "
            "history before %s %s.",
            label, slide, existing_first.date(), planned_first.date(),
            max(existing_rows - planned_rows, 0), planned_first.date(),
            (
                "are retained by the cumulative prepend "
                "(alpha-engine-config-I10054)"
                if cumulative
                else "are gone from ArcticDB's head version"
            ),
        )
    return None


def _staleness_regression(
    label: str,
    planned,
    reference_date: "str | pd.Timestamp | None",
) -> "str | None":
    """Return a staleness string when ``planned``'s last row lags ``reference_date``
    by more than ``_MACRO_STALENESS_TOLERANCE_TRADING_DAYS`` NYSE trading days.

    alpha-engine-config-I9287: catches a rewrite-with-stale-source that the
    shrink/first-date checks in ``_history_regression`` cannot — a series
    whose row count and start date never move because its writer keeps
    re-deriving the same frozen upstream value. ``reference_date=None``
    (no run_date available to the caller) skips the check rather than
    guessing "today", since a caller with no reference has no way to know
    what "stale" means here.
    """
    if reference_date is None:
        return None
    last = _planned_last_date(planned)
    if last is None:
        return None
    from nousergon_lib.dates import trading_days_stale

    ref = pd.Timestamp(reference_date).date()
    lag = trading_days_stale(last.date(), ref)
    if lag > _MACRO_STALENESS_TOLERANCE_TRADING_DAYS:
        return (
            f"{label}: last_date={last.date()} is {lag} trading days behind "
            f"reference={ref} (tolerance {_MACRO_STALENESS_TOLERANCE_TRADING_DAYS}) "
            "— writer is rewriting a FROZEN upstream value, not advancing it"
        )
    return None


def _warn_if_short_first_write(lib, symbol: str, df: "pd.DataFrame") -> None:
    """Log loudly when a symbol's FIRST write is short next to established siblings.

    alpha-engine-config-I9289: a first write is never REFUSED (a genuinely
    short-lived listing is legitimate), but it must be VISIBLE rather than
    silently indistinguishable from a healthy new symbol — the sub-sector
    ETFs sat at 26 rows for a month before anyone noticed. Best-effort: any
    failure reading a reference sibling only skips the check, it never blocks
    the write this function does not perform.
    """
    try:
        reference_rows, _ = _existing_row_span(lib, "SPY")
    except Exception:
        return
    if reference_rows <= 0:
        return
    planned_rows, _ = _series_row_span(df)
    if planned_rows < reference_rows * _MACRO_FIRST_WRITE_SIBLING_FRACTION:
        log.warning(
            "MACRO_NEW_SYMBOL_SHORT_HISTORY macro.%s: first write carries %d rows "
            "against SPY's %d (%.0f%% of the %.0f%%-of-siblings floor) — a new "
            "macro symbol at birth, or a never-backfilled one. See "
            "alpha-engine-config-I9289.",
            symbol, planned_rows, reference_rows,
            100 * planned_rows / reference_rows,
            100 * _MACRO_FIRST_WRITE_SIBLING_FRACTION,
        )


def _naive_normalized_index(idx) -> "pd.DatetimeIndex":
    """``idx`` as a tz-naive, midnight-normalized DatetimeIndex."""
    out = pd.DatetimeIndex(idx)
    if out.tz is not None:
        out = out.tz_convert("UTC").tz_localize(None)
    return out.normalize()


def _cumulative_prepend(symbol: str, existing, planned):
    """Prepend the existing rows strictly older than ``planned``'s first date.

    alpha-engine-config-I10054 option (a). For a symbol declared in
    ``CUMULATIVE_MACRO_SYMBOLS`` — un-adjusted, FRED-sourced indices, where
    splicing is EXACT because no split/dividend re-adjustment ever moves an
    old row's scale — the wholesale ``lib.write`` becomes the union
    ``(existing rows older than planned_first) ∪ planned`` instead of the
    rolling source window alone. Overlapping dates always take the PLANNED
    (fresh) value: only strictly-older rows are taken from ArcticDB, and the
    de-duplication below keeps the last occurrence as a second line of
    defence. The result is idempotent — a second run over the same source
    finds the same union already at the head and reproduces it exactly.

    Everything not declared cumulative (SPY, GLD, USO, XL*, the sub-sector
    ETFs) is returned untouched and stays rolling: ``auto_adjust=True``
    retroactively re-adjusts those series, so old rows are on a different
    scale and splicing them would create a silent mid-series seam.

    Returns ``(frame_to_write, n_prepended, first_prepended_date)``.
    """
    if symbol not in CUMULATIVE_MACRO_SYMBOLS or existing is None or len(existing) == 0:
        return planned, 0, None

    _, planned_first = _series_row_span(planned)
    if planned_first is None:
        return planned, 0, None

    existing_idx = _naive_normalized_index(existing.index)
    older_mask = existing_idx < planned_first
    if not bool(older_mask.any()):
        return planned, 0, None

    older = existing.loc[older_mask]
    older = older.set_axis(existing_idx[older_mask], axis=0)

    # Shape alignment. A column set that no longer matches means the macro
    # write schema changed under an existing symbol; splicing across that
    # would fabricate NaN columns, so the prepend is SKIPPED and said so
    # loudly. Skipping is safe rather than a silent degrade because the
    # cumulative invariant asserted by the caller then refuses the write
    # outright — the loss cannot pass unnoticed either way.
    if isinstance(planned, pd.DataFrame):
        if not isinstance(older, pd.DataFrame) or set(older.columns) != set(planned.columns):
            log.warning(
                "MACRO_HISTORY_PREPEND_SKIPPED symbol=%s reason=column_mismatch "
                "existing=%s planned=%s — see alpha-engine-config-I10054.",
                symbol,
                sorted(getattr(older, "columns", [])),
                sorted(planned.columns),
            )
            return planned, 0, None
        older = older[list(planned.columns)]
    else:
        # A planned Series (the preflight's shape). Take the matching column
        # from the stored frame, or the frame's single column.
        if isinstance(older, pd.DataFrame):
            candidates = [c for c in (planned.name, "Close") if c in older.columns]
            if candidates:
                older = older[candidates[0]]
            elif older.shape[1] == 1:
                older = older.iloc[:, 0]
            else:
                log.warning(
                    "MACRO_HISTORY_PREPEND_SKIPPED symbol=%s reason=ambiguous_column "
                    "existing=%s — see alpha-engine-config-I10054.",
                    symbol, sorted(older.columns),
                )
                return planned, 0, None
        older = older.rename(planned.name)

    planned_normalized = planned.set_axis(_naive_normalized_index(planned.index), axis=0)
    merged = pd.concat([older, planned_normalized])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    merged.index.name = planned.index.name or "date"

    n_prepended = len(older)
    first_prepended = pd.Timestamp(older.index[0]).date()
    log.info(
        "MACRO_HISTORY_PREPENDED symbol=%s rows=%d first=%s planned_first=%s total=%s",
        symbol, n_prepended, first_prepended, planned_first.date(), len(merged),
    )
    return merged, n_prepended, first_prepended


def _cumulative_invariant_violation(
    symbol: str,
    to_write,
    existing_rows: int,
    existing_first: "pd.Timestamp | None",
) -> "str | None":
    """Refuse a declared-cumulative symbol whose WRITTEN series still slides.

    The whole point of ``CUMULATIVE_MACRO_SYMBOLS`` is that the head version
    of these symbols never loses a row it once held, so any forward movement
    of the written first date — or any net row loss beyond the restatement
    tolerance — means the prepend did not do its job (a skipped prepend, an
    unreadable existing frame, a future call site that bypassed it). That is
    a refusal, not a warning: publishing a truncated head under a cumulative
    contract is exactly the silent loss this closes.
    """
    if symbol not in CUMULATIVE_MACRO_SYMBOLS or existing_rows == 0:
        return None
    written_rows, written_first = _series_row_span(to_write)
    if (
        written_first is not None
        and existing_first is not None
        and written_first > existing_first
    ):
        return (
            f"macro.{symbol}: written_first={written_first.date()} > "
            f"existing_first={existing_first.date()} on a symbol declared "
            f"CUMULATIVE — a cumulative series never slides"
        )
    if written_rows < existing_rows - _MACRO_HISTORY_SHRINK_TOLERANCE_ROWS:
        return (
            f"macro.{symbol}: written_rows={written_rows} < "
            f"existing_rows={existing_rows} on a symbol declared CUMULATIVE "
            f"(tolerance {_MACRO_HISTORY_SHRINK_TOLERANCE_ROWS})"
        )
    return None


def _write_macro_series_no_shrink(
    lib, symbol: str, df: "pd.DataFrame", reference_date: "str | None" = None,
) -> None:
    """Write a full macro series, REFUSING any write that would truncate OR
    silently re-freeze it.

    The write boundary is the last line of defence: ``_assert_no_arctic_regression``
    runs once per backfill over the planned frames, but the per-ticker
    ``--rebuild-macro`` override and any future call site reach the library
    directly. A wholesale ``lib.write()`` is destructive — ArcticDB keeps prior
    versions, but every reader sees the truncated (or stale) head immediately
    and the predictor's ``dropna()`` turns one short optional column into a
    total loss of the regime frame (alpha-engine-config-I9255 / I9256 / I9287).

    Raises RuntimeError rather than degrading: a producer that has lost its
    own history, or is rewriting a frozen upstream value, must page, not
    publish. ``reference_date=None`` skips the staleness half of the check
    (no run_date known to the caller) but still enforces no-shrink.
    """
    existing = _read_existing_macro(lib, symbol)
    existing_rows, existing_first = _existing_span_of(existing)
    cumulative = symbol in CUMULATIVE_MACRO_SYMBOLS
    regression = _history_regression(
        f"macro.{symbol}", df, existing_rows, existing_first, cumulative=cumulative,
    )
    if regression is not None:
        raise RuntimeError(
            "Macro write refused — it would TRUNCATE an existing series. "
            f"{regression}. Source is the price_cache parquet + daily_closes delta; "
            "check reference/price_cache/"
            f"{symbol}.parquet row count before re-running. "
            "See alpha-engine-config-I9256."
        )
    # A first write (existing_rows == 0) is never a staleness regression —
    # there is nothing established to lag behind, matching
    # ``_history_regression``'s own first-write carve-out.
    staleness = (
        _staleness_regression(f"macro.{symbol}", df, reference_date)
        if existing_rows > 0 else None
    )
    if staleness is not None:
        raise RuntimeError(
            "Macro write refused — it is a rewrite of a FROZEN source, not an "
            f"advance. {staleness}. Check reference/price_cache/{symbol}.parquet's "
            "LastModified and the collector that owns it "
            "(collectors/fred_history.py for a FRED-only symbol). "
            "See alpha-engine-config-I9287."
        )
    if existing_rows == 0:
        _warn_if_short_first_write(lib, symbol, df)
    # alpha-engine-config-I10054 option (a): un-adjusted (FRED-sourced) series
    # are cumulative — union the rows we already hold that predate the source
    # window with the fresh window, so the head version never loses history.
    # Adjusted ETF/equity series fall through untouched and stay rolling.
    to_write, _, _ = _cumulative_prepend(symbol, existing, df)
    violation = _cumulative_invariant_violation(
        symbol, to_write, existing_rows, existing_first,
    )
    if violation is not None:
        raise RuntimeError(
            "Macro write refused — it would truncate a series declared "
            f"CUMULATIVE. {violation}. The rows should have been restored by "
            "``_cumulative_prepend``; check the MACRO_HISTORY_PREPEND_SKIPPED "
            "log line for why they were not. See alpha-engine-config-I10054."
        )
    lib.write(symbol, to_write)


def _assert_no_arctic_regression(
    bucket: str,
    planned_macro: dict[str, "pd.Series"],
    planned_universe: dict[str, "pd.DataFrame"],
    run_date: str,
    sample_size: int = _REGRESSION_PREFLIGHT_SAMPLE_SIZE,
) -> None:
    """Refuse to run backfill if its planned data is older than what ArcticDB has.

    Backfill rewrites every macro/sector and (sampled) universe symbol with
    full-series ``lib.write()`` calls, so any regression at the source
    instantly knocks every downstream consumer stale. Postflight catches
    the symptom afterwards but by then the damage is done — this preflight
    fails BEFORE any feature compute or write so the operator gets a clean
    actionable error and ArcticDB stays at its current freshness.

    Origin: 2026-05-02 weekly SF. MorningEnrich appended Friday's polygon
    fill to ArcticDB; price cache passed the mtime "current" check (cache
    parquets refreshed 4/30) so neither prices nor slim_cache rewrote the
    cache; backfill loaded that 4/30-ending cache, computed features over
    it, and ``lib.write()`` regressed every macro/sector/universe symbol
    from 5/1 → 4/30. Postflight rejected. Pipeline halted at DataPhase1.

    The check is sampled on the universe side (matching
    ``validators/postflight._UNIVERSE_SAMPLE_SIZE``) because exhaustive
    ``tail()`` over 900 symbols would dominate backfill runtime on every
    Saturday. Sample seed is the run_date so reruns hit the same tickers.

    ``_UNIVERSE_EXTRA`` members (currently: SPY) are HARD-PINNED benchmark
    symbols, never churn-eligible — they are excepted from the
    ``_SKIP_TICKERS`` exclusion here via the same ``(... not in
    _SKIP_TICKERS or ... in _UNIVERSE_EXTRA)`` carve-out the write-path
    predicate and the ``daily_append.py`` scoping predicates use, so SPY
    stays eligible for the regression-preflight sample pool (config-I2704,
    the narrower sibling of config-I2703's daily_append.py fix — this site
    is a sampled preflight gap, not a masked freshness-scan blind spot,
    since the macro-side loop above already checks SPY's ``macro.SPY``
    Close-only row unconditionally).
    """
    import random as _rand

    macro_lib = get_macro_lib(bucket)
    universe_lib = get_universe_lib(bucket)

    regressions: list[str] = []

    for key, series in planned_macro.items():
        planned_last = _planned_last_date(series)
        existing_last = _existing_last_date(macro_lib, key)
        if planned_last is not None and existing_last is not None and planned_last < existing_last:
            regressions.append(
                f"macro.{key}: planned={planned_last.date()} < existing={existing_last.date()}"
            )
        # HISTORY-length regression (alpha-engine-config-I9256). The last-date
        # check above is blind to a series that still ends today but has lost
        # every row before last month — which is exactly how VIX3M went from
        # 2509 rows to 16 while passing this preflight clean twice.
        existing_rows, existing_first = _existing_row_span(macro_lib, key)
        hist = _history_regression(
            f"macro.{key}", series, existing_rows, existing_first,
            cumulative=key in CUMULATIVE_MACRO_SYMBOLS,
        )
        if hist is not None:
            regressions.append(hist)
        # STALENESS regression (alpha-engine-config-I9287). Neither check
        # above catches a writer that keeps re-deriving the same frozen
        # upstream value every run — row count and first date never move,
        # only the ArcticDB *version* advances. Catch it here too so a
        # ticker_filter/--rebuild-macro run that bypasses the per-write
        # boundary still can't leave a symbol silently stuck.
        stale = (
            _staleness_regression(f"macro.{key}", series, run_date)
            if existing_rows > 0 else None
        )
        if stale is not None:
            regressions.append(stale)

    try:
        arctic_syms = set(universe_lib.list_symbols())
    except Exception as exc:
        raise RuntimeError(
            f"Backfill regression preflight: could not list ArcticDB universe symbols: {exc}"
        ) from exc

    candidates = sorted(
        t for t in planned_universe
        if t in arctic_syms
        and admits_universe_write(t)
    )
    if len(candidates) > sample_size:
        rng = _rand.Random(run_date)
        sample = rng.sample(candidates, sample_size)
    else:
        sample = candidates

    for ticker in sample:
        planned_last = _planned_last_date(planned_universe.get(ticker))
        existing_last = _existing_last_date(universe_lib, ticker)
        if planned_last is None or existing_last is None:
            continue
        if planned_last < existing_last:
            regressions.append(
                f"universe.{ticker}: planned={planned_last.date()} < existing={existing_last.date()}"
            )

    if regressions:
        raise RuntimeError(
            f"Backfill regression preflight failed: {len(regressions)} symbols would regress "
            f"if backfill proceeded (each entry names its own check: last-date regression, "
            f"history-length truncation, or frozen-source staleness). For a LAST-DATE "
            f"regression the source (predictor/price_cache + daily_closes delta) "
            f"ends earlier than what ArcticDB already has. Most common cause: the price cache "
            f"mtime 'current' check skipped the weekly refresh, so the cache lags "
            f"MorningEnrich/daily_append writes — and ``_apply_daily_delta`` failed to bridge "
            f"the gap (e.g. its ``slim_last_date`` was poisoned by a single freshly-refreshed "
            f"ticker, leaving ``bdate_range`` empty on a Saturday). To recover: redrive the "
            f"failed SF execution after confirming ``features/compute.py::_apply_daily_delta`` "
            f"uses ``min(valid_dates)`` so per-ticker mtime variation can't suppress delta "
            f"loading. Regressions detected (showing first 10 of {len(regressions)}): {regressions[:10]}"
        )

    log.info(
        "Backfill regression preflight: OK — %d macro/sector + %d sampled universe symbols "
        "all >= existing ArcticDB last_date.",
        len(planned_macro), len(sample),
    )


def backfill(
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
    ticker_filter: str | None = None,
    validate: bool = False,
    rebuild_macro: bool = False,
    run_date: str | None = None,
) -> dict:
    """
    Run the full historical backfill: load 10y prices, compute features, write to ArcticDB.

    Args:
        bucket: S3 bucket name
        dry_run: compute but skip ArcticDB writes
        ticker_filter: if set, only process this single ticker (for testing)
        validate: if True, run spot-check validation after backfill
        rebuild_macro: when ticker_filter is set, also rewrite the macro
            library from parquet (opt-in override — defaults to False so
            per-ticker patches don't regress macro freshness)
        run_date: the Phase 1 run date (YYYY-MM-DD). When set, the
            constituents filter reads ``market_data/weekly/{run_date}/
            constituents.json`` directly instead of following the
            ``latest_weekly.json`` pointer — required because the pointer
            isn't advanced until ``_write_manifest`` at end-of-Phase-1,
            so a backfill that follows the pointer mid-Phase-1 sees last
            week's constituents and excludes this week's new entrants.

    Returns:
        Summary dict with counts and timing.
    """
    s3 = boto3.client("s3")
    t0 = time.time()

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Load data ─────────────────────────────────────────────────────────
    if ticker_filter:
        log.info(
            "Loading 10-year price cache for %s plus its %d macro/ETF context series...",
            ticker_filter, len(_PER_TICKER_CACHE_CONTEXT),
        )
        price_data = _load_full_cache(
            s3, bucket, only=_PER_TICKER_CACHE_CONTEXT | {ticker_filter},
        )
    else:
        log.info("Loading full 10-year price cache...")
        price_data = _load_full_cache(s3, bucket)
    if not price_data:
        return {"status": "error", "error": "no_price_data"}

    # Apply daily_closes delta on top of the 10y cache so the backfill source
    # captures rows written between the last cache refresh and today (e.g.
    # MorningEnrich's polygon-T+1 fill, weekday EOD CaptureSnapshot). Without
    # this, a price cache that's "current" by S3 mtime can still source data
    # older than what daily_append already pushed into ArcticDB, and the
    # full-series ``lib.write()`` calls below regress every symbol. Mirrors
    # ``features/compute.py::_apply_daily_delta`` so both feature-snapshot and
    # backfill share the same freshness semantics.
    if not dry_run:
        # Shared registry: the same instance drives detection/restatement in
        # ``_apply_daily_delta`` AND the post-condition audit below, so the
        # audit sees this run's just-detected actions.
        registry = _build_registry(s3, bucket)
        price_data, split_tickers = _apply_daily_delta(
            s3, bucket, today_str, price_data, registry=registry,
        )
        if split_tickers:
            # data#1298: these tickers had a registered split detected and their
            # FULL history restated by the polygon-authoritative factor before
            # the full-series ``lib.write`` below, so ArcticDB is rewritten on
            # one continuous adjusted scale (train == serve), not windowed.
            log.info(
                "Split restatement applied to %d ticker(s) before ArcticDB "
                "rewrite (data#1298): %s",
                len(split_tickers), sorted(split_tickers),
            )

        # BLOCKING, registry-aware split-jump audit (PR3 §3, config#1433): the
        # post-condition on the materialized series, evaluated BEFORE any
        # ``lib.write``. A residual jump that a registered action EXPLAINS is a
        # MISSED restatement of a KNOWN action (data#1298 corruption) → RAISE so
        # the discontinuity cannot land in ArcticDB. A residual with NO
        # registered action is SUSPECTED (legit large move / polygon-missed) →
        # WARN and proceed (a real ±33% move must not halt training).
        audit = audit_action_jumps(price_data, registry)
        if audit.suspected:
            log.warning(
                "Split-jump audit: %d ticker(s) carry a SUSPECTED large move "
                "with NO registered action (legitimate move or polygon-missed) "
                "— proceeding, not blocking: %s",
                len(audit.suspected),
                {t: audit.suspected[t] for t in sorted(audit.suspected)[:20]},
            )
        if audit.missed:
            raise CorporateActionAuditError(
                f"Split-jump audit: {len(audit.missed)} ticker(s) STILL carry "
                f"an un-flattened KNOWN registered split (data#1298) after "
                f"delta+restate — refusing to write the discontinuity to "
                f"ArcticDB: "
                f"{ {t: audit.missed[t] for t in sorted(audit.missed)[:20]} }"
            )

    macro = _extract_macro_series(price_data)
    sector_map = _load_sector_map(s3, bucket)

    fundamentals = _load_cached_fundamentals(s3, bucket, today_str)
    alt_data = _load_cached_alternative(s3, bucket)

    # Defense-in-depth: refuse to write if planned data is older than what
    # ArcticDB has. Skipped on per-ticker invocations (those route through
    # ``skip_macro`` and don't touch the universe sample). Cheap (a handful
    # of tail() reads) so it runs before the multi-minute feature compute.
    if not dry_run and ticker_filter is None:
        _assert_no_arctic_regression(bucket, macro, price_data, today_str)

    t_load = time.time() - t0
    log.info(
        "Data loaded in %.1fs: %d tickers, %d macro series, %d sector mappings",
        t_load, len(price_data), len(macro), len(sector_map),
    )

    # ── 2. Filter to stock tickers ───────────────────────────────────────────
    # Two-tier filter:
    #   universe_tickers: every non-skip stock ticker with data — gets written
    #     to ArcticDB universe as raw OHLCV. Lets Research scan fresh listings
    #     (e.g. recent S&P 500/400 additions with <1y of history) which only
    #     need OHLCV columns, not engineered features.
    #   tickers_with_features: subset with enough history for feature
    #     computation (rolling 252-day vol/momentum etc.). Only these get
    #     OHLCV + feature columns written; short-history tickers get OHLCV
    #     only, and are skipped by feature-consuming predictors downstream.
    #
    # Constituents filter: drop any price_cache ticker that isn't in the
    # current S&P 500 / 400 constituents. Without this, backfill recreates
    # ArcticDB rows for tickers that were just pruned by
    # ``builders.prune_delisted_tickers`` because their parquet files still
    # exist in ``predictor/price_cache/`` (price_cache parquets are kept for
    # historical lookup; arctic represents the active investable universe).
    # The 2026-05-02 SF redrive #6 caught this: pre-MorningEnrich prune
    # dropped 8 stragglers, then Phase 1 step 8 (this function) recreated
    # them, then Backtester's universe-freshness preflight halted on 7 of
    # them being 8 days stale. Filtering here closes the loop so prune +
    # backfill stay coherent.
    if not dry_run:
        try:
            constituents_set = _load_current_constituents(s3, bucket, run_date=run_date)
            log.info(
                "Loaded current constituents: %d tickers — backfill will only "
                "write tickers in this set",
                len(constituents_set),
            )
        except Exception as exc:
            # Fail loud — without a constituents reference we'd silently
            # recreate every parquet-backed ticker, undoing prune work.
            raise RuntimeError(
                f"Backfill could not load current constituents (needed to "
                f"filter the universe write set): {exc}. Without this, "
                f"backfill would recreate any pruned ticker that still has "
                f"a price_cache parquet — see PR closing the prune+backfill "
                f"loop. Refresh constituents.json upstream and retry."
            ) from exc
    else:
        constituents_set = set(price_data)  # dry-run: don't restrict

    universe_tickers = [
        t for t in price_data
        if admits_universe_write(t)
        and price_data[t] is not None
        # A declared benchmark proxy is never in constituents.json — admit it
        # explicitly; it is still written Close-only to `macro` separately.
        and (t in constituents_set or t in UNIVERSE_BENCHMARK_PROXIES)
    ]
    excluded_by_constituents = sorted(
        t for t in price_data
        if t not in _SKIP_TICKERS
        and not _is_sector_etf(t)
        and price_data[t] is not None
        and t not in constituents_set
    )
    if excluded_by_constituents:
        log.info(
            "Backfill skipping %d price_cache ticker(s) absent from current "
            "constituents (parquet preserved for historical lookup; arctic "
            "row not written): %s",
            len(excluded_by_constituents),
            excluded_by_constituents[:20],
        )
    # Post-PR-#78: ``compute_features`` returns rows with NaN for features
    # whose rolling-window warmup exceeds available history (e.g. ATR-14
    # computes on 14 rows; dist_from_52w_high stays NaN under 252 rows).
    # We no longer split into "feature" vs "OHLCV-only" paths — every
    # ticker gets the unified schema with per-feature graceful degrade.
    # ``n_short_history`` is retained as an observability counter so the
    # completion log still reports how many tickers got partial features.
    n_short_history_in_scope = sum(
        1 for t in universe_tickers
        if len(price_data[t]) < MIN_ROWS_FOR_FEATURES
    )

    if ticker_filter:
        if ticker_filter not in universe_tickers:
            # Split the three reasons so operators / CW alarms can distinguish
            # transient data absence from configuration drift. The single
            # "no data or in skip list" string previously here masked the
            # 2026-05-27 PSTG case (ticker dropped from constituents but
            # still in chronic_polygon_gaps allowlist) by collapsing it into
            # a "no data" framing.
            if (
                ticker_filter in _SKIP_TICKERS
                and ticker_filter not in UNIVERSE_BENCHMARK_PROXIES
            ):
                log.error(
                    "Ticker %s is in _SKIP_TICKERS and not promoted via "
                    "_UNIVERSE_EXTRA — not eligible for universe write.",
                    ticker_filter,
                )
                return {
                    "status": "error",
                    "error": f"ticker_in_skip_list: {ticker_filter}",
                }
            # alpha-engine-config-I10704: a DECLARED proxy is exempt — four
            # of the six (XLK/XLV/XLF/XLE) are sector ETFs by the prefix test,
            # so without this carve-out `--ticker XLE` refuses the very write
            # the declaration exists to authorise.
            if (
                _is_sector_etf(ticker_filter)
                and ticker_filter not in UNIVERSE_BENCHMARK_PROXIES
            ):
                log.error(
                    "Ticker %s is a sector ETF (prefix in _SECTOR_ETF_PREFIXES) "
                    "— not eligible for universe write.",
                    ticker_filter,
                )
                return {
                    "status": "error",
                    "error": f"ticker_is_sector_etf: {ticker_filter}",
                }
            if (
                price_data.get(ticker_filter) is None
                or ticker_filter not in price_data
            ):
                log.error(
                    "Ticker %s has no parquet rows in the price_cache read "
                    "prefix chain — backfill cannot proceed.",
                    ticker_filter,
                )
                return {
                    "status": "error",
                    "error": f"ticker_no_data: {ticker_filter}",
                }
            # Last reason: not in current constituents (the 2026-05-27 PSTG
            # case). All other gates passed, parquet was read, ticker just
            # isn't a constituent — usually means the caller's allowlist /
            # config has drifted past a recent S&P remove.
            log.error(
                "Ticker %s has price_cache data but is not in current "
                "constituents (run_date=%s) — drop the upstream allowlist "
                "entry (e.g. chronic_polygon_gaps) and retry.",
                ticker_filter, run_date,
            )
            return {
                "status": "error",
                "error": f"ticker_not_in_constituents: {ticker_filter}",
            }
        universe_tickers = [ticker_filter]
        n_short_history_in_scope = (
            1 if len(price_data[ticker_filter]) < MIN_ROWS_FOR_FEATURES else 0
        )

    log.info(
        "Writing %d tickers to ArcticDB (%d below MIN_ROWS_FOR_FEATURES — partial-feature rows expected)",
        len(universe_tickers),
        n_short_history_in_scope,
    )

    # ── 3. Extract macro series ──────────────────────────────────────────────
    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")
    hyoas_series = macro.get("HYOAS")

    # ── 4. Compute features and write to ArcticDB ────────────────────────────
    if not dry_run:
        universe_lib = get_universe_lib(bucket)
        macro_lib = get_macro_lib(bucket)

    n_ok = 0
    n_skip = 0
    n_err = 0
    n_partial = 0  # written successfully with ≥1 NaN feature (short-history warmup)
    t_compute_start = time.time()

    for i, ticker in enumerate(universe_tickers):
        try:
            df = price_data[ticker]

            # Unified path (post-PR-#78): every ticker goes through
            # ``compute_features``. Rolling features whose warmup exceeds
            # the ticker's available history return NaN for the affected
            # rows; the row itself is preserved. Downstream consumers
            # (predictor training, research scanner) apply their own NaN
            # policy. The previous "OHLCV-only fresh listing" fork would
            # regress PR #79's schema migration on the next Saturday run
            # by writing a stripped-column frame that ``lib.update()``
            # then rejected.
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None
            ticker_alt = alt_data.get(ticker, {})

            featured_df = compute_features(
                df,
                spy_series=spy_series,
                vix_series=vix_series,
                sector_etf_series=sector_etf_series,
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                hyoas_series=hyoas_series,
                earnings_data=ticker_alt.get("earnings"),
                revision_data=ticker_alt.get("revisions"),
                options_data=ticker_alt.get("options"),
                fundamental_data=fundamentals.get(ticker),
            )

            if featured_df.empty:
                n_skip += 1
                continue

            # NaN-fill VWAP when missing from the input parquet so the
            # written schema is canonical [O,H,L,C,V,VWAP, FEATURES]. The
            # predictor/price_cache parquets are yfinance-sourced and have
            # no VWAP column; without this, keep_cols silently drops VWAP
            # and the next daily_append's update() rejects every ticker
            # with a column-position mismatch (incident 2026-05-01: full
            # 904/904 EOD failure traced to backfill-2026-04-30 dropping
            # VWAP across the universe).
            if "VWAP" not in featured_df.columns:
                featured_df["VWAP"] = np.nan

            # Default provenance: every row in the price_cache + delta
            # source data is yfinance-origin unless ``_apply_daily_delta``
            # tagged a row with a different source (polygon / fred from
            # the daily_closes delta). When the delta loader doesn't
            # surface a per-row source, the column stays "yfinance" —
            # the safer over-credit (price_cache parquets ARE yfinance-
            # sourced; the delta overlay may upgrade specific rows to
            # "polygon" but the row's underlying provenance origin is
            # still the yfinance baseline if the delta loader hasn't
            # tagged it).
            if PROVENANCE_COL not in featured_df.columns:
                # Default-fill as categorical to keep per-ticker memory
                # ~50x smaller than object dtype (~125KB → ~2.5KB per
                # 10y series). Saturday backfill rewrites 900 tickers
                # universe-wide; the savings is ~108MB peak.
                featured_df[PROVENANCE_COL] = make_source_series(
                    ["yfinance"] * len(featured_df), index=featured_df.index,
                )

            # Column-order projection + drop-non-canonical happens at the
            # write boundary via ``to_arctic_canonical``. The local copy
            # is still needed so the float32-cast below doesn't mutate
            # the in-memory price cache shared across the run.
            symbol_df = featured_df.copy()

            for f in FEATURES:
                if f in symbol_df.columns:
                    symbol_df[f] = symbol_df[f].astype("float32")

            symbol_df.index.name = "date"

            feature_cols_present = [f for f in FEATURES if f in symbol_df.columns]
            last_row_nan_features = [
                f for f in feature_cols_present
                if pd.isna(symbol_df[f].iloc[-1])
            ]
            if last_row_nan_features:
                n_partial += 1
                log.info(
                    "partial-features ticker=%s rows=%d nan_last_row=%d/%d features=%s",
                    ticker, len(symbol_df), len(last_row_nan_features),
                    len(feature_cols_present),
                    last_row_nan_features[:5] + (["..."] if len(last_row_nan_features) > 5 else []),
                )

            if not dry_run:
                # ``to_arctic_canonical`` re-projects to
                # ``OHLCV + source + FEATURES`` order, drops non-canonical
                # cols, and strips Categorical dtypes — single chokepoint
                # for the universe library's column-order + dtype
                # contract (closes the 2026-05-14 + 2026-05-21 EOD
                # column-order recurrence class).
                universe_lib.write(ticker, to_arctic_canonical(symbol_df))

            n_ok += 1

            if (i + 1) % 100 == 0:
                log.info(
                    "Progress: %d / %d tickers processed (%d ok, %d partial-features)",
                    i + 1, len(universe_tickers), n_ok, n_partial,
                )

        except Exception as exc:
            log.warning("Failed to write %s: %s", ticker, exc)
            n_err += 1

    log.info(
        "Backfill write complete: %d ok (%d with partial features on last row), %d skipped, %d errors",
        n_ok, n_partial, n_skip, n_err,
    )

    t_compute = time.time() - t_compute_start

    # ── 5. Write macro features ──────────────────────────────────────────────
    # Macro writes are a SIDE EFFECT of full-universe backfill. On a
    # single-ticker invocation (``--ticker X``) we skip them: the parquet
    # price cache's macro series may be stale relative to what
    # daily_append has been appending into ArcticDB, so rewriting macro
    # from parquet during a per-ticker patch would silently regress SPY/
    # VIX/XL* last_date (this is exactly what happened 2026-04-22 when a
    # SOLS backfill knocked macro back from 4/20 to 4/17). Operators who
    # genuinely want to rebuild macro must run a full-universe backfill
    # (``--rebuild-macro`` opt-in with ``--ticker`` is an explicit override).
    skip_macro = (ticker_filter is not None) and (not rebuild_macro)
    macro_df = pd.DataFrame()  # populated below when we do write macro
    if skip_macro:
        log.info(
            "Skipping macro library rewrite — ticker_filter=%s is set and "
            "--rebuild-macro was not passed. Macro library is preserved as "
            "last written by daily_append / full-universe backfill.",
            ticker_filter,
        )
    else:
        macro_df = _build_macro_features_df(macro)
        if not macro_df.empty and not dry_run:
            macro_lib.write("features", to_arctic_safe(macro_df))
            log.info("Wrote macro features: %d dates", len(macro_df))

        # Write raw macro series (SPY, VIX, etc.) for consumers that need them.
        # HYOAS added config#939 (credit spreads) — mirrors the existing
        # VIX/TNX/etc. raw-series persistence so daily_append's read-back
        # loop (macro_lib.read("HYOAS")) resolves after this backfill runs.
        if not dry_run:
            # alpha-engine-config-I9289: the sub-sector benchmark ETFs join
            # the raw-series write loop so a repaired price cache (full
            # history via ``repair_macro_series.py``) actually reaches
            # ArcticDB on the next Saturday run, rather than staying frozen
            # at whatever ``daily_append`` alone could accumulate one row
            # at a time.
            for key in [*_RAW_MACRO_SERIES, *_SUB_SECTOR_ETFS]:
                series = macro.get(key)
                if series is not None:
                    macro_series_df = pd.DataFrame({"Close": series}, index=series.index)
                    macro_series_df.index.name = "date"
                    _write_macro_series_no_shrink(
                        macro_lib, key, to_arctic_safe(macro_series_df), reference_date=today_str,
                    )

            # Write sector ETFs
            for key in macro:
                if key.startswith("XL"):
                    sector_df = pd.DataFrame({"Close": macro[key]}, index=macro[key].index)
                    sector_df.index.name = "date"
                    _write_macro_series_no_shrink(
                        macro_lib, key, to_arctic_safe(sector_df), reference_date=today_str,
                    )

    # ── 5b. Factor-momentum second pass (W2.3, L4469) ────────────────────────
    # ``factor_momentum_ratio`` is a cross-sectional-time-series feature: date
    # t's value ranks the WHOLE cross-section and builds factor-return
    # portfolios, so it can't be produced inside the per-ticker compute_features
    # loop above. Run it as a second pass over the just-written universe lib,
    # reading back the slim (close + loadings) panel and writing the column
    # back per ticker (canonical projection keeps it in its FEATURES position).
    # Full-universe only — a ``--ticker X`` patch can't reconstruct the
    # cross-section, so the column is left to the next full backfill / the
    # daily go-forward path. Runs BEFORE the snapshot so the snapshot is
    # complete.
    if not dry_run and ticker_filter is None:
        fm_result = materialize_factor_momentum(
            universe_lib,
            universe_tickers,
            canonical_fn=to_arctic_canonical,
        )
        log.info("Factor-momentum second pass: %s", json.dumps(fm_result, default=str))

    # ── 5c. Factor-loading z-score second pass (C.1 / C.2b) ───────────────────
    # The 9 *_zscore Barra loadings are cross-sectional (apply_factor_zscores).
    # S3 feature-store compute.py already emits them; ArcticDB (predictor
    # training cache + risk_model_persist) needs the same second pass so C.2b
    # can build F + D from tmp_cache. Full-universe only — a ``--ticker X``
    # patch can't reconstruct the cross-section.
    if not dry_run and ticker_filter is None:
        flz_result = materialize_factor_loading_zscores(
            universe_lib,
            universe_tickers,
            canonical_fn=to_arctic_canonical,
        )
        log.info(
            "Factor-loading z-score second pass: %s",
            json.dumps(flz_result, default=str),
        )

    # ── 6. Snapshot ──────────────────────────────────────────────────────────
    if not dry_run:
        snapshot_name = f"backfill-{today_str}"
        try:
            universe_lib.snapshot(snapshot_name)
            log.info("Created snapshot: %s", snapshot_name)
        except Exception as exc:
            log.warning("Snapshot creation failed (non-fatal): %s", exc)

    # ── 6b. Universe-freshness receipt — Saturday DataPhase1 emit ─────────────
    # Closes 5/23-SF P0 sweep item (d). Pre-fix the receipt only fired from
    # the weekday `daily_append` path; the Saturday DataPhase1 backfill
    # wrote universe symbols without emitting a corresponding freshness
    # signature. L1316 + L1322 closes-when criteria explicitly reference
    # `s3://alpha-engine-research/health/universe_freshness.json` containing
    # BNY/P/SN as universe symbols — without Saturday emit, operator
    # closure-audit takes an extra weekday-SF cycle. Per-ticker-only
    # invocations (`ticker_filter` set) skip the emit since the receipt
    # is a system-wide signature, not per-ticker. Skipped on dry_run for
    # the same reason `daily_append` skips dry_run emits.
    if not dry_run and ticker_filter is None:
        try:
            receipt = _scan_universe_and_emit_freshness_receipt(
                s3=s3,
                bucket=bucket,
                universe_lib=universe_lib,
                expected_tickers=sorted(constituents_set),
            )
            log.info(
                "Saturday DataPhase1 universe-freshness receipt emitted: "
                "n=%d all_fresh stalest=%s(%d trading-d)",
                receipt["n_symbols_checked"],
                receipt["stalest_symbol"],
                receipt["stalest_age_trading_days"],
            )
        except Exception as exc:
            # Receipt emit failure is loud-fail per [[feedback_no_silent_fails]]
            # since the receipt IS the closure signature for L1316/L1322. A
            # backfill that completed its writes but couldn't verify them
            # is structurally incomplete — better to crash here so the SF
            # Catch surfaces it than to declare backfill "ok" with a
            # missing receipt.
            log.error(
                "Saturday DataPhase1 universe-freshness emit FAILED: %s. "
                "Backfill writes are on-disk but the closure signature is "
                "missing. Investigate before declaring this cycle done.",
                exc,
            )
            raise

    t_total = time.time() - t0

    result = {
        "status": "ok",
        "tickers_written": n_ok,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "macro_dates": len(macro_df) if not macro_df.empty else 0,
        "load_seconds": round(t_load, 1),
        "compute_seconds": round(t_compute, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
        "universe_freshness_receipt_emitted": (
            not dry_run and ticker_filter is None
        ),
    }

    log.info("Backfill complete: %s", json.dumps(result, default=str))

    # ── 7. Validation (optional) ─────────────────────────────────────────────
    if validate and not dry_run:
        _run_validation(
            universe_lib, price_data, macro, sector_map, fundamentals, alt_data,
            tickers=[ticker_filter] if ticker_filter else None,
        )

    return result


def _run_validation(
    universe_lib,
    price_data: dict[str, pd.DataFrame],
    macro: dict[str, pd.Series],
    sector_map: dict[str, str],
    fundamentals: dict[str, dict],
    alt_data: dict[str, dict],
    tickers: list[str] | None = None,
):
    """Spot-check: recompute features inline and compare to ArcticDB.

    ``tickers`` names what to check; ``None`` checks the first 10 universe
    symbols. A per-ticker backfill passes its own ticker, because it loads
    only that ticker's parquet (``_PER_TICKER_CACHE_CONTEXT``).
    """
    if tickers is not None:
        check_tickers = list(tickers)
    else:
        check_tickers = sorted(universe_lib.list_symbols())[:10]

    log.info("Running validation on %d tickers: %s", len(check_tickers), check_tickers)

    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")
    hyoas_series = macro.get("HYOAS")

    passed = 0
    failed = 0

    for ticker in check_tickers:
        try:
            stored = universe_lib.read(ticker).data

            df = price_data[ticker]
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None
            ticker_alt = alt_data.get(ticker, {})

            recomputed = compute_features(
                df,
                spy_series=spy_series,
                vix_series=vix_series,
                sector_etf_series=sector_etf_series,
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                hyoas_series=hyoas_series,
                earnings_data=ticker_alt.get("earnings"),
                revision_data=ticker_alt.get("revisions"),
                options_data=ticker_alt.get("options"),
                fundamental_data=fundamentals.get(ticker),
            )

            # Compare row counts
            if len(stored) != len(recomputed):
                log.warning(
                    "FAIL %s: row count mismatch (stored=%d, recomputed=%d)",
                    ticker, len(stored), len(recomputed),
                )
                failed += 1
                continue

            # Compare feature values on last 10 rows
            feature_cols = [f for f in FEATURES if f in stored.columns and f in recomputed.columns]
            tail_stored = stored[feature_cols].tail(10).values
            tail_recomputed = recomputed[feature_cols].tail(10).values.astype("float32")

            if np.allclose(tail_stored, tail_recomputed, atol=1e-5, equal_nan=True):
                log.info("PASS %s: features match (%d rows, %d features)", ticker, len(stored), len(feature_cols))
                passed += 1
            else:
                max_diff = np.nanmax(np.abs(tail_stored - tail_recomputed))
                log.warning("FAIL %s: max feature diff = %.6f", ticker, max_diff)
                failed += 1

        except Exception as exc:
            log.warning("FAIL %s: validation error: %s", ticker, exc)
            failed += 1

    log.info("Validation complete: %d passed, %d failed", passed, failed)


def main():
    parser = argparse.ArgumentParser(description="Backfill ArcticDB universe from S3 price cache")
    parser.add_argument("--dry-run", action="store_true", help="Compute but skip ArcticDB writes")
    parser.add_argument("--ticker", default=None, help="Process single ticker (for testing)")
    parser.add_argument("--validate", action="store_true", help="Run spot-check validation after backfill")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"S3 bucket (default: {DEFAULT_BUCKET})")
    parser.add_argument(
        "--rebuild-macro",
        action="store_true",
        help=(
            "Force macro-library rewrite even when --ticker is set. "
            "Default: per-ticker invocations SKIP macro writes to avoid "
            "regressing SPY/XL* freshness from the stale parquet cache."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = backfill(
        bucket=args.bucket,
        dry_run=args.dry_run,
        ticker_filter=args.ticker,
        validate=args.validate,
        rebuild_macro=args.rebuild_macro,
    )

    if result["status"] != "ok":
        log.error("Backfill failed: %s", result.get("error"))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
