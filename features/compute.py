"""
features/compute.py — Standalone feature computation for the full universe.

Decouples feature computation from the predictor module entirely. Loads
price + macro data from S3 (slim cache + daily_closes delta), computes all
53 features for every ticker in the universe, and writes dated Parquet
snapshots to S3.

NO imports from alpha-engine-predictor. All S3 loading is self-contained.

Usage:
    python -m features.compute                          # today's date
    python -m features.compute --date 2026-04-03        # specific date
    python -m features.compute --dry-run                # compute but skip S3 write

Data sources:
    Prices:       predictor/price_cache_slim/*.parquet + staging/daily_closes/{date}.parquet
    Macro:        SPY, VIX, TNX, IRX, GLD, USO, VIX3M (from slim cache)
    Sector map:   data/sector_map.json
    Fundamentals: archive/fundamentals/{date}.json (cached by prior inference)
    Alt data:     market_data/weekly/{latest}/alternative/{TICKER}.json (from DataPhase2)
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import hashlib
from dataclasses import dataclass

import corporate_actions as ca
from features.cross_sectional import apply_factor_zscores
from features.factor_momentum import DEFAULT_FACTOR_LOADINGS, compute_factor_momentum_feature
from features.feature_engineer import FEATURES, FEATURE_CFG, MIN_ROWS_FOR_FEATURES, compute_features
from features.metron_supplemental import compute_metron_supplemental_features, write_metron_supplemental_snapshot
from features.postflight import (
    ALL_NULL_EXPECTED,
    ZERO_VARIANCE_EXEMPT,
    assert_no_dead_feature_columns,
    find_all_null_columns,
    find_zero_variance_columns,
)
from features.private_pack import apply_private_features
from features.registry import GROUPS, upload_registry
from features.writer import write_feature_snapshot

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_BUCKET = "alpha-engine-research"
FEATURE_STORE_PREFIX = "features/"

# Registry-aware split-jump audit (PR3, config#1433). The screen threshold is
# the DIAGNOSTIC band — it must be low enough to SURFACE sub-45% splits
# (3-for-2 = -33%, 4-for-3 = -25%) so the latent bug the old >45% heuristic
# missed becomes visible. The BLOCKING raise condition is NOT this magnitude —
# it is registry-driven (a residual jump that a registered action EXPLAINS),
# which is what lets a sub-45% registered split be caught WITHOUT false-failing
# on a legitimate large move (a real ±33% earnings move has no registered
# action, so it is only ever WARN-classified as "suspected").
_ACTION_JUMP_SCREEN_THRESHOLD = 0.18
# A residual un-flattened split jump is the split factor multiplied by the real
# overnight move, so the observed boundary ratio is "factor × (1 ± small)" — a
# loose relative tol (vs the registry's exact 0.5% same-date tol) confirms the
# residual jump IS the split (not a coincident legit move on a flattened
# boundary) without requiring the move to equal the factor to feed-rounding.
_AUDIT_FACTOR_REL_TOL = 0.15
# The un-flattened jump appears at the first trading row on/after the split's
# ex_date; allow a few days' slack for weekend/holiday gaps between ex_date and
# the first observed row.
_AUDIT_EX_DATE_WINDOW_DAYS = 4
# The logical store split restatement targets (shared by the Saturday backfill
# and the daily feature-snapshot delta — see corporate_actions.STORE_*).
_RESTATE_STORE = ca.STORE_ARCTICDB_UNIVERSE

# Closed-set of provenance source values written to the `source` column on
# universe rows. Stored as a pandas Categorical so the in-memory cost is
# ~1 byte per row (category code) instead of ~50 bytes per row (object
# string pointer). On a full-universe pass through ``_apply_daily_delta``
# (900 tickers × 2500 rows of 10y history each) the savings is ~108MB
# peak resident memory — material on the 2GB t3.small trading instance
# where daily_append sits alongside SSM agent + IB Gateway + (any
# transient daemon restarts) and OOM is a real constraint. Order is
# stable so the category codes don't shift between writers; "unknown"
# anchors the unset / pre-migration case.
SOURCE_CATEGORIES: tuple[str, ...] = ("polygon", "yfinance", "fred", "unknown")


def make_source_series(values: list[str] | pd.Series, index: pd.Index | None = None) -> pd.Series:
    """Build a categorical Series for the ``source`` provenance column.

    Use this instead of `pd.Series(["yfinance"] * n)` or
    `df["source"] = "yfinance"` anywhere the assignment covers a full-
    history slice. Values outside SOURCE_CATEGORIES are coerced to
    "unknown" rather than added to the category — keeps the category
    set fixed across all writers so downstream readers can rely on it.
    """
    if isinstance(values, pd.Series):
        values = values.astype(str).tolist()
    cleaned = [v if v in SOURCE_CATEGORIES else "unknown" for v in values]
    cat = pd.Categorical(cleaned, categories=SOURCE_CATEGORIES)
    return pd.Series(cat, index=index)

# Rows to keep per ticker before feature computation. The longest STACKED
# rolling window is residual_momentum_ratio (alpha-engine-config-I7539): it
# needs beta_60d's 60-row warmup BEFORE the 231-row cumulative-residual
# window can start, then a 21-row shift on top — 60 + 231 + 21 = 312 rows
# minimum for the latest (most recent) row to be non-NaN. Below that, the
# per-ticker value is NaN for every ticker, and the compute.py store-row
# fallback (`row[f] = float(val) if pd.notna(val) else 0.0`) silently turns
# a universe-wide NaN into a universe-wide 0.0 — a zero-variance column that
# passes every null-coverage check. 340 keeps a ~28-row buffer, matching the
# ~10% margin the original 252->280 buffer used. Do not lower this without
# re-deriving the deepest stacked window across feature_engineer.py.
#
# alpha-engine-config-I7539 (2026-08-17): RAISED 340 -> 585. 340 covered the
# deepest per-ticker STACKED window, and the factor-momentum SECOND PASS needs
# strictly more than that — it is a cross-sectional time series built ON TOP of
# the per-ticker output, so its warmup composes with theirs rather than sitting
# inside it:
#
#   dist_from_52w_high, the deepest DEFAULT_FACTOR_LOADINGS member,
#   is NaN for its first 252 rows (FEATURE_CFG["weeks_52_days"], and
#   compute_features deliberately does NOT dropna since 2026-04-21), so
#   compute_daily_factor_returns' min_names=20 gate drops every one of those
#   dates for that factor.                                              252
#   compute_factor_momentum_series then needs
#   rolling(window - skip = 231, min_periods=231).sum().shift(21)
#   dates of FACTOR RETURNS, all of which must survive the gate above. 231 + 21
#                                                                     ---------
#                                                                          504
#
# At 340 rows only 340 - 252 = 88 dates carried a usable
# dist_from_52w_high loading — far under the 252 the rolling window needs — so
# factor_momentum_ratio was NaN for the ENTIRE universe on the daily path and
# fell back to the FEATURES-loop 0.0 default. Measured on the 2026-08-17
# production EOD: "Factor-momentum second pass (daily): 0/901 tickers got a
# non-NaN factor_momentum_ratio". That is the constant column
# alpha-engine-config-I7539 was filed about, and why #1410's revival of the
# second pass ran but produced nothing: the pass was never the problem, the
# window it was handed was.
#
# 585 = 504 + 81, the same ~16% buffer 340 carried over its own 312. Do not
# lower it without re-deriving BOTH the per-ticker stacked window and the
# factor-momentum composition above — tests/test_feature_warmup_rows_i7539.py
# derives the floor from the live constants and fails if either moves.
_FEATURE_WARMUP_ROWS = 585

# alpha-engine-config-I7572 (2026-08-19, second half of the factor-momentum
# fix): a10a95cb raised the TRADING-day floor above to 585 but nothing
# raised the CALENDAR-day window the price source is actually READ with.
# `_load_price_source` below calls `load_universe_ohlcv` / `load_macro_series`
# (nousergon_lib.arcticdb) with no `lookback_days` override, so both default
# to `_SLIM_EQUIVALENT_LOOKBACK_DAYS = 730` CALENDAR days — a
# `date_range=(end - 730d, end)` ArcticDB read, not a row count. 730 calendar
# days is ~730 * 252/365.25 ≈ 504 TRADING days before a single holiday is
# subtracted — already short of the 585-row floor above, so the per-ticker
# trim a few lines below this constant's use (`if len(df) >
# _FEATURE_WARMUP_ROWS: trim`) was a permanent no-op: every ticker capped
# out under 585 rows, `compute_daily_factor_returns`' warmup gate was never
# satisfied for the whole universe, and `factor_momentum_ratio` produced
# 0/902 non-null on the daily path EVERY run since a10a95cb landed — measured
# live 2026-08-19 on `s3://alpha-engine-research/features/2026-08-19/
# technical.parquet`. The second pass itself was never broken; like I7539's
# first round, it was starved of the window it needed.
#
# Convert the 585 TRADING-day floor to a CALENDAR-day request (252 trading
# days / 365.25 calendar days, the standard convention) with a 20% buffer
# for holiday clustering — deliberately wider than _FEATURE_WARMUP_ROWS' own
# ~16%, since this margin has to survive an actual ArcticDB date-range read
# (weekends AND market holidays), not just a row-count derivation.
_ARCTICDB_LOOKBACK_DAYS = math.ceil(_FEATURE_WARMUP_ROWS * 365.25 / 252 * 1.20)

# Sub-sector benchmark ETFs (config#934) — SMH/IGV/XBI/PPH/XOP/KRE/ITA/GDX,
# the distinct non-XL* symbols in constituents.GICS_SUBINDUSTRY_TO_ETF. Like
# the XL* sector ETFs (excluded via _is_sector_etf) these are benchmark
# series, NOT stocks: they must NOT get full-universe feature computation and
# must NOT be flagged as constituents-churn stragglers by the coverage diff.
# The XL* prefix test can't catch them (SMH/IGV/… don't start with "XL"), so
# they are enumerated into _SKIP_TICKERS explicitly.
_SUB_SECTOR_ETFS = frozenset({"SMH", "IGV", "XBI", "PPH", "XOP", "KRE", "ITA", "GDX"})

# ── The DECLARED benchmark-proxy set ────────────────────────────────────────
#
# Macro/index/ETF symbols ALSO promoted to full `universe` members (full OHLCV
# + engineered features), in addition to their Close-only `macro`-library
# write. This frozenset is THE declaration — every scoping predicate in
# builders/backfill.py and builders/daily_append.py reads it (through
# ``admits_universe_write`` below), and collectors/prices.py's
# ``_ALWAYS_DOWNLOAD`` is held to it by a lockstep test. There is no second
# hand-kept list anywhere; adding a proxy here is the only edit needed to make
# the whole producer maintain it.
#
# Why each member:
#   SPY                 — became a held core position with the 2026-05-13
#                         portfolio-optimizer cutover, so every held-position
#                         code path (eod_reconcile #181, morning-planner ATR
#                         #185) needs SPY's engineered features from
#                         `universe`.
#   IWM, XLK, XLV,      — the size/sector ATTRIBUTION proxies declared by
#   XLF, XLE              `alpha-engine-config/strategy/slots/attribution.yaml`
#                         and read by `crucible/slots/__init__.py::
#                         attribution_factor_symbols`. Since crucible-PR271
#                         (alpha-engine-config-I10683) every panel compile
#                         fetches EVERY declared proxy from the `universe`
#                         library and REFUSES on a missing one — a proxy that
#                         silently drops out is survivorship bias. Measured
#                         2026-09-14 in-region (alpha-engine-config-I10704):
#                         the library held SPY alone, so `data.weekly` would
#                         have failed at its first stage on 2026-09-19.
#
# Members deliberately STAY in ``_SKIP_TICKERS`` so prune_delisted_tickers
# (none of them are in constituents.json → would otherwise all be prune
# candidates), the daily_append coverage-diff accounting and the
# constituents-drift check keep treating them as non-stock. The declared set
# only WIDENS the universe-WRITE candidate set, nothing else. NOT a macro-lib
# teardown: the same symbols keep their Close-only `macro` rows, and
# VIX/VIX3M/TNX/IRX/GLD/USO have no tradeable OHLCV we hold, so they stay
# macro-only.
UNIVERSE_BENCHMARK_PROXIES = frozenset({
    "SPY",
    "IWM", "XLK", "XLV", "XLF", "XLE",
})

# Legacy name for the SAME object (not a copy) — ~40 call sites and tests
# spell it this way. Kept as an alias so there is exactly one set in memory
# and `is`-identity holds; new code should use UNIVERSE_BENCHMARK_PROXIES.
_UNIVERSE_EXTRA = UNIVERSE_BENCHMARK_PROXIES

# Tickers that are macro/index series, not stocks
_SKIP_TICKERS = {
    "SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
    "^VIX", "^VIX3M", "^TNX", "^IRX",
    *_SUB_SECTOR_ETFS,
    # Every declared benchmark proxy is skip-protected (see the block above).
    # IWM is here because the "XL" prefix test cannot reach it; the XL*
    # proxies are already excluded by ``_is_sector_etf`` but are enumerated
    # anyway so the `UNIVERSE_BENCHMARK_PROXIES <= _SKIP_TICKERS` invariant
    # (tests/test_spy_universe_member.py) holds by construction rather than by
    # coincidence of two independent predicates.
    *UNIVERSE_BENCHMARK_PROXIES,
}

# Sector ETFs to skip (not individual stocks)
_SECTOR_ETF_PREFIXES = {"XL"}


def _is_sector_etf(ticker: str) -> bool:
    return len(ticker) == 3 and ticker[:2] in _SECTOR_ETF_PREFIXES


def admits_universe_write(ticker: str) -> bool:
    """THE universe-write scoping predicate — one implementation, one list.

    ``True`` when ``ticker`` is eligible to be written as a symbol in the
    ArcticDB ``universe`` library: either it is a DECLARED benchmark proxy
    (``UNIVERSE_BENCHMARK_PROXIES``), or it is an ordinary stock — neither a
    macro/index series (``_SKIP_TICKERS``) nor a sector ETF
    (``_is_sector_etf``).

    Eligibility ONLY. Callers still apply their own site conditions (a
    constituents-membership test, "has price data", a per-run ticker filter).

    Why this is a function and not six copies of a boolean expression:
    alpha-engine-config-I2703 and I2704 were both the SAME defect — a
    replicated predicate that DRIFTED at one call site, so SPY silently
    vanished from the freshness accounting while the write path still wrote
    it. alpha-engine-config-I10704 is the third instance of the class: the
    ``_UNIVERSE_EXTRA`` carve-out existed at every site but was ANDed with a
    bare ``not _is_sector_etf(t)``, so an XL* proxy could never be admitted no
    matter what the declared list said. Centralising the whole expression is
    what makes the next proxy a one-line declaration instead of a six-site
    edit with one site forgotten.

    ``^``-prefixed spellings (``^VIX``) normalise to their bare form, which is
    what two of the historical call sites did by hand and the others did not.
    """
    stem = ticker.lstrip("^")
    if stem in UNIVERSE_BENCHMARK_PROXIES:
        return True
    return stem not in _SKIP_TICKERS and not _is_sector_etf(stem)


# ── S3 data loading (self-contained, no predictor imports) ───────────────────

def _load_sector_map(s3, bucket: str) -> dict[str, str]:
    """Load ticker -> sector ETF mapping from S3."""
    try:
        obj = s3.get_object(Bucket=bucket, Key="data/sector_map.json")
        return json.loads(obj["Body"].read())
    except Exception as exc:
        log.warning("Failed to load sector_map.json: %s", exc)
        return {}


def _load_sub_sector_etf_map(s3, bucket: str) -> dict[str, str]:
    """Load ticker -> sub-sector benchmark ETF mapping from S3 (config#934).

    Best-effort/non-blocking, mirroring _load_sector_map: a missing file
    (e.g. before the weekly collector has written it, or on an S3 read
    failure) returns an empty map, which degrades every ticker's
    sub_sector_vs_benchmark_* to its neutral default rather than raising.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key="data/sub_sector_etf_map.json")
        return json.loads(obj["Body"].read())
    except Exception as exc:
        log.warning("Failed to load sub_sector_etf_map.json: %s", exc)
        return {}


# Shared S3 parquet loaders live in store.parquet_loader so non-feature
# callers (e.g. collectors.macro's breadth computation) can reuse the same
# normalized DataFrame shape without importing private helpers. Slim cache
# (2y) is sufficient here — features only use the latest row and 2y gives
# enough warmup for every indicator.
from store.parquet_loader import load_parquet_from_s3 as _load_parquet_from_s3
from nousergon_lib.arcticdb import (
    load_universe_ohlcv,
    load_macro_series,
    open_macro_lib,
)


def _safe_last_date(idx: pd.Index) -> pd.Timestamp | None:
    """Return the normalized last date from a DatetimeIndex, or None if empty/NaT."""
    if idx is None or idx.empty:
        return None
    last = idx.max()
    if pd.isna(last):
        return None
    return pd.Timestamp(last).normalize()


def _load_delta_from_daily_closes(
    s3, bucket: str, start_date: pd.Timestamp, end_date: pd.Timestamp,
) -> dict[str, list[dict]]:
    """
    Load daily_closes parquets for every trading day in (start_date, end_date].

    The daily_closes format has index=ticker (string) and columns including
    date, open, high, low, close, adj_close, volume (all lowercase).

    Returns dict: ticker -> list of row dicts with capitalized OHLCV keys.
    """
    delta_dates = [
        d.strftime("%Y-%m-%d")
        for d in pd.bdate_range(start_date + pd.Timedelta(days=1), end_date)
    ]

    if not delta_dates:
        return {}

    log.info(
        "Loading daily_closes delta: %d trading days (%s -> %s)",
        len(delta_dates), delta_dates[0], delta_dates[-1],
    )

    ticker_rows: dict[str, list[dict]] = {}

    n_missing_dates = 0
    for d in delta_dates:
        key = f"staging/daily_closes/{d}.parquet"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
        except s3.exceptions.NoSuchKey:
            # Market holiday within the business-day range (e.g., Good Friday).
            log.warning("daily_closes/%s.parquet missing (market holiday?)", d)
            n_missing_dates += 1
            continue
        except Exception as exc:
            raise RuntimeError(
                f"Unexpected S3 error reading daily_closes/{d}.parquet: {exc}"
            ) from exc
        buf = io.BytesIO(obj["Body"].read())
        day_df = pd.read_parquet(buf, engine="pyarrow")
        for ticker, row in day_df.iterrows():
            if ticker not in ticker_rows:
                ticker_rows[ticker] = []
            # Per-row provenance from the daily_closes parquet's ``source``
            # column (set by daily_closes.collect to "polygon" / "yfinance"
            # / "fred"). Surfaced through to the delta merge in
            # ``_apply_daily_delta`` so downstream ArcticDB writes can
            # tag each row with where its values came from.
            src_raw = row.get("source")
            # VWAP (alpha-engine-config-I7569): daily_closes.parquet DOES
            # carry a VWAP column (collectors/daily_closes.py schema), but
            # this dict was never including it, so pd.concat in
            # _apply_daily_delta unioned it in as NaN for every delta-merged
            # (i.e. every "latest") row, every ticker, every day. That NaN
            # fed feature_engineer.compute_features's
            # ``(Close - VWAP) / VWAP`` as NaN, which the FEATURES-loop
            # fallback below (`row[f] = ... else 0.0`) then silently turned
            # into a universe-wide constant 0.0 for vwap_divergence_pct —
            # the same fallback line I7539 already implicated once. VWAP is
            # genuinely None on yfinance-fallback/FRED rows (no true
            # volume-weighted price to report) — preserved as NaN, not
            # backfilled, so those days still correctly neutral-default.
            vwap_raw = row.get("VWAP")
            ticker_rows[ticker].append({
                "date":   pd.Timestamp(d),
                "Open":   float(row.get("Open", np.nan)),
                "High":   float(row.get("High", np.nan)),
                "Low":    float(row.get("Low", np.nan)),
                "Close":  float(row.get("Close", np.nan)),
                "Volume": int(row.get("Volume", 0)),
                "VWAP":   float(vwap_raw) if pd.notna(vwap_raw) else np.nan,
                "source": str(src_raw) if pd.notna(src_raw) else "unknown",
            })

    n_tickers = len(ticker_rows)
    n_rows = sum(len(v) for v in ticker_rows.values())
    log.info(
        "Delta loaded: %d rows across %d tickers (%d/%d dates missing)",
        n_rows, n_tickers, n_missing_dates, len(delta_dates),
    )
    if delta_dates and n_missing_dates == len(delta_dates):
        raise RuntimeError(
            f"Every date in delta range was missing ({len(delta_dates)} dates) — "
            "daily_closes writer is likely broken upstream"
        )
    return ticker_rows


def _build_registry(s3, bucket: str):
    """Construct a ``CorporateActionRegistry`` from an S3 client + bucket, or
    ``None`` when no usable client is available.

    Returning ``None`` keeps legacy positional / dry-run callers (and unit
    tests that pass ``s3=None``) free of S3 + polygon I/O: with no registry,
    ``_apply_daily_delta`` performs no corporate-action detection or
    restatement (production callers — backfill, feature-snapshot — always pass
    a registry).
    """
    if s3 is None:
        return None
    try:
        return ca.CorporateActionRegistry(s3, bucket)
    except Exception as exc:  # noqa: BLE001 - degrade, never hard-fail the load
        log.warning(
            "could not build corporate-action registry (%s) — split "
            "restatement degraded this pass", exc,
        )
        return None


def _detect_split_actions(
    start_date, end_date, registry, *, run_id: str,
) -> dict[str, list]:
    """AUTHORITATIVELY detect splits executing in ``[start_date, end_date]`` via
    polygon, persist each in the registry (write-if-absent), and group them by
    ticker.

    This REPLACES the old ">45% single-day return" magnitude heuristic that
    *triggered* restatement: that heuristic silently MISSED sub-45% splits
    (3-for-2 = -33%, 4-for-3 = -25%) and could false-trigger on a legitimate
    large move. The polygon split feed is the authoritative trigger; magnitude
    no longer gates restatement. Returns ``{}`` (degrade) on any detection
    failure — a detection miss must never hard-fail the load; the blocking
    audit is the backstop for a genuinely missed restatement.
    """
    start_str = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    try:
        actions = ca.detect_splits(start_str, end_str)
    except Exception as exc:  # noqa: BLE001 - detect_splits already degrades
        log.warning(
            "corporate-action split detection failed (%s) — no restatement "
            "this pass", exc,
        )
        return {}
    by_ticker: dict[str, list] = {}
    for action in actions:
        if registry is not None:
            try:
                registry.record_detected(action, run_id=run_id)
            except Exception as exc:  # noqa: BLE001 - provenance write best-effort
                log.warning(
                    "record_detected failed for %s (%s) — continuing",
                    action.action_id, exc,
                )
        by_ticker.setdefault(action.ticker, []).append(action)
    return by_ticker


def _apply_daily_delta(
    s3, bucket: str, date_str: str, price_data: dict[str, pd.DataFrame],
    *, registry=None,
) -> tuple[dict[str, pd.DataFrame], set[str]]:
    """
    Append daily_closes delta rows to price DataFrames.

    Matches the predictor's ``load_price_data_from_cache`` behaviour:
    1. Loads ALL daily_closes files between the slim cache's last date and
       the target date (not just the target date's file).
    2. Uses ``duplicated(keep='last')`` so delta rows override cache rows
       on the same date.
    3. Restates EVERY registered split (regardless of magnitude) through
       ``corporate_actions.apply`` — authoritative polygon detection replaces
       the old >45% trigger, fixing the latent sub-45% miss (PR3, config#1433).
       Restatement is exactly-once via the registry's applied markers, so the
       feature-snapshot path's load of an already-restated ArcticDB series is a
       no-op rather than a double-apply.

    ``registry`` (keyword-only) is a ``CorporateActionRegistry``; when ``None``
    (legacy positional callers / ``s3=None`` tests) NO corporate-action
    detection or restatement is performed.

    Returns (updated_price_data, split_tickers).
    """
    # Find the OLDEST ticker's last_date so the delta load covers every
    # ticker that needs catching up. ``min`` not ``max``: if even one
    # ticker is freshly refreshed (e.g. ``prices.collect`` flagged a single
    # stale ticker via mtime check on yfinance refresh), ``max`` would
    # advance ``slim_last_date`` to that one ticker's end — and on a
    # Saturday run that's exactly when ``bdate_range(slim_last_date+1,
    # today)`` yields zero business days, so the loader returns empty and
    # every OTHER ticker stays stuck at its older cache last_date.
    # Origin: 2026-05-09 weekly SF — VEEV got refreshed via yfinance to
    # 5/8, every other parquet ended at 5/6, ``max`` picked 5/8 → today
    # 5/9 → empty bdate_range → backfill regression preflight failed at
    # planned=5/6 < existing=5/8 across SPY/VIX/XL*/sampled-universe.
    candidate_dates = [_safe_last_date(df.index) for df in price_data.values()]
    valid_dates = [d for d in candidate_dates if d is not None]
    if not valid_dates:
        return price_data, set()

    slim_last_date = min(valid_dates)
    today = pd.Timestamp(date_str).normalize()

    # Load all delta files between slim cache last date and target date
    ticker_rows = _load_delta_from_daily_closes(s3, bucket, slim_last_date, today)

    if not ticker_rows:
        log.info("No daily_closes delta files found — using cache as-is")
        return price_data, set()

    # Registry-driven, authoritative split detection over the delta window
    # (PR3, config#1433). No-op when no registry (legacy / dry-run callers).
    actions_by_ticker: dict[str, list] = {}
    if registry is not None:
        actions_by_ticker = _detect_split_actions(
            slim_last_date, today, registry,
            run_id=f"apply_daily_delta:{date_str}",
        )

    split_tickers: set[str] = set()
    n_updated = 0

    for ticker, slim_df in list(price_data.items()):
        # VWAP included (alpha-engine-config-I7569): this whitelist used to
        # silently drop VWAP off the cache side even when the slim frame
        # carried it, and the delta_df whitelist below dropped it off every
        # delta (i.e. "latest") row unconditionally — the second, decisive
        # gate that zeroed vwap_divergence_pct universe-wide even after
        # _load_delta_from_daily_closes started returning it.
        base_cols = ["Open", "High", "Low", "Close", "Volume", "VWAP"]
        base = slim_df[[c for c in base_cols if c in slim_df.columns]].copy()
        # Tag pre-delta rows as yfinance-origin (price_cache parquets are
        # yfinance-sourced) so the merged frame carries provenance per
        # row. Delta rows below override this on overlap via dedup
        # keep="last". Categorical dtype (vs object/string) cuts the
        # per-ticker memory of this column from ~125KB to ~2.5KB — across
        # 900 tickers that's ~108MB less peak resident memory on the
        # universe-wide pass.
        base["source"] = make_source_series(["yfinance"] * len(base), index=base.index)

        delta = ticker_rows.get(ticker, [])
        if not delta:
            price_data[ticker] = base
            continue

        # Build delta DataFrame with capitalized columns (matches slim cache schema)
        delta_df = pd.DataFrame(
            [
                {
                    **{k: r[k] for k in ["Open", "High", "Low", "Close", "Volume"]},
                    "VWAP": r.get("VWAP", np.nan),
                }
                for r in delta
            ],
            index=pd.DatetimeIndex([r["date"] for r in delta]),
        )
        delta_df["source"] = make_source_series(
            [r.get("source", "unknown") for r in delta], index=delta_df.index,
        )

        combined = pd.concat([base, delta_df])
        # keep="last" so delta rows win on duplicate dates (matches predictor)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()

        # Registry-driven full-history RESTATEMENT (data#1298, PR3 config#1433).
        #
        # The ArcticDB universe is append-only + windowed: a split restates the
        # FULL adjusted history, but ArcticDB only ever got a recent window
        # patched, leaving a split-boundary discontinuity that corrupts
        # cross-boundary TRAINING features. We back-adjust the ENTIRE pre-split
        # window by the polygon-authoritative split factor here, where the full
        # series is materialized for the downstream ``lib.write`` (train ==
        # serve, continuous on one adjusted scale).
        #
        # Restatement is now triggered by an AUTHORITATIVE registered split (not
        # the old >45% magnitude heuristic, which silently missed sub-45%
        # splits), and routed through ``corporate_actions.apply`` with
        # registry-backed exactly-once idempotency: an action already marked
        # applied to this store is skipped, so re-applying to an already-
        # restated series (the daily snapshot loads the restated ArcticDB) is a
        # no-op rather than a double-apply.
        ticker_actions = actions_by_ticker.get(ticker)
        if ticker_actions:
            combined, applied = ca.apply(
                combined, ticker_actions,
                store=_RESTATE_STORE,
                registry=registry,
                run_id=f"apply_daily_delta:{date_str}",
            )
            if any(
                r["status"] == "applied" and r["n_rows_adjusted"] > 0
                for r in applied
            ):
                split_tickers.add(ticker)

        price_data[ticker] = combined
        n_updated += 1

    log.info("Applied daily delta: %d tickers updated", n_updated)
    return price_data, split_tickers


@dataclass(frozen=True)
class ActionJumpAudit:
    """Result of :func:`audit_action_jumps` — residual jumps partitioned by
    whether a registered corporate action EXPLAINS them.

    ``missed`` (``{ticker: [(date, daily_return, action_id), ...]}``) is the
    BLOCKING class: a registered split that was left un-flattened (data#1298
    corruption). ``suspected`` (``{ticker: [(date, daily_return), ...]}``) is a
    large move with NO registered action — a legit move or polygon-missed
    action — WARN only, never blocking.
    """

    missed: dict[str, list[tuple[str, float, str]]]
    suspected: dict[str, list[tuple[str, float]]]


def _explaining_split(actions: list, jump_date: pd.Timestamp, ret: float):
    """Return the registered split action that EXPLAINS an un-flattened jump at
    ``jump_date`` (daily return ``ret``), or ``None``.

    A match requires BOTH (i) the action's ex_date sits at the jump (the
    un-flattened boundary appears at the first row on/after the ex_date) and
    (ii) the observed boundary ratio ``close[d]/close[d-1] = 1+ret`` matches the
    split factor within ``_AUDIT_FACTOR_REL_TOL`` — so it is the SPLIT, not a
    coincident legitimate move on an already-flattened boundary.
    """
    observed = 1.0 + float(ret)  # close[d] / close[d-1]
    for action in actions:
        try:
            ex = pd.Timestamp(action.ex_date).normalize()
        except Exception:  # noqa: BLE001 - malformed ex_date, skip candidate
            continue
        if abs((ex - jump_date).days) > _AUDIT_EX_DATE_WINDOW_DAYS:
            continue
        try:
            expected = ca.expected_factor(action)  # == split_from / split_to
        except Exception:  # noqa: BLE001 - non-split / missing ratio, skip
            continue
        if expected <= 0:
            continue
        if abs(observed - expected) <= _AUDIT_FACTOR_REL_TOL * expected:
            return action
    return None


def audit_action_jumps(
    price_data: dict[str, pd.DataFrame],
    registry,
    *,
    screen_threshold: float = _ACTION_JUMP_SCREEN_THRESHOLD,
) -> ActionJumpAudit:
    """Registry-aware data-quality post-condition over the materialized series.

    For every residual ``|daily move| > screen_threshold``, classify it:

      * **MISSED** — a registered split's ex_date sits at the jump AND the move
        matches its factor → the restatement of a KNOWN action was missed (the
        data#1298 corruption class). BLOCKING at the training-write chokepoint.
      * **SUSPECTED** — a large move with NO registered action explaining it (a
        legit ±33% earnings move, or a polygon-missed action). WARN only.

    The RAISE condition (``missed``) is registry-driven, NOT raw magnitude —
    which is exactly what lets a sub-45% registered split be caught without
    false-failing on a legitimate large move. ``screen_threshold`` is the
    diagnostic floor (low enough to surface sub-45% splits). ``registry`` may
    be ``None`` — then no action can explain anything and every residual is
    ``suspected``.
    """
    splits_by_ticker: dict[str, list] = {}
    if registry is not None:
        try:
            for action in registry.list_actions(types=["split"]):
                splits_by_ticker.setdefault(action.ticker, []).append(action)
        except Exception as exc:  # noqa: BLE001 - degrade to all-suspected
            log.warning(
                "audit_action_jumps: registry list_actions failed (%s) — "
                "treating all residuals as suspected", exc,
            )

    missed: dict[str, list[tuple[str, float, str]]] = {}
    suspected: dict[str, list[tuple[str, float]]] = {}
    for ticker, df in price_data.items():
        if df is None or df.empty or "Close" not in df.columns:
            continue
        returns = df["Close"].pct_change().dropna()
        hits = returns[returns.abs() > screen_threshold]
        if hits.empty:
            continue
        ticker_actions = splits_by_ticker.get(ticker, [])
        for idx, val in hits.items():
            jump_date = pd.Timestamp(idx).normalize()
            date_str = jump_date.strftime("%Y-%m-%d")
            action = _explaining_split(ticker_actions, jump_date, float(val))
            if action is not None:
                missed.setdefault(ticker, []).append(
                    (date_str, float(val), action.action_id)
                )
            else:
                suspected.setdefault(ticker, []).append((date_str, float(val)))
    return ActionJumpAudit(missed=missed, suspected=suspected)


_MACRO_SLIM_KEYS = {
    "SPY": "SPY",
    "VIX": "VIX",     # stored as VIX, yfinance ticker is ^VIX
    "VIX3M": "VIX3M", # stored as VIX3M, yfinance ticker is ^VIX3M
    "TNX": "TNX",     # stored as TNX, yfinance ticker is ^TNX
    "IRX": "IRX",
    "GLD": "GLD",
    "USO": "USO",
    "HYOAS": "HYOAS", # config#939 — credit spreads; FRED-only index ticker
}


def _extract_macro(
    price_data: dict[str, pd.DataFrame],
    slim_data: dict[str, pd.DataFrame],
) -> dict[str, pd.Series]:
    """
    Extract macro series (SPY, VIX, TNX, IRX, GLD, USO, VIX3M) and sector ETFs
    from the price data dict. Trusts upstream DailyData for freshness.
    """
    macro: dict[str, pd.Series] = {}

    for key, stem in _MACRO_SLIM_KEYS.items():
        source = price_data.get(stem) if stem in price_data else slim_data.get(stem)
        if source is not None and "Close" in source.columns:
            macro[key] = source["Close"].dropna()

    # Sector ETFs (XL*) + sub-sector benchmark ETFs (config#934: SMH/IGV/…,
    # collectors/prices.py::_SUB_SECTOR_ETFS — the "XL" prefix test can't
    # catch them, alpha-engine-config-I7569). Both are collected daily
    # alongside the stock universe (excluded from per-ticker feature
    # computation via _SKIP_TICKERS) but were never surfaced into `macro`,
    # so `sub_sector_etf_series = macro.get(sub_sector_etf_sym)` resolved to
    # None for every ticker and sub_sector_vs_benchmark_* neutral-defaulted
    # for the whole universe.
    for stem, df in slim_data.items():
        if (stem.startswith("XL") or stem in _SUB_SECTOR_ETFS) and "Close" in df.columns:
            source = price_data.get(stem) if stem in price_data else df
            macro[stem] = source["Close"].dropna()

    return macro


def _load_price_source(s3, bucket: str) -> dict | None:
    """The ~full-universe price+macro symbol set from ArcticDB.

    Wave-4 terminal state (predictor/price_cache_slim deleted). This feeds
    the ENTIRE feature-compute pipeline (price_data) AND _extract_macro.
    The set is the union of two ArcticDB libraries — the slim cache that
    formerly carried them in one flat parquet dict no longer exists:

      - universe lib  -> equities + SPY      (load_universe_ohlcv)
      - macro lib     -> VIX/VIX3M/TNX/IRX/GLD/USO + XL* sector ETFs
                         (load_macro_series; XL* discovered via
                         open_macro_lib().list_symbols())

    The 5/23 parity observation (WAVE4_PARITY_METRIC compute) confirmed
    slim<->ArcticDB equivalence over the overlap before the slim fallback
    + dual-read were removed here.

    Returns None if the ArcticDB read fails (caller then returns empty —
    the existing no-data contract; matches the pre-Wave-4 behaviour when
    the single price source was unavailable). ``s3`` is retained in the
    signature for caller compatibility but is no longer used.
    """
    try:
        # lookback_days=_ARCTICDB_LOOKBACK_DAYS (alpha-engine-config-I7572):
        # the library default (730 calendar days, ~504 trading days) reads
        # fewer rows than _FEATURE_WARMUP_ROWS needs, starving the
        # factor-momentum second pass of warmup on every daily run — see
        # that constant's definition for the measured evidence.
        prices = load_universe_ohlcv(
            bucket, lookback_days=_ARCTICDB_LOOKBACK_DAYS,
        )  # equities + SPY
        macro_syms = set(_MACRO_SLIM_KEYS.values())
        try:
            mlib = open_macro_lib(bucket)
            macro_syms |= {
                sym for sym in mlib.list_symbols() if sym.startswith("XL")
            }
        except Exception as exc:  # noqa: BLE001 - XL* discovery best-effort
            log.warning("macro-lib symbol listing failed: %s", exc)
        macro_frames = load_macro_series(
            bucket, macro_syms, lookback_days=_ARCTICDB_LOOKBACK_DAYS,
        )
        return {**prices, **macro_frames} or None
    except Exception as exc:  # noqa: BLE001 - return empty, don't run blind
        log.warning("ArcticDB universe/macro read failed: %s", exc)
        return None


def _load_prices_and_macro(
    s3, bucket: str, date_str: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.Series]]:
    """
    Load price data and macro series — ArcticDB primary, slim fallback
    (see _load_price_source) + daily delta.

    Trusts upstream data quality — DailyData collects fresh prices,
    Saturday DataPhase1 handles splits during full price refresh.
    No yfinance calls; no external API dependencies.
    """
    source = _load_price_source(s3, bucket)
    if not source:
        return {}, {}

    price_data = dict(source)
    registry = _build_registry(s3, bucket)
    price_data, _split_tickers = _apply_daily_delta(
        s3, bucket, date_str, price_data, registry=registry,
    )

    # Inference-side post-condition: LOUD-BUT-LOGGED, never raises here. The
    # snapshot must not silently halt inference on a residual, and the BLOCKING
    # gate is the backfill training-write chokepoint (a residual here is a
    # known-issue signal, not a corruption of the written snapshot per se).
    if registry is not None:
        audit = audit_action_jumps(price_data, registry)
        if audit.missed:
            log.error(
                "feature-snapshot load: %d ticker(s) carry an un-flattened "
                "KNOWN registered split (data#1298) — %s",
                len(audit.missed),
                {t: audit.missed[t] for t in sorted(audit.missed)[:20]},
            )
        if audit.suspected:
            log.warning(
                "feature-snapshot load: %d ticker(s) carry a suspected large "
                "move with no registered action (legit move / polygon-missed) "
                "— %s",
                len(audit.suspected),
                {t: audit.suspected[t] for t in sorted(audit.suspected)[:20]},
            )

    macro = _extract_macro(price_data, source)

    return price_data, macro


def _load_cached_fundamentals(s3, bucket: str, date_str: str) -> dict[str, dict]:
    """Load cached fundamental data from S3 (written by prior inference)."""
    # Try exact date, then scan for most recent
    for key in [
        f"archive/fundamentals/{date_str}.json",
    ]:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read())
            log.info("Loaded cached fundamentals from s3://%s/%s (%d tickers)", bucket, key, len(data))
            return data
        except Exception:
            # CARVE-OUT (alpha-engine-config-I10226): (a) failure mode
            # swallowed — the exact-date S3 key missing or unreadable. (c)
            # recording surface — none needed at THIS step because it is a
            # documented in-band fallback cascade: control falls through to
            # the "Scan for most recent fundamentals file" block below, whose
            # own failure IS recorded loud (`log.warning` at line ~838), and
            # whose total-absence outcome is ALSO recorded (`log.info` at
            # line ~840). This is a read-side cache lookup, not a writer —
            # nothing is persisted here, so no data is silently corrupted.
            # See `.debug-swallow-allowlist.yaml`.
            pass

    # Scan for most recent fundamentals file
    try:
        resp = s3.list_objects_v2(
            Bucket=bucket, Prefix="archive/fundamentals/", MaxKeys=100,
        )
        keys = sorted(
            [c["Key"] for c in resp.get("Contents", []) if c["Key"].endswith(".json")],
            reverse=True,
        )
        if keys:
            obj = s3.get_object(Bucket=bucket, Key=keys[0])
            data = json.loads(obj["Body"].read())
            log.info("Loaded cached fundamentals from s3://%s/%s (%d tickers)", bucket, keys[0], len(data))
            return data
    except Exception as exc:
        log.warning("Failed to scan for cached fundamentals: %s", exc)

    log.info("No cached fundamentals found — fundamental features will use defaults")
    return {}


class AlternativeCoverageError(RuntimeError):
    """DataPhase2 published alternative data that this reader could not read.

    Raised only for the unambiguous case: the manifest states N>0 tickers were
    written and we loaded ZERO. Every alternative feature would silently become
    0.0 for the whole universe, which is indistinguishable from a genuine zero
    (see ``compute_and_write``'s ``val ... else 0.0``) — a red run is the honest
    outcome. A PARTIAL shortfall is logged at ERROR rather than raised, so the
    daily append still lands on a provider hiccup.
    """


def _alt_entry_from_payload(ticker_data: dict) -> dict:
    """Project one raw per-ticker alt payload onto the feature-store shape.

    ``or {}`` rather than ``.get(k, {})`` throughout: a sub-section that is
    present-but-null (a provider returning ``null`` for a section it could not
    fill) would otherwise raise ``AttributeError`` on the nested ``.get`` and,
    before this was extracted, be swallowed by a bare ``except Exception: pass``
    — dropping that ticker's alternative data entirely and invisibly.
    """
    eps = ticker_data.get("eps_revision") or {}
    consensus = ticker_data.get("analyst_consensus") or {}
    options = ticker_data.get("options_flow") or {}
    # Neither ``eps_revision.surprise_pct`` nor ``analyst_consensus.surprise_pct``
    # exist in the producer's payload shape — verified live against
    # s3://alpha-engine-research/market_data/weekly/2026-08-14/alternative/AAPL.json
    # (alpha-engine-config-I7569): the real data is
    # ``analyst_consensus.earnings_surprises``, a list of per-quarter
    # {date, actual, estimated, surprise_pct} dicts ordered most-recent-first.
    # The two direct-key lookups above always missed, so ``surprise`` was
    # always None and every ticker silently fell to the literal 0.0 default.
    surprise = eps.get("surprise_pct")
    if surprise is None:
        surprises = consensus.get("earnings_surprises") or []
        surprise = surprises[0].get("surprise_pct") if surprises else 0.0
    # registry.py documents earnings_surprise_pct as "decimal pct (_pct
    # suffix)" (e.g. 0.01 for 1%), but the producer's surprise_pct is a
    # percent-point number (AAPL 2026-06-30: actual=1.91 vs
    # estimated=1.9271 -> -0.887% -> emitted as -0.8873, not -0.008873).
    # Convert here so the feature matches its declared unit convention.
    if surprise is not None:
        surprise = surprise / 100.0
    return {
        "earnings": {
            "surprise_pct": surprise,
            "days_since_earnings": eps.get("days_since_earnings", 0.0),
        },
        "revisions": {
            "eps_revision_4w": eps.get("revision_4w", 0.0),
            "revision_streak": eps.get("streak", 0),
        },
        "options": {
            "put_call_ratio": options.get("put_call_ratio"),
            "iv_rank": options.get("iv_rank"),
            "atm_iv": options.get("expected_move_pct"),
        },
    }


def _load_cached_alternative(s3, bucket: str) -> dict[str, dict]:
    """Load cached alternative data from the most recent DataPhase2 output.

    Reads EVERY object under the partition, and reconciles what it loaded
    against the count DataPhase2 recorded in ``manifest.json``.

    Why the reconciliation exists (alpha-engine-config-I5811): this function
    used to call ``list_objects_v2`` ONCE with ``MaxKeys=200`` and no
    pagination, so it silently returned at most the alphabetically-first 200
    tickers of a partition holding up to 903. Every ticker past the cap got
    ``alt_data.get(ticker, {})`` -> ``{}`` -> ``0.0`` for all seven registered
    ``alternative``-family features (``earnings_surprise_pct``,
    ``days_since_earnings``, ``eps_revision_4w``, ``revision_streak``,
    ``put_call_ratio``, ``iv_rank``, ``iv_vs_rv`` — see ``features/registry.py``
    ``CATALOG``), for every one of the five feature-store writers that call
    this. A ceiling with no counterpart on the write side reports success at
    any universe size, so nothing failed and nothing said so; the population
    that kept real values was decided by the alphabet.
    """
    try:
        # Find latest weekly date
        obj = s3.get_object(Bucket=bucket, Key="market_data/latest_weekly.json")
        latest = json.loads(obj["Body"].read())
        latest_date = latest.get("date", "")
        prefix = f"market_data/weekly/{latest_date}/alternative/"

        # Paginate. The producer writes one object per ticker; a single
        # list_objects_v2 page is 1000 keys and the universe is ~903 today,
        # so an unpaginated read is latently wrong even without the MaxKeys
        # cap that made it actively wrong.
        keys: list[str] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith("manifest.json") or not key.endswith(".json"):
                    continue
                keys.append(key)

        alt_data: dict[str, dict] = {}
        unreadable: list[str] = []
        for key in keys:
            ticker = key.split("/")[-1].replace(".json", "")
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                alt_data[ticker] = _alt_entry_from_payload(json.loads(obj["Body"].read()))
            except Exception as exc:  # noqa: BLE001 — recorded, then reported below
                unreadable.append(f"{ticker}: {type(exc).__name__}")

        if unreadable:
            # Was a bare `except Exception: pass`. A per-ticker read failure is
            # a silently-zeroed feature row, so it is named rather than dropped.
            log.error(
                "Alternative data: %d of %d objects unreadable under %s — those "
                "tickers get 0.0 for every alternative feature. First 10: %s",
                len(unreadable), len(keys), prefix, unreadable[:10],
            )

        expected = _expected_alternative_count(s3, bucket, prefix)
        if expected is None:
            log.warning(
                "Alternative data: no readable manifest at %smanifest.json — "
                "loaded %d tickers with no producer count to reconcile against, "
                "so an under-read cannot be detected this run.",
                prefix, len(alt_data),
            )
        elif len(alt_data) < expected:
            if not alt_data:
                raise AlternativeCoverageError(
                    f"Alternative data: manifest at {prefix}manifest.json reports "
                    f"{expected} tickers written for {latest_date}, loaded ZERO. "
                    "Every alternative feature would be 0.0 for the whole universe "
                    "and indistinguishable from a genuine zero."
                )
            log.error(
                "Alternative data coverage SHORTFALL for %s: loaded %d of %d "
                "tickers the producer recorded (%.1f%%). The %d missing tickers "
                "get 0.0 for every alternative feature, which is not "
                "distinguishable downstream from a real zero.",
                latest_date, len(alt_data), expected,
                100.0 * len(alt_data) / expected, expected - len(alt_data),
            )
        else:
            log.info(
                "Loaded cached alternative data for %d tickers from %s "
                "(manifest recorded %d — full coverage)",
                len(alt_data), latest_date, expected,
            )
        return alt_data

    except AlternativeCoverageError:
        raise
    except Exception as exc:
        log.warning("No cached alternative data loaded — alternative features will use defaults: %s", exc)
        return {}


def _expected_alternative_count(s3, bucket: str, prefix: str) -> int | None:
    """``tickers_succeeded`` from DataPhase2's manifest, or None if unreadable.

    The producer writes the manifest BEFORE its own quality gate can raise, so
    it is present even on a degraded Phase 2 — which is exactly the run where a
    reader most needs to know how many rows it should have seen.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=f"{prefix}manifest.json")
        manifest = json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001 — absence is reported by the caller
        log.debug("Alternative manifest unreadable at %smanifest.json: %s", prefix, exc)
        return None
    value = manifest.get("tickers_succeeded")
    return int(value) if isinstance(value, (int, float)) else None


# ── Main computation ─────────────────────────────────────────────────────────

def compute_and_write(
    date_str: str,
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
    zero_variance_fatal: bool = True,
) -> dict:
    """
    Compute all 53 features for the full universe and write to S3.

    Returns summary dict with counts and timing. When the zero-variance
    postflight finds offending columns, the returned dict carries them under
    ``zero_variance_columns`` regardless of which mode ran — the caller decides
    what that means for ITS pipeline.

    ``zero_variance_fatal`` (alpha-engine-config-I7572) selects between the two
    correct answers to "a feature column is a cross-sectional constant":

    * ``True`` (default — backfill, weekly, any offline recompute): raise
      BEFORE the snapshot is written. Nothing downstream is waiting on the
      artifact, so refusing to produce a known-defective one is right.

    * ``False`` (the EOD daily path): write the snapshot, THEN report. On
      2026-08-17 the fatal form cost far more than the defect it caught. Eight
      columns were constants; the raise sits before ``write_feature_snapshot``,
      so the OTHER ~200 columns of that day's snapshot were destroyed too, the
      collector exited 1, the SF's data-spot workload failed, and
      ``LaunchPostMarketArcticAppendSpot`` was therefore never reached — so the
      day's SPY close never landed in ArcticDB, the freshness sentinel stayed
      on the prior trading day, ``EODReconcile`` was skipped, and the self-heal
      loop re-ran the same deterministic failure twice before paging
      ``HealNonConvergent``. A defect in eight analytics columns took the
      price-append and reconcile path down with it.

      The guard is NOT weakened here and no column is exempted: it runs over
      exactly the same columns and its verdict is reported at ERROR, carried in
      the summary, and surfaced by the caller. What changes is only what the
      verdict is allowed to destroy.
    """
    import boto3

    s3 = boto3.client("s3")
    t0 = time.time()

    # ── 1. Load data ─────────────────────────────────────────────────────────
    price_data, macro = _load_prices_and_macro(s3, bucket, date_str)
    if not price_data:
        log.error("No price data loaded — cannot compute features")
        return {"status": "error", "error": "no_price_data"}

    sector_map = _load_sector_map(s3, bucket)
    sub_sector_map = _load_sub_sector_etf_map(s3, bucket)
    fundamentals = _load_cached_fundamentals(s3, bucket, date_str)
    alt_data = _load_cached_alternative(s3, bucket)

    t_load = time.time() - t0
    log.info(
        "Data loaded in %.1fs: %d tickers, %d macro series, %d sector mappings, "
        "%d sub-sector mappings, %d fundamentals, %d alt data",
        t_load, len(price_data), len(macro), len(sector_map),
        len(sub_sector_map), len(fundamentals), len(alt_data),
    )

    # Trim price DataFrames to the last _FEATURE_WARMUP_ROWS rows before the
    # compute loop — see the constant's definition above for the derived
    # minimum (alpha-engine-config-I7539). Trimming here reduces peak RSS on
    # t3.micro by ~40%+ vs holding the full 2y slim cache in memory during
    # feature computation.
    for _t in list(price_data.keys()):
        df = price_data[_t]
        if len(df) > _FEATURE_WARMUP_ROWS:
            price_data[_t] = df.iloc[-_FEATURE_WARMUP_ROWS:]

    # ── 2. Compute features for each ticker ──────────────────────────────────
    store_rows: list[dict] = []
    n_ok = 0
    n_skip = 0
    n_err = 0

    # Filter to stock tickers only
    universe_tickers = [
        t for t in price_data
        if t not in _SKIP_TICKERS
        and not _is_sector_etf(t)
        and price_data[t] is not None
        and len(price_data[t]) >= MIN_ROWS_FOR_FEATURES
    ]

    log.info("Computing features for %d tickers...", len(universe_tickers))

    # Extract macro series once
    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")
    hyoas_series = macro.get("HYOAS")

    # alpha-engine-config-I7539: factor_momentum_ratio (W2.3, Gupta-Kelly) is a
    # cross-sectional-time-series column — it needs the WHOLE universe panel's
    # (date, close, loading) history at once (compute_factor_momentum_feature),
    # not a single ticker's latest row. builders/backfill.py runs this as a
    # second pass over ArcticDB, but nothing ran the equivalent second pass on
    # this S3 daily snapshot path, so the FEATURES-loop fallback below
    # (`row[f] = ... else 0.0`) silently wrote a universe-wide constant 0.0
    # every day. Collect the slim (date, close, *loading) frame per ticker
    # here — while featured_df is already in hand — so the second pass below
    # can rebuild the long panel without re-computing or re-loading anything.
    fm_frames: dict[str, pd.DataFrame] = {}

    for ticker in universe_tickers:
        try:
            df = price_data.pop(ticker)  # release as we go to avoid holding all 900 DFs
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None
            # Sub-sector benchmark ETF (config#934), resolved the same way as
            # the sector ETF above — mirrors builders/daily_append.py's
            # ArcticDB go-forward path (alpha-engine-config-I7569).
            sub_sector_etf_sym = sub_sector_map.get(ticker)
            sub_sector_etf_series = (
                macro.get(sub_sector_etf_sym) if sub_sector_etf_sym else None
            )

            # Get alt data for this ticker (if available)
            ticker_alt = alt_data.get(ticker, {})
            earnings_data = ticker_alt.get("earnings")
            revision_data = ticker_alt.get("revisions")
            options_data = ticker_alt.get("options")
            fundamental_data = fundamentals.get(ticker)

            featured_df = compute_features(
                df,
                spy_series=spy_series,
                vix_series=vix_series,
                sector_etf_series=sector_etf_series,
                sub_sector_etf_series=sub_sector_etf_series,
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                hyoas_series=hyoas_series,
                earnings_data=earnings_data,
                revision_data=revision_data,
                options_data=options_data,
                fundamental_data=fundamental_data,
            )

            if featured_df.empty:
                n_skip += 1
                continue

            _fm_loading_cols = [c for c in DEFAULT_FACTOR_LOADINGS if c in featured_df.columns]
            if _fm_loading_cols:
                _fm_slim = featured_df[["Close", *_fm_loading_cols]].rename(columns={"Close": "close"})
                _fm_slim = _fm_slim.reset_index(names="date")
                _fm_slim.insert(0, "ticker", ticker)
                fm_frames[ticker] = _fm_slim

            latest = featured_df.iloc[-1]
            row = {"ticker": ticker}
            for f in FEATURES:
                val = latest[f] if f in latest.index else 0.0
                if pd.notna(val):
                    row[f] = float(val)
                elif f in ALL_NULL_EXPECTED:
                    # alpha-engine-config-I7572: a generic "not yet known" ->
                    # 0.0 fallback silently laundered vwap_divergence_pct's
                    # legitimate NaN (today's VWAP isn't known until the
                    # NEXT trading day's morning enrichment pass — see
                    # postflight.ALL_NULL_EXPECTED) into a fabricated
                    # universe-wide constant zero, every day. 0.0 is a LEGAL
                    # divergence reading, so preserve NaN — the registry's
                    # documented contract — instead of manufacturing one.
                    row[f] = float("nan")
                else:
                    row[f] = 0.0
            store_rows.append(row)
            n_ok += 1

        except Exception as exc:
            log.warning("Feature computation failed for %s: %s", ticker, exc)
            n_err += 1
            price_data.pop(ticker, None)  # still release on error path

    t_compute = time.time() - t0 - t_load
    log.info(
        "Feature computation complete in %.1fs: %d OK, %d skipped, %d errors "
        "(of %d universe tickers)",
        t_compute, n_ok, n_skip, n_err, len(universe_tickers),
    )

    if not store_rows:
        raise RuntimeError(
            "Feature store compute produced zero features — nothing to write"
        )

    if universe_tickers and n_err / len(universe_tickers) > 0.05:
        raise RuntimeError(
            f"Feature computation error rate {n_err / len(universe_tickers):.1%} exceeds 5% threshold "
            f"(n_ok={n_ok} n_err={n_err} n_skip={n_skip} of {len(universe_tickers)})"
        )

    # ── 3. Write to S3 ───────────────────────────────────────────────────────
    features_df = pd.DataFrame(store_rows)

    # alpha-engine-config-I7539: factor_momentum_ratio second pass. Rebuild the
    # long panel from the slim per-ticker frames collected in the compute loop
    # above and run the SAME cross-sectional-time-series construction backfill
    # uses (features.factor_momentum.materialize_factor_momentum), just against
    # the in-memory panel instead of a re-read of ArcticDB — this snapshot
    # already holds the whole universe's (date, close, loading) history for the
    # trimmed warmup window, which is all compute_factor_momentum_feature needs.
    # A failure here is caught and logged (log.exception, full traceback) so
    # it doesn't take down the per-ticker compute / zscore / private-pack
    # producers that already succeeded — but it deliberately does NOT
    # swallow the outcome: leaving factor_momentum_ratio at the FEATURES-loop
    # default (0.0) is caught downstream by the postflight zero-variance
    # guard below, which raises before anything is written. So the net
    # effect of a failure here is still a loud, pipeline-halting failure —
    # just with the stack trace preserved and the OTHER columns' failure
    # isolated from this one's.
    if fm_frames:
        try:
            _fm_panel = pd.concat(fm_frames.values(), ignore_index=True)
            _fm_signal = compute_factor_momentum_feature(_fm_panel)
            _fm_panel = _fm_panel.assign(factor_momentum_ratio=_fm_signal)
            _fm_latest = (
                _fm_panel.sort_values("date")
                .groupby("ticker", sort=False)["factor_momentum_ratio"]
                .last()
            )
            _fm_map = _fm_latest.dropna()
            n_fm = int(features_df["ticker"].map(_fm_map).notna().sum())
            # NOT `.fillna(0.0)` (alpha-engine-config-I7539). 0.0 is a LEGAL
            # factor-momentum reading, so back-filling it makes an uncomputed
            # ticker indistinguishable from one whose tilt really is zero —
            # and when the pass produced nothing at all, the whole column read
            # as a plausible universe-wide zero. Measured 2026-08-18: this
            # snapshot carried 0.0 for all 901 tickers while ArcticDB's daily
            # second pass (builders/daily_append.update_factor_momentum_latest)
            # held real values for the same date (AAPL -0.049, MSFT +0.069,
            # XOM -0.101). Two stores disagreeing about one registered column,
            # with the S3 side fabricating the disagreement.
            #
            # NaN is the honest state: absent, recoverable, and visible to the
            # postflight guard below, which now reports an entirely-empty
            # column as loudly as a constant one.
            features_df["factor_momentum_ratio"] = (
                features_df["ticker"].map(_fm_map).astype(float)
            )
            log.info(
                "Factor-momentum second pass (daily): %d/%d tickers got a non-NaN "
                "factor_momentum_ratio (window/skip warmup — the rest stay NaN, "
                "never a fabricated 0.0)",
                n_fm, len(features_df),
            )
        except Exception:
            log.exception(
                "Factor-momentum second pass failed — factor_momentum_ratio "
                "is left as the FEATURES-loop default for this snapshot and "
                "the postflight guard below reports it"
            )
    else:
        log.warning(
            "Factor-momentum second pass skipped: no per-ticker loading frames "
            "collected — factor_momentum_ratio will be the FEATURES-loop "
            "default (0.0) for every ticker"
        )

    # C.1 (optimizer-sota-upgrades-260526.md §C.1): append cross-sectional
    # factor-loading z-scores AFTER per-ticker compute, BEFORE write. These
    # are the columns of the factor-loading matrix B that workstream C.3
    # (alpha-engine executor) consumes to build Σ = B·F·Bᵀ + D. Winsorized
    # at ±3σ then standardized (Barra USE4 / AQR convention).
    features_df = apply_factor_zscores(features_df)

    # alpha-engine-config#1032: append private-pack alpha-bearing columns
    # AFTER the public per-ticker compute + cross-sectional zscores, BEFORE
    # write — the same extension point as apply_factor_zscores above. No-op
    # (features_df unchanged) unless NOUSERGON_PRIVATE_FEATURE_PACK is set;
    # every public/CI run takes this no-op path. See features/private_pack.py.
    features_df = apply_private_features(features_df)

    # alpha-engine-config-I7539: postflight zero-variance guard, run AFTER
    # every producer above (per-ticker compute, factor-momentum second pass,
    # cross-sectional zscores, private pack) has had its chance to fill a
    # column and BEFORE the snapshot is written. Macro-group columns are
    # excluded — they broadcast one value to the whole universe on a date BY
    # CONSTRUCTION (e.g. the VIX level), so zero cross-sectional variance
    # there is the expected shape, not a defect.
    _non_macro_features = [f for f in FEATURES if f not in GROUPS.get("macro", ())]
    # alpha-engine-config-I7572: ALL_NULL_EXPECTED (vwap_divergence_pct) is
    # all-NaN on the latest row EVERY daily run by pipeline design (see
    # postflight.ALL_NULL_EXPECTED) — union it into the exempt set so the
    # dead-column guard doesn't false-positive-degrade every run.
    # assert_no_dead_feature_columns shares one `exempt` set across both its
    # constant- and empty-column checks, so this also exempts
    # vwap_divergence_pct from the zero-variance check — harmless in
    # practice, since a column that is structurally all-NaN can never
    # accumulate the min_non_null floor that check requires to fire.
    _dead_column_exempt = ZERO_VARIANCE_EXEMPT | ALL_NULL_EXPECTED
    if zero_variance_fatal:
        assert_no_dead_feature_columns(
            features_df, _non_macro_features, exempt=_dead_column_exempt,
        )
        _zero_variance: dict[str, int] = {}
        _all_null: list[str] = []
    else:
        # Same columns, same predicate — only the consequence differs. See the
        # zero_variance_fatal note in this function's docstring.
        _zero_variance = find_zero_variance_columns(features_df, _non_macro_features)
        _all_null = [c for c in find_all_null_columns(features_df, _non_macro_features)
                     if c not in _dead_column_exempt]
        if _all_null:
            log.error(
                "EMPTY feature column(s) on the daily path: zero non-null values "
                "over the whole universe — a producer that ran and wrote nothing "
                "(alpha-engine-config-I7539). The snapshot IS still written and "
                "this run is reported DEGRADED, not failed, matching the "
                "zero-variance verdict's blast radius (I7572). Columns: %s",
                _all_null,
            )
        if _zero_variance:
            log.error(
                "ZERO-VARIANCE feature column(s) detected on the daily path: every "
                "non-null value is identical across the whole universe, so the column "
                "passes every null-coverage check and carries zero signal "
                "(alpha-engine-config-I7539/I7572). The snapshot IS still written — "
                "the other columns are good and downstream needs them — and this run "
                "is reported DEGRADED, not failed. Offending columns "
                "(column: non_null_count): %s",
                _zero_variance,
            )

    supplemental_written: dict[str, int] = {}
    if dry_run:
        log.info(
            "[dry-run] Would write feature snapshot: %d tickers, %d features, date=%s",
            len(features_df), len(FEATURES), date_str,
        )
        summary = {
            "groups": {
                g: len(features_df)
                for g in ["technical", "macro", "interaction", "alternative", "fundamental"]
            },
        }
    else:
        summary = write_feature_snapshot(
            date_str, features_df, bucket,
            prefix=FEATURE_STORE_PREFIX,
        )
        upload_registry(bucket, prefix=FEATURE_STORE_PREFIX)

        # Write schema version alongside snapshot for training consistency checks
        _schema_content = json.dumps({"features": FEATURES, "config": FEATURE_CFG}, sort_keys=True)
        _schema_hash = hashlib.sha256(_schema_content.encode()).hexdigest()[:12]
        _version_doc = {
            "schema_version": 1,
            "schema_hash": _schema_hash,
            "n_features": len(FEATURES),
            "features": FEATURES,
            "date": date_str,
        }
        try:
            import boto3 as _b3_ver
            _b3_ver.client("s3").put_object(
                Bucket=bucket,
                Key=f"{FEATURE_STORE_PREFIX}{date_str}/schema_version.json",
                Body=json.dumps(_version_doc, indent=2),
                ContentType="application/json",
            )
        except Exception as _ver_exc:
            # Schema version is metadata consumed by downstream drift detection.
            # A failure here doesn't corrupt the features themselves, so we
            # don't halt the pipeline — but we surface it as WARNING so the
            # drift-check can't silently run against stale metadata.
            log.warning("Schema version write failed (non-fatal): %s", _ver_exc)

        log.info(
            "Feature snapshot + registry written to s3://%s/%s%s/ (schema=%s)",
            bucket, FEATURE_STORE_PREFIX, date_str, _schema_hash,
        )

        # Metron-held/watchlisted tickers outside the S&P500+400 universe above
        # (metron-ops#177) — a SEPARATE, additive snapshot crucible-research's
        # factor_scoring.py optionally reads for Attractiveness coverage. Runs
        # strictly AFTER the core snapshot write above, and is swallowed
        # (logged WARNING, not raised): (a) failure mode swallowed is a fetch/
        # compute error for this display-only supplemental ticker set; (b) the
        # primary deliverable — the ML training/risk-model feature snapshot
        # Predictor and Executor depend on — is already durably written by this
        # point and must not be taken down by a Metron-coverage nice-to-have;
        # (c) recording surface is this log.warning, which the weekly SF's log
        # aggregation surfaces same as any other WARNING.
        try:
            supp_features_df, supp_sector_map = compute_metron_supplemental_features(
                bucket, s3, set(features_df["ticker"]), macro,
            )
            supplemental_written = write_metron_supplemental_snapshot(
                date_str, supp_features_df, supp_sector_map, bucket, s3_client=s3,
            )
        except Exception as supp_exc:
            log.warning("Metron supplemental factor-scoring compute failed (non-fatal): %s", supp_exc)

    t_total = time.time() - t0

    result = {
        # DEGRADED, not ok: the snapshot was written and every other column in
        # it is usable, but at least one column is a cross-sectional constant.
        # A distinct status rather than "ok" so no caller can read a run whose
        # feature store carries a dead column as a fully clean one
        # (alpha-engine-config-I7572).
        "status": "degraded" if (_zero_variance or _all_null) else "ok",
        "zero_variance_columns": _zero_variance,
        # Separate key, not folded into the one above: "constant" and "empty"
        # send the reader to different producer questions.
        "all_null_columns": _all_null,
        "date": date_str,
        "tickers_computed": n_ok,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "groups_written": summary,
        # alpha-engine-config-I10855: the swallowed metron-supplemental write's
        # own group->rows summary (features/writer.py::write_feature_snapshot's
        # return), empty when the write was skipped (dry_run) or swallowed
        # (compute/write failure) — the caller's only truthful source of
        # whether `features/metron_supplemental/` was actually written this run.
        "metron_supplemental_written": supplemental_written,
        "load_seconds": round(t_load, 1),
        "compute_seconds": round(t_compute, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
    }

    log.info("Feature store compute complete: %s", json.dumps(result, default=str))
    return result


# ── CLI entry point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compute and write feature store snapshots to S3",
    )
    parser.add_argument(
        # config#1014: default resolved below on the trading-day axis (not
        # calendar now()) so a Saturday run keys features/{Fri}/ not /{Sat}/.
        "--date", default=None,
        help="Target date (YYYY-MM-DD, default: last closed trading day)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute features but skip S3 write",
    )
    parser.add_argument(
        "--bucket", default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if args.date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        args.date = default_run_date()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = compute_and_write(
        date_str=args.date,
        bucket=args.bucket,
        dry_run=args.dry_run,
    )

    if result["status"] != "ok":
        log.error("Feature compute failed: %s", result.get("error"))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
