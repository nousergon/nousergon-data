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
from features.compute import (
    DEFAULT_BUCKET,
    _SKIP_TICKERS,
    _apply_daily_delta,
    _is_sector_etf,
    _load_parquet_from_s3,
    _load_sector_map,
    _load_cached_fundamentals,
    _load_cached_alternative,
)
from store.arctic_store import get_universe_lib, get_macro_lib

log = logging.getLogger(__name__)

# OHLCV columns to keep alongside features in ArcticDB.
# VWAP added 2026-04-17 (Phase 7 VWAP centralization). Backfill source
# (``predictor/price_cache/*.parquet``) is OHLCV only and predates polygon
# adoption, so historical rows have no source for true volume-weighted VWAP.
# Per the 2026-04-17 decision, we do NOT synthesize a ``(H+L+C)/3`` proxy —
# that misrepresents arithmetic typical price as VWAP. Historical rows get
# NaN VWAP; the column becomes populated from the first daily_append run
# against a polygon-sourced daily_closes parquet. See ROADMAP "VWAP
# centralization" for the full rationale.
OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume", "VWAP"]

# Per-row data-provenance column (mirrors builders/daily_append.py).
# Backfill sources rows from ``predictor/price_cache/*.parquet`` (10y
# yfinance-sourced) plus the daily_closes delta (mixed polygon / yfinance
# / fred per row). Pre-delta rows default to ``"yfinance"``; delta rows
# get whatever source the daily_closes parquet recorded. Closes the
# audit trail of "where did this row's value come from" at row
# granularity even after a full backfill rewrite.
PROVENANCE_COL = "source"


def _load_current_constituents(s3, bucket: str) -> set[str]:
    """Load the current S&P 500 / 400 constituents set via the
    ``market_data/latest_weekly.json`` pointer.

    Used by ``backfill`` to filter out tickers absent from the current
    investable universe before writing arctic rows. Mirrors the
    ``prune_delisted_tickers`` lookup so the two sites agree on what's
    "in the universe today".
    """
    pointer_obj = s3.get_object(Bucket=bucket, Key="market_data/latest_weekly.json")
    pointer = json.loads(pointer_obj["Body"].read())
    weekly_date = pointer["date"]
    prefix = pointer["s3_prefix"].rstrip("/")
    cons_obj = s3.get_object(Bucket=bucket, Key=f"{prefix}/constituents.json")
    payload = json.loads(cons_obj["Body"].read())
    tickers = payload.get("tickers")
    if not tickers:
        raise RuntimeError(
            f"constituents.json at s3://{bucket}/{prefix}/constituents.json "
            f"(weekly_date={weekly_date}) has no `tickers` field — refusing "
            f"to filter against an empty constituents set (would write zero "
            f"tickers to arctic universe)."
        )
    return set(tickers)


def _load_full_cache(s3, bucket: str, prefix: str = "predictor/price_cache/") -> dict[str, pd.DataFrame]:
    """Load all 10-year price cache parquets from S3 (concurrent)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        log.error("No parquets found in s3://%s/%s", bucket, prefix)
        return {}

    log.info("Downloading %d full cache parquets from s3://%s/%s ...", len(keys), bucket, prefix)

    price_data: dict[str, pd.DataFrame] = {}
    errors = 0

    def _download(key: str) -> tuple[str, pd.DataFrame | None]:
        ticker = key.split("/")[-1].replace(".parquet", "")
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
    macro_keys = {
        "SPY": "SPY", "VIX": "VIX", "VIX3M": "VIX3M",
        "TNX": "TNX", "IRX": "IRX", "GLD": "GLD", "USO": "USO",
    }
    macro: dict[str, pd.Series] = {}
    for key, stem in macro_keys.items():
        df = price_data.get(stem)
        if df is not None and "Close" in df.columns:
            macro[key] = df["Close"].dropna()

    # Sector ETFs
    for stem, df in price_data.items():
        if stem.startswith("XL") and len(stem) <= 4 and "Close" in df.columns:
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
    """
    import random as _rand

    macro_lib = get_macro_lib(bucket)
    universe_lib = get_universe_lib(bucket)

    regressions: list[str] = []

    for key, series in planned_macro.items():
        planned_last = _planned_last_date(series)
        existing_last = _existing_last_date(macro_lib, key)
        if planned_last is None or existing_last is None:
            continue
        if planned_last < existing_last:
            regressions.append(
                f"macro.{key}: planned={planned_last.date()} < existing={existing_last.date()}"
            )

    try:
        arctic_syms = set(universe_lib.list_symbols())
    except Exception as exc:
        raise RuntimeError(
            f"Backfill regression preflight: could not list ArcticDB universe symbols: {exc}"
        ) from exc

    candidates = sorted(
        t for t in planned_universe
        if t in arctic_syms and t not in _SKIP_TICKERS and not _is_sector_etf(t)
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
            f"if backfill proceeded. Source data (predictor/price_cache + daily_closes delta) "
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

    Returns:
        Summary dict with counts and timing.
    """
    s3 = boto3.client("s3")
    t0 = time.time()

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Load data ─────────────────────────────────────────────────────────
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
        price_data, _split_tickers = _apply_daily_delta(s3, bucket, today_str, price_data)

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
            constituents_set = _load_current_constituents(s3, bucket)
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
        if t not in _SKIP_TICKERS
        and not _is_sector_etf(t)
        and price_data[t] is not None
        and t in constituents_set
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
            log.error("Ticker %s not found in universe (no data or in skip list)", ticker_filter)
            return {"status": "error", "error": f"ticker_not_found: {ticker_filter}"}
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
                featured_df[PROVENANCE_COL] = "yfinance"

            keep_cols = (
                [c for c in OHLCV_COLS if c in featured_df.columns]
                + [PROVENANCE_COL]
                + [f for f in FEATURES if f in featured_df.columns]
            )
            symbol_df = featured_df[keep_cols].copy()

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
                universe_lib.write(ticker, symbol_df)

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
            macro_lib.write("features", macro_df)
            log.info("Wrote macro features: %d dates", len(macro_df))

        # Write raw macro series (SPY, VIX, etc.) for consumers that need them
        if not dry_run:
            for key in ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]:
                series = macro.get(key)
                if series is not None:
                    macro_series_df = pd.DataFrame({"Close": series}, index=series.index)
                    macro_series_df.index.name = "date"
                    macro_lib.write(key, macro_series_df)

            # Write sector ETFs
            for key in macro:
                if key.startswith("XL"):
                    sector_df = pd.DataFrame({"Close": macro[key]}, index=macro[key].index)
                    sector_df.index.name = "date"
                    macro_lib.write(key, sector_df)

    # ── 6. Snapshot ──────────────────────────────────────────────────────────
    if not dry_run:
        snapshot_name = f"backfill-{today_str}"
        try:
            universe_lib.snapshot(snapshot_name)
            log.info("Created snapshot: %s", snapshot_name)
        except Exception as exc:
            log.warning("Snapshot creation failed (non-fatal): %s", exc)

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
    }

    log.info("Backfill complete: %s", json.dumps(result, default=str))

    # ── 7. Validation (optional) ─────────────────────────────────────────────
    if validate and not dry_run:
        _run_validation(universe_lib, price_data, macro, sector_map, fundamentals, alt_data)

    return result


def _run_validation(
    universe_lib,
    price_data: dict[str, pd.DataFrame],
    macro: dict[str, pd.Series],
    sector_map: dict[str, str],
    fundamentals: dict[str, dict],
    alt_data: dict[str, dict],
):
    """Spot-check: recompute features inline for 10 tickers and compare to ArcticDB."""
    symbols = universe_lib.list_symbols()
    check_tickers = sorted(symbols)[:10]

    log.info("Running validation on %d tickers: %s", len(check_tickers), check_tickers)

    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")

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
