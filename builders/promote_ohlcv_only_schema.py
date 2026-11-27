"""
builders/promote_ohlcv_only_schema.py — one-shot schema migration.

Identifies ArcticDB universe symbols whose stored schema lacks feature
columns (OHLCV-only), recomputes features against the current feature
set, and rewrites the symbol so daily_append's ``universe_lib.update()``
stops rejecting today's row with a column-mismatch error.

Context:
    PR #76 (2026-04-21 morning) introduced a short-history write branch
    that persisted OHLCV-only rows for new-listing tickers (SNDK after
    the 2026 WDC spinoff, plus a handful of others). PR #78 (same day
    evening) unified the write path so every row now includes the full
    FEATURE schema — with NaN for features whose rolling-window warmup
    exceeds available history.

    The two regimes left some symbols in a transitional state: their
    stored rows are OHLCV-only, but ``daily_append`` now wants to write
    full-schema rows. ArcticDB's ``update()`` enforces schema match —
    mismatched updates surface as n_err and the symbol's today row never
    lands. 2026-04-21 daily_append post-#78 reported n_err=2.

    This migration reads each affected symbol, runs ``compute_features``
    on its OHLCV history (which now returns partial-feature rows per PR
    #78), and calls ``lib.write()`` to replace the symbol with the full
    schema. ``write()`` is authoritative for schema; ``update()`` is
    incremental. After this runs once, every symbol shares the canonical
    full-feature schema and daily_append's update() succeeds on every
    row.

    This is NOT a pipeline component — it runs once (or once per incident
    if a similar schema split ever recurs) and is safe to run multiple
    times (idempotent — symbols already at full schema are skipped).

Usage:
    python -m builders.promote_ohlcv_only_schema                   # migrate
    python -m builders.promote_ohlcv_only_schema --dry-run         # report only
    python -m builders.promote_ohlcv_only_schema --ticker SNDK     # single symbol
"""

# Legacy ArcticDB/SDK seams in this module are dynamically typed.
# ruff: noqa: ANN001, ANN201, ANN202, ANN204


from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

from features.feature_engineer import FEATURES, compute_features
from features.compute import (
    DEFAULT_BUCKET,
    _load_sector_map,
    _load_cached_fundamentals,
    _load_cached_alternative,
    assert_alternative_arctic_coverage,
)
from store.arctic_store import get_universe_lib, get_macro_lib

log = logging.getLogger(__name__)

OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume", "VWAP"]

# Macro keys daily_append pulls for feature compute. Mirrored here so
# migrated rows use the same inputs the live pipeline uses.
MACRO_KEYS = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]


def _needs_promotion(df: pd.DataFrame) -> bool:
    """True if the symbol is OHLCV-only (missing FEATURE columns).

    Any symbol whose schema is missing *any* canonical feature column is
    a promotion candidate — PR #78's unified write path will reject
    partial-schema symbols on the next daily_append run.
    """
    return any(f not in df.columns for f in FEATURES)


def _load_macro_series(macro_lib, bucket: str) -> dict[str, pd.Series]:
    """Load SPY/VIX/TNX/IRX/GLD/USO/VIX3M + every sector ETF as Close series.

    Matches daily_append's macro-loading semantics (loud fail on missing
    or schema-drifted series — these are critical feature inputs).
    """
    macro: dict[str, pd.Series] = {}
    for key in MACRO_KEYS:
        mdf = macro_lib.read(key).data
        if "Close" not in mdf.columns:
            raise RuntimeError(
                f"Macro series {key} has no Close column — ArcticDB schema drift"
            )
        macro[key] = mdf["Close"].dropna()

    # Sector ETFs — discover dynamically from the macro library so we
    # don't miss any that the live feature pipeline uses.
    for sym in macro_lib.list_symbols():
        if sym.startswith("XL"):
            mdf = macro_lib.read(sym).data
            if "Close" not in mdf.columns:
                raise RuntimeError(
                    f"Sector ETF {sym} has no Close column — ArcticDB schema drift"
                )
            macro[sym] = mdf["Close"].dropna()
    return macro


def _promote_symbol(
    ticker: str,
    universe_lib,
    macro: dict[str, pd.Series],
    sector_map: dict[str, str],
    fundamentals: dict,
    alt_data: dict,
    dry_run: bool,
) -> dict:
    """Migrate one symbol: recompute features against stored OHLCV and
    replace via ``lib.write()``.

    Returns a per-ticker status dict including which features resolved
    vs stayed NaN — the same observability daily_append's n_partial log
    emits.
    """
    hist = universe_lib.read(ticker).data

    ohlcv_cols = [c for c in OHLCV_COLS if c in hist.columns]
    if not ohlcv_cols:
        return {"ticker": ticker, "status": "skipped", "reason": "no OHLCV columns"}

    combined = hist[ohlcv_cols].copy()

    sector_etf_sym = sector_map.get(ticker)
    sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None
    ticker_alt = alt_data.get(ticker, {})

    featured = compute_features(
        combined,
        spy_series=macro.get("SPY"),
        vix_series=macro.get("VIX"),
        sector_etf_series=sector_etf_series,
        tnx_series=macro.get("TNX"),
        irx_series=macro.get("IRX"),
        gld_series=macro.get("GLD"),
        uso_series=macro.get("USO"),
        vix3m_series=macro.get("VIX3M"),
        earnings_data=ticker_alt.get("earnings"),
        revision_data=ticker_alt.get("revisions"),
        options_data=ticker_alt.get("options"),
        fundamental_data=fundamentals.get(ticker),
    )

    if featured.empty:
        return {"ticker": ticker, "status": "error", "reason": "compute_features empty"}

    keep_cols = [c for c in OHLCV_COLS if c in featured.columns] + \
                [f for f in FEATURES if f in featured.columns]
    out = featured[keep_cols].copy()

    # Match stored OHLCV dtypes per-column; features uniformly float32
    # (the training schema, and the default for new feature columns).
    for col in out.columns:
        if col in hist.columns:
            out[col] = out[col].astype(hist.dtypes[col])
        elif col in FEATURES:
            out[col] = out[col].astype("float32")
    out.index.name = "date"

    # Per-ticker feature coverage on the most recent row, for
    # observability parity with daily_append's n_partial log.
    last = out.iloc[-1]
    nan_features = [
        f for f in FEATURES
        if f in out.columns and isinstance(last[f], float) and np.isnan(last[f])
    ]

    if not dry_run:
        # write() (not update()) replaces the whole symbol with this
        # frame. That's the whole point: switching from OHLCV-only to
        # OHLCV + FEATURE schema requires a full rewrite.
        universe_lib.write(ticker, out)

    return {
        "ticker": ticker,
        "status": "promoted" if not dry_run else "would_promote",
        "rows": len(out),
        "features_ok": len(FEATURES) - len(nan_features),
        "features_nan": len(nan_features),
        "nan_feature_names": nan_features,
    }


def promote_schemas(
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
    ticker_filter: str | None = None,
) -> dict:
    """Scan every universe symbol and promote those with OHLCV-only schema."""
    t0 = time.time()
    universe_lib = get_universe_lib(bucket)
    macro_lib = get_macro_lib(bucket)

    symbols = universe_lib.list_symbols()
    if ticker_filter:
        symbols = [s for s in symbols if s == ticker_filter]
        if not symbols:
            raise RuntimeError(f"Ticker {ticker_filter} not in universe library")

    log.info("Scanning %d symbols for OHLCV-only schema ...", len(symbols))

    # Identify candidates first. A fast read per symbol; avoids loading
    # supporting data if nothing needs promotion.
    candidates: list[str] = []
    errors: list[dict] = []
    for sym in symbols:
        try:
            df = universe_lib.read(sym).data
        except Exception as exc:
            errors.append({"ticker": sym, "status": "error", "reason": f"read failed: {exc}"})
            continue
        if _needs_promotion(df):
            candidates.append(sym)

    log.info("Found %d symbols needing promotion: %s", len(candidates), candidates)
    if not candidates:
        return {
            "status": "ok",
            "scanned": len(symbols),
            "needs_promotion": 0,
            "promoted": [],
            "errors": errors,
            "total_seconds": round(time.time() - t0, 1),
            "dry_run": dry_run,
        }

    # Load supporting data. Must use a real boto3 S3 client — passing
    # None causes the loaders to silently default sector_map / fundamentals
    # / alternative to empty dicts, which would compute_features then
    # silently write 0 (not NaN) for every sector-relative / fundamental /
    # alternative feature. Silent zeroing of half the feature schema is
    # the exact failure mode feedback_no_silent_fails forbids.
    s3 = boto3.client("s3")
    sector_map = _load_sector_map(s3, bucket)
    fundamentals = _load_cached_fundamentals(
        s3, bucket,
        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    alt_data = _load_cached_alternative(s3, bucket)
    assert_alternative_arctic_coverage(alt_data)
    macro = _load_macro_series(macro_lib, bucket)

    results: list[dict] = []
    for sym in candidates:
        try:
            r = _promote_symbol(
                sym, universe_lib, macro,
                sector_map, fundamentals, alt_data,
                dry_run,
            )
            log.info(
                "%s ticker=%s rows=%s features_ok=%s features_nan=%s",
                r.get("status"), r["ticker"],
                r.get("rows"), r.get("features_ok"), r.get("features_nan"),
            )
            if r.get("features_nan", 0) > 0:
                log.warning(
                    "partial-features ticker=%s nan=%d/%d features=%s",
                    r["ticker"], r["features_nan"], len(FEATURES),
                    r.get("nan_feature_names", []),
                )
            results.append(r)
        except Exception as exc:
            log.warning("Failed to promote %s: %s", sym, exc)
            errors.append({"ticker": sym, "status": "error", "reason": str(exc)})

    return {
        "status": "ok",
        "scanned": len(symbols),
        "needs_promotion": len(candidates),
        "promoted": results,
        "errors": errors,
        "total_seconds": round(time.time() - t0, 1),
        "dry_run": dry_run,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Promote OHLCV-only universe symbols to the full FEATURE schema"
    )
    parser.add_argument(
        "--bucket", default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scan + report which symbols would be promoted; no writes",
    )
    parser.add_argument(
        "--ticker", default=None,
        help="Migrate a single ticker (for targeted retries)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = promote_schemas(
        bucket=args.bucket,
        dry_run=args.dry_run,
        ticker_filter=args.ticker,
    )

    import json
    print(json.dumps(result, indent=2, default=str))

    if result["errors"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
