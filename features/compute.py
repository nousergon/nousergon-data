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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import hashlib

from features.cross_sectional import apply_factor_zscores
from features.feature_engineer import FEATURES, FEATURE_CFG, MIN_ROWS_FOR_FEATURES, compute_features
from features.registry import upload_registry
from features.writer import write_feature_snapshot

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_BUCKET = "alpha-engine-research"
FEATURE_STORE_PREFIX = "features/"

# Large-move warning threshold (>45% daily return, e.g. stock splits, VIX spikes)
_SPLIT_RETURN_THRESHOLD = 0.45

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

# Rows to keep per ticker before feature computation. The longest rolling
# window is 252 rows (52w high/low); 280 provides a small buffer. Trimming
# before the compute loop cuts base memory ~44% vs the full 2y slim cache
# and lets each ticker's DataFrame be freed incrementally via pop().
_FEATURE_WARMUP_ROWS = 280

# Tickers that are macro/index series, not stocks
_SKIP_TICKERS = {
    "SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
    "^VIX", "^VIX3M", "^TNX", "^IRX",
}

# Macro/index symbols ALSO promoted to full `universe` members (full OHLCV +
# engineered features), in addition to their Close-only `macro`-library write.
# SPY became a held core position with the 2026-05-13 portfolio-optimizer
# cutover, so every held-position code path (eod_reconcile #181,
# morning-planner ATR #185) needs SPY's engineered features (atr_14_pct, ...)
# from `universe`. Members deliberately STAY in _SKIP_TICKERS so
# prune_delisted_tickers (SPY ∉ constituents.json → would otherwise be a
# prune candidate) and the daily_append coverage-diff accounting keep
# treating them as non-stock — _UNIVERSE_EXTRA only widens the universe-WRITE
# candidate set, nothing else. NOT a macro-lib teardown: VIX/TNX/IRX have no
# tradeable OHLCV and are never held, so they stay macro-only.
_UNIVERSE_EXTRA = frozenset({"SPY"})

# Sector ETFs to skip (not individual stocks)
_SECTOR_ETF_PREFIXES = {"XL"}


def _is_sector_etf(ticker: str) -> bool:
    return len(ticker) == 3 and ticker[:2] in _SECTOR_ETF_PREFIXES


# ── S3 data loading (self-contained, no predictor imports) ───────────────────

def _load_sector_map(s3, bucket: str) -> dict[str, str]:
    """Load ticker -> sector ETF mapping from S3."""
    try:
        obj = s3.get_object(Bucket=bucket, Key="data/sector_map.json")
        return json.loads(obj["Body"].read())
    except Exception as exc:
        log.warning("Failed to load sector_map.json: %s", exc)
        return {}


# Shared S3 parquet loaders live in store.parquet_loader so non-feature
# callers (e.g. collectors.macro's breadth computation) can reuse the same
# normalized DataFrame shape without importing private helpers. Slim cache
# (2y) is sufficient here — features only use the latest row and 2y gives
# enough warmup for every indicator.
from store.parquet_loader import load_parquet_from_s3 as _load_parquet_from_s3
from alpha_engine_lib.arcticdb import (
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
            ticker_rows[ticker].append({
                "date":   pd.Timestamp(d),
                "Open":   float(row.get("Open", np.nan)),
                "High":   float(row.get("High", np.nan)),
                "Low":    float(row.get("Low", np.nan)),
                "Close":  float(row.get("Close", np.nan)),
                "Volume": int(row.get("Volume", 0)),
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


def _apply_daily_delta(
    s3, bucket: str, date_str: str, price_data: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], set[str]]:
    """
    Append daily_closes delta rows to price DataFrames.

    Matches the predictor's ``load_price_data_from_cache`` behaviour:
    1. Loads ALL daily_closes files between the slim cache's last date and
       the target date (not just the target date's file).
    2. Uses ``duplicated(keep='last')`` so delta rows override cache rows
       on the same date.
    3. Detects splits (>45% single-day return) and returns those tickers
       for yfinance re-fetch.

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

    split_tickers: set[str] = set()
    n_updated = 0

    for ticker, slim_df in list(price_data.items()):
        base_cols = ["Open", "High", "Low", "Close", "Volume"]
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
                {k: r[k] for k in ["Open", "High", "Low", "Close", "Volume"]}
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

        # Split detection → full-history RESTATEMENT (data#1298).
        #
        # The ArcticDB universe is append-only + windowed: a split restates the
        # FULL adjusted history, but ArcticDB only ever got a recent window
        # patched, leaving a split-boundary discontinuity that corrupts
        # cross-boundary TRAINING features (verified on DD 2026-06-24). The
        # root-cause fix is to back-adjust the ENTIRE pre-split window by the
        # polygon-AUTHORITATIVE split factor here, at the point the full series
        # is materialized for the downstream ``lib.write`` — so the series
        # written to ArcticDB is continuous and on one adjusted scale
        # (train == serve). We restate at WRITE time (not read time) because the
        # backfill path already rewrites the full symbol; doing the restate in
        # the read/window path would have to re-derive factors on every query
        # and would not durably fix the stored discontinuity.
        #
        # yfinance ``auto_adjust`` LAGS a fresh split, so it cannot be trusted
        # for restatement — the factor must come from polygon (see split_factor).
        returns = combined["Close"].pct_change().dropna()
        if (returns.abs() > _SPLIT_RETURN_THRESHOLD).any():
            restated = _restate_split_window(ticker, combined)
            if restated is not None:
                combined = restated
                split_tickers.add(ticker)
            else:
                log.warning(
                    "Large move in %s (>45%% daily return) but no polygon split "
                    "factor available — using data as-is (audit guard should flag)",
                    ticker,
                )

        price_data[ticker] = combined
        n_updated += 1

    log.info("Applied daily delta: %d tickers updated", n_updated)
    return price_data, split_tickers


def _restate_split_window(
    ticker: str, df: pd.DataFrame, *, client=None,
) -> pd.DataFrame | None:
    """Back-adjust ``df``'s pre-split history by the polygon-authoritative
    cumulative split factor so the full series is split-consistent (data#1298).

    Returns the restated frame, or ``None`` when no polygon split factor is
    available (no events / polygon unreachable / events all predate the series)
    so the caller can fall back to "use as-is" + the audit guard. The actual
    factor math lives in :mod:`split_factor`; this wrapper isolates the
    (network) polygon lookup so it can be patched out in tests.
    """
    try:
        from split_factor import restate_series_for_splits, split_events

        events = split_events(ticker, client=client)
    except Exception as exc:  # network / auth / import — never hard-fail the run
        log.warning(
            "Split restatement for %s could not load polygon factors (%s) — "
            "leaving series un-restated; audit guard should flag it",
            ticker, exc,
        )
        return None
    if not events:
        return None
    restated = restate_series_for_splits(df, events)
    if restated is df:
        # No row actually changed (every split predates the series) — treat as
        # "no restatement available" so the >45%% move is still surfaced.
        return None
    log.info(
        "Restated %s full history on polygon split factor (%d event(s)) — "
        "ArcticDB write will be split-consistent (data#1298)",
        ticker, len(events),
    )
    return restated


def audit_split_jumps(
    price_data: dict[str, pd.DataFrame],
    *,
    threshold: float = _SPLIT_RETURN_THRESHOLD,
) -> dict[str, list[tuple[str, float]]]:
    """Data-quality invariant: scan each series for an un-restated split jump.

    Returns ``{ticker: [(date_str, daily_return), ...]}`` for every ticker whose
    Close series still contains a ``|daily return| > threshold`` move. A clean,
    fully-restated universe returns ``{}``. Surfacing this makes the data#1298
    bug class impossible to land SILENTLY — a residual discontinuity (the
    restate failed, or a new split slipped past) is now a visible invariant
    violation an operator/cron can alert on and auto-restate.
    """
    offenders: dict[str, list[tuple[str, float]]] = {}
    for ticker, df in price_data.items():
        if df is None or df.empty or "Close" not in df.columns:
            continue
        returns = df["Close"].pct_change().dropna()
        hits = returns[returns.abs() > threshold]
        if not hits.empty:
            offenders[ticker] = [
                (pd.Timestamp(idx).strftime("%Y-%m-%d"), float(val))
                for idx, val in hits.items()
            ]
    return offenders


_MACRO_SLIM_KEYS = {
    "SPY": "SPY",
    "VIX": "VIX",     # stored as VIX, yfinance ticker is ^VIX
    "VIX3M": "VIX3M", # stored as VIX3M, yfinance ticker is ^VIX3M
    "TNX": "TNX",     # stored as TNX, yfinance ticker is ^TNX
    "IRX": "IRX",
    "GLD": "GLD",
    "USO": "USO",
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

    # Sector ETFs
    for stem, df in slim_data.items():
        if stem.startswith("XL") and "Close" in df.columns:
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
        prices = load_universe_ohlcv(bucket)  # equities + SPY
        macro_syms = set(_MACRO_SLIM_KEYS.values())
        try:
            mlib = open_macro_lib(bucket)
            macro_syms |= {
                sym for sym in mlib.list_symbols() if sym.startswith("XL")
            }
        except Exception as exc:  # noqa: BLE001 - XL* discovery best-effort
            log.warning("macro-lib symbol listing failed: %s", exc)
        macro_frames = load_macro_series(bucket, macro_syms)
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
    price_data, _split_tickers = _apply_daily_delta(s3, bucket, date_str, price_data)
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


def _load_cached_alternative(s3, bucket: str) -> dict[str, dict]:
    """Load cached alternative data from the most recent DataPhase2 output."""
    try:
        # Find latest weekly date
        obj = s3.get_object(Bucket=bucket, Key="market_data/latest_weekly.json")
        latest = json.loads(obj["Body"].read())
        latest_date = latest.get("date", "")
        prefix = f"market_data/weekly/{latest_date}/alternative/"

        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=200)
        contents = resp.get("Contents", [])

        alt_data: dict[str, dict] = {}
        for item in contents:
            key = item["Key"]
            if key.endswith("manifest.json") or not key.endswith(".json"):
                continue
            ticker = key.split("/")[-1].replace(".json", "")
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                ticker_data = json.loads(obj["Body"].read())
                alt_data[ticker] = {
                    "earnings": {
                        "surprise_pct": ticker_data.get("eps_revision", {}).get("surprise_pct",
                                        ticker_data.get("analyst_consensus", {}).get("surprise_pct", 0.0)),
                        "days_since_earnings": ticker_data.get("eps_revision", {}).get("days_since_earnings", 0.0),
                    },
                    "revisions": {
                        "eps_revision_4w": ticker_data.get("eps_revision", {}).get("revision_4w", 0.0),
                        "revision_streak": ticker_data.get("eps_revision", {}).get("streak", 0),
                    },
                    "options": {
                        "put_call_ratio": ticker_data.get("options_flow", {}).get("put_call_ratio"),
                        "iv_rank": ticker_data.get("options_flow", {}).get("iv_rank"),
                        "atm_iv": ticker_data.get("options_flow", {}).get("expected_move_pct"),
                    },
                }
            except Exception:
                pass

        if alt_data:
            log.info("Loaded cached alternative data for %d tickers from %s", len(alt_data), latest_date)
        return alt_data

    except Exception as exc:
        log.warning("No cached alternative data loaded — alternative features will use defaults: %s", exc)
        return {}


# ── Main computation ─────────────────────────────────────────────────────────

def compute_and_write(
    date_str: str,
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
) -> dict:
    """
    Compute all 53 features for the full universe and write to S3.

    Returns summary dict with counts and timing.
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
    fundamentals = _load_cached_fundamentals(s3, bucket, date_str)
    alt_data = _load_cached_alternative(s3, bucket)

    t_load = time.time() - t0
    log.info(
        "Data loaded in %.1fs: %d tickers, %d macro series, %d sector mappings, "
        "%d fundamentals, %d alt data",
        t_load, len(price_data), len(macro), len(sector_map),
        len(fundamentals), len(alt_data),
    )

    # Trim price DataFrames to the last _FEATURE_WARMUP_ROWS rows before the
    # compute loop. The longest rolling window is 252 rows; keeping 280 is
    # sufficient. Trimming here reduces peak RSS on t3.micro by ~40%+ vs
    # holding the full 2y slim cache in memory during feature computation.
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

    for ticker in universe_tickers:
        try:
            df = price_data.pop(ticker)  # release as we go to avoid holding all 900 DFs
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None

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
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                earnings_data=earnings_data,
                revision_data=revision_data,
                options_data=options_data,
                fundamental_data=fundamental_data,
            )

            if featured_df.empty:
                n_skip += 1
                continue

            latest = featured_df.iloc[-1]
            row = {"ticker": ticker}
            for f in FEATURES:
                val = latest[f] if f in latest.index else 0.0
                row[f] = float(val) if pd.notna(val) else 0.0
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

    # C.1 (optimizer-sota-upgrades-260526.md §C.1): append cross-sectional
    # factor-loading z-scores AFTER per-ticker compute, BEFORE write. These
    # are the columns of the factor-loading matrix B that workstream C.3
    # (alpha-engine executor) consumes to build Σ = B·F·Bᵀ + D. Winsorized
    # at ±3σ then standardized (Barra USE4 / AQR convention).
    features_df = apply_factor_zscores(features_df)

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

    t_total = time.time() - t0

    result = {
        "status": "ok",
        "date": date_str,
        "tickers_computed": n_ok,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "groups_written": summary,
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
