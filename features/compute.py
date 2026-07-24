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
from dataclasses import dataclass

import corporate_actions as ca
from features.cross_sectional import apply_factor_zscores
from features.feature_engineer import FEATURES, FEATURE_CFG, MIN_ROWS_FOR_FEATURES, compute_features
from features.metron_supplemental import compute_metron_supplemental_features, write_metron_supplemental_snapshot
from features.private_pack import apply_private_features
from features.registry import upload_registry
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

# Rows to keep per ticker before feature computation. The longest rolling
# window is 252 rows (52w high/low); 280 provides a small buffer. Trimming
# before the compute loop cuts base memory ~44% vs the full 2y slim cache
# and lets each ticker's DataFrame be freed incrementally via pop().
_FEATURE_WARMUP_ROWS = 280

# Sub-sector benchmark ETFs (config#934) — SMH/IGV/XBI/PPH/XOP/KRE/ITA/GDX,
# the distinct non-XL* symbols in constituents.GICS_SUBINDUSTRY_TO_ETF. Like
# the XL* sector ETFs (excluded via _is_sector_etf) these are benchmark
# series, NOT stocks: they must NOT get full-universe feature computation and
# must NOT be flagged as constituents-churn stragglers by the coverage diff.
# The XL* prefix test can't catch them (SMH/IGV/… don't start with "XL"), so
# they are enumerated into _SKIP_TICKERS explicitly.
_SUB_SECTOR_ETFS = frozenset({"SMH", "IGV", "XBI", "PPH", "XOP", "KRE", "ITA", "GDX"})

# Tickers that are macro/index series, not stocks
_SKIP_TICKERS = {
    "SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
    "^VIX", "^VIX3M", "^TNX", "^IRX",
    *_SUB_SECTOR_ETFS,
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
    hyoas_series = macro.get("HYOAS")

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
                hyoas_series=hyoas_series,
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

    # alpha-engine-config#1032: append private-pack alpha-bearing columns
    # AFTER the public per-ticker compute + cross-sectional zscores, BEFORE
    # write — the same extension point as apply_factor_zscores above. No-op
    # (features_df unchanged) unless NOUSERGON_PRIVATE_FEATURE_PACK is set;
    # every public/CI run takes this no-op path. See features/private_pack.py.
    features_df = apply_private_features(features_df)

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
            write_metron_supplemental_snapshot(
                date_str, supp_features_df, supp_sector_map, bucket, s3_client=s3,
            )
        except Exception as supp_exc:
            log.warning("Metron supplemental factor-scoring compute failed (non-fatal): %s", supp_exc)

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
