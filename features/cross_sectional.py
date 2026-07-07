"""
features/cross_sectional.py — Cross-sectional factor-loading transforms.

C.1 of the optimizer-sota-upgrades-260526 arc (factor-risk decomposition).
The executor's risk decomposition Σ = B·F·Bᵀ + D (workstream C.3) consumes
a (N × K) factor-loading matrix B where columns are CROSS-SECTIONALLY
z-scored style factors (mean 0, std 1 across the universe at each date).
This module adds those columns to the feature-store panel and the ArcticDB
universe library after the per-ticker feature computation completes.

Convention: Barra USE4 / AQR / BlackRock Aladdin — winsorize at ±3σ then
re-standardize. The winsorization prevents a single ticker's outlier from
dominating the factor-return cross-sectional regression that produces F
(in workstream C.2, alpha-engine-predictor).

Plan: alpha-engine-docs/private/optimizer-sota-upgrades-260526.md §C.1
"""

from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Source column → emitted z-score column. Adding a factor here also requires:
#   1. Appending the *_zscore name to features/feature_engineer.FEATURES
#   2. Adding a FeatureEntry to features/registry.py::CATALOG
#   3. Documenting the row in features/SCHEMA.md §3
# The test_schema_contract suite enforces 1+2+3 parity.
#
# Selected for v1 institutional Barra-style factor set:
#   • momentum (short + medium horizon) — Barra's MOMENTUM family
#   • beta — Barra's BETA factor (market sensitivity)
#   • residual vol — Barra's RESVOL (idiosyncratic risk)
#   • realized vol — Barra's VOLATILITY (total risk; complements RESVOL)
#   • dist-from-52w-high — proximity-to-high / reversal-risk factor
#   • value (1/PE proxy) — Barra's VALUE family
#   • quality (ROE) — Barra's QUALITY / profitability factor
#   • size (log market cap) — Barra's SIZE factor (config#1142) — completes
#     the institutional Barra set; uses a log pre-transform (see
#     FACTOR_LOADING_TRANSFORMS) because SIZE is canonically z(log(mktcap)).
FACTOR_LOADING_SOURCES: dict[str, str] = {
    "momentum_20d":         "momentum_20d_zscore",
    "return_60d":           "return_60d_zscore",
    "beta_60d":             "beta_60d_zscore",
    "idio_vol_60d":         "idio_vol_60d_zscore",
    "realized_vol_63d":     "realized_vol_63d_zscore",
    "dist_from_52w_high":   "dist_from_52w_high_zscore",
    "pe_ratio":             "pe_ratio_zscore",
    "roe":                  "roe_zscore",
    "market_cap_raw":       "size_zscore",
}


def _log_positive(series: pd.Series) -> pd.Series:
    """np.log with a non-positive guard: x <= 0 (or non-finite) -> NaN.

    Barra SIZE = z-score of log(market cap). Raw market cap is fat-right-
    tailed, so the z-score is taken on the log scale. A 0.0 / negative /
    missing market cap (the collector's NEUTRAL default for a capless or
    uncovered ticker) maps to NaN here, which the cross-sectional z-score
    excludes — the ticker is dropped from the SIZE cross-section rather than
    being assigned a spurious extreme-small loading. Fail-soft, additive.
    """
    arr = series.astype(float)
    return pd.Series(
        np.where(arr > 0.0, np.log(arr.where(arr > 0.0)), np.nan),
        index=series.index,
    )


# Optional per-source PRE-TRANSFORM applied before winsorize+z-score.
# Default (source absent here) is identity. SIZE is the first user: it
# z-scores log(market_cap_raw), not raw market cap.
FACTOR_LOADING_TRANSFORMS: dict[str, "callable"] = {
    "market_cap_raw": _log_positive,
}

_WINSORIZE_SIGMA = 3.0


def factor_loading_columns() -> list[str]:
    """Emitted column names. Order matches FACTOR_LOADING_SOURCES dict
    iteration (insertion-ordered)."""
    return list(FACTOR_LOADING_SOURCES.values())


def _winsorize_and_zscore(series: pd.Series) -> pd.Series:
    """Winsorize at ±3σ then standardize to z-scores.

    Uses median + MAD-derived σ for the winsorization step (robust to
    outliers; standard Barra USE4 / AQR approach for fat-tailed financial
    data). A naive mean+std winsorization is itself contaminated by
    extreme outliers — a single 100σ value drags both μ₀ AND σ₀ and the
    clipped bound ends up far above the bulk of the distribution.

    Pipeline:
      1. compute median m and MAD = median(|x − m|)
      2. derive robust σ_r = 1.4826 · MAD (Fisher consistency for Gaussian)
      3. clip to [m − 3·σ_r, m + 3·σ_r]
      4. compute mean μ₁ + std σ₁ on the clipped values
      5. emit (clipped − μ₁) / σ₁

    Returns NaN-only series if the input has fewer than 2 finite values
    or σ_r == 0 OR σ₁ == 0 (degenerate distribution).

    References:
      - Hampel 1974 "The Influence Curve and its Role in Robust Estimation"
        (origin of the 1.4826 MAD-to-σ Fisher consistency constant)
      - Barra USE4 Methodology Notes §3 — z-score standardization with
        robust winsorization
    """
    finite = series[np.isfinite(series)]
    if len(finite) < 2:
        return pd.Series(np.nan, index=series.index)
    median = float(finite.median())
    mad = float((finite - median).abs().median())
    sigma_robust = 1.4826 * mad
    if sigma_robust <= 0.0:
        # MAD degeneracy: ≥50% of values are identical to the median.
        # No meaningful cross-section even though some values may differ.
        return pd.Series(np.nan, index=series.index)
    lower = median - _WINSORIZE_SIGMA * sigma_robust
    upper = median + _WINSORIZE_SIGMA * sigma_robust
    clipped = series.clip(lower=lower, upper=upper)
    clipped_finite = clipped[np.isfinite(clipped)]
    mu1 = float(clipped_finite.mean())
    sigma1 = float(clipped_finite.std(ddof=0))
    if sigma1 <= 0.0:
        return pd.Series(np.nan, index=series.index)
    return (clipped - mu1) / sigma1


def apply_factor_zscores(
    features_df: pd.DataFrame,
    sources: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Add *_zscore columns to a cross-sectional feature panel.

    ``features_df`` is one row per ticker for a single date — the panel
    assembled in features/compute.py after compute_features runs per
    ticker. This function appends the C.1 factor-loading z-scores using
    the cross-sectional distribution at this date.

    Missing source columns are tolerated (emitted z-score is all-NaN
    with a WARN log). This preserves partial-rollout compatibility: a
    feature store snapshot that pre-dates a given source column still
    gets the *_zscore column with NaN values, so downstream consumers
    don't fail on schema-shape mismatch.

    Returns a copy with the new columns appended.
    """
    if sources is None:
        sources = FACTOR_LOADING_SOURCES
    out = features_df.copy()
    for src, dst in sources.items():
        if src not in out.columns:
            log.warning(
                "Factor-loading source column %r missing from feature panel; "
                "emitting all-NaN for %r (downstream consumers tolerate NaN "
                "per partial-rollout contract)",
                src, dst,
            )
            out[dst] = np.nan
            continue
        col = out[src].astype(float)
        transform = FACTOR_LOADING_TRANSFORMS.get(src)
        if transform is not None:
            col = transform(col)
        out[dst] = _winsorize_and_zscore(col)
    return out


def factor_loading_source_columns() -> list[str]:
    """Raw per-ticker columns that feed the cross-sectional z-score pass."""
    return list(FACTOR_LOADING_SOURCES.keys())


def materialize_factor_loading_zscores(
    universe_lib,
    tickers,
    *,
    write: bool = True,
    canonical_fn=None,
) -> dict:
    """Second-pass library builder: materialize C.1 ``*_zscore`` loadings in ArcticDB.

    Factor-loading z-scores are cross-sectional — each date's values depend on
    the WHOLE universe's raw loadings at that date. Per-ticker streaming writes
    in ``builders/backfill.py`` / ``builders/daily_append.py`` cannot compute
    them inline, so this runs as a separate pass (mirrors
    ``factor_momentum.materialize_factor_momentum``):

      * Pass 1 — read back the slim ``(source loadings)`` panel per ticker,
        group by date, call :func:`apply_factor_zscores` per cross-section.
      * Pass 2 — read-modify-write the ``*_zscore`` columns per ticker.

    Best-effort per ticker on read/write failures; all-NaN z-score counts are
    logged LOUD. Never raises into the caller pipeline.
    """
    src_cols = factor_loading_source_columns()
    dst_cols = factor_loading_columns()

    frames: list[pd.DataFrame] = []
    read_fail = 0
    for t in tickers:
        try:
            df = universe_lib.read(t).data
        except Exception as exc:
            log.warning(
                "factor-loading-zscores: read failed for %s (skipped): %s", t, exc,
            )
            read_fail += 1
            continue
        if df is None or df.empty:
            continue
        sub = pd.DataFrame({"ticker": t, "date": df.index})
        for c in src_cols:
            if c in df.columns:
                # .to_numpy() — sub uses RangeIndex; df[c] is DatetimeIndex-aligned
                # and would otherwise assign all-NaN via index mismatch.
                sub[c] = df[c].astype(float).to_numpy()
        frames.append(sub.reset_index(drop=True))

    if not frames:
        log.warning(
            "factor-loading-zscores: no readable tickers (read_fail=%d) — "
            "nothing materialized",
            read_fail,
        )
        return {"status": "empty", "tickers_written": 0, "read_fail": read_fail}

    panel = pd.concat(frames, ignore_index=True)
    zscore_parts: list[pd.DataFrame] = []
    for date, grp in panel.groupby("date", sort=True):
        cross = grp.drop(columns=["date"]).copy()
        z = apply_factor_zscores(cross)
        part = z[["ticker", *dst_cols]].copy()
        part["date"] = date
        zscore_parts.append(part)

    zpanel = pd.concat(zscore_parts, ignore_index=True)

    n_written = 0
    n_all_nan = 0
    write_fail = 0
    n_tickers = zpanel["ticker"].nunique()
    for t, grp in zpanel.groupby("ticker", sort=False):
        z_by_date = grp.set_index("date")[dst_cols].astype(float)
        if not np.isfinite(z_by_date.to_numpy()).any():
            n_all_nan += 1
        if not write:
            continue
        try:
            df = universe_lib.read(t).data
            for col in dst_cols:
                df[col] = z_by_date[col].reindex(df.index).astype("float32")
            out = canonical_fn(df) if canonical_fn is not None else df
            universe_lib.write(t, out)
            n_written += 1
        except Exception as exc:
            log.warning(
                "factor-loading-zscores: write failed for %s (skipped): %s", t, exc,
            )
            write_fail += 1

    if n_all_nan:
        log.warning(
            "factor-loading-zscores: %d/%d tickers have all-NaN z-score loadings "
            "(missing source columns / degenerate cross-section → excluded from "
            "risk-model B matrix downstream, NOT a silent zero).",
            n_all_nan, n_tickers,
        )
    log.info(
        "factor-loading-zscores materialized: %d written, %d all-NaN, "
        "%d read-fail, %d write-fail (of %d panel tickers)",
        n_written, n_all_nan, read_fail, write_fail, n_tickers,
    )
    return {
        "status": "ok",
        "tickers_written": n_written,
        "tickers_all_nan": n_all_nan,
        "read_fail": read_fail,
        "write_fail": write_fail,
    }


def update_factor_loading_zscores_latest(
    universe_lib,
    tickers,
    as_of_ts,
    *,
    write: bool = True,
    canonical_fn=None,
) -> dict:
    """Daily go-forward update of C.1 ``*_zscore`` loadings (ArcticDB second pass).

    Runs AFTER ``builders/daily_append`` has written today's per-ticker raw
    loading columns so the cross-section is complete. Reads today's rows,
    applies :func:`apply_factor_zscores`, and updates ONLY ``as_of_ts`` via
    ``update_batch``. Best-effort — never raises into the daily pipeline.
    """
    from arcticdb.version_store.library import ReadRequest, UpdatePayload

    src_cols = factor_loading_source_columns()
    dst_cols = factor_loading_columns()
    as_of_ts = pd.Timestamp(as_of_ts)
    tickers = list(tickers)

    rows: list[dict] = []
    read_fail = 0
    try:
        src_results = universe_lib.read_batch([
            ReadRequest(symbol=t, date_range=(as_of_ts, as_of_ts), columns=src_cols)
            for t in tickers
        ])
    except Exception as exc:
        log.warning(
            "factor-loading-zscores daily: source read_batch failed (skipped): %s", exc,
        )
        return {"status": "read_error", "error": str(exc), "tickers_written": 0}

    for t, res in zip(tickers, src_results):
        data = getattr(res, "data", None)
        if data is None or data.empty or as_of_ts not in data.index:
            read_fail += 1
            continue
        row = {"ticker": t}
        for c in src_cols:
            row[c] = float(data.loc[as_of_ts, c]) if c in data.columns else np.nan
        rows.append(row)

    if not rows:
        log.warning(
            "factor-loading-zscores daily: no readable tickers @ %s (read_fail=%d)",
            as_of_ts.date(), read_fail,
        )
        return {"status": "empty", "tickers_written": 0, "read_fail": read_fail}

    zscored = apply_factor_zscores(pd.DataFrame(rows))
    z_by_ticker = {
        r["ticker"]: {c: float(r[c]) for c in dst_cols}
        for _, r in zscored.iterrows()
    }
    n_all_nan = sum(
        1 for vals in z_by_ticker.values()
        if not any(np.isfinite(v) for v in vals.values())
    )
    if not write:
        return {
            "status": "ok",
            "tickers_written": 0,
            "tickers_all_nan": n_all_nan,
            "read_fail": read_fail,
            "n_computed": len(z_by_ticker),
        }

    write_tickers = list(z_by_ticker)
    try:
        today_results = universe_lib.read_batch(
            [ReadRequest(symbol=t, date_range=(as_of_ts, as_of_ts)) for t in write_tickers]
        )
    except Exception as exc:
        log.warning(
            "factor-loading-zscores daily: today read_batch failed (skipped): %s", exc,
        )
        return {
            "status": "read_error",
            "error": str(exc),
            "tickers_written": 0,
            "read_fail": read_fail,
        }

    payloads = []
    for t, res in zip(write_tickers, today_results):
        data = getattr(res, "data", None)
        if data is None or data.empty or as_of_ts not in data.index:
            continue
        row = data.copy()
        for col in dst_cols:
            row.loc[as_of_ts, col] = np.float32(z_by_ticker[t][col])
        out = canonical_fn(row) if canonical_fn is not None else row
        payloads.append(UpdatePayload(symbol=t, data=out))

    n_written = 0
    write_fail = 0
    if payloads:
        try:
            universe_lib.update_batch(payloads)
            n_written = len(payloads)
        except Exception as exc:
            log.warning(
                "factor-loading-zscores daily: update_batch failed (skipped): %s", exc,
            )
            write_fail = len(payloads)

    if n_all_nan:
        log.warning(
            "factor-loading-zscores daily: %d/%d tickers all-NaN @ %s",
            n_all_nan, len(z_by_ticker), as_of_ts.date(),
        )
    log.info(
        "factor-loading-zscores daily update @ %s: %d written, %d all-NaN, "
        "%d read-fail, %d write-fail (of %d computed)",
        as_of_ts.date(), n_written, n_all_nan, read_fail, write_fail, len(z_by_ticker),
    )
    return {
        "status": "ok",
        "tickers_written": n_written,
        "tickers_all_nan": n_all_nan,
        "read_fail": read_fail,
        "write_fail": write_fail,
        "n_computed": len(z_by_ticker),
    }
