"""
features/cross_sectional.py — Cross-sectional factor-loading transforms.

C.1 of the optimizer-sota-upgrades-260526 arc (factor-risk decomposition).
The executor's risk decomposition Σ = B·F·Bᵀ + D (workstream C.3) consumes
a (N × K) factor-loading matrix B where columns are CROSS-SECTIONALLY
z-scored style factors (mean 0, std 1 across the universe at each date).
This module adds those columns to the feature-store panel after the
per-ticker feature computation completes.

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
FACTOR_LOADING_SOURCES: dict[str, str] = {
    "momentum_20d":         "momentum_20d_zscore",
    "return_60d":           "return_60d_zscore",
    "beta_60d":             "beta_60d_zscore",
    "idio_vol_60d":         "idio_vol_60d_zscore",
    "realized_vol_63d":     "realized_vol_63d_zscore",
    "dist_from_52w_high":   "dist_from_52w_high_zscore",
    "pe_ratio":             "pe_ratio_zscore",
    "roe":                  "roe_zscore",
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
        out[dst] = _winsorize_and_zscore(out[src].astype(float))
    return out
