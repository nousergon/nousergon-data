"""Point-in-time (as-of) invariant for feature engineering (config#833 / L3293).

The backtest replay path (crucible-backtester `synthetic/predictor_backtest.py`)
computes features ONCE over each ticker's ENTIRE history and then slices the result
by decision date. That is only sound if every feature at row T depends solely on
rows <= T — i.e. `compute_features(df[:k]).loc[t] == compute_features(df).loc[t]`
for every t < k. A single full-sample statistic (a `.mean()`/`.std()`/`.iloc[-1]`
over the whole series) silently injects future rows into every historical value.

This test is the regression tripwire. It caught `avg_volume_20d`, which normalized
the rolling-20d volume by a WHOLE-SERIES `volume.mean()` — now an expanding
(backward-only) mean. Any future feature that reaches forward in time trips it.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.feature_engineer import compute_features

# Features fully computable from Close+Volume alone (no macro/cross-sectional/
# snapshot inputs), hence expected to be strictly as-of stable. The options/
# earnings/revision dict inputs are intentional current-snapshot broadcasts and
# are excluded by construction (their dicts are not passed).
_ASOF_COLUMNS = [
    "avg_volume_20d",        # the fixed one (was full-sample volume.mean())
    "avg_volume_20d_raw",
    "momentum_5d",
    "momentum_20d",
    "price_vs_ma50",
    "rsi_14",
    "realized_vol_20d",
    "vol_ratio_10_60",
    "return_60d",
]


def _synthetic_ohlcv(n: int, seed: int = 7, start: str = "2019-01-01") -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    daily = rng.normal(0, 0.012, n)
    close = 100.0 * np.exp(np.cumsum(daily))
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=idx,
    )


def test_features_are_as_of_stable_under_truncation():
    """Truncating the input to df[:k] must not change any as-of feature at t < k."""
    full = _synthetic_ohlcv(n=300)
    k = 240
    prefix = full.iloc[:k].copy()

    f_full = compute_features(full)
    f_pref = compute_features(prefix)

    # Compare a band of fully-warmed-up rows strictly inside the prefix.
    compare_idx = full.index[200:k]
    common = [c for c in _ASOF_COLUMNS if c in f_full.columns and c in f_pref.columns]
    assert "avg_volume_20d" in common, "avg_volume_20d must be present to guard the fix"

    for col in common:
        a = f_full.loc[compare_idx, col]
        b = f_pref.loc[compare_idx, col]
        # equal_nan so warmup NaNs (identical in both) don't spuriously fail.
        assert np.allclose(a.to_numpy(dtype=float), b.to_numpy(dtype=float),
                           rtol=1e-9, atol=1e-9, equal_nan=True), (
            f"{col} is NOT point-in-time stable: value at rows <k differs between "
            f"compute_features(df[:k]) and compute_features(df) — a look-ahead leak."
        )


def test_avg_volume_20d_uses_backward_only_normalizer():
    """Direct guard: the avg_volume_20d normalizer must be expanding, not full-sample.

    Appending FUTURE rows must not change avg_volume_20d at earlier dates.
    """
    full = _synthetic_ohlcv(n=260)
    base = full.iloc[:200].copy()
    # Same first 200 rows as `base`, but a 5x high-volume FUTURE tail. That tail
    # would drag a full-sample volume.mean() up and shrink every earlier
    # avg_volume_20d ratio; an expanding (as-of) mean sees only volume<=t and is
    # therefore identical to `base` on the shared rows.
    spiked = full.copy()
    spiked.iloc[200:, spiked.columns.get_loc("Volume")] *= 5.0

    v_base = compute_features(base)["avg_volume_20d"]
    v_spiked = compute_features(spiked)["avg_volume_20d"]

    shared = base.index[100:200]
    assert np.allclose(
        v_base.loc[shared].to_numpy(dtype=float),
        v_spiked.loc[shared].to_numpy(dtype=float),
        rtol=1e-9, atol=1e-9, equal_nan=True,
    ), "avg_volume_20d at earlier dates changed when future volume was appended — leak."
