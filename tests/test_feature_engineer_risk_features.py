"""Tests for the v3.2 per-ticker risk features (Stage 2 of regime-
conditioning rebuild).

Validates the 4 new features added to features/feature_engineer.py:
- beta_60d:        rolling regression slope of stock vs SPY log-returns
- idio_vol_60d:    residual vol after removing beta exposure
- vol_of_vol_30d:  rolling stdev of realized_vol_20d
- max_drawdown_60d: worst peak-to-trough within trailing 60d

Plan doc: ~/Development/alpha-engine-docs/private/regime-conditioning-260510.md
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.feature_engineer import FEATURES, compute_features


def _synthetic_ohlcv(
    n: int = 400,
    seed: int = 0,
    start: str = "2018-01-01",
):
    """Build (n,)-day OHLCV DataFrame for compute_features."""
    rng = np.random.default_rng(seed)
    daily_returns = rng.normal(0, 0.012, n)
    close = 100.0 * np.exp(np.cumsum(daily_returns))
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.DataFrame({
        "Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume,
    }, index=idx)


def _synthetic_spy(
    n: int = 400,
    seed: int = 99,
    start: str = "2018-01-01",
):
    rng = np.random.default_rng(seed)
    daily_returns = rng.normal(0, 0.008, n)
    close = 300.0 * np.exp(np.cumsum(daily_returns))
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.Series(close, index=idx)


# ── Schema contract ─────────────────────────────────────────────────────


class TestFeatureSchema:

    def test_new_risk_features_in_features_list(self):
        for name in (
            "beta_60d", "idio_vol_60d", "vol_of_vol_30d", "max_drawdown_60d",
            "realized_vol_63d",
        ):
            assert name in FEATURES, f"{name} missing from FEATURES list"

    def test_compute_features_emits_new_columns(self):
        df = _synthetic_ohlcv()
        spy = _synthetic_spy()
        out = compute_features(df, spy_series=spy)
        for name in (
            "beta_60d", "idio_vol_60d", "vol_of_vol_30d", "max_drawdown_60d",
            "realized_vol_63d",
        ):
            assert name in out.columns


# ── beta_60d ──────────────────────────────────────────────────────────


class TestBeta60d:

    def test_beta_finite_after_warmup(self):
        df = _synthetic_ohlcv(n=300)
        spy = _synthetic_spy(n=300)
        out = compute_features(df, spy_series=spy)
        # Beyond the 60d warmup, beta should be finite.
        assert np.isfinite(out["beta_60d"].iloc[-1])
        # Pre-warmup is NaN (rolling min_periods=60 enforced).
        assert pd.isna(out["beta_60d"].iloc[30])

    def test_beta_with_perfect_market_correlation_is_one(self):
        # Stock = SPY exactly → beta should be 1.0.
        spy = _synthetic_spy(n=300)
        df = pd.DataFrame({
            "Open": spy, "High": spy * 1.001, "Low": spy * 0.999,
            "Close": spy, "Volume": np.full(300, 5e6, dtype=float),
        }, index=spy.index)
        out = compute_features(df, spy_series=spy)
        # Beta of stock-vs-self is 1.0 (variances cancel).
        last_beta = out["beta_60d"].iloc[-1]
        assert abs(last_beta - 1.0) < 0.01

    def test_beta_with_zero_market_correlation_near_zero(self):
        # Stock returns independent of SPY → beta should be near zero.
        df = _synthetic_ohlcv(n=400, seed=1)
        spy = _synthetic_spy(n=400, seed=2)
        out = compute_features(df, spy_series=spy)
        # Two independent random series → expected beta ≈ 0 (large
        # samples). Loose bound for stochastic test.
        beta_tail = out["beta_60d"].iloc[-50:].abs().mean()
        assert beta_tail < 0.5

    def test_beta_nan_when_spy_missing(self):
        df = _synthetic_ohlcv(n=300)
        out = compute_features(df, spy_series=None)
        assert out["beta_60d"].isna().all()


# ── idio_vol_60d ──────────────────────────────────────────────────────


class TestIdioVol60d:

    def test_idio_vol_finite_after_warmup(self):
        df = _synthetic_ohlcv(n=300)
        spy = _synthetic_spy(n=300)
        out = compute_features(df, spy_series=spy)
        assert np.isfinite(out["idio_vol_60d"].iloc[-1])
        assert pd.isna(out["idio_vol_60d"].iloc[30])

    def test_idio_vol_zero_when_stock_equals_spy(self):
        # Stock exactly tracks SPY → residuals are zero → idio_vol ≈ 0.
        spy = _synthetic_spy(n=300)
        df = pd.DataFrame({
            "Open": spy, "High": spy * 1.001, "Low": spy * 0.999,
            "Close": spy, "Volume": np.full(300, 5e6, dtype=float),
        }, index=spy.index)
        out = compute_features(df, spy_series=spy)
        # Tolerance allows rounding noise from rolling-window math.
        assert out["idio_vol_60d"].iloc[-1] < 0.01

    def test_idio_vol_positive_when_stock_independent_of_spy(self):
        # Independent series have non-zero residual vol.
        df = _synthetic_ohlcv(n=400, seed=1)
        spy = _synthetic_spy(n=400, seed=2)
        out = compute_features(df, spy_series=spy)
        assert out["idio_vol_60d"].iloc[-1] > 0

    def test_idio_vol_nan_when_spy_missing(self):
        df = _synthetic_ohlcv(n=300)
        out = compute_features(df, spy_series=None)
        assert out["idio_vol_60d"].isna().all()


# ── vol_of_vol_30d ─────────────────────────────────────────────────────


class TestVolOfVol30d:

    def test_vol_of_vol_finite_after_warmup(self):
        df = _synthetic_ohlcv(n=300)
        out = compute_features(df, spy_series=_synthetic_spy(n=300))
        # vol_of_vol_30d depends on realized_vol_20d (warmup 20) +
        # additional 30 → finite from index ~50 onwards.
        assert np.isfinite(out["vol_of_vol_30d"].iloc[-1])

    def test_vol_of_vol_nonnegative(self):
        df = _synthetic_ohlcv(n=400)
        out = compute_features(df, spy_series=_synthetic_spy(n=400))
        finite = out["vol_of_vol_30d"].dropna()
        assert (finite >= 0).all()


# ── realized_vol_63d ──────────────────────────────────────────────────


class TestRealizedVol63d:

    def test_realized_vol_63d_finite_after_warmup(self):
        df = _synthetic_ohlcv(n=200)
        out = compute_features(df, spy_series=_synthetic_spy(n=200))
        # Warmup is 63 days; finite from index 63 onward.
        assert np.isfinite(out["realized_vol_63d"].iloc[-1])
        assert pd.isna(out["realized_vol_63d"].iloc[30])

    def test_realized_vol_63d_nonnegative(self):
        df = _synthetic_ohlcv(n=300)
        out = compute_features(df, spy_series=_synthetic_spy(n=300))
        finite = out["realized_vol_63d"].dropna()
        assert (finite >= 0).all()

    def test_realized_vol_63d_smoother_than_20d(self):
        # 63d window is wider → vol estimate is more stable across time
        # than 20d. Loose check: stdev of the 63d series across its
        # finite range should be lower than stdev of the 20d series over
        # the same range. Stochastic, generous tolerance.
        df = _synthetic_ohlcv(n=400, seed=42)
        out = compute_features(df, spy_series=_synthetic_spy(n=400))
        # Compare on the slice where both are finite.
        rv20 = out["realized_vol_20d"].iloc[63:].dropna()
        rv63 = out["realized_vol_63d"].iloc[63:].dropna()
        assert rv63.std() < rv20.std()


# ── max_drawdown_60d ──────────────────────────────────────────────────


class TestMaxDrawdown60d:

    def test_drawdown_finite_after_warmup(self):
        df = _synthetic_ohlcv(n=300)
        out = compute_features(df, spy_series=_synthetic_spy(n=300))
        assert np.isfinite(out["max_drawdown_60d"].iloc[-1])
        assert pd.isna(out["max_drawdown_60d"].iloc[30])

    def test_drawdown_always_nonpositive(self):
        # max drawdown is always ≤ 0 by definition (price relative to
        # rolling max).
        df = _synthetic_ohlcv(n=400)
        out = compute_features(df, spy_series=_synthetic_spy(n=400))
        finite = out["max_drawdown_60d"].dropna()
        assert (finite <= 1e-9).all()

    def test_drawdown_zero_for_monotonically_increasing(self):
        # Price strictly rising → no drawdown → 0.
        n = 300
        idx = pd.date_range("2018-01-01", periods=n, freq="B")
        rising_close = pd.Series(np.linspace(100, 200, n), index=idx)
        df = pd.DataFrame({
            "Open": rising_close,
            "High": rising_close * 1.001,
            "Low": rising_close * 0.999,
            "Close": rising_close,
            "Volume": np.full(n, 5e6, dtype=float),
        }, index=idx)
        spy = _synthetic_spy(n=n)
        out = compute_features(df, spy_series=spy)
        finite = out["max_drawdown_60d"].dropna()
        # Monotone-increasing → drawdown is always 0 (close == rolling max).
        assert np.allclose(finite, 0.0, atol=1e-9)

    def test_drawdown_negative_after_decline(self):
        # Build a series with a known 20% drop from rolling-60d max.
        n = 200
        idx = pd.date_range("2018-01-01", periods=n, freq="B")
        # First 100 days flat at 100, then drops to 80, then flat.
        prices = np.concatenate([
            np.full(100, 100.0),
            np.full(n - 100, 80.0),
        ])
        cs = pd.Series(prices, index=idx)
        df = pd.DataFrame({
            "Open": cs, "High": cs, "Low": cs, "Close": cs,
            "Volume": np.full(n, 5e6, dtype=float),
        }, index=idx)
        spy = _synthetic_spy(n=n)
        out = compute_features(df, spy_series=spy)
        # Post-drop, within 60d window the drawdown should reflect ~-20%.
        post_drop = out["max_drawdown_60d"].iloc[150]
        assert post_drop < -0.15  # negative depth ~20%
