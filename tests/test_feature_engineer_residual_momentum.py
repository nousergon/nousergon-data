"""Tests for the W2 (L4469) residual-momentum features.

Validates the 3 new feature-store columns added to feature_engineer.py:
- residual_momentum_ratio : vol-scaled cumulative residual (idiosyncratic)
  log-return over the 12-1 skip-month window — REUSES the same beta-residualized
  return series as idio_vol_60d (no beta recompute).
- mom_12_1_pct            : 12-1 skip-month raw price momentum.
- sector_mom_pct          : sector-ETF own 12-1 skip-month momentum.

Plan doc: ~/Development/alpha-engine-docs/private/predictor-improvement-260530.md
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.feature_engineer import FEATURES, compute_features

_W2_COLS = ("residual_momentum_ratio", "mom_12_1_pct", "sector_mom_pct")


def _ohlcv(n=400, seed=0, drift=0.0, vol=0.012, start="2018-01-01"):
    rng = np.random.default_rng(seed)
    r = drift + rng.normal(0, vol, n)
    close = 100.0 * np.exp(np.cumsum(r))
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.DataFrame({
        "Open": close * (1 + rng.normal(0, 0.003, n)),
        "High": close * (1 + np.abs(rng.normal(0, 0.005, n))),
        "Low": close * (1 - np.abs(rng.normal(0, 0.005, n))),
        "Close": close,
        "Volume": rng.integers(1_000_000, 10_000_000, n).astype(float),
    }, index=idx)


def _series(n=400, seed=99, drift=0.0, vol=0.008, start="2018-01-01", base=300.0):
    rng = np.random.default_rng(seed)
    close = base * np.exp(np.cumsum(drift + rng.normal(0, vol, n)))
    return pd.Series(close, index=pd.date_range(start, periods=n, freq="B"))


class TestSchema:
    def test_w2_columns_in_features_list(self):
        for name in _W2_COLS:
            assert name in FEATURES, f"{name} missing from FEATURES"

    def test_compute_features_emits_w2_columns(self):
        out = compute_features(_ohlcv(), spy_series=_series(), sector_etf_series=_series(seed=7))
        for name in _W2_COLS:
            assert name in out.columns


class TestResidualMomentumRatio:
    def test_finite_after_warmup_nan_before(self):
        out = compute_features(_ohlcv(n=400), spy_series=_series(n=400))
        assert np.isfinite(out["residual_momentum_ratio"].iloc[-1])
        # Pre-warmup (window 252 + skip 21) is NaN.
        assert pd.isna(out["residual_momentum_ratio"].iloc[100])

    def test_pure_beta_stock_has_near_zero_residual_momentum(self):
        # Stock == SPY exactly → residual return ≈ 0 → residual momentum ≈ 0,
        # even though the market trends. (Distinguishes residual from raw.)
        spy = _series(n=400, drift=0.0008)  # market trends up
        df = pd.DataFrame({
            "Open": spy, "High": spy * 1.001, "Low": spy * 0.999,
            "Close": spy, "Volume": np.full(400, 5e6, dtype=float),
        }, index=spy.index)
        out = compute_features(df, spy_series=spy)
        assert abs(out["residual_momentum_ratio"].iloc[-1]) < 0.05

    def test_nan_when_spy_missing(self):
        out = compute_features(_ohlcv(), spy_series=None)
        assert out["residual_momentum_ratio"].isna().all()


class TestMom121:
    def test_skip_month_excludes_recent_window(self):
        # mom_12_1_pct at the last date must NOT depend on the most-recent 21d.
        df = _ohlcv(n=400)
        out1 = compute_features(df.copy(), spy_series=_series(n=400))
        df2 = df.copy()
        df2.iloc[-21:, df2.columns.get_loc("Close")] *= 1.3  # perturb last month
        out2 = compute_features(df2, spy_series=_series(n=400))
        assert out1["mom_12_1_pct"].iloc[-1] == out2["mom_12_1_pct"].iloc[-1]

    def test_finite_after_warmup(self):
        out = compute_features(_ohlcv(n=400), spy_series=_series(n=400))
        assert np.isfinite(out["mom_12_1_pct"].iloc[-1])
        assert pd.isna(out["mom_12_1_pct"].iloc[100])


class TestSectorMom:
    def test_finite_with_sector_etf(self):
        out = compute_features(
            _ohlcv(n=400), spy_series=_series(n=400),
            sector_etf_series=_series(n=400, seed=7, drift=0.0005),
        )
        assert np.isfinite(out["sector_mom_pct"].iloc[-1])

    def test_nan_when_sector_etf_missing(self):
        out = compute_features(_ohlcv(n=400), spy_series=_series(n=400), sector_etf_series=None)
        assert out["sector_mom_pct"].isna().all()
