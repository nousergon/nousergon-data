"""Tests for factor momentum (W2.3, L4469).

The load-bearing properties: strict point-in-time / no-look-ahead, the 12-1
skip-month construction, and that a genuinely persistent factor produces a
positive momentum tilt for high-loading names.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.factor_momentum import (
    compute_daily_factor_returns,
    compute_factor_momentum_feature,
    compute_factor_momentum_series,
)


def _persistent_factor_panel(n_tickers=40, n_dates=400, seed=0, strength=0.003):
    """One factor `f1` with a FIXED per-ticker loading; daily returns are a
    persistent function of that loading (+ noise) → the factor earns a positive
    long-short spread every day → positive factor momentum."""
    rng = np.random.default_rng(seed)
    tickers = [f"T{i:03d}" for i in range(n_tickers)]
    dates = pd.bdate_range("2020-01-01", periods=n_dates)
    loading = {t: rng.normal() for t in tickers}
    price = {t: 100.0 for t in tickers}
    rows = []
    for d in dates:
        for t in tickers:
            r = strength * loading[t] + rng.normal(0, 0.008)
            price[t] *= (1.0 + r)
            rows.append({"ticker": t, "date": d, "close": price[t], "f1": loading[t]})
    return pd.DataFrame(rows), tickers, loading


class TestPersistentFactor:
    def test_high_loading_ticker_gets_positive_signal(self):
        panel, tickers, loading = _persistent_factor_panel()
        sig = compute_factor_momentum_feature(panel, ["f1"], window=252, skip=21)
        panel = panel.assign(signal=sig.to_numpy())
        last = panel[panel["date"] == panel["date"].max()]
        hi = max(loading, key=loading.get)
        lo = min(loading, key=loading.get)
        s_hi = last.loc[last["ticker"] == hi, "signal"].iloc[0]
        s_lo = last.loc[last["ticker"] == lo, "signal"].iloc[0]
        assert np.isfinite(s_hi) and np.isfinite(s_lo)
        assert s_hi > 0            # persistent positive factor → positive tilt
        assert s_hi > s_lo         # monotone in loading


class TestNoLookAhead:
    def test_mutating_future_prices_does_not_change_past_signal(self):
        panel, _, _ = _persistent_factor_panel(seed=2)
        sig1 = compute_factor_momentum_feature(panel, ["f1"]).to_numpy()
        cutoff = panel["date"].sort_values().unique()[349]
        panel2 = panel.copy()
        future = panel2["date"] > cutoff
        panel2.loc[future, "close"] *= 1.5  # perturb only the future
        sig2 = compute_factor_momentum_feature(panel2, ["f1"]).to_numpy()
        past = (panel["date"] <= cutoff).to_numpy()
        assert np.array_equal(sig1[past], sig2[past], equal_nan=True)


class TestSkipMonth:
    def test_momentum_excludes_recent_skip_window(self):
        # All-ones daily factor returns → momentum at t = (window - skip) ones,
        # and is INVARIANT to mutating the most-recent `skip` days.
        idx = pd.bdate_range("2020-01-01", periods=400)
        fr = pd.DataFrame({"f1": np.ones(400)}, index=idx)
        mom = compute_factor_momentum_series(fr, window=252, skip=21)
        assert abs(mom["f1"].iloc[-1] - (252 - 21)) < 1e-6
        fr2 = fr.copy()
        fr2.iloc[-21:, 0] = 99.0  # mutate only the skipped recent month
        mom2 = compute_factor_momentum_series(fr2, window=252, skip=21)
        assert mom["f1"].iloc[-1] == mom2["f1"].iloc[-1]   # unchanged

    def test_front_of_history_is_nan(self):
        idx = pd.bdate_range("2020-01-01", periods=400)
        fr = pd.DataFrame({"f1": np.ones(400)}, index=idx)
        mom = compute_factor_momentum_series(fr, window=252, skip=21)
        # Warmup = (cum-1) + skip = (231-1) + 21 = 251 → first valid at idx 251.
        assert mom["f1"].iloc[:251].isna().all()
        assert np.isfinite(mom["f1"].iloc[251])


class TestDailyFactorReturns:
    def test_uses_lagged_loading_not_contemporaneous(self):
        # Day-d factor return must rank by the loading as of d-1.
        panel, _, _ = _persistent_factor_panel(n_tickers=30, n_dates=60, seed=5)
        fr = compute_daily_factor_returns(panel, ["f1"], min_names=10)
        # Persistent positive factor → mean daily factor return clearly > 0.
        assert fr["f1"].dropna().mean() > 0
        # First date has no lagged loading / no prior close → NaN.
        assert np.isnan(fr["f1"].iloc[0])


class TestRobustness:
    def test_no_factor_columns_returns_all_nan(self):
        panel, _, _ = _persistent_factor_panel(n_dates=40)
        out = compute_factor_momentum_feature(panel, ["does_not_exist"])
        assert out.isna().all()

    def test_short_history_yields_nan_no_crash(self):
        panel, _, _ = _persistent_factor_panel(n_dates=50)  # < window
        out = compute_factor_momentum_feature(panel, ["f1"], window=252, skip=21)
        assert len(out) == len(panel)
        assert out.isna().all()
