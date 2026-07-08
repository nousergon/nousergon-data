"""Tests for the config#939 feature gaps: VWAP divergence, buying/selling
pressure, credit spreads.

Validates the 3 new features added to features/feature_engineer.py:
- vwap_divergence_pct:    (Close - VWAP) / VWAP
- cmf_20_ratio:           Chaikin Money Flow (20d) — buying/selling pressure
- hy_oas_credit_spread_pct: ICE BofA US HY Index OAS (FRED BAMLH0A0HYM2)

Refs nousergon/alpha-engine-config#939.
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
    with_vwap: bool = True,
):
    """Build (n,)-day OHLCV(+VWAP) DataFrame for compute_features."""
    rng = np.random.default_rng(seed)
    daily_returns = rng.normal(0, 0.012, n)
    close = 100.0 * np.exp(np.cumsum(daily_returns))
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)
    idx = pd.date_range(start, periods=n, freq="B")
    data = {
        "Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume,
    }
    if with_vwap:
        data["VWAP"] = close * (1 + rng.normal(0, 0.001, n))
    return pd.DataFrame(data, index=idx)


def _synthetic_hyoas(n: int = 400, seed: int = 7, start: str = "2018-01-01") -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.Series(rng.normal(4.0, 0.3, n).clip(min=1.0), index=idx)


# ── Schema contract ─────────────────────────────────────────────────────


class TestFeatureSchema:

    def test_new_features_in_features_list(self):
        for name in ("vwap_divergence_pct", "cmf_20_ratio", "hy_oas_credit_spread_pct"):
            assert name in FEATURES, f"{name} missing from FEATURES list"

    def test_compute_features_emits_new_columns(self):
        df = _synthetic_ohlcv()
        hyoas = _synthetic_hyoas()
        out = compute_features(df, hyoas_series=hyoas)
        for name in ("vwap_divergence_pct", "cmf_20_ratio", "hy_oas_credit_spread_pct"):
            assert name in out.columns

    def test_no_namespace_collision_with_regime_hy_oas_level(self):
        # config#939: crucible-predictor's model/regime_predictor.py already
        # ships a SEPARATE market-wide regime feature named "hy_oas_level" /
        # "hy_oas_change_21d" (consumed via cfg.MACRO_NORM_FEATURES). This
        # repo's per-ticker/date feature store must use a distinct name.
        assert "hy_oas_level" not in FEATURES
        assert "hy_oas_credit_spread_pct" in FEATURES


# ── vwap_divergence_pct ──────────────────────────────────────────────────


class TestVwapDivergencePct:

    def test_finite_when_vwap_present(self):
        df = _synthetic_ohlcv()
        out = compute_features(df)
        assert out["vwap_divergence_pct"].notna().any()

    def test_known_good_value(self):
        n = 300
        idx = pd.date_range("2024-01-01", periods=n, freq="B")
        close = pd.Series(100.0, index=idx)
        vwap = pd.Series(95.0, index=idx)
        df = pd.DataFrame({
            "Open": close, "High": close + 1, "Low": close - 1,
            "Close": close, "Volume": 1_000_000.0, "VWAP": vwap,
        }, index=idx)
        out = compute_features(df)
        expected = (100.0 - 95.0) / 95.0
        assert abs(out["vwap_divergence_pct"].iloc[-1] - expected) < 1e-9

    def test_nan_when_vwap_column_absent(self):
        # yfinance-fallback-only frames (no VWAP column at all) must not
        # crash — propagate NaN, matching the High/Low-optional pattern.
        df = _synthetic_ohlcv(with_vwap=False)
        out = compute_features(df)
        assert out["vwap_divergence_pct"].isna().all()

    def test_nan_when_vwap_values_are_nan(self):
        # Documented 2026-04-17->23 Polygon outage window: VWAP column
        # present but NaN for the affected rows.
        df = _synthetic_ohlcv()
        df["VWAP"] = np.nan
        out = compute_features(df)
        assert out["vwap_divergence_pct"].isna().all()

    def test_nan_guarded_when_vwap_is_zero(self):
        # Degenerate zero-VWAP row must not raise a ZeroDivisionError /
        # produce +-inf — guarded to NaN like volume_trend/obv_slope_10d.
        df = _synthetic_ohlcv()
        df.loc[df.index[200], "VWAP"] = 0.0
        out = compute_features(df)
        assert pd.isna(out["vwap_divergence_pct"].iloc[200])
        assert np.isfinite(out["vwap_divergence_pct"].iloc[-1])


# ── cmf_20_ratio ──────────────────────────────────────────────────────────


class TestCmf20Ratio:

    def test_bounded_range(self):
        # CMF is a volume-weighted average of a [-1, 1] signal — must stay
        # within that band (small float slop tolerated).
        df = _synthetic_ohlcv()
        out = compute_features(df)
        cmf = out["cmf_20_ratio"].dropna()
        assert len(cmf) > 0
        assert cmf.between(-1.0001, 1.0001).all()

    def test_all_buying_pressure_is_positive_one(self):
        # Close == High every day (price closes at the top of its range) ->
        # money-flow multiplier is +1 every day -> CMF_20 == +1.
        n = 60
        idx = pd.date_range("2024-01-01", periods=n, freq="B")
        close = pd.Series(np.linspace(100, 130, n), index=idx)
        high = close
        low = close - 2.0
        df = pd.DataFrame({
            "Open": low, "High": high, "Low": low, "Close": close,
            "Volume": 1_000_000.0,
        }, index=idx)
        out = compute_features(df)
        tail = out["cmf_20_ratio"].dropna()
        assert len(tail) > 0
        assert np.allclose(tail.tail(5), 1.0)

    def test_all_selling_pressure_is_negative_one(self):
        # Close == Low every day -> money-flow multiplier is -1 every day
        # -> CMF_20 == -1.
        n = 60
        idx = pd.date_range("2024-01-01", periods=n, freq="B")
        close = pd.Series(np.linspace(130, 100, n), index=idx)
        low = close
        high = close + 2.0
        df = pd.DataFrame({
            "Open": high, "High": high, "Low": low, "Close": close,
            "Volume": 1_000_000.0,
        }, index=idx)
        out = compute_features(df)
        tail = out["cmf_20_ratio"].dropna()
        assert len(tail) > 0
        assert np.allclose(tail.tail(5), -1.0)

    def test_nan_guarded_when_high_equals_low(self):
        # Halted/illiquid session: High == Low degenerates the money-flow
        # multiplier's denominator to zero — must not raise, must not
        # silently zero-fill; NaN-guarded like volume_trend/obv_slope_10d.
        df = _synthetic_ohlcv()
        df.loc[df.index[50], "High"] = df.loc[df.index[50], "Low"]
        out = compute_features(df)
        # No crash, and the feature stays finite/NaN-only (no inf).
        assert not np.isinf(out["cmf_20_ratio"].dropna()).any()

    def test_uses_rolling_sum_not_rolling_mean_denominator(self):
        # Regression guard: CMF_20 = rolling_SUM(mfm*vol, 20) /
        # rolling_SUM(vol, 20). An earlier draft divided by the rolling
        # MEAN volume (reusing volume_trend's vol_20) instead of the
        # rolling SUM, which inflated the ratio ~20x out of [-1, 1].
        df = _synthetic_ohlcv()
        out = compute_features(df)
        cmf = out["cmf_20_ratio"].dropna()
        assert cmf.abs().max() <= 1.0001


# ── hy_oas_credit_spread_pct ────────────────────────────────────────────


class TestHyOasCreditSpreadPct:

    def test_finite_when_hyoas_series_provided(self):
        df = _synthetic_ohlcv()
        hyoas = _synthetic_hyoas()
        out = compute_features(df, hyoas_series=hyoas)
        assert out["hy_oas_credit_spread_pct"].notna().all()

    def test_tracks_input_series_after_alignment(self):
        n = 300
        idx = pd.date_range("2024-01-01", periods=n, freq="B")
        df = _synthetic_ohlcv(n=n, start="2024-01-01")
        hyoas = pd.Series(4.25, index=idx)
        out = compute_features(df, hyoas_series=hyoas)
        assert np.allclose(out["hy_oas_credit_spread_pct"], 4.25)

    def test_neutral_default_when_series_absent(self):
        # HYOAS is FRED-license-gated to 2023+; pre-2023 backfills or any
        # run where the macro dict lacks "HYOAS" must fall back to the
        # neutral default (0.0), never hard-fail — same pattern as
        # gold_mom_5d / oil_mom_5d.
        df = _synthetic_ohlcv()
        out = compute_features(df)  # hyoas_series defaults to None
        assert (out["hy_oas_credit_spread_pct"] == 0.0).all()

    def test_neutral_default_when_series_partially_missing(self):
        # hyoas_series covers only the tail of df's date range (mimics the
        # FRED 2023+ license gate against a longer per-ticker history) —
        # rows before the series' first observation ffill to NaN, then
        # neutral-default to 0.0 rather than propagating NaN or raising.
        df = _synthetic_ohlcv(n=400, start="2018-01-01")
        hyoas = _synthetic_hyoas(n=100, start="2023-01-02")
        out = compute_features(df, hyoas_series=hyoas)
        assert out["hy_oas_credit_spread_pct"].notna().all()
        early_rows = out["hy_oas_credit_spread_pct"].loc[:"2022-12-31"]
        assert (early_rows == 0.0).all()
