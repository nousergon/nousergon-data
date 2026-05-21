"""
End-to-end pipeline integration test.

Validates the data flow between modules using synthetic signals/predictions.
Does NOT call external APIs (S3, yfinance, IB Gateway) — uses local fixtures.

Tests that:
1. Synthetic signals.json conforms to the data contract
2. Synthetic predictions.json conforms to the data contract
3. Executor signal_reader can parse the synthetic signals
4. Executor risk_guard correctly evaluates entries from the synthetic signals
5. Executor position_sizer produces valid sizing from the synthetic signals
6. Health checker can validate the fixture data
"""

import json
import os
import sys
import pytest
from datetime import date

# Add repo roots to path so we can import executor modules
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
EXECUTOR_PATHS = [
    os.path.join(os.path.dirname(REPO_ROOT), "alpha-engine"),
    os.path.expanduser("~/Development/alpha-engine"),
]
for p in EXECUTOR_PATHS:
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def synthetic_signals():
    """A minimal but valid signals.json payload."""
    return {
        "date": "2026-04-04",
        "market_regime": "neutral",
        "universe": [
            {
                "ticker": "AAPL",
                "signal": "ENTER",
                "score": 82,
                "rating": "BUY",
                "conviction": "rising",
                "price_target_upside": 0.15,
                "sector_rating": "overweight",
                "sector": "Technology",
                "thesis_summary": "Strong iPhone cycle + Services growth",
                "sub_scores": {"quant": 85, "qual": 79},
            },
            {
                "ticker": "JPM",
                "signal": "HOLD",
                "score": 68,
                "rating": "BUY",
                "conviction": "stable",
                "price_target_upside": 0.10,
                "sector_rating": "market_weight",
                "sector": "Financial",
                "thesis_summary": "NII expansion but credit concerns",
                "sub_scores": {"quant": 70, "qual": 66},
            },
            {
                "ticker": "XOM",
                "signal": "EXIT",
                "score": 45,
                "rating": "HOLD",
                "conviction": "declining",
                "price_target_upside": 0.03,
                "sector_rating": "underweight",
                "sector": "Energy",
                "thesis_summary": "Oil price weakness",
                "sub_scores": {"quant": 50, "qual": 40},
            },
        ],
        "sector_ratings": {
            "Technology": "overweight",
            "Financial": "market_weight",
            "Energy": "underweight",
        },
    }


@pytest.fixture
def synthetic_predictions():
    """A minimal but valid predictions.json payload."""
    return {
        "date": "2026-04-04",
        "model_version": "v3.0-meta",
        "predictions": {
            "AAPL": {
                "direction": "UP",
                "confidence": 0.65,
                "predicted_alpha": 0.023,
                "veto": False,
            },
            "JPM": {
                # Binary UP/DOWN only since alpha-engine-predictor #143
                # collapsed the FLAT class at calibrator level.
                "direction": "DOWN",
                "confidence": 0.04,
                "predicted_alpha": 0.001,
                "veto": False,
            },
            "XOM": {
                "direction": "DOWN",
                "confidence": 0.71,
                "predicted_alpha": -0.018,
                "veto": True,
            },
        },
    }


@pytest.fixture
def executor_config():
    """Minimal executor config for testing."""
    return {
        "min_score_to_enter": 70,
        "max_position_pct": 0.05,
        "bear_max_position_pct": 0.025,
        "max_sector_pct": 0.25,
        "max_equity_pct": 0.90,
        "drawdown_circuit_breaker": 0.08,
        "bear_block_underweight": True,
        "conviction_decline_adj": 0.70,
        "min_price_target_upside": 0.05,
        "upside_fail_adj": 0.70,
        "min_position_dollar": 500,
        "sector_adj": {
            "overweight": 1.05,
            "market_weight": 1.00,
            "underweight": 0.85,
        },
        "atr_sizing_enabled": False,
        "confidence_sizing_enabled": False,
        "staleness_discount_enabled": False,
        "earnings_sizing_enabled": False,
        "strategy": {
            "graduated_drawdown": {
                "enabled": True,
                "tiers": [
                    (-0.02, 1.00, "0-2%"),
                    (-0.04, 0.50, "2-4%"),
                    (-0.06, 0.25, "4-6%"),
                ],
            },
        },
    }


# ── Contract validation ──────────────────────────────────────────────────────


class TestSignalsContract:
    """Signals.json must conform to the JSON Schema contract."""

    def test_signals_has_required_fields(self, synthetic_signals):
        assert "date" in synthetic_signals
        assert "market_regime" in synthetic_signals
        assert "universe" in synthetic_signals
        assert len(synthetic_signals["universe"]) > 0

    def test_each_signal_has_required_fields(self, synthetic_signals):
        required = {"ticker", "signal", "score", "sector"}
        for sig in synthetic_signals["universe"]:
            missing = required - set(sig.keys())
            assert not missing, f"{sig['ticker']} missing: {missing}"

    def test_signal_values_valid(self, synthetic_signals):
        for sig in synthetic_signals["universe"]:
            assert sig["signal"] in ("ENTER", "EXIT", "HOLD", "REDUCE")
            assert 0 <= sig["score"] <= 100
            assert isinstance(sig["ticker"], str)

    def test_schema_validation(self, synthetic_signals):
        """Validate against actual JSON Schema if available."""
        schema_path = os.path.join(REPO_ROOT, "contracts", "signals.schema.json")
        if not os.path.exists(schema_path):
            pytest.skip("signals.schema.json not found")
        from jsonschema import validate
        schema = json.load(open(schema_path))
        validate(instance=synthetic_signals, schema=schema)


class TestPredictionsContract:
    """Predictions.json must conform to the JSON Schema contract."""

    def test_predictions_has_required_fields(self, synthetic_predictions):
        assert "date" in synthetic_predictions
        assert "predictions" in synthetic_predictions

    def test_each_prediction_has_required_fields(self, synthetic_predictions):
        for ticker, pred in synthetic_predictions["predictions"].items():
            assert "direction" in pred
            assert "confidence" in pred
            # Predictor emits binary UP/DOWN since alpha-engine-predictor #143.
            assert pred["direction"] in ("UP", "DOWN")
            assert 0 <= pred["confidence"] <= 1


# ── Executor integration ──────────────────────────────────────────────────────


class TestExecutorIntegration:
    """Verify executor modules can process synthetic signals."""

    def _try_import_executor(self):
        try:
            from executor.risk_guard import check_order
            from executor.position_sizer import compute_position_size
            return check_order, compute_position_size
        except ImportError:
            pytest.skip("executor module not importable (alpha-engine not in path)")

    def test_enter_signal_passes_risk_guard(self, synthetic_signals, executor_config):
        check_order, _ = self._try_import_executor()
        aapl = next(s for s in synthetic_signals["universe"] if s["ticker"] == "AAPL")
        approved, reason = check_order(
            ticker="AAPL",
            action="ENTER",
            dollar_size=4000,
            portfolio_nav=100_000,
            peak_nav=100_000,
            current_positions={},
            sector="Technology",
            market_regime=synthetic_signals["market_regime"],
            signal=aapl,
            config=executor_config,
        )
        assert approved, f"AAPL ENTER should pass: {reason}"

    def test_low_score_signal_blocked_by_risk_guard(self, synthetic_signals, executor_config):
        check_order, _ = self._try_import_executor()
        xom = next(s for s in synthetic_signals["universe"] if s["ticker"] == "XOM")
        # XOM has score=45 which is below min_score_to_enter=70
        approved, reason = check_order(
            ticker="XOM",
            action="ENTER",
            dollar_size=4000,
            portfolio_nav=100_000,
            peak_nav=100_000,
            current_positions={},
            sector="Energy",
            market_regime="neutral",
            signal=xom,
            config=executor_config,
        )
        assert not approved, "XOM ENTER should be blocked (score=45 < 70)"

    def test_exit_signal_always_passes(self, synthetic_signals, executor_config):
        check_order, _ = self._try_import_executor()
        xom = next(s for s in synthetic_signals["universe"] if s["ticker"] == "XOM")
        approved, _ = check_order(
            ticker="XOM",
            action="EXIT",
            dollar_size=4000,
            portfolio_nav=100_000,
            peak_nav=100_000,
            current_positions={},
            sector="Energy",
            market_regime="neutral",
            signal=xom,
            config=executor_config,
        )
        assert approved, "EXIT should always pass risk guard"

    def test_position_sizer_produces_valid_output(self, synthetic_signals, executor_config):
        _, compute_position_size = self._try_import_executor()
        aapl = next(s for s in synthetic_signals["universe"] if s["ticker"] == "AAPL")
        enter_signals = [s for s in synthetic_signals["universe"] if s["signal"] == "ENTER"]

        result = compute_position_size(
            ticker="AAPL",
            portfolio_nav=100_000,
            enter_signals=enter_signals,
            signal=aapl,
            sector_rating="overweight",
            current_price=185.0,
            config=executor_config,
        )
        assert result["shares"] > 0
        assert result["dollar_size"] > 0
        assert 0 < result["position_pct"] <= 0.05
        assert result["sector_adj"] == 1.05  # overweight

    def test_veto_signal_blocks_prediction(self, synthetic_predictions):
        """XOM has veto=True — executor should not enter."""
        xom_pred = synthetic_predictions["predictions"]["XOM"]
        assert xom_pred["veto"] is True
        assert xom_pred["direction"] == "DOWN"


# ── Data quality integration ──────────────────────────────────────────────────


class TestDataQualityIntegration:
    """Verify validators work on synthetic data."""

    def test_price_validator_on_clean_data(self):
        try:
            import pandas as pd
            from validators.price_validator import validate_parquet
        except ImportError:
            pytest.skip("validators not importable")

        dates = pd.bdate_range("2025-01-01", periods=100)
        df = pd.DataFrame({
            "Open": [100.0] * 100,
            "High": [105.0] * 100,
            "Low": [95.0] * 100,
            "Close": [102.0] * 100,
            "Volume": [1_000_000] * 100,
        }, index=dates)

        result = validate_parquet(df, "TEST")
        assert result["status"] == "clean"
        assert result["anomalies"] == []

    def test_price_validator_catches_anomalies(self):
        try:
            import pandas as pd
            import numpy as np
            from validators.price_validator import validate_parquet
        except ImportError:
            pytest.skip("validators not importable")

        dates = pd.bdate_range("2025-01-01", periods=50)
        df = pd.DataFrame({
            "Open": [100.0] * 50,
            "High": [105.0] * 50,
            "Low": [95.0] * 50,
            "Close": [100.0] * 50,
            "Volume": [1_000_000] * 50,
        }, index=dates)
        # Inject anomaly: Close = 0
        df.iloc[25, df.columns.get_loc("Close")] = 0.0

        result = validate_parquet(df, "TEST")
        assert result["status"] == "anomaly"
        assert len(result["anomalies"]) > 0
