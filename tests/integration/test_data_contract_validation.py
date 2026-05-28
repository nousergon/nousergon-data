"""Integration test: validate data contracts against synthetic and real schema files."""
import json
import os
import pytest
from jsonschema import validate, ValidationError

# Schema files live in alpha-engine-data/contracts/
CONTRACTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "contracts")


def _load_schema(name: str) -> dict:
    path = os.path.join(CONTRACTS_DIR, name)
    with open(path) as f:
        return json.load(f)


# ── Schema file existence ─────────────────────────────────────────────────


class TestSchemaFilesExist:
    def test_signals_schema_exists(self):
        assert os.path.exists(os.path.join(CONTRACTS_DIR, "signals.schema.json"))

    def test_predictions_schema_exists(self):
        assert os.path.exists(os.path.join(CONTRACTS_DIR, "predictions.schema.json"))

    def test_executor_params_schema_exists(self):
        assert os.path.exists(os.path.join(CONTRACTS_DIR, "executor_params.schema.json"))


# ── Schema validation ─────────────────────────────────────────────────────


class TestSignalsContract:
    def test_valid_signals_pass(self):
        schema = _load_schema("signals.schema.json")
        valid_signals = {
            "date": "2026-04-03",
            "run_time": "00:30:00",
            "market_regime": "neutral",
            "sector_ratings": {},
            "universe": [
                {
                    "ticker": "AAPL",
                    "signal": "HOLD",
                    "score": 65.0,
                    "conviction": "stable",
                    "rating": "HOLD",
                    "sector": "Technology",
                }
            ],
            "buy_candidates": [
                {
                    "ticker": "MSFT",
                    "signal": "ENTER",
                    "score": 82.0,
                    "conviction": "rising",
                    "rating": "BUY",
                    "sector": "Technology",
                }
            ],
        }
        # Should not raise
        validate(instance=valid_signals, schema=schema)

    def test_missing_required_field_fails(self):
        schema = _load_schema("signals.schema.json")
        bad_signals = {"date": "2026-04-03"}  # Missing required fields
        with pytest.raises(ValidationError):
            validate(instance=bad_signals, schema=schema)

    def test_market_regime_enum_is_3class(self):
        # 3-class Ang-Bekaert taxonomy (v0.42.0 / 2026-05-28).
        # Legacy 4-class "caution" retired per
        # caution-regime-retirement-260528.md.
        schema = _load_schema("signals.schema.json")
        regime_field = schema["properties"]["market_regime"]
        assert regime_field["enum"] == ["bull", "neutral", "bear"]

    def test_market_regime_caution_rejected(self):
        schema = _load_schema("signals.schema.json")
        bad_signals = {
            "date": "2026-04-03",
            "run_time": "00:30:00",
            "market_regime": "caution",
            "sector_ratings": {},
            "universe": [],
            "buy_candidates": [],
        }
        with pytest.raises(ValidationError):
            validate(instance=bad_signals, schema=schema)

    def test_each_3class_regime_accepted(self):
        schema = _load_schema("signals.schema.json")
        for regime in ("bull", "neutral", "bear"):
            signals = {
                "date": "2026-04-03",
                "run_time": "00:30:00",
                "market_regime": regime,
                "sector_ratings": {},
                "universe": [],
                "buy_candidates": [],
            }
            # Should not raise
            validate(instance=signals, schema=schema)


class TestPredictionsContract:
    def test_valid_predictions_pass(self):
        schema = _load_schema("predictions.schema.json")
        valid_preds = {
            "date": "2026-04-03",
            "model_version": "gbm_v2",
            "n_predictions": 1,
            "predictions": [
                {
                    "ticker": "AAPL",
                    "predicted_direction": "UP",
                    "prediction_confidence": 0.65,
                    "predicted_alpha": 0.015,
                    "p_up": 0.65,
                    "p_flat": 0.20,
                    "p_down": 0.15,
                }
            ],
        }
        validate(instance=valid_preds, schema=schema)


class TestExecutorParamsContract:
    def test_valid_params_pass(self):
        schema = _load_schema("executor_params.schema.json")
        valid_params = {
            "min_score_to_enter": 70,
            "max_position_pct": 0.05,
        }
        validate(instance=valid_params, schema=schema)
