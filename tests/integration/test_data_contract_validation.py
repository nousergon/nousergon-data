"""Integration test: data contracts resolve + validate.

Slot schemas (signals/predictions) are served from ``nousergon_lib.contracts``
(SoT since lib v0.59.x, M0 — config#989) via this repo's ``contracts`` package
delegation; ``executor_params`` stays repo-local. Fixtures are contract-complete
v1 payloads mirroring what the producers actually emit.
"""
import os

import pytest
from jsonschema import ValidationError, validate

from contracts import _load_schema, validate_predictions, validate_signals

CONTRACTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "contracts")


def _signal_entry(**o):
    e = {
        "ticker": "AAPL", "signal": "HOLD", "score": 65.0, "rating": "HOLD",
        "conviction": "stable", "sector": "Information Technology",
        "sector_rating": "market_weight", "price_target_upside": 0.05,
    }
    e.update(o)
    return e


def _signals_payload(**o):
    payload = {
        "date": "2026-06-11", "market_regime": "neutral",
        "sector_ratings": {"Technology": {"rating": "overweight", "modifier": 1.1}},
        "sector_modifiers": {"Technology": 1.1},
        "universe": [_signal_entry()],
        "buy_candidates": [_signal_entry(ticker="MSFT", signal="ENTER", score=82.0,
                                         rating="BUY", conviction="rising",
                                         sector_rating="overweight",
                                         price_target_upside=0.18)],
    }
    payload.update(o)
    return payload


def _prediction_entry(**o):
    e = {
        "ticker": "AAPL", "predicted_direction": "UP", "prediction_confidence": 0.65,
        "predicted_alpha": 0.015, "combined_rank": 1, "gbm_veto": False,
        "momentum_veto": False,
    }
    e.update(o)
    return e


def _predictions_payload(**o):
    payload = {
        "date": "2026-06-11", "model_version": "v3.0", "n_predictions": 1,
        "predictions": [_prediction_entry()],
    }
    payload.update(o)
    return payload


class TestSchemaResolution:
    def test_slot_schemas_resolve_from_lib(self):
        for name in ("signals", "predictions"):
            schema = _load_schema(name)
            assert "nousergon.ai/schemas" in schema.get("$id", ""), (
                f"{name} must be served from nousergon_lib.contracts (SoT)"
            )
        # the stranded local copies must NOT come back
        for fname in ("signals.schema.json", "predictions.schema.json"):
            assert not os.path.exists(os.path.join(CONTRACTS_DIR, fname)), (
                f"{fname} re-appeared repo-locally — lib is the SoT (config#989)"
            )

    def test_executor_params_schema_stays_local(self):
        assert os.path.exists(os.path.join(CONTRACTS_DIR, "executor_params.schema.json"))
        assert _load_schema("executor_params")["title"]


class TestSignalsContract:
    def test_valid_signals_pass(self):
        validate(instance=_signals_payload(), schema=_load_schema("signals"))

    def test_missing_required_field_fails(self):
        with pytest.raises(ValidationError):
            validate(instance={"date": "2026-06-11"}, schema=_load_schema("signals"))

    def test_market_regime_enum_is_3class(self):
        # 3-class Ang-Bekaert taxonomy (v0.42.0 / 2026-05-28); 'caution' retired.
        assert _load_schema("signals")["properties"]["market_regime"]["enum"] == [
            "bull", "neutral", "bear",
        ]

    def test_market_regime_caution_rejected(self):
        with pytest.raises(ValidationError):
            validate(instance=_signals_payload(market_regime="caution"),
                     schema=_load_schema("signals"))

    @pytest.mark.parametrize("regime", ["bull", "neutral", "bear"])
    def test_each_3class_regime_accepted(self, regime):
        validate(instance=_signals_payload(market_regime=regime),
                 schema=_load_schema("signals"))

    def test_advisory_validator_returns_warnings_not_raises(self):
        warnings = validate_signals({"date": "2026-06-11"})
        assert warnings and isinstance(warnings, list)
        assert validate_signals(_signals_payload()) == []


class TestPredictionsContract:
    def test_valid_predictions_pass(self):
        validate(instance=_predictions_payload(), schema=_load_schema("predictions"))

    def test_missing_required_entry_field_fails(self):
        entry = _prediction_entry()
        del entry["gbm_veto"]
        with pytest.raises(ValidationError):
            validate(instance=_predictions_payload(predictions=[entry]),
                     schema=_load_schema("predictions"))

    def test_nullable_observe_blocks_tolerated(self):
        validate(
            instance=_predictions_payload(output_distribution_gate=None,
                                          level_neutralization=None),
            schema=_load_schema("predictions"),
        )

    def test_advisory_validator_returns_warnings_not_raises(self):
        warnings = validate_predictions({"date": "2026-06-11"})
        assert warnings and isinstance(warnings, list)
        assert validate_predictions(_predictions_payload()) == []
