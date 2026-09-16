"""Producer contract test for feature_registry.schema.json (D12, data-collector plan
P-07, alpha-engine-config-I10870).

D12's per-COLUMN contract (CATALOG <-> SCHEMA.md §3) is already covered by
tests/test_schema_contract.py. This test covers the SEPARATE S3-published
registry.json artifact crucible-dashboard actually GETs
(``views/13_Feature_Store.py::_load_registry`` -> ``features/registry.json``),
validating the REAL ``generate_registry_json()`` output — the whole live
CATALOG, not a toy subset — against the new schema, plus a hand-built minimal
fixture independent of the producer code.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("jsonschema")

from contracts import validate_feature_registry
from features.registry import generate_registry_json


class TestRealCatalogValidates:
    def test_live_catalog_serialization_validates(self):
        payload = json.loads(generate_registry_json())
        errors = validate_feature_registry(payload)
        assert errors == [], errors

    def test_every_entry_has_the_dashboard_required_fields(self):
        payload = json.loads(generate_registry_json())
        assert payload["features"], "CATALOG must not be empty"
        for entry in payload["features"]:
            assert entry["name"]
            assert entry["group"]


class TestHandBuiltFixtureValidates:
    def _payload(self, **overrides) -> dict:
        payload = {
            "features": [
                {
                    "name": "rsi_14",
                    "group": "technical",
                    "description": "RSI(14), range 0-100",
                    "dtype": "float32",
                    "source": "yfinance",
                    "refresh": "daily",
                    "per_ticker": True,
                    "compute": "",
                    "units": "0-100 score",
                    "formula": "Wilder's RSI(14)",
                    "consumers": "predictor + scanner",
                    "display_order": 0,
                },
            ],
        }
        payload.update(overrides)
        return payload

    def test_full_record_validates(self):
        assert validate_feature_registry(self._payload()) == []

    def test_missing_required_top_level_field_is_rejected(self):
        assert validate_feature_registry({}) != []

    def test_entry_missing_required_field_is_rejected(self):
        payload = self._payload()
        del payload["features"][0]["group"]
        assert validate_feature_registry(payload) != []
