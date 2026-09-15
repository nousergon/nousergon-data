"""Producer/consumer contract test for alternative_ticker.schema.json (D15, data-collector
plan P-07, alpha-engine-config-I10870).

D15 is SELF-CONSUMED (alpha-engine-config#10873): the sole reader of
``market_data/weekly/{date}/alternative/{ticker}.json`` is this same repo's
``features/compute.py::_load_cached_alternative`` / ``_alt_entry_from_payload`` — there
is no cross-repo boundary to pin a copy across (mirrors the D26
``technical_rating_ledger`` in-producer pattern, PR1747).

Covers:
  - A real-shaped fixture (``collect``'s ``data`` before it becomes the S3 body,
    per ``collectors/alternative.py::_fetch_all_alternative``'s documented per-source
    keys) validates against the schema.
  - The schema is itself a valid JSON Schema.
  - The REAL consumer reader, ``features.compute._alt_entry_from_payload``, extracts
    the fields it actually depends on from a schema-conformant payload — the surprise_pct
    percent-point-to-decimal conversion (alpha-engine-config-I7569) included.
  - A payload missing a required top-level field is rejected.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("jsonschema")

from contracts import validate_alternative_ticker
from features.compute import _alt_entry_from_payload


def _schema() -> dict:
    path = Path(__file__).parent.parent / "contracts" / "alternative_ticker.schema.json"
    return json.loads(path.read_text())


def _payload(**overrides) -> dict:
    payload = {
        "ticker": "AAPL",
        "fetched_at": "2026-09-15T05:00:00+00:00",
        "analyst_consensus": {
            "earnings_surprises": [
                {"date": "2026-06-30", "actual": 1.91, "estimated": 1.9271, "surprise_pct": -0.8873},
            ],
        },
        "eps_revision": {
            "surprise_pct": None,
            "days_since_earnings": 12.0,
            "revision_4w": 0.02,
            "streak": 3,
        },
        "options_flow": {
            "put_call_ratio": 0.85,
            "iv_rank": 42.0,
            "expected_move_pct": 0.031,
        },
        "insider_activity": {},
        "institutional": {"accumulation": False, "funds_increasing": 0, "funds_decreasing": 0},
        "news": {},
    }
    payload.update(overrides)
    return payload


class TestSchemaIsValid:
    def test_schema_file_parses_as_json_schema(self):
        import jsonschema

        jsonschema.Draft202012Validator.check_schema(_schema())


class TestRealShapedPayloadValidates:
    def test_full_payload_validates(self):
        assert validate_alternative_ticker(_payload()) == []

    def test_missing_required_top_level_field_is_rejected(self):
        payload = _payload()
        del payload["fetched_at"]
        assert validate_alternative_ticker(payload) != []

    def test_null_sub_sections_still_validate(self):
        """A provider that returns nothing for a section (not empty-dict, real
        null) must not fail the schema — _alt_entry_from_payload's `or {}`
        handling depends on this staying legal."""
        payload = _payload(eps_revision=None, analyst_consensus=None, options_flow=None)
        assert validate_alternative_ticker(payload) == []


class TestRealConsumerReaderExtractsPinnedFields:
    """features.compute._alt_entry_from_payload is the ONLY reader of this
    artifact — exercising it directly against a schema-conformant fixture is
    the consumer half of this contract."""

    def test_reader_falls_back_to_analyst_consensus_surprise_and_converts_units(self):
        entry = _alt_entry_from_payload(_payload())
        # eps_revision.surprise_pct is None above -> falls back to
        # analyst_consensus.earnings_surprises[0].surprise_pct, converted from
        # a percent-point number (-0.8873) to the _pct decimal convention.
        assert entry["earnings"]["surprise_pct"] == pytest.approx(-0.008873)

    def test_reader_extracts_revisions_and_options(self):
        entry = _alt_entry_from_payload(_payload())
        assert entry["revisions"]["eps_revision_4w"] == 0.02
        assert entry["revisions"]["revision_streak"] == 3
        assert entry["options"]["put_call_ratio"] == 0.85
        assert entry["options"]["iv_rank"] == 42.0
        assert entry["options"]["atm_iv"] == pytest.approx(0.031)

    def test_reader_survives_null_sub_sections(self):
        payload = _payload(eps_revision=None, analyst_consensus=None, options_flow=None)
        entry = _alt_entry_from_payload(payload)
        assert entry["earnings"]["surprise_pct"] == 0.0
        assert entry["options"]["put_call_ratio"] is None
