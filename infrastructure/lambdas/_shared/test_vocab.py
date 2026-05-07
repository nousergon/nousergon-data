"""Smoke tests for the shared vocab module used by both auto-emit Lambdas.

  python3 infrastructure/lambdas/_shared/test_vocab.py
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import vocab  # noqa: E402


def _valid_incident_entry(**overrides):
    base = {
        "schema_version": "1.0.0",
        "event_id": "2026-05-08T12-00-00_alpha-engine-alerts_abc1234",
        "ts_utc": "2026-05-08T12:00:00Z",
        "event_type": "incident",
        "severity": "high",
        "subsystem": "infrastructure",
        "root_cause_category": "infrastructure_failure",
        "resolution_type": None,
        "summary": "Step Function failure: DeployDriftCheck",
    }
    base.update(overrides)
    return base


class VocabConstantsTests(unittest.TestCase):
    """Source-of-truth (alpha-engine-config/changelog/vocab.yaml) values
    must all appear in the vendored frozensets."""

    def test_event_types_cover_canonical_enum(self):
        # Anchor on the ones today's auto-emit Lambdas actually emit
        for v in ("incident", "change", "recovery", "investigation"):
            self.assertIn(v, vocab.EVENT_TYPES, f"missing event_type {v!r}")

    def test_severities_cover_canonical_enum(self):
        for v in ("critical", "high", "medium", "low", "informational"):
            self.assertIn(v, vocab.SEVERITIES)

    def test_subsystems_include_lambda_inferred_values(self):
        # cloudwatch-mirror Lambda's _SUBSYSTEM_MAP maps log groups to these
        for v in ("predictor", "research", "data_pipeline", "eval", "infrastructure"):
            self.assertIn(v, vocab.SUBSYSTEMS)

    def test_root_cause_categories_cover_canonical_enum(self):
        for v in ("data_quality", "infrastructure_failure", "prompt_regression"):
            self.assertIn(v, vocab.ROOT_CAUSE_CATEGORIES)


class ValidateEntryTests(unittest.TestCase):
    """Happy + edge-case validation on incident-shaped entries."""

    def test_canonical_entry_passes(self):
        self.assertEqual(vocab.validate_entry(_valid_incident_entry()), [])
        self.assertTrue(vocab.is_valid(_valid_incident_entry()))

    def test_unknown_severity_fails(self):
        errs = vocab.validate_entry(_valid_incident_entry(severity="catastrophic"))
        self.assertTrue(any("severity" in e for e in errs), errs)

    def test_unknown_subsystem_fails(self):
        errs = vocab.validate_entry(_valid_incident_entry(subsystem="garbage"))
        self.assertTrue(any("subsystem" in e for e in errs), errs)

    def test_unknown_event_type_fails(self):
        errs = vocab.validate_entry(_valid_incident_entry(event_type="explosion"))
        self.assertTrue(any("event_type" in e for e in errs), errs)

    def test_unknown_root_cause_fails(self):
        errs = vocab.validate_entry(
            _valid_incident_entry(root_cause_category="aliens")
        )
        self.assertTrue(
            any("root_cause_category" in e for e in errs), errs,
        )

    def test_schema_version_mismatch_fails(self):
        errs = vocab.validate_entry(_valid_incident_entry(schema_version="0.9.0"))
        self.assertTrue(any("schema_version" in e for e in errs), errs)

    def test_missing_required_field_fails(self):
        e = _valid_incident_entry()
        del e["summary"]
        errs = vocab.validate_entry(e)
        self.assertTrue(any("summary" in err for err in errs), errs)

    def test_empty_required_string_fails(self):
        errs = vocab.validate_entry(_valid_incident_entry(summary="   "))
        self.assertTrue(any("summary" in err for err in errs), errs)

    def test_null_resolution_type_tolerated(self):
        # Auto-emit Lambdas emit resolution_type=None until operator follow-up;
        # validation must NOT reject this on a fresh incident.
        self.assertEqual(
            vocab.validate_entry(_valid_incident_entry(resolution_type=None)), [],
        )

    def test_null_root_cause_tolerated(self):
        # If a future auto-emit path sets root_cause_category=None, validation
        # must not reject — the current default ("infrastructure_failure")
        # is a Lambda-side choice, not a vocab-required field.
        self.assertEqual(
            vocab.validate_entry(_valid_incident_entry(root_cause_category=None)), [],
        )

    def test_non_string_vocab_field_fails(self):
        errs = vocab.validate_entry(_valid_incident_entry(severity=42))
        self.assertTrue(any("severity" in e for e in errs), errs)

    def test_multiple_errors_accumulate(self):
        errs = vocab.validate_entry(_valid_incident_entry(
            severity="catastrophic",
            subsystem="garbage",
            schema_version="0.9.0",
        ))
        self.assertGreaterEqual(len(errs), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
