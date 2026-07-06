"""Unit tests for the deterministic SNS classifier.

Run from the repo root:

  python3 infrastructure/lambdas/_shared/test_classify.py
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vocab as _vocab  # noqa: E402
from classify import classify_sns, derive_fields, infer_subsystem  # noqa: E402


class ClassifyTests(unittest.TestCase):
    def _ev(self, subject, message=""):
        return classify_sns(subject, message)

    # ---- CloudWatch alarm state transitions --------------------------------
    def test_alarm_is_incident_high(self):
        et, sev, _sub, rcc = self._ev('ALARM: "alpha-engine-saturday-sf-failed" in US East')
        self.assertEqual((et, sev, rcc), ("incident", "high", "infrastructure_failure"))

    def test_ok_is_recovery_informational_no_rcc(self):
        et, sev, _sub, rcc = self._ev('OK: "alpha-engine-research-runner-timeout" in US East')
        self.assertEqual((et, sev, rcc), ("recovery", "informational", None))

    def test_insufficient_data_is_low_incident(self):
        et, sev, _sub, _rcc = self._ev('INSUFFICIENT_DATA: "some-alarm"')
        self.assertEqual((et, sev), ("incident", "low"))

    # ---- nousergon_lib.alerts severity tags -----------------------------
    def test_error_tag_is_incident_high(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine alert [ERROR] — alpha-engine-backtester/analysis")
        self.assertEqual((et, sev), ("incident", "high"))

    def test_warn_tag_is_incident_medium(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine alert [WARN] — research:score_aggregator")
        self.assertEqual((et, sev), ("incident", "medium"))

    def test_warning_tag_is_incident_medium(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine alert [WARNING] — box-health")
        self.assertEqual((et, sev), ("incident", "medium"))

    def test_critical_tag_is_incident_critical(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine alert [CRITICAL] — executor")
        self.assertEqual((et, sev), ("incident", "critical"))

    def test_info_tag_is_change_informational(self):
        et, sev, _sub, rcc = self._ev("Alpha Engine alert [INFO] — box-health")
        self.assertEqual((et, sev, rcc), ("change", "informational", None))

    # ---- pipeline result suffixes ------------------------------------------
    def test_pipeline_failed_is_incident(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine Saturday Pipeline — FAILED")
        self.assertEqual((et, sev), ("incident", "high"))

    def test_pipeline_success_is_change_informational(self):
        et, sev, _sub, rcc = self._ev("Alpha Engine Saturday Pipeline — SUCCESS")
        self.assertEqual((et, sev, rcc), ("change", "informational", None))

    def test_shell_run_passed_is_change(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine Saturday Pipeline — SHELL RUN PASSED (Friday dry run)")
        self.assertEqual((et, sev), ("change", "informational"))

    def test_market_holiday_skip_is_change(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine Weekday Pipeline — Skipped (Market Holiday)")
        self.assertEqual((et, sev), ("change", "informational"))

    def test_failed_beats_nothing_in_predictor_training(self):
        et, sev, sub, _rcc = self._ev("Alpha Engine Saturday Pipeline — PredictorTraining FAILED (early)")
        self.assertEqual((et, sev), ("incident", "high"))
        self.assertEqual(sub, "predictor")

    # ---- default fail-loud --------------------------------------------------
    def test_unrecognized_defaults_to_incident_high(self):
        et, sev, _sub, _rcc = self._ev("Alpha Engine — Data Staleness Alert")
        self.assertEqual((et, sev), ("incident", "high"))

    def test_empty_subject_falls_back_to_message(self):
        et, sev, _sub, _rcc = self._ev("", "DeployDriftCheck timed out after 60s")
        self.assertEqual((et, sev), ("incident", "high"))

    # ---- subsystem inference ------------------------------------------------
    def test_subsystem_backtester(self):
        self.assertEqual(infer_subsystem("[ERROR] — alpha-engine-backtester/analysis"), "backtester")

    def test_subsystem_research(self):
        self.assertEqual(infer_subsystem("[WARN] — research:score_aggregator"), "research")

    def test_subsystem_default_infrastructure(self):
        self.assertEqual(infer_subsystem("Alpha Engine Saturday Pipeline — FAILED"), "infrastructure")

    # ---- output always vocab-valid -----------------------------------------
    def test_every_classification_is_vocab_valid(self):
        subjects = [
            'ALARM: "x"', 'OK: "x"', 'INSUFFICIENT_DATA: "x"',
            "[ERROR] — y", "[WARN] — y", "[WARNING] — y", "[CRITICAL] — y", "[INFO] — y",
            "Saturday Pipeline — FAILED", "Saturday Pipeline — SUCCESS",
            "Weekday Pipeline — Skipped (Market Holiday)", "Some unknown alert",
        ]
        for subj in subjects:
            fields = derive_fields(subj)
            # Build a minimal entry and validate vocab membership.
            entry = {
                "schema_version": _vocab.SCHEMA_VERSION,
                "event_id": "x", "ts_utc": "2026-06-07T00:00:00Z",
                "summary": "s", "resolution_type": None,
                **fields,
            }
            errors = _vocab.validate_entry(entry)
            self.assertEqual(errors, [], f"{subj!r} -> {fields} produced {errors}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
