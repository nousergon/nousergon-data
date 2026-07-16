"""Pins the REMOVAL of PredictorHealthCheck + PredictorDriftCheck from the
WEEKDAY SF, and their re-homing onto direct EventBridge triggers.

Origin: alpha-engine-config-I2722 (Brian ruling 2026-07-16) — approach (a)/(b)
per state, NO new bundled health SF (ARCHITECTURE §36: "adding redundant
scheduled paths 'for safety' is the wrong fix"). Both states were non-blocking
observability producers (config#1853 for the drift check), never trading
gates, so a plain scheduled Lambda invoke is the correct-weight mechanism —
see ``PredictorHealthCheckTrigger`` / ``PredictorDriftCheckTrigger`` in
``infrastructure/cloudformation/alpha-engine-orchestration.yaml`` (13:40 UTC
weekdays, after the preopen SF's observed ~13:15 P99 completion).

This file previously pinned PredictorDriftCheck's wiring immediately after
PredictorHealthCheck inside this SF (see git history for that version,
superseded here) — both states are now DELETED. Their former predecessors
(``CoverageGapChoice`` and ``FinalCoverageGate``, both Choice states whose
Default used to be ``PredictorHealthCheck``) now Default straight to
``CheckSkipMorningPlanner``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"
_CFN_PATH = (
    _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
)


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


class TestPredictorHealthAndDriftCheckRemovedFromSF:
    @pytest.mark.parametrize("name", ["PredictorHealthCheck", "PredictorDriftCheck"])
    def test_state_absent(self, states, name):
        assert name not in states, (
            f"{name} must NOT be in the weekday SF — re-homed onto its own "
            "direct EventBridge trigger (alpha-engine-config-I2722)."
        )

    def test_coverage_gap_choice_defaults_to_morning_planner_gate(self, states):
        assert states["CoverageGapChoice"]["Default"] == "CheckSkipMorningPlanner"

    def test_final_coverage_gate_defaults_to_morning_planner_gate(self, states):
        assert states["FinalCoverageGate"]["Default"] == "CheckSkipMorningPlanner"

    def test_no_dangling_reference_to_removed_states(self, states):
        def _targets(o):
            out = []
            if isinstance(o, dict):
                for k, v in o.items():
                    if k in ("Next", "Default") and isinstance(v, str):
                        out.append(v)
                    else:
                        out.extend(_targets(v))
            elif isinstance(o, list):
                for x in o:
                    out.extend(_targets(x))
            return out

        for name, st in states.items():
            for t in _targets(st):
                assert t not in ("PredictorHealthCheck", "PredictorDriftCheck"), (
                    f"{name} references removed state {t!r}"
                )


class TestReHomedOntoDirectEventBridgeTriggers:
    """The two removed states' invocation shape (function alias + Payload
    action) is preserved verbatim on their new standalone EventBridge rules —
    only the trigger mechanism changed, not what gets invoked."""

    @pytest.fixture(scope="class")
    def cfn_text(self) -> str:
        return _CFN_PATH.read_text()

    def test_predictor_health_check_trigger_present(self, cfn_text):
        assert "PredictorHealthCheckTrigger:" in cfn_text
        assert "alpha-engine-predictor-health-check:live" in cfn_text
        assert '"action": "check"' in cfn_text

    def test_predictor_drift_check_trigger_present(self, cfn_text):
        assert "PredictorDriftCheckTrigger:" in cfn_text
        assert '"action": "check_drift"' in cfn_text

    def test_both_triggers_fire_after_preopen_p99(self, cfn_text):
        # 13:40 UTC — after the preopen SF's observed ~13:15 UTC P99
        # completion, so both checks observe the same trading_day artifacts
        # they used to score inside the SF.
        assert cfn_text.count("cron(40 13 ? * MON-FRI *)") == 2

    def test_both_triggers_grant_lambda_permission(self, cfn_text):
        assert "PredictorHealthCheckTriggerPermission:" in cfn_text
        assert "PredictorDriftCheckTriggerPermission:" in cfn_text
