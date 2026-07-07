"""Pins the daily prediction-health producer wiring (config#1853).

Background: predictor/metrics/drift_{trading_day}.json is registered
``cadence: eod_sf`` in ARTIFACT_REGISTRY.yaml (artifact_id
predictor_drift_detection), but crucible-predictor#305 (config#1282) only
added the ``action=check_drift`` handler branch — no Step Function state
ever invoked it, so the artifact silently stopped being produced. This adds
a fail-soft Lambda-invoke state, PredictorDriftCheck, immediately after
PredictorHealthCheck (predictions for trading_day must already exist before
drift can be scored against them) mirroring the existing fail-soft Catch
pattern used elsewhere in this SF (e.g. ChronicGapSelfHeal, and
PredictorHealthCheck's own Catch): a Lambda failure must never block the
morning planner / order-placement path, since this is an observability
producer, not a trading gate.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


class TestPredictorDriftCheckWiredAfterHealthCheck:
    def test_health_check_next_is_drift_check(self, states):
        assert states["PredictorHealthCheck"]["Next"] == "PredictorDriftCheck"

    def test_health_check_catch_also_routes_to_drift_check(self, states):
        # A PredictorHealthCheck failure must still reach the drift-check
        # producer, not skip straight to the planner — it's the same
        # fail-soft posture, just re-pointed at the newly-inserted state.
        catch_targets = [c["Next"] for c in states["PredictorHealthCheck"]["Catch"]]
        assert catch_targets == ["PredictorDriftCheck"]


class TestPredictorDriftCheckTask:
    def test_is_lambda_invoke(self, states):
        state = states["PredictorDriftCheck"]
        assert state["Type"] == "Task"
        assert state["Resource"] == "arn:aws:states:::lambda:invoke"

    def test_targets_same_lambda_alias_as_predictor_inference(self, states):
        drift_fn = states["PredictorDriftCheck"]["Parameters"]["FunctionName"]
        inference_fn = states["PredictorInference"]["Parameters"]["FunctionName"]
        assert drift_fn == inference_fn
        assert drift_fn == "alpha-engine-predictor-inference:live"

    def test_invokes_check_drift_action(self, states):
        payload = states["PredictorDriftCheck"]["Parameters"]["Payload"]
        assert payload["action"] == "check_drift"

    def test_date_payload_threads_trading_day_not_calendar_date(self, states):
        # Must use the SF's own trading-day gate result (the day predictions
        # were actually produced for), not a fresh wall-clock "today" that
        # could straddle a midnight-crossing execution.
        payload = states["PredictorDriftCheck"]["Parameters"]["Payload"]
        assert payload["date.$"] == "$.trading_day_gate.Payload.check_date"

    def test_proceeds_to_morning_planner_gate_on_success(self, states):
        assert states["PredictorDriftCheck"]["Next"] == "CheckSkipMorningPlanner"


class TestPredictorDriftCheckFailSoft:
    def test_catch_all_errors(self, states):
        catches = states["PredictorDriftCheck"]["Catch"]
        assert len(catches) == 1
        assert catches[0]["ErrorEquals"] == ["States.ALL"]

    def test_catch_routes_to_the_same_next_state_as_success(self, states):
        # Fail-soft: whatever normally follows PredictorHealthCheck's
        # producer chain today (CheckSkipMorningPlanner) must still run
        # even if the drift-check Lambda errors.
        state = states["PredictorDriftCheck"]
        assert state["Catch"][0]["Next"] == state["Next"] == "CheckSkipMorningPlanner"

    def test_catch_has_dedicated_resultpath(self, states):
        assert states["PredictorDriftCheck"]["Catch"][0]["ResultPath"] == "$.predictor_drift_error"
