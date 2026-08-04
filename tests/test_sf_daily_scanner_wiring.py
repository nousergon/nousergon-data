"""Pins the weekday Scanner wiring in ``step_function_daily.json``.

alpha-engine-config-I6494 (Brian ruled option A, 2026-08-04): enable a daily
Scanner schedule so ``universe_membership/{date}`` + factor profiles (+
provenance) refresh on trading days without requiring the full Saturday
weekly SF.

SOTA: ONE Scanner entrypoint (``alpha-engine-research-scanner:live``), TWO
cadences:

  * Saturday — ``ne-weekly-freshness-pipeline`` Branch A ``Scanner`` state
    (pinned by ``test_sf_scanner_wiring.py``)
  * Weekday  — ``ne-preopen-trading-pipeline`` ``Scanner`` state (this file),
    after MorningArcticAppend and before PredictorInference, riding the
    existing America/Los_Angeles ``cron(15 5) MON-FRI`` preopen schedule
    (TradingDayGate already skips NYSE holidays)

Distinct from I5489 (weekday exercise of the FULL weekly SF).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"
_WEEKLY_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def weekly_scanner() -> dict:
    weekly = json.loads(_WEEKLY_SF_PATH.read_text())
    flat: dict = dict(weekly["States"])
    for st in weekly["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat["Scanner"]


class TestStatesPresent:
    def test_scanner_state_exists(self, states):
        assert "Scanner" in states
        assert states["Scanner"]["Type"] == "Task"

    def test_skip_gate_defaults_to_running(self, states):
        assert states["CheckSkipScanner"]["Type"] == "Choice"
        assert states["CheckSkipScanner"]["Default"] == "Scanner"
        skip = states["CheckSkipScanner"]["Choices"][0]
        assert skip["Next"] == "CheckSkipPredictorInference"
        variables = {c["Variable"] for c in skip["And"]}
        assert variables == {"$.skip_scanner"}


class TestSameEntrypointAsWeekly:
    def test_lambda_function_matches_weekly(self, states, weekly_scanner):
        assert (
            states["Scanner"]["Parameters"]["FunctionName"]
            == weekly_scanner["Parameters"]["FunctionName"]
            == "alpha-engine-research-scanner:live"
        )
        assert states["Scanner"]["Resource"] == weekly_scanner["Resource"] == (
            "arn:aws:states:::lambda:invoke"
        )

    def test_timeout_matches_weekly(self, states, weekly_scanner):
        assert states["Scanner"]["TimeoutSeconds"] == weekly_scanner["TimeoutSeconds"] == 600


class TestPayloadAndRunDate:
    def test_payload_threads_run_date(self, states):
        payload = states["Scanner"]["Parameters"]["Payload"]
        assert payload["run_date.$"] == "$.run_date"

    def test_initialize_input_seeds_run_date(self, states):
        merged = states["InitializeInput"]["Parameters"]["merged.$"]
        assert "run_date" in merged
        assert "$$.Execution.StartTime" in merged
        assert "States.Format" in merged


class TestEdges:
    def test_arctic_append_success_enters_scanner_gate(self, states):
        success = [
            c["Next"]
            for c in states["CheckMorningArcticAppendSpotStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["CheckSkipScanner"]

    def test_data_phase_continue_paths_enter_scanner_gate(self, states):
        assert states["CheckSkipMorningEnrich"]["Choices"][0]["Next"] == (
            "CheckSkipScanner"
        )
        assert states["CheckMorningEnrichSpotLaunched"]["Default"] == (
            "CheckSkipScanner"
        )
        assert states["CheckMorningArcticAppendSpotLaunched"]["Default"] == (
            "CheckSkipScanner"
        )
        pub = states["PublishDataSpotFailureImmediate"]
        assert pub["Next"] == "CheckSkipScanner"
        assert pub["Catch"][0]["Next"] == "CheckSkipScanner"

    def test_scanner_success_and_catch_converge_on_predictor_gate(self, states):
        assert states["Scanner"]["Next"] == "CheckSkipPredictorInference"
        catches = states["Scanner"]["Catch"]
        assert any(
            c["Next"] == "CheckSkipPredictorInference"
            and "States.ALL" in c["ErrorEquals"]
            for c in catches
        )

    def test_no_direct_edge_from_arctic_to_predictor_bypassing_scanner(self, states):
        """Regression: Arctic success must not skip the new Scanner gate."""
        success = [
            c["Next"]
            for c in states["CheckMorningArcticAppendSpotStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert "CheckSkipPredictorInference" not in success


class TestResultPaths:
    def test_success_result_lands_under_scanner_result(self, states):
        assert states["Scanner"]["ResultPath"] == "$.scanner_result"

    def test_failure_result_lands_under_scanner_error(self, states):
        catch_all = next(
            c for c in states["Scanner"]["Catch"] if "States.ALL" in c["ErrorEquals"]
        )
        assert catch_all["ResultPath"] == "$.scanner_error"
