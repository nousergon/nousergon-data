"""Structural wiring for the declared weekly exercise-cadence gate
(alpha-engine-config-I6689) inserted into step_function_eod_reconcile.json between
StopTradingInstance and LaunchWeeklyExerciseRun:

  StopTradingInstance -> ReadExerciseCadence -> CheckExerciseCadence
    -> "daily"                -> LaunchWeeklyExerciseRun
    -> "weekly-only" / "off"  -> CheckDegradedOutcome (skip the launch)
    -> anything else (Default) -> SetCadenceUnknownValueDegraded -> alert -> LaunchWeeklyExerciseRun

ReadExerciseCadence's own Catch(States.ALL) -> SetCadenceReadDegraded -> alert
-> CheckExerciseCadence (re-enters with value floored to "daily").

These tests pin the properties that make the gate safe to deploy before its
own IAM grant lands (alpha-engine-config#6689 PR body): every failure mode
here fails OPEN toward running the exercise, never toward silently skipping
it, and every fail-open path pages instead of degrading silently. Separately,
test_sf_weekly_exercise_chain_wiring.py and
test_sf_eod_precondition_probe_wiring.py pin that the pre-existing launch
(LaunchWeeklyExerciseRun onward) and postclose-failure-isolation properties
are unchanged by this insertion.

alpha-engine-config-I11269: the whole tail (StopTradingInstance onward) moved
from step_function_eod.json into the collector-gated
step_function_eod_reconcile.json when the post-close SF was split, so the
fixture reads that file. The 16:00 machine carries none of these states —
pinned in test_sf_postclose_reconcile_split.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_ALERTS_TOPIC_ARN = "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts"
_SSM_PARAM_NAME = "/alpha-engine/weekly-sf/exercise-cadence"


@pytest.fixture(scope="module")
def eod() -> dict:
    # alpha-engine-config-I11269: the cadence gate lives in the reconcile machine.
    return json.loads((_INFRA / "step_function_eod_reconcile.json").read_text())["States"]


class TestReadExerciseCadence:
    def test_reads_the_declared_ssm_parameter(self, eod):
        st = eod["ReadExerciseCadence"]
        assert st["Resource"] == "arn:aws:states:::aws-sdk:ssm:getParameter"
        assert st["Parameters"]["Name"] == _SSM_PARAM_NAME

    def test_has_a_small_bounded_timeout(self, eod):
        assert eod["ReadExerciseCadence"]["TimeoutSeconds"] <= 30

    def test_catches_everything_and_never_reaches_handle_failure(self, eod):
        catch = eod["ReadExerciseCadence"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "SetCadenceReadDegraded"
        assert catch["Next"] != "HandleFailure"

    def test_result_is_selected_and_isolated(self, eod):
        st = eod["ReadExerciseCadence"]
        assert st["ResultSelector"] == {"value.$": "$.Parameter.Value"}
        assert st["ResultPath"] == "$.exercise_cadence_param"

    def test_success_enters_the_check(self, eod):
        assert eod["ReadExerciseCadence"]["Next"] == "CheckExerciseCadence"


class TestReadFailureFailsOpenAndPages:
    def test_floors_to_daily(self, eod):
        st = eod["SetCadenceReadDegraded"]
        assert st["Type"] == "Pass"
        assert st["Result"] == {"value": "daily"}
        assert st["ResultPath"] == "$.exercise_cadence_param"
        assert st["Next"] == "PublishCadenceReadDegraded"

    def test_publishes_to_the_alerts_topic_hardcoded(self, eod):
        """The EOD SF was hardened 2026-05-14 to hardcode the alerts-topic ARN
        in every Publish state, never bind $.sns_topic_arn from input — this
        alert must follow the SAME convention as every sibling Publish*Degraded
        state in this file, not the weekly SF's $.sns_topic_arn.$ convention."""
        st = eod["PublishCadenceReadDegraded"]
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        assert st["Parameters"]["TopicArn"] == _ALERTS_TOPIC_ARN
        assert "TopicArn.$" not in st["Parameters"]

    def test_publish_is_best_effort_and_still_reaches_the_check(self, eod):
        st = eod["PublishCadenceReadDegraded"]
        assert st["Next"] == "CheckExerciseCadence"
        catch = st["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "CheckExerciseCadence"


class TestCheckExerciseCadence:
    def _choice(self, eod, value):
        return next(c for c in eod["CheckExerciseCadence"]["Choices"] if c.get("StringEquals") == value)

    def test_daily_launches(self, eod):
        assert self._choice(eod, "daily")["Next"] == "LaunchWeeklyExerciseRun"

    @pytest.mark.parametrize("value", ["weekly-only", "off"])
    def test_weekly_only_and_off_skip_to_the_launchs_own_success_target(self, eod, value):
        """Must be LaunchWeeklyExerciseRun's OWN Next on success — never
        WeeklyExerciseLaunchFailed's routing, which exists only to alert when
        the launch is ATTEMPTED and fails. Skipping the launch is not a
        launch failure."""
        assert self._choice(eod, value)["Next"] == eod["LaunchWeeklyExerciseRun"]["Next"]
        assert self._choice(eod, value)["Next"] == "CheckDegradedOutcome"

    def test_variable_is_the_read_result(self, eod):
        for c in eod["CheckExerciseCadence"]["Choices"]:
            assert c["Variable"] == "$.exercise_cadence_param.value"

    def test_default_is_the_unknown_value_path_not_a_silent_skip(self, eod):
        assert eod["CheckExerciseCadence"]["Default"] == "SetCadenceUnknownValueDegraded"

    def test_all_three_declared_values_are_distinct_branches(self, eod):
        values = {c.get("StringEquals") for c in eod["CheckExerciseCadence"]["Choices"]}
        assert values == {"daily", "weekly-only", "off"}


class TestUnknownValueFailsOpenAndPages:
    def test_floors_to_daily_and_preserves_the_offending_raw_value(self, eod):
        st = eod["SetCadenceUnknownValueDegraded"]
        assert st["Type"] == "Pass"
        assert st["Parameters"]["value"] == "daily"
        assert st["Parameters"]["raw.$"] == "$.exercise_cadence_param.value"
        assert st["ResultPath"] == "$.exercise_cadence_param"
        assert st["Next"] == "PublishCadenceUnknownValueDegraded"

    def test_publish_names_the_offending_value_and_uses_the_hardcoded_topic(self, eod):
        st = eod["PublishCadenceUnknownValueDegraded"]
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        assert st["Parameters"]["TopicArn"] == _ALERTS_TOPIC_ARN
        assert "TopicArn.$" not in st["Parameters"]
        assert "$.exercise_cadence_param.raw" in st["Parameters"]["Message.$"]

    def test_unknown_value_still_launches_after_alerting(self, eod):
        """Decided posture (config#6689): treat as daily AND page — a missed
        run is worse than an extra one, but 'ran on garbage input' must never
        look identical to 'ran because cadence=daily was read correctly.'"""
        st = eod["PublishCadenceUnknownValueDegraded"]
        assert st["Next"] == "LaunchWeeklyExerciseRun"
        catch = st["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "LaunchWeeklyExerciseRun"


class TestNoDanglingReferences:
    def test_every_new_state_next_target_resolves(self, eod):
        new_states = [
            "ReadExerciseCadence", "SetCadenceReadDegraded", "PublishCadenceReadDegraded",
            "CheckExerciseCadence", "SetCadenceUnknownValueDegraded", "PublishCadenceUnknownValueDegraded",
        ]
        names = set(eod)
        for state_name in new_states:
            assert state_name in eod
            st = eod[state_name]
            targets = []
            if st.get("Next"):
                targets.append(st["Next"])
            if st.get("Default"):
                targets.append(st["Default"])
            for c in st.get("Choices", []) or []:
                if c.get("Next"):
                    targets.append(c["Next"])
            for c in st.get("Catch", []) or []:
                if c.get("Next"):
                    targets.append(c["Next"])
            dangling = [t for t in targets if t not in names]
            assert not dangling, f"{state_name} references unresolvable target(s): {dangling}"

    def test_none_of_the_new_states_route_to_handle_failure(self, eod):
        new_states = [
            "ReadExerciseCadence", "SetCadenceReadDegraded", "PublishCadenceReadDegraded",
            "CheckExerciseCadence", "SetCadenceUnknownValueDegraded", "PublishCadenceUnknownValueDegraded",
        ]
        for state_name in new_states:
            st = eod[state_name]
            for c in st.get("Catch", []) or []:
                assert c["Next"] != "HandleFailure", f"{state_name}'s Catch must not fail postclose itself"
