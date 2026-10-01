"""Structural wiring for the postclose → weekly-exercise chain (config-I5489).

The weekly pipeline reached a successful terminal state in only 11 of its last
60 executions (41 FAILED, 5 ABORTED). Brian's 2026-07-29 ruling: run it every
trading day until the bugs are worked out — iteration rate, not compute cost,
is the binding constraint.

These tests pin the three properties that make the chain safe to run daily:
postclose can never be failed by it, the run-day gate is bypassed by role
rather than by editing the gate, and a launch failure is loud.

alpha-engine-config-I11269: the chain moved from the 16:00
ne-postclose-trading-pipeline (step_function_eod.json) into the collector-gated
ne-postclose-reconcile-pipeline (step_function_eod_reconcile.json) — an
exercise launched at ~16:10 would read the day before the collector wrote it.
"postclose" below means that reconcile machine; the last test pins that the
16:00 machine no longer launches anything.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_WEEKLY_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"


@pytest.fixture(scope="module")
def eod() -> dict:
    # alpha-engine-config-I11269: the exercise chain lives in the reconcile machine.
    return json.loads((_INFRA / "step_function_eod_reconcile.json").read_text())["States"]


@pytest.fixture(scope="module")
def postclose_1600() -> dict:
    return json.loads((_INFRA / "step_function_eod.json").read_text())["States"]


@pytest.fixture(scope="module")
def weekly() -> dict:
    return json.loads((_INFRA / "step_function.json").read_text())["States"]


def test_postclose_tail_routes_through_the_exercise_launch(eod):
    """StopTradingInstance is the cost guard and always runs — chaining after
    it means the exercise run fires on both the green and degraded paths.
    alpha-engine-config-I6689 inserted the declared-cadence read + gate
    (ReadExerciseCadence -> CheckExerciseCadence) ahead of the launch itself,
    so this is now a three-hop path on the cadence=daily branch — pinned in
    full (including the fail-open routes) by
    tests/test_sf_weekly_cadence_wiring.py."""
    assert eod["StopTradingInstance"]["Next"] == "ReadExerciseCadence"
    assert eod["ReadExerciseCadence"]["Next"] == "CheckExerciseCadence"
    daily_choice = next(
        c for c in eod["CheckExerciseCadence"]["Choices"] if c.get("StringEquals") == "daily"
    )
    assert daily_choice["Next"] == "LaunchWeeklyExerciseRun"
    assert eod["LaunchWeeklyExerciseRun"]["Next"] == "CheckDegradedOutcome"


def test_launch_is_fire_and_forget_not_sync(eod):
    """`.sync` would make postclose WAIT on a ~4h pipeline and inherit its
    failure. The whole point is that postclose's terminal status stays a
    statement about postclose."""
    resource = eod["LaunchWeeklyExerciseRun"]["Resource"]
    assert resource == "arn:aws:states:::states:startExecution"
    assert ".sync" not in resource


def test_launch_targets_the_weekly_pipeline(eod):
    params = eod["LaunchWeeklyExerciseRun"]["Parameters"]
    assert params["StateMachineArn"] == _WEEKLY_ARN


def test_exercise_role_is_not_weekly(eod):
    """Load-bearing. step_function.json's CheckWeeklyRunDayGate applies the
    run-day gate ONLY when pipeline_role == 'weekly'; any other role bypasses
    it. If this ever became 'weekly', every weekday launch would be skipped by
    the gate and the exercise cadence would silently stop."""
    role = eod["LaunchWeeklyExerciseRun"]["Parameters"]["Input"]["pipeline_role"]
    assert role == "exercise"
    assert role != "weekly"


def test_the_run_day_gate_really_does_key_on_that_role(weekly):
    """Pins the OTHER half of the invariant above, in the other file — the
    bypass is a property of the gate's condition, not an assumption about it.
    A guard asserting both halves but never the connection is its own recorded
    bug class."""
    gate = weekly["CheckWeeklyRunDayGate"]
    conditions = gate["Choices"][0]["And"]
    # The gate guards the dereference first (IsPresent) and compares second,
    # so select on the comparison rather than on position.
    role_equals = [
        c for c in conditions
        if c.get("Variable") == "$.pipeline_role" and "StringEquals" in c
    ]
    assert role_equals, "gate no longer keys on $.pipeline_role"
    assert role_equals[0]["StringEquals"] == "weekly"
    # ...and anything that is not 'weekly' must fall through to the Default,
    # i.e. straight into the pipeline without the gate.
    # alpha-engine-config-I8809: NormalizeRunDates sits between the gate and
    # CheckRunMode — the graph's ONE date normalization. Chain unchanged past it.
    assert gate["Default"] == "NormalizeRunDates"


def test_launch_failure_alerts_and_never_fails_postclose(eod):
    """A silent non-launch is indistinguishable from a day nobody looked at.

    alpha-engine-config#6722: the launch failure previously alerted via SNS
    (WeeklyExerciseLaunchFailed) but was never threaded into
    $.degraded_summary — the Catch now routes through
    SetWeeklyExerciseDegradedFlag first, unchanged continuation otherwise."""
    catch = eod["LaunchWeeklyExerciseRun"]["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "SetWeeklyExerciseDegradedFlag"

    flag = eod["SetWeeklyExerciseDegradedFlag"]
    assert flag["Type"] == "Pass"
    assert flag["ResultPath"] == "$.degraded_summary"
    assert flag["Parameters"]["degraded"] is True
    assert flag["Next"] == "WeeklyExerciseLaunchFailed"

    failed = eod["WeeklyExerciseLaunchFailed"]
    assert failed["Resource"] == "arn:aws:states:::sns:publish"
    assert failed["Next"] == "CheckDegradedOutcome"
    # even the alert failing must not strand the execution
    assert failed["Catch"][0]["Next"] == "CheckDegradedOutcome"


def test_launch_does_not_route_to_handle_failure(eod):
    """HandleFailure marks the postclose execution failed. A downstream
    pipeline's launch problem is not a postclose failure."""
    assert eod["LaunchWeeklyExerciseRun"]["Catch"][0]["Next"] != "HandleFailure"
    assert eod["WeeklyExerciseLaunchFailed"]["Catch"][0]["Next"] != "HandleFailure"


def test_every_next_target_resolves(eod):
    """States.Runtime on an unresolvable Next has ended this pipeline at its
    very last state before (config-I2767)."""
    names = set(eod)
    dangling = [
        (k, v["Next"]) for k, v in eod.items() if v.get("Next") and v["Next"] not in names
    ]
    for k, v in eod.items():
        for c in v.get("Catch", []) or []:
            if c.get("Next") and c["Next"] not in names:
                dangling.append((k, c["Next"]))
        for c in v.get("Choices", []) or []:
            if c.get("Next") and c["Next"] not in names:
                dangling.append((k, c["Next"]))
    assert not dangling, f"unresolvable Next targets: {dangling}"


def test_the_1600_machine_launches_no_exercise_run(postclose_1600):
    """alpha-engine-config-I11269: exactly one machine launches the exercise
    run per trading day. A copy left in the 16:00 machine would double-launch
    it (and the first launch would read pre-collection data)."""
    for name in ("ReadExerciseCadence", "CheckExerciseCadence", "LaunchWeeklyExerciseRun"):
        assert name not in postclose_1600, name
    launches = [
        n for n, st in postclose_1600.items()
        if st.get("Resource", "").startswith("arn:aws:states:::states:startExecution")
        and "ne-weekly-freshness-pipeline" in json.dumps(st.get("Parameters", {}))
    ]
    assert launches == []
