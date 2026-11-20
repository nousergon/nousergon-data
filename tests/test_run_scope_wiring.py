"""RunScope's wiring into the weekly SF (alpha-engine-config-I7620).

The Lambda's own derivation is tested in
``infrastructure/lambdas/weekly-run-scope/test_run_scope.py`` against real
captured executions. What is pinned HERE is the half that cannot be tested from
inside the Lambda: that the state exists, that it sits where every work stage is
already behind it, that it is handed the three context values it cannot obtain
for itself, and that it can never fail the run.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_SF = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF.read_text())["States"]


def test_the_state_exists_and_targets_the_run_scope_lambda(states):
    scope = states["RunScope"]
    assert scope["Type"] == "Task"
    assert (
        scope["Parameters"]["FunctionName"] == "alpha-engine-weekly-run-scope"
    )


def test_it_receives_the_context_values_it_cannot_derive_for_itself(states):
    """Execution id, state-machine id, and the ORIGINAL execution input.

    The first two are how it fetches its own history and definition — the two
    authorities the whole design rests on. The third is the run's `skip_*`
    flags, used only to EXPLAIN a NOT_REACHED row; a disposition is always
    decided by the execution record, never by what the input asked for.
    """
    payload = states["RunScope"]["Parameters"]["Payload"]
    assert payload["execution_arn.$"] == "$$.Execution.Id"
    assert payload["state_machine_arn.$"] == "$$.StateMachine.Id"
    assert payload["execution_input.$"] == "$$.Execution.Input"


def test_execution_local_scope_controls_the_terminal_through_degraded_route(states):
    choice = states["CheckExecutionScope"]
    routes = {
        next(part["StringEquals"] for part in rule["And"] if "StringEquals" in part):
        rule["Next"]
        for rule in choice["Choices"]
    }
    assert routes == {
        "PARTIAL": "MarkExecutionScopeIncomplete",
        "UNAVAILABLE": "MarkExecutionScopeIncomplete",
    }
    assert choice["Default"] == "CheckSkipReportCard"
    assert states["MarkExecutionScopeIncomplete"]["Next"] == "CheckSkipReportCard"
    assert states["MarkExecutionScopeIncomplete"]["Parameters"]["degraded"] is True


def test_completion_markers_retain_the_execution_scope_verdict(states):
    for name in (
        "WriteCompletionMarker",
        "WriteCompletionMarkerCalendar",
        "WriteCompletionMarkerDegraded",
        "WriteCompletionMarkerDegradedCalendar",
    ):
        body = states[name]["Parameters"]["Body.$"]
        assert "$.run_scope_result.Payload.execution_scope" in body
        assert "skipped_producers" in body


def test_it_honours_the_shell_run_dry_contract(states):
    """`research_dry` is this pipeline's canonical dry signal.

    Every advisory producer on the tail runs dry rather than skipping, so the
    Friday-PM shell run exercises the real IAM grants and the real derivation
    without writing an artifact for a day that had no run.
    """
    assert states["RunScope"]["Parameters"]["Payload"]["dry_run.$"] == "$.research_dry"


def test_it_runs_after_every_work_stage_and_before_the_report_card(states):
    """Placement is the whole correctness argument for reading history.

    Both routes into the post-eval tail must pass through it, or a run that
    fail-opened its substrate health check would produce a card with no scope
    beside it.
    """
    assert states["RunScope"]["Next"] == "CheckExecutionScope"
    predecessors = {
        name for name, body in states.items()
        if name != "RunScope" and '"RunScope"' in json.dumps(
            {k: v for k, v in body.items() if k != "Comment"}
        )
    }
    assert predecessors == {
        "CheckSubstrateHealthCheckStatus",
        "SetSubstrateHealthCheckDegradedSummary",
        # alpha-engine-config-I8167: CheckSkipSaturdayHealthCheck's bypass
        # route (skip_saturday_health_check=true) lands here directly,
        # bypassing SaturdayHealthCheck/WeeklySubstrateHealthCheck entirely
        # — a third route into the post-eval tail, alongside the two above.
        "CheckSkipSaturdayHealthCheck",
    }


def test_nothing_else_still_routes_straight_to_the_report_card_gate(states):
    """The insertion must have moved BOTH edges, not one.

    A surviving edge would let one path reach the card with no scope block —
    and an absent scope artifact is indistinguishable from a Lambda that never
    got deployed.
    """
    stragglers = {
        name for name, body in states.items()
        if name != "RunScope"
        and '"CheckSkipReportCard"' in json.dumps(
            {k: v for k, v in body.items() if k != "Comment"}
        )
    }
    assert stragglers == {"CheckExecutionScope", "MarkExecutionScopeIncomplete"}


def test_it_can_never_fail_the_run(states):
    """An advisory scope artifact must not kill a run that produced real
    trading artifacts. Its Catch rejoins the tail rather than routing to any
    failure handler."""
    catches = states["RunScope"]["Catch"]
    assert [c["ErrorEquals"] for c in catches] == [["States.ALL"]]
    assert all(c["Next"] == "CheckExecutionScope" for c in catches)
    assert all(c["ResultPath"] == "$.run_scope_error" for c in catches)


def test_its_result_does_not_overwrite_the_execution_state(states):
    assert states["RunScope"]["ResultPath"] == "$.run_scope_result"


def test_the_state_timeout_binds_before_the_lambda_timeout(states):
    """Two API calls and a pure derivation — and the SF ceiling must be the one
    that binds.

    45s here against the Lambda's own 60s. A state whose TimeoutSeconds sits
    above the function's cannot be reasoned about from the definition, and a
    generous ceiling would let a hung advisory state hold the tail of the
    weekly run. Pinned fleet-wide by
    tests/test_sf_lambda_timeout_ordering.py.
    """
    assert states["RunScope"]["TimeoutSeconds"] == 45


# ---------------------------------------------------------------------------
# In-band delivery to the Report Card (alpha-engine-config-I7392)
#
# The scope artifact was the ONLY delivery path, and it is the one path a
# REHEARSAL cannot use: the Lambda derives the scope and skips its put_object
# when dry_run is true. So on the 2026-08-29T00:47Z Friday shell run
# (execution offcycle-shell-20260829-004717) RunScope derived the right answer
# — Parity: DISABLED, CheckSkipParity took its skip branch — and the card could
# not read it, resolved contamination to UNKNOWN rather than NOT_IN_SCOPE, and
# paged ERROR at 01:44Z on a run in which nothing failed. The correct answer
# existed in memory and was discarded on the way to its consumer.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def floor(states) -> dict:
    """``InitializeInput``'s innermost defaults blob — the seeded floor every
    ``.$`` reference in a Task Payload must resolve against on EVERY path that
    reaches that Task. Same extraction as tests/test_sf_gate_state_wiring.py."""
    merged = states["InitializeInput"]["Parameters"]["merged.$"]
    start = merged.index("States.StringToJson('") + len("States.StringToJson('")
    end = merged.index("')", start)
    return json.loads(merged[start:end])


def test_the_report_card_receives_the_scope_in_band(states):
    """The run's own scope travels WITH the run, not via a side-channel
    artifact a rehearsal is forbidden to write."""
    payload = states["ReportCard"]["Parameters"]["Payload"]
    assert payload["run_scope.$"] == "$.run_scope_result.Payload"


def test_the_in_band_path_names_this_states_own_result_path(states):
    """The two halves of the thread must agree, or the reference resolves to
    the floor on every run and the fix is inert."""
    assert states["RunScope"]["ResultPath"] == "$.run_scope_result"
    assert states["ReportCard"]["Parameters"]["Payload"]["run_scope.$"].startswith(
        states["RunScope"]["ResultPath"] + "."
    )


def test_the_reference_is_seeded_at_the_initialize_input_floor(floor):
    """The hazard this class keeps producing (I7282, I7812, and the
    ``TrainSpecDispatch`` States.Runtime on the 2026-08-28 EOD shell run): a
    ``.$`` reference that is absent on a VALID path throws States.Runtime past
    valid ASL and green CI.

    Two valid paths reach ``CheckSkipReportCard`` without ``RunScope`` having
    written its ``ResultPath``: ``RunScope``'s own ``Catch`` (which writes
    ``$.run_scope_error`` instead) and ``SetSubstrateHealthCheckDegradedSummary``.
    """
    assert "run_scope_result" in floor
    assert "Payload" in floor["run_scope_result"]


def test_the_floor_grades_nothing(floor):
    """BOTH POLARITIES, and the seed must be HONEST.

    A floor that looked like a clean scope would let a run whose RunScope stage
    never ran grade the full stage list — confidently wrong. The consumer
    (``crucible-evaluator grading/run_scope.py::read_run_scope``) resolves a
    ``degraded`` block, and an empty ``stages`` map, to ``UNKNOWN`` with an
    empty graded set. Either alone is sufficient; both are asserted so a future
    edit to one cannot silently make the floor read as a complete run.
    """
    seeded = floor["run_scope_result"]["Payload"]
    assert seeded["degraded"] is True
    assert seeded["degraded_reason"]
    assert seeded["stages"] == {}


def test_the_s3_artifact_remains_the_fallback_not_the_removal(states):
    """In-band is PREFERRED, never exclusive.

    The Lambda still writes ``backtest/{run_date}/run_scope.json`` on a real
    run, which is what a manual/CLI card build or a snapshot rebuild — neither
    of which has an SF payload behind it — reads. Pinned here because deleting
    the write is the tempting simplification, and it would silently remove the
    only scope a rebuilt card can see.
    """
    handler = (
        pathlib.Path(__file__).resolve().parents[1]
        / "infrastructure" / "lambdas" / "weekly-run-scope" / "index.py"
    ).read_text()
    assert "put_object" in handler
    assert "KEY_TEMPLATE.format(run_date=run_date)" in handler
