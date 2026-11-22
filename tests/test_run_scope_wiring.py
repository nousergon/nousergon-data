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
    assert states["RunScope"]["Next"] == "CheckSkipReportCard"
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
    assert stragglers == set()


def test_it_can_never_fail_the_run(states):
    """An advisory scope artifact must not kill a run that produced real
    trading artifacts. Its Catch rejoins the tail rather than routing to any
    failure handler."""
    catches = states["RunScope"]["Catch"]
    assert [c["ErrorEquals"] for c in catches] == [["States.ALL"]]
    assert all(c["Next"] == "CheckSkipReportCard" for c in catches)
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


# ---------------------------------------------------------------------------
# This execution's own producer scope decides the terminal
# (alpha-engine-config-I13864)
#
# `VacuousRun` (I9693) fails an execution that entered NO spine stage. One that
# skipped SOME declared producer entered the spine and ended SUCCEEDED, so a
# recovery that skipped twenty producers read like a full run. The verdict is
# RunScope's `execution_scope`, computed from THIS execution's rows before the
# cycle-level merge, and it is acted on at `CheckExecutionScope` - BEFORE the
# completion notifier and marker, so a partial scope rides the existing degraded
# chokepoint (DEGRADED marker, run-slot release, DegradedRun terminal that
# pages) and never reaches a Succeed.
#
# It is deliberately NOT at the observe-only tail after WeeklyCoverageSweep: that
# tail may not reach a Fail other than VacuousRun
# (tests/sf_degraded_summary_helpers.py::assert_observe_only_tail). Two earlier
# attempts put the new Fail terminals there and broke 150 contract tests.
# ---------------------------------------------------------------------------

_VERDICT = "$.run_scope_result.Payload.execution_scope.verdict"


def _walk(states: dict, start: str, data: dict) -> list[str]:
    """Follow the definition from ``start`` with ASL Choice semantics, over a
    minimal state document. Task and Pass states are assumed to succeed and to
    leave the document unchanged except for a Pass state's own ``ResultPath``.
    Returns the visited state names, ending at a Succeed, a Fail or a loop."""

    def lookup(var: str):
        cursor = data
        for segment in var.removeprefix("$.").split("."):
            if not isinstance(cursor, dict) or segment not in cursor:
                return False, None
            cursor = cursor[segment]
        return True, cursor

    def holds(rule: dict) -> bool:
        if "And" in rule:
            return all(holds(r) for r in rule["And"])
        if "Or" in rule:
            return any(holds(r) for r in rule["Or"])
        if "Not" in rule:
            return not holds(rule["Not"])
        present, value = lookup(rule["Variable"])
        if "IsPresent" in rule:
            return present == rule["IsPresent"]
        if not present:
            return False
        if "StringEquals" in rule:
            return value == rule["StringEquals"]
        if "BooleanEquals" in rule:
            return value is rule["BooleanEquals"]
        raise AssertionError(f"walker does not model {rule}")

    path, cur = [], start
    while cur not in path:
        path.append(cur)
        state = states[cur]
        if state["Type"] in ("Succeed", "Fail"):
            return path
        if state["Type"] == "Choice":
            cur = next(
                (r["Next"] for r in state["Choices"] if holds(r)), state.get("Default")
            )
            assert cur, f"{path[-1]} has no matching rule and no Default"
            continue
        if state["Type"] == "Pass" and state.get("ResultPath") == "$.degraded_summary":
            data = {**data, "degraded_summary": {"degraded": True}}
        assert "Next" in state, f"{cur} ends the walk without a terminal"
        cur = state["Next"]
    return path


def _scope_doc(verdict: str | None) -> dict:
    doc: dict = {"degraded_summary": {"degraded": False, "reasons": {}}}
    if verdict is not None:
        doc["run_scope_result"] = {"Payload": {"execution_scope": {"verdict": verdict}}}
    return doc


def test_the_real_run_reaches_the_scope_choice_before_the_degraded_gate(states):
    assert states["CheckShellRunNotify"]["Default"] == "CheckExecutionScope"
    shell = states["CheckShellRunNotify"]["Choices"]
    assert [rule["Next"] for rule in shell] == ["NotifyShellRunComplete"]


def test_the_scope_choice_reads_the_run_scope_verdict_and_nothing_an_operator_writes(states):
    """An acknowledgment is a record: no rule or setter may dereference it."""
    # The Comment prose may NAME the keys to say they are ignored; behaviour
    # (Variable/Parameters/Choices) may not read them.
    for name in ("CheckExecutionScope", "MarkExecutionScopeIncomplete", "MarkExecutionScopeUnavailable"):
        body = {k: v for k, v in states[name].items() if k != "Comment"}
        behaviour = json.dumps(body)
        assert "vacuous_rerun_accepted_reason" not in behaviour
        assert "skipped_producer_reasons" not in behaviour


@pytest.mark.parametrize(
    ("verdict", "first_hop"),
    [
        ("COMPLETE", "CheckGateDegradedNotify"),
        ("VACUOUS", "CheckGateDegradedNotify"),
        ("PARTIAL", "MarkExecutionScopeIncomplete"),
        ("UNAVAILABLE", "MarkExecutionScopeUnavailable"),
        ("SOMETHING_NEW", "MarkExecutionScopeUnavailable"),
        (None, "MarkExecutionScopeUnavailable"),
    ],
)
def test_each_scope_outcome_takes_its_own_branch(states, verdict, first_hop):
    walked = _walk(states, "CheckExecutionScope", _scope_doc(verdict))
    assert walked[1] == first_hop


@pytest.mark.parametrize(
    "verdict", ["PARTIAL", "UNAVAILABLE", "SOMETHING_NEW", None]
)
def test_a_partial_or_unavailable_scope_can_only_end_in_the_degraded_terminal(states, verdict):
    walked = _walk(states, "CheckExecutionScope", _scope_doc(verdict))
    assert walked[-1] == "DegradedRun", walked
    assert states["DegradedRun"]["Type"] == "Fail"
    assert "WriteCompletionMarker" not in walked  # the clean marker
    assert "WriteCompletionMarkerDegraded" in walked
    assert "NotifyCompleteMultipleDegraded" in walked  # the page, before the marker
    assert walked.index("NotifyCompleteMultipleDegraded") < walked.index(
        "WriteCompletionMarkerDegraded"
    )


def test_a_complete_scope_remains_eligible_for_succeeded(states):
    walked = _walk(states, "CheckExecutionScope", _scope_doc("COMPLETE"))
    assert "NotifyComplete" in walked and "WriteCompletionMarker" in walked
    assert "MarkExecutionScopeIncomplete" not in walked
    assert "MarkExecutionScopeUnavailable" not in walked


def test_a_vacuous_scope_is_left_to_the_existing_vacuous_terminal(states):
    """Preserved, not replaced: the VacuousRun gate is downstream and unchanged."""
    walked = _walk(states, "CheckExecutionScope", _scope_doc("VACUOUS"))
    assert "MarkExecutionScopeIncomplete" not in walked
    assert states["CheckExecutionDidWork"]["Choices"][0]["Next"] == (
        "CheckRunSlotHeldBeforeVacuousRun"
    )
    assert states["CheckRunSlotHeldBeforeVacuousRun"]["Default"] == "VacuousRun"


@pytest.mark.parametrize(
    ("name", "reason"),
    [
        ("MarkExecutionScopeIncomplete", "execution_scope_partial"),
        ("MarkExecutionScopeUnavailable", "execution_scope_unavailable"),
    ],
)
def test_the_setters_record_the_verdict_and_accumulate_into_the_summary(states, name, reason):
    state = states[name]
    params = state["Parameters"]
    assert state["Type"] == "Pass"
    assert state["ResultPath"] == "$.degraded_summary"
    assert params["degraded"] is True
    assert params["reason"] == reason
    assert "$.degraded_summary.reasons" in params["reasons.$"]
    assert "execution_scope" in params["reasons.$"]
    assert "execution_scope" in params or "execution_scope.$" in params
    assert "PARTITION FAMILY = trading_day" in state["Comment"]


def test_the_partial_setter_carries_the_skipped_producers_and_the_unavailable_one_derefs_nothing(states):
    assert states["MarkExecutionScopeIncomplete"]["Parameters"]["execution_scope.$"] == (
        "$.run_scope_result.Payload.execution_scope"
    )
    # The unavailable path exists precisely when $.run_scope_result may be
    # absent, so it may not dereference it.
    assert "run_scope_result" not in json.dumps(
        states["MarkExecutionScopeUnavailable"]["Parameters"]
    )


def test_the_new_fail_states_are_not_reachable_from_the_observe_only_tail(states):
    """The constraint two attempts broke: no Fail other than VacuousRun may be
    reachable from WeeklyCoverageSweep. This change adds no Fail state at all."""
    new = {"CheckExecutionScope", "MarkExecutionScopeIncomplete", "MarkExecutionScopeUnavailable"}
    seen, stack = set(), ["WeeklyCoverageSweep"]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        body = states[name]
        stack.extend(
            [body["Next"]] if "Next" in body else []
        )
        stack.extend(rule["Next"] for rule in body.get("Choices", []))
        if body.get("Default"):
            stack.append(body["Default"])
        stack.extend(c["Next"] for c in body.get("Catch", []))
    assert not (seen & new)


def test_the_cadence_declared_skip_set_matches_the_trigger_declaration():
    """`run_scope.CADENCE_DECLARED_SKIP_FLAGS` is the set of skips the scheduled
    run is declared to make; a flag added to the SaturdayTrigger input without
    being added there would make every scheduled Saturday end partial."""
    import importlib.util
    import sys

    root = pathlib.Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "weekly_sf_rerun_for_cadence", root / "scripts" / "weekly_sf_rerun.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    import run_scope  # on sys.path via weekly_sf_rerun's own insertion

    assert run_scope.CADENCE_DECLARED_SKIP_FLAGS == frozenset(module.cadence_declared_skips())
