"""Pins config#2201/#2311/#4803 end-of-SF sweep wiring in the groom-dispatch SF.

Brian design 2026-07-10: ONE Haiku run_mode=sweep spot box per trigger cycle,
dispatched by the SF AFTER the groom Map fully winds down — and equally on the
zero-launches path — replacing the config#2129 per-box partitioned sweeps.

config#2311 (2026-07-11 live incident): a THIRD path — a genuine MapLaunches
iteration failure (e.g. an uncaught Ssm.SdkClientException 2h43m into a poll
loop) — previously skipped DispatchEndOfSfSweep entirely, contradicting the
"unconditional coverage" invariant below. Fixed via a Catch on MapLaunches
routing through RecordMapLaunchFailure -> DispatchEndOfSfSweep -> (after the
sweep fires) CheckMapLaunchOutcome -> GroomMapLaunchFailed, so the execution
still ends FAILED for Fleet-SF Watch without starving the sweep.

config#4803 (2026-07-28): per-lane FailExecution replaced with RecordLaneFailure
(a recording Pass) — lane failures no longer abort sibling iterations mid-work
(ToleratedFailurePercentage=0). The Map always completes with one result per
lane; CheckMapLaneOutcomes inspects $.mapOutcome[*].laneOutcome.laneFailed and
routes to SetMapFailureFromLanes (which shapes $.mapFailure) → DispatchEndOfSfSweep
if any lane recorded a failure. The Map's Catch now only handles genuine
Map-level failures (States.Runtime on a malformed definition, etc.).

This test catches regressions like:
- any converging path (Map success with/without lane failures / zero-launches
  AllSkipped / Map-level Catch) no longer reaching DispatchEndOfSfSweep (the
  unconditional-coverage property is the whole point: the drain-the-backlog
  end state must never starve the PR sweep)
- the sweep payload drifting off the launch_decided sweep contract the
  dispatcher expects (run_mode=sweep + launch_decided + a lib-valid
  issue_filter — 'sweep' itself is a TAG value, never a filter)
- the Catch being dropped or rerouted to a Fail state (a sweep-launch failure
  must be recorded + notified but NEVER fail the groom SF execution)
- the failure record / SNS notify losing the no-silent-caps guarantees
- a genuine Map-launch failure silently stopping being reported as FAILED
  (the sweep fix must not also swallow real lane-failure alerting)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_groom.json"


@pytest.fixture(scope="module")
def doc() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(doc) -> dict:
    return doc["States"]


def test_map_success_path_routes_through_lane_outcome_check(states):
    """config#4803: MapLaunches.Next routes to CheckMapLaneOutcomes (not directly
    to DispatchEndOfSfSweep). The post-Map aggregation inspects every iteration's
    output for a laneOutcome.laneFailed marker — per-lane failures no longer abort
    sibling iterations (FailExecution replaced with RecordLaneFailure, a recording
    Pass). If no lane failed, the Default routes to DispatchEndOfSfSweep. If any
    lane recorded a failure, SetMapFailureFromLanes shapes $.mapFailure and routes
    to DispatchEndOfSfSweep — the sweep still fires unconditionally either way."""
    assert states["MapLaunches"]["Next"] == "CheckMapLaneOutcomes"
    check = states["CheckMapLaneOutcomes"]
    assert check["Type"] == "Choice"
    assert check["Default"] == "DispatchEndOfSfSweep"
    # Every non-default choice must route to SetMapFailureFromLanes
    for choice in check["Choices"]:
        assert choice["Next"] == "SetMapFailureFromLanes"
    set_fail = states["SetMapFailureFromLanes"]
    assert set_fail["Type"] == "Pass"
    assert set_fail["ResultPath"] == "$.mapFailure"
    assert set_fail["Parameters"]["failed"] is True
    assert set_fail["Next"] == "DispatchEndOfSfSweep"


def test_zero_launches_path_reaches_sweep(states):
    """AllSkipped must be a pass-through to the sweep, not a terminal Succeed —
    the zero-groom-launch cycle still gets its unconditional PR sweep."""
    assert states["CheckAnyLaunches"]["Default"] == "AllSkipped"
    all_skipped = states["AllSkipped"]
    assert all_skipped["Type"] == "Pass"
    assert all_skipped["Next"] == "DispatchEndOfSfSweep"


def test_sweep_payload_is_the_launch_decided_sweep_contract(states):
    st = states["DispatchEndOfSfSweep"]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::lambda:invoke"
    params = st["Parameters"]
    assert params["FunctionName"] == "alpha-engine-scheduled-groom-dispatcher"
    payload = params["Payload"]
    # LITERAL payload (no JSONPath) — the sweep is unconditional by design.
    assert payload == {
        "run_mode": "sweep",
        "launch_decided": True,
        "model": "deepseek-v4-flash",
        "issue_filter": "mid-only",
        "schedule": "end-of-sf-sweep",
    }


def _reaches(states, start, target, *, follow_catch=True):
    """Is `target` reachable from `start` by Next/Catch edges (top level only)?

    2026-07-28: these assertions used to pin DIRECT edges to
    CheckMapLaunchOutcome. Inserting NotifyCycleComplete on the shared
    convergence broke them without touching the invariant they actually guard —
    "every converging path still reaches the outcome check after the sweep has
    been attempted". Reachability is the more faithful expression of that
    invariant and does not re-break the next time a state is inserted on the
    same run, so per groom-sweep-policy §9 the guards are rewritten here rather
    than the change being bent to fit them.
    """
    seen, stack = set(), [start]
    while stack:
        name = stack.pop()
        if name == target:
            return True
        if name in seen or name not in states:
            continue
        seen.add(name)
        st = states[name]
        if st.get("Next"):
            stack.append(st["Next"])
        if follow_catch:
            stack.extend(c["Next"] for c in st.get("Catch", []) if c.get("Next"))
    return False


def test_sweep_launch_failure_is_nonfatal_recorded_and_notified(states):
    """Catch → record (Pass, dispatched:false into $.sweep) → best-effort SNS
    → CheckMapLaunchOutcome (config#2311: no longer directly to the terminal
    Succeed — the outcome check re-asserts FAILED if a Map-launch failure was
    ALSO recorded, independent of the sweep's own outcome). A sweep-launch
    failure alone must still never route to Fail (no-silent-caps: the skip is
    recorded in the execution output, never converted into an execution
    failure by itself)."""
    st = states["DispatchEndOfSfSweep"]
    catches = st["Catch"]
    assert len(catches) == 1
    assert catches[0]["ErrorEquals"] == ["States.ALL"]
    assert catches[0]["Next"] == "RecordSweepDispatchFailure"
    assert catches[0]["ResultPath"] == "$.sweepDispatchError"

    record = states["RecordSweepDispatchFailure"]
    assert record["Type"] == "Pass"
    assert record["ResultPath"] == "$.sweep"
    assert record["Parameters"]["dispatched"] is False
    assert record["Parameters"]["error.$"] == "$.sweepDispatchError"
    assert record["Next"] == "NotifySweepDispatchFailure"

    notify = states["NotifySweepDispatchFailure"]
    assert notify["Resource"] == "arn:aws:states:::sns:publish"
    assert "$.sweepDispatchError" in notify["Parameters"]["Message.$"]
    assert _reaches(states, notify["Next"], "CheckMapLaunchOutcome")
    assert _reaches(states, notify["Catch"][0]["Next"], "CheckMapLaunchOutcome")

    assert states["GroomDispatchComplete"]["Type"] == "Succeed"
    # A sweep-dispatch failure alone (Catch -> Record -> Notify) must never
    # itself route directly to a Fail state.
    for name in ("DispatchEndOfSfSweep", "RecordSweepDispatchFailure",
                 "NotifySweepDispatchFailure"):
        st = states[name]
        nexts = [st.get("Next")] + [c.get("Next") for c in st.get("Catch", [])]
        for nxt in nexts:
            if nxt is None:
                continue
            assert states[nxt].get("Type") != "Fail", (
                f"{name} routes to Fail state {nxt} — a sweep-dispatch "
                "failure alone must never fail the groom SF execution "
                "(config#2201)")


def test_sweep_success_path_records_result_and_succeeds(states):
    st = states["DispatchEndOfSfSweep"]
    assert st["ResultPath"] == "$.sweep"
    assert _reaches(states, st["Next"], "CheckMapLaunchOutcome")


def test_sweep_is_fire_and_forget_no_polling_loop(states):
    """Brian removed the wait — the sweep dispatch must NOT feed the per-box
    poll/relaunch machinery (that lives inside MapLaunches's ItemProcessor for
    groom boxes only)."""
    assert states["DispatchEndOfSfSweep"]["Next"] not in (
        "CheckLaunchedCallback", "RelaunchNotifyGate")
    # config-I4333: no SSM polling state exists — replaced by .waitForTaskToken
    top_level_ssm_polls = [
        n for n, s in states.items()
        if s.get("Resource") == "arn:aws:states:::aws-sdk:ssm:getCommandInvocation"
    ]
    assert top_level_ssm_polls == []


def test_no_groom_box_partition_fields_remain_in_sf(doc):
    """config#2201 retired the config#2129 per-box sweep partitions — the SF
    definition must carry no partition plumbing."""
    raw = json.dumps(doc)
    assert "partition_index" not in raw
    assert "partition_count" not in raw


def test_launch_groom_spot_uses_wait_for_task_token(states):
    """config-I4333: LaunchGroomSpot uses .waitForTaskToken so the SF pauses
    until the box calls send-task-success instead of polling SSM every 15s.

    2026-07-27: this asserted `"TaskToken.$" in lg["Parameters"]` — pinning the
    exact shape Step Functions rejects at runtime, since `TaskToken` is not a
    field of the Lambda Invoke API. The 20:00 UTC run died 11s in on it while
    this test was green. Third instance today of a guard outliving (or, here,
    never matching) the design it claims to protect; see groom-sweep-policy §9.
    The token must be carried INSIDE Payload — via the context object's
    $$.Task — which is what is asserted now.
    """
    lg = states["MapLaunches"]["ItemProcessor"]["States"]["LaunchGroomSpot"]
    assert lg["Resource"] == "arn:aws:states:::lambda:invoke.waitForTaskToken"
    # 13800 (3h50m) since 2026-08-03. The lane bound must clear the box's 3.75h
    # dead-man (alpha-engine-config groom_spot_bootstrap.sh) so the SF waits for a
    # box that is winding down rather than abandoning it. Full ladder:
    # tests/test_groom_cycle_notifications.py::test_sf_timeout_sits_above_the_box_budget_hierarchy
    assert lg["TimeoutSeconds"] == 13800
    assert "TaskToken.$" not in lg["Parameters"]
    assert "TaskToken" not in lg["Parameters"]
    assert "$$.Task" in lg["Parameters"]["Payload.$"]
    # Timeout → CheckCompletionMarkerTaskToken
    timeout_catchers = [c for c in lg["Catch"] if "States.Timeout" in c["ErrorEquals"]]
    assert len(timeout_catchers) == 1
    assert timeout_catchers[0]["Next"] == "CheckCompletionMarkerTaskToken"



def test_map_level_catch_still_reaches_sweep(states):
    """config#2311 + config#4803: the MapLaunches Catch now handles only genuine
    Map-level failures (e.g. States.Runtime from a malformed definition) — per-lane
    failures are handled by CheckMapLaneOutcomes → SetMapFailureFromLanes instead.
    The Catch must still route to RecordMapLaunchFailure → DispatchEndOfSfSweep
    so the sweep fires unconditionally even on a Map-level failure, then
    CheckMapLaunchOutcome re-asserts FAILED via GroomMapLaunchFailed."""
    catches = states["MapLaunches"]["Catch"]
    assert len(catches) == 1
    assert catches[0]["ErrorEquals"] == ["States.ALL"]
    assert catches[0]["Next"] == "RecordMapLaunchFailure"
    assert catches[0]["ResultPath"] == "$.mapLaunchError"

    record = states["RecordMapLaunchFailure"]
    assert record["Type"] == "Pass"
    assert record["ResultPath"] == "$.mapFailure"
    assert record["Parameters"]["failed"] is True
    assert record["Parameters"]["error.$"] == "$.mapLaunchError"
    assert record["Next"] == "DispatchEndOfSfSweep"


def test_map_launch_failure_still_terminates_execution_failed(states):
    """config#2311: closing the sweep gap must NOT also swallow real
    lane-failure alerting — Fleet-SF Watch's EventBridge pattern listens for
    the execution's own terminal status, so a genuine Map-iteration failure
    must still end the execution FAILED, just AFTER the sweep has already
    been dispatched (not instead of it)."""
    # Every path that can ultimately terminate the execution successfully
    # must funnel through CheckMapLaunchOutcome first — DispatchEndOfSfSweep's
    # OWN success Next, and NotifySweepDispatchFailure's Next/Catch (the tail
    # of the sweep-failure sub-path). DispatchEndOfSfSweep's Catch is exempt
    # here — that's the sweep's OWN failure path, which correctly detours
    # through RecordSweepDispatchFailure/NotifySweepDispatchFailure first.
    assert _reaches(states, states["DispatchEndOfSfSweep"]["Next"],
                    "CheckMapLaunchOutcome")
    notify = states["NotifySweepDispatchFailure"]
    nexts = [notify.get("Next")] + [c.get("Next") for c in notify.get("Catch", [])]
    for nxt in nexts:
        assert _reaches(states, nxt, "CheckMapLaunchOutcome"), (
            f"NotifySweepDispatchFailure routes to {nxt}, from which "
            "CheckMapLaunchOutcome is unreachable — a Map-launch failure "
            "recorded in $.mapFailure would be lost")

    check = states["CheckMapLaunchOutcome"]
    assert check["Type"] == "Choice"
    choices = check["Choices"]
    assert len(choices) == 1
    assert choices[0]["Variable"] == "$.mapFailure"
    assert choices[0]["IsPresent"] is True
    assert choices[0]["Next"] == "GroomMapLaunchFailed"
    assert check["Default"] == "GroomDispatchComplete"

    fail = states["GroomMapLaunchFailed"]
    assert fail["Type"] == "Fail"


def test_healthy_paths_do_not_route_through_fail_state(states):
    """The healthy paths (no lane failure, and AllSkipped) must still reach the
    terminal Succeed — CheckMapLaunchOutcome's IsPresent check must not misfire
    when $.mapFailure was never set.

    config#4803: MapLaunches.Next now routes to CheckMapLaneOutcomes (per-lane
    failure check) rather than directly to DispatchEndOfSfSweep. The healthy
    no-failure path goes: MapLaunches → CheckMapLaneOutcomes.Default →
    DispatchEndOfSfSweep → CheckMapLaunchOutcome.Default → GroomDispatchComplete.
    AllSkipped still goes directly to DispatchEndOfSfSweep."""
    assert states["CheckMapLaneOutcomes"]["Default"] == "DispatchEndOfSfSweep"
    assert states["AllSkipped"]["Next"] == "DispatchEndOfSfSweep"
    assert states["GroomDispatchComplete"]["Type"] == "Succeed"


# ── config#4803: per-lane failure no longer aborts siblings ─────────────


def test_no_per_lane_fail_execution_in_item_processor(states):
    """config#4803: the Map's ItemProcessor must NOT contain a FailExecution
    state — per-lane failures are now recording Pass states (RecordLaneFailure
    + LaneCompleted) so no iteration can abort its siblings."""
    processor = states["MapLaunches"]["ItemProcessor"]["States"]
    fails = [n for n, s in processor.items() if s.get("Type") == "Fail"]
    assert fails == [], (
        f"Map ItemProcessor contains Fail state(s) {fails} — per-lane failures "
        "must not abort sibling iterations (config#4803)"
    )


def test_record_lane_failure_produces_lane_failed_marker(states):
    """config#4803: RecordLaneFailure must record laneFailed=true into
    $.laneOutcome so CheckMapLaneOutcomes can detect it post-Map."""
    processor = states["MapLaunches"]["ItemProcessor"]["States"]
    rlf = processor["RecordLaneFailure"]
    assert rlf["Type"] == "Pass"
    assert rlf["Parameters"]["laneFailed"] is True
    assert rlf["ResultPath"] == "$.laneOutcome"
    assert rlf["Next"] == "LaneCompleted"


def test_record_lane_failure_is_reached_from_handle_failure(states):
    """config#4803: HandleFailure must route to RecordLaneFailure (not
    FailExecution) so the SNS alert still fires and the lane records its
    failure without aborting siblings."""
    processor = states["MapLaunches"]["ItemProcessor"]["States"]
    hf = processor["HandleFailure"]
    assert hf["Next"] == "RecordLaneFailure"
    # The Catch defense-in-depth must also route to RecordLaneFailure
    for catch in hf.get("Catch", []):
        assert catch["Next"] == "RecordLaneFailure"


def test_lane_failure_path_reaches_sweep_via_set_map_failure(states):
    """config#4803: when CheckMapLaneOutcomes detects a lane failure, it routes
    to SetMapFailureFromLanes which shapes $.mapFailure and proceeds to
    DispatchEndOfSfSweep — the sweep still fires, then CheckMapLaunchOutcome
    routes to GroomMapLaunchFailed."""
    set_fail = states["SetMapFailureFromLanes"]
    assert set_fail["Next"] == "DispatchEndOfSfSweep"
    assert set_fail["Parameters"]["failed"] is True
    assert set_fail["Parameters"]["reason"] == "one_or_more_lanes_failed"
