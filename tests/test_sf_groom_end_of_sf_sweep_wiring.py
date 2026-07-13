"""Pins config#2201/#2311 end-of-SF sweep wiring in the groom-dispatch SF.

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

This test catches regressions like:
- any of the three paths (Map success / zero-launches AllSkipped / Map
  iteration failure) no longer reaching DispatchEndOfSfSweep (the
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


def test_map_wind_down_path_reaches_sweep(states):
    assert states["MapLaunches"]["Next"] == "DispatchEndOfSfSweep"


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
        "model": "claude-haiku-4-5",
        "issue_filter": "mid-only",
        "schedule": "end-of-sf-sweep",
    }


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
    assert notify["Next"] == "CheckMapLaunchOutcome"
    assert notify["Catch"][0]["Next"] == "CheckMapLaunchOutcome"

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
    assert st["Next"] == "CheckMapLaunchOutcome"


def test_sweep_is_fire_and_forget_no_polling_loop(states):
    """Brian removed the wait — the sweep dispatch must NOT feed the per-box
    poll/relaunch machinery (that lives inside MapLaunches's ItemProcessor for
    groom boxes only)."""
    assert states["DispatchEndOfSfSweep"]["Next"] not in (
        "PollGroomCommand", "CheckLaunched", "RelaunchNotifyGate")
    # And no top-level SSM polling state exists for the sweep.
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


def test_poll_groom_command_retries_transient_ssm_sdk_errors(states):
    """config#2311: a single Ssm.SdkClientException (bare network blip) must
    not kill a lane outright — this loop fires every ~15s across a run that
    can span hours, so a transient blip somewhere in that window is a
    near-certainty, not an edge case (this is what actually happened live on
    2026-07-11, 2h43m into an otherwise-healthy run)."""
    poll = states["MapLaunches"]["ItemProcessor"]["States"]["PollGroomCommand"]
    retries = poll["Retry"]
    error_sets = [set(r["ErrorEquals"]) for r in retries]
    assert {"Ssm.InvocationDoesNotExistException", "Ssm.InvocationDoesNotExist"} in error_sets
    sdk_retry = next(
        (r for r in retries if "Ssm.SdkClientException" in r["ErrorEquals"]), None
    )
    assert sdk_retry is not None, "no Retry entry covers Ssm.SdkClientException"
    assert "States.Timeout" in sdk_retry["ErrorEquals"]
    assert sdk_retry["MaxAttempts"] >= 1


def test_map_launch_failure_still_reaches_sweep(states):
    """config#2311: the third path — a genuine MapLaunches iteration failure
    — must reach DispatchEndOfSfSweep exactly like the success and
    zero-launches paths, closing the gap that caused the 2026-07-11 live
    incident (the sweep never dispatched that cycle)."""
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
    assert states["DispatchEndOfSfSweep"]["Next"] == "CheckMapLaunchOutcome"
    notify = states["NotifySweepDispatchFailure"]
    nexts = [notify.get("Next")] + [c.get("Next") for c in notify.get("Catch", [])]
    for nxt in nexts:
        assert nxt == "CheckMapLaunchOutcome", (
            f"NotifySweepDispatchFailure routes to {nxt} instead of "
            "CheckMapLaunchOutcome — a Map-launch failure recorded in "
            "$.mapFailure would be lost")

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
    """The two healthy paths (Map success, AllSkipped) must still reach the
    terminal Succeed — CheckMapLaunchOutcome's IsPresent check must not
    misfire when $.mapFailure was never set."""
    assert states["MapLaunches"]["Next"] == "DispatchEndOfSfSweep"
    assert states["AllSkipped"]["Next"] == "DispatchEndOfSfSweep"
    assert states["GroomDispatchComplete"]["Type"] == "Succeed"
