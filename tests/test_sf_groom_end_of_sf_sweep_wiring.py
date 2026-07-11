"""Pins config#2201 end-of-SF sweep wiring in the groom-dispatch SF.

Brian design 2026-07-10: ONE Haiku run_mode=sweep spot box per trigger cycle,
dispatched by the SF AFTER the groom Map fully winds down — and equally on the
zero-launches path — replacing the config#2129 per-box partitioned sweeps.

This test catches regressions like:
- either path (Map wind-down / zero-launches AllSkipped) no longer reaching
  DispatchEndOfSfSweep (the unconditional-coverage property is the whole
  point: the drain-the-backlog end state must never starve the PR sweep)
- the sweep payload drifting off the launch_decided sweep contract the
  dispatcher expects (run_mode=sweep + launch_decided + a lib-valid
  issue_filter — 'sweep' itself is a TAG value, never a filter)
- the Catch being dropped or rerouted to a Fail state (a sweep-launch failure
  must be recorded + notified but NEVER fail the groom SF execution)
- the failure record / SNS notify losing the no-silent-caps guarantees
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
    → the SAME terminal Succeed the success path uses. No route to any Fail
    state anywhere on the sweep dispatch's failure path (no-silent-caps: the
    skip is recorded in the execution output, never converted into an
    execution failure)."""
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
    assert notify["Next"] == "GroomDispatchComplete"
    assert notify["Catch"][0]["Next"] == "GroomDispatchComplete"

    assert states["GroomDispatchComplete"]["Type"] == "Succeed"
    # No sweep-path state may terminate in a Fail.
    for name in ("DispatchEndOfSfSweep", "RecordSweepDispatchFailure",
                 "NotifySweepDispatchFailure"):
        st = states[name]
        nexts = [st.get("Next")] + [c.get("Next") for c in st.get("Catch", [])]
        for nxt in nexts:
            if nxt is None:
                continue
            assert states[nxt].get("Type") != "Fail", (
                f"{name} routes to Fail state {nxt} — the end-of-SF sweep "
                "must never fail the groom SF execution (config#2201)")


def test_sweep_success_path_records_result_and_succeeds(states):
    st = states["DispatchEndOfSfSweep"]
    assert st["ResultPath"] == "$.sweep"
    assert st["Next"] == "GroomDispatchComplete"


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
