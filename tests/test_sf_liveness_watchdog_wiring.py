"""Pins the config#1811 liveness-watchdog wiring in the WEEKDAY SF.

Origin: 2026-07-06 (config#1807) — the trading box wedged under memory
pressure mid-MorningArcticAppend. The SSM agent went ConnectionLost, so
the command's own executionTimeout (enforced by that agent, INSIDE the
box) could not fire; the SF poll loop read a frozen InProgress for 62
minutes (22 past the timeout) until the agent self-recovered. Meanwhile
RunDaemon — the sole intraday order executor — sat blocked with the
market open. A watchdog that dies with its watchee is not a watchdog.

The fix: every SSM poll loop in the weekday SF goes through the
ssm-liveness-poller Lambda (command status + independent PingStatus +
bounded budgets, evaluated OUTSIDE the box), and INSTANCE_UNRESPONSIVE
routes deterministically to stamp → ForceStopUnresponsiveInstance →
HandleFailure. Detection budget: ~3 polls (~1 min), not 62.

This test catches regressions like:
- A new SSM step added with a bare getCommandInvocation poll instead of
  the liveness poller (re-introducing the copy-drift that left
  MorningArcticAppend uncapped after #970 only patched MorningEnrich).
- An INSTANCE_UNRESPONSIVE branch dropped or rerouted around the
  force-stop.
- A poll loop whose Init state stops seeding both counters, or whose
  invoke stops round-tripping them (the Lambda is stateless — the SF
  state IS the counter storage).
- The CodeFreshnessGate unhooked from the SSM-ready path (re-opening
  the 40-minutes-to-discover-stale-code gap).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_POLLER_ARN_FRAGMENT = "alpha-engine-ssm-liveness-poller"

# loop name -> (init, poll, check, wait, poll slot, stamp)
_LOOPS = {
    "code-freshness-gate": (
        "InitCodeFreshnessPoll", "WaitForCodeFreshness",
        "CheckCodeFreshnessStatus", "CodeFreshnessWait",
        "$.code_freshness_poll", "StampCodeFreshnessUnresponsive",
    ),
    "morning-enrich": (
        "InitMorningEnrichPoll", "WaitForMorningEnrich",
        "CheckMorningEnrichStatus", "MorningEnrichWait",
        "$.morning_enrich_poll", "StampMorningEnrichUnresponsive",
    ),
    "morning-arctic-append": (
        "InitMorningArcticAppendPoll", "WaitForMorningArcticAppend",
        "CheckMorningArcticAppendStatus", "MorningArcticAppendWait",
        "$.arctic_append_poll", "StampMorningArcticAppendUnresponsive",
    ),
    "chronic-gap-heal": (
        "InitChronicGapPoll", "WaitForChronicGap",
        "CheckChronicGapStatus", "ChronicGapWait",
        "$.chronic_gap_poll", "StampChronicGapUnresponsive",
    ),
    "morning-planner": (
        "InitMorningPlannerPoll", "WaitForMorningPlanner",
        "CheckMorningPlannerStatus", "MorningPlannerWait",
        "$.planner_poll", "StampMorningPlannerUnresponsive",
    ),
}


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


def test_no_bare_get_command_invocation_polls_remain(states):
    """Every weekday SSM poll must go through the liveness poller — a bare
    getCommandInvocation poll has no independent liveness signal and no
    shared budget contract (the exact copy-drift class that left
    MorningArcticAppend uncapped)."""
    bare = [
        n for n, st in states.items()
        if "getCommandInvocation" in str(st.get("Resource", ""))
    ]
    assert not bare, (
        f"bare getCommandInvocation polls in weekday SF: {bare} — use the "
        f"ssm-liveness-poller (config#1811)"
    )


@pytest.mark.parametrize("step", sorted(_LOOPS))
def test_loop_wiring(states, step):
    init, poll, check, wait, slot, stamp = _LOOPS[step]

    # Init seeds BOTH counters into the poll slot the invoke reads.
    st_init = states[init]
    assert st_init["ResultPath"] == slot
    assert st_init["Result"]["attempts"] == 0
    assert st_init["Result"]["ping_misses"] == 0
    assert st_init["Next"] == poll

    # The poll invokes the liveness poller and round-trips the counters.
    st_poll = states[poll]
    assert st_poll["Resource"] == "arn:aws:states:::lambda:invoke"
    fn = st_poll["Parameters"]["FunctionName"]
    assert _POLLER_ARN_FRAGMENT in fn
    payload = st_poll["Parameters"]["Payload"]
    assert payload["attempts.$"] == f"{slot}.attempts"
    assert payload["ping_misses.$"] == f"{slot}.ping_misses"
    assert payload["step"] == step
    assert payload["max_ping_misses"] == 3
    assert st_poll["ResultPath"] == slot
    assert st_poll["Next"] == check

    # Choice: IN_PROGRESS loops via Wait; INSTANCE_UNRESPONSIVE stamps.
    nexts = {
        c["StringEquals"]: c["Next"]
        for c in states[check]["Choices"]
        if "StringEquals" in c
    }
    assert nexts["IN_PROGRESS"] == wait
    assert states[wait]["Next"] == poll
    assert nexts["INSTANCE_UNRESPONSIVE"] == stamp

    # Stamp carries the poller's detail into $.error, then force-stops the
    # wedged host. config#1807: the three DATA loops run on the daily data
    # spot, so their remediation terminates the SPOT; the trading-box loops
    # keep the force-stop.
    st_stamp = states[stamp]
    assert st_stamp["ResultPath"] == "$.error"
    assert st_stamp["Parameters"]["Cause.$"] == f"{slot}.detail"
    _data_spot_steps = ("morning-enrich", "morning-arctic-append", "chronic-gap-heal")
    expected_force = (
        "ForceTerminateUnresponsiveDataSpot" if step in _data_spot_steps
        else "ForceStopUnresponsiveInstance"
    )
    assert st_stamp["Next"] == expected_force


def test_force_stop_is_forceful_and_alerts(states):
    st = states["ForceStopUnresponsiveInstance"]
    assert st["Resource"] == "arn:aws:states:::aws-sdk:ec2:stopInstances"
    assert st["Parameters"]["Force"] is True
    assert st["Parameters"]["InstanceIds.$"] == "$.trading_instance_id"
    # Alert either way: success AND stop-failure both end at HandleFailure,
    # and neither may clobber the $.error stamped by the Stamp state.
    assert st["Next"] == "HandleFailure"
    assert st["Catch"][0]["Next"] == "HandleFailure"
    assert st["ResultPath"] != "$.error"
    assert st["Catch"][0]["ResultPath"] != "$.error"


def test_code_freshness_gate_front_loads_the_drift_check(states):
    """The gate must sit between SSM-ready and the first morning work gate,
    verify all three repos, self-heal once, and fail loud — closing the
    2026-07-06 gap where stale code was only discovered ~40 min in at
    RunMorningPlanner."""
    online = [
        c["Next"] for c in states["SSMReadyChoice"]["Choices"] if "And" in c
    ]
    assert online == ["CodeFreshnessGate"]

    cmds = "\n".join(
        states["CodeFreshnessGate"]["Parameters"]["Parameters"]["commands"]
    )
    for repo in ("alpha-engine", "alpha-engine-data", "alpha-engine-config"):
        assert repo in cmds
    assert "rev-parse origin/main" in cmds, "must compare against origin/main"
    assert "chown -R ec2-user:ec2-user" in cmds, "self-heal must reclaim ownership"
    assert "reset --hard origin/main" in cmds, "self-heal must reset to main"
    assert "CODE-STALE-AFTER-HEAL" in cmds and "exit 1" in cmds, (
        "persistent drift after the one self-heal must exit non-zero (fail loud)"
    )
    assert "ast.parse" in cmds, "executor syntax gate must run (broken main may not proceed)"

    fresh = [
        c["Next"] for c in states["CheckCodeFreshnessStatus"]["Choices"]
        if c.get("StringEquals") == "SUCCESS"
    ]
    # config#1807: freshness success synchronizes with the data-spot launch
    # (CheckDataSpotLaunched -> ReadDataSpotId) before the morning gates.
    assert fresh == ["CheckDataSpotLaunched"]
    assert states["CheckCodeFreshnessStatus"]["Default"] == "HandleFailure"
