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
- A new SSM step added on the TRADING BOX with a bare getCommandInvocation
  poll instead of the liveness poller (re-introducing the copy-drift that
  left MorningArcticAppend uncapped after #970 only patched MorningEnrich).
- An INSTANCE_UNRESPONSIVE branch dropped or rerouted around the
  force-stop.
- A poll loop whose Init state stops seeding both counters, or whose
  invoke stops round-tripping them (the Lambda is stateless — the SF
  state IS the counter storage).
- The CodeFreshnessGate unhooked from the SSM-ready path (re-opening
  the 40-minutes-to-discover-stale-code gap).

config#1767 (Phase 2, merged the same week): MorningEnrich and
MorningArcticAppend were relocated OFF the trading box onto TWO
independent, single-workload, self-terminating ephemeral spot boxes
(alpha-engine-data-spot-dispatcher). Their poll loops (PollMorningEnrichSpot
/ PollMorningArcticAppendSpot) intentionally use a bare
``ssm:getCommandInvocation`` Task instead of the liveness poller — see
``test_sf_data_spot_relocation_wiring.py`` for their dedicated wiring pins.
The wedged-SHARED-HOST risk config#1811 exists to catch does not apply the
same way to a single-purpose ephemeral box with its own bootstrap watchdog
(InstanceInitiatedShutdownBehavior=terminate), so they are excluded here by
name — this test still fails loud on any OTHER bare poll (a real copy-drift
on a persistent/shared target). ChronicGapSelfHeal stays on the trading box
(small, fail-soft, 300s timeout) and keeps its liveness-poller loop,
retargeted to the trading box instead of the now-retired shared data spot.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_POLLER_ARN_FRAGMENT = "alpha-engine-ssm-liveness-poller"

# config#1767 (Phase 2): these two dual-spot poll states intentionally use a
# bare ssm:getCommandInvocation Task (mirroring the groom SF's
# PollGroomCommand convention) against a single-purpose, self-terminating
# ephemeral spot — not a persistent/shared host, so config#1811's liveness
# pattern does not apply to them.
_BARE_POLL_EXEMPT = {"PollMorningEnrichSpot", "PollMorningArcticAppendSpot"}

# loop name -> (init, poll, check, wait, poll slot, stamp)
_LOOPS = {
    "code-freshness-gate": (
        "InitCodeFreshnessPoll", "WaitForCodeFreshness",
        "CheckCodeFreshnessStatus", "CodeFreshnessWait",
        "$.code_freshness_poll", "StampCodeFreshnessUnresponsive",
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
    """Every weekday SSM poll AGAINST A PERSISTENT/SHARED HOST must go
    through the liveness poller — a bare getCommandInvocation poll has no
    independent liveness signal and no shared budget contract (the exact
    copy-drift class that left MorningArcticAppend uncapped). The two
    config#1767 dual-spot poll states are exempt by name (see module
    docstring) — they poll a single-purpose, self-terminating ephemeral box,
    not a persistent/shared one."""
    bare = [
        n for n, st in states.items()
        if "getCommandInvocation" in str(st.get("Resource", ""))
        and n not in _BARE_POLL_EXEMPT
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
    # wedged host. config#1767 (Phase 2): the shared "daily data spot" this
    # comment used to describe was retired — MorningEnrich/MorningArcticAppend
    # moved to two independent, self-terminating ephemeral spots with their
    # OWN (non-liveness-poller) poll loops, so no loop in THIS registry targets
    # a spot anymore. All three remaining loops (code-freshness-gate,
    # chronic-gap-heal, morning-planner) run on the persistent trading box and
    # force-STOP (never terminate) it.
    st_stamp = states[stamp]
    assert st_stamp["ResultPath"] == "$.error"
    assert st_stamp["Parameters"]["Cause.$"] == f"{slot}.detail"
    assert st_stamp["Next"] == "ForceStopUnresponsiveInstance"


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
    assert "import executor.main" in cmds and "executor.daemon" in cmds and "executor.eod_reconcile" in cmds, (
        "executor import smoke test must run (broken main or any transitive "
        "import may not proceed; config#2353 upgraded this from an ast.parse-only "
        "syntax check to a real import so ImportErrors in non-entrypoint modules "
        "are caught too)"
    )

    fresh = [
        c["Next"] for c in states["CheckCodeFreshnessStatus"]["Choices"]
        if c.get("StringEquals") == "SUCCESS"
    ]
    # config#1767 (Phase 2): freshness success enters the first morning work
    # gate directly — the Phase-1 shared-spot synchronization hop
    # (CheckDataSpotLaunched -> ReadDataSpotId) this comment used to describe
    # was retired; each Phase-2 spot now launches lazily from its own
    # CheckSkipMorningEnrich gate.
    assert fresh == ["CheckSkipMorningEnrich"]
    assert states["CheckCodeFreshnessStatus"]["Default"] == "HandleFailure"
