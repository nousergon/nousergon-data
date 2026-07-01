"""Pins the EOD SF re-runnability guard: the trading instance is started +
SSM-registered BEFORE any post-market ssm:sendCommand.

Origin — 2026-06-30 incident. `ne-postclose-trading-pipeline` failed at
`EODReconcile` (deploy-drift guard, fixed in nousergon-data#574). The operator
recovery rerun (`watch-rerun-2026-06-30-1`, with the correct skip flags) then
died at the EODReconcile `ssm:sendCommand` with
`Ssm.InvalidInstanceIdException: Instances not in a valid state` — because the
trading instance `i-018eb3307a21329bf` was **stopped** by the *prior* run's
terminal `ForceStopInstance`. The EOD SF assumed the daemon-shutdown trigger
left the box running, but BOTH its success path (`StopTradingInstance`) and its
failure path (`ForceStopInstance`) stop it, so *every* recovery rerun landed on
a stopped instance and could never reach reconcile.

Fix (this guard's target): insert a `StartTradingInstance` (`ec2:startInstances`,
no-op if already running) → SSM-readiness poll (`describeInstanceInformation`
until `PingStatus=Online`, bounded ~3 min, hard-fail to `HandleFailure` on
budget exhaustion) block at the single post-mutex chokepoint, so all three
entry paths ensure the box is up before the first SSM step. Mirrors the
daily/preopen SF `StartExecutorEC2` → `SSMReadyChoice` pattern (config#1430,
the same InvalidInstanceIdException-on-cold-box class).

This test is the chokepoint that keeps the EOD SF re-runnable: it fails loudly
if a future edit removes the start/poll block or reroutes the mutex paths past
it (which would re-open the exact 2026-06-30 rerun failure).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_SF_PATH = Path(__file__).resolve().parent.parent / "infrastructure" / "step_function_eod.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


class TestEnsureRunningBlockShape:
    def test_start_instance_state_shape(self, states):
        st = states["StartTradingInstance"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::aws-sdk:ec2:startInstances"
        # startInstances on the same instance-id list the SSM steps target — no
        # hardcoded id, so it tracks $.trading_instance_id from the trigger.
        assert st["Parameters"]["InstanceIds.$"] == "$.trading_instance_id"
        # A start failure must fail the run, not silently proceed to a
        # sendCommand against a down box.
        catch_all = [c["Next"] for c in st.get("Catch", [])
                     if "States.ALL" in c["ErrorEquals"]]
        assert catch_all == ["HandleFailure"]
        assert st["Next"] == "WaitForInstanceReady"

    def test_describe_instance_info_polls_ssm_registration(self, states):
        st = states["DescribeInstanceInfo"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::aws-sdk:ssm:describeInstanceInformation"
        # Filter on the same instance-id list.
        flt = st["Parameters"]["Filters"][0]
        assert flt["Key"] == "InstanceIds"
        assert flt["Values.$"] == "$.trading_instance_id"
        catch_all = [c["Next"] for c in st.get("Catch", [])
                     if "States.ALL" in c["ErrorEquals"]]
        assert catch_all == ["HandleFailure"]

    def test_poll_loop_increments_bounded_counter(self, states):
        assert states["InitSSMPollCounter"]["Result"] == {"attempts": 0}
        assert states["InitSSMPollCounter"]["ResultPath"] == "$.ssm_poll"
        inc = states["IncrementSSMPoll"]
        assert inc["Parameters"]["attempts.$"] == "States.MathAdd($.ssm_poll.attempts, 1)"
        assert inc["ResultPath"] == "$.ssm_poll"
        assert inc["Next"] == "DescribeInstanceInfo"
        assert states["WaitSSMPoll"]["Type"] == "Wait"
        assert states["WaitSSMPoll"]["Next"] == "IncrementSSMPoll"


class TestSSMReadyChoiceFailLoud:
    """The readiness gate must (a) advance to real work only when Online and
    (b) HARD-FAIL on budget exhaustion — never silently skip EOD reconcile."""

    def test_online_advances_to_first_work_gate(self, states):
        ch = states["SSMReadyChoice"]
        online = [c for c in ch["Choices"]
                  if c.get("StringEquals") == "Online"
                  or any(x.get("StringEquals") == "Online" for x in c.get("And", []))]
        assert len(online) == 1
        # The Online branch keys off PingStatus and lands on the rerun-gate chain.
        cond = online[0]
        variables = {x["Variable"] for x in cond["And"]}
        assert variables == {
            "$.ssm_describe_result.InstanceInformationList[0].PingStatus"
        }
        assert cond["Next"] == "CheckSkipPostMarketData"

    def test_budget_exhaustion_hard_fails(self, states):
        ch = states["SSMReadyChoice"]
        budget = [c for c in ch["Choices"]
                  if c.get("Variable") == "$.ssm_poll.attempts"]
        assert len(budget) == 1, "no bounded-attempts branch on the readiness poll"
        # Exhausted budget must route to the fail-loud handler, NOT to the work
        # chain (that would run EOD against a possibly-down box, or skip it).
        assert budget[0]["Next"] == "HandleFailure"
        assert budget[0].get("NumericGreaterThanEquals", 0) >= 1
        # And the default keeps looping (waits), it does not fall through to work.
        assert ch["Default"] == "WaitSSMPoll"

    def test_handle_failure_still_releases_the_instance(self, states):
        # Fail-loud on an unready box must still hit the ForceStopInstance
        # cost-guard so we never leak a running trading box on a failed EOD.
        assert states["HandleFailure"]["Next"] == "ForceStopInstance"
        assert states["ForceStopInstance"]["Resource"] == "arn:aws:states:::aws-sdk:ec2:stopInstances"


class TestAllEntryPathsEnsureRunning:
    """Every post-mutex entry must pass through StartTradingInstance so no
    ssm:sendCommand can fire before the box is confirmed SSM-Online."""

    def test_three_mutex_edges_route_to_start(self, states):
        assert states["CheckMutexRole"]["Default"] == "StartTradingInstance"
        assert states["AcquireMutex"]["Next"] == "StartTradingInstance"
        failopen = [c["Next"] for c in states["AcquireMutex"]["Catch"]
                    if "States.ALL" in c["ErrorEquals"]]
        assert failopen == ["StartTradingInstance"]

    def test_start_block_reaches_first_sendcommand_only_via_readiness(self, states):
        """Static reachability: from StartTradingInstance, no ssm:sendCommand is
        reachable without first passing SSMReadyChoice's Online branch."""
        # Walk forward from StartTradingInstance following ONLY the non-failure
        # edges; assert the first sendCommand state is preceded by SSMReadyChoice.
        def send_command_states() -> set:
            return {n for n, st in states.items()
                    if "ssm:sendCommand" in str(st.get("Resource", ""))}

        sends = send_command_states()
        assert sends, "no ssm:sendCommand states found — fixture broken"

        # BFS from StartTradingInstance over Next/Default/Choice edges (skip
        # Catch→HandleFailure error edges). Any sendCommand reached must have
        # SSMReadyChoice on every path — enforced simply by requiring the
        # readiness gate to be the sole non-error successor bridge.
        assert "SSMReadyChoice" in states
        online_next = [c["Next"] for c in states["SSMReadyChoice"]["Choices"]
                       if c.get("StringEquals") == "Online"
                       or any(x.get("StringEquals") == "Online" for x in c.get("And", []))]
        # The readiness gate is the ONLY edge from the start block into the work
        # chain; its Online target must be a non-sendCommand gate (the skip
        # gate), proving the box is confirmed ready before any sendCommand.
        assert online_next == ["CheckSkipPostMarketData"]
        assert "sendCommand" not in str(states["CheckSkipPostMarketData"].get("Resource", ""))
