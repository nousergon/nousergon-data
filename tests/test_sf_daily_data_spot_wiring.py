"""Pin the daily data-spot decouple topology (config#1807, 2026-07-06).

The pre-open data phase (MorningEnrich + MorningArcticAppend +
ChronicGapSelfHeal) runs on a short-lived spot instance instead of the
trading box: on 2026-07-06 the arctic append swap-thrashed the t3.small
trading box into SSM darkness and blocked RunDaemon at market open.
Topology: launch-once (fire-and-forget on ae-dashboard) -> per-state SSM
dispatch to the spot (task-split + liveness polling preserved) ->
explicit terminate, with the spot's own systemd watchdog as the orphan
backstop.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_INFRA = pathlib.Path(__file__).parent.parent / "infrastructure"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads((_INFRA / "step_function_daily.json").read_text())["States"]


class TestLaunch:
    def test_launch_is_fire_and_forget_on_dashboard(self, states):
        st = states["LaunchDailyDataSpot"]
        assert st["Resource"] == "arn:aws:states:::aws-sdk:ssm:sendCommand"
        # ae-dashboard, already carried by the WeekdayTrigger CFN Input.
        assert st["Parameters"]["InstanceIds.$"] == "$.ec2_instance_id"
        cmds = st["Parameters"]["Parameters"]["commands.$"]
        assert "--launch-only" in cmds
        assert "spot_data_weekly.sh" in cmds
        assert "--max-runtime-seconds 10800" in cmds, "dead-man watchdog budget"
        assert "export FLOW_DOCTOR_ENABLED=1" in cmds
        # Date-keyed artifact — a stale prior-day artifact can never be read.
        assert "ops/daily_data_spot/{}.json" in cmds
        assert "$$.Execution.StartTime" in cmds
        # Fire-and-forget: proceeds to the box boot, no poll loop.
        assert st["Next"] == "StartExecutorEC2"

    def test_launch_skipped_when_all_three_data_states_skip(self, states):
        gate = states["CheckSkipDataSpot"]
        (rule,) = gate["Choices"]
        flags = {c["Variable"] for c in rule["And"]}
        assert flags == {
            "$.skip_morning_enrich",
            "$.skip_morning_arctic_append",
            "$.skip_chronic_gap_heal",
        }
        assert rule["Next"] == "StartExecutorEC2"
        assert gate["Default"] == "LaunchDailyDataSpot"


class TestReadArtifact:
    def test_read_is_the_synchronization_point(self, states):
        st = states["ReadDataSpotId"]
        assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:getObject"
        assert st["Parameters"]["Bucket"] == "alpha-engine-research"
        assert "ops/daily_data_spot/{}.json" in st["Parameters"]["Key.$"]
        (retry,) = st["Retry"]
        # Retry-until-present covers the overlapped spot bootstrap; exhaustion
        # fails loud (a failed/capacity-starved launch surfaces HERE).
        assert retry["MaxAttempts"] * retry["IntervalSeconds"] >= 300
        assert st["Catch"][0]["Next"] == "HandleFailure"
        assert st["Next"] == "ParseDataSpotId"

    def test_read_only_when_launch_dispatched(self, states):
        gate = states["CheckDataSpotLaunched"]
        (rule,) = gate["Choices"]
        assert rule["Variable"] == "$.data_spot_launch"
        assert rule["IsPresent"] is True
        assert rule["Next"] == "ReadDataSpotId"
        assert gate["Default"] == "CheckSkipMorningEnrich"


class TestDataStatesTargetSpot:
    @pytest.mark.parametrize(
        "state", ["MorningEnrich", "MorningArcticAppend", "ChronicGapSelfHeal"]
    )
    def test_state_targets_spot_not_trading_box(self, states, state):
        st = states[state]
        assert st["Parameters"]["InstanceIds.$"] == (
            "States.Array($.data_spot.info.instance_id)"
        ), f"{state} must run on the daily data spot (config#1807)"
        cmds = "\n".join(st["Parameters"]["Parameters"]["commands"])
        assert "trading_instance_id" not in cmds
        # Spot flavor: fresh bootstrap owns venv/pin/config — no pull, no
        # .alpha-engine.env, no .venv (weekly-spot convention).
        assert "git pull" not in cmds and "git -C" not in cmds
        assert ".alpha-engine.env" not in cmds
        assert "source .venv" not in cmds
        assert "export FLOW_DOCTOR_ENABLED=1" in cmds

    @pytest.mark.parametrize(
        "wait", ["WaitForMorningEnrich", "WaitForMorningArcticAppend", "WaitForChronicGap"]
    )
    def test_liveness_poller_watches_the_spot(self, states, wait):
        payload = states[wait]["Parameters"]["Payload"]
        assert payload["instance_id.$"] == "$.data_spot.info.instance_id"

    def test_morning_enrich_keeps_task_split_flags(self, states):
        cmds = "\n".join(states["MorningEnrich"]["Parameters"]["Parameters"]["commands"])
        assert "--skip-chronic-heal --skip-arctic-append" in cmds, (
            "the L4608/2026-06-11 task-split flags must survive the spot move"
        )


class TestTerminate:
    def test_every_data_phase_exit_converges_on_the_hook(self, states):
        assert states["CheckChronicGapStatus"]["Default"] == "CheckDataSpotToTerminate"
        assert states["ChronicGapSelfHeal"]["Catch"][0]["Next"] == "CheckDataSpotToTerminate"
        assert states["WaitForChronicGap"]["Catch"][0]["Next"] == "CheckDataSpotToTerminate"
        skip_edge = [
            c for c in states["CheckSkipChronicGapHeal"]["Choices"]
            if c["Next"] == "CheckDataSpotToTerminate"
        ]
        assert len(skip_edge) == 1

    def test_terminate_is_best_effort_and_proceeds(self, states):
        st = states["TerminateDailyDataSpot"]
        assert st["Resource"] == "arn:aws:states:::aws-sdk:ec2:terminateInstances"
        assert st["Next"] == "CheckSkipPredictorInference"
        # Cleanup failure must not fail a pipeline whose data work succeeded;
        # the spot watchdog is the named backstop.
        assert st["Catch"][0]["Next"] == "CheckSkipPredictorInference"

    def test_unresponsive_data_spot_terminates_spot_not_trading_box(self, states):
        st = states["ForceTerminateUnresponsiveDataSpot"]
        assert st["Resource"] == "arn:aws:states:::aws-sdk:ec2:terminateInstances"
        assert st["Parameters"]["InstanceIds.$"] == (
            "States.Array($.data_spot.info.instance_id)"
        )
        assert st["Next"] == "HandleFailure"
        assert st["Catch"][0]["Next"] == "HandleFailure"


class TestIamAndLauncher:
    def test_sf_role_carries_the_three_new_grants(self):
        policy = json.loads(
            (_INFRA / "iam" / "alpha-engine-step-functions-role.json").read_text()
        )
        sids = {st.get("Sid") for st in policy["Statement"]}
        assert {"SendCommandDailyDataSpot", "TerminateDailyDataSpot",
                "ReadDailyDataSpotArtifact"} <= sids

    def test_launcher_has_launch_only_mode(self):
        sh = (_INFRA / "spot_data_weekly.sh").read_text()
        assert "--launch-only" in sh
        assert "--id-artifact-key" in sh
        assert 'KEEP_INSTANCE=1' in sh
        # The Name tag routes the IAM condition (alpha-engine-data-*).
        assert "alpha-engine-data-" in sh
