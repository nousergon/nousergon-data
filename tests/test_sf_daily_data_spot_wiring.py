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
        # 2026-07-06 live-verify lesson: spot RunInstances/PassRole authorize
        # via the box env credentials (the dashboard instance role has
        # neither) — the env source is LOAD-BEARING, mirror of weekly
        # DataPhase1.
        assert "source /home/ec2-user/.alpha-engine.env" in cmds
        # config#1897 (2026-07-07): execution-scoped id artifact
        # (ops/daily_data_spot/{date}/{$$.Execution.Name}.json). Date-scoping
        # alone let a same-day recovery rerun read the PRIOR attempt's
        # now-terminated spot id; the execution name (unique per
        # StartExecution) makes ReadDataSpotId's NoSuchKey-until-present loop
        # block on THIS launch, never a prior attempt's.
        assert "ops/daily_data_spot/{}/{}.json" in cmds
        assert "$$.Execution.StartTime" in cmds
        assert "$$.Execution.Name" in cmds
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
        assert "ops/daily_data_spot/{}/{}.json" in st["Parameters"]["Key.$"]
        (retry,) = st["Retry"]
        # Retry-until-present covers the overlapped spot bootstrap; exhaustion
        # fails loud (a failed/capacity-starved launch surfaces HERE).
        assert retry["MaxAttempts"] * retry["IntervalSeconds"] >= 300
        assert st["Catch"][0]["Next"] == "HandleFailure"
        assert st["Next"] == "ParseDataSpotId"

    def test_id_artifact_key_is_execution_scoped_and_reader_matches_writer(self, states):
        """config#1897 regression: writer and reader must build the SAME
        execution-scoped key.

        The 2026-07-07 failure: the id artifact was keyed by DATE only.
        MorningArcticAppend lost its spot to a spot reclaim; the recovery
        rerun (same run_date) launched a fresh spot, but ReadDataSpotId's
        S3.NoSuchKey retry-until-present found the PRIOR attempt's date-key
        already present and returned instantly with the reclaimed
        (terminated) spot id -> MorningEnrich sendCommand hit
        Ssm.InvalidInstanceIdException on a dead box. Keying by
        $$.Execution.Name (unique per StartExecution) makes the sync loop
        block on THIS execution's launch. Writer and reader must stay in
        lockstep or the reader would wait forever / read the wrong key.
        """
        reader_key = states["ReadDataSpotId"]["Parameters"]["Key.$"]
        writer_cmds = states["LaunchDailyDataSpot"]["Parameters"]["Parameters"]["commands.$"]

        # Both build the identical two-segment {date}/{execution-name} key.
        assert "ops/daily_data_spot/{}/{}.json" in reader_key
        assert "ops/daily_data_spot/{}/{}.json" in writer_cmds

        # Execution-scoped, not merely date-scoped: the execution name is the
        # per-attempt discriminator that defends same-day reruns.
        assert "$$.Execution.Name" in reader_key
        assert "$$.Execution.Name" in writer_cmds

        # And still date-grouped for ops legibility.
        assert "$$.Execution.StartTime" in reader_key
        assert "$$.Execution.StartTime" in writer_cmds

        # Lockstep: the reader's key template + its two format args must be a
        # substring of the writer's command (identical construction), so a
        # future edit to one that forgets the other is caught here.
        template = (
            "States.Format('ops/daily_data_spot/{}/{}.json', "
            "States.ArrayGetItem(States.StringSplit($$.Execution.StartTime,'T'),0), "
            "$$.Execution.Name)"
        )
        assert template in reader_key
        # writer embeds the same key + args inside its --id-artifact-key arg
        assert "--id-artifact-key ops/daily_data_spot/{}/{}.json" in writer_cmds
        assert (
            "States.ArrayGetItem(States.StringSplit($$.Execution.StartTime,'T'),0), "
            "$$.Execution.Name" in writer_cmds
        )

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
                "ReadDailyDataSpotArtifact",
                # 2026-07-06 live-verify lesson: without ListBucket, a
                # missing artifact surfaces as AccessDenied (S3 masks 404 as
                # 403) instead of the retryable NoSuchKey.
                "ListDailyDataSpotArtifactPrefix"} <= sids

    def test_launcher_has_launch_only_mode(self):
        sh = (_INFRA / "spot_data_weekly.sh").read_text()
        assert "--launch-only" in sh
        assert "--id-artifact-key" in sh
        assert 'KEEP_INSTANCE=1' in sh
        # The Name tag routes the IAM condition (alpha-engine-data-*).
        assert "alpha-engine-data-" in sh


class TestIntrinsicsWellFormed:
    """config#1897 (2026-07-07 deploy-red): every States.* intrinsic in the
    definition must be a *well-formed* intrinsic, not merely contain the right
    substrings.

    The regression: PR #676 added a second arg ($$.Execution.Name) to
    LaunchDailyDataSpot's inner States.Format but left the original single-arg
    version's trailing ')))' — one paren too many. The wiring tests above
    substring-match the key template + args, so they stayed green, but AWS
    rejected the definition at UpdateStateMachine time
    (SCHEMA_VALIDATION_FAILED: 'commands.$' must be a valid ... intrinsic
    function call) and the "Deploy Infrastructure" workflow went red on main.

    Parenthesis balance is the exact invariant that broke and the cheapest
    offline proxy for "is this a parseable intrinsic call". This walks the
    WHOLE definition so a malformed intrinsic anywhere — not just the two
    fields the wiring tests happen to name — fails loud in CI, pre-merge,
    instead of at deploy time (post-merge, partial-apply).
    """

    @staticmethod
    def _intrinsic_fields(node, path=""):
        if isinstance(node, dict):
            for k, v in node.items():
                if k.endswith(".$") and isinstance(v, str) and v.lstrip().startswith("States."):
                    yield f"{path}/{k}", v
                yield from TestIntrinsicsWellFormed._intrinsic_fields(v, f"{path}/{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                yield from TestIntrinsicsWellFormed._intrinsic_fields(v, f"{path}[{i}]")

    def test_all_states_intrinsics_have_balanced_parens(self):
        defn = json.loads((_INFRA / "step_function_daily.json").read_text())
        fields = list(self._intrinsic_fields(defn))
        assert fields, "expected at least one States.* intrinsic to guard"
        offenders = []
        for path, expr in fields:
            # Only count parens outside single-quoted literals — a bash command
            # string embedded in States.Array may legitimately contain
            # unbalanced parens inside its quotes; only the intrinsic-call
            # structure between literals must balance.
            depth, in_str, prev = 0, False, ""
            for ch in expr:
                if ch == "'" and prev != "\\":
                    in_str = not in_str
                elif not in_str and ch == "(":
                    depth += 1
                elif not in_str and ch == ")":
                    depth -= 1
                prev = ch
                if depth < 0:
                    break
            if depth != 0:
                offenders.append(f"{path}: paren balance {depth:+d} in {expr!r}")
        assert not offenders, "malformed intrinsic(s):\n" + "\n".join(offenders)
