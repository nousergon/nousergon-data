"""Pins the config#1767 (Phase 2) relocation of the data-heavy weekday/EOD work
OFF the always-on ae-trading box onto an ephemeral EC2 spot box.

Background (alpha-engine-config#1767): the weekday pre-open pipeline
(step_function_daily.json: MorningEnrich + MorningArcticAppend) and the EOD
post-close pipeline (step_function_eod.json: PostMarketData + PostMarketArcticAppend)
used to SSM-invoke ~30-50 min of daily_closes fetch + ArcticDB append ON the
trading box (i-018eb3307a21329bf), filling /tmp and competing with IB Gateway +
the daemon. Phase 2 moves that data phase onto a fresh spot box launched by the
alpha-engine-data-spot-dispatcher Lambda — MIRRORING the Saturday spot pattern
(the fleet's SF-expressible spot launcher is the scheduled-groom-dispatcher
Lambda + step_function_groom.json's LaunchGroomSpot -> CheckLaunched -> poll,
which itself mirrors spot_data_weekly.sh).

This test pins, structurally (no live infra needed):
  1. The new spot launch/poll states exist with the correct Type/Resource and
     select the data-spot dispatcher Lambda (deliverable #1).
  2. The trading path no longer contains the relocated on-trading
     MorningEnrich/PostMarketData SSM states (deliverable #2).
  3. FAILURE ISOLATION (deliverable #4, LOAD-BEARING): a data-spot failure —
     launch Catch, poll Catch, or a non-Success terminal SSM status — routes to
     the CONTINUE path (the predictor/daemon path on weekday; the
     reconcile/snapshot/stop path on EOD), NEVER to HandleFailure/FailExecution.
     Mirrors the Saturday ResearchPredictorParallel branch-error pattern
     (record-as-data, fail-open).
  4. The dispatcher Lambda's workload map runs the SAME weekly_collector.py
     entrypoints the on-trading states ran (M0 data contract preserved).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DAILY = _REPO_ROOT / "infrastructure" / "step_function_daily.json"
_EOD = _REPO_ROOT / "infrastructure" / "step_function_eod.json"
_DISPATCHER = _REPO_ROOT / "infrastructure" / "lambdas" / "data-spot-dispatcher"
_SF_ROLE = _REPO_ROOT / "infrastructure" / "iam" / "alpha-engine-step-functions-role.json"

_LAMBDA_INVOKE = "arn:aws:states:::lambda:invoke"
_SSM_POLL = "arn:aws:states:::aws-sdk:ssm:getCommandInvocation"
_SSM_SEND = "arn:aws:states:::aws-sdk:ssm:sendCommand"
_DISPATCHER_FN = "alpha-engine-data-spot-dispatcher"


@pytest.fixture(scope="module")
def daily() -> dict:
    return json.loads(_DAILY.read_text())["States"]


@pytest.fixture(scope="module")
def eod() -> dict:
    return json.loads(_EOD.read_text())["States"]


# ── Terminal HALT states each SF must NEVER reach from a data-spot failure ────
_HALT = {"HandleFailure", "FailExecution", "ForceStopInstance"}


def _all_targets(state: dict) -> list[str]:
    """Every Next/Default/Choice.Next/Catch.Next target of a state."""
    t: list[str] = []
    for k in ("Next", "Default"):
        if k in state:
            t.append(state[k])
    for c in state.get("Choices", []):
        if "Next" in c:
            t.append(c["Next"])
    for c in state.get("Catch", []):
        if "Next" in c:
            t.append(c["Next"])
    return t


# ══════════════════════════════════════════════════════════════════════════
# WEEKDAY (step_function_daily.json)
# ══════════════════════════════════════════════════════════════════════════
class TestWeekdaySpotStatesPresent:
    _LAUNCH = ["LaunchMorningEnrichSpot", "LaunchMorningArcticAppendSpot"]
    _POLL = ["PollMorningEnrichSpot", "PollMorningArcticAppendSpot"]
    _LAUNCHED_GATE = ["CheckMorningEnrichSpotLaunched", "CheckMorningArcticAppendSpotLaunched"]
    _STATUS = ["CheckMorningEnrichSpotStatus", "CheckMorningArcticAppendSpotStatus"]

    @pytest.mark.parametrize("name", _LAUNCH)
    def test_launch_state_invokes_dispatcher(self, daily, name):
        st = daily[name]
        assert st["Type"] == "Task"
        assert st["Resource"] == _LAMBDA_INVOKE
        assert st["Parameters"]["FunctionName"] == _DISPATCHER_FN
        # Each launch selects exactly one workload.
        assert set(st["Parameters"]["Payload"]) == {"workload"}
        assert st["Parameters"]["Payload"]["workload"] in {
            "morning-enrich", "morning-arctic-append"
        }

    @pytest.mark.parametrize("name", _POLL)
    def test_poll_state_polls_ssm(self, daily, name):
        st = daily[name]
        assert st["Type"] == "Task"
        assert st["Resource"] == _SSM_POLL
        # Polls the command_id + instance_id the dispatcher returned.
        p = st["Parameters"]
        assert p["CommandId.$"].endswith(".Payload.data_spot.command_id")
        assert p["InstanceId.$"].endswith(".Payload.data_spot.instance_id")

    @pytest.mark.parametrize("name", _LAUNCHED_GATE)
    def test_launched_gate_is_choice(self, daily, name):
        assert daily[name]["Type"] == "Choice"

    @pytest.mark.parametrize("name", _STATUS)
    def test_status_check_is_choice(self, daily, name):
        assert daily[name]["Type"] == "Choice"


class TestWeekdayDataPhaseOffTrading:
    """Deliverable #2: the on-trading data-phase SSM states are GONE."""

    @pytest.mark.parametrize(
        "gone",
        [
            "MorningEnrich", "MorningArcticAppend",
            "WaitForMorningEnrich", "WaitForMorningArcticAppend",
            "CheckMorningEnrichStatus", "CheckMorningArcticAppendStatus",
            "CheckSkipMorningArcticAppend", "MorningEnrichPollTimeout",
        ],
    )
    def test_relocated_state_absent(self, daily, gone):
        assert gone not in daily, f"{gone} must move to the data-spot dispatcher"

    def test_no_ssm_send_targets_trading_instance_for_data(self, daily):
        # No remaining ssm:sendCommand state runs a weekly_collector data workload.
        from tests.sf_command_utils import extract_commands
        for name, st in daily.items():
            if st.get("Resource") != _SSM_SEND:
                continue
            joined = "\n".join(extract_commands(st))
            assert "--morning-enrich" not in joined, f"{name} still runs enrich on-box"
            assert "--morning-arctic-append" not in joined, f"{name} still appends on-box"


class TestWeekdayFailureIsolation:
    """Deliverable #4 (LOAD-BEARING): a data-spot failure must NOT block daemon
    start — it routes to the continue path, never HandleFailure."""

    _CONTINUE = "CheckSkipChronicGapHeal"

    def test_launch_catch_is_fail_open(self, daily):
        for name in ("LaunchMorningEnrichSpot", "LaunchMorningArcticAppendSpot"):
            catch = daily[name]["Catch"]
            targets = {c["Next"] for c in catch}
            assert targets == {"ExtractDataSpotError"}, (
                f"{name} launch Catch must fail-open to ExtractDataSpotError, "
                f"got {targets}"
            )
            for c in catch:
                assert c["Next"] not in _HALT

    def test_poll_catch_is_fail_open(self, daily):
        for name in ("PollMorningEnrichSpot", "PollMorningArcticAppendSpot"):
            for c in daily[name]["Catch"]:
                assert c["Next"] == "ExtractDataSpotError"
                assert c["Next"] not in _HALT

    def test_status_default_is_fail_open_not_handlefailure(self, daily):
        # The OLD on-trading CheckMorningEnrichStatus Default was HandleFailure.
        # The spot status check Default must be the fail-open normalizer instead.
        for name in ("CheckMorningEnrichSpotStatus", "CheckMorningArcticAppendSpotStatus"):
            assert daily[name]["Default"] == "ExtractDataSpotError"
            assert daily[name]["Default"] not in _HALT

    def test_error_normalizer_continues_not_halts(self, daily):
        # ExtractDataSpotError -> PublishDataSpotFailureImmediate -> CONTINUE.
        assert daily["ExtractDataSpotError"]["Type"] == "Pass"
        assert daily["ExtractDataSpotError"]["Next"] == "PublishDataSpotFailureImmediate"
        pub = daily["PublishDataSpotFailureImmediate"]
        assert pub["Next"] == self._CONTINUE
        # Even the SNS publish's own Catch is fail-open.
        for c in pub.get("Catch", []):
            assert c["Next"] == self._CONTINUE
            assert c["Next"] not in _HALT

    def test_no_data_spot_state_reaches_a_halt(self, daily):
        """Exhaustive: walking every data-spot state's targets, none escapes to a
        HALT state (HandleFailure/FailExecution)."""
        data_states = [
            "LaunchMorningEnrichSpot", "CheckMorningEnrichSpotLaunched",
            "PollMorningEnrichSpot", "CheckMorningEnrichSpotStatus", "MorningEnrichSpotWait",
            "LaunchMorningArcticAppendSpot", "CheckMorningArcticAppendSpotLaunched",
            "PollMorningArcticAppendSpot", "CheckMorningArcticAppendSpotStatus",
            "MorningArcticAppendSpotWait",
            "ExtractDataSpotError", "PublishDataSpotFailureImmediate",
        ]
        for name in data_states:
            for tgt in _all_targets(daily[name]):
                assert tgt not in _HALT, (
                    f"{name} routes to HALT state {tgt} — a data-spot failure would "
                    "block daemon start (config#1767 #4 violation)"
                )

    def test_skip_and_launched_false_route_to_continue(self, daily):
        # skip_morning_enrich skips the whole phase; launched:false kill-switch
        # also lands on the continue path.
        assert daily["CheckSkipMorningEnrich"]["Choices"][0]["Next"] == self._CONTINUE
        for gate in ("CheckMorningEnrichSpotLaunched", "CheckMorningArcticAppendSpotLaunched"):
            assert daily[gate]["Default"] == self._CONTINUE


# ══════════════════════════════════════════════════════════════════════════
# EOD (step_function_eod.json)
# ══════════════════════════════════════════════════════════════════════════
class TestEODSpotStatesPresent:
    _LAUNCH = ["LaunchPostMarketDataSpot", "LaunchPostMarketArcticAppendSpot"]
    _POLL = ["PollPostMarketDataSpot", "PollPostMarketArcticAppendSpot"]

    @pytest.mark.parametrize("name", _LAUNCH)
    def test_launch_state_invokes_dispatcher(self, eod, name):
        st = eod[name]
        assert st["Type"] == "Task"
        assert st["Resource"] == _LAMBDA_INVOKE
        assert st["Parameters"]["FunctionName"] == _DISPATCHER_FN
        assert st["Parameters"]["Payload"]["workload"] in {
            "post-market-data", "post-market-arctic-append"
        }

    @pytest.mark.parametrize("name", _POLL)
    def test_poll_state_polls_ssm(self, eod, name):
        st = eod[name]
        assert st["Type"] == "Task"
        assert st["Resource"] == _SSM_POLL


class TestEODDataPhaseOffTrading:
    @pytest.mark.parametrize(
        "gone",
        [
            "PostMarketData", "PostMarketArcticAppend",
            "WaitForPostMarketData", "WaitForPostMarketArcticAppend",
            "CheckPostMarketStatus", "CheckPostMarketArcticAppendStatus",
            "CheckSkipPostMarketArcticAppend",
            "PostMarketStatusError", "PostMarketArcticAppendStatusError",
        ],
    )
    def test_relocated_state_absent(self, eod, gone):
        assert gone not in eod, f"{gone} must move to the data-spot dispatcher"

    def test_reconcile_snapshot_stop_path_intact(self, eod):
        # Deliverable #2: the reconcile/snapshot/instance-stop path stays on the box.
        for kept in ("CaptureSnapshot", "EODReconcile", "StopTradingInstance"):
            assert kept in eod, f"{kept} must remain in the EOD trading path"

    def test_no_ssm_send_targets_trading_instance_for_data(self, eod):
        from tests.sf_command_utils import extract_commands
        for name, st in eod.items():
            if st.get("Resource") != _SSM_SEND:
                continue
            joined = "\n".join(extract_commands(st))
            assert "--post-market-data" not in joined, f"{name} still fetches on-box"
            assert "--post-market-arctic-append" not in joined, f"{name} still appends on-box"


class TestEODFailureIsolation:
    """Deliverable #4: an EOD data-spot failure must NOT block reconcile +
    instance-stop — it routes to CheckSkipCaptureSnapshot, never HandleFailure."""

    _CONTINUE = "CheckSkipCaptureSnapshot"

    def test_launch_catch_is_fail_open(self, eod):
        for name in ("LaunchPostMarketDataSpot", "LaunchPostMarketArcticAppendSpot"):
            for c in eod[name]["Catch"]:
                assert c["Next"] == "ExtractDataSpotError"
                assert c["Next"] not in _HALT

    def test_poll_catch_is_fail_open(self, eod):
        for name in ("PollPostMarketDataSpot", "PollPostMarketArcticAppendSpot"):
            for c in eod[name]["Catch"]:
                assert c["Next"] == "ExtractDataSpotError"
                assert c["Next"] not in _HALT

    def test_status_default_is_fail_open_not_handlefailure(self, eod):
        for name in ("CheckPostMarketDataSpotStatus", "CheckPostMarketArcticAppendSpotStatus"):
            assert eod[name]["Default"] == "ExtractDataSpotError"

    def test_error_normalizer_continues_to_reconcile_path(self, eod):
        assert eod["ExtractDataSpotError"]["Next"] == "PublishDataSpotFailureImmediate"
        pub = eod["PublishDataSpotFailureImmediate"]
        assert pub["Next"] == self._CONTINUE
        for c in pub.get("Catch", []):
            assert c["Next"] == self._CONTINUE
            assert c["Next"] not in _HALT

    def test_no_data_spot_state_reaches_a_halt(self, eod):
        data_states = [
            "LaunchPostMarketDataSpot", "CheckPostMarketDataSpotLaunched",
            "PollPostMarketDataSpot", "CheckPostMarketDataSpotStatus", "PostMarketDataSpotWait",
            "LaunchPostMarketArcticAppendSpot", "CheckPostMarketArcticAppendSpotLaunched",
            "PollPostMarketArcticAppendSpot", "CheckPostMarketArcticAppendSpotStatus",
            "PostMarketArcticAppendSpotWait",
            "ExtractDataSpotError", "PublishDataSpotFailureImmediate",
        ]
        for name in data_states:
            for tgt in _all_targets(eod[name]):
                assert tgt not in _HALT, (
                    f"{name} routes to HALT state {tgt} — an EOD data-spot failure "
                    "would block reconcile + instance-stop (config#1767 #4 violation)"
                )

    def test_skip_and_launched_false_route_to_continue(self, eod):
        assert eod["CheckSkipPostMarketData"]["Choices"][0]["Next"] == self._CONTINUE
        for gate in ("CheckPostMarketDataSpotLaunched", "CheckPostMarketArcticAppendSpotLaunched"):
            assert eod[gate]["Default"] == self._CONTINUE


# ══════════════════════════════════════════════════════════════════════════
# Dispatcher Lambda + IAM (deliverables #1, #3)
# ══════════════════════════════════════════════════════════════════════════
class TestDispatcherLambdaAndIam:
    def test_dispatcher_package_present(self):
        for f in ("index.py", "iam-policy.json", "sf-execution-iam-policy.json", "requirements.txt"):
            assert (_DISPATCHER / f).exists(), f"data-spot-dispatcher/{f} missing"

    def test_workload_map_preserves_collector_contract(self):
        # M0 contract: the spot workloads run the SAME weekly_collector.py entry
        # points the on-trading states ran — unchanged args = unchanged data paths.
        src = (_DISPATCHER / "index.py").read_text()
        for token in (
            "--morning-enrich",
            "--morning-arctic-append",
            "--post-market-data",
            "--post-market-arctic-append",
        ):
            assert token in src, f"dispatcher workload map missing {token}"
        # The enrich workload must still skip the inline heal + inline append.
        assert "--skip-chronic-heal" in src
        assert "--skip-arctic-append" in src

    def test_dispatcher_uses_executor_profile_no_ib_exposure(self):
        # Deliverable #3: the spot reuses the Saturday spot's Arctic-write/S3
        # profile (alpha-engine-executor-profile) and the standard fleet SG (no
        # IB port). This mirrors spot_data_weekly.sh rather than minting a role.
        src = (_DISPATCHER / "index.py").read_text()
        assert "alpha-engine-executor-profile" in src
        # No IB gateway port opened anywhere in the launcher.
        assert "4001" not in src and "4002" not in src

    def test_sf_role_can_invoke_dispatcher(self):
        # Deliverable #3 / asymmetric-grant guard: the SF role grants
        # lambda:InvokeFunction on the dispatcher. (test_sf_iam_lambda_grants.py
        # enforces the general rule; this pins the specific ARN prefix.)
        role = json.loads(_SF_ROLE.read_text())
        invoke_resources: list[str] = []
        for st in role["Statement"]:
            acts = st["Action"]
            acts = acts if isinstance(acts, list) else [acts]
            if "lambda:InvokeFunction" in acts:
                res = st["Resource"]
                invoke_resources += res if isinstance(res, list) else [res]
        assert any(
            _DISPATCHER_FN in r for r in invoke_resources
        ), "SF role missing lambda:InvokeFunction grant for the data-spot dispatcher"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-q"]))
