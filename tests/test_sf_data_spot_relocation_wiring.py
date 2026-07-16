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
        assert st["Parameters"]["Payload"]["workload"] in {
            "morning-enrich", "morning-arctic-append"
        }
        # config#2542: force_on_demand.$ threads the retry-budget's on-demand
        # override into the dispatcher (see TestWeekdayDataSpotRetryBudget),
        # mirroring the EOD launch states.
        assert set(st["Parameters"]["Payload"]) == {"workload", "force_on_demand.$"}

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

    # alpha-engine-config-I2717 (2026-07-16): the continue path used to be the
    # CheckSkipChronicGapHeal gate; that gate (and the heal behind it) was
    # removed entirely — the continue path now rejoins directly at
    # CheckSkipPredictorInference.
    _CONTINUE = "CheckSkipPredictorInference"

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
        # The spot status check Default must route to the retry-budget check
        # (config#2542) — itself always fail-open, never HandleFailure.
        assert daily["CheckMorningEnrichSpotStatus"]["Default"] == "CheckMorningEnrichRetryBudget"
        assert daily["CheckMorningArcticAppendSpotStatus"]["Default"] == "CheckMorningArcticAppendRetryBudget"
        for name in ("CheckMorningEnrichRetryBudget", "CheckMorningArcticAppendRetryBudget"):
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
            "InitMorningEnrichRetryCounter",
            "LaunchMorningEnrichSpot", "CheckMorningEnrichSpotLaunched",
            "PollMorningEnrichSpot", "CheckMorningEnrichSpotStatus", "MorningEnrichSpotWait",
            "CheckMorningEnrichRetryBudget", "IncrementMorningEnrichRetry",
            "InitMorningArcticAppendRetryCounter",
            "LaunchMorningArcticAppendSpot", "CheckMorningArcticAppendSpotLaunched",
            "PollMorningArcticAppendSpot", "CheckMorningArcticAppendSpotStatus",
            "MorningArcticAppendSpotWait",
            "CheckMorningArcticAppendRetryBudget", "IncrementMorningArcticAppendRetry",
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


class TestWeekdayDataSpotRetryBudget:
    """config#2542: audit of the SAME spot-reclaim/hard-fail bug class fixed for
    EOD by PR813 (2026-07-14 incident) — a spot-reclaimed morning-enrich or
    morning-arctic-append workload gets ONE relaunch-on-a-fresh-box retry
    before the pipeline accepts the failure and falls through to the
    pre-existing fail-open path. Mirrors TestEODDataSpotRetryBudget exactly."""

    def test_retry_counters_initialized_before_first_launch(self, daily):
        assert daily["CheckSkipMorningEnrich"]["Default"] == "InitMorningEnrichRetryCounter"
        assert daily["InitMorningEnrichRetryCounter"]["Type"] == "Pass"
        assert daily["InitMorningEnrichRetryCounter"]["ResultPath"] == "$.morning_enrich_retry"
        assert daily["InitMorningEnrichRetryCounter"]["Result"] == {"attempts": 0, "force_on_demand": False}
        assert daily["InitMorningEnrichRetryCounter"]["Next"] == "LaunchMorningEnrichSpot"

        assert daily["CheckMorningEnrichSpotStatus"]["Choices"][0]["Next"] == "InitMorningArcticAppendRetryCounter"
        assert daily["InitMorningArcticAppendRetryCounter"]["Type"] == "Pass"
        assert daily["InitMorningArcticAppendRetryCounter"]["ResultPath"] == "$.morning_arctic_append_retry"
        assert daily["InitMorningArcticAppendRetryCounter"]["Result"] == {"attempts": 0, "force_on_demand": False}
        assert daily["InitMorningArcticAppendRetryCounter"]["Next"] == "LaunchMorningArcticAppendSpot"

    @pytest.mark.parametrize(
        "launch_state,counter_field",
        [
            ("LaunchMorningEnrichSpot", "morning_enrich_retry"),
            ("LaunchMorningArcticAppendSpot", "morning_arctic_append_retry"),
        ],
    )
    def test_launch_threads_force_on_demand_from_retry_counter(self, daily, launch_state, counter_field):
        payload = daily[launch_state]["Parameters"]["Payload"]
        assert payload["force_on_demand.$"] == f"$.{counter_field}.force_on_demand"

    @pytest.mark.parametrize(
        "budget_state,counter_field,increment_state,relaunch_state",
        [
            ("CheckMorningEnrichRetryBudget", "$.morning_enrich_retry.attempts",
             "IncrementMorningEnrichRetry", "LaunchMorningEnrichSpot"),
            ("CheckMorningArcticAppendRetryBudget", "$.morning_arctic_append_retry.attempts",
             "IncrementMorningArcticAppendRetry", "LaunchMorningArcticAppendSpot"),
        ],
    )
    def test_one_retry_then_give_up(
        self, daily, budget_state, counter_field, increment_state, relaunch_state
    ):
        st = daily[budget_state]
        assert st["Type"] == "Choice"
        assert len(st["Choices"]) == 1
        cond = st["Choices"][0]
        assert cond["Variable"] == counter_field
        assert cond["NumericLessThan"] == 1
        assert cond["Next"] == increment_state
        # Retry budget exhausted -> the pre-existing fail-open path, never a HALT.
        assert st["Default"] == "ExtractDataSpotError"

        inc = daily[increment_state]
        assert inc["Type"] == "Pass"
        assert inc["ResultPath"] == counter_field.rsplit(".", 1)[0]
        assert inc["Parameters"]["attempts.$"] == f"States.MathAdd({counter_field}, 1)"
        # The one retry must never gamble on spot a second time.
        assert inc["Parameters"]["force_on_demand"] is True
        # The retry relaunches on a FRESH box — same launch state, not a
        # separate "retry launch" — a plain Lambda invoke each time.
        assert inc["Next"] == relaunch_state


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
        # 2026-07-14: force_on_demand.$ threads the retry-budget's on-demand
        # override into the dispatcher (see TestEODDataSpotRetryBudget).
        assert set(st["Parameters"]["Payload"]) == {"workload", "force_on_demand.$"}

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
        # 2026-07-14 (root cause: AWS spot reclaim mid-job, Server.SpotInstanceTermination):
        # a terminal non-Success poll status now routes through a one-shot
        # retry-budget Choice BEFORE falling through to the fail-open
        # ExtractDataSpotError normalizer — see TestEODDataSpotRetryBudget.
        assert eod["CheckPostMarketDataSpotStatus"]["Default"] == "CheckDataSpotRetryBudget"
        assert eod["CheckPostMarketArcticAppendSpotStatus"]["Default"] == "CheckDataSpotArcticRetryBudget"
        assert eod["CheckDataSpotRetryBudget"]["Default"] == "ExtractDataSpotError"
        assert eod["CheckDataSpotArcticRetryBudget"]["Default"] == "ExtractDataSpotError"

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
            "CheckDataSpotRetryBudget", "IncrementDataSpotRetry",
            "InitDataSpotArcticRetryCounter",
            "LaunchPostMarketArcticAppendSpot", "CheckPostMarketArcticAppendSpotLaunched",
            "PollPostMarketArcticAppendSpot", "CheckPostMarketArcticAppendSpotStatus",
            "PostMarketArcticAppendSpotWait",
            "CheckDataSpotArcticRetryBudget", "IncrementDataSpotArcticRetry",
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


class TestEODDataSpotRetryBudget:
    """2026-07-14 incident fix: a spot-reclaimed data-spot workload (AWS
    Server.SpotInstanceTermination — first observed 2026-07-14, ~22min into a
    post-market-data run) gets ONE relaunch-on-a-fresh-box retry before the
    pipeline accepts the failure and falls through to the pre-existing
    fail-open path. Bounded, not unbounded — a second consecutive
    interruption still falls through, so this can never loop indefinitely."""

    def test_retry_counters_initialized_before_first_launch(self, eod):
        assert eod["CheckSkipPostMarketData"]["Default"] == "InitDataSpotRetryCounter"
        assert eod["InitDataSpotRetryCounter"]["Type"] == "Pass"
        assert eod["InitDataSpotRetryCounter"]["ResultPath"] == "$.data_spot_retry"
        assert eod["InitDataSpotRetryCounter"]["Result"] == {"attempts": 0, "force_on_demand": False}
        assert eod["InitDataSpotRetryCounter"]["Next"] == "LaunchPostMarketDataSpot"

        assert eod["CheckPostMarketDataSpotStatus"]["Choices"][0]["Next"] == "InitDataSpotArcticRetryCounter"
        assert eod["InitDataSpotArcticRetryCounter"]["Type"] == "Pass"
        assert eod["InitDataSpotArcticRetryCounter"]["ResultPath"] == "$.data_spot_arctic_retry"
        assert eod["InitDataSpotArcticRetryCounter"]["Result"] == {"attempts": 0, "force_on_demand": False}
        assert eod["InitDataSpotArcticRetryCounter"]["Next"] == "LaunchPostMarketArcticAppendSpot"

    @pytest.mark.parametrize(
        "launch_state,counter_field",
        [
            ("LaunchPostMarketDataSpot", "data_spot_retry"),
            ("LaunchPostMarketArcticAppendSpot", "data_spot_arctic_retry"),
        ],
    )
    def test_launch_threads_force_on_demand_from_retry_counter(self, eod, launch_state, counter_field):
        payload = eod[launch_state]["Parameters"]["Payload"]
        assert payload["force_on_demand.$"] == f"$.{counter_field}.force_on_demand"

    @pytest.mark.parametrize(
        "budget_state,counter_field,increment_state,relaunch_state",
        [
            ("CheckDataSpotRetryBudget", "$.data_spot_retry.attempts",
             "IncrementDataSpotRetry", "LaunchPostMarketDataSpot"),
            ("CheckDataSpotArcticRetryBudget", "$.data_spot_arctic_retry.attempts",
             "IncrementDataSpotArcticRetry", "LaunchPostMarketArcticAppendSpot"),
        ],
    )
    def test_one_retry_then_give_up(
        self, eod, budget_state, counter_field, increment_state, relaunch_state
    ):
        st = eod[budget_state]
        assert st["Type"] == "Choice"
        assert len(st["Choices"]) == 1
        cond = st["Choices"][0]
        assert cond["Variable"] == counter_field
        assert cond["NumericLessThan"] == 1
        assert cond["Next"] == increment_state
        # Retry budget exhausted -> the pre-existing fail-open path, never a HALT.
        assert st["Default"] == "ExtractDataSpotError"

        inc = eod[increment_state]
        assert inc["Type"] == "Pass"
        assert inc["ResultPath"] == counter_field.rsplit(".", 1)[0]
        assert inc["Parameters"]["attempts.$"] == f"States.MathAdd({counter_field}, 1)"
        # The one retry must never gamble on spot a second time.
        assert inc["Parameters"]["force_on_demand"] is True
        # The retry relaunches on a FRESH box — same launch state, not a
        # separate "retry launch" — a plain Lambda invoke each time.
        assert inc["Next"] == relaunch_state


class TestEODReconcileSkippedOnDataGap:
    """2026-07-14 incident fix, part 2: even with the retry budget above, the
    data-spot phase can still end in $.data_spot_error (retry exhausted). That
    condition GUARANTEES eod_reconcile.py's _spy_close hard-fail (no fallback
    by design — today's SPY close was never written to ArcticDB), so
    CheckSkipEODReconcile must route around EODReconcile entirely instead of
    letting a guaranteed crash fall through to the generic HandleFailure ->
    FailExecution path (which mislabels a known, self-healing data gap as a
    pipeline defect — the false 'EOD Pipeline — FAILED' page from 2026-07-14)."""

    def test_data_gap_branch_precedes_default(self, eod):
        # config-I2702 (2026-07-15): the $.data_spot_error launch-phase flag
        # test was REPLACED by a fresh verify-by-artifact probe result — see
        # test_sf_eod_precondition_probe_wiring.py for the full pinning of
        # ProbeEODReconcilePrecondition + the closed self-heal loop this
        # branch now feeds into. This test only re-confirms the Choice shape
        # at CheckSkipEODReconcile itself.
        st = eod["CheckSkipEODReconcile"]
        assert st["Type"] == "Choice"
        gap_choices = [
            c for c in st["Choices"]
            if any(cond.get("Variable") == "$.precondition_probe.Payload.precondition_met"
                   for cond in c.get("And", []))
        ]
        assert len(gap_choices) == 1
        conds = gap_choices[0]["And"]
        assert any(c.get("IsPresent") is True for c in conds)
        assert any(c.get("BooleanEquals") is False for c in conds)
        assert gap_choices[0]["Next"] == "SkipEODReconcileDataGap"
        # No leftover reference to the old flag anywhere in this Choice.
        assert not any(c.get("Variable") == "$.data_spot_error" for c in st["Choices"])
        # The pre-existing operator-replay skip_eod_reconcile branch is untouched.
        assert st["Default"] == "EODReconcile"

    def test_skip_state_is_sns_publish_not_a_swallow(self, eod):
        # feedback_no_silent_fails: a skip must still be LOUD — a distinct,
        # accurately-worded SNS publish, not a bare Pass-through.
        st = eod["SkipEODReconcileDataGap"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        subject = st["Parameters"]["Subject"]
        assert 0 < len(subject) <= 100
        assert "\n" not in subject
        assert "SKIPPED" in subject
        assert "FAILED" not in subject, (
            "must read as a known/self-healing skip, not the generic pipeline-"
            "failed alert it replaces"
        )
        message_fmt = st["Parameters"]["Message.$"]
        # I2702: the skip is now decided by the precondition PROBE (verify-by-
        # artifact), not the launch-phase $.data_spot_error flag — the message
        # must reference the probe result and the closed-loop self-heal, and
        # must NOT resurrect the retired manual-operator-replay instruction.
        assert "States.JsonToString($.precondition_probe)" in message_fmt
        assert "self-heal" in message_fmt
        assert "operator-replay" not in message_fmt

    def test_skip_state_never_reaches_a_halt(self, eod):
        # config-I2702: SkipEODReconcileDataGap now enters the closed
        # self-heal loop (SetDegradedFlag) instead of jumping straight to the
        # substrate-check gate — the loop's own reachability (never hitting
        # _HALT, always eventually reaching StopTradingInstance) is pinned in
        # test_sf_eod_precondition_probe_wiring.py.
        for tgt in _all_targets(eod["SkipEODReconcileDataGap"]):
            assert tgt not in _HALT
        assert eod["SkipEODReconcileDataGap"]["Next"] == "SetDegradedFlag"

    def test_skip_states_own_sns_failure_still_continues(self, eod):
        # Mirrors HandleFailure's defense-in-depth: an SNS-side failure here
        # must not block entry into the self-heal loop (config-I2702).
        catches = eod["SkipEODReconcileDataGap"].get("Catch", [])
        assert any(
            c["ErrorEquals"] == ["States.ALL"] and c["Next"] == "SetDegradedFlag"
            for c in catches
        )


# ══════════════════════════════════════════════════════════════════════════
# Dispatcher Lambda + IAM (deliverables #1, #3)
# ══════════════════════════════════════════════════════════════════════════
class TestDispatcherLambdaAndIam:
    def test_dispatcher_package_present(self):
        # deploy.sh is load-bearing, NOT optional: like every sibling dispatcher
        # (scheduled-groom-dispatcher, spot-orphan-reaper), the function is
        # operator-deployed OUTSIDE CloudFormation, so a runnable deploy script
        # IS the deployment mechanism. #643 (config#1767 Phase 2) shipped this
        # dispatcher's source + IAM + SF wiring but NO deploy.sh, so step 1 of
        # the README rollout ("create the Lambda + role") had no tooling and was
        # skipped — the live function was never created and the 2026-07-08 EOD
        # LaunchPostMarketDataSpot got a 404 ResourceNotFoundException. This guard
        # fails loud so a data-spot dispatcher can never again merge un-deployable.
        for f in ("index.py", "iam-policy.json", "sf-execution-iam-policy.json",
                  "requirements.txt", "deploy.sh"):
            assert (_DISPATCHER / f).exists(), f"data-spot-dispatcher/{f} missing"

    def test_dispatcher_deploy_sh_creates_the_function(self):
        # A deploy.sh that exists but doesn't actually create the Lambda would
        # re-open the same gap. Pin the two commands that make it a real,
        # first-time-capable deployer for THIS function.
        deploy = (_DISPATCHER / "deploy.sh").read_text()
        assert "alpha-engine-data-spot-dispatcher" in deploy, \
            "deploy.sh must target the alpha-engine-data-spot-dispatcher function"
        assert "aws lambda create-function" in deploy, \
            "deploy.sh must be able to CREATE the function (first-time bootstrap), not only update it"

    def test_workload_map_preserves_collector_contract(self):
        # M0 contract: the spot workloads run the SAME weekly_collector.py entry
        # points the on-trading states ran — unchanged args = unchanged data paths.
        # The workload KEYS are post-market-* (SF-facing); the VALUES must mirror
        # the old on-trading SSM commands (--daily*, NOT invented --post-market-*).
        src = (_DISPATCHER / "index.py").read_text()
        for token in (
            "--morning-enrich",
            "--morning-arctic-append",
            '"post-market-data"',
            '"post-market-arctic-append"',
        ):
            assert token in src, f"dispatcher workload map missing {token}"
        assert '"post-market-data":' in src
        assert "python weekly_collector.py --daily --skip-arctic-append" in src
        assert '"post-market-arctic-append":' in src
        assert "python weekly_collector.py --daily-arctic-append" in src
        # #643 shipped bogus --post-market-* CLI flags that weekly_collector.py
        # never defined — broke the 2026-07-08 EOD run on first live spot path.
        assert "--post-market-data" not in src.replace(
            '"post-market-data"', ""
        ).replace('"post-market-arctic-append"', "")
        assert "--post-market-arctic-append" not in src.replace(
            '"post-market-arctic-append"', ""
        )
        # The enrich workload must still skip the inline heal + inline append.
        assert "--skip-chronic-heal" in src
        assert "--skip-arctic-append" in src

    def test_daily_heal_workload_present(self):
        # alpha-engine-config-I2717 (2026-07-16): the standalone daily-heal
        # workload, invoked directly by its own EventBridge rule (NOT by
        # either SF — see infrastructure/cloudformation/alpha-engine-
        # orchestration.yaml DailyHealTrigger). Bundles the universe-gap
        # self-heal (formerly the head of --morning-arctic-append) and the
        # chronic-polygon-gap heal (formerly the weekday SF's own
        # ChronicGapSelfHeal state) into one weekly_collector.py invocation.
        src = (_DISPATCHER / "index.py").read_text()
        assert '"daily-heal":' in src
        assert "python weekly_collector.py --daily-heal" in src

    def test_daily_heal_workload_key_satisfies_strict_allowlist_regex(self):
        # _resolve_workload's defense-in-depth allowlist regex
        # (^[a-z][a-z-]{0,63}$) gates every workload key against
        # shell-metacharacter injection — "daily-heal" must satisfy it (mirrors
        # the same check the module already applies to every other key; kept
        # as a literal regex here rather than importing index.py directly, to
        # match this file's existing text-only-assertion convention and avoid
        # a real boto3/nousergon_lib import at collection time).
        import re
        assert re.match(r"^[a-z][a-z-]{0,63}$", "daily-heal")

    def test_bootstrap_clones_private_config_package(self):
        # weekly_collector.load_config resolves experiments/reference/data/config.yaml
        # from a shallow alpha-engine-config clone (2026-07-08 EOD: missing clone →
        # FileNotFoundError on the first live spot path after the CLI-flag fix).
        src = (_DISPATCHER / "index.py").read_text()
        assert "alpha-engine-config" in src
        assert "ssm get-parameter" in src
        assert "/alpha-engine/saturday_sf_watch/github_pat" in src
        assert "ALPHA_ENGINE_EXPERIMENT_ID=reference" in src

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
