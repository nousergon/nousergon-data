"""Pins the alpha-engine-config-I2702 closed-loop EOD self-heal: verify-by-
artifact precondition probe (deliverable #1), the closed heal loop
(deliverable #3), and the degraded terminal state (deliverable #4).

Background (2026-07-15 incident, config-I2699 + config-I2702): the EOD SF's
old ``CheckSkipEODReconcile`` skip decision keyed on a LAUNCH-PHASE flag
(``$.data_spot_error``), which a poll-side transient AWS SDK hiccup could set
even while the underlying collector was still running and later finished
rc=0 — a decision-on-stale-signal bug (same class as the groom auto-clear
trusting a text match, 2026-07-11). Recovery bottomed out in a MANUAL
operator-replay step. This test pins the replacement: a fresh S3 read of a
readback-verified artifact (deliverable #1), an automatic dispatch-reprobe-
replay loop bounded by attempts + a deadline (deliverable #3), and a
terminal state that can never look plain-green when EODReconcile was skipped
(deliverable #4).

Companion tests:
  * test_sf_eod_skipgate_wiring.py — the CheckSkipEODReconcile Choice shape
    + the per-task rerun gate chain (unaffected Choices[0] branch).
  * test_sf_data_spot_relocation_wiring.py — the CheckSkipEODReconcile
    data-gap branch's Choice condition + SkipEODReconcileDataGap's
    fail-open Catch (both updated in the same PR to route into this file's
    heal loop instead of straight to the cost-guard tail).
  * test_sf_eod_substrate_check_wiring.py — StopTradingInstance's routing
    change (End:true -> Next: CheckDegradedOutcome).
  * test_sf_payload_uniqueness.py — the new top-level ``$.<X>`` fields this
    file introduces, in the closed namespace registry.
  * test_sf_iam_lambda_grants.py — the new
    ``alpha-engine-eod-precondition-probe`` Lambda ARN is IAM-grantable.
  * infrastructure/lambdas/eod-precondition-probe/test_handler.py — the
    probe Lambda's own unit tests (S3 sentinel read + evaluate + deadline math).
"""

from __future__ import annotations

import json
from collections import deque
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"
_SF_ROLE = _REPO_ROOT / "infrastructure" / "iam" / "alpha-engine-step-functions-role.json"

_PROBE_FN = "alpha-engine-eod-precondition-probe"
_DISPATCHER_FN = "alpha-engine-data-spot-dispatcher"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def doc() -> dict:
    return json.loads(_SF_PATH.read_text())


def _targets(state: dict) -> list[str]:
    out: list[str] = []
    if "Next" in state:
        out.append(state["Next"])
    if "Default" in state:
        out.append(state["Default"])
    for c in state.get("Choices", []):
        if "Next" in c:
            out.append(c["Next"])
    for c in state.get("Catch", []):
        if "Next" in c:
            out.append(c["Next"])
    return out


# ── Whole-doc structural sanity (belt-and-suspenders on top of the manual
#    per-state pins below — catches typos in any Next/Default the targeted
#    tests don't happen to enumerate) ────────────────────────────────────────


def test_every_state_reference_resolves(states):
    missing = {
        (name, tgt)
        for name, st in states.items()
        for tgt in _targets(st)
        if tgt not in states
    }
    assert not missing


def test_every_state_reachable_from_start(doc, states):
    seen = set()
    q = deque([doc["StartAt"]])
    while q:
        n = q.popleft()
        if n in seen:
            continue
        seen.add(n)
        q.extend(t for t in _targets(states[n]) if t not in seen)
    assert set(states) - seen == set()


def test_succeed_and_fail_states_have_no_next(states):
    for name, st in states.items():
        if st["Type"] in ("Succeed", "Fail"):
            assert "Next" not in st, f"{name} is Type={st['Type']} but has a Next"


# ── Deliverable #1: ProbeEODReconcilePrecondition ────────────────────────────


class TestPreconditionProbe:
    def test_probe_invokes_the_new_lambda(self, states):
        st = states["ProbeEODReconcilePrecondition"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::lambda:invoke"
        assert st["Parameters"]["FunctionName"] == _PROBE_FN
        assert st["Parameters"]["Payload"] == {"run_date.$": "$.run_date"}
        assert st["ResultPath"] == "$.precondition_probe"
        assert st["Next"] == "CheckSkipEODReconcile"

    def test_probe_infra_failure_falls_through_to_reconcile_not_skip(self, states):
        # Deliberately fail-SAFE-toward-reconcile: a probe-infra failure must
        # NOT set $.precondition_probe at all, so CheckSkipEODReconcile's
        # IsPresent+BooleanEquals(false) test cannot match and falls through
        # to Default (EODReconcile) — the proven _spy_close hard-fail remains
        # the backstop if data is genuinely absent.
        st = states["ProbeEODReconcilePrecondition"]
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == "CheckSkipEODReconcile"
        assert catches[0]["ResultPath"] is None

    def test_reconcile_gate_reads_the_probe_not_the_old_flag(self, states):
        cser = states["CheckSkipEODReconcile"]
        gap_choice = next(
            c for c in cser["Choices"]
            if any(cond.get("Variable") == "$.precondition_probe.Payload.precondition_met"
                   for cond in c.get("And", []))
        )
        assert gap_choice["Next"] == "SkipEODReconcileDataGap"
        assert not any(
            cond.get("Variable") == "$.data_spot_error"
            for c in cser["Choices"] for cond in c.get("And", [])
        )

    def test_probe_is_called_again_inside_the_heal_loop(self, states):
        # HealReProbe is a second call site of the SAME Lambda, re-verifying
        # fresh (never trusting the pre-dispatch probe result) after the
        # heal loop dispatches the missing workload(s).
        st = states["HealReProbe"]
        assert st["Type"] == "Task"
        assert st["Parameters"]["FunctionName"] == _PROBE_FN
        assert st["ResultPath"] == "$.precondition_probe"

    def test_probe_lambda_package_present(self):
        d = _REPO_ROOT / "infrastructure" / "lambdas" / "eod-precondition-probe"
        for f in ("index.py", "test_handler.py", "deploy.sh", "iam-policy.json", "requirements.txt"):
            assert (d / f).exists(), f"missing {d / f}"


# ── Deliverable #4: degraded terminal state ──────────────────────────────────


class TestDegradedTerminalState:
    def test_gap_detection_sets_the_degraded_flag(self, states):
        assert states["SkipEODReconcileDataGap"]["Next"] == "SetDegradedFlag"
        sdf = states["SetDegradedFlag"]
        assert sdf["Type"] == "Pass"
        assert sdf["Parameters"]["degraded"] is True
        assert sdf["ResultPath"] == "$.degraded_summary"

    def test_stop_trading_instance_leads_to_the_degraded_check(self, states):
        assert "End" not in states["StopTradingInstance"]
        assert states["StopTradingInstance"]["Next"] == "CheckDegradedOutcome"

    def test_degraded_outcome_routes_on_the_flag(self, states):
        cdo = states["CheckDegradedOutcome"]
        assert cdo["Type"] == "Choice"
        c = cdo["Choices"][0]
        assert c["Variable"] == "$.degraded_summary.degraded"
        assert c["BooleanEquals"] is True
        assert c["Next"] == "DegradedSucceeded"
        assert cdo["Default"] == "NormalSucceeded"

    def test_two_distinct_succeed_states_exist(self, states):
        assert states["NormalSucceeded"]["Type"] == "Succeed"
        assert states["DegradedSucceeded"]["Type"] == "Succeed"
        assert states["NormalSucceeded"] != states["DegradedSucceeded"]

    def test_a_run_that_never_hits_the_gap_cannot_reach_degraded_succeeded(self, states):
        # Structural sanity: DegradedSucceeded is reachable ONLY via
        # CheckDegradedOutcome, which is reachable ONLY via StopTradingInstance
        # — there is no direct edge from anywhere else in the file.
        producers = [
            name for name, st in states.items()
            if "DegradedSucceeded" in _targets(st)
        ]
        assert producers == ["CheckDegradedOutcome"]


# ── Deliverable #3: closed self-heal loop ────────────────────────────────────


class TestHealLoopEligibility:
    def test_operator_replay_does_not_recurse(self, states):
        # A replay execution that still finds the precondition unmet must
        # page immediately, not spawn a further heal loop (replays run with
        # skip_post_market_data=true — no data-spot phase to retry).
        chle = states["CheckHealLoopEligible"]
        assert chle["Type"] == "Choice"
        c = chle["Choices"][0]
        assert c["Variable"] == "$.pipeline_role"
        assert c["StringEquals"] == "operator-replay"
        assert c["Next"] == "HealNonConvergent"
        assert chle["Default"] == "InitHealLoop"

    def test_set_degraded_flag_enters_eligibility_check(self, states):
        assert states["SetDegradedFlag"]["Next"] == "CheckHealLoopEligible"


class TestHealLoopBound:
    def test_init_starts_at_zero_attempts(self, states):
        st = states["InitHealLoop"]
        assert st["Type"] == "Pass"
        assert st["Result"] == {"attempts": 0}
        assert st["ResultPath"] == "$.heal_loop"
        assert st["Next"] == "HealLoopGate"

    def test_gate_trips_on_attempts_or_deadline(self, states):
        gate = states["HealLoopGate"]
        assert gate["Type"] == "Choice"
        c = gate["Choices"][0]
        assert "Or" in c
        variables = {cond["Variable"] for cond in c["Or"]}
        assert variables == {"$.heal_loop.attempts", "$.precondition_probe.Payload.past_deadline"}
        assert c["Next"] == "HealNonConvergent"
        assert gate["Default"] == "HealLaunchPostMarketDataSpot"

    def test_attempts_bound_is_two(self, states):
        gate = states["HealLoopGate"]
        attempts_cond = next(
            c for c in gate["Choices"][0]["Or"] if c["Variable"] == "$.heal_loop.attempts"
        )
        assert attempts_cond["NumericGreaterThanEquals"] == 2

    def test_increment_advances_the_counter_and_loops_back(self, states):
        inc = states["HealLoopIncrement"]
        assert inc["Type"] == "Pass"
        assert inc["Parameters"]["attempts.$"] == "States.MathAdd($.heal_loop.attempts, 1)"
        assert inc["ResultPath"] == "$.heal_loop"
        assert inc["Next"] == "HealLoopGate"

    @pytest.mark.parametrize("failure_state", [
        "HealLaunchPostMarketDataSpot", "HealCheckPostMarketDataSpotLaunched",
        "HealPollPostMarketDataSpot", "HealCheckPostMarketDataSpotStatus",
        "HealLaunchArcticAppendSpot", "HealCheckArcticAppendSpotLaunched",
        "HealPollArcticAppendSpot", "HealCheckArcticAppendSpotStatus",
        "HealReProbe", "HealCheckConverged",
    ])
    def test_every_failure_mode_in_the_loop_reaches_the_increment(self, states, failure_state):
        # No dead end anywhere in the dispatch-poll-reprobe chain — every
        # non-success branch must funnel back to HealLoopIncrement so the
        # attempts/deadline bound (not an unbounded retry) is what stops it.
        assert "HealLoopIncrement" in _targets(states[failure_state]), (
            f"{failure_state} has a branch that does not reach HealLoopIncrement: "
            f"{_targets(states[failure_state])}"
        )


class TestHealLoopDispatchChain:
    def test_launch_postmarket_dispatches_with_force_on_demand(self, states):
        st = states["HealLaunchPostMarketDataSpot"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::lambda:invoke"
        assert st["Parameters"]["FunctionName"] == _DISPATCHER_FN
        assert st["Parameters"]["Payload"] == {
            "workload": "post-market-data", "force_on_demand": True,
        }
        assert st["Next"] == "HealCheckPostMarketDataSpotLaunched"

    def test_postmarket_success_chains_to_arctic_append(self, states):
        succ = [c["Next"] for c in states["HealCheckPostMarketDataSpotStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["HealLaunchArcticAppendSpot"]

    def test_postmarket_inprogress_loops_on_the_ssm_command_state(self, states):
        # Deliverable #5: poll the SSM command's own state — unbounded wait
        # loop (no attempt cap), bounded only by the box's own watchdog.
        inprog = [c["Next"] for c in states["HealCheckPostMarketDataSpotStatus"]["Choices"]
                  if c.get("StringEquals") == "InProgress"]
        assert inprog == ["HealPostMarketDataSpotWait"]
        assert states["HealPostMarketDataSpotWait"]["Next"] == "HealPollPostMarketDataSpot"

    def test_launch_arctic_dispatches_with_force_on_demand(self, states):
        st = states["HealLaunchArcticAppendSpot"]
        assert st["Parameters"]["FunctionName"] == _DISPATCHER_FN
        assert st["Parameters"]["Payload"] == {
            "workload": "post-market-arctic-append", "force_on_demand": True,
        }

    def test_arctic_success_chains_to_reprobe(self, states):
        succ = [c["Next"] for c in states["HealCheckArcticAppendSpotStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["HealReProbe"]

    def test_reprobe_success_chains_to_convergence_check(self, states):
        assert states["HealReProbe"]["Next"] == "HealCheckConverged"

    def test_converged_choice_dispatches_the_replay(self, states):
        hcc = states["HealCheckConverged"]
        c = hcc["Choices"][0]
        assert c["Variable"] == "$.precondition_probe.Payload.precondition_met"
        assert c["BooleanEquals"] is True
        assert c["Next"] == "HealDispatchReplay"
        assert hcc["Default"] == "HealLoopIncrement"


class TestHealDispatchReplay:
    """Deliverable #3(c): the auto-replay reuses I2700's PROVEN input shape
    exactly (skip_post_market_data + skip_capture_snapshot), as a SEPARATE
    self-referential execution (never an in-place jump back through
    CaptureSnapshot, which already ran once earlier in this same execution)."""

    def test_self_referential_start_execution(self, states):
        st = states["HealDispatchReplay"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::states:startExecution"
        assert st["Parameters"]["StateMachineArn.$"] == "$$.StateMachine.Id"

    def test_replay_input_matches_i2700_proven_shape(self, states):
        inp = states["HealDispatchReplay"]["Parameters"]["Input"]
        assert inp["pipeline_role"] == "operator-replay"
        assert inp["skip_post_market_data"] is True
        assert inp["skip_capture_snapshot"] is True
        assert inp["run_date.$"] == "$.run_date"
        assert inp["trading_instance_id.$"] == "$.trading_instance_id"
        assert inp["ec2_instance_id.$"] == "$.ec2_instance_id"

    def test_replay_dispatch_failure_pages_not_silently_succeeds(self, states):
        st = states["HealDispatchReplay"]
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == "HealReplayDispatchFailed"

    def test_success_reaches_converged_notify(self, states):
        assert states["HealDispatchReplay"]["Next"] == "HealConvergedNotify"


class TestHealOutcomeNotifications:
    """The three SNS publishes downstream of the loop — each must be an SNS
    Task (loud, not a swallow) and each must eventually reach the
    cost-guard tail (CheckSkipDailySubstrateHealthCheck) regardless of its
    own success/failure, mirroring every other best-effort-notify Catch
    already established in this file (PublishDataSpotFailureImmediate,
    PublishSubstrateHealthCheckDegradedAlert)."""

    @pytest.mark.parametrize("state_name,subject_substr", [
        ("HealConvergedNotify", "CONVERGED"),
        ("HealReplayDispatchFailed", "REPLAY DISPATCH FAILED"),
        ("HealNonConvergent", "DID NOT CONVERGE"),
    ])
    def test_is_sns_publish_with_distinct_subject(self, states, state_name, subject_substr):
        st = states[state_name]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        assert subject_substr in st["Parameters"]["Subject"]
        assert 0 < len(st["Parameters"]["Subject"]) <= 100
        assert "\n" not in st["Parameters"]["Subject"]

    @pytest.mark.parametrize("state_name", [
        "HealConvergedNotify", "HealReplayDispatchFailed", "HealNonConvergent",
    ])
    def test_reaches_cost_guard_tail_on_success_and_on_sns_failure(self, states, state_name):
        st = states[state_name]
        assert st["Next"] == "CheckSkipDailySubstrateHealthCheck"
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == "CheckSkipDailySubstrateHealthCheck"

    def test_nonconvergent_never_reaches_a_halt_state(self, states):
        _HALT = {"HandleFailure", "FailExecution", "ForceStopInstance"}
        for tgt in _targets(states["HealNonConvergent"]):
            assert tgt not in _HALT


# ── IAM: the SF role can invoke the new Lambda + self-start-execution ───────


class TestIamGrants:
    @pytest.fixture(scope="class")
    def policy(self):
        return json.loads(_SF_ROLE.read_text())

    def test_probe_lambda_is_grantable(self, policy):
        lambda_stmt = next(
            s for s in policy["Statement"] if s.get("Action") == "lambda:InvokeFunction"
        )
        assert any(
            r.rstrip("*") == f"arn:aws:lambda:us-east-1:711398986525:function:{_PROBE_FN}"
            for r in lambda_stmt["Resource"]
        )

    def test_self_start_execution_is_granted(self, policy):
        assert any(
            s.get("Action") == "states:StartExecution"
            and s.get("Resource") == "arn:aws:states:us-east-1:711398986525:stateMachine:ne-postclose-trading-pipeline"
            for s in policy["Statement"]
        )
