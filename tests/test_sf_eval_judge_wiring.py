"""Pins the LLM-as-judge wiring in the Saturday Step Functions JSON.

Catches regressions like: someone re-routes CheckBacktesterStatus.Success
back to SaturdayHealthCheck and accidentally drops the eval state, or
flips the Default branch of the cadence Choice and ships every Saturday
on the (more expensive) monthly Sonnet sweep.

Legacy single-Lambda design (EvalJudgeFirstSaturday + EvalJudgeWeekly
Task states) was replaced 2026-05-07 by the Anthropic Message Batches
API chain — Submit → Poll-loop → Process — closing ROADMAP P1 §1642.
The 50% batch cost discount + decoupled submit/pickup structurally
bypass the Lambda 15-min timeout class that nearly fired on the
2026-05-06 manual midweek SF run.

The corresponding alpha-engine-research Lambdas
(``alpha-engine-research-eval-judge-{submit,poll,process}:live``) are
in the companion research-repo PR; this test only asserts the SF
wiring, not handler shape.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    """Flattened state view: top-level states UNION every Parallel
    branch's states.

    Post the 2026-05-16 Research || PredictorTraining SF Parallel
    restructure (plan
    alpha-engine-docs/private/research-predictor-parallel-260516.md) the
    entire eval-judge + agent-justification chain moved INSIDE Branch A
    of the ResearchPredictorParallel state, and PredictorTraining moved
    into Branch B. Every per-state shape assertion in this file (payload,
    retry, timeout, Catch posture, in-chain Next edges) is still true —
    the states just nest one level deeper. Flattening keeps those
    assertions intact while the few tests that pinned the OLD
    cross-boundary edges (Counterfactual → CheckSkipPredictorTraining)
    are updated to the new branch-local terminal + post-join semantics.
    """
    flat: dict = dict(sf["States"])
    for st in sf["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat


# ── State presence ────────────────────────────────────────────────────────



def converges_on(states, start, target, limit=6):
    """Walk ``Next`` from *start* through Pass hops and report whether *target*
    is reached.

    alpha-engine-config-I9636. Every one of these assertions used to pin
    ``catch["Next"] == "MarkEvalJudgeDegraded"`` literally, which pinned the
    EDGE rather than the INVARIANT — and the invariant is that no eval-judge
    fail-open exit reaches EvalRollingMean unmarked. The literal form made the
    correct fix (an Extract* hop that names the phase, the shape
    ExtractEvalJudgeProcessError has used since I9329) look like a regression
    in fifteen tests, which is a good way to talk a future session out of
    naming the phase. This form is strictly stronger: it still fails if the
    edge stops converging, and it stays true when a hop is inserted.
    """
    seen = set()
    cur = start
    for _ in range(limit):
        if cur == target:
            return True
        if cur in seen or cur not in states:
            return False
        seen.add(cur)
        state = states[cur]
        if state.get("Type") != "Pass":
            return False
        cur = state.get("Next")
    return False


def phase_named_by(states, name):
    """The ``phase`` an Extract* hop stamps, or None if *name* stamps none."""
    return (states.get(name, {}).get("Parameters") or {}).get("phase")


class TestStatesPresent:
    def test_all_eval_judge_states_exist(self, states):
        for name in (
            "CheckSkipEvalJudge",
            "ComputeEvalCadence",
            "CheckMonthlyCadence",
            # Batches API chain (replaces EvalJudgeFirstSaturday +
            # EvalJudgeWeekly Task states from the legacy single-Lambda
            # design, ROADMAP P1 §1642 closure 2026-05-07).
            "EvalJudgeSubmitFirstSaturday",
            "EvalJudgeSubmitWeekly",
            # alpha-engine-config-I9329: the four EvalJudgePoll* states are
            # gone with the provider batch API they drove, and Process runs on
            # a dedicated spot box reached over SSM.
            "EvalJudgeSubmitOutcome",
            "EvalJudgeEmptyPlan",
            "PrepareEvalJudgeSpotDispatch",
            "DispatchEvalJudgeSpot",
            "MergeEvalJudgeSpotInstanceId",
            "InitEvalJudgeSpotBootstrapPollCount",
            "WaitForEvalJudgeSpotBootstrap",
            "CheckEvalJudgeSpotBootstrapStatus",
            "EvalJudgeSpotBootstrapLivenessGate",
            "EvalJudgeSpotRelaunch",
            "EvalJudgeProcess",
            "InitEvalJudgeProcessPollCount",
            "WaitForEvalJudgeProcess",
            "CheckEvalJudgeProcessStatus",
            "EvalJudgeProcessLivenessGate",
            "EvalRollingMean",
            "CheckSkipRationaleClustering",
            "RationaleClustering",
            "CheckSkipCounterfactual",
            "Counterfactual",
        ):
            assert name in states, f"missing SF state: {name}"

    def test_the_poll_chain_is_gone(self, states):
        """alpha-engine-config-I9329, verified RED against pre-fix code.

        Submit -> Poll -> Process existed to drive an ASYNCHRONOUS provider
        batch API. That API is retired (-I9263), and with no batch rung there
        is nothing to poll: `poll_batch` returned terminal immediately for both
        synthetic id prefixes, so the states could only ever fall straight
        through. Pinned as an ABSENCE sweep rather than four named assertions
        because the defect it guards is a redrive under the old names.
        """
        stale = sorted(n for n in states if n.startswith("EvalJudgePoll"))
        assert not stale, f"EvalJudgePoll* states still present: {stale}"

    def test_legacy_single_lambda_states_removed(self, states):
        """The legacy single-Lambda Task states were replaced by the
        batch chain. Pin the absence so a redrive of the old code path
        can't silently ship under the old names."""
        assert "EvalJudgeFirstSaturday" not in states
        assert "EvalJudgeWeekly" not in states


# ── Backtester success → evaluator skip-gate ──────────────────────────────


class TestBacktesterTransition:
    def test_success_routes_to_evaluator_skip_gate(self, states):
        # Post-2026-05-07 split: Backtester success routed to
        # CheckSkipEvaluator (the gate in front of the standalone
        # Evaluator state).
        #
        # Post-2026-05-16 preflight-task-split P1: the parity stage was
        # split out of the combined Backtester state into its own Parity
        # quartet, reached via CheckSkipParity.
        #
        # Post-2026-05-31 L4472 phase-split: the backtest stage is further
        # decomposed by --mode into Backtester (simulate) → PredictorBacktest
        # → PortfolioOptimizerBacktest → CheckSkipParity. CheckSkipEvaluator
        # (the eval-judge gate) stays reachable transitively through the whole
        # chain; pinned here by walking each status gate's Success edge.
        # Post-config#830: skip-gates (CheckSkipPredictorBacktest /
        # CheckSkipPortfolioOptimizerBacktest) precede the predictor + optimizer
        # stages so mode=backtest-eval can bypass them; each defaults to running
        # its stage, so the chain to CheckSkipEvaluator is unchanged on a normal run.
        bt = states["CheckBacktesterStatus"]
        success_choice = next(
            c for c in bt["Choices"] if c.get("StringEquals") == "Success"
        )
        assert success_choice["Next"] == "CheckSkipPredictorBacktest"
        assert states["CheckSkipPredictorBacktest"]["Default"] == "PredictorBacktest"

        def _success(check):
            return next(
                c for c in states[check]["Choices"]
                if c.get("StringEquals") == "Success"
            )["Next"]

        # Walk the L4472 split chain (through the config#830 skip-gates) to the
        # parity skip-gate.
        assert _success("CheckPredictorBacktestStatus") == "CheckSkipPortfolioOptimizerBacktest"
        assert states["CheckSkipPortfolioOptimizerBacktest"]["Default"] == "PortfolioOptimizerBacktest"
        assert _success("CheckPortfolioOptimizerBacktestStatus") == "CheckSkipParity"

        # skip_parity short-circuit reaches the Evaluator gate via
        # MarkParityVerdictUnknownByCadence (alpha-engine-config-I11103),
        # which stamps $.parity_verdict_unknown and changes nothing else.
        skip_parity = states["CheckSkipParity"]
        assert skip_parity["Choices"][0]["Next"] == "MarkParityVerdictUnknownByCadence"
        assert states["MarkParityVerdictUnknownByCadence"]["Next"] == "CheckSkipEvaluator"
        # Default = run the parity family (alpha-engine-config#6030:
        # ParityParallel → compare join); the compare's success terminal
        # hands off to CheckSkipEvaluator.
        assert skip_parity["Default"] == "ParityParallel"
        compare_success = next(
            c
            for c in states["CheckPitParityCompareStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert compare_success["Next"] == "PitParityCompareComplete"
        assert states["PitParityCompareComplete"]["Next"] == "CheckSkipEvaluator"


# ── Skip gate ─────────────────────────────────────────────────────────────


class TestSkipBacktesterPreservesEvalJudge:
    """Pins the 2026-05-03 fix (eval-judge always reachable from a
    skip_backtester=true operator) AND the 2026-05-07 simplification
    (skip_backtester decouples from skip_evaluator). The skip-path now
    routes to CheckSkipEvaluator, which by construction always converges
    to CheckSkipEvalJudge regardless of which branch it takes. So the
    silent-bypass-to-SaturdayHealthCheck class is still impossible while
    the operator gets independent skip flags.

    Caught by SF eval-pipeline-validation-5 (2026-05-03) when Research
    succeeded + new-format captures landed on S3 but the eval-judge state
    silently never fired because skip_backtester=true had been
    short-circuiting past it.
    """

    def test_skip_backtester_routes_to_evaluator_gate_not_health(self, states):
        skip = states["CheckSkipBacktester"]
        choice = skip["Choices"][0]
        # The skip-true branch hits CheckSkipEvaluator (decoupled flag
        # 2026-05-07). CheckSkipEvaluator's both branches still converge
        # to CheckSkipEvalJudge, so eval-judge stays reachable.
        assert choice["Next"] == "CheckSkipEvaluator"
        # Critically NOT routed to SaturdayHealthCheck — that was the
        # 2026-05-03 silent-bypass bug.
        assert choice["Next"] != "SaturdayHealthCheck"

    def test_evaluator_skip_gate_always_reaches_health_check(self, states):
        """Both branches of CheckSkipEvaluator must converge into the
        post-eval tail — the skip path and the run path
        (EvaluatorOptimize → CheckEvaluatorOptimizeStatus → Success) both exit to
        CheckSkipPostEval, the config#830 tail skip-gate, which defaults
        to SaturdayHealthCheck (full observability tail).

        Post-2026-05-07 reorder: the eval-judge chain runs UPSTREAM of
        Evaluator (after DataPhase2, before PredictorTraining), so the
        question this class previously asked (does eval-judge stay
        reachable from any skip-flag combination?) is now answered at
        the upstream junction (CheckSkipDataPhase2 → CheckSkipEvalJudge
        regardless of skip_data_phase2). At THIS junction, both
        branches simply exit to the tail; no judge gate downstream to protect.

        config#830: CheckSkipPostEval lets a mid-week mode=backtest-eval run
        stop after Evaluator (skip the advisory tail), but it DEFAULTS to
        SaturdayHealthCheck so a normal Saturday run still runs the full tail.
        """
        gate = states["CheckSkipEvaluator"]
        skip_choice = gate["Choices"][0]
        assert skip_choice["Next"] == "CheckSkipPostEval"
        assert gate["Default"] == "EvaluatorDiagnostics"
        # Run path success also exits to the tail gate (judge already ran upstream).
        assert (
            states["CheckEvaluatorOptimizeStatus"]["Choices"][0]["Next"]
            == "CheckSkipPostEval"
        )
        # The tail gate defaults to the full health-check tail (normal run).
        # alpha-engine-config-I8167: the tail gate now defaults one hop
        # downstream, to the new health-check-only skip gate — which itself
        # defaults to SaturdayHealthCheck on a normal run.
        assert states["CheckSkipPostEval"]["Default"] == "CheckSkipSaturdayHealthCheck"
        assert states["CheckSkipSaturdayHealthCheck"]["Default"] == "SaturdayHealthCheck"


class TestSkipEvalJudge:
    def test_skip_flag_bypasses_to_rationale_clustering_gate(self, states):
        """Skipping the judge must NOT also skip rationale clustering —
        they are independent observability paths reading different
        sources (clustering reads decision_artifacts/, judge reads its
        own _eval/). The skip path lands on CheckSkipRationaleClustering
        rather than SaturdayHealthCheck so the clustering Lambda still
        fires unless its own skip flag is set."""
        skip = states["CheckSkipEvalJudge"]
        choice = skip["Choices"][0]
        # Both presence + boolean equality must be checked (matches
        # other skip gates like CheckSkipResearch).
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_eval_judge"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "CheckSkipRationaleClustering"
        # Critically NOT routed to SaturdayHealthCheck — that would
        # bundle-skip both observability paths.
        assert choice["Next"] != "SaturdayHealthCheck"

    def test_default_runs_eval(self, states):
        assert states["CheckSkipEvalJudge"]["Default"] == "ComputeEvalCadence"


# ── Cadence computation ───────────────────────────────────────────────────


class TestComputeEvalCadence:
    def test_extracts_day_of_month_and_eval_date(self, states):
        params = states["ComputeEvalCadence"]["Parameters"]
        # Both intrinsic-function expressions must be present so the
        # downstream Choice + Payload can reference them.
        assert "day_of_month.$" in params
        assert "eval_date.$" in params
        # Reference shape — protect against accidental rename of either
        # JSONPath that would leave the Choice state matching nothing.
        assert "$$.Execution.StartTime" in params["day_of_month.$"]
        assert "$$.Execution.StartTime" in params["eval_date.$"]

    def test_writes_to_eval_cadence_path(self, states):
        assert states["ComputeEvalCadence"]["ResultPath"] == "$.eval_cadence"

    def test_routes_to_cadence_choice(self, states):
        assert states["ComputeEvalCadence"]["Next"] == "CheckMonthlyCadence"


# ── Monthly cadence Choice ────────────────────────────────────────────────


class TestCheckMonthlyCadence:
    def test_default_is_weekly_submit(self, states):
        # Default = the COMMON path (every other Saturday). Must NOT
        # be EvalJudgeSubmitFirstSaturday — that would ship every
        # weekly run on the expensive monthly Sonnet sweep.
        assert states["CheckMonthlyCadence"]["Default"] == "EvalJudgeSubmitWeekly"

    def test_first_saturday_branch_uses_lex_compare_under_08(self, states):
        choice = states["CheckMonthlyCadence"]["Choices"][0]
        assert choice["Variable"] == "$.eval_cadence.day_of_month"
        assert choice["StringLessThan"] == "08"
        assert choice["Next"] == "EvalJudgeSubmitFirstSaturday"


class TestComputeEvalCadenceBatch:
    """Pins the batch-chain-specific additions to ComputeEvalCadence.
    submit_iso is propagated to EvalJudgePoll for elapsed-time +
    fail-soft cap; without it the poll Lambda would have no signal
    to terminate a runaway loop."""

    def test_submit_iso_extracted_for_poll_elapsed_check(self, states):
        params = states["ComputeEvalCadence"]["Parameters"]
        assert "submit_iso.$" in params
        assert params["submit_iso.$"] == "$$.Execution.StartTime"


# ── Lambda invocation contract — batch chain ──────────────────────────────


class TestEvalJudgeSubmitContract:
    @pytest.mark.parametrize(
        "state_name,expected_force_sonnet",
        [
            ("EvalJudgeSubmitFirstSaturday", True),
            ("EvalJudgeSubmitWeekly", False),
        ],
    )
    def test_payload_carries_correct_force_sonnet_flag(
        self, states, state_name, expected_force_sonnet,
    ):
        payload = states[state_name]["Parameters"]["Payload"]
        assert payload["force_sonnet_pass"] is expected_force_sonnet

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly"],
    )
    def test_payload_passes_eval_date(self, states, state_name):
        payload = states[state_name]["Parameters"]["Payload"]
        assert payload["date.$"] == "$.eval_cadence.eval_date"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly"],
    )
    def test_invokes_submit_lambda_live_alias(self, states, state_name):
        params = states[state_name]["Parameters"]
        assert (
            params["FunctionName"]
            == "alpha-engine-research-eval-judge-submit:live"
        )

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly"],
    )
    def test_submit_timeout_matches_lambda_cap(self, states, state_name):
        # Submit Lambda is configured for 300s — plan-build + manifest
        # write + one batch-create call all complete in seconds.
        assert states[state_name]["TimeoutSeconds"] == 300

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly"],
    )
    def test_submit_routes_to_the_outcome_choice_on_success(
        self, states, state_name,
    ):
        assert states[state_name]["Next"] == "EvalJudgeSubmitOutcome"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly"],
    )
    def test_submit_catch_routes_to_rolling_mean_not_failure(
        self, states, state_name,
    ):
        # alpha-engine-config#6722: routes through the shared
        # MarkEvalJudgeDegraded convergence before EvalRollingMean.
        catch = states[state_name]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        # alpha-engine-config-I9636: through the phase-naming hop.
        assert converges_on(states, catch["Next"], "MarkEvalJudgeDegraded")
        assert phase_named_by(states, catch["Next"]) is not None
        assert states["MarkEvalJudgeDegraded"]["Next"] == "EvalRollingMean"


class TestEvalJudgeSubmitOutcome:
    """The Choice that replaced EvalJudgePollChoice (alpha-engine-config-I9329).

    The poll states went; this Choice's FAIL-SOFT DEFAULT did not, and that is
    the part worth pinning. alpha-engine-config-I9058 records what its absence
    costs: on 2026-08-22 EvalJudgeSubmitWeekly SUCCEEDED while returning
    ``{"status": "ERROR"}``, the chain jumped Submit -> EvalRollingMean in 25s,
    ``decision_artifacts/_eval/latest.json`` was never rewritten, and NOTHING
    in the execution recorded a degradation. It surfaced five days later as a
    freshness-monitor CRITICAL.
    """

    def test_a_real_empty_plan_never_launches_a_box(self, states):
        """DELIBERATE deviation from I9329's issue body, which said EMPTY
        should keep routing to EvalJudgeProcess. That was written for a Lambda
        Process that could emit a clean empty result for free. On a spot box
        the same routing would launch an instance to grade zero artifacts, and
        `judge_spot_run` refuses to run without a plan (exit 2) -- so it would
        also FAIL the stage. The Pass emits the identical result shape at no
        cost, and does NOT mark degraded: nothing to grade is an outcome, not a
        failure.
        """
        choice = next(
            c for c in states["EvalJudgeSubmitOutcome"]["Choices"]
            if any(leaf.get("StringEquals") == "EMPTY" for leaf in c.get("And", []))
        )
        assert choice["Next"] == "EvalJudgeEmptyPlan"
        assert states["EvalJudgeEmptyPlan"]["Next"] == "EvalRollingMean"
        # Comment excluded: a state's PROSE may legitimately name the flag
        # it deliberately does not set, and asserting over prose would make
        # documenting the choice fail the test that checks the choice.
        body = {k: v for k, v in states["EvalJudgeEmptyPlan"].items() if k != "Comment"}
        assert "research_degraded_local" not in json.dumps(body)

    def test_a_plan_that_exists_reaches_the_spot_dispatch(self, states):
        choice = next(
            c for c in states["EvalJudgeSubmitOutcome"]["Choices"]
            if any(leaf.get("StringEquals") == "OK" for leaf in c.get("And", []))
        )
        assert choice["Next"] == "PrepareEvalJudgeSpotDispatch"
        assert states["PrepareEvalJudgeSpotDispatch"]["Next"] == "DispatchEvalJudgeSpot"

    def test_the_dry_branch_is_first_so_the_friday_preflight_boots_the_box(
        self, states
    ):
        """Ordering is load-bearing. On the Friday shell run `$.research_dry`
        is true AND Submit returns the EMPTY sentinel; if the EMPTY branch
        matched first, the preflight would route past the spot entirely and
        the ONLY path that exercises the new substrate before Saturday would
        exercise nothing."""
        choices = states["EvalJudgeSubmitOutcome"]["Choices"]
        dry_index = next(
            i for i, c in enumerate(choices)
            if any(leaf.get("Variable") == "$.research_dry" for leaf in c.get("And", []))
        )
        empty_index = next(
            i for i, c in enumerate(choices)
            if any(leaf.get("StringEquals") == "EMPTY" for leaf in c.get("And", []))
        )
        assert dry_index < empty_index
        assert choices[dry_index]["Next"] == "PrepareEvalJudgeSpotDispatch"

    def test_default_is_fail_soft_but_marks_degraded(self, states):
        # alpha-engine-config-I9636: the Default now names the phase first.
        # This is the exact edge the 2026-08-29 scheduled run took, carrying an
        # Anthropic 400 ("credit balance is too low") in
        # $.eval_judge_submit.Payload.error that nothing recorded.
        assert converges_on(
            states, states["EvalJudgeSubmitOutcome"]["Default"],
            "MarkEvalJudgeDegraded",
        )
        assert phase_named_by(
            states, states["EvalJudgeSubmitOutcome"]["Default"]
        ) == "EvalJudgeSubmit"
        assert states["MarkEvalJudgeDegraded"]["Type"] == "Pass"
        assert states["MarkEvalJudgeDegraded"]["Next"] == "EvalRollingMean"
        assert (
            states["MarkEvalJudgeDegraded"]["ResultPath"]
            == "$.research_degraded_local"
        )


class TestEvalJudgeSpotSubstrate:
    """The dispatch + two bounded poll loops that replaced the batch poll loop.

    Shape is deliberately the RAGIngestion / DispatchWeeklyFreshnessSpot one,
    not a new idiom: launch, poll the bootstrap to Success, send the work,
    poll the work to Success.
    """

    def test_dispatch_invokes_the_dedicated_dispatcher(self, states):
        params = states["DispatchEvalJudgeSpot"]["Parameters"]
        assert params["FunctionName"] == (
            "alpha-engine-research-eval-judge-spot-dispatcher"
        )
        # The name is chosen so the Step Functions role's existing
        # `...function:alpha-engine-research-eval-judge*` invoke wildcard
        # already covers it -- verified live 2026-08-29, which is why this
        # cutover needs no SF-role invoke grant.
        assert params["FunctionName"].startswith("alpha-engine-research-eval-judge")

    def test_the_judge_box_is_its_own_and_never_the_shared_launcher(self, states):
        """$.ec2_instance_id is the weekly launcher every other stage in this
        pipeline addresses. Writing over it here -- or sending the judge to it
        -- would couple an LLM workload's disk and memory to thirteen unrelated
        stages, which is precisely why I9329 forbade a sixth clone on
        weekly-freshness-spot-dispatcher."""
        assert (
            states["EvalJudgeProcess"]["Parameters"]["InstanceIds.$"]
            == "$.eval_judge_instance_id"
        )
        for name in (
            "MergeEvalJudgeSpotInstanceId",
            "WaitForEvalJudgeSpotBootstrap",
            "WaitForEvalJudgeProcess",
        ):
            body = {k: v for k, v in states[name].items() if k != "Comment"}
            assert "$.ec2_instance_id" not in json.dumps(body)

    def test_no_work_is_sent_before_the_bootstrap_reaches_success(self, states):
        """The venv, the private prompts and the router env file do not exist
        until the bootstrap command is Success. A sendCommand issued earlier
        would fail on a box that is merely booting, and the SF cannot tell that
        apart from a judge defect."""
        check = states["CheckEvalJudgeSpotBootstrapStatus"]
        success = next(
            c for c in check["Choices"] if c.get("StringEquals") == "Success"
        )
        assert success["Next"] == "EvalJudgeProcess"

    @pytest.mark.parametrize(
        "check,var,cap,execution_timeout",
        [
            ("CheckEvalJudgeSpotBootstrapStatus", "$.eval_judge_bootstrap_polls", 72, 1800),
            ("CheckEvalJudgeProcessStatus", "$.eval_judge_process_polls", 432, 10800),
        ],
    )
    def test_each_poll_budget_is_derived_from_its_own_execution_timeout(
        self, states, check, var, cap, execution_timeout
    ):
        """cap = ceil(executionTimeout / 30s wait) * 1.2, the
        RAGIngestion/DataPhase2 precedent (alpha-engine-config-I5687). Derived
        here rather than restated so a timeout change that leaves the cap
        behind fails, instead of silently capping the loop below the work."""
        import math

        expected = int(math.ceil(execution_timeout / 30) * 1.2)
        assert cap == expected
        loop = next(
            c for c in states[check]["Choices"]
            if any(leaf.get("Variable") == var and "NumericLessThan" in leaf
                   for leaf in c.get("And", []))
        )
        bound = next(
            leaf["NumericLessThan"] for leaf in loop["And"]
            if leaf.get("Variable") == var and "NumericLessThan" in leaf
        )
        assert bound == cap

    def test_the_bootstrap_execution_timeout_matches_the_dispatchers(self):
        """One number, two owners: the dispatcher Lambda sets the SSM command's
        own executionTimeout, and the poll cap above is derived from it. Read
        the dispatcher rather than restating the literal, so a bump there
        cannot leave this loop capped below the work it waits on."""
        src = (
            _SF_PATH.parent / "lambdas" / "eval-judge-spot-dispatcher" / "index.py"
        ).read_text()
        assert '"EVAL_JUDGE_SPOT_BOOTSTRAP_TIMEOUT_SECONDS", "1800"' in src

    @pytest.mark.parametrize(
        "gate,poll_var",
        [
            ("EvalJudgeSpotBootstrapLivenessGate", "$.eval_judge_bootstrap_poll"),
            ("EvalJudgeProcessLivenessGate", "$.eval_judge_process_poll"),
        ],
    )
    def test_a_reclaim_relaunches_once_and_a_workload_failure_does_not(
        self, states, gate, poll_var
    ):
        """The reclaim design, stated where it is enforced.

        Spot is interruptible by default and the judge run is 60-145 minutes,
        so a reclaim is the failure this substrate is most exposed to -- and
        coverage is a HARD failure (Brian, 2026-08-29), so losing the box must
        not silently lose the week's evals. SUBSTRATE LOST relaunches ONCE, on
        demand. A WORKLOAD failure (a coverage shortfall, ResponseCode 3) does
        NOT: it is a real verdict, and re-grading the same corpus on a second
        box spends money to reproduce it.
        """
        rule = states[gate]["Choices"][0]
        variables = {leaf.get("Variable") for leaf in rule["And"] if "Variable" in leaf}
        assert f"{poll_var}.StatusDetails" in variables
        details = next(op for op in rule["And"] if "Or" in op)["Or"]
        assert {d["StringEquals"] for d in details} == {"Undeliverable", "Terminated"}
        assert any(
            leaf.get("Variable") == "$.eval_judge_spot_relaunches"
            and leaf.get("NumericLessThan") == 1
            for leaf in rule["And"]
        )
        assert rule["Next"] == "EvalJudgeSpotRelaunch"
        assert states[gate]["Default"].startswith("ExtractEvalJudge")

    def test_the_relaunch_is_bounded_and_escalates_off_spot(self, states):
        relaunch = states["EvalJudgeSpotRelaunch"]
        blob = json.dumps(relaunch)
        assert "eval_judge_spot_relaunches" in blob
        assert "eval_judge_spot_force_on_demand" in blob
        assert "true" in blob
        assert relaunch["Next"] == "DispatchEvalJudgeSpot"

    def test_the_counter_is_seeded_on_the_first_attempt(self, states):
        """alpha-engine-config-I7282: a field written only by the relaunch path
        would be ABSENT on every healthy run, and both gates dereference it."""
        blob = json.dumps(states["PrepareEvalJudgeSpotDispatch"])
        assert "eval_judge_spot_relaunches" in blob
        assert "eval_judge_spot_force_on_demand" in blob

    def test_no_eval_judge_failure_path_reaches_rolling_mean_unmarked(
        self, states
    ):
        """The invariant, stated once over the whole chain
        (alpha-engine-config-I9058, extended by -I9329).

        Every way the eval-judge chain can end WITHOUT writing
        ``decision_artifacts/_eval/latest.json`` must pass through
        ``MarkEvalJudgeDegraded``. Written as a whole-chain sweep rather than
        per-branch assertions because the defect is ADDITIVE: the failure mode
        is someone adding another fail-soft exit later and wiring it straight
        to EvalRollingMean. The cutover added four such exits, which is exactly
        the growth this shape was written for.

        EvalJudgeEmptyPlan is the one legitimate direct route, and it is not a
        failure path: it says the judge ran and there was nothing to grade.
        CheckEvalJudgeProcessStatus's Success branch is the write path itself.
        """
        offenders = []
        for name, state in states.items():
            if not name.startswith(("EvalJudge", "DispatchEvalJudge",
                                    "CheckEvalJudge", "WaitForEvalJudge",
                                    "ExtractEvalJudge", "PrepareEvalJudge",
                                    "MergeEvalJudge", "InitEvalJudge")):
                continue
            if name in ("EvalJudgeEmptyPlan", "CheckEvalJudgeProcessStatus"):
                continue
            targets = [(name, "Default", state.get("Default")),
                       (name, "Next", state.get("Next"))]
            targets += [(name, f"Choices[{i}]", c.get("Next"))
                        for i, c in enumerate(state.get("Choices", []))]
            targets += [(name, f"Catch[{i}]", c.get("Next"))
                        for i, c in enumerate(state.get("Catch", []))]
            offenders += [t for t in targets if t[2] == "EvalRollingMean"]
        assert offenders == [], (
            "eval-judge exits reach EvalRollingMean without passing through "
            f"MarkEvalJudgeDegraded: {offenders}"
        )
        assert states["MarkEvalJudgeDegraded"]["Next"] == "EvalRollingMean"


class TestEvalJudgeProcessContract:
    """EvalJudgeProcess keeps its NAME and changes its SUBSTRATE
    (alpha-engine-config-I9329).

    The name is load-bearing beyond aesthetics:
    ``eval_artifact_latest.produced_by`` names EvalJudgeProcess,
    ``AggregateCosts.required_producers`` keys on it, and the stage-coverage
    registry has a row for it. All three statements stay true because the stage
    still produces exactly what they say it does.
    """

    def test_it_is_a_send_command_stage(self, states):
        assert states["EvalJudgeProcess"]["Resource"] == (
            "arn:aws:states:::aws-sdk:ssm:sendCommand"
        )
        assert states["EvalJudgeProcess"]["Parameters"]["DocumentName"] == (
            "AWS-RunShellScript"
        )

    def test_execution_timeout_is_strictly_below_the_state_timeout(self, states):
        """The INVERSE of the lambda:invoke rule, and the more dangerous
        direction (alpha-engine-config-I6948). Inverted, the state abandons a
        command Step Functions cannot cancel: the stage fails with a bare
        States.Timeout naming nothing while the spot instance keeps billing.

        10800s is budgeted from the MEASURED 83 artifacts x 45-105s = 60-145
        minutes serial, with headroom for corpus growth. A budget, not an
        accommodation (sf-pipeline-policy.md section 4).
        """
        st = states["EvalJudgeProcess"]
        execution_timeout = int(st["Parameters"]["Parameters"]["executionTimeout"][0])
        assert execution_timeout == 10800
        assert execution_timeout < st["TimeoutSeconds"]

    def test_the_command_reads_batch_id_and_plan_key_from_submit(self, states):
        """The reason there is no poll stage left to starve. If the command
        took either field from a poll result, deleting the poll chain would
        have left it unresolvable."""
        cmd = states["EvalJudgeProcess"]["Parameters"]["Parameters"]["commands.$"]
        assert "$.eval_judge_submit.Payload.batch_id" in cmd
        assert "$.eval_judge_submit.Payload.plan_s3_key" in cmd
        assert "eval_judge_poll" not in cmd

    def test_the_command_sources_the_router_environment_file(self, states):
        """A separate SSM command is a SEPARATE shell. The dispatcher writes
        the router addressing to a file on the box precisely because an export
        made during bootstrap would be gone by now -- and
        ``judge_exec_context()`` would then answer "lambda" from a spot box,
        asking the router the wrong question with no error anywhere."""
        cmd = states["EvalJudgeProcess"]["Parameters"]["Parameters"]["commands.$"]
        assert "/home/ec2-user/eval-judge.env" in cmd

    def test_the_command_never_names_a_model_provider_or_endpoint(self, states):
        """principles.md section 2.8, and Brian's 2026-08-29 ruling that the
        direct Anthropic API is retired. Addressed by registry GROUP through the
        router, never by vendor identity."""
        cmd = states["EvalJudgeProcess"]["Parameters"]["Parameters"]["commands.$"].lower()
        for forbidden in ("anthropic", "openrouter", "claude-", "gpt-", "api_key"):
            assert forbidden not in cmd

    def test_the_box_tears_itself_down_on_every_exit_path(self, states):
        """Cost-management: the box bills until something stops it, and the
        watchdog is a 4.5h backstop, not a plan. The 120s delay is what lets
        SSM report the terminal status to this Step Function before the
        instance disappears -- an immediate shutdown would race the
        ResponseCode this stage reads."""
        cmd = states["EvalJudgeProcess"]["Parameters"]["Parameters"]["commands.$"]
        assert "shutdown -h now" in cmd
        assert "--on-active=120" in cmd
        assert "EXIT" in cmd

    def test_the_run_is_wrapped_in_the_shared_log_capture_cli(self, states):
        """The institutional chokepoint, not a hand-rolled trap+tee: SSM's
        StandardOutputContent is capped at 24KB, so a failure on a box that
        then terminates is otherwise unrecoverable."""
        cmd = states["EvalJudgeProcess"]["Parameters"]["Parameters"]["commands.$"]
        assert "krepis.ssm_log_capture run --correlation-id" in cmd
        assert "--slug eval-judge" in cmd

    def test_process_routes_to_its_poll_loop_then_rolling_mean(self, states):
        assert states["EvalJudgeProcess"]["Next"] == "InitEvalJudgeProcessPollCount"
        success = next(
            c for c in states["CheckEvalJudgeProcessStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert success["Next"] == "EvalRollingMean"

    def test_process_catch_routes_to_rolling_mean_not_failure(self, states):
        catch = states["EvalJudgeProcess"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        # alpha-engine-config-I9636: through the phase-naming hop.
        assert converges_on(states, catch["Next"], "MarkEvalJudgeDegraded")
        assert phase_named_by(states, catch["Next"]) == "EvalJudgeProcess"
        assert states["MarkEvalJudgeDegraded"]["Next"] == "EvalRollingMean"


class TestBatchChainNonBlocking:
    """Eval is observability per ROADMAP §1635 — every failure surface
    in the batch chain must converge to EvalRollingMean so the rolling
    metric still runs against historical data even when the current
    week's batch fails."""

    @pytest.mark.parametrize(
        "state_name",
        [
            "EvalJudgeSubmitFirstSaturday",
            "EvalJudgeSubmitWeekly",
            # alpha-engine-config-I9329: EvalJudgePoll left this list with the
            # rest of the poll chain; the spot dispatcher took its place as a
            # fail-open owner, and it is the one that can now fail for an
            # INFRASTRUCTURE reason (no spot capacity) rather than a domain one.
            "DispatchEvalJudgeSpot",
            "EvalJudgeProcess",
        ],
    )
    def test_states_all_states_catch_routes_to_rolling_mean(
        self, states, state_name,
    ):
        # alpha-engine-config#6722: all four share the MarkEvalJudgeDegraded
        # convergence, which itself continues to EvalRollingMean unchanged.
        catch = states[state_name]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        # alpha-engine-config-I9636: through the phase-naming hop.
        assert converges_on(states, catch["Next"], "MarkEvalJudgeDegraded")
        assert phase_named_by(states, catch["Next"]) is not None
        assert catch["Next"] != "HandleFailure"
        assert states["MarkEvalJudgeDegraded"]["Next"] == "EvalRollingMean"


# ── EvalRollingMean state (PR 4c) ─────────────────────────────────────────


class TestEvalRollingMean:
    def test_invokes_live_alias(self, states):
        params = states["EvalRollingMean"]["Parameters"]
        assert params["FunctionName"] == "alpha-engine-research-eval-rolling-mean:live"

    def test_payload_passes_execution_start_time(self, states):
        # SF passes its own start time so the rolling-mean window aligns
        # with the SF execution date — keeps replay/backfill paths
        # deterministic instead of "whenever the Lambda happened to run."
        payload = states["EvalRollingMean"]["Parameters"]["Payload"]
        assert payload["end_time_iso.$"] == "$$.Execution.StartTime"

    def test_timeout_is_the_declared_guard_band_over_a_self_deadlining_function(self, states):
        """The SF budget sits deliberately ABOVE the function's 900s maximum.

        Was `== 300`, exactly equal to the Lambda's configured timeout. Equal is
        the one value that guarantees the SF cannot be the thing that stops the
        state: whichever ceiling fires first is a coin toss, and on 2026-08-28
        the Lambda won — `EvalRollingMean` burned its full 300s after 1.6s of
        handler work, the stop arrived as `States.Timeout`, the
        research/predictor branch fail-opened, and the run terminated FAILED
        having done its work (alpha-engine-config#9102).

        MEASURED cause, from that invocation's own log stream: the handler never
        returned. It sat 298s inside
        `scripts.build_agent_quality -> evals.judge_outcome_ic.open_research_db`,
        which downloads a 356 MB SQLite snapshot into a 512 MB function.
        Nothing held the runtime open AFTER the handler returned — on the
        healthy 2026-08-21/-22 runs END lands 1.3-1.5s after the last handler
        log line.

        The repair is in the handler: its four secondary aggregations now run
        under `invocation_budget.run_bounded`, so the stage returns its primary
        deliverable on its own budget. That makes the function SELF-DEADLINING,
        and the ordering rule inverts for that class — a self-deadlining
        function is pinned at Lambda's 900s service maximum and the state
        carries a guard band ABOVE it, so the FUNCTION's ceiling is the backstop
        that fires and emits a REPORT line, instead of the state killing the
        graceful return at the wall. `EvalJudgeProcess` carries the
        identical 960 for the identical reason
        (alpha-engine-config-I7181); `tests/test_sf_lambda_timeout_ordering.py`
        is where that rule is stated and enforced fleet-wide.
        """
        sf_budget = states["EvalRollingMean"]["TimeoutSeconds"]
        lambda_cap = 900  # Lambda's service maximum; the function is pinned there.
        assert sf_budget > lambda_cap, (
            f"SF budget {sf_budget}s must sit ABOVE the self-deadlining function's "
            f"{lambda_cap}s ceiling, or the state pre-empts the graceful partial "
            "return the handler exists to make"
        )
        assert sf_budget == 960, (
            "the guard band is a declared number, not a range — a drifting band "
            "is back to being decorative (test_sf_lambda_timeout_ordering.py "
            "pins the same value in _SERVICE_MAX_GUARD_BAND)"
        )

    def test_success_continues_to_rationale_clustering_gate(self, states):
        # Rolling-mean converges to CheckSkipRationaleClustering (the
        # gate in front of the cross-week clustering Lambda) rather
        # than directly to SaturdayHealthCheck.
        assert states["EvalRollingMean"]["Next"] == "CheckSkipRationaleClustering"

    def test_catch_routes_to_rationale_clustering_gate_not_failure(self, states):
        # alpha-engine-config#6722: routes through MarkEvalRollingMeanDegraded
        # before converging on CheckSkipRationaleClustering exactly as before.
        catch = states["EvalRollingMean"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "MarkEvalRollingMeanDegraded"
        assert catch["Next"] != "HandleFailure"
        assert states["MarkEvalRollingMeanDegraded"]["Next"] == "CheckSkipRationaleClustering"

    def test_retries_on_transient_lambda_errors(self, states):
        # Same retry posture as the eval-judge state — one retry on
        # AWS-side transient errors (ServiceException / Throttling),
        # not on application errors.
        retry = states["EvalRollingMean"]["Retry"][0]
        assert "Lambda.ServiceException" in retry["ErrorEquals"]
        assert "Lambda.TooManyRequestsException" in retry["ErrorEquals"]
        assert retry["MaxAttempts"] == 1


# ── Rationale clustering skip-gate + state ───────────────────────────────


class TestSkipRationaleClustering:
    def test_skip_flag_bypasses_to_counterfactual_gate(self, states):
        """Skipping clustering must NOT also skip counterfactual — they
        are independent agent-justification signals (clustering = cross-
        week templating; counterfactual = 3-deep decision-tree match).
        The skip path lands on CheckSkipCounterfactual rather than
        SaturdayHealthCheck so the counterfactual Lambda still fires
        unless its own skip flag is set. It landed on
        CheckSkipReplayConcordance until alpha-engine-config-I10539
        (Brian ruling 2026-09-16, option b) retired that stage."""
        skip = states["CheckSkipRationaleClustering"]
        choice = skip["Choices"][0]
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_rationale_clustering"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "CheckSkipCounterfactual"
        # Critically NOT routed directly to SaturdayHealthCheck — that
        # would bundle-skip both observability paths.
        assert choice["Next"] != "SaturdayHealthCheck"

    def test_default_runs_clustering(self, states):
        assert states["CheckSkipRationaleClustering"]["Default"] == "RationaleClustering"


class TestRationaleClustering:
    def test_invokes_live_alias(self, states):
        params = states["RationaleClustering"]["Parameters"]
        assert params["FunctionName"] == "alpha-engine-research-rationale-clustering:live"

    def test_payload_passes_execution_start_time(self, states):
        payload = states["RationaleClustering"]["Parameters"]["Payload"]
        assert payload["end_time_iso.$"] == "$$.Execution.StartTime"

    def test_timeout_matches_lambda_cap(self, states):
        # Clustering Lambda is configured with timeout=900s (the AWS
        # Lambda ceiling — alpha-engine-research infrastructure/deploy.sh,
        # bumped 600s -> 900s to absorb corpus growth) — SF state
        # TimeoutSeconds must equal that ceiling, else the SF kills the
        # lambda:invoke wait independently of (and before) the Lambda's
        # own configured timeout (config#1650).
        assert states["RationaleClustering"]["TimeoutSeconds"] == 900

    def test_success_continues_to_counterfactual_gate(self, states):
        # Clustering converges to CheckSkipCounterfactual (the gate in
        # front of the counterfactual Lambda) rather than directly to
        # SaturdayHealthCheck. It converged on CheckSkipReplayConcordance
        # until alpha-engine-config-I10539 retired that stage.
        assert states["RationaleClustering"]["Next"] == "CheckSkipCounterfactual"

    def test_catch_routes_to_counterfactual_gate_not_failure(self, states):
        # alpha-engine-config#6722: routes through MarkRationaleClusteringDegraded
        # before converging on CheckSkipCounterfactual exactly as before.
        catch = states["RationaleClustering"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "MarkRationaleClusteringDegraded"
        assert catch["Next"] != "HandleFailure"
        assert states["MarkRationaleClusteringDegraded"]["Next"] == "CheckSkipCounterfactual"

    def test_retries_on_transient_lambda_errors(self, states):
        retry = states["RationaleClustering"]["Retry"][0]
        assert "Lambda.ServiceException" in retry["ErrorEquals"]
        assert "Lambda.TooManyRequestsException" in retry["ErrorEquals"]
        assert retry["MaxAttempts"] == 1


# ── Replay concordance: RETIRED (alpha-engine-config-I10539) ─────────────


class TestReplayConcordanceRetired:
    """alpha-engine-config-I10539 (Brian ruling 2026-09-16, option b).

    The stage replayed a corpus that no longer exists: its default
    ``agent_filter`` named the six-team research agents retired under
    ``-I1580`` on 2026-07-20, and the 56-day window slid past the last
    matching DecisionArtifact (2026-07-11) between the 2026-08-29 run
    (``n_artifacts_candidate: 46``) and the 2026-09-05 run (``0``). The
    zero-call run then failed the 2026-09-12 canonical weekly SF at
    ``AggregateCosts`` with ``CostCoverageError``.

    These assertions are the class-level guard, not an instance fix: a
    measurement stage exists only while its population exists, so the
    three states and the skip flag must stay gone together. Re-adding one
    of them without the others is the partial-retirement defect.
    """

    def test_states_are_gone(self, states):
        for name in (
            "CheckSkipReplayConcordance",
            "ReplayConcordance",
            "MarkReplayConcordanceDegraded",
        ):
            assert name not in states, f"retired SF state is back: {name}"

    def test_nothing_still_points_at_the_retired_states(self, states):
        import json

        blob = json.dumps(states)
        for name in (
            "CheckSkipReplayConcordance",
            "ReplayConcordance",
            "MarkReplayConcordanceDegraded",
        ):
            for key in ('"Next": "%s"' % name, '"Default": "%s"' % name):
                assert key not in blob, f"dangling edge to retired state: {key}"


# ── Counterfactual rule fit skip-gate + state ────────────────────────────


class TestSkipCounterfactual:
    def test_skip_flag_bypasses_to_branch_terminal(self, states):
        """Skipping Counterfactual now lands on the AggregateCosts
        skip-gate (ROADMAP L1146 — SF-wired daily cost aggregator
        added 2026-05-25), not directly on BranchAComplete. The cost
        aggregator reads cost JSONLs written by upstream LLM states
        (Research / eval-judge / rationale-clustering /
        counterfactual); a counterfactual skip does NOT invalidate those
        upstream rows, so the aggregator MUST still run. The three
        remaining observability skip flags (skip_counterfactual /
        skip_rationale_clustering / skip_aggregate_costs) are
        independent; skip_replay_concordance went with the stage retired
        under alpha-engine-config-I10539. Pre-L1146 this assertion
        pinned ``BranchAComplete``; the L1146 wire-up rerouted through
        ``CheckSkipAggregateCosts``, and alpha-engine-config-I7194 moved
        that gate to the TOP LEVEL — so the branch terminal is once again
        the direct target, and cost aggregation now runs AFTER Director
        instead of before it."""
        skip = states["CheckSkipCounterfactual"]
        choice = skip["Choices"][0]
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_counterfactual"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "BranchAComplete"

    def test_default_runs_counterfactual(self, states):
        assert states["CheckSkipCounterfactual"]["Default"] == "Counterfactual"


class TestCounterfactual:
    def test_invokes_live_alias(self, states):
        params = states["Counterfactual"]["Parameters"]
        assert params["FunctionName"] == "alpha-engine-replay-counterfactual:live"

    def test_payload_carries_required_fields(self, states):
        payload = states["Counterfactual"]["Parameters"]["Payload"]
        assert payload["end_time_iso.$"] == "$$.Execution.StartTime"
        # 8-week trailing window — same as concordance + clustering.
        assert payload["window_days"] == 56
        # Default tree depth pinned at the SF level so the production
        # cadence is reproducible.
        assert payload["max_depth"] == 3

    def test_timeout_matches_lambda_cap(self, states):
        # Counterfactual Lambda is configured with timeout=600s
        # (alpha-engine-backtester infrastructure/deploy_counterfactual.sh).
        # Lighter than concordance (no LLM calls — sklearn fits run
        # in seconds; 600s is comfortable headroom for S3 listing
        # across 8 weeks of corpus).
        assert states["Counterfactual"]["TimeoutSeconds"] == 600

    def test_success_exits_to_branch_terminal(self, states):
        # Counterfactual is Branch A's LAST load-bearing state again: the
        # L1146 wire-up (2026-05-25) inserted AggregateCosts after it, and
        # alpha-engine-config-I7194 moved that aggregator to the top level
        # so it runs after Director (whose director-plan rows it could
        # never see from inside this Parallel). Persisted S3 artifacts are
        # still available to the downstream Evaluator, which runs AFTER
        # the Parallel join.
        assert states["Counterfactual"]["Next"] == "BranchAComplete"

    def test_catch_routes_to_branch_terminal_not_failure(self, states):
        # Same Catch posture as the rest of the agent-justification
        # triple — Counterfactual is observability, not load-bearing, so
        # failures fall through to the next observability step (the cost
        # aggregator) rather than halting the pipeline (and crucially
        # NOT to HandleFailure, which would abort the sibling
        # PredictorTraining branch). Pre-L1146 this routed directly to
        # BranchAComplete; the cost aggregator inserted between
        # Counterfactual and the branch terminal is itself a separate
        # observability layer with its own Catch routing to
        # BranchAComplete.
        # alpha-engine-config#6722: routes through MarkCounterfactualDegraded
        # first. alpha-engine-config-I7194: that fold now converges on
        # BranchAComplete, the aggregator having moved to the top level.
        catch = states["Counterfactual"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "MarkCounterfactualDegraded"
        assert catch["Next"] != "HandleFailure"
        assert states["MarkCounterfactualDegraded"]["Next"] == "BranchAComplete"

    def test_retries_on_transient_lambda_errors(self, states):
        retry = states["Counterfactual"]["Retry"][0]
        assert "Lambda.ServiceException" in retry["ErrorEquals"]
        assert "Lambda.TooManyRequestsException" in retry["ErrorEquals"]
        assert retry["MaxAttempts"] == 1


# ── Pipeline ordering invariant ──────────────────────────────────────────


class TestJudgeChainBeforePredictor:
    """Pins the 2026-05-07 reorder — the eval-judge + agent-justification
    triple (judge, rolling-mean, clustering, concordance, counterfactual)
    must run AFTER Research/DataPhase2 and BEFORE PredictorTraining, so
    their persisted S3 artifacts are available to Evaluator's email when
    it runs at the end of the pipeline.

    Pre-reorder ordering: Research → ... → Predictor → Backtester →
    Evaluator → judge chain → SaturdayHealthCheck. The Evaluator email
    was generated BEFORE judge results landed in S3, so the operator's
    weekly review never saw rubric scores / clustering / concordance /
    counterfactual outcomes — that was the user-surfaced gap that
    motivated this reorder.

    Post-reorder ordering: Research → DataPhase2 → judge chain →
    Predictor → Backtester → Evaluator → SaturdayHealthCheck. The
    judge chain's S3 outputs (decision_artifacts/_eval/, _clustering/,
    _concordance/, _counterfactual/) are populated for the current
    run_date by the time Evaluator's reporter.build_report() runs, so
    they can be pulled into the weekly email.
    """

    def test_data_phase2_exits_to_judge_skip_gate_not_predictor(self, states):
        """DataPhase2's success path enters the judge chain, not
        predictor training. This is the load-bearing invariant — if
        someone ever rewires DataPhase2.Next to CheckSkipPredictorTraining
        (the pre-reorder target), the judge chain bypass is silent."""
        # alpha-engine-config-I5759 moved DataPhase2 from a lambda:invoke to
        # the spot dispatch->poll quartet, so the success edge is no longer
        # DataPhase2.Next — it is the Success arm of CheckDataPhase2Status.
        # The invariant is unchanged and still load-bearing: whatever the
        # stage's success edge IS, it must enter the judge chain.
        assert states["DataPhase2"]["Next"] == "InitDataPhase2PollCount"
        success_arm = [
            c for c in states["CheckDataPhase2Status"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert len(success_arm) == 1, "CheckDataPhase2Status lost its Success arm"
        assert success_arm[0]["Next"] == "CheckSkipEvalJudge"
        assert (
            states["CheckSkipDataPhase2"]["Choices"][0]["Next"]
            == "CheckSkipEvalJudge"
        )

    def test_counterfactual_exits_to_branch_terminal(self, states):
        """Counterfactual's three exit edges (Next + Catch + the
        skip-gate above it) all converge on BranchAComplete
        (alpha-engine-config-I7194 moved the AggregateCosts skip-gate that
        ROADMAP L1146 inserted here to the top-level tail). The Evaluator-sees-judge-
        artifacts ordering invariant is still satisfied because
        Evaluator runs AFTER the Parallel join, by which point Branch A
        (including the inserted cost-aggregator step) has completed and
        its S3 artifacts are landed. Edge target history:
        pre-2026-05-07 SaturdayHealthCheck → 2026-05-07→05-16
        CheckSkipPredictorTraining → 2026-05-16→05-25 BranchAComplete →
        L1146 CheckSkipAggregateCosts → alpha-engine-config-I7194
        (2026-08-25) BranchAComplete again."""
        # alpha-engine-config#6722: the Catch edge detours through
        # MarkCounterfactualDegraded, still reaching BranchAComplete.
        assert states["Counterfactual"]["Next"] == "BranchAComplete"
        assert (
            states["Counterfactual"]["Catch"][0]["Next"]
            == "MarkCounterfactualDegraded"
        )
        assert (
            states["MarkCounterfactualDegraded"]["Next"]
            == "BranchAComplete"
        )
        assert (
            states["CheckSkipCounterfactual"]["Choices"][0]["Next"]
            == "BranchAComplete"
        )

    def test_evaluator_exits_directly_to_health_check(self, states):
        """Evaluator's success path no longer enters the judge chain
        (judge ran upstream). It exits to the post-eval tail gate
        (CheckSkipPostEval, config#830), which defaults to SaturdayHealthCheck."""
        success = next(
            c for c in states["CheckEvaluatorOptimizeStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert success["Next"] == "CheckSkipPostEval"
        # And the skip-evaluator path also goes to CheckSkipPostEval
        # (the previous pre-reorder target was CheckSkipEvalJudge).
        assert (
            states["CheckSkipEvaluator"]["Choices"][0]["Next"]
            == "CheckSkipPostEval"
        )
        # The tail gate defaults to the full health-check tail on a normal run.
        # alpha-engine-config-I8167: the tail gate now defaults one hop
        # downstream, to the new health-check-only skip gate — which itself
        # defaults to SaturdayHealthCheck on a normal run.
        assert states["CheckSkipPostEval"]["Default"] == "CheckSkipSaturdayHealthCheck"
        assert states["CheckSkipSaturdayHealthCheck"]["Default"] == "SaturdayHealthCheck"


class TestSyncRungRouting:
    """alpha-engine-config-I9263 — Brian's ruling 2026-08-29, verbatim:
    *"I will not fund the anthropic account, at this point we shouldn't be
    using the anthropic api at all."*

    ``crucible-research-PR759`` migrated the eval-judge chain off the direct
    Anthropic SDK onto the krepis router. When no batch-capable route resolves,
    Submit takes the synchronous judge rung and returns
    ``processing_status='ended_sync'`` with a ``sync-{date}`` batch id. These
    tests pin the SF's half of that contract.
    """

    def test_ended_sync_routes_straight_to_process(self, states):
        """A sync-rung run must reach Process, and must not enter the Wait loop.

        Pre-I9263 there was no such branch: an ``ended_sync`` payload carried
        ``status='OK'``, so it entered the 60s Wait and burned a Poll
        invocation to learn what Submit had already stated.
        """
        choices = states["EvalJudgeSubmitOutcome"]["Choices"]
        sync = [
            c for c in choices
            if any(
                cond.get("StringEquals") == "ended_sync"
                for cond in c.get("And", [])
            )
        ]
        assert len(sync) == 1, (
            "exactly one ended_sync branch expected in EvalJudgeSubmitOutcome"
        )
        assert sync[0]["Next"] == "PrepareEvalJudgeSpotDispatch"

        # Ordering is load-bearing: SF evaluates Choices in order, and the
        # `status == "OK"` branch would otherwise match first and send a
        # sync-rung run into the Wait loop.
        ok_index = next(
            i for i, c in enumerate(choices)
            if any(
                cond.get("StringEquals") == "OK" for cond in c.get("And", [])
            )
        )
        assert choices.index(sync[0]) < ok_index

    def test_the_sync_branch_is_ispresent_guarded(self, states):
        """A submit payload with no processing_status must still reach the
        fail-soft Default, not throw States.Runtime (config#2275's rule)."""
        choices = states["EvalJudgeSubmitOutcome"]["Choices"]
        sync = next(
            c for c in choices
            if any(
                cond.get("StringEquals") == "ended_sync"
                for cond in c.get("And", [])
            )
        )
        assert any(cond.get("IsPresent") is True for cond in sync["And"])
        assert converges_on(
            states, states["EvalJudgeSubmitOutcome"]["Default"],
            "MarkEvalJudgeDegraded",
        )

    def test_process_reads_its_inputs_from_submit_not_from_poll(self, states):
        """The reason skipping Poll is safe at all.

        If Process took batch_id or plan_s3_key from ``$.eval_judge_poll``, the
        ended_sync shortcut would starve it. Pinned so a future edit that moves
        either field to the poll payload fails here rather than in production."""
        cmd = states["EvalJudgeProcess"]["Parameters"]["Parameters"]["commands.$"]
        assert "$.eval_judge_submit.Payload.batch_id" in cmd
        assert "$.eval_judge_submit.Payload.plan_s3_key" in cmd

    def test_no_sf_comment_still_claims_the_poll_calls_anthropic(self, sf):
        """The chain no longer calls ``anthropic.messages.batches.retrieve``.

        `doc-maintenance-policy`: an instruction is load-bearing, not a
        historical log. A comment asserting a call that no longer exists sends
        the next reader to the wrong provider during an incident."""
        raw = json.dumps(sf)
        assert "Calls anthropic.messages.batches.retrieve" not in raw


class TestEveryEvalJudgeDegradationNamesItsPhase:
    """alpha-engine-config-I9636 — the sibling half of
    ``test_no_eval_judge_failure_path_reaches_rolling_mean_unmarked``.

    That test makes every fail-open exit VISIBLE. This one makes it LEGIBLE.
    ``MarkEvalJudgeDegraded`` writes ``$.research_degraded_local = true`` and
    nothing else, so nine distinct incidents — the submit refused for billing,
    the plan malformed, the spot request unfulfilled, the box never booted, the
    judge under-covering the corpus — arrived at the notifier as one boolean.

    Its own ``Comment`` has asserted since I9329 that *"each arrival first
    passes through an Extract* state that names its phase"*. Measured against
    the live definition on 2026-08-31 that held for 2 of 9. The 2026-08-29
    scheduled run degraded through one of the other 7 (``EvalJudgeSubmitOutcome``'s
    Default, on an Anthropic ``400 ... credit balance is too low``), and the
    cause had to be recovered from 7,858 execution-history events two days
    later because the run recorded a bare true. A comment is not a mechanism;
    this is the mechanism.
    """

    def _arrivals(self, states):
        out = []
        for name, body in states.items():
            edges = [("Next", body.get("Next")), ("Default", body.get("Default"))]
            edges += [(f"Choices[{i}]", c.get("Next"))
                      for i, c in enumerate(body.get("Choices", []))]
            edges += [(f"Catch[{i}]", c.get("Next"))
                      for i, c in enumerate(body.get("Catch", []))]
            out += [(name, kind) for kind, tgt in edges
                    if tgt == "MarkEvalJudgeDegraded"]
        return out

    def test_every_arrival_is_a_phase_naming_extract(self, states):
        unnamed = [
            (name, kind) for name, kind in self._arrivals(states)
            if not phase_named_by(states, name)
        ]
        assert unnamed == [], (
            "these edges reach MarkEvalJudgeDegraded without naming which "
            f"phase degraded: {unnamed}. The degraded record is then a bare "
            "boolean and the run cannot be diagnosed from its own artifacts."
        )

    def test_the_extracts_write_the_error_path_and_do_not_change_continuation(
        self, states,
    ):
        for name, _ in self._arrivals(states):
            hop = states[name]
            assert hop["Type"] == "Pass", name
            assert hop["ResultPath"] == "$.eval_judge_error", name
            assert hop["Next"] == "MarkEvalJudgeDegraded", name
            assert hop["Parameters"].get("source"), (
                f"{name} names a phase but not the EDGE it came in on; two "
                "arrivals can share a phase and mean different things"
            )

    def test_the_submit_outcome_default_carries_the_payload_it_was_holding(
        self, states,
    ):
        """The 2026-08-29 path specifically.

        Submit returned status=ERROR with the provider's own message in
        ``.error`` and the SF had it in hand at the Choice. Whatever else this
        hop does, it must not drop that.
        """
        hop = states[states["EvalJudgeSubmitOutcome"]["Default"]]
        carried = json.dumps(hop["Parameters"])
        assert "$.eval_judge_submit.Payload" in carried
        # Total-by-construction: JsonToString on the whole Payload, never a
        # pick of .status/.error, because this Default is reached BOTH by a
        # payload that has .error and by one that has no status at all, and
        # the second throws States.Runtime on an unguarded pick.
        assert "States.JsonToString" in carried

    def test_no_two_arrivals_claim_the_same_phase_and_source(self, states):
        seen = {}
        for name, _ in self._arrivals(states):
            p = states[name]["Parameters"]
            key = (p["phase"], p["source"])
            assert key not in seen, (
                f"{name} and {seen[key]} both report {key} — two incidents "
                "collapsed into one label is the defect, one layer up"
            )
            seen[key] = name
