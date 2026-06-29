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
            "EvalJudgePollChoice",
            "EvalJudgePollWait",
            "EvalJudgePoll",
            "EvalJudgePollDecision",
            "EvalJudgeProcess",
            "EvalRollingMean",
            "CheckSkipRationaleClustering",
            "RationaleClustering",
            "CheckSkipReplayConcordance",
            "ReplayConcordance",
            "CheckSkipCounterfactual",
            "Counterfactual",
        ):
            assert name in states, f"missing SF state: {name}"

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
        bt = states["CheckBacktesterStatus"]
        success_choice = next(
            c for c in bt["Choices"] if c.get("StringEquals") == "Success"
        )
        assert success_choice["Next"] == "PredictorBacktest"

        def _success(check):
            return next(
                c for c in states[check]["Choices"]
                if c.get("StringEquals") == "Success"
            )["Next"]

        # Walk the L4472 split chain to the parity skip-gate.
        assert _success("CheckPredictorBacktestStatus") == "PortfolioOptimizerBacktest"
        assert _success("CheckPortfolioOptimizerBacktestStatus") == "CheckSkipParity"

        # skip_parity short-circuit reaches the Evaluator gate directly.
        skip_parity = states["CheckSkipParity"]
        assert skip_parity["Choices"][0]["Next"] == "CheckSkipEvaluator"
        # Default = run Parity; Parity success → CheckSkipEvaluator.
        assert skip_parity["Default"] == "Parity"
        parity_success = next(
            c
            for c in states["CheckParityStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert parity_success["Next"] == "CheckSkipEvaluator"


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
        health-check observability tail — the skip path goes to
        SaturdayHealthCheck directly, and the run path goes to
        Evaluator → CheckEvaluatorStatus → Success → SaturdayHealthCheck.

        Post-2026-05-07 reorder: the eval-judge chain runs UPSTREAM of
        Evaluator (after DataPhase2, before PredictorTraining), so the
        question this class previously asked (does eval-judge stay
        reachable from any skip-flag combination?) is now answered at
        the upstream junction (CheckSkipDataPhase2 → CheckSkipEvalJudge
        regardless of skip_data_phase2). At THIS junction, both
        branches simply exit to the health-check tail; no judge gate
        downstream to protect.
        """
        gate = states["CheckSkipEvaluator"]
        skip_choice = gate["Choices"][0]
        # groom #830: the health-check tail now sits behind CheckSkipSaturday-
        # HealthCheck; its Default leads to SaturdayHealthCheck, so both the
        # skip-evaluator and success paths still converge on the same tail.
        assert skip_choice["Next"] == "CheckSkipSaturdayHealthCheck"
        assert gate["Default"] == "Evaluator"
        # Run path success also exits to the same gate (judge already ran upstream).
        assert (
            states["CheckEvaluatorStatus"]["Choices"][0]["Next"]
            == "CheckSkipSaturdayHealthCheck"
        )
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
    def test_submit_routes_to_poll_choice_on_success(
        self, states, state_name,
    ):
        assert states[state_name]["Next"] == "EvalJudgePollChoice"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly"],
    )
    def test_submit_catch_routes_to_rolling_mean_not_failure(
        self, states, state_name,
    ):
        catch = states[state_name]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "EvalRollingMean"


class TestEvalJudgePollChoice:
    """EvalJudgePollChoice is the first Choice after Submit. EMPTY
    short-circuits the poll loop; OK enters the loop; anything else
    fail-softs to EvalRollingMean. Pinning these branches matters —
    silently routing EMPTY through the loop would burn 60s + 1
    Lambda invocation per poll cycle for hours."""

    def test_empty_routes_directly_to_process(self, states):
        choice = next(
            c for c in states["EvalJudgePollChoice"]["Choices"]
            if c.get("StringEquals") == "EMPTY"
        )
        assert choice["Next"] == "EvalJudgeProcess"
        assert choice["Variable"] == "$.eval_judge_submit.Payload.status"

    def test_ok_routes_to_poll_wait(self, states):
        choice = next(
            c for c in states["EvalJudgePollChoice"]["Choices"]
            if c.get("StringEquals") == "OK"
        )
        assert choice["Next"] == "EvalJudgePollWait"

    def test_default_is_fail_soft_to_rolling_mean(self, states):
        # Anything other than EMPTY/OK (ERROR, malformed) must NOT
        # halt the pipeline.
        assert states["EvalJudgePollChoice"]["Default"] == "EvalRollingMean"


class TestEvalJudgePollLoop:
    def test_poll_wait_60s(self, states):
        # 60s polls strike a balance between pickup latency and Lambda
        # invocation cost over Anthropic's typical sub-1h batch latency.
        assert states["EvalJudgePollWait"]["Seconds"] == 60
        assert states["EvalJudgePollWait"]["Next"] == "EvalJudgePoll"

    def test_poll_invokes_poll_lambda_live_alias(self, states):
        params = states["EvalJudgePoll"]["Parameters"]
        assert (
            params["FunctionName"]
            == "alpha-engine-research-eval-judge-poll:live"
        )

    def test_poll_payload_passes_batch_id_and_submit_iso(self, states):
        payload = states["EvalJudgePoll"]["Parameters"]["Payload"]
        assert payload["batch_id.$"] == "$.eval_judge_submit.Payload.batch_id"
        assert payload["submit_iso.$"] == "$.eval_cadence.submit_iso"
        # 6-hour fail-soft cap matches the Poll Lambda default.
        assert payload["max_wait_seconds"] == 21600

    def test_poll_decision_ended_routes_to_process(self, states):
        ended_choice = next(
            c for c in states["EvalJudgePollDecision"]["Choices"]
            if "Or" in c and any(
                clause.get("StringEquals") == "ended"
                for clause in c["Or"]
            )
        )
        assert ended_choice["Next"] == "EvalJudgeProcess"
        # ended_empty (synthetic empty-batch sentinel) must converge
        # to the same Process state — it's not a separate code path
        # in Process.
        assert any(
            clause.get("StringEquals") == "ended_empty"
            for clause in ended_choice["Or"]
        )

    def test_poll_decision_max_wait_routes_to_rolling_mean(self, states):
        max_wait_choice = next(
            c for c in states["EvalJudgePollDecision"]["Choices"]
            if c.get("Variable", "").endswith("exceeded_max_wait")
        )
        assert max_wait_choice["BooleanEquals"] is True
        # Fail-soft — Anthropic retains batch results for 29 days, so
        # operator can re-run Process offline against the same batch_id.
        assert max_wait_choice["Next"] == "EvalRollingMean"

    def test_poll_decision_default_loops_back_to_wait(self, states):
        # Continue polling until ended OR max_wait exceeded.
        assert states["EvalJudgePollDecision"]["Default"] == "EvalJudgePollWait"


class TestEvalJudgeProcessContract:
    def test_invokes_process_lambda_live_alias(self, states):
        params = states["EvalJudgeProcess"]["Parameters"]
        assert (
            params["FunctionName"]
            == "alpha-engine-research-eval-judge-process:live"
        )

    def test_payload_carries_batch_id_and_plan_key(self, states):
        payload = states["EvalJudgeProcess"]["Parameters"]["Payload"]
        assert payload["batch_id.$"] == "$.eval_judge_submit.Payload.batch_id"
        assert (
            payload["plan_s3_key.$"]
            == "$.eval_judge_submit.Payload.plan_s3_key"
        )

    def test_process_timeout_matches_lambda_cap(self, states):
        # Process Lambda has the 15-min ceiling — covers streaming
        # results + the synchronous Sonnet-escalation tail (typically
        # 1-3 calls × 5-8s each, well inside the cap).
        assert states["EvalJudgeProcess"]["TimeoutSeconds"] == 900

    def test_process_routes_to_rolling_mean_on_success(self, states):
        assert states["EvalJudgeProcess"]["Next"] == "EvalRollingMean"

    def test_process_catch_routes_to_rolling_mean_not_failure(self, states):
        catch = states["EvalJudgeProcess"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "EvalRollingMean"


# ── Non-blocking failure semantics — preserved across the chain ──────────


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
            "EvalJudgePoll",
            "EvalJudgeProcess",
        ],
    )
    def test_states_all_states_catch_routes_to_rolling_mean(
        self, states, state_name,
    ):
        catch = states[state_name]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "EvalRollingMean"
        assert catch["Next"] != "HandleFailure"


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

    def test_timeout_matches_lambda_cap(self, states):
        # Rolling-mean Lambda is configured with timeout=300s
        # (alpha-engine-research infrastructure/deploy.sh) — SF state
        # TimeoutSeconds must equal that ceiling.
        assert states["EvalRollingMean"]["TimeoutSeconds"] == 300

    def test_success_continues_to_rationale_clustering_gate(self, states):
        # Rolling-mean converges to CheckSkipRationaleClustering (the
        # gate in front of the cross-week clustering Lambda) rather
        # than directly to SaturdayHealthCheck.
        assert states["EvalRollingMean"]["Next"] == "CheckSkipRationaleClustering"

    def test_catch_routes_to_rationale_clustering_gate_not_failure(self, states):
        catch = states["EvalRollingMean"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "CheckSkipRationaleClustering"
        assert catch["Next"] != "HandleFailure"

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
    def test_skip_flag_bypasses_to_concordance_gate(self, states):
        """Skipping clustering must NOT also skip concordance — they
        are independent agent-justification signals (clustering = cross-
        week templating; concordance = same-input cross-model agreement).
        The skip path lands on CheckSkipReplayConcordance rather than
        SaturdayHealthCheck so the concordance Lambda still fires
        unless its own skip flag is set."""
        skip = states["CheckSkipRationaleClustering"]
        choice = skip["Choices"][0]
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_rationale_clustering"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "CheckSkipReplayConcordance"
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
        assert states["RationaleClustering"]["TimeoutSeconds"] == 600

    def test_success_continues_to_concordance_gate(self, states):
        # Clustering converges to CheckSkipReplayConcordance (the gate
        # in front of the cheap-model concordance Lambda) rather than
        # directly to SaturdayHealthCheck.
        assert states["RationaleClustering"]["Next"] == "CheckSkipReplayConcordance"

    def test_catch_routes_to_concordance_gate_not_failure(self, states):
        catch = states["RationaleClustering"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "CheckSkipReplayConcordance"
        assert catch["Next"] != "HandleFailure"

    def test_retries_on_transient_lambda_errors(self, states):
        retry = states["RationaleClustering"]["Retry"][0]
        assert "Lambda.ServiceException" in retry["ErrorEquals"]
        assert "Lambda.TooManyRequestsException" in retry["ErrorEquals"]
        assert retry["MaxAttempts"] == 1


# ── Replay concordance skip-gate + state ─────────────────────────────────


class TestSkipReplayConcordance:
    def test_skip_flag_bypasses_to_counterfactual_gate(self, states):
        """Skipping concordance must NOT also skip counterfactual —
        they are independent agent-justification signals (concordance
        = same-input cross-model agreement; counterfactual = 3-deep
        decision-tree match). The skip path lands on
        CheckSkipCounterfactual rather than SaturdayHealthCheck so the
        counterfactual Lambda still fires unless its own skip flag is
        set."""
        skip = states["CheckSkipReplayConcordance"]
        choice = skip["Choices"][0]
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_replay_concordance"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "CheckSkipCounterfactual"
        assert choice["Next"] != "SaturdayHealthCheck"

    def test_default_runs_concordance(self, states):
        assert states["CheckSkipReplayConcordance"]["Default"] == "ReplayConcordance"


class TestReplayConcordance:
    def test_invokes_live_alias(self, states):
        params = states["ReplayConcordance"]["Parameters"]
        assert params["FunctionName"] == "alpha-engine-replay-concordance:live"

    def test_payload_carries_required_fields(self, states):
        payload = states["ReplayConcordance"]["Parameters"]["Payload"]
        assert payload["end_time_iso.$"] == "$$.Execution.StartTime"
        assert payload["target_models"] == ["claude-haiku-4-5"]
        assert payload["window_days"] == 56
        assert payload["max_artifacts"] == 150

    def test_timeout_matches_lambda_cap(self, states):
        assert states["ReplayConcordance"]["TimeoutSeconds"] == 900

    def test_success_continues_to_counterfactual_gate(self, states):
        # Concordance converges to CheckSkipCounterfactual rather than
        # directly to SaturdayHealthCheck — counterfactual is the next
        # leg of the agent-justification triple.
        assert states["ReplayConcordance"]["Next"] == "CheckSkipCounterfactual"

    def test_catch_routes_to_counterfactual_gate_not_failure(self, states):
        catch = states["ReplayConcordance"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "CheckSkipCounterfactual"
        assert catch["Next"] != "HandleFailure"

    def test_retries_on_transient_lambda_errors(self, states):
        retry = states["ReplayConcordance"]["Retry"][0]
        assert "Lambda.ServiceException" in retry["ErrorEquals"]
        assert "Lambda.TooManyRequestsException" in retry["ErrorEquals"]
        assert retry["MaxAttempts"] == 1


# ── Counterfactual rule fit skip-gate + state ────────────────────────────


class TestSkipCounterfactual:
    def test_skip_flag_bypasses_to_aggregate_costs_gate(self, states):
        """Skipping Counterfactual now lands on the AggregateCosts
        skip-gate (ROADMAP L1146 — SF-wired daily cost aggregator
        added 2026-05-25), not directly on BranchAComplete. The cost
        aggregator reads cost JSONLs written by upstream LLM states
        (Research / eval-judge / rationale-clustering / replay-
        concordance / counterfactual); a counterfactual skip does NOT
        invalidate those upstream rows, so the aggregator MUST still
        run. The four observability skip flags (skip_counterfactual /
        skip_rationale_clustering / skip_replay_concordance /
        skip_aggregate_costs) are independent. Pre-L1146 this assertion
        pinned ``BranchAComplete``; the L1146 wire-up reroutes through
        ``CheckSkipAggregateCosts`` which transitively reaches the
        Branch-A terminal."""
        skip = states["CheckSkipCounterfactual"]
        choice = skip["Choices"][0]
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_counterfactual"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "CheckSkipAggregateCosts"

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

    def test_success_exits_to_aggregate_costs_gate(self, states):
        # Counterfactual is now the SECOND-to-last load-bearing state in
        # Branch A — the L1146 wire-up (2026-05-25) inserted the
        # AggregateCosts cost-telemetry aggregator after it. Success now
        # exits to CheckSkipAggregateCosts, which transitively reaches
        # BranchAComplete (End:true). Persisted S3 artifacts are still
        # available to the downstream Evaluator, which runs AFTER the
        # Parallel join. Pre-L1146: Counterfactual.Next == BranchAComplete.
        assert states["Counterfactual"]["Next"] == "CheckSkipAggregateCosts"

    def test_catch_routes_to_aggregate_costs_gate_not_failure(self, states):
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
        catch = states["Counterfactual"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "CheckSkipAggregateCosts"
        assert catch["Next"] != "HandleFailure"

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
        assert states["DataPhase2"]["Next"] == "CheckSkipEvalJudge"
        assert (
            states["CheckSkipDataPhase2"]["Choices"][0]["Next"]
            == "CheckSkipEvalJudge"
        )

    def test_counterfactual_exits_to_aggregate_costs_gate(self, states):
        """Counterfactual's three exit edges (Next + Catch + the
        skip-gate above it) all converge on the AggregateCosts skip-gate
        added by ROADMAP L1146 (2026-05-25). The Evaluator-sees-judge-
        artifacts ordering invariant is still satisfied because
        Evaluator runs AFTER the Parallel join, by which point Branch A
        (including the inserted cost-aggregator step) has completed and
        its S3 artifacts are landed. Edge target history:
        pre-2026-05-07 SaturdayHealthCheck → 2026-05-07→05-16
        CheckSkipPredictorTraining → 2026-05-16→05-25 BranchAComplete →
        post-L1146 CheckSkipAggregateCosts. The transitive reach to
        BranchAComplete is preserved (CheckSkipAggregateCosts.Default →
        AggregateCosts.Next → BranchAComplete; CheckSkipAggregateCosts's
        skip-branch → BranchAComplete directly)."""
        assert states["Counterfactual"]["Next"] == "CheckSkipAggregateCosts"
        assert (
            states["Counterfactual"]["Catch"][0]["Next"]
            == "CheckSkipAggregateCosts"
        )
        assert (
            states["CheckSkipCounterfactual"]["Choices"][0]["Next"]
            == "CheckSkipAggregateCosts"
        )

    def test_evaluator_exits_directly_to_health_check(self, states):
        """Evaluator's success path no longer enters the judge chain
        (judge ran upstream). It exits to the health-check tail, which
        groom #830 fronted with CheckSkipSaturdayHealthCheck so the
        backtest-eval mode preset can stop the run after Evaluator."""
        success = next(
            c for c in states["CheckEvaluatorStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert success["Next"] == "CheckSkipSaturdayHealthCheck"
        # And the skip-evaluator path also goes to the same gate
        # (the previous pre-reorder target was CheckSkipEvalJudge).
        assert (
            states["CheckSkipEvaluator"]["Choices"][0]["Next"]
            == "CheckSkipSaturdayHealthCheck"
        )
        # The gate's Default is the unchanged SaturdayHealthCheck.
        assert states["CheckSkipSaturdayHealthCheck"]["Default"] == "SaturdayHealthCheck"
