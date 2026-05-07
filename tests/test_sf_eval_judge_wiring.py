"""Pins the LLM-as-judge wiring in the Saturday Step Functions JSON.

Catches regressions like: someone re-routes CheckBacktesterStatus.Success
back to SaturdayHealthCheck and accidentally drops the eval state, or
flips the Default branch of the cadence Choice and ships every Saturday
on the (more expensive) monthly Sonnet sweep.

The corresponding alpha-engine-research Lambda
(``alpha-engine-research-eval-judge:live``) is in PR #91; this test only
asserts the SF wiring, not the handler shape.
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
    return sf["States"]


# ── State presence ────────────────────────────────────────────────────────


class TestStatesPresent:
    def test_all_eval_judge_states_exist(self, states):
        for name in (
            "CheckSkipEvalJudge",
            "ComputeEvalCadence",
            "CheckMonthlyCadence",
            "EvalJudgeFirstSaturday",
            "EvalJudgeWeekly",
            "EvalRollingMean",
            "CheckSkipRationaleClustering",
            "RationaleClustering",
            "CheckSkipReplayConcordance",
            "ReplayConcordance",
            "CheckSkipCounterfactual",
            "Counterfactual",
        ):
            assert name in states, f"missing SF state: {name}"


# ── Backtester success → evaluator skip-gate ──────────────────────────────


class TestBacktesterTransition:
    def test_success_routes_to_evaluator_skip_gate(self, states):
        # Post-2026-05-07 split: Backtester success now routes to
        # CheckSkipEvaluator (the new gate in front of the standalone
        # Evaluator state) instead of CheckSkipEvalJudge. The evaluator
        # then converges back into the eval-judge chain on success.
        bt = states["CheckBacktesterStatus"]
        success_choice = next(
            c for c in bt["Choices"] if c.get("StringEquals") == "Success"
        )
        assert success_choice["Next"] == "CheckSkipEvaluator"


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

    def test_evaluator_skip_gate_always_reaches_eval_judge(self, states):
        """Both branches of CheckSkipEvaluator must keep eval-judge
        reachable — the skip path goes to CheckSkipEvalJudge directly,
        and the run path goes to Evaluator → CheckEvaluatorStatus →
        Success → CheckSkipEvalJudge. Together with the skip_backtester
        decoupling, this guarantees no skip-flag combination bypasses
        eval-judge."""
        gate = states["CheckSkipEvaluator"]
        skip_choice = gate["Choices"][0]
        assert skip_choice["Next"] == "CheckSkipEvalJudge"
        # Default routes to Evaluator, which converges back via
        # CheckEvaluatorStatus's Success branch.
        assert gate["Default"] == "Evaluator"
        assert (
            states["CheckEvaluatorStatus"]["Choices"][0]["Next"]
            == "CheckSkipEvalJudge"
        )


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
    def test_default_is_weekly(self, states):
        # Default = the COMMON path (every other Saturday). Must NOT
        # be EvalJudgeFirstSaturday — that would ship every weekly run
        # on the expensive monthly Sonnet sweep.
        assert states["CheckMonthlyCadence"]["Default"] == "EvalJudgeWeekly"

    def test_first_saturday_branch_uses_lex_compare_under_08(self, states):
        choice = states["CheckMonthlyCadence"]["Choices"][0]
        assert choice["Variable"] == "$.eval_cadence.day_of_month"
        assert choice["StringLessThan"] == "08"
        assert choice["Next"] == "EvalJudgeFirstSaturday"


# ── Lambda invocation contract ────────────────────────────────────────────


class TestEvalJudgeLambdaContract:
    @pytest.mark.parametrize(
        "state_name,expected_force_sonnet",
        [
            ("EvalJudgeFirstSaturday", True),
            ("EvalJudgeWeekly", False),
        ],
    )
    def test_payload_carries_correct_force_sonnet_flag(
        self, states, state_name, expected_force_sonnet,
    ):
        payload = states[state_name]["Parameters"]["Payload"]
        assert payload["force_sonnet_pass"] is expected_force_sonnet

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_payload_passes_eval_date(self, states, state_name):
        payload = states[state_name]["Parameters"]["Payload"]
        # SF passes the SF-execution-start-date so the Lambda evaluates
        # the same partition the captures landed in (avoids UTC-rollover
        # edge cases where the Lambda starts on day X+1).
        assert payload["date.$"] == "$.eval_cadence.eval_date"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_invokes_live_alias(self, states, state_name):
        params = states[state_name]["Parameters"]
        assert params["FunctionName"] == "alpha-engine-research-eval-judge:live"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_timeout_matches_lambda_max(self, states, state_name):
        # Lambda's hard timeout is 900s (set in alpha-engine-research
        # infrastructure/deploy.sh). SF state TimeoutSeconds must not be
        # less — otherwise SF would kill an in-progress eval prematurely.
        assert states[state_name]["TimeoutSeconds"] == 900


# ── Non-blocking failure semantics ────────────────────────────────────────


class TestEvalJudgeNonBlocking:
    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_success_continues_to_rolling_mean(self, states, state_name):
        # Eval-judge branches converge to EvalRollingMean (PR 4c)
        # rather than SaturdayHealthCheck — ensures the rolling-mean
        # derived metric runs every week with the freshest raw data.
        assert states[state_name]["Next"] == "EvalRollingMean"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_catch_routes_to_rolling_mean_not_failure(self, states, state_name):
        # Eval is observability per ROADMAP §1635 — failures must NOT
        # halt the pipeline. Even if eval-judge errors out at the infra
        # level, rolling-mean still runs against whatever historical
        # data IS available (the prior 4 weeks are unaffected).
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
    def test_skip_flag_bypasses_to_health_check(self, states):
        skip = states["CheckSkipCounterfactual"]
        choice = skip["Choices"][0]
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_counterfactual"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "SaturdayHealthCheck"

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

    def test_success_continues_to_health_check(self, states):
        assert states["Counterfactual"]["Next"] == "SaturdayHealthCheck"

    def test_catch_routes_to_health_check_not_failure(self, states):
        catch = states["Counterfactual"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "SaturdayHealthCheck"
        assert catch["Next"] != "HandleFailure"

    def test_retries_on_transient_lambda_errors(self, states):
        retry = states["Counterfactual"]["Retry"][0]
        assert "Lambda.ServiceException" in retry["ErrorEquals"]
        assert "Lambda.TooManyRequestsException" in retry["ErrorEquals"]
        assert retry["MaxAttempts"] == 1
