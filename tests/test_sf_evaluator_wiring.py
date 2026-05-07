"""Pins the Evaluator state wiring in the Saturday Step Functions JSON.

The Evaluator state was split from the consolidated Backtester state on
2026-05-07 (plan: alpha-engine-docs/private/evaluator-split-260507.md)
for failure isolation, per-stage email, and independent CloudWatch
heartbeats. This test pins the split topology so a future operator
doesn't accidentally reroute Backtester success straight back to
CheckSkipEvalJudge (the pre-split shape) or merge the two states again
without a deliberate ROADMAP item.

Distinct from test_sf_eval_judge_wiring.py: that file pins the
LLM-as-judge Lambda chain (Haiku/Sonnet rubric scoring); this one pins
the spot-based evaluate.py state (per-signal grading + optimizer
auto-apply).
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
    def test_all_evaluator_states_exist(self, states):
        for name in (
            "CheckSkipEvaluator",
            "Evaluator",
            "WaitForEvaluator",
            "CheckEvaluatorStatus",
            "EvaluatorWait",
            "ExtractEvaluatorError",
        ):
            assert name in states, f"missing SF state: {name}"


# ── Skip gate ─────────────────────────────────────────────────────────────


class TestSkipEvaluator:
    def test_skip_flag_bypasses_to_eval_judge_gate(self, states):
        """Skipping the Evaluator state must NOT also skip the LLM-judge
        chain — they're independent (evaluator = per-signal grading +
        optimizer auto-apply against backtest artifacts; eval-judge =
        LLM rubric scoring of decision_artifacts/). The skip path lands
        on CheckSkipEvalJudge so the judge chain still fires unless its
        own skip flag is set.
        """
        skip = states["CheckSkipEvaluator"]
        choice = skip["Choices"][0]
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_evaluator"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "CheckSkipEvalJudge"
        # Critically NOT routed to SaturdayHealthCheck — that would
        # bundle-skip both the evaluator and the entire eval-judge
        # observability chain.
        assert choice["Next"] != "SaturdayHealthCheck"

    def test_default_runs_evaluator(self, states):
        assert states["CheckSkipEvaluator"]["Default"] == "Evaluator"


class TestSkipBacktesterAlsoSkipsEvaluator:
    """Pins the contract that {"skip_backtester": true} skips BOTH
    Backtester and Evaluator (since the evaluator reads same-cohort
    backtest artifacts — running it without a fresh backtest would grade
    against stale data). The CheckSkipBacktester skip path therefore
    routes to CheckSkipEvalJudge (skipping past CheckSkipEvaluator
    entirely), preserving the LLM-judge chain downstream.
    """

    def test_skip_backtester_bypasses_evaluator(self, states):
        skip = states["CheckSkipBacktester"]
        choice = skip["Choices"][0]
        # The skip-true branch must skip past the Evaluator state.
        # Routing to CheckSkipEvalJudge is correct — CheckSkipEvaluator
        # is between CheckBacktesterStatus and CheckSkipEvalJudge in
        # the success path, so jumping straight to CheckSkipEvalJudge
        # bypasses the evaluator without re-implementing the gate.
        assert choice["Next"] == "CheckSkipEvalJudge"
        assert choice["Next"] != "CheckSkipEvaluator"


# ── Evaluator task contract ───────────────────────────────────────────────


class TestEvaluatorTask:
    def test_invokes_ssm_send_command(self, states):
        assert (
            states["Evaluator"]["Resource"]
            == "arn:aws:states:::aws-sdk:ssm:sendCommand"
        )

    def test_command_passes_skip_stages_backtest_parity(self, states):
        # The Evaluator state reuses spot_backtest.sh — the canonical
        # dispatch surface — and skips the backtest + parity stages so
        # only evaluate.py runs. If a future operator drops --skip-stages
        # the spot will re-run the full 121-min backtest and the split
        # collapses silently.
        cmds = states["Evaluator"]["Parameters"]["Parameters"]["commands"]
        spot_cmd = next(c for c in cmds if "spot_backtest.sh" in c)
        assert "--skip-stages=backtest,parity" in spot_cmd

    def test_writes_to_evaluator_log(self, states):
        # Tee output into /var/log/evaluator.log so it's distinguishable
        # from /var/log/backtester.log on the spot host.
        cmds = states["Evaluator"]["Parameters"]["Parameters"]["commands"]
        spot_cmd = next(c for c in cmds if "spot_backtest.sh" in c)
        assert "/var/log/evaluator.log" in spot_cmd

    def test_timeout_is_60_min(self, states):
        # Evaluator runtime is ~30 min for full mode (per evaluate.py
        # historical runs). 3600s ceiling gives 2x headroom + bootstrap.
        assert states["Evaluator"]["Parameters"]["TimeoutSeconds"] == 3600
        # SF state TimeoutSeconds wraps with +60s safety buffer (matches
        # Backtester's 7200/7260 ratio).
        assert states["Evaluator"]["TimeoutSeconds"] == 3660

    def test_retry_mirrors_backtester_posture(self, states):
        # Spot interruption handling: 2 attempts, 180s initial backoff,
        # 2.0x multiplier — matches Backtester for symmetry.
        retry = states["Evaluator"]["Retry"][0]
        assert retry["MaxAttempts"] == 2
        assert retry["IntervalSeconds"] == 180
        assert retry["BackoffRate"] == 2.0

    def test_catch_routes_to_handle_failure(self, states):
        # Evaluator failure halts the pipeline (unlike eval-judge which
        # is observability-only). The optimizer auto-apply contract
        # means a silent evaluator failure could leave stale configs in
        # production — fail loud.
        catch = states["Evaluator"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "HandleFailure"


# ── Poll loop ─────────────────────────────────────────────────────────────


class TestEvaluatorPollLoop:
    def test_evaluator_routes_to_wait_state(self, states):
        assert states["Evaluator"]["Next"] == "WaitForEvaluator"

    def test_wait_for_evaluator_polls_evaluator_command(self, states):
        params = states["WaitForEvaluator"]["Parameters"]
        assert params["CommandId.$"] == "$.evaluator_result.Command.CommandId"

    def test_wait_for_evaluator_routes_to_check_status(self, states):
        assert states["WaitForEvaluator"]["Next"] == "CheckEvaluatorStatus"

    def test_check_status_success_continues_to_eval_judge(self, states):
        # On evaluator success the pipeline picks up the LLM-judge chain
        # at the same skip-gate that pre-split Backtester success used.
        bt = states["CheckEvaluatorStatus"]
        success_choice = next(
            c for c in bt["Choices"] if c.get("StringEquals") == "Success"
        )
        assert success_choice["Next"] == "CheckSkipEvalJudge"

    def test_check_status_in_progress_loops_to_wait(self, states):
        bt = states["CheckEvaluatorStatus"]
        ip_choice = next(
            c for c in bt["Choices"] if c.get("StringEquals") == "InProgress"
        )
        assert ip_choice["Next"] == "EvaluatorWait"

    def test_check_status_default_extracts_error(self, states):
        assert states["CheckEvaluatorStatus"]["Default"] == "ExtractEvaluatorError"

    def test_evaluator_wait_loops_back_to_poll(self, states):
        assert states["EvaluatorWait"]["Next"] == "WaitForEvaluator"


# ── Failure normalization ────────────────────────────────────────────────


class TestExtractEvaluatorError:
    def test_phase_label_is_evaluator(self, states):
        params = states["ExtractEvaluatorError"]["Parameters"]
        assert params["phase"] == "Evaluator"

    def test_carries_evaluator_poll_into_error(self, states):
        params = states["ExtractEvaluatorError"]["Parameters"]
        assert params["poll.$"] == "$.evaluator_poll"

    def test_routes_to_handle_failure(self, states):
        assert states["ExtractEvaluatorError"]["Next"] == "HandleFailure"
