"""Pins the AggregateCosts Lambda wiring in the Saturday Step Functions JSON.

ROADMAP L1146 — SF-wire ``scripts/aggregate_costs.py`` CLI. The
companion alpha-engine-research PR adds the
``alpha-engine-research-aggregate-costs:live`` Lambda; this test only
asserts the SF wiring.

Pin the chain end of Branch A:

    ... Counterfactual → CheckSkipAggregateCosts → AggregateCosts
                                                 → BranchAComplete

The aggregator must sit AFTER the entire LLM chain (Research /
eval-judge / rationale-clustering / replay-concordance / counterfactual)
so every upstream LLM-emitting state has finished writing its
``_cost_raw/{date}/*.jsonl`` rows by the time the aggregator runs.
Catches regressions like: a future SF refactor that drops the new
state, or reroutes Counterfactual back to BranchAComplete bypassing
the aggregator.
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
    branch's states. Mirrors the helper in test_sf_eval_judge_wiring.py.
    """
    flat: dict = dict(sf["States"])
    for st in sf["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat


# ── State presence ────────────────────────────────────────────────────────


class TestStatesPresent:
    def test_aggregate_costs_states_exist(self, states):
        assert "CheckSkipAggregateCosts" in states
        assert "AggregateCosts" in states

    def test_aggregate_costs_is_a_task(self, states):
        assert states["AggregateCosts"]["Type"] == "Task"

    def test_aggregate_costs_check_skip_is_a_choice(self, states):
        assert states["CheckSkipAggregateCosts"]["Type"] == "Choice"


# ── Lambda target + payload ───────────────────────────────────────────────


class TestLambdaTarget:
    def test_lambda_function_arn(self, states):
        params = states["AggregateCosts"]["Parameters"]
        assert (
            params["FunctionName"]
            == "alpha-engine-research-aggregate-costs:live"
        )
        assert states["AggregateCosts"]["Resource"] == (
            "arn:aws:states:::lambda:invoke"
        )

    def test_payload_threads_run_date(self, states):
        # The handler hard-requires event["date"] — must be threaded
        # from $.run_date (seeded by InitializeInput from
        # $$.Execution.StartTime).
        payload = states["AggregateCosts"]["Parameters"]["Payload"]
        assert payload["date.$"] == "$.run_date"

    def test_payload_threads_shell_run_dry_flag(self, states):
        # dry_run_llm threading mirrors the rationale_clustering /
        # eval-judge chain — Friday-Preflight shell runs short-circuit
        # the S3 read + parquet write.
        payload = states["AggregateCosts"]["Parameters"]["Payload"]
        assert payload["dry_run_llm.$"] == "$.research_dry"


# ── Failure isolation ─────────────────────────────────────────────────────


class TestFailureIsolation:
    def test_catch_routes_to_branch_a_complete(self, states):
        # Cost telemetry is observability — aggregator failure must NOT
        # halt the pipeline. Mirrors the rationale-clustering Catch.
        catches = states["AggregateCosts"]["Catch"]
        assert len(catches) >= 1
        assert any(
            c["Next"] == "BranchAComplete"
            and "States.ALL" in c["ErrorEquals"]
            for c in catches
        )

    def test_retry_only_on_lambda_service_errors(self, states):
        # Same shape as rationale-clustering — service-level retries
        # (Lambda.ServiceException, TooManyRequestsException) but NO
        # retry on application-level errors (which would mask
        # aggregator bugs).
        retries = states["AggregateCosts"]["Retry"]
        assert any(
            "Lambda.ServiceException" in r["ErrorEquals"]
            and "Lambda.TooManyRequestsException" in r["ErrorEquals"]
            for r in retries
        )


# ── Wiring: edges into and out of AggregateCosts ──────────────────────────


class TestEdges:
    def test_counterfactual_routes_to_check_skip_aggregate_costs(self, states):
        # Default path (success) — Counterfactual's Next must hit the
        # new skip-gate, NOT BranchAComplete directly.
        cf = states["Counterfactual"]
        assert cf["Next"] == "CheckSkipAggregateCosts"

    def test_counterfactual_catch_routes_to_check_skip_aggregate_costs(self, states):
        # Counterfactual failure must STILL run the aggregator —
        # upstream LLM cost rows are independent of counterfactual's
        # success. (The aggregator's own Catch routes to
        # BranchAComplete so a downstream failure-of-failure can't
        # ladder back into the failed counterfactual error path.)
        cf = states["Counterfactual"]
        catches = cf["Catch"]
        assert any(
            c["Next"] == "CheckSkipAggregateCosts"
            for c in catches
        )

    def test_check_skip_counterfactual_default_unchanged(self, states):
        # CheckSkipCounterfactual's Default → Counterfactual stays
        # unchanged; only the SKIP branch is rerouted to the new
        # CheckSkipAggregateCosts.
        skip = states["CheckSkipCounterfactual"]
        assert skip["Default"] == "Counterfactual"

    def test_check_skip_counterfactual_skip_routes_to_aggregate_costs_gate(
        self, states,
    ):
        # When operator skips counterfactual, the flow MUST still hit
        # the aggregator gate (an operator skipping counterfactual is
        # NOT also skipping cost-telemetry — those are independent
        # skip flags per the comment on CheckSkipAggregateCosts).
        skip = states["CheckSkipCounterfactual"]
        skip_choices = skip["Choices"]
        assert len(skip_choices) == 1
        assert skip_choices[0]["Next"] == "CheckSkipAggregateCosts"

    def test_aggregate_costs_success_routes_to_branch_a_complete(self, states):
        assert states["AggregateCosts"]["Next"] == "BranchAComplete"

    def test_check_skip_aggregate_costs_skip_routes_to_branch_a_complete(
        self, states,
    ):
        skip = states["CheckSkipAggregateCosts"]
        skip_choices = skip["Choices"]
        assert len(skip_choices) == 1
        assert skip_choices[0]["Next"] == "BranchAComplete"

    def test_check_skip_aggregate_costs_default_is_aggregate_costs(self, states):
        assert states["CheckSkipAggregateCosts"]["Default"] == "AggregateCosts"


# ── Skip-flag semantics ───────────────────────────────────────────────────


class TestSkipFlagSemantics:
    def test_skip_flag_named_skip_aggregate_costs(self, states):
        skip = states["CheckSkipAggregateCosts"]
        # Each choice's conjunction names the variable being inspected.
        choice = skip["Choices"][0]
        # Mirror the pattern used by the other observability skip
        # gates: IsPresent + BooleanEquals true.
        variables = [c["Variable"] for c in choice["And"]]
        assert all(v == "$.skip_aggregate_costs" for v in variables)
        assert any(c.get("BooleanEquals") is True for c in choice["And"])


# ── Result paths (state-merge contract) ───────────────────────────────────


class TestResultPaths:
    def test_success_result_lands_under_aggregate_costs_result(self, states):
        # Mirrors rationale_clustering_result / counterfactual_result —
        # each observability Lambda result is namespaced under its own
        # ResultPath so the parent state doesn't get clobbered.
        assert (
            states["AggregateCosts"]["ResultPath"]
            == "$.aggregate_costs_result"
        )

    def test_failure_result_lands_under_aggregate_costs_error(self, states):
        catches = states["AggregateCosts"]["Catch"]
        catch_all = next(
            c for c in catches if "States.ALL" in c["ErrorEquals"]
        )
        assert catch_all["ResultPath"] == "$.aggregate_costs_error"


# ── Timeout ───────────────────────────────────────────────────────────────


class TestTimeout:
    def test_timeout_is_bounded(self, states):
        # The handler's expected wallclock is ~minutes for thousands
        # of small S3 reads + one parquet write. 600s (10 min) gives
        # generous headroom while still tripping a hung run.
        assert states["AggregateCosts"]["TimeoutSeconds"] == 600
