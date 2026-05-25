"""Pins the Scanner Lambda wiring in the Saturday Step Functions JSON.

ROADMAP L1995 Phase 2 — gated default-off via `enable_standalone_scanner`
flag. The companion alpha-engine-research PR #235 adds the
`alpha-engine-research-scanner:live` Lambda (Phase 1); this PR inserts
the SF state that invokes it when the operator flips the flag (Phase 3
soak), so RAGIngestion (Phase 4) can later read the new
`candidates.json` artifact.

Pin the chain between DataPhase1 and CheckSkipRAGIngestion:

    DataPhase1 (skip path)    ──┐
                                ├──→ CheckEnableStandaloneScanner ──→ Scanner ──→ CheckSkipRAGIngestion
    CheckDataPhase1Status.Success┘                              └──(default)──→ CheckSkipRAGIngestion

When the flag is false/absent (Phase 2 default), the path through
CheckEnableStandaloneScanner is byte-identical to the pre-Phase-2 chain
— Choice.Default goes directly to CheckSkipRAGIngestion, no Lambda
invocation occurs. When the flag is true, the Lambda runs in
parallel-observe mode and Catch ensures failure does NOT halt the
pipeline.
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
    def test_scanner_states_exist(self, states):
        assert "CheckEnableStandaloneScanner" in states
        assert "Scanner" in states

    def test_scanner_is_a_task(self, states):
        assert states["Scanner"]["Type"] == "Task"

    def test_check_enable_is_a_choice(self, states):
        assert states["CheckEnableStandaloneScanner"]["Type"] == "Choice"


# ── Lambda target + payload ───────────────────────────────────────────────


class TestLambdaTarget:
    def test_lambda_function_arn(self, states):
        params = states["Scanner"]["Parameters"]
        assert (
            params["FunctionName"]
            == "alpha-engine-research-scanner:live"
        )
        assert states["Scanner"]["Resource"] == (
            "arn:aws:states:::lambda:invoke"
        )

    def test_payload_threads_run_date(self, states):
        # The handler hard-requires event["run_date"] — must be threaded
        # from $.run_date (seeded by InitializeInput from
        # $$.Execution.StartTime).
        payload = states["Scanner"]["Parameters"]["Payload"]
        assert payload["run_date.$"] == "$.run_date"

    def test_payload_threads_shell_run_dry_flag(self, states):
        # dry_run_llm threading mirrors the rationale_clustering /
        # eval-judge / aggregate_costs chain — Friday-Preflight shell
        # runs short-circuit the S3 read + Lambda S3 write.
        payload = states["Scanner"]["Parameters"]["Payload"]
        assert payload["dry_run_llm.$"] == "$.research_dry"


# ── Failure isolation ─────────────────────────────────────────────────────


class TestFailureIsolation:
    def test_catch_routes_to_check_skip_rag(self, states):
        # Phase 2 observe-only contract — scanner failure must NOT halt
        # the pipeline. Research Lambda's internal scanner still runs
        # in parallel and the artifact is consumer-less today.
        catches = states["Scanner"]["Catch"]
        assert len(catches) >= 1
        assert any(
            c["Next"] == "CheckSkipRAGIngestion"
            and "States.ALL" in c["ErrorEquals"]
            for c in catches
        )

    def test_retry_only_on_lambda_service_errors(self, states):
        # Same shape as rationale-clustering / aggregate_costs —
        # service-level retries but NO retry on application-level errors
        # (which would mask scanner bugs during Phase 3 soak).
        retries = states["Scanner"]["Retry"]
        assert any(
            "Lambda.ServiceException" in r["ErrorEquals"]
            and "Lambda.TooManyRequestsException" in r["ErrorEquals"]
            for r in retries
        )


# ── Gate semantics (default-off) ──────────────────────────────────────────


class TestGateDefaultOff:
    def test_default_path_is_check_skip_rag(self, states):
        # Phase 2 ships gated default-off: when the flag is absent or
        # false the chain is byte-identical to pre-Phase-2.
        assert (
            states["CheckEnableStandaloneScanner"]["Default"]
            == "CheckSkipRAGIngestion"
        )

    def test_flag_true_routes_to_scanner(self, states):
        gate = states["CheckEnableStandaloneScanner"]
        choices = gate["Choices"]
        assert len(choices) == 1
        choice = choices[0]
        assert choice["Next"] == "Scanner"
        # Conjunction pins the variable + bool semantics (IsPresent AND
        # BooleanEquals true).
        variables = [c["Variable"] for c in choice["And"]]
        assert all(v == "$.enable_standalone_scanner" for v in variables)
        assert any(c.get("BooleanEquals") is True for c in choice["And"])


# ── Wiring: edges into and out of the new states ──────────────────────────


class TestEdges:
    def test_data_phase1_skip_path_routes_to_scanner_gate(self, states):
        # CheckSkipDataPhase1's skip branch (operator passes
        # skip_data_phase1=true) previously went to CheckSkipRAGIngestion
        # directly. Phase 2 re-routes it through the new gate so the
        # Phase-2-enabled path is honored even on a phase1-skip rerun.
        skip = states["CheckSkipDataPhase1"]
        skip_choices = skip["Choices"]
        assert any(
            c["Next"] == "CheckEnableStandaloneScanner"
            for c in skip_choices
        )

    def test_data_phase1_success_path_routes_to_scanner_gate(self, states):
        # CheckDataPhase1Status.Success previously went to
        # CheckSkipRAGIngestion directly; Phase 2 re-routes through the
        # new gate.
        status = states["CheckDataPhase1Status"]
        success = next(
            c for c in status["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert success["Next"] == "CheckEnableStandaloneScanner"

    def test_scanner_success_routes_to_check_skip_rag(self, states):
        assert states["Scanner"]["Next"] == "CheckSkipRAGIngestion"

    def test_default_path_byte_identical_to_pre_phase_2(self, states):
        # The end of this chain MUST equal what the pre-Phase-2 chain
        # had — CheckSkipRAGIngestion. Anyone changing the gate's
        # Default without updating this assertion is silently breaking
        # the observe-only Phase 2 contract.
        assert (
            states["CheckEnableStandaloneScanner"]["Default"]
            == "CheckSkipRAGIngestion"
        )


# ── Result paths (state-merge contract) ───────────────────────────────────


class TestResultPaths:
    def test_success_result_lands_under_scanner_result(self, states):
        # Mirrors rationale_clustering_result / counterfactual_result /
        # aggregate_costs_result — each observability Lambda result is
        # namespaced under its own ResultPath so the parent state
        # doesn't get clobbered.
        assert states["Scanner"]["ResultPath"] == "$.scanner_result"

    def test_failure_result_lands_under_scanner_error(self, states):
        catches = states["Scanner"]["Catch"]
        catch_all = next(
            c for c in catches if "States.ALL" in c["ErrorEquals"]
        )
        assert catch_all["ResultPath"] == "$.scanner_error"


# ── Timeout ───────────────────────────────────────────────────────────────


class TestTimeout:
    def test_timeout_is_bounded(self, states):
        # The handler's expected wallclock is ~minutes for the full
        # ~903-ticker filter pass + small artifact write. 600s gives
        # generous headroom while still tripping a hung run.
        assert states["Scanner"]["TimeoutSeconds"] == 600
