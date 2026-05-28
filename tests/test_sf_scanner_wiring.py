"""Pins the Scanner Lambda wiring in the Saturday Step Functions JSON.

ROADMAP L1995 Phase 2 — Scanner runs UNCONDITIONALLY on every Saturday
SF firing (parallel-observe mode, non-blocking Catch). The prior
``CheckEnableStandaloneScanner`` Choice gate was removed 2026-05-28 per
``feedback_observe_mode_unconditional_gates_govern_cutover`` — observe-
mode producer code must never be flag-gated, since silent absence-of-
artifact is itself the failure mode the soak is meant to detect. The
Phase 4/5 consumer-cutover flag (Research / RAG reading
``candidates.json`` load-bearingly) belongs at the consumer side, not
the producer side.

Pin the chain between DataPhase1 and CheckSkipRAGIngestion:

    DataPhase1 (skip path)        ──┐
                                    ├──→ Scanner ──→ CheckSkipRAGIngestion
    CheckDataPhase1Status.Success ──┘    │
                                         └─(Catch)──→ CheckSkipRAGIngestion

Scanner failure must NOT halt the pipeline (Research Lambda's internal
scanner still produces the load-bearing universe today; the standalone
artifact is parallel-observe and consumer-less until Phase 4).
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
    def test_scanner_state_exists(self, states):
        assert "Scanner" in states

    def test_scanner_is_a_task(self, states):
        assert states["Scanner"]["Type"] == "Task"

    def test_no_enable_scanner_choice_gate(self, states):
        # The Choice gate was removed 2026-05-28 — observe-mode must not
        # be flag-gated. Re-introducing it silently disables observation
        # any time the enable_standalone_scanner input is absent or false.
        assert "CheckEnableStandaloneScanner" not in states, (
            "CheckEnableStandaloneScanner Choice gate must not exist. "
            "Per feedback_observe_mode_unconditional_gates_govern_cutover, "
            "Scanner runs unconditionally — gates belong at the consumer "
            "side (Phase 4/5), not the producer side."
        )


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


# ── Wiring: edges into and out of Scanner ─────────────────────────────────


class TestEdges:
    def test_data_phase1_skip_path_routes_to_scanner(self, states):
        # CheckSkipDataPhase1's skip branch (operator passes
        # skip_data_phase1=true) routes through Scanner unconditionally,
        # matching the success path. Anyone re-introducing a gate here
        # silently disables observation on phase1-skip reruns.
        skip = states["CheckSkipDataPhase1"]
        skip_choices = skip["Choices"]
        assert any(
            c["Next"] == "Scanner" for c in skip_choices
        ), "CheckSkipDataPhase1 skip branch must route directly to Scanner."

    def test_data_phase1_success_path_routes_to_scanner(self, states):
        status = states["CheckDataPhase1Status"]
        success = next(
            c for c in status["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert success["Next"] == "Scanner", (
            "CheckDataPhase1Status Success branch must route directly "
            "to Scanner — no intermediate gate."
        )

    def test_scanner_success_routes_to_check_skip_rag(self, states):
        assert states["Scanner"]["Next"] == "CheckSkipRAGIngestion"


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
