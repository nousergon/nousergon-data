"""Pins the ne-weekly-advisory-pipeline child SF (alpha-engine-config-I2544).

Origin: Phase 2 of the weekly-SF load-reduction plan — the eval-judge chain
(EvalJudge submit/poll/process -> EvalRollingMean -> RationaleClustering ->
ReplayConcordance -> Counterfactual -> AggregateCosts), PLUS ReportCard and
Director, were lifted OUT of the main ne-weekly-freshness-pipeline's Branch A
/ top-level tail into this async child SF (infrastructure/
step_function_advisory.json), fired fire-and-forget (states:startExecution,
NOT .sync) by the main SF's StartAdvisoryPipeline state once DataPhase2
completes (see test_sf_research_predictor_parallel_wiring.py for that side).

Every lifted state's Payload/Retry/Catch semantics were REQUIRED to be
preserved byte-for-byte (I2544 build instruction) — this file re-pins the
same behavioral assertions that used to live in
test_sf_research_predictor_parallel_wiring.py's TestBranchAContents /
TestPerBranchErrorIsolation classes, now pointed at this file, PLUS new
coverage for the child-SF-only scaffolding (InitializeAdvisoryInput's
defaults floor, the single-branch Parallel catch-all wrapper, the terminal
notify pair, and the child's own top-level TimeoutSeconds).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_advisory.json"

_LIFTED_TAIL_STATES = {
    "CheckSkipEvalJudge", "ComputeEvalCadence", "CheckMonthlyCadence",
    "EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly",
    "EvalJudgePollChoice", "EvalJudgePollWait", "EvalJudgePoll",
    "EvalJudgePollDecision", "EvalJudgeProcess", "EvalRollingMean",
    "CheckSkipRationaleClustering", "RationaleClustering",
    "CheckSkipReplayConcordance", "ReplayConcordance",
    "CheckSkipCounterfactual", "Counterfactual",
    "CheckSkipAggregateCosts", "AggregateCosts",
    "ReportCard", "PublishReportCardDegraded", "Director",
    "PublishDirectorDegraded",
}


def _own_targets(st: dict) -> list[str]:
    out: list[str] = []

    def rec(o) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ("Branches", "ItemProcessor", "Iterator"):
                    continue
                if k in ("Next", "Default") and isinstance(v, str):
                    out.append(v)
                elif k == "Catch":
                    for c in v:
                        out.append(c["Next"])
                else:
                    rec(v)
        elif isinstance(o, list):
            for it in o:
                rec(it)

    rec(st)
    return out


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


@pytest.fixture(scope="module")
def wrapper(states) -> dict:
    return states["AdvisoryPipelineWrapper"]


@pytest.fixture(scope="module")
def inner(wrapper) -> dict:
    return wrapper["Branches"][0]["States"]


class TestJsonParsesAndTopLevel:
    def test_json_parses(self, sf):
        assert isinstance(sf, dict)
        assert sf["StartAt"] in sf["States"]

    def test_start_at_initializes_input(self, sf):
        assert sf["StartAt"] == "InitializeAdvisoryInput"

    def test_top_level_timeout_clears_eval_judge_poll_cap(self, sf):
        # Also pinned in test_sf_global_timeout.py; re-pinned here for
        # locality with the rest of this SF's coverage.
        assert sf["TimeoutSeconds"] == 28800

    def test_initialize_advisory_input_defaults_floor(self, states):
        init = states["InitializeAdvisoryInput"]
        assert init["Type"] == "Pass"
        merge_expr = init["Parameters"]["merged.$"]
        for flag in (
            "research_dry", "skip_eval_judge", "skip_rationale_clustering",
            "skip_replay_concordance", "skip_counterfactual",
            "skip_aggregate_costs",
        ):
            assert f'"{flag}":false' in merge_expr
        assert "$$.Execution.Input" in merge_expr
        assert init["Next"] == "AdvisoryPipelineWrapper"


class TestWrapperParallelCatchAll:
    """Single-branch Parallel retrofit for a machine-level catch-all,
    WITHOUT altering any lifted state's own (deliberately non-fatal)
    Catch semantics."""

    def test_wrapper_is_single_branch_parallel(self, wrapper):
        assert wrapper["Type"] == "Parallel"
        assert len(wrapper["Branches"]) == 1

    def test_wrapper_starts_at_check_skip_eval_judge(self, wrapper):
        assert wrapper["Branches"][0]["StartAt"] == "CheckSkipEvalJudge"

    def test_wrapper_catch_routes_to_failure_funnel(self, wrapper):
        catches = wrapper["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "AdvisoryNormalizeFailureContext"
            and c["ResultPath"] == "$.error"
            for c in catches
        )

    def test_failure_funnel_reaches_fail_state(self, states):
        assert states["AdvisoryNormalizeFailureContext"]["Next"] == (
            "AdvisoryHandleFailure"
        )
        assert states["AdvisoryHandleFailure"]["Next"] == "AdvisoryFailExecution"
        assert states["AdvisoryFailExecution"]["Type"] == "Fail"


class TestLiftedStatesPresent:
    @pytest.mark.parametrize("name", sorted(_LIFTED_TAIL_STATES))
    def test_lifted_state_present(self, inner, name):
        assert name in inner


class TestEvalJudgeChainSemanticsPreserved:
    """Re-pins the behavioral assertions that used to live in
    test_sf_research_predictor_parallel_wiring.py::TestBranchAContents /
    TestPerBranchErrorIsolation before the I2544 lift."""

    def test_eval_chain_entry_skip_gate(self, inner):
        assert inner["CheckSkipEvalJudge"]["Default"] == "ComputeEvalCadence"
        assert inner["CheckSkipEvalJudge"]["Choices"][0]["Next"] == (
            "CheckSkipRationaleClustering"
        )

    def test_eval_judge_quartet_preserved(self, inner):
        assert inner["EvalJudgePollChoice"]["Type"] == "Choice"
        assert inner["EvalJudgePollWait"]["Type"] == "Wait"
        assert inner["EvalJudgePollWait"]["Next"] == "EvalJudgePoll"
        assert inner["EvalJudgePoll"]["Next"] == "EvalJudgePollDecision"

    def test_eval_chain_fail_soft_catches_preserved_in_advisory_pipeline(self, inner):
        """The eval/agent-justification observability Catches must stay
        fail-soft (route forward within the wrapper branch) — never to
        AdvisoryNormalizeFailureContext/AdvisoryHandleFailure. They were
        never SF-halting pre-lift and must not become so post-lift."""
        for n in (
            "EvalJudgeSubmitWeekly", "EvalJudgeProcess", "EvalRollingMean",
            "RationaleClustering", "ReplayConcordance", "Counterfactual",
        ):
            for c in inner[n].get("Catch", []):
                assert c["Next"] not in (
                    "AdvisoryNormalizeFailureContext", "AdvisoryHandleFailure",
                ), f"{n} observability Catch escaped to the machine-level failure funnel"

    def test_aggregate_costs_now_feeds_report_card(self, inner):
        """alpha-engine-config-I2544: the old branch terminal
        (BranchAComplete) that used to follow AggregateCosts is replaced by
        ReportCard — this child SF has no Parallel join of its own."""
        assert inner["CheckSkipAggregateCosts"]["Choices"][0]["Next"] == "ReportCard"
        assert inner["AggregateCosts"]["Next"] == "ReportCard"
        assert inner["AggregateCosts"]["Catch"][0]["Next"] == "ReportCard"


class TestReportCardAndDirectorWiring:
    def test_report_card_success_feeds_director(self, inner):
        assert inner["ReportCard"]["Next"] == "Director"

    def test_report_card_failure_skips_director(self, inner):
        """Director's own Comment says it 'runs only after a SUCCESSFUL
        ReportCard' — preserved from the parent SF verbatim."""
        assert inner["ReportCard"]["Catch"][0]["Next"] == (
            "PublishReportCardDegraded"
        )
        assert inner["PublishReportCardDegraded"]["Next"] == (
            "AdvisoryNotifyComplete"
        )
        assert inner["PublishReportCardDegraded"]["Catch"][0]["Next"] == (
            "AdvisoryNotifyComplete"
        )

    def test_director_success_and_failure_converge_on_notify(self, inner):
        assert inner["Director"]["Next"] == "AdvisoryNotifyComplete"
        assert inner["Director"]["Catch"][0]["Next"] == "PublishDirectorDegraded"
        assert inner["PublishDirectorDegraded"]["Next"] == "AdvisoryNotifyComplete"
        assert inner["PublishDirectorDegraded"]["Catch"][0]["Next"] == (
            "AdvisoryNotifyComplete"
        )

    def test_report_card_and_director_payload_shape_unchanged(self, inner):
        """Same FunctionName/Payload shape as the (now-removed) main SF
        states — dry_run.$=$.research_dry preflight-aware convention
        preserved."""
        assert inner["ReportCard"]["Parameters"]["FunctionName"] == (
            "alpha-engine-evaluator:live"
        )
        assert inner["ReportCard"]["Parameters"]["Payload"]["dry_run.$"] == (
            "$.research_dry"
        )
        assert inner["Director"]["Parameters"]["FunctionName"] == (
            "alpha-engine-evaluator-director:live"
        )
        assert inner["Director"]["Parameters"]["Payload"]["dry_run.$"] == (
            "$.research_dry"
        )

    def test_report_card_snapshot_flag_true(self, inner):
        """alpha-engine-config-I2556 (persistent report card with weekly
        snapshots): this Saturday-cadence ReportCard invocation is the
        WEEKLY FREEZE — snapshot=true writes the dated historical card.
        The Sunday ModelZoo child SF's re-grade tail passes snapshot=false
        (see test_sf_modelzoo_pipeline_wiring.py)."""
        assert inner["ReportCard"]["Parameters"]["Payload"]["snapshot"] is True


class TestTerminalNotify:
    def test_notify_complete_is_constants_only(self, inner):
        """config#1819 discipline: Subject/Message must be hardcoded
        constants, never States.Format against unbounded input."""
        n = inner["AdvisoryNotifyComplete"]
        assert n["Resource"] == "arn:aws:states:::sns:publish"
        assert "Subject.$" not in n["Parameters"]
        assert "Message.$" not in n["Parameters"]
        assert n["End"] is True
        assert n["Catch"][0]["Next"] == "AdvisoryNotifyCompleteDegraded"

    def test_notify_complete_degraded_records_data(self, inner):
        d = inner["AdvisoryNotifyCompleteDegraded"]
        assert d["Type"] == "Pass"
        assert d["End"] is True
        assert d["Parameters"]["degraded"] is True


class TestNoDanglingTargets:
    def test_inner_no_dangling(self, wrapper):
        names = set(wrapper["Branches"][0]["States"])
        assert wrapper["Branches"][0]["StartAt"] in names
        for n, st in wrapper["Branches"][0]["States"].items():
            for t in _own_targets(st):
                assert t in names, f"dangling: {n} -> {t}"

    def test_top_level_no_dangling(self, states):
        top = set(states)
        for n, st in states.items():
            for t in _own_targets(st):
                assert t in top, f"top-level dangling: {n} -> {t}"
