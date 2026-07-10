"""Pins the Research || PredictorTraining SF Parallel restructure.

Origin: 2026-05-16, plan
alpha-engine-docs/private/research-predictor-parallel-260516.md.

Research and PredictorTraining are DATA-INDEPENDENT (no S3/db data flows
between them — CLAUDE.md Architecture). They previously ran sequentially
ONLY to "spread API load", a now-STALE rationale: predictor TRAINING
(alpha-engine-predictor/training/train_handler.py) reads ArcticDB + CPU
LightGBM and makes NO Anthropic calls (the yfinance fallback was removed
by predictor PR #6). Research's only heavy load is Anthropic. They do not
contend on the rate-limited API.

This restructures the sequential
  ... -> Research -> DataPhase2 -> eval-judge chain -> ... ->
      Counterfactual -> PredictorTraining -> DriftDetection -> ...
into an SF Parallel:
  Branch A = CheckSkipResearch -> Research -> DataPhase2 -> eval-judge
             chain -> EvalRollingMean -> RationaleClustering ->
             ReplayConcordance -> Counterfactual
  Branch B = CheckSkipPredictorTraining -> PredictorTraining quartet
  join    -> AggregateBranchOutcomes -> CheckBranchOutcomes ->
             CheckSkipBacktester (config#902: the standalone DriftDetection
             state was collapsed — drift is now bundled onto the
             PredictorTraining spot inside Branch B — so the join routes
             straight to the backtester skip-gate)

CORRECTNESS-CRITICAL: SF Parallel's default semantics cancel sibling
branches when one branch errors. With strict-Research hard-failing and
PredictorTraining being an expensive weight-promoting spot, each branch
must SUCCEED (End:true) and record OK/FAILED as DATA so a Research-branch
hard-fail never aborts/wastes an in-flight (or completed+S3-promoted)
PredictorTraining branch, and vice versa. The SF is failed AFTER the join
(post-aggregation) if either branch recorded FAILED.

This test catches regressions like:
- Someone re-serializes Research -> PredictorTraining.
- Someone moves DataPhase2 / the eval chain out of Branch A.
- Someone moves Backtester before the Parallel join.
- A branch terminal gets End removed / re-points to HandleFailure
  (re-introduces cross-branch cancellation — the whole bug this guards).
- The post-join fail-if-either-FAILED gate is dropped (a failed branch
  silently continues).
- A CheckSkip*/Wait-Check status-poll quartet inside a branch is dropped.
- Dangling Next/Default/Catch target anywhere (top level or in-branch).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"

_BRANCH_A_STATES = {
    # config#885: the Scanner→RAGIngestion→RegimeSubstrate→
    # RegimeRetrospectiveEval chain was relocated FROM top level INTO
    # Branch A's head (Scanner is Branch A's StartAt) so PredictorTraining
    # (Branch B) forks parallel to it directly after DataPhase1.
    "Scanner", "CheckSkipRAGIngestion", "RAGIngestion",
    "WaitForRAGIngestion", "CheckRAGIngestionStatus", "RAGIngestionWait",
    "RAGIngestionRetryGate", "RAGIngestionReissue", "ExtractRAGIngestionError",
    "CheckSkipRegimeSubstrate", "RegimeSubstrate",
    "CheckSkipRegimeRetrospectiveEval", "RegimeRetrospectiveEval",
    "CheckSkipResearch", "Research", "CheckResearchStatus",
    "CheckSkipDataPhase2", "DataPhase2", "CheckSkipEvalJudge",
    "ComputeEvalCadence", "CheckMonthlyCadence",
    "EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly",
    "EvalJudgePollChoice", "EvalJudgePollWait", "EvalJudgePoll",
    "EvalJudgePollDecision", "EvalJudgeProcess", "EvalRollingMean",
    "CheckSkipRationaleClustering", "RationaleClustering",
    "CheckSkipReplayConcordance", "ReplayConcordance",
    "CheckSkipCounterfactual", "Counterfactual", "ExtractResearchError",
    "PublishResearchFailureImmediate",
    "BranchAComplete", "BranchAFailed",
}
_BRANCH_B_STATES = {
    "CheckSkipPredictorTraining", "PredictorTraining",
    "WaitForPredictorTraining", "CheckPredictorStatus", "PredictorWait",
    "ExtractPredictorError", "PublishPredictorFailureImmediate",
    # config#1083 parallel model-zoo fan-out: ResolveZooSpecs -> Map -> Select.
    "ResolveZooSpecs", "WaitResolveZoo", "CheckResolveZooStatus",
    "ResolveZooWait", "ExtractModelZooResolveError", "ParseZooSpecs",
    "ModelZooTrainMap", "ModelZooSelect",
    "WaitForModelZoo", "CheckModelZooStatus", "ModelZooWait",
    "ExtractModelZooSelectError", "PublishModelZooFailureImmediate",
    "BranchBComplete", "BranchBFailed",
}


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


@pytest.fixture(scope="module")
def parallel(states) -> dict:
    return states["ResearchPredictorParallel"]


@pytest.fixture(scope="module")
def branch_a(parallel) -> dict:
    return parallel["Branches"][0]["States"]


@pytest.fixture(scope="module")
def branch_b(parallel) -> dict:
    return parallel["Branches"][1]["States"]


def _own_targets(st: dict) -> list[str]:
    """Next/Default/Catch.Next of THIS state, NOT descending into a
    Parallel's Branches or a Map's ItemProcessor/Iterator (those are
    validated in their own state space)."""
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


class TestJsonParses:
    def test_json_parses(self, sf):
        assert isinstance(sf, dict)
        assert sf["StartAt"] in sf["States"]


class TestParallelStatePresence:
    def test_parallel_state_exists(self, states):
        assert "ResearchPredictorParallel" in states

    def test_parallel_state_type(self, parallel):
        assert parallel["Type"] == "Parallel"

    def test_parallel_has_exactly_two_branches(self, parallel):
        assert len(parallel["Branches"]) == 2

    def test_branch_a_starts_at_scanner(self, parallel):
        # config#885: Branch A now leads with the relocated Scanner chain
        # (Scanner → RAG → RegimeSubstrate → RegimeRetrospectiveEval →
        # CheckSkipResearch → ...). CheckSkipResearch is no longer the
        # StartAt — it is the chain's continuation inside Branch A.
        assert parallel["Branches"][0]["StartAt"] == "Scanner"
        branch_a = parallel["Branches"][0]["States"]
        assert "CheckSkipResearch" in branch_a

    def test_branch_b_starts_at_check_skip_predictor_training(self, parallel):
        assert (
            parallel["Branches"][1]["StartAt"]
            == "CheckSkipPredictorTraining"
        )

    def test_join_target_is_aggregate(self, parallel):
        assert parallel["Next"] == "AggregateBranchOutcomes"

    def test_parallel_result_path_does_not_clobber_input(self, parallel):
        # No InputPath/Parameters → each branch gets full input incl
        # $.ec2_instance_id (Branch B's SSM calls need it). ResultPath
        # writes to a side path so input fields survive the join.
        assert "InputPath" not in parallel
        assert "Parameters" not in parallel
        assert parallel["ResultPath"] == "$.parallel_result"

    def test_moved_states_gone_from_top_level(self, states):
        for n in (_BRANCH_A_STATES | _BRANCH_B_STATES):
            assert n not in states, (
                f"{n} must live INSIDE a Parallel branch, not top level"
            )


class TestResearchAndPredictorAreSiblingBranches:
    """The core decoupling: Research and PredictorTraining must be in
    SIBLING Parallel branches, never serialized."""

    def test_research_in_branch_a(self, branch_a):
        assert "Research" in branch_a

    def test_predictor_training_in_branch_b(self, branch_b):
        assert "PredictorTraining" in branch_b

    def test_research_not_in_branch_b(self, branch_b):
        assert "Research" not in branch_b

    def test_predictor_training_not_in_branch_a(self, branch_a):
        assert "PredictorTraining" not in branch_a

    def test_no_research_to_predictor_serial_edge_anywhere(self, sf):
        """Defensive: no state's Next/Default/Catch may point Research →
        PredictorTraining or chain them sequentially. The old serial edge
        was CheckSkipCounterfactual/Counterfactual → CheckSkipPredictor
        Training; that must now be a branch-local terminal."""
        a = sf["States"]["ResearchPredictorParallel"]["Branches"][0][
            "States"
        ]
        for n in ("Counterfactual", "CheckSkipCounterfactual"):
            assert "CheckSkipPredictorTraining" not in _own_targets(a[n]), (
                f"{n} still routes to CheckSkipPredictorTraining — Research "
                f"and PredictorTraining are re-serialized."
            )


class TestBranchAContents:
    """Everything that consumes Research output stays in Branch A, in
    current order, with skip-gates/quartets intact."""

    @pytest.mark.parametrize(
        "name",
        sorted(_BRANCH_A_STATES - {"BranchAComplete", "BranchAFailed"}),
    )
    def test_branch_a_state_present(self, branch_a, name):
        assert name in branch_a

    def test_data_phase2_after_research_in_branch_a(self, branch_a):
        # Research success → CheckSkipDataPhase2 → DataPhase2
        ok = [
            c["Next"]
            for c in branch_a["CheckResearchStatus"]["Choices"]
            if c.get("StringEquals") == "OK"
        ]
        assert ok == ["CheckSkipDataPhase2"]
        assert branch_a["CheckSkipDataPhase2"]["Default"] == "DataPhase2"

    def test_eval_chain_after_dataphase2_in_branch_a(self, branch_a):
        assert branch_a["DataPhase2"]["Next"] == "CheckSkipEvalJudge"
        assert branch_a["CheckSkipEvalJudge"]["Default"] == "ComputeEvalCadence"

    def test_skip_research_still_routes_into_branch(self, branch_a):
        """skip_research must still bypass to DataPhase2's skip-gate
        (preserved skip-gate semantics) — and stay INSIDE Branch A."""
        c = branch_a["CheckSkipResearch"]["Choices"][0]
        assert c["Next"] == "CheckSkipDataPhase2"
        assert c["Next"] in branch_a

    def test_eval_judge_quartet_preserved(self, branch_a):
        assert branch_a["EvalJudgePollChoice"]["Type"] == "Choice"
        assert branch_a["EvalJudgePollWait"]["Type"] == "Wait"
        assert branch_a["EvalJudgePollWait"]["Next"] == "EvalJudgePoll"
        assert (
            branch_a["EvalJudgePoll"]["Next"] == "EvalJudgePollDecision"
        )


class TestBranchBContents:
    """The PredictorTraining quartet + skip-gate intact."""

    @pytest.mark.parametrize(
        "name",
        sorted(_BRANCH_B_STATES - {"BranchBComplete", "BranchBFailed"}),
    )
    def test_branch_b_state_present(self, branch_b, name):
        assert name in branch_b

    def test_skip_predictor_training_gate_preserved(self, branch_b):
        c = branch_b["CheckSkipPredictorTraining"]["Choices"][0]
        variables = {cond["Variable"] for cond in c["And"]}
        assert variables == {"$.skip_predictor_training"}
        # skip → branch-local completion (NOT the old CheckSkipDrift edge)
        assert c["Next"] == "BranchBComplete"
        assert branch_b["CheckSkipPredictorTraining"]["Default"] == (
            "PredictorTraining"
        )

    def test_predictor_status_poll_quartet_preserved(self, branch_b):
        assert branch_b["PredictorTraining"]["Next"] == (
            "WaitForPredictorTraining"
        )
        assert branch_b["WaitForPredictorTraining"]["Next"] == (
            "CheckPredictorStatus"
        )
        nexts = {
            c["StringEquals"]: c["Next"]
            for c in branch_b["CheckPredictorStatus"]["Choices"]
        }
        assert nexts["InProgress"] == "PredictorWait"
        assert nexts["Pending"] == "PredictorWait"
        assert branch_b["PredictorWait"]["Next"] == (
            "WaitForPredictorTraining"
        )

    def test_predictor_success_routes_to_resolve_zoo_specs(self, branch_b):
        # config#1083: champion-retrain success now flows into the parallel
        # model-zoo fan-out, starting with ResolveZooSpecs (skip path still →
        # BranchBComplete).
        success = [
            c["Next"]
            for c in branch_b["CheckPredictorStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["ResolveZooSpecs"]

    def test_zoo_fanout_pipeline_wiring(self, branch_b):
        """config#1083: ResolveZooSpecs → (poll) → ParseZooSpecs → ModelZooTrainMap
        (Map, per-spec spot) → ModelZooSelect → (poll) → BranchBComplete. Every
        failure path is best-effort (routes via the alert, never BranchBFailed)."""
        # ResolveZooSpecs dispatches list-rotation-specs on the box.
        resolve = branch_b["ResolveZooSpecs"]
        assert resolve["Parameters"]["InstanceIds.$"] == "$.ec2_instance_id"
        rcmd = resolve["Parameters"]["Parameters"]["commands.$"]
        assert "list-rotation-specs" in rcmd
        assert all(c["Next"] != "BranchBFailed" for c in resolve["Catch"])
        assert resolve["Next"] == "WaitResolveZoo"
        # Resolve poll → CheckResolveZooStatus: Success → ParseZooSpecs.
        check_resolve = branch_b["CheckResolveZooStatus"]
        rnexts = {c["StringEquals"]: c["Next"] for c in check_resolve["Choices"]}
        assert rnexts["Success"] == "ParseZooSpecs"
        # Default routes through ExtractModelZooResolveError (mirrors
        # ExtractPredictorError/ExtractResearchError/ExtractRAGIngestionError)
        # — a Choice.Default transition does not populate $.model_zoo_error the
        # way a Task Catch's ResultPath does, and PublishModelZooFailureImmediate's
        # Message calls States.JsonToString($.model_zoo_error); a direct
        # Choice->Task jump on this edge died with States.Runtime, masking the
        # real zoo-resolve failure (observed live 2026-07-10, config#2160 arc).
        assert check_resolve["Default"] == "ExtractModelZooResolveError"
        extract_resolve = branch_b["ExtractModelZooResolveError"]
        assert extract_resolve["Type"] == "Pass"
        assert extract_resolve["ResultPath"] == "$.model_zoo_error"
        assert extract_resolve["Parameters"]["poll.$"] == "$.resolve_zoo_poll"
        assert extract_resolve["Next"] == "PublishModelZooFailureImmediate"
        # ParseZooSpecs lifts the JSON array into $.parsed_zoo.zoo_specs.
        parse = branch_b["ParseZooSpecs"]
        assert parse["Type"] == "Pass"
        assert "StringToJson" in parse["Parameters"]["zoo_specs.$"]
        assert "Catch" not in parse  # a Pass cannot carry a Catch (AWS schema)
        assert parse["Next"] == "ModelZooTrainMap"

    def test_model_zoo_train_map_per_spec_isolation(self, branch_b):
        """THE robustness property: the Map fans out one spot PER spec, and each
        iteration self-terminates as success (recording status as data), so one
        challenger crashing never aborts its siblings."""
        m = branch_b["ModelZooTrainMap"]
        assert m["Type"] == "Map"
        assert m["ItemsPath"] == "$.parsed_zoo.zoo_specs"
        assert isinstance(m["MaxConcurrency"], int) and m["MaxConcurrency"] >= 1
        # Backstop tolerance so a Map-engine error never aborts survivors.
        assert m["ToleratedFailurePercentage"] == 100
        # Each item carries the spec id + shared SSM context.
        assert m["ItemSelector"]["spec_id.$"] == "$$.Map.Item.Value"
        assert m["ItemSelector"]["ec2_instance_id.$"] == "$.ec2_instance_id"
        proc = m["ItemProcessor"]["States"]
        # The dispatch invokes the per-spec spot mode with the item's spec id.
        dcmd = proc["TrainSpecDispatch"]["Parameters"]["Parameters"]["commands.$"]
        assert "--model-zoo-spec" in dcmd
        assert "$.spec_id" in dcmd
        assert "$.preflight_args" in dcmd
        # PER-ITERATION ISOLATION: both terminals are End:true Pass states
        # recording status as DATA — the iteration NEVER throws.
        for term in ("TrainSpecOK", "TrainSpecFailed"):
            assert proc[term]["Type"] == "Pass"
            assert proc[term]["End"] is True
        # A failed/cancelled/timed-out spec routes to TrainSpecFailed (data),
        # NOT a throw — siblings proceed.
        cts = proc["CheckTrainSpecStatus"]
        assert cts["Default"] == "TrainSpecFailed"
        # The dispatch + poll Catches record failure as data (TrainSpecFailed),
        # never throwing out of the iteration.
        assert all(c["Next"] == "TrainSpecFailed" for c in proc["TrainSpecDispatch"]["Catch"])
        assert all(c["Next"] == "TrainSpecFailed" for c in proc["WaitTrainSpec"]["Catch"])
        # The Map state's own Catch is a best-effort backstop, never BranchBFailed.
        assert all(c["Next"] != "BranchBFailed" for c in m["Catch"])
        assert m["Next"] == "ModelZooSelect"

    def test_model_zoo_select_is_best_effort(self, branch_b):
        """config#1083: ModelZooSelect runs the selection on ONE spot after the
        Map joins; every failure path converges to BranchBComplete via the alert,
        never BranchBFailed (the champion already trained+promoted)."""
        sel = branch_b["ModelZooSelect"]
        assert sel["Parameters"]["InstanceIds.$"] == "$.ec2_instance_id"
        scmd = sel["Parameters"]["Parameters"]["commands.$"]
        assert "--model-zoo-select" in scmd
        assert "$.preflight_args" in scmd
        assert any(
            c["Next"] == "PublishModelZooFailureImmediate" and "States.ALL" in c["ErrorEquals"]
            for c in sel["Catch"]
        )
        assert all(c["Next"] != "BranchBFailed" for c in sel["Catch"])
        assert sel["Next"] == "WaitForModelZoo"
        # Select poll Catch is best-effort; routes via the alert, never BranchBFailed.
        wait = branch_b["WaitForModelZoo"]
        assert all(c["Next"] != "BranchBFailed" for c in wait["Catch"])
        check = branch_b["CheckModelZooStatus"]
        # Default routes through ExtractModelZooSelectError — same rationale
        # as ExtractModelZooResolveError above: CheckModelZooStatus.Default
        # does not populate $.model_zoo_error, and a direct jump to
        # PublishModelZooFailureImmediate died with States.Runtime (observed
        # live 2026-07-10, config#2160 arc).
        assert check["Default"] == "ExtractModelZooSelectError"
        extract_select = branch_b["ExtractModelZooSelectError"]
        assert extract_select["Type"] == "Pass"
        assert extract_select["ResultPath"] == "$.model_zoo_error"
        assert extract_select["Parameters"]["poll.$"] == "$.model_zoo_poll"
        assert extract_select["Next"] == "PublishModelZooFailureImmediate"
        nexts = {c["StringEquals"]: c["Next"] for c in check["Choices"]}
        assert nexts["InProgress"] == "ModelZooWait"
        assert nexts["Pending"] == "ModelZooWait"
        assert nexts["Success"] == "BranchBComplete"
        assert branch_b["ModelZooWait"]["Next"] == "WaitForModelZoo"
        # The alert state is itself best-effort.
        alert = branch_b["PublishModelZooFailureImmediate"]
        assert alert["Resource"] == "arn:aws:states:::sns:publish"
        assert alert["Next"] == "BranchBComplete"
        assert all(c["Next"] == "BranchBComplete" for c in alert["Catch"])
        assert "PREDICTOR_DEFER_TRAINING_EMAIL" in alert["Parameters"]["Message.$"]

    def test_model_zoo_map_iterator_no_dangling(self, branch_b):
        """The Map's iterator namespace is self-consistent (all Next/Default/Catch
        targets resolve within the iterator's own States)."""
        proc = branch_b["ModelZooTrainMap"]["ItemProcessor"]
        names = set(proc["States"])
        assert proc["StartAt"] in names
        for n, st in proc["States"].items():
            for t in _own_targets(st):
                assert t in names, f"Map iterator dangling: {n} -> {t}"

    def test_branch_b_ssm_can_resolve_instance_id(self, branch_b):
        """Branch B's SSM calls reference $.ec2_instance_id — which is
        only present because the Parallel state does NOT scope branch
        input via InputPath/Parameters (asserted separately)."""
        assert (
            branch_b["PredictorTraining"]["Parameters"]["InstanceIds.$"]
            == "$.ec2_instance_id"
        )
        assert (
            branch_b["WaitForPredictorTraining"]["Parameters"][
                "InstanceId.$"
            ]
            == "$.ec2_instance_id[0]"
        )


class TestPerBranchErrorIsolation:
    """THE correctness-critical guard. A branch must NEVER throw — it must
    end as success (End:true) recording OK/FAILED as data, so SF
    Parallel's cancel-all-siblings-on-error behaviour can never abandon a
    running or completed+promoted sibling."""

    def test_branch_a_terminals_end_true(self, branch_a):
        for t in ("BranchAComplete", "BranchAFailed"):
            assert branch_a[t]["Type"] == "Pass"
            assert branch_a[t]["End"] is True

    def test_branch_b_terminals_end_true(self, branch_b):
        for t in ("BranchBComplete", "BranchBFailed"):
            assert branch_b[t]["Type"] == "Pass"
            assert branch_b[t]["End"] is True

    def test_branch_a_records_status(self, branch_a):
        ok = branch_a["BranchAComplete"]
        assert ok["Result"]["branch_a_status"] == "OK"
        assert ok["ResultPath"] == "$.branch_a"
        bad = branch_a["BranchAFailed"]
        assert bad["Parameters"]["branch_a_status"] == "FAILED"
        assert bad["Parameters"]["branch_a_error.$"] == "$.error"
        assert bad["ResultPath"] == "$.branch_a"

    def test_branch_b_records_status(self, branch_b):
        ok = branch_b["BranchBComplete"]
        assert ok["Result"]["branch_b_status"] == "OK"
        assert ok["ResultPath"] == "$.branch_b"
        bad = branch_b["BranchBFailed"]
        assert bad["Parameters"]["branch_b_status"] == "FAILED"
        assert bad["Parameters"]["branch_b_error.$"] == "$.error"

    def test_no_branch_state_routes_to_top_level_handle_failure(
        self, parallel
    ):
        """The whole point: NO in-branch state may route to HandleFailure
        / FailExecution / CheckSkipBacktester. Failures are recorded
        as data and the branch SUCCEEDS; the SF is failed AFTER the join.
        A leak here re-introduces cross-branch cancellation. (config#902:
        the post-join continue target is now CheckSkipBacktester, since the
        standalone DriftDetection state + its CheckSkipDriftDetection gate
        were collapsed when drift was bundled onto the PredictorTraining
        spot.)"""
        for bi, b in enumerate(parallel["Branches"]):
            names = set(b["States"])
            for n, st in b["States"].items():
                for t in _own_targets(st):
                    assert t not in (
                        "HandleFailure",
                        "FailExecution",
                        "CheckSkipBacktester",
                    ), (
                        f"Branch{bi} {n} -> {t}: an in-branch state escapes "
                        f"to a top-level halt/continue target — this "
                        f"re-introduces SF Parallel cross-branch "
                        f"cancellation (the exact bug this guards)."
                    )
                    assert t in names, (
                        f"Branch{bi} {n} -> {t} dangles within the branch"
                    )

    def test_research_hardfail_routes_to_branch_a_failed(self, branch_a):
        """strict-Research hard-fail (Task Catch) + the soft-fail status
        path (ExtractResearchError → PublishResearchFailureImmediate) must
        record FAILED, not halt the SF. The PublishResearchFailureImmediate
        intermediate is the fast-SNS-alert state added 2026-05-24; both
        success and failure paths through it terminate at BranchAFailed."""
        catch_targets = [
            c["Next"] for c in branch_a["Research"]["Catch"]
        ]
        assert catch_targets == ["BranchAFailed"]
        # ExtractError → PublishImmediate → BranchAFailed
        assert (
            branch_a["ExtractResearchError"]["Next"]
            == "PublishResearchFailureImmediate"
        )
        publish = branch_a["PublishResearchFailureImmediate"]
        assert publish["Type"] == "Task"
        assert publish["Resource"] == "arn:aws:states:::sns:publish"
        assert publish["Next"] == "BranchAFailed"
        # SNS-publish-fails escape hatch also lands at BranchAFailed
        for c in publish.get("Catch", []):
            assert c["Next"] == "BranchAFailed"
        # CheckResearchStatus non-OK/SKIPPED → ExtractResearchError
        assert (
            branch_a["CheckResearchStatus"]["Default"]
            == "ExtractResearchError"
        )

    def test_dataphase2_failure_routes_to_branch_a_failed(self, branch_a):
        assert [c["Next"] for c in branch_a["DataPhase2"]["Catch"]] == [
            "BranchAFailed"
        ]

    def test_predictor_failure_routes_to_branch_b_failed(self, branch_b):
        """PredictorTraining failures (Task Catch + WaitForPredictorTraining
        Catch + CheckPredictorStatus default) route through
        ExtractPredictorError → PublishPredictorFailureImmediate (fast SNS
        alert added 2026-05-24) → BranchBFailed. Salvage semantics
        preserved: SF still fails at the join via CheckBranchOutcomes."""
        assert [
            c["Next"] for c in branch_b["PredictorTraining"]["Catch"]
        ] == ["BranchBFailed"]
        assert [
            c["Next"]
            for c in branch_b["WaitForPredictorTraining"]["Catch"]
        ] == ["BranchBFailed"]
        assert (
            branch_b["ExtractPredictorError"]["Next"]
            == "PublishPredictorFailureImmediate"
        )
        publish = branch_b["PublishPredictorFailureImmediate"]
        assert publish["Type"] == "Task"
        assert publish["Resource"] == "arn:aws:states:::sns:publish"
        assert publish["Next"] == "BranchBFailed"
        for c in publish.get("Catch", []):
            assert c["Next"] == "BranchBFailed"
        assert (
            branch_b["CheckPredictorStatus"]["Default"]
            == "ExtractPredictorError"
        )

    def test_eval_chain_fail_soft_catches_preserved(self, branch_a):
        """The eval/agent-justification observability Catches must stay
        fail-soft (route forward within the branch), NOT to BranchAFailed
        — they were never SF-halting and must not become so."""
        for n in (
            "EvalJudgeSubmitWeekly",
            "EvalJudgeProcess",
            "EvalRollingMean",
            "RationaleClustering",
            "ReplayConcordance",
            "Counterfactual",
        ):
            for c in branch_a[n].get("Catch", []):
                assert c["Next"] != "BranchAFailed", (
                    f"{n} observability Catch became a hard branch fail — "
                    f"it must stay fail-soft (forward within Branch A)."
                )
                assert c["Next"] != "HandleFailure"


class TestPostJoinAggregationAndFailure:
    """The SF must be failed AFTER the join if EITHER branch recorded
    FAILED — so the other branch's completed work (incl. an already
    S3-promoted PredictorTraining) persists and the recovery skip-set can
    skip whichever branch genuinely completed."""

    def test_aggregate_state_present(self, states):
        a = states["AggregateBranchOutcomes"]
        assert a["Type"] == "Pass"
        assert a["Next"] == "CheckBranchOutcomes"
        # Hoists both branch statuses out of the 2-element parallel array
        p = a["Parameters"]
        assert (
            p["branch_a_status.$"]
            == "$.parallel_result[0].branch_a.branch_a_status"
        )
        assert (
            p["branch_b_status.$"]
            == "$.parallel_result[1].branch_b.branch_b_status"
        )

    def test_check_branch_outcomes_fails_if_either_failed(self, states):
        c = states["CheckBranchOutcomes"]
        assert c["Type"] == "Choice"
        # An Or over both branch statuses == FAILED → error path
        choice = c["Choices"][0]
        or_vars = {
            cond["Variable"] for cond in choice["Or"]
        }
        assert or_vars == {
            "$.branch_outcomes.branch_a_status",
            "$.branch_outcomes.branch_b_status",
        }
        for cond in choice["Or"]:
            assert cond["StringEquals"] == "FAILED"
        assert choice["Next"] == "ExtractParallelBranchError"
        # Both OK → continue downstream. config#902 collapsed the standalone
        # DriftDetection state (drift is now bundled onto the PredictorTraining
        # spot inside Branch B), so the join routes straight to the backtester
        # skip-gate.
        assert c["Default"] == "CheckSkipBacktester"

    def test_extract_parallel_branch_error_routes_to_handle_failure(
        self, states
    ):
        # config#1819: routes through NormalizeFailureContext, not
        # HandleFailure directly (was HandleFailure pre-fix).
        e = states["ExtractParallelBranchError"]
        assert e["Type"] == "Pass"
        assert e["ResultPath"] == "$.error"
        assert e["Next"] == "NormalizeFailureContext"
        assert e["Parameters"]["phase"] == "ResearchPredictorParallel"

    def test_parallel_catch_is_backstop_to_handle_failure(self, parallel):
        """A Parallel-level Catch must exist as defense-in-depth for a
        genuine SF-engine Parallel error, routing to the EXISTING shared
        HandleFailure via NormalizeFailureContext (config#1819: the single
        chokepoint in front of HandleFailure) — no new error channel."""
        catches = parallel["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "NormalizeFailureContext"
            and c["ResultPath"] == "$.error"
            for c in catches
        )

    def test_parallel_retry_is_noop(self, parallel):
        """MaxAttempts:0 — a completed PredictorTraining must never be
        re-run by an accidental default Parallel retry."""
        retry = parallel["Retry"]
        assert any(
            r["ErrorEquals"] == ["States.ALL"] and r["MaxAttempts"] == 0
            for r in retry
        )


class TestInboundRewireAndDownstreamUnchanged:
    def test_data_phase1_forks_into_parallel(self, states):
        """config#885: DataPhase1 now routes DIRECTLY into the Parallel
        (both the skip path and the poll-Success path), so PredictorTraining
        (Branch B) forks parallel to the relocated Scanner chain (Branch A
        head). This is the whole point of the change — Predictor's ~91 min
        overlaps Scanner+RAG+Research instead of stacking after it."""
        assert any(
            c["Next"] == "ResearchPredictorParallel"
            for c in states["CheckSkipDataPhase1"]["Choices"]
        )
        success = next(
            c for c in states["CheckDataPhase1Status"]["Choices"]
            if c.get("StringEquals") == "Success"
        )
        assert success["Next"] == "ResearchPredictorParallel"

    def test_relocated_chain_threads_through_branch_a(self, branch_a):
        """The relocated Scanner chain's terminal RegimeRetrospectiveEval
        (and its skip-gate + non-blocking Catch) continue to
        CheckSkipResearch IN-BRANCH — never the parent Parallel (invalid
        branch→parent) nor top-level HandleFailure (cross-branch cancel)."""
        assert branch_a["Scanner"]["Next"] == "CheckSkipRAGIngestion"
        assert (
            branch_a["RegimeRetrospectiveEval"]["Next"] == "CheckSkipResearch"
        )
        assert [
            c["Next"]
            for c in branch_a["RegimeRetrospectiveEval"]["Catch"]
        ] == ["CheckSkipResearch"]
        c = branch_a["CheckSkipRegimeRetrospectiveEval"]
        assert c["Choices"][0]["Next"] == "CheckSkipResearch"
        assert c["Default"] == "RegimeRetrospectiveEval"

    def test_relocated_chain_gone_from_top_level(self, states):
        for n in (
            "Scanner", "RAGIngestion", "RegimeSubstrate",
            "RegimeRetrospectiveEval", "CheckSkipRegimeRetrospectiveEval",
        ):
            assert n not in states, (
                f"{n} must live inside Branch A, not top level (config#885)."
            )

    def test_relocated_rag_error_edges_route_to_branch_fail_path(
        self, branch_a
    ):
        """The relocated RAGIngestion error edges that USED to hit the
        top-level HandleFailure must now route to the branch-fail path
        (PublishResearchFailureImmediate → BranchAFailed), mirroring
        ExtractResearchError — a branch state pointing at the non-branch
        HandleFailure is an invalid ASL transition AND would re-introduce
        cross-branch cancellation."""
        assert [c["Next"] for c in branch_a["RAGIngestion"]["Catch"]] == [
            "PublishResearchFailureImmediate"
        ]
        assert [
            c["Next"] for c in branch_a["WaitForRAGIngestion"]["Catch"]
        ] == ["PublishResearchFailureImmediate"]
        assert (
            branch_a["ExtractRAGIngestionError"]["Next"]
            == "PublishResearchFailureImmediate"
        )
        assert (
            branch_a["PublishResearchFailureImmediate"]["Next"]
            == "BranchAFailed"
        )

    def test_drift_state_collapsed_join_routes_to_backtester(self, states):
        """config#902: the standalone DriftDetection state (and its
        CheckSkipDriftDetection skip-gate) were collapsed — drift is now
        bundled onto the PredictorTraining spot (crucible-predictor
        spot_train.sh), running non-blocking after training succeeds. So the
        parallel join routes DIRECTLY to CheckSkipBacktester and neither drift
        state remains."""
        assert "DriftDetection" not in states
        assert "CheckSkipDriftDetection" not in states
        assert states["CheckBranchOutcomes"]["Default"] == "CheckSkipBacktester"
        assert states["CheckSkipBacktester"]["Default"] == "Backtester"

    def test_backtester_after_parallel_join_and_reachable(self, sf):
        """Walk the top-level happy path (Parallel as a single node);
        Backtester must be visited strictly AFTER the Parallel join — it
        needs BOTH Research signal history and PredictorTraining
        weights."""
        states = sf["States"]

        def is_sink(name) -> bool:
            return (
                name is None
                or name.startswith("Extract")
                or name.startswith("NormalizeFailureContext")
                or name.endswith("Wait")
                or name.endswith("RetryGate")
                or name.endswith("Reissue")
                or name in ("HandleFailure", "FailExecution")
            )

        order: list[str] = []
        seen: set[str] = set()
        cur = sf["StartAt"]
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            order.append(cur)
            st = states[cur]
            if st.get("Type") == "Choice":
                df = st.get("Default")
                if not is_sink(df):
                    cur = df
                else:
                    fw = [
                        c["Next"]
                        for c in st.get("Choices", [])
                        if not is_sink(c.get("Next"))
                    ]
                    cur = fw[0] if fw else df
            else:
                cur = st.get("Next")
            if cur == "Backtester":
                order.append(cur)
                break
        assert "ResearchPredictorParallel" in order, order
        assert "Backtester" in order, order
        assert order.index("ResearchPredictorParallel") < order.index(
            "Backtester"
        ), (
            "Backtester must run AFTER the Parallel join — it depends on "
            "BOTH branches (Research signal history + Predictor weights)."
        )
        # The post-join aggregation gate must be on the happy path too.
        assert "AggregateBranchOutcomes" in order
        assert "CheckBranchOutcomes" in order
        assert order.index("CheckBranchOutcomes") < order.index(
            "Backtester"
        )


class TestNoDanglingTargetsAnywhere:
    def test_top_level_no_dangling(self, states):
        top = set(states)
        for n, st in states.items():
            for t in _own_targets(st):
                assert t in top, f"top-level dangling: {n} -> {t}"

    def test_in_branch_no_dangling(self, parallel):
        for bi, b in enumerate(parallel["Branches"]):
            names = set(b["States"])
            assert b["StartAt"] in names
            for n, st in b["States"].items():
                for t in _own_targets(st):
                    assert t in names, (
                        f"Branch{bi} dangling: {n} -> {t}"
                    )

    def test_exactly_one_end_terminal_class_per_branch(self, parallel):
        for bi, b in enumerate(parallel["Branches"]):
            ends = {
                k for k, v in b["States"].items() if v.get("End") is True
            }
            # Each branch has exactly its 2 Complete/Failed terminals.
            assert len(ends) == 2, (bi, ends)
