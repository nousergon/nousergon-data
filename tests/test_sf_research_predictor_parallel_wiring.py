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
             CheckSkipDriftDetection (unchanged downstream)

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
    Parallel's Branches (those are validated in their own state space)."""
    out: list[str] = []

    def rec(o) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                if k == "Branches":
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

    def test_branch_a_starts_at_check_skip_research(self, parallel):
        assert parallel["Branches"][0]["StartAt"] == "CheckSkipResearch"

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

    def test_predictor_success_routes_to_model_zoo_rotation(self, branch_b):
        # L4544: champion-retrain success now flows into the best-effort model-zoo
        # rotation before the branch completes (skip path still → BranchBComplete).
        success = [
            c["Next"]
            for c in branch_b["CheckPredictorStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["ModelZooRotation"]

    def test_model_zoo_rotation_is_best_effort(self, branch_b):
        """L4544: the rotation must NEVER fail Branch B — every terminal path
        (task Catch, poll Catch, any non-success poll Status) converges to
        BranchBComplete, since the champion already trained+promoted.

        Post-defer-email change: failures now route via the
        PublishModelZooFailureImmediate SNS alert FIRST (so a deferred-base-email
        + zoo-failure run is never silent), then on to BranchBComplete. The
        invariant is unchanged: no zoo failure path ever reaches BranchBFailed."""
        zoo = branch_b["ModelZooRotation"]
        # Task-level Catch routes failure to the alert (then BranchBComplete),
        # NEVER to BranchBFailed.
        assert any(
            c["Next"] == "PublishModelZooFailureImmediate" and "States.ALL" in c["ErrorEquals"]
            for c in zoo["Catch"]
        )
        assert all(c["Next"] != "BranchBFailed" for c in zoo["Catch"])
        assert zoo["Next"] == "WaitForModelZoo"
        # Same shared instance as the champion retrain.
        assert zoo["Parameters"]["InstanceIds.$"] == "$.ec2_instance_id"
        # The command invokes the rotation entrypoint, honoring shell-run preflight.
        cmd = zoo["Parameters"]["Parameters"]["commands.$"]
        assert "--model-zoo-weekly" in cmd
        assert "$.preflight_args" in cmd
        # Poll Catch is best-effort; routes via the alert, never to BranchBFailed.
        wait = branch_b["WaitForModelZoo"]
        assert any(
            c["Next"] == "PublishModelZooFailureImmediate" and "States.ALL" in c["ErrorEquals"]
            for c in wait["Catch"]
        )
        assert all(c["Next"] != "BranchBFailed" for c in wait["Catch"])
        check = branch_b["CheckModelZooStatus"]
        # A non-terminal poll waits; Success completes cleanly; everything else
        # (failed/cancelled/timedout) routes to the alert then BranchBComplete.
        assert check["Default"] == "PublishModelZooFailureImmediate"
        nexts = {c["StringEquals"]: c["Next"] for c in check["Choices"]}
        assert nexts["InProgress"] == "ModelZooWait"
        assert nexts["Pending"] == "ModelZooWait"
        assert nexts["Success"] == "BranchBComplete"
        assert branch_b["ModelZooWait"]["Next"] == "WaitForModelZoo"

        # The alert state is itself best-effort: it publishes SNS and then
        # converges to BranchBComplete (both the success path and its own Catch),
        # so an SNS failure cannot fail the branch.
        alert = branch_b["PublishModelZooFailureImmediate"]
        assert alert["Resource"] == "arn:aws:states:::sns:publish"
        assert alert["Next"] == "BranchBComplete"
        assert all(c["Next"] == "BranchBComplete" for c in alert["Catch"])
        assert "PREDICTOR_DEFER_TRAINING_EMAIL" in alert["Parameters"]["Message.$"]

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
        / FailExecution / CheckSkipDriftDetection. Failures are recorded
        as data and the branch SUCCEEDS; the SF is failed AFTER the join.
        A leak here re-introduces cross-branch cancellation."""
        for bi, b in enumerate(parallel["Branches"]):
            names = set(b["States"])
            for n, st in b["States"].items():
                for t in _own_targets(st):
                    assert t not in (
                        "HandleFailure",
                        "FailExecution",
                        "CheckSkipDriftDetection",
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
        # Both OK → continue to the unchanged downstream
        assert c["Default"] == "CheckSkipDriftDetection"

    def test_extract_parallel_branch_error_routes_to_handle_failure(
        self, states
    ):
        e = states["ExtractParallelBranchError"]
        assert e["Type"] == "Pass"
        assert e["ResultPath"] == "$.error"
        assert e["Next"] == "HandleFailure"
        assert e["Parameters"]["phase"] == "ResearchPredictorParallel"

    def test_parallel_catch_is_backstop_to_handle_failure(self, parallel):
        """A Parallel-level Catch must exist as defense-in-depth for a
        genuine SF-engine Parallel error, routing to the EXISTING shared
        HandleFailure (no new error channel)."""
        catches = parallel["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "HandleFailure"
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
    def test_regime_retrospective_eval_routes_into_parallel(self, states):
        assert (
            states["RegimeRetrospectiveEval"]["Next"]
            == "ResearchPredictorParallel"
        )
        assert [
            c["Next"]
            for c in states["RegimeRetrospectiveEval"]["Catch"]
        ] == ["ResearchPredictorParallel"]

    def test_skip_regime_retrospective_eval_routes_into_parallel(
        self, states
    ):
        c = states["CheckSkipRegimeRetrospectiveEval"]
        assert c["Choices"][0]["Next"] == "ResearchPredictorParallel"
        assert c["Default"] == "RegimeRetrospectiveEval"

    def test_drift_to_backtester_chain_unchanged(self, states):
        assert (
            states["CheckSkipDriftDetection"]["Default"] == "DriftDetection"
        )
        assert states["DriftDetection"]["Next"] == "CheckSkipBacktester"
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
                or name.endswith("Wait")
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
