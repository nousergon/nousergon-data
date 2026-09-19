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
  Branch A (as of the 2026-05-16 origin) = CheckSkipResearch -> Research ->
             DataPhase2 -> eval-judge chain -> EvalRollingMean ->
             RationaleClustering -> Counterfactual
             (ReplayConcordance sat between those two until
             alpha-engine-config-I10539 retired it, 2026-09-16)
  Branch B = CheckSkipPredictorTraining -> PredictorTraining quartet
  join    -> AggregateBranchOutcomes -> CheckBranchOutcomes ->
             CheckSkipBacktester (config#902: the standalone DriftDetection
             state was collapsed — drift is now bundled onto the
             PredictorTraining spot inside Branch B — so the join routes
             straight to the backtester skip-gate)

UPDATE (alpha-engine-config-I2515 Phase B): the multi-agent Research graph
runner (CheckSkipResearch/Research/CheckResearchStatus) was REMOVED from
Branch A. Current Branch A head: Scanner -> CheckSkipRegimeSubstrate ->
RegimeSubstrate -> SignalsEnvelope (new load-bearing signals.json
producer) -> ChallengerShadow (new, non-blocking) -> CheckSkipRAGIngestion
-> RAG chain ->
CheckSkipRegimeRetrospectiveEval -> RegimeRetrospectiveEval ->
CheckSkipDataPhase2 -> DataPhase2 -> eval-judge chain -> ... ->
Counterfactual. The Parallel/Branch-B/join structure and the
sibling-branch decoupling invariant this file pins are unchanged.

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
    #
    # alpha-engine-config-I2515 Phase B: the multi-agent Research graph
    # runner (CheckSkipResearch/Research/CheckResearchStatus) was REMOVED.
    # RegimeSubstrate now runs BEFORE the RAG chain (moved ahead of RAG/
    # ThinkTank), followed by the new SignalsEnvelope (thin envelope
    # producer replacing Research as the signals.json producer) and
    # ChallengerShadow (keeps the no_agent champion-baseline shadow alive).
    # ExtractResearchError was renamed ExtractSignalsEnvelopeError.
    #
    # 2026-08-10 (Brian ruling): the ThinkTankCoverage chain
    # (CheckSkipThinkTankCoverage / ThinkTankCoverage / CheckThinkTankLaunched
    # / InitThinkTankPollCount / WaitForThinkTank / CheckThinkTankStatus /
    # ThinkTankWait / ThinkTankPollWait / MergeThinkTankPollCount /
    # ThinkTankDegraded) was REMOVED from this branch. The Think Tank runs
    # daily in shadow mode on its own EventBridge cadence
    # (alpha-research-thinktank-daily -> alpha-engine-thinktank-spot-
    # dispatcher) and is not part of the weekly pipeline; the RAG chain now
    # lands directly on CheckSkipRegimeRetrospectiveEval.
    "Scanner", "CheckSkipRegimeSubstrate", "RegimeSubstrate",
    "SignalsEnvelope", "ChallengerShadow",
    "CheckSkipRAGIngestion", "RAGIngestion",
    "WaitForRAGIngestion", "CheckRAGIngestionStatus", "RAGIngestionWait",
    "RAGIngestionRetryGate", "RAGIngestionReissue", "ExtractRAGIngestionError",
    "CheckSkipRegimeRetrospectiveEval", "RegimeRetrospectiveEval",
    "CheckSkipDataPhase2", "DataPhase2", "CheckSkipEvalJudge",
    "ComputeEvalCadence", "CheckMonthlyCadence",
    "EvalJudgeSubmitFirstSaturday", "EvalJudgeSubmitWeekly",
    # alpha-engine-config-I9329: the four EvalJudgePoll* states were deleted
    # with the provider batch API they existed to drive, and EvalJudgeProcess
    # moved from lambda:invoke onto an ssm:sendCommand against a dedicated
    # spot box. The dispatch + two poll loops below are what replaced them.
    "EvalJudgeSubmitOutcome", "EvalJudgeEmptyPlan",
    "PrepareEvalJudgeSpotDispatch", "DispatchEvalJudgeSpot",
    "MergeEvalJudgeSpotInstanceId", "InitEvalJudgeSpotBootstrapPollCount",
    "WaitForEvalJudgeSpotBootstrap", "CheckEvalJudgeSpotBootstrapStatus",
    "EvalJudgeSpotBootstrapWait", "EvalJudgeSpotBootstrapPollWait",
    "MergeEvalJudgeSpotBootstrapPollCount", "EvalJudgeSpotBootstrapLivenessGate",
    "ExtractEvalJudgeSpotBootstrapError", "EvalJudgeSpotRelaunch",
    "EvalJudgeProcess", "InitEvalJudgeProcessPollCount",
    "WaitForEvalJudgeProcess", "CheckEvalJudgeProcessStatus",
    "EvalJudgeProcessWait", "EvalJudgeProcessPollWait",
    "MergeEvalJudgeProcessPollCount", "EvalJudgeProcessLivenessGate",
    "ExtractEvalJudgeProcessError", "EvalRollingMean",
    "CheckSkipRationaleClustering", "RationaleClustering",
    # alpha-engine-config-I10539: CheckSkipReplayConcordance +
    # ReplayConcordance + MarkReplayConcordanceDegraded are retired.
    "CheckSkipCounterfactual", "Counterfactual", "ExtractSignalsEnvelopeError",
    "PublishResearchFailureImmediate",
    "BranchAComplete", "BranchAFailed",
}
_BRANCH_B_STATES = {
    "CheckSkipPredictorTraining", "PredictorTraining",
    "WaitForPredictorTraining", "CheckPredictorStatus", "PredictorWait",
    "ExtractPredictorError", "PublishPredictorFailureImmediate",
    # config#1083 parallel model-zoo fan-out: ResolveZooSpecs -> Map -> Select.
    "ResolveZooSpecs", "WaitResolveZoo", "CheckResolveZooStatus",
    "ResolveZooWait", "ModelZooResolveLivenessGate", "ExtractModelZooResolveError",
    "ExtractModelZooResolveSubstrateLostError", "ParseZooSpecs",
    "ModelZooTrainMap", "ModelZooSelect",
    "WaitForModelZoo", "CheckModelZooStatus", "ModelZooWait",
    "ModelZooSelectLivenessGate", "ExtractModelZooSelectError",
    "ExtractModelZooSelectSubstrateLostError", "PredictorTrainingLivenessGate",
    "PublishModelZooFailureImmediate",
    # config#2253 validated skip path: skip_predictor_training=true routes
    # through a manifest-freshness HeadObject before the branch may read
    # as succeeded (backtest-eval preset bypasses validation by design).
    "ValidatePredictorSkipWeightsFresh", "CheckPredictorSkipWeightsFresh",
    "PredictorSkipWeightsStale", "PredictorTrainingSkipped",
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
        # config#885: Branch A now leads with the relocated Scanner chain.
        # alpha-engine-config-I2515 Phase B: the chain is now Scanner →
        # RegimeSubstrate → SignalsEnvelope → ChallengerShadow → RAG →
        # RegimeRetrospectiveEval → DataPhase2 → ...
        # (the multi-agent Research state and CheckSkipResearch were
        # removed; SignalsEnvelope is the chain's continuation inside
        # Branch A). config#3134: Branch A's StartAt was CheckSkipScanner
        # (Scanner's own new skip gate), not Scanner directly.
        # alpha-engine-config#6722: now InitResearchDegradedFlag (seeds
        # $.research_degraded_local before anything else runs), which
        # falls through to CheckSkipScanner unconditionally.
        assert parallel["Branches"][0]["StartAt"] == "InitResearchDegradedFlag"
        branch_a = parallel["Branches"][0]["States"]
        assert branch_a["InitResearchDegradedFlag"]["Next"] == "CheckSkipScanner"
        assert "SignalsEnvelope" in branch_a

    def test_branch_b_starts_at_check_skip_predictor_training(self, parallel):
        # alpha-engine-config#6722: now InitPredictorDegradedFlag (seeds
        # $.research_degraded_local), falling through unconditionally.
        assert (
            parallel["Branches"][1]["StartAt"]
            == "InitPredictorDegradedFlag"
        )
        branch_b = parallel["Branches"][1]["States"]
        assert (
            branch_b["InitPredictorDegradedFlag"]["Next"]
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
    """The core decoupling: the research module's Branch A work and
    PredictorTraining must be in SIBLING Parallel branches, never
    serialized. alpha-engine-config-I2515 Phase B removed the multi-agent
    Research state entirely; SignalsEnvelope is its load-bearing successor
    and stands in as the proof-point here."""

    def test_signals_envelope_in_branch_a(self, branch_a):
        assert "SignalsEnvelope" in branch_a

    def test_predictor_training_in_branch_b(self, branch_b):
        assert "PredictorTraining" in branch_b

    def test_signals_envelope_not_in_branch_b(self, branch_b):
        assert "SignalsEnvelope" not in branch_b

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

    def test_data_phase2_after_regime_retrospective_eval_in_branch_a(self, branch_a):
        # alpha-engine-config-I2515 Phase B: the removed multi-agent
        # Research state (and CheckResearchStatus) used to sit between the
        # regime chain and DataPhase2. RegimeRetrospectiveEval's success
        # path now goes straight to CheckSkipDataPhase2 → DataPhase2.
        assert branch_a["RegimeRetrospectiveEval"]["Next"] == "CheckSkipDataPhase2"
        assert branch_a["CheckSkipDataPhase2"]["Default"] == "DataPhase2"

    def test_eval_chain_after_dataphase2_in_branch_a(self, branch_a):
        # alpha-engine-config-I5759: DataPhase2 dispatches to spot, so its
        # success edge is the Success arm of CheckDataPhase2Status rather
        # than DataPhase2.Next. Same shape as RAGIngestion above it.
        assert branch_a["DataPhase2"]["Next"] == "InitDataPhase2PollCount"
        success_arm = [
            c for c in branch_a["CheckDataPhase2Status"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert len(success_arm) == 1
        assert success_arm[0]["Next"] == "CheckSkipEvalJudge"
        assert branch_a["CheckSkipEvalJudge"]["Default"] == "ComputeEvalCadence"

    def test_data_phase2_poll_loop_is_bounded(self, branch_a):
        """alpha-engine-config-I5687: a poll loop added AFTER that finding
        ships with its budget, and budget exhaustion must not converge on the
        success path — an exhausted bound that reaches CheckSkipEvalJudge
        would render a timed-out collection as a completed one."""
        assert branch_a["InitDataPhase2PollCount"]["Result"] == 0
        assert branch_a["InitDataPhase2PollCount"]["ResultPath"] == "$.data_phase2_polls"
        bound = [
            c for c in branch_a["CheckDataPhase2Status"]["Choices"]
            if "And" in c
        ]
        assert len(bound) == 1, "the bounded in-progress arm is missing"
        caps = [
            cond["NumericLessThan"] for cond in bound[0]["And"]
            if "NumericLessThan" in cond
        ]
        assert caps == [216], f"poll bound changed to {caps} without updating this test"
        # The counter must actually advance, or the bound is decorative.
        assert (
            branch_a["DataPhase2Wait"]["Parameters"]["polls.$"]
            == "States.MathAdd($.data_phase2_polls, 1)"
        )
        assert branch_a["MergeDataPhase2PollCount"]["ResultPath"] == "$.data_phase2_polls"
        # Exhaustion falls to the retry gate, never to the judge chain.
        assert branch_a["CheckDataPhase2Status"]["Default"] == "DataPhase2RetryGate"
        # Position-independent: config#5688 inserted the substrate-loss branch
        # ahead of the exhaustion arm (a dead instance must be caught before
        # the re-issue decision), so what matters is that the exhaustion route
        # is still REACHABLE from this gate, not that it is Choices[0].
        assert "SetDataPhase2ExhaustedError" in [
            c["Next"] for c in branch_a["DataPhase2RetryGate"]["Choices"]
        ]
        # alpha-engine-config-I7048 (2026-08-12): targets the defensive
        # $.error-normalizer chokepoint ahead of PublishResearchFailureImmediate.
        assert branch_a["SetDataPhase2ExhaustedError"]["Next"] == (
            "NormalizeBranchAFailureContext"
        )
        assert branch_a["SetDataPhase2ExhaustedError"]["ResultPath"] == "$.error"
        assert branch_a["SetDataPhase2ExhaustedError"]["Parameters"]["phase"] == "DataPhase2"

    def test_the_eval_judge_poll_chain_is_gone(self, branch_a):
        """alpha-engine-config-I9329, verified RED against pre-fix code.

        Submit -> Poll -> Process existed to drive an ASYNCHRONOUS provider
        batch API. That API is retired (alpha-engine-config-I9263), and with no
        batch rung there is nothing to poll: poll_batch returned terminal
        immediately for both synthetic id prefixes, so the states could only
        ever fall straight through — a reader's trap.
        """
        stale = sorted(n for n in branch_a if n.startswith("EvalJudgePoll"))
        assert not stale, f"EvalJudgePoll* states still present: {stale}"

    def test_eval_judge_process_is_a_send_command_stage_on_its_own_box(
        self, branch_a
    ):
        """The stage KEEPS its name and changes its substrate. The name is
        load-bearing: eval_artifact_latest.produced_by names EvalJudgeProcess,
        AggregateCosts.required_producers keys on it, and the stage-coverage
        registry has a row for it."""
        st = branch_a["EvalJudgeProcess"]
        assert st["Resource"] == "arn:aws:states:::aws-sdk:ssm:sendCommand"
        # Its OWN box, never the shared weekly launcher: a judge that filled
        # that disk or OOM'd would take down every other stage addressing
        # $.ec2_instance_id.
        assert st["Parameters"]["InstanceIds.$"] == "$.eval_judge_instance_id"
        assert "$.ec2_instance_id" not in json.dumps(st)
        # executionTimeout STRICTLY below TimeoutSeconds — the inverse of the
        # lambda:invoke rule (alpha-engine-config-I6948). Inverted, the state
        # abandons a command SF cannot cancel and the spot keeps billing.
        assert int(st["Parameters"]["Parameters"]["executionTimeout"][0]) < st[
            "TimeoutSeconds"
        ]


class TestBranchBContents:
    """The PredictorTraining quartet + skip-gate intact."""

    @pytest.mark.parametrize(
        "name",
        sorted(_BRANCH_B_STATES - {"BranchBComplete", "BranchBFailed"}),
    )
    def test_branch_b_state_present(self, branch_b, name):
        assert name in branch_b

    def test_skip_predictor_training_gate_preserved(self, branch_b):
        """config#2253: the skip gate is now a TWO-rule Choice. Rule order
        is load-bearing (ASL evaluates in order, first match wins):
        rule 0 = backtest-eval preset (skip AND mode) → straight to the
        skip terminal, NO freshness validation (the config#830 replay
        preset's contract is 'existing artifacts, whatever their vintage');
        rule 1 = plain skip → the manifest-freshness validation task.
        Absent flag still defaults to PredictorTraining (scheduled runs
        unaffected)."""
        gate = branch_b["CheckSkipPredictorTraining"]
        assert len(gate["Choices"]) == 2
        preset_rule, plain_rule = gate["Choices"]
        # Rule 0: skip + mode=backtest-eval — must come FIRST or the plain
        # skip rule would shadow it and mid-week replays would hard-fail
        # on a stale manifest.
        preset_vars = {cond["Variable"] for cond in preset_rule["And"]}
        assert preset_vars == {"$.skip_predictor_training", "$.mode"}
        assert any(
            cond.get("StringEquals") == "backtest-eval"
            for cond in preset_rule["And"]
        )
        assert preset_rule["Next"] == "PredictorTrainingSkipped"
        # Rule 1: plain skip → validated-skip path (present-and-true; a
        # missing flag must never match).
        plain_vars = {cond["Variable"] for cond in plain_rule["And"]}
        assert plain_vars == {"$.skip_predictor_training"}
        assert any(
            cond.get("IsPresent") is True for cond in plain_rule["And"]
        )
        assert any(
            cond.get("BooleanEquals") is True for cond in plain_rule["And"]
        )
        assert plain_rule["Next"] == "ValidatePredictorSkipWeightsFresh"
        assert gate["Default"] == "PredictorTraining"

    def test_validated_skip_path_wiring(self, branch_b):
        """config#2253: the plain-skip path must VALIDATE the operator's
        weights-are-already-live claim against the live weights manifest
        before the branch may read as succeeded.

        Surface choice is load-bearing: manifest.json is written
        UNCONDITIONALLY by every non-dry training run (promotion-gate
        independent), while weights/meta/archive/{date}/ is TRADING-DAY
        keyed (config#1015 — Friday) and would NEVER match the Saturday
        calendar $.run_date this validation compares against."""
        v = branch_b["ValidatePredictorSkipWeightsFresh"]
        assert v["Type"] == "Task"
        assert v["Resource"] == "arn:aws:states:::aws-sdk:s3:headObject"
        assert v["Parameters"]["Bucket"] == "alpha-engine-research"
        assert v["Parameters"]["Key"] == (
            "predictor/weights/meta/manifest.json"
        )
        # ResultSelector lifts the DATE part of LastModified so the Choice
        # can do a lexicographic (== chronological for YYYY-MM-DD) compare.
        sel = v["ResultSelector"]["manifest_last_modified_date.$"]
        assert "States.StringSplit($.LastModified, 'T')" in sel
        assert v["ResultPath"] == "$.predictor_skip_validation"
        # Fail loud: any S3/serialization error is a branch failure, not a
        # silent fall-through to either skipping OR re-training.
        assert [c["Next"] for c in v["Catch"]] == ["BranchBFailed"]
        assert all(c["ResultPath"] == "$.error" for c in v["Catch"])
        assert v["Next"] == "CheckPredictorSkipWeightsFresh"

    def test_validated_skip_freshness_choice(self, branch_b):
        """manifest date >= run_date → skip terminal; stale → synthesized
        $.error → BranchBFailed (never a silent skip onto stale weights,
        never an implicit re-run of the 1h training spot the operator
        asked to skip)."""
        c = branch_b["CheckPredictorSkipWeightsFresh"]
        assert c["Type"] == "Choice"
        (fresh,) = c["Choices"]
        conds = {
            k: v for cond in fresh["And"] for k, v in cond.items()
            if k != "Variable"
        }
        assert all(
            cond["Variable"]
            == "$.predictor_skip_validation.manifest_last_modified_date"
            for cond in fresh["And"]
        )
        # Shape guard: no fleet SF consumed the aws-sdk:s3 LastModified
        # serialization before — a non-ISO value (HTTP-date/epoch) must
        # fail LOUD, not silently wrong-pass a lexicographic compare.
        assert conds["StringMatches"] == "20*-*-*"
        # alpha-engine-config-I8809: the left side is an S3 LastModified — a
        # wall-clock write time — so the reference is the execution's CALENDAR
        # date. $.run_date became the cycle's TRADING day at NormalizeRunDates,
        # and comparing against it would make this guard strictly WEAKER on
        # every Saturday run.
        assert conds["StringGreaterThanEqualsPath"] == "$.calendar_date"
        assert fresh["Next"] == "PredictorTrainingSkipped"
        assert c["Default"] == "PredictorSkipWeightsStale"
        # The stale Pass synthesizes $.error (a Choice.Default transition
        # does not populate an error path — the config#2160 States.Runtime
        # trap) and routes to BranchBFailed.
        stale = branch_b["PredictorSkipWeightsStale"]
        assert stale["Type"] == "Pass"
        assert stale["ResultPath"] == "$.error"
        assert stale["Parameters"]["Error"] == "PredictorSkipWeightsStale"
        # alpha-engine-config-I8809: the Cause prints the reference it actually
        # compared against, which is now the CALENDAR date.
        assert "$.calendar_date" in stale["Parameters"]["Cause.$"]
        assert stale["Next"] == "BranchBFailed"

    def test_skip_terminal_reads_as_succeeded_branch(self, branch_b):
        """THE aggregator seam (config#2253): AggregateBranchOutcomes reads
        ONLY $.parallel_result[1].branch_b.branch_b_status, so the skip
        terminal must record the IDENTICAL success contract as
        BranchBComplete — same ResultPath, same branch_b_status=OK — plus
        an explicit skipped marker (and nothing fabricated beyond that).
        End:true preserves the per-branch error-isolation invariant."""
        skipped = branch_b["PredictorTrainingSkipped"]
        assert skipped["Type"] == "Pass"
        assert skipped["End"] is True
        # alpha-engine-config-I8194: the envelope moved INSIDE Parameters
        # under the key the old ResultPath named, and ResultPath is gone —
        # a branch terminal must REPLACE its payload, not merge into it, or
        # the branch returns its whole ~108 KB effective input and the join
        # trips States.DataLimitExceeded. Every post-join JSONPath is
        # unchanged. tests/test_sf_parallel_branch_payload.py owns the
        # definition-derived form of this invariant.
        assert "ResultPath" not in skipped
        assert set(skipped["Result"]) == {"branch_b"}
        assert skipped["Result"]["branch_b"]["branch_b_status"] == "OK"
        assert skipped["Result"]["branch_b"]["skipped"] is True
        # Exactly the success contract + the marker + the degraded field
        # (alpha-engine-config#6722: fixed false — this path never reaches
        # the model-zoo rotation, and the field must exist unconditionally
        # so AggregateBranchOutcomes' Parameters.$ extraction never throws).
        assert set(skipped["Result"]["branch_b"]) == {
            "branch_b_status", "skipped", "branch_b_degraded",
            # alpha-engine-config-I11073: every terminal carries the route
            # accumulator the join extracts, empty where no route fired.
            "branch_b_routes",
        }
        assert skipped["Result"]["branch_b"]["branch_b_degraded"] is False
        # Contract equivalence with the real success terminal.
        complete = branch_b["BranchBComplete"]
        assert (
            skipped["Result"]["branch_b"]["branch_b_status"]
            == complete["Parameters"]["branch_b"]["branch_b_status"]
        )
        assert set(skipped["Result"]) == set(complete["Parameters"])
        assert "ResultPath" not in complete

    # test_validation_head_object_key_is_iam_granted was ported to
    # nous-ergon-ops — the SF role policy
    # (infrastructure/iam/alpha-engine-step-functions-role.json) now lives
    # there. The invariant (Sid HeadPredictorWeightsManifest s3:GetObject
    # grant) is enforced in nous-ergon-ops/tests/ per the
    # infra/drop-iam-moved-to-ops cleanup.

    def test_predictor_status_poll_quartet_preserved(self, branch_b):
        # alpha-engine-config-I5687: PredictorTraining dispatches through the
        # poll-budget seed (InitPredictorPollCount) before the first poll,
        # and the loop-back branch is a bounded And[], mirroring the
        # DataPhase2/ThinkTank precedent.
        assert branch_b["PredictorTraining"]["Next"] == (
            "InitPredictorPollCount"
        )
        assert branch_b["InitPredictorPollCount"]["Next"] == (
            "WaitForPredictorTraining"
        )
        assert branch_b["WaitForPredictorTraining"]["Next"] == (
            "CheckPredictorStatus"
        )
        bounded = next(
            c for c in branch_b["CheckPredictorStatus"]["Choices"] if "And" in c
        )
        variables = {cond.get("Variable") for cond in bounded["And"]}
        assert "$.predictor_polls" in variables
        or_block = next(cond["Or"] for cond in bounded["And"] if "Or" in cond)
        statuses = {c["StringEquals"] for c in or_block}
        assert statuses == {"InProgress", "Pending"}
        assert bounded["Next"] == "PredictorWait"
        assert branch_b["PredictorWait"]["Next"] == "PredictorPollWait"
        assert branch_b["PredictorPollWait"]["Next"] == "MergePredictorPollCount"
        assert branch_b["MergePredictorPollCount"]["Next"] == (
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
        # alpha-engine-config-I5687: ResolveZooSpecs dispatches through the
        # poll-budget seed (InitResolveZooPollCount) before the first poll.
        assert resolve["Next"] == "InitResolveZooPollCount"
        assert branch_b["InitResolveZooPollCount"]["Next"] == "WaitResolveZoo"
        # Resolve poll → CheckResolveZooStatus: Success → ParseZooSpecs.
        check_resolve = branch_b["CheckResolveZooStatus"]
        rnexts = {
            c["StringEquals"]: c["Next"]
            for c in check_resolve["Choices"] if "StringEquals" in c
        }
        assert rnexts["Success"] == "ParseZooSpecs"
        # The bounded And[] loop-back branch, mirroring DataPhase2/ThinkTank.
        bounded = next(c for c in check_resolve["Choices"] if "And" in c)
        variables = {cond.get("Variable") for cond in bounded["And"]}
        assert "$.resolve_zoo_polls" in variables
        assert bounded["Next"] == "ResolveZooWait"
        assert branch_b["ResolveZooWait"]["Next"] == "ResolveZooPollWait"
        assert branch_b["ResolveZooPollWait"]["Next"] == "MergeResolveZooPollCount"
        assert branch_b["MergeResolveZooPollCount"]["Next"] == "WaitResolveZoo"
        # Default routes through ExtractModelZooResolveError (mirrors
        # ExtractPredictorError/ExtractSignalsEnvelopeError/ExtractRAGIngestionError)
        # — a Choice.Default transition does not populate $.model_zoo_error the
        # way a Task Catch's ResultPath does, and PublishModelZooFailureImmediate's
        # Message calls States.JsonToString($.model_zoo_error); a direct
        # Choice->Task jump on this edge died with States.Runtime, masking the
        # real zoo-resolve failure (observed live 2026-07-10, config#2160 arc).
        assert check_resolve["Default"] == "ModelZooResolveLivenessGate"  # config#6938
        assert branch_b["ModelZooResolveLivenessGate"]["Default"] == "ExtractModelZooResolveError"
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
        # The dispatch invokes the per-spec spot script with the item's spec
        # id. alpha-engine-config-I4442/I4497 predictor-leg cutover
        # (2026-08-09, crucible-predictor#436+#458): spot_train.sh
        # --model-zoo-spec <id> -> spot_train_spec_dispatch.sh --spec-id <id>.
        dcmd = proc["TrainSpecDispatch"]["Parameters"]["Parameters"]["commands.$"]
        assert "infrastructure/spot_train_spec_dispatch.sh" in dcmd
        assert "--spec-id" in dcmd
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
        # alpha-engine-config-I4442/I4497 predictor-leg cutover (2026-08-09,
        # crucible-predictor#436+#458): spot_train.sh --model-zoo-select ->
        # spot_model_zoo_select.sh --select-only.
        assert "infrastructure/spot_model_zoo_select.sh" in scmd
        assert "--select-only" in scmd
        assert "$.preflight_args" in scmd
        assert any(
            c["Next"] == "PublishModelZooFailureImmediate" and "States.ALL" in c["ErrorEquals"]
            for c in sel["Catch"]
        )
        assert all(c["Next"] != "BranchBFailed" for c in sel["Catch"])
        # alpha-engine-config-I5687: ModelZooSelect dispatches through the
        # poll-budget seed (InitModelZooPollCount) before the first poll.
        assert sel["Next"] == "InitModelZooPollCount"
        assert branch_b["InitModelZooPollCount"]["Next"] == "WaitForModelZoo"
        # Select poll Catch is best-effort; routes via the alert, never BranchBFailed.
        wait = branch_b["WaitForModelZoo"]
        assert all(c["Next"] != "BranchBFailed" for c in wait["Catch"])
        check = branch_b["CheckModelZooStatus"]
        # Default routes through ExtractModelZooSelectError — same rationale
        # as ExtractModelZooResolveError above: CheckModelZooStatus.Default
        # does not populate $.model_zoo_error, and a direct jump to
        # PublishModelZooFailureImmediate died with States.Runtime (observed
        # live 2026-07-10, config#2160 arc).
        assert check["Default"] == "ModelZooSelectLivenessGate"  # config#6938
        assert branch_b["ModelZooSelectLivenessGate"]["Default"] == "ExtractModelZooSelectError"
        extract_select = branch_b["ExtractModelZooSelectError"]
        assert extract_select["Type"] == "Pass"
        assert extract_select["ResultPath"] == "$.model_zoo_error"
        assert extract_select["Parameters"]["poll.$"] == "$.model_zoo_poll"
        assert extract_select["Next"] == "PublishModelZooFailureImmediate"
        nexts = {
            c["StringEquals"]: c["Next"] for c in check["Choices"] if "StringEquals" in c
        }
        assert nexts["Success"] == "BranchBComplete"
        # alpha-engine-config-I5687: bounded And[] loop-back, mirroring
        # DataPhase2/ThinkTank.
        bounded = next(c for c in check["Choices"] if "And" in c)
        variables = {cond.get("Variable") for cond in bounded["And"]}
        assert "$.model_zoo_polls" in variables
        or_block = next(cond["Or"] for cond in bounded["And"] if "Or" in cond)
        statuses = {c["StringEquals"] for c in or_block}
        assert statuses == {"InProgress", "Pending"}
        assert bounded["Next"] == "ModelZooWait"
        assert branch_b["ModelZooWait"]["Next"] == "ModelZooPollWait"
        assert branch_b["ModelZooPollWait"]["Next"] == "MergeModelZooPollCount"
        assert branch_b["MergeModelZooPollCount"]["Next"] == "WaitForModelZoo"
        # The alert state is itself best-effort. alpha-engine-config#6722:
        # routes through MarkModelZooDegraded (sole convergence for all five
        # model-zoo fail-open Catches) before BranchBComplete.
        alert = branch_b["PublishModelZooFailureImmediate"]
        assert alert["Resource"] == "arn:aws:states:::sns:publish"
        assert alert["Next"] == "MarkModelZooDegraded"
        assert all(c["Next"] == "MarkModelZooDegraded" for c in alert["Catch"])
        assert branch_b["MarkModelZooDegraded"]["Next"] == "BranchBComplete"
        assert branch_b["MarkModelZooDegraded"]["ResultPath"] == "$.research_degraded_local"
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
        # alpha-engine-config#6722: BranchAComplete moved from a bare
        # Result to Parameters so it can also hoist branch_a_degraded.$
        # from the branch-local $.research_degraded_local marker.
        # alpha-engine-config-I8194: the envelope moved INSIDE Parameters
        # under the key the old ResultPath named, and ResultPath is gone —
        # a branch terminal must REPLACE its payload, not merge into it, or
        # the branch returns its whole ~108 KB effective input and the join
        # trips States.DataLimitExceeded. Every post-join JSONPath is
        # unchanged. tests/test_sf_parallel_branch_payload.py owns the
        # definition-derived form of this invariant.
        ok = branch_a["BranchAComplete"]["Parameters"]["branch_a"]
        assert ok["branch_a_status"] == "OK"
        assert ok["branch_a_degraded.$"] == "$.research_degraded_local.degraded"
        assert ok["branch_a_routes.$"] == "$.research_degraded_local.routes"
        assert "ResultPath" not in branch_a["BranchAComplete"]
        bad = branch_a["BranchAFailed"]["Parameters"]["branch_a"]
        assert bad["branch_a_status"] == "FAILED"
        assert bad["branch_a_error.$"] == "$.error"
        assert bad["branch_a_degraded"] is False
        assert "ResultPath" not in branch_a["BranchAFailed"]

    def test_branch_b_records_status(self, branch_b):
        # alpha-engine-config-I8194: the envelope moved INSIDE Parameters
        # under the key the old ResultPath named, and ResultPath is gone —
        # a branch terminal must REPLACE its payload, not merge into it, or
        # the branch returns its whole ~108 KB effective input and the join
        # trips States.DataLimitExceeded. Every post-join JSONPath is
        # unchanged. tests/test_sf_parallel_branch_payload.py owns the
        # definition-derived form of this invariant.
        ok = branch_b["BranchBComplete"]["Parameters"]["branch_b"]
        assert ok["branch_b_status"] == "OK"
        assert ok["branch_b_degraded.$"] == "$.research_degraded_local.degraded"
        assert ok["branch_b_routes.$"] == "$.research_degraded_local.routes"
        assert "ResultPath" not in branch_b["BranchBComplete"]
        bad = branch_b["BranchBFailed"]["Parameters"]["branch_b"]
        assert bad["branch_b_status"] == "FAILED"
        assert bad["branch_b_error.$"] == "$.error"
        assert bad["branch_b_degraded"] is False
        assert "ResultPath" not in branch_b["BranchBFailed"]

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

    def test_signals_envelope_hardfail_routes_to_branch_a_failed(self, branch_a):
        """alpha-engine-config-I2515 Phase B: SignalsEnvelope replaces the
        removed multi-agent Research state as Branch A's load-bearing
        producer. Its hard-fail (Task Catch) routes through
        ExtractSignalsEnvelopeError (renamed from ExtractResearchError) →
        PublishResearchFailureImmediate (the fast-SNS-alert state added
        2026-05-24, shared with RAGIngestion failures) → BranchAFailed —
        NO non-blocking Catch-to-continue, unlike ChallengerShadow.

        alpha-engine-config-I7048 (2026-08-12): ExtractSignalsEnvelopeError now
        targets NormalizeBranchAFailureContext, a defensive $.error-normalizer
        Pass chokepoint inserted ahead of PublishResearchFailureImmediate
        (whose own name/Type/Resource are UNCHANGED — kept off a rename so
        the nousergon_lib.pipeline_status.registry entry stays valid without
        a companion nousergon-lib PR)."""
        catch_targets = [
            c["Next"] for c in branch_a["SignalsEnvelope"]["Catch"]
        ]
        assert catch_targets == ["ExtractSignalsEnvelopeError"]
        # ExtractError → normalizer → PublishResearchFailureImmediate → BranchAFailed
        assert (
            branch_a["ExtractSignalsEnvelopeError"]["Next"]
            == "NormalizeBranchAFailureContext"
        )
        normalizer = branch_a["NormalizeBranchAFailureContext"]
        assert normalizer["Type"] == "Pass"
        assert normalizer["Next"] == "PublishResearchFailureImmediate"
        publish = branch_a["PublishResearchFailureImmediate"]
        assert publish["Type"] == "Task"
        assert publish["Resource"] == "arn:aws:states:::sns:publish"
        assert publish["Next"] == "BranchAFailed"
        # SNS-publish-fails escape hatch also lands at BranchAFailed
        for c in publish.get("Catch", []):
            assert c["Next"] == "BranchAFailed"

    def test_challenger_shadow_is_non_blocking(self, branch_a):
        """alpha-engine-config-I2515 Phase B: unlike SignalsEnvelope,
        ChallengerShadow is observe-only (producer leaderboard shadow feed)
        and must never hard-fail Branch A. alpha-engine-config#6722: the
        Catch now routes through MarkChallengerShadowDegraded before
        converging on CheckSkipRAGIngestion exactly as before."""
        catch_targets = [
            c["Next"] for c in branch_a["ChallengerShadow"]["Catch"]
        ]
        assert catch_targets == ["MarkChallengerShadowDegraded"]
        assert branch_a["MarkChallengerShadowDegraded"]["Next"] == "CheckSkipRAGIngestion"
        assert "BranchAFailed" not in catch_targets

    def test_dataphase2_failure_routes_to_branch_a_failed(self, branch_a):
        assert [c["Next"] for c in branch_a["DataPhase2"]["Catch"]] == [
            "BranchAFailed"
        ]

    def test_thinktank_chain_is_absent_from_the_weekly_pipeline(self, branch_a):
        """Brian ruling 2026-08-10: the Think Tank runs daily in shadow mode
        and is NOT part of the weekly SF.

        It used to be a gap_fill top-up inside this branch — a ten-state chain
        (skip gate, spot dispatch, launch check, poll quartet, degraded
        convergence) whose only job was to top up coverage the daily cadence
        already owns. The daily EventBridge rule
        ``alpha-research-thinktank-daily`` -> ``alpha-engine-thinktank-spot-
        dispatcher`` is now the single producer, so the weekly pipeline neither
        launches spot for it nor waits on it.

        This asserts absence rather than deletion history: a future change that
        re-adds any of these states to the weekly pipeline reverses a ruling and
        must be a deliberate edit to this test, not a quiet re-wire."""
        removed = {
            "CheckSkipThinkTankCoverage", "ThinkTankCoverage",
            "CheckThinkTankLaunched", "InitThinkTankPollCount",
            "WaitForThinkTank", "CheckThinkTankStatus", "ThinkTankWait",
            "ThinkTankPollWait", "MergeThinkTankPollCount", "ThinkTankDegraded",
        }
        present = removed & set(branch_a)
        assert not present, (
            f"the weekly SF carries Think Tank state(s) {sorted(present)}. The "
            "Think Tank runs daily in shadow mode on its own cadence (Brian "
            "ruling 2026-08-10) — the weekly pipeline must not launch or poll it."
        )
        # Nothing may still route at the removed chain, and the RAG chain must
        # land on the successor the chain used to converge on.
        import json as _json
        blob = _json.dumps(branch_a)
        for name in removed:
            assert f'"{name}"' not in blob, f"dangling reference to {name}"
        assert (
            branch_a["CheckSkipRAGIngestion"]["Choices"][0]["Next"]
            == "CheckSkipRegimeRetrospectiveEval"
        )
        assert (
            branch_a["CheckRAGIngestionStatus"]["Choices"][0]["Next"]
            == "CheckSkipRegimeRetrospectiveEval"
        )

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
            == "NormalizeBranchBFailureContext"
        )
        # alpha-engine-config-I7048 (2026-08-12): ExtractPredictorError now
        # targets NormalizeBranchBFailureContext, a defensive $.error
        # normalizer Pass chokepoint mirroring NormalizeBranchAFailureContext,
        # inserted ahead of PublishPredictorFailureImmediate (name/Type/
        # Resource unchanged — no registry companion PR needed).
        normalizer = branch_b["NormalizeBranchBFailureContext"]
        assert normalizer["Type"] == "Pass"
        assert normalizer["Next"] == "PublishPredictorFailureImmediate"
        publish = branch_b["PublishPredictorFailureImmediate"]
        assert publish["Type"] == "Task"
        assert publish["Resource"] == "arn:aws:states:::sns:publish"
        assert publish["Next"] == "BranchBFailed"
        for c in publish.get("Catch", []):
            assert c["Next"] == "BranchBFailed"
        # config#6938: the non-Success arm reaches the normalizer THROUGH the
        # liveness gate, which separates a reclaimed launcher from a training
        # failure. Assert the route, not one hop of it.
        assert (
            branch_b["CheckPredictorStatus"]["Default"]
            == "PredictorTrainingLivenessGate"
        )
        assert (
            branch_b["PredictorTrainingLivenessGate"]["Default"]
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
            # alpha-engine-config-I10539: ReplayConcordance is retired.
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
        # spot inside Branch B). alpha-engine-config#6722 spliced
        # CheckResearchPredictorDegraded (the branch-degraded fold) onto
        # this edge — it still falls through unconditionally to
        # CheckSkipBacktester on a clean run (see
        # tests/test_sf_research_predictor_degraded_wiring.py for the fold
        # itself).
        assert c["Default"] == "CheckResearchPredictorDegraded"

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
        CheckSkipDataPhase2 IN-BRANCH — never the parent Parallel (invalid
        branch→parent) nor top-level HandleFailure (cross-branch cancel).
        alpha-engine-config-I2515 Phase B: Scanner's successor is now
        CheckSkipRegimeSubstrate (RegimeSubstrate moved ahead of RAG/
        ThinkTank), and the removed multi-agent Research state's
        CheckSkipResearch successor is replaced by CheckSkipDataPhase2."""
        assert branch_a["Scanner"]["Next"] == "CheckSkipRegimeSubstrate"
        assert (
            branch_a["RegimeRetrospectiveEval"]["Next"] == "CheckSkipDataPhase2"
        )
        # alpha-engine-config#6722: routes through
        # MarkRegimeRetrospectiveEvalDegraded before converging on
        # CheckSkipDataPhase2 exactly as before.
        assert [
            c["Next"]
            for c in branch_a["RegimeRetrospectiveEval"]["Catch"]
        ] == ["MarkRegimeRetrospectiveEvalDegraded"]
        assert (
            branch_a["MarkRegimeRetrospectiveEvalDegraded"]["Next"]
            == "CheckSkipDataPhase2"
        )
        c = branch_a["CheckSkipRegimeRetrospectiveEval"]
        assert c["Choices"][0]["Next"] == "CheckSkipDataPhase2"
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
        ExtractSignalsEnvelopeError — a branch state pointing at the
        non-branch HandleFailure is an invalid ASL transition AND would re-introduce
        cross-branch cancellation."""
        # alpha-engine-config-I7048 (2026-08-12): these edges now target
        # NormalizeBranchAFailureContext, the defensive $.error-normalizer
        # Pass chokepoint inserted ahead of PublishResearchFailureImmediate.
        assert [c["Next"] for c in branch_a["RAGIngestion"]["Catch"]] == [
            "NormalizeBranchAFailureContext"
        ]
        assert [
            c["Next"] for c in branch_a["WaitForRAGIngestion"]["Catch"]
        ] == ["NormalizeBranchAFailureContext"]
        assert (
            branch_a["ExtractRAGIngestionError"]["Next"]
            == "NormalizeBranchAFailureContext"
        )
        assert (
            branch_a["NormalizeBranchAFailureContext"]["Next"]
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
        # alpha-engine-config#6722: CheckResearchPredictorDegraded (the
        # branch-degraded fold) is spliced onto this edge but still falls
        # through unconditionally to CheckSkipBacktester on a clean run.
        assert states["CheckBranchOutcomes"]["Default"] == "CheckResearchPredictorDegraded"
        assert states["CheckResearchPredictorDegraded"]["Default"] == "CheckSkipBacktester"
        # config#2362 Option A: CheckSkipBacktester's Default now falls
        # through the additive CheckSkipBacktesterStageOnly gate before
        # Backtester.
        assert states["CheckSkipBacktester"]["Default"] == "CheckSkipBacktesterStageOnly"
        assert states["CheckSkipBacktesterStageOnly"]["Default"] == "Backtester"

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
                or name.endswith("LivenessGate")  # config#6938: error-side branch
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

    def test_branch_terminal_sets_pinned(self, parallel):
        """Pin the CLOSED set of End:true terminals per branch, and require
        every terminal to record its branch status as data (the Parallel
        error-isolation contract). Branch B gained a third terminal in
        config#2253: PredictorTrainingSkipped (validated-skip success)."""
        expected = [
            ("branch_a", {"BranchAComplete", "BranchAFailed"}),
            (
                "branch_b",
                {
                    "BranchBComplete",
                    "BranchBFailed",
                    "PredictorTrainingSkipped",
                },
            ),
        ]
        for bi, b in enumerate(parallel["Branches"]):
            ends = {
                k for k, v in b["States"].items() if v.get("End") is True
            }
            envelope_key, names = expected[bi]
            assert ends == names, (bi, ends)
            for k in ends:
                st = b["States"][k]
                assert st["Type"] == "Pass", (bi, k)
                # alpha-engine-config-I8194: the terminal REPLACES the
                # branch payload — the envelope key that used to be the
                # ResultPath is now the sole top-level key of
                # Parameters/Result, and ResultPath is absent.
                assert "ResultPath" not in st, (bi, k)
                body = st.get("Parameters", st.get("Result"))
                assert set(body) == {envelope_key}, (bi, k)
