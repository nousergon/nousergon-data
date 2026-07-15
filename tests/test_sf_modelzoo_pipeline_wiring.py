"""Pins the ne-modelzoo-sunday-pipeline child SF (alpha-engine-config-I2545).

Origin: Phase 3 of the weekly-SF load-reduction plan — the config#1083
model-zoo fan-out (ResolveZooSpecs -> Map -> ModelZooSelect) was moved OFF
Saturday's Branch B into its own Sunday-09:00-UTC-triggered child SF
(infrastructure/step_function_modelzoo.json), Brian-ruled 2026-07-14. Ends
with a re-invoke of the crucible-evaluator grading Lambda as the
persistent-dash re-grade (same ruling: the grading Lambda is stateless/
pull-based and keys to trading_day, so a Sunday run updates the SAME Friday
card the Saturday run wrote).

Re-pins the behavioral assertions that used to live in
test_sf_research_predictor_parallel_wiring.py::TestBranchBContents before
the I2545 lift, plus new coverage for the child-SF-only scaffolding.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_modelzoo.json"

_LIFTED_ZOO_STATES = {
    "ResolveZooSpecs", "WaitResolveZoo", "CheckResolveZooStatus",
    "ExtractModelZooResolveError", "ResolveZooWait", "ParseZooSpecs",
    "ModelZooTrainMap", "ModelZooSelect", "WaitForModelZoo",
    "CheckModelZooStatus", "ExtractModelZooSelectError",
    "PublishModelZooFailureImmediate", "ModelZooWait",
    "GradingLambdaReGrade", "PublishGradingDegradedAlert",
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
    return states["ModelZooPipelineWrapper"]


@pytest.fixture(scope="module")
def inner(wrapper) -> dict:
    return wrapper["Branches"][0]["States"]


class TestJsonParsesAndTopLevel:
    def test_json_parses(self, sf):
        assert isinstance(sf, dict)
        assert sf["StartAt"] in sf["States"]

    def test_start_at_initializes_input(self, sf):
        assert sf["StartAt"] == "InitializeModelZooInput"

    def test_top_level_timeout(self, sf):
        assert sf["TimeoutSeconds"] == 21600
        assert sf["TimeoutSeconds"] <= 24 * 3600

    def test_initialize_modelzoo_input_derives_run_date_from_own_execution(self, states):
        """The Sunday execution's OWN calendar date is used for $.run_date —
        deliberate, since trading_day = last_closed_trading_day(now) is
        backward-looking and both Saturday and Sunday resolve to the same
        last-closed Friday (see this state's Comment for the full rationale
        + the flagged, NOT-closed cross-repo audit gap on the zoo Task
        states' own internal date handling)."""
        init = states["InitializeModelZooInput"]
        assert init["Type"] == "Pass"
        merge_expr = init["Parameters"]["merged.$"]
        assert "$$.Execution.StartTime" in merge_expr
        assert "$$.Execution.Input" in merge_expr
        assert '"research_dry":false' in merge_expr
        assert init["Next"] == "ModelZooPipelineWrapper"


class TestWrapperParallelCatchAll:
    def test_wrapper_is_single_branch_parallel(self, wrapper):
        assert wrapper["Type"] == "Parallel"
        assert len(wrapper["Branches"]) == 1

    def test_wrapper_starts_at_resolve_zoo_specs(self, wrapper):
        assert wrapper["Branches"][0]["StartAt"] == "ResolveZooSpecs"

    def test_wrapper_catch_routes_to_failure_funnel(self, wrapper):
        catches = wrapper["Catch"]
        assert any(
            c["ErrorEquals"] == ["States.ALL"]
            and c["Next"] == "ModelZooNormalizeFailureContext"
            and c["ResultPath"] == "$.error"
            for c in catches
        )

    def test_failure_funnel_reaches_fail_state(self, states):
        assert states["ModelZooNormalizeFailureContext"]["Next"] == (
            "ModelZooHandleFailure"
        )
        assert states["ModelZooHandleFailure"]["Next"] == "ModelZooFailExecution"
        assert states["ModelZooFailExecution"]["Type"] == "Fail"


class TestLiftedStatesPresent:
    @pytest.mark.parametrize("name", sorted(_LIFTED_ZOO_STATES))
    def test_lifted_state_present(self, inner, name):
        assert name in inner


class TestZooFanoutSemanticsPreserved:
    """Re-pins the behavioral assertions that used to live in
    test_sf_research_predictor_parallel_wiring.py::TestBranchBContents
    before the I2545 lift."""

    def test_zoo_fanout_pipeline_wiring(self, inner):
        resolve = inner["ResolveZooSpecs"]
        assert resolve["Parameters"]["InstanceIds.$"] == "$.ec2_instance_id"
        rcmd = resolve["Parameters"]["Parameters"]["commands.$"]
        assert "list-rotation-specs" in rcmd
        assert resolve["Next"] == "WaitResolveZoo"
        check_resolve = inner["CheckResolveZooStatus"]
        rnexts = {c["StringEquals"]: c["Next"] for c in check_resolve["Choices"]}
        assert rnexts["Success"] == "ParseZooSpecs"
        assert check_resolve["Default"] == "ExtractModelZooResolveError"
        extract_resolve = inner["ExtractModelZooResolveError"]
        assert extract_resolve["Type"] == "Pass"
        assert extract_resolve["ResultPath"] == "$.model_zoo_error"
        assert extract_resolve["Next"] == "PublishModelZooFailureImmediate"
        parse = inner["ParseZooSpecs"]
        assert parse["Type"] == "Pass"
        assert "StringToJson" in parse["Parameters"]["zoo_specs.$"]
        assert "Catch" not in parse
        assert parse["Next"] == "ModelZooTrainMap"

    def test_model_zoo_train_map_per_spec_isolation(self, inner):
        m = inner["ModelZooTrainMap"]
        assert m["Type"] == "Map"
        assert m["ItemsPath"] == "$.parsed_zoo.zoo_specs"
        assert isinstance(m["MaxConcurrency"], int) and m["MaxConcurrency"] >= 1
        assert m["ToleratedFailurePercentage"] == 100
        assert m["ItemSelector"]["spec_id.$"] == "$$.Map.Item.Value"
        assert m["ItemSelector"]["ec2_instance_id.$"] == "$.ec2_instance_id"
        proc = m["ItemProcessor"]["States"]
        dcmd = proc["TrainSpecDispatch"]["Parameters"]["Parameters"]["commands.$"]
        assert "--model-zoo-spec" in dcmd
        assert "$.spec_id" in dcmd
        for term in ("TrainSpecOK", "TrainSpecFailed"):
            assert proc[term]["Type"] == "Pass"
            assert proc[term]["End"] is True
        cts = proc["CheckTrainSpecStatus"]
        assert cts["Default"] == "TrainSpecFailed"
        assert m["Next"] == "ModelZooSelect"

    def test_model_zoo_map_iterator_no_dangling(self, inner):
        proc = inner["ModelZooTrainMap"]["ItemProcessor"]
        names = set(proc["States"])
        assert proc["StartAt"] in names
        for n, st in proc["States"].items():
            for t in _own_targets(st):
                assert t in names, f"Map iterator dangling: {n} -> {t}"

    def test_model_zoo_select_is_best_effort(self, inner):
        sel = inner["ModelZooSelect"]
        assert sel["Parameters"]["InstanceIds.$"] == "$.ec2_instance_id"
        scmd = sel["Parameters"]["Parameters"]["commands.$"]
        assert "--model-zoo-select" in scmd
        assert sel["Next"] == "WaitForModelZoo"
        check = inner["CheckModelZooStatus"]
        assert check["Default"] == "ExtractModelZooSelectError"
        extract_select = inner["ExtractModelZooSelectError"]
        assert extract_select["Next"] == "PublishModelZooFailureImmediate"
        nexts = {c["StringEquals"]: c["Next"] for c in check["Choices"]}
        assert nexts["InProgress"] == "ModelZooWait"
        assert nexts["Pending"] == "ModelZooWait"
        # alpha-engine-config-I2545: Success now feeds the grading tail
        # (was BranchBComplete before the lift — no Parallel join here).
        assert nexts["Success"] == "GradingLambdaReGrade"
        assert inner["ModelZooWait"]["Next"] == "WaitForModelZoo"

    def test_model_zoo_failure_alert_converges_on_grading_tail(self, inner):
        """alpha-engine-config-I2545: every best-effort ModelZoo failure
        path converges on GradingLambdaReGrade (was BranchBComplete
        pre-lift) — the persistent-dash re-grade must still run even if
        the rotation itself degraded."""
        alert = inner["PublishModelZooFailureImmediate"]
        assert alert["Resource"] == "arn:aws:states:::sns:publish"
        assert alert["Next"] == "GradingLambdaReGrade"
        assert all(c["Next"] == "GradingLambdaReGrade" for c in alert["Catch"])
        assert "PREDICTOR_DEFER_TRAINING_EMAIL" in alert["Parameters"]["Message.$"]


class TestGradingLambdaReGradeTail:
    def test_grading_lambda_payload_shape_matches_report_card(self, inner):
        """Same FunctionName/Payload shape as the parent SF's ReportCard
        state (and the advisory child's own ReportCard) — same Lambda,
        same contract, per Brian's 2026-07-14 ruling."""
        g = inner["GradingLambdaReGrade"]
        assert g["Resource"] == "arn:aws:states:::lambda:invoke"
        assert g["Parameters"]["FunctionName"] == "alpha-engine-evaluator:live"
        assert g["Parameters"]["Payload"]["date.$"] == "$.run_date"
        assert g["Parameters"]["Payload"]["dry_run.$"] == "$.research_dry"

    def test_grading_lambda_snapshot_flag_false(self, inner):
        """alpha-engine-config-I2556 (persistent report card with weekly
        snapshots): this Sunday re-grade only refreshes the standing
        latest.json — snapshot=false, never a second same-week dated
        snapshot (the advisory child SF's ReportCard owns that, with
        snapshot=true)."""
        g = inner["GradingLambdaReGrade"]
        assert g["Parameters"]["Payload"]["snapshot"] is False

    def test_grading_lambda_non_blocking(self, inner):
        """I2545 build item 2: a grading failure must not fail the zoo run."""
        g = inner["GradingLambdaReGrade"]
        assert g["Catch"][0]["Next"] == "PublishGradingDegradedAlert"
        assert g["Next"] == "ModelZooSundayNotifyComplete"
        alert = inner["PublishGradingDegradedAlert"]
        assert alert["Next"] == "ModelZooSundayNotifyComplete"
        assert all(
            c["Next"] == "ModelZooSundayNotifyComplete" for c in alert["Catch"]
        )


class TestTerminalNotify:
    def test_notify_complete_is_constants_only(self, inner):
        n = inner["ModelZooSundayNotifyComplete"]
        assert n["Resource"] == "arn:aws:states:::sns:publish"
        assert "Subject.$" not in n["Parameters"]
        assert "Message.$" not in n["Parameters"]
        assert n["End"] is True
        assert n["Catch"][0]["Next"] == "ModelZooSundayNotifyCompleteDegraded"

    def test_notify_complete_degraded_records_data(self, inner):
        d = inner["ModelZooSundayNotifyCompleteDegraded"]
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
