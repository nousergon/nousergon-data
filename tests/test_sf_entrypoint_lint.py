"""Tests for the enforcing S-46 Step Functions portability lint."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any
import pytest
import yaml
from validators import sf_entrypoint_lint as lint

REPO = Path(__file__).resolve().parents[1]
DECLARED: lint.Json = {"dispatcher_lambdas": ["dispatcher"], "lambda_functions": ["declared", "notify-ops"], "state_machines": ["child"], "jobs": ["daily", "container"]}

def task(resource: str = lint.LAMBDA_PREFIX, **params: Any) -> lint.Json:
    return {"Type": "Task", "Resource": resource, "Parameters": params, "End": True}

def choice(variable: str, operator: str = "StringEquals") -> lint.Json:
    return {"Type": "Choice", "Choices": [{"Variable": variable, operator: "SUCCEEDED", "Next": "Done"}]}

@pytest.mark.parametrize(("state", "allowed"), [
    (task(FunctionName="declared"), True),
    (task(FunctionName="dispatcher", Payload={"job": "daily"}), True),
    (task(FunctionName="dispatcher", Payload={"job.$": "$.job"}), False),
    (task(FunctionName="dispatcher", Payload={"job": "unknown"}), False),
    (task(lint.STATES_PREFIX + ".sync", StateMachineArn="arn:aws:states:x:1:stateMachine:child"), True),
    (task(lint.ECS_PREFIX + ".sync", TaskDefinition="container"), True),
    (task(lint.ECS_PREFIX, TaskDefinition="$.dynamic"), False),
])
def test_tasks_require_literal_declared_entrypoints(state: lint.Json, allowed: bool) -> None:
    assert lint.task_allowed(state, DECLARED) is allowed

@pytest.mark.parametrize(("state", "allowed"), [
    (choice("$.status"), True), (choice("$.ok", "BooleanEquals"), True),
    (choice("$.error", "IsPresent"), True), (choice("$.count", "NumericGreaterThan"), False),
    (choice("$.date"), False),
])
def test_choices_only_inspect_success_or_failure(state: lint.Json, allowed: bool) -> None:
    assert lint.choice_allowed(state) is allowed

def test_retry_catch_parallel_and_other_business_logic() -> None:
    states = {"Fail": {"Type": "Fail"}, "Notify": task(FunctionName="notify-ops")}
    good = task(FunctionName="declared") | {"Retry": [{"ErrorEquals": list(lint.TRANSPORT_ERRORS), "MaxAttempts": 3}], "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fail"}]}
    assert lint.state_findings(good, states, DECLARED) == []
    assert "business-retry" in lint.state_findings(good | {"Retry": [{"ErrorEquals": ["States.ALL"]}]}, states, DECLARED)
    assert "business-retry" in lint.state_findings(good | {"Retry": [{"ErrorEquals": ["States.Timeout"], "MaxAttempts": 4}]}, states, DECLARED)
    assert "business-catch" in lint.state_findings(good | {"Catch": [{"Next": "Missing"}]}, states, DECLARED)
    parallel = {"Type": "Parallel", "Branches": [{"States": {"Only": task(FunctionName="declared")}}]}
    assert lint.state_findings(parallel, states, DECLARED) == []
    parallel["Branches"][0]["States"]["Logic"] = {"Type": "Pass"}
    assert "parallel-business-logic" in lint.state_findings(parallel, states, DECLARED)
    assert "dynamic-wait" in lint.state_findings({"Type": "Wait", "SecondsPath": "$.delay"}, states, DECLARED)
    assert "data-derived-map" in lint.state_findings({"Type": "Map", "ItemsPath": "$.items"}, states, DECLARED)
    assert "computing-intrinsic" in lint.state_findings({"Type": "Pass", "Parameters": {"x.$": "States.MathAdd($.x, 1)"}}, states, DECLARED)

def test_walks_nested_states() -> None:
    nested = {"P": {"Type": "Parallel", "Branches": [{"States": {"C": choice("$.count", "NumericGreaterThan")}}]}}
    assert [name for name, _ in lint.walk_states(nested)] == ["P", "P[0]/C"]

def write_repo(tmp_path: Path, name: str) -> None:
    infra = tmp_path / "infrastructure"; infra.mkdir(exist_ok=True)
    (infra / "step_function_x.json").write_text(json.dumps({"States": {name: task(FunctionName="missing")}}))
    (tmp_path / "declared.json").write_text(json.dumps(DECLARED))

def test_baseline_is_exact_shrink_only_and_rejects_substitution(tmp_path: Path) -> None:
    write_repo(tmp_path, "Old")
    rows = lint.findings(tmp_path, DECLARED); baseline = lint.finding_keys(rows)
    baseline_path = tmp_path / "baseline.json"; baseline_path.write_text(lint.render_baseline(baseline))
    assert lint.main(["--repo", str(tmp_path), "--declared", str(tmp_path / "declared.json"), "--baseline", str(baseline_path)]) == 0
    write_repo(tmp_path, "Replacement")
    assert lint.main(["--repo", str(tmp_path), "--declared", str(tmp_path / "declared.json"), "--baseline", str(baseline_path)]) == 1
    assert lint.main(["--repo", str(tmp_path), "--declared", str(tmp_path / "declared.json"), "--baseline", str(baseline_path), "--update"]) == 1
    (tmp_path / "infrastructure" / "step_function_x.json").write_text(json.dumps({"States": {"Done": {"Type": "Succeed"}}}))
    assert lint.main(["--repo", str(tmp_path), "--declared", str(tmp_path / "declared.json"), "--baseline", str(baseline_path), "--update"]) == 0
    assert lint.load_baseline(baseline_path) == set()

def test_committed_tree_and_ci_are_enforcing() -> None:
    assert lint.finding_keys(lint.findings(REPO, lint.load_declared(lint.DECLARED_PATH))) == lint.load_baseline(lint.BASELINE_PATH)
    ci = yaml.safe_load((REPO / ".github/workflows/ci.yml").read_text())
    steps = [step for job in ci["jobs"].values() for step in job.get("steps", []) if "validators/sf_entrypoint_lint.py" in str(step.get("run", ""))]
    assert len(steps) == 1 and not steps[0].get("continue-on-error")
