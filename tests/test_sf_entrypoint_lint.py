"""S-46 lint: Step Functions states call declared entrypoints only (observe mode).

alpha-engine-config-I12533. The rule classification is tested on synthetic
definitions. The committed baseline and declaration files are checked for
SHAPE only, except for the definitions in ``ENFORCED_DEFINITIONS``, whose live
counts must equal the baseline exactly (observe mode never fails CI on the
others).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from validators import sf_entrypoint_lint as lint

REPO = Path(__file__).resolve().parents[1]
DECLARED = {"lambda_functions": {"ae-declared"}, "state_machines": {"ne-child"}}


def _task(resource: str, **params: Any) -> dict:
    return {"Type": "Task", "Resource": resource, "Parameters": params, "End": True}


def _choice(*rules: dict) -> dict:
    return {"Type": "Choice", "Choices": list(rules), "Default": "X"}


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (_task(lint.LAMBDA_PREFIX, FunctionName="ae-declared:live"), None),
        (_task(lint.LAMBDA_PREFIX, FunctionName="arn:aws:lambda:us-east-1:1:function:ae-declared"), None),
        (_task(lint.LAMBDA_PREFIX + ".waitForTaskToken", FunctionName="ae-declared"), None),
        (_task(lint.LAMBDA_PREFIX, FunctionName="ae-new"), ("task-undeclared-entrypoint", "lambda ae-new")),
        (_task(lint.LAMBDA_PREFIX, **{"FunctionName.$": "$.fn"}),
         ("task-undeclared-entrypoint", "lambda <dynamic FunctionName>")),
        (_task(lint.STATES_PREFIX + ".sync:2", StateMachineArn="arn:aws:states:us-east-1:1:stateMachine:ne-child"), None),
        (_task(lint.STATES_PREFIX, StateMachineArn="arn:aws:states:us-east-1:1:stateMachine:ne-other"),
         ("task-undeclared-entrypoint", "state machine ne-other")),
        (_task("arn:aws:states:::sns:publish", TopicArn="t"), ("task-not-an-entrypoint", "sns:publish")),
        (_task("arn:aws:states:::aws-sdk:ssm:getCommandInvocation"),
         ("task-not-an-entrypoint", "aws-sdk:ssm:getCommandInvocation")),
        (_task(lint.SSM_SEND, Parameters={"commands": ["set -eo pipefail", "python3 run_units.py --unit D21"]}), None),
        (_task(lint.SSM_SEND, Parameters={"commands": ["if [ -f x ]; then run; fi"]}),
         ("task-inline-shell-logic", "shell control flow 'if'")),
        (_task(lint.SSM_SEND, Parameters={"commands.$": "States.Array('a || true')"}),
         ("task-inline-shell-logic", "shell control flow '||'")),
        (_task(lint.SSM_SEND, Parameters={"commands": ["bp_wait() { sleep 1; }"]}),
         ("task-inline-shell-logic", "shell control flow '() {'")),
    ],
)
def test_task_classification(state: dict, expected: tuple[str, str] | None) -> None:
    assert lint.classify_task(state, DECLARED) == expected


def test_shell_keywords_inside_words_are_not_logic() -> None:
    state = _task(lint.SSM_SEND, Parameters={"commands": ["python3 -m verify_for_date --notify"]})
    assert lint.classify_task(state, DECLARED) is None


def test_ecs_job_must_be_a_declared_literal() -> None:
    declared = {**DECLARED, "jobs": {"daily-chain"}}
    assert lint.classify_task(_task(lint.ECS_PREFIX, JobName="daily-chain"), declared) is None
    assert lint.classify_task(_task(lint.ECS_PREFIX, **{"JobName.$": "$.job"}), declared) == (
        "task-undeclared-entrypoint", "ECS job <dynamic>",
    )


@pytest.mark.parametrize(
    ("state", "outcome_only"),
    [
        (_choice({"Variable": "$.result.Status", "StringEquals": "Success", "Next": "A"}), False),
        (_choice({"Variable": "$.error", "IsPresent": True, "Next": "A"}), True),
        (_choice({"Not": {"Variable": "$.Payload.ok", "BooleanEquals": True}, "Next": "A"}), False),
        (_choice({"And": [{"Variable": "$.a.Status", "StringEquals": "Failed"},
                          {"Variable": "$.a.StatusDetails", "StringEquals": "Failed"}], "Next": "A"}), False),
        (_choice({"Variable": "$.skip_backtester", "BooleanEquals": True, "Next": "A"}), False),
        (_choice({"Variable": "$.attempts", "NumericLessThan": 3, "Next": "A"}), False),
        (_choice({"Variable": "$.Status", "StringEquals": "Success", "Next": "A"},
                 {"Variable": "$.pipeline_role", "StringEquals": "rehearsal", "Next": "B"}), False),
        (_choice({"Variable": "$.ResponseCode", "IsNumeric": True, "Next": "A"}), False),
        (_choice({"Condition": "{% $states.input.ok %}", "Next": "A"}), False),
    ],
)
def test_choice_branches_only_on_outcome(state: dict, outcome_only: bool) -> None:
    assert lint.choice_is_outcome_only(state) is outcome_only


def _write_repo(tmp_path: Path, states: dict) -> Path:
    infra = tmp_path / "infrastructure"
    infra.mkdir()
    (infra / "step_function_x.json").write_text(json.dumps({"StartAt": "A", "States": states}))
    (tmp_path / "declared.json").write_text(json.dumps({
        "lambda_functions": {"ae-declared": "n"}, "state_machines": {},
    }))
    return tmp_path


def test_walk_reaches_parallel_branches_and_map_processors(tmp_path: Path) -> None:
    states = {
        "P": {"Type": "Parallel", "Branches": [
            {"StartAt": "S", "States": {"S": _task("arn:aws:states:::sns:publish")}},
        ], "Next": "M"},
        "M": {"Type": "Map", "ItemProcessor": {"StartAt": "C", "States": {
            "C": _choice({"Variable": "$.phase", "StringEquals": "x", "Next": "D"}),
            "D": _task(lint.LAMBDA_PREFIX, FunctionName="ae-declared"),
        }}, "End": True},
    }
    repo = _write_repo(tmp_path, states)
    rows = lint.findings(repo, lint.load_declared(repo / "declared.json"))
    assert {(r["state"], r["rule"]) for r in rows} == {
        ("P[0]/S", "task-not-an-entrypoint"),
        ("P", "parallel-business-logic"),
        ("M/C", "choice-beyond-outcome"),
    }


def _run(repo: Path, *args: str) -> int:
    return lint.main(["--repo", str(repo), "--declared", str(repo / "declared.json"),
                      "--baseline", str(repo / "baseline.json"), *args])


def test_observe_never_fails_and_enforce_fails_on_new(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    repo = _write_repo(tmp_path, {"S": _task("arn:aws:states:::sns:publish")})
    (repo / "baseline.json").write_text(lint.render_baseline({}))
    assert _run(repo, "--mode", "observe") == 0
    assert "1 new, 0 stale" in capsys.readouterr().out
    assert _run(repo, "--mode", "enforce") == 1


def test_update_lowers_but_never_raises(tmp_path: Path) -> None:
    repo = _write_repo(tmp_path, {"S": _task("arn:aws:states:::sns:publish")})
    path = "infrastructure/step_function_x.json"
    (repo / "baseline.json").write_text(lint.render_baseline({
        path: {"S": ["task-not-an-entrypoint", "choice-beyond-outcome"]},
    }))
    assert _run(repo, "--update") == 0
    assert lint.load_baseline(repo / "baseline.json") == {path: {"S": ["task-not-an-entrypoint"]}}

    (repo / "infrastructure" / "step_function_x.json").write_text(json.dumps({"StartAt": "S", "States": {
        "S": _task("arn:aws:states:::sns:publish", T=1), "T": _task("arn:aws:states:::sns:publish"),
    }}))
    assert _run(repo, "--update") == 0
    assert lint.load_baseline(repo / "baseline.json") == {path: {"S": ["task-not-an-entrypoint"]}}
    assert _run(repo, "--mode", "enforce") == 1


def test_committed_files_have_the_declared_shape() -> None:
    declared = lint.load_declared(lint.DECLARED_PATH)
    assert declared["lambda_functions"] and declared["state_machines"]
    baseline = lint.load_baseline(lint.BASELINE_PATH)
    assert set(baseline) <= {p.relative_to(REPO).as_posix() for p in lint.definition_files(REPO)}


def test_ci_runs_it_inside_an_existing_job_and_lets_enforced_definitions_block() -> None:
    """Observe for most definitions, but the step must be able to fail.

    ``continue-on-error`` would swallow the exit 1 an ENFORCED definition
    raises, turning enforcement back into observation without any signal.
    """
    ci = yaml.safe_load((REPO / ".github/workflows/ci.yml").read_text())
    steps = [
        (job, step) for job, spec in ci["jobs"].items() for step in spec.get("steps", [])
        if "validators/sf_entrypoint_lint.py" in str(step.get("run", ""))
    ]
    assert len(steps) == 1
    job, step = steps[0]
    assert job == "ssm-log-capture-contract-lint"
    assert not step.get("continue-on-error")
    assert "--mode enforce" not in step["run"]


def test_enforced_definitions_are_real_baselined_and_off_the_weekly_path() -> None:
    real = {p.relative_to(REPO).as_posix() for p in lint.definition_files(REPO)}
    assert lint.ENFORCED_DEFINITIONS, "the enforced set only grows; it may not empty"
    assert lint.ENFORCED_DEFINITIONS <= real
    # The weekly SF stays observe-only until it is reworked deliberately.
    assert "infrastructure/step_function.json" not in lint.ENFORCED_DEFINITIONS
    assert "infrastructure/step_function_daily.json" in lint.ENFORCED_DEFINITIONS
    assert "infrastructure/step_function_groom.json" in lint.ENFORCED_DEFINITIONS
    assert "infrastructure/step-functions/data-collection.asl.json" in lint.ENFORCED_DEFINITIONS


def test_the_committed_tree_passes_the_enforced_definitions() -> None:
    """Live counts for ENFORCED definitions must equal the baseline exactly."""
    declared = lint.load_declared(lint.DECLARED_PATH)
    current = lint.counts_of(lint.findings(REPO, declared))
    new, stale = lint.compare(current, lint.load_baseline(lint.BASELINE_PATH))
    blocking = [r for r in new + stale if r["path"] in lint.ENFORCED_DEFINITIONS]
    assert blocking == []


def test_observe_mode_blocks_only_on_an_enforced_definition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    repo = _write_repo(tmp_path, {"S": _task("arn:aws:states:::sns:publish")})
    path = "infrastructure/step_function_x.json"
    (repo / "baseline.json").write_text(lint.render_baseline({}))

    monkeypatch.setattr(lint, "ENFORCED_DEFINITIONS", frozenset({"infrastructure/step_function_other.json"}))
    assert _run(repo, "--mode", "observe") == 0
    assert "observe mode: reporting only." in capsys.readouterr().out

    monkeypatch.setattr(lint, "ENFORCED_DEFINITIONS", frozenset({path}))
    assert _run(repo, "--mode", "observe") == 1
    out = capsys.readouterr().out
    assert "NEW (enforced) " + path in out
    assert "failing" in out

    (repo / "baseline.json").write_text(lint.render_baseline({
        path: {"S": ["task-not-an-entrypoint", "choice-beyond-outcome"]},
    }))
    assert _run(repo, "--mode", "observe") == 1
    assert "STALE (enforced) " + path in capsys.readouterr().out
    assert _run(repo, "--update") == 0
    assert _run(repo, "--mode", "observe") == 0


@pytest.mark.parametrize(
    ("state", "rule"),
    [
        ({"Type": "Task", "Resource": lint.LAMBDA_PREFIX, "Parameters": {"FunctionName": "ae-declared"},
          "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 1}], "End": True},
         "retry-business-logic"),
        ({"Type": "Task", "Resource": lint.LAMBDA_PREFIX, "Parameters": {"FunctionName": "ae-declared"},
          "Retry": [{"ErrorEquals": ["States.Timeout"], "MaxAttempts": 4}], "End": True},
         "retry-business-logic"),
        ({"Type": "Wait", "SecondsPath": "$.seconds", "End": True}, "wait-dynamic"),
        ({"Type": "Map", "ItemsPath": "$.items", "Iterator": {"StartAt": "Done", "States": {
            "Done": {"Type": "Succeed"},
        }}, "End": True}, "map-data-derived"),
        ({"Type": "Pass", "Parameters": {"x.$": "States.MathAdd($.x, 1)"}, "End": True},
         "computing-intrinsic"),
    ],
)
def test_business_logic_states_are_findings(tmp_path: Path, state: dict, rule: str) -> None:
    repo = _write_repo(tmp_path, {"S": state})
    rows = lint.findings(repo, lint.load_declared(repo / "declared.json"))
    assert rule in {row["rule"] for row in rows if row["state"] == "S"}


def test_transport_retry_and_terminal_catch_are_allowed(tmp_path: Path) -> None:
    task = _task(lint.LAMBDA_PREFIX, FunctionName="ae-declared")
    task.update({
        "Retry": [{"ErrorEquals": ["States.Timeout", "Lambda.ServiceException"], "MaxAttempts": 3}],
        "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}],
    })
    repo = _write_repo(tmp_path, {"S": task, "Failed": {"Type": "Fail"}})
    rows = lint.findings(repo, lint.load_declared(repo / "declared.json"))
    assert not {"retry-business-logic", "catch-business-logic"} & {row["rule"] for row in rows}


def test_baseline_is_keyed_by_state_and_rejects_substitution(tmp_path: Path) -> None:
    repo = _write_repo(tmp_path, {"Old": _task("arn:aws:states:::sns:publish")})
    path = "infrastructure/step_function_x.json"
    (repo / "baseline.json").write_text(lint.render_baseline({
        path: {"Old": ["task-not-an-entrypoint"]},
    }))
    (repo / "infrastructure" / "step_function_x.json").write_text(json.dumps({
        "StartAt": "New", "States": {"New": _task("arn:aws:states:::sns:publish")},
    }))
    assert _run(repo) == 1
