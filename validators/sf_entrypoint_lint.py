"""Enforce S-46: Step Functions contain transport plumbing, not business logic."""
# pyright: reportExplicitAny=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations
import argparse
import json
import re
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DECLARED_PATH = REPO_ROOT / "validators/sf_entrypoints.json"
BASELINE_PATH = REPO_ROOT / "validators/sf_entrypoint_baseline.json"
DEFINITION_GLOBS = ("infrastructure/step_function*.json", "infrastructure/step-functions/*.asl.json")
SCHEMA_VERSION = 2
TRANSPORT_ERRORS = frozenset({"Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException", "States.Timeout"})
COMPUTING_INTRINSICS = re.compile(r"States\.(?:MathAdd|ArrayGetItem|StringSplit|Format)\s*\(")
SUCCESS_FIELDS = frozenset({"status", "ok", "error"})
LAMBDA_PREFIX = "arn:aws:states:::lambda:invoke"
STATES_PREFIX = "arn:aws:states:::states:startExecution"
ECS_PREFIX = "arn:aws:states:::ecs:runTask"
Json = dict[str, Any]
Finding = dict[str, str]

def definition_files(repo: Path) -> list[Path]:
    return sorted({p for pattern in DEFINITION_GLOBS for p in repo.glob(pattern)})

def walk_states(states: Mapping[str, Json], prefix: str = "") -> Iterator[tuple[str, Json]]:
    for name, state in states.items():
        path = f"{prefix}{name}"
        yield path, state
        if state.get("Type") == "Parallel":
            for i, branch in enumerate(state.get("Branches", [])):
                yield from walk_states(branch.get("States", {}), f"{path}[{i}]/")
        elif state.get("Type") == "Map":
            processor = state.get("ItemProcessor") or state.get("Iterator") or {}
            yield from walk_states(processor.get("States", {}), f"{path}/")

def _literal(params: Json, *names: str) -> str | None:
    for name in names:
        value = params.get(name)
        if isinstance(value, str) and not value.startswith("$") and "States." not in value:
            return value
    return None

def _short_name(value: str, marker: str) -> str:
    return value.split(marker, 1)[-1].split(":", 1)[0]

def task_job(state: Json) -> str | None:
    params = state.get("Parameters") or state.get("Arguments") or {}
    resource = str(state.get("Resource", ""))
    if resource.startswith(LAMBDA_PREFIX):
        value = _literal(params, "FunctionName")
        return _short_name(value, ":function:") if value else None
    if resource.startswith(STATES_PREFIX):
        value = _literal(params, "StateMachineArn")
        return _short_name(value, ":stateMachine:") if value else None
    if resource.startswith(ECS_PREFIX):
        overrides = params.get("Overrides", {})
        containers = overrides.get("ContainerOverrides", []) if isinstance(overrides, dict) else []
        if len(containers) == 1 and isinstance(containers[0], dict):
            return _literal(containers[0], "Name")
        return _literal(params, "TaskDefinition")
    return None

def task_allowed(state: Json, declared: Json) -> bool:
    params = state.get("Parameters") or state.get("Arguments") or {}
    resource, job = str(state.get("Resource", "")), task_job(state)
    if resource.startswith(LAMBDA_PREFIX):
        if job in set(declared["dispatcher_lambdas"]):
            dispatched = _literal(params, "job", "Job", "dispatch_unit", "DispatchUnit")
            payload = params.get("Payload", {})
            if dispatched is None and isinstance(payload, dict):
                dispatched = _literal(payload, "job", "Job", "dispatch_unit", "DispatchUnit")
            return dispatched in set(declared["jobs"])
        return job in set(declared["lambda_functions"])
    if resource.startswith(STATES_PREFIX):
        return job in set(declared["state_machines"])
    if resource.startswith(ECS_PREFIX):
        return job in set(declared["jobs"])
    return False

def _choice_leaves(rule: Json) -> Iterator[Json]:
    for connective in ("And", "Or"):
        if connective in rule:
            for child in rule[connective]:
                yield from _choice_leaves(child)
            return
    if "Not" in rule:
        yield from _choice_leaves(rule["Not"])
    else:
        yield rule

def choice_allowed(state: Json) -> bool:
    choices = state.get("Choices", [])
    for choice in choices:
        if "Condition" in choice:
            return False
        for leaf in _choice_leaves(choice):
            variable = leaf.get("Variable")
            field = str(variable).rsplit(".", 1)[-1].lower() if isinstance(variable, str) else ""
            ops = set(leaf) - {"Variable", "Next", "Comment"}
            if field not in SUCCESS_FIELDS or len(ops) != 1 or not ops <= {"StringEquals", "BooleanEquals", "IsPresent"}:
                return False
    return bool(choices)

def _retry_allowed(state: Json) -> bool:
    return all(isinstance(r.get("ErrorEquals"), list) and set(r["ErrorEquals"]) <= TRANSPORT_ERRORS and isinstance(r.get("MaxAttempts", 3), int) and r.get("MaxAttempts", 3) <= 3 for r in state.get("Retry", []))

def _catch_allowed(state: Json, states: Mapping[str, Json], declared: Json) -> bool:
    for catcher in state.get("Catch", []):
        target = states.get(catcher.get("Next", ""), {})
        if target.get("Type") == "Fail":
            continue
        job = task_job(target) or ""
        if target.get("Type") != "Task" or not task_allowed(target, declared) or ("notify" not in job.lower() and "notifier" not in job.lower()):
            return False
    return True

def _parallel_allowed(state: Json, declared: Json) -> bool:
    branches = state.get("Branches", [])
    return bool(branches) and all(len(b.get("States", {})) == 1 and task_allowed(next(iter(b["States"].values())), declared) for b in branches)

def state_findings(state: Json, states: Mapping[str, Json], declared: Json) -> list[str]:
    kind = state.get("Type")
    found: list[str] = []
    if kind == "Task" and not task_allowed(state, declared): found.append("task-undeclared-entrypoint")
    if kind == "Choice" and not choice_allowed(state): found.append("choice-business-logic")
    if kind == "Wait" and {"SecondsPath", "TimestampPath"} & set(state): found.append("dynamic-wait")
    if kind == "Map" and any(k in state for k in ("ItemsPath", "Items", "Items.$")): found.append("data-derived-map")
    if kind == "Parallel" and not _parallel_allowed(state, declared): found.append("parallel-business-logic")
    if not _retry_allowed(state): found.append("business-retry")
    if not _catch_allowed(state, states, declared): found.append("business-catch")
    if COMPUTING_INTRINSICS.search(json.dumps(state)): found.append("computing-intrinsic")
    return found

def findings(repo: Path, declared: Json) -> list[Finding]:
    result: list[Finding] = []
    for path in definition_files(repo):
        doc = json.loads(path.read_text(encoding="utf-8"))
        states = doc.get("States", {})
        for name, state in walk_states(states):
            for rule in state_findings(state, states, declared):
                result.append({"definition": path.relative_to(repo).as_posix(), "state": name, "rule": rule})
    return result

def load_declared(path: Path) -> Json:
    doc = json.loads(path.read_text(encoding="utf-8"))
    for key in ("dispatcher_lambdas", "lambda_functions", "state_machines", "jobs"):
        if not isinstance(doc.get(key), list) or not all(isinstance(x, str) for x in doc[key]):
            raise SystemExit(f"{path}: {key} must be a list of literal names")
    return doc

def finding_keys(rows: list[Finding]) -> set[str]:
    return {f"{r['definition']}::{r['state']}::{r['rule']}" for r in rows}

def load_baseline(path: Path) -> set[str]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("schema_version") != SCHEMA_VERSION or not isinstance(doc.get("findings"), list):
        raise SystemExit(f"{path}: invalid S-46 baseline")
    return set(doc["findings"])

def render_baseline(keys: set[str]) -> str:
    return json.dumps({"schema_version": SCHEMA_VERSION, "guard": "sf-entrypoint-lint", "note": "Existing findings keyed by definition, state and rule; this list may only shrink.", "findings": sorted(keys)}, indent=2) + "\n"

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(REPO_ROOT)); parser.add_argument("--declared"); parser.add_argument("--baseline"); parser.add_argument("--update", action="store_true")
    args = parser.parse_args(argv); repo = Path(args.repo).resolve()
    declared = load_declared(Path(args.declared) if args.declared else repo / "validators/sf_entrypoints.json")
    baseline_path = Path(args.baseline) if args.baseline else repo / "validators/sf_entrypoint_baseline.json"
    current, baseline = finding_keys(findings(repo, declared)), load_baseline(baseline_path)
    new, stale = sorted(current - baseline), sorted(baseline - current)
    if args.update:
        if new: print("refusing to add new findings to the shrink-only baseline", file=sys.stderr); return 1
        baseline_path.write_text(render_baseline(current), encoding="utf-8"); return 0
    for key in new: print(f"NEW {key}")
    for key in stale: print(f"STALE {key}")
    print(f"sf-entrypoint-lint: {len(current)} finding(s); {len(new)} new, {len(stale)} stale")
    return int(bool(new or stale))

if __name__ == "__main__": sys.exit(main())
