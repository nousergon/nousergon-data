"""Enforce the S-46 portability seam for Step Functions definitions.

The lint walks every state (including nested Parallel and Map states) in the
repository's established definition globs. Tasks may call only registered
Lambda, Step Functions, or ECS entrypoints. Choices may inspect only
``$.status``, ``$.ok``, or ``$.error``; retries are limited to the five
transport errors in ``TRANSPORT_ERRORS`` and at most three attempts. It also
rejects non-terminal catches, multi-state Parallel branches, data-derived
Maps and Waits, and computing Step Functions intrinsics.

Existing findings live in a shrink-only baseline keyed by definition path,
full nested state path, and rule. Enforcing mode is the default, so either a
new finding or a stale entry fails CI. ``--update`` can only intersect the
baseline with current findings and therefore cannot add or substitute an
entry.

Run from the repository root with ``python3 validators/sf_entrypoint_lint.py``.
The implementation is stdlib-only.
"""

# ASL is recursively shaped, schemaless JSON. Keep strict checking for concrete
# values while permitting the decoded JSON boundary used throughout this module.
# pyright: reportExplicitAny=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false, reportMissingTypeArgument=false

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

REPO_ROOT = Path(__file__).resolve().parent.parent
DECLARED_PATH = REPO_ROOT / "validators" / "sf_entrypoints.json"
BASELINE_PATH = REPO_ROOT / "validators" / "sf_entrypoint_baseline.json"
DEFINITION_GLOBS = ("infrastructure/step_function*.json", "infrastructure/step-functions/*.asl.json")

GUARD = "sf-entrypoint-lint"
TRACKER = "alpha-engine-config-I12533"
SCHEMA_VERSION = 2
# Definitions graduated from observe to enforce (grow-only; see the docstring).
# step_function_groom.json (alpha-engine-groom-dispatch) first, 2026-10-23:
# off the weekly and trading paths, and its baseline was exact (0 new, 0 stale).
# step-functions/data-collection.asl.json (the four ne-data-collection-* state
# machines) second, 2026-10-24: the smallest remaining baseline (3 + 6), exact
# on main. Enforcing changes no definition; it only stops the count rising.
# step_function_daily.json third, 2026-11-10: smallest remaining baseline, exact on main; enforcing changes no definition.
ENFORCED_DEFINITIONS: frozenset[str] = frozenset({
    "infrastructure/step_function_daily.json",
    "infrastructure/step_function_groom.json",
    "infrastructure/step-functions/data-collection.asl.json",
})
RULES = (
    "task-undeclared-entrypoint",
    "task-not-an-entrypoint",
    "task-inline-shell-logic",
    "choice-beyond-outcome",
    "retry-business-logic",
    "catch-business-logic",
    "wait-dynamic",
    "map-data-derived",
    "computing-intrinsic",
    "parallel-business-logic",
)

LAMBDA_PREFIX = "arn:aws:states:::lambda:invoke"
STATES_PREFIX = "arn:aws:states:::states:startExecution"
SSM_SEND = "arn:aws:states:::aws-sdk:ssm:sendCommand"
ECS_PREFIX = "arn:aws:states:::ecs:runTask"
TRANSPORT_ERRORS = frozenset({
    "Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException",
    "Lambda.TooManyRequestsException", "States.Timeout",
})
COMPUTING_INTRINSICS = re.compile(r"States\.(?:MathAdd|ArrayGetItem|StringSplit|Format)\s*\(")

# Leaf names a Choice may branch on: a task's own outcome, never a decision.
OUTCOME_FIELDS = frozenset({
    "status", "statusdetails", "statuscode", "responsecode", "outcome",
    "ok", "success", "succeeded", "failed", "error", "errors", "cause",
    "exit_code", "exitcode",
})
OUTCOME_OPS = frozenset({
    "StringEquals", "BooleanEquals", "NumericEquals", "IsPresent", "IsNull",
})

SHELL_LOGIC = re.compile(
    r"(?:^|[\s;'\"(])(?P<kw>if|while|for|until|case)\s"
    r"|(?P<op>&&|\|\||\(\)\s*\{)"
)

Counts = dict[str, dict[str, list[str]]]


# --------------------------------------------------------------------------
# Scan
# --------------------------------------------------------------------------


def definition_files(repo: Path) -> list[Path]:
    out: set[Path] = set()
    for pattern in DEFINITION_GLOBS:
        out.update(repo.glob(pattern))
    return sorted(out)


def walk_states(states: dict, prefix: str = "") -> Iterator[tuple[str, dict]]:
    """Every state, depth first, named by its path through Parallel/Map."""
    for name, state in states.items():
        path = f"{prefix}{name}"
        yield path, state
        if state.get("Type") == "Parallel":
            for i, branch in enumerate(state.get("Branches") or []):
                yield from walk_states(branch.get("States") or {}, f"{path}[{i}]/")
        elif state.get("Type") == "Map":
            inner = state.get("ItemProcessor") or state.get("Iterator") or {}
            yield from walk_states(inner.get("States") or {}, f"{path}/")


def lambda_name(value: str) -> str:
    """``arn:...:function:NAME[:alias]`` or ``NAME[:alias]`` -> ``NAME``."""
    if ":function:" in value:
        value = value.split(":function:", 1)[1]
    return value.split(":", 1)[0]


def state_machine_name(value: str) -> str:
    return value.rsplit(":stateMachine:", 1)[-1] if ":stateMachine:" in value else value


def _rule_leaves(rule: dict) -> Iterator[tuple[str | None, list[str]]]:
    for key in ("And", "Or"):
        if key in rule:
            for sub in rule[key]:
                yield from _rule_leaves(sub)
            return
    if "Not" in rule:
        yield from _rule_leaves(rule["Not"])
        return
    ops = [k for k in rule if k not in ("Variable", "Next", "Comment", "Assign", "Output")]
    yield rule.get("Variable"), ops


def choice_is_outcome_only(state: dict) -> bool:
    for rule in state.get("Choices") or []:
        if "Condition" in rule:
            return False
        for variable, ops in _rule_leaves(rule):
            path = str(variable or "")
            leaf = path.rsplit(".", 1)[-1].lower()
            if path not in {"$.status", "$.ok", "$.error"} or not ops or any(op not in OUTCOME_OPS for op in ops):
                return False
            if "IsPresent" in ops and leaf != "error":
                return False
    return True


def classify_task(state: dict, declared: dict[str, set[str]]) -> tuple[str, str] | None:
    """``(rule, detail)`` for a Task that breaks a rule, else ``None``."""
    resource = str(state.get("Resource") or "")
    params = cast(dict[str, Any], state.get("Parameters") or state.get("Arguments") or {})
    if resource.startswith(LAMBDA_PREFIX):
        if "FunctionName" not in params:
            return "task-undeclared-entrypoint", "lambda <dynamic FunctionName>"
        name = lambda_name(str(params["FunctionName"]))
        if name not in declared["lambda_functions"]:
            return "task-undeclared-entrypoint", f"lambda {name}"
        return None
    if resource.startswith(STATES_PREFIX):
        if "StateMachineArn" not in params:
            return "task-undeclared-entrypoint", "state machine <dynamic StateMachineArn>"
        name = state_machine_name(str(params["StateMachineArn"]))
        if name not in declared["state_machines"]:
            return "task-undeclared-entrypoint", f"state machine {name}"
        return None
    if resource.startswith(ECS_PREFIX):
        job = params.get("JobName") or params.get("job")
        if not isinstance(job, str) or job not in declared.get("jobs", set()):
            return "task-undeclared-entrypoint", f"ECS job {job or '<dynamic>'}"
        return None
    if resource == SSM_SEND:
        text = json.dumps(params.get("Parameters") or {})
        match = SHELL_LOGIC.search(text.replace("\\n", "\n"))
        if match:
            token = match.group("kw") or match.group("op")
            return "task-inline-shell-logic", f"shell control flow {token!r}"
        return None
    return "task-not-an-entrypoint", resource.removeprefix("arn:aws:states:::")


def _intrinsic(value: object) -> bool:
    if isinstance(value, str):
        return COMPUTING_INTRINSICS.search(value) is not None
    if isinstance(value, dict):
        return any(_intrinsic(v) for v in cast(dict[str, object], value).values())
    if isinstance(value, list):
        return any(_intrinsic(v) for v in cast(list[object], value))
    return False


def _retry_is_transport_only(state: dict[str, Any]) -> bool:
    for retry in state.get("Retry") or []:
        errors = retry.get("ErrorEquals") or []
        attempts = retry.get("MaxAttempts", 3)
        if not errors or any(error not in TRANSPORT_ERRORS for error in errors):
            return False
        if not isinstance(attempts, int) or attempts > 3:
            return False
    return True


def _catch_is_allowed(
    state: dict[str, Any], states: dict[str, dict[str, Any]], declared: dict[str, set[str]],
) -> bool:
    for catch in state.get("Catch") or []:
        target = states.get(catch.get("Next"), {})
        if target.get("Type") == "Fail":
            continue
        if target.get("Type") != "Task" or classify_task(target, declared) is not None:
            return False
        name = str(target.get("Parameters", {}).get("FunctionName", "")).lower()
        if "notify" not in name and "notifier" not in name:
            return False
    return True


def _parallel_is_allowed(state: dict[str, Any], declared: dict[str, set[str]]) -> bool:
    branches = state.get("Branches") or []
    return bool(branches) and all(
        len(branch.get("States") or {}) == 1
        and next(iter((branch.get("States") or {}).values())).get("Type") == "Task"
        and classify_task(next(iter((branch.get("States") or {}).values())), declared) is None
        for branch in branches
    )


def findings(repo: Path, declared: dict[str, set[str]]) -> list[dict]:
    out: list[dict] = []
    for path in definition_files(repo):
        rel = path.relative_to(repo).as_posix()
        doc = json.loads(path.read_text(encoding="utf-8"))
        top_states = cast(dict[str, dict[str, Any]], doc.get("States") or {})
        containers: dict[int, dict[str, dict[str, Any]]] = {}

        def index(states: dict[str, dict[str, Any]]) -> None:
            for candidate in states.values():
                containers[id(candidate)] = states
                if candidate.get("Type") == "Parallel":
                    for branch in candidate.get("Branches") or []:
                        index(cast(dict[str, dict[str, Any]], branch.get("States") or {}))
                elif candidate.get("Type") == "Map":
                    inner = candidate.get("ItemProcessor") or candidate.get("Iterator") or {}
                    index(cast(dict[str, dict[str, Any]], inner.get("States") or {}))

        index(top_states)
        for state_path, state in walk_states(top_states):
            typed_state = cast(dict[str, Any], state)
            kind = state.get("Type")
            hit: tuple[str, str] | None = None
            if kind == "Task":
                hit = classify_task(state, declared)
            elif kind == "Choice" and not choice_is_outcome_only(state):
                hit = "choice-beyond-outcome", "branches on a decision, not an outcome"
            if hit:
                out.append({"path": rel, "state": state_path, "rule": hit[0], "detail": hit[1]})
            extra: list[tuple[str, str]] = []
            if not _retry_is_transport_only(typed_state):
                extra.append(("retry-business-logic", "retry is not limited to transport errors and three attempts"))
            if state.get("Catch") and not _catch_is_allowed(typed_state, containers[id(state)], declared):
                extra.append(("catch-business-logic", "catch does not route to Fail or a notify Task"))
            if kind == "Wait" and ("SecondsPath" in state or "TimestampPath" in state):
                extra.append(("wait-dynamic", "dynamic scheduling belongs outside ASL"))
            if kind == "Map" and any(key in state for key in ("ItemsPath", "Items", "Items.$")):
                extra.append(("map-data-derived", "Map iterates over data"))
            if kind == "Parallel" and not _parallel_is_allowed(typed_state, declared):
                extra.append(("parallel-business-logic", "each branch must contain one allowed Task"))
            if _intrinsic(state):
                extra.append(("computing-intrinsic", "ASL computes a value"))
            out.extend(
                {"path": rel, "state": state_path, "rule": rule, "detail": detail}
                for rule, detail in extra
            )
    return out


def counts_of(rows: list[dict]) -> Counts:
    out: Counts = {}
    for row in rows:
        per = out.setdefault(row["path"], {})
        per.setdefault(row["state"], []).append(row["rule"])
    for per in out.values():
        for rules in per.values():
            rules.sort()
    return out


def load_declared(path: Path) -> dict[str, set[str]]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, set[str]] = {}
    for key in ("lambda_functions", "state_machines", "jobs"):
        entries = doc.get(key)
        if key == "jobs" and entries is None:
            entries = {}
        if not isinstance(entries, dict):
            raise SystemExit(f"{path}: {key!r} must be an object of name -> note")
        out[key] = set(entries)
    return out


# --------------------------------------------------------------------------
# Baseline (shrink-only, same shape as the S-04 guard)
# --------------------------------------------------------------------------


def totals(counts: Counts) -> dict[str, int]:
    out: dict[str, int] = dict.fromkeys(RULES, 0)
    for per in counts.values():
        for rules in per.values():
            for rule in rules:
                out[rule] += 1
    out["files"] = len(counts)
    return out


def load_baseline(path: Path) -> Counts:
    if not path.exists():
        return {}
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("schema_version") != SCHEMA_VERSION or doc.get("guard") != GUARD:
        raise SystemExit(f"{path}: not a {GUARD} baseline (schema_version {SCHEMA_VERSION})")
    files = doc.get("files")
    if not isinstance(files, dict):
        raise SystemExit(f"{path}: 'files' must be an object")
    out: Counts = {}
    for rel, per in files.items():
        if not isinstance(per, dict) or any(
            not isinstance(state, str) or not isinstance(rules, list)
            or not rules or any(rule not in RULES for rule in rules)
            for state, rules in per.items()
        ):
            raise SystemExit(f"{path}: bad entry for {rel!r}: {per!r}")
        out[rel] = {state: sorted(rules) for state, rules in per.items()}
    return out


def render_baseline(counts: Counts) -> str:
    doc = {
        "schema_version": SCHEMA_VERSION,
        "guard": GUARD,
        "tracker": TRACKER,
        "note": (
            "Shrink-only ceiling of S-46 findings per SF definition per rule. Never raise an "
            "entry; lower it with `python3 validators/sf_entrypoint_lint.py --update` when a "
            "finding is removed."
        ),
        "totals": totals(counts),
        "files": {rel: {state: counts[rel][state] for state in sorted(counts[rel])}
                  for rel in sorted(counts)},
    }
    return json.dumps(doc, indent=2) + "\n"


def compare(current: Counts, baseline: Counts) -> tuple[list[dict], list[dict]]:
    """``(new, stale)`` rows, each ``{path, rule, baseline, current}``."""
    new, stale = [], []
    for rel in sorted(set(current) | set(baseline)):
        cur, base = current.get(rel, {}), baseline.get(rel, {})
        for state in sorted(set(cur) | set(base)):
            current_rules = set(cur.get(state, []))
            baseline_rules = set(base.get(state, []))
            new.extend({"path": rel, "state": state, "rule": rule, "baseline": 0, "current": 1}
                       for rule in sorted(current_rules - baseline_rules))
            stale.extend({"path": rel, "state": state, "rule": rule, "baseline": 1, "current": 0}
                         for rule in sorted(baseline_rules - current_rules))
    return new, stale


def lowered(current: Counts, baseline: Counts) -> Counts:
    """The baseline lowered to what reproduces; never raised."""
    out: Counts = {}
    for rel, base in baseline.items():
        cur = current.get(rel, {})
        per = {state: sorted(set(rules) & set(cur.get(state, [])))
               for state, rules in base.items()}
        per = {state: rules for state, rules in per.items() if rules}
        if per:
            out[rel] = per
    return out


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def _esc(s: str) -> str:
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def is_enforced(path: str, mode: str) -> bool:
    """A finding on ``path`` blocks in ``mode``."""
    return mode == "enforce" or path in ENFORCED_DEFINITIONS


def report(rows: list[dict], new: list[dict], stale: list[dict], mode: str) -> list[str]:
    t = totals(counts_of(rows))
    lines = [
        f"{GUARD} ({mode}): {len(rows)} finding(s) in {t['files']} definition(s) "
        f"[{', '.join(f'{r}={t[r]}' for r in RULES)}]; "
        f"{len(new)} new, {len(stale)} stale vs baseline."
    ]
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    for row in new:
        states = [
            f"{f['state']} ({f['detail']})"
            for f in rows if f["path"] == row["path"] and f["rule"] == row["rule"]
        ]
        msg = (
            f"{row['path']}: {row['rule']} {row['baseline']} -> {row['current']}. "
            f"States: {'; '.join(states[:5])}"
        )
        lines.append(("NEW   " if not is_enforced(row["path"], mode) else "NEW (enforced) ") + msg)
        if in_actions:
            level = "error" if is_enforced(row["path"], mode) else "warning"
            print(f"::{level} file={row['path']},title={GUARD}::{_esc(msg)}")
    for row in stale:
        tag = "STALE (enforced)" if is_enforced(row["path"], mode) else "STALE"
        lines.append(
            f"{tag} {row['path']}: {row['rule']} baseline {row['baseline']} but {row['current']} "
            "now. Lower the ceiling: `python3 validators/sf_entrypoint_lint.py --update`"
        )
    if in_actions and stale:
        level = "error" if any(is_enforced(r["path"], mode) for r in stale) else "warning"
        print(f"::{level} title={GUARD}::{len(stale)} baseline entr(y/ies) no longer reproduce; "
              "run --update to lower the baseline (shrink-only)")
    if mode == "observe" and (new or stale):
        if any(is_enforced(r["path"], mode) for r in new + stale):
            lines.append(
                "observe mode, but an ENFORCED definition changed its count: failing. "
                f"Enforced: {', '.join(sorted(ENFORCED_DEFINITIONS))}"
            )
        else:
            lines.append("observe mode: reporting only.")
    return lines


def write_summary(lines: list[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"### {GUARD} ({TRACKER})\n\n```\n" + "\n".join(lines[:200]) + "\n```\n")
    except OSError:
        pass


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--repo", default=str(REPO_ROOT), help="repo root")
    ap.add_argument("--declared", help="declared entrypoints JSON")
    ap.add_argument("--baseline", help="baseline JSON")
    ap.add_argument("--mode", choices=("observe", "enforce"), default="enforce")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--init", action="store_true", help="write the first baseline; refuses if one exists")
    g.add_argument("--update", action="store_true", help="lower the baseline to what reproduces; never raises")
    g.add_argument("--json", action="store_true", help="print every finding as JSON")
    args = ap.parse_args(argv)

    repo = Path(args.repo).resolve()
    declared = load_declared(Path(args.declared) if args.declared else repo / DECLARED_PATH.relative_to(REPO_ROOT))
    bpath = Path(args.baseline) if args.baseline else repo / BASELINE_PATH.relative_to(REPO_ROOT)
    rows = findings(repo, declared)
    current = counts_of(rows)

    if args.json:
        print(json.dumps({"totals": totals(current), "findings": rows}, indent=2))
        return 0
    if args.init:
        if bpath.exists():
            print(f"{bpath} exists; use --update (shrink-only)", file=sys.stderr)
            return 2
        bpath.write_text(render_baseline(current))
        print(f"wrote {bpath}: {totals(current)}")
        return 0

    baseline = load_baseline(bpath)
    if args.update:
        lowered_counts = lowered(current, baseline)
        bpath.write_text(render_baseline(lowered_counts))
        print(f"wrote {bpath}: {totals(lowered_counts)}")
        return 0

    new, stale = compare(current, baseline)
    lines = report(rows, new, stale, args.mode)
    print("\n".join(lines))
    write_summary(lines)
    if any(is_enforced(r["path"], args.mode) for r in new + stale):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
