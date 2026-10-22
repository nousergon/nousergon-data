"""Step Functions states call declared entrypoints only (plan S-46, observe mode).

alpha-engine-config-I12533, under epic alpha-engine-config-I12488 (staging-first
environments, portability seam 4): "No business logic in Step Functions. Each
state calls one entrypoint; ordering, retries and gates live in Python, as
``crucible.daily_chain`` already does. That keeps the SF definitions thin and
replaceable."

This lint walks every state, including Parallel branches and Map item
processors, of every definition in this repo
(``infrastructure/step_function*.json`` and
``infrastructure/step-functions/*.asl.json``) and reports four rules:

``task-undeclared-entrypoint``
    A Task invokes a Lambda function or a state machine that is not declared in
    ``validators/sf_entrypoints.json``, or names it dynamically (``.$``), so
    no declaration can cover it. Adding an entrypoint is a deliberate,
    reviewable line in that file, not a side effect of an ASL edit.
``task-not-an-entrypoint``
    A Task calls an AWS service directly (``sns:publish``,
    ``aws-sdk:ec2:startInstances``, ``aws-sdk:ssm:getCommandInvocation`` and so
    on). That is plumbing or a poll loop written in ASL. Only
    ``lambda:invoke``, ``states:startExecution`` and ``ssm:sendCommand`` are
    entrypoint-shaped.
``task-inline-shell-logic``
    An ``ssm:sendCommand`` whose command text carries shell control flow
    (``if``/``while``/``for``/``until``/``case``, ``&&``, ``||``, a shell
    function). The box should run one entrypoint, and the logic belongs in
    that entrypoint.
``choice-beyond-outcome``
    A Choice that branches on anything other than a task's outcome. A rule
    may test only an outcome field (``Status``, ``StatusDetails``, ``status``,
    ``ResponseCode``, ``outcome``, ``ok``, ``success``, ``error`` and similar)
    by equality or presence. Anything else, such as a skip flag, a role, a
    deadline or a counter, is a gate or ordering decision written in ASL.
    JSONata ``Condition`` rules cannot be classified and are reported.

The existing violations are recorded in a shrink-only baseline
(``validators/sf_entrypoint_baseline.json``, a count per definition file per
rule), mirroring the S-04 hard-coded-environment guard
(nousergon-lib ``scripts/hardcoded_env_refs.py``). A count above its baseline
is NEW, and a count below it is STALE. Lower the ceiling with ``--update``,
which never raises an entry.

OBSERVE mode, the default, reports in the log, as ``::warning``
annotations and in the job summary, and exits 0. ``--mode enforce`` exits 1
on any NEW or STALE finding.

Run locally from the repo root: ``python3 validators/sf_entrypoint_lint.py``
(``--json`` lists every finding by state). Stdlib only.
CI: ``.github/workflows/ci.yml`` (``ssm-log-capture-contract-lint`` job).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Iterator
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DECLARED_PATH = REPO_ROOT / "validators" / "sf_entrypoints.json"
BASELINE_PATH = REPO_ROOT / "validators" / "sf_entrypoint_baseline.json"
DEFINITION_GLOBS = ("infrastructure/step_function*.json", "infrastructure/step-functions/*.asl.json")

GUARD = "sf-entrypoint-lint"
TRACKER = "alpha-engine-config-I12533"
SCHEMA_VERSION = 1
RULES = (
    "task-undeclared-entrypoint",
    "task-not-an-entrypoint",
    "task-inline-shell-logic",
    "choice-beyond-outcome",
)

LAMBDA_PREFIX = "arn:aws:states:::lambda:invoke"
STATES_PREFIX = "arn:aws:states:::states:startExecution"
SSM_SEND = "arn:aws:states:::aws-sdk:ssm:sendCommand"

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

Counts = dict[str, dict[str, int]]


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
            leaf = str(variable or "").rsplit(".", 1)[-1].lower()
            if leaf not in OUTCOME_FIELDS or not ops or any(op not in OUTCOME_OPS for op in ops):
                return False
    return True


def classify_task(state: dict, declared: dict[str, set[str]]) -> tuple[str, str] | None:
    """``(rule, detail)`` for a Task that breaks a rule, else ``None``."""
    resource = str(state.get("Resource") or "")
    params = state.get("Parameters") or state.get("Arguments") or {}
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
    if resource == SSM_SEND:
        text = json.dumps(params.get("Parameters") or {})
        match = SHELL_LOGIC.search(text.replace("\\n", "\n"))
        if match:
            token = match.group("kw") or match.group("op")
            return "task-inline-shell-logic", f"shell control flow {token!r}"
        return None
    return "task-not-an-entrypoint", resource.removeprefix("arn:aws:states:::")


def findings(repo: Path, declared: dict[str, set[str]]) -> list[dict]:
    out: list[dict] = []
    for path in definition_files(repo):
        rel = path.relative_to(repo).as_posix()
        doc = json.loads(path.read_text(encoding="utf-8"))
        for state_path, state in walk_states(doc.get("States") or {}):
            kind = state.get("Type")
            hit: tuple[str, str] | None = None
            if kind == "Task":
                hit = classify_task(state, declared)
            elif kind == "Choice" and not choice_is_outcome_only(state):
                hit = "choice-beyond-outcome", "branches on a decision, not an outcome"
            if hit:
                out.append({"path": rel, "state": state_path, "rule": hit[0], "detail": hit[1]})
    return out


def counts_of(rows: list[dict]) -> Counts:
    out: Counts = {}
    for row in rows:
        per = out.setdefault(row["path"], {})
        per[row["rule"]] = per.get(row["rule"], 0) + 1
    return out


def load_declared(path: Path) -> dict[str, set[str]]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, set[str]] = {}
    for key in ("lambda_functions", "state_machines"):
        entries = doc.get(key)
        if not isinstance(entries, dict):
            raise SystemExit(f"{path}: {key!r} must be an object of name -> note")
        out[key] = set(entries)
    return out


# --------------------------------------------------------------------------
# Baseline (shrink-only, same shape as the S-04 guard)
# --------------------------------------------------------------------------


def totals(counts: Counts) -> dict[str, int]:
    out = dict.fromkeys(RULES, 0)
    for per in counts.values():
        for rule, n in per.items():
            out[rule] += n
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
            r not in RULES or not isinstance(n, int) or n < 1 for r, n in per.items()
        ):
            raise SystemExit(f"{path}: bad entry for {rel!r}: {per!r}")
        out[rel] = dict(per)
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
        "files": {
            rel: {r: counts[rel][r] for r in RULES if r in counts[rel]} for rel in sorted(counts)
        },
    }
    return json.dumps(doc, indent=2) + "\n"


def compare(current: Counts, baseline: Counts) -> tuple[list[dict], list[dict]]:
    """``(new, stale)`` rows, each ``{path, rule, baseline, current}``."""
    new, stale = [], []
    for rel in sorted(set(current) | set(baseline)):
        cur, base = current.get(rel, {}), baseline.get(rel, {})
        for rule in RULES:
            c, b = cur.get(rule, 0), base.get(rule, 0)
            row = {"path": rel, "rule": rule, "baseline": b, "current": c}
            if c > b:
                new.append(row)
            elif c < b:
                stale.append(row)
    return new, stale


def lowered(current: Counts, baseline: Counts) -> Counts:
    """The baseline lowered to what reproduces; never raised."""
    out: Counts = {}
    for rel, base in baseline.items():
        cur = current.get(rel, {})
        per = {r: min(n, cur.get(r, 0)) for r, n in base.items()}
        per = {r: n for r, n in per.items() if n}
        if per:
            out[rel] = per
    return out


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def _esc(s: str) -> str:
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def report(rows: list[dict], new: list[dict], stale: list[dict], mode: str) -> list[str]:
    level = "error" if mode == "enforce" else "warning"
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
        lines.append("NEW   " + msg)
        if in_actions:
            print(f"::{level} file={row['path']},title={GUARD}::{_esc(msg)}")
    for row in stale:
        lines.append(
            f"STALE {row['path']}: {row['rule']} baseline {row['baseline']} but {row['current']} "
            "now. Lower the ceiling: `python3 validators/sf_entrypoint_lint.py --update`"
        )
    if in_actions and stale:
        print(f"::{level} title={GUARD}::{len(stale)} baseline entr(y/ies) no longer reproduce; "
              "run --update to lower the baseline (shrink-only)")
    if mode == "observe" and (new or stale):
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
    ap.add_argument("--mode", choices=("observe", "enforce"), default="observe")
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
    if args.mode == "enforce" and (new or stale):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
