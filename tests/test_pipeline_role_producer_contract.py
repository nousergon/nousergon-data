"""Every ``pipeline_role`` this repo puts into an SF execution input is in the lib vocabulary.

alpha-engine-config-I5592 step 2, under Brian's ruling of 2026-10-24, "(a) Execution
input". ``pipeline_role`` is minted here and allow-listed by consumers (the
crucible-dashboard Fleet Status filters, ``nousergon_lib.pipeline_status.read``'s
``role_filter`` walk, the sf-watch dispatcher). When a role is minted outside the
shared vocabulary, every consumer's filter silently fails to match it. On 2026-07-29
``pipeline_role="exercise"`` shipped and Fleet Status reported a 4-day-old FAILED
cadence run as current while an exercise run was executing.

So the contract is checked here, in the repo that mints the role, at mint time: every
literal is checked against ``nousergon_lib.pipeline_status.roles.ALL_ROLES``.

SCOPE IS EXECUTION INPUT ONLY, BY A STATED RULE AND NOT BY AN ALLOW-LIST
------------------------------------------------------------------------
A value is in scope when it is one of:

1. A ``pipeline_role`` a **declared entry-contract caller** sends. Callers are read
   from ``infrastructure/sf_entry_contract.json``, which
   ``test_sf_entry_contract_matches_callers.py`` already pins against every in-repo
   StartExecution.
2. A ``pipeline_role`` a definition sends in the ``Input`` of a
   ``states:startExecution`` Task (``LaunchWeeklyExerciseRun``).
3. A value a definition's Choice rule compares ``$.pipeline_role`` against. That is
   the SF reading its own execution input, so an out-of-vocabulary literal there is
   a branch no conforming caller can reach.
4. A ``pipeline_role`` a Python module under ``scripts/`` or
   ``infrastructure/lambdas/`` places in input it passes to ``start_execution``, or
   prints for an operator's ``start-execution``. These are the rerun, replay and
   proof-input tools. The module names one of those two calls and is not a test. A
   value resolved through another module's attribute (``_rerun.EMITTED_ROLE``) is
   checked where that module defines it.

The same key name also appears in a second namespace, the spot-dispatch payload and
EC2-tag role. Examples are ``weekly-thinktank-gap-fill``, ``weekly-eval-judge``,
``preflight-sweep`` (which an IAM condition keys on) and the Map ``$.collection``
role. Those are Lambda payloads, not execution input, and rule 2's
``states:startExecution`` restriction is what keeps them out. Per the ruling there is
no second vocabulary. ``test_the_dispatch_namespace_stays_out_of_scope`` pins that the
exclusion is the rule's doing and not an accident of the walk.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

# A plain import, never importorskip: a skip would be a silent pass.
from nousergon_lib.pipeline_status import roles

_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _ROOT / "infrastructure"
_CONTRACT = json.loads((_INFRA / "sf_entry_contract.json").read_text())

# A pipeline_role key followed by a literal value, in JSON, in CFN-escaped nested JSON
# (\"pipeline_role\": \"weekly\") or in a Python dict literal.
_LITERAL_RE = re.compile(r'\\?["\']pipeline_role\\?["\']\s*:\s*\\?["\']([A-Za-z0-9_.-]+)\\?["\']')
_START_EXEC_RE = re.compile(r"states:::states:startExecution")


def _caller_paths() -> list[Path]:
    paths: set[Path] = set()
    for name, entry in _CONTRACT.items():
        if name.startswith("_"):
            continue
        for caller in entry.get("callers", []):
            rel = caller.split(" :: ", 1)[0].strip()
            if "PASS-THROUGH" in caller:
                continue  # replays the original input verbatim; it mints nothing
            paths.add(_ROOT / rel)
    return sorted(paths)


def _definitions() -> list[Path]:
    return sorted(_INFRA.glob("step_function*.json")) + sorted(
        (_INFRA / "step-functions").glob("*.json"))


def _walk(node):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v)


def _definition_roles(path: Path) -> list[tuple[str, str]]:
    doc = json.loads(path.read_text())
    out: list[tuple[str, str]] = []
    for node in _walk(doc):
        # Rule 3: Choice rules on the SF's own input.
        if node.get("Variable") == "$.pipeline_role":
            for op in ("StringEquals",):
                if isinstance(node.get(op), str):
                    out.append((node[op], f"{path.name} Choice on $.pipeline_role"))
        # Rule 2: the Input of a child StartExecution.
        if _START_EXEC_RE.search(str(node.get("Resource", ""))):
            inp = (node.get("Parameters") or node.get("Arguments") or {}).get("Input")
            if isinstance(inp, dict) and isinstance(inp.get("pipeline_role"), str):
                out.append((inp["pipeline_role"], f"{path.name} startExecution Input"))
    return out


def _module_constants(tree: ast.Module) -> dict[str, str]:
    consts: dict[str, str] = {}
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Constant) \
                and isinstance(stmt.value.value, str):
            for t in stmt.targets:
                if isinstance(t, ast.Name):
                    consts[t.id] = stmt.value.value
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) \
                and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
            consts[stmt.target.id] = stmt.value.value
    return consts


def _python_roles(path: Path) -> list[tuple[str, str]]:
    tree = ast.parse(path.read_text())
    consts = _module_constants(tree)
    rel = str(path.relative_to(_ROOT))

    def value(node) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return consts.get(node.id)
        return None

    out: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for k, v in zip(node.keys, node.values, strict=True):
                if isinstance(k, ast.Constant) and k.value == "pipeline_role" \
                        and (s := value(v)) is not None:
                    out.append((s, f"{rel} dict literal"))
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant) \
                        and t.slice.value == "pipeline_role" \
                        and (s := value(node.value)) is not None:
                    out.append((s, f"{rel} subscript assignment"))
        elif isinstance(node, ast.keyword) and node.arg in ("pipeline_role", "emitted_role") \
                and (s := value(node.value)) is not None:
            out.append((s, f"{rel} keyword {node.arg}="))
    return out


def _rerun_modules() -> list[Path]:
    found = []
    for base in (_ROOT / "scripts", _INFRA / "lambdas"):
        for path in sorted(base.rglob("*.py")):
            if path.name.startswith("test_") or "/tests/" in str(path):
                continue
            text = path.read_text(errors="ignore")
            if ("start_execution" in text or "start-execution" in text) \
                    and "pipeline_role" in text:
                found.append(path)
    return found


def _minted() -> list[tuple[str, str]]:
    minted: list[tuple[str, str]] = []
    for path in _caller_paths():
        if path.suffix == ".py":
            minted += _python_roles(path)
        else:
            rel = str(path.relative_to(_ROOT))
            minted += [(m, f"{rel} literal") for m in _LITERAL_RE.findall(path.read_text())]
    for path in _definitions():
        minted += _definition_roles(path)
    for path in _rerun_modules():
        minted += _python_roles(path)
    return minted


def test_every_execution_input_role_is_in_the_lib_vocabulary():
    outside = sorted({(r, where) for r, where in _minted() if r not in roles.ALL_ROLES})
    assert not outside, (
        "pipeline_role values minted into SF execution input but absent from "
        "nousergon_lib.pipeline_status.roles.ALL_ROLES. Every consumer's role filter "
        "will silently fail to match them (alpha-engine-config-I5592). Add the role to "
        "the lib vocabulary, in exactly one bucket, and bump the pin:\n  "
        + "\n  ".join(f"{r!r}  <- {w}" for r, w in outside))


def test_the_walk_is_not_vacuous():
    """A broken extractor must fail loudly rather than pass on an empty set.

    These are the roles the tree is known to mint into execution input, measured
    2026-10-24. If a launch path is retired, drop its role here in the same change.
    """
    found = {r for r, _ in _minted()}
    expected = {"weekly", "daily", "eod", "exercise", "shell-run", "watch-rerun",
                "operator-replay"}
    missing = expected - found
    assert not missing, f"the producer walk no longer finds {sorted(missing)}; it is broken"


def test_every_source_class_contributes():
    wheres = [w for _, w in _minted()]
    assert any("orchestration.yaml" in w for w in wheres), "rule 1 (CFN callers) found nothing"
    assert any("startExecution Input" in w for w in wheres), "rule 2 found nothing"
    assert any("Choice on $.pipeline_role" in w for w in wheres), "rule 3 found nothing"
    assert any(w.startswith("scripts/") for w in wheres), "rule 4 (rerun tools) found nothing"


def test_exercise_is_never_a_recovery_role():
    """The issue's binding constraint: a recovery role can mark a cadence cycle
    COMPLETE, and an exercise run must never clear a failed Saturday cadence run."""
    assert "exercise" in roles.EXERCISE_ROLES
    assert "exercise" not in roles.RECOVERY_ROLES


def test_the_dispatch_namespace_stays_out_of_scope():
    """The ruling excludes the spot-dispatch / EC2-tag role namespace by rule.

    Pin that those values really are present in the tree, so the rule is doing the
    excluding, and that the walk does not pick them up.
    """
    sf = (_INFRA / "step_function.json").read_text()
    assert "weekly-eval-judge" in sf, "dispatch-namespace fixture moved; re-measure this test"
    found = {r for r, _ in _minted()}
    for dispatch_role in ("weekly-thinktank-gap-fill", "weekly-eval-judge", "preflight-sweep"):
        assert dispatch_role not in found, (
            f"{dispatch_role!r} is a Lambda dispatch payload role, not execution input; "
            f"the walk has widened past the ruled scope")
