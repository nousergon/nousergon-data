"""SSM log-capture contract lint for Step Function definitions (config#4223).

Validates every ``krepis.ssm_log_capture`` invocation embedded in the
``infrastructure/step_function*.json`` definitions against the CLI contract
the krepis module ACTUALLY enforces — pinned empirically against
``krepis.ssm_log_capture.main`` (2026-07-31, config#4223), not against the
issue text: the incident's recollection of the "correct" flag placement
("before ``run``, boolean, not value-taking") does not match the module.
The only form argparse accepts is::

    python -m krepis.ssm_log_capture run --correlation-id <value> --slug <S> \
        --log <L> [--step-name <N>] [--bucket <B>] -- <inner-cmd...>

Verified failure shapes (each is a hard argparse error on the live CLI):

* ``--correlation-id`` BEFORE the ``run`` subcommand —
  ``error: argument cmd: invalid choice: '<value>' (choose from run)``.
* ``--correlation-id`` with no value (boolean-style) —
  ``error: argument --correlation-id: expected one argument``. The option
  is VALUE-TAKING; ``--correlation-id=<value>`` is the alternative form.
* ``--slug`` / ``--log`` missing or valueless (both are required).
* missing ``--`` separator, or an empty inner command after it.

Why this lint exists (config#4223): the 2026-07-25 weekly-SF kill — the
krepis correlation-id chokepoint landed between SF phases and every
``ssm_log_capture`` caller in the pipeline started hard-failing
mid-execution; restoring the invocation shape took 4 re-run attempts. The
module now auto-generates a correlation id when none is declared (so the
*absence* class is healed at the module layer), but a MALFORMED invocation
is still a hard CLI error that would re-kill the same pipeline. This check
turns that class into a CI failure at definition-edit time instead of a
mid-pipeline kill (deployment-time preflight per the issue's "additionally"
scope item).

Run locally from the repo root: ``python3 validators/ssm_log_capture_contract.py``
CI: ``.github/workflows/ci.yml`` (``ssm-log-capture-contract-lint`` job).

Exit codes: 0 clean, 1 violations found.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA_DIR = REPO_ROOT / "infrastructure"

# Element of a ``States.Array(...)`` expression: a single- or double-quoted
# shell string, allowing backslash escapes inside the quotes (States.Format
# renders ``{}`` placeholders as literal text in the definition — the lint
# validates the TEMPLATE, matching how the krepis CLI would parse the
# rendered command).
_QUOTED_ELEMENT_RE = re.compile(
    r"'((?:[^'\\]|\\.)*)'|\"((?:[^\"\\]|\\.)*)\""
)
_SSM_LOG_CAPTURE_RE = re.compile(r"\bssm_log_capture\b")

# Tokens of one shell command element: quoted strings (single or double,
# backslash escapes allowed) or bare non-space runs. Keeps ``--log
# '/var/log/with space.log'`` as two tokens instead of three.
_TOKEN_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|\S+")

REQUIRED_AFTER_RUN = ("--slug", "--log")


def _invocations_in_commands_string(commands: str) -> list[str]:
    """Return every ``ssm_log_capture`` invocation element in a
    ``commands.$`` (``States.Array(...)``) string, verbatim."""
    out = []
    for m in _QUOTED_ELEMENT_RE.finditer(commands):
        element = m.group(1) if m.group(1) is not None else m.group(2)
        if _SSM_LOG_CAPTURE_RE.search(element):
            out.append(element)
    return out


def _invocations_in_definition(definition) -> list[tuple[str, str]]:
    """Walk a parsed SF definition and return ``(state_path, invocation)``
    pairs for every ``commands.$`` string that calls ``ssm_log_capture``.

    ``state_path`` is a dotted key path (e.g. ``States.PredictorTraining``)
    so violations name the exact state that must be fixed.
    """
    found: list[tuple[str, str]] = []

    def walk(node, path: str) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                child = f"{path}.{k}" if path else str(k)
                if k == "commands.$" and isinstance(v, str) and _SSM_LOG_CAPTURE_RE.search(v):
                    for inv in _invocations_in_commands_string(v):
                        found.append((child, inv))
                else:
                    walk(v, child)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{path}[{i}]")

    walk(definition, "")
    return found


def _tokens(invocation: str) -> list[str]:
    return [t.strip("'\"") for t in _TOKEN_RE.findall(invocation)]


def validate_invocation(invocation: str) -> list[str]:
    """Return the contract violations for one invocation (empty = conformant).

    Mirrors the verified argparse behavior of ``krepis.ssm_log_capture.main``
    (config#4223) — see the module docstring for the pinned failure shapes.
    """
    errors: list[str] = []
    tokens = _tokens(invocation)

    if "run" not in tokens:
        errors.append("missing `run` subcommand")
        return errors
    run_idx = tokens.index("run")

    if "--correlation-id" in tokens:
        cid_idx = tokens.index("--correlation-id")
        if cid_idx < run_idx:
            errors.append(
                "`--correlation-id` appears BEFORE the `run` subcommand — "
                "argparse rejects this with `invalid choice: '<value>' "
                "(choose from run)`; it must come AFTER `run`"
            )
        has_value = cid_idx + 1 < len(tokens) and not tokens[cid_idx + 1].startswith("--")
        if not has_value:
            errors.append(
                "`--correlation-id` has no value — it is a VALUE-TAKING "
                "option (not a boolean flag); use `--correlation-id <value>` "
                "or `--correlation-id=<value>` (absence is fine — the module "
                "auto-generates an id — but the valueless form is a hard "
                "argparse error)"
            )
    else:
        # Absence is not a violation: krepis now auto-generates a correlation
        # id with a WARNING (config#4223). It is the module's own degraded
        # posture, not a definition defect.
        pass

    for req in REQUIRED_AFTER_RUN:
        if req not in tokens[run_idx:]:
            errors.append(f"missing required `{req}` argument")
            continue
        idx = tokens.index(req, run_idx)
        if idx + 1 >= len(tokens) or tokens[idx + 1].startswith("--"):
            errors.append(f"`{req}` has no value")

    if "--" not in tokens:
        errors.append("missing `--` separator before the inner command")
    elif not tokens[tokens.index("--") + 1:]:
        errors.append("empty inner command after `--`")

    return errors


def validate_definition(definition, source: str = "<definition>") -> list[str]:
    """Validate one parsed SF definition; returns human-readable violation
    lines (empty = clean)."""
    out = []
    for state_path, invocation in _invocations_in_definition(definition):
        violations = validate_invocation(invocation)
        for v in violations:
            out.append(f"{source}: {state_path}: {v}")
    return out


def main(argv: list[str] | None = None) -> int:
    infra_dir = Path(argv[1]) if argv and len(argv) > 1 else INFRA_DIR
    files = sorted(infra_dir.glob("step_function*.json"))
    if not files:
        print(f"❌ no step_function*.json found under {infra_dir}", file=sys.stderr)
        return 1

    violations: list[str] = []
    checked = 0
    for path in files:
        try:
            definition = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"❌ cannot read {path}: {e}", file=sys.stderr)
            return 1
        violations.extend(validate_definition(definition, source=path.name))
        checked += len(_invocations_in_definition(definition))

    for line in violations:
        print(f"❌ ssm_log_capture contract violation: {line}", file=sys.stderr)
    if violations:
        print(
            f"\n{len(violations)} violation(s) across {checked} ssm_log_capture "
            f"invocation(s) in {len(files)} SF definition(s) — a malformed "
            "invocation is a hard krepis CLI error that would kill the "
            "pipeline mid-execution (config#4223).",
            file=sys.stderr,
        )
        return 1
    print(
        f"✅ {checked} ssm_log_capture invocation(s) across {len(files)} SF "
        "definition(s) conform to the krepis CLI contract."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
