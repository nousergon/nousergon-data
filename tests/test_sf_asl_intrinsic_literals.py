"""No single quotes inside ASL intrinsic-function string literals.

AWS Step Functions intrinsic-function string literals (States.Format /
States.Array / ...) CANNOT contain or escape an embedded single quote —
shell-style ``'\\''`` doubling is NOT valid ASL and fails AWS's
SCHEMA_VALIDATION at create/update time with "must be a valid JSONPath or
a valid intrinsic function call". The repo's structural wiring tests
validate JSON shape, not AWS's ASL grammar, so this class is invisible
locally and only detonates at deploy (2026-07-01 first instance,
confirmed live via validate-state-machine-definition; 2026-07-14 second
instance took down the alpha-engine-orchestration CFN update when the
advisory + modelzoo child pipelines failed CREATE — apostrophes inside
their HandleFailure States.Format messages). The full structural fix
(validate every definition against the real AWS API before
UpdateStateMachine) is alpha-engine-config#1897; this test closes the
one grammar rule that has actually fired, cheaply and offline.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
SF_FILES = sorted(_INFRA.glob("step_function*.json"))


def _iter_intrinsic_fields(states: dict, path: str = ""):
    for name, state in states.items():
        here = f"{path}/{name}"
        for key, value in (state.get("Parameters") or {}).items():
            if isinstance(value, str) and value.startswith("States."):
                yield here, key, value
        if "States" in state:
            yield from _iter_intrinsic_fields(state["States"], here)
        for branch in state.get("Branches", []):
            yield from _iter_intrinsic_fields(branch.get("States", {}), here)
        for proc_key in ("ItemProcessor", "Iterator"):
            proc = state.get(proc_key)
            if proc and "States" in proc:
                yield from _iter_intrinsic_fields(proc["States"], here)


@pytest.mark.parametrize("sf_path", SF_FILES, ids=lambda p: p.name)
def test_no_single_quote_inside_intrinsic_string_literals(sf_path: Path):
    doc = json.loads(sf_path.read_text())
    offenders = []
    for state_path, key, value in _iter_intrinsic_fields(doc.get("States", {})):
        # every '...'-quoted literal segment inside the intrinsic call
        for literal in re.findall(r"'((?:[^'])*)'", value):
            # re can't see an interior quote inside a '-delimited match by
            # construction — the real tell is the shell-style '\'' splice,
            # which parses as literal-end + escaped-quote + literal-start:
            pass
        if re.search(r"'\\''", value) or re.search(r"''", value):
            offenders.append(f"{sf_path.name}{state_path} :: {key}")
    assert not offenders, (
        "ASL intrinsic string literals must not contain (or shell-escape) "
        "single quotes — AWS rejects the definition at deploy time "
        "(SCHEMA_VALIDATION_FAILED). Rephrase without apostrophes:\n  "
        + "\n  ".join(offenders)
    )
