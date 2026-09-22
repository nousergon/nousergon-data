"""Merge-time half of `check_tool_contracts` — over the COMMITTED definition.

`alpha-engine-config-I11313` deliverable 3, option (a): the runtime check
reads the LIVE state machine via `describe_state_machine`, which needs AWS
and a sibling checkout and therefore only ever ran where neither was
available. `infrastructure/step_function.json` in this repo is the source of
truth that definition is deployed FROM, so the static half of the assertion
is a pure in-repo test that runs on every PR — strictly earlier than
preflight, which is the whole point (I11112 deliverable 3 / `sf-pipeline-
policy.md` §2.2).

## The defect this pins, measured 2026-09-22

`check_tool_contracts` returned `ok` with the message
`0 command(s) checked; all flags match pinned versions` — on EVERY
environment, including one with full capabilities. `_parse_checkout_repo`
read only `parts[0]` of each `commands.$` value, and every one of them is a
`States.Array('line','line',...)` intrinsic whose first token is
`States.Array('set`. The walker skipped all of them and returned green over
an empty set. That is `principles.md` §2.7 exactly: no data rendered as
green.

So this file asserts NON-VACUITY first and coverage second. A test that only
checked "every parsed checkout resolves" would have passed on zero parsed
checkouts, reproducing the bug it exists to catch.

## What this does NOT cover

The flag-versus-pinned-version half. That reads `requirements.txt` /
`requirements/common.in` / `pyproject.toml` in five SIBLING repos, which no
CI job in this repo checks out, so it stays on the weekly spot box's
`FULL_CAPABILITIES` run (`alpha-engine-config-I11312`) and
`check_tool_contracts` stays `CHECK_REQUIRED = True`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import sf_preflight as sfp

DEFINITION = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def command_values() -> "list[str]":
    """Every `commands.$` string in the committed definition, from anywhere
    in the graph — including inside Parallel and Map branches."""
    found: "list[str]" = []

    def walk(node):
        if isinstance(node, dict):
            for key, val in node.items():
                if key == "commands.$" and isinstance(val, str):
                    found.append(val)
                else:
                    walk(val)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(json.loads(DEFINITION.read_text()))
    return found


def test_the_definition_still_shells_out(command_values):
    """Floor for everything below. If the definition stops carrying
    `commands.$` at all, every assertion in this file becomes vacuous and
    must say so rather than pass."""
    assert command_values, (
        f"no commands.$ found in {DEFINITION} — either the pipeline stopped "
        f"shelling out to the spot box entirely (say so here and retire this "
        f"file) or the definition's shape changed under this walker."
    )


def test_the_scanner_parses_a_checkout_from_the_real_definition(command_values):
    """THE NON-VACUITY GUARD — the assertion whose absence let
    `check_tool_contracts` report green over zero commands for months."""
    parsed = {c for v in command_values for c in sfp.scan_command_checkouts(v)}
    assert parsed, (
        "scan_command_checkouts matched NO checkout venv in any of the "
        f"{len(command_values)} commands.$ value(s) in the committed "
        "definition. This is the exact vacuous-green shape "
        "alpha-engine-config-I11112 was filed over: the runtime check "
        "reports `0 command(s) checked; all flags match pinned versions` and "
        "every reader takes it for a pass."
    )


def test_every_parsed_checkout_resolves_to_a_governing_repo(command_values):
    """A checkout the mapping does not know is a tool contract nothing can
    verify. Catches the rename drift directly: the definition names BOTH
    `alpha-engine-dashboard` and `crucible-research`, and a table carrying
    only one spelling turns the other into an unverifiable stage."""
    parsed = sorted({c for v in command_values for c in sfp.scan_command_checkouts(v)})
    unresolved = [c for c in parsed if sfp._resolve_governing_repo(c) is None]
    assert not unresolved, (
        f"checkout(s) in the SF definition with no governing repo: "
        f"{unresolved}. Add them to sf_preflight._SIBLING_REPO_ALIASES — "
        f"until then check_tool_contracts cannot verify any flag the SF "
        f"passes to them. Parsed: {parsed}"
    )


def test_the_runtime_check_is_not_vacuous_on_the_committed_definition(monkeypatch):
    """End-to-end over the runtime check itself: fed the COMMITTED
    definition, `check_tool_contracts` must examine a non-zero number of
    commands. Pins the guard rather than the parser, so a future rewrite of
    either cannot silently restore the empty-set green."""
    definition = DEFINITION.read_text()

    class _FakeSfn:
        def describe_state_machine(self, stateMachineArn):  # noqa: N803 - boto3 kwarg
            return {"definition": definition}

    class _FakeBoto3:
        @staticmethod
        def client(name, *args, **kwargs):
            assert name == "stepfunctions"
            return _FakeSfn()

    import sys

    monkeypatch.setitem(sys.modules, "boto3", _FakeBoto3)
    result = sfp.check_tool_contracts(object())

    assert result.details.get("checked", 0) > 0, (
        f"check_tool_contracts examined {result.details.get('checked')} "
        f"command(s) against the committed definition and returned "
        f"status={result.status!r} ({result.message!r}). Zero is not a pass."
    )
    assert result.status != "skip"


def test_zero_parsed_commands_is_a_failure_not_a_pass(monkeypatch):
    """NEGATIVE CONTROL for the guard added under I11112. A definition with
    no recognisable shell-out must FAIL the check, not return ok."""
    class _FakeSfn:
        def describe_state_machine(self, stateMachineArn):  # noqa: N803
            return {"definition": json.dumps({"States": {"NoOp": {"Type": "Succeed"}}})}

    class _FakeBoto3:
        @staticmethod
        def client(name, *args, **kwargs):
            return _FakeSfn()

    import sys

    monkeypatch.setitem(sys.modules, "boto3", _FakeBoto3)
    result = sfp.check_tool_contracts(object())

    assert result.status == "fail", (
        f"a definition shelling out to nothing returned {result.status!r} — "
        f"that is the vacuous green I11112 exists to remove"
    )
    assert "ZERO" in result.message


def test_scanner_handles_both_definition_shapes():
    """Shape-agnostic by contract: the bare-argv form the original parser
    expected AND the States.Array intrinsic the definition actually uses."""
    bare = "/home/ec2-user/alpha-engine-data/.venv/bin/python -m collectors.daily --x"
    assert sfp.scan_command_checkouts(bare) == ["alpha-engine-data"]

    intrinsic = (
        "States.Array('set -eo pipefail','cd /home/ec2-user/crucible-research',"
        "'/home/ec2-user/crucible-research/.venv/bin/python -m graph.run --correlation-id z')"
    )
    assert sfp.scan_command_checkouts(intrinsic) == ["crucible-research"]

    assert sfp.scan_command_checkouts("") == []
    assert sfp.scan_command_checkouts("echo hello") == []
