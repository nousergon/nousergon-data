"""The weekly-preflight deploy's smoke gate classifies every status the handler returns.

alpha-engine-config-I11408: alpha-engine-config-I11112 (#1856) taught the
handler to return status="DEGRADED" when a REQUIRED check could not run. From
a Lambda invoke that is the steady state — the runtime provides no arctic /
repo_modules / polygon / checkout capability — but deploy.sh's smoke `case`
had no DEGRADED arm, so it fell through to "handler could not execute" and
every push to main went red from 2026-09-21, after the code had already gone
live.

These tests execute the real `case` block from deploy.sh (not a copy of it)
against a canned response for each status, and pin the handler's status
vocabulary so a new status cannot ship without being classified here.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
from pathlib import Path

import pytest

LAMBDA_DIR = (
    Path(__file__).resolve().parent.parent
    / "infrastructure"
    / "lambdas"
    / "weekly-preflight"
)
DEPLOY_SH = LAMBDA_DIR / "deploy.sh"
INDEX_PY = LAMBDA_DIR / "index.py"

# ERROR is the only status meaning "the handler could not execute"; the smoke
# gate blocks on it (and on an unparseable response). Every other status is a
# successful execution reporting on system state, which must not block the
# deploy of the very code that may fix it.
BLOCKING = {"ERROR"}
PASSING = {"OK", "DEGRADED", "FAIL"}


def _smoke_case_block() -> str:
    text = DEPLOY_SH.read_text()
    match = re.search(
        r'^\s*case "\$\{SMOKE_STATUS\}" in\n.*?^\s*esac\n', text, re.S | re.M
    )
    assert match, (
        "deploy.sh no longer has the smoke-status case block this test executes"
    )
    return match.group(0)


def _run_smoke(status: str, tmp_path: Path) -> subprocess.CompletedProcess:
    resp = tmp_path / "resp.json"
    resp.write_text(
        json.dumps(
            {
                "status": status,
                "ran_count": 5,
                "skip_count": 10,
                "warn_count": 0,
                "required_skip_count": 8,
                "required_skip_names": ["arctic_connectivity"],
                "failures": [],
            }
        )
    )
    script = f"SMOKE_STATUS={status}\nRESP={resp}\n{_smoke_case_block()}exit 0\n"
    return subprocess.run(
        ["bash", "-e", "-c", script], capture_output=True, text=True, check=False
    )


def _handler_return_statuses() -> set[str]:
    """Every literal top-level `status` the handler() function returns."""
    tree = ast.parse(INDEX_PY.read_text())
    handler = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "handler"
    )
    statuses = set()
    for node in ast.walk(handler):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            for key, value in zip(node.value.keys, node.value.values):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "status"
                    and isinstance(value, ast.Constant)
                ):
                    statuses.add(value.value)
    return statuses


def test_handler_status_vocabulary_is_fully_classified():
    returned = _handler_return_statuses()
    assert returned >= {"OK", "ERROR"}, (
        f"extracted only {sorted(returned)} from handler() — the extraction has rotted"
    )
    unclassified = returned - BLOCKING - PASSING
    assert not unclassified, (
        f"index.py returns status(es) {sorted(unclassified)} that deploy.sh's smoke gate "
        "has not been classified for — add an arm to the case block and to this test"
    )


@pytest.mark.parametrize("status", sorted(PASSING))
def test_executed_handler_does_not_fail_the_deploy(status, tmp_path):
    result = _run_smoke(status, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "could not execute" not in result.stdout


def test_degraded_discloses_the_unreachable_required_checks(tmp_path):
    result = _run_smoke("DEGRADED", tmp_path)
    assert "arctic_connectivity" in result.stdout


@pytest.mark.parametrize("status", sorted(BLOCKING | {"MALFORMED"}))
def test_handler_that_could_not_execute_fails_the_deploy(status, tmp_path):
    result = _run_smoke(status, tmp_path)
    assert result.returncode == 1
    assert "could not execute" in result.stdout
