"""Protect the repository-wide coverage measurement scope."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
JUSTIFIED_OMISSIONS = ["tests/*", ".venv/*", "venv/*"]


def test_ci_measures_the_whole_repository() -> None:
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    cov_targets = re.findall(r"(?:^|\s)--cov=([^\s]+)", workflow)

    assert cov_targets == ["."], "CI must have one repository-wide --cov=. target"

    measured_root = (ROOT / cov_targets[0]).resolve()
    source_modules = [
        path
        for path in ROOT.rglob("*.py")
        if not any(part in {"tests", ".venv", "venv", ".git"} for part in path.parts)
    ]
    assert source_modules, "the repository should contain source modules"
    assert all(path.resolve().is_relative_to(measured_root) for path in source_modules)


def test_coverage_floor_and_omissions_are_pinned() -> None:
    pyproject_text = PYPROJECT.read_text(encoding="utf-8")
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    config = tomllib.loads(pyproject_text)
    coverage = config["tool"]["coverage"]

    assert coverage["run"]["source"] == ["."]
    assert coverage["run"]["omit"] == JUSTIFIED_OMISSIONS

    configured_floors = re.findall(r"(?m)^fail_under\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*$", pyproject_text)
    command_floors = re.findall(r"--cov-fail-under(?:=|\s+)([0-9]+(?:\.[0-9]+)?)", workflow)
    floors = [float(value) for value in configured_floors + command_floors]
    assert len(floors) == 1, "coverage must enforce exactly one floor"
    assert floors[0] >= 0, "coverage floor must not fall below the current ratchet"
