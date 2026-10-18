"""A Lambda's deploy workflow must fire on every repo file its deploy.sh vendors.

`alpha-engine-config-I10782`, stage 2 follow-up. nousergon-data#2180 made
``run_units.record_pit_stamps`` import ``contracts.pit``, so
``infrastructure/lambdas/crypto-balances/deploy.sh`` began copying
``contracts/__init__.py`` and ``contracts/pit.py`` into the D38 zip. Its
``deploy-crypto-balances.yml`` ``paths:`` filter was not widened with it, so a
change to ``contracts/pit.py`` alone would merge and never reach the Lambda:
the "merged fix that never went live" shape ``deploy-pipeline-watchdog.yml``
already names for ``scripts/weekly_sf_silence_deadman.py`` (config#6738).

The property, for every ``infrastructure/lambdas/<name>/deploy.sh`` that has a
``.github/workflows/deploy-<name>.yml``: each ``cp "${REPO_ROOT}/<path>"``
source is matched by one of that workflow's ``on.push.paths`` globs.
"""

from __future__ import annotations

import fnmatch
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
LAMBDAS = REPO_ROOT / "infrastructure" / "lambdas"
WORKFLOWS = REPO_ROOT / ".github" / "workflows"

# `cp [-flags] "${REPO_ROOT}/some/path" ...` or `cp $REPO_ROOT/some/path ...`
_VENDORED = re.compile(r'^\s*cp\s+(?:-\w+\s+)*"?\$\{?REPO_ROOT\}?/([^"\s]+)"?', re.MULTILINE)


def _vendored_sources(deploy_sh: Path) -> list[str]:
    return sorted(set(_VENDORED.findall(deploy_sh.read_text())))


def _push_paths(workflow: Path) -> list[str]:
    definition = yaml.safe_load(workflow.read_text())
    # PyYAML (YAML 1.1) loads the bare key `on` as the boolean True.
    triggers = definition.get("on", definition.get(True)) or {}
    push = triggers.get("push") or {}
    return list(push.get("paths") or [])


def _covered(path: str, globs: list[str]) -> bool:
    for glob in globs:
        if glob.endswith("/**") and path.startswith(glob[: -len("**")]):
            return True
        if fnmatch.fnmatchcase(path, glob):
            return True
    return False


def _pairs() -> list[tuple[str, Path, Path]]:
    pairs = []
    for deploy_sh in sorted(LAMBDAS.glob("*/deploy.sh")):
        name = deploy_sh.parent.name
        workflow = WORKFLOWS / f"deploy-{name}.yml"
        if workflow.is_file() and _vendored_sources(deploy_sh):
            pairs.append((name, deploy_sh, workflow))
    return pairs


def test_the_survey_finds_the_lambdas_that_vendor_repo_files():
    """A regex that silently matched nothing would pass every case below."""
    names = {name for name, _, _ in _pairs()}
    assert {"crypto-balances", "pipeline-watchdog", "weekly-preflight"} <= names
    assert "contracts/pit.py" in _vendored_sources(LAMBDAS / "crypto-balances" / "deploy.sh")


@pytest.mark.parametrize(
    ("name", "deploy_sh", "workflow"), _pairs(), ids=[p[0] for p in _pairs()]
)
def test_deploy_workflow_paths_cover_every_vendored_file(name, deploy_sh, workflow):
    globs = _push_paths(workflow)
    assert globs, f"{workflow.name} has no on.push.paths filter to check"
    missing = [p for p in _vendored_sources(deploy_sh) if not _covered(p, globs)]
    assert not missing, (
        f"{deploy_sh.relative_to(REPO_ROOT)} vendors {missing} into the {name} zip, but "
        f"{workflow.relative_to(REPO_ROOT)} on.push.paths does not list them: a change to "
        "any of them merges and never redeploys the Lambda"
    )


def test_coverage_matcher():
    assert _covered("infrastructure/lambdas/x/index.py", ["infrastructure/lambdas/x/**"])
    assert _covered("run_units.py", ["run_units.py"])
    assert not _covered("contracts/pit.py", ["run_units.py", "contracts/__init__.py"])
