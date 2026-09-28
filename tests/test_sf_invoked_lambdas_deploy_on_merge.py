"""Every SF-invoked Lambda whose code lives in this repo deploys on merge.

alpha-engine-config-I10172: nousergon-data#1657 wired the SubstrateHealthGate
stage-coverage verdict into ``infrastructure/lambdas/substrate-health-gate``
and merged green on 2026-09-08, but that directory had no ``deploy-*.yml`` —
the Lambda was operator-deployed only, and nobody ran the operator step. For
three weekly cycles the SF entered SubstrateHealthGate, the stale 2026-08-14
code returned HEALTHY, and no verdict was written; the six sibling stages
instrumented by the same PR each had a deploy workflow and all went live.

The invariant this pins: if a Step Function definition in this repo invokes a
Lambda, and that Lambda's ``deploy.sh`` lives under
``infrastructure/lambdas/<dir>/``, then some ``.github/workflows/deploy-*.yml``
triggers on a push touching ``infrastructure/lambdas/<dir>/**``. Otherwise a
merged fix to a stage the SF runs every week is correct on ``main`` and absent
from the account, and nothing reports the difference.

Lambdas owned by other repos (``alpha-engine-research-runner:live`` etc.)
have no ``deploy.sh`` here and are out of scope; their repos deploy them.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
LAMBDAS = INFRA / "lambdas"
WORKFLOWS = REPO_ROOT / ".github" / "workflows"

# SF-invoked, in-repo, and still operator-deployed only. This set may only
# SHRINK: a directory leaves it in the PR that adds its deploy workflow, and
# the stale-entry test below fails if one is left behind. Never add to it —
# a new SF-invoked Lambda ships with its deploy workflow.
GRANDFATHERED: frozenset[str] = frozenset(
    {
        "ssm-liveness-poller",  # daily SF
        "eod-precondition-probe",  # EOD SF
    }
)

_FUNCTION_NAME_RE = re.compile(r'^FUNCTION_NAME="([^"]+)"', re.MULTILINE)
_LAMBDA_PATH_TRIGGER_RE = re.compile(r"infrastructure/lambdas/([A-Za-z0-9_.-]+)/\*\*")


def _walk_function_names(node, out: set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "FunctionName" and isinstance(value, str):
                out.add(value)
            _walk_function_names(value, out)
    elif isinstance(node, list):
        for item in node:
            _walk_function_names(item, out)


def _normalise(function_ref: str) -> str:
    """``arn:...:function:NAME[:alias]`` or ``NAME[:alias]`` -> ``NAME``."""
    return function_ref.rsplit("function:", 1)[-1].split(":", 1)[0]


def _sf_invoked_function_names() -> set[str]:
    names: set[str] = set()
    for path in sorted(INFRA.glob("step_function*.json")):
        refs: set[str] = set()
        _walk_function_names(json.loads(path.read_text()), refs)
        names.update(_normalise(r) for r in refs)
    return names


def _function_name_to_dir() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for deploy_sh in sorted(LAMBDAS.glob("*/deploy.sh")):
        match = _FUNCTION_NAME_RE.search(deploy_sh.read_text())
        if match:
            mapping[match.group(1)] = deploy_sh.parent.name
    return mapping


def _dirs_with_deploy_workflow() -> dict[str, list[str]]:
    covered: dict[str, list[str]] = {}
    for workflow in sorted(WORKFLOWS.glob("deploy-*.yml")):
        text = workflow.read_text()
        if "push:" not in text:
            continue
        for match in _LAMBDA_PATH_TRIGGER_RE.finditer(text):
            covered.setdefault(match.group(1), []).append(workflow.name)
    return covered


def _sf_invoked_in_repo_dirs() -> set[str]:
    by_name = _function_name_to_dir()
    return {by_name[n] for n in _sf_invoked_function_names() if n in by_name}


def test_discovery_is_not_vacuous():
    """A regex or layout change that finds nothing must fail, not pass."""
    dirs = _sf_invoked_in_repo_dirs()
    assert "substrate-health-gate" in dirs
    assert "weekly-run-scope" in dirs
    assert len(dirs) >= 8, sorted(dirs)


def test_every_sf_invoked_in_repo_lambda_deploys_on_merge():
    covered = _dirs_with_deploy_workflow()
    missing = sorted(
        d
        for d in _sf_invoked_in_repo_dirs()
        if d not in covered and d not in GRANDFATHERED
    )
    assert not missing, (
        "SF-invoked Lambda(s) with no deploy-on-merge workflow — a merged fix "
        "would never reach the account (alpha-engine-config-I10172). Add a "
        ".github/workflows/deploy-<dir>.yml modelled on "
        f"deploy-weekly-run-scope.yml for: {missing}"
    )


def test_substrate_health_gate_deploys_on_merge():
    """The specific regression I10172 closed on."""
    workflows = _dirs_with_deploy_workflow().get("substrate-health-gate", [])
    assert "deploy-substrate-health-gate.yml" in workflows
    text = (WORKFLOWS / "deploy-substrate-health-gate.yml").read_text()
    assert "bash infrastructure/lambdas/substrate-health-gate/deploy.sh" in text
    # --smoke issues a real SSM command against the trading box; a merge must
    # never do that.
    assert "--smoke" not in text.split("jobs:", 1)[1]


def test_grandfathered_entries_are_still_needed():
    """The allow-list only shrinks: an entry that now has a workflow, or is no
    longer SF-invoked, must be removed in the same PR."""
    covered = _dirs_with_deploy_workflow()
    invoked = _sf_invoked_in_repo_dirs()
    stale = sorted(d for d in GRANDFATHERED if d in covered or d not in invoked)
    assert not stale, f"remove from GRANDFATHERED: {stale}"
