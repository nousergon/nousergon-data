"""Deploy-coverage guard: every orchestration Step Function definition in
``infrastructure/`` MUST be auto-deployed by ``deploy-infrastructure.sh``.

This pins the fix for config#1173: the EOD SF (``step_function_eod.json`` →
``alpha-engine-eod-pipeline``) used to be deployed ONLY by the manual
``update_eod_pipeline_sf.sh``, which nothing triggered on merge. Merged EOD SF
changes therefore silently never reached the live state machine (drift hit
2026-06-22 via #458's ``nousergon_lib`` migration — weekday + Saturday
auto-deployed, EOD did not).

Same deploy-gap class as the CFN-target-uniqueness / drift-stamp guards: a new
``step_function_*.json`` added to ``infrastructure/`` without a matching
deploy block in ``deploy-infrastructure.sh`` reintroduces exactly this silent
drift. This test fails loudly the moment that happens, forcing the author to
wire the new SF into the auto-deploy flow.

For each ``infrastructure/step_function*.json`` we assert the deploy script:
  1. uploads the (stamped) definition to ``s3://.../infrastructure/<file>``, and
  2. applies it via ``aws stepfunctions update-state-machine``.
"""

from __future__ import annotations

import fnmatch
import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_DEPLOY = _INFRA / "deploy-infrastructure.sh"
_GHA_DEPLOY_POLICY = _INFRA / "iam" / "github-actions-lambda-deploy.json"

# The orchestration SF definitions that ship in infrastructure/ and are expected
# to be auto-deployed on merge. Discovered by glob so a newly added SF file is
# covered automatically.
_SF_FILES = sorted(p.name for p in _INFRA.glob("step_function*.json"))


def test_deploy_script_and_sf_definitions_present() -> None:
    assert _DEPLOY.is_file(), f"missing {_DEPLOY}"
    # Guards against the glob silently matching nothing (e.g. a refactor that
    # renames the files) and turning the per-file checks into vacuous passes.
    assert _SF_FILES, "no step_function*.json definitions found in infrastructure/"


@pytest.mark.parametrize("sf_file", _SF_FILES)
def test_sf_definition_uploaded_to_s3(sf_file: str) -> None:
    """Every SF definition is staged to S3 by the deploy script."""
    script = _DEPLOY.read_text()
    assert (
        f"s3://$BUCKET/infrastructure/{sf_file}" in script
    ), (
        f"{sf_file} is not uploaded to S3 by deploy-infrastructure.sh — a "
        f"merged change to it would silently never reach the live state "
        f"machine (config#1173). Add an `aws s3 cp` line mirroring the "
        f"Saturday/weekday/EOD blocks."
    )


def test_update_state_machine_called_for_each_sf() -> None:
    """Each SF definition has a corresponding update-state-machine deploy.

    We count the ``update-state-machine`` invocations rather than tie each to a
    literal ARN string (the ARNs are built from shell vars) and require at least
    one per SF definition file.
    """
    script = _DEPLOY.read_text()
    n_updates = script.count("aws stepfunctions update-state-machine")
    assert n_updates >= len(_SF_FILES), (
        f"deploy-infrastructure.sh has {n_updates} update-state-machine "
        f"call(s) for {len(_SF_FILES)} SF definition file(s) "
        f"({_SF_FILES}); every orchestration SF must be applied on merge "
        f"(config#1173)."
    )


def test_eod_state_machine_is_auto_deployed() -> None:
    """Explicit pin for the config#1173 regression target.

    The EOD SF + its state-machine name must both be wired into the deploy
    script so the auto-deploy path covers ``alpha-engine-eod-pipeline``.
    """
    script = _DEPLOY.read_text()
    assert "step_function_eod.json" in script, (
        "EOD SF definition not referenced by deploy-infrastructure.sh "
        "(config#1173 regression)."
    )
    assert "alpha-engine-eod-pipeline" in script, (
        "alpha-engine-eod-pipeline state machine not deployed by "
        "deploy-infrastructure.sh (config#1173 regression)."
    )


# ---------------------------------------------------------------------------
# IAM-grant coverage — the gap that let config#1173's deploy block merge green
# yet fail at runtime with AccessDeniedException on states:UpdateStateMachine.
#
# The script-coverage tests above prove deploy-infrastructure.sh *attempts* to
# update every SF. They do NOT prove the GHA deploy role is *allowed* to. On
# 2026-06-24 the EOD deploy block ran but the role's inline policy listed only
# the Saturday + weekday ARNs, so the EOD update-state-machine call 403'd, the
# CF stamp never advanced, and the weekday pipeline's DeployDriftCheck halted
# the live trading run. This guard closes that bug class: every state machine
# the deploy script applies MUST be granted states:UpdateStateMachine in
# github-actions-lambda-deploy.json.
# ---------------------------------------------------------------------------


def _sf_names_deployed_by_script() -> set[str]:
    """State-machine names the deploy script applies via update-state-machine.

    The ARNs are assembled from shell vars (``...:stateMachine:<name>``); we
    lift the trailing pipeline names from the literal ARN-suffix strings rather
    than resolve the shell, which is sufficient because the names are written
    in full in the script.
    """
    script = _DEPLOY.read_text()
    return set(re.findall(r"stateMachine:(alpha-engine-[a-z0-9-]+pipeline)", script))


def _sf_arn_patterns_granted_in_policy() -> list[str]:
    """state-machine ARN patterns granted states:UpdateStateMachine by the role.

    Returns the trailing ``stateMachine:<pattern>`` portion of every Resource
    ARN on a statement that grants ``states:UpdateStateMachine``. Patterns may
    contain IAM ``*`` wildcards (e.g. ``alpha-engine-*-pipeline``); coverage is
    matched with fnmatch so a naming-convention wildcard correctly covers each
    concrete pipeline.
    """
    policy = json.loads(_GHA_DEPLOY_POLICY.read_text())
    patterns: list[str] = []
    for stmt in policy.get("Statement", []):
        actions = stmt.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        if "states:UpdateStateMachine" not in actions:
            continue
        resources = stmt.get("Resource", [])
        if isinstance(resources, str):
            resources = [resources]
        for arn in resources:
            m = re.search(r"stateMachine:([A-Za-z0-9*_.-]+)$", arn)
            if m:
                patterns.append(m.group(1))
    return patterns


def test_every_deployed_sf_is_granted_update_state_machine() -> None:
    """Each SF the deploy script applies must be UpdateStateMachine-grantable.

    Pins the config#1173 / 2026-06-24 runtime AccessDenied: a new SF wired into
    deploy-infrastructure.sh whose name is not covered by the GHA deploy role's
    UpdateStateMachine grant fails CI here instead of silently 403'ing on merge
    and halting the next live pipeline run on stale-stamp drift.
    """
    deployed = _sf_names_deployed_by_script()
    patterns = _sf_arn_patterns_granted_in_policy()
    assert deployed, "no state-machine names parsed from deploy-infrastructure.sh"
    assert patterns, (
        "github-actions-lambda-deploy.json grants states:UpdateStateMachine on "
        "no stateMachine resource"
    )
    missing = sorted(
        name for name in deployed
        if not any(fnmatch.fnmatchcase(name, pat) for pat in patterns)
    )
    assert not missing, (
        f"deploy-infrastructure.sh applies update-state-machine to {missing} "
        f"but no Resource pattern in github-actions-lambda-deploy.json grants "
        f"states:UpdateStateMachine on them (granted patterns: {patterns}). The "
        f"GHA deploy role will 403 at runtime, the CF stamp will not advance, "
        f"and the next pipeline run halts on DeployDriftCheck (config#1173). "
        f"Add/extend an ARN pattern in the InfraDeploySFDefinition statement."
    )
