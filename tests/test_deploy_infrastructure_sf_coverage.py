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

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_DEPLOY = _INFRA / "deploy-infrastructure.sh"

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
