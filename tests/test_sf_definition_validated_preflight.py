"""Preflight guard: ``deploy-infrastructure.sh`` MUST validate every Step
Function definition BEFORE it applies any of them (config#1897).

Root cause this pins: on 2026-07-07 a malformed intrinsic (an unbalanced
``commands.$`` in the daily SF, #676) passed the in-repo unit guard
(``TestIntrinsicsWellFormed``, which only paren-balances) and was rejected by
AWS at ``UpdateStateMachine`` time — POST-merge, on ``main``. Because the
deploy script updates state machines one at a time, the weekly SF had already
been updated when the daily SF was rejected, leaving the fleet stamped at mixed
SHAs (#677).

The structural fix (this test guards it): a validate-ALL preflight that calls
``aws stepfunctions validate-state-machine-definition`` — the SAME validation
AWS runs at deploy time, catching the broad malformed-intrinsic class the unit
guard can't — for every stamped definition, BEFORE the first S3 upload or
``update-state-machine``/``create-state-machine`` call, aborting all-or-nothing
if any fails. A resource-less IAM action, so the GHA deploy role must grant it.

This test fails loudly the moment the preflight is removed, moved after an
apply, stops covering a definition, or the IAM grant is dropped.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_DEPLOY = _INFRA / "deploy-infrastructure.sh"
_GHA_DEPLOY_POLICY = _INFRA / "iam" / "github-actions-lambda-deploy.json"

# The stamped SF definition variables the deploy script builds + applies. Every
# one of these must be fed to the validate preflight.
_STAMPED_VARS = ("$SAT_STAMPED", "$DAILY_STAMPED", "$EOD_STAMPED", "$GROOM_STAMPED")


def _script_text() -> str:
    assert _DEPLOY.is_file(), f"missing {_DEPLOY}"
    return _DEPLOY.read_text()


def _first_index(text: str, pattern: str) -> int:
    """Char offset of the first regex match, or -1 if absent."""
    m = re.search(pattern, text)
    return m.start() if m else -1


def test_preflight_calls_validate_state_machine_definition() -> None:
    text = _script_text()
    assert "validate-state-machine-definition" in text, (
        "deploy-infrastructure.sh must run "
        "`aws stepfunctions validate-state-machine-definition` as a preflight "
        "(config#1897) — no invocation found."
    )


def test_every_stamped_definition_is_validated() -> None:
    """The preflight must cover ALL four SF definitions, not a subset — the
    2026-07-07 gap was exactly a subset (unbalanced intrinsic slipped through)."""
    text = _script_text()
    # Restrict to the validate helper call sites so we assert the preflight
    # itself covers each definition (not merely that the var appears anywhere).
    validate_calls = "\n".join(
        line for line in text.splitlines() if "validate_sf_definition" in line
    )
    for var in _STAMPED_VARS:
        assert var in validate_calls, (
            f"stamped definition {var} is not passed to the validate preflight — "
            "every definition the script deploys must be validated (config#1897)."
        )


def test_validation_runs_before_any_apply() -> None:
    """All-or-nothing: the validate preflight must precede the first S3 upload
    AND the first update/create-state-machine call, so a bad definition aborts
    the deploy while nothing has been applied yet (no mixed-SHA fleet)."""
    text = _script_text()
    validate_at = _first_index(text, r"validate-state-machine-definition")
    upload_at = _first_index(text, r"aws s3 cp .*s3://\$BUCKET/infrastructure/")
    update_at = _first_index(text, r"aws stepfunctions (update|create)-state-machine")

    assert validate_at != -1
    assert upload_at != -1, "expected an S3 upload of the SF definitions"
    assert update_at != -1, "expected an update/create-state-machine apply"
    assert validate_at < upload_at, (
        "validate preflight must run BEFORE uploading definitions to S3 "
        "(config#1897)."
    )
    assert validate_at < update_at, (
        "validate preflight must run BEFORE any update/create-state-machine "
        "call, or a bad definition partially applies (config#1897)."
    )


def test_abort_keys_on_result_field_not_diagnostics() -> None:
    """AWS documents that diagnostic codes/wording may change; the pass/fail
    decision must key on the `result` field (OK|FAIL) only."""
    text = _script_text()
    assert "result" in text, (
        "preflight must read the `result` field from "
        "validate-state-machine-definition output (config#1897)."
    )
    # A hard failure path must exist (the script aborts on FAIL).
    assert re.search(r"VALIDATION_FAILED=true", text), (
        "preflight must set a failure flag and abort when a definition is "
        "invalid (config#1897)."
    )
    assert re.search(r"exit 1", text)


def test_gha_deploy_role_grants_validate_action() -> None:
    """`states:ValidateStateMachineDefinition` is resource-less; without the
    grant the preflight AccessDenies and (fail-closed) breaks every deploy."""
    policy = json.loads(_GHA_DEPLOY_POLICY.read_text())
    actions: set[str] = set()
    for stmt in policy["Statement"]:
        act = stmt.get("Action", [])
        actions.update([act] if isinstance(act, str) else act)
    assert "states:ValidateStateMachineDefinition" in actions, (
        "github-actions-lambda-deploy.json must grant "
        "states:ValidateStateMachineDefinition or the config#1897 preflight "
        "fail-closes every Deploy Infrastructure run."
    )
