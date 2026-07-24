"""Pins attach_overseer_put_events_policy.sh (alpha-engine-config-I2822;
role-coverage verified alpha-engine-config-I2875/I2900).

The script's coverage of newly-created fleet roles depends entirely on it
enumerating `alpha-engine-*` Lambda roles by WILDCARD (`list-functions` +
`starts_with`), never a hardcoded role name list — a static list is exactly
what goes stale the moment the next Lambda is deployed (this is the failure
mode I2875 found live: 4 roles created since the script last ran had zero
attachment). These tests pin that structural property plus the idempotency
and dry-run mechanics the fix relies on.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "attach_overseer_put_events_policy.sh"


@pytest.fixture(scope="module")
def script_text() -> str:
    return _SCRIPT.read_text()


def test_script_exists_and_is_executable():
    assert _SCRIPT.is_file()
    assert _SCRIPT.stat().st_mode & 0o111, "script must be chmod +x"


def test_bash_syntax_is_valid():
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    result = subprocess.run([bash, "-n", str(_SCRIPT)], capture_output=True)
    assert result.returncode == 0, result.stderr.decode()


class TestWildcardRoleEnumeration:
    """The structural fix for I2875: coverage of new roles must come from a
    live wildcard query, never a hardcoded, driftable role list."""

    def test_enumerates_by_wildcard_prefix(self, script_text):
        assert "starts_with(FunctionName, 'alpha-engine-')" in script_text

    def test_no_hardcoded_role_list(self, script_text):
        # The four roles I2875 found missing must NOT appear in executable
        # code (only in prose comments, which may reference them for
        # provenance) — they are covered by the wildcard query above. A
        # hardcoded list in the CODE would silently reintroduce the exact
        # drift I2875/I2900 closed.
        code_lines = "\n".join(
            line for line in script_text.splitlines() if not line.strip().startswith("#")
        )
        for role in (
            "alpha-engine-alert-drain-dispatcher-role",
            "alpha-engine-overseer-dispatcher-role",
            "alpha-engine-expense-collector-role",
            "alpha-engine-substrate-health-gate-role",
        ):
            assert role not in code_lines, (
                f"{role} must not be hardcoded in executable code — it is (and "
                "must stay) covered by the wildcard `alpha-engine-*` Lambda-role "
                "enumeration; a static list here is the drift I2875/I2900 fixed, "
                "reintroduced."
            )

    def test_supports_extra_roles_for_non_lambda_grants(self, script_text):
        # EC2/GHA-OIDC roles aren't Lambda functions, so they can't be swept
        # by the wildcard — the script must still accept them explicitly.
        assert "EXTRA_ROLES=" in script_text


class TestIdempotency:
    def test_policy_creation_checks_existence_first(self, script_text):
        pos_check = script_text.find("aws iam get-policy")
        pos_create = script_text.find("aws iam create-policy")
        assert pos_check != -1 and pos_create != -1
        assert pos_check < pos_create

    def test_attach_checks_existing_attachment_first(self, script_text):
        pos_check = script_text.find("list-attached-role-policies")
        pos_attach = script_text.find("aws iam attach-role-policy")
        assert pos_check != -1 and pos_attach != -1
        assert pos_check < pos_attach


class TestDryRun:
    def test_supports_dry_run_flag(self, script_text):
        assert '"${1:-}" == "--dry-run"' in script_text

    def test_dry_run_gates_mutating_calls(self, script_text):
        assert 'DRY_RUN == "1"' in script_text or '"$DRY_RUN" == "1"' in script_text


class TestPolicyScope:
    def test_policy_scoped_to_single_bus_action(self, script_text):
        assert '"Action": "events:PutEvents"' in script_text
        assert "nousergon-alerts" in script_text
