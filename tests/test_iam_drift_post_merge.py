"""Unit tests for infrastructure/iam/check-drift.py --post-merge (config#3697).

The PR-triggered IAM drift check is structurally circular: a PR that codifies
new IAM is compared against live AWS state that hasn't been applied yet, so it
is guaranteed to show drift until apply.sh runs. --post-merge instead applies
each drifted role via apply.sh and re-checks, only failing on residual
(real) drift. These tests mock both the AWS-calling layer (_aws_iam) and the
apply-invocation layer (_apply_role) so no real AWS/subprocess calls happen.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

_SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "infrastructure" / "iam" / "check-drift.py"
)
_spec = importlib.util.spec_from_file_location("check_drift", _SCRIPT_PATH)
check_drift = importlib.util.module_from_spec(_spec)
sys.modules["check_drift"] = check_drift
_spec.loader.exec_module(check_drift)


class _FakeCompletedProcess:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _write_policy_file(dir_path: Path, name: str, policy_doc: dict) -> None:
    (dir_path / f"{name}.json").write_text(json.dumps(policy_doc))


def _write_trust_file(dir_path: Path, name: str, trust_doc: dict) -> None:
    (dir_path / f"{name}.trust.json").write_text(json.dumps(trust_doc))


def test_post_merge_resolves_when_apply_fixes_inline_drift(tmp_path, monkeypatch):
    """apply.sh succeeds and the re-check is clean — should exit 0."""
    _write_policy_file(
        tmp_path,
        "test-role",
        {"Version": "2012-10-17", "Statement": []},
    )
    monkeypatch.setattr(check_drift, "SCRIPT_DIR", tmp_path)

    # Initial scan: role has no inline policy on AWS → "missing in AWS"
    # Re-check after apply: policy now exists with matching content
    live_states: list[dict] = [
        {},  # get-role-policy returns empty (missing)
        {"PolicyDocument": {"Version": "2012-10-17", "Statement": []}},  # re-check
    ]

    def fake_aws_iam(*args):
        return live_states.pop(0)

    with patch.object(check_drift, "_aws_iam", side_effect=fake_aws_iam):
        with patch.object(
            check_drift,
            "_apply_role",
            return_value=_FakeCompletedProcess(0, "applied\n"),
        ) as mock_apply:
            with patch.object(sys, "argv", ["check-drift.py", "--post-merge"]):
                exit_code = check_drift.main()

    assert exit_code == 0
    mock_apply.assert_called_once_with("test-role")


def test_post_merge_fails_on_residual_drift(tmp_path, monkeypatch):
    """apply.sh 'succeeds' but the re-check still shows drift — real, exit 1."""
    _write_policy_file(
        tmp_path,
        "test-role",
        {"Version": "2012-10-17", "Statement": []},
    )
    monkeypatch.setattr(check_drift, "SCRIPT_DIR", tmp_path)

    # Both initial scan and re-check show the policy missing — apply.sh
    # reported success but the drift didn't clear (real unexpected drift).
    live_states: list[dict] = [
        {},  # initial scan: missing
        {},  # re-check: still missing
    ]

    def fake_aws_iam(*args):
        return live_states.pop(0)

    with patch.object(check_drift, "_aws_iam", side_effect=fake_aws_iam):
        with patch.object(
            check_drift,
            "_apply_role",
            return_value=_FakeCompletedProcess(0, "applied\n"),
        ):
            with patch.object(sys, "argv", ["check-drift.py", "--post-merge"]):
                exit_code = check_drift.main()

    assert exit_code == 1


def test_post_merge_fails_when_apply_itself_fails(tmp_path, monkeypatch):
    """apply.sh itself fails — should exit 1 without re-checking."""
    _write_policy_file(
        tmp_path,
        "test-role",
        {"Version": "2012-10-17", "Statement": []},
    )
    monkeypatch.setattr(check_drift, "SCRIPT_DIR", tmp_path)

    live_states: list[dict] = [{}, {}]  # initial scan only (no re-check needed)

    def fake_aws_iam(*args):
        return live_states.pop(0)

    with patch.object(check_drift, "_aws_iam", side_effect=fake_aws_iam):
        with patch.object(
            check_drift,
            "_apply_role",
            return_value=_FakeCompletedProcess(1, "", "apply.sh: AccessDenied"),
        ):
            with patch.object(sys, "argv", ["check-drift.py", "--post-merge"]):
                exit_code = check_drift.main()

    assert exit_code == 1


def test_post_merge_resolves_trust_drift(tmp_path, monkeypatch):
    """apply.sh fixes trust drift — should exit 0."""
    trust_src = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    _write_trust_file(tmp_path, "test-role", trust_src)
    monkeypatch.setattr(check_drift, "SCRIPT_DIR", tmp_path)

    # Initial scan: trust is stale (old principal order). apply.sh fixes it.
    trust_stale = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "scheduler.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    # re-check: trust now matches codified
    live_trust_states = [
        {"Role": {"AssumeRolePolicyDocument": trust_stale}},
        {"Role": {"AssumeRolePolicyDocument": json.loads(json.dumps(trust_src))}},
    ]

    def fake_aws_iam(*args):
        return live_trust_states.pop(0)

    with patch.object(check_drift, "_aws_iam", side_effect=fake_aws_iam):
        with patch.object(
            check_drift,
            "_apply_role",
            return_value=_FakeCompletedProcess(0, "trust applied\n"),
        ) as mock_apply:
            with patch.object(sys, "argv", ["check-drift.py", "--post-merge"]):
                exit_code = check_drift.main()

    assert exit_code == 0
    mock_apply.assert_called_once_with("test-role")


def test_clean_state_no_drift_never_invokes_apply(tmp_path, monkeypatch):
    """No drift at all — --post-merge should exit 0 without calling apply."""
    _write_policy_file(
        tmp_path,
        "test-role",
        {"Version": "2012-10-17", "Statement": []},
    )
    monkeypatch.setattr(check_drift, "SCRIPT_DIR", tmp_path)

    # Both inline and trust are clean.
    live_states: list[dict] = [
        {"PolicyDocument": {"Version": "2012-10-17", "Statement": []}},
    ]

    def fake_aws_iam(*args):
        return live_states.pop(0)

    with patch.object(check_drift, "_aws_iam", side_effect=fake_aws_iam):
        with patch.object(
            check_drift, "_apply_role", return_value=_FakeCompletedProcess(0)
        ) as mock_apply:
            with patch.object(sys, "argv", ["check-drift.py", "--post-merge"]):
                exit_code = check_drift.main()

    assert exit_code == 0
    mock_apply.assert_not_called()
