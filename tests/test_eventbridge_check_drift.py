"""Tests for infrastructure/eventbridge/check-drift.py (alpha-engine-config#1464).

Covers the EventBridge SF-ARN drift guard: discovery of codified
`EVENT_PATTERN` heredocs from `infrastructure/lambdas/*/deploy.sh`, and the
compare-against-live logic (mocked `aws events` CLI calls — no real AWS
access; mirrors this repo's existing subprocess-mock convention, e.g.
tests/test_artifact_registry_coverage.py).
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "infrastructure" / "eventbridge" / "check-drift.py"


@pytest.fixture(scope="module")
def cd():
    """Load check-drift.py as a module (hyphenated filename, not importable
    via a normal `import` statement)."""
    spec = importlib.util.spec_from_file_location("eventbridge_check_drift", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── Discovery against the real repo state ───────────────────────────────────


def test_discovers_historically_bitten_rules(cd):
    """Rules named in config#1464 as historically orphaned by the 2026-06-29
    ne-* rename must be discovered from their deploy.sh. (The third such rule,
    alpha-engine-saturday-succeeded-groom, was retired with its
    saturday-sf-success-groom-dispatcher Lambda — config#2201: the end-of-SF
    sweep + gate_sf_run_sweep made the event-driven post-SF groom redundant.)"""
    rules = cd._discover_codified_rules()
    rule_names = {r["rule_name"] for r in rules}
    assert "alpha-engine-sf-status-change" in rule_names  # sf-telegram-notifier
    assert "alpha-engine-friday-shell-run-report" in rule_names
    assert "alpha-engine-saturday-succeeded-groom" not in rule_names  # retired config#2201


def test_discovers_sweep_artifact_monitor_rule(cd):
    """config#2392: the post-SF sweep-artifact validation Lambda's rule must
    be discovered like every other SF-status-keyed EventBridge rule — no
    registry to maintain here, so this pins the deploy.sh shape (EVENT_
    PATTERN heredoc + RULE_NAME literal) staying scannable."""
    rules = {r["rule_name"]: r for r in cd._discover_codified_rules()}
    assert "alpha-engine-sweep-artifact-monitor" in rules
    monitor = rules["alpha-engine-sweep-artifact-monitor"]
    assert "error" not in monitor
    arns = cd._extract_state_machine_arns(monitor["expected_pattern"])
    assert any("alpha-engine-groom-dispatch" in arn for arn in arns)
    # SUCCEEDED-only — acceptance criterion 3 (never alert on FAILED) is
    # enforced at the EventBridge rule level, not just in the handler.
    assert monitor["expected_pattern"]["detail"]["status"] == ["SUCCEEDED"]


def test_discovered_rules_have_no_parse_errors(cd):
    """Every discovered rule under today's repo state must parse cleanly —
    a source-error here means a deploy.sh's EVENT_PATTERN heredoc changed
    shape in a way this script's regex no longer handles."""
    rules = cd._discover_codified_rules()
    assert rules, "expected at least one codified EventBridge rule"
    errors = [r for r in rules if "error" in r]
    assert not errors, f"unexpected parse errors: {errors}"


def test_discovered_pattern_references_expected_state_machine(cd):
    rules = {r["rule_name"]: r for r in cd._discover_codified_rules()}
    sf_status_change = rules["alpha-engine-sf-status-change"]
    arns = cd._extract_state_machine_arns(sf_status_change["expected_pattern"])
    assert any("ne-weekly-freshness-pipeline" in arn for arn in arns)


# ── _canonical_json — order/whitespace-insensitive comparison ──────────────


def test_canonical_json_ignores_array_order():
    from_a = {"detail": {"stateMachineArn": ["a", "b"], "status": ["X", "Y"]}}
    from_b = {"detail": {"status": ["Y", "X"], "stateMachineArn": ["b", "a"]}}
    # Need module-level function; import via fixture pattern replicated here
    # for a standalone unit test without the AWS-call surface.
    spec = importlib.util.spec_from_file_location("eventbridge_check_drift2", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module._canonical_json(from_a) == module._canonical_json(from_b)


def test_canonical_json_detects_real_difference(cd):
    a = {"detail": {"stateMachineArn": ["a", "b"]}}
    b = {"detail": {"stateMachineArn": ["a", "c"]}}
    assert cd._canonical_json(a) != cd._canonical_json(b)


# ── _check_rule — mocked AWS CLI ────────────────────────────────────────────


def _fake_run(returncode=0, stdout="", stderr=""):
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    return result


def test_check_rule_no_drift_when_live_matches_codified(cd):
    rule = {
        "rule_name": "alpha-engine-sf-status-change",
        "source_file": _REPO_ROOT / "infrastructure" / "lambdas" / "sf-telegram-notifier" / "deploy.sh",
        "expected_pattern": {
            "source": ["aws.states"],
            "detail-type": ["Step Functions Execution Status Change"],
            "detail": {
                "stateMachineArn": [
                    "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
                ],
                "status": ["SUCCEEDED"],
            },
        },
    }
    live_response = {
        "Name": "alpha-engine-sf-status-change",
        "EventPattern": json.dumps(rule["expected_pattern"]),
    }
    with patch.object(cd.subprocess, "run", return_value=_fake_run(0, json.dumps(live_response))):
        findings = cd._check_rule(rule)
    assert findings == []


def test_check_rule_detects_stale_state_machine_arn(cd):
    """The exact bug class this guard exists for: live rule still matches
    the OLD (pre-rename) SF name/ARN."""
    rule = {
        "rule_name": "alpha-engine-sf-status-change",
        "source_file": _REPO_ROOT / "infrastructure" / "lambdas" / "sf-telegram-notifier" / "deploy.sh",
        "expected_pattern": {
            "source": ["aws.states"],
            "detail-type": ["Step Functions Execution Status Change"],
            "detail": {
                "stateMachineArn": [
                    "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
                ],
                "status": ["SUCCEEDED"],
            },
        },
    }
    stale_live = {
        "source": ["aws.states"],
        "detail-type": ["Step Functions Execution Status Change"],
        "detail": {
            "stateMachineArn": [
                # Pre-rename ARN — the exact 2026-06-29 orphaning scenario.
                "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-saturday",
            ],
            "status": ["SUCCEEDED"],
        },
    }
    live_response = {"Name": "alpha-engine-sf-status-change", "EventPattern": json.dumps(stale_live)}
    with patch.object(cd.subprocess, "run", return_value=_fake_run(0, json.dumps(live_response))):
        findings = cd._check_rule(rule)
    assert len(findings) == 1
    assert "stateMachineArn set differs" in findings[0]
    assert "alpha-engine-saturday" in findings[0]


def test_check_rule_missing_rule_on_aws(cd):
    rule = {
        "rule_name": "alpha-engine-sf-status-change",
        "source_file": _REPO_ROOT / "infrastructure" / "lambdas" / "sf-telegram-notifier" / "deploy.sh",
        "expected_pattern": {"detail": {"stateMachineArn": ["x"]}},
    }
    with patch.object(
        cd.subprocess,
        "run",
        return_value=_fake_run(254, "", "An error occurred (ResourceNotFoundException)"),
    ):
        findings = cd._check_rule(rule)
    assert len(findings) == 1
    assert "not found on AWS" in findings[0]


def test_check_rule_reports_precomputed_error(cd):
    rule = {"rule_name": "broken-rule", "source_file": Path("x"), "error": "boom"}
    findings = cd._check_rule(rule)
    assert findings == ["broken-rule: boom"]


def test_check_rule_no_event_pattern_on_live_rule(cd):
    rule = {
        "rule_name": "alpha-engine-sf-status-change",
        "source_file": _REPO_ROOT / "infrastructure" / "lambdas" / "sf-telegram-notifier" / "deploy.sh",
        "expected_pattern": {"detail": {"stateMachineArn": ["x"]}},
    }
    live_response = {"Name": "alpha-engine-sf-status-change", "ScheduleExpression": "rate(1 day)"}
    with patch.object(cd.subprocess, "run", return_value=_fake_run(0, json.dumps(live_response))):
        findings = cd._check_rule(rule)
    assert len(findings) == 1
    assert "no EventPattern" in findings[0]


# ── main() exit codes ───────────────────────────────────────────────────────


def test_main_returns_zero_when_clean(cd, monkeypatch):
    fake_rule = {
        "rule_name": "fake-rule",
        "source_file": _REPO_ROOT / "infrastructure" / "lambdas" / "sf-telegram-notifier" / "deploy.sh",
        "expected_pattern": {"detail": {"stateMachineArn": ["x"]}},
    }
    monkeypatch.setattr(cd, "_discover_codified_rules", lambda: [fake_rule])
    monkeypatch.setattr(cd, "_check_rule", lambda rule: [])
    monkeypatch.setattr("sys.argv", ["check-drift.py"])
    assert cd.main() == 0


def test_main_returns_one_on_drift(cd, monkeypatch):
    fake_rule = {
        "rule_name": "fake-rule",
        "source_file": _REPO_ROOT / "infrastructure" / "lambdas" / "sf-telegram-notifier" / "deploy.sh",
        "expected_pattern": {"detail": {"stateMachineArn": ["x"]}},
    }
    monkeypatch.setattr(cd, "_discover_codified_rules", lambda: [fake_rule])
    monkeypatch.setattr(cd, "_check_rule", lambda rule: ["fake-rule: drifted"])
    monkeypatch.setattr("sys.argv", ["check-drift.py"])
    assert cd.main() == 1


def test_main_returns_zero_when_nothing_to_check(cd, monkeypatch):
    monkeypatch.setattr(cd, "_discover_codified_rules", lambda: [])
    monkeypatch.setattr("sys.argv", ["check-drift.py"])
    assert cd.main() == 0


def test_main_rule_filter_no_match_returns_two(cd, monkeypatch, capsys):
    monkeypatch.setattr(cd, "_discover_codified_rules", lambda: [])
    monkeypatch.setattr("sys.argv", ["check-drift.py", "--rule", "does-not-exist"])
    assert cd.main() == 2
