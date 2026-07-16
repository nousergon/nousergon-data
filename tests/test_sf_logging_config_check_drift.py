"""Tests for infrastructure/step-functions/check-drift.py (alpha-engine-config#1464).

Covers the SF LoggingConfiguration drift guard: discovery of the expected
LoggingConfiguration per state machine (parsed from
infrastructure/cloudformation/alpha-engine-orchestration.yaml +
infrastructure/deploy-infrastructure.sh), and the compare-against-live logic
(mocked `aws stepfunctions` CLI calls — no real AWS access).
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "infrastructure" / "step-functions" / "check-drift.py"


@pytest.fixture(scope="module")
def cd():
    spec = importlib.util.spec_from_file_location("sf_logging_check_drift", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_run(returncode=0, stdout="", stderr=""):
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    return result


# ── Discovery against the real repo state ───────────────────────────────────


def test_discovers_all_six_orchestrated_state_machines(cd):
    entries = cd._discover_expected_logging_configs()
    names = {e["sf_name"] for e in entries}
    assert names == {
        "ne-weekly-freshness-pipeline",
        "ne-preopen-trading-pipeline",
        "ne-postclose-trading-pipeline",
        "alpha-engine-groom-dispatch",
        # alpha-engine-config-I2544/I2545: both are CFN
        # AWS::StepFunctions::StateMachine resources with a
        # LoggingConfiguration block, auto-discovered via
        # _discover_expected_from_cfn's regex walk of the CFN template —
        # no code change needed in check-drift.py itself.
        "ne-weekly-advisory-pipeline",
        "ne-modelzoo-sunday-pipeline",
    }


def test_no_discovery_parse_errors(cd):
    entries = cd._discover_expected_logging_configs()
    errors = [e for e in entries if "error" in e]
    assert not errors, f"unexpected parse errors: {errors}"


def test_cfn_owned_sfs_expect_error_level_logging(cd):
    entries = {e["sf_name"]: e for e in cd._discover_expected_logging_configs()}
    weekly = entries["ne-weekly-freshness-pipeline"]
    assert weekly["expected_level"] == "ERROR"
    assert weekly["expected_include_execution_data"] is True
    assert weekly["expected_log_group_name"] == "/aws/stepfunctions/ne-weekly-freshness-pipeline"

    preopen = entries["ne-preopen-trading-pipeline"]
    assert preopen["expected_level"] == "ERROR"
    assert preopen["expected_include_execution_data"] is True
    assert preopen["expected_log_group_name"] == "/aws/stepfunctions/ne-preopen-trading-pipeline"


def test_eod_sf_expects_error_level_logging_from_deploy_script(cd):
    entries = {e["sf_name"]: e for e in cd._discover_expected_logging_configs()}
    eod = entries["ne-postclose-trading-pipeline"]
    assert eod["expected_level"] == "ERROR"
    assert eod["expected_include_execution_data"] is True
    assert eod["expected_log_group_name"] == "/aws/stepfunctions/ne-postclose-trading-pipeline"


def test_groom_sf_expects_no_logging(cd):
    """deploy-infrastructure.sh's update_or_create call for groom omits the
    logging arg on purpose — this guard's codified expectation must match
    that, not silently assume logging is wanted everywhere."""
    entries = {e["sf_name"]: e for e in cd._discover_expected_logging_configs()}
    groom = entries["alpha-engine-groom-dispatch"]
    assert groom["expected_level"] == "OFF"


# ── _live_log_group_name ────────────────────────────────────────────────────


def test_live_log_group_name_strips_arn_wrapper(cd):
    logging_config = {
        "level": "ERROR",
        "destinations": [
            {
                "cloudWatchLogsLogGroup": {
                    "logGroupArn": "arn:aws:logs:us-east-1:711398986525:log-group:/aws/stepfunctions/ne-weekly-freshness-pipeline:*"
                }
            }
        ],
    }
    assert cd._live_log_group_name(logging_config) == "/aws/stepfunctions/ne-weekly-freshness-pipeline"


def test_live_log_group_name_none_when_no_destinations(cd):
    assert cd._live_log_group_name({"level": "OFF"}) is None


# ── _check_sf — mocked AWS CLI ───────────────────────────────────────────────


def test_check_sf_no_drift_when_live_matches_codified(cd):
    entry = {
        "sf_name": "ne-weekly-freshness-pipeline",
        "source_file": _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml",
        "expected_level": "ERROR",
        "expected_include_execution_data": True,
        "expected_log_group_name": "/aws/stepfunctions/ne-weekly-freshness-pipeline",
    }
    live = {
        "loggingConfiguration": {
            "level": "ERROR",
            "includeExecutionData": True,
            "destinations": [
                {
                    "cloudWatchLogsLogGroup": {
                        "logGroupArn": "arn:aws:logs:us-east-1:711398986525:log-group:/aws/stepfunctions/ne-weekly-freshness-pipeline:*"
                    }
                }
            ],
        }
    }
    with patch.object(cd.subprocess, "run", return_value=_fake_run(0, json.dumps(live))):
        findings = cd._check_sf(entry)
    assert findings == []


def test_check_sf_detects_recreate_dropped_logging():
    """The exact bug class config#1464 documents: CFN recreate drops
    LoggingConfiguration entirely — live level reverts to OFF."""
    spec = importlib.util.spec_from_file_location("sf_logging_check_drift2", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    entry = {
        "sf_name": "ne-weekly-freshness-pipeline",
        "source_file": _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml",
        "expected_level": "ERROR",
        "expected_include_execution_data": True,
        "expected_log_group_name": "/aws/stepfunctions/ne-weekly-freshness-pipeline",
    }
    live = {"loggingConfiguration": {"level": "OFF"}}
    with patch.object(module.subprocess, "run", return_value=_fake_run(0, json.dumps(live))):
        findings = module._check_sf(entry)
    assert len(findings) == 1
    assert "level drift" in findings[0]
    assert "ERROR" in findings[0] and "OFF" in findings[0]


def test_check_sf_detects_wrong_log_group(cd):
    entry = {
        "sf_name": "ne-weekly-freshness-pipeline",
        "source_file": _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml",
        "expected_level": "ERROR",
        "expected_include_execution_data": True,
        "expected_log_group_name": "/aws/stepfunctions/ne-weekly-freshness-pipeline",
    }
    live = {
        "loggingConfiguration": {
            "level": "ERROR",
            "includeExecutionData": True,
            "destinations": [
                {
                    "cloudWatchLogsLogGroup": {
                        "logGroupArn": "arn:aws:logs:us-east-1:711398986525:log-group:/aws/stepfunctions/some-other-log-group:*"
                    }
                }
            ],
        }
    }
    with patch.object(cd.subprocess, "run", return_value=_fake_run(0, json.dumps(live))):
        findings = cd._check_sf(entry)
    assert len(findings) == 1
    assert "log group drift" in findings[0]


def test_check_sf_missing_state_machine_on_aws(cd):
    entry = {
        "sf_name": "ne-weekly-freshness-pipeline",
        "source_file": _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml",
        "expected_level": "ERROR",
        "expected_include_execution_data": True,
        "expected_log_group_name": "/aws/stepfunctions/ne-weekly-freshness-pipeline",
    }
    with patch.object(
        cd.subprocess,
        "run",
        return_value=_fake_run(255, "", "StateMachineDoesNotExist"),
    ):
        findings = cd._check_sf(entry)
    assert len(findings) == 1
    assert "not found on AWS" in findings[0]


def test_check_sf_groom_no_drift_when_live_is_off(cd):
    entry = {
        "sf_name": "alpha-engine-groom-dispatch",
        "source_file": _REPO_ROOT / "infrastructure" / "deploy-infrastructure.sh",
        "expected_level": "OFF",
        "expected_include_execution_data": None,
        "expected_log_group_name": None,
    }
    live = {}  # describe-state-machine omits loggingConfiguration entirely when disabled
    with patch.object(cd.subprocess, "run", return_value=_fake_run(0, json.dumps(live))):
        findings = cd._check_sf(entry)
    assert findings == []


def test_check_sf_reports_precomputed_error(cd):
    entry = {"sf_name": "broken-sf", "source_file": Path("x"), "error": "boom"}
    findings = cd._check_sf(entry)
    assert findings == ["broken-sf: boom"]


# ── main() exit codes ───────────────────────────────────────────────────────


def test_main_returns_zero_when_clean(cd, monkeypatch):
    fake_entry = {
        "sf_name": "fake-sf",
        "source_file": _REPO_ROOT / "infrastructure" / "deploy-infrastructure.sh",
        "expected_level": "OFF",
        "expected_include_execution_data": None,
        "expected_log_group_name": None,
    }
    monkeypatch.setattr(cd, "_discover_expected_logging_configs", lambda: [fake_entry])
    monkeypatch.setattr(cd, "_check_sf", lambda entry: [])
    monkeypatch.setattr("sys.argv", ["check-drift.py"])
    assert cd.main() == 0


def test_main_returns_one_on_drift(cd, monkeypatch):
    fake_entry = {
        "sf_name": "fake-sf",
        "source_file": _REPO_ROOT / "infrastructure" / "deploy-infrastructure.sh",
        "expected_level": "OFF",
        "expected_include_execution_data": None,
        "expected_log_group_name": None,
    }
    monkeypatch.setattr(cd, "_discover_expected_logging_configs", lambda: [fake_entry])
    monkeypatch.setattr(cd, "_check_sf", lambda entry: ["fake-sf: drifted"])
    monkeypatch.setattr("sys.argv", ["check-drift.py"])
    assert cd.main() == 1


def test_main_name_filter_no_match_returns_two(cd, monkeypatch):
    monkeypatch.setattr(cd, "_discover_expected_logging_configs", lambda: [])
    monkeypatch.setattr("sys.argv", ["check-drift.py", "--name", "does-not-exist"])
    assert cd.main() == 2
