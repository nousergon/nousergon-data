"""Tests for infrastructure/step-functions/check-lambda-existence.py
(alpha-engine-config#1464, 2026-07-08 EOD incident follow-up).

Covers: discovery of lambda:invoke states (top-level, nested inside Map
Iterator/ItemProcessor, and Parallel Branches), FunctionName normalization
(bare name / full ARN / :version-or-alias-qualified), and the
compare-against-live existence check (mocked `aws lambda get-function` CLI
calls — no real AWS access, mirrors the sibling check-drift.py tests'
module-load + mocked-subprocess pattern).
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "infrastructure" / "step-functions" / "check-lambda-existence.py"


@pytest.fixture(scope="module")
def cle():
    spec = importlib.util.spec_from_file_location("sf_lambda_existence_check", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_run(returncode=0, stdout="", stderr=""):
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    return result


# ── the codified map against the real repo ──────────────────────────────────


def test_map_covers_all_four_orchestrated_state_machines(cle):
    names = {e["sf_name"] for e in cle.SF_DEFINITIONS}
    assert names == {
        "ne-weekly-freshness-pipeline",
        "ne-preopen-trading-pipeline",
        "ne-postclose-trading-pipeline",
        "alpha-engine-groom-dispatch",
    }


def test_discovers_the_2026_07_08_incident_reference(cle):
    """The exact bug class this guard exists for: step_function_eod.json's
    LaunchPostMarketDataSpot / LaunchPostMarketArcticAppendSpot states invoke
    alpha-engine-data-spot-dispatcher (config#1767 Phase 2, nousergon-data#643)."""
    refs = cle._discover_referenced_functions("ne-postclose-trading-pipeline", "step_function_eod.json")
    errors = [r for r in refs if "error" in r]
    assert not errors, f"unexpected parse errors: {errors}"
    normalized = {r["normalized_name"] for r in refs}
    assert "alpha-engine-data-spot-dispatcher" in normalized


def test_every_codified_sf_definition_parses_cleanly(cle):
    """Every real SF definition file in the repo must be discoverable without
    a source-error — a parse failure here means the JSON shape changed in a
    way this script's walker no longer handles."""
    for entry in cle.SF_DEFINITIONS:
        refs = cle._discover_referenced_functions(entry["sf_name"], entry["definition_file"])
        errors = [r for r in refs if "error" in r]
        assert not errors, f"{entry['sf_name']}: unexpected parse errors: {errors}"


# ── _normalize_function_name ────────────────────────────────────────────────


def test_normalize_bare_name(cle):
    assert cle._normalize_function_name("alpha-engine-scheduled-groom-dispatcher") == (
        "alpha-engine-scheduled-groom-dispatcher"
    )


def test_normalize_full_arn(cle):
    arn = "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-ssm-liveness-poller"
    assert cle._normalize_function_name(arn) == "alpha-engine-ssm-liveness-poller"


def test_normalize_full_arn_with_qualifier(cle):
    arn = "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-predictor-inference:live"
    assert cle._normalize_function_name(arn) == "alpha-engine-predictor-inference"


def test_normalize_bare_name_with_qualifier(cle):
    assert cle._normalize_function_name("alpha-engine-predictor-inference:live") == (
        "alpha-engine-predictor-inference"
    )


# ── _walk_states — discovery across Map/Parallel nesting ───────────────────


def test_walk_states_finds_top_level_task(cle):
    states = {
        "Invoke": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {"FunctionName": "my-fn"},
        },
    }
    found = cle._walk_states(states)
    assert found == [{"state_name": "Invoke", "function_name": "my-fn"}]


def test_walk_states_finds_sync_suffixed_resource(cle):
    states = {
        "Invoke": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke.waitForTaskToken",
            "Parameters": {"FunctionName": "my-fn"},
        },
    }
    found = cle._walk_states(states)
    assert len(found) == 1


def test_walk_states_ignores_non_lambda_task(cle):
    states = {
        "Poll": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:ssm:getCommandInvocation",
            "Parameters": {},
        },
    }
    assert cle._walk_states(states) == []


def test_walk_states_descends_into_map_iterator(cle):
    states = {
        "MapState": {
            "Type": "Map",
            "Iterator": {
                "StartAt": "Invoke",
                "States": {
                    "Invoke": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::lambda:invoke",
                        "Parameters": {"FunctionName": "nested-fn"},
                    },
                },
            },
        },
    }
    found = cle._walk_states(states)
    assert found == [{"state_name": "Invoke", "function_name": "nested-fn"}]


def test_walk_states_descends_into_map_item_processor(cle):
    """Newer ASL uses ItemProcessor instead of Iterator for Map states."""
    states = {
        "MapState": {
            "Type": "Map",
            "ItemProcessor": {
                "StartAt": "Invoke",
                "States": {
                    "Invoke": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::lambda:invoke",
                        "Parameters": {"FunctionName": "nested-fn-2"},
                    },
                },
            },
        },
    }
    found = cle._walk_states(states)
    assert found == [{"state_name": "Invoke", "function_name": "nested-fn-2"}]


def test_walk_states_descends_into_parallel_branches(cle):
    states = {
        "ParallelState": {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": "InvokeA",
                    "States": {
                        "InvokeA": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {"FunctionName": "branch-fn"},
                        },
                    },
                },
            ],
        },
    }
    found = cle._walk_states(states)
    assert found == [{"state_name": "InvokeA", "function_name": "branch-fn"}]


def test_walk_states_flags_missing_function_name(cle):
    states = {
        "Invoke": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {},
        },
    }
    found = cle._walk_states(states)
    assert found == [{"state_name": "Invoke", "function_name": None}]


# ── _discover_referenced_functions — file-level ─────────────────────────────


def test_discover_missing_definition_file(cle, monkeypatch):
    monkeypatch.setattr(cle, "REPO_ROOT", Path("/nonexistent-root"))
    refs = cle._discover_referenced_functions("fake-sf", "fake.json")
    assert len(refs) == 1
    assert "not found" in refs[0]["error"]


def test_discover_unparseable_definition_file(cle, tmp_path, monkeypatch):
    infra = tmp_path / "infrastructure"
    infra.mkdir()
    (infra / "broken.json").write_text("{not valid json")
    monkeypatch.setattr(cle, "REPO_ROOT", tmp_path)
    refs = cle._discover_referenced_functions("fake-sf", "broken.json")
    assert len(refs) == 1
    assert "not valid JSON" in refs[0]["error"]


def test_discover_flags_missing_function_name_as_error(cle, tmp_path, monkeypatch):
    infra = tmp_path / "infrastructure"
    infra.mkdir()
    defn = {
        "StartAt": "Invoke",
        "States": {
            "Invoke": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {},
                "End": True,
            },
        },
    }
    (infra / "fake.json").write_text(json.dumps(defn))
    monkeypatch.setattr(cle, "REPO_ROOT", tmp_path)
    refs = cle._discover_referenced_functions("fake-sf", "fake.json")
    assert len(refs) == 1
    assert "no (or non-string) Parameters.FunctionName" in refs[0]["error"]


# ── _aws_lambda_get_function — mocked AWS CLI ───────────────────────────────


def test_get_function_exists(cle):
    with patch.object(cle.subprocess, "run", return_value=_fake_run(0, "{}")):
        assert cle._aws_lambda_get_function("some-fn") is True


def test_get_function_not_found(cle):
    with patch.object(
        cle.subprocess, "run",
        return_value=_fake_run(254, "", "An error occurred (ResourceNotFoundException) when calling the GetFunction operation"),
    ):
        assert cle._aws_lambda_get_function("missing-fn") is False


def test_get_function_other_aws_error_is_fatal(cle):
    """An unrelated AWS CLI failure (auth, throttling) must exit non-zero
    rather than silently report false-clean."""
    with patch.object(
        cle.subprocess, "run",
        return_value=_fake_run(255, "", "An error occurred (AccessDeniedException)"),
    ):
        with pytest.raises(SystemExit) as exc_info:
            cle._aws_lambda_get_function("some-fn")
    assert exc_info.value.code == 2


# ── _check_sf — end-to-end per-SF, mocked AWS CLI ───────────────────────────


@pytest.fixture()
def fake_repo(cle, tmp_path, monkeypatch):
    infra = tmp_path / "infrastructure"
    infra.mkdir()
    defn = {
        "StartAt": "Invoke",
        "States": {
            "Invoke": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": "fake-target-fn"},
                "End": True,
            },
        },
    }
    (infra / "fake.json").write_text(json.dumps(defn))
    monkeypatch.setattr(cle, "REPO_ROOT", tmp_path)
    return {"sf_name": "fake-sf", "definition_file": "fake.json"}


def test_check_sf_clean_when_function_exists(cle, fake_repo):
    with patch.object(cle.subprocess, "run", return_value=_fake_run(0, "{}")):
        findings = cle._check_sf(fake_repo)
    assert findings == []


def test_check_sf_reports_missing_function(cle, fake_repo):
    """The exact 2026-07-08 incident class: SF references a Lambda that was
    never deployed."""
    with patch.object(
        cle.subprocess, "run",
        return_value=_fake_run(254, "", "An error occurred (ResourceNotFoundException)"),
    ):
        findings = cle._check_sf(fake_repo)
    assert len(findings) == 1
    assert "fake-target-fn" in findings[0]
    assert "does not exist on AWS" in findings[0]


def test_check_sf_reports_precomputed_source_error(cle, monkeypatch, tmp_path):
    monkeypatch.setattr(cle, "REPO_ROOT", tmp_path)  # empty dir -> file missing
    findings = cle._check_sf({"sf_name": "fake-sf", "definition_file": "absent.json"})
    assert len(findings) == 1
    assert "not found" in findings[0]


# ── main() — CLI surface ────────────────────────────────────────────────────


def test_main_unknown_name_exits_2(cle, capsys):
    with patch.object(cle.sys, "argv", ["check-lambda-existence.py", "--name", "not-a-real-sf"]):
        exit_code = cle.main()
    assert exit_code == 2
    assert "no codified definition mapping" in capsys.readouterr().err


def test_main_clean_run_exits_0(cle, tmp_path, monkeypatch, capsys):
    infra = tmp_path / "infrastructure"
    infra.mkdir()
    defn = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
    (infra / "fake.json").write_text(json.dumps(defn))
    monkeypatch.setattr(cle, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cle, "SF_DEFINITIONS", ({"sf_name": "fake-sf", "definition_file": "fake.json"},))
    with patch.object(cle.sys, "argv", ["check-lambda-existence.py"]):
        exit_code = cle.main()
    assert exit_code == 0
    assert "OK:" in capsys.readouterr().out


def test_main_drift_exits_1(cle, tmp_path, monkeypatch, capsys):
    infra = tmp_path / "infrastructure"
    infra.mkdir()
    defn = {
        "StartAt": "Invoke",
        "States": {
            "Invoke": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": "gone-fn"},
                "End": True,
            },
        },
    }
    (infra / "fake.json").write_text(json.dumps(defn))
    monkeypatch.setattr(cle, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cle, "SF_DEFINITIONS", ({"sf_name": "fake-sf", "definition_file": "fake.json"},))
    with patch.object(cle.sys, "argv", ["check-lambda-existence.py"]), \
         patch.object(cle.subprocess, "run", return_value=_fake_run(254, "", "ResourceNotFoundException")):
        exit_code = cle.main()
    assert exit_code == 1
    assert "drift detected" in capsys.readouterr().out
