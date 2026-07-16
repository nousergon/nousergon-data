"""Tests for infrastructure/step-functions/check-definition-drift.py
(alpha-engine-config#2273).

Covers the SF DEFINITION drift guard: the codified file->state-machine map,
git-stamp normalization (a stamp-only difference is NOT drift), and the
compare-against-live + compare-against-S3-staged-copy logic (mocked
`aws` CLI calls — no real AWS access). Mirrors the sibling
test_sf_logging_config_check_drift.py's module-load + mocked-subprocess
pattern.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "infrastructure" / "step-functions" / "check-definition-drift.py"


@pytest.fixture(scope="module")
def cd():
    spec = importlib.util.spec_from_file_location("sf_definition_check_drift", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_SAMPLE_DEF = {
    "Comment": "weekly pipeline",
    "StartAt": "A",
    "States": {
        "A": {"Type": "Pass", "Next": "B"},
        "B": {"Type": "Succeed"},
    },
}


def _fake_run(returncode=0, stdout="", stderr=""):
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    return result


def _dispatcher(live_def=None, s3_def=None, sf_missing=False, s3_missing=False):
    """subprocess.run side_effect routing describe-state-machine vs s3 cp."""

    def run(cmd, **kwargs):
        if "stepfunctions" in cmd:
            if sf_missing:
                return _fake_run(255, "", "StateMachineDoesNotExist")
            return _fake_run(0, json.dumps({"definition": json.dumps(live_def)}))
        if "s3" in cmd:
            if s3_missing:
                return _fake_run(1, "", "fatal error: An error occurred (404) when calling the HeadObject operation: Not Found")
            return _fake_run(0, json.dumps(s3_def))
        raise AssertionError(f"unexpected aws call: {cmd}")

    return run


@pytest.fixture()
def fake_repo(cd, tmp_path, monkeypatch):
    """Point the module at a tmp repo holding one definition file."""
    infra = tmp_path / "infrastructure"
    infra.mkdir()
    (infra / "fake.json").write_text(json.dumps(_SAMPLE_DEF, indent=2))
    monkeypatch.setattr(cd, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cd, "INFRA_DIR", infra)
    return {"sf_name": "fake-sf", "definition_file": "fake.json"}


# ── codified map against the real repo ──────────────────────────────────────


def test_map_covers_all_six_orchestrated_state_machines(cd):
    names = {e["sf_name"] for e in cd.SF_DEFINITIONS}
    assert names == {
        "ne-weekly-freshness-pipeline",
        "ne-preopen-trading-pipeline",
        "ne-postclose-trading-pipeline",
        "alpha-engine-groom-dispatch",
        # alpha-engine-config-I2544/I2545: advisory + Sunday-modelzoo child SFs.
        "ne-weekly-advisory-pipeline",
        "ne-modelzoo-sunday-pipeline",
    }


def test_every_mapped_definition_file_exists(cd):
    for entry in cd.SF_DEFINITIONS:
        path = cd.INFRA_DIR / entry["definition_file"]
        assert path.is_file(), f"{entry['sf_name']} maps to missing {path}"


def test_map_covers_every_sf_definition_file_in_infrastructure(cd):
    """A new step_function*.json must be added to the drift map — same
    deploy-coverage class as test_deploy_infrastructure_sf_coverage.py."""
    mapped = {e["definition_file"] for e in cd.SF_DEFINITIONS}
    on_disk = {p.name for p in cd.INFRA_DIR.glob("step_function*.json")}
    assert on_disk == mapped, (
        f"drift map out of sync with infrastructure/: on disk {sorted(on_disk)} "
        f"vs mapped {sorted(mapped)} — add the new SF to SF_DEFINITIONS."
    )


def test_s3_constants_match_deploy_script(cd):
    """The staged-copy location must be the one the deploy paths write."""
    deploy = (_REPO_ROOT / "infrastructure" / "deploy-infrastructure.sh").read_text()
    assert f'BUCKET="{cd.S3_BUCKET}"' in deploy
    assert cd.S3_PREFIX == "infrastructure/"


def test_groom_entry_uses_live_dispatch_name_not_stale_pipeline_name(cd):
    """alpha-engine-config#2391: deploy-infrastructure.sh targets
    `alpha-engine-groom-dispatch` — the OLD `alpha-engine-groom-pipeline`
    name is a dead state machine. This map previously still carried the
    stale name, which meant this drift guard would have quietly checked
    nothing real even if it had been wired up (describe-state-machine on
    a dead ARN reads as missing-in-aws, not as "extend coverage")."""
    entries = {e["sf_name"]: e for e in cd.SF_DEFINITIONS}
    assert "alpha-engine-groom-pipeline" not in entries
    assert entries["alpha-engine-groom-dispatch"]["definition_file"] == "step_function_groom.json"


def test_groom_arn_in_deploy_script_matches_map(cd):
    """The map's groom sf_name must be the same literal deploy-infrastructure.sh
    actually creates/updates (GROOM_ARN), not a name that merely looks plausible."""
    deploy = (_REPO_ROOT / "infrastructure" / "deploy-infrastructure.sh").read_text()
    entries = {e["sf_name"]: e for e in cd.SF_DEFINITIONS}
    groom_name = entries["alpha-engine-groom-dispatch"]["sf_name"]
    assert f'GROOM_ARN="arn:aws:states:$REGION:${{ACCOUNT_ID}}:stateMachine:{groom_name}"' in deploy


# ── normalization ───────────────────────────────────────────────────────────


def test_normalize_strips_git_stamp(cd):
    stamped = dict(_SAMPLE_DEF, Comment="[git:abc1234] weekly pipeline")
    assert cd._normalize(stamped) == cd._normalize(_SAMPLE_DEF)


def test_normalize_detects_real_comment_change(cd):
    changed = dict(_SAMPLE_DEF, Comment="[git:abc1234] a DIFFERENT comment")
    assert cd._normalize(changed) != cd._normalize(_SAMPLE_DEF)


def test_normalize_is_order_insensitive(cd):
    reordered = json.loads(json.dumps(_SAMPLE_DEF))
    reordered["States"] = dict(reversed(list(reordered["States"].items())))
    assert cd._normalize(reordered) == cd._normalize(_SAMPLE_DEF)


def test_normalize_does_not_mutate_input(cd):
    stamped = dict(_SAMPLE_DEF, Comment="[git:abc1234] weekly pipeline")
    cd._normalize(stamped)
    assert stamped["Comment"] == "[git:abc1234] weekly pipeline"


# ── _check_sf — mocked AWS CLI ──────────────────────────────────────────────


def _stamped(d):
    out = json.loads(json.dumps(d))
    out["Comment"] = f"[git:deadbeef] {out.get('Comment', '')}".rstrip()
    return out


def test_check_sf_clean_when_all_three_copies_match(cd, fake_repo):
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(_SAMPLE_DEF), s3_def=_stamped(_SAMPLE_DEF)),
    ):
        findings = cd._check_sf(fake_repo)
    assert findings == []


def test_check_sf_detects_live_drift_and_names_the_state(cd, fake_repo):
    drifted = json.loads(json.dumps(_SAMPLE_DEF))
    drifted["States"]["B"] = {"Type": "Fail"}
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(drifted), s3_def=_stamped(_SAMPLE_DEF)),
    ):
        findings = cd._check_sf(fake_repo)
    assert len(findings) == 1
    assert "LIVE" in findings[0]
    assert "B" in findings[0]


def test_check_sf_detects_stale_s3_staged_copy(cd, fake_repo):
    stale = json.loads(json.dumps(_SAMPLE_DEF))
    stale["States"]["A"] = {"Type": "Pass", "Next": "B", "ResultPath": "$.x"}
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(_SAMPLE_DEF), s3_def=_stamped(stale)),
    ):
        findings = cd._check_sf(fake_repo)
    assert len(findings) == 1
    assert "S3 staged copy" in findings[0]
    assert "CFN" in findings[0]  # the rollback hazard must be spelled out


def test_check_sf_stamp_only_difference_is_not_drift(cd, fake_repo):
    """The exact false-positive class: live+S3 carry the deploy git stamp,
    the repo file does not."""
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(_SAMPLE_DEF), s3_def=_SAMPLE_DEF),
    ):
        findings = cd._check_sf(fake_repo)
    assert findings == []


def test_check_sf_missing_state_machine_on_aws(cd, fake_repo):
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(s3_def=_stamped(_SAMPLE_DEF), sf_missing=True),
    ):
        findings = cd._check_sf(fake_repo)
    assert len(findings) == 1
    assert "not found" in findings[0]


def test_check_sf_missing_s3_staged_object(cd, fake_repo):
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(_SAMPLE_DEF), s3_missing=True),
    ):
        findings = cd._check_sf(fake_repo)
    assert len(findings) == 1
    assert "missing" in findings[0]


def test_check_sf_missing_repo_file(cd, fake_repo):
    entry = {"sf_name": "fake-sf", "definition_file": "nope.json"}
    findings = cd._check_sf(entry)
    assert len(findings) == 1
    assert "not found" in findings[0]


# ── groom-dispatch specifically (alpha-engine-config#2391) ──────────────────
#
# Exercises the real SF_DEFINITIONS entry for alpha-engine-groom-dispatch
# (not a fake_repo stand-in) against the actual on-disk
# infrastructure/step_function_groom.json, mirroring how the weekly/daily/eod
# entries are covered above — same _dispatcher mocked-CLI shape, just pointed
# at the groom entry and the real repo file instead of a tmp_path fixture.


def _groom_entry(cd):
    return next(e for e in cd.SF_DEFINITIONS if e["sf_name"] == "alpha-engine-groom-dispatch")


def test_check_sf_groom_dispatch_clean_when_all_three_copies_match(cd):
    entry = _groom_entry(cd)
    repo_def = json.loads((_REPO_ROOT / "infrastructure" / entry["definition_file"]).read_text())
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(repo_def), s3_def=_stamped(repo_def)),
    ):
        findings = cd._check_sf(entry)
    assert findings == []


def test_check_sf_groom_dispatch_detects_live_drift(cd):
    """The exact incident class config#2391 documents: an ASL change lands in
    the repo but never reaches the live groom-dispatch state machine (e.g.
    because a deploy path targeted the wrong/dead SF name) — this must be
    reported as LIVE drift and fail the check."""
    entry = _groom_entry(cd)
    repo_def = json.loads((_REPO_ROOT / "infrastructure" / entry["definition_file"]).read_text())
    stale_live = json.loads(json.dumps(repo_def))
    stale_live["Comment"] = "a stale groom comment from 11 days ago"
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(stale_live), s3_def=_stamped(repo_def)),
    ):
        findings = cd._check_sf(entry)
    assert len(findings) == 1
    assert "alpha-engine-groom-dispatch" in findings[0]
    assert "LIVE" in findings[0]


def test_check_sf_groom_dispatch_detects_stale_s3_staged_copy(cd):
    entry = _groom_entry(cd)
    repo_def = json.loads((_REPO_ROOT / "infrastructure" / entry["definition_file"]).read_text())
    stale_s3 = json.loads(json.dumps(repo_def))
    stale_s3["Comment"] = "a stale S3-staged groom comment"
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(live_def=_stamped(repo_def), s3_def=_stamped(stale_s3)),
    ):
        findings = cd._check_sf(entry)
    assert len(findings) == 1
    assert "alpha-engine-groom-dispatch" in findings[0]
    assert "S3 staged copy" in findings[0]


def test_check_sf_groom_dispatch_missing_on_aws(cd):
    """If deploy-infrastructure.sh regresses back to the dead
    alpha-engine-groom-pipeline name (or any other wrong target), describe-
    state-machine on the codified alpha-engine-groom-dispatch ARN comes back
    missing — this must fail loud, not silently pass."""
    entry = _groom_entry(cd)
    repo_def = json.loads((_REPO_ROOT / "infrastructure" / entry["definition_file"]).read_text())
    with patch.object(
        cd.subprocess,
        "run",
        side_effect=_dispatcher(s3_def=_stamped(repo_def), sf_missing=True),
    ):
        findings = cd._check_sf(entry)
    assert len(findings) == 1
    assert "not found" in findings[0]


def test_aws_cli_hard_exits_on_unexpected_failure(cd):
    """A broken CLI/creds state must never read as 'no drift'."""
    with patch.object(cd.subprocess, "run", return_value=_fake_run(255, "", "AccessDenied")):
        with pytest.raises(SystemExit) as exc:
            cd._aws_cli("stepfunctions", "describe-state-machine")
    assert exc.value.code == 2


# ── main() exit codes ───────────────────────────────────────────────────────


def test_main_returns_zero_when_clean(cd, monkeypatch):
    monkeypatch.setattr(cd, "_check_sf", lambda entry: [])
    monkeypatch.setattr("sys.argv", ["check-definition-drift.py"])
    assert cd.main() == 0


def test_main_returns_one_on_drift(cd, monkeypatch):
    monkeypatch.setattr(cd, "_check_sf", lambda entry: [f"{entry['sf_name']}: drifted"])
    monkeypatch.setattr(cd, "_alert_on_drift", lambda findings, **kw: None)
    monkeypatch.setattr("sys.argv", ["check-definition-drift.py"])
    assert cd.main() == 1


def test_main_name_filter_no_match_returns_two(cd, monkeypatch):
    monkeypatch.setattr("sys.argv", ["check-definition-drift.py", "--name", "does-not-exist"])
    assert cd.main() == 2


def test_main_name_filter_scopes_to_one_sf(cd, monkeypatch):
    checked = []
    monkeypatch.setattr(cd, "_check_sf", lambda entry: checked.append(entry["sf_name"]) or [])
    monkeypatch.setattr(
        "sys.argv", ["check-definition-drift.py", "--name", "ne-weekly-freshness-pipeline"]
    )
    assert cd.main() == 0
    assert checked == ["ne-weekly-freshness-pipeline"]


# ── SNS alerting on drift (alpha-engine-config#2391 acceptance criterion 3) ──
#
# Mirrors validators/constituents_drift_check.py's lazy-import +
# try/except-around-publish pattern; nousergon_lib isn't installed in this
# test environment, so these tests fake the module via sys.modules rather
# than requiring the real dependency (same spirit as mocking the aws CLI
# above — no real network/SNS access in CI).


class _FakePublishResult:
    def __init__(self, sns_ok=True, telegram_ok=True):
        self.sns = type("Chan", (), {"ok": sns_ok})()
        self.telegram = type("Chan", (), {"ok": telegram_ok})()
        self.any_ok = sns_ok or telegram_ok


@pytest.fixture()
def fake_nousergon_lib_alerts(monkeypatch):
    """Install a fake nousergon_lib.alerts module and return the recorded
    publish() calls list."""
    import sys
    import types

    calls: list[dict] = []

    def fake_publish(message, **kwargs):
        calls.append({"message": message, **kwargs})
        return _FakePublishResult()

    fake_alerts_module = types.ModuleType("nousergon_lib.alerts")
    fake_alerts_module.publish = fake_publish
    fake_lib_module = types.ModuleType("nousergon_lib")
    fake_lib_module.alerts = fake_alerts_module

    monkeypatch.setitem(sys.modules, "nousergon_lib", fake_lib_module)
    monkeypatch.setitem(sys.modules, "nousergon_lib.alerts", fake_alerts_module)
    return calls


def test_alert_on_drift_publishes_via_nousergon_lib(cd, fake_nousergon_lib_alerts):
    cd._alert_on_drift(["alpha-engine-groom-dispatch: definition drift (LIVE vs x)"])
    assert len(fake_nousergon_lib_alerts) == 1
    call = fake_nousergon_lib_alerts[0]
    assert "alpha-engine-groom-dispatch" in call["message"]
    assert call["severity"] == "error"
    assert "check-definition-drift.py" in call["source"]


def test_alert_on_drift_missing_nousergon_lib_does_not_raise(cd, monkeypatch):
    """No nousergon_lib installed (this test env's actual default state) must
    degrade to a logged warning, never an exception — the drift check's exit
    code is the authoritative signal even when alerting is unavailable."""
    import builtins

    real_import = builtins.__import__

    def blocking_import(name, *args, **kwargs):
        if name == "nousergon_lib" or name.startswith("nousergon_lib."):
            raise ImportError("no module named nousergon_lib")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocking_import)
    cd._alert_on_drift(["fake-sf: drifted"])  # must not raise


def test_main_fires_alert_on_drift(cd, monkeypatch, fake_nousergon_lib_alerts):
    fake_entry = {
        "sf_name": "alpha-engine-groom-dispatch",
        "definition_file": "step_function_groom.json",
    }
    monkeypatch.setattr(cd, "SF_DEFINITIONS", (fake_entry,))
    monkeypatch.setattr(cd, "_check_sf", lambda entry: [f"{entry['sf_name']}: drifted"])
    monkeypatch.setattr("sys.argv", ["check-definition-drift.py"])
    assert cd.main() == 1
    assert len(fake_nousergon_lib_alerts) == 1


def test_main_no_drift_does_not_fire_alert(cd, monkeypatch, fake_nousergon_lib_alerts):
    fake_entry = {
        "sf_name": "alpha-engine-groom-dispatch",
        "definition_file": "step_function_groom.json",
    }
    monkeypatch.setattr(cd, "SF_DEFINITIONS", (fake_entry,))
    monkeypatch.setattr(cd, "_check_sf", lambda entry: [])
    monkeypatch.setattr("sys.argv", ["check-definition-drift.py"])
    assert cd.main() == 0
    assert fake_nousergon_lib_alerts == []


def test_main_no_alert_flag_suppresses_alert_even_on_drift(cd, monkeypatch, fake_nousergon_lib_alerts):
    fake_entry = {
        "sf_name": "alpha-engine-groom-dispatch",
        "definition_file": "step_function_groom.json",
    }
    monkeypatch.setattr(cd, "SF_DEFINITIONS", (fake_entry,))
    monkeypatch.setattr(cd, "_check_sf", lambda entry: [f"{entry['sf_name']}: drifted"])
    monkeypatch.setattr("sys.argv", ["check-definition-drift.py", "--no-alert"])
    assert cd.main() == 1
    assert fake_nousergon_lib_alerts == []
