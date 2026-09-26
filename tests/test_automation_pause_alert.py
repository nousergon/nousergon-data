#!/usr/bin/env python3
"""``automation_pause.py --check`` gets a delivery path independent of the
paused groom (alpha-engine-config-I8110 deliverable 1).

Separate file from ``tests/test_automation_pause.py`` by construction: that
file is owned by other in-flight PRs on this repo (#1495/#1496/#1497) and
this change must not touch it. These tests cover only what this issue adds:
``alert_on_findings`` / ``--alert-on-fail`` in ``automation_pause.py``, and
the new ``pause-check-alert.yml`` workflow that runs it on its own hourly-
scale schedule.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
MODULE_PATH = INFRA / "automation_pause.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "pause-check-alert.yml"
DRIFT_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "sf-arn-drift-check.yml"


def _load_module():
    spec = importlib.util.spec_from_file_location("automation_pause_alert_test", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["automation_pause_alert_test"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


SAMPLE_FINDINGS = [
    {
        "trigger": "alpha-engine-alert-drain-2200utc",
        "surface": "scheduler",
        "kind": "unexpectedly-enabled",
        "detail": "paused on 2026-08-07 but live state is ENABLED",
    },
]


# --- alert_on_findings() ----------------------------------------------------

def test_alert_publishes_at_error_severity(mod):
    """error/critical is the only severity that reliably phone-pushes with no
    other consumer watching the channel (alpha-engine-config-I7857)."""
    with patch("krepis.alerts.publish") as mock_publish:
        mod.alert_on_findings(SAMPLE_FINDINGS, source="test-source")
    assert mock_publish.called
    _, kwargs = mock_publish.call_args
    assert kwargs["severity"] == "error"
    assert kwargs["source"] == "test-source"


def test_default_source_is_stable_and_registered(mod):
    """The default source is a repo-relative literal with an alert_classes row.

    It used to default to ``__file__``, which in CI is the runner's absolute
    checkout path, so every page arrived as registry drift under a key that
    moves with the runner layout (alpha-engine-config-I9409).
    """
    import yaml

    with patch("krepis.alerts.publish") as mock_publish:
        mod.alert_on_findings(SAMPLE_FINDINGS)
    source = mock_publish.call_args.kwargs["source"]
    assert source == mod.ALERT_SOURCE == "nousergon-data/infrastructure/automation_pause.py"
    assert not source.startswith("/"), "an absolute path is not a stable registry key"

    playbooks = yaml.safe_load(
        (INFRA / "overseer" / "playbooks.yaml").read_text()
    )
    assert any(r["source"] == source for r in playbooks["alert_classes"]), (
        f"source={source!r} has no alert_classes row in playbooks.yaml"
    )


def test_alert_message_names_every_finding(mod):
    with patch("krepis.alerts.publish") as mock_publish:
        mod.alert_on_findings(SAMPLE_FINDINGS)
    (message,), kwargs = mock_publish.call_args
    assert "alpha-engine-alert-drain-2200utc" in message
    assert "unexpectedly-enabled" in message
    assert "paused on 2026-08-07 but live state is ENABLED" in message


def test_alert_dedup_key_changes_with_the_finding_set(mod):
    """A different finding set must mint a different dedup key, or a NEW
    drift collapses into an unresolved OLD one's dedup window and never
    pages."""
    other_finding = [{**SAMPLE_FINDINGS[0], "trigger": "some-other-schedule"}]
    with patch("krepis.alerts.publish") as mock_publish:
        mod.alert_on_findings(SAMPLE_FINDINGS)
        key_a = mock_publish.call_args.kwargs["dedup_key"]
        mock_publish.reset_mock()
        mod.alert_on_findings(other_finding)
        key_b = mock_publish.call_args.kwargs["dedup_key"]
    assert key_a != key_b


def test_alert_dedup_window_is_shorter_than_the_schedule_cadence(mod):
    """The dedup window must not outlive the workflow's own schedule tick, or
    an unresolved drift silently stops re-paging even though the manifest
    still disagrees with live AWS."""
    with patch("krepis.alerts.publish") as mock_publish:
        mod.alert_on_findings(SAMPLE_FINDINGS)
    window = mock_publish.call_args.kwargs["dedup_window_min"]
    assert window < 4 * 60, "dedup window must stay under the 4-hour schedule cadence"


# --- CLI wiring --------------------------------------------------------------

def test_alert_on_fail_requires_check(mod):
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["automation_pause.py", "--enforce", "--alert-on-fail"]):
            mod.main()


def test_check_without_findings_never_calls_publish(mod):
    """A clean run must not import/call krepis at all — the whole point of
    the lazy import is that plain --check keeps needing nothing beyond the
    stdlib."""
    with patch.object(mod, "check", return_value=[]):
        with patch.object(sys, "argv", ["automation_pause.py", "--check", "--alert-on-fail"]):
            with patch("krepis.alerts.publish") as mock_publish:
                mod.main()
    mock_publish.assert_not_called()


def test_check_with_findings_and_alert_on_fail_calls_publish(mod):
    with patch.object(mod, "check", return_value=SAMPLE_FINDINGS):
        with patch.object(sys, "argv", ["automation_pause.py", "--check", "--alert-on-fail"]):
            with patch("krepis.alerts.publish") as mock_publish:
                mod.main()
    mock_publish.assert_called_once()


def test_check_with_findings_and_no_alert_flag_never_calls_publish(mod):
    """--alert-on-fail must be opt-in — sf-arn-drift-check.yml's existing
    bare `--check` step must not start paging as a side effect of this
    change."""
    with patch.object(mod, "check", return_value=SAMPLE_FINDINGS):
        with patch.object(sys, "argv", ["automation_pause.py", "--check"]):
            with patch("krepis.alerts.publish") as mock_publish:
                mod.main()
    mock_publish.assert_not_called()


# --- the workflow itself ------------------------------------------------------

def test_the_workflow_exists_and_runs_check_alert_on_fail():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "automation_pause.py --check --alert-on-fail" in text


def test_the_check_step_carries_no_if_gate():
    """The reachability regression test_ci_runs_the_pause_check exists to
    prevent, applied to this workflow's own step."""
    text = WORKFLOW.read_text(encoding="utf-8")
    idx = text.index("automation_pause.py --check --alert-on-fail")
    step = text[max(0, idx - 600):idx + 50]
    assert "if:" not in step.split("- name:")[-1]


def test_the_workflow_has_its_own_schedule_independent_of_the_daily_sweep():
    """Deliverable 1 requires its OWN schedule, cadence in hours — not a line
    riding inside sf-arn-drift-check.yml's daily cron."""
    text = WORKFLOW.read_text(encoding="utf-8")
    match = re.search(r"cron:\s*'([^']+)'", text)
    assert match, "no cron schedule found in pause-check-alert.yml"
    minute_field, hour_field, *_ = match.group(1).split()
    # An hours-cadence cron fires via a `*/N` (or explicit list) hour field,
    # never a bare `*` (every minute) or a single fixed hour (once daily).
    assert hour_field != "*", "hour field of '*' is a sub-hourly cadence, not 'hours'"
    ticks_per_day = 24 if hour_field.startswith("*/") is False and "," in hour_field else None
    if hour_field.startswith("*/"):
        step = int(hour_field[2:])
        ticks_per_day = 24 // step
    elif "," in hour_field:
        ticks_per_day = len(hour_field.split(","))
    else:
        ticks_per_day = 1
    assert 2 <= ticks_per_day <= 24, (
        f"cadence resolves to {ticks_per_day} tick(s)/day — deliverable 1 requires a cadence "
        "measured in hours, strictly more frequent than the daily sweep it must not depend on"
    )


def test_the_workflow_installs_krepis_before_calling_alert_on_fail():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "pip install \"krepis" in text
    run_call_idx = text.index("run: python3 infrastructure/automation_pause.py")
    assert text.index("pip install \"krepis") < run_call_idx


def test_the_workflow_is_not_a_pull_request_check():
    """The `on:` trigger block carries no pull_request key — comments
    mentioning the term (explaining why not) are expected and fine."""
    text = WORKFLOW.read_text(encoding="utf-8")
    on_block = text[text.index("\non:"):text.index("\npermissions:")]
    assert "pull_request" not in on_block


def test_deliverable_two_already_runs_enforce_alarms_only_on_a_schedule():
    """Deliverable 2 (scheduled --enforce --alarms-only) is not new: it
    already runs unconditionally on sf-arn-drift-check.yml's daily cron
    trigger, independent of the groom. This test pins that it stays wired so
    a future edit cannot silently drop it while this issue's tracker reads
    it as done."""
    text = DRIFT_WORKFLOW.read_text(encoding="utf-8")
    assert "--enforce --alarms-only" in text
    idx = text.index("automation_pause.py --enforce --alarms-only")
    step = text[max(0, idx - 400):idx + 50]
    assert "if:" not in step.split("- name:")[-1]
    assert re.search(r"schedule:\s*\n\s*-\s*cron:", text), (
        "sf-arn-drift-check.yml must keep a schedule trigger for the --alarms-only "
        "reconciliation to run without a human"
    )
