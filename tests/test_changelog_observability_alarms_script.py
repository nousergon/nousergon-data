"""Pins the watch-the-watchers alarm setup script (config#1273 Phase B).

`infrastructure/setup_changelog_observability_alarms.sh` puts CloudWatch
Errors alarms on the alert/monitoring infra Lambdas → the alpha-engine-alerts
SNS topic. A silent regression here is dangerous (the alarms are the only
direct failure signal for the changelog mirrors, which are excluded from the
log-capture path), so this test pins the load-bearing constants:

- SNS topic typo → alarms fire but no one is paged.
- Wrong metric namespace/name → the alarm watches nothing.
- A changelog mirror dropped from the target list → its failures go fully dark
  (no capture AND no alarm).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parent.parent
    / "infrastructure"
    / "setup_changelog_observability_alarms.sh"
)


@pytest.fixture(scope="module")
def script_text() -> str:
    return _SCRIPT.read_text()


def test_script_exists_and_is_executable():
    assert _SCRIPT.is_file()
    assert _SCRIPT.stat().st_mode & 0o111, "script must be chmod +x"


def test_bash_syntax_is_valid():
    result = subprocess.run(["bash", "-n", str(_SCRIPT)], capture_output=True)
    assert result.returncode == 0, result.stderr.decode()


def test_sns_topic_is_alpha_engine_alerts(script_text):
    assert "alpha-engine-alerts" in script_text
    assert "--alarm-actions" in script_text and "--ok-actions" in script_text


def test_watches_the_lambda_errors_metric(script_text):
    assert '--namespace "AWS/Lambda"' in script_text
    assert '--metric-name "Errors"' in script_text
    # schedule-driven Lambdas must stay quiet between invocations
    assert '--treat-missing-data "notBreaching"' in script_text


@pytest.mark.parametrize(
    "mirror",
    [
        "alpha-engine-changelog-cloudwatch-mirror",
        "alpha-engine-changelog-incident-mirror",
    ],
)
def test_both_changelog_mirrors_are_alarmed(script_text, mirror):
    # The mirrors are EXCLUDED from changelog-cloudwatch-mirror's
    # TARGET_FUNCTIONS (recursion guard), so this alarm is their only failure
    # signal — they must never drop out of the target list.
    assert mirror in script_text, f"{mirror} must be in the alarm target list"
