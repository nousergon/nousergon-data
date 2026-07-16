"""Pins the config-runner launch-rate alarm setup script
(alpha-engine-config#2697).

setup_config_runner_launch_rate_alarm.sh puts a CloudWatch alarm on the
AlphaEngine/Infra config_runner_launches custom metric (emitted by
config-runner-dispatcher/index.py's _emit_launch_metric() on every successful
spot launch) — the early-warning signal for the 2026-07-15 spot-quota-
starvation runaway (~150 launches/45min with only a single quota page at the
very end). These tests pin the load-bearing shape:

- The script must exist, be executable, and be valid bash.
- It alarms on the correct custom namespace/metric (not AWS/Lambda Errors —
  that alarms on FAILURE; this alarms on launch RATE, independent of
  success/failure).
- Sum >= 10 over a 15-minute (900s) window, single evaluation period.
- Routes to the existing alpha-engine-alerts SNS topic (this is a
  production-impacting resource alarm like setup_research_runner_timeout_alarm.sh,
  not a watch-the-watchers alarm, so it deliberately does NOT need the
  independent backstop topic).
- The topic is verified before any put-metric-alarm call (fail-fast before a
  dangling alarm), and supports --dry-run.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "setup_config_runner_launch_rate_alarm.sh"


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


class TestMetricSemantics:
    def test_custom_namespace_and_metric_name(self, script_text):
        assert 'NAMESPACE="AlphaEngine/Infra"' in script_text
        assert 'METRIC_NAME="config_runner_launches"' in script_text
        assert '--namespace "$NAMESPACE"' in script_text
        assert '--metric-name "$METRIC_NAME"' in script_text

    def test_fires_at_ten_launches_per_fifteen_minutes(self, script_text):
        assert 'THRESHOLD="10"' in script_text
        assert 'PERIOD_SECONDS="900"' in script_text
        assert '--comparison-operator "GreaterThanOrEqualToThreshold"' in script_text
        assert '--statistic "Sum"' in script_text

    def test_single_evaluation_period(self, script_text):
        assert "--evaluation-periods 1" in script_text
        assert "--datapoints-to-alarm 1" in script_text

    def test_treat_missing_data_is_not_breaching(self, script_text):
        # Zero launches in a 15-minute window is the healthy common case
        # between CI bursts — AWS emits no datapoint at all, and that must
        # not page.
        assert '--treat-missing-data "notBreaching"' in script_text


class TestAlertRouting:
    def test_routes_to_primary_alerts_topic(self, script_text):
        # Unlike the watch-plane alarms, this is a production-impacting
        # resource alarm (like setup_research_runner_timeout_alarm.sh), not a
        # watch-the-watchers alarm — the primary alpha-engine-alerts topic is
        # correct here.
        assert 'SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"' in script_text
        assert '--alarm-actions "$SNS_TOPIC_ARN"' in script_text
        assert '--ok-actions "$SNS_TOPIC_ARN"' in script_text

    def test_topic_verified_before_alarm_wiring(self, script_text):
        verify_pos = script_text.find("aws sns get-topic-attributes")
        alarm_pos = script_text.find("aws cloudwatch put-metric-alarm")
        assert verify_pos != -1
        assert alarm_pos != -1
        assert verify_pos < alarm_pos


class TestRegionDefault:
    def test_region_defaults_to_us_east_1(self, script_text):
        assert 'REGION="${AWS_REGION:-us-east-1}"' in script_text


class TestDryRun:
    def test_supports_dry_run_flag(self, script_text):
        assert '"${1:-}" == "--dry-run"' in script_text

    def test_dry_run_gates_the_alarm_call_via_run_helper(self, script_text):
        assert "run() {" in script_text
        assert "run aws cloudwatch put-metric-alarm" in script_text
