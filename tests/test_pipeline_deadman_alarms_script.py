"""Pins the pipeline deadman-alarm setup script (config#856 infra item b).

setup_pipeline_deadman_alarms.sh puts a CloudWatch "zero executions" alarm on
each of the three Alpha Engine orchestration state machines, routed through a
SEPARATE SNS topic from the existing alpha-engine-alerts one. The independence
is the entire point (a blackout of the primary alert channel must not also
silence the one alarm that watches for "pipeline didn't even start"), so these
tests pin the load-bearing shape:

- The backstop topic must be DIFFERENT from alpha-engine-alerts.
- All three canonical state machines must be alarmed — dropping one silently
  loses deadman coverage for that pipeline.
- TreatMissingData must be "breaching" (not "notBreaching") — AWS/States does
  not emit an explicit zero datapoint for a quiet state machine, so "breaching"
  is required for the alarm to ever fire at all; "notBreaching" would silently
  make this alarm a no-op.
- The backstop topic must be created/verified before any put-metric-alarm call,
  mirroring the fail-fast-before-dangling-alarms convention used by
  setup_substrate_alarms.sh / setup_changelog_observability_alarms.sh.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "setup_pipeline_deadman_alarms.sh"


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


class TestIndependentBackstopTopic:
    """The whole point of this script: a topic separate from alpha-engine-alerts."""

    def test_uses_a_dedicated_backstop_topic_name(self, script_text):
        assert 'BACKSTOP_TOPIC_NAME="alpha-engine-alarm-backstop"' in script_text

    def test_backstop_topic_is_not_the_primary_alerts_topic(self, script_text):
        # The primary alerts topic name is fine in prose comments (contrasting
        # it), but it must never be constructed as an ARN and used as an
        # actual alarm-routing variable — that would silently reintroduce the
        # single-point-of-failure this script exists to remove.
        assert ":alpha-engine-alerts" not in script_text
        assert 'SNS_TOPIC_ARN="arn:aws:sns' not in script_text

    def test_topic_created_before_any_alarm(self, script_text):
        create_pos = script_text.find("aws sns create-topic")
        first_alarm_pos = script_text.find("aws cloudwatch put-metric-alarm")
        assert create_pos != -1
        assert first_alarm_pos != -1
        assert create_pos < first_alarm_pos

    def test_topic_existence_verified_before_alarm_wiring(self, script_text):
        # Fail-fast-on-missing-topic convention shared with
        # setup_substrate_alarms.sh / setup_changelog_observability_alarms.sh.
        verify_pos = script_text.find("aws sns get-topic-attributes")
        first_alarm_pos = script_text.find("aws cloudwatch put-metric-alarm")
        assert verify_pos != -1
        assert verify_pos < first_alarm_pos

    def test_email_subscription_is_wired(self, script_text):
        assert "aws sns subscribe" in script_text
        assert "--protocol email" in script_text

    def test_all_alarms_route_to_backstop_topic(self, script_text):
        assert '--alarm-actions "$BACKSTOP_TOPIC_ARN"' in script_text
        assert '--ok-actions "$BACKSTOP_TOPIC_ARN"' in script_text


class TestStateMachineCoverage:
    """All three canonical orchestration state machines must be alarmed."""

    @pytest.mark.parametrize(
        "sf_name",
        [
            "ne-weekly-freshness-pipeline",
            "ne-preopen-trading-pipeline",
            "ne-postclose-trading-pipeline",
        ],
    )
    def test_state_machine_is_present(self, script_text, sf_name):
        assert sf_name in script_text, (
            f"{sf_name} must be in the deadman alarm target set — dropping it "
            "silently loses 'did this pipeline even start' coverage for that SF."
        )

    def test_groom_pipeline_intentionally_excluded(self, script_text):
        # Not part of the pipeline-reporting-revamp operator-facing set
        # (crucible-dashboard views/25_Pipeline_Status.py _SF_ORDER). The name
        # may appear in a prose comment explaining the exclusion, but it must
        # never be a value inside the actual STATE_MACHINES array.
        block = script_text[
            script_text.find("declare -A STATE_MACHINES=(") : script_text.find(")", script_text.find("declare -A STATE_MACHINES=("))
        ]
        assert "alpha-engine-groom-pipeline" not in block

    def test_three_state_machines_declared(self, script_text):
        assert re.search(r"declare -A STATE_MACHINES=\(", script_text)
        # Exactly 3 quoted values assigned in the associative array block.
        block = script_text[
            script_text.find("declare -A STATE_MACHINES=(") : script_text.find(")", script_text.find("declare -A STATE_MACHINES=("))
        ]
        assert block.count('"ne-') == 3


class TestAlarmSemantics:
    def test_watches_executions_started_metric(self, script_text):
        assert '--namespace "AWS/States"' in script_text
        assert '--metric-name "ExecutionsStarted"' in script_text

    def test_dimension_is_state_machine_arn(self, script_text):
        assert "Name=StateMachineArn,Value=" in script_text

    def test_fires_below_one_execution(self, script_text):
        assert '--comparison-operator "LessThanThreshold"' in script_text
        assert "--threshold 1" in script_text
        assert '--statistic "Sum"' in script_text

    def test_treat_missing_data_is_breaching(self, script_text):
        # Regression guard: AWS/States emits NO datapoint at all during a
        # quiet period, so "zero executions" IS missing data. notBreaching
        # would silently make this alarm never fire — the opposite of a
        # deadman switch.
        assert '--treat-missing-data "breaching"' in script_text
        assert '--treat-missing-data "notBreaching"' not in script_text

    def test_period_is_seven_days(self, script_text):
        # A 7-day trailing window avoids the weekday/weekend missing-data
        # ambiguity a 1-day window would have for the two weekday-cadence
        # pipelines (AWS/States reports no datapoint on ANY quiet day,
        # expected-weekend or genuinely-broken-weekday alike).
        assert "--period 604800" in script_text

    def test_evaluation_window_is_single_period(self, script_text):
        assert "--evaluation-periods 1" in script_text
        assert "--datapoints-to-alarm 1" in script_text


class TestRegionDefault:
    def test_region_defaults_to_us_east_1(self, script_text):
        assert 'REGION="${AWS_REGION:-us-east-1}"' in script_text


class TestDryRun:
    def test_supports_dry_run_flag(self, script_text):
        assert '"${1:-}" == "--dry-run"' in script_text

    def test_dry_run_gates_mutating_calls_via_run_helper(self, script_text):
        assert "run() {" in script_text
        # The alarm-creation call must be dispatched through the run() gate
        # (or an explicit DRY_RUN check) so --dry-run never mutates AWS.
        alarm_block = script_text[script_text.find("Creating per-state-machine") :]
        assert "run aws cloudwatch put-metric-alarm" in alarm_block
