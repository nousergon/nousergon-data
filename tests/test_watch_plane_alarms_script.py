"""Pins the watch-plane Lambda alarm setup script (config#2266; roster
extended to 8 functions + an intake-queue age alarm by config-I2900/I2910).

setup_watch_plane_alarms.sh puts CloudWatch Errors + Throttles alarms on each
of the watch-plane Lambdas — the components whose job is to notice fleet
failures, and whose own failures were previously the one unmonitored failure
mode (docstrings claimed an "error metric + CW alarm" backstop that did not
exist). These tests pin the load-bearing shape:

- All four watch-plane Lambdas must be covered — dropping one silently
  reopens the "who watches the watcher" gap for that function.
- Alarms must route to the INDEPENDENT alpha-engine-alarm-backstop topic,
  never alpha-engine-alerts (the watch plane IS a primary channel; its own
  failures must page via the independent one — setup_pipeline_deadman_alarms.sh
  precedent).
- TreatMissingData must be "notBreaching" — the OPPOSITE of the deadman
  alarms: these alarm on presence of errors, and AWS/Lambda emits no Errors
  datapoint during quiet periods, so missing data is the healthy steady state
  ("breaching" would page continuously on every idle window).
- The topic must be verified before any put-metric-alarm call (fail-fast
  before dangling alarms), and this script must NOT provision the topic —
  setup_pipeline_deadman_alarms.sh is its sole owner.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "setup_watch_plane_alarms.sh"


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


class TestWatchPlaneCoverage:
    """All eight watch-plane Lambdas must be alarmed."""

    @pytest.mark.parametrize(
        "fn_name",
        [
            "alpha-engine-saturday-sf-watch-dispatcher",
            "alpha-engine-sf-watch-spot-dispatcher",
            "alpha-engine-ci-watch-dispatcher",
            "alpha-engine-sf-watch-liveness-probe",
            # The registry-driven unified probe (alpha-engine-config-I2831) — its
            # own Errors metric is the dead-probe backstop the fail-loud contract
            # assumes, same as the sibling probes.
            "alpha-engine-overseer-liveness-probe",
            "alpha-engine-overseer-dispatcher",
            # Added config-I2900: both were Active with ZERO alarm coverage
            # (the same "new Lambda, forgot to onboard it" miss, twice).
            "alpha-engine-alert-drain-dispatcher",
            "alpha-engine-substrate-health-gate",
        ],
    )
    def test_lambda_is_present(self, script_text, fn_name):
        assert fn_name in script_text, (
            f"{fn_name} must be in the watch-plane alarm target set — dropping "
            "it silently reopens the 'who watches the watcher' gap (config#2266)."
        )

    def test_all_functions_declared(self, script_text):
        block = script_text[
            script_text.find("declare -A WATCH_PLANE_FUNCTIONS=(") : script_text.find(
                ")", script_text.find("declare -A WATCH_PLANE_FUNCTIONS=(")
            )
        ]
        assert block.count('"alpha-engine-') == 8

    def test_both_errors_and_throttles_alarmed(self, script_text):
        assert "for metric in Errors Throttles" in script_text


class TestIndependentBackstopTopic:
    def test_routes_to_backstop_topic(self, script_text):
        assert 'BACKSTOP_TOPIC_NAME="alpha-engine-alarm-backstop"' in script_text
        assert '--alarm-actions "$BACKSTOP_TOPIC_ARN"' in script_text
        assert '--ok-actions "$BACKSTOP_TOPIC_ARN"' in script_text

    def test_never_routes_to_primary_alerts_topic(self, script_text):
        # alpha-engine-alerts may appear in prose comments (contrasting it) but
        # must never be constructed as an ARN / used as a routing variable.
        assert ":alpha-engine-alerts" not in script_text
        assert 'SNS_TOPIC_ARN="arn:aws:sns' not in script_text

    def test_topic_verified_before_alarm_wiring(self, script_text):
        verify_pos = script_text.find("aws sns get-topic-attributes")
        first_alarm_pos = script_text.find("aws cloudwatch put-metric-alarm")
        assert verify_pos != -1
        assert first_alarm_pos != -1
        assert verify_pos < first_alarm_pos

    def test_does_not_provision_the_topic(self, script_text):
        # Single-writer convention: setup_pipeline_deadman_alarms.sh is the
        # backstop topic's sole provisioner; this script only consumes it.
        assert "aws sns create-topic" not in script_text
        assert "aws sns subscribe" not in script_text


class TestAlarmSemantics:
    def test_lambda_namespace_and_dimension(self, script_text):
        assert '--namespace "AWS/Lambda"' in script_text
        assert "Name=FunctionName,Value=" in script_text

    def test_fires_at_one_error(self, script_text):
        assert '--comparison-operator "GreaterThanOrEqualToThreshold"' in script_text
        assert "--threshold 1" in script_text
        assert '--statistic "Sum"' in script_text

    def test_treat_missing_data_is_not_breaching(self, script_text):
        # Opposite of the deadman alarms: these alarm on PRESENCE of errors,
        # and AWS/Lambda emits no Errors datapoint when idle — "breaching"
        # would page continuously on every quiet 5-minute window.
        assert '--treat-missing-data "notBreaching"' in script_text
        assert '--treat-missing-data "breaching"' not in script_text

    def test_five_minute_single_period_window(self, script_text):
        assert "--period 300" in script_text
        assert "--evaluation-periods 1" in script_text
        assert "--datapoints-to-alarm 1" in script_text

    def test_alarm_naming_convention(self, script_text):
        assert 'alarm_name="alpha-engine-watch-plane-${label}-${metric_lc}"' in script_text


class TestRegionDefault:
    def test_region_defaults_to_us_east_1(self, script_text):
        assert 'REGION="${AWS_REGION:-us-east-1}"' in script_text


class TestDryRun:
    def test_supports_dry_run_flag(self, script_text):
        assert '"${1:-}" == "--dry-run"' in script_text

    def test_dry_run_gates_mutating_calls_via_run_helper(self, script_text):
        assert "run() {" in script_text
        alarm_block = script_text[script_text.find("Creating per-Lambda") :]
        assert "run aws cloudwatch put-metric-alarm" in alarm_block


class TestDocstringsNowTruthful:
    """config#2266 deliverable 3: the dispatcher docstrings that claimed the
    alarm must now point at the script that actually provisions it."""

    def test_saturday_dispatcher_docstring_names_the_alarm_script(self):
        index_py = (
            _REPO_ROOT
            / "infrastructure"
            / "lambdas"
            / "saturday-sf-watch-dispatcher"
            / "index.py"
        ).read_text()
        assert index_py.count("setup_watch_plane_alarms.sh") >= 2, (
            "Both fail-loud docstrings (module header + _write_watch_log) must "
            "name the alarm-provisioning script — the pre-#2266 wording claimed "
            "a CW alarm that did not exist."
        )


class TestOverseerIntakeDlqAlarm:
    """The intake DLQ depth alarm (alpha-engine-config-I2823) rides this
    script so its backstop-topic + fail-fast discipline applies to it too."""

    def test_dlq_alarm_present(self, script_text):
        assert "nousergon-overseer-intake-dlq" in script_text
        assert "ApproximateNumberOfMessagesVisible" in script_text

    def test_dlq_alarm_routes_to_backstop(self, script_text):
        dlq_block = script_text[script_text.find("overseer-intake-dlq-depth"):]
        assert '--alarm-actions "$BACKSTOP_TOPIC_ARN"' in dlq_block


class TestOverseerIntakeAgeAlarm:
    """The intake queue age-of-oldest-message alarm (alpha-engine-config-I2910)
    — a dead drain never fails delivery, so it never reaches the DLQ; this is
    the alarm that catches messages that were never received at all."""

    def _age_block(self, script_text: str) -> str:
        pos = script_text.find("overseer-intake-age")
        assert pos != -1, "alpha-engine-watch-plane-overseer-intake-age alarm not found"
        return script_text[pos:]

    def test_age_alarm_present_and_targets_the_live_queue_not_the_dlq(self, script_text):
        block = self._age_block(script_text)
        assert "ApproximateAgeOfOldestMessage" in block
        # Must target the live intake queue, NOT the DLQ (that's the point —
        # a dead drain leaves messages on the live queue, never on the DLQ).
        dims_line = block[block.find("--dimensions"): block.find("--dimensions") + 80]
        assert "Value=nousergon-overseer-intake" in dims_line
        assert "Value=nousergon-overseer-intake-dlq" not in dims_line

    def test_age_alarm_routes_to_backstop(self, script_text):
        block = self._age_block(script_text)
        assert '--alarm-actions "$BACKSTOP_TOPIC_ARN"' in block
        assert '--ok-actions "$BACKSTOP_TOPIC_ARN"' in block

    def test_threshold_within_issue_band_18_to_24h(self, script_text):
        # alpha-engine-config-I2910: "~18-24h, comfortably above the 12h
        # drain cadence". Pin the threshold to that band in seconds.
        block = self._age_block(script_text)
        assert "--threshold 72000" in block
        assert 18 * 3600 <= 72000 <= 24 * 3600

    def test_age_alarm_missing_data_not_breaching(self, script_text):
        block = self._age_block(script_text)
        threshold_pos = block.find("--threshold 72000")
        tail = block[threshold_pos:]
        assert '--treat-missing-data "notBreaching"' in tail

    def test_age_alarm_rides_alongside_dlq_alarm(self, script_text):
        # I2910 explicitly asks for this alarm "alongside the existing DLQ
        # alarm" in the same script, not a separate provisioning path.
        assert script_text.find("overseer-intake-dlq-depth") < script_text.find(
            "overseer-intake-age"
        )
