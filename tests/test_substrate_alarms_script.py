"""Pins the substrate alarm setup script to the lib's metric namespace.

The setup_substrate_alarms.sh script is idempotent and run once per
threshold change, but its alarms are useless if the namespace + metric
name don't match what alpha_engine_lib.transparency.emit_cloudwatch_metrics
publishes.

These tests catch that drift class:
- Namespace mismatch → alarms never fire
- Metric name mismatch → alarms never fire
- SNS topic typo → alarms fire but no one is paged
- Row enumeration syntactically broken → script aborts before creating
  per-row alarms but still creates the aggregate (deceptive partial
  success)
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "setup_substrate_alarms.sh"


@pytest.fixture(scope="module")
def script_text() -> str:
    return _SCRIPT.read_text()


def test_script_exists_and_is_executable():
    assert _SCRIPT.is_file()
    # Must be executable so the operator can run it directly.
    assert _SCRIPT.stat().st_mode & 0o111


def test_bash_syntax_is_valid():
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    result = subprocess.run([bash, "-n", str(_SCRIPT)], capture_output=True)
    assert result.returncode == 0, result.stderr.decode()


class TestNamespaceAlignmentWithLib:
    """The script's namespace + metric names must match what the lib emits."""

    def test_namespace_matches_lib_constant(self, script_text):
        from alpha_engine_lib.transparency import DEFAULT_NAMESPACE_OUT

        assert f'NAMESPACE="{DEFAULT_NAMESPACE_OUT}"' in script_text, (
            f"Script namespace must match alpha_engine_lib.transparency."
            f"DEFAULT_NAMESPACE_OUT={DEFAULT_NAMESPACE_OUT!r} — otherwise "
            f"alarms attach to a namespace nothing emits to."
        )

    def test_per_row_metric_matches_lib(self, script_text):
        # The lib emits per-row metrics named "SubstrateRowOK" with a
        # RowID dimension; the alarm must reference the exact same
        # metric name.
        assert 'PER_ROW_METRIC="SubstrateRowOK"' in script_text

    def test_aggregate_metric_matches_lib(self, script_text):
        assert 'AGGREGATE_METRIC="SubstrateChecksFailed"' in script_text


class TestSNSTarget:
    """The alarm target must be the existing alpha-engine-alerts topic."""

    def test_sns_topic_is_alpha_engine_alerts(self, script_text):
        # Matches the topic created by deploy_step_function.sh, reused
        # across pipeline alerts.
        assert "alpha-engine-alerts" in script_text

    def test_topic_existence_check_runs_before_alarm_creation(self, script_text):
        # Pattern: get-topic-attributes ... exit 1 must appear before
        # any put-metric-alarm call. This avoids creating alarms with
        # broken SNS targets (silent paging failures).
        topic_check_pos = script_text.find("get-topic-attributes")
        first_alarm_pos = script_text.find("put-metric-alarm")
        assert topic_check_pos != -1
        assert first_alarm_pos != -1
        assert topic_check_pos < first_alarm_pos


class TestRowEnumeration:
    """Row IDs come from the lib's inventory — no hardcoded list to drift."""

    def test_row_enumeration_uses_lib(self, script_text):
        assert "from alpha_engine_lib.transparency import load_inventory" in script_text

    def test_row_enumeration_iterates_inventory(self, script_text):
        # Sanity-check the comprehension still iterates the inventory key.
        assert re.search(
            r"r\['id'\] for r in load_inventory\(\)\['inventory'\]",
            script_text,
        )

    def test_aborts_when_enumeration_returns_empty(self, script_text):
        # The script must hard-exit if ROW_IDS is empty — otherwise it
        # silently skips per-row alarms and creates only the aggregate.
        assert "could not enumerate inventory rows" in script_text


class TestAlarmSemantics:
    """Per-row + aggregate alarms must use the right comparison + statistic."""

    def test_per_row_alarm_fires_below_one(self, script_text):
        # The lib emits 1=ok/pending, 0=fail. Alarm must fire when the
        # value drops below 1.
        assert '--comparison-operator "LessThanThreshold"' in script_text
        assert "--threshold 1" in script_text

    def test_per_row_alarm_uses_minimum_statistic(self, script_text):
        # Minimum across the period — if any datapoint is 0, the alarm
        # fires. Average would let a single fail get diluted.
        assert '--statistic "Minimum"' in script_text

    def test_aggregate_alarm_fires_above_zero(self, script_text):
        assert '--comparison-operator "GreaterThanThreshold"' in script_text
        # Aggregate threshold lives near the aggregate alarm definition.
        agg_block = script_text[script_text.find("aggregate_name"):]
        assert "--threshold 0" in agg_block

    def test_treat_missing_data_is_not_breaching(self, script_text):
        # Weekly-cadence rows emit once per Sat SF — between emissions,
        # CloudWatch sees missing data. notBreaching means missing days
        # don't fire alarms; only emitted-and-failed days fire.
        assert '--treat-missing-data "notBreaching"' in script_text

    def test_period_is_one_hour_for_hourly_refresh(self, script_text):
        """Period=3600 (1h) is the post-2026-05-08 cadence: alarm state
        reflects the most recent SF emission within ~1h instead of the
        ~24-37h lag of the original Period=86400 (24h) cadence.
        Regression for the cadence-lag P0 (ROADMAP line 2082)."""
        assert "--period 3600" in script_text
        # Old daily-period must not creep back in.
        assert "--period 86400" not in script_text, (
            "Period=86400 reintroduces the 24-37h alarm-state lag — "
            "regression of the 2026-05-08 cadence fix"
        )

    def test_evaluation_window_stays_24_hours(self, script_text):
        """EvalPeriods=24 × Period=3600 keeps the same 24h trailing
        window as before — only refresh cadence changes. Pinning the
        product so a future "tighten Period further" PR doesn't
        accidentally narrow the actual window."""
        assert "--evaluation-periods 24" in script_text
        assert "--datapoints-to-alarm 1" in script_text


class TestRegionDefault:
    def test_region_defaults_to_us_east_1(self, script_text):
        # All Alpha Engine infra lives in us-east-1; matches the
        # eval-quality alarm script.
        assert 'REGION="${AWS_REGION:-us-east-1}"' in script_text
