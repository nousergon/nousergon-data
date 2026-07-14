"""L4464 — named research-runner timeout alarm.

Pins: the research-runner timeout alarm script exists and alarms on the
Lambda Duration approaching the 900s ceiling (a timeout-specific signal
the existing -errors alarm misses, since a hard timeout doesn't hit the
Errors metric and runs no in-process code). alpha-engine-runner:live is
still invoked at TimeoutSeconds=900 by the ChallengerShadow state
(alpha-engine-config-I2515 Phase B), so this alarm remains load-bearing
even though the multi-agent Research state's own weekly_run invocation
(and its skip_dry_run_gate perf pin, previously tested here) was removed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_ALARM = _REPO / "infrastructure" / "setup_research_runner_timeout_alarm.sh"


class TestTimeoutAlarm:
    @pytest.fixture(scope="class")
    def alarm_src(self) -> str:
        assert _ALARM.exists(), f"{_ALARM.name} must exist (L4464 named timeout alarm)"
        return _ALARM.read_text()

    def test_alarms_on_lambda_duration(self, alarm_src):
        assert '--namespace "AWS/Lambda"' in alarm_src
        assert '--metric-name "Duration"' in alarm_src
        assert "alpha-engine-research-runner" in alarm_src

    def test_threshold_near_900s_ceiling(self, alarm_src):
        # 30s below the 900000ms ceiling — fires on timeout AND near-miss.
        assert re.search(r'THRESHOLD="8[0-9]{5}"', alarm_src), (
            "threshold should be just below the 900000ms Lambda ceiling"
        )
        assert "GreaterThanOrEqualToThreshold" in alarm_src
        assert "Maximum" in alarm_src

    def test_routes_to_alerts_topic(self, alarm_src):
        assert "alpha-engine-alerts" in alarm_src
