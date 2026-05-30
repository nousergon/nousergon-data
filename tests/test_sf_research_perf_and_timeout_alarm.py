"""L4464 — Research-stage perf cleanup + named timeout alarm.

Pins:
  1. The Saturday SF Research Lambda payload sets skip_dry_run_gate=true so
     the scheduled production path skips the in-handler stub-LLM dry-run gate
     (a full second graph pass + a redundant ~4-min fetch_data — ~8 min of
     the 900s budget). The gate's wiring validation lives in CI + the Friday
     shell-run preflight, not the hot path.
  2. The research-runner timeout alarm script exists and alarms on the
     Lambda Duration approaching the 900s ceiling (a timeout-specific signal
     the existing -errors alarm misses, since a hard timeout doesn't hit the
     Errors metric and runs no in-process code).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_SF = _REPO / "infrastructure" / "step_function.json"
_ALARM = _REPO / "infrastructure" / "setup_research_runner_timeout_alarm.sh"


def _find_state(states: dict, name: str) -> dict | None:
    """Recursively locate a state by name (it lives inside a Parallel branch)."""
    if name in states:
        return states[name]
    for s in states.values():
        for br in s.get("Branches", []) or []:
            found = _find_state(br.get("States", {}), name)
            if found:
                return found
    return None


@pytest.fixture(scope="module")
def research_payload() -> dict:
    sf = json.loads(_SF.read_text())
    research = _find_state(sf["States"], "Research")
    assert research is not None, "Research state not found in SF"
    return research["Parameters"]["Payload"]


class TestSkipDryRunGate:
    def test_skip_dry_run_gate_present_and_true(self, research_payload):
        assert research_payload.get("skip_dry_run_gate") is True, (
            "Research payload must set skip_dry_run_gate=true so the scheduled "
            "production path skips the redundant stub graph pass + double "
            "fetch_data (L4464 perf)."
        )

    def test_research_dry_path_preserved(self, research_payload):
        # The shell-run dry signal must still thread through (Friday preflight).
        assert research_payload.get("dry_run_llm.$") == "$.research_dry"


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
