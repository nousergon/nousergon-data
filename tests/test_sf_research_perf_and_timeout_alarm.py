"""L4464 — Research-stage perf cleanup + named timeout alarm.

config#1687 (2026-07-06): the weekly heavy pass migrated off the Lambda onto
spot-EC2 (spot_research_weekly.sh -> weekly_box_runner.py). The two original
pins survive in migrated form:
  1. skip_dry_run_gate=true is now the BOX RUNNER's default (pinned in
     crucible-research tests/test_weekly_box_runner.py) — here we pin that
     the SF command invokes the launcher with --force and WITHOUT
     --no-skip-dry-run-gate, and that the shell-run dry path routes via
     $.preflight_args like every other spot state.
  2. The research-runner timeout alarm script still exists and alarms on
     Lambda Duration near the 900s ceiling — the runner Lambda STAYS for
     intraday alerts + operator modes, so the alarm remains load-bearing
     for those invokes.
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
def research_command() -> str:
    sf = json.loads(_SF.read_text())
    research = _find_state(sf["States"], "Research")
    assert research is not None, "Research state not found in SF"
    return research["Parameters"]["Parameters"]["commands.$"]


class TestSkipDryRunGate:
    def test_launcher_invoked_with_production_defaults(self, research_command):
        # --force mirrors the retired Lambda payload's force:true; the
        # skip_dry_run_gate=true production optimization is the box runner's
        # DEFAULT (crucible-research tests pin it) — asserting the override
        # flag is ABSENT keeps the L4464 perf semantics.
        assert "spot_research_weekly.sh --force" in research_command
        assert "--no-skip-dry-run-gate" not in research_command

    def test_research_dry_path_preserved(self, research_command):
        # The shell-run dry signal routes via $.preflight_args (Option-C spot
        # mechanism) instead of the retired dry_run_llm Lambda payload ref.
        assert "$.preflight_args" in research_command


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
