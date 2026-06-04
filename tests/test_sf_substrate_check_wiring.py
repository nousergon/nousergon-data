"""Pins the Phase 2 → 3 substrate-health-check wiring in the Saturday SF.

The new states ``WeeklySubstrateHealthCheck`` and
``WaitForWeeklySubstrateHealthCheck`` chain off the end of the existing
``WaitForSaturdayHealthCheck`` and run the row-driven
``alpha_engine_lib.transparency`` checker on the dashboard EC2.

Catches regressions like:
- Someone reroutes ``WaitForSaturdayHealthCheck.Next`` back to
  ``NotifyComplete`` and silently drops the substrate check.
- Someone removes the substrate state thinking it's redundant with the
  artifact-freshness check (it isn't — different abstractions, see PR
  body for the deprecation timeline).
- Someone flips the substrate Catch into a hard-fail and starts halting
  the pipeline on row-level failure (per-row alarms own paging — the
  Catch is for SSM/infra failures only).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


class TestStatePresence:
    """Both new states must exist and chain after the existing freshness check."""

    def test_weekly_substrate_check_state_exists(self, states):
        assert "WeeklySubstrateHealthCheck" in states

    def test_wait_for_weekly_substrate_check_exists(self, states):
        assert "WaitForWeeklySubstrateHealthCheck" in states


class TestChainOrdering:
    """Wiring goes: SaturdayHealthCheck → WaitForSat → Substrate → WaitForSubstrate → Notify."""

    def test_wait_for_saturday_health_check_routes_to_substrate(self, states):
        wait_state = states["WaitForSaturdayHealthCheck"]
        assert wait_state["Next"] == "WeeklySubstrateHealthCheck", (
            "WaitForSaturdayHealthCheck must hand off to the substrate check, "
            "not skip directly to NotifyComplete."
        )

    def test_wait_for_saturday_catch_routes_to_substrate(self, states):
        catches = states["WaitForSaturdayHealthCheck"]["Catch"]
        assert any(c["Next"] == "WeeklySubstrateHealthCheck" for c in catches), (
            "If freshness polling fails, substrate check must still run — "
            "they're independent observability paths."
        )

    def test_substrate_check_routes_to_wait_state(self, states):
        assert states["WeeklySubstrateHealthCheck"]["Next"] == (
            "WaitForWeeklySubstrateHealthCheck"
        )

    def test_wait_for_substrate_routes_to_notify_complete(self, states):
        # Post Friday-PM shell-run spine (feat/sf-friday-shell-run): the
        # success edge is gated through CheckShellRunNotify so a Friday
        # dry-pass gets a shell-run-tagged email. The gate's Default is the
        # unchanged NotifyComplete, so the REAL Saturday run (no shell_run
        # input) still ends at NotifyComplete — strict superset preserved.
        #
        # The non-fatal ReportCard state (evaluator Report Card v2) now sits
        # between the substrate poll and the notify gate; both its Next and its
        # Catch land on CheckShellRunNotify, preserving the success edge.
        assert (
            states["WaitForWeeklySubstrateHealthCheck"]["Next"] == "ReportCard"
        )
        assert states["ReportCard"]["Next"] == "CheckShellRunNotify"
        assert all(c["Next"] == "CheckShellRunNotify" for c in states["ReportCard"]["Catch"])
        assert states["CheckShellRunNotify"]["Default"] == "NotifyComplete"


class TestCatchSemantics:
    """Substrate failures must NOT halt the pipeline.

    Per-row CloudWatch alarms own paging; the SF Catch only fires on
    infra-level failures (SSM unreachable, EC2 down). Either way, the
    failure path must terminate at NotifyComplete, not HandleFailure.
    """

    def test_substrate_check_catch_is_non_blocking(self, states):
        catches = states["WeeklySubstrateHealthCheck"]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["Next"] == "NotifyComplete", (
                f"Substrate Catch must continue to NotifyComplete, not "
                f"{c['Next']!r} — the substrate check is observability, not gating."
            )

    def test_substrate_wait_catch_is_non_blocking(self, states):
        catches = states["WaitForWeeklySubstrateHealthCheck"]["Catch"]
        for c in catches:
            assert c["Next"] == "NotifyComplete"


class TestCommandShape:
    """The SSM command must invoke the lib CLI with --cadence weekly --alert.

    Drops here would silently neuter the check (e.g. dropping --alert
    suppresses SNS without changing exit code; dropping --cadence flips
    to argparse error).
    """

    @pytest.fixture
    def commands(self, states) -> list[str]:
        return states["WeeklySubstrateHealthCheck"]["Parameters"]["Parameters"]["commands"]

    def test_invokes_transparency_module(self, commands):
        assert any(
            "python -m alpha_engine_lib.transparency" in cmd for cmd in commands
        )

    def test_passes_cadence_weekly(self, commands):
        joined = " ".join(commands)
        assert "--cadence weekly" in joined

    def test_passes_alert_flag(self, commands):
        joined = " ".join(commands)
        assert "--alert" in joined, (
            "Without --alert, row-level failures emit metrics but no SNS. "
            "Removing this flag silently degrades the gate."
        )

    def test_runs_on_dashboard_ec2(self, commands):
        # The dispatcher EC2 has the lib installed; confirm we cd there.
        joined = " ".join(commands)
        assert "alpha-engine-dashboard" in joined

    def test_pulls_latest_dashboard_main_before_running(self, commands):
        # Stale repo on the dispatcher would run an outdated lib pin.
        joined = " ".join(commands)
        assert "git" in joined and "pull" in joined


class TestResultPathIsolation:
    """The substrate state must not stomp on the freshness state's result."""

    def test_distinct_result_paths(self, states):
        sat_path = states["SaturdayHealthCheck"]["ResultPath"]
        sub_path = states["WeeklySubstrateHealthCheck"]["ResultPath"]
        assert sat_path != sub_path, (
            "Both states use ssm:sendCommand and need separate ResultPath "
            "fields so the wait states can resolve the right CommandId."
        )

    def test_wait_state_reads_substrate_command_id(self, states):
        params = states["WaitForWeeklySubstrateHealthCheck"]["Parameters"]
        # SF Parameters use ``CommandId.$`` (the dot-dollar suffix marks
        # the value as a JSONPath reference rather than a literal).
        cmd_id = params["CommandId.$"]
        assert "substrate_check_result" in cmd_id, (
            "WaitForWeeklySubstrateHealthCheck must poll the substrate "
            "command, not the freshness command."
        )
