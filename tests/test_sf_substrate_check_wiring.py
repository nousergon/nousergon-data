"""Pins the Phase 2 → 3 substrate-health-check wiring in the Saturday SF.

The new states ``WeeklySubstrateHealthCheck`` and
``WaitForWeeklySubstrateHealthCheck`` chain off the end of the existing
``WaitForSaturdayHealthCheck`` and run the row-driven
``nousergon_lib.transparency`` checker on the dashboard EC2.

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

from tests.sf_command_utils import extract_commands


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
    """Wiring goes: SaturdayHealthCheck → WaitForSat → CheckSatStatus →
    Substrate → WaitForSubstrate → CheckSubStatus → Notify (config#2276
    turned each check-once poll into a poll-to-terminal-status loop)."""

    def test_wait_for_saturday_health_check_routes_to_status_choice(self, states):
        wait_state = states["WaitForSaturdayHealthCheck"]
        assert wait_state["Next"] == "CheckSaturdayHealthCheckStatus", (
            "WaitForSaturdayHealthCheck must hand off to the terminal-status "
            "Choice (config#2276 poll loop), not fire-and-forget onward."
        )

    def test_saturday_status_success_routes_to_substrate(self, states):
        choice = states["CheckSaturdayHealthCheckStatus"]
        success = next(
            r for r in choice["Choices"] if r.get("StringEquals") == "Success"
        )
        assert success["Next"] == "WeeklySubstrateHealthCheck", (
            "A successful freshness check must hand off to the substrate "
            "check, not skip directly to NotifyComplete."
        )

    def test_wait_for_saturday_catch_routes_to_degraded_then_substrate(self, states):
        catches = states["WaitForSaturdayHealthCheck"]["Catch"]
        assert any(c["Next"] == "SaturdayHealthCheckDegraded" for c in catches), (
            "If freshness polling fails, the degraded flag must be set — "
            "pre-config#2276 this continued silently."
        )
        assert (
            states["SaturdayHealthCheckDegraded"]["Next"]
            == "WeeklySubstrateHealthCheck"
        ), (
            "A degraded freshness check must still run the substrate check — "
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
        # Two non-fatal advisory states (evaluator Report Card v2, then the
        # Director) sit between the substrate poll and the notify gate. ReportCard's
        # SUCCESS edge feeds the Director (which weighs the fresh card); ReportCard's
        # Catch routes to PublishReportCardDegraded (config#2302: a WARNING alert —
        # advisory grading failed silently for 9 days pre-fix) which then continues to
        # notify (no card to weigh). The Director's own Next lands on CheckShellRunNotify;
        # its Catch routes to PublishDirectorDegraded (same config#2302 WARNING-alert
        # shape) which then continues to notify. Every path still preserves the success
        # edge. On the Friday preflight both states RUN (dry, via
        # dry_run.$=$.research_dry — ROADMAP L4504), they are not skipped, so the wiring
        # is identical on real + preflight runs.
        # config#2276: the substrate poll now resolves to a terminal status
        # first; its Success edge is what feeds ReportCard (pinned below in
        # test_wait_for_substrate_routes_via_status_choice).
        assert states["ReportCard"]["Next"] == "Director"
        assert all(
            c["Next"] == "PublishReportCardDegraded" for c in states["ReportCard"]["Catch"]
        )
        assert states["PublishReportCardDegraded"]["Next"] == "CheckShellRunNotify"
        assert states["Director"]["Next"] == "CheckShellRunNotify"
        assert all(
            c["Next"] == "PublishDirectorDegraded" for c in states["Director"]["Catch"]
        )
        assert states["PublishDirectorDegraded"]["Next"] == "CheckShellRunNotify"
        # config#2278: the real-run success edge now passes through the
        # gate-degraded completion Choice before NotifyComplete.
        assert states["CheckShellRunNotify"]["Default"] == "CheckGateDegradedNotify"
        assert states["CheckGateDegradedNotify"]["Default"] == "NotifyComplete"

    def test_wait_for_substrate_routes_via_status_choice(self, states):
        # config#2276: the substrate poll resolves to a terminal status
        # before ReportCard, so a failing/hung checker is visible.
        assert (
            states["WaitForWeeklySubstrateHealthCheck"]["Next"]
            == "CheckSubstrateHealthCheckStatus"
        )
        choice = states["CheckSubstrateHealthCheckStatus"]
        success = next(
            r for r in choice["Choices"] if r.get("StringEquals") == "Success"
        )
        assert success["Next"] == "ReportCard"


class TestCatchSemantics:
    """Substrate failures must NOT halt the pipeline — but must be VISIBLE.

    Per-row CloudWatch alarms own paging; the SF Catch only fires on
    infra-level failures (SSM unreachable, EC2 down). config#2276: the
    failure path sets $.health_check_degraded (SubstrateHealthCheckDegraded
    Pass) and CONTINUES to the advisory tail — never HandleFailure, and
    never the plain-SUCCESS NotifyComplete either (that was the silent-skip
    masking this issue closed). Full degraded-flag threading is pinned in
    tests/test_sf_health_check_honesty_wiring.py.
    """

    def test_substrate_check_catch_is_non_blocking_but_visible(self, states):
        catches = states["WeeklySubstrateHealthCheck"]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["Next"] == "SubstrateHealthCheckDegraded", (
                f"Substrate Catch must set the degraded flag, not go to "
                f"{c['Next']!r} — observability, not gating; visible, not silent."
            )

    def test_substrate_wait_catch_is_non_blocking_but_visible(self, states):
        catches = states["WaitForWeeklySubstrateHealthCheck"]["Catch"]
        for c in catches:
            assert c["Next"] == "SubstrateHealthCheckDegraded"

    def test_substrate_degraded_continues_to_advisory_tail(self, states):
        degraded = states["SubstrateHealthCheckDegraded"]
        assert degraded["Type"] == "Pass"
        assert degraded["Next"] == "ReportCard", (
            "A degraded substrate check must not skip the ReportCard/Director "
            "Lambda tail — it is independent of the dashboard box."
        )


class TestCommandShape:
    """The SSM command must invoke the lib CLI with --cadence weekly --alert.

    Drops here would silently neuter the check (e.g. dropping --alert
    suppresses SNS without changing exit code; dropping --cadence flips
    to argparse error).

    config#2322: the commands array was converted from a static ``commands``
    list to a ``commands.$`` ASL intrinsic (``States.Array(...)``) so the
    phase-marker-sweep command can thread ``export RUN_DATE.$=$.run_date``
    (same shape as the Backtester/Parity/Evaluator spot stages —
    tests/test_sf_run_date_threading.py) — extract_commands() reads through
    either shape.
    """

    @pytest.fixture
    def commands(self, states) -> list[str]:
        return extract_commands(states["WeeklySubstrateHealthCheck"])

    def test_invokes_transparency_module(self, commands):
        assert any(
            "python -m nousergon_lib.transparency" in cmd for cmd in commands
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

    def test_no_runtime_pip_install(self, commands):
        # config#2276: deps are synced at deploy time (crucible-dashboard
        # infrastructure/deploy-on-merge.sh pip-installs on requirements.txt
        # diff; nousergon-lib is tag-pinned there) — a live PyPI dependency
        # mid-pipeline is forbidden.
        joined = " ".join(commands)
        assert "pip install" not in joined


class TestPhaseMarkerSweep:
    """config#2322: post-run phase-marker sweep must run after the
    constituents-drift check, on the same run_date the backtest chain
    wrote its `.phases/` markers under, and must not be able to abort the
    SF (the existing States.ALL Catch on this state already makes any
    non-zero exit non-blocking — see TestCatchSemantics)."""

    @pytest.fixture
    def commands(self, states) -> list[str]:
        return extract_commands(states["WeeklySubstrateHealthCheck"])

    def test_invokes_phase_marker_sweep_module(self, commands):
        assert any(
            "python -m validators.phase_marker_sweep" in cmd for cmd in commands
        )

    def test_phase_marker_sweep_passes_alert_flag(self, commands):
        sweep_cmd = next(
            c for c in commands if "validators.phase_marker_sweep" in c
        )
        assert "--alert" in sweep_cmd

    def test_phase_marker_sweep_runs_after_constituents_drift(self, commands):
        drift_idx = next(
            i for i, c in enumerate(commands)
            if "validators.constituents_drift_check" in c
        )
        sweep_idx = next(
            i for i, c in enumerate(commands)
            if "validators.phase_marker_sweep" in c
        )
        assert drift_idx < sweep_idx

    def test_run_date_exported_before_phase_marker_sweep(self, commands):
        rd_idx = next(
            i for i, c in enumerate(commands)
            if c.startswith("export RUN_DATE=")
        )
        sweep_idx = next(
            i for i, c in enumerate(commands)
            if "validators.phase_marker_sweep" in c
        )
        assert rd_idx < sweep_idx

    def test_phase_marker_sweep_reads_exported_run_date(self, commands):
        sweep_cmd = next(
            c for c in commands if "validators.phase_marker_sweep" in c
        )
        assert '--run-date "$RUN_DATE"' in sweep_cmd

    def test_run_date_threaded_from_sf_run_date(self, states):
        # value is threaded from the SF-stamped $.run_date (States.Format),
        # same contract as tests/test_sf_run_date_threading.py's spot stages.
        raw_expr = states["WeeklySubstrateHealthCheck"]["Parameters"]["Parameters"]["commands.$"]
        assert "States.Format('export RUN_DATE=" in raw_expr
        assert "$.run_date" in raw_expr


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
