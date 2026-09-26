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
from tests.sf_degraded_summary_helpers import assert_degraded_continuation


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
        assert_degraded_continuation(
            states, "SaturdayHealthCheckDegraded", "WeeklySubstrateHealthCheck",
        )  # a degraded freshness check must still run the substrate check

    def test_substrate_check_routes_to_wait_state(self, states):
        # alpha-engine-config-I5687: WeeklySubstrateHealthCheck dispatches
        # through the poll-budget seed (InitSubstrateHealthCheckPollCount)
        # before the first poll, mirroring DataPhase2/ThinkTank.
        assert states["WeeklySubstrateHealthCheck"]["Next"] == (
            "InitSubstrateHealthCheckPollCount"
        )
        assert states["InitSubstrateHealthCheckPollCount"]["Next"] == (
            "WaitForWeeklySubstrateHealthCheck"
        )
        assert states["InitSubstrateHealthCheckPollCount"]["ResultPath"] == (
            "$.substrate_check_polls"
        )

    def test_wait_for_substrate_routes_to_notify_complete(self, states):
        # Post Friday-PM shell-run spine (feat/sf-friday-shell-run): the
        # success edge is gated through CheckShellRunNotify so a Friday
        # dry-pass gets a shell-run-tagged email. The gate's Default is the
        # unchanged NotifyComplete, so the REAL Saturday run (no shell_run
        # input) still ends at NotifyComplete — strict superset preserved.
        #
        # Two advisory states (evaluator Report Card v2, then the Director)
        # sit between the substrate poll and the notify gate. ReportCard's
        # SUCCESS edge feeds the Director (which weighs the fresh card);
        # ReportCard's Catch routes to ReportCardDegraded (config#6685: sets
        # $.report_card_degraded so it threads into the terminal-notify
        # selection) which then continues, unchanged, to
        # PublishReportCardDegraded (config#2302: a WARNING alert — advisory
        # grading failed silently for 9 days pre-fix) and on to notify (no
        # card to weigh). The Director's own Next lands on CheckShellRunNotify
        # on success. config#6408 (Brian's 2026-08-04 operator ruling):
        # Director's Catch now routes to NormalizeFailureContext — a
        # Director failure terminates the execution FAILED rather than
        # reporting degraded success.
        # config#6054: ReportCard's success edge lands on the Director's
        # per-stage skip gate (Default: Director); Director's success edge
        # lands on the DirectorComplete rerun witness before the notify gate.
        assert states["ReportCard"]["Next"] == "CheckSkipDirector"
        assert states["CheckSkipDirector"]["Default"] == "Director"
        assert all(
            c["Next"] == "ReportCardDegraded" for c in states["ReportCard"]["Catch"]
        )
        assert_degraded_continuation(states, "ReportCardDegraded", "PublishReportCardDegraded")
        # alpha-engine-config-I7813: both edges now pass through the
        # observe-only scanner leaderboard leaf's gate before the notify gate.
        assert states["PublishReportCardDegraded"]["Next"] == "CheckSkipScannerLeaderboard"
        # alpha-engine-config-I11299: Director's success edge now passes through
        # CheckDirectorSubResults (§2.3b), whose Default is DirectorComplete and
        # whose degraded branch converges on it too. The witness property is
        # unchanged: every route here descends from that ONE success edge.
        assert states["Director"]["Next"] == "CheckDirectorRetroRefused"
        assert states["CheckDirectorRetroRefused"]["Default"] == "CheckDirectorSubResults"
        assert states["CheckDirectorSubResults"]["Default"] == "DirectorComplete"
        assert states["DirectorComplete"]["Next"] == "CheckSkipScannerLeaderboard"
        assert states["CheckSkipScannerLeaderboard"]["Default"] == "ScannerLeaderboard"
        # alpha-engine-config-I7194: the leaf now hands off to the
        # cost-aggregation gate, the tail's last stage, which runs the
        # aggregator AFTER Director and then enters the notify gate.
        assert states["ScannerLeaderboardComplete"]["Next"] == "CheckSkipAggregateCosts"
        assert states["AggregateCosts"]["Next"] == "CheckShellRunNotify"
        assert all(
            c["Next"] == "NormalizeFailureContext" for c in states["Director"]["Catch"]
        )
        assert all(
            c["ResultPath"] == "$.error" for c in states["Director"]["Catch"]
        )
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
        # config#6054: success lands on the advisory tail (gate Default:
        # ReportCard). alpha-engine-config-I7620: RunScope is now the tail's
        # head — it derives this execution's own scope and must sit after every
        # work stage and before the card that renders grades against it, so both
        # routes into the tail pass through it.
        assert success["Next"] == "RunScope"
        assert states["RunScope"]["Next"] == "CheckSkipReportCard"
        assert states["CheckSkipReportCard"]["Default"] == "ReportCard"


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
        assert states["SubstrateHealthCheckDegraded"]["Type"] == "Pass"
        # A degraded substrate check must not skip the ReportCard/Director
        # Lambda tail — it is independent of the dashboard box. (config#6054:
        # the tail entry is the skip gate, Default: ReportCard;
        # alpha-engine-config-I7620: RunScope is now its head, so a fail-open
        # substrate check still produces a scope block beside the card.)
        assert_degraded_continuation(
            states, "SubstrateHealthCheckDegraded", "RunScope",
        )
        assert states["RunScope"]["Next"] == "CheckSkipReportCard"
        assert states["CheckSkipReportCard"]["Default"] == "ReportCard"


class TestCommandShape:
    """alpha-engine-config-I7047 (2026-08-12): the three inline commands
    (transparency sweep --cadence weekly --alert, constituents_drift_check,
    phase_marker_sweep --run-date --alert) moved out of this SF definition
    into crucible-dashboard infrastructure/substrate_health_check.sh,
    invoked here through krepis.ssm_log_capture — mirrors the 17 other
    Saturday SF stages already on that wrapper instead of the inline
    `trap 'aws s3 cp ... EXIT'` pattern, which collapsed under ASL's
    States.Array escape semantics on every run using it (`trap: s3: invalid
    signal specification`, rc=127 — the 2026-08-08 scheduled run finished
    DEGRADED for exactly this reason despite every real work stage
    succeeding).

    Content-level assertions on the three checks themselves (module names,
    --cadence/--alert flags, ordering, run-date threading) now live in
    crucible-dashboard tests/test_substrate_health_check_weekly_wiring.py,
    the repo that actually owns that content. What THIS repo can still pin
    is the *wiring*: the SF dispatches through the krepis wrapper to the
    extracted script, on the dashboard box, with no runtime pip.
    """

    @pytest.fixture
    def commands(self, states) -> list[str]:
        return extract_commands(states["WeeklySubstrateHealthCheck"])

    def test_no_inline_trap_anti_pattern(self, commands):
        joined = " ".join(commands)
        assert "trap 'aws s3 cp" not in joined, (
            "I7047: the inline trap/log-ship anti-pattern must not return — "
            "krepis.ssm_log_capture is the sole log-capture path"
        )

    def test_invokes_krepis_log_capture_wrapper(self, commands):
        assert any("krepis.ssm_log_capture" in cmd for cmd in commands)

    def test_invokes_extracted_script_with_run_date(self, commands):
        assert any(
            "bash infrastructure/substrate_health_check.sh --run-date" in cmd
            for cmd in commands
        )

    def test_runs_on_dashboard_ec2(self, commands):
        # The dispatcher EC2 has the lib installed; confirm we cd there.
        joined = " ".join(commands)
        assert "alpha-engine-dashboard" in joined

    def test_pulls_latest_dashboard_and_data_main_before_running(self, commands):
        # Stale repo on the dispatcher would run an outdated lib pin or an
        # outdated substrate_health_check.sh. The script itself needs BOTH
        # alpha-engine-dashboard (transparency module + the script) and
        # alpha-engine-data (validators.*) checked out fresh — the SF's own
        # command array pulls both before invoking the script, mirroring
        # MorningEnrich's convention.
        # alpha-engine-config-I11570: both go through exec_code_pin.sh, which
        # pulls main on the execution's first use and checks out that pinned
        # SHA afterwards, so the check runs the execution's code.
        joined = " ".join(commands)
        pin = "bash /home/ec2-user/alpha-engine-data/infrastructure/exec_code_pin.sh {} "
        assert pin + "/home/ec2-user/alpha-engine-dashboard" in joined
        assert pin + "/home/ec2-user/alpha-engine-data" in joined

    def test_no_runtime_pip_install(self, commands):
        # config#2276: deps are synced at deploy time (crucible-dashboard
        # infrastructure/deploy-on-merge.sh pip-installs on requirements.txt
        # diff; nousergon-lib is tag-pinned there) — a live PyPI dependency
        # mid-pipeline is forbidden.
        joined = " ".join(commands)
        assert "pip install" not in joined

    def test_run_date_threaded_from_sf_run_date(self, states):
        # value is threaded from the SF-stamped $.run_date into the
        # krepis-wrapped script invocation via States.Format, same contract
        # as tests/test_sf_run_date_threading.py's spot stages.
        raw_expr = states["WeeklySubstrateHealthCheck"]["Parameters"]["Parameters"]["commands.$"]
        assert "$.run_date" in raw_expr
        assert "$$.Execution.Name" in raw_expr, (
            "krepis.ssm_log_capture's --correlation-id must be the SF "
            "execution name, per the 17-sibling-stage convention (§116 "
            "rule 6 chokepoint)."
        )


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
