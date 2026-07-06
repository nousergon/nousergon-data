"""Pins the Lambda-based NYSE trading-day gate on the WEEKDAY SF (config#1430).

Incident (2026-06-30): the prior holiday check ran an SSM command on the
EC2 trading box AFTER booting it. On a cold boot the on-box command exited 0
but SSM returned empty stdout (output-capture race), so the result Choice's
Default branch fired a false "Trading Day Check Failed (Proceeding)" SNS alert.

Robust fix: move the holiday check OFF the box into the predictor Lambda
(``action=check_trading_day``, pure calendar math) and gate on it via a Lambda
task BEFORE StartExecutorEC2 — mirroring the existing DeployDriftCheck
Lambda-gate pattern in the same SF. On a holiday the box is never booted.

These assertions guard the full new gate contract and that the six now-dead
on-box-check states are gone.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

# The on-box trading-day check states removed by config#1430.
_REMOVED_STATES = [
    "CheckTradingDay",
    "WaitForTradingDayCheck",
    "CheckTradingDayResult",
    "TradingDayCheckWait",
    "TradingDayCheckFailed",
    "StopExecutorOnHoliday",
]


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


class TestTradingDayGateTask:
    def test_gate_is_lambda_invoke(self, states):
        gate = states["TradingDayGate"]
        assert gate["Type"] == "Task"
        assert gate["Resource"] == "arn:aws:states:::lambda:invoke"

    def test_gate_invokes_check_trading_day_action(self, states):
        params = states["TradingDayGate"]["Parameters"]
        assert params["Payload"]["action"] == "check_trading_day"
        assert "predictor-inference" in params["FunctionName"]

    def test_gate_catches_to_failed_and_sets_resultpath(self, states):
        gate = states["TradingDayGate"]
        assert gate["ResultPath"] == "$.trading_day_gate"
        catch_targets = [c["Next"] for c in gate["Catch"]]
        assert "TradingDayGateFailed" in catch_targets
        assert gate["Next"] == "TradingDayGateChoice"


class TestGateRunsBeforeBox:
    def test_deploy_drift_gate_routes_to_trading_day_gate(self, states):
        # The gate runs BEFORE StartExecutorEC2 — the box never boots on holidays.
        assert states["DeployDriftGate"]["Default"] == "TradingDayGate"


class TestTradingDayGateChoice:
    def test_false_branch_skips_to_holiday(self, states):
        false_branch = [
            c["Next"]
            for c in states["TradingDayGateChoice"]["Choices"]
            if c.get("BooleanEquals") is False
        ]
        assert false_branch == ["NotifyHolidaySkip"]

    def test_default_proceeds_to_start_box(self, states):
        # config#1807: trading day confirmed -> dispatch the data-spot launch
        # first (fire-and-forget), then boot the box.
        assert states["TradingDayGateChoice"]["Default"] == "CheckSkipDataSpot"
        assert states["CheckSkipDataSpot"]["Default"] == "LaunchDailyDataSpot"
        assert states["LaunchDailyDataSpot"]["Next"] == "StartExecutorEC2"

    def test_choice_reads_is_trading_day_off_gate_payload(self, states):
        var = states["TradingDayGateChoice"]["Choices"][0]["Variable"]
        assert var == "$.trading_day_gate.Payload.is_trading_day"


class TestTradingDayGateFailed:
    def test_failed_proceeds_to_start_box(self, states):
        failed = states["TradingDayGateFailed"]
        assert failed["Resource"] == "arn:aws:states:::sns:publish"
        # config#1807: fail-open path also routes through the spot-launch gate.
        assert failed["Next"] == "CheckSkipDataSpot"


class TestDeadStatesRemoved:
    @pytest.mark.parametrize("dead", _REMOVED_STATES)
    def test_removed_state_absent(self, states, dead):
        assert dead not in states, f"{dead} should have been removed (config#1430)"


class TestHolidayNotifyTerminal:
    def test_notify_holiday_skip_is_terminal(self, states):
        nhs = states["NotifyHolidaySkip"]
        assert nhs.get("End") is True
        assert "Next" not in nhs
