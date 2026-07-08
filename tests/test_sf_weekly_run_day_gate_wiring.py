"""Pin the weekly run-day gate wiring (config#1824, Brian-ratified 2026-07-06).

Policy: the weekly pipeline runs ONE calendar day after the LAST trading
session of its Mon-Fri week — Saturday normally, Friday when Friday is an
NYSE holiday (real precedent 2026-07-03), Thursday on a Thu+Fri double
holiday. Mechanism: the EventBridge cron fires THU-SAT and the SF's
WeeklyRunDayGate (predictor Lambda ``action=check_weekly_run_day``, pure
calendar math per the config#1430 posture) self-selects the single correct
firing, Succeed-skipping the rest BEFORE any spend.

Pins:
  1. CFN cron is THU-SAT (not the old fixed SAT).
  2. InitializeInput defaults ``skip_weekly_run_day_gate`` false and routes
     into the gate choice first.
  3. The gate applies ONLY to ``pipeline_role == "weekly"`` (scheduled
     cadence) — manual/recovery/watch-rerun/shell runs bypass automatically,
     and the skip flag is the explicit operator bypass.
  4. Non-run-day exits via a Succeed state (green skip — SF-watch and
     success metrics must not read it as a failure).
  5. Gate infra-failure is fail-open: alert then proceed (mirror of the
     weekday TradingDayGateFailed), with all message fields structurally
     guaranteed (config#1819 notifier-totality lesson).
"""
from __future__ import annotations

import json
import pathlib

import pytest

_INFRA = pathlib.Path(__file__).parent.parent / "infrastructure"
_SF_JSON = _INFRA / "step_function.json"
_CFN = _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_JSON.read_text())["States"]


class TestCronSchedule:
    def test_saturday_trigger_fires_thu_through_sat(self):
        cfn = _CFN.read_text()
        block = cfn.split("SaturdayTrigger:", 1)[1].split("WeekdayTrigger:", 1)[0]
        assert "cron(0 9 ? * THU-SAT *)" in block, (
            "weekly cron must fire THU-SAT so the run-day gate can "
            "self-select holiday-shortened weeks (config#1824)"
        )
        assert "cron(0 9 ? * SAT *)" not in block


class TestGateRouting:
    def test_initialize_input_routes_to_gate_choice(self, states):
        assert states["InitializeInput"]["Next"] == "CheckWeeklyRunDayGate"

    def test_skip_flag_defaulted_false(self, states):
        merged = states["InitializeInput"]["Parameters"]["merged.$"]
        assert '\\"skip_weekly_run_day_gate\\":false' in json.dumps(merged) or (
            '"skip_weekly_run_day_gate":false' in merged
        ), "InitializeInput must default skip_weekly_run_day_gate=false"

    def test_gate_scoped_to_scheduled_weekly_role_only(self, states):
        choice = states["CheckWeeklyRunDayGate"]
        assert choice["Type"] == "Choice"
        assert choice["Default"] == "CheckRunMode", (
            "non-weekly roles (manual/recovery/watch/shell) must bypass the gate"
        )
        (rule,) = choice["Choices"]
        conds = {
            (c["Variable"], next(k for k in c if k not in ("Variable",))): c
            for c in rule["And"]
        }
        assert ("$.pipeline_role", "IsPresent") in conds
        assert conds[("$.pipeline_role", "StringEquals")]["StringEquals"] == "weekly"
        assert conds[("$.skip_weekly_run_day_gate", "BooleanEquals")]["BooleanEquals"] is False
        assert rule["Next"] == "WeeklyRunDayGate"

    def test_gate_invokes_predictor_calendar_action(self, states):
        gate = states["WeeklyRunDayGate"]
        assert gate["Parameters"]["FunctionName"] == "alpha-engine-predictor-inference:live"
        assert gate["Parameters"]["Payload"] == {"action": "check_weekly_run_day"}
        assert gate["Next"] == "WeeklyRunDayGateChoice"
        (catch,) = gate["Catch"]
        assert catch["Next"] == "WeeklyRunDayGateFailed"
        assert catch["ResultPath"] == "$.weekly_run_day_gate_error"


class TestGateOutcomes:
    def test_non_run_day_is_green_succeed_skip(self, states):
        choice = states["WeeklyRunDayGateChoice"]
        (rule,) = choice["Choices"]
        assert rule["Variable"] == "$.weekly_run_day_gate.Payload.is_weekly_run_day"
        assert rule["BooleanEquals"] is False
        assert rule["Next"] == "WeeklyRunDaySkip"
        assert states["WeeklyRunDaySkip"]["Type"] == "Succeed"

    def test_run_day_proceeds_to_normal_head(self, states):
        assert states["WeeklyRunDayGateChoice"]["Default"] == "CheckRunMode"

    def test_gate_failure_is_fail_open_with_alert(self, states):
        failed = states["WeeklyRunDayGateFailed"]
        assert failed["Resource"] == "arn:aws:states:::sns:publish"
        assert failed["Next"] == "CheckRunMode", "fail-open: proceed as run day"
        # Notifier totality (config#1819): Subject constant + short; every
        # JSONPath in the message structurally guaranteed on this path.
        subject = failed["Parameters"]["Subject"]
        assert isinstance(subject, str) and len(subject) <= 100 and "\n" not in subject
        assert "$.weekly_run_day_gate_error" in failed["Parameters"]["Message.$"]
        assert failed["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
