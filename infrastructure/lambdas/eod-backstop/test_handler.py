"""Unit tests for the alpha-engine-eod-backstop Lambda (config#1229).

The backstop starts the EOD SF IFF the trading box is still running AND no
EOD ran today; it is a no-op otherwise and fail-loud on AWS errors.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import index


def _ec2(state: str | None):
    """An EC2 client mock whose trading instance reports ``state`` (None →
    instance absent)."""
    cli = MagicMock()
    if state is None:
        cli.describe_instances.return_value = {"Reservations": []}
    else:
        cli.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": state}}]}]
        }
    return cli


def _sf(exec_start: datetime | None):
    """A Step Functions client mock. ``exec_start`` (a tz-aware datetime) seeds
    one EOD execution under the RUNNING status; None → no executions."""
    cli = MagicMock()

    def _list(**kwargs):
        if kwargs.get("statusFilter") == "RUNNING" and exec_start is not None:
            return {"executions": [{"name": "eod-x", "startDate": exec_start}]}
        return {"executions": []}

    cli.list_executions.side_effect = _list
    cli.start_execution.return_value = {
        "executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-postclose-trading-pipeline:eod-backstop-x"
    }
    return cli


# ── Detection helpers ─────────────────────────────────────────────────────────


class TestBoxRunning:
    def test_running_true(self):
        assert index._trading_box_running(_ec2("running")) is True

    def test_stopped_false(self):
        assert index._trading_box_running(_ec2("stopped")) is False

    def test_absent_false(self):
        assert index._trading_box_running(_ec2(None)) is False


class TestEodRanToday:
    NOW = datetime(2026, 6, 25, 22, 30, tzinfo=timezone.utc)

    def test_execution_started_today_is_true(self):
        started = datetime(2026, 6, 25, 20, 15, tzinfo=timezone.utc)
        assert index._eod_ran_today(self.NOW, _sf(started)) is True

    def test_yesterdays_execution_is_not_today(self):
        started = datetime(2026, 6, 24, 20, 15, tzinfo=timezone.utc)
        assert index._eod_ran_today(self.NOW, _sf(started)) is False

    def test_no_executions_is_false(self):
        assert index._eod_ran_today(self.NOW, _sf(None)) is False


# ── Handler decision matrix ───────────────────────────────────────────────────


class TestHandler:
    TRADING_NOW = datetime(2026, 6, 25, 22, 30, tzinfo=timezone.utc)  # Thursday

    def _run(self, *, trading_day=True, box="running", eod_started=None):
        with patch("index.datetime") as dt, \
             patch("index.is_trading_day", return_value=trading_day), \
             patch("index.last_closed_trading_day", return_value=self.TRADING_NOW.date()), \
             patch("index._trading_box_running", return_value=(box == "running")), \
             patch("index._eod_ran_today", return_value=(eod_started is not None)), \
             patch("index._start_eod", return_value="arn:exec:backstop") as start:
            dt.now.return_value = self.TRADING_NOW
            result = index.handler({}, None)
        return result, start

    def test_starts_eod_when_box_up_and_no_eod_today(self):
        result, start = self._run(box="running", eod_started=None)
        assert result["action"] == "started_eod"
        assert result["execution_arn"] == "arn:exec:backstop"
        start.assert_called_once()

    def test_noop_when_box_stopped(self):
        result, start = self._run(box="stopped")
        assert result["action"] == "noop" and result["reason"] == "trading_box_not_running"
        start.assert_not_called()

    def test_noop_when_eod_already_ran(self):
        result, start = self._run(box="running", eod_started=self.TRADING_NOW)
        assert result["action"] == "noop" and result["reason"] == "eod_already_ran_today"
        start.assert_not_called()

    def test_noop_when_not_a_trading_day(self):
        result, start = self._run(trading_day=False)
        assert result["action"] == "noop" and result["reason"] == "not_a_trading_day"
        start.assert_not_called()


class TestStartEodInput:
    def test_start_execution_mirrors_daemon_input(self):
        sf = _sf(None)
        index._start_eod("2026-06-25", sf)
        kwargs = sf.start_execution.call_args.kwargs
        assert kwargs["stateMachineArn"].endswith("ne-postclose-trading-pipeline")
        assert kwargs["name"].startswith("eod-backstop-2026-06-25-")
        import json
        payload = json.loads(kwargs["input"])
        assert payload["triggered_by"] == "backstop"
        assert payload["pipeline_role"] == "eod"
        assert payload["run_date"] == "2026-06-25"
        assert payload["trading_instance_id"] == [index.TRADING_INSTANCE_ID]


# ── Fail-loud ─────────────────────────────────────────────────────────────────


class TestFailLoud:
    def test_describe_instances_error_raises(self):
        cli = MagicMock()
        cli.describe_instances.side_effect = RuntimeError("ec2 down")
        with pytest.raises(RuntimeError):
            index._trading_box_running(cli)

    def test_list_executions_error_raises(self):
        cli = MagicMock()
        cli.list_executions.side_effect = RuntimeError("states down")
        with pytest.raises(RuntimeError):
            index._eod_ran_today(datetime(2026, 6, 25, 22, 30, tzinfo=timezone.utc), cli)
