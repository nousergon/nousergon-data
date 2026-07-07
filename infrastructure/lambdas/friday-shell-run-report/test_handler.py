"""Unit tests for the friday-shell-run-report handler.

The lib (``nousergon_lib.trading_calendar``) and boto3 are stubbed so tests
do not hit AWS or require the lib install. The handler resolves trading_day from
the execution NAME in the happy path, so the lib stub is only the fallback.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Stub nousergon_lib.trading_calendar before importing the handler.
_ael = types.ModuleType("nousergon_lib")
_tc = types.ModuleType("nousergon_lib.trading_calendar")
_tc.last_closed_trading_day = lambda dt: dt.date()  # fallback only
_ael.trading_calendar = _tc
sys.modules.setdefault("nousergon_lib", _ael)
sys.modules.setdefault("nousergon_lib.trading_calendar", _tc)

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402

SAT_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
_EXEC_ARN = "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:friday-shell-2026-06-12-eodname"


def _ts(sec: int) -> datetime:
    return datetime(2026, 6, 12, 14, 30, sec, tzinfo=timezone.utc)


def _entered(name, sec):
    return {"type": "TaskStateEntered", "timestamp": _ts(sec),
            "stateEnteredEventDetails": {"name": name}}


def _exited(name, sec):
    return {"type": "TaskStateExited", "timestamp": _ts(sec),
            "stateExitedEventDetails": {"name": name}}


def _event(status="SUCCEEDED", name="friday-shell-2026-06-12-eodname",
           sm_arn=SAT_ARN, shell_run=True):
    inp = json.dumps({"shell_run": True, "pipeline_role": "shell-run"}) if shell_run else "{}"
    return {
        "detail": {
            "status": status, "stateMachineArn": sm_arn, "executionArn": _EXEC_ARN,
            "name": name, "input": inp, "stopDate": 1781015400000,
        }
    }


def _fake_clients(history_events):
    """Return a boto3.client side_effect dispatching by service name."""
    sfn = MagicMock()
    sfn.get_execution_history.return_value = {"events": history_events}  # no nextToken
    s3 = MagicMock()
    sns = MagicMock()
    clients = {"stepfunctions": sfn, "s3": s3, "sns": sns}
    return lambda svc, **kw: clients[svc], s3, sns


def test_successful_shell_run_writes_go_saturday_report():
    history = [
        _entered("MorningEnrich", 0), _exited("MorningEnrich", 5),
        _entered("DataPhase1", 5), _exited("DataPhase1", 20),
    ]
    side, s3, sns = _fake_clients(history)
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event("SUCCEEDED"), None)

    assert out["reported"] is True
    assert out["readiness"] == "GO_SATURDAY"
    # report.json written to the trading_day-keyed prefix
    put = s3.put_object.call_args.kwargs
    assert put["Key"] == "friday-shell-run/2026-06-12/report.json"
    body = json.loads(put["Body"])
    assert body["summary"] == {"n_states": 2, "passed": 2, "failed": 0,
                               "readiness": "GO_SATURDAY"}
    assert {p["name"] for p in body["per_state"]} == {"MorningEnrich", "DataPhase1"}
    assert all(p["status"] == "PASS" for p in body["per_state"])
    sns.publish.assert_called_once()


def test_failed_shell_run_flags_hold_and_names_failing_state():
    # DataPhase1 entered but never exited → the failure point.
    history = [
        _entered("MorningEnrich", 0), _exited("MorningEnrich", 5),
        _entered("DataPhase1", 5),
        {"type": "ExecutionFailed", "timestamp": _ts(30),
         "executionFailedEventDetails": {"error": "States.TaskFailed", "cause": "boom"}},
    ]
    side, s3, sns = _fake_clients(history)
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event("FAILED"), None)

    assert out["readiness"] == "HOLD_INVESTIGATE"
    body = json.loads(s3.put_object.call_args.kwargs["Body"])
    failing = [p["name"] for p in body["per_state"] if p["status"] == "FAIL"]
    assert failing == ["DataPhase1"]
    assert body["failure"]["error"] == "States.TaskFailed"


def test_non_shell_run_is_a_noop():
    side, s3, sns = _fake_clients([])
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event("SUCCEEDED", name="weekly-run", shell_run=False), None)
    assert out == {"reported": False, "reason": "not_shell_run"}
    s3.put_object.assert_not_called()


def test_wrong_state_machine_ignored():
    side, s3, _ = _fake_clients([])
    bad = _event("SUCCEEDED", sm_arn="arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline")
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(bad, None)
    assert out["reported"] is False and out["reason"] == "wrong_event"
    s3.put_object.assert_not_called()


def test_non_terminal_status_ignored():
    side, s3, _ = _fake_clients([])
    with patch("index.boto3.client", side_effect=side):
        out = index.handler(_event("RUNNING"), None)
    assert out["reported"] is False
    s3.put_object.assert_not_called()
