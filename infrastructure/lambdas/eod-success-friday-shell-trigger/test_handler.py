"""Unit tests for eod-success-friday-shell-trigger index.handler.

Stubs ``alpha_engine_lib.trading_calendar.last_closed_trading_day`` and
``boto3.client`` so tests do not hit AWS or the lib. Each test asserts the
handler's invoke-or-skip decision plus the input shape passed to
``states:StartExecution``.
"""

from __future__ import annotations

import sys
import types
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Stub `alpha_engine_lib.trading_calendar` before importing the handler so
# test environments without the lib installed (CI runners pre-pip-install)
# still pass. The handler depends only on this one import path from the lib.
_lib_pkg = types.ModuleType("alpha_engine_lib")
_tc_mod = types.ModuleType("alpha_engine_lib.trading_calendar")
_tc_mod.last_closed_trading_day = MagicMock()
_lib_pkg.trading_calendar = _tc_mod
sys.modules.setdefault("alpha_engine_lib", _lib_pkg)
sys.modules.setdefault("alpha_engine_lib.trading_calendar", _tc_mod)

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402


EOD_ARN = (
    "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-eod-pipeline"
)
SATURDAY_ARN = (
    "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-saturday-pipeline"
)


def _epoch_ms(year: int, month: int, day: int, hour: int = 20, minute: int = 25) -> int:
    """UTC epoch ms for a wall-clock UTC datetime."""
    return int(
        datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000
    )


def _event(
    status: str = "SUCCEEDED",
    sm_arn: str = EOD_ARN,
    stop_date_ms: int | None = None,
    **detail_overrides,
) -> dict:
    detail: dict = {
        "status": status,
        "stateMachineArn": sm_arn,
        "executionArn": "arn:aws:states:us-east-1:711398986525:execution:alpha-engine-eod-pipeline:exec-001",
        "name": "exec-001",
        "startDate": _epoch_ms(2026, 5, 22, 20, 15),
        "stopDate": stop_date_ms
        if stop_date_ms is not None
        else _epoch_ms(2026, 5, 22, 20, 25),
    }
    detail.update(detail_overrides)
    return {"detail": detail}


@pytest.fixture(autouse=True)
def reset_mocks():
    _tc_mod.last_closed_trading_day.reset_mock()
    _tc_mod.last_closed_trading_day.side_effect = None
    _tc_mod.last_closed_trading_day.return_value = date(2026, 5, 22)  # Friday
    yield


def _patch_sfn(start_execution_return: dict | None = None, side_effect=None):
    fake_client = MagicMock()
    if side_effect is not None:
        fake_client.start_execution.side_effect = side_effect
    else:
        fake_client.start_execution.return_value = (
            start_execution_return
            or {
                "executionArn": "arn:aws:states:us-east-1:711398986525:execution:alpha-engine-saturday-pipeline:friday-shell-test",
                "startDate": datetime(2026, 5, 22, 20, 25, tzinfo=timezone.utc),
            }
        )
    return patch("index.boto3.client", return_value=fake_client), fake_client


def test_friday_eod_success_fires_saturday_sf_with_shell_run_input():
    """Friday EOD SUCCEEDED → StartExecution on saturday SF with shell_run:true."""
    _tc_mod.last_closed_trading_day.return_value = date(2026, 5, 22)  # Friday
    sfn_patch, fake_client = _patch_sfn()
    with sfn_patch:
        result = index.handler(_event(), None)

    fake_client.start_execution.assert_called_once()
    call_kwargs = fake_client.start_execution.call_args.kwargs
    assert call_kwargs["stateMachineArn"] == SATURDAY_ARN
    assert call_kwargs["name"].startswith("friday-shell-2026-05-22-")

    import json as _json

    input_dict = _json.loads(call_kwargs["input"])
    assert input_dict["shell_run"] is True
    assert input_dict["ec2_instance_id"] == [index.TRADING_EC2_INSTANCE_ID]
    assert input_dict["sns_topic_arn"] == index.SNS_TOPIC_ARN

    assert result["fired"] is True
    assert result["trading_day"] == "2026-05-22"
    assert result["saturday_execution_arn"].endswith(":friday-shell-test")


@pytest.mark.parametrize(
    "trading_day,weekday_label",
    [
        (date(2026, 5, 18), "Mon"),
        (date(2026, 5, 19), "Tue"),
        (date(2026, 5, 20), "Wed"),
        (date(2026, 5, 21), "Thu"),
    ],
)
def test_non_friday_eod_success_does_not_fire(trading_day, weekday_label):
    _tc_mod.last_closed_trading_day.return_value = trading_day
    sfn_patch, fake_client = _patch_sfn()
    with sfn_patch:
        result = index.handler(_event(), None)

    fake_client.start_execution.assert_not_called()
    assert result["fired"] is False
    assert result["reason"] == "not_friday"
    assert result["trading_day"] == trading_day.isoformat()


def test_saturday_morning_eod_rerun_for_friday_trading_day_fires():
    """A late re-run that succeeds Sat AM still binds to Fri trading_day."""
    # stopDate at Sat 10:00 PT = Sat 17:00 UTC. The lib's
    # last_closed_trading_day correctly returns Fri (the last closed
    # session) — and the handler trusts the lib's answer.
    _tc_mod.last_closed_trading_day.return_value = date(2026, 5, 22)  # Friday
    sfn_patch, fake_client = _patch_sfn()
    with sfn_patch:
        result = index.handler(
            _event(stop_date_ms=_epoch_ms(2026, 5, 23, 17, 0)), None
        )

    fake_client.start_execution.assert_called_once()
    assert result["fired"] is True
    assert result["trading_day"] == "2026-05-22"


def test_missing_stop_date_raises_fail_loud():
    """SUCCEEDED without detail.stopDate is an upstream contract violation."""
    sfn_patch, fake_client = _patch_sfn()
    with sfn_patch, pytest.raises(RuntimeError, match="missing detail.stopDate"):
        index.handler(_event(stop_date_ms=None, stopDate=None), None)
    fake_client.start_execution.assert_not_called()


def test_wrong_state_machine_arn_logs_and_returns_without_firing():
    """Defensive: rule filter mismatch should NOT raise but also not fire."""
    sfn_patch, fake_client = _patch_sfn()
    weekday_arn = (
        "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-weekday-pipeline"
    )
    with sfn_patch:
        result = index.handler(_event(sm_arn=weekday_arn), None)
    fake_client.start_execution.assert_not_called()
    assert result["fired"] is False
    assert result["reason"] == "wrong_event"


def test_non_succeeded_status_does_not_fire():
    """FAILED / TIMED_OUT / RUNNING never trigger the shell run."""
    sfn_patch, fake_client = _patch_sfn()
    for bad_status in ("RUNNING", "FAILED", "TIMED_OUT", "ABORTED"):
        with sfn_patch:
            result = index.handler(_event(status=bad_status), None)
        assert result["fired"] is False
        assert result["reason"] == "wrong_event"
    fake_client.start_execution.assert_not_called()


def test_trading_calendar_lookup_failure_raises():
    """Lib-side error must surface, not be swallowed."""
    _tc_mod.last_closed_trading_day.side_effect = RuntimeError("calendar unavailable")
    sfn_patch, fake_client = _patch_sfn()
    with sfn_patch, pytest.raises(RuntimeError, match="calendar unavailable"):
        index.handler(_event(), None)
    fake_client.start_execution.assert_not_called()


def test_start_execution_failure_raises():
    """boto3 StartExecution failure is a fail-loud — never silent fall-through."""
    _tc_mod.last_closed_trading_day.return_value = date(2026, 5, 22)  # Friday
    sfn_patch, fake_client = _patch_sfn(
        side_effect=RuntimeError("StateMachineDoesNotExist")
    )
    with sfn_patch, pytest.raises(RuntimeError, match="StateMachineDoesNotExist"):
        index.handler(_event(), None)


def test_execution_name_truncates_to_80_chars():
    """SF execution name max is 80 chars; build truncates safely."""
    _tc_mod.last_closed_trading_day.return_value = date(2026, 5, 22)
    sfn_patch, fake_client = _patch_sfn()
    long_name = "x" * 200
    with sfn_patch:
        index.handler(_event(name=long_name), None)
    assert len(fake_client.start_execution.call_args.kwargs["name"]) <= 80


def test_derive_trading_day_passes_tz_aware_utc_to_lib():
    """The lib receives a tz-aware UTC datetime, not naive."""
    stop_ms = _epoch_ms(2026, 5, 22, 20, 25)
    index._derive_trading_day_utc_ms(stop_ms)
    _tc_mod.last_closed_trading_day.assert_called_once()
    arg = _tc_mod.last_closed_trading_day.call_args.args[0]
    assert isinstance(arg, datetime)
    assert arg.tzinfo is not None
    assert arg.tzinfo.utcoffset(arg).total_seconds() == 0  # UTC
