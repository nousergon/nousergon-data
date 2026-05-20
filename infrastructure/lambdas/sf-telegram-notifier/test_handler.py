"""Unit tests for sf-telegram-notifier index.handler.

Mocks alpha_engine_lib.telegram.send_message so tests do not hit the live
Telegram API. Each test asserts the exact (text, disable_notification) tuple
the handler hands to the primitive, plus the return value shape.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Stub `alpha_engine_lib.telegram` before importing the handler so test
# environments without the lib installed (CI runners pre-pip-install) still
# pass — the handler only depends on this one import path from the lib.
_lib_pkg = types.ModuleType("alpha_engine_lib")
_telegram_mod = types.ModuleType("alpha_engine_lib.telegram")
_telegram_mod.send_message = MagicMock(return_value=True)
_lib_pkg.telegram = _telegram_mod
sys.modules.setdefault("alpha_engine_lib", _lib_pkg)
sys.modules.setdefault("alpha_engine_lib.telegram", _telegram_mod)

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402


SATURDAY_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-saturday-pipeline"
WEEKDAY_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-weekday-pipeline"
EOD_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-eod-pipeline"


def _event(status: str, sm_arn: str = SATURDAY_ARN, **detail_overrides) -> dict:
    detail = {
        "status": status,
        "stateMachineArn": sm_arn,
        "executionArn": f"arn:aws:states:us-east-1:711398986525:execution:{sm_arn.rsplit(':', 1)[-1]}:exec-001",
        "name": "exec-001",
        "startDate": 1_700_000_000_000,
        "stopDate": 1_700_000_060_000,  # 60s after start
    }
    detail.update(detail_overrides)
    return {"detail": detail}


@pytest.fixture(autouse=True)
def reset_send_message():
    _telegram_mod.send_message.reset_mock()
    _telegram_mod.send_message.return_value = True
    yield


def test_running_sends_silent_message_without_duration_or_cause():
    event = _event("RUNNING", stopDate=None)
    result = index.handler(event, None)

    _telegram_mod.send_message.assert_called_once()
    text, kwargs = _telegram_mod.send_message.call_args.args[0], _telegram_mod.send_message.call_args.kwargs
    assert "Saturday SF — RUNNING" in text
    assert "Execution: exec-001" in text
    assert "Duration:" not in text
    assert "Cause:" not in text
    assert kwargs["disable_notification"] is True
    assert result["status"] == "RUNNING"
    assert result["silent"] is True
    assert result["telegram_sent"] is True


def test_succeeded_sends_loud_message_with_duration():
    event = _event("SUCCEEDED", sm_arn=WEEKDAY_ARN)
    result = index.handler(event, None)

    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "Weekday SF — SUCCEEDED" in text
    assert "Duration: 1m" in text
    assert kwargs["disable_notification"] is False
    assert result["silent"] is False


def test_succeeded_long_duration_formats_hours_and_minutes():
    # 4h 12m → start 0, stop = (4*3600 + 12*60) * 1000
    event = _event("SUCCEEDED", startDate=0, stopDate=(4 * 3600 + 12 * 60) * 1000)
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    assert "Duration: 4h 12m" in text


def test_failed_fetches_and_includes_cause():
    event = _event("FAILED", sm_arn=EOD_ARN)
    fake_sf_client = MagicMock()
    fake_sf_client.describe_execution.return_value = {
        "error": "States.TaskFailed",
        "cause": "EODReconcile state failed: NoCredentialsError",
    }
    with patch("index.boto3.client", return_value=fake_sf_client) as boto_client:
        result = index.handler(event, None)

    boto_client.assert_called_once_with("stepfunctions", region_name=index.REGION)
    fake_sf_client.describe_execution.assert_called_once_with(
        executionArn=event["detail"]["executionArn"]
    )
    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "EOD SF — FAILED" in text
    assert "Cause: States.TaskFailed: EODReconcile state failed: NoCredentialsError" in text
    assert kwargs["disable_notification"] is False
    assert result["status"] == "FAILED"


def test_failed_with_describe_execution_error_still_sends():
    """DescribeExecution failures must not block the Telegram send."""
    event = _event("FAILED")
    fake_sf_client = MagicMock()
    fake_sf_client.describe_execution.side_effect = RuntimeError("API throttled")
    with patch("index.boto3.client", return_value=fake_sf_client):
        result = index.handler(event, None)

    text = _telegram_mod.send_message.call_args.args[0]
    assert "Saturday SF — FAILED" in text
    assert "Cause:" not in text  # enrichment silently dropped
    assert result["telegram_sent"] is True


def test_failed_truncates_long_cause():
    event = _event("FAILED")
    fake_sf_client = MagicMock()
    fake_sf_client.describe_execution.return_value = {
        "error": "E",
        "cause": "x" * 500,
    }
    with patch("index.boto3.client", return_value=fake_sf_client):
        index.handler(event, None)

    text = _telegram_mod.send_message.call_args.args[0]
    cause_line = [line for line in text.splitlines() if line.startswith("Cause:")][0]
    # "Cause: " prefix + cap (_CAUSE_MAX_CHARS) + ellipsis (1) = bounded
    assert len(cause_line) <= len("Cause: ") + index._CAUSE_MAX_CHARS


def test_timed_out_sends_loud_message():
    event = _event("TIMED_OUT")
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "Saturday SF — TIMED_OUT" in text
    assert kwargs["disable_notification"] is False


def test_aborted_sends_loud_message():
    event = _event("ABORTED")
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert "Saturday SF — ABORTED" in text
    assert kwargs["disable_notification"] is False


def test_unknown_sf_arn_falls_back_to_arn_tail():
    unknown_arn = "arn:aws:states:us-east-1:711398986525:stateMachine:alpha-engine-future-pipeline"
    event = _event("SUCCEEDED", sm_arn=unknown_arn)
    index.handler(event, None)
    text = _telegram_mod.send_message.call_args.args[0]
    assert "alpha-engine-future-pipeline — SUCCEEDED" in text


def test_send_message_failure_returned_in_result():
    _telegram_mod.send_message.return_value = False
    result = index.handler(_event("SUCCEEDED"), None)
    assert result["telegram_sent"] is False
    assert result["status"] == "SUCCEEDED"


def test_label_lookup_table_covers_all_three_sfs():
    assert index._SF_LABELS["alpha-engine-saturday-pipeline"] == "Saturday SF"
    assert index._SF_LABELS["alpha-engine-weekday-pipeline"] == "Weekday SF"
    assert index._SF_LABELS["alpha-engine-eod-pipeline"] == "EOD SF"


def test_format_duration_handles_missing_timestamps():
    assert index._format_duration(None, None) == ""
    assert index._format_duration(1000, None) == ""
    assert index._format_duration(None, 2000) == ""
    assert index._format_duration(0, 1000) == "0m"  # sub-minute rounds down
