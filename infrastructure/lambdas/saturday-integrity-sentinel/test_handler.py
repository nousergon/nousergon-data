"""Unit tests for saturday-integrity-sentinel (M4).

Stubs alpha_engine_lib.telegram.send_message and mocks the S3 client. Asserts
the GO/NO-GO evaluation (complete / incomplete / uncertain), loud-vs-silent
Telegram, fail-loud marker write, and best-effort Telegram.
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_lib_pkg = types.ModuleType("alpha_engine_lib")
_telegram_mod = types.ModuleType("alpha_engine_lib.telegram")
_telegram_mod.send_message = MagicMock(return_value=True)
_lib_pkg.telegram = _telegram_mod
sys.modules.setdefault("alpha_engine_lib", _lib_pkg)
sys.modules.setdefault("alpha_engine_lib.telegram", _telegram_mod)

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402


class FakeClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _s3(verdict_doc=None, missing=False, put=None):
    s3 = MagicMock()
    if missing:
        s3.get_object.side_effect = FakeClientError("404")
    else:
        body = MagicMock()
        body.read.return_value = json.dumps(verdict_doc).encode()
        s3.get_object.return_value = {"Body": body}
    if put is not None:
        s3.put_object.side_effect = put
    return s3


@pytest.fixture(autouse=True)
def reset_telegram():
    _telegram_mod.send_message.reset_mock()
    _telegram_mod.send_message.return_value = True
    yield


def _run(s3):
    with patch("index.boto3.client", return_value=s3):
        return index.handler({}, None)


def test_go_when_saturday_cycle_complete():
    doc = {"verdicts": [{"cadence": "saturday_sf", "complete": True,
                         "n_required": 10, "n_satisfied": 10}]}
    s3 = _s3(doc)
    result = _run(s3)
    assert result["go"] is True
    assert result["uncertain"] is False
    s3.put_object.assert_called_once()
    # GO pings silent (heartbeat), not a loud alert.
    assert _telegram_mod.send_message.call_args.kwargs["disable_notification"] is True
    text = _telegram_mod.send_message.call_args.args[0]
    assert "Saturday Integrity — GO" in text


def test_nogo_when_incomplete_pages_loud():
    doc = {"verdicts": [{"cadence": "saturday_sf", "complete": False,
                         "n_required": 10, "n_satisfied": 8,
                         "missing": ["research_signals"], "stale": []}]}
    s3 = _s3(doc)
    result = _run(s3)
    assert result["go"] is False
    assert result["uncertain"] is False
    kwargs = _telegram_mod.send_message.call_args.kwargs
    assert kwargs["disable_notification"] is False  # LOUD
    text = _telegram_mod.send_message.call_args.args[0]
    assert "NO-GO" in text
    assert "research_signals" in text
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert written["go"] is False
    assert written["missing"] == ["research_signals"]


def test_uncertain_when_verdict_missing():
    s3 = _s3(missing=True)
    result = _run(s3)
    assert result["go"] is False
    assert result["uncertain"] is True
    assert "unavailable" in result["reason"]
    assert _telegram_mod.send_message.call_args.kwargs["disable_notification"] is False


def test_uncertain_when_verdict_stale():
    doc = {"run_at": "2020-01-01T00:00:00+00:00",
           "verdicts": [{"cadence": "saturday_sf", "complete": True,
                         "n_required": 10, "n_satisfied": 10}]}
    s3 = _s3(doc)
    result = _run(s3)
    assert result["go"] is False
    assert result["uncertain"] is True
    assert "stale" in result["reason"]


def test_uncertain_when_no_saturday_row():
    doc = {"verdicts": [{"cadence": "weekday_sf", "complete": True}]}
    s3 = _s3(doc)
    result = _run(s3)
    assert result["go"] is False
    assert result["uncertain"] is True


def test_marker_write_failure_raises_fail_loud():
    doc = {"verdicts": [{"cadence": "saturday_sf", "complete": True,
                         "n_required": 10, "n_satisfied": 10}]}
    s3 = _s3(doc, put=RuntimeError("S3 down"))
    with pytest.raises(RuntimeError, match="S3 down"):
        _run(s3)


def test_telegram_failure_is_non_fatal():
    _telegram_mod.send_message.side_effect = RuntimeError("bot down")
    doc = {"verdicts": [{"cadence": "saturday_sf", "complete": False,
                         "n_required": 10, "n_satisfied": 9, "missing": ["x"]}]}
    s3 = _s3(doc)
    result = _run(s3)
    assert result["telegram_sent"] is False
    s3.put_object.assert_called_once()  # primary deliverable survived
