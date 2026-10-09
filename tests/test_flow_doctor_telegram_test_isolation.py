"""Tests can't reach Telegram through ``notify_via_flow_doctor``'s raw fallback.

Regression for the groom-cycle leak: ``tests/test_groom_cycle_notifications.py``
ran the dispatcher's real ``_notify_cycle_complete`` with
``infrastructure/lambdas`` importable. Flow-doctor was off, so
``notify_via_flow_doctor`` fell back to a raw ``send_message`` — which skips
the kill switch and dedup — and an agent session's ``TELEGRAM_BOT_TOKEN`` /
``TELEGRAM_CHAT_ID`` made it a real send. The operator received fake
"Groom cycle 0 12 * * * COMPLETE — degraded" pings replaying a fixture.

These tests import the REAL module, set FAKE Telegram credentials, and mock
the HTTP transport underneath the real ``send_message`` (``requests.post`` in
``krepis.telegram``), so a regression shows up as a recorded POST rather than
as a message on somebody's phone.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

LAMBDAS_DIR = Path(__file__).resolve().parents[1] / "infrastructure" / "lambdas"
if str(LAMBDAS_DIR) not in sys.path:
    sys.path.insert(0, str(LAMBDAS_DIR))

krepis_telegram = pytest.importorskip("krepis.telegram")
import flow_doctor_telegram  # noqa: E402

_CALL = dict(
    silent=False,
    severity="warning",
    dedup_key="groom-cycle-complete:0 12 * * *",
    flow_name="backlog-groom-cycle-test-isolation",
    topics=(),
    db_basename="groom_cycle_test_isolation",
)


@pytest.fixture
def transport(monkeypatch):
    """The real ``send_message`` with its network call mocked, and fake creds."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "000000:fake-token-for-tests")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-1000000000000")
    monkeypatch.setenv("ALPHA_ENGINE_SECRETS_SOURCE", "env")
    post = mock.Mock(return_value=SimpleNamespace(status_code=200, text="{}"))
    monkeypatch.setattr(
        krepis_telegram, "requests",
        SimpleNamespace(post=post, RequestException=Exception))
    monkeypatch.setattr(
        krepis_telegram, "fleet_events",
        SimpleNamespace(emission_suppressed=lambda: True,
                        emit_alert_event=lambda **k: None))
    # The module under test must hold the REAL transport — a stub here would
    # make every assertion below vacuous.
    assert flow_doctor_telegram._is_real_transport(flow_doctor_telegram.send_message)
    flow_doctor_telegram.reset_flow_doctor_cache()
    yield post
    flow_doctor_telegram.reset_flow_doctor_cache()


def test_flow_doctor_disabled_never_falls_back_to_a_raw_send(transport, monkeypatch):
    """conftest's FLOW_DOCTOR_DISABLED is a kill switch, not a cue to go raw."""
    monkeypatch.setenv("FLOW_DOCTOR_DISABLED", "1")
    monkeypatch.delenv("FLOW_DOCTOR_ENABLED", raising=False)

    sent = flow_doctor_telegram.notify_via_flow_doctor(
        "*Groom cycle 0 12 * * * COMPLETE — degraded*", **_CALL)

    assert sent is False
    transport.assert_not_called()


def test_under_pytest_a_flow_doctor_init_failure_never_falls_back(transport, monkeypatch):
    """Even with the kill switch unset, pytest is never a production context."""
    monkeypatch.delenv("FLOW_DOCTOR_DISABLED", raising=False)
    monkeypatch.setenv("FLOW_DOCTOR_ENABLED", "0")
    assert flow_doctor_telegram.os.environ.get("PYTEST_CURRENT_TEST")

    sent = flow_doctor_telegram.notify_via_flow_doctor(
        "*Groom cycle 0 12 * * * COMPLETE — degraded*", **_CALL)

    assert sent is False
    transport.assert_not_called()


def test_production_fallback_still_sends_when_flow_doctor_fails_to_start(
        transport, monkeypatch):
    """The guard must not take the real fallback away from production.

    Outside pytest and with no kill switch, flow-doctor merely failing to
    start still delivers raw — the reason the fallback exists. Proved against
    the same mocked POST, so this test sends nothing either.
    """
    monkeypatch.delenv("FLOW_DOCTOR_DISABLED", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("FLOW_DOCTOR_ENABLED", "0")

    sent = flow_doctor_telegram.notify_via_flow_doctor("prod fallback", **_CALL)

    assert sent is True
    transport.assert_called_once()


def test_a_mocked_transport_is_still_called_under_pytest(transport, monkeypatch):
    """Handler tests that assert on the fallback replace ``send_message`` with
    a double; only the REAL transport is refused, so they keep working."""
    monkeypatch.setenv("FLOW_DOCTOR_DISABLED", "1")
    double = mock.Mock(return_value=True)
    monkeypatch.setattr(flow_doctor_telegram, "send_message", double)

    assert flow_doctor_telegram.notify_via_flow_doctor("x", **_CALL) is True
    double.assert_called_once_with("x", disable_notification=False)
    transport.assert_not_called()
