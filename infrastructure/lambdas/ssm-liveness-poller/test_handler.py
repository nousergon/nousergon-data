"""Unit tests for the ssm-liveness-poller handler.

Covers every verdict the ne-preopen-trading-pipeline Choice states
branch on, plus the two incident shapes that motivated the Lambda:
- config#1807 / 2026-07-06: command frozen InProgress while the SSM
  agent is ConnectionLost → INSTANCE_UNRESPONSIVE after N consecutive
  ping misses (was: 62 minutes of blind polling).
- #970 / 2026-06-11: command stuck InProgress with a healthy agent →
  POLL_BUDGET_EXHAUSTED at the attempt cap.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402


def _event(**over):
    base = {
        "instance_id": "i-018eb3307a21329bf",
        "command_id": "cmd-123",
        "attempts": 0,
        "ping_misses": 0,
        "max_attempts": 210,
        "max_ping_misses": 3,
        "step": "morning-enrich",
    }
    base.update(over)
    return base


def _mock_ssm(status="InProgress", ping="Online", rc=-1, stderr="",
              invocation_exists=True, instance_registered=True,
              status_details=None):
    ssm = mock.Mock()
    if invocation_exists:
        ssm.get_command_invocation.return_value = {
            "Status": status,
            "ResponseCode": rc,
            # SSM's StatusDetails is usually a finer-grained string than
            # Status (e.g. Status=Failed + StatusDetails=Undeliverable for a
            # non-delivery). Default to mirroring Status for the common case.
            "StatusDetails": status if status_details is None else status_details,
            "StandardErrorContent": stderr,
        }
    else:
        ssm.get_command_invocation.side_effect = ClientError(
            {"Error": {"Code": "InvocationDoesNotExist", "Message": "x"}},
            "GetCommandInvocation",
        )
    ssm.describe_instance_information.return_value = {
        "InstanceInformationList": (
            [{"PingStatus": ping}] if instance_registered else []
        )
    }
    return ssm


def test_success():
    with mock.patch.object(index, "_ssm", _mock_ssm(status="Success", rc=0)):
        out = index.handler(_event(), None)
    assert out["verdict"] == "SUCCESS"
    assert out["attempts"] == 1


@pytest.mark.parametrize("status", ["Failed", "TimedOut", "Cancelled", "Cancelling"])
def test_terminal_failure(status):
    with mock.patch.object(
        index, "_ssm", _mock_ssm(status=status, rc=137, stderr="boom")
    ):
        out = index.handler(_event(), None)
    assert out["verdict"] == "COMMAND_FAILED"
    assert "137" in out["detail"]
    assert out["stderr_tail"] == "boom"


def test_terminal_failure_genuine_script_error_stays_command_failed():
    """rc>=0 + StatusDetails=Failed + agent Online = a real script failure.

    Regression guard for the 2026-07-07 fix: reclassifying delivery
    failures must NOT swallow a genuine non-zero exit into a liveness
    verdict. A script that ran and exited 1 on a healthy box is
    COMMAND_FAILED.
    """
    with mock.patch.object(
        index, "_ssm",
        _mock_ssm(status="Failed", rc=1, stderr="traceback", ping="Online",
                  status_details="Failed"),
    ):
        out = index.handler(_event(), None)
    assert out["verdict"] == "COMMAND_FAILED"


def test_undeliverable_spot_interruption_is_instance_unresponsive():
    """The 2026-07-07 incident shape: the daily data spot was reclaimed
    mid-MorningArcticAppend. SSM marks the invocation Failed with
    StatusDetails=Undeliverable, rc=-1, and the agent is NotRegistered.
    This is a host-death (spot interruption), NOT a script failure, so it
    must carry the liveness verdict — routing to terminate-spot + alert,
    not the COMMAND_FAILED stale-artifact path.
    """
    with mock.patch.object(
        index, "_ssm",
        _mock_ssm(status="Failed", rc=-1, status_details="Undeliverable",
                  instance_registered=False),
    ):
        out = index.handler(_event(step="morning-arctic-append"), None)
    assert out["verdict"] == "INSTANCE_UNRESPONSIVE"
    assert out["ping_status"] == "NotRegistered"
    assert "vanished" in out["detail"].lower()


@pytest.mark.parametrize("detail", ["Undeliverable", "Terminated", "DeliveryTimedOut"])
def test_ssm_delivery_failure_details_are_liveness(detail):
    """Every SSM non-execution StatusDetails is a host/delivery failure,
    even if the agent momentarily still reports Online."""
    with mock.patch.object(
        index, "_ssm",
        _mock_ssm(status="Failed", rc=-1, status_details=detail, ping="Online"),
    ):
        out = index.handler(_event(), None)
    assert out["verdict"] == "INSTANCE_UNRESPONSIVE"


def test_terminal_failed_no_exit_code_and_agent_dark_is_liveness():
    """Belt-and-suspenders: even without a delivery StatusDetails, a
    terminal failure that captured no exit code (rc == -1) while the agent
    is off Online is a host-death, not a command failure."""
    with mock.patch.object(
        index, "_ssm",
        _mock_ssm(status="Failed", rc=-1, status_details="Failed",
                  ping="ConnectionLost"),
    ):
        out = index.handler(_event(), None)
    assert out["verdict"] == "INSTANCE_UNRESPONSIVE"


def test_in_progress_healthy_resets_ping_misses():
    with mock.patch.object(index, "_ssm", _mock_ssm(status="InProgress", ping="Online")):
        out = index.handler(_event(ping_misses=2), None)
    assert out["verdict"] == "IN_PROGRESS"
    assert out["ping_misses"] == 0  # Online resets the consecutive counter


def test_ping_miss_accumulates_below_threshold():
    with mock.patch.object(
        index, "_ssm", _mock_ssm(status="InProgress", ping="ConnectionLost")
    ):
        out = index.handler(_event(ping_misses=1), None)
    assert out["verdict"] == "IN_PROGRESS"
    assert out["ping_misses"] == 2


def test_instance_unresponsive_at_threshold_config1807_shape():
    """The 2026-07-06 incident: InProgress + ConnectionLost, 3rd miss."""
    with mock.patch.object(
        index, "_ssm", _mock_ssm(status="InProgress", ping="ConnectionLost")
    ):
        out = index.handler(_event(ping_misses=2), None)
    assert out["verdict"] == "INSTANCE_UNRESPONSIVE"
    assert "force-stop" in out["detail"].lower()


def test_unregistered_instance_counts_as_ping_miss():
    with mock.patch.object(
        index, "_ssm",
        _mock_ssm(status="InProgress", instance_registered=False),
    ):
        out = index.handler(_event(ping_misses=2), None)
    assert out["verdict"] == "INSTANCE_UNRESPONSIVE"
    assert out["ping_status"] == "NotRegistered"


def test_poll_budget_exhausted_970_shape():
    """#970: healthy agent, command frozen InProgress → budget cap."""
    with mock.patch.object(index, "_ssm", _mock_ssm(status="InProgress", ping="Online")):
        out = index.handler(_event(attempts=209), None)
    assert out["verdict"] == "POLL_BUDGET_EXHAUSTED"
    assert out["attempts"] == 210


def test_unresponsive_wins_over_budget_when_both_trip():
    with mock.patch.object(
        index, "_ssm", _mock_ssm(status="InProgress", ping="ConnectionLost")
    ):
        out = index.handler(_event(attempts=209, ping_misses=2), None)
    assert out["verdict"] == "INSTANCE_UNRESPONSIVE"


def test_registration_window_is_in_progress_not_error():
    with mock.patch.object(index, "_ssm", _mock_ssm(invocation_exists=False)):
        out = index.handler(_event(), None)
    assert out["verdict"] == "IN_PROGRESS"
    assert out["status"] == "Registering"


def test_registration_window_still_bounded_by_attempts():
    with mock.patch.object(index, "_ssm", _mock_ssm(invocation_exists=False)):
        out = index.handler(_event(attempts=209), None)
    assert out["verdict"] == "POLL_BUDGET_EXHAUSTED"


def test_unexpected_client_error_raises_fail_loud():
    ssm = mock.Mock()
    ssm.get_command_invocation.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "x"}},
        "GetCommandInvocation",
    )
    with mock.patch.object(index, "_ssm", ssm):
        with pytest.raises(ClientError):
            index.handler(_event(), None)


def test_success_ignores_stale_ping_state():
    """A completed command is SUCCESS even if the agent is currently dark
    (e.g. it finished right before a blip) — command outcome outranks
    liveness once terminal."""
    with mock.patch.object(
        index, "_ssm", _mock_ssm(status="Success", rc=0, ping="ConnectionLost")
    ):
        out = index.handler(_event(ping_misses=2), None)
    assert out["verdict"] == "SUCCESS"
