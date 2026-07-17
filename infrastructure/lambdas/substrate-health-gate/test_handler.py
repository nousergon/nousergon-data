"""Unit tests for the substrate-health-gate handler.

config#2249: covers the two "closes-when" failure shapes named in the
issue — a simulated dead/full dispatch box producing a distinctly-named
SubstrateUnhealthy verdict fast, instead of falling through to a generic
retry-then-fail path — plus the healthy pass-through case.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest import mock

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402


def _event(**over):
    base = {"instance_id": "i-018eb3307a21329bf"}
    base.update(over)
    return base


def _ssm_stub(send_command_side_effect=None, invocation_sequence=None):
    """Build a mock SSM client.

    invocation_sequence: list of either a dict (get_command_invocation
    return value) or a ClientError instance (raised), consumed in order on
    successive get_command_invocation calls.
    """
    ssm = mock.Mock()
    if send_command_side_effect is not None:
        ssm.send_command.side_effect = send_command_side_effect
    else:
        ssm.send_command.return_value = {"Command": {"CommandId": "cmd-abc"}}

    if invocation_sequence is not None:
        ssm.get_command_invocation.side_effect = list(invocation_sequence)
    return ssm


def _not_registered_error():
    return ClientError(
        {"Error": {"Code": "InvocationDoesNotExist", "Message": "x"}},
        "GetCommandInvocation",
    )


def _df_success(used_percent: int):
    return {
        "Status": "Success",
        "ResponseCode": 0,
        "StatusDetails": "Success",
        "StandardOutputContent": (
            f"/dev/xvda1 20961280 18642176 1264104 {used_percent}% /\n"
        ),
    }


def test_healthy_low_disk_usage():
    ssm = _ssm_stub(invocation_sequence=[_df_success(42)])
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        out = index.handler(_event(), None)
    assert out["verdict"] == "HEALTHY"
    assert out["disk_used_percent"] == 42


def test_disk_full_verdict_is_named_substrate_unhealthy():
    """The issue's headline scenario: disk 100% full must produce a NAMED
    SubstrateUnhealthy verdict (not a generic failure)."""
    ssm = _ssm_stub(invocation_sequence=[_df_success(100)])
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        out = index.handler(_event(), None)
    assert out["verdict"] == "SUBSTRATE_UNHEALTHY"
    assert out["reason"] == "disk_full"
    assert "disk 100%" in out["message"]
    assert out["disk_used_percent"] == 100


def test_disk_at_warn_threshold_is_unhealthy():
    ssm = _ssm_stub(invocation_sequence=[_df_success(index.DISK_WARN_PERCENT)])
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        out = index.handler(_event(), None)
    assert out["verdict"] == "SUBSTRATE_UNHEALTHY"
    assert out["reason"] == "disk_full"


def test_disk_just_under_threshold_is_healthy():
    ssm = _ssm_stub(
        invocation_sequence=[_df_success(index.DISK_WARN_PERCENT - 1)]
    )
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        out = index.handler(_event(), None)
    assert out["verdict"] == "HEALTHY"


def test_ssm_command_never_registers_is_distinctly_named():
    """The issue's second scenario: SSM agent unresponsive so the command
    silently never registers (InvocationDoesNotExist forever) — must
    produce a DISTINCT named verdict from disk_full, and must not hang past
    the poll budget."""
    ssm = _ssm_stub(
        invocation_sequence=[_not_registered_error() for _ in range(1000)]
    )
    fake_clock = {"t": 0.0}

    def _monotonic():
        return fake_clock["t"]

    def _sleep(seconds):
        fake_clock["t"] += seconds

    with mock.patch.object(index, "_ssm", ssm), \
         mock.patch.object(time, "monotonic", _monotonic), \
         mock.patch.object(time, "sleep", _sleep):
        out = index.handler(_event(), None)

    assert out["verdict"] == "SUBSTRATE_UNHEALTHY"
    assert out["reason"] == "ssm_command_never_registered"
    assert "never registered" in out["message"]
    # Distinct from the disk-full reason — callers must be able to tell
    # the two failure classes apart.
    assert out["reason"] != "disk_full"


def test_command_registers_but_never_reaches_success_is_ssm_unresponsive():
    """The agent picked up the command (it registered) but the box never
    finished it — wedged/thrashing, not a clean disk-full readout."""
    ssm = _ssm_stub(
        invocation_sequence=[{"Status": "InProgress"} for _ in range(1000)]
    )
    fake_clock = {"t": 0.0}

    def _monotonic():
        return fake_clock["t"]

    def _sleep(seconds):
        fake_clock["t"] += seconds

    with mock.patch.object(index, "_ssm", ssm), \
         mock.patch.object(time, "monotonic", _monotonic), \
         mock.patch.object(time, "sleep", _sleep):
        out = index.handler(_event(), None)

    assert out["verdict"] == "SUBSTRATE_UNHEALTHY"
    assert out["reason"] == "ssm_unresponsive"


def test_terminal_non_success_status_is_ssm_unresponsive():
    ssm = _ssm_stub(
        invocation_sequence=[
            {
                "Status": "TimedOut",
                "ResponseCode": -1,
                "StatusDetails": "TimedOut",
                "StandardOutputContent": "",
            }
        ]
    )
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        out = index.handler(_event(), None)
    assert out["verdict"] == "SUBSTRATE_UNHEALTHY"
    assert out["reason"] == "ssm_unresponsive"


def test_success_with_unparseable_output_does_not_assume_healthy():
    ssm = _ssm_stub(
        invocation_sequence=[
            {
                "Status": "Success",
                "ResponseCode": 0,
                "StatusDetails": "Success",
                "StandardOutputContent": "garbage, not a df line\n",
            }
        ]
    )
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        out = index.handler(_event(), None)
    assert out["verdict"] == "SUBSTRATE_UNHEALTHY"
    assert out["reason"] == "ssm_unresponsive"


def test_unexpected_client_error_raises_fail_loud():
    ssm = mock.Mock()
    ssm.send_command.return_value = {"Command": {"CommandId": "cmd-abc"}}
    ssm.get_command_invocation.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "x"}},
        "GetCommandInvocation",
    )
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        with pytest.raises(ClientError):
            index.handler(_event(), None)


def test_send_command_error_raises_fail_loud():
    ssm = _ssm_stub(
        send_command_side_effect=ClientError(
            {"Error": {"Code": "InvalidInstanceId", "Message": "x"}},
            "SendCommand",
        )
    )
    with mock.patch.object(index, "_ssm", ssm):
        with pytest.raises(ClientError):
            index.handler(_event(), None)


def test_two_distinct_named_failure_reasons_are_never_conflated():
    """Explicit cross-check per the issue's closes-when: disk-full and
    ssm-unresponsive must be reachable and produce DIFFERENT reason values,
    not just different message text."""
    ssm_disk_full = _ssm_stub(invocation_sequence=[_df_success(100)])
    with mock.patch.object(index, "_ssm", ssm_disk_full), \
         mock.patch.object(time, "sleep"):
        disk_out = index.handler(_event(), None)

    ssm_unresponsive = _ssm_stub(
        invocation_sequence=[_not_registered_error() for _ in range(1000)]
    )
    fake_clock = {"t": 0.0}
    with mock.patch.object(index, "_ssm", ssm_unresponsive), \
         mock.patch.object(time, "monotonic", lambda: fake_clock["t"]), \
         mock.patch.object(time, "sleep", lambda s: fake_clock.__setitem__("t", fake_clock["t"] + s)):
        unresponsive_out = index.handler(_event(), None)

    assert disk_out["verdict"] == unresponsive_out["verdict"] == "SUBSTRATE_UNHEALTHY"
    assert disk_out["reason"] == "disk_full"
    assert unresponsive_out["reason"] == "ssm_command_never_registered"
    assert disk_out["reason"] != unresponsive_out["reason"]


def test_ssm_delivery_timeout_respects_api_minimum():
    """SSM SendCommand rejects TimeoutSeconds < 30 with ParamValidationError
    BEFORE the command is sent. The mocked-ssm tests above can never catch a
    violation (botocore param validation only runs against the real client),
    so pin the contract here: 15 broke the 2026-07-17 Friday-shell preflight
    at SubstrateHealthGate on the gate's first-ever live invocation."""
    assert index._PROBE_DELIVERY_TIMEOUT_SECONDS >= 30
