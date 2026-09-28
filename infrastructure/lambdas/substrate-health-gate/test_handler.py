"""Unit tests for the substrate-health-gate handler.

config#2249: covers the two "closes-when" failure shapes named in the
issue — a simulated dead/full dispatch box producing a distinctly-named
SubstrateUnhealthy verdict fast, instead of falling through to a generic
retry-then-fail path — plus the healthy pass-through case.
"""

from __future__ import annotations

import json
import sys
import time
import types
from datetime import datetime, timezone
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


# ── alpha-engine-config-I10172: the stage-coverage self-assertion ───────────


def test_stage_coverage_is_unmeasured_without_a_run_date():
    """No run_date on the event (an operator off-cycle invocation, or a test
    fixture predating I8155) must report UNMEASURED, never fabricate a date
    (alpha-engine-config-I8155) and never attempt the krepis import/AWS call."""
    ssm = _ssm_stub(invocation_sequence=[_df_success(42)])
    with mock.patch.object(index, "_ssm", ssm), mock.patch.object(time, "sleep"):
        out = index.handler(_event(), None)
    assert out["stage_coverage"] == {
        "stage": "SubstrateHealthGate",
        "status": "UNMEASURED",
        "reason": "no run_date on state input",
    }


def _fake_stage_coverage_module(calls: list):
    """A stand-in ``krepis.stage_coverage`` that records each call and never
    touches AWS — so these tests are hermetic whether or not krepis is
    installed (CI installs requirements.txt; deploy.sh installs only boto3)."""
    fake = types.ModuleType("krepis.stage_coverage")

    def assert_stage_coverage(stage, *, run_date, window_start=None, **_):
        calls.append({"stage": stage, "run_date": run_date, "window_start": window_start})
        return {"stage": stage, "status": "COVERED_NO_OUTPUT", "run_date": run_date}

    fake.assert_stage_coverage = assert_stage_coverage
    return fake


def _clock_that_never_expires():
    """Monotonic clock stub for the poll-budget-exhaustion paths without
    sleeping: each call advances 1s, so the 45s budget runs out in 45 polls."""
    ticks = iter(range(0, 10_000))
    return lambda: float(next(ticks))


# Every exit path of handler(): (label, invocation_sequence, verdict, reason).
_EXIT_PATHS = [
    ("healthy", [_df_success(10)], "HEALTHY", None),
    ("disk_full", [_df_success(95)], "SUBSTRATE_UNHEALTHY", "disk_full"),
    (
        "terminal_non_success",
        [{"Status": "Failed", "ResponseCode": 1, "StatusDetails": "Failed"}],
        "SUBSTRATE_UNHEALTHY",
        "ssm_unresponsive",
    ),
    (
        "unparseable_df",
        [{"Status": "Success", "ResponseCode": 0, "StandardOutputContent": "garbage"}],
        "SUBSTRATE_UNHEALTHY",
        "ssm_unresponsive",
    ),
    (
        "never_registered",
        [_not_registered_error() for _ in range(100)],
        "SUBSTRATE_UNHEALTHY",
        "ssm_command_never_registered",
    ),
    (
        "registered_never_terminal",
        [{"Status": "InProgress"} for _ in range(100)],
        "SUBSTRATE_UNHEALTHY",
        "ssm_unresponsive",
    ),
]


@pytest.mark.parametrize(
    "label,sequence,verdict,reason", _EXIT_PATHS, ids=[p[0] for p in _EXIT_PATHS]
)
def test_stage_coverage_is_asserted_exactly_once_on_every_exit_path(
    monkeypatch, label, sequence, verdict, reason
):
    """The coverage question ('did this stage run and declare itself') is
    orthogonal to the health question — asserted once on EVERY return,
    HEALTHY and each SUBSTRATE_UNHEALTHY reason alike, keyed by the SF's
    trading-day run_date (alpha-engine-config-I10172)."""
    calls: list = []
    monkeypatch.setitem(sys.modules, "krepis.stage_coverage", _fake_stage_coverage_module(calls))
    ssm = _ssm_stub(invocation_sequence=sequence)
    before = datetime.now(timezone.utc)
    with (
        mock.patch.object(index, "_ssm", ssm),
        mock.patch.object(time, "sleep"),
        mock.patch.object(time, "monotonic", _clock_that_never_expires()),
    ):
        out = index.handler(_event(run_date="2026-09-25"), None)

    assert out["verdict"] == verdict
    assert out.get("reason") == reason
    assert len(calls) == 1, f"{label}: asserted {len(calls)} times"
    assert calls[0]["stage"] == "SubstrateHealthGate"
    assert calls[0]["run_date"] == "2026-09-25"
    # Window opens at handler ENTRY (alpha-engine-config-I7214).
    assert before <= calls[0]["window_start"] <= datetime.now(timezone.utc)
    assert out["stage_coverage"] == {
        "stage": "SubstrateHealthGate",
        "status": "COVERED_NO_OUTPUT",
        "run_date": "2026-09-25",
    }


@pytest.mark.parametrize("label", ["healthy", "disk_full", "never_registered"])
def test_real_krepis_records_covered_no_output_to_the_trading_day_partition(
    monkeypatch, tmp_path, label
):
    """End to end through the REAL krepis.stage_coverage (skipped where the
    deploy.sh preflight installs only boto3): with the registry row
    ARTIFACT_REGISTRY.yaml carries for this stage (`output: none`), the
    handler writes COVERED_NO_OUTPUT to
    `_stage_coverage/<run_date>/SubstrateHealthGate.json` on the healthy and
    unhealthy paths alike. AWS is replaced with mocks — nothing is written."""
    real = pytest.importorskip("krepis.stage_coverage")
    registry = tmp_path / "registry.yaml"
    registry.write_text(
        "pipeline_stages:\n"
        "  - stage: SubstrateHealthGate\n"
        "    stage_class: infrastructure\n"
        "    output: none\n"
        "    reason: constructs only an SSM client and sends one disk probe\n"
    )
    s3 = mock.Mock()
    cloudwatch = mock.Mock()
    wrapper = types.ModuleType("krepis.stage_coverage")
    wrapper.assert_stage_coverage = lambda stage, **kw: real.assert_stage_coverage(
        stage,
        s3_client=s3,
        cloudwatch_client=cloudwatch,
        registry_local_path=str(registry),
        **kw,
    )
    monkeypatch.setitem(sys.modules, "krepis.stage_coverage", wrapper)
    sequence = dict((p[0], p[1]) for p in _EXIT_PATHS)[label]
    ssm = _ssm_stub(invocation_sequence=sequence)
    with (
        mock.patch.object(index, "_ssm", ssm),
        mock.patch.object(time, "sleep"),
        mock.patch.object(time, "monotonic", _clock_that_never_expires()),
    ):
        out = index.handler(_event(run_date="2026-09-25"), None)

    assert out["stage_coverage"]["status"] == "COVERED_NO_OUTPUT"
    assert out["stage_coverage"]["stage"] == "SubstrateHealthGate"
    s3.put_object.assert_called_once()
    put = s3.put_object.call_args.kwargs
    assert put["Bucket"] == "alpha-engine-research"
    assert put["Key"] == "_stage_coverage/2026-09-25/SubstrateHealthGate.json"
    assert json.loads(put["Body"])["status"] == "COVERED_NO_OUTPUT"


def test_stage_coverage_assertion_is_import_guarded_and_loud():
    """The nousergon-lib/krepis pin may predate the module; an inert
    assertion must stay distinguishable from a covered stage and must not
    change the gate's own verdict."""
    body = (Path(__file__).parent / "index.py").read_text()
    assert "from krepis.stage_coverage import assert_stage_coverage" in body
    assert "except ImportError as exc:" in body
    assert "UNMEASURED" in body


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
