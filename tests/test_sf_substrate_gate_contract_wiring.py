"""Contract guard for the pre-dispatch SubstrateHealthGate (config#2249).

Regression guard for the 2026-07-17 Friday-PM preflight failure. The gate
Task's ``ResultSelector`` selected ``reason.$: $.Payload.reason``
unconditionally, but the ``alpha-engine-substrate-health-gate`` Lambda's
DOCUMENTED contract (see its ``index.py`` docstring) emits ``reason`` ONLY on
the ``SUBSTRATE_UNHEALTHY`` verdicts (disk_full / ssm_unresponsive /
ssm_command_never_registered) — the ``HEALTHY`` payload deliberately omits it.
Every healthy substrate (the normal case) therefore crashed the state with
``States.Runtime`` "The JSONPath '$.Payload.reason' specified for the field
'reason.$' could not be found", so the gate could NEVER pass on a healthy box.

The fix passes the WHOLE Lambda payload through under ``.gate`` and lets each
downstream consumer read only the fields the producer guarantees for the
branch it is reached on: the Choice reads ``.gate.verdict`` (always present);
``ExtractSubstrateHealthGateError`` reads ``.gate.reason`` / ``.gate.message``
and is reachable ONLY on the non-HEALTHY Default (where the producer
guarantees them).

These tests pin that contract by cross-checking the SF's ResultSelector
against the ACTUAL keys the Lambda emits on each verdict — so any future
regression that reintroduces a per-key ``$.Payload.<field>`` select of a
field the healthy path omits fails HERE, pre-merge, instead of only in a
live (or preflight) Saturday run.
"""

from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path
from unittest import mock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"
_LAMBDA_INDEX = (
    _REPO_ROOT
    / "infrastructure"
    / "lambdas"
    / "substrate-health-gate"
    / "index.py"
)


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def gate_module():
    """Load the gate Lambda handler by explicit path under a UNIQUE module
    name. Other ``infrastructure/lambdas/*/index.py`` files share the bare
    name ``index``; loading via a distinct name avoids any ``sys.modules``
    collision if another test imported a different ``index`` first (which is
    why the repo otherwise runs lambda handler tests in isolated processes).
    """
    spec = importlib.util.spec_from_file_location(
        "substrate_gate_index_under_test", _LAMBDA_INDEX
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _df_success(used_percent: int) -> dict:
    return {
        "Status": "Success",
        "ResponseCode": 0,
        "StatusDetails": "Success",
        "StandardOutputContent": (
            f"/dev/xvda1 20961280 18642176 1264104 {used_percent}% /\n"
        ),
    }


def _run_handler(mod, invocation: dict) -> dict:
    """Invoke the real handler with a mocked SSM client returning ``invocation``
    on the first ``get_command_invocation`` poll — mirrors the Lambda's own
    ``test_handler.py`` harness."""
    ssm = mock.Mock()
    ssm.send_command.return_value = {"Command": {"CommandId": "cmd-under-test"}}
    ssm.get_command_invocation.side_effect = [invocation]
    with mock.patch.object(mod, "_ssm", ssm), mock.patch.object(time, "sleep"):
        return mod.handler({"instance_id": "i-contract-test"}, None)


@pytest.fixture(scope="module")
def healthy_output(gate_module) -> dict:
    out = _run_handler(gate_module, _df_success(42))
    assert out["verdict"] == "HEALTHY", "fixture precondition: expected HEALTHY"
    return out


@pytest.fixture(scope="module")
def disk_full_output(gate_module) -> dict:
    out = _run_handler(gate_module, _df_success(100))
    assert out["verdict"] == "SUBSTRATE_UNHEALTHY", (
        "fixture precondition: expected SUBSTRATE_UNHEALTHY"
    )
    return out


def _payload_fields_selected(result_selector: dict) -> list[str]:
    """Top-level keys a ResultSelector cherry-picks straight out of
    ``$.Payload`` (e.g. ``reason.$: $.Payload.reason`` -> ``reason``). A bare
    ``gate.$: $.Payload`` (full passthrough) selects NO individual key and so
    returns nothing — exactly the shape that cannot crash on a missing key."""
    fields = []
    for value in result_selector.values():
        if isinstance(value, str) and value.startswith("$.Payload."):
            rest = value[len("$.Payload.") :]
            top = rest.split(".")[0].split("[")[0]
            fields.append(top)
    return fields


class TestResultSelectorMatchesProducerContract:
    """The heart of the guard: the SF must never select a $.Payload key the
    Lambda omits on a verdict the state can actually receive."""

    def test_resultselector_never_selects_a_key_absent_on_healthy(
        self, states, healthy_output
    ):
        rs = states["SubstrateHealthGate"]["ResultSelector"]
        selected = _payload_fields_selected(rs)
        missing = [f for f in selected if f not in healthy_output]
        assert not missing, (
            f"SubstrateHealthGate ResultSelector selects $.Payload.{missing} "
            f"but the HEALTHY Lambda payload omits it "
            f"(healthy keys: {sorted(healthy_output)}). An unconditional "
            f"per-key select of a field the healthy path does not emit is the "
            f"2026-07-17 States.Runtime crash (the gate can never pass a "
            f"healthy box). Pass the full payload through — gate.$ : $.Payload "
            f"— and read per-field downstream instead."
        )

    def test_healthy_payload_indeed_omits_reason(self, healthy_output):
        # Locks the producer half of the contract this guard depends on: if a
        # future lib change starts emitting `reason` on HEALTHY too, this test
        # documents that the coupling above became moot (update deliberately).
        assert "reason" not in healthy_output
        assert healthy_output["verdict"] == "HEALTHY"


class TestDownstreamReadsAreSafeOnTheirBranch:
    """Every field a downstream consumer reads from the gate result must be
    guaranteed present on the branch that consumer is reached on."""

    def test_extract_error_fields_present_on_unhealthy_payload(
        self, states, disk_full_output
    ):
        # ExtractSubstrateHealthGateError is reached ONLY via the non-HEALTHY
        # Default, so it may rely on the unhealthy contract carrying these.
        params = states["ExtractSubstrateHealthGateError"]["Parameters"]
        for key in ("verdict.$", "reason.$", "detail.$"):
            ref = params[key]
            field = ref.rsplit(".", 1)[-1]  # ...gate.reason -> reason
            assert field in disk_full_output, (
                f"ExtractSubstrateHealthGateError reads {ref} (-> $.Payload."
                f"{field}) but the SUBSTRATE_UNHEALTHY payload omits {field!r} "
                f"({sorted(disk_full_output)}); the named-failure alert would "
                f"crash on a real unhealthy box."
            )


class TestWiringPinsGateScopedReads:
    """Pin the .gate passthrough shape so the ResultSelector and its consumers
    stay in lockstep (a rename of the passthrough key must update both)."""

    def test_resultselector_captures_full_payload_under_gate(self, states):
        rs = states["SubstrateHealthGate"]["ResultSelector"]
        assert rs == {"gate.$": "$.Payload"}, (
            "SubstrateHealthGate must pass the whole Lambda payload through "
            "under .gate; cherry-picking $.Payload.<key> reintroduces the "
            "missing-key crash class."
        )
        assert (
            states["SubstrateHealthGate"]["ResultPath"] == "$.substrate_gate_result"
        )

    def test_choice_branches_on_gate_verdict_with_ispresent_guard(self, states):
        # config#2275: `.gate.verdict` is a 3-segment Lambda-payload path and
        # is therefore NEVER floorable — it MUST be IsPresent-guarded inside an
        # And whose earlier operand guards the exact same path. This is the
        # guard-blessed idiom for a Choice reading an opaque Lambda output.
        rule = states["CheckSubstrateHealthGate"]["Choices"][0]
        assert rule["Next"] == "MorningEnrich"
        operands = rule["And"]
        var = "$.substrate_gate_result.gate.verdict"
        assert operands[0] == {"Variable": var, "IsPresent": True}, (
            "the FIRST And operand must IsPresent-guard the verdict path "
            "(ASL short-circuits on the first false operand)"
        )
        assert {"Variable": var, "StringEquals": "HEALTHY"} in operands

    def test_extract_reads_gate_scoped_fields(self, states):
        params = states["ExtractSubstrateHealthGateError"]["Parameters"]
        assert params["verdict.$"] == "$.substrate_gate_result.gate.verdict"
        assert params["reason.$"] == "$.substrate_gate_result.gate.reason"
        assert params["detail.$"] == "$.substrate_gate_result.gate.message"
