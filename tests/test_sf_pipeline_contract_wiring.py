"""Pins the pre-spend pipeline-contract preflight gate in the Saturday SF
(L4595 / config#693).

The gate (`PipelineContractCheck` -> `PipelineContractGate`) MUST run right
after the lib-pin-drift gate passes, and BEFORE any spot launch, hard-failing
the SF on a CONFIRMED `PIPELINE_CONTRACT.yaml` self-consistency / dangling
`artifact_id` violation, while failing OPEN on the probe's own error. These
tests catch regressions like: someone reorders it after a spot launch
(defeating "fail before spend"), drops the fail-open Catch (probe fragility
false-halts the weekly run), or inverts the gate's halt condition.

Mirrors `tests/test_sf_lib_pin_drift_wiring.py` exactly — this is the sibling
gate composed directly after `LibPinDriftGate`'s pass-through.

Pairs with alpha-engine-predictor `inference/pipeline_contract_check.py` (the
`action=check_pipeline_contract` Lambda handler).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf():
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf):
    return sf["States"]


@pytest.mark.parametrize(
    "name",
    ["PipelineContractCheck", "PipelineContractGate", "ExtractPipelineContractError"],
)
def test_gate_states_exist(states, name):
    assert name in states, f"{name} missing from Saturday SF States"


def test_lib_pin_drift_gate_pass_through_routes_here(states):
    # LibPinDriftGate's no-drift Default used to go straight to CheckMutexRole;
    # config#693 inserts this gate immediately after it in the preflight chain.
    assert states["LibPinDriftGate"]["Default"] == "PipelineContractCheck"


def test_check_invokes_predictor_lambda_with_action(states):
    chk = states["PipelineContractCheck"]
    assert chk["Type"] == "Task"
    assert chk["Resource"] == "arn:aws:states:::lambda:invoke"
    assert chk["Parameters"]["FunctionName"] == "alpha-engine-predictor-inference:live"
    assert chk["Parameters"]["Payload"]["action"] == "check_pipeline_contract"
    assert chk["ResultPath"] == "$.pipeline_contract_result"
    assert chk["Next"] == "PipelineContractGate"


def test_check_fails_open_via_catch(states):
    # The probe's own failure must proceed into the pipeline, NOT halt the
    # weekly run.
    catch = states["PipelineContractCheck"]["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "CheckMutexRole"  # the pipeline, not HandleFailure


def test_gate_halts_only_on_confirmed_violation(states):
    gate = states["PipelineContractGate"]
    assert gate["Type"] == "Choice"
    c = gate["Choices"][0]
    assert c["Variable"] == "$.pipeline_contract_result.Payload.has_violation"
    assert c["BooleanEquals"] is True
    # Confirmed violation halts, but routes through the $.error normalizer
    # FIRST (not straight to HandleFailure) — see
    # test_violation_halt_normalizes_error.
    assert c["Next"] == "ExtractPipelineContractError"
    # No violation -> proceed into the pipeline, same target LibPinDriftGate
    # used before this gate was inserted.
    assert gate["Default"] == "CheckMutexRole"


def test_violation_halt_normalizes_error_before_handle_failure(states):
    """The gate is a Choice, so — unlike a Task Catch (ResultPath $.error) —
    its transition does NOT populate $.error. HandleFailure's Message calls
    States.JsonToString($.error), so a direct Choice->HandleFailure jump would
    kill the SF with an opaque States.Runtime ('$.error could not be found')
    that swallows the violation list. The violation path must first hit an
    Extract*Error normalizer that writes $.error, matching every other
    HandleFailure entry (see ExtractLibPinDriftError)."""
    norm = states["ExtractPipelineContractError"]
    assert norm["Type"] == "Pass"
    assert norm["ResultPath"] == "$.error"
    assert norm["Next"] == "HandleFailure"
    # References only the whole probe Payload — guaranteed present because the
    # gate already dereferenced Payload.has_violation to route here — so the
    # normalizer cannot itself raise a missing-field States.Runtime.
    assert norm["Parameters"]["violation.$"] == "$.pipeline_contract_result.Payload"


def test_every_handle_failure_entry_populates_error(states):
    """CHOKEPOINT for the whole failure class (2026-07-03): HandleFailure's
    Message template hard-requires $.error via States.JsonToString($.error), so
    EVERY transition that lands on HandleFailure must guarantee $.error is set —
    either a Task Catch with ResultPath '$.error', or a Pass normalizer with
    ResultPath '$.error'. A future soft-path/Choice that jumps straight to
    HandleFailure would re-introduce the opaque States.Runtime meta-crash; this
    test fails loudly if any such path appears. Duplicated from
    test_sf_lib_pin_drift_wiring.py so this file is a self-contained sibling
    check for the pipeline-contract gate's own contribution to that
    invariant."""
    offenders = []
    for name, st in states.items():
        # Catch-based entries.
        for cat in st.get("Catch", []):
            if cat.get("Next") == "HandleFailure" and cat.get("ResultPath") != "$.error":
                offenders.append(f"{name} Catch ResultPath={cat.get('ResultPath')!r}")
        # State-transition entries (Next / Default / Choice).
        transitions = [st.get("Next"), st.get("Default")]
        transitions += [c.get("Next") for c in st.get("Choices", [])]
        if "HandleFailure" in transitions:
            # The source state must itself write $.error before handing off —
            # i.e. be a Pass normalizer with ResultPath '$.error'.
            if not (st.get("Type") == "Pass" and st.get("ResultPath") == "$.error"):
                offenders.append(
                    f"{name} (Type={st.get('Type')}) transitions to HandleFailure "
                    f"without setting $.error (ResultPath={st.get('ResultPath')!r})"
                )
    assert not offenders, (
        "State(s) reach HandleFailure without populating $.error — "
        "HandleFailure will die with States.Runtime: " + "; ".join(offenders)
    )


def test_gate_runs_before_any_spot_launch(sf, states):
    """Walk the happy path (Next / Choice-Default) from StartAt and assert
    PipelineContractGate is reached BEFORE the first ssm:sendCommand spot
    launch — the whole point of L4595 is to fail before any spot spend, same
    as L4517's lib-pin-drift gate."""
    seen_gate = False
    cur = sf["StartAt"]
    for _ in range(40):  # bounded walk
        st = states[cur]
        res = st.get("Resource", "")
        if "ssm:sendCommand" in res:
            assert seen_gate, (
                f"spot launch {cur} reached before PipelineContractGate — the "
                f"gate must precede all spot spend"
            )
            return
        if cur == "PipelineContractGate":
            seen_gate = True
        nxt = st.get("Next") or st.get("Default")
        if nxt is None:
            break
        cur = nxt
    assert seen_gate, "walk never reached PipelineContractGate"


def test_gate_runs_immediately_after_lib_pin_drift_gate(sf, states):
    """Walk the happy path and assert PipelineContractGate is reached right
    after LibPinDriftGate (with only the PipelineContractCheck Task state in
    between) — the composed 'preflight the pipeline's invariants' chain from
    config#693."""
    cur = sf["StartAt"]
    seen_lib_pin_gate = False
    for _ in range(40):
        st = states[cur]
        if seen_lib_pin_gate:
            # The very next states after LibPinDriftGate's pass-through must
            # be PipelineContractCheck then PipelineContractGate.
            assert cur == "PipelineContractCheck", (
                f"expected PipelineContractCheck immediately after "
                f"LibPinDriftGate's pass-through, got {cur}"
            )
            assert st["Next"] == "PipelineContractGate"
            return
        if cur == "LibPinDriftGate":
            seen_lib_pin_gate = True
        nxt = st.get("Next") or st.get("Default")
        if nxt is None:
            break
        cur = nxt
    assert seen_lib_pin_gate, "walk never reached LibPinDriftGate"
