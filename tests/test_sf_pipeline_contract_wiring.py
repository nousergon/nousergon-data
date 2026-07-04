"""Pins the pre-spend PIPELINE_CONTRACT.yaml preflight gate in the Saturday
SF (L4595 / config#693).

The gate (`CheckPipelineContract` → `PipelineContractGate`) MUST run before
any spot launch and hard-fail the SF on a CONFIRMED PIPELINE_CONTRACT.yaml
self-consistency break (a dangling `artifact_id` or a boundary missing a
required key), while failing OPEN on the probe's own error. These tests
catch regressions like: someone reorders it after a spot launch (defeating
"fail before spend"), drops the fail-open Catch (probe fragility false-halts
the weekly run), or inverts the gate's halt condition.

Pairs with alpha-engine-predictor `inference/pipeline_contract_check.py`
(the `action=check_pipeline_contract` Lambda handler, PR crucible-predictor
#297). Modeled directly on `test_sf_lib_pin_drift_wiring.py` — the sibling
gate this one runs immediately after.
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
    [
        "CheckSkipPipelineContractCheck",
        "CheckPipelineContract",
        "PipelineContractGate",
    ],
)
def test_gate_states_exist(states, name):
    assert name in states, f"{name} missing from Saturday SF States"


def test_runs_immediately_after_lib_pin_drift_gate(states):
    # The gate must be the next WORKLOAD gate after LibPinDriftGate — no spot
    # launch or other work state may sit between the two pre-spend probes.
    assert states["LibPinDriftGate"]["Default"] == "CheckSkipPipelineContractCheck"


def test_skip_gate_default_runs_check_and_skip_bypasses(states):
    skip = states["CheckSkipPipelineContractCheck"]
    assert skip["Default"] == "CheckPipelineContract"
    c = skip["Choices"][0]
    # skip_pipeline_contract_check == true bypasses straight into the pipeline
    assert c["Next"] == "CheckMutexRole"
    variables = {x["Variable"] for x in c["And"]}
    assert variables == {"$.skip_pipeline_contract_check"}


def test_check_invokes_predictor_lambda_with_action(states):
    chk = states["CheckPipelineContract"]
    assert chk["Type"] == "Task"
    assert chk["Resource"] == "arn:aws:states:::lambda:invoke"
    assert chk["Parameters"]["FunctionName"] == "alpha-engine-predictor-inference:live"
    assert chk["Parameters"]["Payload"]["action"] == "check_pipeline_contract"
    assert chk["ResultPath"] == "$.pipeline_contract_result"
    assert chk["Next"] == "PipelineContractGate"


def test_check_fails_open_via_catch(states):
    # The probe's own failure must proceed into the pipeline, NOT halt the
    # weekly run.
    catch = states["CheckPipelineContract"]["Catch"][0]
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
    # No violation → proceed into the pipeline.
    assert gate["Default"] == "CheckMutexRole"


def test_violation_halt_normalizes_error_before_handle_failure(states):
    """The gate is a Choice, so — unlike a Task Catch (ResultPath $.error) —
    its transition does NOT populate $.error. HandleFailure's Message calls
    States.JsonToString($.error), so a direct Choice→HandleFailure jump would
    kill the SF with an opaque States.Runtime that swallows the violation
    reason (the exact 2026-07-03 lib-pin-drift incident this mirrors). The
    violation path must first hit a normalizer that writes $.error, matching
    every other HandleFailure entry."""
    norm = states["ExtractPipelineContractError"]
    assert norm["Type"] == "Pass"
    assert norm["ResultPath"] == "$.error"
    assert norm["Next"] == "HandleFailure"
    # References only the whole probe Payload — guaranteed present because
    # the gate already dereferenced Payload.has_violation to route here — so
    # the normalizer cannot itself raise a missing-field States.Runtime.
    assert norm["Parameters"]["violation.$"] == "$.pipeline_contract_result.Payload"


def test_gate_runs_before_any_spot_launch(sf, states):
    """Walk the happy path (Next / Choice-Default) from StartAt and assert
    PipelineContractGate is reached BEFORE the first ssm:sendCommand spot
    launch — the whole point of this gate is to fail before any spot spend."""
    seen_gate = False
    cur = sf["StartAt"]
    for _ in range(40):  # bounded walk
        st = states[cur]
        res = st.get("Resource", "")
        if "ssm:sendCommand" in res:
            assert seen_gate, (
                f"spot launch {cur} reached before PipelineContractGate — "
                f"the gate must precede all spot spend"
            )
            return
        if cur == "PipelineContractGate":
            seen_gate = True
        nxt = st.get("Next") or st.get("Default")
        if nxt is None:
            break
        cur = nxt
    assert seen_gate, "walk never reached PipelineContractGate"


def test_backtest_eval_preset_skips_pipeline_contract_check(states):
    # config#830's mid-week Backtester→Evaluator-only preset must also skip
    # this gate — a partial rerun has no reason to re-validate the contract.
    preset = states["ApplyBacktestEvalPreset"]
    assert '"skip_pipeline_contract_check":true' in preset["Parameters"]["merged.$"]
