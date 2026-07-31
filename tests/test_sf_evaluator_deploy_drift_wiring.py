"""Pins the preventive evaluator Lambda-SHA drift gate pair in the Saturday SF
(config#2348 — 2026-07-13 operator ruling).

Before this gate, a failed or skipped post-merge deploy of crucible-evaluator
left `alpha-engine-evaluator` and/or `alpha-engine-evaluator-director`'s
`:live` alias silently behind `origin/main`, with only a best-effort Telegram
CI-failure message as the signal — no automated weekly check, no scripted
rollback. This gate (`EvaluatorDeployDriftCheck` -> `EvaluatorDeployDriftGate`
-> `EvaluatorDirectorDeployDriftCheck` -> `EvaluatorDirectorDeployDriftGate`)
checks BOTH Lambda aliases independently (same image, but the two `:live`
pointers can drift apart if only one got promoted) and hard-fails the SF on a
CONFIRMED `sha_mismatch`, while failing OPEN on the probe's own error.

Deliberately a NEW, STANDALONE pre-boot check — does NOT touch or extend the
weekday pipeline's existing `DeployDriftCheck`/`DeployDriftGate`
(alpha-engine-predictor Lambda, a separate load-bearing trading invariant
covering different Lambdas). Composed as the THIRD pre-spend sibling gate,
directly after `PipelineContractGate`'s pass-through.

Mirrors `tests/test_sf_lib_pin_drift_wiring.py` / `test_sf_pipeline_contract_
wiring.py` structurally; see `tests/test_sf_prespend_gate_alerting.py` for the
degraded-chain-alerting half (shared parametrized coverage across all four
pre-spend gates).

Pairs with crucible-evaluator `grading/deploy_drift.py` (the
`action=check_deploy_drift` handler dispatch shared by
`grading.handler.handler` and `director.handler.handler`).
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
        "EvaluatorDeployDriftCheck", "EvaluatorDeployDriftGate", "ExtractEvaluatorDeployDriftError",
        "EvaluatorGateDegraded", "PublishEvaluatorGateDegraded",
        "EvaluatorDirectorDeployDriftCheck", "EvaluatorDirectorDeployDriftGate",
        "ExtractEvaluatorDirectorDeployDriftError",
        "EvaluatorDirectorGateDegraded", "PublishEvaluatorDirectorGateDegraded",
    ],
)
def test_gate_states_exist(states, name):
    assert name in states, f"{name} missing from Saturday SF States"


def test_pipeline_contract_gate_pass_through_routes_here(states):
    # PipelineContractGate's no-violation Default used to go straight to
    # CheckMutexRole; config#2348 inserts this gate pair immediately after it.
    assert states["PipelineContractGate"]["Default"] == "EvaluatorDeployDriftCheck"


def test_check_invokes_grading_lambda_with_action(states):
    chk = states["EvaluatorDeployDriftCheck"]
    assert chk["Type"] == "Task"
    assert chk["Resource"] == "arn:aws:states:::lambda:invoke"
    assert chk["Parameters"]["FunctionName"] == "alpha-engine-evaluator:live"
    assert chk["Parameters"]["Payload"]["action"] == "check_deploy_drift"
    assert chk["ResultPath"] == "$.evaluator_deploy_drift_result"
    assert chk["Next"] == "EvaluatorDeployDriftGate"


def test_check_invokes_director_lambda_with_action(states):
    chk = states["EvaluatorDirectorDeployDriftCheck"]
    assert chk["Type"] == "Task"
    assert chk["Resource"] == "arn:aws:states:::lambda:invoke"
    assert chk["Parameters"]["FunctionName"] == "alpha-engine-evaluator-director:live"
    assert chk["Parameters"]["Payload"]["action"] == "check_deploy_drift"
    assert chk["ResultPath"] == "$.evaluator_director_deploy_drift_result"
    assert chk["Next"] == "EvaluatorDirectorDeployDriftGate"


@pytest.mark.parametrize("check", ["EvaluatorDeployDriftCheck", "EvaluatorDirectorDeployDriftCheck"])
def test_check_fails_open_via_catch(states, check):
    catch = states[check]["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]


@pytest.mark.parametrize(("gate", "result_path", "extract", "default"), [
    ("EvaluatorDeployDriftGate", "$.evaluator_deploy_drift_result",
     "ExtractEvaluatorDeployDriftError", "EvaluatorDirectorDeployDriftCheck"),
    ("EvaluatorDirectorDeployDriftGate", "$.evaluator_director_deploy_drift_result",
     "ExtractEvaluatorDirectorDeployDriftError", "CheckMutexRole"),
])
def test_gate_halts_only_on_confirmed_drift(states, gate, result_path, extract, default):
    g = states[gate]
    assert g["Type"] == "Choice"
    c = g["Choices"][0]
    guard, comparison = c["And"]
    assert guard == {"Variable": f"{result_path}.Payload.has_drift", "IsPresent": True}
    assert comparison["Variable"] == f"{result_path}.Payload.has_drift"
    assert comparison["BooleanEquals"] is True
    assert c["Next"] == extract
    assert g["Default"] == default


@pytest.mark.parametrize(("extract", "result_path"), [
    ("ExtractEvaluatorDeployDriftError", "$.evaluator_deploy_drift_result"),
    ("ExtractEvaluatorDirectorDeployDriftError", "$.evaluator_director_deploy_drift_result"),
])
def test_drift_halt_normalizes_error_before_handle_failure(states, extract, result_path):
    norm = states[extract]
    assert norm["Type"] == "Pass"
    assert norm["ResultPath"] == "$.error"
    assert norm["Next"] == "NormalizeFailureContext"
    assert norm["Parameters"]["drift.$"] == f"{result_path}.Payload"


def test_gate_runs_before_any_spot_launch(sf, states):
    """Walk the happy path from StartAt and assert both new gates are reached
    BEFORE the first ssm:sendCommand spot launch."""
    seen_grading_gate = False
    seen_director_gate = False
    cur = sf["StartAt"]
    for _ in range(45):
        st = states[cur]
        res = st.get("Resource", "")
        if "ssm:sendCommand" in res:
            assert seen_grading_gate and seen_director_gate, (
                f"spot launch {cur} reached before the evaluator drift gate pair "
                f"completed — the gate must precede all spot spend"
            )
            return
        if cur == "EvaluatorDeployDriftGate":
            seen_grading_gate = True
        if cur == "EvaluatorDirectorDeployDriftGate":
            seen_director_gate = True
        nxt = st.get("Next") or st.get("Default")
        if nxt is None:
            break
        cur = nxt
    assert seen_grading_gate and seen_director_gate, "walk never reached both evaluator drift gates"


def test_gate_runs_immediately_after_pipeline_contract_gate(sf, states):
    """Walk the happy path and assert the evaluator gate pair is reached right
    after PipelineContractGate (with only the two Check Task states in
    between) — the composed 'preflight the pipeline's invariants' chain."""
    cur = sf["StartAt"]
    seen_contract_gate = False
    seen_grading_check = False
    for _ in range(45):
        st = states[cur]
        if seen_contract_gate and not seen_grading_check:
            assert cur == "EvaluatorDeployDriftCheck", (
                f"expected EvaluatorDeployDriftCheck immediately after "
                f"PipelineContractGate's pass-through, got {cur}"
            )
            assert st["Next"] == "EvaluatorDeployDriftGate"
            seen_grading_check = True
        elif seen_grading_check:
            # after EvaluatorDeployDriftGate's own pass-through
            if cur == "EvaluatorDirectorDeployDriftCheck":
                assert st["Next"] == "EvaluatorDirectorDeployDriftGate"
                return
        if cur == "PipelineContractGate":
            seen_contract_gate = True
        nxt = st.get("Next") or st.get("Default")
        if nxt is None:
            break
        cur = nxt
    assert seen_grading_check, "walk never reached EvaluatorDeployDriftCheck"


def test_does_not_touch_weekday_deploy_drift_gate():
    """config#2348 operator ruling: this is a NEW, INDEPENDENT weekly-SF gate
    — the weekday pipeline's existing DeployDriftCheck/DeployDriftGate
    (predictor Lambda, a different load-bearing trading invariant) must be
    byte-for-byte untouched by this feature."""
    weekday = json.loads((_REPO_ROOT / "infrastructure" / "step_function_daily.json").read_text())
    weekday_states = weekday["States"]
    assert "DeployDriftCheck" in weekday_states
    assert "DeployDriftGate" in weekday_states
    chk = weekday_states["DeployDriftCheck"]
    assert chk["Parameters"]["FunctionName"] == "alpha-engine-predictor-inference:live"
    # No evaluator-named states leaked into the weekday SF.
    assert not any("Evaluator" in name for name in weekday_states)
