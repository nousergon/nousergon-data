"""Pins the WeeklyPreflight pre-spend gate in the Saturday SF (I4494).

The gate (`WeeklyPreflight` → `WeeklyPreflightGate`) MUST run before any
spot launch (`AcquireMutex` / `sendCommand` states) and hard-fail the SF
on a failing preflight check, while routing through ExtractWeeklyPreflightError
→ NormalizeFailureContext (the same chokepoint all pre-spend gate failures use).

These tests catch regressions like: someone reorders it after AcquireMutex
(defeating "fail before spend"), drops the fail-closed Catch (preflight that
cannot check silently proceeds), or changes the fail path to jump directly to
HandleFailure (which would die with States.Runtime extracting $.error from a
Choice transition that doesn't populate it).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"
_LAMBDA_NAME = "alpha-engine-weekly-preflight:live"


@pytest.fixture(scope="module")
def sf():
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf):
    return sf["States"]


def test_preflight_state_exists(states):
    assert "WeeklyPreflight" in states, "WeeklyPreflight state missing from Saturday SF"


def test_preflight_gate_state_exists(states):
    assert "WeeklyPreflightGate" in states, "WeeklyPreflightGate state missing from Saturday SF"


def test_extract_error_state_exists(states):
    assert "ExtractWeeklyPreflightError" in states, "ExtractWeeklyPreflightError state missing from Saturday SF"


def test_preflight_positioned_before_mutex_and_spot(states):
    """WeeklyPreflight MUST run before AcquireMutex and any sendCommand.

    Walk the transition graph from WeeklyPreflight; it must reach CheckMutexRole
    (the Choice that gates the mutex) BEFORE AcquireMutex.
    """
    preflight = states["WeeklyPreflight"]
    assert preflight["Next"] == "WeeklyPreflightGate", (
        "WeeklyPreflight must transition to WeeklyPreflightGate"
    )

    gate = states["WeeklyPreflightGate"]
    assert gate["Type"] == "Choice"
    assert gate["Default"] == "CheckMutexRole", (
        "WeeklyPreflightGate default (pass) must go to CheckMutexRole, "
        f"got {gate['Default']}"
    )


def test_preflight_routes_failure_via_error_normalizer(states):
    """A violating preflight must route through ExtractWeeklyPreflightError,
    NOT directly to HandleFailure, to populate $.error first."""
    gate = states["WeeklyPreflightGate"]
    for choice in gate["Choices"]:
        if choice.get("Next") == "ExtractWeeklyPreflightError":
            break
    else:
        pytest.fail("No WeeklyPreflightGate choice routes to ExtractWeeklyPreflightError")

    error_norm = states["ExtractWeeklyPreflightError"]
    assert error_norm["Type"] == "Pass"
    assert "phase" in error_norm["Parameters"], (
        "ExtractWeeklyPreflightError must carry a 'phase' parameter"
    )
    assert error_norm["Next"] == "NormalizeFailureContext", (
        "ExtractWeeklyPreflightError must route to NormalizeFailureContext, "
        f"got {error_norm['Next']}"
    )


def test_preflight_invokes_correct_lambda(states):
    chk = states["WeeklyPreflight"]
    assert chk["Type"] == "Task"
    assert chk["Resource"] == "arn:aws:states:::lambda:invoke"
    assert chk["Parameters"]["FunctionName"] == _LAMBDA_NAME
    assert chk["ResultPath"] == "$.weekly_preflight_result"
    assert chk["Next"] == "WeeklyPreflightGate"


def test_preflight_fails_closed_on_lambda_error(states):
    """A preflight that cannot check (Lambda crash) must NOT silently proceed.
    Contrast the advisory gates (LibPinDriftCheck/PipelineContractCheck) which
    fail-open — the preflight is the last gate before spend and its whole purpose
    is to stop before spending."""
    catch = states["WeeklyPreflight"]["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "ExtractWeeklyPreflightError", (
        "Lambda error must route to ExtractWeeklyPreflightError, "
        f"got {catch['Next']}"
    )


def test_preflight_gate_has_fail_closed_malformed_check(states):
    """A preflight that returns a non-violating but malformed payload
    (missing has_violation) must also halt, not silently proceed."""
    gate = states["WeeklyPreflightGate"]
    for choice in gate["Choices"]:
        variables = {c.get("Variable") for c in choice.get("And", [])}
        if "$.weekly_preflight_result.Payload.has_violation" in variables:
            continue  # this is the has_violation=true check
        # Check for the malformed-payload guard
        not_clause = choice.get("Not", {})
        if not_clause.get("Variable") == "$.weekly_preflight_result.Payload.has_violation":
            assert choice["Next"] == "ExtractWeeklyPreflightError", (
                "Malformed preflight payload (missing has_violation) must route "
                "to ExtractWeeklyPreflightError, not proceed"
            )
            break
    else:
        pytest.fail("No malformed-payload guard found in WeeklyPreflightGate")


def test_preflight_state_precedes_every_send_command(states):
    """Walk the state graph from the start. Every path to a sendCommand state
    must pass through WeeklyPreflight first."""
    # Build a simple reachability graph: for each state, what states can
    # follow it?
    transitions: dict[str, list[str]] = {}
    for name, state in states.items():
        transitions[name] = []
        if "Next" in state:
            transitions[name].append(state["Next"])
        if "Default" in state:
            transitions[name].append(state["Default"])
        if "Choices" in state:
            for choice in state["Choices"]:
                transitions[name].append(choice["Next"])
        for branch in ("Branches",):
            if branch in state:
                for b in state[branch]:
                    if "StartAt" in b:
                        transitions[name].append(b["StartAt"])
        if "Catch" in state:
            for c in state["Catch"]:
                if "Next" in c:
                    transitions[name].append(c["Next"])

    # Collect all sendCommand resources
    send_command_states = [
        name for name, state in states.items()
        if state.get("Resource", "").endswith(":sendCommand")
    ]

    if not send_command_states:
        pytest.skip("No sendCommand states found in SF")

    # BFS from InitializeInput — does every path to a sendCommand state
    # go through WeeklyPreflight?
    def _paths_through_preflight(
        current: str, visited: set[str], found_violation: bool
    ) -> bool:
        """Returns True if the current path reaches a sendCommand state
        WITHOUT passing through WeeklyPreflight (a violation)."""
        if current in visited:
            return False  # cycle, not a violation
        if current == "WeeklyPreflight":
            found_violation = True
        if current in send_command_states:
            return not found_violation  # violation = reached sendCommand without preflight

        visited = visited | {current}
        for next_state in transitions.get(current, []):
            if next_state == current:
                continue
            if _paths_through_preflight(next_state, visited, found_violation):
                return True
        return False

    violations = []
    for sc in send_command_states:
        if _paths_through_preflight("InitializeInput", set(), False):
            violations.append(sc)

    assert not violations, (
        f"sendCommand state(s) reachable without passing through WeeklyPreflight: "
        f"{violations}"
    )


def test_pipeline_contract_gate_defers_to_preflight(states):
    """PipelineContractGate's pass path must eventually reach WeeklyPreflight
    before any spend, not jump directly to CheckMutexRole. It does not need
    to go there directly: main's EvaluatorDeployDriftCheck/EvaluatorDirector
    pre-spend gates (config#2348) were added after this test was first
    written and now sit between PipelineContractGate and WeeklyPreflight,
    composed per the sibling-gate convention — so the immediate next hop is
    EvaluatorDeployDriftCheck, with WeeklyPreflight still guaranteed downstream
    (asserted generically by test_preflight_state_precedes_every_send_command)."""
    gate = states["PipelineContractGate"]
    assert gate["Default"] == "EvaluatorDeployDriftCheck", (
        "PipelineContractGate default must go to EvaluatorDeployDriftCheck "
        "(the next pre-spend gate in the composed chain), "
        f"got {gate['Default']}"
    )


def test_pipeline_contract_degraded_defers_to_preflight(states):
    """The degraded path from PipelineContractGate must also route into the
    composed pre-spend gate chain, not bypass it straight to CheckMutexRole.
    See test_pipeline_contract_gate_defers_to_preflight for why the immediate
    hop is EvaluatorDeployDriftCheck rather than WeeklyPreflight directly."""
    degraded = states["PublishPipelineContractGateDegraded"]
    next_states = []
    if "Next" in degraded:
        next_states.append(degraded["Next"])
    if "Catch" in degraded:
        for c in degraded["Catch"]:
            if "Next" in c:
                next_states.append(c["Next"])

    for n in next_states:
        assert n == "EvaluatorDeployDriftCheck", (
            f"PublishPipelineContractGateDegraded must route to EvaluatorDeployDriftCheck, "
            f"got {n} (one or more paths bypass the composed pre-spend chain)"
        )
