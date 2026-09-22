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
    # alpha-engine-config-I11112 deliverable 4: the clean arm records the
    # run's assertion counts on $.weekly_preflight_blind_spot before
    # continuing, so a reader of the completion marker can tell "every
    # declared assertion executed" from "the gate never ran". It is a Pass
    # with no side effects and the SAME onward target.
    clean = gate["Default"]
    assert clean == "WeeklyPreflightFullyObserved", (
        f"WeeklyPreflightGate default (pass) must go to the clean count "
        f"declarer, got {clean}"
    )
    assert states[clean]["Type"] == "Pass"
    assert states[clean]["Next"] == "CheckMutexRole", (
        "the clean count declarer must continue to CheckMutexRole, "
        f"got {states[clean]['Next']}"
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
    # alpha-engine-config#5950: it used to route to ExtractWeeklyPreflightError,
    # which dereferences $.weekly_preflight_result.Payload — a field the SUCCESS
    # path writes. On the Catch path the invoke failed, so it was never written,
    # and every preflight crash died in States.Runtime inside the state meant to
    # report it. ExtractWeeklyPreflightCrash reads $.weekly_preflight_error,
    # which is exactly what this Catch's own ResultPath writes.
    assert catch["Next"] == "ExtractWeeklyPreflightCrash", (
        "Lambda error must route to ExtractWeeklyPreflightCrash, "
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

    # Reachability over (state, has-passed-WeeklyPreflight) pairs, memoised.
    #
    # This walk used to enumerate PATHS — the visited set was carried per
    # branch, so a converging graph re-explored the same subgraph once per
    # distinct route into it. That is exponential in the number of joins, and
    # the weekly definition is nothing but joins: adding one convergence state
    # (alpha-engine-config-I6891's CheckDegradedOutcome, which eight completion
    # paths now pass through) moved this test from slow to effectively
    # non-terminating. It also ran the identical whole-graph search once per
    # sendCommand state and appended the LOOP VARIABLE on success, so the
    # reported violation list named every sendCommand state whenever any one of
    # them was reachable, and named the wrong ones.
    #
    # The question is per-state and has only two carried bits, so the state
    # space is 2*|States| and one pass answers it for every sendCommand at once.
    seen: set[tuple[str, bool]] = set()
    frontier = [("InitializeInput", False)]
    violations: set[str] = set()
    while frontier:
        current, passed_preflight = frontier.pop()
        if (current, passed_preflight) in seen:
            continue
        seen.add((current, passed_preflight))
        if current == "WeeklyPreflight":
            passed_preflight = True
        if current in send_command_states:
            if not passed_preflight:
                violations.add(current)
            # Do not descend past a sendCommand state — the question is how it
            # was REACHED, and expanding it would attribute its successors'
            # reachability to a path that already spent.
            continue
        for next_state in transitions.get(current, []):
            if next_state == current:
                continue
            frontier.append((next_state, passed_preflight))

    assert not violations, (
        f"sendCommand state(s) reachable without passing through WeeklyPreflight: "
        f"{sorted(violations)}"
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


# --------------------------------------------------------------------------
# alpha-engine-config-I11112 — a capability-gap skip is a VERDICT about the
# probe's own environment (mirrors ModelZooUnservableDeclared,
# sf-pipeline-policy.md §2.3a), not a stage degradation, and must terminate
# the run SUCCEEDED. Every OTHER pre-spend gate in this family (LibPin/
# PipelineContract/Evaluator/EvaluatorDirector) sets $.degraded_summary and
# therefore terminates at the DegradedRun Fail state (config-I6891) — correct
# for a rare, transient probe failure, wrong for a persistent, tracked
# capability gap that would otherwise Fail-and-page every Saturday. These
# tests pin the PROPERTY (where the run ends up), mirroring
# test_sf_model_zoo_unservable_wiring.py's _first_path_to walk rather than a
# fresh helper.
# --------------------------------------------------------------------------


def _edges(state: dict) -> list[str]:
    out = []
    if "Next" in state:
        out.append(state["Next"])
    if "Default" in state:
        out.append(state["Default"])
    for rule in state.get("Choices", []) or []:
        if "Next" in rule:
            out.append(rule["Next"])
    for catch in state.get("Catch", []) or []:
        if "Next" in catch:
            out.append(catch["Next"])
    return out


def _first_terminal(states: dict, start: str, max_steps: int = 400) -> tuple[str, list[str]]:
    """DFS from `start` to the first Succeed or Fail state reached, returning
    (terminal_name, path). Fails loudly on an undefined state name or on
    exceeding max_steps, mirroring test_sf_model_zoo_unservable_wiring.py's
    _first_path_to."""
    seen: set[str] = set()
    stack = [[start]]
    while stack:
        path = stack.pop()
        node = path[-1]
        if node in seen or len(path) > max_steps:
            continue
        seen.add(node)
        state = states.get(node)
        assert state is not None, f"path {path} references undefined state {node!r}"
        if state.get("Type") in ("Succeed", "Fail") and node != start:
            return node, path
        for nxt in _edges(state):
            stack.append(path + [nxt])
    raise AssertionError(f"no Succeed/Fail terminal reached from {start!r} within {max_steps} steps")


def test_blind_spot_declared_converges_on_the_clean_continuation(states):
    """A run whose ONLY degradation is WeeklyPreflight's unreachable-checks
    skip must rejoin the SAME continuation as a fully clean preflight
    (WeeklyPreflightGate's Default -> CheckMutexRole) within the blind-spot
    Pass/Publish pair itself, and must never write $.degraded_summary or
    $.gate_degraded along the way — those fields are what route a run to
    the DegradedRun Fail terminal via CheckDegradedOutcome (config-I6891),
    verified for the sibling gates by test_only_degraded_passes_set_gate_degraded.

    Deliberately NOT a full graph walk to whatever terminal DFS finds first:
    downstream Parallel branches (e.g. ResearchPredictorParallel) have their
    OWN unrelated Catch->Fail edges that a naive walk reaches regardless of
    what WeeklyPreflight decided, which would make this test's outcome an
    accident of traversal order rather than a fact about the blind-spot arm.
    The three-state chain below (Declared -> Notice -> CheckMutexRole, and
    Notice's Catch -> CheckMutexRole) is the ENTIRE blind-spot-specific
    routing; once it reaches CheckMutexRole it is indistinguishable from any
    other clean run, which is the property that matters.
    """
    chain = ["WeeklyPreflightBlindSpotFromProbe", "WeeklyPreflightBlindSpotDeclared", "PublishWeeklyPreflightBlindSpotNotice"]
    for name in chain:
        st = states[name]
        assert st.get("ResultPath") != "$.degraded_summary", f"{name} writes $.degraded_summary"
        assert st.get("ResultPath") != "$.gate_degraded", f"{name} writes $.gate_degraded"

    notice = states["PublishWeeklyPreflightBlindSpotNotice"]
    assert notice["Next"] == "CheckMutexRole", (
        "PublishWeeklyPreflightBlindSpotNotice must rejoin the clean-preflight "
        f"continuation at CheckMutexRole, got {notice['Next']}"
    )
    (catch,) = notice["Catch"]
    assert catch["Next"] == "CheckMutexRole", (
        "a best-effort notice failure must still rejoin CheckMutexRole, "
        f"got {catch['Next']}"
    )
    clean = states["WeeklyPreflightGate"]["Default"]
    assert states[clean]["Next"] == "CheckMutexRole", (
        "the clean-preflight path must converge on the SAME target the "
        "blind-spot arm does, or 'converges on the clean continuation' is "
        f"not actually true (clean arm {clean!r} -> {states[clean]['Next']!r})"
    )


def test_blind_spot_declared_does_not_reuse_gate_degraded_family(states):
    """WeeklyPreflightBlindSpotDeclared's own ResultPath must be its
    dedicated field, not the shared $.gate_degraded flag every OTHER
    pre-spend gate uses (that flag's consequence — DegradedRun — does not
    apply to a capability-gap VERDICT)."""
    declared = states["WeeklyPreflightBlindSpotDeclared"]
    assert declared["ResultPath"] == "$.weekly_preflight_blind_spot"
    assert declared["Parameters"]["present"] is True


def test_real_preflight_violation_still_reaches_a_fail_terminal(states):
    """Mirror of the above: a REQUIRED check that RUNS and finds a genuine
    violation (has_violation=true) is unaffected by the blind-spot arm and
    still halts — reaches a Fail terminal, never a Succeed one."""
    terminal, path = _first_terminal(states, "ExtractWeeklyPreflightError")
    assert states[terminal]["Type"] == "Fail", (
        f"a confirmed preflight violation must reach a Fail terminal, "
        f"reached {terminal!r} (Type={states[terminal]['Type']}) via {path}"
    )


def test_weekly_preflight_blind_spot_floored_both_polarities(sf):
    """sf-pipeline-policy.md §2.3a rule 3: the field must be present (and
    false/present:false) on a clean run too, or a consumer cannot tell
    'no blind spot' from 'the declaring state never ran because of a bug'."""
    import re
    merged = sf["States"]["InitializeInput"]["Parameters"]["merged.$"]
    m = re.search(r"StringToJson\('(\{.*?\})'\)", merged)
    assert m, "InitializeInput's innermost defaults blob was not found"
    floor = json.loads(m.group(1))
    assert floor["weekly_preflight_blind_spot"] == {
        "present": False,
        "required_skip_count": 0,
        "required_skip_names": [],
        # I11112 deliverable 4: the three counts are floored at 0 so a run
        # that never reached the gate is not indistinguishable from one that
        # ran every assertion — a clean run overwrites these with the real
        # counts (15 today), and 0 means "no observation", not "all clear".
        "ran_count": 0,
        "skip_count": 0,
        "warn_count": 0,
    }


def test_completion_markers_carry_the_blind_spot_field(states):
    """Both completion-marker writers (clean and degraded twins) must embed
    $.weekly_preflight_blind_spot, mirroring how $.model_zoo_unservable is
    already embedded in all four — a field written only on the bad path
    cannot be distinguished from a producer that broke."""
    for name in (
        "WriteCompletionMarker",
        "WriteCompletionMarkerCalendar",
        "WriteCompletionMarkerDegraded",
        "WriteCompletionMarkerDegradedCalendar",
    ):
        body = states[name]["Parameters"]["Body.$"]
        assert "weekly_preflight_blind_spot" in body, f"{name} does not embed weekly_preflight_blind_spot"
        assert "States.JsonToString($.weekly_preflight_blind_spot)" in body, (
            f"{name} must render weekly_preflight_blind_spot via JsonToString, like model_zoo_unservable"
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


# ── I11112 deliverable 4: the counts are named on BOTH polarities ───────────


def test_assertion_counts_recorded_on_both_polarities(states):
    """The 2026-09-19 run reported `status: OK, warn_count: 0` having run 5
    of 15 assertions. `skip_count: 10` was in the Payload and nothing keyed
    on it. Both arms must now lift ran/skip/warn onto
    $.weekly_preflight_blind_spot, or the question "is the preflight still
    running the checks it used to" is answerable only on a bad week.
    """
    for name in ("WeeklyPreflightFullyObserved", "WeeklyPreflightBlindSpotDeclared"):
        params = states[name]["Parameters"]
        assert states[name]["ResultPath"] == "$.weekly_preflight_blind_spot", name
        for field in ("ran_count", "skip_count", "warn_count"):
            key = f"{field}.$"
            assert key in params, f"{name} does not carry {field}"
            assert params[key] == f"$.weekly_preflight_result.Payload.{field}", (
                f"{name}.{key} must read the probe's own Payload, got "
                f"{params[key]!r}"
            )


def test_clean_arm_declares_absence_rather_than_omitting_the_field(states):
    """present/required_skip_count/required_skip_names must be stated on the
    clean arm too (sf-pipeline-policy.md §2.3a rule 3), not left to the
    floor — the floor also covers "the gate never ran"."""
    params = states["WeeklyPreflightFullyObserved"]["Parameters"]
    assert params["present"] is False
    assert params["required_skip_count"] == 0
    assert params["required_skip_names"] == []
