"""Pins the preventive lib-pin drift gate in the Saturday SF (L4517).

The gate (`LibPinDriftCheck` → `LibPinDriftGate`) MUST run before any spot
launch and hard-fail the SF on a CONFIRMED cross-repo `alpha-engine-lib` pin
drift (backtester != predictor co-install parity, or a below-floor pin),
while failing OPEN on the probe's own error. These tests catch regressions
like: someone reorders it after a spot launch (defeating "fail before spend"),
drops the fail-open Catch (probe fragility false-halts the weekly run), or
inverts the gate's halt condition.

Pairs with alpha-engine-predictor `inference/lib_pin_drift.py` (the
`action=check_lib_pin_drift` Lambda handler).
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
    "name", ["CheckSkipLibPinDriftCheck", "LibPinDriftCheck", "LibPinDriftGate"]
)
def test_gate_states_exist(states, name):
    assert name in states, f"{name} missing from Saturday SF States"


def test_runs_first_off_initialize_input(sf, states):
    # The gate is the first WORKLOAD gate after InitializeInput. config#830
    # inserted a cadence-preset gate (CheckRunMode) between InitializeInput and
    # this gate; CheckRunMode.Default → CheckSkipLibPinDriftCheck, so the lib-pin
    # gate still runs first for any non-preset input.
    assert sf["StartAt"] == "InitializeInput"
    assert states["InitializeInput"]["Next"] == "CheckWeeklyRunDayGate"
    # config#1824: run-day gate precedes CheckRunMode; bypass Default keeps chain.
    assert states["CheckWeeklyRunDayGate"]["Default"] == "CheckRunMode"
    assert states["CheckRunMode"]["Default"] == "CheckSkipLibPinDriftCheck"


def test_skip_gate_default_runs_check_and_skip_bypasses(states):
    skip = states["CheckSkipLibPinDriftCheck"]
    assert skip["Default"] == "LibPinDriftCheck"
    c = skip["Choices"][0]
    # skip_lib_pin_drift_check == true bypasses straight into the pipeline
    assert c["Next"] == "CheckMutexRole"
    variables = {x["Variable"] for x in c["And"]}
    assert variables == {"$.skip_lib_pin_drift_check"}


def test_check_invokes_predictor_lambda_with_action(states):
    chk = states["LibPinDriftCheck"]
    assert chk["Type"] == "Task"
    assert chk["Resource"] == "arn:aws:states:::lambda:invoke"
    assert chk["Parameters"]["FunctionName"] == "alpha-engine-predictor-inference:live"
    assert chk["Parameters"]["Payload"]["action"] == "check_lib_pin_drift"
    assert chk["ResultPath"] == "$.libpin_drift_result"
    assert chk["Next"] == "LibPinDriftGate"


def test_check_fails_open_via_catch(states):
    # The probe's own failure (incl. an unknown action pre-PR-A-deploy) must
    # proceed into the pipeline, NOT halt the weekly run.
    catch = states["LibPinDriftCheck"]["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "CheckMutexRole"  # the pipeline, not HandleFailure


def test_gate_halts_only_on_confirmed_drift(states):
    gate = states["LibPinDriftGate"]
    assert gate["Type"] == "Choice"
    c = gate["Choices"][0]
    assert c["Variable"] == "$.libpin_drift_result.Payload.has_drift"
    assert c["BooleanEquals"] is True
    # Confirmed drift halts, but routes through the $.error normalizer FIRST
    # (not straight to HandleFailure) — see test_drift_halt_normalizes_error.
    assert c["Next"] == "ExtractLibPinDriftError"
    # No drift → proceed into the pipeline.
    assert gate["Default"] == "CheckMutexRole"


def test_drift_halt_normalizes_error_before_handle_failure(states):
    """The gate is a Choice, so — unlike a Task Catch (ResultPath $.error) —
    its transition does NOT populate $.error. HandleFailure's Message calls
    States.JsonToString($.error), so a direct Choice→HandleFailure jump killed
    the SF with an opaque States.Runtime ('$.error could not be found') that
    swallowed the drift reason (2026-07-03 offcycle-shell). The drift path must
    first hit an Extract*Error normalizer that writes $.error, matching every
    other HandleFailure entry. config#1819 (2026-07-06/07): HandleFailure is no
    longer reached directly — every path (including this one) now funnels
    through NormalizeFailureContext, the single Catch/Next chokepoint that adds
    a floor-default for $.error/$.sns_topic_arn/$.pipeline_label as
    defense-in-depth on top of this normalizer."""
    norm = states["ExtractLibPinDriftError"]
    assert norm["Type"] == "Pass"
    assert norm["ResultPath"] == "$.error"
    assert norm["Next"] == "NormalizeFailureContext"
    # References only the whole probe Payload — guaranteed present because the
    # gate already dereferenced Payload.has_drift to route here — so the
    # normalizer cannot itself raise a missing-field States.Runtime.
    assert norm["Parameters"]["drift.$"] == "$.libpin_drift_result.Payload"


def test_every_handle_failure_entry_populates_error(states):
    """CHOKEPOINT for the whole failure class (2026-07-03, widened 2026-07-07
    by config#1819): HandleFailure's Message template hard-requires $.error via
    States.JsonToString($.error), so EVERY transition that lands on
    HandleFailure (directly, or via the NormalizeFailureContext chokepoint)
    must guarantee $.error is set somewhere upstream — either a Task Catch
    with ResultPath '$.error', a Pass normalizer with ResultPath '$.error', or
    (config#1819) NormalizeFailureContext itself, which floor-defaults $.error
    via JsonMerge so it is present even if a future Catch forgets its own
    ResultPath. A future soft-path/Choice that jumps straight to HandleFailure
    (bypassing NormalizeFailureContext entirely, as LibPinDriftGate once did)
    would re-introduce the opaque States.Runtime meta-crash; this test fails
    loudly if any such path appears."""
    offenders = []
    for name, st in states.items():
        # Catch-based entries — the ONLY way NormalizeFailureContext (or,
        # pre-fix, HandleFailure) is actually reached in this SF today.
        # Correlate the SPECIFIC Catch clause's own ResultPath — a Task
        # having *some* Catch elsewhere (e.g. a distinct Catch clause
        # routing to a different state) must NOT count as evidence that
        # $.error is set on THIS clause's path.
        for cat in st.get("Catch", []):
            target = cat.get("Next")
            if target in ("HandleFailure", "NormalizeFailureContext") and cat.get("ResultPath") != "$.error":
                offenders.append(f"{name} Catch ResultPath={cat.get('ResultPath')!r} (Next={target})")
        # Direct state-transition entries (Next / Default / Choice) — i.e.
        # NOT via a Catch clause. A Pass normalizer transitioning via its
        # own top-level "Next" (not a Catch) must set $.error itself via
        # ResultPath; any other state type reaching HandleFailure/
        # NormalizeFailureContext this way has no correlated $.error source
        # and is an offender regardless of Type.
        direct_transitions = {st.get("Next"), st.get("Default")}
        direct_transitions |= {c.get("Next") for c in st.get("Choices", [])}
        direct_transitions.discard(None)

        if "HandleFailure" in direct_transitions:
            # Only NormalizeFailureContextPreflightLabel/RealLabel may transition
            # directly to HandleFailure post-fix — both are Pass states that set
            # $.pipeline_label (not $.error, which NormalizeFailureContext already
            # floor-defaulted upstream of them).
            if name not in (
                "NormalizeFailureContextPreflightLabel",
                "NormalizeFailureContextRealLabel",
            ):
                offenders.append(
                    f"{name} (Type={st.get('Type')}) transitions to HandleFailure "
                    f"directly, bypassing the NormalizeFailureContext chokepoint"
                )
        if "NormalizeFailureContext" in direct_transitions:
            # The source state must itself write $.error before handing off
            # via its own top-level Next/Default/Choice (not a Catch, which
            # is checked separately above) — i.e. be a Pass normalizer with
            # ResultPath '$.error'. NormalizeFailureContext's own floor-default
            # is a backstop for Catch-based entries only; a direct (non-Catch)
            # transition bypasses that reasoning entirely and must set
            # $.error itself.
            is_pass_error_normalizer = (
                st.get("Type") == "Pass" and st.get("ResultPath") == "$.error"
            )
            if not is_pass_error_normalizer:
                offenders.append(
                    f"{name} (Type={st.get('Type')}) transitions directly (not "
                    f"via Catch) to NormalizeFailureContext without itself "
                    f"setting $.error (ResultPath={st.get('ResultPath')!r})"
                )
    assert not offenders, (
        "State(s) reach HandleFailure without the guaranteed-$.error chokepoint — "
        "HandleFailure could die with States.Runtime: " + "; ".join(offenders)
    )


def test_gate_runs_before_any_spot_launch(sf, states):
    """Walk the happy path (Next / Choice-Default) from StartAt and assert
    LibPinDriftGate is reached BEFORE the first ssm:sendCommand spot launch —
    the whole point of L4517 is to fail before any spot spend."""
    seen_gate = False
    cur = sf["StartAt"]
    for _ in range(40):  # bounded walk
        st = states[cur]
        res = st.get("Resource", "")
        if "ssm:sendCommand" in res:
            assert seen_gate, (
                f"spot launch {cur} reached before LibPinDriftGate — the gate "
                f"must precede all spot spend"
            )
            return
        if cur == "LibPinDriftGate":
            seen_gate = True
        nxt = st.get("Next") or st.get("Default")
        if nxt is None:
            break
        cur = nxt
    assert seen_gate, "walk never reached LibPinDriftGate"
