"""A degraded weekly cycle is swept before it terminates DegradedRun.

``alpha-engine-config-I11111``. ``WeeklyCoverageSweep`` had one inbound edge,
``WriteCompletionMarkerCalendar``, so a cycle that took the degraded marker
path went straight to ``DegradedRun`` and wrote no
``_stage_coverage/_sweep/ne-weekly-freshness-pipeline/<run_date>.json``. Its own
Comment said such a cycle was "swept out-of-band instead"; nothing did that.
Measured: no 2026-09-18 or 2026-10-09 artifact, and
``alpha-engine-stage-coverage-sweep-dead`` went ALARM on 2026-10-13 after seven
daily periods with no ``StageCoverageSweepRan`` point.

These tests pin the constraints the issue set for the fix:

* the sweep is reachable from BOTH completion-marker paths;
* it cannot change the degraded run's terminal - ``DegradedRun`` stays a Fail
  state, so sf-telegram-notifier's ``Error: DegradedRun`` contract
  (``alpha-engine-config-I6891``) holds;
* the degraded branch cannot reach the clean marker, ``VacuousRun`` or any
  success terminal;
* the completion marker is written BEFORE the sweep, so it never depends on it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.sf_run_slot_release_helpers import through_release

REPO = Path(__file__).resolve().parents[1]
DEFINITION = REPO / "infrastructure" / "step_function.json"

DEGRADED_SWEEP = "WeeklyCoverageSweepDegraded"
CLEAN_SWEEP = "WeeklyCoverageSweep"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(DEFINITION.read_text())["States"]


def _exits(state: dict) -> set[str]:
    """Every state a Task can hand control to: Next plus each Catch."""
    out = {state["Next"]} if "Next" in state else set()
    out |= {c["Next"] for c in state.get("Catch", [])}
    return out


def test_the_sweep_is_reachable_from_both_completion_marker_paths(states: dict) -> None:
    assert states["WriteCompletionMarkerCalendar"]["Next"] == CLEAN_SWEEP
    assert _exits(states["WriteCompletionMarkerDegradedCalendar"]) == {DEGRADED_SWEEP}


def test_both_sweep_states_call_the_same_lambda_with_the_same_payload(states: dict) -> None:
    clean = states[CLEAN_SWEEP]["Parameters"]
    degraded = states[DEGRADED_SWEEP]["Parameters"]
    assert degraded == clean, (
        "the degraded call must send exactly what the clean call sends: the "
        "Lambda is unchanged, so a different payload would be an untested one"
    )


def test_the_degraded_sweep_is_fail_soft_into_the_run_slot_gate(states: dict) -> None:
    """Next AND every Catch land on one gate, so no sweep outcome - including a
    thrown error - can pick a different terminal for the run."""
    state = states[DEGRADED_SWEEP]
    assert state["Type"] == "Task"
    assert state["Next"] == "CheckRunSlotHeldBeforeDegradedRun"
    assert state.get("Catch"), "an un-caught sweep error would fail the run's own terminal"
    assert any(c["ErrorEquals"] == ["States.ALL"] for c in state["Catch"])
    assert _exits(state) == {"CheckRunSlotHeldBeforeDegradedRun"}
    # The result is stored beside the input and nothing downstream reads it.
    assert state["ResultPath"] == "$.degraded_coverage_sweep"
    assert all(c["ResultPath"] not in ("$", None) for c in state["Catch"])


def test_the_degraded_run_still_terminates_degraded_run(states: dict) -> None:
    assert through_release(states, states[DEGRADED_SWEEP]["Next"]) == "DegradedRun"
    for c in states[DEGRADED_SWEEP]["Catch"]:
        assert through_release(states, c["Next"]) == "DegradedRun"
    assert states["DegradedRun"]["Type"] == "Fail"
    assert states["DegradedRun"]["Error"] == "DegradedRun"


def test_the_marker_is_written_before_the_sweep_on_the_degraded_path(states: dict) -> None:
    assert states["WriteCompletionMarkerDegraded"]["Next"] == "WriteCompletionMarkerDegradedCalendar"
    assert states["WriteCompletionMarkerDegradedCalendar"]["Next"] == DEGRADED_SWEEP


def _reachable(states: dict, start: str) -> set[str]:
    seen: set[str] = set()
    todo = [start]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)

        def walk(o) -> None:
            if isinstance(o, dict):
                for k, v in o.items():
                    if k in ("Next", "Default") and isinstance(v, str):
                        todo.append(v)
                    else:
                        walk(v)
            elif isinstance(o, list):
                for x in o:
                    walk(x)

        walk(states[name])
    return seen


def test_the_degraded_branch_cannot_reach_a_clean_or_vacuous_terminal(states: dict) -> None:
    """The sweep must not convert a degraded run into a clean one. Walk
    everything reachable from it: the only terminal is DegradedRun, and neither
    the clean marker, the clean sweep tail nor VacuousRun is on the path."""
    reachable = _reachable(states, DEGRADED_SWEEP)
    terminals = {n for n in reachable if states[n]["Type"] in ("Succeed", "Fail")}
    assert terminals == {"DegradedRun"}
    forbidden = {
        "WriteCompletionMarker",
        "WriteCompletionMarkerCalendar",
        CLEAN_SWEEP,
        "CheckExecutionDidWork",
        "CheckWeeklyCoverageSweepOutcome",
        "VacuousRun",
    }
    assert not (reachable & forbidden), sorted(reachable & forbidden)


def test_the_clean_tail_does_not_reach_the_degraded_sweep(states: dict) -> None:
    assert DEGRADED_SWEEP not in _reachable(states, CLEAN_SWEEP)
