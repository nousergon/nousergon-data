"""The VacuousRun terminal (alpha-engine-config-I9693).

An execution that entered none of the pipeline's declared spine stages must
not report ``SUCCEEDED``. Measured live 2026-09-18: every scheduled weekly run
from 2026-08-15 through 2026-09-12 FAILED — for five DIFFERENT causes — and
every one was "recovered" by an operator rerun with every producer skipped,
which reported ``SUCCEEDED``. ``watch-rerun-2026-09-11-1`` set 26 ``skip_*``
flags, entered ZERO of the 16 declared spine stages, and is indistinguishable
from a four-hour full run in ``list-executions``.

What is pinned here is the wiring the Lambda cannot pin for itself: that the
Choice exists, that it fires ONLY on an explicit boolean false (an unknown is
never manufactured into a red), that its terminal is a ``Fail`` carrying the
``Error`` literal the notifier keys off, and — the property that makes the fix
safe — that neither of the pipeline's two DESIGNED no-ops can reach it.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_SF = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"

#: The flag the Choice reads, as one string, so a rename cannot pass silently.
_VARIABLE = "$.coverage_sweep.payload.observer_did_work"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF.read_text())["States"]


def _successors(state: dict) -> set[str]:
    out: set[str] = set()
    if "Next" in state:
        out.add(state["Next"])
    for choice in state.get("Choices", []):
        out.add(choice["Next"])
    if "Default" in state:
        out.add(state["Default"])
    for catch in state.get("Catch", []):
        out.add(catch["Next"])
    for retry_free in state.get("Branches", []):
        for name, sub in retry_free.get("States", {}).items():
            out |= _successors(sub)
    return out


def _reachable(states: dict, start: str) -> set[str]:
    seen: set[str] = set()
    stack = [start]
    while stack:
        name = stack.pop()
        if name in seen or name not in states:
            continue
        seen.add(name)
        stack.extend(_successors(states[name]))
    return seen


def test_the_coverage_sweep_hands_off_to_the_did_work_choice(states):
    assert states["WeeklyCoverageSweep"]["Next"] == "CheckExecutionDidWork"


def test_the_choice_fires_only_on_an_explicit_boolean_false(states):
    """An absent, null or non-boolean value must take the Default.

    This is the property that keeps the fix from being a widened FAILURE
    condition. A sweep that could not read the cycle reports
    ``outcome=unavailable`` and pages through ``WeeklyCoverageSweepUnavailable``
    — that is the surface for an unmeasured run. Turning it red here would
    manufacture a verdict out of a denial.
    """
    choice = states["CheckExecutionDidWork"]
    assert choice["Type"] == "Choice"
    assert choice["Default"] == "CheckWeeklyCoverageSweepOutcome"
    assert len(choice["Choices"]) == 1
    rule = choice["Choices"][0]
    assert rule["Next"] == "VacuousRun"
    conjuncts = rule["And"]
    assert {c["Variable"] for c in conjuncts} == {_VARIABLE}
    assert any(c.get("IsPresent") is True for c in conjuncts)
    assert any(c.get("IsBoolean") is True for c in conjuncts)
    assert any(c.get("BooleanEquals") is False for c in conjuncts)


def test_the_terminal_is_a_fail_carrying_the_error_the_notifier_keys_off(states):
    """``Error`` is a consumer contract, exactly as ``DegradedRun``'s is.

    ``sf-telegram-notifier``'s ``_is_vacuous_run()`` matches this literal, so a
    run that did no work renders as "NO WORK DONE" rather than as a crash.
    """
    fail = states["VacuousRun"]
    assert fail["Type"] == "Fail"
    assert fail["Error"] == "VacuousRun"
    assert "alpha-engine-config-I9693" in fail["CausePath"]


def test_the_cause_names_the_run_date_and_the_evidence(states):
    """A terminal whose cause cannot be reconstructed later is a page with no
    content (``principles.md`` §2.1). The cause carries the cycle key and the
    sweep's own explanation, not just a label."""
    cause = states["VacuousRun"]["CausePath"]
    assert "$.run_date" in cause
    assert "$.coverage_sweep.payload.observer" in cause


def test_the_thursday_friday_gate_out_cannot_reach_it(states):
    """``WeeklyRunDaySkip`` is a DESIGNED no-op — the weekly SF fires THU-SAT
    and self-selects its run day. Turning that green into a red would page a
    human through working-as-designed behaviour twice a week, which is the
    failure mode ``nousergon_lib.pipeline_status.work`` was written to avoid.
    """
    assert states["WeeklyRunDaySkip"]["Type"] == "Succeed"
    assert "VacuousRun" not in _reachable(states, "WeeklyRunDaySkip")


def test_the_friday_pm_shell_run_cannot_reach_it(states):
    """The shell run DRY-executes the graph on purpose and writes no completion
    marker. ``CheckShellRunNotify`` is where its path diverges from the real
    one: with ``shell_run: true`` it takes ``NotifyShellRunComplete``, whose
    whole downstream closure is ``{ShellRunComplete, DegradedRun}`` — it never
    enters ``WriteCompletionMarker`` (that state's own comment records the
    exclusion as deliberate: "the Friday-PM preflight dry-pass is not a real
    weekly completion; marking it would let a dry run silently satisfy the
    SLA"). So the discriminator, which lives past the marker, cannot turn a
    designed dry run red.

    Asserted on the flag-selected branch rather than on a static walk from
    ``ApplyShellRunDefaults``: a static walk follows both arms of every Choice
    and would report the real path as shell-run-reachable, which is true of the
    graph and false of any shell run.
    """
    notify = states["CheckShellRunNotify"]
    shell_branch = [
        rule["Next"] for rule in notify["Choices"]
        if any(c.get("Variable") == "$.shell_run" for c in rule.get("And", []))
    ]
    assert shell_branch == ["NotifyShellRunComplete"]
    assert notify["Default"] == "CheckGateDegradedNotify"

    reachable = _reachable(states, "NotifyShellRunComplete")
    assert "ShellRunComplete" in reachable
    assert "WriteCompletionMarker" not in reachable
    assert "CheckExecutionDidWork" not in reachable
    assert "VacuousRun" not in reachable


def test_the_marker_and_the_sweep_still_run_before_the_terminal(states):
    """``alpha-engine-config-I8186`` explicitly forbids fixing this by making
    ``WriteCompletionMarker`` unreachable from recovery reruns: a genuine
    recovery that DOES complete the spine must still write it. So the
    discriminator sits AFTER the marker and after the sweep has augmented it
    with the cycle's real shape — a vacuous execution still leaves a complete,
    honest artifact behind; only its own status changes."""
    from_marker = _reachable(states, "WriteCompletionMarker")
    assert "WeeklyCoverageSweep" in from_marker
    assert "VacuousRun" in from_marker


def test_the_coverage_sweep_outcome_branches_still_route_the_same_four_ways(states):
    """The sweep's four outcomes report on the SWEEP, not on the run. This fix
    adds a terminal beside them and must not collapse any of them.

    Their dereference of ``$.coverage_sweep.outcome`` gained an ``IsPresent``
    guard in the same change: the flooring proof that made it safe un-guarded
    named ``WeeklyCoverageSweep`` as the sole predecessor, and the predecessor
    is now this fix's Choice. Behaviour is unchanged — an absent outcome still
    lands on the ``unavailable`` Default — only the proof moved from inherited
    to local (``tests/test_sf_choice_guards.py`` is the gate that caught it).
    """
    choice = states["CheckWeeklyCoverageSweepOutcome"]
    assert {c["Next"] for c in choice["Choices"]} == {
        "WeeklyCoverageSweepClean",
        "WeeklyCoverageSweepFindings",
        "WeeklyCoverageSweepDeferred",
    }
    assert choice["Default"] == "WeeklyCoverageSweepUnavailable"
    for rule in choice["Choices"]:
        assert any(c.get("IsPresent") is True for c in rule["And"])
