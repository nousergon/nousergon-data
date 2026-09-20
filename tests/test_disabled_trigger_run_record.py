"""A unit whose own descriptor declares its trigger DISABLED is not graded
UNMET for leaving no manifest.

`alpha-engine-config-I11194`. `unit_cadence` did
``str(trigger.get("schedule")).split(" — ")[0]`` — **it truncated the schedule
at exactly the word that says the trigger is off.** Two descriptors record
their live state there:

* D33 ``"cron(0 9 ? * MON-FRI *) — DISABLED live"``
* D38 ``"rate(15 minutes) — DISABLED live"``

Both verified DISABLED on 2026-09-20 against the live `alpha-engine-daily-heal`
EventBridge rule and the `alpha-engine-crypto-balances-15min` Scheduler entry.

The parser computed a due instant from a trigger that does not fire, found no
manifest, and reported *"D33 either did not execute or executed without
recording itself"*. It executed **neither** way: it was not asked to. The
register then read both units as missing instrumentation and routed them to the
emitter work of `alpha-engine-config-I10810`, where building an emitter would
have produced nothing and left the clause red.

**What these tests protect, in both directions.** A disabled trigger must not
grade as a finding, and it must not grade as health either — "switched off" is
neither. The moment the annotation is removed the unit must return to ordinary
grading, or this becomes a permanent excuse.
"""

from __future__ import annotations

import pytest

from data_gate.cadence import unit_cadence
from data_gate.clauses import (
    base_clause_name,
    generate,
    is_disabled_trigger,
    is_ungraded,
)
from data_gate.descriptors import load_units
from data_gate.read import evaluate, load_phases

from tests.data_gate_support import EmptyStore, TRADING_DAY


def _units():
    return load_units()


def _clauses():
    return generate(EmptyStore(), _units(), load_phases(), trading_day=TRADING_DAY)


def _disabled_unit_ids():
    return [u.unit_id for u in _units() if unit_cadence(u.raw).kind == "disabled"]


def test_the_annotation_is_actually_present_on_some_descriptor():
    """Non-vacuity guard. If every DISABLED annotation were removed, the
    assertions below would iterate nothing and report green over an empty set —
    the unsatisfiable-predicate shape this whole class of issue is about."""
    assert _disabled_unit_ids(), (
        "no descriptor declares a DISABLED trigger annotation; these tests would pass vacuously"
    )


def test_a_disabled_trigger_run_record_is_its_own_state_not_unmet():
    clauses = {c.name: c for c in _clauses()}
    for unit_id in _disabled_unit_ids():
        clause = clauses[base_clause_name(unit_id, "run_record")]
        assert is_disabled_trigger(clause), (
            f"{unit_id} declares a DISABLED trigger but its run_record clause is an ordinary "
            "one, so it grades UNMET for not running a trigger that is switched off"
        )
        assert is_ungraded(clause), f"{unit_id}: a DISABLED run_record must be graded by no gate"
        assert clause.met is False, (
            f"{unit_id}: a disabled trigger must never read MET — a unit that is off is not "
            "healthy, it is off"
        )


def test_no_gate_counts_a_disabled_trigger_clause():
    """The property stated against the gate, not the clause.

    A clause excluded from `is_ungraded` but still selected by `evaluate` would
    look right in isolation and still redden the gate.
    """
    all_clauses = _clauses()
    disabled = {
        base_clause_name(u, "run_record") for u in _disabled_unit_ids()
    }
    for gate in ("data-phase1", "data-phase2", "data-phase3"):
        result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=all_clauses)
        graded = {c.name for c in result.clauses}
        overlap = graded & disabled
        assert not overlap, f"{gate} grades a declared-disabled run_record clause: {sorted(overlap)}"


def test_an_enabled_unit_is_still_graded_normally():
    """The other direction. D19's trigger carries no DISABLED annotation, so a
    missing manifest is still a finding — otherwise this fix would be a blanket
    excuse rather than a classification."""
    clauses = {c.name: c for c in _clauses()}
    clause = clauses[base_clause_name("D19", "run_record")]
    assert not is_disabled_trigger(clause)
    assert not is_ungraded(clause)


@pytest.mark.parametrize(
    "schedule,expected",
    [
        ("cron(0 9 ? * MON-FRI *) — DISABLED live", "disabled"),
        ("rate(15 minutes) — DISABLED live", "disabled"),
        ("cron(0 9 ? * MON-FRI *) — disabled until cutover", "disabled"),
        ("cron(0 9 ? * MON-FRI *)", "scheduled"),
        ("weekdays 16:00 America/New_York", "scheduled"),
    ],
)
def test_only_the_annotation_marks_a_trigger_disabled(schedule, expected):
    """The word is matched in the ANNOTATION, after the em dash — never in the
    schedule expression itself, so a schedule that merely contains the string
    cannot silently disable a live unit."""
    cadence = unit_cadence({"trigger": {"kind": "eventbridge-rule", "schedule": schedule}})
    assert cadence.kind == expected, f"{schedule!r} -> {cadence.kind}"
