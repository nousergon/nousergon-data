"""A unit's ``trigger.owner`` is the machine the DEPLOYED stack runs it in.

`alpha-engine-config-I11832` (audit gap A1, weekly due-fire evidence). After the
decoupled data cutover (`alpha-engine-config-I11269`) the descriptors kept naming
the v1 machines. Each relabel was pinned by a test that carries its OWN literal
(`test_eod_units_declare_the_measured_trigger_time.EOD_OWNER`,
`test_trigger_owner_is_not_a_v1_state`'s weekly list), so a descriptor and its
pin could agree with each other and both be wrong about the stack: D03 sat in the
EOD machine's ``verify_units`` and the weekly one's ``schedule`` text for two
weeks while naming a v1 state, and nothing compared it with the template.

This test derives the answer from the deployed definition instead of remembering
it. `infrastructure/cloudformation/nousergon-data-collection.yaml` declares, per
Scheduler entry, the state machine it starts, the cron that starts it, and the
``verify_units`` that machine is graded on. For every in-service unit that names
one of the stack's machines as ``trigger.owner``:

* the machine exists in the template;
* ``trigger.started_by`` names a Scheduler entry that targets THAT machine
  (`data_gate/trigger_reconcile.py::owner_key` reconciles against it);
* the unit is in that entry's ``verify_units`` (the machine's own claim that it
  runs the unit); and
* ``trigger.schedule`` is the entry's cron, in the descriptor's own notation.

And the converse, so a relabel cannot be skipped: every in-service unit a
Scheduler entry verifies is owned by one of the machines that verify it. D17 is
the legitimate two-grader case (morning and weekly both verify it).

What this cannot do: tell whether the live AWS entry still matches the template
(`data_collection_stack.live_findings` and `data.phase1.triggers_reconciled` do).
"""

from __future__ import annotations

import copy
import re

import pytest

from data_gate.descriptors import Unit, load_units
from infrastructure import data_collection_stack as stack

_CRON = re.compile(r"^cron\((\d+) (\d+) \? \* (\S+) \*\)$")
_DOW = {"SAT": "Sat", "MON-FRI": "weekdays"}


@pytest.fixture(scope="module")
def deployed() -> dict:
    """Machine name -> the Scheduler entries (flattened) that start it."""
    tpl = stack.load_template()
    names = {
        logical: res["Properties"]["StateMachineName"]
        for logical, res in stack.resources_of_type(tpl, "AWS::StepFunctions::StateMachine").items()
    }
    by_machine: dict[str, list[dict]] = {}
    for sched in stack.schedules(tpl):
        if sched["target_kind"] != "state_machine":
            continue
        by_machine.setdefault(names[sched["target_ref"]], []).append(sched)
    return by_machine


def _declared_schedule(sched: dict) -> str:
    match = _CRON.match(sched["expression"])
    assert match, f"{sched['name']}: unrecognised cron {sched['expression']!r}"
    minute, hour, dow = match.groups()
    return f"{_DOW[dow]} {int(hour):02d}:{int(minute):02d} {sched['timezone']}"


def owner_problems(units: list[Unit], deployed: dict) -> list[str]:
    problems = []
    verifying: dict[str, set[str]] = {}
    for machine, scheds in deployed.items():
        for sched in scheds:
            for unit_id in sched["input"].get("verify_units", []):
                verifying.setdefault(unit_id, set()).add(machine)

    for unit in units:
        if unit.retired:
            continue
        trigger = unit.raw.get("trigger") or {}
        owner = str(trigger.get("owner") or "").strip()
        in_stack = owner in deployed
        if owner.startswith("ne-data-collection-") and not in_stack:
            problems.append(f"{unit.unit_id}: owner {owner!r} is not a state machine in the template")
            continue
        if in_stack:
            entries = {f"eventbridge-scheduler:{s['qualified_name']}": s for s in deployed[owner]}
            sched = entries.get(trigger.get("started_by"))
            if sched is None:
                problems.append(
                    f"{unit.unit_id}: started_by {trigger.get('started_by')!r} is not a Scheduler "
                    f"entry targeting {owner!r} (have {sorted(entries)})"
                )
                continue
            if unit.unit_id not in sched["input"].get("verify_units", []):
                problems.append(
                    f"{unit.unit_id}: owner {owner!r} does not verify it "
                    f"(not in {sched['name']}'s verify_units)"
                )
            if trigger.get("schedule") != _declared_schedule(sched):
                problems.append(
                    f"{unit.unit_id}: schedule {trigger.get('schedule')!r} is not "
                    f"{sched['name']}'s {_declared_schedule(sched)!r}"
                )
        elif unit.unit_id in verifying:
            problems.append(
                f"{unit.unit_id}: verified by {sorted(verifying[unit.unit_id])} but owner is {owner!r}"
            )
        # The converse for an owner that IS in the stack but a different machine
        # than every verifier: an owner the unit is not verified by.
        if in_stack and unit.unit_id in verifying and owner not in verifying[unit.unit_id]:
            problems.append(
                f"{unit.unit_id}: owner {owner!r} is not among its verifiers {sorted(verifying[unit.unit_id])}"
            )
    return problems


def test_every_stack_owned_unit_matches_the_deployed_trigger(deployed):
    assert owner_problems(load_units(), deployed) == []


def test_every_verified_in_service_unit_is_owned_by_a_verifier(deployed):
    """The converse: no in-service unit a Scheduler entry verifies still names
    something else (the v1 state D01-D16 and D03 named until 2026-10-05)."""
    verified = {
        unit_id
        for scheds in deployed.values()
        for sched in scheds
        for unit_id in sched["input"].get("verify_units", [])
    }
    by_id = {u.unit_id: u for u in load_units()}
    missing = sorted(uid for uid in verified if uid not in by_id)
    assert not missing, f"verify_units names units with no descriptor: {missing}"
    stragglers = {
        uid: by_id[uid].raw["trigger"].get("owner")
        for uid in verified
        if not by_id[uid].retired and by_id[uid].raw["trigger"].get("owner") not in deployed
    }
    assert stragglers == {}


def test_the_check_is_not_vacuous(deployed):
    """It sees the weekly, EOD, morning and daily-heal machines, and a non-trivial
    population of units under them."""
    assert {
        "ne-data-collection-weekly",
        "ne-data-collection-eod",
        "ne-data-collection-morning",
        "ne-data-collection-daily-heal",
    } <= set(deployed)
    owned = [u for u in load_units() if (u.raw.get("trigger") or {}).get("owner") in deployed]
    assert len(owned) >= 30


@pytest.mark.parametrize(
    "mutation, expect",
    [
        # D03 as it was declared until 2026-10-05.
        (("D03", {"owner": "ne-weekly-freshness-pipeline:DataPhase1"}), "verified by"),
        # D03 relabelled to the weekly machine, which does not verify it (PR2016).
        (
            (
                "D03",
                {
                    "owner": "ne-data-collection-weekly",
                    "started_by": "eventbridge-scheduler:nousergon-data-collection/data-collection-weekly",
                },
            ),
            "does not verify it",
        ),
        (
            ("D01", {"started_by": "eventbridge-scheduler:nousergon-data-collection/data-collection-eod"}),
            "not a Scheduler",
        ),
        (("D19", {"schedule": "weekdays 16:00 America/New_York"}), "is not"),
        (("D01", {"owner": "ne-data-collection-nope"}), "not a state machine"),
    ],
)
def test_the_guard_catches_each_pre_fix_shape(deployed, mutation, expect):
    unit_id, fields = mutation
    edited = []
    for unit in load_units():
        if unit.unit_id == unit_id:
            raw = copy.deepcopy(unit.raw)
            raw["trigger"].update(fields)
            unit = Unit(unit_id=unit.unit_id, path=unit.path, raw=raw)
        edited.append(unit)
    problems = [p for p in owner_problems(edited, deployed) if p.startswith(f"{unit_id}:")]
    assert problems and expect in problems[0], problems
