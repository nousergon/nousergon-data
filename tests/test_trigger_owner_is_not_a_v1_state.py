"""No in-service unit names a v1 Step Functions STATE as its trigger owner.

`data.phase1.triggers_reconciled` (alpha-engine-config-I11189) reconciles a
step-functions unit against the live surface its ``trigger.owner`` names
(`data_gate/trigger_reconcile.py::owner_key`). Until 2026-10-03 fifteen units
(D01-D08, D10, D11, D13-D16, D46) still named ``ne-weekly-freshness-pipeline:
DataPhase1`` / ``:DataPhase2`` / ``:RAGIngestion`` (fourteen are relabelled
here; D03 is registered below as pending), so the clause matched their
declared Saturday fire against that machine's Saturday starts and read MET. But
the decoupled cutover (alpha-engine-config-I11269, nousergon-data-PR1930)
removed DataPhase1 from the v1 weekly definition, and the Saturday cadence
trigger skips DataPhase2 and RAGIngestion (nousergon-data-PR2025): the v1
machine still starts every Saturday, it just no longer runs any of them. A
reconcile against a machine that does not run the unit is green about the
wrong thing. Those units run in ``ne-data-collection-weekly``, started by the
Scheduler entry ``nousergon-data-collection/data-collection-weekly``.

The class: a ``<v1 machine>:<State>`` owner on a unit that is not retired. Every
data stage has left the v1 machines (the standalone stack owns each one), so
such an owner can only be lineage left in the field that is supposed to name
what fires the unit now. Lineage belongs in ``trigger.successor`` ("v1 was ...").
Retired units keep their historical owner; they are out of every reader's scope.
"""

from __future__ import annotations

import copy

from data_gate.descriptors import Unit, load_units

#: The v1 state machines the standalone stack replaces. The same set
#: tests/test_data_collection_stack.py::V1_PIPELINES pins, less the CFN stack
#: name, which is never a trigger owner.
V1_MACHINES = (
    "ne-postclose-trading-pipeline",
    "ne-preopen-trading-pipeline",
    "ne-weekly-freshness-pipeline",
)


#: In-service units still naming a v1 state, each with the change that moves
#: it. May only SHRINK: an entry whose unit no longer names a v1 state fails
#: `test_the_pending_register_only_shrinks`.
#:
#: D03 is in BOTH live entries' verify_units history; nousergon-data-PR2016
#: dropped it from the weekly check, so its graded leg is ne-data-collection-eod
#: (alpha-engine-config-I11832, decision 2). Declaring the EOD fire (18:15 ET)
#: needs nousergon-data-PR2030's fire-selection ceiling, exactly as D19-D32 in
#: nousergon-data-PR2024 do, so it moves with that PR, not this one.
PENDING_V1_STATE_OWNERS = {
    "D03": "nousergon-data-PR2024 (EOD relabel, waits on nousergon-data-PR2030)",
}


def _v1_state(unit: Unit) -> str | None:
    owner = str((unit.raw.get("trigger") or {}).get("owner") or "").strip()
    machine, sep, state = owner.partition(":")
    return state if sep and machine in V1_MACHINES else None


def v1_state_owner_problems(
    units: list[Unit], *, pending: dict[str, str] = PENDING_V1_STATE_OWNERS
) -> list[str]:
    problems = []
    for unit in units:
        if unit.retired or unit.unit_id in pending:
            continue
        trigger = unit.raw.get("trigger") or {}
        owner = str(trigger.get("owner") or "").strip()
        machine, sep, state = owner.partition(":")
        if sep and machine in V1_MACHINES:
            problems.append(
                f"{unit.unit_id}: trigger.owner {owner!r} names the v1 state {state!r}, which "
                "no longer runs this unit; name the machine and schedule entry that do "
                "(owner + started_by) and record the v1 lineage in trigger.successor"
            )
    return problems


def test_no_in_service_unit_names_a_v1_state_as_its_owner():
    assert v1_state_owner_problems(load_units()) == []


def test_the_pending_register_only_shrinks():
    by_id = {u.unit_id: u for u in load_units()}
    stale = [
        unit_id
        for unit_id in PENDING_V1_STATE_OWNERS
        if unit_id not in by_id or by_id[unit_id].retired or _v1_state(by_id[unit_id]) is None
    ]
    assert stale == [], f"remove from PENDING_V1_STATE_OWNERS, now resolved: {stale}"


def test_the_guard_catches_the_pre_fix_shape():
    """Mutation: D01 as it was declared before this change is caught."""
    units = load_units()
    edited = []
    for unit in units:
        if unit.unit_id == "D01":
            raw = copy.deepcopy(unit.raw)
            raw["trigger"].pop("started_by", None)
            raw["trigger"]["owner"] = "ne-weekly-freshness-pipeline:DataPhase1"
            unit = Unit(unit_id=unit.unit_id, path=unit.path, raw=raw)
        edited.append(unit)
    problems = v1_state_owner_problems(edited)
    assert len(problems) == 1 and problems[0].startswith("D01:"), problems


def test_retired_units_keep_their_historical_owner():
    """Not vacuous in the other direction: retired v1-state owners exist and are
    deliberately left alone (D09, D12, D34, D40, D41 on 2026-10-03)."""
    retired_v1 = [
        u.unit_id
        for u in load_units()
        if u.retired
        and str((u.raw.get("trigger") or {}).get("owner") or "").partition(":")[0]
        in V1_MACHINES
    ]
    assert retired_v1, "no retired unit carries a v1 owner; drop this test's premise"


def test_the_weekly_units_reconcile_against_the_v2_weekly_entry():
    from data_gate.trigger_reconcile import owner_key

    by_id = {u.unit_id: u for u in load_units()}
    for unit_id in (
        "D01", "D02", "D04", "D05", "D06", "D07", "D08", "D10", "D11",
        "D13", "D14", "D15", "D16", "D46",
    ):
        assert owner_key(by_id[unit_id]) == (
            "eventbridge-scheduler:nousergon-data-collection/data-collection-weekly"
        ), unit_id
