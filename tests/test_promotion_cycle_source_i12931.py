"""Guard promotion grades units that run outside the collection state machines.

alpha-engine-config-I12931. `guard_promotion.read_promotion` takes its cycles
from the eod, morning and weekly collection machines only, so a required unit
none of them verifies — D38's Scheduler-fired Lambda, D39's GHA cron, D35's
on-demand backfill, D33's daily-heal machine — read "no declared schedule
verifies it" however healthy it was, and its guard could never be promoted.
Each such unit now declares its own cycle source in its descriptor's
``promotion_cycle`` block, graded by `exit_criteria.collect_own_cycles`. Pinned:

* lockstep: every required unit no promotion machine verifies declares a
  source; no machine-verified unit declares one; the source matches the
  unit's trigger;
* a healthy out-of-machine unit reaches clean (READY TO PROMOTE), on its own
  schedule and per invocation;
* a missing run, an unclean or stale invocation, a manual run and a denied
  listing still read short — never clean;
* in-machine grading is unchanged: a declaration on a machine-verified unit is
  ignored, and a machine's cycle still counts only ``scheduled`` manifests.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib

import pytest

from data_gate import exit_criteria as xc
from data_gate import guard_promotion as gp
from data_gate.cadence import unit_cadence
from data_gate.descriptors import (
    DescriptorError,
    Unit,
    _check_promotion_cycle,
    load_units,
)
from tests.data_gate_support import TRADING_DAY, DeniedStore, EmptyStore

UTC = dt.UTC
#: A reading the morning after TRADING_DAY (Mon 2026-09-14), past every grace.
NOW = dt.datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
EMPTY_FRESH = "data_empty_fresh"


@pytest.fixture(scope="module")
def units():
    return load_units()


def _unit(units, unit_id: str) -> Unit:
    return next(u for u in units if u.unit_id == unit_id)


def _machine_verified() -> set[str]:
    verified: set[str] = set()
    for name in xc.PROMOTION_SCHEDULES:
        verified |= set(xc.schedule_named(name)["input"].get("verify_units") or [])
    return verified


def _manifest(unit_id: str, day: str, name: str, *, started: str, trigger: str, verdict: str | None) -> dict:
    guards = [] if verdict is None else [{"guard": EMPTY_FRESH, "mode": "observe", "verdict": verdict}]
    body = {"unit_id": unit_id, "trigger": trigger, "status": "ok", "started": started, "guards": guards}
    return {f"runs/{unit_id}/{day}/{name}.json": json.dumps(body).encode()}


def _commissioned(unit_id: str) -> dict[str, bytes]:
    return {
        f"faults/{unit_id}/empty_fresh/latest.json": json.dumps(
            {"outcome": "induced", "as_of": "2026-09-14"}
        ).encode()
    }


def _read(units, unit_id: str, objects: dict[str, bytes], *, store_cls=EmptyStore):
    """``data_empty_fresh``'s criterion over ``unit_id`` alone, as `clauses.generate` builds it."""
    promotion = dataclasses.replace(gp.GUARD_PROMOTIONS[0], units=(unit_id,))
    assert promotion.name == EMPTY_FRESH
    store = store_cls({**objects, **_commissioned(unit_id)})
    sets = xc.own_cycle_sets(store, units, [], trading_day=TRADING_DAY, now=NOW)
    return gp.read_promotion(store, promotion, units, sets, trading_day=TRADING_DAY)


# --- lockstep with the descriptors and the stack -------------------------------------


def test_every_required_unit_outside_the_machines_declares_a_cycle_source(units):
    verified = _machine_verified()
    undeclared = sorted(
        f"{uid} ({p.name})"
        for p in gp.GUARD_PROMOTIONS
        for uid in gp.required_units(p, units)
        if uid not in verified and "promotion_cycle" not in _unit(units, uid).raw
    )
    assert not undeclared, f"required, unverified by eod/morning/weekly, no promotion_cycle: {undeclared}"


def test_the_issue_units_declare_their_source(units):
    declared = {u.unit_id: u.raw["promotion_cycle"] for u in units if "promotion_cycle" in u.raw}
    assert declared["D35"]["source"] == "per_invocation" and declared["D35"]["trigger"] == "on_demand"
    assert declared["D38"]["source"] == "own_schedule" and declared["D38"]["trigger"] == "scheduled"
    assert declared["D39"]["source"] == "own_schedule" and declared["D39"]["trigger"] == "gha"


def test_no_machine_verified_unit_declares_a_cycle_source(units):
    verified = _machine_verified()
    both = sorted(u.unit_id for u in units if "promotion_cycle" in u.raw and u.unit_id in verified)
    assert not both, f"{both} are verified by a promotion machine and must be graded on its cycles only"


def test_each_declared_source_matches_the_units_trigger(units):
    for unit in units:
        block = unit.raw.get("promotion_cycle")
        if not block:
            continue
        cadence = unit_cadence(unit.raw)
        expected = {"own_schedule": "scheduled", "per_invocation": "on_demand"}[block["source"]]
        assert cadence.kind == expected, f"{unit.unit_id}: {block['source']} over a {cadence.kind} trigger"


@pytest.mark.parametrize(
    ("block", "why"),
    [
        ("own_schedule", "not a mapping"),
        ({"source": "hourly", "trigger": "scheduled", "reason": "r"}, "source"),
        ({"source": "own_schedule", "trigger": "manual", "reason": "r"}, "manual run is never a cycle"),
        ({"source": "per_invocation", "trigger": "on_demand", "reason": " "}, "no `reason`"),
    ],
)
def test_a_half_declared_block_fails_the_load(block, why):
    with pytest.raises(DescriptorError, match=why):
        _check_promotion_cycle({"promotion_cycle": block}, pathlib.Path("D99-x.yaml"))


# --- own schedule: D38 (Scheduler Lambda, cron 22:30 UTC weekdays), D39 (GHA, Sat 12:40 UTC) --


def test_a_healthy_scheduled_lambda_reaches_clean(units):
    run = _manifest("D38", "2026-09-14", "a", started="2026-09-14T22:31:00Z", trigger="scheduled", verdict="ok")
    reading = _read(units, "D38", run)
    assert reading.detail.startswith("READY TO PROMOTE"), reading.detail
    cycles = xc.collect_own_cycles(EmptyStore(run), _unit(units, "D38"), trading_day=TRADING_DAY, now=NOW)
    assert cycles.schedule == "D38 own schedule (trigger.schedule=cron(30 22 ? * MON-FRI *))"
    assert [c.label for c in cycles.cycles] == ["2026-09-14T22:30Z"]


def test_a_healthy_gha_cron_reaches_clean(units):
    run = _manifest("D39", "2026-09-11", "a", started="2026-09-12T12:47:00Z", trigger="gha", verdict="ok")
    reading = _read(units, "D39", run)
    assert reading.detail.startswith("READY TO PROMOTE"), reading.detail


def test_a_missing_run_still_reads_short(units):
    reading = _read(units, "D38", {})
    assert not reading.met and not reading.detail.startswith("READY")
    assert "no scheduled data_empty_fresh verdict recorded" in reading.detail


def test_only_the_latest_due_fire_counts(units):
    """Friday's clean run does not stand in for Monday's missed one."""
    older = _manifest("D38", "2026-09-11", "a", started="2026-09-11T22:31:00Z", trigger="scheduled", verdict="ok")
    reading = _read(units, "D38", older)
    assert not reading.detail.startswith("READY") and "no scheduled" in reading.detail


def test_a_run_outside_the_fires_grace_does_not_count(units):
    late = _manifest("D38", "2026-09-14", "a", started="2026-09-15T05:00:00Z", trigger="scheduled", verdict="ok")
    assert not _read(units, "D38", late).detail.startswith("READY")


@pytest.mark.parametrize("trigger", ["manual", "on_demand", "scheduled"])
def test_only_the_declared_trigger_counts(units, trigger):
    """D39 declares ``gha``; a hand run, or any other trigger, is not its cycle."""
    run = _manifest("D39", "2026-09-11", "a", started="2026-09-12T12:47:00Z", trigger=trigger, verdict="ok")
    reading = _read(units, "D39", run)
    assert not reading.detail.startswith("READY") and "no gha data_empty_fresh verdict" in reading.detail


@pytest.mark.parametrize("verdict", ["empty_fresh", "unmeasurable", "below_floor"])
def test_an_unclean_verdict_on_the_own_schedule_blocks(units, verdict):
    run = _manifest("D38", "2026-09-14", "a", started="2026-09-14T22:31:00Z", trigger="scheduled", verdict=verdict)
    reading = _read(units, "D38", run)
    assert not reading.detail.startswith("READY") and "not clean" in reading.detail


def test_a_denied_listing_is_unmeasurable(units):
    reading = _read(units, "D38", {}, store_cls=DeniedStore)
    assert reading.unmeasurable and not reading.met


def test_own_schedule_on_a_unit_with_no_fixed_schedule_is_unmeasurable(units):
    d35 = _unit(units, "D35")
    raw = {**d35.raw, "promotion_cycle": {"source": "own_schedule", "trigger": "on_demand", "reason": "r"}}
    result = xc.collect_own_cycles(EmptyStore(), dataclasses.replace(d35, raw=raw), trading_day=TRADING_DAY, now=NOW)
    assert result.unmeasurable and "no fixed schedule" in result.reason


# --- per invocation: D35 (on-demand backfill) ----------------------------------------


def test_a_healthy_on_demand_tool_reaches_clean_on_its_latest_invocation(units):
    runs = {
        **_manifest("D35", "2026-09-02", "a", started="2026-09-02T15:00:00Z", trigger="on_demand", verdict="empty_fresh"),
        **_manifest("D35", "2026-09-10", "b", started="2026-09-10T15:00:00Z", trigger="on_demand", verdict="ok"),
    }
    reading = _read(units, "D35", runs)
    assert reading.detail.startswith("READY TO PROMOTE"), reading.detail
    cycles = xc.collect_own_cycles(EmptyStore(runs), _unit(units, "D35"), trading_day=TRADING_DAY, now=NOW)
    assert cycles.schedule == "D35 per invocation"
    assert [c.label for c in cycles.cycles] == ["2026-09-10T15:00Z"]


def test_an_unclean_latest_invocation_blocks_even_after_a_clean_one(units):
    runs = {
        **_manifest("D35", "2026-09-10", "a", started="2026-09-10T15:00:00Z", trigger="on_demand", verdict="ok"),
        **_manifest("D35", "2026-09-10", "b", started="2026-09-10T18:00:00Z", trigger="on_demand", verdict="empty_fresh"),
    }
    reading = _read(units, "D35", runs)
    assert not reading.detail.startswith("READY") and "not clean" in reading.detail


def test_an_on_demand_tool_never_invoked_still_reads_short(units):
    reading = _read(units, "D35", {})
    assert not reading.met and "D35@D35 per invocation: no due cycle" in reading.detail


def test_an_invocation_after_the_reading_day_is_not_graded(units):
    future = _manifest("D35", "2026-09-20", "a", started="2026-09-20T15:00:00Z", trigger="on_demand", verdict="ok")
    reading = _read(units, "D35", future)
    assert "no due cycle" in reading.detail and not reading.detail.startswith("READY")


def test_a_manual_latest_invocation_is_not_counted(units):
    run = _manifest("D35", "2026-09-10", "a", started="2026-09-10T15:00:00Z", trigger="manual", verdict="ok")
    reading = _read(units, "D35", run)
    assert not reading.detail.startswith("READY") and "no on_demand data_empty_fresh verdict" in reading.detail


# --- in-machine units are unchanged --------------------------------------------------


def test_a_declaration_on_a_machine_verified_unit_is_ignored(units):
    d20 = _unit(units, "D20")
    raw = {**d20.raw, "promotion_cycle": {"source": "own_schedule", "trigger": "gha", "reason": "r"}}
    declared = dataclasses.replace(d20, raw=raw)
    machine = xc.CycleSet(schedule="nousergon-data-collection/data-collection-eod", units=[d20])
    assert xc.own_cycle_sets(EmptyStore(), [declared], [machine], trading_day=TRADING_DAY, now=NOW) == []


def test_a_machine_cycle_still_counts_only_scheduled_manifests(units):
    d20 = _unit(units, "D20")
    machine = xc.CycleSet(schedule="nousergon-data-collection/data-collection-eod", units=[d20])
    assert machine.triggers == frozenset({"scheduled"})
    cycle = xc.Cycle(fire=dt.datetime(2026, 9, 14, 22, 15, tzinfo=UTC))
    cycle.manifests["D20"] = [
        ("k", {"trigger": "gha", "status": "ok", "guards": [{"guard": "data_spine_window", "verdict": "ok"}]})
    ]
    machine.cycles.append(cycle)
    promotion = next(p for p in gp.GUARD_PROMOTIONS if p.name == "data_spine_window")
    reading = gp.read_promotion(EmptyStore(), promotion, units, [machine], trading_day=TRADING_DAY)
    assert "no scheduled data_spine_window verdict recorded" in reading.detail


def test_a_unit_declaring_nothing_still_reads_missing(units):
    promotion = dataclasses.replace(gp.GUARD_PROMOTIONS[0], units=("D20",))
    reading = gp.read_promotion(EmptyStore(), promotion, units, [], trading_day=TRADING_DAY)
    assert "no declared schedule verifies it and its descriptor declares no promotion_cycle" in reading.detail

