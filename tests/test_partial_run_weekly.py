"""A weekly fire counts when it completes across partial runs (alpha-engine-config-I11812).

Brian, 2026-10-06: "the weekly sf has failed on its first try every single week
since it began over 6 months ago ... We need to count weekly sf as success if it
runs successfully even if it is made up of partial runs."

`survives_phase4` used to require the fire's own execution to be SUCCEEDED, so
a week the machine finished by reruns read UNMET. These tests pin the standing
rule that replaced that for the weekly schedule, and the edges it keeps: the
trigger must still fire, some run of the week must SUCCEED, each unit must have
been produced by the machine, and no other schedule moves.
"""

from __future__ import annotations

import datetime as dt

import pytest

from data_gate import descriptors, standalone
from tests.test_recovered_fires import (
    FIRE,
    MONDAY,
    MONDAY_READ,
    NEXT_FIRE,
    NEXT_MONDAY,
    NEXT_MONDAY_READ,
    _execution,
    _fire_execution,
    _ok,
    _store,
)

UTC = dt.timezone.utc


@pytest.fixture(scope="module")
def units() -> dict[str, descriptors.Unit]:
    return {u.unit_id: u for u in descriptors.load_units()}


@pytest.fixture(autouse=True)
def _no_declarations(monkeypatch):
    """The rule stands on its own: no recovered_fires.yaml entry is in play."""
    monkeypatch.setattr(standalone, "_committed_recovered_fires", lambda: ())


def _weekly_units() -> list[str]:
    weekly = next(s for s in standalone.stack_schedules() if s["name"] == "data-collection-weekly")
    return list(weekly["input"]["verify_units"])


def _read(store, unit, *, day=NEXT_MONDAY, now=NEXT_MONDAY_READ):
    return standalone.read_survives_phase4(store, unit, trading_day=day, now=now)


def _next_week(tmp_path, *executions, **ok_over):
    """The 10-10 weekly in the 10-03 shape: attempt 0 FAILED after producing D01."""
    _ok(tmp_path, "D01", NEXT_FIRE + dt.timedelta(minutes=10), "OKD01", **ok_over)
    return _store(tmp_path, _fire_execution(NEXT_FIRE, name="fire-1010"), *executions)


def test_a_week_finished_by_a_rerun_counts_with_no_declaration(tmp_path, units):
    rerun = _execution("rerun-1010", "SUCCEEDED", NEXT_FIRE + dt.timedelta(hours=4), 49)
    reading = _read(_next_week(tmp_path, rerun), units["D01"])
    assert reading.met, reading.detail
    assert "partial run" in reading.detail and "rerun-1010" in reading.detail


def test_two_partial_runs_then_a_success_still_count(tmp_path, units):
    """The failing reruns stay in the record; the unit's ok run is in the first one."""
    failed = _execution("rerun-a", "FAILED", NEXT_FIRE + dt.timedelta(hours=4), 30)
    done = _execution("rerun-b", "SUCCEEDED", NEXT_FIRE + dt.timedelta(days=1), 40)
    reading = _read(_next_week(tmp_path, failed, done), units["D01"])
    assert reading.met, reading.detail
    assert "3 partial run(s)" in reading.detail


def test_no_successful_run_of_the_week_is_unmet(tmp_path, units):
    failed = _execution("rerun-a", "FAILED", NEXT_FIRE + dt.timedelta(hours=4), 30)
    running = _execution("rerun-b", "RUNNING", NEXT_FIRE + dt.timedelta(hours=6), 30)
    running["stopDate"] = None
    reading = _read(_next_week(tmp_path, failed, running), units["D01"])
    assert not reading.met
    assert "no rerun of that week" in reading.detail and "FAILED, not SUCCEEDED" in reading.detail


def test_a_rerun_that_finished_after_the_reading_does_not_count_yet(tmp_path, units):
    late = _execution("rerun-late", "SUCCEEDED", NEXT_MONDAY_READ - dt.timedelta(minutes=10), 30)
    assert not _read(_next_week(tmp_path, late), units["D01"]).met


def test_a_success_started_after_the_window_belongs_to_no_week(tmp_path, units):
    """Six days keeps a rerun ahead of the next Saturday; the reading is graded before it."""
    rerun = _execution("rerun-late", "SUCCEEDED", NEXT_FIRE + standalone.PARTIAL_RUN_WINDOW, 30)
    reading = standalone.read_survives_phase4(
        _next_week(tmp_path, rerun),
        units["D01"],
        trading_day=NEXT_MONDAY + dt.timedelta(days=4),
        now=NEXT_FIRE + standalone.PARTIAL_RUN_WINDOW + dt.timedelta(hours=1),
    )
    assert not reading.met


def test_a_unit_with_no_ok_manifest_inside_a_run_is_unmet(tmp_path, units):
    """A rerun finishing the week does not vouch for a unit it never produced."""
    rerun = _execution("rerun-1010", "SUCCEEDED", NEXT_FIRE + dt.timedelta(hours=4), 49)
    _ok(tmp_path, "D02", NEXT_FIRE + dt.timedelta(hours=2, minutes=30), "NAD02", status="not_applicable")
    reading = _read(_next_week(tmp_path, rerun), units["D02"])
    assert not reading.met
    assert "no ok scheduled-trigger manifest" in reading.detail


def test_a_manifest_outside_every_run_does_not_count(tmp_path, units):
    """An ok manifest written between runs (a hand run beside the machine) is not the machine's."""
    rerun = _execution("rerun-1010", "SUCCEEDED", NEXT_FIRE + dt.timedelta(hours=8), 30)
    # Attempt 0 ran 09:00:40 + 198 min; this starts at 13:00, before the rerun at 17:00.
    _ok(tmp_path, "D02", NEXT_FIRE + dt.timedelta(hours=4), "BESIDE")
    assert not _read(_next_week(tmp_path, rerun), units["D02"]).met


def test_a_manual_trigger_manifest_does_not_count(tmp_path, units):
    rerun = _execution("rerun-1010", "SUCCEEDED", NEXT_FIRE + dt.timedelta(hours=4), 49)
    assert not _read(_next_week(tmp_path, rerun, trigger="manual"), units["D01"]).met


def test_a_fire_with_no_execution_of_its_own_is_still_unmet(tmp_path, units):
    """The trigger has to fire: a hand run alone is not a schedule that survives."""
    _ok(tmp_path, "D01", NEXT_FIRE + dt.timedelta(hours=4, minutes=5), "OKD01")
    store = _store(tmp_path, _execution("hand", "SUCCEEDED", NEXT_FIRE + dt.timedelta(hours=4), 49))
    reading = _read(store, units["D01"])
    assert not reading.met and "no execution started for the fire" in reading.detail


def test_the_10_03_week_counts_under_the_rule_alone(tmp_path, units):
    """The week the declaration was written for reads MET without it."""
    for i, unit_id in enumerate(_weekly_units()):
        _ok(tmp_path, unit_id, FIRE + dt.timedelta(minutes=10 + i), f"OK{unit_id}")
    recovery = _execution("weekly-recovery-2026-10-02-1", "SUCCEEDED", FIRE + dt.timedelta(minutes=237), 49)
    store = _store(tmp_path, _fire_execution(), recovery)
    for unit_id in _weekly_units():
        if unit_id == "D17":  # also on the morning schedule; that leg is not in question here
            continue
        reading = standalone.read_survives_phase4(store, units[unit_id], trading_day=MONDAY, now=MONDAY_READ)
        assert reading.met, (unit_id, reading.detail)


def test_only_the_weekly_schedule_takes_partial_runs():
    assert standalone.PARTIAL_RUN_SCHEDULES == frozenset({"data-collection-weekly"})
    names = {s["name"] for s in standalone.stack_schedules()}
    assert standalone.PARTIAL_RUN_SCHEDULES <= names
    assert standalone.PARTIAL_RUN_WINDOW < dt.timedelta(days=7)
