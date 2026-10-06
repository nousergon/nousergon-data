"""A declared recovery of ONE missed fire (alpha-engine-config-I11812).

`data_gate/config/recovered_fires.yaml` lets `survives_phase4` count a ruled,
hand-started recovery of exactly one (schedule, fire) by the cycle counter's
rule. These tests pin the three properties that make it a single-fire
exception rather than a loosening: it clears exactly the units the declared
schedule verifies, it changes nothing for any other fire, and it is inert while
its ruling is pending.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import pathlib

import pytest

from data_gate import descriptors, standalone
from tests.test_run_cadence_and_survives_phase4 import _Scheduler, _Sfn, _iso, _live_store, _write

UTC = dt.timezone.utc

#: Sat 2026-10-03 05:00 America/New_York — the weekly fire the committed file declares.
FIRE = dt.datetime(2026, 10, 3, 9, 0, tzinfo=UTC)
#: The Monday reading C12 predicted against.
MONDAY = dt.date(2026, 10, 5)
MONDAY_READ = dt.datetime(2026, 10, 5, 23, 30, tzinfo=UTC)
#: The next Saturday's fire, which no declaration names.
NEXT_FIRE = dt.datetime(2026, 10, 10, 9, 0, tzinfo=UTC)
NEXT_MONDAY = dt.date(2026, 10, 12)
NEXT_MONDAY_READ = dt.datetime(2026, 10, 12, 23, 30, tzinfo=UTC)

_ARN = "arn:aws:states:us-east-1:000000000000:execution:{machine}:{name}"
WEEKLY = "ne-data-collection-weekly"


def _execution(name: str, status: str, start: dt.datetime, minutes: int, machine: str = WEEKLY) -> dict:
    return {
        "executionArn": _ARN.format(machine=machine, name=name),
        "status": status,
        "startDate": start,
        "stopDate": start + dt.timedelta(minutes=minutes),
    }


def _fire_execution(fire: dt.datetime = FIRE, name: str = "ff6ac0c4-10a8-471f-9905-d73f00012ff5") -> dict:
    return _execution(name, "FAILED", fire + dt.timedelta(seconds=40), 198)


def _recovery(fire: dt.datetime = FIRE, status: str = "SUCCEEDED", after: dt.timedelta = dt.timedelta(minutes=237)):
    return _execution("weekly-recovery-2026-10-02-1", status, fire + after, 49)


DECLARED = standalone.RecoveredFire(
    schedule="data-collection-weekly",
    fire=FIRE,
    machine=WEEKLY,
    fire_execution="ff6ac0c4-10a8-471f-9905-d73f00012ff5",
    recovery_execution="weekly-recovery-2026-10-02-1",
    root_cause_fix="nousergon-data#2036",
    tracker="alpha-engine-config-I11812",
    ruling="https://example.invalid/ruling",
)


@pytest.fixture(scope="module")
def units() -> dict[str, descriptors.Unit]:
    return {u.unit_id: u for u in descriptors.load_units()}


@pytest.fixture(autouse=True)
def _declarations_alone(monkeypatch):
    """Grade the declaration path on its own.

    The standing partial-run rule (tests/test_partial_run_weekly.py) would count
    these weekly fires with or without a declaration, which is what it is for;
    these tests pin what a declaration does when nothing else would.
    """
    monkeypatch.setattr(standalone, "PARTIAL_RUN_SCHEDULES", frozenset())


@pytest.fixture
def declare(monkeypatch):
    """Replace the committed declarations for one test."""

    def _set(*entries: standalone.RecoveredFire) -> None:
        monkeypatch.setattr(standalone, "_committed_recovered_fires", lambda: tuple(entries))

    return _set


def _ok(root: pathlib.Path, unit_id: str, started: dt.datetime, run_id: str, **over) -> None:
    folder = (started - dt.timedelta(days=1)).date().isoformat()
    _write(root, unit_id, folder, run_id, started=_iso(started), finished=_iso(started + dt.timedelta(minutes=5)), **over)


def _read(store, unit, *, day=MONDAY, now=MONDAY_READ):
    return standalone.read_survives_phase4(store, unit, trading_day=day, now=now)


def _store(root, *executions: dict, morning: list[dict] | None = None):
    newest_first = sorted(executions, key=lambda e: e["startDate"], reverse=True)
    return _live_store(root, _Scheduler(), _Sfn({WEEKLY: newest_first, "ne-data-collection-morning": morning or []}))


# ── the committed declaration ───────────────────────────────────────────────


def test_the_committed_file_declares_exactly_the_10_03_weekly_fire():
    entries = standalone.load_recovered_fires(standalone.RECOVERED_FIRES_PATH)
    assert [(e.schedule, e.fire) for e in entries] == [("data-collection-weekly", FIRE)]
    entry = entries[0]
    assert entry.machine == WEEKLY
    assert entry.fire_execution == "ff6ac0c4-10a8-471f-9905-d73f00012ff5"
    assert entry.recovery_execution == "weekly-recovery-2026-10-02-1"
    assert "#2036" in entry.root_cause_fix and entry.tracker == "alpha-engine-config-I11812"


# ── the exception, when ruled ───────────────────────────────────────────────


def test_a_ruled_recovery_clears_exactly_the_declared_schedules_units(tmp_path, units, declare):
    """Every unit the weekly schedule verifies flips; no other unit's reading moves."""
    weekly = next(s for s in standalone.stack_schedules() if s["name"] == "data-collection-weekly")
    verified = list(weekly["input"]["verify_units"])
    # Attempt 0 of the scheduled execution wrote ok manifests, then the retry
    # buried some of them under not_applicable — both inside the window.
    for i, unit_id in enumerate(verified):
        _ok(tmp_path, unit_id, FIRE + dt.timedelta(minutes=10 + i), f"OK{unit_id}")
        _ok(tmp_path, unit_id, FIRE + dt.timedelta(hours=2, minutes=20), f"NA{unit_id}", status="not_applicable")
    # D17 is also on the morning schedule; give that leg a clean Monday run so
    # only the weekly leg is in question.
    morning_start = dt.datetime(2026, 10, 5, 11, 30, 3, tzinfo=UTC)
    _write(tmp_path, "D17", "2026-10-05", "MORNING", started=_iso(morning_start + dt.timedelta(minutes=2)))
    store = _store(
        tmp_path,
        _fire_execution(),
        _recovery(),
        morning=[_execution("m", "SUCCEEDED", morning_start, 20, machine="ne-data-collection-morning")],
    )

    declare(dataclasses.replace(DECLARED, ruling="pending"))
    pending = {uid: _read(store, u) for uid, u in units.items()}
    declare(DECLARED)
    ruled = {uid: _read(store, u) for uid, u in units.items()}

    assert {uid for uid in units if pending[uid].met != ruled[uid].met} == set(verified)
    assert all(pending[uid] == ruled[uid] for uid in units if uid not in verified)
    assert all(not pending[uid].met and ruled[uid].met for uid in verified), {
        uid: ruled[uid].detail for uid in verified if not ruled[uid].met
    }
    assert "weekly-recovery-2026-10-02-1" in ruled["D02"].detail


def test_the_recovery_executions_own_manifest_counts(tmp_path, units, declare):
    """D02's only ok manifest came from the recovery, at 13:00Z."""
    _ok(tmp_path, "D02", FIRE + dt.timedelta(minutes=30), "FAILED0", status="failed")
    _ok(tmp_path, "D02", FIRE + dt.timedelta(hours=4), "RECOVERED")
    declare(DECLARED)
    reading = _read(_store(tmp_path, _fire_execution(), _recovery()), units["D02"])
    assert reading.met is True, reading.detail
    assert any(k.endswith("RECOVERED.json") for k in reading.evidence)


def test_a_unit_with_no_ok_manifest_in_the_window_stays_unmet(tmp_path, units, declare):
    _ok(tmp_path, "D08", FIRE + dt.timedelta(minutes=30), "NA", status="not_applicable")
    _ok(tmp_path, "D08", FIRE + dt.timedelta(hours=2), "FAILED", status="failed")
    declare(DECLARED)
    reading = _read(_store(tmp_path, _fire_execution(), _recovery()), units["D08"])
    assert reading.met is False and reading.unmeasurable is False
    assert "no ok scheduled-trigger manifest" in reading.detail


def test_a_manifest_after_fire_plus_grace_does_not_count(tmp_path, units, declare):
    _ok(tmp_path, "D01", FIRE + dt.timedelta(hours=6, minutes=1), "LATE")
    declare(DECLARED)
    reading = _read(_store(tmp_path, _fire_execution(), _recovery()), units["D01"])
    assert reading.met is False and "no ok scheduled-trigger manifest" in reading.detail


def test_a_hand_trigger_manifest_does_not_count(tmp_path, units, declare):
    _ok(tmp_path, "D01", FIRE + dt.timedelta(hours=4), "HAND", trigger="manual")
    declare(DECLARED)
    reading = _read(_store(tmp_path, _fire_execution(), _recovery()), units["D01"])
    assert reading.met is False


@pytest.mark.parametrize(
    "recovery",
    [
        pytest.param(_recovery(status="FAILED"), id="recovery-failed"),
        pytest.param(_recovery(status="RUNNING"), id="recovery-running"),
        pytest.param(_recovery(after=dt.timedelta(hours=6, minutes=5)), id="recovery-after-grace"),
        pytest.param(None, id="recovery-absent"),
    ],
)
def test_the_declared_recovery_must_have_succeeded_inside_the_grace(tmp_path, units, declare, recovery):
    _ok(tmp_path, "D01", FIRE + dt.timedelta(minutes=10), "OK")
    declare(DECLARED)
    executions = [_fire_execution()] + ([recovery] if recovery else [])
    reading = _read(_store(tmp_path, *executions), units["D01"])
    assert reading.met is False and reading.unmeasurable is False
    assert "weekly-recovery-2026-10-02-1" in reading.detail


def test_a_different_execution_for_the_fire_is_not_excused(tmp_path, units, declare):
    _ok(tmp_path, "D01", FIRE + dt.timedelta(minutes=10), "OK")
    declare(DECLARED)
    store = _store(tmp_path, _fire_execution(name="someone-else"), _recovery())
    reading = _read(store, units["D01"])
    assert reading.met is False and "someone-else" in reading.detail


def test_the_trigger_must_still_have_fired(tmp_path, units, declare):
    """No execution inside the 15-minute window: the declaration excuses nothing."""
    _ok(tmp_path, "D01", FIRE + dt.timedelta(hours=4), "OK")
    declare(DECLARED)
    reading = _read(_store(tmp_path, _recovery()), units["D01"])
    assert reading.met is False and "no execution started for the fire" in reading.detail


# ── everything else is unaffected ───────────────────────────────────────────


def test_an_undeclared_fire_is_graded_exactly_as_before(tmp_path, units, declare):
    """The same shape one Saturday later reads FAILED, with the 10-03 entry ruled."""
    _ok(tmp_path, "D01", NEXT_FIRE + dt.timedelta(minutes=10), "OK")
    store = _store(
        tmp_path,
        _fire_execution(NEXT_FIRE, name="next-scheduled"),
        _execution("weekly-recovery-2026-10-02-1", "SUCCEEDED", NEXT_FIRE + dt.timedelta(hours=4), 49),
    )
    declare(DECLARED)
    with_entry = _read(store, units["D01"], day=NEXT_MONDAY, now=NEXT_MONDAY_READ)
    declare()
    without = _read(store, units["D01"], day=NEXT_MONDAY, now=NEXT_MONDAY_READ)
    assert with_entry == without
    assert with_entry.met is False and "FAILED, not SUCCEEDED" in with_entry.detail
    assert "recover" not in with_entry.detail


def test_a_pending_ruling_changes_no_reading_and_says_so(tmp_path, units, declare):
    _ok(tmp_path, "D01", FIRE + dt.timedelta(minutes=10), "OK")
    store = _store(tmp_path, _fire_execution(), _recovery())
    declare(dataclasses.replace(DECLARED, ruling="pending"))
    reading = _read(store, units["D01"])
    assert reading.met is False and "FAILED, not SUCCEEDED" in reading.detail
    assert "ruling is pending" in reading.detail


def test_a_succeeded_fire_never_consults_the_declaration(tmp_path, units, declare):
    _ok(tmp_path, "D01", FIRE + dt.timedelta(minutes=10), "OK")
    declare(DECLARED)
    store = _store(tmp_path, {**_fire_execution(), "status": "SUCCEEDED"})
    reading = _read(store, units["D01"])
    assert reading.met is True and "recover" not in reading.detail


# ── the loader refuses anything wider than one real fire ────────────────────

_ENTRY = """\
  - schedule: {schedule}
    fire: "{fire}"
    machine: ne-data-collection-weekly
    fire_execution: a
    recovery_execution: b
    root_cause_fix: c
    tracker: d
    ruling: e
"""


def _file(tmp_path, *entries: str, extra: str = "") -> pathlib.Path:
    path = tmp_path / "recovered_fires.yaml"
    path.write_text("schema_version: data_recovered_fires.v1\nrecovered_fires:\n" + "".join(entries) + extra)
    return path


@pytest.mark.parametrize(
    ("entries", "message"),
    [
        pytest.param([_ENTRY.format(schedule="data-collection-weekly", fire="2026-10-03T09:05:00Z")], "not a fire", id="off-fire-minute"),
        pytest.param([_ENTRY.format(schedule="data-collection-weekly", fire="2026-10-04T09:00:00Z")], "not a fire", id="off-fire-day"),
        pytest.param([_ENTRY.format(schedule="data-collection-weekly", fire="2026-10-03")], "YYYY-MM-DD", id="a-date-not-a-fire"),
        pytest.param([_ENTRY.format(schedule="no-such-schedule", fire="2026-10-03T09:00:00Z")], "not in the committed stack", id="unknown-schedule"),
        pytest.param([_ENTRY.format(schedule="data-collection-weekly", fire="2026-10-03T09:00:00Z")] * 2, "declared twice", id="duplicate"),
        pytest.param([_ENTRY.format(schedule="data-collection-weekly", fire="2026-10-03T09:00:00Z").replace("    ruling: e\n", "")], "missing", id="no-ruling"),
        pytest.param([_ENTRY.format(schedule="data-collection-weekly", fire="2026-10-03T09:00:00Z") + "    until: 2026-12-31\n"], "unknown", id="a-window-field"),
    ],
)
def test_the_loader_refuses_anything_but_one_real_fire(tmp_path, entries, message):
    with pytest.raises(ValueError, match=message):
        standalone.load_recovered_fires(_file(tmp_path, *entries))


def test_a_weekday_fire_on_an_nyse_holiday_is_not_a_fire(tmp_path):
    # 2026-11-26 is Thanksgiving; the EOD schedule requires a trading day.
    entry = _ENTRY.format(schedule="data-collection-eod", fire="2026-11-26T23:15:00Z")
    with pytest.raises(ValueError, match="not a fire"):
        standalone.load_recovered_fires(_file(tmp_path, entry))


def test_an_absent_file_declares_nothing(tmp_path):
    assert standalone.load_recovered_fires(tmp_path / "missing.yaml") == ()
