"""The weekly cycle counters apply the partial-run rule `survives_phase4` applies.

Brian, 2026-10-06 (alpha-engine-config-I11812): "We need to count weekly sf as
success if it runs successfully even if it is made up of partial runs."
`standalone._grade_partial_runs` applied that to `survives_phase4` only, so in
one reading the 2026-10-10 week read SURVIVED for D16 and D46 while
`data.phase1.consecutive_weekly_cycles` read the same week INCOMPLETE: the
fire's own execution failed, and `weekly-rerun-2026-10-10-d16-2` finished the
week 10h36m after the fire, past `COMPLETION_GRACE` (6h), which was the only
window `exit_criteria.collect_cycles` bucketed manifests into.

The case pinned here is that week's shape, read from the store, plus the two
lines the rule must not cross: a fire that never ran is not rescued by a
rerun, and no rerun can count for the following Saturday's fire.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
from zoneinfo import ZoneInfo

import pytest

from data_gate import descriptors, exit_criteria, standalone
from data_gate.store import LocalStore

UTC = dt.timezone.utc
ET = ZoneInfo("America/New_York")

#: The Saturday whose week the rule exists for, and a reading after it.
SATURDAY = dt.date(2026, 10, 10)
FRIDAY_BEFORE = dt.date(2026, 10, 9)
READ_DAY = dt.date(2026, 10, 13)
READ_NOW = dt.datetime(2026, 10, 14, 2, 0, tzinfo=UTC)


def _schedule(name: str) -> dict:
    return next(s for s in standalone._stack_schedules() if s["name"] == name)


def _weekly_fire(day: dt.date) -> dt.datetime:
    minute, hour = _schedule("data-collection-weekly")["expression"].removeprefix("cron(").split(" ")[:2]
    return dt.datetime.combine(day, dt.time(int(hour), int(minute)), tzinfo=ET).astimezone(UTC)


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture(scope="module")
def units() -> list[descriptors.Unit]:
    return descriptors.load_units()


@pytest.fixture(scope="module")
def prefixes(units) -> dict[str, str]:
    """Each weekly verify unit's manifest folder, as the store sees it (D46
    records under D16's prefix, so writing one covers both)."""
    by_id = {u.unit_id: u for u in units}
    verify = _schedule("data-collection-weekly")["input"]["verify_units"]
    return {uid: by_id[uid].run_manifest_prefix.removeprefix("data_collection/") for uid in verify}


def _manifest(root: pathlib.Path, prefix: str, run_id: str, started: dt.datetime, *, status: str = "ok") -> None:
    unit_id = prefix.rsplit("/", 1)[-1]
    doc = {
        "schema_version": "data_run_manifest.v1",
        "run_id": run_id,
        "unit_id": unit_id,
        "trigger": "scheduled",
        "trading_day": FRIDAY_BEFORE.isoformat(),
        "status": status,
        "started": _iso(started),
        "finished": _iso(started + dt.timedelta(minutes=10)),
        "rows_out": 1,
        "guards": [],
    }
    path = root / prefix / FRIDAY_BEFORE.isoformat() / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc))


def _week(root: pathlib.Path, prefixes: dict[str, str], *, late: dict[str, dt.timedelta]) -> None:
    """Every verify unit ok inside the fire's grace, except ``late`` units,
    which FAIL in the fire's own run and are finished by a rerun at the offset."""
    fire = _weekly_fire(SATURDAY)
    for prefix in sorted(set(prefixes.values())):
        if prefix in late:
            _manifest(root, prefix, "FIRE", fire + dt.timedelta(minutes=30), status="failed")
            _manifest(root, prefix, "RERUN", fire + late[prefix])
        else:
            _manifest(root, prefix, "FIRE", fire + dt.timedelta(minutes=30))


def _weekly(root: pathlib.Path, units, *, trading_day: dt.date = READ_DAY, now: dt.datetime = READ_NOW):
    return exit_criteria.collect_cycles(
        LocalStore(root),
        units,
        schedule_name="data-collection-weekly",
        count=1,
        trading_day=trading_day,
        now=now,
    )


def test_the_weekly_schedule_is_the_one_the_ruling_names():
    assert "data-collection-weekly" in standalone.PARTIAL_RUN_SCHEDULES
    assert standalone.PARTIAL_RUN_WINDOW < dt.timedelta(days=7)


def test_a_week_finished_by_a_rerun_past_the_grace_is_complete(tmp_path, units, prefixes):
    """THE defect: the 2026-10-10 shape. D16 (and D46, on D16's prefix) failed
    in the fire's own run and were finished by a rerun 10h36m later."""
    _week(tmp_path, prefixes, late={prefixes["D16"]: dt.timedelta(hours=10, minutes=38)})
    cycles = _weekly(tmp_path, units)
    assert not cycles.unmeasurable, cycles.reason
    assert [c.fire for c in cycles.cycles] == [_weekly_fire(SATURDAY)]
    reading = exit_criteria.read_consecutive_cycles(cycles, required=1)
    assert reading.met is True, reading.detail
    assert "partial-run rule" in reading.detail
    assert "D16" in reading.detail and "D46" in reading.detail


def test_inside_the_grace_the_detail_names_no_rerun(tmp_path, units, prefixes):
    _week(tmp_path, prefixes, late={})
    reading = exit_criteria.read_consecutive_cycles(_weekly(tmp_path, units), required=1)
    assert reading.met is True, reading.detail
    assert "partial-run rule" not in reading.detail


def test_a_unit_still_missing_after_its_reruns_keeps_the_week_incomplete(tmp_path, units, prefixes):
    fire = _weekly_fire(SATURDAY)
    _week(tmp_path, prefixes, late={})
    for key in (tmp_path / prefixes["D16"]).rglob("*.json"):
        key.unlink()
    _manifest(tmp_path, prefixes["D16"], "FIRE", fire + dt.timedelta(minutes=30), status="failed")
    _manifest(tmp_path, prefixes["D16"], "RERUN", fire + dt.timedelta(hours=11), status="failed")
    reading = exit_criteria.read_consecutive_cycles(_weekly(tmp_path, units), required=1)
    assert reading.met is False, reading.detail
    assert "'D16'" in reading.detail and "'D46'" in reading.detail


def test_a_fire_that_never_ran_is_not_rescued_by_a_rerun(tmp_path, units, prefixes):
    """"The trigger still has to fire" (`standalone` docstring): every unit ok,
    but every manifest started the day AFTER the fire — a hand-started week,
    not a scheduled one finished by reruns."""
    fire = _weekly_fire(SATURDAY)
    for prefix in sorted(set(prefixes.values())):
        _manifest(tmp_path, prefix, "LATE", fire + dt.timedelta(days=1))
    cycles = _weekly(tmp_path, units)
    assert cycles.cycles[0].fired is False
    reading = exit_criteria.read_consecutive_cycles(cycles, required=1)
    assert reading.met is False, reading.detail
    assert "reruns of that week do not count" in reading.detail


def test_no_rerun_counts_past_the_partial_run_window(tmp_path, units, prefixes):
    """A run started after `PARTIAL_RUN_WINDOW` belongs to no earlier fire, so
    it can never complete one week and then the next."""
    _week(tmp_path, prefixes, late={prefixes["D16"]: standalone.PARTIAL_RUN_WINDOW + dt.timedelta(minutes=1)})
    friday_after = SATURDAY + dt.timedelta(days=6)
    reading = exit_criteria.read_consecutive_cycles(
        _weekly(
            tmp_path,
            units,
            trading_day=friday_after,
            now=dt.datetime.combine(friday_after, dt.time(20, 0), tzinfo=UTC),
        ),
        required=1,
    )
    assert reading.met is False, reading.detail
    assert "'D16'" in reading.detail


def test_the_eod_cycle_window_is_not_widened(tmp_path, units):
    """The rule is weekly-only, by the ruling: an EOD run started past its
    fire's grace still does not complete that fire."""
    eod = _schedule("data-collection-eod")
    minute, hour = eod["expression"].removeprefix("cron(").split(" ")[:2]
    thursday = dt.date(2026, 10, 8)
    fire = dt.datetime.combine(thursday, dt.time(int(hour), int(minute)), tzinfo=ET).astimezone(UTC)
    by_id = {u.unit_id: u for u in units}
    for uid in eod["input"]["verify_units"]:
        prefix = by_id[uid].run_manifest_prefix.removeprefix("data_collection/")
        offset = dt.timedelta(hours=7) if uid == eod["input"]["verify_units"][0] else dt.timedelta(minutes=5)
        path = tmp_path / prefix / thursday.isoformat() / f"{uid}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "schema_version": "data_run_manifest.v1", "run_id": uid, "unit_id": uid,
            "trigger": "scheduled", "trading_day": thursday.isoformat(), "status": "ok",
            "started": _iso(fire + offset), "finished": _iso(fire + offset + dt.timedelta(minutes=5)),
            "rows_out": 1, "guards": [],
        }))
    cycles = exit_criteria.collect_cycles(
        LocalStore(tmp_path), units, schedule_name="data-collection-eod", count=1,
        trading_day=thursday, now=fire + dt.timedelta(hours=8),
    )
    assert [c.fire for c in cycles.cycles] == [fire]
    assert cycles.cycles[0].partial_runs is False
    reading = exit_criteria.read_consecutive_cycles(cycles, required=1)
    assert reading.met is False, reading.detail
