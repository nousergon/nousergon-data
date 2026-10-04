"""A schedule firing after 17:59 ET is graded by the first reading for its own
day taken after its completion grace — by EVERY scheduled-cadence reader.

`alpha-engine-config-I11838`. `cadence.gate_moment` caps a reading for trading
day T at 23:59:59 ET on T, and `latest_due_fire` returns the newest fire whose
``fire + COMPLETION_GRACE`` (6h) has passed. A weekday schedule firing at 18:15
ET (`data-collection-eod`) therefore never had its own day's fire selected by a
T-scoped reading: `run_record`, `completeness` and the exit-criteria cycle
counters graded T-1's run, while `survives_phase4` (fixed alone by
`alpha-engine-config-I11354`) graded T's — two different runs in one reading.
The weekend made it worst: a missed FRIDAY run read green on `run_record` from
Saturday through Monday's 23:30Z reading, because Thursday's run was ok.

The case pinned here is Brian's: a Friday 18:15 ET miss is graded the SAME
night, by a reading for Friday taken once the grace has expired (00:15 ET
Saturday), and by every scheduled reading after that.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib
from zoneinfo import ZoneInfo

import pytest

from data_gate import cadence, descriptors, evidence, exit_criteria, standalone
from data_gate.store import LocalStore

UTC = dt.timezone.utc
ET = ZoneInfo("America/New_York")
THURSDAY = dt.date(2026, 10, 1)
FRIDAY = dt.date(2026, 10, 2)
SATURDAY = dt.date(2026, 10, 3)


def _eod_schedule() -> dict:
    return next(s for s in standalone._stack_schedules() if s["name"] == "data-collection-eod")


def _eod_time() -> dt.time:
    """The EOD fire time, read from the TEMPLATE so a schedule move cannot
    silently turn these tests into tests of a pre-17:59 fire."""
    minute, hour = _eod_schedule()["expression"].removeprefix("cron(").split(" ")[:2]
    return dt.time(int(hour), int(minute))


def _fire(day: dt.date) -> dt.datetime:
    return dt.datetime.combine(day, _eod_time(), tzinfo=ET).astimezone(UTC)


def _label(moment: dt.datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%MZ")


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


#: 00:30 ET Saturday: the first quarter-hour after Friday's fire + 6h grace.
SAME_NIGHT = dt.datetime.combine(SATURDAY, dt.time(0, 30), tzinfo=ET).astimezone(UTC)
#: `data-gate.yml`'s own readings that resolve to trading day FRIDAY.
FRI_2330Z = dt.datetime(2026, 10, 2, 23, 30, tzinfo=UTC)  # 19:30 ET Fri — inside the grace
SAT_0130Z = dt.datetime(2026, 10, 3, 1, 30, tzinfo=UTC)  # 21:30 ET Fri — inside the grace
SAT_1800Z = dt.datetime(2026, 10, 3, 18, 0, tzinfo=UTC)  # the Saturday backstop
MON_1230Z = dt.datetime(2026, 10, 5, 12, 30, tzinfo=UTC)  # `--trading-day yesterday` -> Fri


@pytest.fixture(scope="module")
def eod_unit() -> descriptors.Unit:
    """D19, declaring the fire time the stack actually schedules it at.

    On main D19 still declares the v1 16:00 ET slot (nousergon-data-PR2024
    moves it to 18:15); this test is about the reader, so it pins the unit to
    the template's fire rather than to whichever descriptor merges first.
    """
    base = next(u for u in descriptors.load_units() if u.unit_id == "D19")
    t = _eod_time()
    trigger = {**base.raw["trigger"], "schedule": f"weekdays {t.hour:02d}:{t.minute:02d} America/New_York"}
    unit = dataclasses.replace(base, raw={**base.raw, "trigger": trigger})
    assert cadence.unit_cadence(unit.raw).kind == "scheduled"
    return unit


def test_the_eod_schedule_fires_after_1759_et():
    """The premise. If the EOD fire ever moves back inside 17:59 ET the cases
    below stop exercising the boundary, and that must be a visible change."""
    assert _eod_time() > dt.time(17, 59)


def _manifest(root: pathlib.Path, unit_id: str, day: dt.date, run_id: str) -> None:
    started = _fire(day) + dt.timedelta(minutes=2)
    doc = {
        "schema_version": "data_run_manifest.v1",
        "run_id": run_id,
        "unit_id": unit_id,
        "trigger": "scheduled",
        "trading_day": day.isoformat(),
        "status": "ok",
        "started": _iso(started),
        "finished": _iso(started + dt.timedelta(minutes=4)),
        "rows_out": 1,
        "guards": [],
    }
    path = root / "runs" / unit_id / day.isoformat() / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc))


# ── the shared ceiling ──────────────────────────────────────────────────────


@pytest.mark.parametrize("now", [SAME_NIGHT, SAT_1800Z, MON_1230Z], ids=["00:30ET-sat", "sat-18z", "mon-1230z"])
def test_fridays_fire_is_selected_once_its_grace_has_passed(now):
    eod = cadence.parse_cron(_eod_schedule()["expression"], tz=_eod_schedule()["timezone"], trading_days_only=True)
    assert cadence.due_fire(eod, trading_day=FRIDAY, now=now) == _fire(FRIDAY)


@pytest.mark.parametrize("now", [FRI_2330Z, SAT_0130Z], ids=["fri-2330z", "sat-0130z"])
def test_fridays_fire_is_not_demanded_inside_its_grace(now):
    """The clock still bounds the ceiling: a run legitimately in flight is never
    graded as missed."""
    eod = cadence.parse_cron(_eod_schedule()["expression"], tz=_eod_schedule()["timezone"], trading_days_only=True)
    assert cadence.due_fire(eod, trading_day=FRIDAY, now=now) == _fire(THURSDAY)


def test_no_fire_after_the_end_of_the_trading_day_is_reachable():
    """``fire + grace <= end + grace`` — a reading for Friday can never select a
    Saturday fire, however late it is taken."""
    weekly = cadence.parse_cron("cron(0 5 ? * SAT *)", tz="America/New_York", trading_days_only=False)
    late = dt.datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
    assert cadence.due_fire(weekly, trading_day=FRIDAY, now=late) < dt.datetime.combine(
        SATURDAY, dt.time(0, 0), tzinfo=ET
    )


# ── run_record ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("now", [SAME_NIGHT, SAT_1800Z, MON_1230Z], ids=["00:30ET-sat", "sat-18z", "mon-1230z"])
def test_a_missed_friday_eod_run_reads_unmet_the_same_night(tmp_path, eod_unit, now):
    """THE defect. Thursday ran, Friday did not. Before I11838 every one of these
    readings graded Thursday's run and read MET until Monday 23:30Z."""
    _manifest(tmp_path, "D19", THURSDAY, "THU")
    reading = evidence.read_run_record(LocalStore(tmp_path), eod_unit, trading_day=FRIDAY, now=now)
    assert reading.met is False and reading.unmeasurable is False, reading.detail
    assert _label(_fire(FRIDAY)) in reading.detail


def test_a_friday_eod_run_that_happened_reads_met_the_same_night(tmp_path, eod_unit):
    _manifest(tmp_path, "D19", FRIDAY, "FRI")
    reading = evidence.read_run_record(LocalStore(tmp_path), eod_unit, trading_day=FRIDAY, now=SAME_NIGHT)
    assert reading.met is True, reading.detail
    assert reading.evidence and FRIDAY.isoformat() in reading.evidence[0]


def test_a_run_started_after_midnight_still_counts_toward_its_fire(tmp_path, eod_unit):
    """`manifest_ceiling`: the fire's own completion window, not the end of the
    day, bounds the runs counted toward an 18:15 ET fire."""
    _manifest(tmp_path, "D19", FRIDAY, "FRI")
    path = tmp_path / "runs" / "D19" / FRIDAY.isoformat() / "FRI.json"
    doc = json.loads(path.read_text())
    doc["started"] = _iso(_fire(FRIDAY) + dt.timedelta(hours=5, minutes=50))  # 00:05 ET Saturday
    path.write_text(json.dumps(doc))
    reading = evidence.read_run_record(LocalStore(tmp_path), eod_unit, trading_day=FRIDAY, now=SAT_1800Z)
    assert reading.met is True, reading.detail


def test_inside_the_grace_fridays_run_is_not_yet_demanded(tmp_path, eod_unit):
    _manifest(tmp_path, "D19", THURSDAY, "THU")
    reading = evidence.read_run_record(LocalStore(tmp_path), eod_unit, trading_day=FRIDAY, now=SAT_0130Z)
    assert reading.met is True, reading.detail
    assert reading.evidence == (f"runs/D19/{THURSDAY.isoformat()}/THU.json",)


def test_a_saturday_weekly_run_is_not_counted_toward_the_previous_saturday(tmp_path):
    """The widened SELECTION ceiling must not widen the manifest window: read for
    Friday on Saturday afternoon, the weekly fire due is LAST Saturday's, and
    this morning's run (a different fire) must not stand in for a missed one."""
    d01 = next(u for u in descriptors.load_units() if u.unit_id == "D01")
    this_morning = dt.datetime.combine(SATURDAY, dt.time(5, 2), tzinfo=ET).astimezone(UTC)
    path = tmp_path / "runs" / "D01" / FRIDAY.isoformat() / "SAT.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({
        "schema_version": "data_run_manifest.v1", "run_id": "SAT", "unit_id": "D01",
        "trigger": "scheduled", "trading_day": FRIDAY.isoformat(), "status": "ok",
        "started": _iso(this_morning), "finished": _iso(this_morning + dt.timedelta(hours=1)),
        "rows_out": 1, "guards": [],
    }))
    reading = evidence.read_run_record(LocalStore(tmp_path), d01, trading_day=FRIDAY, now=SAT_1800Z)
    assert reading.met is False, reading.detail
    assert "2026-09-26T09:00Z" in reading.detail


# ── completeness ────────────────────────────────────────────────────────────


def test_completeness_falls_back_to_friday_the_same_night(eod_unit):
    assert evidence.completeness_due_day(eod_unit, trading_day=FRIDAY, now=SAME_NIGHT) == FRIDAY
    assert evidence.completeness_due_day(eod_unit, trading_day=FRIDAY, now=MON_1230Z) == FRIDAY
    assert evidence.completeness_due_day(eod_unit, trading_day=FRIDAY, now=FRI_2330Z) == THURSDAY


# ── exit-criteria cycle counters ────────────────────────────────────────────


def test_the_eod_cycle_counter_grades_fridays_fire_the_same_night(tmp_path, eod_unit):
    _manifest(tmp_path, "D19", THURSDAY, "THU")
    # The schedule's verify_units must all have descriptors; only D19 is the
    # one whose manifests this test writes.
    units = [eod_unit if u.unit_id == "D19" else u for u in descriptors.load_units()]
    cycles = exit_criteria.collect_cycles(
        LocalStore(tmp_path), units, schedule_name="data-collection-eod",
        count=2, trading_day=FRIDAY, now=SAME_NIGHT,
    )
    assert not cycles.unmeasurable, cycles.reason
    assert [c.fire for c in cycles.cycles] == [_fire(FRIDAY), _fire(THURSDAY)]
    reading = exit_criteria.read_consecutive_cycles(cycles, required=1)
    assert reading.met is False, reading.detail


# ── one reading, one run ────────────────────────────────────────────────────


class _Scheduler:
    def get_schedule(self, GroupName, Name):  # noqa: N803 — boto3 kwarg
        schedule = next(s for s in standalone._stack_schedules() if s["name"] == Name)
        machine = Name.replace("data-collection-", "ne-data-collection-")
        return {
            "State": "ENABLED",
            "ScheduleExpression": schedule["expression"],
            "ScheduleExpressionTimezone": schedule["timezone"],
            "Target": {"Arn": f"arn:aws:states:us-east-1:000000000000:stateMachine:{machine}"},
        }


class _Sfn:
    def __init__(self, executions):
        self.executions = executions

    def list_executions(self, stateMachineArn, maxResults, nextToken=None):  # noqa: N803
        return {"executions": self.executions}


def test_survives_phase4_and_run_record_grade_the_same_fire(tmp_path, eod_unit):
    """The inconsistency the issue names: in ONE reading, `survives_phase4`
    graded Friday's run while `run_record` graded Thursday's."""
    _manifest(tmp_path, "D19", THURSDAY, "THU")
    thursday = _fire(THURSDAY) + dt.timedelta(seconds=3)
    store = LocalStore(tmp_path)
    store.scheduler_client = _Scheduler()
    store.sfn_client = _Sfn([{
        "executionArn": "arn:aws:states:us-east-1:000000000000:execution:ne-data-collection-eod:thu",
        "status": "SUCCEEDED", "startDate": thursday, "stopDate": thursday + dt.timedelta(minutes=45),
    }])
    survives = standalone.read_survives_phase4(store, eod_unit, trading_day=FRIDAY, now=SAME_NIGHT)
    record = evidence.read_run_record(store, eod_unit, trading_day=FRIDAY, now=SAME_NIGHT)
    friday = _label(_fire(FRIDAY))
    assert friday in survives.detail and friday in record.detail, (survives.detail, record.detail)
    assert survives.met is False and record.met is False
