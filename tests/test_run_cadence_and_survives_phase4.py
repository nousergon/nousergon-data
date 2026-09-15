"""Run evidence graded against what each unit DECLARES about when it runs.

`alpha-engine-config-I10871` (run_record cadence) and `alpha-engine-config-
I10870` (the survives_phase4 reader). Each test makes one state happen that the
old gate-day-only reader got wrong, or one of the three survives_phase4 states.
No AWS: live Scheduler / Step Functions state is a fake attached to a LocalStore.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest

from data_gate import cadence, descriptors, evidence, standalone
from data_gate.store import LocalStore
from tests.data_gate_support import EmptyStore

UTC = dt.timezone.utc
TUESDAY = dt.date(2026, 9, 15)
WEDNESDAY_NOON = dt.datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def units() -> dict[str, descriptors.Unit]:
    return {u.unit_id: u for u in descriptors.load_units()}


def _write(root: pathlib.Path, unit_id: str, folder: str, run_id: str, **over) -> None:
    doc = {
        "schema_version": "data_run_manifest.v1",
        "run_id": run_id,
        "unit_id": unit_id,
        "trigger": "scheduled",
        "trading_day": folder,
        "status": "ok",
        "started": f"{folder}T21:00:00Z",
        "finished": f"{folder}T21:10:00Z",
        "rows_out": 1,
        "guards": [],
        **over,
    }
    path = root / "runs" / unit_id / folder / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc))


# ── cadence declarations ────────────────────────────────────────────────────


def test_every_descriptor_cadence_is_classified_without_raising(units):
    kinds = {uid: cadence.unit_cadence(u.raw).kind for uid, u in units.items()}
    assert kinds["D01"] == "scheduled" and kinds["D19"] == "scheduled"
    assert kinds["D35"] == "on_demand" and kinds["D43"] == "on_demand"
    assert kinds["D37"] == "continuous"


def test_every_stack_schedule_expression_parses():
    for schedule in standalone._stack_schedules():
        parsed = cadence.parse_cron(schedule["expression"], tz=schedule["timezone"], trading_days_only=False)
        assert parsed.weekdays, schedule["qualified_name"]


def test_a_weekday_schedule_skips_an_nyse_holiday(units):
    eod = cadence.unit_cadence(units["D19"].raw)
    labor_day_evening = dt.datetime(2026, 9, 8, 12, 0, tzinfo=UTC)
    fire = cadence.latest_due_fire(eod, as_of=cadence.gate_moment(dt.date(2026, 9, 7), labor_day_evening))
    assert fire.date() == dt.date(2026, 9, 4)


# ── run_record, by cadence ──────────────────────────────────────────────────


def test_a_weekly_unit_on_a_tuesday_reads_saturdays_run(tmp_path, units):
    """The defect: D01 read UNMET on 09-15 for want of runs/D01/2026-09-15/."""
    _write(tmp_path, "D01", "2026-09-11", "RUN1", started="2026-09-12T10:00:00Z", finished="2026-09-12T11:00:00Z")
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D01"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is True, reading.detail


def test_a_weekly_unit_whose_saturday_run_is_missing_is_unmet(tmp_path, units):
    _write(tmp_path, "D01", "2026-09-04", "OLD", started="2026-09-05T10:00:00Z")
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D01"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is False and reading.unmeasurable is False
    assert "2026-09-12T09:00Z" in reading.detail


def test_an_on_demand_unit_with_an_old_manifest_reads_its_latest_invocation(tmp_path, units):
    _write(tmp_path, "D35", "2026-08-03", "RUN1")
    # D35 writes ArcticDB, so its evidence is also the probe for the day the run
    # COLLECTED — the manifest's trading_day, not the gate's.
    probe = tmp_path / "probes" / "arctic" / "2026-08-03.json"
    probe.parent.mkdir(parents=True)
    probe.write_text(json.dumps({"libraries": {"universe": {"read_ok": True, "row_count": 1}}}))
    reading =evidence.read_run_record(LocalStore(tmp_path), units["D35"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is True and "2026-08-03" in reading.evidence[0]


def test_an_on_demand_unit_never_invoked_is_not_applicable(tmp_path, units):
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D35"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is True and reading.detail.startswith("not applicable")


def test_a_daily_unit_missing_today_is_unmet(tmp_path, units):
    _write(tmp_path, "D19", "2026-09-14", "MONDAY")
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is False and reading.unmeasurable is False


def test_a_daily_unit_is_not_due_before_its_run_and_grace(tmp_path, units):
    """At 10:00 ET the gate cannot demand tonight's 16:45 run: Monday's counts."""
    _write(tmp_path, "D19", "2026-09-14", "MONDAY")
    morning = dt.datetime(2026, 9, 15, 14, 0, tzinfo=UTC)
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TUESDAY, now=morning)
    assert reading.met is True, reading.detail


def test_an_undeclared_cadence_is_named_not_guessed(tmp_path, units):
    unit = units["D36"]
    assert cadence.unit_cadence(unit.raw).kind == "undeclared"
    reading = evidence.read_run_record(LocalStore(tmp_path), unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is False and "I10871" in reading.detail


def test_a_denied_listing_is_unmeasurable_for_every_cadence(units):
    from tests.data_gate_support import DeniedStore

    for uid in ("D01", "D19", "D35", "D36"):
        reading = evidence.read_run_record(DeniedStore(), units[uid], trading_day=TUESDAY, now=WEDNESDAY_NOON)
        assert reading.unmeasurable is True, uid


# ── survives_phase4 ─────────────────────────────────────────────────────────


class _Denied(Exception):
    response = {"Error": {"Code": "AccessDeniedException"}}


class _Scheduler:
    def __init__(self, state="ENABLED", deny=False):
        self.state, self.deny = state, deny

    def get_schedule(self, GroupName, Name):  # noqa: N803 — boto3 kwarg
        if self.deny:
            raise _Denied("not authorized to perform scheduler:GetSchedule")
        machine = Name.replace("data-collection-", "ne-data-collection-")
        schedule = next(s for s in standalone._stack_schedules() if s["name"] == Name)
        return {
            "State": self.state,
            "ScheduleExpression": schedule["expression"],
            "ScheduleExpressionTimezone": schedule["timezone"],
            "Target": {"Arn": f"arn:aws:states:us-east-1:000000000000:stateMachine:{machine}"},
        }


class _Sfn:
    def __init__(self, executions):
        self.executions = executions

    def list_executions(self, stateMachineArn, maxResults, nextToken=None):  # noqa: N803
        return {"executions": self.executions.get(stateMachineArn.rsplit(":", 1)[-1], [])}


def _live_store(root, scheduler, sfn):
    store = LocalStore(root)
    store.scheduler_client = scheduler
    store.sfn_client = sfn
    return store


EOD_EXECUTION = {
    "executionArn": "arn:aws:states:us-east-1:000000000000:execution:ne-data-collection-eod:x",
    "status": "SUCCEEDED",
    "startDate": dt.datetime(2026, 9, 15, 20, 45, 3, tzinfo=UTC),
    "stopDate": dt.datetime(2026, 9, 15, 21, 30, tzinfo=UTC),
}


def test_the_stack_definition_maps_units_to_schedules():
    assert [s["name"] for s in standalone.covering_schedules("D19")] == ["data-collection-eod"]
    assert {s["name"] for s in standalone.covering_schedules("D17")} == {
        "data-collection-morning",
        "data-collection-weekly",
    }


def test_survives_phase4_is_unmeasurable_without_live_clients(units):
    reading = standalone.read_survives_phase4(EmptyStore(), units["D19"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.unmeasurable is True


def test_survives_phase4_is_unmeasurable_on_a_denied_schedule_read(tmp_path, units):
    store = _live_store(tmp_path, _Scheduler(deny=True), _Sfn({}))
    reading = standalone.read_survives_phase4(store, units["D19"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.unmeasurable is True and "GetSchedule" in reading.detail


def test_survives_phase4_is_unmet_while_the_schedule_is_disabled(tmp_path, units):
    """The honest pre-cutover state: UNMET, naming the disabled schedule."""
    store = _live_store(tmp_path, _Scheduler(state="DISABLED"), _Sfn({}))
    reading = standalone.read_survives_phase4(store, units["D19"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is False and reading.unmeasurable is False
    assert "DISABLED" in reading.detail and "data-collection-eod" in reading.detail


def test_survives_phase4_is_met_on_a_standalone_manifest_inside_a_succeeded_execution(tmp_path, units):
    _write(tmp_path, "D19", "2026-09-15", "RUN1", started="2026-09-15T20:50:00Z", finished="2026-09-15T21:05:00Z")
    store = _live_store(tmp_path, _Scheduler(), _Sfn({"ne-data-collection-eod": [EOD_EXECUTION]}))
    reading = standalone.read_survives_phase4(store, units["D19"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is True, reading.detail
    assert EOD_EXECUTION["executionArn"] in reading.evidence


def test_a_hand_run_outside_the_execution_does_not_count(tmp_path, units):
    _write(tmp_path, "D19", "2026-09-15", "HAND", started="2026-09-15T23:00:00Z", finished="2026-09-15T23:05:00Z")
    store = _live_store(tmp_path, _Scheduler(), _Sfn({"ne-data-collection-eod": [EOD_EXECUTION]}))
    reading = standalone.read_survives_phase4(store, units["D19"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is False and "no scheduled-trigger manifest" in reading.detail


def test_a_failed_execution_is_unmet(tmp_path, units):
    store = _live_store(tmp_path, _Scheduler(), _Sfn({"ne-data-collection-eod": [{**EOD_EXECUTION, "status": "FAILED"}]}))
    reading = standalone.read_survives_phase4(store, units["D19"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is False and "FAILED" in reading.detail


def test_an_sf_only_unit_the_stack_does_not_cover_is_unmet(units):
    uncovered = [
        u for u in units.values()
        if not u.retired and (u.raw.get("trigger") or {}).get("kind") == "step-functions"
        and not standalone.covering_schedules(u.unit_id)
    ]
    for unit in uncovered:
        reading = standalone.read_survives_phase4(EmptyStore(), unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)
        assert reading.met is False and reading.unmeasurable is False, unit.unit_id


def test_a_list_valued_contract_declaration_is_read_whole(units, tmp_path):
    """D20-D22 and D37 declare `contract.schema` (and D37 `producer_test`) as
    LISTS; the scalar-only reader returned zero files for them."""
    d37 = units["D37"]
    files = evidence._declared_schema_files(d37)
    assert set(files) >= set(d37.raw["contract"]["schema"]) | set(d37.raw["contract"]["producer_test"])
    synthetic = descriptors.Unit(
        unit_id="DXX",
        path=d37.path,
        raw={**d37.raw, "contract": {"schema": ["a.schema.json", "metron:x.py"], "producer_test": "t.py::case"}},
    )
    assert evidence._declared_schema_files(synthetic) == ["a.schema.json", "t.py"]
    for uid in ("D20", "D21", "D22"):
        assert len(evidence._declared_schema_files(units[uid])) >= 2, uid


def test_d43_in_region_runs_declare_the_component_identity(units):
    from data_gate import unit_readers

    config = unit_readers._load_identities(unit_readers.WRITER_IDENTITIES_PATH)
    runs_on = units["D43"].raw["trigger"]["runs_on"]
    assert config["by_runs_on"][runs_on] == config["by_runs_on"]["ec2-spot"] == "nousergon-data-collection-box-role"
    reading = unit_readers.read_identity(EmptyStore(), units["D43"])
    assert "no workload identity is declared" not in reading.detail


def test_an_on_demand_unit_has_no_schedule_to_lose(units):
    reading = standalone.read_survives_phase4(EmptyStore(), units["D43"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is True and reading.detail.startswith("not applicable")
