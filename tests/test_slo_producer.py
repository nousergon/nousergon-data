"""`data_gate.producers.slo` — the emitter behind the 16 ``data.slo.*`` clauses
(`alpha-engine-config-I10789`).

Every verdict here is built from fixture run manifests, the same
`data_run_manifest.v1` shape the collector writes, read through the store the
producer reads in production. Nothing is graded from a HEAD.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest
from nousergon_lib.run_identity import new_run_id
from nousergon_lib.trading_calendar import subtract_trading_days

from data_gate import evidence
from data_gate.descriptors import Unit, load_units
from data_gate.producers import slo
from tests.data_gate_support import DeniedStore, EmptyStore

LAST_SESSION = dt.date(2026, 10, 2)  # a Friday
NOW = dt.datetime(2026, 10, 3, 1, 0, tzinfo=dt.timezone.utc)  # after Friday's 18:15 ET deadline
SESSIONS = [subtract_trading_days(LAST_SESSION, n) for n in range(slo.WINDOW)]  # newest first


def _unit(unit_id: str, *, family: str = "eod-spine", deadline: str | None = "18:15 America/New_York",
          trigger: dict | None = None, completeness: dict | None = None) -> Unit:
    raw = {
        "unit_id": unit_id,
        "lifecycle": "live",
        "run_manifest_prefix": f"data_collection/runs/{unit_id}",
        "trigger": trigger or {"kind": "step-functions", "schedule": "weekdays 16:00 America/New_York"},
        "freshness": {"family": family, **({"deadline": deadline} if deadline else {})},
        "completeness": completeness
        if completeness is not None
        else {"denominator": "metron/holdings_universe.json", "floor": "1.0", "status": "proposed"},
    }
    return Unit(unit_id=unit_id, path=pathlib.Path(f"{unit_id}.yaml"), raw=raw)


def _manifest(store: EmptyStore, unit_id: str, trading_day: dt.date, *, finished: dt.datetime,
              status: str = "ok", trigger: str = "scheduled", recorded_day: dt.date | None = None,
              cardinality: str | None = "ok") -> None:
    started = finished - dt.timedelta(minutes=2)
    run_id = new_run_id(started)
    guards = [] if cardinality is None else [
        {"guard": "data_cardinality", "verdict": cardinality, "value": 1.0, "baseline": 1.0, "key": None}
    ]
    store.objects[f"runs/{unit_id}/{trading_day.isoformat()}/{run_id}.json"] = json.dumps(
        {
            "schema_version": "data_run_manifest.v1",
            "unit_id": unit_id,
            "run_id": run_id,
            "trading_day": (recorded_day or trading_day).isoformat(),
            "started": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "finished": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": status,
            "trigger": trigger,
            "guards": guards,
        }
    ).encode()


def _eod(day: dt.date, hh: int, mm: int) -> dt.datetime:
    from zoneinfo import ZoneInfo

    return dt.datetime.combine(day, dt.time(hh, mm), tzinfo=ZoneInfo("America/New_York")).astimezone(
        dt.timezone.utc
    )


def _build(store, units):
    return slo.build_documents(store, units, now=NOW, code_sha="0" * 40)


def _fresh(documents, family="eod-spine"):
    return documents[slo.slo_key("freshness", family)]


def _complete(documents, family="eod-spine"):
    return documents[slo.slo_key("completeness", family)]


# ---------------------------------------------------------------------------
# Declarations: every family the descriptors declare can be graded.
# ---------------------------------------------------------------------------


def test_every_declared_family_has_a_session_convention():
    families = {u.freshness_family for u in load_units() if u.freshness_family and not u.retired}
    assert families <= set(slo.SESSION_CONVENTION), sorted(families - set(slo.SESSION_CONVENTION))


def test_every_scheduled_unit_declares_a_deadline_the_producer_can_parse():
    """A scheduled unit whose deadline this parser refuses would drop out of its
    family's grading; that is a code defect here, never a silent exclusion."""
    from data_gate.cadence import unit_cadence

    unparsed = []
    for unit in load_units():
        if not unit.freshness_family or unit.retired:
            continue
        cadence = unit_cadence(unit.raw)
        if cadence.kind in {"on_demand", "disabled", "undeclared"}:
            continue
        deadline = (unit.raw.get("freshness") or {}).get("deadline")
        if slo.parse_deadline(deadline, cadence, slot_minutes=unit.cadence_minutes).kind == "undeclared":
            unparsed.append((unit.unit_id, deadline))
    assert unparsed == []


def test_the_producer_writes_exactly_the_keys_the_clauses_read():
    units = load_units()
    documents, problems = slo.build_documents(EmptyStore(), units, now=NOW)
    assert problems == {}
    expected = {
        f"metrics/slo/{objective}/{family}/latest.json"
        for family in {u.freshness_family for u in units if u.freshness_family and not u.retired}
        for objective in ("freshness", "completeness")
    }
    assert set(documents) == expected
    assert len(expected) == 16
    # An empty store is an ANSWER: every row a breach, none ok.
    assert {d["status"] for d in documents.values()} == {"breach"}


# ---------------------------------------------------------------------------
# Deadline shapes and session conventions.
# ---------------------------------------------------------------------------


def test_deadline_shapes():
    from data_gate.cadence import Cadence, unit_cadence

    weekly = slo.parse_deadline("Sat 13:00 America/New_York", Cadence(kind="scheduled", source="x"))
    assert (weekly.kind, weekly.weekdays, weekly.hour) == ("weekly", frozenset({5}), 13)
    crypto = slo.parse_deadline(
        "23:00 UTC on trading days (R2 (a), alpha-engine-config-I11812)",
        unit_cadence({"trigger": {"schedule": "cron(30 22 ? * MON-FRI *)"}}),
    )
    assert (crypto.kind, crypto.tz, crypto.trading_days_only) == ("daily", "UTC", True)
    slots = slo.parse_deadline(
        "age <= 10 minutes in >= 99% of session slots", Cadence(kind="continuous", source="x"), slot_minutes=5
    )
    assert (slots.kind, slots.max_age_minutes, slots.slot_share, slots.slot_minutes) == ("slots", 10, 0.99, 5)
    assert slo.parse_deadline(None, Cadence(kind="scheduled", source="x")).kind == "undeclared"
    assert slo.parse_deadline("whenever", Cadence(kind="scheduled", source="x")).kind == "undeclared"


def test_session_conventions_match_what_the_producers_file():
    saturday, friday, monday = dt.date(2026, 10, 3), dt.date(2026, 10, 2), dt.date(2026, 10, 5)
    assert slo.expected_trading_day("on_or_before", saturday) == friday  # the weekly files Friday
    assert slo.expected_trading_day("previous", friday) == dt.date(2026, 10, 1)  # morning files T-1
    assert slo.expected_trading_day("on_or_after", saturday) == monday  # news files the next session
    assert slo.expected_trading_day("on_or_after", friday) == friday


def test_deadline_instants_skip_holidays_and_stop_at_now():
    deadline = slo.parse_deadline(
        "18:15 America/New_York", slo.unit_cadence({"trigger": {"schedule": "weekdays 16:00 America/New_York"}})
    )
    points = slo.deadline_instants(deadline, as_of=NOW, count=slo.WINDOW)
    assert [day for _, day in points] == SESSIONS
    assert all(instant <= NOW for instant, _ in points)


# ---------------------------------------------------------------------------
# Freshness.
# ---------------------------------------------------------------------------


def _all_on_time(store, unit_ids, *, skip=()):
    for day in SESSIONS:
        for unit_id in unit_ids:
            if (unit_id, day) in skip:
                continue
            _manifest(store, unit_id, day, finished=_eod(day, 17, 30))


def test_nineteen_of_twenty_on_time_is_ok():
    store = EmptyStore()
    units = [_unit("DX1"), _unit("DX2", completeness={"status": "not_applicable", "na_code": "N/A-MISSING-INPUT"})]
    _all_on_time(store, ["DX1", "DX2"], skip={("DX2", SESSIONS[7])})
    _manifest(store, "DX2", SESSIONS[7], finished=_eod(SESSIONS[7], 19, 0))  # late
    documents, problems = _build(store, units)
    assert problems == {}
    fresh = _fresh(documents)
    assert (fresh["status"], fresh["value"], fresh["cycles_graded"]) == ("ok", 19, 20)
    late = next(c for c in fresh["cycles"] if c["trading_day"] == SESSIONS[7].isoformat())
    assert late["met"] is False and late["misses"]["DX2"].startswith("late")


def test_two_misses_breach():
    store = EmptyStore()
    _all_on_time(store, ["DX1"], skip={("DX1", SESSIONS[3]), ("DX1", SESSIONS[9])})
    fresh = _fresh(_build(store, [_unit("DX1")])[0])
    assert (fresh["status"], fresh["value"]) == ("breach", 18)


def test_a_hand_repair_is_not_a_cycle_met_and_is_counted():
    store = EmptyStore()
    _all_on_time(store, ["DX1"], skip={("DX1", SESSIONS[0]), ("DX1", SESSIONS[1])})
    for day in SESSIONS[:2]:
        _manifest(store, "DX1", day, finished=_eod(day, 17, 0), trigger="manual")
    fresh = _fresh(_build(store, [_unit("DX1")])[0])
    assert (fresh["status"], fresh["value"]) == ("breach", 18)
    assert fresh["excluded_trigger_runs"] == {"manual": 2}


def test_a_run_recording_another_session_fails_the_as_of_check():
    store = EmptyStore()
    _all_on_time(store, ["DX1"], skip={("DX1", SESSIONS[4])})
    _manifest(store, "DX1", SESSIONS[4], finished=_eod(SESSIONS[4], 17, 0), recorded_day=SESSIONS[5])
    fresh = _fresh(_build(store, [_unit("DX1")])[0])
    assert fresh["status"] == "breach", "19/20 on time, but a stale-content cycle fails the 20-of-20 rule"
    assert (fresh["value"], fresh["as_of_matched"], fresh["as_of_checked"]) == (19, 19, 20)


def test_a_failed_run_is_a_miss():
    store = EmptyStore()
    _all_on_time(store, ["DX1"], skip={("DX1", SESSIONS[0]), ("DX1", SESSIONS[1])})
    for day in SESSIONS[:2]:
        _manifest(store, "DX1", day, finished=_eod(day, 17, 0), status="failed")
    fresh = _fresh(_build(store, [_unit("DX1")])[0])
    assert fresh["value"] == 18
    assert "1 failed" in fresh["cycles"][0]["misses"]["DX1"]


def test_a_short_history_is_a_breach_naming_the_window():
    store = EmptyStore()
    for day in SESSIONS[:5]:
        _manifest(store, "DX1", day, finished=_eod(day, 17, 0))
    fresh = _fresh(_build(store, [_unit("DX1")])[0])
    assert (fresh["status"], fresh["value"], fresh["attainment"]) == ("breach", 5, 0.25)


def test_a_family_with_no_scheduled_unit_is_a_breach_not_vacuously_ok():
    manual = _unit("DX9", family="weekly-membership", deadline=None, trigger={"kind": "manual"})
    documents, _ = _build(EmptyStore(), [manual])
    fresh, complete = _fresh(documents, "weekly-membership"), _complete(documents, "weekly-membership")
    assert fresh["status"] == complete["status"] == "breach"
    assert "DX9" in fresh["not_graded"]
    assert "no unit" in fresh["summary"]


def test_a_unit_without_a_deadline_inherits_its_familys_single_declared_one():
    store = EmptyStore()
    _all_on_time(store, ["DX1", "DX2"])
    documents, _ = _build(store, [_unit("DX1"), _unit("DX2", deadline=None)])
    assert _fresh(documents)["graded_units"] == ["DX1", "DX2"]


def test_a_denied_read_writes_no_verdict():
    documents, problems = _build(DeniedStore(), [_unit("DX1")])
    assert documents == {}
    assert "eod-spine" in problems and "AccessDenied" in problems["eod-spine"][0]


# ---------------------------------------------------------------------------
# Intraday slot rule.
# ---------------------------------------------------------------------------


def _intraday_unit() -> Unit:
    return _unit(
        "DX7",
        family="intraday",
        deadline="age <= 10 minutes in >= 99% of session slots",
        trigger={"kind": "systemd-timer", "cadence_minutes": 5},
        completeness={"status": "not_applicable", "na_code": "N/A-NOT-IMPL"},
    )


def _session_runs(store, day, *, gap: tuple[int, int] | None = None):
    moment = _eod(day, 9, 31)
    close = _eod(day, 16, 0)
    while moment <= close:
        minutes = (moment - _eod(day, 9, 30)).total_seconds() / 60
        if not (gap and gap[0] <= minutes < gap[1]):
            _manifest(store, "DX7", day, finished=moment, cardinality=None)
        moment += dt.timedelta(minutes=5)


def test_intraday_slots_fresh_all_session_is_met():
    store = EmptyStore()
    for day in SESSIONS:
        _session_runs(store, day)
    documents, problems = _build(store, [_intraday_unit()])
    assert problems == {}
    fresh = _fresh(documents, "intraday")
    assert (fresh["status"], fresh["value"]) == ("ok", 20)


def test_intraday_half_hour_gap_misses_the_session():
    store = EmptyStore()
    for day in SESSIONS:
        _session_runs(store, day, gap=(60, 90) if day in SESSIONS[:2] else None)
    fresh = _fresh(_build(store, [_intraday_unit()])[0], "intraday")
    assert (fresh["status"], fresh["value"]) == ("breach", 18)
    assert "session slots fresh" in fresh["cycles"][0]["misses"]["DX7"]


# ---------------------------------------------------------------------------
# Completeness.
# ---------------------------------------------------------------------------


def test_completeness_needs_a_clean_cardinality_record_every_cycle():
    store = EmptyStore()
    units = [_unit("DX1"), _unit("DX2", completeness={"status": "not_applicable", "na_code": "N/A-MISSING-INPUT"})]
    _all_on_time(store, ["DX1", "DX2"])
    complete = _complete(_build(store, units)[0])
    assert (complete["status"], complete["value"], complete["declared_exclusions"]) == ("ok", 20, ["DX2"])


def test_a_run_with_no_cardinality_record_is_a_miss_not_an_exclusion():
    store = EmptyStore()
    _all_on_time(store, ["DX1"], skip={("DX1", SESSIONS[2])})
    _manifest(store, "DX1", SESSIONS[2], finished=_eod(SESSIONS[2], 17, 0), cardinality=None)
    complete = _complete(_build(store, [_unit("DX1")])[0])
    assert (complete["status"], complete["value"]) == ("breach", 19)
    assert complete["cycles"][2]["misses"] == {"DX1": "no_cardinality_record"}


def test_below_floor_is_a_miss():
    store = EmptyStore()
    _all_on_time(store, ["DX1"], skip={("DX1", SESSIONS[0])})
    _manifest(store, "DX1", SESSIONS[0], finished=_eod(SESSIONS[0], 17, 0), cardinality="below_floor")
    complete = _complete(_build(store, [_unit("DX1")])[0])
    assert complete["cycles"][0]["misses"] == {"DX1": "below_floor"}


def test_a_family_whose_every_unit_is_excluded_is_a_breach_not_vacuously_ok():
    store = EmptyStore()
    _all_on_time(store, ["DX1"])
    unit = _unit("DX1", completeness={"status": "not_applicable", "na_code": "N/A-NOT-IMPL"})
    complete = _complete(_build(store, [unit])[0])
    assert complete["status"] == "breach"
    assert complete["declared_exclusions"] == ["DX1"]


# ---------------------------------------------------------------------------
# The gate side: what the clause renders from a document.
# ---------------------------------------------------------------------------


def test_the_clause_reads_an_emitted_document_and_carries_its_summary():
    store = EmptyStore()
    _all_on_time(store, ["DX1"])
    documents, _ = _build(store, [_unit("DX1")])
    gate_store = EmptyStore({k: json.dumps(v).encode() for k, v in documents.items()})
    reading = evidence.read_objective(gate_store, slo.slo_key("freshness", "eod-spine"), now=NOW)
    assert reading.met is True and reading.unmeasurable is False
    assert "20/20 cycles met the deadline" in reading.detail


def test_a_stale_document_is_unmeasurable_whatever_it_said():
    store = EmptyStore()
    _all_on_time(store, ["DX1"])
    documents, _ = _build(store, [_unit("DX1")])
    gate_store = EmptyStore({k: json.dumps(v).encode() for k, v in documents.items()})
    later = NOW + slo.STALE_AFTER + dt.timedelta(minutes=1)
    reading = evidence.read_objective(gate_store, slo.slo_key("freshness", "eod-spine"), now=later)
    assert reading.met is False and reading.unmeasurable is True
    assert "stale" in reading.detail


def test_an_unparseable_stale_after_is_unmeasurable():
    gate_store = EmptyStore(
        {"metrics/slo/freshness/x/latest.json": json.dumps({"status": "ok", "stale_after_utc": "soon"}).encode()}
    )
    reading = evidence.read_objective(gate_store, "metrics/slo/freshness/x/latest.json", now=NOW)
    assert reading.unmeasurable is True


def test_a_document_without_stale_after_is_graded_as_before():
    gate_store = EmptyStore({"metrics/cost/monthly/latest.json": json.dumps({"status": "ok", "value": 1}).encode()})
    reading = evidence.read_objective(gate_store, "metrics/cost/monthly/latest.json")
    assert reading.met is True and reading.unmeasurable is False


@pytest.mark.parametrize("argv", [["--store", "{tmp}", "--now", "2026-10-03T01:00:00Z"]])
def test_cli_writes_documents_and_a_run_record_to_the_store(tmp_path, argv):
    argv = [a.replace("{tmp}", str(tmp_path)) for a in argv]
    assert slo.main(argv) == 0
    written = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.json"))
    assert len([p for p in written if p.startswith("metrics/slo/")]) == 16
    assert [p for p in written if p.startswith("runs/slo/")], written
