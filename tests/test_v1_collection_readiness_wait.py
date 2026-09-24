"""The v1 state machines wait on the standalone collector — derived, not remembered.

alpha-engine-config-I11264 (the bounded manifest-readiness waits), -I11266 (the
heal loop starts ne-data-collection-eod; no v1 definition invokes the data-spot
dispatcher) and -I11363 (the EOD ordering is derived from declared caps), all
inside the decoupled data cutover, alpha-engine-config-I11269.

Every number the three `WaitForCollectionManifests` blocks carry — the units,
the lookback, the poll budget — is recomputed here from the collection stack
template, the v1 trigger schedules, the unit descriptors and the dispatcher's
declared runtime caps, so moving a cron, adding a verify_unit or changing a cap
fails this file instead of silently mis-sizing a consumer's wait.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import pathlib
from zoneinfo import ZoneInfo

import pytest

from data_gate.descriptors import load_units
from infrastructure import data_collection_stack as stack

REPO = pathlib.Path(__file__).resolve().parent.parent
INFRA = REPO / "infrastructure"
ET = ZoneInfo("America/New_York")
PT = ZoneInfo("America/Los_Angeles")
DISPATCHER = "alpha-engine-data-spot-dispatcher"
PROBE = "alpha-engine-collection-readiness-probe"
POLL_SECONDS = 300

#: v1 definition -> (its machine name, the standalone schedule it waits on).
V1 = {
    "step_function.json": ("ne-weekly-freshness-pipeline", "data-collection-weekly"),
    "step_function_daily.json": ("ne-preopen-trading-pipeline", "data-collection-morning"),
    "step_function_eod.json": ("ne-postclose-trading-pipeline", "data-collection-eod"),
}

#: Which standalone workload writes each unit a v1 wait reads. Declared once in
#: the stack helper, and checked for completeness below: a waited unit with no
#: declared writer fails, and so does a writer that its schedule does not run.
WRITER = stack.UNIT_WRITERS


@pytest.fixture(scope="module")
def schedules():
    return {s["name"]: s for s in stack.schedules(stack.load_template())}


@pytest.fixture(scope="module")
def owners():
    return {u.unit_id: str((u.raw.get("trigger") or {}).get("owner") or "") for u in load_units()}


def _definition(name: str) -> dict:
    return json.loads((INFRA / name).read_text(encoding="utf-8"))


def _all_states(states: dict):
    for name, state in states.items():
        yield name, state
        for branch in state.get("Branches", []):
            yield from _all_states(branch["States"])
        for key in ("Iterator", "ItemProcessor"):
            if key in state:
                yield from _all_states(state[key]["States"])


def expected_wait_units(definition: dict, verify_units: list[str], owners: dict[str, str]) -> list[str]:
    """The schedule's verify_units, minus any unit whose v1 owner STAGE is still
    in this definition — that stage still produces it inline (DataPhase2 and
    RAGIngestion for D15/D16/D46, alpha-engine-config-I10753's own cutover)."""
    present = {name for name, _ in _all_states(definition["States"])}
    out = []
    for unit in verify_units:
        _machine, _, stage = owners.get(unit, "").partition(":")
        if stage and stage in present:
            continue
        out.append(unit)
    return out


def wait_unit_problems(definition: dict, schedule_input: dict, owners: dict[str, str]) -> list[str]:
    payload = definition["States"]["WaitForCollectionManifests"]["Parameters"]["Payload"]
    want = expected_wait_units(definition, schedule_input["verify_units"], owners)
    problems = []
    if payload["units"] != want:
        problems.append(f"units {payload['units']} != derived {want}")
    if payload["collection"] != schedule_input["collection"]:
        problems.append(f"collection {payload['collection']!r} != {schedule_input['collection']!r}")
    return problems


# ── the units ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("filename", sorted(V1))
def test_each_wait_reads_exactly_the_units_its_schedule_verifies(filename, schedules, owners):
    _machine, schedule = V1[filename]
    assert wait_unit_problems(_definition(filename), schedules[schedule]["input"], owners) == []


def test_a_unit_added_to_a_schedule_fails_its_consumer(schedules, owners):
    """Mutation: the pin is not vacuous — a new EOD verify_unit that the
    postclose wait does not read is caught."""
    schedule_input = copy.deepcopy(schedules["data-collection-eod"]["input"])
    schedule_input["verify_units"].append("D48")
    problems = wait_unit_problems(_definition("step_function_eod.json"), schedule_input, owners)
    assert problems and "D48" in problems[0]


def test_the_weekly_wait_excludes_only_units_whose_v1_stage_still_runs(schedules, owners):
    weekly = schedules["data-collection-weekly"]["input"]["verify_units"]
    units = _definition("step_function.json")["States"]["WaitForCollectionManifests"][
        "Parameters"]["Payload"]["units"]
    assert sorted(set(weekly) - set(units)) == ["D15", "D16", "D46"]
    assert {owners[u] for u in ("D15", "D16", "D46")} == {
        "ne-weekly-freshness-pipeline:DataPhase2",
        "ne-weekly-freshness-pipeline:RAGIngestion",
    }


@pytest.mark.parametrize("schedule", sorted(WRITER))
def test_every_waited_unit_has_a_declared_writer_its_schedule_runs(schedule, schedules):
    workloads = schedules[schedule]["input"]["workloads"]
    for filename, (_m, sched) in V1.items():
        if sched != schedule:
            continue
        units = _definition(filename)["States"]["WaitForCollectionManifests"]["Parameters"][
            "Payload"]["units"]
        for unit in units:
            assert unit in WRITER[schedule], f"{filename}: {unit} has no declared writer"
            assert WRITER[schedule][unit] in workloads, (schedule, unit)


# ── the lookback ─────────────────────────────────────────────────────────────


def _local(hour: int, minute: int, tz: ZoneInfo, day: dt.date) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(hour, minute), tzinfo=tz)


def _cron(schedule: dict) -> tuple[int, int]:
    minute, hour = schedule["expression"][len("cron("):].split()[:2]
    return int(hour), int(minute)


# One EDT and one EST weekday, so a DST-dependent derivation is exercised both ways.
_DAYS = (dt.date(2026, 9, 29), dt.date(2026, 12, 1))


@pytest.mark.parametrize("day", _DAYS)
def test_the_preopen_lookback_is_the_gap_between_the_two_crons(day, schedules):
    """The preopen SF (alpha-engine-weekday, cron(15 5) America/Los_Angeles)
    starts AFTER data-collection-morning; a manifest finished in between is
    this morning's and must count."""
    morning = schedules["data-collection-morning"]
    assert morning["timezone"] == "America/New_York"
    h, m = _cron(morning)
    collection = _local(h, m, ET, day)
    preopen = _local(5, 15, PT, day)
    payload = _definition("step_function_daily.json")["States"]["WaitForCollectionManifests"][
        "Parameters"]["Payload"]
    assert payload["lookback_seconds"] == int((preopen - collection).total_seconds()) == 2700


@pytest.mark.parametrize("day", _DAYS)
def test_the_postclose_and_weekly_lookback_is_zero_because_they_start_first(day, schedules):
    eod_h, eod_m = _cron(schedules["data-collection-eod"])
    assert _local(16, 0, ET, day) < _local(eod_h, eod_m, ET, day)  # daemon shutdown at the close
    wk_h, wk_m = _cron(schedules["data-collection-weekly"])
    saturday = day + dt.timedelta(days=(5 - day.weekday()) % 7)
    v1_weekly = dt.datetime.combine(saturday, dt.time(9, 0), tzinfo=dt.timezone.utc)
    assert v1_weekly <= _local(wk_h, wk_m, ET, saturday)
    for filename in ("step_function_eod.json", "step_function.json"):
        payload = _definition(filename)["States"]["WaitForCollectionManifests"]["Parameters"][
            "Payload"]
        assert payload["lookback_seconds"] == 0, filename
        assert payload["not_before.$"] == "$$.Execution.StartTime"


# ── the budget ───────────────────────────────────────────────────────────────


def _max_polls(filename: str) -> int:
    choices = _definition(filename)["States"]["CheckCollectionReadinessBudget"]["Choices"]
    attempts = "$.collection_readiness_poll.attempts"
    bound = [
        leaf for c in choices for leaf in c.get("And", [c])
        if leaf.get("Variable") == attempts and "NumericGreaterThanEquals" in leaf
    ]
    assert len(bound) == 1, choices
    return int(bound[0]["NumericGreaterThanEquals"])


def _poll_wait(filename: str) -> int:
    return int(_definition(filename)["States"]["CollectionReadinessPollWait"]["Seconds"])


def _worst_case_through_last_waited_unit(filename: str, schedules) -> int:
    _machine, schedule = V1[filename]
    units = _definition(filename)["States"]["WaitForCollectionManifests"]["Parameters"][
        "Payload"]["units"]
    return stack.worst_case_through_units(schedules[schedule], units)


def _ceil_polls(seconds: int) -> int:
    return -(-seconds // POLL_SECONDS)


def test_the_postclose_budget_covers_the_eod_collection_until_its_caps(schedules):
    """I11363: from the ~16:00 ET postclose start, through data-collection-eod's
    cron, through the declared caps of every workload up to the last one that
    writes a waited unit. Exactly that, rounded up to a whole poll."""
    eod_h, eod_m = _cron(schedules["data-collection-eod"])
    lead = (eod_h * 60 + eod_m - 16 * 60) * 60
    need = lead + _worst_case_through_last_waited_unit("step_function_eod.json", schedules)
    assert _poll_wait("step_function_eod.json") == POLL_SECONDS
    assert _max_polls("step_function_eod.json") == _ceil_polls(need) == 59


@pytest.mark.parametrize("day", _DAYS)
def test_the_postclose_ceiling_covers_the_wait_and_one_heal_before_the_cost_guard(day):
    """The EOD definition's whole-execution ceiling (pinned, with its
    derivation, in tests/test_sf_structural_contract.py) is re-derived here from
    the two bounds it has to hold: the readiness wait's budget plus one full
    HealStartCollection, and still ending before the 22:00 PT
    alpha-engine-stop-trading cost guard."""
    eod = _definition("step_function_eod.json")
    wait = _max_polls("step_function_eod.json") * _poll_wait("step_function_eod.json")
    heal = eod["States"]["HealStartCollection"]["TimeoutSeconds"]
    assert eod["TimeoutSeconds"] >= wait + heal
    end = _local(16, 0, ET, day) + dt.timedelta(seconds=eod["TimeoutSeconds"])
    assert end <= _local(22, 0, PT, day)


def test_the_weekly_budget_covers_the_est_hour_and_the_caps(schedules):
    """data-collection-weekly's 05:00 ET trails the v1 09:00 UTC start by up to
    3600 s (EST); then every workload through chronic-gap-heal (D34)."""
    need = 3600 + _worst_case_through_last_waited_unit("step_function.json", schedules)
    assert _poll_wait("step_function.json") == POLL_SECONDS
    assert _max_polls("step_function.json") == _ceil_polls(need) == 87


def test_the_weekly_readers_units_are_not_behind_workloads_it_does_not_read(schedules):
    """chronic-gap-heal (D34) runs before the two workloads whose units the v1
    weekly does not wait on, so they never extend its budget."""
    workloads = schedules["data-collection-weekly"]["input"]["workloads"]
    assert workloads.index("chronic-gap-heal") < workloads.index("alternative-phase-two")
    assert workloads.index("chronic-gap-heal") < workloads.index("rag-weekly-ingestion")


def test_the_preopen_wait_never_runs_past_the_open():
    """No trading morning may be made worse than today's baseline (I11264
    deliverable 3): the wait ends by ~09:15 ET, before the 09:30 ET open."""
    day = _DAYS[0]
    start = _local(5, 15, PT, day)
    end = start + dt.timedelta(seconds=_max_polls("step_function_daily.json") * POLL_SECONDS)
    assert end.astimezone(ET).time() <= dt.time(9, 15)
    assert end.astimezone(ET).time() < dt.time(9, 30)


# ── no v1 definition invokes the dispatcher (I11266 D6, I11265 D3) ────────────


@pytest.mark.parametrize("filename", sorted(V1))
def test_no_v1_state_invokes_the_data_spot_dispatcher(filename):
    offenders = [
        name
        for name, state in _all_states(_definition(filename)["States"])
        if DISPATCHER in json.dumps({k: state.get(k) for k in ("Resource", "Parameters")})
    ]
    assert offenders == [], f"{filename}: {offenders} invoke {DISPATCHER}"


@pytest.mark.parametrize("filename", sorted(V1))
def test_the_wait_asks_the_read_only_probe(filename):
    state = _definition(filename)["States"]["WaitForCollectionManifests"]
    assert state["Resource"] == "arn:aws:states:::lambda:invoke"
    assert state["Parameters"]["FunctionName"] == PROBE
    assert not {"action", "workload"} & set(state["Parameters"]["Payload"])


def test_the_dispatcher_offers_no_readiness_action():
    """The consumer question lives ONLY on the probe: a readiness action on the
    dispatcher would let a v1 definition invoke it again."""
    source = (INFRA / "lambdas" / "data-spot-dispatcher" / "index.py").read_text(encoding="utf-8")
    assert '"readiness-check"' not in source


# ── the heal loop (I11266) ───────────────────────────────────────────────────


def test_the_heal_starts_the_standalone_eod_machine_with_its_own_units(schedules):
    states = _definition("step_function_eod.json")["States"]
    heal = states["HealStartCollection"]
    assert heal["Resource"] == "arn:aws:states:::states:startExecution.sync:2"
    assert heal["Parameters"]["StateMachineArn"].rsplit(":", 1)[-1] == "ne-data-collection-eod"
    eod = schedules["data-collection-eod"]
    assert eod["target_ref"] and "eod" in eod["target_ref"].lower()
    heal_input = heal["Parameters"]["Input"]
    assert heal_input["collection"] == "eod"
    assert heal_input["verify_units"] == eod["input"]["verify_units"]
    assert set(heal_input["workloads"]) <= set(eod["input"]["workloads"])
    # The heal is for the precondition's data: the two writers of the verify_units.
    assert heal_input["workloads"] == ["post-market-data", "post-market-arctic-append"]
    assert heal_input["require_trading_day"] is False


def test_the_heal_loop_keeps_detect_act_verify_and_its_bound():
    states = _definition("step_function_eod.json")["States"]
    assert states["HealLoopGate"]["Default"] == "HealStartCollection"
    assert states["HealStartCollection"]["Next"] == "HealReProbe"
    assert states["HealStartCollection"]["Catch"][0]["Next"] == "HealReProbe"
    for name in ("HealReProbe", "HealCheckConverged", "HealDispatchReplay", "HealNonConvergent"):
        assert name in states, name
    removed = {
        "HealLaunchPostMarketDataSpot", "HealPollPostMarketDataSpot", "HealLaunchArcticAppendSpot",
    }
    assert not removed & set(states)
