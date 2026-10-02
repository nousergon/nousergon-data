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

Post-close split (alpha-engine-config-I11269 follow-up, 2026-09-30): the EOD
wait moved out of the 16:00 ne-postclose-trading-pipeline into
ne-postclose-reconcile-pipeline (step_function_eod_reconcile.json), which is
STARTED by ne-data-collection-eod's terminal status event (eod-backstop Lambda)
or by the 02:15 UTC reconcile backstop. Its wait is therefore a short guard
against manifest publication lag, not a budget covering the collection's run —
the event already waited for that. The derivations below pin both halves.
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
EOD_RECONCILE = "step_function_eod_reconcile.json"
EOD_BACKSTOP_DEPLOY = INFRA / "lambdas" / "eod-backstop" / "deploy.sh"

#: v1 definition -> (its machine name, the standalone schedule it waits on).
V1 = {
    "step_function.json": ("ne-weekly-freshness-pipeline", "data-collection-weekly"),
    "step_function_daily.json": ("ne-preopen-trading-pipeline", "data-collection-morning"),
    "step_function_eod_reconcile.json": ("ne-postclose-reconcile-pipeline", "data-collection-eod"),
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
    post-close reconcile wait does not read is caught."""
    schedule_input = copy.deepcopy(schedules["data-collection-eod"]["input"])
    schedule_input["verify_units"].append("D48")
    problems = wait_unit_problems(_definition(EOD_RECONCILE), schedule_input, owners)
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
def test_the_weekly_lookback_is_zero_because_it_starts_first(day, schedules):
    wk_h, wk_m = _cron(schedules["data-collection-weekly"])
    saturday = day + dt.timedelta(days=(5 - day.weekday()) % 7)
    v1_weekly = dt.datetime.combine(saturday, dt.time(9, 0), tzinfo=dt.timezone.utc)
    assert v1_weekly <= _local(wk_h, wk_m, ET, saturday)
    payload = _definition("step_function.json")["States"]["WaitForCollectionManifests"][
        "Parameters"]["Payload"]
    assert payload["lookback_seconds"] == 0
    assert payload["not_before.$"] == "$$.Execution.StartTime"


#: The reconcile wait's fixed UTC floor on its run_date. Parsed, not assumed.
_RECONCILE_NOT_BEFORE = "States.Format('{}T20:00:00Z', $.run_date)"


def _reconcile_floor(day: dt.date) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(20, 0), tzinfo=dt.timezone.utc)


@pytest.mark.parametrize("day", _DAYS)
def test_the_reconcile_floor_admits_this_sessions_collection_and_nothing_older(day, schedules):
    """The reconcile machine STARTS AFTER the collection (it is started by the
    collection's terminal event, or by the 02:15 UTC backstop), so
    ``$$.Execution.StartTime`` would reject the very manifests it waits for.
    Its floor is a fixed ``{run_date}T20:00:00Z``: at or before the 16:00 ET
    close in both DST regimes (20:00Z EDT, 21:00Z EST), so this session's
    18:15 ET collection is always admitted — and after the latest the PREVIOUS
    session's collection could have finished writing its verify_units, so a
    stale manifest never satisfies the wait."""
    payload = _definition(EOD_RECONCILE)["States"]["WaitForCollectionManifests"]["Parameters"][
        "Payload"]
    assert payload["lookback_seconds"] == 0
    assert payload["not_before.$"] == _RECONCILE_NOT_BEFORE
    eod = schedules["data-collection-eod"]
    assert eod["timezone"] == "America/New_York"
    eod_h, eod_m = _cron(eod)
    floor = _reconcile_floor(day)
    assert floor <= _local(16, 0, ET, day) < _local(eod_h, eod_m, ET, day)
    prior = day - dt.timedelta(days=1)
    prior_worst_end = _local(eod_h, eod_m, ET, prior) + dt.timedelta(
        seconds=_worst_case_through_last_waited_unit(EOD_RECONCILE, schedules))
    assert prior_worst_end < floor


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


def test_the_reconcile_wait_is_a_short_guard_not_a_collection_budget():
    """I11363 sized the OLD in-postclose wait (59 x 300 s) to cover the whole
    collection from a 16:00 ET start. The reconcile machine is started by the
    collection's terminal event, so that budget is already spent before it
    starts; a long wait here would only delay the fail-open path on a
    backstop-started run whose collection never ran. Bounded to ten minutes."""
    total = _max_polls(EOD_RECONCILE) * _poll_wait(EOD_RECONCILE)
    assert _poll_wait(EOD_RECONCILE) == 60
    assert _max_polls(EOD_RECONCILE) == 5
    assert 0 < total <= 600


def _reconcile_backstop_cron() -> tuple[int, int, str]:
    """(hour, minute, day-of-week field) of the reconcile backstop rule, UTC."""
    text = EOD_BACKSTOP_DEPLOY.read_text(encoding="utf-8")
    import re
    m = re.search(
        r'--name "\$\{RECONCILE_BACKSTOP_RULE\}" \\\s*--schedule-expression \'cron\((\d+) (\d+) \? \* (\S+) \*\)\'',
        text,
    )
    assert m, "reconcile backstop put-rule not found in eod-backstop/deploy.sh"
    return int(m.group(2)), int(m.group(1)), m.group(3)


@pytest.mark.parametrize("day", _DAYS)
def test_the_reconcile_backstop_fires_after_the_collections_worst_case(day, schedules):
    """The 02:15 UTC backstop must not fire while the collection could still be
    writing the reconcile's inputs: derived from data-collection-eod's cron and
    the declared caps through the last verify_unit writer, in both DST regimes.
    (It also defers on its own while the collection is RUNNING — pinned in the
    eod-backstop handler tests.)"""
    hour, minute, dow = _reconcile_backstop_cron()
    assert dow == "TUE-SAT"  # the UTC day after each MON-FRI session
    eod_h, eod_m = _cron(schedules["data-collection-eod"])
    worst_end = _local(eod_h, eod_m, ET, day) + dt.timedelta(
        seconds=_worst_case_through_last_waited_unit(EOD_RECONCILE, schedules))
    backstop = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(hour, minute),
                                   tzinfo=dt.timezone.utc)
    assert backstop >= worst_end
    # And it still lands on the same ET session date the handler derives.
    assert backstop.astimezone(ET).date() == day


@pytest.mark.parametrize("day", _DAYS)
def test_the_reconcile_ceiling_covers_the_wait_and_one_heal_before_the_cost_guard(day, schedules):
    """The reconcile definition's whole-execution ceiling (pinned, with its
    derivation, in tests/test_sf_structural_contract.py) is re-derived here from
    the two bounds it has to hold: the readiness guard plus one full
    HealStartCollection, and — started at the collection's worst-case end
    through its verify_units — still ending before the 22:00 PT
    alpha-engine-stop-trading cost guard."""
    rec = _definition(EOD_RECONCILE)
    wait = _max_polls(EOD_RECONCILE) * _poll_wait(EOD_RECONCILE)
    heal = rec["States"]["HealStartCollection"]["TimeoutSeconds"]
    assert rec["TimeoutSeconds"] >= wait + heal
    eod_h, eod_m = _cron(schedules["data-collection-eod"])
    start = _local(eod_h, eod_m, ET, day) + dt.timedelta(
        seconds=_worst_case_through_last_waited_unit(EOD_RECONCILE, schedules))
    end = start + dt.timedelta(seconds=rec["TimeoutSeconds"])
    assert end <= _local(22, 0, PT, day)


@pytest.mark.parametrize("day", _DAYS)
def test_the_postclose_sf_ends_well_before_the_collection(day, schedules):
    """The 16:00 machine waits for nothing: its whole ceiling ends before the
    EOD collection's cron, so nothing it does can depend on that run."""
    post = _definition("step_function_eod.json")
    assert "WaitForCollectionManifests" not in post["States"]
    eod_h, eod_m = _cron(schedules["data-collection-eod"])
    assert _local(16, 0, ET, day) + dt.timedelta(seconds=post["TimeoutSeconds"]) <= _local(
        eod_h, eod_m, ET, day)


def test_the_weekly_budget_covers_the_est_hour_and_the_caps(schedules):
    """data-collection-weekly's 05:00 ET trails the v1 09:00 UTC start by up to
    3600 s (EST); then every workload through weekly-phase-one, the last one
    writing a waited unit since D34 stopped being waited
    (alpha-engine-config-I11812; it was 87 polls through chronic-gap-heal)."""
    need = 3600 + _worst_case_through_last_waited_unit("step_function.json", schedules)
    assert _poll_wait("step_function.json") == POLL_SECONDS
    assert _max_polls("step_function.json") == _ceil_polls(need) == 62


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
    states = _definition(EOD_RECONCILE)["States"]
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
    states = _definition(EOD_RECONCILE)["States"]
    assert states["HealLoopGate"]["Default"] == "HealStartCollection"
    assert states["HealStartCollection"]["Next"] == "HealReProbe"
    assert states["HealStartCollection"]["Catch"][0]["Next"] == "HealReProbe"
    for name in ("HealReProbe", "HealCheckConverged", "HealDispatchReplay", "HealNonConvergent"):
        assert name in states, name
    removed = {
        "HealLaunchPostMarketDataSpot", "HealPollPostMarketDataSpot", "HealLaunchArcticAppendSpot",
    }
    assert not removed & set(states)


def test_the_heal_collection_is_excluded_from_the_reconcile_trigger():
    """HealStartCollection starts ne-data-collection-eod from INSIDE a reconcile
    run. Its terminal event must not start a second reconcile: the name prefix
    it stamps is exactly the one the eod-backstop trigger rule excludes."""
    heal = _definition(EOD_RECONCILE)["States"]["HealStartCollection"]
    assert heal["Parameters"]["Name.$"].startswith("States.Format('v1-eod-heal-")
    deploy = EOD_BACKSTOP_DEPLOY.read_text(encoding="utf-8")
    assert '{"anything-but": {"prefix": "v1-eod-heal-"}}' in deploy
