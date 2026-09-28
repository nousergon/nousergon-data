"""Contract tests for the standalone data-collection stack (alpha-engine-config-I10739).

Nous Ergon's data collection is its own component with its own schedule and
CloudFormation stack (alpha-engine-config architecture.d/146). These tests hold
the properties that must survive every later edit: nothing in it depends on a
Crucible v1 pipeline, every schedule's state is DECLARED in one place and agrees
with automation_pause.json, it names only workloads the dispatcher runs, and the
deploy path proves its own effect.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
HELPER_PATH = REPO / "infrastructure" / "data_collection_stack.py"
WORKFLOW = REPO / ".github" / "workflows" / "deploy-data-collection-stack.yml"
WORKFLOWS = REPO / ".github" / "workflows"
NEW_ROLE = "github-actions-data-collection-stack-deploy"
V1_PIPELINES = (
    "ne-postclose-trading-pipeline",
    "ne-preopen-trading-pipeline",
    "ne-weekly-freshness-pipeline",
    "alpha-engine-orchestration",
)


@pytest.fixture(scope="module")
def stack():
    spec = importlib.util.spec_from_file_location("data_collection_stack", HELPER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def tpl(stack):
    return stack.load_template()


def test_lint_is_clean(stack):
    assert stack.lint() == []


@pytest.mark.skipif(shutil.which("cfn-lint") is None, reason="cfn-lint not installed")
def test_cfn_lint_is_clean(stack):
    proc = subprocess.run(
        ["cfn-lint", str(stack.TEMPLATE)], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_four_schedules_each_driving_its_own_state_machine(stack, tpl):
    """alpha-engine-config-I11233 adds a FIFTH schedule that targets the
    dispatcher Lambda directly rather than a state machine — asserted
    separately below rather than folded in here, so this test keeps meaning
    exactly what its name says."""
    sched = [s for s in stack.schedules(tpl) if s["target_kind"] == "state_machine"]
    assert [s["name"] for s in sched] == [
        "data-collection-daily-heal", "data-collection-eod", "data-collection-morning", "data-collection-weekly",
    ]
    assert {s["group"] for s in sched} == {"nousergon-data-collection"}
    assert len({s["target_ref"] for s in sched}) == 4


@pytest.mark.parametrize(
    "name,workload",
    [
        ("data-collection-shadow-sameday", "shadow-sameday"),
        ("data-collection-shadow-morning", "shadow-morning"),
    ],
)
def test_the_shadow_schedules_target_the_dispatcher_lambda_directly(stack, tpl, name, workload):
    """alpha-engine-config-I11233 / -I11352: the two schedules in this stack
    with no state machine of their own — each invokes
    alpha-engine-data-spot-dispatcher directly, using the dispatcher's own
    {"workload": ...} contract rather than the
    collection/workloads/verify_units shape every state-machine-targeting
    schedule uses."""
    sched = {s["name"]: s for s in stack.schedules(tpl)}[name]
    assert sched["target_kind"] == "lambda"
    assert sched["target_ref"] is None
    assert sched["group"] == "nousergon-data-collection"
    assert sched["input"] == {"workload": workload}


def test_the_morning_shadow_fires_after_v1s_own_morning_run(stack, tpl):
    """alpha-engine-config-I11352. The fifteen minutes ARE the mechanism: v1's
    MorningSchedule writes the keys at 07:30 ET and parity grades against them,
    so a shadow morning run that fired first would read `live_missing` on every
    key it produced. Both crons live in this template and are asserted against
    each other rather than each against a remembered literal."""
    by_name = {s["name"]: s for s in stack.schedules(tpl)}
    assert by_name["data-collection-morning"]["expression"] == "cron(30 7 ? * MON-FRI *)"
    assert by_name["data-collection-shadow-morning"]["expression"] == "cron(45 7 ? * MON-FRI *)"
    assert by_name["data-collection-shadow-morning"]["timezone"] == "America/New_York"
    # And it is NOT the same-day slot: the two morning legs moved off 18:30 ET
    # precisely because Polygon's grouped-daily bar for session D is a D+1 fact.
    assert by_name["data-collection-shadow-sameday"]["expression"] == "cron(30 18 ? * MON-FRI *)"


def test_the_eod_schedule_fires_at_the_declared_settlement_hour(stack, tpl):
    """alpha-engine-config-I11354. The EOD collection fires at
    ``dates.SETTLED_AFTER_ET``, not at some remembered literal.

    It was ``cron(45 16 …)`` — 16:45 ET, chosen as "after the 16:00 close and
    outside the v1 postclose window". Both still hold at 18:15, but after the
    close is not after the bar is final: measured 2026-09-21 across 920 of 920
    price-cache files, a 16:06 ET fetch was short on ``Volume`` for every one of
    them (median 20.5 %, max 62.3 %, never high) and off on ``Close`` for 442
    (median 2.73 bps, max 16.39) against a refetch at 18:41 ET.

    Asserted against the constant rather than against ``"cron(15 18 …)"`` so
    that when alpha-engine-config-I11356's 3-day sample moves the hour, the
    schedule and the two code paths reading the same constant move together or
    this test fails. That is the whole point: one declared hour, three call
    sites, no literal drifting off on its own.
    """
    from dates import SETTLED_AFTER_ET

    hh, mm = (int(p) for p in SETTLED_AFTER_ET.split(":"))
    eod = {s["name"]: s for s in stack.schedules(tpl)}["data-collection-eod"]
    assert eod["expression"] == f"cron({mm} {hh} ? * MON-FRI *)"
    # An exchange clock, not UTC — the settlement hour moves with US DST and a
    # UTC cron would be an hour wrong for half the year.
    assert eod["timezone"] == "America/New_York"


def test_the_eod_schedule_leaves_the_morning_run_a_full_overnight(stack, tpl):
    """The later start must not push EOD collection into the next morning's run.

    alpha-engine-config-I11363: derived from the declared caps, not from one
    remembered default. The whole EOD workload list — including arctic-probe
    and edgar-pit-fundamentals-daily on the 7200 s default — is hard-stopped
    before MorningSchedule fires the next day.
    """
    by_name = {s["name"]: s for s in stack.schedules(tpl)}
    eod = by_name["data-collection-eod"]
    eod_h, eod_m = _cron_hour_minute(eod["expression"])
    morn_h, morn_m = _cron_hour_minute(by_name["data-collection-morning"]["expression"])
    gap_seconds = ((24 * 60) - (eod_h * 60 + eod_m) + (morn_h * 60 + morn_m)) * 60
    workloads = eod["input"]["workloads"]
    whole_run = stack.worst_case_seconds(workloads, through=workloads[-1])
    assert whole_run < gap_seconds, (whole_run, gap_seconds)


def test_the_eod_unit_writers_carry_declared_caps(stack):
    """alpha-engine-config-I11363: the two workloads that write the EOD
    verify_units are capped explicitly (measured 30.4-45.7 and 24.7-28.1 min
    over 2026-09-15..23) rather than riding the 7200 s default, so every
    consumer bound derived from them is a declared number."""
    default, overrides, ssm = stack.dispatcher_runtime_caps()
    assert (default, ssm) == (7200, 300)
    writers = set(stack.UNIT_WRITERS["data-collection-eod"].values())
    assert writers == {"post-market-data", "post-market-arctic-append"}
    assert {w: overrides[w] for w in writers} == {
        "post-market-data": 5400, "post-market-arctic-append": 3600,
    }


def test_the_eod_ordering_chain_is_derived_from_the_template(stack, tpl):
    """alpha-engine-config-I11363. From the EOD cron, the last minute any EOD
    verify_unit can still be written is the cron plus (SSM-online budget + cap)
    of each workload up to the last unit writer: 18:15 + 95 + 65 = 20:55 ET.
    Every number comes from the template or the dispatcher source."""
    by_name = {s["name"]: s for s in stack.schedules(tpl)}
    eod = by_name["data-collection-eod"]
    h, m = _cron_hour_minute(eod["expression"])
    worst = stack.worst_case_through_units(eod, eod["input"]["verify_units"])
    default, overrides, ssm = stack.dispatcher_runtime_caps()
    assert worst == sum(
        ssm + overrides.get(w, default) for w in ("post-market-data", "post-market-arctic-append")
    )
    assert h * 60 + m + -(-worst // 60) == 20 * 60 + 55


def test_the_sameday_shadow_is_not_enabled_beside_a_live_eod_run(stack, tpl):
    """alpha-engine-config-I11363's ordering assertion, decided for a DISABLED
    shadow: the check stays ARMED rather than being deleted with the shadow.
    Its 18:30 ET cron does not clear the 20:55 ET bound above, so the committed
    template is lint-clean only because the shadow is DISABLED — and a
    re-enable that does not also move the cron fails lint (below)."""
    sched = stack.schedules(tpl)
    assert stack.sameday_ordering_problems(sched) == []
    by_name = {s["name"]: s for s in sched}
    assert by_name["data-collection-shadow-sameday"]["declared_state"] == "DISABLED"
    shadow_h, shadow_m = _cron_hour_minute(by_name["data-collection-shadow-sameday"]["expression"])
    assert shadow_h * 60 + shadow_m < 20 * 60 + 55  # the premise the DISABLED state carries


def _with_shadow(stack, tpl, *, state: str, expression: str | None = None) -> list[dict]:
    import copy

    sched = copy.deepcopy(stack.schedules(tpl))
    for s in sched:
        if s["name"] == "data-collection-shadow-sameday":
            s["declared_state"] = state
            if expression:
                s["expression"] = expression
    return sched


def test_re_enabling_the_sameday_shadow_at_its_cron_is_refused(stack, tpl):
    problems = stack.sameday_ordering_problems(_with_shadow(stack, tpl, state="ENABLED"))
    assert len(problems) == 1 and "alpha-engine-config-I11363" in problems[0], problems


@pytest.mark.parametrize(
    "expression,refused",
    [("cron(54 20 ? * MON-FRI *)", True), ("cron(55 20 ? * MON-FRI *)", False)],
)
def test_the_ordering_check_moves_exactly_at_the_derived_bound(stack, tpl, expression, refused):
    sched = _with_shadow(stack, tpl, state="ENABLED", expression=expression)
    assert bool(stack.sameday_ordering_problems(sched)) is refused


def test_lint_carries_the_ordering_check(stack, tpl, monkeypatch):
    """The lint (run by the deploy workflow) is the guard, so it is graded
    rather than assumed: a template with the shadow re-enabled fails lint."""
    assert not any("alpha-engine-config-I11363" in p for p in stack.lint())
    mutated = _with_shadow(stack, tpl, state="ENABLED")
    monkeypatch.setattr(stack, "schedules", lambda _tpl: mutated)
    assert any("alpha-engine-config-I11363" in p for p in stack.lint())


def _cron_hour_minute(expression: str) -> tuple[int, int]:
    """``cron(M H ? * MON-FRI *)`` -> ``(H, M)``."""
    import re

    m = re.fullmatch(r"cron\((\d+) (\d+) \?.*\)", expression)
    assert m, f"unexpected schedule expression shape: {expression!r}"
    return int(m.group(2)), int(m.group(1))


def test_schedule_names_are_unique_without_their_group(stack, tpl):
    """CloudFormation's AWS::EarlyValidation::ResourceExistenceCheck compares a
    schedule's Name WITHOUT its group. The first create (2026-09-14) named one
    `weekly`, collided with crucible-v2's `crucible-v2/weekly`, and rolled the whole
    stack back. Every name carries this component's prefix so no other stack's
    schedule can share it."""
    for s in stack.schedules(tpl):
        assert s["name"].startswith("data-collection-"), s["name"]


def test_declared_states_at_the_decoupled_cutover(stack, tpl):
    """alpha-engine-config-I11269 (Brian's ruling (b), 2026-09-21). This was
    `test_ships_disabled` (alpha-engine-config-I10739 deliverable 2: nothing
    double-writes market_data/* while the v1 SFs still run), and the enable PR
    was to change its expectation for CollectionState in the same change that
    flips the Default. This is that change: the same PR removes the v1 SFs'
    inline data stages, so collection is ENABLED and there is still one writer.

    The daily heal stays DISABLED (its v1 rule is paused by the 2026-08-07
    ruling; enabling it is a separate decision). Both SHADOW schedules turn
    DISABLED: once v1 stops writing the compared keys, a parity run would
    compare the collector with itself. `data.cutover_ready.parity` is frozen at
    the last pre-cutover report instead (tests/test_gate_read_follows_the_parity_publish.py)."""
    defaults = stack.parameter_defaults(tpl)
    assert defaults["CollectionState"] == "ENABLED"
    assert defaults["DailyHealState"] == "DISABLED"
    assert defaults["ShadowSamedayState"] == "DISABLED"
    assert defaults["ShadowMorningState"] == "DISABLED"
    by_name = {s["name"]: s for s in stack.schedules(tpl)}
    assert {n: s["declared_state"] for n, s in by_name.items()} == {
        "data-collection-daily-heal": "DISABLED",
        "data-collection-eod": "ENABLED",
        "data-collection-morning": "ENABLED",
        "data-collection-weekly": "ENABLED",
        "data-collection-shadow-sameday": "DISABLED",
        "data-collection-shadow-morning": "DISABLED",
    }


def test_daily_heal_has_its_own_state_switch(stack, tpl):
    """Its v1 rule is paused by the 2026-08-07 ruling; the cutover must be able
    to enable collection without un-pausing the heal."""
    by_name = {s["name"]: s for s in stack.schedules(tpl)}
    assert by_name["data-collection-daily-heal"]["state_parameter"] == "DailyHealState"
    assert {
        by_name[f"data-collection-{n}"]["state_parameter"] for n in ("eod", "morning", "weekly")
    } == {"CollectionState"}


def test_eod_verifies_every_unit_its_workloads_run(stack, tpl):
    """alpha-engine-config-I10787 (P-20): the completion claim is the run manifest
    of every unit the machine runs, not a HEAD on the two keys Metron reads most.
    The set below is `run_units.PHASE_UNITS` for the "daily" mode (minus the arctic
    append `--skip-arctic-append` defers) plus MODE_UNITS["daily_arctic_append"]."""
    eod = {s["name"]: s for s in stack.schedules(tpl)}["data-collection-eod"]["input"]
    # alpha-engine-config-I11269: edgar-pit-fundamentals-daily joins at the
    # tail (it was the v1 postclose SF's LaunchEdgarPitFundamentalsDailySpot
    # leg). It has no unit descriptor, so verify_units is unchanged.
    assert eod["workloads"] == [
        "post-market-data", "post-market-arctic-append", "arctic-probe",
        "edgar-pit-fundamentals-daily",
    ]
    assert eod["require_trading_day"] is True
    assert "verify_keys" not in eod
    assert set(eod["verify_units"]) == {
        "D03", "D19", "D20", "D21", "D22", "D23", "D24", "D25",
        "D26", "D27", "D28", "D29", "D30", "D31", "D32",
    }


def test_every_schedule_verifies_units_and_none_verifies_keys(stack, tpl):
    """No STATE-MACHINE-TARGETING schedule keeps the "no key verification"
    posture: an SSM exit code was never a completion claim, and the morning
    enrich is what the predictor reads next. Every such schedule names at
    least one unit, and every named unit has a descriptor — a unit nobody
    declared would grade nothing and read as a pass.

    alpha-engine-config-I11233's shadow-sameday schedule is EXCLUDED
    deliberately, not by omission: it targets the dispatcher Lambda directly
    (no state machine, no collection/workloads/verify_units contract) and its
    completion claim is graded downstream by data.cutover_ready.parity, not
    by this stack's verify_units machinery — asserted explicitly below rather
    than left as a silent gap in the loop."""
    declared = stack.declared_units()
    for s in stack.schedules(tpl):
        if s["target_kind"] == "lambda":
            assert "verify_units" not in s["input"], f"{s['name']} unexpectedly declares verify_units"
            continue
        units = s["input"]["verify_units"]
        assert units, f"{s['name']} verifies no units"
        assert not set(units) - declared, f"{s['name']}: undeclared {sorted(set(units) - declared)}"
        assert "verify_keys" not in s["input"]


def test_lint_refuses_an_undeclared_or_empty_verify_units(stack, monkeypatch):
    """The lint is the guard, so it is graded rather than assumed."""
    import json as _json

    real_loader = stack.load_template

    def _mutate(inputs):
        base = real_loader()
        for res in base["Resources"].values():
            if res["Type"] != "AWS::Scheduler::Schedule":
                continue
            payload = _json.loads(res["Properties"]["Target"]["Input"])
            payload["verify_units"] = inputs
            res["Properties"]["Target"]["Input"] = _json.dumps(payload)
        return base

    monkeypatch.setattr(stack, "load_template", lambda *a, **k: _mutate(["D99"]))
    assert any("verify_units ['D99']" in p for p in stack.lint())
    monkeypatch.setattr(stack, "load_template", lambda *a, **k: _mutate([]))
    assert any("verify_units is empty" in p for p in stack.lint())


#: Workloads allowed AFTER arctic-probe: each reads ArcticDB at most and writes
#: none of it, so the probe still describes the day's final ArcticDB state.
#: edgar-pit-fundamentals-daily reads the `universe` library for split-adjusted
#: closes (collectors/edgar_pit_fundamentals.py::ArcticPriceReader) and writes
#: S3 objects only.
_NON_ARCTIC_WRITERS = {"edgar-pit-fundamentals-daily"}


def test_eod_and_morning_probe_arcticdb_after_its_last_writer(stack, tpl):
    """P-05 (alpha-engine-config-I10748): the in-region ArcticDB probe must run
    after every ArcticDB-writing workload of both schedules so data_gate's
    ArcticDB-derived clauses see the day's collection before the probe
    describes it. Until alpha-engine-config-I11269 that meant "last"; the EOD
    schedule now carries edgar after it, which writes no ArcticDB."""
    by_name = {s["name"]: s for s in stack.schedules(tpl)}
    assert by_name["data-collection-morning"]["input"]["workloads"][-1] == "arctic-probe"
    eod = by_name["data-collection-eod"]["input"]["workloads"]
    after = eod[eod.index("arctic-probe") + 1:]
    assert after == ["edgar-pit-fundamentals-daily"]
    assert set(after) <= _NON_ARCTIC_WRITERS


def test_weekly_mirrors_the_v1_order(stack, tpl):
    """alpha-engine-config-I10753: DataPhase2 (D15) and RAGIngestion (D16/D46)
    join morning-enrich/weekly-phase-one in v1 data order. D40/D41 are
    retired (R7) and get no workload; D46 is a substep of rag-weekly-ingestion,
    not its own key. chronic-gap-heal (D34, alpha-engine-config-I11002) has no
    data-order dependency on the other legs; alpha-engine-config-I11269 moves
    it from the tail to THIRD, because the v1 weekly SF now waits on D34 but
    not on D15/D16/D46, and at the tail its bounded wait would have to cover
    two workloads it does not read (tests/test_v1_collection_readiness_wait.py)."""
    weekly = {s["name"]: s for s in stack.schedules(tpl)}["data-collection-weekly"]["input"]
    assert weekly["workloads"] == [
        "morning-enrich", "weekly-phase-one", "chronic-gap-heal", "alternative-phase-two",
        "rag-weekly-ingestion",
    ]
    assert weekly["require_trading_day"] is False


# alpha-engine-config-I10753. Units whose descriptor declares a standalone-stack
# successor but which NO schedule verifies today, each with the tracked issue
# that closes it. This register is asserted for EQUALITY below, not membership:
# a new uncovered unit fails, and covering one of these without deleting its row
# fails too. It is deliberately not a `skip` or a soft warning — the whole point
# of `data.cutover_ready.units_covered` is that an ungraded member never reads
# as covered, and the same must hold for the register that records the
# exceptions to it.
_UNCOVERED_WITH_A_TRACKED_ISSUE: dict[str, str] = {
    # D34 (chronic-gap-heal) fixed by alpha-engine-config-I11002: the
    # `chronic-gap-heal` dispatcher workload now runs it on `WeeklySchedule`,
    # named in that schedule's `verify_units` below. This register is
    # asserted for EQUALITY — leaving the row after the fix fails on purpose.
}


def _unit_descriptors() -> dict[str, dict]:
    out = {}
    for path in sorted((REPO / "registry.d" / "units").glob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        out[doc["unit_id"]] = doc
    return out


def test_every_standalone_successor_unit_is_covered_or_retired(stack, tpl):
    """alpha-engine-config-I10753 closes-when, as an executable predicate.

    The issue's deliverable is per-unit prose ("a workload, or a recorded
    retirement"), which is exactly the shape that reads satisfied while a unit
    quietly falls out of a JSON string in a CloudFormation input. So assert the
    property over EVERY descriptor rather than over the five the issue names —
    a fix that survives the class, not the instance:

    * a unit whose descriptor carries a `retirement:` block must NOT be
      verified by any schedule (D09/D40/D41 — the stack may not assert a
      completeness claim for a producer declared retired, which is green only
      while the retired code still happens to run and is a false red the day
      it stops), and
    * every other unit whose v1 trigger the standalone stack takes over must
      be named in at least one schedule's `verify_units`.
    """
    units = _unit_descriptors()
    verified_by: dict[str, list[str]] = {}
    for sched in stack.schedules(tpl):
        for unit_id in sched["input"].get("verify_units") or []:
            verified_by.setdefault(unit_id, []).append(sched["name"])

    unknown = sorted(set(verified_by) - set(units))
    assert unknown == [], f"verify_units names units with no descriptor: {unknown}"

    retired = {u for u, d in units.items() if d.get("retirement")}
    wrongly_verified = sorted(retired & set(verified_by))
    assert wrongly_verified == [], (
        "these units carry a recorded retirement yet are still verified by a "
        f"schedule: {wrongly_verified}"
    )
    for unit_id in sorted(retired):
        assert units[unit_id]["retirement"].get("ruling"), f"{unit_id} retirement names no ruling"
        # `no_successor_workload` is the field that turns "this unit is retired"
        # into "and that is why no schedule runs it", so it is required exactly
        # of the units the standalone stack would otherwise have had to take
        # over — the ones whose v1 trigger is a Step Functions state. D15L is
        # retired with its code deleted and a `manual` trigger; there was never
        # a schedule to succeed, and demanding the field there would be
        # bookkeeping rather than a claim.
        if ((units[unit_id].get("trigger") or {}).get("kind")) == "step-functions":
            assert units[unit_id]["retirement"].get("no_successor_workload") is True, (
                f"{unit_id} has a retirement: block without no_successor_workload: true"
            )

    # "the standalone stack takes this unit's v1 trigger over" is the SAME
    # property data_gate's units_covered predicate keys on (the declared
    # successor, not trigger.kind — alpha-engine-config-I10908), restated here
    # so the two cannot drift into disagreeing about who is in scope.
    owed = {
        unit_id
        for unit_id, doc in units.items()
        if unit_id not in retired
        and any(
            token in str((doc.get("trigger") or {}).get("successor") or "")
            for token in ("nousergon-data-PR1701", "alpha-engine-config-I10753")
        )
    }
    uncovered = sorted(owed - set(verified_by))
    assert uncovered == sorted(_UNCOVERED_WITH_A_TRACKED_ISSUE), (
        "the set of standalone-successor units no schedule verifies changed: "
        f"{uncovered} (register: {sorted(_UNCOVERED_WITH_A_TRACKED_ISSUE)})"
    )

    # Not asserted: that a unit is verified by exactly ONE schedule. D03
    # (prices) is refreshed by both the EOD collection and weekly phase 1, and
    # D17 (morning-enrich) runs on the morning schedule and again as the weekly
    # chain's first workload — each run writes its own manifest for its own
    # trading day, so a second grader is a second real claim, not a duplicate.


def test_no_dependency_on_a_v1_pipeline(stack):
    for path in (stack.TEMPLATE, stack.DEFINITION, HELPER_PATH, WORKFLOW):
        text = path.read_text(encoding="utf-8")
        for name in V1_PIPELINES:
            # The template's Description names them as what it replaces; no
            # resource, ARN, parameter or substitution may reference one.
            refs = [
                ln for ln in text.splitlines()
                if name in ln and ("arn:" in ln or "!Ref" in ln or "Arn" in ln or "stateMachine" in ln)
            ]
            assert refs == [], f"{path.name} references {name}: {refs}"


def test_pause_manifest_disagreement_is_reported_both_ways(stack, tpl):
    sched = stack.schedules(tpl)
    empty = {"pending": {}, "paused": {}, "not_paused": {}}
    # One problem per schedule against a manifest naming none of them — DISABLED
    # ones for missing pending/paused, ENABLED ones (shadow-sameday,
    # alpha-engine-config-I11233) for missing not_paused. Asserted against
    # len(sched) rather than a literal count so a new schedule changes this
    # test's expectation with it instead of silently drifting past it.
    assert len(stack.pause_manifest_problems(sched, empty)) == len(sched)
    enabled = [dict(s, declared_state="ENABLED") for s in sched]
    listed_pending = {"pending": {s["qualified_name"]: "x" for s in sched}, "not_paused": {}}
    problems = stack.pause_manifest_problems(enabled, listed_pending)
    assert any("not in automation_pause.json not_paused" in p for p in problems)
    assert any("still listed as pending/paused" in p for p in problems)


def test_asl_checker_catches_a_dangling_transition(stack):
    asl = json.loads(stack.DEFINITION.read_text(encoding="utf-8"))
    assert stack.asl_problems(asl) == []
    asl["States"]["Init"]["Next"] = "NoSuchState"
    inner = asl["States"]["RunWorkloads"]["ItemProcessor"]["States"]
    inner["RetryOnDemand"]["Next"] = "Nowhere"
    problems = stack.asl_problems(asl)
    assert any("NoSuchState" in p for p in problems)
    assert any("Nowhere" in p for p in problems)


def test_the_definition_fails_loud_rather_than_skipping(stack):
    """Collection has no downstream stage to protect, so unlike the v1 SFs'
    fail-open data-spot stages every failure path ends in a Fail state and a
    disabled dispatcher is a failure, never a skip."""
    asl = json.loads(stack.DEFINITION.read_text(encoding="utf-8"))
    inner = asl["States"]["RunWorkloads"]["ItemProcessor"]["States"]
    assert inner["DispatchDisabled"]["Type"] == "Fail"
    assert inner["CheckRetryBudget"]["Default"] == "WorkloadFailed"
    assert asl["States"]["NotifyFailure"]["Next"] == "CollectionFailed"
    assert asl["States"]["CollectionFailed"]["Type"] == "Fail"
    # The completion check has no edge to CollectionSucceeded that is not an
    # affirmative measured pass: ok==true is the ONLY choice, a dispatcher error
    # is caught to NotifyFailure, and every failure mode — including one this
    # switch does not know — ends in a Fail state.
    assert asl["States"]["CheckCompletion"]["Default"] == "NotifyCompletionFindings"
    assert [c["Next"] for c in asl["States"]["CheckCompletion"]["Choices"]] == ["CollectionSucceeded"]
    assert asl["States"]["VerifyRunManifests"]["Catch"][0]["Next"] == "NotifyFailure"
    switch = asl["States"]["CompletionFailureMode"]
    assert switch["Default"] == "CompletionModeUnknown"
    for state in ("ManifestMissing", "RunNotOk", "OutputMissing", "RowsBelowFloor",
                  "CompletionModeUnknown"):
        assert asl["States"][state]["Type"] == "Fail"
        # A named error per mode, and a Cause carrying the unit and key.
        assert asl["States"][state]["CausePath"] == "$.completion.summary"
    assert [asl["States"][s]["Error"] for s in
            ("ManifestMissing", "RunNotOk", "OutputMissing", "RowsBelowFloor")] == [
        "DataCollectionManifestMissing", "DataCollectionRunNotOk",
        "DataCollectionOutputMissing", "DataCollectionRowsBelowFloor",
    ]
    # A paging outage must never convert a finding into a green run.
    assert asl["States"]["NotifyCompletionFindings"]["Catch"][0]["Next"] == "CompletionFailureMode"


def test_the_four_failure_modes_the_asl_switches_on_are_the_lambdas_own(stack):
    """The ASL's Choice arms and the shared predicate's precedence tuple are one
    list. Drift here is a mode that fails the execution as "unknown" rather than
    named. The tuple lives in data_gate/run_manifest_predicate.py since
    alpha-engine-config-I11264 moved the predicate out of the dispatcher."""
    import ast

    asl = json.loads(stack.DEFINITION.read_text(encoding="utf-8"))
    arms = [c["StringEquals"] for c in asl["States"]["CompletionFailureMode"]["Choices"]]
    tree = ast.parse((REPO / "data_gate" / "run_manifest_predicate.py").read_text(encoding="utf-8"))
    modes = None
    for node in ast.walk(tree):
        target = getattr(node, "target", None) or (getattr(node, "targets", None) or [None])[0]
        if isinstance(target, ast.Name) and target.id == "COMPLETION_FAILURE_MODES":
            modes = [e.value for e in node.value.elts]
    assert modes is not None, "COMPLETION_FAILURE_MODES not found in the shared predicate"
    assert arms == modes


def test_the_dispatcher_zip_carries_the_descriptors_it_grades(stack):
    """A descriptor edit must reach the deployed function on the merge button
    alone: deploy.sh packages the loader and the descriptors, and the deploy
    workflow's path filter fires on them. Without the filter the Lambda would go
    on grading the previous descriptor set with nothing red."""
    deploy = (REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "deploy.sh").read_text(
        encoding="utf-8"
    )
    assert "data_gate/descriptors.py" in deploy
    assert "data_gate/run_manifest_predicate.py" in deploy
    assert "registry.d/units" in deploy
    paths = yaml.safe_load(
        (WORKFLOWS / "deploy-data-spot-dispatcher.yml").read_text(encoding="utf-8")
    )[True]["push"]["paths"]
    assert "registry.d/units/**" in paths
    assert "data_gate/descriptors.py" in paths
    assert "data_gate/run_manifest_predicate.py" in paths


def test_yaml_aliases_are_refused(stack, tmp_path):
    bad = tmp_path / "t.yaml"
    bad.write_text("Resources:\n  A: &x\n    Type: AWS::SNS::Topic\n  B: *x\n", encoding="utf-8")
    with pytest.raises(ValueError):
        stack.load_template(bad)


def test_definition_key_is_content_addressed(stack, tmp_path):
    a = tmp_path / "a.json"
    a.write_text("{}", encoding="utf-8")
    key1 = stack.definition_key(a)
    a.write_text('{"x": 1}', encoding="utf-8")
    key2 = stack.definition_key(a)
    assert key1 != key2
    assert key1.startswith(stack.DEFINITION_PREFIX) and key1.endswith(".asl.json")


class _Cfn:
    def __init__(self, status, tags):
        self._stack = {"StackStatus": status, "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}

    def describe_stacks(self, StackName):  # noqa: N803 — boto3 kwarg
        return {"Stacks": [self._stack]}


class _Scheduler:
    def __init__(self, overrides=None):
        self.overrides = overrides or {}

    def get_schedule(self, GroupName, Name):  # noqa: N803 — boto3 kwarg
        return self.overrides.get(Name, self._declared[Name])


def _scheduler_matching(stack, tpl, **overrides):
    s = _Scheduler(overrides)
    s._declared = {
        x["name"]: {"State": x["declared_state"], "ScheduleExpression": x["expression"]}
        for x in stack.schedules(tpl)
    }
    return s


def test_check_live_clean_when_live_matches(stack, tpl):
    f = stack.deploy_fields()
    cfn = _Cfn("UPDATE_COMPLETE", {k: f[k] for k in ("template-sha256", "definition-sha256")})
    assert stack.live_findings(cfn, _scheduler_matching(stack, tpl)) == []


def test_check_live_reports_unapplied_template_and_console_flip(stack, tpl):
    cfn = _Cfn("UPDATE_ROLLBACK_COMPLETE", {"template-sha256": "old", "definition-sha256": "old"})
    # The expression comes from the TEMPLATE, not a literal: this test is about
    # the console-flip (State) finding, and a hard-coded cron silently became a
    # second, unintended expression-drift finding the moment the schedule moved
    # (alpha-engine-config-I11354 moved it 16:45 ET -> 18:15 ET).
    _eod = {s["name"]: s for s in stack.schedules(tpl)}["data-collection-eod"]
    _eod_expr = _eod["expression"]
    _flipped = {"ENABLED": "DISABLED", "DISABLED": "ENABLED"}[_eod["declared_state"]]
    sched = _scheduler_matching(
        stack, tpl,
        # The flip is the OPPOSITE of the declared state, whatever that is
        # (ENABLED since alpha-engine-config-I11269), so the finding is a
        # console flip in either direction rather than one remembered value.
        **{"data-collection-eod": {"State": _flipped, "ScheduleExpression": _eod_expr}},
    )
    findings = stack.live_findings(cfn, sched)
    assert any("UPDATE_ROLLBACK_COMPLETE" in x for x in findings)
    assert sum("stack tag" in x for x in findings) == 2
    assert any(
        f"nousergon-data-collection/data-collection-eod is {_flipped} live" in x for x in findings
    )


def test_check_live_key_is_the_one_the_gate_reads(stack):
    """The producer is a standalone script and imports nothing from data_gate,
    so the two spellings are pinned here (alpha-engine-config-I10870)."""
    from data_gate import evidence

    assert stack.CHECK_LIVE_KEY == "data_collection/" + evidence.STACK_CHECK_LIVE_KEY
    assert stack.CHECK_LIVE_SCHEMA == evidence.STACK_CHECK_LIVE_SCHEMA


class _S3:
    def __init__(self, fail=False):
        self.fail, self.puts = fail, []

    def put_object(self, **kw):
        if self.fail:
            raise PermissionError("AccessDenied")
        self.puts.append(kw)


def _fake_boto3(monkeypatch, *, cfn=None, sched=None, s3):
    import sys
    import types

    def client(name):
        if name == "s3":
            return s3
        if name == "cloudformation":
            if cfn is None:
                raise PermissionError("AccessDenied on DescribeStacks")
            return cfn
        return sched

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=client))
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)


def test_check_live_publishes_drift_and_still_exits_non_zero(stack, tpl, monkeypatch):
    s3 = _S3()
    cfn = _Cfn("UPDATE_ROLLBACK_COMPLETE", {"template-sha256": "old", "definition-sha256": "old"})
    _fake_boto3(monkeypatch, cfn=cfn, sched=_scheduler_matching(stack, tpl), s3=s3)
    assert stack.main(["check-live"]) == 1
    (put,) = s3.puts
    doc = json.loads(put["Body"])
    assert (put["Bucket"], put["Key"]) == (stack.CHECK_LIVE_BUCKET, stack.CHECK_LIVE_KEY)
    assert doc["in_sync"] is False and doc["measured"] is True and doc["drift"]
    assert doc["code_sha"] == "b" * 40 and doc["stack"] == stack.STACK_NAME


def test_check_live_publishes_when_it_cannot_measure(stack, monkeypatch):
    s3 = _S3()
    _fake_boto3(monkeypatch, cfn=None, s3=s3)
    assert stack.main(["check-live"]) == 2
    doc = json.loads(s3.puts[0]["Body"])
    assert doc["measured"] is False and doc["in_sync"] is False and "AccessDenied" in doc["error"]


def test_check_live_in_sync_publishes_and_a_failed_publish_is_loud(stack, tpl, monkeypatch):
    f = stack.deploy_fields()
    cfn = _Cfn("UPDATE_COMPLETE", {k: f[k] for k in ("template-sha256", "definition-sha256")})
    s3 = _S3()
    _fake_boto3(monkeypatch, cfn=cfn, sched=_scheduler_matching(stack, tpl), s3=s3)
    assert stack.main(["check-live"]) == 0
    assert json.loads(s3.puts[0]["Body"])["in_sync"] is True
    _fake_boto3(monkeypatch, cfn=cfn, sched=_scheduler_matching(stack, tpl), s3=_S3(fail=True))
    assert stack.main(["check-live"]) == 3


def _workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_workflow_path_filters_match():
    on = _workflow()[True]  # PyYAML reads the bare `on` key as boolean True
    assert on["push"]["paths"] == on["pull_request"]["paths"]
    for p in on["push"]["paths"]:
        assert (REPO / p).exists(), p


def test_only_this_workflow_assumes_the_stack_deploy_role():
    users = sorted(p.name for p in WORKFLOWS.glob("*.yml") if f"role/{NEW_ROLE}" in p.read_text(encoding="utf-8"))
    assert users == [WORKFLOW.name]


def test_apply_runs_only_on_main_and_never_on_a_pull_request():
    jobs = _workflow()["jobs"]
    cond = jobs["deploy"]["if"]
    assert "refs/heads/main" in cond and "pull_request" not in cond
    assert "schedule" in jobs["check-live"]["if"]
    assert jobs["lint"].get("permissions", {}).get("id-token") is None


def test_deploy_script_verifies_its_own_effect():
    script = (REPO / "infrastructure" / "deploy-data-collection-stack.sh").read_text(encoding="utf-8")
    deploy_at = script.index("aws cloudformation deploy")
    assert "check-live" in script[deploy_at:], "a deploy must be followed by the live comparison"
    assert "--no-fail-on-empty-changeset" in script
    for p in ("CollectionState=", "DailyHealState=", "ShadowSamedayState="):
        assert p in script, f"{p} must be passed explicitly so the template Default is authoritative"
