"""Readers for the phase EXIT criteria — the operational counters.

`alpha-engine-config-I10954`. Until this module existed, every rung's `exit:`
line in `data_gate/config/phases.yaml` was PROSE: the gate rolled up the per-cell
and per-guard clauses, and nothing anywhere measured "5 consecutive EOD
successes", "10 consecutive trading days at full universe coverage" or "sustained
over 20 trading days". Once the clause columns went green,
``data_gate read --gate data-phase1`` would have reported MET with not one of
those counters read — the same class as `alpha-engine-config-I10906` / `-I10908`
/ `-I10928`: a predicate keyed on the MECHANISM (the clause columns) rather than
on the PROPERTY the plan's exit names.

Every reader here grades artifacts that already exist — the run manifests under
``data_collection/runs/<unit>/<trading_day>/``, the committed stack schedules,
the published metric documents and the gate's own dated readings. **No new
producer.** Where the plan names a criterion whose evidence has no emitter at
all, the reader returns UNMEASURABLE naming exactly what it will read, which is
this board's standing convention (`clauses.py` module docstring): never MET, and
never a silent absence.

**A cycle is graded from the manifests, not from the scheduler's own verdict.**
A Step Functions execution that SUCCEEDED while a unit inside it wrote nothing is
the failure this board exists to notice, so "the cycle succeeded" means every
unit the schedule verifies recorded an ``ok`` ``scheduled``-trigger manifest that
started inside the cycle's window. The schedule supplies WHEN to look; the run
records supply WHAT happened.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from nousergon_lib.gates import GateStore, read_store_document
from nousergon_lib.trading_calendar import (  # pyright: ignore[reportAttributeAccessIssue]
    subtract_trading_days,
)

from data_gate.cadence import COMPLETION_GRACE, Cadence, gate_moment, latest_due_fire, parse_cron
from data_gate.descriptors import Unit
from data_gate.evidence import EMPTY_FRESH_VERDICT, Reading, empty_fresh_runs, manifests_since
from data_gate.standalone import stack_schedules

__all__ = [
    "CYCLE_COUNTS",
    "CycleSet",
    "EXECUTOR_WRITES_KEY",
    "PHASE1_COST_BASELINE_KEY",
    "PHASE1_COST_BASELINE_WEEKS",
    "PHASE1_EXIT_CONSECUTIVE",
    "PHASE2_EOD_COVERAGE_DAYS",
    "PHASE2_EMPTY_FRESH_CYCLES",
    "PHASE2_VENDOR_CYCLES",
    "PHASE3_SATURDAYS",
    "PHASE3_TRADING_DAYS",
    "RELIABILITY_STREAK_TARGET",
    "SCHEDULE_EOD",
    "SCHEDULE_MORNING",
    "SCHEDULE_WEEKLY",
    "V1_DATA_STAGE_KEY",
    "collect_cycles",
    "read_code_identity_delta",
    "read_consecutive_cycles",
    "read_cost_baseline_measured",
    "read_empty_fresh_free",
    "read_eod_universe_covered",
    "read_executor_collection_writes_zero",
    "read_sustained_window",
    "read_v1_data_stage_quiet",
    "read_vendor_divergence_emitted",
    "schedule_cadence",
]

#: The standalone schedules the plan's exit criteria count cycles of, by the
#: short name `infrastructure/data_collection_stack.py` gives them. The plan
#: §6 writes them with the v1 `ne-` prefix; the stack's own names are these,
#: and the stack definition is the authority on what exists.
SCHEDULE_EOD = "data-collection-eod"
SCHEDULE_MORNING = "data-collection-morning"
SCHEDULE_WEEKLY = "data-collection-weekly"

#: How many cycles of each schedule the readings below need, so ONE listing
#: pass per schedule serves every clause that counts over it. The EOD figure is
#: the largest of its consumers (20 empty-fresh cycles, 20 vendor cycles, 10
#: coverage days, 5 consecutive successes); the weekly figure is phase 3's four
#: Saturdays.
CYCLE_COUNTS: dict[str, int] = {
    SCHEDULE_EOD: 20,
    SCHEDULE_MORNING: 20,
    SCHEDULE_WEEKLY: 4,
}

#: Brian's ruling, 2026-09-21 (verbatim), on why phase 1's own exit no longer
#: needs a repeat count: *"it sounds like the only time gate we should have
#: here is for v2 phase 4 deleting the v1 pipelines, so a time gate here
#: makes sense. as such we should be able to work up to this point without
#: time gates."* Answering the point that nothing at the data-phase1 exit is
#: irreversible (the cutover's rollback is one PR revert) — the one
#: irreversible step in the programme is Crucible v2 phase 4 deleting the v1
#: pipelines and Lambdas (`alpha-engine-config-I10655`), so THAT is where a
#: repeat count belongs, not here.
#:
#: Phase 1's own exit now asks for ONE complete cycle of each schedule —
#: proof the standalone collector ran end to end at all — never a streak.
#: "Complete" keeps its exact prior meaning (`read_consecutive_cycles`:
#: every `verify_units` entry holds an ok scheduled-trigger manifest for that
#: fire); only the repeat count changed.
PHASE1_EXIT_CONSECUTIVE: dict[str, int] = {
    SCHEDULE_EOD: 1,
    SCHEDULE_MORNING: 1,
    SCHEDULE_WEEKLY: 1,
}

#: Plan §6 phase-1 exit's ORIGINAL figures — "5 consecutive EOD and 5
#: morning successes plus 2 Saturdays with complete manifests" — ratified as
#: the reliability bar Crucible v2 phase 4 gates its irreversible v1-pipeline
#: deletion on (`alpha-engine-config-I10655`), per Brian's 2026-09-21 ruling.
#: No longer phase 1's OWN exit count (see :data:`PHASE1_EXIT_CONSECUTIVE`
#: above) — read instead by the standing reliability-streak clauses
#: (`clauses.py::_clause_reliability_streak`), published under the
#: `data-collection-reliability` gate for Crucible v2 to read as its own
#: irreversible-action precondition, the same shape `data-cutover-ready`
#: already uses for the cutover itself.
RELIABILITY_STREAK_TARGET: dict[str, int] = {
    SCHEDULE_EOD: 5,
    SCHEDULE_MORNING: 5,
    SCHEDULE_WEEKLY: 2,
}

#: Plan §6 phase-2 exit figures.
PHASE2_EOD_COVERAGE_DAYS = 10
PHASE2_EMPTY_FRESH_CYCLES = 20
PHASE2_VENDOR_CYCLES = 20

#: Plan §6 phase-3 exit: "sustained over 20 consecutive trading days and 4
#: Saturdays".
PHASE3_TRADING_DAYS = 20
PHASE3_SATURDAYS = 4

#: Phase 1's "a 4-week tagged cost baseline measured". The baseline is the same
#: document `data.cost.monthly` grades against a ceiling at phase 3 — phase 1
#: asks only that it EXISTS and covers four weeks, which is a different
#: question about the same key.
PHASE1_COST_BASELINE_KEY = "metrics/cost/monthly/latest.json"
PHASE1_COST_BASELINE_WEEKS = 4

#: The two criteria with no emitter anywhere today. Named as constants so the
#: UNMEASURABLE row cites the key its future producer must write, and so a
#: search for the key finds the consumer that is already waiting for it.
V1_DATA_STAGE_KEY = "metrics/v1_data_stage/executions_since_cutover.json"
EXECUTOR_WRITES_KEY = "metrics/executor_profile/collection_writes/latest.json"

_SOURCE_CYCLES = "data_collection/runs/<unit>/<trading_day>/ + the committed stack schedules"
_SOURCE_STORE = "data_collection store"
_SOURCE_GATE_HISTORY = "gates/<gate>/<trading_day>/gate.json (the dated readings)"


def schedule_named(short_name: str) -> dict:
    """One committed stack schedule, by short name. Raises if it is gone.

    A raise, never a default: a schedule the plan counts cycles of that no
    longer exists in the stack is a finding, and `contain_clause_exceptions`
    turns the raise into one UNMEASURABLE row rather than a silent zero.
    """
    for schedule in stack_schedules():
        if schedule["name"] == short_name or schedule["qualified_name"].endswith(f"/{short_name}"):
            return schedule
    raise ValueError(
        f"no schedule named {short_name!r} in infrastructure/data_collection_stack.py; the "
        f"plan's exit criteria count its cycles. Present: "
        f"{sorted(s['qualified_name'] for s in stack_schedules())}"
    )


def schedule_cadence(schedule: dict) -> Cadence:
    """The schedule's cadence, from the COMMITTED definition.

    Live Scheduler state is deliberately not an input. These counters are read
    across a window that reaches back before any change to the schedule, and a
    gate that asked live state for "when should this have fired last month"
    would answer with today's expression — plus it would make every one of
    these clauses UNMEASURABLE on a store with no AWS clients, which is every
    test fixture.
    """
    return parse_cron(
        str(schedule["expression"]),
        tz=schedule["timezone"],
        trading_days_only=bool(schedule["input"].get("require_trading_day")),
    )


def _parse_utc(stamp: object) -> dt.datetime | None:
    """An ISO-8601 manifest timestamp as UTC, or ``None`` when it will not parse.

    ``None`` is never treated as "in this cycle": an unparseable `started` is
    recorded as a problem and renders the readings over the window
    UNMEASURABLE, because a run that cannot be placed in time cannot be counted
    as one that happened on time.
    """
    if not isinstance(stamp, str) or not stamp:
        return None
    try:
        parsed = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def due_fires(cadence: Cadence, *, as_of: dt.datetime, count: int) -> list[dt.datetime]:
    """The ``count`` most recent fires whose runs should be finished, newest first."""
    fires: list[dt.datetime] = []
    moment = as_of
    for _ in range(count):
        try:
            fire = latest_due_fire(cadence, as_of=moment, grace=COMPLETION_GRACE)
        except ValueError:
            break
        fires.append(fire)
        # One second before this fire's own completion deadline, so the next
        # walk back cannot return the same fire again.
        moment = fire + COMPLETION_GRACE - dt.timedelta(seconds=1)
    return fires


@dataclass
class Cycle:
    """One due fire of a schedule, and what its units recorded."""

    fire: dt.datetime
    manifests: dict[str, list[tuple[str, dict]]] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return self.fire.strftime("%Y-%m-%dT%H:%MZ")

    def ok_units(self) -> set[str]:
        return {
            unit_id
            for unit_id, docs in self.manifests.items()
            if any(d.get("trigger") == "scheduled" and d.get("status") == "ok" for _, d in docs)
        }

    def all_manifests(self) -> list[dict]:
        return [d for docs in self.manifests.values() for _, d in docs]


@dataclass
class CycleSet:
    """Every graded unit's manifests for the last N due fires of one schedule.

    Built ONCE per schedule per evaluation and shared by every clause that
    counts over it, because each cycle costs a listing per unit per day folder
    and four clauses re-deriving the same window would multiply that by four.
    """

    schedule: str
    units: list[Unit]
    cycles: list[Cycle] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    unreadable: str | None = None
    where: str = ""

    @property
    def unmeasurable(self) -> bool:
        return self.unreadable is not None or bool(self.problems)

    @property
    def reason(self) -> str:
        if self.unreadable is not None:
            return self.unreadable
        return f"manifest(s) unreadable: {sorted(self.problems)[:4]}"

    @property
    def unit_ids(self) -> list[str]:
        return [u.unit_id for u in self.units]


def collect_cycles(
    store: GateStore,
    units: list[Unit],
    *,
    schedule_name: str,
    count: int,
    trading_day: dt.date,
    now: dt.datetime | None = None,
) -> CycleSet:
    """The last ``count`` due fires of ``schedule_name``, with each unit's manifests.

    A listing or read failure is recorded on the set and renders every clause
    over it UNMEASURABLE — never an empty window, which would read as "the
    schedule never fired" and file the finding against the collector instead of
    against the reader's own access.
    """
    try:
        schedule = schedule_named(schedule_name)
        cadence = schedule_cadence(schedule)
    except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
        # The failure mode is a schedule the plan counts cycles of that the
        # committed stack no longer declares, or declares in a cron shape this
        # parser refuses. The board survives; the recording surface is every
        # clause built on this set.
        return CycleSet(
            schedule=schedule_name,
            units=[],
            unreadable=f"{type(exc).__name__}: {exc}",
        )
    verify = list(schedule["input"].get("verify_units") or [])
    by_id = {u.unit_id: u for u in units}
    graded = [by_id[uid] for uid in verify if uid in by_id and not by_id[uid].retired]
    result = CycleSet(schedule=schedule["qualified_name"], units=graded)
    missing = sorted(uid for uid in verify if uid not in by_id)
    if missing:
        result.unreadable = (
            f"{schedule['qualified_name']} verifies {missing}, which have no descriptor — the "
            "board cannot grade a cycle over a unit it does not carry"
        )
        return result
    as_of = gate_moment(trading_day, now)
    fires = due_fires(cadence, as_of=as_of, count=count)
    if not fires:
        result.unreadable = (
            f"no due fire of {schedule['qualified_name']} ({schedule['expression']}) within the "
            f"lookback of {as_of.isoformat()}"
        )
        return result
    result.cycles = [Cycle(fire=fire) for fire in fires]
    # ONE listing pass per unit over the WHOLE window, then bucketed by fire.
    # Listing per (fire, unit) instead would re-list the same day folders once
    # per fire — the same answer at five times the S3 calls, on a reader that
    # already lists 46 units' prefixes elsewhere in the same evaluation.
    oldest = fires[-1]
    for unit in graded:
        try:
            docs, problems, where = manifests_since(store, unit, since=oldest, as_of=as_of)
        except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
            # A denied or failed LIST over the unit's manifest prefix: we could
            # not look, which is never an empty window. The rest of the board
            # survives; the recording surface is every clause built on this set.
            result.unreadable = (
                f"could not list {unit.run_manifest_prefix} over "
                f"[{oldest.isoformat()}, {as_of.isoformat()}]: {type(exc).__name__}: {exc}"
            )
            return result
        result.problems.extend(problems)
        result.where = where
        for cycle in result.cycles:
            cycle.manifests.setdefault(unit.unit_id, [])
        for key, doc in docs:
            started = _parse_utc(doc.get("started"))
            if started is None:
                result.problems.append(f"{key}: `started` does not parse ({doc.get('started')!r})")
                continue
            for cycle in result.cycles:
                if cycle.fire <= started <= cycle.fire + COMPLETION_GRACE:
                    cycle.manifests[unit.unit_id].append((key, doc))
                    break
    return result


def _cycle_verdict(cycles: CycleSet) -> list[tuple[Cycle, set[str], list[str]]]:
    """Per cycle: the units that recorded an ok scheduled manifest, and the rest."""
    rows = []
    for cycle in cycles.cycles:
        ok = cycle.ok_units()
        rows.append((cycle, ok, sorted(set(cycles.unit_ids) - ok)))
    return rows


def read_consecutive_cycles(cycles: CycleSet, *, required: int) -> Reading:
    """``required`` consecutive complete cycles, counted back from the most recent.

    CONSECUTIVE, ending at the latest due fire: a run of five successes three
    weeks ago followed by four failures is not "5 consecutive successes", and a
    counter that tallied successes anywhere in the window would say it was.
    """
    if cycles.unmeasurable:
        return Reading(
            met=False,
            detail=f"cannot count cycles of {cycles.schedule}: {cycles.reason}",
            evidence=(cycles.schedule,),
            unmeasurable=True,
            source=_SOURCE_CYCLES,
        )
    rows = _cycle_verdict(cycles)
    streak = 0
    first_gap = ""
    for cycle, _ok, missing in rows:
        if missing:
            first_gap = (
                f"the {cycle.label} cycle is incomplete: no ok scheduled-trigger manifest for "
                f"{missing[:8]}"
            )
            break
        streak += 1
    detail = (
        f"{streak} consecutive complete cycle(s) of {cycles.schedule} ending at the latest due "
        f"fire, against the {required} the plan's exit names, over {len(rows)} cycle(s) examined "
        f"and {len(cycles.unit_ids)} verified unit(s)"
    )
    if first_gap:
        detail += f"; {first_gap}"
    return Reading(
        met=streak >= required,
        detail=detail,
        evidence=(cycles.schedule, "data_collection/runs/"),
        source=_SOURCE_CYCLES,
        as_of=rows[0][0].label if rows else None,
    )


def read_code_identity_delta(cycles: CycleSet) -> Reading:
    """The collector's OWN code identity, cycle over cycle — visibility only.

    `alpha-engine-config-I10931`: the schedule pulls whatever is on `main` at
    fire time with no pin. `weekly_collector.py` already measures `code_sha`
    honestly for every recorded unit — `run_units.recorded_entry` defaults to
    `nousergon_lib.run_identity.resolve_code_sha()`, which is `git rev-parse
    HEAD` on the box's own post-pull tree (or a validated `$NE_DATA_CODE_SHA`),
    never an env default and never guessed. Established by reading that code
    path, not re-derived per call. So this reader does NOT need to make
    `code_sha` honest — it only needs to make the delta VISIBLE.

    Compares the most recent populated cycle against the one before it and
    renders whether the tree that ran changed. `met` is always True: a code
    change between cycles is not itself a defect (I10931's own "immediate
    mitigation" section: pinning to a stale sha is a worse failure mode than
    running `main`), and giving this a floor would be the "widen the floor to
    make it pass" anti-pattern applied to a visibility gap rather than a real
    threshold. It exists to be READ on the board, not to gate anything.
    """
    if cycles.unmeasurable:
        return Reading(
            met=False,
            detail=f"cannot compare code identity across cycles of {cycles.schedule}: {cycles.reason}",
            evidence=(cycles.schedule,),
            unmeasurable=True,
            source=_SOURCE_CYCLES,
        )
    shas_by_cycle: list[tuple[Cycle, set[str]]] = []
    for cycle in cycles.cycles:
        shas: set[str] = set()
        for docs in cycle.manifests.values():
            for _key, doc in docs:
                if doc.get("trigger") == "scheduled" and doc.get("status") == "ok":
                    sha = str(doc.get("code_sha") or "").strip()
                    if sha:
                        shas.add(sha)
        shas_by_cycle.append((cycle, shas))

    populated = [(cycle, shas) for cycle, shas in shas_by_cycle if shas]
    if not populated:
        return Reading(
            met=True,
            detail=(
                f"no ok scheduled-trigger manifest over the last {len(cycles.cycles)} cycle(s) of "
                f"{cycles.schedule} carries a code_sha — nothing to compare yet"
            ),
            evidence=(cycles.schedule, "data_collection/runs/"),
            source=_SOURCE_CYCLES,
        )
    current_cycle, current_shas = populated[0]
    if len(populated) < 2:
        return Reading(
            met=True,
            detail=(
                f"{current_cycle.label} ran {sorted(current_shas)} — only one populated cycle in "
                f"the last {len(cycles.cycles)} of {cycles.schedule}; no earlier cycle to compare"
            ),
            evidence=(cycles.schedule, "data_collection/runs/"),
            source=_SOURCE_CYCLES,
            as_of=current_cycle.label,
        )
    previous_cycle, previous_shas = populated[1]
    if len(current_shas) > 1 or len(previous_shas) > 1:
        detail = (
            f"{current_cycle.label} recorded {len(current_shas)} distinct code_sha across its ok "
            f"units ({sorted(current_shas)}); {previous_cycle.label} recorded {len(previous_shas)} "
            f"({sorted(previous_shas)}) — a single cycle running more than one tree is its own "
            "finding, not just a delta"
        )
    elif current_shas == previous_shas:
        detail = (
            f"{current_cycle.label} ran the same code as {previous_cycle.label} "
            f"({next(iter(current_shas))})"
        )
    else:
        detail = (
            f"collector code changed between {previous_cycle.label} "
            f"({next(iter(previous_shas))}) and {current_cycle.label} "
            f"({next(iter(current_shas))}) — first production run of this tree"
        )
    return Reading(
        met=True,
        detail=detail,
        evidence=(cycles.schedule, "data_collection/runs/"),
        source=_SOURCE_CYCLES,
        as_of=current_cycle.label,
    )


def read_empty_fresh_free(cycles: CycleSet, *, required_cycles: int) -> Reading:
    """Objective 6 at the phase-2 exit: zero empty-but-fresh writes over N cycles.

    Counted from the guards' own ``empty_fresh`` verdict via
    `evidence.empty_fresh_runs`, never from ``rows_out == 0`` — the two
    questions differ, and the naive form files every legitimately idle phase as
    a breach.

    A window with FEWER than ``required_cycles`` observed is UNMET, not MET:
    "no empty-fresh writes seen in the three cycles that exist" is not the
    twenty-cycle claim the exit makes.
    """
    if cycles.unmeasurable:
        return Reading(
            met=False,
            detail=f"cannot count empty-but-fresh writes over {cycles.schedule}: {cycles.reason}",
            evidence=(cycles.schedule,),
            unmeasurable=True,
            source=_SOURCE_CYCLES,
        )
    offenders: list[str] = []
    observed = 0
    for cycle in cycles.cycles:
        manifests = cycle.all_manifests()
        if manifests:
            observed += 1
        for run_id in empty_fresh_runs(manifests):
            offenders.append(f"{cycle.label}:{run_id}")
    met = observed >= required_cycles and not offenders
    detail = (
        f"{len(offenders)} empty-but-fresh write(s) (guard verdict {EMPTY_FRESH_VERDICT!r}) over "
        f"{observed} observed cycle(s) of {cycles.schedule}, against the {required_cycles} the "
        "exit requires"
    )
    if offenders:
        detail += f"; offending runs: {offenders[:8]}"
    if observed < required_cycles:
        detail += (
            " — fewer cycles have recorded anything than the exit counts over, so the count is "
            "not yet the claim the exit makes"
        )
    return Reading(
        met=met,
        detail=detail,
        evidence=(cycles.schedule, "data_collection/runs/"),
        source=_SOURCE_CYCLES,
    )


#: The guard whose verdict IS the vendor cross-check. Plan §6 phase-2 exit:
#: "vendor divergence emitted 20 of 20 within bound or each breach named".
VENDOR_GUARD = "vendor_crosscheck"

#: `validators/expectations.py::GuardReading.clean` — the two verdicts that are
#: within bound. Everything else in the closed vocabulary is a divergence to
#: NAME, and `unmeasurable` is handled separately because it is the guard
#: failing to look rather than a divergence it found.
_VENDOR_CLEAN = frozenset({"ok", "not_applicable"})


def read_vendor_divergence_emitted(
    cycle_sets: list[CycleSet], *, required_cycles: int
) -> Reading:
    """Emitted on every cycle — a breach is named, a SILENCE is the failure.

    The exit's "or each breach named" is deliberate: a divergence outside the
    bound is a finding about the vendors and does not fail this clause once it
    is stated. A cycle that emitted NO verdict at all is the failure, because
    that is indistinguishable from agreement while being a total absence of
    measurement.
    """
    unmeasurable = [c for c in cycle_sets if c.unmeasurable]
    if unmeasurable:
        return Reading(
            met=False,
            detail="; ".join(f"{c.schedule}: {c.reason}" for c in unmeasurable),
            evidence=tuple(c.schedule for c in cycle_sets),
            unmeasurable=True,
            source=_SOURCE_CYCLES,
        )
    graded: list[str] = []
    silent: list[str] = []
    blind: list[str] = []
    breaches: list[str] = []
    emitted = 0
    total = 0
    for cycles in cycle_sets:
        vendors = [u for u in cycles.units if VENDOR_GUARD in u.guards]
        if not vendors:
            continue
        graded.extend(f"{u.unit_id}@{cycles.schedule}" for u in vendors)
        total += len(cycles.cycles)
        for cycle in cycles.cycles:
            verdicts = [
                str(g.get("verdict"))
                for unit in vendors
                for _key, doc in cycle.manifests.get(unit.unit_id, [])
                for g in (doc.get("guards") or [])
                if str(g.get("guard") or g.get("name") or "") == VENDOR_GUARD
            ]
            if not verdicts:
                silent.append(cycle.label)
                continue
            if any(v == "unmeasurable" for v in verdicts):
                # The guard ran and could not look. Never a pass
                # (`observability-policy` §8.3), and not the same finding as a
                # named breach, so it is counted on its own line.
                blind.append(cycle.label)
                continue
            emitted += 1
            off_bound = sorted({v for v in verdicts if v not in _VENDOR_CLEAN})
            if off_bound:
                breaches.append(f"{cycle.label}:{off_bound}")
    detail = (
        f"a {VENDOR_GUARD} verdict was emitted on {emitted} of {total} examined cycle(s) "
        f"({required_cycles} required) for {sorted(graded) or 'no unit declaring the guard'}"
    )
    if silent:
        detail += f"; cycles with NO verdict at all: {silent[:8]}"
    if blind:
        detail += f"; cycles whose verdict was `unmeasurable` (the guard could not look): {blind[:8]}"
    if breaches:
        detail += f"; named breaches (a breach stated is not a failure of this clause): {breaches[:8]}"
    if not graded:
        detail += (
            " — no unit verified by these schedules declares the guard, so nothing measured the "
            "criterion"
        )
    return Reading(
        met=bool(graded) and emitted >= required_cycles and not silent and not blind,
        detail=detail,
        evidence=tuple(c.schedule for c in cycle_sets) + ("data_collection/runs/",),
        source=_SOURCE_CYCLES,
    )


def read_eod_universe_covered(
    store: GateStore, *, trading_day: dt.date, days: int
) -> Reading:
    """Full EOD universe coverage on ``days`` CONSECUTIVE trading days.

    Reads the completeness `MetricRecord` the cardinality guard already
    publishes per trading day — the same key `evidence.read_completeness_metric`
    grades for one day, counted back across the window. A day with no document
    breaks the streak: a missing measurement is not a passing one.
    """
    statuses: list[tuple[str, str]] = []
    day = trading_day
    for _ in range(days):
        key = f"metrics/eod_completeness/{day.isoformat()}.json"
        read = read_store_document(store, key)
        if read.problem is not None:
            return Reading(
                met=False,
                detail=f"could not read {key}: {read.problem}",
                evidence=(key,),
                unmeasurable=True,
                source=_SOURCE_STORE,
            )
        if read.absent:
            statuses.append((day.isoformat(), "ABSENT"))
        else:
            statuses.append((day.isoformat(), str((read.document or {}).get("status") or "?")))
        day = subtract_trading_days(day, 1)
    streak = 0
    for _day, status in statuses:
        if status != "GREEN":
            break
        streak += 1
    detail = (
        f"{streak} consecutive trading day(s) at full declared coverage (MetricRecord GREEN) "
        f"ending {trading_day.isoformat()}, against the {days} the exit names; the window read "
        f"{dict(statuses[:6])}"
    )
    return Reading(
        met=streak >= days,
        detail=detail,
        evidence=tuple(f"metrics/eod_completeness/{d}.json" for d, _ in statuses[:4]),
        source=_SOURCE_STORE,
    )


def read_cost_baseline_measured(store: GateStore, *, weeks: int) -> Reading:
    """Phase 1's "a 4-week tagged cost baseline measured".

    A different question from `data.cost.monthly`, which grades the same
    document against the ratified ceiling at phase 3. Here the exit asks only
    that the baseline EXISTS and covers the window — a ceiling comparison
    against two days of data is a number, not a baseline.
    """
    key = PHASE1_COST_BASELINE_KEY
    read = read_store_document(store, key)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {key}: {read.problem}",
            evidence=(key,),
            unmeasurable=True,
            source=_SOURCE_STORE,
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"no cost document at {key}. Nothing publishes tagged component=data-collection "
                "spend yet, so there is no baseline to compare a ceiling against — the exit's "
                "measurement has not been taken."
            ),
            evidence=(key,),
            source=_SOURCE_STORE,
        )
    document = read.document or {}
    covered = document.get("days_covered")
    baseline = document.get("baseline")
    try:
        days = int(covered)
    except (TypeError, ValueError):
        return Reading(
            met=False,
            detail=(
                f"{key} carries days_covered={covered!r}, which is not a number of days. The "
                "exit counts a four-week window, so a baseline that does not state its own "
                "coverage cannot answer it."
            ),
            evidence=(key,),
            unmeasurable=True,
            source=_SOURCE_STORE,
            as_of=str(document.get("as_of") or ""),
        )
    needed = weeks * 7
    return Reading(
        met=days >= needed and baseline is not None,
        detail=(
            f"{key}: baseline={baseline}, days_covered={days} against the {needed} days "
            f"({weeks} weeks) the exit names"
        ),
        evidence=(key,),
        source=_SOURCE_STORE,
        as_of=str(document.get("as_of") or ""),
    )


def _pending(key: str, what: str, reader: str) -> Reading:
    """UNMEASURABLE, naming the key and the reader that will answer it.

    The board's standing convention for a criterion whose evidence has no
    emitter: never MET, never silently absent, and the row itself is the
    tracked statement of what is missing.
    """
    return Reading(
        met=False,
        detail=(
            f"{what} No producer writes {key} and no reader exists for it yet, so this exit "
            f"criterion has not been measured. UNMEASURABLE rather than UNMET because nothing "
            f"was denied and nothing was found absent — we have not looked. Needed: {reader}"
        ),
        evidence=(key,),
        unmeasurable=True,
        source="no emitter",
    )


def read_v1_data_stage_quiet(store: GateStore) -> Reading:
    """Phase 1's "v1 SF data-stage executions since cutover = 0".

    Two facts neither of which exists as an artifact today: the cutover instant,
    and the v1 pipelines' executions after it. Both are readable — the cutover
    instant from the moment the standalone schedules were enabled, the
    executions from ``states:ListExecutions`` over the v1 data-stage machines —
    but nothing publishes either, and a gate READS; it never runs a survey of
    its own. So the row states what it will read.
    """
    read = read_store_document(store, V1_DATA_STAGE_KEY)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {V1_DATA_STAGE_KEY}: {read.problem}",
            evidence=(V1_DATA_STAGE_KEY,),
            unmeasurable=True,
            source=_SOURCE_STORE,
        )
    if read.absent:
        return _pending(
            V1_DATA_STAGE_KEY,
            "Phase 1 exits only when the v1 Step Functions data stages have run ZERO times "
            "since the cutover.",
            "an emitter that records the cutover instant and counts states:ListExecutions over "
            "the v1 data-stage machines since it",
        )
    document = read.document or {}
    count = document.get("executions_since_cutover")
    try:
        executions = int(count)
    except (TypeError, ValueError):
        return Reading(
            met=False,
            detail=(
                f"{V1_DATA_STAGE_KEY} carries executions_since_cutover={count!r}, which is not a "
                "count. A field the exit counts on that does not parse is a finding, not a zero."
            ),
            evidence=(V1_DATA_STAGE_KEY,),
            unmeasurable=True,
            source=_SOURCE_STORE,
        )
    return Reading(
        met=executions == 0,
        detail=(
            f"{V1_DATA_STAGE_KEY}: {executions} v1 data-stage execution(s) since the cutover at "
            f"{document.get('cutover_utc')}; the exit requires 0"
        ),
        evidence=(V1_DATA_STAGE_KEY,),
        source=_SOURCE_STORE,
        as_of=str(document.get("as_of") or ""),
    )


def read_executor_collection_writes_zero(store: GateStore, *, days: int = 7) -> Reading:
    """Phase 2's "the executor profile shows no collection writes over 7 days"."""
    read = read_store_document(store, EXECUTOR_WRITES_KEY)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {EXECUTOR_WRITES_KEY}: {read.problem}",
            evidence=(EXECUTOR_WRITES_KEY,),
            unmeasurable=True,
            source=_SOURCE_STORE,
        )
    if read.absent:
        return _pending(
            EXECUTOR_WRITES_KEY,
            "Phase 2 exits only when the executor has written NOTHING into the collection "
            "prefixes for seven days — the separation the whole collector split exists to "
            "establish.",
            "an emitter that profiles the executor's writes from the CloudTrail S3 archive "
            f"(the source data.human_touch.monthly already reads) over a rolling {days} days",
        )
    document = read.document or {}
    writes = document.get("collection_writes")
    try:
        count = int(writes)
    except (TypeError, ValueError):
        return Reading(
            met=False,
            detail=(
                f"{EXECUTOR_WRITES_KEY} carries collection_writes={writes!r}, which is not a "
                "count."
            ),
            evidence=(EXECUTOR_WRITES_KEY,),
            unmeasurable=True,
            source=_SOURCE_STORE,
        )
    covered = document.get("days_covered")
    return Reading(
        met=count == 0 and str(covered) == str(days),
        detail=(
            f"{EXECUTOR_WRITES_KEY}: {count} executor write(s) into collection prefixes over "
            f"days_covered={covered} (the exit names {days} days and 0 writes)"
        ),
        evidence=(EXECUTOR_WRITES_KEY,),
        source=_SOURCE_STORE,
        as_of=str(document.get("as_of") or ""),
    )


def read_sustained_window(
    store: GateStore,
    *,
    gate: str,
    self_clause: str,
    trading_day: dt.date,
    trading_days: int,
    weekly: CycleSet,
    saturdays: int,
) -> Reading:
    """Phase 3's sustain window, read from the gate's OWN dated readings.

    "All clauses MET, 0 UNMEASURABLE, 0 UNREPORTED, sustained over 20
    consecutive trading days and 4 Saturdays" (plan §6). The dated readings
    under ``gates/<gate>/<day>/gate.json`` are the only record of what the board
    said on a past day, and they are never overwritten, so they are what a
    SUSTAIN claim can honestly be built on.

    ``self_clause`` is excluded from every historical reading it grades. Without
    that exclusion the predicate is circular in the way `gate-taxonomy-policy`
    names: this clause is UNMET until the window is clean, the window can never
    be clean while it is UNMET, and the phase could never exit no matter how
    the system behaved.
    """
    days: list[dt.date] = []
    day = trading_day
    for _ in range(trading_days):
        days.append(day)
        day = subtract_trading_days(day, 1)
    clean = 0
    verdicts: list[str] = []
    for day in days:
        key = f"gates/{gate}/{day.isoformat()}/gate.json"
        read = read_store_document(store, key)
        if read.problem is not None:
            return Reading(
                met=False,
                detail=f"could not read {key}: {read.problem}",
                evidence=(key,),
                unmeasurable=True,
                source=_SOURCE_GATE_HISTORY,
            )
        if read.absent:
            verdicts.append(f"{day.isoformat()}:no reading")
            break
        rows = [
            row
            for row in (read.document or {}).get("clauses") or []
            if row.get("name") != self_clause
        ]
        unmet = [str(r.get("name")) for r in rows if not r.get("met") and not r.get("unmeasurable")]
        unmeas = [str(r.get("name")) for r in rows if r.get("unmeasurable")]
        if not rows:
            verdicts.append(f"{day.isoformat()}:reading carries no clauses")
            break
        if unmet or unmeas:
            verdicts.append(
                f"{day.isoformat()}:{len(unmet)} UNMET, {len(unmeas)} UNMEASURABLE "
                f"(first: {sorted(unmet + unmeas)[:3]})"
            )
            break
        clean += 1
        verdicts.append(f"{day.isoformat()}:clean")

    if weekly.unmeasurable:
        return Reading(
            met=False,
            detail=f"cannot count Saturdays: {weekly.reason}",
            evidence=(weekly.schedule,),
            unmeasurable=True,
            source=_SOURCE_CYCLES,
        )
    complete_saturdays = 0
    for cycle, _ok, missing in _cycle_verdict(weekly):
        if missing:
            break
        complete_saturdays += 1

    detail = (
        f"{clean}/{trading_days} consecutive trading day(s) whose dated {gate} reading carried "
        f"0 UNMET and 0 UNMEASURABLE clauses (this clause itself excluded, so the window is not "
        f"its own precondition), and {complete_saturdays}/{saturdays} consecutive complete "
        f"{weekly.schedule} cycle(s); the window read {verdicts[:4]}"
    )
    return Reading(
        met=clean >= trading_days and complete_saturdays >= saturdays,
        detail=detail,
        evidence=(f"gates/{gate}/", weekly.schedule),
        source=_SOURCE_GATE_HISTORY,
    )
