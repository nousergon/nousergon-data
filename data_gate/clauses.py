"""Clause generation for the data collector's red board.

`data_collection_plan_260914.md` §4.1. Four families, all GENERATED from the
committed descriptors — nothing here is hand-listed:

* **base**, one per audit cell: ``data.<unit>.<column>`` over the nine scored
  columns. 46 units x 9 = **414**, matching the audit exactly, and
  `tests/test_every_audit_cell_is_a_clause.py` fails if a cell has no clause or
  a clause has no cell.
* **guard**, one per applicable audit §4.1 class:
  ``data.<unit>.guard.{empty_fresh,cardinality,units,pit,vendor_fallback,
  success_without_output}``, plus ``vendor_crosscheck`` on the two units that
  write one key.
* **objective**: ``data.slo.freshness.<family>``,
  ``data.slo.completeness.<family>``, ``data.cost.monthly``,
  ``data.pages.monthly``, ``data.human_touch.monthly``.
* **inventory**: ``data.inventory.writers_declared``, plus the board's own
  honesty clauses.

**RED BY DEFAULT.** At birth every clause is red, including the 214 cells the
audit scored PRESENT. A PRESENT cell becomes MET only when the gate MEASURES it.
Phase-0 evidence readers that do not exist yet return UNMEASURABLE **naming the
key they will read** — never MET, and never a silent absence. That is the whole
difference between a board that is honest and a board that is green.

Every clause carries `console-policy` §5.1's four fields: state (the clause's
own MET/UNMET/UNMEASURABLE), source, as_of and evidence.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from nousergon_lib.gates import Clause, clause_member_status, contain_clause_exceptions, unmeasurable

from data_gate import evidence as ev
from data_gate.descriptors import AUDIT_COLUMNS, GUARD_CLASSES, OPTIONAL_GUARD_CLASSES, Unit
from data_gate.inventory import scan

__all__ = [
    "BOARD_CLAUSES",
    "CLAUSE_PREFIX",
    "CUTOVER_READY_CLAUSES",
    "RetiredClause",
    "base_clause_name",
    "is_retired",
    "base_clause_names",
    "generate",
    "guard_clause_name",
]

CLAUSE_PREFIX = "data"

#: Which phase's exit each objective clause must hold. Phase 3's exit is "all
#: clauses MET", and every SLO row is ratified there (plan §6).
_OBJECTIVE_PHASE = 3

#: The board's own honesty clauses — phase 0, because the phase-0 gate measures
#: that the board is HONEST, not that it is green (plan §6).
BOARD_CLAUSES: tuple[str, ...] = (
    "data.inventory.writers_declared",
    "data.board.population_complete",
    "data.board.cells_reconciled",
    "data.board.phase_trackers_declared",
    "data.gate.ladder_fresh",
)

#: The `data-cutover-ready` sub-gate's own clauses (plan §6.2 step 3,
#: `alpha-engine-config-I10777`) — tagged `phase="data-cutover-ready"` rather
#: than a numbered phase, so `read.evaluate` selects them by exact phase
#: match instead of by ceiling (`read.GATES["data-cutover-ready"] is None`).
CUTOVER_READY_CLAUSES: tuple[str, ...] = (
    "data.cutover_ready.stack_check_live",
    "data.cutover_ready.units_covered",
    "data.cutover_ready.roles_bootstrapped",
    "data.cutover_ready.parity",
)

#: The two IAM roles plan §6.2 step 3 names for "roles bootstrapped": the
#: standalone stack's own execution role, and the identity that deploys it.
CUTOVER_READY_ROLES: tuple[str, ...] = (
    "nousergon-data-collection-sfn-role",
    "github-actions-data-collection-stack-deploy",
)


def base_clause_name(unit_id: str, column: str) -> str:
    return f"{CLAUSE_PREFIX}.{unit_id}.{column}"


def guard_clause_name(unit_id: str, guard: str) -> str:
    return f"{CLAUSE_PREFIX}.{unit_id}.guard.{guard}"


def base_clause_names(units: list[Unit]) -> list[str]:
    """Exactly one name per audit cell, in (unit, column) order."""
    return [base_clause_name(u.unit_id, column) for u in units for column in AUDIT_COLUMNS]


# ---------------------------------------------------------------------------
# Base clauses — one per audit cell.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetiredClause(Clause):
    """A clause of a unit whose descriptor declares ``lifecycle: retired``.

    `alpha-engine-config-I10823` deliverable 6; `observability-policy` §8.3's
    ``RETIRED``: removed on purpose, the row persists so the absence is STATED.
    Never MET (``met=False``) and never unmeasurable — it is not graded at all.
    `read.evaluate` excludes it from every gate, so it counts in no MET / UNMET
    / UNMEASURABLE denominator and in no phase's red count, and the board
    renders it ``RETIRED`` with the retirement's ruling and reason in `detail`.
    `descriptors.py` refuses a retired descriptor without that reason.
    """

    retirement: str = ""


def is_retired(clause: Clause) -> bool:
    return isinstance(clause, RetiredClause)


def _retired(unit: Unit, name: str, requirement: str, phase: str) -> RetiredClause:
    return RetiredClause(
        name,
        requirement,
        False,
        f"RETIRED: {unit.retirement_summary}. {unit.unit_id} is declared `lifecycle: retired`, so "
        "this clause is not graded and counts toward no gate.",
        (unit.path.relative_to(unit.path.parents[2]).as_posix(),),
        phase=phase,
        source="registry.d/units (lifecycle: retired)",
        retirement=unit.retirement_summary,
    )


def _clause_base(store: ev.GateStore, unit: Unit, column: str, *, trading_day: dt.date) -> Clause:
    name = base_clause_name(unit.unit_id, column)
    phase = f"data-phase{unit.clause_phase[column]}"
    cell = unit.cells[column]
    requirement = ev.BASE_REQUIREMENTS[column].format(unit=unit.unit_id, title=unit.title)
    if unit.retired:
        return _retired(unit, name, requirement, phase)
    reading = ev.read_base(store, unit, column, trading_day=trading_day)

    if reading.unmeasurable:
        return unmeasurable(
            name,
            requirement,
            f"{reading.detail} (audit 2026-09-14 scored this cell {cell}; the cell is the "
            "baseline, never the evidence)",
            reading.evidence,
            phase=phase,
            source=reading.source,
        )
    detail = reading.detail
    if reading.met and cell != "PRESENT":
        detail = (
            f"{detail} — DISAGREES with the audit, which scored this cell {cell} on "
            "2026-09-14. Either the audit was wrong or the system has changed since; "
            "this reading cites its evidence and the disagreement is itself a finding "
            "(plan §4.1 'red by default')."
        )
    if not reading.met and cell == "PRESENT":
        detail = (
            f"{detail} — DISAGREES with the audit, which scored this cell PRESENT on "
            "2026-09-14. A PRESENT cell that will not re-measure is a drift, not a "
            "transcription error to paper over."
        )
    return Clause(
        name,
        requirement,
        reading.met,
        detail,
        reading.evidence,
        phase=phase,
        source=reading.source,
        as_of=reading.as_of,
    )


# ---------------------------------------------------------------------------
# Guard clauses — one per applicable audit §4.1 class.
# ---------------------------------------------------------------------------


def _clause_guard(store: ev.GateStore, unit: Unit, guard: str, *, trading_day: dt.date) -> Clause:
    name = guard_clause_name(unit.unit_id, guard)
    block = unit.guards[guard]
    state = str(block.get("state"))
    note = str(block.get("note") or "").strip()
    requirement = (
        f"{unit.unit_id} halts or refuses on the '{guard}' silent-degradation class, and the "
        "guard has been COMMISSIONED by an induced fault (observability-policy §9.1: a guard "
        "that has never fired is not in service)"
    )
    if unit.retired:
        return _retired(unit, name, requirement, "data-phase2")
    if state == "not_applicable":
        # An N/A with a code from the closed taxonomy is a real answer, and it
        # is MET — but only because `descriptors.py` refuses an N/A without one.
        code = block["na_code"]
        return Clause(
            name,
            requirement,
            True,
            f"not applicable: {code} — {note}",
            (unit.path.relative_to(unit.path.parents[2]).as_posix(),),
            phase="data-phase2",
            source="registry.d/units",
            as_of=str(unit.raw["audit"]["baseline_date"]),
        )
    reading = ev.read_guard_commissioning(store, unit, guard, trading_day=trading_day)
    if reading.unmeasurable:
        return unmeasurable(
            name,
            requirement,
            f"declared state {state!r} ({note}); {reading.detail}",
            reading.evidence,
            phase="data-phase2",
            source=reading.source,
        )
    return Clause(
        name,
        requirement,
        reading.met,
        f"declared state {state!r} ({note}); {reading.detail}",
        reading.evidence,
        phase="data-phase2",
        source=reading.source,
        as_of=reading.as_of,
    )


# ---------------------------------------------------------------------------
# Objective clauses — plan §2.
# ---------------------------------------------------------------------------


def _clause_objective(store: ev.GateStore, name: str, requirement: str, key: str) -> Clause:
    reading = ev.read_objective(store, key)
    if reading.unmeasurable:
        return unmeasurable(
            name,
            requirement,
            reading.detail,
            reading.evidence,
            phase=f"data-phase{_OBJECTIVE_PHASE}",
            source=reading.source,
        )
    return Clause(
        name,
        requirement,
        reading.met,
        reading.detail,
        reading.evidence,
        phase=f"data-phase{_OBJECTIVE_PHASE}",
        source=reading.source,
        as_of=reading.as_of,
    )


# ---------------------------------------------------------------------------
# The board's own honesty clauses — phase 0.
# ---------------------------------------------------------------------------


def _clause_inventory_writers_declared(store: ev.GateStore, units: list[Unit]) -> Clause:
    """Every write site resolves to a descriptor, and every descriptor to a write site.

    The one clause that can notice a unit nobody registered. It reads the source
    tree, not the store, so it is measurable on day one — which is why plan §6
    puts it in the phase-0 EXIT rather than leaving it for later.
    """
    reading = scan(units)
    evidence = ("data_gate/config/writer_inventory.yaml", "registry.d/units/")
    requirement = (
        "every S3 PUT and ArcticDB write call site in the declared producer roots resolves "
        "to a unit descriptor, and every descriptor resolves to a write site"
    )
    if reading.parse_failures:
        return unmeasurable(
            "data.inventory.writers_declared",
            requirement,
            f"{len(reading.parse_failures)} file(s) would not parse, so the scan's own "
            f"denominator is incomplete: {sorted(reading.parse_failures)[:5]}. A file that "
            "will not parse is not 'no write sites'.",
            evidence,
            phase="data-phase0",
            source="data_gate.inventory",
        )
    problems: list[str] = []
    if reading.undeclared:
        problems.append(
            f"{len(reading.undeclared)} write site file(s) with NO descriptor: "
            f"{reading.undeclared[:8]}"
        )
    if reading.units_without_write_site:
        problems.append(
            f"{len(reading.units_without_write_site)} descriptor(s) whose code_path has no "
            f"write site: {reading.units_without_write_site[:8]}"
        )
    coverage = (
        f"scanned {len(reading.write_sites)} producer file(s); "
        f"{len(reading.units_via_shared_writer)} unit(s) write through a declared shared "
        f"writer rather than a site of their own; {len(reading.units_unverifiable)} unit(s) "
        f"cannot be checked in the descriptor->site direction at all "
        f"({sorted(reading.units_unverifiable)})"
    )
    return Clause(
        "data.inventory.writers_declared",
        requirement,
        reading.ok,
        f"{'; '.join(problems) if problems else 'bijection holds'}. {coverage}",
        evidence,
        phase="data-phase0",
        source="data_gate.inventory",
    )


def _clause_board_population_complete(store: ev.GateStore, units: list[Unit]) -> Clause:
    """One descriptor per audit unit, and the count says so out loud."""
    expected = 46
    return Clause(
        "data.board.population_complete",
        f"every one of the audit's {expected} units has a committed descriptor",
        len(units) == expected,
        f"{len(units)} descriptor(s) under registry.d/units against {expected} audit units"
        + ("" if len(units) == expected else f"; missing or extra: {len(units) - expected:+d}"),
        ("registry.d/units/",),
        phase="data-phase0",
        source="registry.d/units",
    )


def _clause_board_cells_reconciled(store: ev.GateStore, units: list[Unit]) -> Clause:
    """The transcribed baseline still reproduces the audit's own summary counts.

    Not a tautology: the descriptors are a HAND transcription of a 46x9 table,
    and the audit's §5 summary is an independent total published beside it. A
    transcription error shows up here as a count that does not add up, which is
    the only cheap check that exists against the one input everything else is
    built on.
    """
    totals: dict[str, int] = {}
    for unit in units:
        for state in unit.cells.values():
            totals[state] = totals.get(state, 0) + 1
    expected = {"PRESENT": 214, "ABSENT": 126, "BROKEN": 46, "UNVERIFIED": 28}
    met = totals == expected
    return Clause(
        "data.board.cells_reconciled",
        "the transcribed audit cells reproduce the audit's §5 summary counts "
        "(214 PRESENT / 126 ABSENT / 46 BROKEN / 28 UNVERIFIED over 414 cells)",
        met,
        f"transcribed {sum(totals.values())} cells as {totals}; audit §5 published {expected}"
        + ("" if met else " — the transcription and the audit disagree"),
        ("alpha-engine-config/private-docs/data_collection_audit_260914.md §5",),
        phase="data-phase0",
        source="registry.d/units",
        as_of="2026-09-14",
    )


def _clause_board_phase_trackers_declared(store: ev.GateStore, phases) -> Clause:
    """Every rung of the ladder names its OWN tracker issue.

    Plan item P-26. A rung pointed at the parent KEY issue renders a ladder that
    looks tracked and is not: closing the parent would close four phases at
    once, and no phase would have a body anyone could execute from.
    """
    undeclared = [p.id for p in phases if p.tracker_is_placeholder]
    return Clause(
        "data.board.phase_trackers_declared",
        "each data phase has its own alpha-engine-config issue as its ladder Decision ref",
        not undeclared,
        (
            f"{len(undeclared)} rung(s) still point at the parent KEY issue: {undeclared} (P-26)"
            if undeclared
            else "every rung names its own tracker"
        ),
        ("data_gate/config/phases.yaml",),
        phase="data-phase0",
        source="data_gate/config/phases.yaml",
    )


def _clause_gate_ladder_fresh(store: ev.GateStore, *, trading_day: dt.date) -> Clause:
    """The observer observed (plan §4.1, §2 row 11 page condition 3).

    GitHub Actions outcomes are invisible to the console by ruling
    (`alpha-engine-config-I6843`), so the gate's own liveness IS the ladder's
    `as_of`. Without this clause the whole board could stop being written and
    every row would keep rendering its last state, indefinitely, in green.
    """
    reading = ev.read_ladder_freshness(store, trading_day=trading_day, max_age_hours=26)
    requirement = "gates/ladder.json is no older than 26 hours"
    if reading.unmeasurable:
        return unmeasurable(
            "data.gate.ladder_fresh",
            requirement,
            reading.detail,
            reading.evidence,
            phase="data-phase0",
            source=reading.source,
        )
    return Clause(
        "data.gate.ladder_fresh",
        requirement,
        reading.met,
        reading.detail,
        reading.evidence,
        phase="data-phase0",
        source=reading.source,
        as_of=reading.as_of,
    )


# ---------------------------------------------------------------------------
# The `data-cutover-ready` sub-gate — plan §6.2 step 3, `alpha-engine-
# config-I10777`. A precondition inside phase 1, deliberately NOT tagged with
# a numbered phase: `read.evaluate` selects these four by exact phase match
# rather than by "<= ceiling", the same distinction `registry.d/phases.yaml`
# draws between a rung and a sub-gate.
# ---------------------------------------------------------------------------


def _sf_only_units(units: list[Unit]) -> list[Unit]:
    """Units whose only v1 trigger is a Step Functions pipeline this stack
    (or its tracked gap, I10753) replaces — plan §6.2 condition (b)'s "37
    SF-only units", derived from the descriptors rather than hand-listed.

    Plan §6.2 and this issue cite 37; this predicate is what the units_covered
    clause below counts and reconciles against that number, naming any
    disagreement as a finding rather than papering over it (plan §4.1
    "red by default").
    """
    out: list[Unit] = []
    for unit in units:
        trigger = unit.raw.get("trigger") or {}
        successor = str(trigger.get("successor") or "")
        if trigger.get("kind") == "step-functions" and (
            "nousergon-data-PR1701" in successor or "alpha-engine-config-I10753" in successor
        ):
            out.append(unit)
    return out


def _clause_cutover_ready_stack_check_live(store: ev.GateStore) -> Clause:
    name = "data.cutover_ready.stack_check_live"
    requirement = (
        "the nousergon-data-collection stack's live state (CFN + EventBridge Scheduler) "
        "matches this checkout, per `infrastructure/data_collection_stack.py check-live` "
        "— read from a published reading, never from GitHub Actions directly "
        "(alpha-engine-config-I6843: GitHub Actions outcomes are invisible to the console)"
    )
    reading = ev.read_stack_check_live(store)
    if reading.unmeasurable:
        return unmeasurable(
            name, requirement, reading.detail, reading.evidence, phase="data-cutover-ready", source=reading.source
        )
    return Clause(
        name,
        requirement,
        reading.met,
        reading.detail,
        reading.evidence,
        phase="data-cutover-ready",
        source=reading.source,
        as_of=reading.as_of,
    )


def _clause_cutover_ready_units_covered(store: ev.GateStore, units: list[Unit], *, trading_day: dt.date) -> Clause:
    name = "data.cutover_ready.units_covered"
    # A retired unit's survives_phase4 is satisfied by its recorded retirement
    # decision, which is exactly what the requirement accepts — it is not a
    # member to grade (alpha-engine-config-I10823).
    sf_units = [u for u in _sf_only_units(units) if not u.retired]
    members =[_clause_base(store, unit, "survives_phase4", trading_day=trading_day) for unit in sf_units]
    statuses = [clause_member_status(m) for m in members]
    met_n = statuses.count("MET")
    unmet = sorted(m.name for m, s in zip(members, statuses, strict=True) if s == "UNMET")
    unmeas = sorted(m.name for m, s in zip(members, statuses, strict=True) if s == "UNMEASURABLE")
    requirement = (
        "every SF-only unit — a step-functions trigger whose successor names PR1701 or "
        "I10753 — has a standalone workload or a recorded retirement decision, read from "
        "its own data.<unit>.survives_phase4 base clause"
    )
    detail = f"{met_n}/{len(members)} survives_phase4 MET, {len(unmet)} UNMET, {len(unmeas)} UNMEASURABLE"
    if unmet:
        detail += f"; unmet: {unmet[:12]}"
    if unmeas:
        detail += f"; unmeasurable: {unmeas[:12]}"
    if len(sf_units) != 37:
        detail += (
            f". plan §6.2 and this issue cite 37 SF-only units; the descriptor-derived "
            f"population is {len(sf_units)} — a disagreement named rather than reconciled "
            "quietly (plan §4.1 'red by default')"
        )
    evidence = tuple(m.name for m in members)
    if unmeas:
        return unmeasurable(
            name,
            requirement,
            detail,
            evidence,
            phase="data-cutover-ready",
            source="data_gate.clauses (rollup of survives_phase4)",
        )
    return Clause(
        name,
        requirement,
        not unmet,
        detail,
        evidence,
        phase="data-cutover-ready",
        source="data_gate.clauses (rollup of survives_phase4)",
    )


def _clause_cutover_ready_roles_bootstrapped(store: ev.GateStore) -> Clause:
    name = "data.cutover_ready.roles_bootstrapped"
    requirement = (
        "the standalone stack's execution role and its deploy identity exist — "
        f"{list(CUTOVER_READY_ROLES)} — read via iam:ListRolePolicies (the gate-read "
        "identity holds no iam:GetRole grant, alpha-engine-config-I10777)"
    )
    reading = ev.read_roles_bootstrapped(store, CUTOVER_READY_ROLES)
    if reading.unmeasurable:
        return unmeasurable(
            name, requirement, reading.detail, reading.evidence, phase="data-cutover-ready", source=reading.source
        )
    return Clause(
        name,
        requirement,
        reading.met,
        reading.detail,
        reading.evidence,
        phase="data-cutover-ready",
        source=reading.source,
        as_of=reading.as_of,
    )


def _clause_cutover_ready_parity(store: ev.GateStore, *, trading_day: dt.date) -> Clause:
    name = "data.cutover_ready.parity"
    requirement = (
        "pre-cutover parity is published per key at "
        "data_collection/parity/{report_trading_day}.json (plan §6.2 step 4), produced by "
        f"P-11 (alpha-engine-config-I10778), and the most recent report is within "
        f"{ev.PARITY_FRESHNESS_TRADING_DAYS} trading days of this gate's own trading day "
        "(alpha-engine-config-I10857) — a shadow run is a one-off for a completed day, "
        "never keyed to the gate's own running day"
    )
    reading = ev.read_parity(store, trading_day=trading_day)
    if reading.unmeasurable:
        return unmeasurable(
            name, requirement, reading.detail, reading.evidence, phase="data-cutover-ready", source=reading.source
        )
    return Clause(
        name,
        requirement,
        reading.met,
        reading.detail,
        reading.evidence,
        phase="data-cutover-ready",
        source=reading.source,
        as_of=reading.as_of,
    )


# Every `_clause_*` above is wrapped so a raising clause becomes ONE
# UNMEASURABLE row instead of darkening the ladder. Applied by walking this
# module's globals, so a clause added tomorrow is contained without anyone
# remembering to decorate it (`nousergon_lib.gates.contain_clause_exceptions`).
contain_clause_exceptions(globals())


def generate(store: ev.GateStore, units: list[Unit], phases, *, trading_day: dt.date) -> list[Clause]:
    """Every clause of the data board, in a stable order.

    Order is (board, base, guard, cutover-ready, objective) and within each,
    descriptor order — stable so a diff between two readings is a diff in
    STATE rather than in layout.
    """
    clauses: list[Clause] = [
        _clause_inventory_writers_declared(store, units),
        _clause_board_population_complete(store, units),
        _clause_board_cells_reconciled(store, units),
        _clause_board_phase_trackers_declared(store, phases),
        _clause_gate_ladder_fresh(store, trading_day=trading_day),
    ]
    for unit in units:
        for column in AUDIT_COLUMNS:
            clauses.append(_clause_base(store, unit, column, trading_day=trading_day))
    for unit in units:
        for guard in GUARD_CLASSES + OPTIONAL_GUARD_CLASSES:
            if guard in unit.guards:
                clauses.append(_clause_guard(store, unit, guard, trading_day=trading_day))
    clauses.append(_clause_cutover_ready_stack_check_live(store))
    clauses.append(_clause_cutover_ready_units_covered(store, units, trading_day=trading_day))
    clauses.append(_clause_cutover_ready_roles_bootstrapped(store))
    clauses.append(_clause_cutover_ready_parity(store, trading_day=trading_day))
    for family in sorted({u.freshness_family for u in units if u.freshness_family}):
        clauses.append(
            _clause_objective(
                store,
                f"data.slo.freshness.{family}",
                f"the {family} family met its declared deadline on >= 19 of the last 20 "
                "scheduled cycles, AND the payload's own as_of equalled the cycle's trading "
                "day on 20 of 20 (a fresh write of stale content fails)",
                f"metrics/slo/freshness/{family}/latest.json",
            )
        )
        clauses.append(
            _clause_objective(
                store,
                f"data.slo.completeness.{family}",
                f"every published key in the {family} family met its declared floor against "
                "its declared denominator minus DECLARED exclusions",
                f"metrics/slo/completeness/{family}/latest.json",
            )
        )
    clauses.append(
        _clause_objective(
            store,
            "data.cost.monthly",
            "AWS spend tagged component=data-collection is within the ratified monthly "
            "ceiling, read from the cost-and-usage export (never per-run Cost Explorer API "
            "calls, billed at $0.01 each)",
            "metrics/cost/monthly/latest.json",
        )
    )
    clauses.append(
        _clause_objective(
            store,
            "data.pages.monthly",
            "pages <= 2 per month outside declared vendor outages",
            "metrics/pages/monthly/latest.json",
        )
    )
    clauses.append(
        _clause_objective(
            store,
            "data.human_touch.monthly",
            "human-originated mutating CloudTrail calls on component resources = 0 per month "
            "outside the reserved list, read from the CloudTrail S3 ARCHIVE rather than "
            "lookup-events",
            "metrics/human_touch/monthly/latest.json",
        )
    )
    return clauses
