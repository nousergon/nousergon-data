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
* **completeness**, one per unit that declares a ``completeness`` block with a
  denominator: ``data.<unit>.completeness`` — the phase-1 OBSERVE-mode reading
  of the day's single `MetricRecord` (plan §2 row 2, P-13), distinct from both
  the guard-commissioning clause above and the rolling SLO below.
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
from data_gate import exit_criteria as xc
from data_gate import standalone
from data_gate.descriptors import AUDIT_COLUMNS, GUARD_CLASSES, OPTIONAL_GUARD_CLASSES, Unit
from data_gate.inventory import scan

__all__ = [
    "BOARD_CLAUSES",
    "CLAUSE_PREFIX",
    "CUTOVER_READY_CLAUSES",
    "EXIT_CRITERION_CLAUSES",
    "PHASE1_UNITS_PRODUCED_CLAUSE",
    "RetiredClause",
    "UnconnectedClause",
    "base_clause_name",
    "is_retired",
    "is_unconnected",
    "is_ungraded",
    "base_clause_names",
    "completeness_clause_name",
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
#: The phase-1 exit's own rollup clause (plan §6.2 item 7) — the production
#: evidence that `data-cutover-ready` must NOT carry, because it cannot be
#: answered until the cutover has run (`alpha-engine-config-I10989`).
PHASE1_UNITS_PRODUCED_CLAUSE = "data.phase1.units_produced"

#: The phase EXIT criteria that had no clause at all until `alpha-engine-config-
#: I10954`: the operational counters `data_gate/config/phases.yaml` names in its
#: `exit:` lists. Every one reads UNMET or UNMEASURABLE today, which is the
#: correct reading and is the proof the gate can see them: before they existed,
#: `data_gate read --gate data-phase1` would have reported MET with not one of
#: them measured.
EXIT_CRITERION_CLAUSES: tuple[str, ...] = (
    "data.phase1.consecutive_eod_cycles",
    "data.phase1.consecutive_morning_cycles",
    "data.phase1.consecutive_weekly_cycles",
    "data.phase1.v1_data_stage_quiet",
    "data.phase1.cost_baseline_measured",
    "data.phase2.eod_universe_covered",
    "data.phase2.empty_fresh_free",
    "data.phase2.vendor_divergence_emitted",
    "data.phase2.executor_collection_writes_zero",
    "data.phase3.sustained_window",
)

CUTOVER_READY_ROLES: tuple[str, ...] = (
    "nousergon-data-collection-sfn-role",
    "github-actions-data-collection-stack-deploy",
)


def base_clause_name(unit_id: str, column: str) -> str:
    return f"{CLAUSE_PREFIX}.{unit_id}.{column}"


def guard_clause_name(unit_id: str, guard: str) -> str:
    return f"{CLAUSE_PREFIX}.{unit_id}.guard.{guard}"


def completeness_clause_name(unit_id: str) -> str:
    return f"{CLAUSE_PREFIX}.{unit_id}.completeness"


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


@dataclass(frozen=True)
class UnconnectedClause(Clause):
    """A unit's ``consumers`` clause when the unit is KEPT with no surviving
    consumer by a recorded decision (`alpha-engine-config-I10873`).

    Brian, 2026-09-15: *"the console should say they exist but there are no
    consumers ... I will definitely want to know what is connected and what
    isn't."* So it is neither green nor red:

    * never MET (``met=False``) — "kept, nothing reads it" is not health;
    * never UNMET — it is a closed decision, not an open finding, so
      `read.evaluate` excludes it from every gate exactly as it excludes
      `RetiredClause`, and the phase-1 exit does not go red on it;
    * rendered on the board as ``UNCONNECTED`` (console ``DISABLED``: declared,
      with reason, owner and re-exam — `observability-policy` §8.3).

    Only the ``consumers`` column changes; every other column of the unit is
    still graded. A unit that is unconnected WITHOUT a decision stays an
    ordinary UNMET clause (plan §3 finding).
    """

    decision: str = ""


def is_unconnected(clause: Clause) -> bool:
    return isinstance(clause, UnconnectedClause)


def is_ungraded(clause: Clause) -> bool:
    """Published on the board, graded by no gate: RETIRED or UNCONNECTED."""
    return is_retired(clause) or is_unconnected(clause)


def _clause_consumers_unconnected(unit: Unit, name: str, requirement: str, phase: str) -> Clause:
    """The ``consumers`` clause of a unit with no SURVIVING consumer.

    A consumer list that names only v1 repos (retiring in v2 phase 4) is
    treated exactly like an empty one — `data_gate/config/consumer_repos.yaml`.
    """
    evidence = (unit.path.relative_to(unit.path.parents[2]).as_posix(),)
    decision = unit.consumers_decision
    if decision:
        return UnconnectedClause(
            name,
            requirement,
            False,
            f"UNCONNECTED: {unit.unit_id} is collected and graded on every other column, but no "
            f"surviving consumer reads it — {unit.connection_reason}. Graded by no gate.",
            evidence,
            phase=phase,
            source="registry.d/units (consumers_decision)",
            as_of=decision["ruled_on"],
            decision=f"{decision['decision']} by {decision['ruled_by']} {decision['ruled_on']} ({decision['ruling']})",
        )
    return Clause(
        name,
        requirement,
        False,
        f"unconnected with no recorded decision: {unit.connection_reason}. A key with no surviving "
        "consumer needs a keep (`consumers_decision`) or retire (`lifecycle: retired`) ruling "
        "(plan §3).",
        evidence,
        phase=phase,
        source="registry.d/units",
    )


#: The phase a kept-unconnected unit's `schema_contract` clause is deferred to.
#: Plan §3 ("a key with no surviving consumer after phase 4 gets **no** new
#: contract") read together with §8's per-unit rows, which route D02 and D14 —
#: both R7 keeps — to "P2 contract (no reader yet, so after keyed readers)".
_UNCONNECTED_SCHEMA_CONTRACT_PHASE = 2


def _clause_phase(unit: Unit, column: str) -> str:
    """The phase a unit's `column` clause is graded in.

    Normally the descriptor's own `clause_phase` map. The ONE case this
    function exists for is `schema_contract` on a unit that is unconnected by
    a RECORDED keep (`consumers_decision`, Brian R7 2026-09-14, restated
    2026-09-15 — `alpha-engine-config-I11186`).

    The requirement is *"publishes a versioned schema with a producer test
    that validates a real fixture, **and every consumer pins a copy**"*. On a
    unit ruled to have no consumer, the second half can never be satisfied by
    any amount of work, so the clause graded red in phase 1 for work that is
    not phase-1 work and, on eight units, not work at all until a reader
    exists. Its sibling `consumers` clause already reads the SAME absence as a
    closed decision (`UnconnectedClause`) — two clauses on one unit
    disagreeing in direction about one fact.

    Plan §3 settles which way: contracts are built *for keys with a surviving
    consumer* in phase 1, and the kept zero-consumer remainder in phase 2. So
    the clause is DEFERRED, not carved out — it is still graded, still red
    until a contract exists, and it lands in the phase the plan schedules it
    in. `max` so a descriptor that already declares a later phase keeps it:
    this only ever moves a clause later, never earlier.
    """
    declared = unit.clause_phase[column]
    if (
        column == "schema_contract"
        and unit.connection == "unconnected"
        and unit.consumers_decision
    ):
        declared = max(declared, _UNCONNECTED_SCHEMA_CONTRACT_PHASE)
    return f"data-phase{declared}"


def _clause_base(store: ev.GateStore, unit: Unit, column: str, *, trading_day: dt.date) -> Clause:
    name = base_clause_name(unit.unit_id, column)
    phase = _clause_phase(unit, column)
    cell = unit.cells[column]
    requirement = ev.BASE_REQUIREMENTS[column].format(unit=unit.unit_id, title=unit.title)
    if unit.retired:
        return _retired(unit, name, requirement, phase)
    if column == "consumers" and unit.connection == "unconnected":
        return _clause_consumers_unconnected(unit, name, requirement, phase)
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
# Completeness clause — one per unit that declares a `completeness` block
# with a denominator (`data_collection_plan_260914.md` §2 row 2, plan item
# P-13; `alpha-engine-config-I10780`). D20 (the EOD spine) is the only unit
# declaring one today.
#
# Distinct from the per-guard `data.<unit>.guard.cardinality` clause above
# (which reads a `faults/` induced-fault COMMISSIONING record, phase 2, per
# `observability-policy` §9.1) and from `data.slo.completeness.<family>`
# below (a phase-3 rolling 20-cycle SLO over a whole freshness family). This
# clause reads the single-day `MetricRecord` the guard publishes and is the
# phase-1 proof that the guard actually RAN and measured something today,
# alongside the observe-mode stack (plan §2 row 2 amendment 3).
# ---------------------------------------------------------------------------


def _clause_completeness(store: ev.GateStore, unit: Unit, *, trading_day: dt.date) -> Clause:
    name = completeness_clause_name(unit.unit_id)
    block = unit.completeness or {}
    denominator = block.get("denominator") or "?"
    floor = block.get("floor")
    requirement = (
        f"{unit.unit_id} covers >= {floor} of its declared universe ({denominator}) minus "
        "DECLARED exclusions (contracts/exclusions/unpriced_symbols.yaml), after suffix "
        "normalization (contracts/exclusions/suffix_normalization.yaml) — phase 1 OBSERVE "
        "(data_collection_plan_260914.md §2 row 2, plan item P-13)"
    )
    reading = ev.read_completeness_metric(store, unit, trading_day=trading_day)
    if reading.unmeasurable:
        return unmeasurable(
            name,
            requirement,
            reading.detail,
            reading.evidence,
            phase="data-phase1",
            source=reading.source,
        )
    return Clause(
        name,
        requirement,
        reading.met,
        reading.detail,
        reading.evidence,
        phase="data-phase1",
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


# The plan's §6.2 condition (b) figure. Kept as a named constant so the
# reconciliation below reads as arithmetic rather than a magic literal.
PLAN_SF_ONLY_UNITS = 37


def _replaced_by_standalone_stack(unit: Unit) -> bool:
    """Does this unit's v1 trigger disappear when phase 4 turns the SFs off?

    The test is the DECLARED SUCCESSOR, not the trigger's kind. It used to be
    `kind == "step-functions"` as well, and that silently dropped **D33**:
    its kind is `eventbridge-rule`, its successor is
    `ne-data-collection-daily-heal (nousergon-data-PR1701, DISABLED)`, so the
    standalone stack replaces it exactly like the others — and
    `data.cutover_ready.units_covered` would have read a clean 33/33 MET with
    D33's `survives_phase4` never graded at all. A gate that can read MET over
    an ungraded member is worse than a gate that reads UNMET, so the predicate
    keys on the successor, which is the property the requirement is about.
    Any future unit whose v1 trigger is an EventBridge rule, a scheduler entry
    or anything else the stack takes over is covered by the same rule without
    a second edit here (alpha-engine-config-I10908).
    """
    trigger = unit.raw.get("trigger") or {}
    successor = str(trigger.get("successor") or "")
    if "nousergon-data-PR1701" in successor or "alpha-engine-config-I10753" in successor:
        return True
    # ...AND the v1 trigger kind, because RECORDING A RETIREMENT REWRITES THE
    # SUCCESSOR. D09, D40 and D41 are step-functions units whose retirement is
    # recorded (D40/D41 under Brian's R7 ruling, 2026-09-14); their successor
    # no longer names the standalone stack, so a successor-only predicate drops
    # them out of the population BEFORE the retirement count runs. That made
    # `_plan_reconciliation` print "0 carry a recorded retirement" as a
    # structural constant and then report a residual disagreement against the
    # plan that does not exist (alpha-engine-config-I11014).
    #
    # Measured 2026-09-17 over all 46 descriptors: successor-token 34,
    # step-functions 36, UNION **37** — exactly plan §6.2's figure — of which 3
    # are retired, leaving 34 graded, exactly what the board reads. The plan
    # was right; the population was undercounting.
    return str(trigger.get("kind") or "") == "step-functions"


def _sf_only_units(units: list[Unit]) -> list[Unit]:
    """Units the standalone stack (or its tracked gap, I10753) replaces —
    plan §6.2 condition (b)'s "37 SF-only units", derived from the descriptors
    rather than hand-listed.

    Includes retired members; `_clause_cutover_ready_units_covered` drops them
    for grading and uses the difference to reconcile against the plan's 37.
    """
    return [unit for unit in units if _replaced_by_standalone_stack(unit)]


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


def _cutover_population(units: list[Unit]) -> tuple[list[Unit], list[str], list[Unit]]:
    """(declared, recorded retirements, units left to grade) for §6.2 item 3."""
    declared = _sf_only_units(units)
    retired_members = sorted(u.unit_id for u in declared if u.retired)
    graded = [u for u in declared if not u.retired]
    return declared, retired_members, graded


def _plan_reconciliation(declared: list[Unit], retired_members: list[str], graded: list[Unit]) -> str:
    """The plan-vs-descriptors arithmetic, printed by both rollups.

    Reconcile by arithmetic, never by asserting a disagreement away: the graded
    population is the declared population minus the units whose retirement
    decision is already recorded (I10823), and naming the retirements is what
    makes the two numbers comparable at all.
    """
    detail = (
        f". plan §6.2 cites {PLAN_SF_ONLY_UNITS} SF-only units; descriptors declare "
        f"{len(declared)}, of which {len(retired_members)} carry a recorded retirement "
        f"({retired_members or 'none'}), leaving {len(graded)} graded here"
    )
    if len(declared) == PLAN_SF_ONLY_UNITS:
        # Reconciled. Printed as the equation rather than dropped, so a reader
        # can check the arithmetic instead of trusting that it was checked.
        detail += (
            f" ({len(declared)} declared - {len(retired_members)} retired = {len(graded)} graded, "
            f"reconciled against plan §6.2)"
        )
    else:
        detail += (
            f" — and {len(declared)} != {PLAN_SF_ONLY_UNITS}, a residual disagreement "
            "between the plan and the descriptors, named rather than reconciled quietly "
            "(plan §4.1 'red by default')"
        )
    return detail


def _clause_cutover_ready_units_covered(store: ev.GateStore, units: list[Unit], *, trading_day: dt.date) -> Clause:
    """Plan §6.2 item 3, and ONLY that: a standalone workload is DECLARED for
    every SF-only unit, or its retirement is recorded.

    A static question, answered from the committed stack definition and the
    committed descriptors with no live state of any kind — `alpha-engine-
    config-I10989`. It used to roll up `survives_phase4`, which requires an
    ENABLED schedule and a manifest produced inside its execution; all four
    schedules are deliberately DISABLED until the cutover, and the cutover is
    gated on this very sub-gate, so that leg was satisfiable only by the action
    it guards. The production evidence is not lost: it is graded, unchanged, by
    `data.phase1.units_produced` at the phase-1 exit, where §6.2 item 7 puts it.

    A retired unit's requirement is satisfied by its recorded retirement
    decision, which is exactly what the requirement accepts — it is not a
    member to grade (alpha-engine-config-I10823).
    """
    name = "data.cutover_ready.units_covered"
    declared, retired_members, graded = _cutover_population(units)
    readings = {unit.unit_id: standalone.read_standalone_workload_declared(unit) for unit in graded}
    missing = sorted(uid for uid, r in readings.items() if not r.met)
    met_n = len(readings) - len(missing)
    requirement = (
        "every unit the standalone stack replaces — any trigger whose declared successor "
        "names PR1701 or I10753, whatever its kind — has a standalone workload DECLARED in "
        "the nousergon-data-collection stack's verify_units, or a recorded retirement "
        "decision. Live schedule state is not an input: this sub-gate is read BEFORE the "
        "window that enables the schedules (plan §6.2 item 3, alpha-engine-config-I10989)"
    )
    detail = f"{met_n}/{len(readings)} SF-only units have a declared standalone workload"
    if missing:
        detail += (
            f"; no verify_units entry for {missing} — "
            + "; ".join(readings[uid].detail for uid in missing[:4])
        )
    detail += _plan_reconciliation(declared, retired_members, graded)
    evidence = tuple(dict.fromkeys(e for r in readings.values() for e in r.evidence))
    return Clause(
        name,
        requirement,
        not missing,
        detail,
        evidence,
        phase="data-cutover-ready",
        source="data_gate.clauses (rollup of declared standalone workloads)",
    )


def _clause_phase1_units_produced(store: ev.GateStore, units: list[Unit], *, trading_day: dt.date) -> Clause:
    """Plan §6.2 item 7 — the PRODUCTION evidence, at the phase-1 exit.

    Exactly the reading `data.cutover_ready.units_covered` used to carry: every
    SF-only unit's `survives_phase4`, which needs an ENABLED schedule, a
    SUCCEEDED execution for the latest due fire, and an ok scheduled-trigger
    manifest inside it. It reads UNMET until the cutover has run, which is
    correct and is not circular, because nothing is gated on it: phase 1 exits
    AFTER the cutover, "read after the cutover's first 5 trading days".
    """
    name = "data.phase1.units_produced"
    declared, retired_members, graded = _cutover_population(units)
    members = [_clause_base(store, unit, "survives_phase4", trading_day=trading_day) for unit in graded]
    statuses = [clause_member_status(m) for m in members]
    met_n = statuses.count("MET")
    unmet = sorted(m.name for m, s in zip(members, statuses, strict=True) if s == "UNMET")
    unmeas = sorted(m.name for m, s in zip(members, statuses, strict=True) if s == "UNMEASURABLE")
    requirement = (
        "after the cutover, every unit the standalone stack replaces has PRODUCED under its "
        "own enabled schedule — read from its data.<unit>.survives_phase4 base clause: the "
        "schedule ENABLED in live state, a SUCCEEDED execution for the latest due fire, and "
        "an ok scheduled-trigger manifest inside it (plan §6.2 item 7)"
    )
    detail = f"{met_n}/{len(members)} survives_phase4 MET, {len(unmet)} UNMET, {len(unmeas)} UNMEASURABLE"
    if unmet:
        detail += f"; unmet: {unmet[:12]}"
    if unmeas:
        detail += f"; unmeasurable: {unmeas[:12]}"
    detail += _plan_reconciliation(declared, retired_members, graded)
    evidence = tuple(m.name for m in members)
    if unmeas:
        return unmeasurable(
            name,
            requirement,
            detail,
            evidence,
            phase="data-phase1",
            source="data_gate.clauses (rollup of survives_phase4)",
        )
    return Clause(
        name,
        requirement,
        not unmet,
        detail,
        evidence,
        phase="data-phase1",
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


# ---------------------------------------------------------------------------
# Phase EXIT criteria — the operational counters (`alpha-engine-config-I10954`).
#
# Until these existed, every `exit:` line of `data_gate/config/phases.yaml` was
# PROSE. `read.evaluate` is a pure rollup of the clause list, so once the cell
# and guard columns went green the gate would have reported MET without "5
# consecutive EOD successes", "10 consecutive trading days", "0 empty-but-fresh
# over 20 cycles" or "sustained over 20 trading days and 4 Saturdays" having
# been measured at all — a predicate keyed on the MECHANISM (the columns) rather
# than on the PROPERTY the plan's exit names (`alpha-engine-config-I10906` /
# -I10908 / -I10928).
#
# Each reads artifacts that already exist: the run manifests under
# `data_collection/runs/<unit>/<trading_day>/`, the committed stack schedules,
# the published metric documents, and the gate's own dated readings. Every one
# of them reads UNMET or UNMEASURABLE today. That is the correct reading, and it
# is the proof the gate can see them at all.
# ---------------------------------------------------------------------------


def _exit_clause(name: str, requirement: str, reading: ev.Reading, *, phase: str) -> Clause:
    """One exit-criterion clause out of its reading, MET / UNMET / UNMEASURABLE."""
    if reading.unmeasurable:
        return unmeasurable(
            name, requirement, reading.detail, reading.evidence, phase=phase, source=reading.source
        )
    return Clause(
        name,
        requirement,
        reading.met,
        reading.detail,
        reading.evidence,
        phase=phase,
        source=reading.source,
        as_of=reading.as_of,
    )


def _clause_phase1_consecutive_eod_cycles(cycles: xc.CycleSet) -> Clause:
    required = xc.PHASE1_CONSECUTIVE[xc.SCHEDULE_EOD]
    return _exit_clause(
        "data.phase1.consecutive_eod_cycles",
        (
            f"{required} CONSECUTIVE complete EOD cycles ending at the latest due fire — every "
            "unit the schedule verifies recorded an ok scheduled-trigger manifest inside the "
            "cycle (plan §6 phase-1 exit)"
        ),
        xc.read_consecutive_cycles(cycles, required=required),
        phase="data-phase1",
    )


def _clause_phase1_consecutive_morning_cycles(cycles: xc.CycleSet) -> Clause:
    required = xc.PHASE1_CONSECUTIVE[xc.SCHEDULE_MORNING]
    return _exit_clause(
        "data.phase1.consecutive_morning_cycles",
        (
            f"{required} CONSECUTIVE complete morning cycles ending at the latest due fire "
            "(plan §6 phase-1 exit)"
        ),
        xc.read_consecutive_cycles(cycles, required=required),
        phase="data-phase1",
    )


def _clause_phase1_consecutive_weekly_cycles(cycles: xc.CycleSet) -> Clause:
    required = xc.PHASE1_CONSECUTIVE[xc.SCHEDULE_WEEKLY]
    return _exit_clause(
        "data.phase1.consecutive_weekly_cycles",
        (
            f"{required} CONSECUTIVE Saturdays of the weekly schedule with complete manifests "
            "(plan §6 phase-1 exit)"
        ),
        xc.read_consecutive_cycles(cycles, required=required),
        phase="data-phase1",
    )


def _clause_board_collector_code_identity(cycles: xc.CycleSet) -> Clause:
    """Visibility only (`alpha-engine-config-I10931`) — never gates anything."""
    reading = xc.read_code_identity_delta(cycles)
    return _exit_clause(
        "data.board.collector_code_identity",
        (
            "the weekly schedule's code identity (code_sha), compared between the two most "
            "recent populated cycles — reported, never blocking; a change is not a defect"
        ),
        reading,
        phase="data-phase0",
    )


def _clause_phase1_v1_data_stage_quiet(store: ev.GateStore) -> Clause:
    return _exit_clause(
        "data.phase1.v1_data_stage_quiet",
        (
            "v1 Step Functions data-stage executions since the cutover = 0 — the collector is "
            "not running twice (plan §6 phase-1 exit)"
        ),
        xc.read_v1_data_stage_quiet(store),
        phase="data-phase1",
    )


def _clause_phase1_cost_baseline_measured(store: ev.GateStore) -> Clause:
    return _exit_clause(
        "data.phase1.cost_baseline_measured",
        (
            f"a {xc.PHASE1_COST_BASELINE_WEEKS}-week tagged cost baseline has been MEASURED "
            "(plan §6 phase-1 exit) — a different question from data.cost.monthly, which grades "
            "the same document against the ratified ceiling at phase 3"
        ),
        xc.read_cost_baseline_measured(store, weeks=xc.PHASE1_COST_BASELINE_WEEKS),
        phase="data-phase1",
    )


def _clause_phase2_eod_universe_covered(store: ev.GateStore, *, trading_day: dt.date) -> Clause:
    return _exit_clause(
        "data.phase2.eod_universe_covered",
        (
            f"the EOD spine priced the declared universe minus DECLARED exclusions on "
            f"{xc.PHASE2_EOD_COVERAGE_DAYS} CONSECUTIVE trading days (plan §6 phase-2 exit); a "
            "day with no completeness MetricRecord breaks the streak, because a missing "
            "measurement is not a passing one"
        ),
        xc.read_eod_universe_covered(
            store, trading_day=trading_day, days=xc.PHASE2_EOD_COVERAGE_DAYS
        ),
        phase="data-phase2",
    )


def _clause_phase2_empty_fresh_free(cycles: xc.CycleSet) -> Clause:
    return _exit_clause(
        "data.phase2.empty_fresh_free",
        (
            f"zero empty-but-fresh writes over {xc.PHASE2_EMPTY_FRESH_CYCLES} EOD cycles, "
            "counted from the guards' own `empty_fresh` verdict and never from rows_out == 0 "
            "(plan §2 objective 6, phase-2 exit)"
        ),
        xc.read_empty_fresh_free(cycles, required_cycles=xc.PHASE2_EMPTY_FRESH_CYCLES),
        phase="data-phase2",
    )


def _clause_phase2_vendor_divergence_emitted(cycle_sets: list[xc.CycleSet]) -> Clause:
    return _exit_clause(
        "data.phase2.vendor_divergence_emitted",
        (
            f"a vendor cross-check verdict was EMITTED on {xc.PHASE2_VENDOR_CYCLES} of "
            f"{xc.PHASE2_VENDOR_CYCLES} cycles, within bound or with each breach named (plan §6 "
            "phase-2 exit). A silent cycle is the failure: no verdict is indistinguishable from "
            "agreement while being a total absence of measurement"
        ),
        xc.read_vendor_divergence_emitted(cycle_sets, required_cycles=xc.PHASE2_VENDOR_CYCLES),
        phase="data-phase2",
    )


def _clause_phase2_executor_collection_writes_zero(store: ev.GateStore) -> Clause:
    return _exit_clause(
        "data.phase2.executor_collection_writes_zero",
        (
            "the executor profile shows no collection writes over 7 days (plan §6 phase-2 exit) "
            "— the producer/consumer separation the collector split exists to establish"
        ),
        xc.read_executor_collection_writes_zero(store),
        phase="data-phase2",
    )


def _clause_phase3_sustained_window(
    store: ev.GateStore, weekly: xc.CycleSet, *, trading_day: dt.date
) -> Clause:
    name = "data.phase3.sustained_window"
    return _exit_clause(
        name,
        (
            f"every clause MET with 0 UNMEASURABLE and 0 UNREPORTED, SUSTAINED over "
            f"{xc.PHASE3_TRADING_DAYS} consecutive trading days and {xc.PHASE3_SATURDAYS} "
            "Saturdays (plan §6 phase-3 exit), read from the gate's own dated readings — which "
            "are never overwritten, and are therefore the only record a sustain claim can "
            "honestly be built on. This clause is excluded from the readings it grades, so the "
            "window is not its own precondition"
        ),
        xc.read_sustained_window(
            store,
            gate="data-phase3",
            self_clause=name,
            trading_day=trading_day,
            trading_days=xc.PHASE3_TRADING_DAYS,
            weekly=weekly,
            saturdays=xc.PHASE3_SATURDAYS,
        ),
        phase="data-phase3",
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
    clauses.append(_clause_phase1_units_produced(store, units, trading_day=trading_day))
    # `data_collection/metrics/eod_completeness/{trading_day}.json` is a single
    # non-unit-scoped key (`validators/expectations.py::publish_completeness_metric`;
    # plan §2 row 2, P-13) — it names the EOD spine specifically, not a generic
    # per-unit path. Many units declare a `completeness` block (most `status:
    # proposed`, no reader behind them yet), so this clause is generated for the
    # ONE unit the key actually describes rather than looped over every
    # descriptor that happens to carry the field. Generalizing past D20 needs a
    # per-unit metric key first — declared here, not inferred from the
    # descriptor, so a second unit never silently collides on this key.
    for unit in units:
        if unit.unit_id == "D20" and (unit.completeness or {}).get("denominator"):
            clauses.append(_clause_completeness(store, unit, trading_day=trading_day))
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
    # The phase EXIT criteria (`alpha-engine-config-I10954`). The cycle sets are
    # collected ONCE per schedule and shared by every clause that counts over
    # them: four clauses re-deriving the same twenty-cycle window would list the
    # same manifest prefixes four times for the same answer.
    cycle_sets = {
        schedule: xc.collect_cycles(
            store,
            units,
            schedule_name=schedule,
            count=count,
            trading_day=trading_day,
        )
        for schedule, count in xc.CYCLE_COUNTS.items()
    }
    eod = cycle_sets[xc.SCHEDULE_EOD]
    morning = cycle_sets[xc.SCHEDULE_MORNING]
    weekly = cycle_sets[xc.SCHEDULE_WEEKLY]
    clauses.append(_clause_phase1_consecutive_eod_cycles(eod))
    clauses.append(_clause_phase1_consecutive_morning_cycles(morning))
    clauses.append(_clause_phase1_consecutive_weekly_cycles(weekly))
    clauses.append(_clause_board_collector_code_identity(weekly))
    clauses.append(_clause_phase1_v1_data_stage_quiet(store))
    clauses.append(_clause_phase1_cost_baseline_measured(store))
    clauses.append(_clause_phase2_eod_universe_covered(store, trading_day=trading_day))
    clauses.append(_clause_phase2_empty_fresh_free(eod))
    clauses.append(_clause_phase2_vendor_divergence_emitted([eod, morning, weekly]))
    clauses.append(_clause_phase2_executor_collection_writes_zero(store))
    clauses.append(_clause_phase3_sustained_window(store, weekly, trading_day=trading_day))
    return clauses
