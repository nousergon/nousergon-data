"""`data-cutover-ready` is a PRE-WINDOW readiness gate: no clause of it may
depend on live schedule state.

`alpha-engine-config-I10989`. The sub-gate is read BEFORE the v2 phase-4
maintenance window, and the cutover PR is allowed only once it is MET. All four
`nousergon-data-collection` schedules are deliberately DISABLED until that
cutover (`infrastructure/automation_pause.json`), so a clause needing an ENABLED
schedule is satisfiable only by the action it guards — the circular predicate
`gate-taxonomy-policy` names. `data.cutover_ready.units_covered` was exactly
that, and read a permanent 0/34.

The test is DIFFERENTIAL, not a hardcoded list of today's four clauses: the
whole board is evaluated against stores that differ ONLY in live schedule
state, and every clause tagged `data-cutover-ready` must read identically
across all of them. A clause added tomorrow is held to the same rule without
anyone remembering to add it here.

No AWS: every store is in-memory.
"""

from __future__ import annotations

import pytest

from data_gate import clauses as clause_module
from data_gate.descriptors import load_units
from data_gate.read import load_phases

from tests.data_gate_support import TRADING_DAY, EmptyStore

_MACHINE = "arn:aws:states:us-east-1:000000000000:stateMachine:fake-standalone"


class _FakeScheduler:
    def __init__(self, state: str) -> None:
        self.state = state

    def get_schedule(self, *, GroupName: str, Name: str) -> dict:  # boto3 kwarg spelling
        return {"State": self.state, "Target": {"Arn": _MACHINE}}


class _FakeSfn:
    def list_executions(self, **_kwargs) -> dict:
        return {"executions": []}


class _SchedulerStore(EmptyStore):
    """An EmptyStore that also answers Scheduler/Step Functions reads."""

    def __init__(self, state: str) -> None:
        super().__init__()
        self.uri = f"memory://scheduler-{state.lower()}"
        self.scheduler_client = _FakeScheduler(state)
        self.sfn_client = _FakeSfn()


def _fingerprint(clause) -> tuple:
    return (clause.name, clause.met, clause.unmeasurable, clause.detail, tuple(clause.evidence), clause.source)


def _board(store):
    return clause_module.generate(store, load_units(), load_phases(), trading_day=TRADING_DAY)


@pytest.fixture(scope="module")
def boards() -> dict[str, list]:
    return {
        "disabled": _board(_SchedulerStore("DISABLED")),
        "enabled": _board(_SchedulerStore("ENABLED")),
        "no-clients": _board(EmptyStore()),
    }


def _select(board, phase: str) -> dict[str, tuple]:
    return {c.name: _fingerprint(c) for c in board if c.phase == phase}


def test_no_cutover_ready_clause_changes_with_live_schedule_state(boards):
    """The class-level guard. Every `data-cutover-ready` clause must read the
    same whether the schedules are DISABLED, ENABLED, or unreadable — because
    the gate is read while they are off, by design."""
    baseline = _select(boards["disabled"], "data-cutover-ready")
    assert baseline, "the sub-gate must have clauses, or this test proves nothing"
    for label in ("enabled", "no-clients"):
        other = _select(boards[label], "data-cutover-ready")
        assert set(other) == set(baseline), f"clause population differs under {label}"
        for name, fingerprint in baseline.items():
            assert other[name] == fingerprint, (
                f"{name} reads differently when the schedules are {label}: it depends on live "
                "schedule state, so it cannot be answered before the window it guards. Move the "
                "live reading to a phase-1-exit clause (alpha-engine-config-I10989)."
            )


def test_the_differential_bites_somewhere(boards):
    """This test exists so the one above cannot pass vacuously. If the fake
    clients were not wired, NOTHING on the board would move between DISABLED
    and ENABLED and the guard would be meaningless. The live reading now lives
    in phase 1 — `survives_phase4` and the rollup over it — so phase 1 must
    move where `data-cutover-ready` does not."""
    disabled = _select(boards["disabled"], "data-phase1")
    enabled = _select(boards["enabled"], "data-phase1")
    moved = {n for n in disabled if disabled[n] != enabled.get(n)}
    assert {n for n in moved if n.endswith(".survives_phase4")}, (
        "no survives_phase4 clause moved between DISABLED and ENABLED: the fake Scheduler "
        "client is not reaching the reading, so the guard above proves nothing"
    )


def test_units_covered_is_met_or_names_the_units_with_the_schedules_off(boards):
    """The gate's Closes-when: with every schedule DISABLED, `units_covered` is
    MET or names the specific units genuinely missing a workload/retirement —
    never blocked on the schedules themselves."""
    clause = next(
        c for c in boards["disabled"] if c.name == "data.cutover_ready.units_covered"
    )
    assert clause.unmeasurable is False
    # Not "the string DISABLED is absent" — a descriptor's successor field
    # legitimately names the disabled workload. The rule is that no LIVE
    # schedule reading appears in the verdict.
    assert "in live state" not in clause.detail
    assert "until the cutover enables" not in clause.detail
    if not clause.met:
        assert "verify_units" in clause.detail


def test_phase1_units_produced_is_tagged_phase1_not_cutover_ready(boards):
    clause = next(
        c for c in boards["disabled"] if c.name == clause_module.PHASE1_UNITS_PRODUCED_CLAUSE
    )
    assert clause.phase == "data-phase1"
    assert clause_module.PHASE1_UNITS_PRODUCED_CLAUSE not in clause_module.CUTOVER_READY_CLAUSES


# ── the population must survive a retirement (alpha-engine-config-I11014) ────


class TestARetirementDoesNotLeaveThePopulation:
    """Recording a retirement REWRITES `trigger.successor`.

    The population predicate used to key on that field alone, so a retired unit
    fell out of the set before the retirement count ran. Two numbers were
    artifacts of the same exclusion: the clause printed "0 carry a recorded
    retirement" as a structural constant, and then reported a residual
    disagreement against plan §6.2's 37 that did not exist.

    Measured 2026-09-17 over all 46 descriptors: successor-token 34,
    step-functions 36, union 37 — the plan's figure — of which 3 are retired,
    leaving the 34 the board grades. The plan was right the whole time.
    """

    def test_a_retired_step_functions_unit_is_still_declared(self):
        from data_gate import clauses
        from data_gate.descriptors import load_units

        declared, retired, graded = clauses._cutover_population(load_units())
        declared_ids = {u.unit_id for u in declared}

        # The three whose successor a retirement rewrote. Named rather than
        # counted: a count would pass again if a different unit went missing.
        for unit_id in ("D09", "D40", "D41"):
            assert unit_id in declared_ids, (
                f"{unit_id} is a step-functions unit with a recorded retirement and it "
                "dropped out of the declared population — the predicate is keying on a "
                "field the retirement rewrote (alpha-engine-config-I11014)"
            )
            assert unit_id in retired
            assert unit_id not in {u.unit_id for u in graded}

    def test_d33_is_still_declared_though_its_kind_is_not_step_functions(self):
        """The successor leg must keep carrying D33 (alpha-engine-config-I10908).

        Its kind is `eventbridge-rule`; only its successor names the standalone
        stack. Adding the kind leg must not become a replacement for the
        successor leg.
        """
        from data_gate import clauses
        from data_gate.descriptors import load_units

        declared, _, graded = clauses._cutover_population(load_units())
        assert "D33" in {u.unit_id for u in declared}
        assert "D33" in {u.unit_id for u in graded}

    def test_the_reconciliation_resolves_and_says_so(self):
        from data_gate import clauses
        from data_gate.descriptors import load_units

        declared, retired, graded = clauses._cutover_population(load_units())
        detail = clauses._plan_reconciliation(declared, retired, graded)

        assert len(declared) == clauses.PLAN_SF_ONLY_UNITS
        assert len(declared) - len(retired) == len(graded)
        assert "residual disagreement" not in detail
        assert "reconciled against plan" in detail
        # The retirement count must be able to be non-zero, which is the whole
        # defect: it was structurally 0 before.
        assert len(retired) > 0
