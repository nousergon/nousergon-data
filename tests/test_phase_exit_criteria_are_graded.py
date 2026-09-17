"""Every phase EXIT criterion is GRADED by a clause the phase's gate reads.

`alpha-engine-config-I10954`. `data_gate/read.py::evaluate` is a pure rollup of
the clause list filtered by a phase ceiling, so a phase's exit is exactly what
its clauses measure and nothing else. Until this test existed, every operational
exit criterion in `data_gate/config/phases.yaml` — "5 consecutive EOD successes",
"10 consecutive trading days", "0 empty-but-fresh over 20 cycles", "sustained
over 20 trading days and 4 Saturdays" — was PROSE beside the clause list. Once
the cell and guard columns went green, `data_gate read --gate data-phase1` would
have reported MET with not one of those counters read.

That is the same class as `alpha-engine-config-I10906` / `-I10908` / `-I10928`: a
predicate keyed on the MECHANISM (the clause columns) rather than on the PROPERTY
(the plan's exit). The fix has to be a test rather than a convention, because the
failure mode is silent by construction — a criterion nobody wired up produces no
signal at all, and the gate renders greener for it.

Shape borrowed from `tests/test_every_audit_cell_is_a_clause.py`, which grades
the same kind of bijection over the audit cells.
"""

from __future__ import annotations

import datetime as dt
import fnmatch

import pytest

from data_gate.clauses import EXIT_CRITERION_CLAUSES, generate
from data_gate.descriptors import load_units
from data_gate.read import GATES, _phase_number, evaluate, load_phases

from tests.data_gate_support import EmptyStore, TRADING_DAY


@pytest.fixture(scope="module")
def board():
    """One evaluation, shared: the clause list every assertion here resolves against."""
    return generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)


@pytest.fixture(scope="module")
def phases():
    return load_phases()


def _gate_clause_names(clauses, gate: str) -> set[str]:
    """The clause names `evaluate` actually grades for ``gate`` — not a re-derivation.

    Asking `evaluate` rather than re-implementing the ceiling filter is the whole
    point: a criterion is satisfied only if the clause lands in the list the GATE
    rolls up, and a second copy of that filter here could agree with the plan
    while disagreeing with the gate.
    """
    result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=clauses)
    return {c.name for c in result.clauses}


def test_every_phase_declares_at_least_one_exit_criterion(phases):
    for phase in phases:
        assert phase.exit_criteria, (
            f"{phase.id} declares no exit criteria. A rung whose exit is empty exits on the "
            "clauses that happen to be tagged with its number, which is the accident this "
            "test exists to forbid."
        )


def test_every_exit_criterion_names_a_generated_clause(board, phases):
    """(a) a graded clause, or (b) an `exit_measured_by` pointing at one."""
    generated = {c.name for c in board}
    unmatched: list[str] = []
    for phase in phases:
        for criterion in phase.exit_criteria:
            for pattern in criterion.patterns:
                if not fnmatch.filter(sorted(generated), pattern):
                    unmatched.append(f"{phase.id}: {criterion.text!r} -> {pattern!r}")
    assert not unmatched, (
        "exit criteria naming a clause the board does not generate — a prose exit matched by "
        f"neither (a) nor (b): {unmatched}"
    )


def test_every_exit_criterion_is_graded_by_its_own_phases_gate(board, phases):
    """A clause outside the rung's gate does not hold that rung's exit.

    A criterion of phase 1 measured by a clause tagged phase 3 is not measured by
    the phase-1 gate at all: `evaluate` selects by ceiling, so the phase would
    exit without it and the criterion would be enforced a whole phase late.
    """
    ungraded: list[str] = []
    for phase in phases:
        assert phase.gate in GATES, f"{phase.id} names gate {phase.gate!r}, which is unregistered"
        graded = _gate_clause_names(board, phase.gate)
        for criterion in phase.exit_criteria:
            for pattern in criterion.patterns:
                matched = fnmatch.filter(sorted(graded), pattern)
                if not matched:
                    ungraded.append(
                        f"{phase.id} ({phase.gate}): {criterion.text!r} -> {pattern!r} matches no "
                        "clause this gate rolls up"
                    )
    assert not ungraded, ungraded


def test_the_operational_counters_exist_and_are_tagged_with_their_phase(board):
    """The clauses I10954 added, by name, each on the phase whose exit names it.

    Named explicitly rather than derived: these are exactly the counters that did
    not exist, and a test that regenerated the list from the clause module would
    pass again the moment one of them was deleted.
    """
    by_name = {c.name: c for c in board}
    missing = sorted(set(EXIT_CRITERION_CLAUSES) - set(by_name))
    assert not missing, f"exit-criterion clause(s) gone from the board: {missing}"
    for name in EXIT_CRITERION_CLAUSES:
        expected = f"data-phase{name.split('.')[1][len('phase'):]}"
        assert by_name[name].phase == expected, (
            f"{name} is tagged {by_name[name].phase!r}, not {expected!r} — a counter tagged with "
            "another phase is rolled up by the wrong gate"
        )
        assert _phase_number(by_name[name].phase) is not None


def test_no_counter_reads_met_against_an_empty_store(board):
    """Red by default, and loudly.

    Against a store with nothing in it every counter must read UNMET or
    UNMEASURABLE. A counter that reads MET over an empty store is measuring
    something other than what it claims — which is precisely how the gate came to
    be able to report MET with nothing measured.
    """
    by_name = {c.name: c for c in board}
    green = sorted(name for name in EXIT_CRITERION_CLAUSES if by_name[name].met)
    assert not green, f"exit counter(s) MET over an EMPTY store: {green}"


def test_every_counter_states_what_it_read(board):
    """`console-policy` §5.1's four fields, on a family with no unit row to inherit from."""
    by_name = {c.name: c for c in board}
    for name in EXIT_CRITERION_CLAUSES:
        clause = by_name[name]
        assert clause.requirement.strip(), f"{name} states no requirement"
        assert clause.detail.strip(), f"{name} renders no detail"
        assert clause.evidence, f"{name} cites no evidence"
        assert clause.source, f"{name} names no source"


def test_the_sustain_window_excludes_itself(board):
    """`gate-taxonomy-policy`: a predicate satisfiable only by its own satisfaction.

    Phase 3's window grades past readings of the phase-3 gate, which CONTAIN this
    clause. Counting itself would make the window clean only on days it was
    already MET, and it can never have been — the phase could then never exit
    however the system behaved.
    """
    clause = next(c for c in board if c.name == "data.phase3.sustained_window")
    assert "excluded" in clause.requirement or "excluded" in clause.detail


def test_a_prose_exit_is_refused_by_the_loader(tmp_path):
    """The rule with teeth: a rung whose `exit:` is a paragraph will not load.

    The defect this whole issue is about was a `exit: >-` block of prose. If the
    loader accepted one, every assertion above would pass over a phases file that
    had quietly gone back to it.
    """
    import yaml

    from data_gate.read import PHASES_PATH, load_phases as load

    document = yaml.safe_load(PHASES_PATH.read_text(encoding="utf-8"))
    document["phases"][0]["exit"] = "five consecutive EOD successes and a cost baseline"
    path = tmp_path / "phases.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="not a non-empty list"):
        load(path)


def test_a_criterion_naming_no_clause_is_refused_by_the_loader(tmp_path):
    import yaml

    from data_gate.read import PHASES_PATH, load_phases as load

    document = yaml.safe_load(PHASES_PATH.read_text(encoding="utf-8"))
    document["phases"][1]["exit"] = [{"criterion": "the collector is fine"}]
    path = tmp_path / "phases.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="exactly one"):
        load(path)


def test_the_bijection_check_bites(board, phases):
    """The guard is not vacuous: an invented criterion must fail it.

    Without this, `test_every_exit_criterion_names_a_generated_clause` would keep
    passing if `exit_criteria` ever came back empty.
    """
    generated = {c.name for c in board}
    assert not fnmatch.filter(sorted(generated), "data.phase9.*")
    assert sum(len(p.exit_criteria) for p in phases) >= 20, (
        "the phases file declares fewer criteria than the plan's §6 exits list; a criterion "
        "dropped from the file is a criterion nothing grades"
    )


def test_the_trading_day_fixture_is_a_date():
    assert isinstance(TRADING_DAY, dt.date)
