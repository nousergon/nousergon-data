"""Every audit cell is a clause, and every base clause is an audit cell.

`data_collection_plan_260914.md` §4.1: *"Base clauses, one per audit cell:
`data.<unit>.<column>` for the nine scored columns. That is 46 x 9 = 414,
matching the audit exactly, and a contract test fails if an audit cell has no
clause or a clause has no cell."*

This is that test. It is the one check that keeps the board's population claim
honest: without it, a unit can be dropped from `registry.d/units/` and the board
will render complete over 45 units and 405 clauses, with nothing anywhere saying
the denominator moved. `observability-policy` §2.2's whole point is that a
hand-maintained monitored-things list drifts *invisibly*, because the missing
rows produce no signal.

The audit itself lives in the PRIVATE `alpha-engine-config` repository, so this
public test cannot read the source table. What it grades instead is stronger
than a re-parse of the same document would be: the descriptors carry the
transcribed `audit.cells` block, and the per-column and total state counts are
reconciled against the audit's §5 summary table — an INDEPENDENT tally the audit
published beside the rows. A transcription error changes a count, and a count
that does not add up fails here. Re-reading the same 46 rows to compare them
with themselves would not have caught that.
"""

from __future__ import annotations

import collections

from data_gate.clauses import base_clause_name, base_clause_names, generate
from data_gate.descriptors import AUDIT_COLUMNS, load_units
from data_gate.read import load_phases

from tests.data_gate_support import EmptyStore, TRADING_DAY

#: The audit's own §5 summary table, by column. Hard-coded on purpose: it is the
#: independent reading this test reconciles the transcription against, and a
#: constant that could be regenerated from the thing it checks would check
#: nothing.
AUDIT_SUMMARY: dict[str, dict[str, int]] = {
    "identity": {"PRESENT": 43, "ABSENT": 0, "BROKEN": 1, "UNVERIFIED": 2},
    "consumers": {"PRESENT": 37, "ABSENT": 6, "BROKEN": 0, "UNVERIFIED": 3},
    "schema_contract": {"PRESENT": 10, "ABSENT": 35, "BROKEN": 0, "UNVERIFIED": 1},
    "artifact_registry": {"PRESENT": 27, "ABSENT": 14, "BROKEN": 2, "UNVERIFIED": 3},
    "observability_row": {"PRESENT": 3, "ABSENT": 40, "BROKEN": 3, "UNVERIFIED": 0},
    "run_record": {"PRESENT": 37, "ABSENT": 5, "BROKEN": 0, "UNVERIFIED": 4},
    "detector": {"PRESENT": 23, "ABSENT": 12, "BROKEN": 3, "UNVERIFIED": 8},
    "console_entity": {"PRESENT": 27, "ABSENT": 14, "BROKEN": 0, "UNVERIFIED": 5},
    "survives_phase4": {"PRESENT": 7, "ABSENT": 0, "BROKEN": 37, "UNVERIFIED": 2},
}

AUDIT_UNITS = 46
AUDIT_CELLS = AUDIT_UNITS * len(AUDIT_COLUMNS)


def test_the_audit_has_forty_six_units():
    units = load_units()
    assert len(units) == AUDIT_UNITS, (
        f"{len(units)} descriptors against the audit's {AUDIT_UNITS} units. A unit without a "
        "descriptor is a unit the board renders complete without."
    )


def test_every_cell_has_exactly_one_base_clause():
    units = load_units()
    names = base_clause_names(units)
    assert len(names) == AUDIT_CELLS == 414
    assert len(set(names)) == len(names), "duplicate base clause name"

    expected = {
        base_clause_name(unit.unit_id, column) for unit in units for column in unit.cells
    }
    assert set(names) == expected


def test_every_generated_base_clause_maps_back_to_a_cell():
    """The reverse direction: no clause that grades a question nobody scored."""
    units = load_units()
    clauses = generate(EmptyStore(), units, load_phases(), trading_day=TRADING_DAY)
    generated = {c.name for c in clauses}
    cells = set(base_clause_names(units))

    orphan_clauses = {
        name
        for name in generated
        if name.count(".") == 2
        and not name.startswith(
            (
                "data.board.",
                "data.inventory.",
                "data.gate.",
                "data.slo.",
                "data.cost.",
                "data.pages.",
                "data.human_touch.",
                # `data-cutover-ready` sub-gate clauses (alpha-engine-config-
                # I10777) — a precondition of phase 1, not a per-unit audit
                # cell, so they never map to `base_clause_names`.
                "data.cutover_ready.",
            )
        )
    } - cells
    assert not orphan_clauses, f"base-shaped clauses with no audit cell: {sorted(orphan_clauses)}"
    assert cells <= generated, f"audit cells with no clause: {sorted(cells - generated)}"


def test_the_transcription_reconciles_with_the_audits_own_summary():
    """Per column, then in total. A transcription slip moves a count."""
    units = load_units()
    measured: dict[str, collections.Counter] = {
        column: collections.Counter() for column in AUDIT_COLUMNS
    }
    for unit in units:
        for column, state in unit.cells.items():
            measured[column][state] += 1

    for column, expected in AUDIT_SUMMARY.items():
        got = {state: measured[column][state] for state in expected}
        assert got == expected, (
            f"column {column!r}: descriptors transcribe {got}, the audit's §5 summary "
            f"published {expected}"
        )

    totals = collections.Counter()
    for counter in measured.values():
        totals.update(counter)
    assert dict(totals) == {"PRESENT": 214, "ABSENT": 126, "BROKEN": 46, "UNVERIFIED": 28}
    assert sum(totals.values()) == AUDIT_CELLS


def test_every_clause_carries_the_console_four_fields():
    """`console-policy` §5.1: state, source, as-of, evidence — on every row."""
    units = load_units()
    clauses = generate(EmptyStore(), units, load_phases(), trading_day=TRADING_DAY)
    for clause in clauses:
        assert clause.phase, f"{clause.name} declares no phase"
        assert clause.evidence, f"{clause.name} cites no evidence"
        assert clause.requirement.strip(), f"{clause.name} states no requirement"
        assert clause.detail.strip(), f"{clause.name} renders no detail"
        assert clause.source, f"{clause.name} names no source"
