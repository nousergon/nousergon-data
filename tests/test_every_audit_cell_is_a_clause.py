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

**THE AUDIT IS A FROZEN BASELINE, NOT A LIVING REGISTER** (Brian ruling
2026-09-21, `alpha-engine-config-I11319`). The audit is dated in its own
filename and its whole value here is that it was taken independently at a point
in time. Units added after it are registered, generate their nine clauses and
count in the board's denominator — but they are NOT audit cells, because no
audit scored them, and amending a dated document to include units that did not
exist when it was written would make it a record of nothing.

So this file grades two populations, deliberately:

* `BASELINE_UNIT_IDS` — the 46 the audit scored. `AUDIT_SUMMARY` and the totals
  are reconciled over exactly these, and the drift check is unchanged: a
  baseline unit that disappears from `registry.d/units/` still fails here,
  which is the whole reason the constants are hard-coded.
* `POST_BASELINE_UNIT_IDS` — units registered since. Pinned, so adding one is
  still a deliberate act that cannot slip in silently; their clauses are
  required to exist, and their self-declared `audit.cells` are NOT folded into
  the audit's published tallies.

The alternative considered and rejected was editing `AUDIT_SUMMARY` to absorb
each new unit. That would have satisfied the test while destroying it: a
constant maintained to match the descriptors it checks checks nothing, and every
future unit would re-open the same question.
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

#: The unit ids the 2026-09-14 audit scored. Frozen with `AUDIT_SUMMARY` and
#: part of the same independent reading: the summary's counts are only
#: interpretable against a known population, so pinning the totals without
#: pinning WHICH units they cover would let a swap (one unit out, one in) keep
#: every count intact.
BASELINE_UNIT_IDS: frozenset[str] = frozenset({
    "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09", "D10",
    "D11", "D12", "D13", "D14", "D15", "D15L", "D16", "D17", "D18", "D19",
    "D20", "D21", "D22", "D23", "D24", "D25", "D26", "D27", "D28", "D29",
    "D30", "D31", "D32", "D33", "D34", "D35", "D36", "D37", "D38", "D39",
    "D40", "D41", "D42", "D43", "D46", "D47",
})

#: Units registered AFTER the audit baseline (`alpha-engine-config-I11319`).
#: Pinned rather than inferred as "whatever is not in the baseline": an
#: unpinned remainder would let a new unit appear with no acknowledgement
#: anywhere, which is the drift this file exists to make impossible — just in
#: the opposite direction from a unit going missing.
POST_BASELINE_UNIT_IDS: frozenset[str] = frozenset()

AUDIT_UNITS = 46
AUDIT_CELLS = AUDIT_UNITS * len(AUDIT_COLUMNS)


def test_the_audit_has_forty_six_units():
    """The baseline population, unchanged. A baseline unit that loses its
    descriptor still fails here — that is the drift check, and it is not
    weakened by letting the register grow past the audit."""
    assert len(BASELINE_UNIT_IDS) == AUDIT_UNITS
    declared = {unit.unit_id for unit in load_units()}
    missing = BASELINE_UNIT_IDS - declared
    assert not missing, (
        f"baseline unit(s) with no descriptor: {sorted(missing)}. A unit without a descriptor "
        "is a unit the board renders complete without."
    )


def test_every_descriptor_is_either_baseline_or_pinned_as_post_baseline():
    """Adding a unit stays a deliberate act. Post-baseline units are legal
    (`alpha-engine-config-I11319`) but must be named in
    `POST_BASELINE_UNIT_IDS`, or a new unit could appear with no
    acknowledgement anywhere — the same invisible drift as one going missing,
    from the other direction."""
    declared = {unit.unit_id for unit in load_units()}
    unaccounted = declared - BASELINE_UNIT_IDS - POST_BASELINE_UNIT_IDS
    assert not unaccounted, (
        f"descriptor(s) in neither population pin: {sorted(unaccounted)}. A unit added after the "
        "2026-09-14 audit baseline is registered by adding its id to POST_BASELINE_UNIT_IDS in "
        "this file — NOT by editing AUDIT_SUMMARY, which is the audit's independent reading and "
        "checks nothing once it is maintained to match the descriptors."
    )
    retired = POST_BASELINE_UNIT_IDS - declared
    assert not retired, (
        f"POST_BASELINE_UNIT_IDS names unit(s) with no descriptor: {sorted(retired)} — remove the "
        "pin when the unit goes, so the pin cannot outlive what it registers."
    )


def test_every_cell_has_exactly_one_base_clause():
    """Baseline cells are exactly 414 and each maps to one clause; EVERY
    descriptor, baseline or not, contributes its nine and no duplicates."""
    units = load_units()
    names = base_clause_names(units)
    assert len(set(names)) == len(names), "duplicate base clause name"

    baseline = [u for u in units if u.unit_id in BASELINE_UNIT_IDS]
    baseline_names = base_clause_names(baseline)
    assert len(baseline_names) == AUDIT_CELLS == 414

    # The register's own population, which grows: nine clauses per descriptor.
    assert len(names) == len(units) * len(AUDIT_COLUMNS)

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
                # The phase-1 exit's production rollup (alpha-engine-config-
                # I10989) — a rollup over `survives_phase4`, not a per-unit
                # audit cell of its own — and the phase EXIT criterion counters
                # (alpha-engine-config-I10954), which grade a phase's own exit
                # rather than any one unit's cell.
                "data.phase1.",
                "data.phase2.",
                "data.phase3.",
                # `alpha-engine-config-I10793`/`-I10788`, Brian's 2026-09-21
                # ruling: the three reliability-streak STANDING clauses,
                # published under `data-collection-reliability` for Crucible
                # v2 phase 4's irreversible v1-pipeline deletion — a
                # data-phase exit precondition, not a per-unit audit cell.
                "data.standing.",
            )
        )
        and not name.endswith(".completeness")
    } - cells
    assert not orphan_clauses, f"base-shaped clauses with no audit cell: {sorted(orphan_clauses)}"
    assert cells <= generated, f"audit cells with no clause: {sorted(cells - generated)}"


def test_the_transcription_reconciles_with_the_audits_own_summary():
    """Per column, then in total. A transcription slip moves a count."""
    # Baseline units ONLY. A post-baseline unit's cells are its own
    # self-assessment, not something the audit published a count for, and
    # folding them in would move a tally the audit is the sole authority on.
    units = [u for u in load_units() if u.unit_id in BASELINE_UNIT_IDS]
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
