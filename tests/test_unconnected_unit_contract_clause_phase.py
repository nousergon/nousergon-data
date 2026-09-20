"""A kept-unconnected unit's `schema_contract` clause is not graded by phase 1.

`alpha-engine-config-I11186`. Measured 2026-09-20 on
`data_collection/gates/board/latest.json`: eight units read
`consumers = UNCONNECTED` under a recorded keep AND `schema_contract = UNMET`
for *"NO consumer pin is declared"* — two clauses on one unit grading the same
absence in opposite directions, one as a closed ruling and one as an open
failure. The `schema_contract` half could not be satisfied by any amount of
work, because Brian ruled (R7, 2026-09-14, restated 2026-09-15) that these
units are KEPT with no consumer.

The plan settles which way it resolves, and it is not "carve the clause out":

* §3: *"a key with no surviving consumer after phase 4 gets **no** new
  contract. It gets a retirement decision (R7), or a declared `consumers: []`
  with a reason, which renders as a finding."*
* §8, D02 (an R7 keep): *"R7 keep -> P1 row + registry (P-08, P-09), **P2
  contract** (no reader yet, so after keyed readers)."* D14 carries the same
  routing.

So the contract is still owed and still graded — in **phase 2**, after the
keyed readers. Deferred, not forgiven.

What this test grades is the PROPERTY, not the eight instances: no unit may
have its `consumers` clause read as a recorded keep while a clause the SAME
gate grades demands a consumer pin from it. An instance list would pass the
day a ninth unit is ruled a keep and say nothing.
"""

from __future__ import annotations

from data_gate.clauses import base_clause_name, generate, is_unconnected
from data_gate.descriptors import load_units
from data_gate.read import evaluate, load_phases

from tests.data_gate_support import EmptyStore, TRADING_DAY


def _kept_unconnected_units():
    return [u for u in load_units() if u.connection == "unconnected" and u.consumers_decision]


def test_there_is_at_least_one_kept_unconnected_unit():
    """Guard against the whole suite passing vacuously.

    If R7's keeps were ever all retired or reconnected, every assertion below
    would iterate an empty list and report green over nothing — the
    unsatisfiable-predicate shape this issue exists to end.
    """
    assert _kept_unconnected_units(), (
        "no unit reads unconnected-with-a-recorded-keep; the tests below would pass vacuously"
    )


def test_a_kept_unconnected_units_schema_contract_is_deferred_past_phase_one():
    clauses = {c.name: c for c in generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)}
    for unit in _kept_unconnected_units():
        clause = clauses[base_clause_name(unit.unit_id, "schema_contract")]
        assert clause.phase != "data-phase1", (
            f"{unit.unit_id}: schema_contract is graded in phase 1, but the unit is a recorded "
            f"keep with no consumer ({unit.connection_reason}). The clause requires 'every "
            "consumer pins a copy', which no work can satisfy here. Plan §3/§8 route the "
            "contract for a kept zero-consumer unit to phase 2."
        )


def test_the_phase_one_gate_never_demands_a_consumer_pin_from_a_recorded_keep():
    """The property, stated against the gate rather than against the clause.

    `evaluate` selects on `phase <= ceiling`, so a clause tagged phase 2 is
    absent from the phase-1 gate. This asserts the consequence directly: the
    gate that grades a unit's `consumers` clause as a closed decision does not,
    in the same reading, grade another of that unit's clauses red for the
    absence that decision records.
    """
    all_clauses = generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)
    by_name = {c.name: c for c in all_clauses}
    result = evaluate(EmptyStore(), gate="data-phase1", trading_day=TRADING_DAY, all_clauses=all_clauses)
    graded = {c.name for c in result.clauses}

    for unit in _kept_unconnected_units():
        consumers = by_name[base_clause_name(unit.unit_id, "consumers")]
        assert is_unconnected(consumers), (
            f"{unit.unit_id}: expected its consumers clause to be an UnconnectedClause"
        )
        contract = base_clause_name(unit.unit_id, "schema_contract")
        assert contract not in graded, (
            f"{unit.unit_id}: the data-phase1 gate grades {contract}, which demands a consumer "
            "pin, while the same unit's consumers clause records a keep with no consumer. One "
            "gate cannot read one fact two ways."
        )


def test_the_deferral_only_ever_moves_a_clause_later():
    """`max`, not assignment — a descriptor declaring phase 3 keeps phase 3."""
    clauses = {c.name: c for c in generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)}
    for unit in _kept_unconnected_units():
        clause = clauses[base_clause_name(unit.unit_id, "schema_contract")]
        declared = unit.clause_phase["schema_contract"]
        assert int(clause.phase.removeprefix("data-phase")) >= declared, (
            f"{unit.unit_id}: schema_contract was moved EARLIER than its descriptor declares "
            f"({clause.phase} < data-phase{declared})"
        )
