"""A kept-unconnected unit's `schema_contract` clause renders UNCONNECTED.

`alpha-engine-config-I10933`. Brian's R7 keep (2026-09-14, restated
2026-09-15 in `alpha-engine-config-I10873`) left eight units with
`consumers: []` by recorded decision. Their `consumers` clause already reads
that as a closed decision (`UnconnectedClause`, graded by no gate). Their
`schema_contract` clause demanded *"every consumer pins a copy"* from the same
absence, so it was UNMET forever: measured 2026-10-04 on a
`data_gate read --gate data-phase2 --dry-run`, `schema_contract` read 9 UNMET,
8 of them kept-unconnected units with no consumer declared anywhere.

The carve-out reads the SAME predicate and the SAME declaration the
`consumers` clause reads. Graded here as properties, with a non-vacuity guard,
plus the two edges that must stay graded: no recorded decision, and a contract
that names a consumer despite the decision.
"""

from __future__ import annotations

import yaml

from data_gate import clauses as clause_module
from data_gate import descriptors
from data_gate.clauses import base_clause_name, generate, is_unconnected
from data_gate.descriptors import load_units
from data_gate.read import GATES, _board_document, evaluate, load_phases

from tests.data_gate_support import TRADING_DAY, EmptyStore

_DECISION = {
    "decision": "kept-unconnected",
    "ruled_by": "Brian",
    "ruled_on": "2026-09-15",
    "ruling": "Brian 2026-09-15 (alpha-engine-config-I10873)",
    "reason": "kept, reincorporation possible",
    "reexam": "a v2 component proposes reading this key",
}
_NO_CONTRACT = {"schema": None, "producer_test": None, "consumer_pins": []}


def _d01(tmp_path, **overrides):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base.update(overrides)
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    [unit] = descriptors.load_units(tmp_path)
    return unit


def _schema_clause(unit):
    return clause_module._clause_base(EmptyStore(), unit, "schema_contract", trading_day=TRADING_DAY)


def _graded_by_any_gate(clause) -> list[str]:
    return [
        gate
        for gate in GATES
        if clause.name
        in {c.name for c in evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=[clause]).clauses}
    ]


def _carved_out_units():
    return [u for u in load_units() if clause_module._schema_contract_is_unconnected(u)]


def test_there_is_at_least_one_carved_out_unit():
    """Non-vacuity: the properties below must not pass over an empty set."""
    assert _carved_out_units(), "no unit qualifies for the schema_contract carve-out; the tests below are vacuous"


def test_on_the_real_board_consumers_and_schema_contract_agree_for_every_carved_out_unit():
    """The property: one recorded decision, one reading, on both columns."""
    by_name = {c.name: c for c in generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)}
    for unit in _carved_out_units():
        consumers = by_name[base_clause_name(unit.unit_id, "consumers")]
        contract = by_name[base_clause_name(unit.unit_id, "schema_contract")]
        assert is_unconnected(consumers), unit.unit_id
        assert is_unconnected(contract), (
            f"{unit.unit_id}: consumers reads the recorded keep as UNCONNECTED but schema_contract "
            "still demands a consumer pin from the same absence"
        )
        assert contract.met is False and contract.unmeasurable is False, unit.unit_id
        assert contract.detail.startswith("UNCONNECTED:"), unit.unit_id
        assert not _graded_by_any_gate(contract), unit.unit_id


def test_a_kept_unit_with_no_consumer_anywhere_is_unconnected_on_both_columns(tmp_path):
    unit = _d01(
        tmp_path, consumers=[], consumers_reason="no reader", consumers_decision=dict(_DECISION), contract=_NO_CONTRACT
    )
    clause = _schema_clause(unit)
    assert is_unconnected(clause)
    assert clause.met is False and clause.unmeasurable is False
    assert "no schema or producer test is declared" in clause.detail
    assert clause.decision == "kept-unconnected by Brian 2026-09-15 (Brian 2026-09-15 (alpha-engine-config-I10873))"
    assert not _graded_by_any_gate(clause)

    document = _board_document([clause], trading_day=TRADING_DAY, generated_utc="t", store_uri=None, units=[unit])
    [row] = document["rows"]
    assert row["state"] == "UNCONNECTED" and row["console_state"] == "DISABLED"
    assert document["clauses_total"] == 0 and document["clauses_unconnected"] == 1


def test_the_detail_still_says_what_the_producer_side_holds():
    """D03 has a producer schema and test on main: the carve-out must not hide it."""
    [d03] = [u for u in load_units() if u.unit_id == "D03"]
    assert clause_module._schema_contract_is_unconnected(d03)
    clause = _schema_clause(d03)
    assert is_unconnected(clause)
    assert "producer side present" in clause.detail


def test_an_unconnected_unit_without_a_decision_stays_an_ordinary_finding(tmp_path):
    unit = _d01(tmp_path, consumers=["crucible-research:agents/macro_agent.py"], contract=_NO_CONTRACT)
    assert unit.connection == "unconnected" and not unit.consumers_decision
    clause = _schema_clause(unit)
    assert not is_unconnected(clause)
    assert clause.met is False and not clause.unmeasurable
    assert "declares no schema and no producer test" in clause.detail
    assert _graded_by_any_gate(clause)


def test_a_contract_naming_a_consumer_keeps_the_clause_graded_despite_the_decision(tmp_path):
    """The narrowing: a descriptor that records "no consumer" and names one in its
    own contract contradicts itself, and the graded reading is what says so."""
    unit = _d01(
        tmp_path,
        consumers=[],
        consumers_reason="no reader",
        consumers_decision=dict(_DECISION),
        contract={**_NO_CONTRACT, "unpinned_consumers": ["crucible:crucible/data/universe.py"]},
    )
    assert not clause_module._schema_contract_is_unconnected(unit)
    clause = _schema_clause(unit)
    assert not is_unconnected(clause)
    assert _graded_by_any_gate(clause)


def test_a_connected_unit_is_unaffected():
    connected = [u for u in load_units() if u.connection == "connected"]
    assert connected
    for unit in connected:
        assert not clause_module._schema_contract_is_unconnected(unit), unit.unit_id
