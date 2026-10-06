"""The `console_entity` base-column reader, one fixture per unit and per property.

`alpha-engine-config-I10795` (A3, part 2). Every unit the reader grades gets the
same three cases against its REAL descriptor: no receipt is UNMET, a receipt
proving all three reachability paths and an honest state is MET, and another
component's receipt is never MET. The withholding tests below take each
property away in turn — identity, currency, each of the six doctor links, and
each honesty rule — and assert the clause cannot read MET.
"""

from __future__ import annotations

import copy
import dataclasses
import datetime as dt
import json

import pytest

from data_gate import clauses as clause_module
from data_gate import console_entity_readers as cer
from data_gate.descriptors import load_units
from data_gate.read import load_phases
from tests.data_gate_support import TRADING_DAY, DeniedStore, EmptyStore

NOW = dt.datetime(2026, 10, 7, 15, 0, tzinfo=dt.timezone.utc)
CHECKED = "2026-10-07T14:00:00Z"

ALL_UNITS = load_units()
#: The units this reader grades: not retired at the clause layer, no declared
#: `partial_exclusion` of this column. Their count is the stub count it replaced.
GRADED = [
    u
    for u in ALL_UNITS
    if u.lifecycle != "retired"
    and "console_entity" not in ((u.raw.get("partial_exclusion") or {}).get("columns") or [])
]
EXCLUDED = ["D35", "D43"]


@pytest.fixture(scope="module")
def units():
    return {u.unit_id: u for u in ALL_UNITS}


def _receipt(unit, overrides: dict | None = None) -> dict:
    document = {
        "schema": cer.RECEIPT_SCHEMA,
        "unit_id": unit.unit_id,
        "component_id": unit.component_id,
        "checked_at": CHECKED,
        "doctor": {
            "identifier": unit.component_id,
            "ok": True,
            "steps": [{"name": name, "ok": True, "detail": "ok"} for name in cer.REQUIRED_LINKS],
        },
        "entity": {"kind": "component", "state": "HEALTHY", "reporting_claims": 2},
    }
    document.update(overrides or {})
    return document


def _store(unit_id: str, document: dict | bytes) -> EmptyStore:
    payload = document if isinstance(document, bytes) else json.dumps(document).encode()
    return EmptyStore({f"console_entity/{unit_id}/latest.json": payload})


def _read(unit, document) -> cer.Reading:
    return cer.read_console_entity(_store(unit.unit_id, document), unit, now=NOW)


# ---------------------------------------------------------------------------
# One fixture per graded unit, on its committed descriptor.
# ---------------------------------------------------------------------------


def test_the_reader_grades_every_unit_the_stub_covered():
    """40 graded + the 2 declared exclusions = the 42 phase-0 stubs it replaced."""
    assert len(GRADED) + len(EXCLUDED) == 42, [u.unit_id for u in GRADED]


@pytest.mark.parametrize("unit", GRADED, ids=lambda u: u.unit_id)
def test_unit_without_a_receipt_is_unmet_and_names_the_key(unit):
    reading = cer.read_console_entity(EmptyStore(), unit, now=NOW)
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert f"console_entity/{unit.unit_id}/latest.json" in reading.evidence
    assert "no console reachability receipt" in reading.detail


@pytest.mark.parametrize("unit", GRADED, ids=lambda u: u.unit_id)
def test_unit_with_a_proving_receipt_is_met(unit):
    state = cer._DECLARED_LIFECYCLE_STATES.get(unit.lifecycle, "HEALTHY")
    reading = _read(unit, _receipt(unit, {"entity": {"kind": "component", "state": state, "reporting_claims": 1}}))
    assert reading.met and not reading.unmeasurable, reading.detail
    assert "by name, structure and relation" in reading.detail
    assert reading.as_of == CHECKED


@pytest.mark.parametrize("unit", GRADED, ids=lambda u: u.unit_id)
def test_unit_receipt_for_another_component_is_never_met(unit):
    other = "data-collector-someone-else"
    document = _receipt(unit, {"component_id": other})
    document["doctor"]["identifier"] = other
    reading = _read(unit, document)
    assert not reading.met and not reading.unmeasurable
    assert other in reading.detail


@pytest.mark.parametrize("unit_id", EXCLUDED)
def test_a_declared_partial_exclusion_is_honoured(units, unit_id):
    reading = cer.read_console_entity(DeniedStore(), units[unit_id], now=NOW)
    assert reading.met and not reading.unmeasurable
    assert reading.detail.startswith("not applicable: N/A-NOT-RUN")


# ---------------------------------------------------------------------------
# Withholding, batched by property: take one away and the clause is never MET.
# ---------------------------------------------------------------------------


@pytest.fixture()
def d03(units):
    return units["D03"]


@pytest.mark.parametrize(
    "overrides, finding",
    [
        ({"schema": "something.v0"}, "schema"),
        ({"unit_id": "D04"}, "unit_id"),
        ({"component_id": "data-collector-d04-fred-macro-history"}, "component_id"),
    ],
)
def test_identity_a_receipt_for_someone_else_is_unmet(d03, overrides, finding):
    reading = _read(d03, _receipt(d03, overrides))
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert finding in reading.detail


@pytest.mark.parametrize(
    "checked_at, finding",
    [
        (None, "not an ISO-8601"),
        ("yesterday", "not an ISO-8601"),
        ("2026-10-06T12:59:00Z", "older than 26 hours"),
    ],
)
def test_currency_a_missing_or_stale_check_is_unmet(d03, checked_at, finding):
    reading = _read(d03, _receipt(d03, {"checked_at": checked_at}))
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert finding in reading.detail


def test_currency_a_check_inside_the_window_is_met(d03):
    reading = _read(d03, _receipt(d03, {"checked_at": "2026-10-06T13:01:00Z"}))
    assert reading.met, reading.detail


@pytest.mark.parametrize("link", cer.REQUIRED_LINKS)
def test_reachability_each_failed_link_is_unmet_and_named(d03, link):
    document = _receipt(d03)
    for step in document["doctor"]["steps"]:
        if step["name"] == link:
            step.update(ok=False, detail="nothing links to it")
    document["doctor"]["ok"] = False
    reading = _read(d03, document)
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert f"`{link}` failed: nothing links to it" in reading.detail


@pytest.mark.parametrize("link", cer.REQUIRED_LINKS)
def test_reachability_each_unwalked_link_is_unmet(d03, link):
    document = _receipt(d03)
    document["doctor"]["steps"] = [s for s in document["doctor"]["steps"] if s["name"] != link]
    reading = _read(d03, document)
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert "did not report" in reading.detail and link in reading.detail


def test_reachability_a_failed_descriptor_binding_is_unmet(d03):
    document = _receipt(d03)
    document["doctor"]["steps"].append({"name": "binding log-source", "ok": False, "detail": "no such log group"})
    document["doctor"]["ok"] = False
    reading = _read(d03, document)
    assert not reading.met
    assert "`binding log-source` failed" in reading.detail


@pytest.mark.parametrize(
    "doctor, finding",
    [
        (None, "no `doctor` record"),
        ({"identifier": "data-collector-d03-prices", "ok": None, "steps": []}, "did not report"),
    ],
)
def test_reachability_no_diagnosis_is_unmet(d03, doctor, finding):
    reading = _read(d03, _receipt(d03, {"doctor": doctor}))
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert finding in reading.detail


def test_reachability_doctor_ok_false_with_every_link_ok_contradicts_itself(d03):
    document = _receipt(d03)
    document["doctor"]["ok"] = False
    reading = _read(d03, document)
    assert not reading.met
    assert "contradicts itself" in reading.detail


@pytest.mark.parametrize(
    "entity, finding",
    [
        (None, "no `entity` record"),
        ({"kind": "artifact", "state": "HEALTHY", "reporting_claims": 1}, "must be a Component"),
        ({"kind": "component", "state": "UNKNOWN", "reporting_claims": 1}, "closed vocabulary"),
        ({"kind": "component", "state": "HEALTHY", "reporting_claims": "2"}, "is not a count"),
        ({"kind": "component", "state": "HEALTHY", "reporting_claims": True}, "is not a count"),
        ({"kind": "component", "state": "HEALTHY", "reporting_claims": 0}, "green with nothing to say"),
        ({"kind": "component", "state": "RETIRED", "reporting_claims": 1}, "renders RETIRED for a unit declared"),
        ({"kind": "component", "state": "UNREGISTERED", "reporting_claims": 1}, "renders UNREGISTERED"),
    ],
)
def test_honesty_a_dishonest_render_is_unmet(d03, entity, finding):
    reading = _read(d03, _receipt(d03, {"entity": entity}))
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert finding in reading.detail


@pytest.mark.parametrize("state", ["UNREPORTED", "FAILED", "DEGRADED", "ARMED"])
def test_honesty_a_non_green_state_is_reachable_and_honest(d03, state):
    """The column grades reachability and honesty, not health: a red, honest render is MET."""
    reading = _read(d03, _receipt(d03, {"entity": {"kind": "component", "state": state, "reporting_claims": 0}}))
    assert reading.met, reading.detail


def test_honesty_a_declared_lifecycle_must_render_as_declared(d03):
    deprecated = dataclasses.replace(d03, raw={**copy.deepcopy(d03.raw), "lifecycle": "deprecated"})
    wrong = _read(deprecated, _receipt(deprecated))
    assert not wrong.met and "must render DEPRECATED" in wrong.detail
    right = _read(
        deprecated,
        _receipt(deprecated, {"entity": {"kind": "component", "state": "DEPRECATED", "reporting_claims": 1}}),
    )
    assert right.met, right.detail


def test_a_denied_read_is_unmeasurable_never_absent(d03):
    reading = cer.read_console_entity(DeniedStore(), d03, now=NOW)
    assert reading.unmeasurable and not reading.met
    assert "could not read" in reading.detail


def test_a_malformed_receipt_is_unmeasurable(d03):
    reading = cer.read_console_entity(_store("D03", b"{not json"), d03, now=NOW)
    assert reading.unmeasurable and not reading.met


# ---------------------------------------------------------------------------
# The board: no console_entity clause is a stub, and none is MET on an empty store.
# ---------------------------------------------------------------------------


def test_no_console_entity_clause_renders_the_phase0_stub_or_reads_met_without_a_receipt():
    board = clause_module.generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)
    rows = [c for c in board if c.name.endswith(".console_entity") and not clause_module.is_retired(c)]
    assert len(rows) == 42, len(rows)
    for clause in rows:
        assert "no reader is built" not in clause.detail, clause.name
        assert not getattr(clause, "unmeasurable", False), clause.name
        if clause.met:
            assert clause.detail.startswith("not applicable:"), clause.name
