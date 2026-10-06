"""The `detector` base-column reader, one fixture per detector family.

`alpha-engine-config-I10795` (A3, part 1). Every family gets the same three
cases against its REAL descriptor: no receipt is UNMET, a receipt proving
induce -> fire on the right subject -> clear is MET, and a receipt naming some
other subject is never MET. The withholding tests below the families take each
of §9.1's three facts away in turn and assert the clause cannot read MET.
"""

from __future__ import annotations

import copy
import dataclasses
import json

import pytest

from data_gate import clauses as clause_module
from data_gate import detector_readers
from data_gate.descriptors import load_units
from data_gate.read import load_phases
from tests.data_gate_support import TRADING_DAY, DeniedStore, EmptyStore


@pytest.fixture(scope="module")
def units():
    return {u.unit_id: u for u in load_units()}


def _receipt(unit_id: str, kind: str, subject: str, overrides: dict | None = None) -> dict:
    document = {
        "schema": detector_readers.RECEIPT_SCHEMA,
        "unit_id": unit_id,
        "detector": {"kind": kind},
        "outcome": "induced",
        "induced": {"at": "2026-10-07T14:00:00Z", "condition": "withheld the artifact for one cycle"},
        "fired": {"at": "2026-10-07T14:20:00Z", "subject": subject, "evidence": "alert ref"},
        "cleared": {"at": "2026-10-07T15:05:00Z", "evidence": "stand-down ref"},
    }
    document.update(overrides or {})
    return document


def _store(unit_id: str, kind: str, document: dict | bytes) -> EmptyStore:
    payload = document if isinstance(document, bytes) else json.dumps(document).encode()
    return EmptyStore({f"commissioning/{unit_id}/{kind}/latest.json": payload})


def _with_detectors(unit, detectors):
    raw = copy.deepcopy(unit.raw)
    raw["detectors"] = detectors
    return dataclasses.replace(unit, raw=raw)


# ---------------------------------------------------------------------------
# One case per family, on the unit's committed descriptor.
#
# (unit, kind, the subject the descriptor makes checkable)
# ---------------------------------------------------------------------------

FAMILIES = [
    pytest.param("D03", "freshness-monitor", "price_cache_freshness_sentinel", id="freshness-monitor"),
    # `(indirect, depends_on)` annotation stripped to the row id.
    pytest.param("D18", "freshness-monitor", "feature_store_freshness_sentinel", id="freshness-monitor-sentinel"),
    pytest.param("D17", "sf-execution", "ne-preopen-trading-pipeline", id="sf-execution"),
    pytest.param("D19", "eventbridge-scheduler", "eod-snapshot-existence-check", id="eventbridge-scheduler"),
]


@pytest.mark.parametrize("unit_id, kind, subject", FAMILIES)
def test_family_without_a_receipt_is_unmet_and_names_the_key(units, unit_id, kind, subject):
    reading = detector_readers.read_detector(EmptyStore(), units[unit_id])
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert f"commissioning/{unit_id}/{kind}/latest.json" in reading.evidence
    assert "no commissioning receipt" in reading.detail


@pytest.mark.parametrize("unit_id, kind, subject", FAMILIES)
def test_family_with_a_proving_receipt_is_met(units, unit_id, kind, subject):
    store = _store(unit_id, kind, _receipt(unit_id, kind, subject))
    reading = detector_readers.read_detector(store, units[unit_id])
    assert reading.met and not reading.unmeasurable, reading.detail
    assert "commissioned" in reading.detail
    assert reading.as_of == "2026-10-07T15:05:00Z"


@pytest.mark.parametrize("unit_id, kind, subject", FAMILIES)
def test_family_receipt_on_another_units_subject_is_never_met(units, unit_id, kind, subject):
    store = _store(unit_id, kind, _receipt(unit_id, kind, "some_other_units_row"))
    reading = detector_readers.read_detector(store, units[unit_id])
    assert not reading.met and not reading.unmeasurable
    assert "not D" in reading.detail and "some_other_units_row" in reading.detail


#: Families whose `via` is prose: the subject must be DECLARED on the entry.
DECLARED_FAMILIES = [
    pytest.param("D33", "cloudwatch-alarm", id="cloudwatch-alarm"),
    pytest.param("D36", "box-timer-health", id="box-timer-health"),
    pytest.param("D37", "code-refusal", id="code-refusal"),
    pytest.param("D47", "crucible-gate", id="crucible-gate"),
]


@pytest.mark.parametrize("unit_id, kind", DECLARED_FAMILIES)
def test_declared_family_without_a_receipt_is_unmet(units, unit_id, kind):
    reading = detector_readers.read_detector(EmptyStore(), units[unit_id])
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert f"commissioning/{unit_id}/{kind}/latest.json" in reading.evidence


@pytest.mark.parametrize("unit_id, kind", DECLARED_FAMILIES)
def test_declared_family_receipt_without_a_declared_subject_is_unmeasurable(units, unit_id, kind):
    """The reader will not guess which alarm "no-heartbeat" means."""
    unit = units[unit_id]
    only = _with_detectors(unit, [d for d in unit.raw["detectors"] if d["kind"] == kind])
    store = _store(unit_id, kind, _receipt(unit_id, kind, "anything"))
    reading = detector_readers.read_detector(store, only)
    assert reading.unmeasurable and not reading.met
    assert "declare `subject:`" in reading.detail


@pytest.mark.parametrize("unit_id, kind", DECLARED_FAMILIES)
def test_declared_family_with_a_declared_subject_grades_the_receipt(units, unit_id, kind):
    unit = units[unit_id]
    entry = dict(next(d for d in unit.raw["detectors"] if d["kind"] == kind), subject="the-declared-subject")
    declared = _with_detectors(unit, [entry])
    right = detector_readers.read_detector(
        _store(unit_id, kind, _receipt(unit_id, kind, "the-declared-subject")), declared
    )
    assert right.met, right.detail
    wrong = detector_readers.read_detector(_store(unit_id, kind, _receipt(unit_id, kind, "elsewhere")), declared)
    assert not wrong.met and not wrong.unmeasurable


def test_one_commissioned_detector_is_enough_and_the_other_is_still_named(units):
    """D17 declares two detectors; the requirement is "has A detector"."""
    store = _store("D17", "sf-execution", _receipt("D17", "sf-execution", "ne-preopen-trading-pipeline"))
    reading = detector_readers.read_detector(store, units["D17"])
    assert reading.met
    assert "commissioning/D17/freshness-monitor/latest.json" in reading.detail


def test_a_unit_declaring_no_detector_is_unmet(units):
    reading = detector_readers.read_detector(EmptyStore(), units["D02"])
    assert not reading.met and not reading.unmeasurable
    assert "declares no detector" in reading.detail


@pytest.mark.parametrize("unit_id", ["D35", "D43"])
def test_a_declared_partial_exclusion_is_honoured(units, unit_id):
    reading = detector_readers.read_detector(DeniedStore(), units[unit_id])
    assert reading.met and not reading.unmeasurable
    assert reading.detail.startswith("not applicable: N/A-NOT-RUN")


def test_an_unknown_family_is_unmeasurable_not_guessed(units):
    unit = _with_detectors(units["D03"], [{"kind": "tea-leaves", "via": "x", "state": "PRESENT"}])
    reading = detector_readers.read_detector(EmptyStore(), unit)
    assert reading.unmeasurable and not reading.met
    assert "tea-leaves" in reading.detail


# ---------------------------------------------------------------------------
# Withholding: take one of §9.1's facts away and the clause is never MET.
# ---------------------------------------------------------------------------

D03_SUBJECT = "price_cache_freshness_sentinel"


def _withheld(units, overrides):
    document = _receipt("D03", "freshness-monitor", D03_SUBJECT, overrides)
    return detector_readers.read_detector(_store("D03", "freshness-monitor", document), units["D03"])


@pytest.mark.parametrize(
    "overrides, finding",
    [
        ({"outcome": "absorbed"}, "only an `induced` record"),
        ({"schema": "something.v0"}, "schema"),
        ({"unit_id": "D04"}, "unit_id"),
        ({"detector": {"kind": "sf-execution"}}, "detector.kind"),
        ({"induced": None}, "no `induced` record"),
        ({"fired": None}, "no `fired` record"),
        ({"cleared": None}, "no `cleared` record"),
        ({"induced": {"at": "2026-10-07T14:00:00Z", "condition": ""}}, "induced.condition"),
        ({"fired": {"at": "2026-10-07T14:20:00Z", "subject": D03_SUBJECT, "evidence": ""}}, "fired.evidence"),
        ({"fired": {"at": "2026-10-07T14:20:00Z", "subject": "", "evidence": "x"}}, "fired.subject"),
        ({"cleared": {"at": "2026-10-07T15:05:00Z", "evidence": ""}}, "cleared.evidence"),
        ({"cleared": {"at": "not-a-time", "evidence": "x"}}, "not an ISO-8601"),
        (
            {"fired": {"at": "2026-10-07T13:00:00Z", "subject": D03_SUBJECT, "evidence": "x"}},
            "fired before the fault was induced",
        ),
        ({"cleared": {"at": "2026-10-07T14:20:00Z", "evidence": "x"}}, "no stand-down was observed"),
    ],
)
def test_a_receipt_missing_any_fact_is_unmet(units, overrides, finding):
    reading = _withheld(units, overrides)
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert finding in reading.detail


def test_a_denied_read_is_unmeasurable_never_absent(units):
    reading = detector_readers.read_detector(DeniedStore(), units["D03"])
    assert reading.unmeasurable and not reading.met
    assert "could not read" in reading.detail


def test_a_malformed_receipt_is_unmeasurable(units):
    reading = detector_readers.read_detector(_store("D03", "freshness-monitor", b"{not json"), units["D03"])
    assert reading.unmeasurable and not reading.met


# ---------------------------------------------------------------------------
# The board: no detector clause is a stub, and none is MET on an empty store.
# ---------------------------------------------------------------------------


def test_no_detector_clause_renders_the_phase0_stub_or_reads_met_without_a_receipt():
    board = clause_module.generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)
    detector = [c for c in board if c.name.endswith(".detector") and not clause_module.is_retired(c)]
    assert detector, "no detector clauses were generated"
    for clause in detector:
        assert "no reader is built" not in clause.detail, clause.name
        assert not getattr(clause, "unmeasurable", False), clause.name
        if clause.met:
            assert clause.detail.startswith("not applicable:"), clause.name
