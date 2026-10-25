"""Decision-time and vintage contracts: the ``x-pit`` declaration, replay, and the gate.

`alpha-engine-config-I12567` (audit DATA-01), first slice. Three surfaces:

* every stamped contract either carries a SOUND ``x-pit`` declaration or is
  named, with its reason, in the shrink-only ``PENDING_DECLARATIONS`` register;
* :func:`contracts.vintage.admit` replays a decision at T: late releases,
  same-day filings and unknown availability are excluded and counted, and a
  correction published after T never changes what T admits;
* the gate's ``read_pit`` fails on an UNSOUND declaration and reports an
  absent one in observe mode without changing the verdict.

No AWS: stores are local directories.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib

import pytest

from contracts import pit, vintage
from data_gate import descriptors, evidence
from data_gate.consumer_pins import shape_hash
from data_gate.store import LocalStore
from tests.test_pit_contract import KEY, TUESDAY, WEDNESDAY_NOON, _stamped, _write_manifest

UTC = dt.timezone.utc
NY = vintage.MARKET_TZ
REPO = pathlib.Path(__file__).resolve().parents[1]
CONTRACTS = REPO / "contracts"


def _schemas() -> dict[str, dict]:
    return {p.name: json.loads(p.read_text()) for p in sorted(CONTRACTS.glob("*.schema.json"))}


def _stamped_contract(schema: dict) -> bool:
    """A contract whose records carry the `contracts/pit.py` ``as_of`` stamp."""
    return pit.AS_OF in vintage._row_properties(schema)


# ── the declarations on main ───────────────────────────────────────────────


def test_every_declaration_in_contracts_is_sound():
    unsound = {name: vintage.declaration_errors(s) for name, s in _schemas().items()}
    assert not {k: v for k, v in unsound.items() if v}


def test_every_stamped_contract_declares_or_is_registered_pending():
    missing = [
        name
        for name, schema in _schemas().items()
        if _stamped_contract(schema)
        and vintage.DECLARATION_KEY not in schema
        and name not in vintage.PENDING_DECLARATIONS
    ]
    assert not missing, (
        f"{missing} carry `as_of` but no `{vintage.DECLARATION_KEY}`: declare one (contracts/vintage.py) "
        "or register the contract in PENDING_DECLARATIONS with the reason"
    )


def test_the_pending_register_only_shrinks():
    schemas = _schemas()
    stale = [n for n in vintage.PENDING_DECLARATIONS if n in schemas and vintage.DECLARATION_KEY in schemas[n]]
    absent = [n for n in vintage.PENDING_DECLARATIONS if n not in schemas]
    assert not stale, f"{stale} now declare {vintage.DECLARATION_KEY}: remove them from PENDING_DECLARATIONS"
    assert not absent, f"{absent} name no contract in contracts/"


def test_the_edgar_session_contract_declares_its_filing_date_rule():
    decl = vintage.load_declaration(_schemas()["edgar_pit_fundamentals_session.schema.json"])
    assert decl is not None
    assert (decl.available_at_basis, decl.admissible_when) == ("source_date_rule", "available_before_session")
    assert (decl.vintage_field, decl.revisions) == ("latest_accession", "superseding")
    assert vintage.replay_supported(decl)


def test_the_fred_macro_contract_declares_its_revision_gap_explicitly():
    """DATA-01's own evidence: a date-bounded FRED fetch is not vintage-bounded."""
    decl = vintage.load_declaration(_schemas()["metron_macro.schema.json"])
    assert decl is not None
    assert (decl.available_at_basis, decl.revisions) == ("collection_time", "overwritten")
    assert vintage.replay_supported(decl) is False


def test_the_annotation_does_not_move_a_consumer_shape_pin():
    schema = _schemas()["technicals.schema.json"]
    bare = {k: v for k, v in schema.items() if k != vintage.DECLARATION_KEY}
    assert shape_hash(schema) == shape_hash(bare)


def test_every_uncontracted_pit_dataset_names_a_real_collector_and_a_known_basis():
    for path, facts in vintage.UNCONTRACTED_PIT_DATASETS.items():
        assert (REPO / path).is_file(), path
        assert facts["basis"] in vintage.AVAILABILITY_BASES, path
        assert facts["revisions"] in vintage.REVISION_POLICIES, path
        assert facts["gap"], path


# ── what makes a declaration unsound ───────────────────────────────────────


def _schema(**decl) -> dict:
    base = {
        "event_time": "as_of",
        "available_at_basis": "collection_time",
        "admissible_when": "available_at_or_before_decision",
        "revisions": "overwritten",
        "rule": "why",
    }
    return {
        "type": "object",
        "properties": {"as_of": {}, "filed": {}, "accession": {}},
        vintage.DECLARATION_KEY: {**base, **decl},
    }


def test_a_sound_declaration_parses_and_an_absent_one_is_none():
    assert vintage.declaration_errors(_schema()) == []
    assert vintage.load_declaration({"properties": {}}) is None


@pytest.mark.parametrize(
    ("decl", "fragment"),
    [
        ({"event_time": "period_end"}, "names no property"),
        ({"revisions": "superseding"}, "vintage_field is required"),
        ({"available_at_basis": "source_date_rule"}, "available_at_field is required"),
        ({"available_at_basis": "rumour"}, "is not one of"),
        ({"admissible_when": "whenever"}, "is not one of"),
        ({"rule": ""}, "rule is required"),
        ({"note": "x"}, "is not a declaration key"),
        ({"vintage_field": "revision_id", "revisions": "preserved"}, "names no property"),
    ],
)
def test_an_unsound_declaration_is_named(decl, fragment):
    errors = vintage.declaration_errors(_schema(**decl))
    assert any(fragment in e for e in errors), errors
    with pytest.raises(ValueError):
        vintage.load_declaration(_schema(**decl))


# ── replay at a decision time ──────────────────────────────────────────────

FILING = vintage.VintageDeclaration(
    event_time="period",
    available_at_basis="source_date_rule",
    admissible_when="available_before_session",
    revisions="superseding",
    rule="filed date; admissible strictly before the session",
    available_at_field="filed",
    vintage_field="accession",
)
#: Decision: Thursday 2026-10-22 09:00 ET, before the open.
THURSDAY_OPEN = dt.datetime(2026, 10, 22, 9, 0, tzinfo=NY)


def _row(cik: int, filed: str, accession: str, value: float) -> dict:
    return {"cik": cik, "period": "2026-06-30", "filed": filed, "accession": accession, "value": value}


def _admit(rows, decision=THURSDAY_OPEN, **kw):
    return vintage.admit(rows, FILING, decision=decision, record_key=("cik", "period"), **kw)


def test_a_same_day_filing_is_not_admissible_for_its_own_session():
    out = _admit([_row(1, "2026-10-22", "a1", 1.0)])
    assert out.admitted == () and out.not_yet_available == 1


def test_a_filing_from_the_prior_day_is_admitted_the_next_session():
    out = _admit([_row(1, "2026-10-21", "a1", 1.0)])
    assert [r["value"] for r in out.admitted] == [1.0]


def test_a_late_release_after_the_decision_is_excluded_and_counted():
    out = _admit([_row(1, "2026-10-21", "a1", 1.0), _row(2, "2026-10-27", "b1", 2.0)])
    assert [r["cik"] for r in out.admitted] == [1]
    assert out.not_yet_available == 1 and "not yet available" in out.reasons[0]


def test_a_correction_known_by_the_decision_supersedes_the_original():
    out = _admit([_row(1, "2026-10-15", "a1", 1.0), _row(1, "2026-10-20", "a2", 1.5)])
    assert [r["accession"] for r in out.admitted] == ["a2"] and out.superseded == 1


def test_replay_at_t_is_unchanged_by_a_later_correction():
    """I12567 closes-when: replay at T is unchanged by later corrections."""
    before = _admit([_row(1, "2026-10-15", "a1", 1.0)])
    after = _admit([_row(1, "2026-10-15", "a1", 1.0), _row(1, "2026-10-29", "a2", 9.9)])
    assert before.admitted == after.admitted
    assert after.not_yet_available == 1 and after.superseded == 0


def test_unknown_availability_is_excluded_and_counted_never_guessed_in():
    out = _admit([_row(1, "", "a1", 1.0), _row(2, "not-a-date", "b1", 2.0), _row(3, "2026-10-20", "c1", 3.0)])
    assert [r["cik"] for r in out.admitted] == [3]
    assert out.unknown_availability == 2


def test_a_contract_declared_unknown_admits_nothing():
    unknown = dataclasses.replace(FILING, available_at_basis="unknown", vintage_field=None, revisions="final")
    out = vintage.admit([_row(1, "2026-10-01", "a1", 1.0)], unknown, decision=THURSDAY_OPEN)
    assert out.admitted == () and out.unknown_availability == 1


COLLECTED = vintage.VintageDeclaration(
    event_time="as_of",
    available_at_basis="collection_time",
    admissible_when="available_at_or_before_decision",
    revisions="overwritten",
    rule="the run's data_pit stamp",
)


def test_a_collection_time_contract_needs_the_runs_stamp_to_admit_anything():
    rows = [{"as_of": "2026-10-21", "x": 1}]
    assert vintage.admit(rows, COLLECTED, decision=THURSDAY_OPEN).unknown_availability == 1
    stamp = dt.datetime(2026, 10, 21, 22, 30, tzinfo=UTC)
    assert vintage.admit(rows, COLLECTED, decision=THURSDAY_OPEN, stamp_available_at=stamp).admitted == tuple(rows)
    later = dt.datetime(2026, 10, 22, 22, 30, tzinfo=UTC)
    assert vintage.admit(rows, COLLECTED, decision=THURSDAY_OPEN, stamp_available_at=later).not_yet_available == 1


def test_a_per_row_instant_is_compared_with_the_decision_instant():
    decl = dataclasses.replace(COLLECTED, available_at_field="available_at")
    rows = [{"available_at": "2026-10-22T12:59:59Z"}, {"available_at": "2026-10-22T13:00:01Z"}]
    out = vintage.admit(rows, decl, decision=THURSDAY_OPEN)  # 09:00 ET = 13:00Z
    assert out.admitted == (rows[0],) and out.not_yet_available == 1


def test_a_naive_decision_is_refused():
    with pytest.raises(ValueError):
        vintage.admit([], COLLECTED, decision=dt.datetime(2026, 10, 22, 9, 0))


# ── the gate ───────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def units() -> dict[str, descriptors.Unit]:
    return {u.unit_id: u for u in descriptors.load_units()}


def _with_schema(unit: descriptors.Unit, name: str) -> descriptors.Unit:
    raw = {**unit.raw, "contract": {**(unit.raw.get("contract") or {}), "schema": name, "producer_test": None}}
    return dataclasses.replace(unit, raw=raw)


def _green(tmp_path):
    _write_manifest(tmp_path, outputs=[KEY], guards=[pit.pit_guard_entry(KEY, _stamped())])


def test_a_sound_declaration_is_named_on_the_clause(tmp_path, units):
    _green(tmp_path)
    unit = _with_schema(units["D01"], "contracts/metron_macro.schema.json")
    reading = evidence.read_pit(LocalStore(tmp_path), unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is True, reading.detail
    assert "collection_time/overwritten, replay at a past T not reproducible" in reading.detail


def test_an_absent_declaration_is_observed_not_graded(tmp_path, units):
    _green(tmp_path)
    unit = _with_schema(units["D01"], "contracts/universe_classification.schema.json")
    reading = evidence.read_pit(LocalStore(tmp_path), unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is True, reading.detail
    assert "observe" in reading.detail and "reported, not graded" in reading.detail


def test_an_unsound_declaration_fails_the_clause(tmp_path, units, monkeypatch):
    _green(tmp_path)
    tree = tmp_path / "repo"
    (tree / "contracts").mkdir(parents=True)
    (tree / "contracts" / "bad.schema.json").write_text(json.dumps(_schema(revisions="preserved")))
    monkeypatch.setattr(evidence, "REPO_ROOT", tree)
    unit = _with_schema(units["D01"], "contracts/bad.schema.json")
    reading = evidence.read_pit(LocalStore(tmp_path), unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert reading.met is False
    assert "INVALID" in reading.detail and "vintage_field is required" in reading.detail


def test_every_unit_on_main_reads_no_unsound_declaration(units):
    for unit in units.values():
        _, invalid, _ = evidence._vintage_declarations(unit)
        assert not invalid, (unit.unit_id, invalid)

