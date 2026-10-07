"""Point-in-time stamps: `as_of` + `available_at`, recorded and graded.

`alpha-engine-config-I10782` (plan item P-15; `data_collection_plan_260914.md`
§2 objective 4). Three surfaces, one definition (`contracts/pit.py`):

* the producer stamps a payload and records one `data_pit` guard entry per key;
* the run manifest carries that entry in its existing closed `GuardVerdict` shape;
* the gate's `data.<unit>.guard.pit` clause grades
  ``as_of <= available_at <= manifest.finished`` for every published key, and an
  output with no stamp is UNMET and named — never skipped.

No AWS: stores are local directories or in-memory fakes.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib

import jsonschema
import pytest
from nousergon_lib import run_manifest
from nousergon_lib.contracts import load_schema

import weekly_collector
from contracts import pit
from data_gate import clauses, descriptors, evidence
from data_gate.consumer_pins import schema_shape
from data_gate.store import LocalStore
from tests.data_gate_support import DeniedStore, EmptyStore

UTC = dt.timezone.utc
REPO = pathlib.Path(__file__).resolve().parents[1]

# D01 is a weekly scheduled unit (Saturday run, filed under Friday's trading
# day); the cadence reader selects runs that STARTED after the due fire.
TUESDAY = dt.date(2026, 9, 15)
WEDNESDAY_NOON = dt.datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
FRIDAY = "2026-09-11"
KEY = f"market_data/weekly/{FRIDAY}/constituents.json"


@pytest.fixture(scope="module")
def units() -> dict[str, descriptors.Unit]:
    return {u.unit_id: u for u in descriptors.load_units()}


def _stamped(as_of: str = FRIDAY, available_at: str = "2026-09-12T10:05:00Z") -> dict:
    return {"date": as_of, "tickers": ["AAPL"], "as_of": as_of, "available_at": available_at}


def _write_manifest(root: pathlib.Path, *, outputs: list[str], guards: list[dict], **over) -> None:
    doc = {
        "schema_version": "data_run_manifest.v1",
        "run_id": "RUN1",
        "unit_id": "D01",
        "trigger": "scheduled",
        "trading_day": FRIDAY,
        "status": "ok",
        "started": "2026-09-12T10:00:00Z",
        "finished": "2026-09-12T11:00:00Z",
        "outputs": [{"key": k, "rows_out": 1} for k in outputs],
        "rows_out": len(outputs),
        "guards": guards,
        **over,
    }
    path = root / "runs" / "D01" / FRIDAY / "RUN1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc))


# ── the stamp ──────────────────────────────────────────────────────────────


def test_stamp_adds_both_fields_in_their_contract_form():
    moment = dt.datetime(2026, 9, 11, 22, 15, 3, tzinfo=UTC)
    out = pit.stamp({"x": 1}, as_of=dt.date(2026, 9, 11), available_at=moment)
    assert out == {"x": 1, "as_of": "2026-09-11", "available_at": "2026-09-11T22:15:03Z"}
    schema = {"type": "object", "properties": pit.PIT_PROPERTIES, "required": [pit.AS_OF, pit.AVAILABLE_AT]}
    jsonschema.validate(out, schema)


def test_stamp_refuses_to_restamp_a_payload_that_already_says_otherwise():
    with pytest.raises(ValueError, match="refusing to restamp"):
        pit.stamp({"as_of": "2026-09-10"}, as_of="2026-09-11", available_at=dt.datetime(2026, 9, 11, 22, tzinfo=UTC))
    # Agreeing with an existing stamp is not a restamp.
    assert pit.stamp({"as_of": "2026-09-11"}, as_of="2026-09-11", now=dt.datetime(2026, 9, 11, 22, tzinfo=UTC))


def test_stamp_refuses_a_record_knowable_before_the_day_it_describes():
    # 03:00 UTC on 09-11 is still 09-10 in New York: the 09-11 session has not begun.
    with pytest.raises(ValueError, match="precedes the as_of day"):
        pit.stamp({}, as_of="2026-09-11", available_at=dt.datetime(2026, 9, 11, 3, 0, tzinfo=UTC))
    with pytest.raises(ValueError, match="timezone-aware"):
        pit.stamp({}, as_of="2026-09-11", available_at=dt.datetime(2026, 9, 11, 22, 0))


def test_available_at_must_be_utc_z_form():
    assert pit.parse_available_at("2026-09-11T22:15:03Z") == dt.datetime(2026, 9, 11, 22, 15, 3, tzinfo=UTC)
    assert pit.parse_available_at("2026-09-11T22:15:03+00:00") is None
    assert pit.parse_available_at("2026-09-11T22:15:03") is None
    assert pit.parse_as_of("2026-09-11T00:00:00Z") is None


# ── the manifest entry ─────────────────────────────────────────────────────


def _guard_verdict_validator() -> jsonschema.Draft202012Validator:
    manifest = load_schema("data_run_manifest")
    return jsonschema.Draft202012Validator({**manifest["$defs"]["GuardVerdict"], "$defs": manifest["$defs"]})


@pytest.mark.parametrize(
    "key,document,verdict",
    [
        (KEY, _stamped(), "ok"),
        (KEY, {"as_of": FRIDAY}, "unmeasurable"),
        (KEY, None, "unmeasurable"),
        (KEY, _stamped(available_at="yesterday"), "unmeasurable"),
        (KEY, _stamped(available_at="2026-09-11T03:00:00Z"), "unmeasurable"),
        ("arcticdb/universe", None, "not_applicable"),
    ],
    ids=["stamped", "no-available_at", "no-document", "unparseable", "contradictory", "arcticdb"],
)
def test_the_entry_fits_the_manifests_closed_guard_shape(key, document, verdict):
    """`data_run_manifest.v1` closes `GuardVerdict` (additionalProperties false,
    closed verdict enum) — the entry must ride it without a lib schema change."""
    entry = pit.pit_guard_entry(key, document)
    assert entry["verdict"] == verdict
    errors = list(_guard_verdict_validator().iter_errors(entry))
    assert not errors, [e.message for e in errors]


def test_the_entry_carries_its_stamps_as_numbers_the_gate_grades_without_parsing_detail():
    entry = pit.pit_guard_entry(KEY, _stamped())
    assert entry["value"] == dt.datetime(2026, 9, 12, 10, 5, tzinfo=UTC).timestamp()
    # 00:00 America/New_York (EDT, UTC-4) on the as_of day.
    assert entry["baseline"] == dt.datetime(2026, 9, 11, 4, 0, tzinfo=UTC).timestamp()
    read = pit.read_pit_entry(entry)
    assert read is not None and read.verdict == "ok" and read.key == KEY
    assert read.available_at == dt.datetime(2026, 9, 12, 10, 5, tzinfo=UTC)
    assert pit.read_pit_entry({"guard": "data_empty_fresh", "key": KEY}) is None


def test_a_missing_stamp_is_named_in_the_entry():
    entry = pit.pit_guard_entry(KEY, {"as_of": FRIDAY})
    assert "available_at" in entry["detail"] and entry["value"] is None


# ── the contracts ──────────────────────────────────────────────────────────


def test_available_at_is_declared_provenance_in_the_canonical_fragment():
    """A run timestamp that grades as data makes every re-production a breach."""
    assert pit.PIT_PROPERTIES[pit.AVAILABLE_AT]["x-provenance"] is True


def _contract_problems(schema: dict) -> list[str]:
    """The rule a contract adopting the stamps is held to (stage 2 adds them)."""
    props = schema.get("properties") or {}
    if pit.AVAILABLE_AT not in props:
        return []
    problems = []
    required = set(schema.get("required") or ())
    for field in (pit.AS_OF, pit.AVAILABLE_AT):
        if field not in props:
            problems.append(f"declares available_at without {field}")
        elif field not in required:
            problems.append(f"{field} is not required")
    if schema_shape(props[pit.AVAILABLE_AT]) != schema_shape(pit.PIT_PROPERTIES[pit.AVAILABLE_AT]):
        problems.append("available_at differs from contracts/pit.py::PIT_PROPERTIES")
    if props[pit.AVAILABLE_AT].get("x-provenance") is not True:
        problems.append("available_at is not x-provenance")
    if pit.AS_OF in props and props[pit.AS_OF].get("type") != "string":
        problems.append("as_of is not a string")
    return problems


def test_the_contract_rule_reads_an_adopting_schema_correctly():
    good = {"properties": dict(pit.PIT_PROPERTIES), "required": [pit.AS_OF, pit.AVAILABLE_AT]}
    assert _contract_problems(good) == []
    assert _contract_problems({"properties": {"as_of": {"type": "string"}}}) == []  # not adopted yet
    half = {"properties": {pit.AVAILABLE_AT: pit.PIT_PROPERTIES[pit.AVAILABLE_AT]}, "required": [pit.AVAILABLE_AT]}
    assert "declares available_at without as_of" in _contract_problems(half)
    loose = {"properties": {**pit.PIT_PROPERTIES, pit.AVAILABLE_AT: {"type": "string"}}, "required": []}
    assert {"as_of is not required", "available_at is not required"} <= set(_contract_problems(loose))
    assert "available_at differs from contracts/pit.py::PIT_PROPERTIES" in _contract_problems(loose)


@pytest.mark.parametrize("path", sorted((REPO / "contracts").glob("*.schema.json")), ids=lambda p: p.name)
def test_every_contract_that_adopts_available_at_takes_the_canonical_form(path):
    assert _contract_problems(json.loads(path.read_text())) == []


# ── the gate ───────────────────────────────────────────────────────────────


def _read(tmp_path, units) -> evidence.Reading:
    return evidence.read_pit(LocalStore(tmp_path), units["D01"], trading_day=TUESDAY, now=WEDNESDAY_NOON)


def test_every_output_stamped_inside_its_run_reads_met(tmp_path, units):
    _write_manifest(tmp_path, outputs=[KEY], guards=[pit.pit_guard_entry(KEY, _stamped())])
    reading = _read(tmp_path, units)
    assert reading.met is True, reading.detail
    assert "1/1 published key(s) stamped" in reading.detail


def test_an_output_with_no_stamp_is_unmet_and_named_not_skipped(tmp_path, units):
    other = f"market_data/weekly/{FRIDAY}/other.json"
    _write_manifest(tmp_path, outputs=[KEY, other], guards=[pit.pit_guard_entry(KEY, _stamped())])
    reading = _read(tmp_path, units)
    assert reading.met is False and reading.unmeasurable is False
    assert other in reading.detail and "1/2" in reading.detail


def test_a_stamp_after_the_run_finished_is_unmet(tmp_path, units):
    late = _stamped(available_at="2026-09-12T11:30:00Z")  # finished is 11:00Z
    _write_manifest(tmp_path, outputs=[KEY], guards=[pit.pit_guard_entry(KEY, late)])
    reading = _read(tmp_path, units)
    assert reading.met is False and "after the publishing run finished" in reading.detail


def test_an_unmeasurable_stamp_is_unmet_with_the_producers_reason(tmp_path, units):
    _write_manifest(tmp_path, outputs=[KEY], guards=[pit.pit_guard_entry(KEY, {"as_of": FRIDAY})])
    reading = _read(tmp_path, units)
    assert reading.met is False and "carries no available_at" in reading.detail


def test_an_arcticdb_library_is_exempt_and_named(tmp_path, units):
    _write_manifest(tmp_path, outputs=["arcticdb/universe"], guards=[])
    reading = _read(tmp_path, units)
    assert reading.met is True and "exempt" in reading.detail and "UniverseFreshnessViolation" in reading.detail


def test_a_run_that_published_nothing_is_not_graded_clean(tmp_path, units):
    _write_manifest(tmp_path, outputs=[], guards=[])
    reading = _read(tmp_path, units)
    assert reading.met is False and "nothing graded is not graded clean" in reading.detail


def test_no_manifest_is_unmet_and_a_denied_store_is_unmeasurable(tmp_path, units):
    absent = _read(tmp_path, units)
    assert absent.met is False and absent.unmeasurable is False
    denied = evidence.read_pit(DeniedStore(), units["D01"], trading_day=TUESDAY, now=WEDNESDAY_NOON)
    assert denied.met is False and denied.unmeasurable is True


def test_the_pit_clause_keeps_commissioning_as_its_second_half(units):
    """Stamps alone do not make the guard clause MET: phase 2 exits on guards
    enforcing AND commissioned. The detail names both halves."""
    unit = units["D01"]
    clause = clauses._clause_guard(EmptyStore(), unit, "pit", trading_day=TUESDAY)
    assert clause.name == "data.D01.guard.pit"
    assert "available_at" in clause.requirement and "COMMISSIONED" in clause.requirement
    assert "point-in-time:" in clause.detail and "commissioning:" in clause.detail
    assert "faults/D01/pit/latest.json" in clause.evidence


def test_the_pit_clause_reads_met_only_with_stamps_and_an_induced_fault(tmp_path, units, monkeypatch):
    unit = units["D01"]
    _write_manifest(tmp_path, outputs=[KEY], guards=[pit.pit_guard_entry(KEY, _stamped())])
    real = evidence.read_pit
    monkeypatch.setattr(
        evidence,
        "read_pit",
        lambda store, u, *, trading_day: real(store, u, trading_day=trading_day, now=WEDNESDAY_NOON),
    )
    store = LocalStore(tmp_path)
    stamps_only = clauses._clause_guard(store, unit, "pit", trading_day=TUESDAY)
    assert stamps_only.met is False and "no induced-fault record" in stamps_only.detail
    fault = tmp_path / "faults" / "D01" / "pit" / "latest.json"
    fault.parent.mkdir(parents=True)
    fault.write_text(json.dumps({"outcome": "induced", "as_of": "2026-09-14"}))
    both = clauses._clause_guard(store, unit, "pit", trading_day=TUESDAY)
    assert both.met is True, both.detail


def test_other_guard_clauses_are_unchanged(units):
    clause = clauses._clause_guard(EmptyStore(), units["D01"], "empty_fresh", trading_day=TUESDAY)
    assert "point-in-time" not in clause.detail and "available_at" not in clause.requirement


# ── end to end: producer → manifest → gate ─────────────────────────────────


def test_a_collector_returned_stamp_reaches_the_gate_through_the_generic_hook(tmp_path, units):
    """No shared-path change is needed for a producer to stamp: a collector puts
    `pit_guard_entry` in its result's `guards`, and the hook every collector's
    readings already pass through files it on the manifest."""
    payload = pit.stamp(
        {"date": FRIDAY, "tickers": ["AAPL"]}, as_of=FRIDAY, now=dt.datetime(2026, 9, 12, 10, 5, tzinfo=UTC)
    )
    result = {"status": "ok", "guards": [pit.pit_guard_entry(KEY, payload)]}

    def body(ctx):
        ctx.record_output(KEY, rows_out=1)
        weekly_collector._record_collector_guards(ctx, result)

    run_manifest.run_unit(
        "D01",
        body,
        sink=run_manifest.LocalDirManifestSink(str(tmp_path)),
        trigger="scheduled",
        trading_day=FRIDAY,
        log_location="local://test",
        now=dt.datetime(2026, 9, 12, 10, 0, tzinfo=UTC),
        code_sha="a" * 40,
    )
    reading = evidence.read_pit(
        LocalStore(tmp_path / "data_collection"), units["D01"], trading_day=TUESDAY, now=WEDNESDAY_NOON
    )
    assert reading.met is True, reading.detail
    manifest = json.loads(next((tmp_path / "data_collection" / "runs" / "D01").rglob("*.json")).read_text())
    assert [g["guard"] for g in manifest["guards"]] == [pit.PIT_GUARD_NAME]
    assert dataclasses.asdict(pit.read_pit_entry(manifest["guards"][0]))["verdict"] == "ok"
