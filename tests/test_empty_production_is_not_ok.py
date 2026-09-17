"""A producer unit that publishes NOTHING never records `ok`.

`alpha-engine-config-I11011`. The 2026-09-15 shadow run recorded ten units
(D20, D22-D30) whose manifests read ``status: ok``, ``rows_out: 0``,
``outputs: []`` — a zero-second run that produced nothing, filed as a success.
Every surface that consumes the manifest counted it as a recorded run; the
parity comparator was the only thing that noticed, and only because it went
looking for outputs that were not there. Until the manifest can say "this ran
and produced nothing", every fix to the underlying no-op is unverifiable,
because success and silence render identically (`engagement-protocol-policy`
§5: detection blindness outranks the defects it hides).

**The property graded here, over EVERY descriptor in `registry.d/units/`**
(deliverable 3, the same shape as `test_every_audit_cell_is_a_clause.py`): for
every unit, a manifest with ``rows_out: 0`` and ``outputs: []`` either maps to
a terminal state that is not ``ok``, or the unit's descriptor carries the
declaration that makes zero output legitimate for it. There is no third
answer, and the declaration is never the default reading of an empty result.

A whole-descriptor-set test rather than a per-unit one for the reason the
descriptors exist at all: a unit added without a declaration must be caught by
the population, not by someone remembering to add a case.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest

import run_units
from data_gate import descriptors, evidence
from data_gate.store import LocalStore
from nousergon_lib import run_manifest

TRADING_DAY = dt.date(2026, 9, 14)
#: Late enough on the trading day that every unit's cadence has a due fire whose
#: window contains the manifest below — the reading under test is the terminal
#: state, never the schedule.
_AS_OF = dt.datetime(2026, 9, 14, 23, 0, tzinfo=dt.timezone.utc)


@pytest.fixture(scope="module")
def units() -> list[descriptors.Unit]:
    return descriptors.load_units()


def _manifest(unit_id: str = "D19", **over) -> dict:
    base = {
        "schema_version": "data_run_manifest.v1",
        "run_id": "01JBX0000000000000000000AA",
        "unit_id": unit_id,
        "trigger": "scheduled",
        "trading_day": TRADING_DAY.isoformat(),
        "calendar_date": TRADING_DAY.isoformat(),
        "status": "ok",
        "reason": "",
        "started": "2026-09-14T12:00:00Z",
        "finished": "2026-09-14T12:00:00Z",
        "code_sha": "a" * 40,
        "log_location": "cloudwatch:/alpha-engine/data-spot",
        "inputs": [],
        "outputs": [],
        "rows_in": 0,
        "rows_out": 0,
        "rows_rejected": [],
        "cost_usd": 0.0,
        "compute": {"instance_type": "c6i.large", "spot": True},
    }
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# Deliverable 3 — the whole-descriptor-set property.
# ---------------------------------------------------------------------------


def test_every_descriptor_maps_an_empty_run_to_a_non_ok_state_or_declares_it(units):
    """For every unit: empty is not `ok`, or the descriptor says why it may be."""
    offenders: list[str] = []
    for unit in units:
        manifest = _manifest(unit_id=unit.unit_id)
        declared = run_units.empty_declaration(unit.raw)
        if declared is None and not run_units.is_empty_success(manifest):
            # An undeclared unit whose empty manifest does NOT read as an empty
            # success would mean the predicate stopped recognising the shape.
            offenders.append(f"{unit.unit_id}: predicate no longer reads an empty run as empty")
        if declared is not None and declared.reason not in run_manifest.NOT_APPLICABLE_REASONS:
            offenders.append(f"{unit.unit_id}: declared reason {declared.reason!r} is not closed-list")
    assert not offenders, (
        "a unit's empty run must map to a non-`ok` terminal state or carry an "
        f"`{run_units.EMPTY_IS_VALID_FIELD}` declaration: {offenders}"
    )


def test_every_declaration_is_well_formed_and_carries_its_evidence(units):
    """A declaration switches off a RAISE, so it names a reason AND a note."""
    for unit in units:
        declared = run_units.empty_declaration(unit.raw)
        if declared is None:
            continue
        assert declared.note, f"{unit.unit_id}: {run_units.EMPTY_IS_VALID_FIELD} has no note"
        assert declared.reason in run_manifest.NOT_APPLICABLE_REASONS


def test_the_board_does_not_count_an_undeclared_empty_run_for_any_unit(tmp_path, units):
    """Deliverable 2, over the whole population — not one hand-picked unit."""
    for unit in units:
        if unit.retired or run_units.empty_declaration(unit.raw) is not None:
            continue
        store = _store_with(tmp_path / unit.unit_id, unit, _manifest(unit_id=unit.unit_id))
        reading = evidence.read_run_record(store, unit, trading_day=TRADING_DAY, now=_AS_OF)
        assert not reading.met, f"{unit.unit_id}: an empty `ok` manifest was counted as a run"
        assert "published NOTHING" in reading.detail


def test_a_malformed_declaration_is_refused_at_descriptor_load(units):
    """Not-declared is the safe reading for the run and the wrong one for the author."""
    document = dict(units[0].raw)
    document[run_units.EMPTY_IS_VALID_FIELD] = {"reason": "because we said so", "note": "x"}
    with pytest.raises(descriptors.DescriptorError):
        descriptors._validate(units[0].unit_id, document, units[0].path)

    document[run_units.EMPTY_IS_VALID_FIELD] = {"reason": "not_a_trading_day", "note": "  "}
    with pytest.raises(descriptors.DescriptorError):
        descriptors._validate(units[0].unit_id, document, units[0].path)

    document[run_units.EMPTY_IS_VALID_FIELD] = True
    with pytest.raises(descriptors.DescriptorError):
        descriptors._validate(units[0].unit_id, document, units[0].path)


# ---------------------------------------------------------------------------
# The predicate itself — one predicate, read by producer and reader alike.
# ---------------------------------------------------------------------------


def test_the_predicate_reads_the_property_not_a_producer_set_marker():
    assert run_units.is_empty_success(_manifest())
    # A run that published something is never empty, whatever its row count.
    assert not run_units.is_empty_success(
        _manifest(outputs=[{"key": "market_data/eod_closes/latest.json", "rows_out": 0}])
    )
    # Rows without a recorded output key (an ArcticDB write addressed by prefix)
    # is a published run too.
    assert not run_units.is_empty_success(_manifest(rows_out=12))
    # The honest terminal states are already distinguishable; they are not
    # re-graded here.
    assert not run_units.is_empty_success(_manifest(status="failed"))
    assert not run_units.is_empty_success(_manifest(status="not_applicable"))


def test_the_ten_shadow_manifests_are_named_by_the_reader():
    """The measured 2026-09-15 shape, verbatim — the record that started this."""
    d20 = _manifest(
        unit_id="D20",
        guards=[
            {
                "guard": "data_empty_fresh",
                "mode": "observe",
                "verdict": "not_applicable",
                "detail": "D20 published nothing on this run (same-date auto-skip)",
                "key": "market_data/eod_closes/2026-09-14.json",
                "value": None,
                "baseline": None,
            }
        ],
    )
    # Keyed on the PROPERTY: the guard verdict the producer happened to record
    # is the mechanism, and a reader keyed on it goes blind the moment the
    # producer stops setting it.
    assert evidence.empty_success_runs([d20]) == [d20["run_id"]]
    assert evidence.empty_fresh_runs([d20]) == []


# ---------------------------------------------------------------------------
# The producer half — the record moves, the exit code does not.
# ---------------------------------------------------------------------------


class _Ctx:
    """Enough of `nousergon_lib.run_manifest.UnitRun` to record a guard."""

    def __init__(self) -> None:
        self.guards: list[dict] = []

    def record_guard(self, guard: str, **fields) -> None:
        self.guards.append({"guard": guard, **fields})


def test_an_undeclared_empty_run_raises_and_records_the_guard(monkeypatch):
    monkeypatch.setattr(run_units, "empty_declaration_for", lambda unit_id: None)
    ctx = _Ctx()
    with pytest.raises(run_units.EmptyProduction):
        run_units.record_empty_production(ctx, "D20", detail="phase wrote no key")
    assert [g["guard"] for g in ctx.guards] == [run_units.EMPTY_PRODUCTION_GUARD]
    assert ctx.guards[0]["verdict"] == "empty_fresh"
    assert ctx.guards[0]["mode"] == "enforce"


def test_a_declared_empty_run_records_not_applicable_with_its_closed_reason(monkeypatch):
    monkeypatch.setattr(
        run_units,
        "empty_declaration_for",
        lambda unit_id: run_units.EmptyDeclaration(
            reason="no_new_data_declared", note="a heal pass with nothing to heal"
        ),
    )
    ctx = _Ctx()
    with pytest.raises(run_manifest.NotApplicable) as raised:
        run_units.record_empty_production(ctx, "D33", detail="nothing to heal")
    assert raised.value.reason == "no_new_data_declared"
    assert ctx.guards[0]["verdict"] == "not_applicable"
    assert "heal" in ctx.guards[0]["detail"]


def test_a_recorded_entry_that_wrote_nothing_files_failed_without_changing_its_return(monkeypatch):
    """The record becomes honest; the entry point's contract does not move."""
    monkeypatch.setattr(run_units, "empty_declaration_for", lambda unit_id: None)
    written: dict[str, dict] = {}

    class _Sink:
        def write(self, key: str, payload: bytes) -> None:
            written[key] = json.loads(payload)

    monkeypatch.setattr(run_units, "manifest_sink", lambda *a, **k: _Sink())

    def _body(ctx):
        return {"status": "ok", "rows": 0}

    value = run_units.recorded_entry(
        "D36",
        _body,
        trigger="scheduled",
        trading_day=TRADING_DAY.isoformat(),
        code_sha="b" * 40,
    )
    assert value == {"status": "ok", "rows": 0}
    (manifest,) = written.values()
    assert manifest["status"] == "failed"
    assert "EmptyProduction" in manifest["reason"]
    assert manifest["guards"][0]["guard"] == run_units.EMPTY_PRODUCTION_GUARD
    # And the honest record is what the board then reads.
    assert not run_units.is_empty_success(manifest)


def test_a_recorded_entry_that_published_something_is_untouched(monkeypatch):
    written: dict[str, dict] = {}

    class _Sink:
        def write(self, key: str, payload: bytes) -> None:
            written[key] = json.loads(payload)

    monkeypatch.setattr(run_units, "manifest_sink", lambda *a, **k: _Sink())

    def _body(ctx):
        ctx.record_output("news/daily/2026-09-14.json", rows_out=12)
        return "done"

    assert (
        run_units.recorded_entry(
            "D36",
            _body,
            trigger="scheduled",
            trading_day=TRADING_DAY.isoformat(),
            code_sha="b" * 40,
        )
        == "done"
    )
    (manifest,) = written.values()
    assert manifest["status"] == "ok"
    assert [g["guard"] for g in manifest.get("guards", [])] == []


# ---------------------------------------------------------------------------


def _store_with(root: pathlib.Path, unit: descriptors.Unit, manifest: dict) -> LocalStore:
    # The unit's OWN declared prefix — D46 deliberately shares D16's, because it
    # runs inside D16's unit, and a test that assumed `runs/<unit_id>` would file
    # its fixture where that unit's reader never looks.
    prefix = evidence._store_relative(unit.run_manifest_prefix)
    path = root / prefix / TRADING_DAY.isoformat() / f"{manifest['run_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest))
    return LocalStore(root)
