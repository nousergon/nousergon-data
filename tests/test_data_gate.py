"""The data gate's own invariants — including the phase-0 commissioning test.

Plan §6 phase-0 exit **(e)**, verbatim: *"commissioning: in a test store,
withholding the ladder renders the ladder rows UNREPORTED and stale, withholding
one manifest renders that clause UNMEASURABLE, and neither renders green. The
phase-0 gate measures that the board is HONEST, not that it is green."*

Everything here runs against an in-memory or tmp-dir store. No AWS.
"""

from __future__ import annotations

import json
import textwrap

import pytest
import yaml

from data_gate import clauses as clause_module
from data_gate import descriptors, evidence
from data_gate.descriptors import DescriptorError, load_units
from data_gate.inventory import scan
from data_gate.read import GATES, BOARD_KEY, evaluate, load_phases, run
from data_gate.store import DryRunWriteRefusedError, LocalStore, open_store, parse_store_uri

from tests.data_gate_support import TRADING_DAY, DeniedStore, EmptyStore


@pytest.fixture(scope="module")
def units():
    return load_units()


@pytest.fixture(scope="module")
def phases():
    return load_phases()


@pytest.fixture(scope="module")
def board(units, phases):
    return clause_module.generate(EmptyStore(), units, phases, trading_day=TRADING_DAY)


# ---------------------------------------------------------------------------
# Phase-0 exit (e) — the commissioning test.
# ---------------------------------------------------------------------------


def test_withholding_the_ladder_renders_unreported_never_green(tmp_path):
    """No ladder in the store: the freshness clause fails and nothing is green."""
    store = LocalStore(tmp_path)
    result, ladder, board_doc = run(store, gate="data-phase0", trading_day=TRADING_DAY)

    fresh = next(c for c in result.clauses if c.name == "data.gate.ladder_fresh")
    assert fresh.met is False
    assert "never been published" in fresh.detail

    assert result.met is False
    # Every rung that has never been read renders red on the console — never
    # HEALTHY, and never a number that reads as a partial pass.
    for row in ladder["phases"]:
        assert row["console_state"] in {"DEGRADED", "FAILED", "UNREPORTED"}
        assert row["state"] != "MET"


def test_a_stale_ladder_pages_rather_than_rendering_its_last_state(tmp_path):
    """Page condition 3: the observer observed.

    Without this the whole board freezes in its last state and keeps rendering
    it, indefinitely, with nothing saying the gate stopped running.
    """
    store = LocalStore(tmp_path)
    run(store, gate="data-phase0", trading_day=TRADING_DAY)
    document = json.loads((tmp_path / "gates" / "ladder.json").read_text())
    document["generated_utc"] = "2026-09-01T00:00:00Z"
    (tmp_path / "gates" / "ladder.json").write_text(json.dumps(document))

    result, _ladder, _board = run(store, gate="data-phase0", trading_day=TRADING_DAY)
    fresh = next(c for c in result.clauses if c.name == "data.gate.ladder_fresh")
    assert fresh.met is False
    assert "page condition 3" in fresh.detail


def test_withholding_a_manifest_renders_unmeasurable_never_green(board):
    """A clause whose evidence has no reader yet is UNMEASURABLE, with its key."""
    run_record = next(c for c in board if c.name == "data.D01.run_record")
    assert run_record.unmeasurable is True
    assert run_record.met is False
    assert "data_collection/runs/D01" in " ".join(run_record.evidence)


def test_a_denied_store_never_produces_a_met_gate(units, phases):
    """Withholding the whole store: red, and red for OUR reason, not theirs."""
    clauses = clause_module.generate(DeniedStore(), units, phases, trading_day=TRADING_DAY)
    assert any(c.unmeasurable for c in clauses)
    for gate in GATES:
        result = evaluate(DeniedStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=clauses)
        assert result.met is False
        # One unmeasurable clause makes the whole ratio None: a number computed
        # over a partial read is an overclaim.
        assert result.met_ratio is None


def test_no_clause_is_met_without_a_read(board):
    """`never MET without a read` — the rule evidence.py exists to enforce."""
    for clause in board:
        if not clause.met:
            continue
        assert clause.evidence, f"{clause.name} reads MET citing no evidence"
        assert "no reader is built" not in clause.detail, (
            f"{clause.name} reads MET off a phase-0 stub — the one thing a stub may never do"
        )


def test_the_board_is_red_at_birth(board):
    """214 cells were scored PRESENT; none of them is green on day one."""
    base = [c for c in board if c.name.startswith("data.D") and ".guard." not in c.name]
    assert len(base) == 414
    met = [c for c in base if c.met]
    # Only `schema_contract` has a real reader in phase 0, and it reads MET only
    # where a schema, a producer test AND a consumer pin all exist.
    assert all(c.name.endswith(".schema_contract") for c in met), sorted(c.name for c in met)
    assert len(met) < 20, "far more base clauses read MET than have real readers"


# ---------------------------------------------------------------------------
# The gate contract
# ---------------------------------------------------------------------------


def test_an_unregistered_gate_raises_rather_than_grading_nothing():
    with pytest.raises(KeyError, match="unknown gate"):
        evaluate(EmptyStore(), gate="data-phase9", trading_day=TRADING_DAY)


def test_each_gate_grades_only_the_clauses_tagged_at_or_below_its_phase(board):
    ceilings = {}
    for gate, ceiling in GATES.items():
        result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board)
        ceilings[gate] = len(result.clauses)
        for clause in result.clauses:
            assert int(clause.phase.removeprefix("data-phase")) <= ceiling
    assert ceilings["data-phase0"] < ceilings["data-phase1"] < ceilings["data-phase3"]
    assert ceilings["data-phase3"] == len(board), "phase 3's exit is 'all clauses MET'"


def test_every_reading_names_its_store_and_its_commit(board):
    """Plan §6: one number per phase, quoted WITH its store and commit."""
    result = evaluate(EmptyStore(), gate="data-phase0", trading_day=TRADING_DAY, all_clauses=board)
    assert result.store == "memory://empty"
    assert result.code_sha
    assert result.coverage and "out of" in result.coverage


def test_a_dry_run_writes_nothing(tmp_path):
    store = LocalStore(tmp_path, dry_run=True)
    run(store, gate="data-phase0", trading_day=TRADING_DAY, dry_run=True)
    assert not list(tmp_path.rglob("*")), "a dry run left artifacts behind"


def test_a_dry_run_store_refuses_a_write_outright(tmp_path):
    with pytest.raises(DryRunWriteRefusedError):
        LocalStore(tmp_path, dry_run=True).put_bytes("gates/ladder.json", b"{}")


def test_a_real_run_publishes_the_reading_the_ladder_and_the_board(tmp_path):
    store = LocalStore(tmp_path)
    run(store, gate="data-phase0", trading_day=TRADING_DAY)
    assert (tmp_path / "gates" / "ladder.json").is_file()
    assert (tmp_path / BOARD_KEY).is_file()
    assert (tmp_path / "gates" / "data-phase0" / "2026-09-14" / "gate.json").is_file()


def test_the_board_document_publishes_the_transparency_gap(tmp_path):
    """observability-policy §8.4: published even at zero, or absence reads as health."""
    _result, _ladder, board_doc = run(LocalStore(tmp_path), gate="data-phase0", trading_day=TRADING_DAY)
    assert "transparency_gap" in board_doc
    assert board_doc["transparency_gap"] > 0, "on day one the gap is most of the board"
    assert board_doc["clauses_total"] == board_doc["clauses_met"] + board_doc["clauses_unmet"] + board_doc["transparency_gap"]
    for row in board_doc["rows"]:
        assert set(row) >= {"clause", "state", "console_state", "source", "as_of", "evidence"}


def test_the_ladder_and_the_gate_reading_cannot_disagree(tmp_path):
    """One evaluation, several projections — the crucible I10575 defect, refused."""
    store = LocalStore(tmp_path)
    result, ladder, _board = run(store, gate="data-phase0", trading_day=TRADING_DAY)
    row = next(r for r in ladder["phases"] if r["gate"] == "data-phase0")
    assert row["clauses_total"] == len(result.clauses)
    assert row["clauses_met"] == sum(1 for c in result.clauses if c.met)


def test_the_cli_exit_codes_separate_not_met_from_unmeasured(tmp_path, monkeypatch):
    from data_gate.__main__ import EXIT_NOT_MET, EXIT_UNMEASURED, main

    code = main(["read", "--gate", "data-phase0", "--store", str(tmp_path), "--trading-day", "2026-09-14"])
    assert code == EXIT_NOT_MET

    def _explode(*_args, **_kwargs):
        raise RuntimeError("the descriptors would not load")

    monkeypatch.setattr("data_gate.read.run", _explode)
    assert (
        main(["read", "--gate", "data-phase0", "--store", str(tmp_path), "--dry-run"])
        == EXIT_UNMEASURED
    )


def test_store_uris_resolve(tmp_path):
    assert parse_store_uri("s3://bucket/prefix") == ("s3", "bucket/prefix")
    store = open_store("s3://alpha-engine-research/data_collection")
    assert store.uri == "s3://alpha-engine-research/data_collection"
    assert isinstance(open_store(str(tmp_path)), LocalStore)


# ---------------------------------------------------------------------------
# Descriptor discipline — the refusals
# ---------------------------------------------------------------------------


def _write_descriptor(tmp_path, overrides: dict) -> None:
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base.update(overrides)
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))


def test_an_empty_units_directory_is_refused(tmp_path):
    with pytest.raises(DescriptorError, match="no unit descriptors"):
        descriptors.load_units(tmp_path)


def test_a_missing_audit_column_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["audit"]["cells"].pop("detector")
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="scores exactly"):
        descriptors.load_units(tmp_path)


def test_an_unknown_cell_state_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["audit"]["cells"]["detector"] = "PROBABLY FINE"
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="four cell states"):
        descriptors.load_units(tmp_path)


def test_a_not_applicable_guard_without_a_taxonomy_code_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["guards"]["cardinality"] = {"state": "not_applicable", "note": "does not apply"}
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="no `na_code`"):
        descriptors.load_units(tmp_path)


def test_an_na_code_outside_the_closed_taxonomy_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["guards"]["cardinality"] = {
        "state": "not_applicable",
        "na_code": "N/A-WHATEVER",
        "note": "x",
    }
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="closed taxonomy"):
        descriptors.load_units(tmp_path)


def test_a_missing_guard_class_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["guards"].pop("pit")
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="missing"):
        descriptors.load_units(tmp_path)


def test_empty_consumers_without_a_reason_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["consumers"] = []
    base.pop("consumers_reason", None)
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="no consumers and gives no reason"):
        descriptors.load_units(tmp_path)


def test_a_filename_that_disagrees_with_the_unit_id_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    (tmp_path / "D99-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="filename"):
        descriptors.load_units(tmp_path)


def test_every_declared_na_uses_the_closed_taxonomy(units):
    for unit in units:
        for name, block in unit.guards.items():
            if block.get("state") == "not_applicable":
                assert block["na_code"] in descriptors.NA_TAXONOMY, f"{unit.unit_id}/{name}"


# ---------------------------------------------------------------------------
# The writer inventory — the one clause that notices a unit nobody registered
# ---------------------------------------------------------------------------


def test_the_writer_inventory_reconciles_today(units):
    reading = scan(units)
    assert reading.undeclared == [], (
        "write-site file(s) with no descriptor and no declared parent: "
        f"{reading.undeclared}. Add the unit descriptor, or attribute the module to its "
        "parent in registry.d/writer_inventory.yaml with a reason."
    )
    assert reading.units_without_write_site == []
    assert not reading.parse_failures


def test_a_new_writer_with_no_descriptor_turns_the_board_red(units, tmp_path, monkeypatch):
    """The mechanism, exercised: an unclaimed write site fails the clause."""
    fake_root = tmp_path / "repo"
    (fake_root / "collectors").mkdir(parents=True)
    (fake_root / "collectors" / "brand_new_collector.py").write_text(
        textwrap.dedent(
            """
            def collect(client):
                client.put_object(Bucket="b", Key="k", Body=b"{}")
            """
        )
    )
    monkeypatch.setattr("data_gate.inventory.REPO_ROOT", fake_root)
    reading = scan(units)
    assert "collectors/brand_new_collector.py" in reading.undeclared


def test_a_file_that_will_not_parse_is_not_no_write_sites(units, tmp_path, monkeypatch):
    fake_root = tmp_path / "repo"
    (fake_root / "collectors").mkdir(parents=True)
    (fake_root / "collectors" / "broken.py").write_text("def f(:\n")
    monkeypatch.setattr("data_gate.inventory.REPO_ROOT", fake_root)
    reading = scan(units)
    assert "collectors/broken.py" in reading.parse_failures
    assert reading.ok is False


# ---------------------------------------------------------------------------
# Evidence readers
# ---------------------------------------------------------------------------


def test_a_guard_is_not_commissioned_by_an_absorbed_fault_record(units):
    """Only an `induced` record is evidence the guard actually fired."""
    unit = next(u for u in units if u.unit_id == "D20")
    store = EmptyStore(
        {
            "faults/D20/cardinality/latest.json": json.dumps(
                {"outcome": "absorbed", "as_of": "2026-09-14"}
            ).encode()
        }
    )
    reading = evidence.read_guard_commissioning(store, unit, "cardinality", trading_day=TRADING_DAY)
    assert reading.met is False
    assert "absorbed" in reading.detail


def test_a_guard_commissioned_by_an_induced_fault_reads_met(units):
    unit = next(u for u in units if u.unit_id == "D20")
    store = EmptyStore(
        {
            "faults/D20/cardinality/latest.json": json.dumps(
                {"outcome": "induced", "as_of": "2026-09-14"}
            ).encode()
        }
    )
    reading = evidence.read_guard_commissioning(store, unit, "cardinality", trading_day=TRADING_DAY)
    assert reading.met is True


def test_an_objective_status_outside_the_closed_set_is_a_finding():
    store = EmptyStore(
        {"metrics/cost/monthly/latest.json": json.dumps({"status": "probably ok"}).encode()}
    )
    reading = evidence.read_objective(store, "metrics/cost/monthly/latest.json")
    assert reading.met is False
    assert "closed set" in reading.detail


def test_an_objective_with_no_emitter_is_unobserved_not_met():
    reading = evidence.read_objective(EmptyStore(), "metrics/cost/monthly/latest.json")
    assert reading.met is False
    assert reading.unmeasurable is False, "absence is an ANSWER; only a failed read is unmeasurable"


def test_a_denied_objective_read_is_unmeasurable_not_a_finding():
    reading = evidence.read_objective(DeniedStore(), "metrics/cost/monthly/latest.json")
    assert reading.unmeasurable is True


# ---------------------------------------------------------------------------
# The phases declaration
# ---------------------------------------------------------------------------


def test_every_declared_phase_has_a_registered_gate(phases):
    for phase in phases:
        assert phase.gate in GATES, f"{phase.id} names an unregistered gate {phase.gate!r}"


def test_a_rung_pointing_at_the_parent_issue_holds_the_board_clause(board):
    clause = next(c for c in board if c.name == "data.board.phase_trackers_declared")
    assert clause.met is False, (
        "P-26 has not filed the per-phase issues yet; while any rung points at the parent "
        "KEY issue this clause must be UNMET, or the ladder looks tracked and is not"
    )
    assert "P-26" in clause.detail
