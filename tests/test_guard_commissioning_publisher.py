"""The commissioning-record publisher writes only what a passing run proved.

`alpha-engine-config-I10786`. The phase-2 gate MET-s a guard clause on the
record alone (`data_gate.evidence.read_guard_commissioning`), so the publisher
is the only thing standing between "the test passed" and "the gate says
commissioned". These cases pin its refusals; the harness itself is
`tests/test_guard_commissioning_empty_fresh.py`.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from data_gate import descriptors, evidence
from data_gate import guard_commissioning as gc
from data_gate.store import LocalStore


@pytest.fixture
def clean_tree(monkeypatch):
    monkeypatch.setattr(gc, "_git", lambda *a: "a" * 40 if a[0] == "rev-parse" else "")


def _passed(nodeids):
    return {n: "passed" for n in nodeids}


def test_write_publishes_a_record_the_gate_reads_met(tmp_path, monkeypatch, clean_tree):
    monkeypatch.setattr(gc, "_run_harness", _passed)
    assert gc.main(["--guard", "empty_fresh", "--unit", "D23", "--store", str(tmp_path), "--write"]) == 0

    document = json.loads((tmp_path / "faults/D23/empty_fresh/latest.json").read_text())
    assert document["outcome"] == "induced"
    assert document["code_sha"] == "a" * 40
    assert document["test"].endswith("[D23]")
    unit = next(u for u in descriptors.load_units() if u.unit_id == "D23")
    reading = evidence.read_guard_commissioning(
        LocalStore(tmp_path), unit, "empty_fresh", trading_day=dt.date(2026, 10, 15)
    )
    assert reading.met is True, reading.detail


def test_one_failed_node_publishes_nothing_for_any_unit(tmp_path, monkeypatch, clean_tree):
    monkeypatch.setattr(
        gc, "_run_harness", lambda ns: {n: ("failed" if n.endswith("[D24]") else "passed") for n in ns}
    )
    argv = ["--guard", "empty_fresh", "--unit", "D23", "--unit", "D24", "--store", str(tmp_path), "--write"]
    assert gc.main(argv) == 1
    assert not (tmp_path / "faults").exists()


def test_a_node_that_was_not_collected_is_not_a_pass(tmp_path, monkeypatch, clean_tree):
    monkeypatch.setattr(gc, "_run_harness", lambda ns: {n: "not_collected" for n in ns})
    assert gc.main(["--guard", "empty_fresh", "--unit", "D23", "--store", str(tmp_path), "--write"]) == 1
    assert not (tmp_path / "faults").exists()


def test_dry_run_is_the_default_and_writes_nothing(tmp_path, monkeypatch, clean_tree):
    monkeypatch.setattr(gc, "_run_harness", _passed)
    assert gc.main(["--guard", "empty_fresh", "--unit", "D23", "--store", str(tmp_path)]) == 0
    assert not (tmp_path / "faults").exists()


def test_write_from_a_dirty_tree_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(gc, "_git", lambda *a: "a" * 40 if a[0] == "rev-parse" else " M weekly_collector.py")
    monkeypatch.setattr(gc, "_run_harness", _passed)
    assert gc.main(["--guard", "empty_fresh", "--unit", "D23", "--store", str(tmp_path), "--write"]) == 2
    assert not (tmp_path / "faults").exists()


def test_a_unit_without_an_induced_fault_case_is_refused(tmp_path, clean_tree):
    with pytest.raises(SystemExit, match="no induced-fault case"):
        gc.main(["--guard", "empty_fresh", "--unit", "D13", "--store", str(tmp_path)])


def test_the_harness_runner_reads_real_pytest_outcomes():
    """End to end once, with no monkeypatching: one real node, run and read back."""
    node = gc.HARNESSES["empty_fresh"].nodeid("D23")
    assert gc._run_harness([node]) == {node: "passed"}


def test_success_without_output_publishes_its_own_observed_lines(tmp_path, monkeypatch, clean_tree):
    """The second harness (`tests/test_guard_commissioning_success_without_output.py`).

    Its record carries what THAT harness asserts (one completion-check page, no
    retry), never the `empty_fresh` record's NotifyFailure lines.
    """
    monkeypatch.setattr(gc, "_run_harness", _passed)
    argv = ["--guard", "success_without_output", "--unit", "D26", "--store", str(tmp_path), "--write"]
    assert gc.main(argv) == 0
    document = json.loads((tmp_path / "faults/D26/success_without_output/latest.json").read_text())
    assert document["outcome"] == "induced"
    assert document["observed"] == list(gc.SUCCESS_WITHOUT_OUTPUT_OBSERVED)
    assert document["test"] == gc.HARNESSES["success_without_output"].nodeid("D26")
    unit = next(u for u in descriptors.load_units() if u.unit_id == "D26")
    reading = evidence.read_guard_commissioning(
        LocalStore(tmp_path), unit, "success_without_output", trading_day=dt.date(2026, 10, 15)
    )
    assert reading.met is True, reading.detail


def test_success_without_output_refuses_a_unit_it_has_no_case_for(tmp_path, clean_tree):
    """D23 has an `empty_fresh` case but no `success_without_output` one: nothing to publish."""
    with pytest.raises(SystemExit, match="no induced-fault case"):
        gc.main(["--guard", "success_without_output", "--unit", "D23", "--store", str(tmp_path)])


def test_the_harness_runner_reads_a_real_success_without_output_outcome():
    node = gc.HARNESSES["success_without_output"].nodeid("D03")
    assert gc._run_harness([node]) == {node: "passed"}


def test_cardinality_publishes_its_own_observed_lines(tmp_path, monkeypatch, clean_tree):
    """The third harness (`tests/test_guard_commissioning_cardinality.py`).

    Its record carries what THAT harness asserts (a truncated universe refused by
    the unit's own coverage check), never another class's lines.
    """
    monkeypatch.setattr(gc, "_run_harness", _passed)
    argv = ["--guard", "cardinality", "--unit", "D29", "--store", str(tmp_path), "--write"]
    assert gc.main(argv) == 0
    document = json.loads((tmp_path / "faults/D29/cardinality/latest.json").read_text())
    assert document["outcome"] == "induced"
    assert document["observed"] == list(gc.CARDINALITY_OBSERVED)
    assert document["test"] == gc.HARNESSES["cardinality"].nodeid("D29")
    unit = next(u for u in descriptors.load_units() if u.unit_id == "D29")
    reading = evidence.read_guard_commissioning(
        LocalStore(tmp_path), unit, "cardinality", trading_day=dt.date(2026, 10, 15)
    )
    assert reading.met is True, reading.detail


def test_cardinality_refuses_a_unit_it_has_no_case_for(tmp_path, clean_tree):
    """D20's cardinality guard is the observe-only shared reading: no case, nothing to publish."""
    with pytest.raises(SystemExit, match="no induced-fault case"):
        gc.main(["--guard", "cardinality", "--unit", "D20", "--store", str(tmp_path)])


def test_the_harness_runner_reads_a_real_cardinality_outcome():
    node = gc.HARNESSES["cardinality"].nodeid("D22")
    assert gc._run_harness([node]) == {node: "passed"}


def test_units_publishes_its_own_observed_lines(tmp_path, monkeypatch, clean_tree):
    """The fourth harness (`tests/test_guard_commissioning_units.py`), on D01's own detection."""
    monkeypatch.setattr(gc, "_run_harness", _passed)
    argv = ["--guard", "units", "--unit", "D01", "--store", str(tmp_path), "--write"]
    assert gc.main(argv) == 0
    document = json.loads((tmp_path / "faults/D01/units/latest.json").read_text())
    assert document["observed"] == list(gc.UNITS_OBSERVED)
    assert document["test"] == gc.HARNESSES["units"].nodeid("D01")
    unit = next(u for u in descriptors.load_units() if u.unit_id == "D01")
    reading = evidence.read_guard_commissioning(
        LocalStore(tmp_path), unit, "units", trading_day=dt.date(2026, 10, 15)
    )
    assert reading.met is True, reading.detail


def test_units_refuses_a_unit_it_has_no_case_for(tmp_path, clean_tree):
    """D02 declares no units check: no case, nothing to publish."""
    with pytest.raises(SystemExit, match="no induced-fault case"):
        gc.main(["--guard", "units", "--unit", "D02", "--store", str(tmp_path)])


def test_the_harness_runner_reads_a_real_units_outcome():
    node = gc.HARNESSES["units"].nodeid("D01")
    assert gc._run_harness([node]) == {node: "passed"}
