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
