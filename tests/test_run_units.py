"""The phase → audit-unit table, graded against the committed descriptors.

`alpha-engine-config-I10773` (P-06). The table in `run_units.py` is the one
hand-declared link in the run-record chain — `registry.d/units/` is the source
of units and `data_gate/descriptors.py` is the source of truth for that set, but
neither carries a machine-resolvable pointer to the call site. So the link is
declared once and graded HERE, in both directions: an entry naming a unit that
has no descriptor fails, and a `_phase_collect` call site with no entry fails.

That two-way grading is the whole point. A one-way check would let a collector
be added with no unit — which is a unit whose executions are invisible, the
exact defect the run-record objective exists to end.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import run_units
from data_gate.descriptors import load_units

REPO_ROOT = Path(__file__).resolve().parents[1]
WEEKLY_COLLECTOR = REPO_ROOT / "weekly_collector.py"


@pytest.fixture(scope="module")
def declared_unit_ids() -> set[str]:
    return {u.unit_id for u in load_units()}


def test_every_mapped_unit_has_a_descriptor(declared_unit_ids):
    mapped = {pu.unit_id for pu in run_units.PHASE_UNITS.values()}
    mapped |= set(run_units.MODE_UNITS.values())
    unknown = sorted(mapped - declared_unit_ids)
    assert not unknown, (
        f"run_units maps collector phases to unit id(s) with no descriptor under "
        f"registry.d/units/: {unknown}. The descriptor is what puts the unit on the "
        "board's denominator; a manifest written under an undeclared id lands where "
        "nothing reads it."
    )


def test_every_phase_collect_call_site_has_a_unit():
    """Two-way: the call sites in the module, graded against the table."""
    source = WEEKLY_COLLECTOR.read_text()
    # `_phase_collect(\n  reg, "name",` — the shape every call site uses.
    called = set(re.findall(r'_phase_collect\(\s*\n\s*reg,\s*"([a-z0-9_]+)"', source))
    assert called, "found no _phase_collect call sites — the scan pattern has drifted"
    declared = {phase for (_mode, phase) in run_units.PHASE_UNITS}
    unmapped = sorted(called - declared)
    assert not unmapped, (
        f"collector phase(s) called in weekly_collector.py with no row in "
        f"run_units.PHASE_UNITS: {unmapped}. Every execution of every unit writes a run "
        "manifest (data_collection_plan_260914.md §2 row 7); a phase with no unit id has "
        "nowhere to write one."
    )
    stale = sorted(declared - called)
    assert not stale, (
        f"run_units.PHASE_UNITS declares phase(s) no longer called in "
        f"weekly_collector.py: {stale}. A stale row makes the table's coverage look "
        "larger than it is."
    )


def test_every_mode_unit_is_dispatched():
    source = " ".join(WEEKLY_COLLECTOR.read_text().split())
    for mode in run_units.MODE_UNITS:
        assert f'_run_whole_mode_unit( "{mode}"' in source or f'_run_whole_mode_unit("{mode}"' in source, (
            f"mode {mode!r} is declared in run_units.MODE_UNITS but is not dispatched "
            "through the run-manifest wrapper in run_weekly"
        )


def test_unit_for_refuses_an_undeclared_phase():
    with pytest.raises(KeyError, match="no declared audit unit"):
        run_units.unit_for("daily", "a_collector_nobody_declared")


def test_a_phase_name_reused_across_modes_resolves_to_different_units():
    """`prices` and `features` each appear in two modes — the mode is half the key."""
    assert run_units.unit_for("phase1", "features").unit_id == "D12"
    assert run_units.unit_for("daily", "features").unit_id == "D31"
    assert run_units.unit_for("phase1", "prices").unit_id == "D03"


def test_trigger_and_log_location_prefer_what_the_box_declared(monkeypatch):
    monkeypatch.delenv(run_units.TRIGGER_ENV, raising=False)
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.delenv("NE_DATA_INSTANCE_TYPE", raising=False)
    assert run_units.resolve_trigger("scheduled") == "scheduled"
    # Off a box the answer names the host and pid — a true statement about where
    # the logs are, rather than a CloudWatch group this run never wrote to.
    assert run_units.resolve_log_location().startswith("local:")

    monkeypatch.setenv(run_units.TRIGGER_ENV, "manual")
    monkeypatch.setenv(run_units.LOG_LOCATION_ENV, "cloudwatch:/alpha-engine/data-spot:s-1")
    assert run_units.resolve_trigger("scheduled") == "manual"
    assert run_units.resolve_log_location().endswith(":s-1")


def test_manifests_go_to_the_bucket_the_writer_identity_already_holds():
    sink = run_units.manifest_sink(run_units.MANIFEST_BUCKET)
    assert sink.bucket == "alpha-engine-research"
    assert sink.prefix == "data_collection/runs"
