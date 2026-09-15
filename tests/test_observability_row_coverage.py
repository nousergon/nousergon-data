"""Every write site resolves to an observability row, and back (P-08).

`alpha-engine-config-I10775` (plan §4.1 item P-08): closes audit gap 5 — 43 of
46 units carried no observability row of their own. `scripts/
gen_observability_rows.py` GENERATES one `registry.d/<component_id>.yaml` per
unit descriptor rather than hand-listing them (`observability-policy` §2.2);
this is the coverage check that fails when a write site has no row, mirroring
`test_every_audit_cell_is_a_clause.py`'s "a hand-maintained list drifts
invisibly" concern one layer down — for the observability rows specifically,
not the clause ladder.

Three things are graded:

1. **Generated == committed.** `gen_observability_rows.py check` is clean: what
   is on disk under `registry.d/data-collector-*.yaml` is exactly what the
   generator would produce from the current unit descriptors. A unit
   descriptor edited without regenerating its row is the drift this test
   exists to catch.
2. **Every unit has a row — generated or externally owned.** Every one of the
   46 (minus the D39 external-row carve-out) resolves to a generated
   `registry.d/<component_id>.yaml`. A unit with neither is the exact "obs A"
   gap this issue closes.
3. **Every declared write site resolves to a unit that has a row.** Reuses the
   P-01/P-02 writer inventory (`data_gate/inventory.py`) — the one mechanism
   that notices a write site NOBODY registered a unit for — and adds the
   observability-row bijection on top: a write site can resolve to a unit
   descriptor (P-01's clause) and that unit can still lack an observability
   row, which is exactly the gap P-08 exists to close and P-01/P-02 do not
   check.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import gen_observability_rows as gen  # noqa: E402

from data_gate.descriptors import load_units  # noqa: E402
from data_gate.inventory import load_inventory_scope, scan  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
REGISTRY_D = REPO_ROOT / "registry.d"


def _generated_component_ids() -> set[str]:
    return {p.stem for p in REGISTRY_D.glob("data-collector-*.yaml")}


def test_generator_output_matches_committed_rows():
    """`gen_observability_rows.py check` is clean — no un-regenerated drift."""
    assert gen.main(["check"]) == 0, (
        "registry.d/data-collector-*.yaml is stale relative to registry.d/units/*.yaml — "
        "run `python3 scripts/gen_observability_rows.py generate` and commit the result."
    )


def test_every_unit_has_an_observability_row():
    """Every unit resolves to a generated row, or a declared external one."""
    generated = _generated_component_ids()
    missing: list[str] = []
    for unit in load_units():
        component_id = unit.raw["component_id"]
        if unit.unit_id in gen.EXTERNALLY_OWNED_ROWS:
            continue
        if component_id not in generated:
            missing.append(f"{unit.unit_id} ({component_id})")
    assert not missing, (
        f"{len(missing)} unit(s) with no observability row, generated or external: {missing}"
    )


def test_externally_owned_rows_are_declared_and_exist_in_ops():
    """D39-style carve-outs name a real ops-side row, not a dangling promise.

    This repo cannot read `nous-ergon-ops` (private, different repo) directly,
    so it asserts the weaker but still load-bearing half: the carve-out names
    exactly one unit and one row, and that unit is NOT also present in the
    generated set (no unit is both generated and externally owned).
    """
    generated = _generated_component_ids()
    unit_ids = {u.unit_id for u in load_units()}
    for unit_id, external_component_id in gen.EXTERNALLY_OWNED_ROWS.items():
        assert unit_id in unit_ids, f"EXTERNALLY_OWNED_ROWS names unknown unit {unit_id!r}"
        assert external_component_id not in generated, (
            f"{unit_id}'s external row {external_component_id!r} must not also be a "
            "generated component_id — one unit, one row."
        )


def test_all_trigger_kinds_resolve_to_a_substrate():
    """Every trigger.kind in use has a deliberate SUBSTRATE_BY_TRIGGER_KIND entry.

    Guards the ValueError `gen_observability_rows._substrate` raises for an
    unmapped kind — a new trigger kind silently defaulting to something would
    be the same hand-listed drift this whole module exists to avoid.
    """
    kinds = {(u.raw.get("trigger") or {}).get("kind") for u in load_units()}
    unmapped = kinds - set(gen.SUBSTRATE_BY_TRIGGER_KIND)
    assert not unmapped, f"trigger kinds with no substrate mapping: {unmapped}"


def test_all_unit_lifecycles_resolve_to_an_observability_lifecycle():
    lifecycles = {u.raw.get("lifecycle") for u in load_units()}
    unmapped = lifecycles - set(gen.LIFECYCLE_BY_UNIT_LIFECYCLE)
    assert not unmapped, f"unit lifecycles with no observability-lifecycle mapping: {unmapped}"


def test_generated_rows_carry_the_required_observability_fields():
    """Mirrors `observability_registry.py::REQUIRED_FIELDS` without importing
    across repos: this repo cannot import `nous-ergon-ops`'s script, so the
    field list is restated here, deliberately, as a smaller closed set that
    fails loud if it and the real schema diverge (caught downstream by
    `observability_registry.py validate --gathered`, exercised in CI on the
    companion ops PR and manually confirmed at review time for this change).
    """
    required_top = {
        "component_id",
        "owning_repo",
        "substrate",
        "origin",
        "owner",
        "lifecycle",
        "authority_tier",
        "signals",
        "log_location",
        "alert_channel",
        "severity_source",
        "console_surface",
        "retention",
    }
    signal_classes = {"execution", "cost", "resource", "data", "outcome"}
    signal_values = {"emitted", "absent", "n/a", "unknown"}
    lifecycle_values = {"in-service", "pending", "disabled", "deprecated", "retired"}

    for path in sorted(REGISTRY_D.glob("data-collector-*.yaml")):
        row = yaml.safe_load(path.read_text(encoding="utf-8"))
        missing = required_top - row.keys()
        assert not missing, f"{path.name}: missing required fields {missing}"
        assert row["lifecycle"] in lifecycle_values, f"{path.name}: bad lifecycle {row['lifecycle']!r}"
        signals = row["signals"]
        assert set(signals) == signal_classes, f"{path.name}: signals classes {set(signals)}"
        for cls, block in signals.items():
            assert block["status"] in signal_values, f"{path.name}: signals.{cls}.status invalid"
            assert block.get("reason"), f"{path.name}: signals.{cls} has no reason"


def test_every_declared_write_site_resolves_to_a_unit_with_a_row():
    """The P-08 half of the inventory bijection: write site -> unit -> row.

    `data_gate/inventory.py::scan` already asserts write site -> unit
    descriptor (P-01/P-02's `data.inventory.writers_declared` clause). This
    adds the missing link: a unit found by that scan must ALSO have an
    observability row (generated or externally owned), or a write site is
    effectively unobserved even though a descriptor claims it.
    """
    reading = scan(load_units(), load_inventory_scope())
    if reading.parse_failures:
        pytest.fail(f"writer inventory scan had parse failures: {reading.parse_failures}")
    generated = _generated_component_ids()
    unit_by_id = {u.unit_id: u for u in load_units()}
    uncovered: list[str] = []
    for unit_ids in reading.write_sites.values():
        for unit_id in unit_ids:
            unit = unit_by_id.get(unit_id)
            if unit is None:
                continue  # a dangling id is P-01's own clause's problem, not this test's
            if unit_id in gen.EXTERNALLY_OWNED_ROWS:
                continue
            if unit.raw["component_id"] not in generated:
                uncovered.append(unit_id)
    uncovered = sorted(set(uncovered))
    assert not uncovered, (
        f"write site(s) resolve to unit(s) with no observability row: {uncovered}"
    )
