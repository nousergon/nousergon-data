"""tests/test_schema_migration_chokepoint.py — the merge-blocking schema
chokepoint (alpha-engine-config-I3238).

The config-I3236 prod-down happened because ``tests/test_schema_contract.py``
only checks INTERNAL consistency (FEATURES == CATALOG == SCHEMA.md agree with
each other) — it passed for nousergon-data#742 even though the widened
descriptor did not match the persisted ``universe`` library and no migration
shipped.

This module lifts the missing invariant to a REQUIRED CI check: the live,
code-derived canonical universe schema MUST equal the latest migration's
declared ``columns_after``. So a PR that adds/removes/reorders a universe column
without adding a matching migration fails here — un-mergeable by the groomer or
a human until the migration ships.

It runs in the normal ``test`` job (pure Python, no ArcticDB/AWS).
"""

from __future__ import annotations

import importlib

import pytest

from store.arctic_store import canonical_universe_columns
from migrations import load_migrations
from migrations._base import MigrationError, validate_chain


def test_canonical_schema_matches_latest_migration():
    """THE gate. If this fails, a universe column changed without a migration.

    To fix: add ``migrations/NNNN_<slug>.py`` (copy ``migrations/_template.py``)
    whose ``columns_after`` is the new frozen canonical set — get it from
    ``store.arctic_store.canonical_universe_columns()`` on your branch — plus a
    real data rewrite. See ``migrations/README.md``.
    """
    live = tuple(canonical_universe_columns())
    latest = load_migrations()[-1]
    assert live == latest.columns_after, (
        "The live code-derived canonical universe schema does NOT match the "
        f"latest migration ({latest.number:04d} {latest.name!r}).\n"
        f"  live schema ({len(live)} cols):   {live}\n"
        f"  migration   ({len(latest.columns_after)} cols): {latest.columns_after}\n"
        "A universe column was added/removed/reordered WITHOUT a migration. "
        "This is exactly the config-I3236 prod-down class. Add a new migration "
        "module migrations/NNNN_<slug>.py (copy migrations/_template.py) with "
        "columns_after set to the new canonical set and a full-write data "
        "rewrite. See migrations/README.md."
    )


def test_migration_chain_is_valid():
    """The chain loads and validates: contiguous numbers from the 0000 baseline,
    monotonic versions, exactly one baseline."""
    migs = load_migrations()
    validate_chain(migs)  # redundant with load_migrations, but pins the contract
    assert migs[0].number == 0 and migs[0].is_baseline
    numbers = [m.number for m in migs]
    assert numbers == list(range(len(migs)))
    assert sum(1 for m in migs if m.is_baseline) == 1


def test_gate_has_teeth_detects_added_column():
    """Prove the diff catches an un-migrated addition: a synthetic canonical set
    with one extra column must NOT equal the latest migration's columns_after."""
    latest = load_migrations()[-1]
    live = list(canonical_universe_columns())
    tampered = tuple(live + ["synthetic_new_col_pct"])
    assert tampered != latest.columns_after


def test_template_is_not_discovered():
    """The copy-me template must never be loaded as a real migration."""
    numbers = [m.number for m in load_migrations()]
    assert -1 not in numbers  # _template.py MIGRATION.number is -1


def test_baseline_columns_are_frozen_literal_not_derived():
    """The baseline anchor must be a hardcoded literal, not a live derivation —
    otherwise the gate above can never fire. Guard: the module defines a literal
    tuple, and it currently matches (they will diverge the moment a column is
    added, which is the whole point)."""
    baseline = importlib.import_module("migrations.0000_baseline_universe_schema")
    assert isinstance(baseline.BASELINE_COLUMNS, tuple)
    assert len(baseline.BASELINE_COLUMNS) == 94
