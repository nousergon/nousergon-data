"""migrations/_template.py — COPY ME to author a new schema migration.

This module is NOT a real migration: its leading underscore keeps it out of
discovery (``migrations.load_migrations`` only imports ``NNNN_<slug>.py``).

To add a schema change (e.g. re-land the reverted sub-sector features):

  1. Copy this file to ``migrations/NNNN_<slug>.py`` where ``NNNN`` is the next
     integer (zero-padded, e.g. ``0001_add_sub_sector_features.py``).
  2. Set ``number`` == ``schema_version_after`` == that integer, and
     ``schema_version_before`` == the previous migration's ``schema_version_after``.
  3. Set ``columns_after`` to the FULL frozen canonical column set the library
     will conform to AFTER this migration — the previous ``columns_after`` plus
     your new columns, IN CANONICAL ORDER. Get it from
     ``store.arctic_store.canonical_universe_columns()`` on your branch (after
     you add the columns to ``feature_engineer.FEATURES`` + ``registry.CATALOG``
     + ``SCHEMA.md``) and paste the literal here so it is frozen.
  4. Fill ``backfill_policy`` with the reviewed decision for HISTORY rows of the
     new columns (NaN vs. retro-computed) — this is a human/research call made
     at PR time, not a default.
  5. Keep ``run``/``verify`` as below for a purely additive column-add; the
     shared helpers already encode the config#2459 full-write-not-update rule.
  6. Adding the file makes the chokepoint test
     (``tests/test_schema_migration_chokepoint.py``) go green again; the CI
     migration harness proves an additive migration runs against a seeded LMDB
     library. The one-time run against LIVE data is executed IN-REGION by the
     runner (config-I3242) — never from a laptop (write-heavy ArcticDB rule).
"""

from __future__ import annotations

import numpy as np

from migrations._base import (
    Migration,
    rewrite_symbols_full,
    verify_additive,
)

# EDIT: the full frozen canonical column set AFTER this migration.
COLUMNS_AFTER: tuple[str, ...] = (
    # ...previous migration's columns_after...,
    # "my_new_column_pct",
)

# EDIT: the new columns this migration adds, mapped to their HISTORY fill value.
# Use a TYPED fill matching the producer's emitted dtype — feature columns are
# float32, so a not-retro-computable column takes ``np.float32("nan")``. A bare
# ``np.nan`` (float64) would land the column at the wrong dtype and re-introduce
# a StreamDescriptorMismatch on the next real update_batch (config#2459 trap).
NEW_COLUMNS = {
    # "my_new_column_pct": np.float32("nan"),   # not retro-computable -> NaN
}


def _run(lib, meta_lib) -> None:
    from store.schema_version import write_schema_version

    rewrite_symbols_full(
        lib, expected_columns=COLUMNS_AFTER, new_columns=NEW_COLUMNS
    )
    # Stamp LAST, only after the rewrite completes.
    write_schema_version(
        meta_lib,
        MIGRATION.schema_version_after,
        migration_number=MIGRATION.number,
        columns_after=COLUMNS_AFTER,
    )


def _verify(lib) -> None:
    verify_additive(lib, expected_columns=COLUMNS_AFTER)


MIGRATION = Migration(
    number=-1,  # EDIT to NNNN
    name="template_do_not_use",
    target_library="universe",
    symbol_scope="all universe symbols",
    schema_version_before=-1,  # EDIT to previous schema_version_after
    schema_version_after=-1,  # EDIT to NNNN (== number)
    columns_after=COLUMNS_AFTER or ("placeholder",),
    backfill_policy="EDIT: reviewed NaN-vs-recompute decision for history rows",
    run=_run,
    verify=_verify,
)
