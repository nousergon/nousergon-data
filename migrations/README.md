# ArcticDB schema migrations (`universe` data plane)

Migrations-as-code for the `universe` ArcticDB library, closing the
schema-change gap that caused the 2026-07-21 fleet-wide prod-down
(alpha-engine-config-I3236). Framework: **alpha-engine-config-I3241**.
Merge-blocking chokepoint: **alpha-engine-config-I3238**.

## Why this exists

A schema-changing PR has two halves:

- the **code** (new feature columns in `features/feature_engineer.FEATURES` +
  `features/registry.CATALOG` + `features/SCHEMA.md`) — ships on merge, and
- the **data migration** (a one-time rewrite of the ~900 existing static-schema
  `universe` symbols so their ArcticDB descriptor gains the new columns).

`nousergon-data#742` shipped only the code half. The `universe` symbols are
**static-schema**: the daily append-at-head path (`update_batch`) requires an
identical descriptor, so the first append after merge failed 904/904 with
`StreamDescriptorMismatch`, cascading to an EOD NAV-reconcile prod-down. Per
the config#2459 lesson, **ArcticDB `update()` cannot cross an additive
descriptor change — only a full `write()` rewrite can.** This framework makes
the data half tested, discoverable code that lands with the schema change and
cannot be forgotten.

## The three enforcement legs

1. **Migrations-as-code** (this directory). One numbered module per schema
   change, each a `Migration` (see `_base.py`) with a `run(lib, meta_lib)`
   (idempotent full-write rewrite) and a `verify(lib)` (post-conditions incl. a
   live `update_batch` probe).
2. **Data-plane version stamp** + **producer pre-append assert**
   (`store/schema_version.py`). The `universe` data plane carries a monotonic
   integer schema version; `builders/daily_append` asserts it matches
   `migrations.EXPECTED_SCHEMA_VERSION` **before touching any symbol**, failing
   loud and naming the pending migration instead of cascading mid-write.
3. **CI chokepoint** (`tests/test_schema_migration_chokepoint.py`, config-I3238).
   Diffs the live code-derived canonical schema against the latest migration's
   declared `columns_after`; a schema-additive PR without a matching migration
   fails a **required** check — un-mergeable by the groomer or a human.

## Discovery contract (also used by the config-I3242 in-region runner)

- Each migration is a module `migrations/NNNN_<slug>.py` (`NNNN` = zero-padded
  monotonic int), exposing a module-level `MIGRATION` of type
  `migrations.Migration`.
- Leading-underscore modules (`_base.py`, `_template.py`) are **not** discovered.
- `migrations.load_migrations()` → chain-validated list ordered by `.number`.
- `migrations.pending_migrations(current)` → migrations with `.number > current`.
- A runner discovers pending work mechanically:
  ```python
  from store.schema_version import _open_meta_lib, read_schema_version, BASELINE_SCHEMA_VERSION
  from migrations import pending_migrations
  meta = _open_meta_lib(bucket)
  current = read_schema_version(meta)
  current = BASELINE_SCHEMA_VERSION if current is None else current
  for m in pending_migrations(current):
      m.run(universe_lib, meta)
      m.verify(universe_lib)
  ```

## Version-stamp location and its failure modes

The stamp lives in a **dedicated `universe_schema_meta` ArcticDB library**
(symbol `schema_version`), **not** as a reserved symbol inside `universe`.
Reason: `nousergon_lib.arcticdb.get_universe_symbols()` returns
`lib.list_symbols()` **unfiltered**, and that set is the fleet-wide tradable
ticker roster (executor, backtester, predictor). A reserved symbol inside
`universe` would leak into every consumer as a phantom ticker. Failure modes:

| Situation | Behavior |
|---|---|
| Meta library absent/empty (legacy or fresh bucket) | `read_schema_version` → `None`; producers treat the library as **baseline v0** (it already conforms), so merging this framework does not brick a live-but-unstamped pipeline. The stamp is materialized when migration `0000` runs in-region. |
| Crash between the data rewrite and the stamp | Migrations stamp **last**, only after `verify()` passes, and are idempotent — re-running re-verifies and re-stamps. |
| Producer code older than the applied migrations | `assert_schema_version` raises (effective > expected) — the producer would emit fewer columns than persisted. Fix the code; never downgrade the library. |

`EXPECTED_SCHEMA_VERSION` is **derived** from the chain (`max(number)`), never
hand-kept — the Dockerfile-duplicate-pin drift bug class.

## Authoring a migration

Copy `_template.py` → `migrations/NNNN_<slug>.py` and follow its header. For a
purely additive column-add the shared `_base.rewrite_symbols_full` /
`verify_additive` helpers already encode the full-write-not-`update()` rule.
The **backfill policy** for history rows of the new columns (NaN vs.
retro-computed) is a reviewed decision recorded on the migration and in the PR.

The one-time run against **live** data is executed **in-region** (write-heavy
ArcticDB rule — never from a laptop) by the runner (config-I3242).

## Baseline (`0000`)

`0000_baseline_universe_schema.py` freezes the 94-column canonical schema as of
the config-I3236 revert (the known-good schema **without** the reverted
`sub_sector_vs_benchmark_*` columns) as version 0. It is the chokepoint's anchor
and the chain's root; it performs **no** data rewrite (the live universe already
is the baseline) — its `run()` only stamps after asserting conformance.
