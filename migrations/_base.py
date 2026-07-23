"""migrations/_base.py — the ArcticDB schema-migration primitives
(alpha-engine-config-I3241).

A schema-changing PR has two halves: the *code* (ships on merge) and the
*data migration* (historically an operator-remembered one-off — no control at
all once the groomer auto-merges). This module makes the data half tested code
that lives beside the schema change and is discoverable/executable mechanically.

Discovery contract (also consumed by the config-I3242 in-region runner):
    Each schema change is one numbered module ``migrations/NNNN_<slug>.py``
    exposing a module-level ``MIGRATION`` of type :class:`Migration`.
    ``migrations.load_migrations()`` returns them ordered by ``.number``;
    ``migrations.pending_migrations(current)`` returns those with
    ``.number > current``. A runner discovers pending work with
    ``pending_migrations(read_schema_version(meta_lib) or BASELINE)``.

The config#2459 lesson is baked into :func:`rewrite_symbols_full`: ArcticDB
``update()`` throws ``StreamDescriptorMismatch`` on an additive schema change
against an existing symbol; only a full ``write()`` rewrite can cross a
descriptor change. Every additive migration therefore FULL-WRITES each symbol
and then :func:`verify_additive` proves the production write primitive
(``update_batch``) succeeds again post-migration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


class MigrationError(RuntimeError):
    """A migration chain is malformed, or a migration's run/verify failed.
    Always raised (never swallowed) — a partial or unverifiable migration is a
    data-integrity failure, not a warning."""


@dataclass(frozen=True)
class Migration:
    """One schema change to the ``universe`` data plane.

    Fields:
      number                — monotonic int, MUST equal the ``NNNN`` filename
                              prefix and ``schema_version_after``.
      name                  — short human slug.
      target_library        — the ArcticDB library the migration rewrites
                              (``"universe"`` for every current case).
      symbol_scope          — human description of which symbols run over
                              (e.g. ``"all universe symbols"``).
      schema_version_before — the version a library must be at to receive this
                              migration; ``None`` only for the baseline (0000).
      schema_version_after  — the version the library is stamped to on success.
      columns_after         — the FROZEN canonical column set the library
                              conforms to AFTER this migration. Frozen (not
                              derived live) so the chokepoint can diff the live
                              code-derived schema against the latest migration's
                              declaration and detect an un-migrated column add.
      backfill_policy       — explicit, human-readable, reviewed at PR time
                              (e.g. "history rows: NaN; not retro-computable").
      run                   — ``run(lib, meta_lib) -> None``; idempotent
                              one-time migration. Stamps LAST.
      verify                — ``verify(lib) -> None``; raises on any failed
                              post-condition (column set, row-count sample,
                              live ``update_batch`` probe).
      is_baseline           — True only for 0000 (the anchor; no data rewrite).
    """

    number: int
    name: str
    target_library: str
    symbol_scope: str
    schema_version_before: int | None
    schema_version_after: int
    columns_after: tuple[str, ...]
    backfill_policy: str
    run: Callable[[Any, Any], None]
    verify: Callable[[Any], None]
    is_baseline: bool = False

    def __post_init__(self) -> None:
        if self.number != self.schema_version_after:
            raise MigrationError(
                f"migration {self.name!r}: number ({self.number}) must equal "
                f"schema_version_after ({self.schema_version_after})"
            )
        if not self.columns_after:
            raise MigrationError(
                f"migration {self.name!r}: columns_after must be non-empty"
            )
        if len(set(self.columns_after)) != len(self.columns_after):
            raise MigrationError(
                f"migration {self.name!r}: columns_after has duplicate columns"
            )


def validate_chain(migrations: list[Migration]) -> None:
    """Fail loud unless the migration list forms a contiguous, monotonic chain
    anchored at the baseline (0000). Called by ``load_migrations``."""
    if not migrations:
        raise MigrationError(
            "no migrations discovered — the baseline (0000) migration is "
            "mandatory as the schema anchor"
        )
    first = migrations[0]
    if first.number != 0 or not first.is_baseline:
        raise MigrationError(
            f"the first migration must be the baseline (number 0, "
            f"is_baseline=True); got number={first.number}, "
            f"is_baseline={first.is_baseline}"
        )
    if first.schema_version_before is not None:
        raise MigrationError(
            "the baseline migration must have schema_version_before=None"
        )
    for i, m in enumerate(migrations):
        if m.number != i:
            raise MigrationError(
                f"migration numbers must be contiguous from 0; expected {i}, "
                f"got {m.number} ({m.name!r}). Gaps/dupes break mechanical "
                f"discovery."
            )
        if i > 0:
            prev = migrations[i - 1]
            if m.is_baseline:
                raise MigrationError(
                    f"only migration 0000 may be a baseline; {m.name!r} is not 0"
                )
            if m.schema_version_before != prev.schema_version_after:
                raise MigrationError(
                    f"migration {m.name!r} schema_version_before "
                    f"({m.schema_version_before}) must equal the previous "
                    f"migration's schema_version_after ({prev.schema_version_after})"
                )


# ── Shared additive-migration mechanism ──────────────────────────────────────
# Reused by real additive migrations (0001+) and by the CI harness's synthetic
# migration. The config#2459 full-write-not-update rule lives here so no
# migration author can reintroduce the update()-across-descriptor bug.


def rewrite_symbols_full(
    lib,
    *,
    expected_columns: tuple[str, ...],
    new_columns: dict[str, Any] | None = None,
    new_columns_fn: Callable[[str, pd.DataFrame], dict[str, Any]] | None = None,
    project: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> int:
    """FULL-WRITE every symbol in ``lib`` to the ``expected_columns`` schema.

    This is the ONLY descriptor-crossing primitive (config#2459): each symbol's
    complete history is read, widened with ``new_columns``/``new_columns_fn``,
    re-projected to the canonical order via ``project`` (defaults to
    ``store.arctic_store.to_arctic_canonical``), then written with
    ``write_batch(prune_previous_versions=True)``. ``update()`` is deliberately
    NOT used — it cannot evolve a static-schema descriptor.

    ``new_columns``: name -> uniform fill value (e.g. ``np.float32("nan")`` for
    a not-retro-computable additive column) — applied identically to every
    symbol.

    ``new_columns_fn``: ``(symbol, df) -> {name: value_or_series}`` — called
    per-symbol AFTER ``new_columns`` is applied, for a RECOMPUTE backfill
    policy where the new column's history is retro-computed from real data
    rather than filled. ``df`` is the symbol's full pre-migration history
    (its existing persisted columns, e.g. ``sector_vs_spy_*``, are readable
    for reuse); a returned ``pd.Series`` is assigned by index-alignment, a
    scalar is broadcast. Only columns not already present in ``df`` are set,
    same precedence as ``new_columns``.

    Idempotent: re-running against already-migrated symbols re-derives the same
    canonical frame and rewrites it in place. Returns the number of symbols
    rewritten. Raises (fail-loud) if any symbol does not end at
    ``expected_columns``.
    """
    from arcticdb.version_store.library import WritePayload

    if project is None:
        from store.arctic_store import to_arctic_canonical as project  # type: ignore

    symbols = list(lib.list_symbols())
    payloads = []
    for sym in symbols:
        df = lib.read(sym).data
        if new_columns:
            for col, fill in new_columns.items():
                if col not in df.columns:
                    # ``np.full`` preserves the fill scalar's dtype, so an
                    # author who passes a TYPED fill (e.g. ``np.float32("nan")``
                    # for a float32 feature column) lands the column at exactly
                    # the dtype the producer emits. A bare Python/``np.nan``
                    # scalar would upcast to float64 and re-introduce a
                    # descriptor mismatch on the next real ``update_batch``
                    # (whose feature columns are float32) — the exact trap this
                    # dtype discipline prevents.
                    df[col] = np.full(len(df), fill)
        if new_columns_fn:
            computed = new_columns_fn(sym, df)
            for col, value in computed.items():
                if col not in df.columns:
                    df[col] = value
        canonical = project(df)
        got = tuple(canonical.columns)
        if got != expected_columns:
            raise MigrationError(
                f"symbol {sym!r}: post-projection columns {got} != declared "
                f"columns_after {expected_columns} — migration would persist a "
                f"non-conforming descriptor. Aborting (fail-loud)."
            )
        payloads.append(WritePayload(symbol=sym, data=canonical))
    if payloads:
        results = lib.write_batch(payloads, prune_previous_versions=True)
        errs = [r for r in results if "Error" in type(r).__name__]
        if errs:
            raise MigrationError(
                f"write_batch reported {len(errs)} per-symbol error(s) during "
                f"migration rewrite: {errs[:3]!r} ..."
            )
    return len(payloads)


def verify_additive(
    lib,
    *,
    expected_columns: tuple[str, ...],
    project: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    sample: int = 5,
) -> None:
    """Post-migration verification (fail-loud). Asserts, on up to ``sample``
    symbols: (1) the persisted column set equals ``expected_columns`` in order;
    (2) row-count is preserved (non-empty); and (3) the PRODUCTION write
    primitive ``update_batch`` — the config#2459 lesson — now appends a
    next-day row cleanly (no ``StreamDescriptorMismatch``). The probe row is
    removed afterward so verification leaves the data unchanged."""
    from arcticdb.version_store.library import UpdatePayload

    if project is None:
        from store.arctic_store import to_arctic_canonical as project  # type: ignore

    symbols = list(lib.list_symbols())
    if not symbols:
        raise MigrationError(
            "verify_additive: no symbols present to verify — a migration that "
            "silently ran over an empty library is suspicious; investigate."
        )
    for sym in symbols[:sample]:
        item = lib.read(sym)
        got = tuple(item.data.columns)
        if got != expected_columns:
            raise MigrationError(
                f"verify: symbol {sym!r} persisted columns {got} != "
                f"expected {expected_columns}"
            )
        if len(item.data) == 0:
            raise MigrationError(f"verify: symbol {sym!r} lost all rows")

        # Live update_batch probe — the exact production append primitive.
        last = pd.Timestamp(item.data.index.max())
        probe_ts = last + pd.Timedelta(days=1)
        probe = item.data.iloc[[-1]].copy()
        probe.index = pd.DatetimeIndex([probe_ts], name=item.data.index.name)
        res = lib.update_batch(
            [UpdatePayload(symbol=sym, data=project(probe))], upsert=True
        )
        if any("StreamDescriptorMismatch" in str(r) for r in res):
            raise MigrationError(
                f"verify: update_batch probe on {sym!r} STILL returns "
                f"StreamDescriptorMismatch after migration — the rewrite did "
                f"not evolve the descriptor (config#2459 regression): {res!r}"
            )
        # Remove the probe row so verification is side-effect-free: rewrite the
        # symbol back to its pre-probe history.
        healed = project(item.data)
        from arcticdb.version_store.library import WritePayload

        lib.write_batch(
            [WritePayload(symbol=sym, data=healed)], prune_previous_versions=True
        )
