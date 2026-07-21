"""store/schema_version.py — ArcticDB data-plane schema-version stamp + the
producer-side pre-append assert (alpha-engine-config-I3241).

Motivating incident (config-I3236, 2026-07-21): a schema-additive feature PR
(nousergon-data#742) shipped the *code* half (three new columns) but not the
*data* half (a one-time rewrite of the existing static-schema ``universe``
symbols). The first ``daily_append`` after merge emitted the widened column
set against the old descriptor → 904/904 ``StreamDescriptorMismatch`` → EOD
NAV-reconcile prod-down. The mismatch surfaced two layers downstream as an
opaque ``RuntimeError`` in ``executor/eod_reconcile.py``.

This module is the runtime half of the fix: the ``universe`` data plane now
carries a monotonic integer schema version, and producers assert it matches
what their code emits BEFORE touching any symbol — converting the failure
from a mass mid-write descriptor cascade into a single loud, actionable
pre-flight error that names the pending migration.

Stamp LOCATION — a dedicated ``universe_schema_meta`` library, NOT a reserved
symbol inside ``universe``:

    ``nousergon_lib.arcticdb.get_universe_symbols()`` returns
    ``lib.list_symbols()`` UNFILTERED, and that set is consumed fleet-wide as
    the tradable-ticker roster (executor VWAP/ATR guards, backtester replay,
    predictor inference). A reserved ``_schema_meta`` symbol placed in
    ``universe`` would therefore leak into every consumer's ticker roster as a
    phantom "ticker" — a contract-bypassing cross-module coupling. Isolating
    the stamp in its own library keeps the ``universe`` symbol namespace pure
    (zero consumer changes) at the cost of one extra library-open per producer
    run.

    Failure modes of this choice, and how they are handled:
      * meta library absent/empty (legacy pre-framework state, or a fresh
        bucket) → ``read_schema_version`` returns ``None`` → callers treat the
        universe as being at the BASELINE version (see ``assert_schema_version``).
      * the stamp and the ``universe`` data are written non-atomically during a
        migration → migrations stamp LAST, only after ``verify()`` passes, and
        every migration is idempotent, so a crash between the data rewrite and
        the stamp leaves a re-runnable state (re-run re-verifies, re-stamps).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

#: The single symbol inside the ``universe_schema_meta`` library
#: (``store.arctic_store.SCHEMA_META_LIB`` / ``get_schema_meta_lib``) that
#: carries the version. The library is opened via
#: ``store.arctic_store.get_schema_meta_lib`` — the single mockable open-seam.
SCHEMA_VERSION_SYMBOL = "schema_version"

#: The version an unstamped (legacy / pre-framework) universe library is
#: assumed to be at. It is the baseline migration's ``schema_version_after``
#: (``0000_baseline_universe_schema``). Encoded here as a constant to avoid a
#: ``store -> migrations`` import cycle; the chain-integrity test asserts the
#: baseline migration's number equals this value so the two can never drift.
BASELINE_SCHEMA_VERSION = 0


class SchemaVersionMismatch(RuntimeError):
    """The universe data plane's stamped schema version does not match what
    the running producer code emits. Raised BEFORE any symbol is written, so
    a schema-additive code deploy that lacks its data migration fails loud and
    early instead of cascading as per-symbol ``StreamDescriptorMismatch``."""


def read_schema_version(meta_lib) -> int | None:
    """Return the stamped universe schema version, or ``None`` if unstamped.

    ``None`` means the library predates this framework (legacy) or the bucket
    is fresh — callers MUST map that to :data:`BASELINE_SCHEMA_VERSION` rather
    than treating it as an error (see ``assert_schema_version``). The version
    lives in the symbol's ArcticDB metadata (authoritative) with a mirror in
    the single-cell data frame for human inspection via ``lib.read``.
    """
    try:
        has = meta_lib.has_symbol(SCHEMA_VERSION_SYMBOL)
    except Exception as exc:  # pragma: no cover - arcticdb health failure
        raise RuntimeError(
            f"failed to probe universe_schema_meta.{SCHEMA_VERSION_SYMBOL}: {exc}"
        ) from exc
    if not has:
        return None
    item = meta_lib.read(SCHEMA_VERSION_SYMBOL)
    meta = item.metadata or {}
    if "schema_version" in meta:
        return int(meta["schema_version"])
    # Fallback to the data mirror if metadata was ever written without the key.
    return int(item.data["schema_version"].iloc[-1])


def write_schema_version(
    meta_lib,
    version: int,
    *,
    migration_number: int,
    columns_after: tuple[str, ...] | list[str],
) -> None:
    """Stamp the universe data plane at ``version`` (called by a migration,
    LAST, only after its ``verify()`` passes).

    The stamp records the version, the migration that set it, an applied-at
    UTC timestamp, and the full declared column set — enough for an operator
    or the (config-I3242) runner to audit what a library conforms to without
    reading a ticker symbol.
    """
    applied = datetime.now(timezone.utc).isoformat()
    cols = list(columns_after)
    metadata = {
        "schema_version": int(version),
        "migration_number": int(migration_number),
        "applied_utc": applied,
        "columns": cols,
        "n_columns": len(cols),
    }
    frame = pd.DataFrame(
        {"schema_version": [int(version)], "migration_number": [int(migration_number)]},
        index=pd.DatetimeIndex([pd.Timestamp(applied)], name="applied_utc"),
    )
    # write (not update): the stamp is a single logical fact, always fully
    # overwritten; prune old versions so the meta symbol stays a point value.
    meta_lib.write(
        SCHEMA_VERSION_SYMBOL, frame, metadata=metadata, prune_previous_versions=True
    )
    log.info(
        "Stamped universe_schema_meta.%s: schema_version=%d (migration %04d, %d columns)",
        SCHEMA_VERSION_SYMBOL,
        version,
        migration_number,
        len(cols),
    )


def assert_schema_version(
    meta_lib,
    expected_version: int,
    *,
    pending_migrations: list[int] | None = None,
) -> int:
    """Fail loud unless the universe data plane is at ``expected_version``.

    Called by every universe PRODUCER (``daily_append`` and, through it, the
    weekly collector) immediately after opening the library and BEFORE any
    write. Returns the effective version on success.

    Semantics — an unstamped library is treated as :data:`BASELINE_SCHEMA_VERSION`,
    NOT as an error, so merging this framework onto a live-but-unstamped bucket
    does not itself brick the pipeline (the live universe already conforms to
    the baseline schema; the stamp is bootstrapped when baseline migration 0000
    runs in-region). Both directions of mismatch raise:

      * effective < expected  → a schema-additive code deploy landed without
        its data migration. Names the pending migration(s) to run in-region.
        This is the config-I3236 failure, now caught pre-write.
      * effective > expected  → the running producer code is STALE / rolled
        back relative to applied migrations (it would emit fewer columns than
        persisted → ``StreamDescriptorMismatch`` on write). Do NOT downgrade
        the library; update the producer code.
    """
    stamp = read_schema_version(meta_lib)
    effective = BASELINE_SCHEMA_VERSION if stamp is None else stamp

    if effective == expected_version:
        if stamp is None:
            log.info(
                "universe schema stamp absent — treating library as baseline "
                "v%d (matches producer). Run migration 0000 in-region to "
                "materialize the stamp.",
                BASELINE_SCHEMA_VERSION,
            )
        return effective

    if effective < expected_version:
        pend = pending_migrations or list(range(effective + 1, expected_version + 1))
        raise SchemaVersionMismatch(
            f"universe data plane is at schema v{effective} but this producer "
            f"emits schema v{expected_version}: a schema-additive change was "
            f"deployed WITHOUT its data migration. Pending migration(s) "
            f"{['%04d' % n for n in pend]} must be applied in-region (see "
            f"migrations/README.md) before any universe append. Refusing to "
            f"write — this is the config-I3236 failure caught pre-write "
            f"instead of as 904/904 StreamDescriptorMismatch."
        )

    raise SchemaVersionMismatch(
        f"universe data plane is at schema v{effective} but this producer only "
        f"knows schema v{expected_version}: the producer code is STALE / rolled "
        f"back relative to the applied migrations. It would emit fewer columns "
        f"than the persisted descriptor (StreamDescriptorMismatch on write). "
        f"Update the producer code to the current schema; do NOT downgrade the "
        f"library."
    )
