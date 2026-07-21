"""migrations — ArcticDB schema-migration framework for the ``universe`` data
plane (alpha-engine-config-I3241, structural fix for the config-I3236 prod-down).

Public API:
    load_migrations()                 -> list[Migration]   (chain-validated)
    latest_version()                  -> int
    EXPECTED_SCHEMA_VERSION           : int   (== latest_version(), import-time)
    pending_migrations(current)       -> list[Migration]
    get_migration(number)             -> Migration
    assert_universe_schema_current(bucket=None)            (producer pre-append)

Discovery contract (also for the config-I3242 in-region runner): numbered
modules ``migrations/NNNN_<slug>.py`` each expose a module-level ``MIGRATION``
of type :class:`Migration`. See ``migrations/README.md``.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
import re

from migrations._base import Migration, MigrationError, validate_chain

log = logging.getLogger(__name__)

# ``NNNN_<slug>`` — the discovered-module naming rule. Leading-underscore
# helpers (``_base``, ``_template``) are excluded by this pattern, so the
# copy-me template is never mistaken for a real migration.
_MODULE_RE = re.compile(r"^(\d{4})_[a-z0-9_]+$")


def _discover_modules() -> list[str]:
    names: list[str] = []
    for mod in pkgutil.iter_modules(__path__):
        if _MODULE_RE.match(mod.name):
            names.append(mod.name)
    return sorted(names)


def load_migrations() -> list[Migration]:
    """Import every numbered migration module, collect its ``MIGRATION``, sort
    by number, and validate the chain (contiguous from the 0000 baseline,
    monotonic versions). Raises ``MigrationError`` on any malformation."""
    migrations: list[Migration] = []
    for name in _discover_modules():
        module = importlib.import_module(f"migrations.{name}")
        if not hasattr(module, "MIGRATION"):
            raise MigrationError(
                f"migration module {name!r} exposes no module-level MIGRATION "
                f"object (discovery contract violation)"
            )
        mig = module.MIGRATION
        if not isinstance(mig, Migration):
            raise MigrationError(
                f"migration module {name!r}: MIGRATION is {type(mig).__name__}, "
                f"expected migrations.Migration"
            )
        expected_number = int(name[:4])
        if mig.number != expected_number:
            raise MigrationError(
                f"migration module {name!r}: MIGRATION.number ({mig.number}) "
                f"does not match the filename prefix ({expected_number})"
            )
        migrations.append(mig)
    migrations.sort(key=lambda m: m.number)
    validate_chain(migrations)
    return migrations


def latest_version() -> int:
    """The highest declared schema version = the schema the current producer
    code must emit. Derived from the migration chain, never hand-kept (the
    Dockerfile-duplicate-pin bug class)."""
    return load_migrations()[-1].number


def get_migration(number: int) -> Migration:
    for m in load_migrations():
        if m.number == number:
            return m
    raise MigrationError(f"no migration with number {number}")


def pending_migrations(current_version: int) -> list[Migration]:
    """Migrations a library at ``current_version`` still needs, in order.
    Mechanical work-discovery for the in-region runner (config-I3242)."""
    return [m for m in load_migrations() if m.number > current_version]


#: The schema version the producer code in THIS checkout emits. Import-time
#: derivation from the chain — a producer can never drift from its migrations.
EXPECTED_SCHEMA_VERSION: int = latest_version()


def assert_universe_schema_current(meta_lib) -> int:
    """Producer pre-append guard: fail loud (before touching any symbol) unless
    the live universe data plane is stamped at :data:`EXPECTED_SCHEMA_VERSION`.

    Called by ``builders/daily_append`` (and, through it, the weekly collector)
    right after opening the universe library, with the schema-meta library
    opened via ``store.arctic_store.get_schema_meta_lib`` (the single mockable
    S3 open-seam — this function performs no I/O of its own). On mismatch raises
    ``store.schema_version.SchemaVersionMismatch`` naming the pending
    migration(s). Returns the effective version on success.
    """
    from store.schema_version import (
        BASELINE_SCHEMA_VERSION,
        assert_schema_version,
        read_schema_version,
    )

    current = read_schema_version(meta_lib)
    effective = BASELINE_SCHEMA_VERSION if current is None else current
    pend = [m.number for m in pending_migrations(effective)]
    return assert_schema_version(
        meta_lib, EXPECTED_SCHEMA_VERSION, pending_migrations=pend
    )


__all__ = [
    "Migration",
    "MigrationError",
    "load_migrations",
    "latest_version",
    "get_migration",
    "pending_migrations",
    "EXPECTED_SCHEMA_VERSION",
    "assert_universe_schema_current",
]
