"""Seed a shadow run's ArcticDB libraries from live state, read-only against live.

`alpha-engine-config-I10866`. The mechanism, its rationale and its time budget
are stated in ``shadow/root.py``'s module docstring ("ArcticDB reads"); this
module is the implementation, and ``tests/test_shadow_root.py`` is where its
properties are asserted.

In one paragraph: on the FIRST open of any shadow library under an active
shadow root, every live library in ``LIVE_ARCTIC_LIBRARIES`` is copied into its
``shadow_{YYYYMMDD}_<name>`` twin, bounded to rows strictly BEFORE the shadow
trading day. The data libraries are copied first and verified. The live
schema-version stamp is copied after them, verbatim, and is never
synthesised. A seed manifest is committed LAST. The manifest is the only thing
that marks a stamp as seeded: a crash anywhere before it leaves an unseeded
stamp, and the next open discards the partial shadow libraries and re-seeds
from scratch.

Fail loud. A symbol that cannot be read, a symbol whose history cannot be
bounded to the trading day, a batch write that returns a ``DataError``, a
verification mismatch, and an exhausted time budget each raise
``ShadowSeedError`` and kill the run. A partially seeded shadow universe would
produce a parity report computed over the wrong history, and it would be read
as evidence.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import threading
import time
from typing import Any, Callable

import pandas as pd

from shadow.root import LIVE_ARCTIC_LIBRARIES, ShadowGuardViolation, ShadowRoot

log = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_SEED_BUDGET_SECONDS",
    "ENV_SEED_BUDGET_SECONDS",
    "MANIFEST_LIBRARY",
    "MANIFEST_SYMBOL",
    "SCHEMA_META_LIBRARY",
    "SEEDED_DATA_LIBRARIES",
    "ShadowSeedError",
    "ensure_seeded",
    "read_manifest",
    "seed_cutoff",
]

#: Libraries holding time-series data, copied (bounded by the cutoff) before
#: the stamp.
SEEDED_DATA_LIBRARIES: tuple[str, ...] = ("universe", "macro", "delisted_history")

#: The stamp library. It is copied AFTER the data libraries have been verified,
#: and copied verbatim. The shadow stamp is valid only because the shadow data
#: really is a copy of live data at that version.
SCHEMA_META_LIBRARY = "universe_schema_meta"

#: Under the shadow prefix, ``shadow_{YYYYMMDD}_seed_manifest``. It is never a
#: live library.
MANIFEST_LIBRARY = "seed_manifest"
MANIFEST_SYMBOL = "seed"

#: Symbols per read_batch/write_batch round. About 50 × 1.5 MB per universe
#: symbol keeps peak memory near 75 MB, and ArcticDB still parallelises the
#: S3 I/O inside each batch.
SEED_CHUNK_SIZE = 50

ENV_SEED_BUDGET_SECONDS = "DATA_COLLECTION_SHADOW_SEED_BUDGET_SECONDS"
#: 45 minutes. The shadow/root.py docstring bounds a whole-universe copy at
#: about 30 minutes in-region, even assuming the copy were sequential. A seed
#: that exceeds this raises instead of being killed silently by the box's
#: runtime cap.
DEFAULT_SEED_BUDGET_SECONDS = 2700

# The class guard. A library added to LIVE_ARCTIC_LIBRARIES without a seeding
# rule would open EMPTY under a shadow root. That is the exact defect behind
# I10866 (universe_schema_meta read as v0), so it refuses at import rather
# than at the first shadow run.
_COVERED = frozenset(SEEDED_DATA_LIBRARIES) | {SCHEMA_META_LIBRARY}
if _COVERED != LIVE_ARCTIC_LIBRARIES:
    raise ShadowGuardViolation(
        f"shadow seeding covers {sorted(_COVERED)} but LIVE_ARCTIC_LIBRARIES is "
        f"{sorted(LIVE_ARCTIC_LIBRARIES)} — every live library needs a seeding rule, or a "
        "shadow run reads it empty (alpha-engine-config-I10866)"
    )

_LOCK = threading.Lock()

#: The only Library methods seeding may call on a LIVE handle. Everything else,
#: write/update/append/delete/snapshot included, raises.
_LIVE_READ_METHODS = frozenset(
    {"list_symbols", "read_batch", "read", "has_symbol", "options", "get_description_batch"}
)


class ShadowSeedError(RuntimeError):
    """The shadow libraries could not be seeded faithfully. Always fatal."""


class _ReadOnlyLibrary:
    """A live library handle that can only be read.

    This is what makes "seeding never writes live" structural rather than a
    matter of care. The seeding code never holds a writable handle to a live
    name.
    """

    def __init__(self, name: str, library: Any) -> None:
        self._name = name
        self._library = library

    def __getattr__(self, attr: str) -> Any:
        if attr not in _LIVE_READ_METHODS:
            raise ShadowGuardViolation(
                f"shadow seeding attempted {attr!r} on the LIVE ArcticDB library "
                f"{self._name!r}; only {sorted(_LIVE_READ_METHODS)} are permitted"
            )
        return getattr(self._library, attr)


def seed_cutoff(root: ShadowRoot) -> pd.Timestamp:
    """The last instant copied: one nanosecond before the shadow trading day.

    "As of just before the shadow trading day" (I10866 deliverable 1). By the
    time a shadow run for a past trading day starts, live v1 has usually
    already appended that day and later ones. Copying them would let the
    shadow append overwrite a row instead of producing it.
    """
    return pd.Timestamp(root.trading_day) - pd.Timedelta(1, "ns")


def _shadow_name(root: ShadowRoot, name: str) -> str:
    shadowed = root.arctic_library(name)
    if not shadowed.startswith(root.arctic_prefix) or shadowed in LIVE_ARCTIC_LIBRARIES:
        raise ShadowGuardViolation(
            f"refusing to seed into {shadowed!r}: not under the shadow prefix {root.arctic_prefix!r}"
        )
    return shadowed


def _live_handle(arctic: Any, name: str, *, required: bool) -> "_ReadOnlyLibrary | None":
    if name not in LIVE_ARCTIC_LIBRARIES:
        raise ShadowGuardViolation(f"{name!r} is not a live ArcticDB library; nothing to seed from")
    if not arctic.has_library(name):
        if required:
            raise ShadowSeedError(
                f"live ArcticDB library {name!r} does not exist, so a shadow run cannot read "
                "the state the live run reads. Refusing to seed an empty shadow."
            )
        return None
    return _ReadOnlyLibrary(name, arctic.get_library(name, create_if_missing=False))


def _create_shadow(arctic: Any, root: ShadowRoot, name: str, library_options: Any) -> Any:
    shadowed = _shadow_name(root, name)
    if arctic.has_library(shadowed):
        raise ShadowSeedError(
            f"{shadowed!r} still exists after the pre-seed reset; refusing to copy into a "
            "library whose contents this seed did not produce"
        )
    return arctic.get_library(shadowed, create_if_missing=True, library_options=library_options)


def _data_error_type() -> type:
    from arcticdb_ext.version_store import DataError

    return DataError


def _copy_library(
    live: _ReadOnlyLibrary,
    shadow: Any,
    *,
    name: str,
    cutoff: pd.Timestamp,
    deadline: float,
    clock: Callable[[], float],
    chunk_size: int,
) -> dict:
    from arcticdb.version_store.library import ReadRequest, WritePayload

    data_error = _data_error_type()
    symbols = sorted(live.list_symbols())
    copied_rows: dict[str, int] = {}
    absent_before_cutoff: list[str] = []
    for start in range(0, len(symbols), chunk_size):
        _check_deadline(deadline, clock, where=f"{name} chunk {start // chunk_size}")
        chunk = symbols[start : start + chunk_size]
        results = live.read_batch([ReadRequest(symbol=s, date_range=(None, cutoff)) for s in chunk])
        payloads = []
        for symbol, result in zip(chunk, results):
            if isinstance(result, data_error):
                raise ShadowSeedError(
                    f"could not read live {name}.{symbol} bounded to {cutoff} — a symbol the "
                    f"shadow run cannot see as of the trading day makes its parity meaningless: "
                    f"{result}"
                )
            if len(result.data) == 0:
                # Not a swallow. The symbol had no rows before the trading day,
                # so as of the cutoff it did not exist, and the faithful copy
                # omits it. The manifest records every such symbol.
                absent_before_cutoff.append(symbol)
                continue
            payloads.append(WritePayload(symbol=symbol, data=result.data, metadata=result.metadata))
            copied_rows[symbol] = len(result.data)
        if payloads:
            written = shadow.write_batch(payloads, prune_previous_versions=True)
            failures = [str(w) for w in written if isinstance(w, data_error)]
            if failures:
                raise ShadowSeedError(
                    f"{len(failures)} shadow write(s) into {name} failed: {failures[:3]}"
                )

    present = set(shadow.list_symbols())
    if present != set(copied_rows):
        raise ShadowSeedError(
            f"shadow {name} verification failed: missing {sorted(set(copied_rows) - present)[:5]}, "
            f"unexpected {sorted(present - set(copied_rows))[:5]}"
        )
    for start in range(0, len(copied_rows), chunk_size):
        chunk = sorted(copied_rows)[start : start + chunk_size]
        for symbol, description in zip(chunk, shadow.get_description_batch(chunk)):
            if isinstance(description, data_error) or description.row_count != copied_rows[symbol]:
                raise ShadowSeedError(
                    f"shadow {name}.{symbol} verification failed: expected "
                    f"{copied_rows[symbol]} rows, found {description}"
                )
    return {
        "symbols": len(copied_rows),
        "rows": int(sum(copied_rows.values())),
        "absent_before_cutoff": absent_before_cutoff,
    }


def _check_deadline(deadline: float, clock: Callable[[], float], *, where: str) -> None:
    if clock() > deadline:
        raise ShadowSeedError(
            f"shadow ArcticDB seed exceeded its time budget at {where} "
            f"({ENV_SEED_BUDGET_SECONDS}); refusing to continue with a partial seed"
        )


def _budget_seconds(env: "dict[str, str] | None") -> float:
    source = os.environ if env is None else env
    raw = (source.get(ENV_SEED_BUDGET_SECONDS) or "").strip()
    if not raw:
        return float(DEFAULT_SEED_BUDGET_SECONDS)
    try:
        value = float(raw)
    except ValueError as exc:
        raise ShadowSeedError(f"{ENV_SEED_BUDGET_SECONDS}={raw!r} is not a number") from exc
    if value <= 0:
        raise ShadowSeedError(f"{ENV_SEED_BUDGET_SECONDS}={raw!r} must be positive")
    return value


def read_manifest(arctic: Any, root: ShadowRoot) -> "dict | None":
    """The committed seed manifest for ``root``, or ``None`` when unseeded."""
    manifest_name = _shadow_name(root, MANIFEST_LIBRARY)
    if not arctic.has_library(manifest_name):
        return None
    library = arctic.get_library(manifest_name, create_if_missing=False)
    if not library.has_symbol(MANIFEST_SYMBOL):
        return None
    return dict(library.read(MANIFEST_SYMBOL).metadata or {})


def ensure_seeded(
    arctic: Any,
    root: ShadowRoot,
    *,
    clock: Callable[[], float] = time.monotonic,
    env: "dict[str, str] | None" = None,
    chunk_size: int = SEED_CHUNK_SIZE,
) -> dict:
    """Seed every shadow library for ``root`` from live, once per stamp.

    Returns the manifest. Idempotent: once a manifest is committed, a call is
    two metadata reads.
    """
    if root is None:
        raise ShadowGuardViolation("ensure_seeded called with no active shadow root")
    with _LOCK:
        manifest = read_manifest(arctic, root)
        if manifest is not None:
            return manifest

        started = clock()
        deadline = started + _budget_seconds(env)
        cutoff = seed_cutoff(root)
        log.warning(
            "shadow: seeding ArcticDB libraries %s for trading day %s from live, rows <= %s",
            sorted(LIVE_ARCTIC_LIBRARIES), root.trading_day, cutoff,
        )

        # No manifest means whatever exists under this stamp was not produced
        # by a completed seed. That covers a crashed seed, and also the empty
        # libraries the pre-I10866 code created on 2026-09-15. Discard all of
        # it, so the copy starts from nothing and its verification is exact.
        for name in (*LIVE_ARCTIC_LIBRARIES, MANIFEST_LIBRARY):
            shadowed = _shadow_name(root, name)
            if arctic.has_library(shadowed):
                log.warning("shadow: discarding unseeded library %s before seeding", shadowed)
                arctic.delete_library(shadowed)

        libraries: dict[str, dict] = {}
        for name in SEEDED_DATA_LIBRARIES:
            live = _live_handle(arctic, name, required=True)
            shadow = _create_shadow(arctic, root, name, live.options())
            libraries[name] = _copy_library(
                live, shadow, name=name, cutoff=cutoff, deadline=deadline,
                clock=clock, chunk_size=chunk_size,
            )
            log.warning(
                "shadow: seeded %s — %d symbols, %d rows, %d absent before cutoff",
                _shadow_name(root, name), libraries[name]["symbols"], libraries[name]["rows"],
                len(libraries[name]["absent_before_cutoff"]),
            )

        _check_deadline(deadline, clock, where="schema stamp")
        schema_version = _copy_schema_stamp(arctic, root)

        manifest = {
            "trading_day": root.trading_day.isoformat(),
            "cutoff": cutoff.isoformat(),
            "schema_version": schema_version,
            "libraries": libraries,
            "seeded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "duration_seconds": round(clock() - started, 3),
            "issue": "alpha-engine-config-I10866",
        }
        manifest_lib = arctic.get_library(_shadow_name(root, MANIFEST_LIBRARY), create_if_missing=True)
        manifest_lib.write(
            MANIFEST_SYMBOL,
            pd.DataFrame({"schema_version": [-1 if schema_version is None else schema_version]}),
            metadata=manifest,
            prune_previous_versions=True,
        )
        log.warning("shadow: seed manifest committed for %s", root.trading_day)
        return manifest


def _copy_schema_stamp(arctic: Any, root: ShadowRoot) -> "int | None":
    """Copy the live stamp verbatim; never write a version the live stamp lacks.

    If live is unstamped, the shadow library is created empty. Both then read
    as baseline v0 through the same ``read_schema_version`` path, so the
    pre-append assert treats shadow exactly as it treats live.
    """
    from store.schema_version import SCHEMA_VERSION_SYMBOL, read_schema_version

    live = _live_handle(arctic, SCHEMA_META_LIBRARY, required=False)
    options = live.options() if live is not None else None
    shadow = _create_shadow(arctic, root, SCHEMA_META_LIBRARY, options)
    if live is None or not live.has_symbol(SCHEMA_VERSION_SYMBOL):
        return None
    live_version = read_schema_version(live)
    item = live.read(SCHEMA_VERSION_SYMBOL)
    shadow.write(SCHEMA_VERSION_SYMBOL, item.data, metadata=item.metadata, prune_previous_versions=True)
    copied = read_schema_version(shadow)
    if copied != live_version:
        raise ShadowSeedError(
            f"shadow schema stamp reads v{copied} after copying live v{live_version}"
        )
    return live_version
