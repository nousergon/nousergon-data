"""The in-region ArcticDB comparator (`alpha-engine-config-I10819`).

`shadow.parity` grades every S3 key from the laptop but can never open
ArcticDB — it is unreadable from the laptop entirely (`alpha-engine-config-
I9771`, measured: an explicit S3 Deny blocks even `ne-admin` on
`ListObjectsV2`/`GetBucketPolicy`) and carries its own C++ S3 client the
`shadow.interceptor` boto3 hook cannot see either way. So every ArcticDB
library `shadow.parity.run_parity` declares (`arcticdb/universe`, `arcticdb/
macro`, `arcticdb/delisted_history`, `arcticdb/universe_schema_meta`) is
published with verdict ``in_region_only`` — a row naming the next action
("run this in-region"), never a row this module's caller silently treats as
passing.

**This module IS that next action, and it MUST run in-region** — never on the
laptop, for the same reason `shadow.parity` cannot: it opens
`store.arctic_store._open_library` directly. Invoke it from the data-spot box
(``python -m shadow arctic-parity ...``), not from a laptop shell.

**What is compared, per plan §6.2 step 4 — identical semantics to the S3
side**: symbol set, row count, schema, value tolerance. This reuses
`shadow.parity._compare_frames` rather than re-implementing tolerance/breach
logic a second time, so the two comparators cannot silently diverge on what
"close enough" means.

**Bounded to `trading_day`, never the library's current tail**
(alpha-engine-config-I10819 finding 1). Every read is
``date_range=(trading_day, trading_day)`` on both sides. A shadow library is
seeded from live rows strictly BEFORE `trading_day`
(`shadow/arctic_seed.py::ensure_seeded`) and only gains a `trading_day` row if
THIS run's own append wrote one; live gets v1's own append the *next* evening
regardless of what this run does. Comparing against live's current tail would
silently re-admit the superseded-live defect the day-bound already exists to
avoid on the S3 side (`alpha-engine-config-I10892`).

**`shadow_missing_day`, never a `match` on the seed** (finding 2). A library
where not one symbol has a `trading_day` row in the shadow side, while live
does, means the append this run was supposed to perform did not happen —
D18/D32 published "zero rows via arcticdb.tickers_appended" in the 2026-09-14
shadow run, plausibly the same units-auto-skipped-against-live-phase-markers
cause as I10891, though that root cause is not verifiable from the laptop.
Grading that `match` would validate an untouched seed, not the run; a new
verdict makes the report say which happened rather than folding it into
`mismatch` or `unmeasurable`.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from shadow.parity import (
    DEFAULT_ABSOLUTE_TOLERANCE,
    DEFAULT_RELATIVE_TOLERANCE,
    PASSING_VERDICTS as PARITY_PASSING_VERDICTS,
    _compare_frames,
    parity_key,
)
from shadow.root import LIVE_ARCTIC_LIBRARIES, ShadowRoot

__all__ = [
    "ARCTIC_PARITY_LIBRARIES",
    "ARCTIC_PARITY_UNIT_ID",
    "LibraryPair",
    "LiveUnitNotReady",
    "await_live_units",
    "compare_all",
    "compare_library",
    "latest_live_manifest",
    "open_library_pairs",
    "rewrite_report",
    "run_arctic_parity",
    "write_run_manifest",
]

log = logging.getLogger(__name__)

#: The four libraries `shadow.parity` reports `in_region_only` for. Declared
#: as the sorted form of `shadow.root.LIVE_ARCTIC_LIBRARIES` rather than a
#: second hand-typed list, so the two can never drift on which libraries are
#: "the ArcticDB surface".
ARCTIC_PARITY_LIBRARIES: tuple[str, ...] = tuple(sorted(LIVE_ARCTIC_LIBRARIES))

#: Not a `D##` audit unit — this is a cross-cutting comparator over four
#: units' declared write targets (D13/D14/D18/D32/D34/D35/D42/D43 all resolve
#: to one of `ARCTIC_PARITY_LIBRARIES`), so it does not go through
#: `nousergon_lib.run_manifest.run_unit` (its `unit_id` regex is `D[0-9]{2}[A-
#: Z]?`). `write_run_manifest` below reuses the same schema and sink
#: primitives directly instead.
ARCTIC_PARITY_UNIT_ID = "arctic-parity"

#: `{prefix}/{unit_id}/{trading_day}/{run_id}.json` — issue's own closes-when.
_MANIFEST_PREFIX = "data_collection/runs"

#: The same prefix, relative to the `data_collection` store the report is
#: published to — where `await_live_units` reads v1's manifests from.
_LIVE_RUNS_PREFIX = "runs"

#: Not a verdict: a breakdown of `match` (`shadow.parity.ParityReport.summary`,
#: `data_gate.evidence.SUMMARY_NON_VERDICT_FIELDS`).
_SETTLING_BAR_KEYS = "settling_bar_keys"

_TERMINAL_VERDICTS = frozenset(
    {"match", "mismatch", "shadow_missing_day", "both_missing", "unmeasurable"}
)


@dataclass(frozen=True)
class LibraryPair:
    """One library's already-opened live + shadow handles."""

    name: str
    live: Any
    shadow: Any


def open_library_pairs(
    trading_day: dt.date, bucket: str, libraries: Iterable[str] = ARCTIC_PARITY_LIBRARIES
) -> list[LibraryPair]:
    """Open every declared library's live + shadow pair.

    Routed through `store.arctic_store._open_library` — the repo's one
    ArcticDB open chokepoint (`tests/test_shadow_parity.py::
    test_arctic_store_routes_every_library_open_through_the_shadow_helper`
    enforces every open goes through it) — so this reader can never diverge
    from how every writer opens the same libraries. `create_if_missing=False`
    on both sides: a comparator that silently creates an empty library it
    meant only to read would grade that empty library `both_missing` at best
    and mask a real open failure at worst.
    """
    from store.arctic_store import _open_library

    root = ShadowRoot(trading_day)
    pairs: list[LibraryPair] = []
    for name in libraries:
        live = _open_library(name, bucket, create_if_missing=False)
        shadow = _open_library(root.arctic_library(name), bucket, create_if_missing=False)
        pairs.append(LibraryPair(name, live, shadow))
    return pairs


def _day_frame(lib: Any, symbols: list[str], trading_day: dt.date):
    """One row per symbol that HAS a `trading_day` row, indexed by symbol.

    Bounded read (`date_range=(trading_day, trading_day)`) on both call
    sites — this is what makes finding 1 (never compare against the live
    tail) structural rather than a convention callers must remember.

    Never a partial read: a `DataError` anywhere in the batch raises rather
    than being dropped, silently shrinking the symbol set being compared.
    """
    import pandas as pd
    from arcticdb.version_store.library import ReadRequest

    if not symbols:
        return pd.DataFrame(columns=["symbol"]).set_index("symbol")

    ts = pd.Timestamp(trading_day)
    requests = [ReadRequest(symbol=s, date_range=(ts, ts)) for s in symbols]
    results = lib.read_batch(requests)

    try:
        import arcticdb as adb

        data_error_cls: type | tuple = getattr(adb, "DataError", ())
    except ImportError:  # pragma: no cover - arcticdb is a hard dependency here
        data_error_cls = ()

    rows = []
    for symbol, result in zip(symbols, results):
        if data_error_cls and isinstance(result, data_error_cls):
            raise RuntimeError(
                f"arctic_parity: read_batch({trading_day.isoformat()}) failed for "
                f"{symbol!r}: {getattr(result, 'exception_string', None) or result}"
            )
        frame = getattr(result, "data", result)
        if frame is None or len(frame) == 0:
            continue
        row = frame.iloc[[-1]].copy()
        row["symbol"] = symbol
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["symbol"]).set_index("symbol")
    combined = pd.concat(rows, ignore_index=True)
    return combined.set_index("symbol")


def compare_library(
    name: str,
    live_lib: Any,
    shadow_lib: Any,
    trading_day: dt.date,
    *,
    rel: float = DEFAULT_RELATIVE_TOLERANCE,
    absolute: float = DEFAULT_ABSOLUTE_TOLERANCE,
) -> dict[str, Any]:
    """One library's row body for `data_parity_report.v1`.

    Verdict is one of `match` / `mismatch` / `shadow_missing_day` /
    `both_missing` — never `in_region_only` (this IS the in-region
    comparison) and never `unmeasurable` unless the caller catches an
    exception from here and records it as such (a read failure is raised,
    not swallowed into a verdict — fail loud on a comparator the gate trusts).
    """
    live_symbols = sorted(live_lib.list_symbols())
    shadow_symbols = sorted(shadow_lib.list_symbols())
    live_frame = _day_frame(live_lib, live_symbols, trading_day)
    shadow_frame = _day_frame(shadow_lib, shadow_symbols, trading_day)

    if len(live_frame) > 0 and len(shadow_frame) == 0:
        return {
            "verdict": "shadow_missing_day",
            "comparator": "arcticdb",
            "detail": (
                f"live has {len(live_frame)} symbol(s) with a {trading_day.isoformat()} row; "
                "shadow has none. The seed holds only rows strictly before trading_day "
                "(shadow/arctic_seed.py), so an empty shadow-day frame means this run's own "
                "append did not write — never graded as a match on the seeded history "
                "(alpha-engine-config-I10819)"
            ),
        }
    if len(live_frame) == 0 and len(shadow_frame) == 0:
        return {
            "verdict": "both_missing",
            "comparator": "arcticdb",
            "detail": f"neither side has a row on {trading_day.isoformat()}",
        }

    body = _compare_frames(live_frame.reset_index(), shadow_frame.reset_index(), rel, absolute)
    matched = (
        body["row_count"]["live"] == body["row_count"]["shadow"]
        and body["schema"]["match"]
        and not body["symbol_set"]["only_live"]
        and not body["symbol_set"]["only_shadow"]
        and body["values"]["breaches"] == 0
    )
    body["verdict"] = "match" if matched else "mismatch"
    body["comparator"] = "arcticdb"
    return body


def compare_all(
    trading_day: dt.date,
    bucket: str,
    *,
    pairs: list[LibraryPair] | None = None,
    rel: float = DEFAULT_RELATIVE_TOLERANCE,
    absolute: float = DEFAULT_ABSOLUTE_TOLERANCE,
) -> dict[str, dict[str, Any]]:
    """Every declared library's row body, keyed by `arcticdb/{name}` — the
    same key `shadow.parity.run_parity` publishes for the `in_region_only`
    placeholder row this replaces."""
    resolved = pairs if pairs is not None else open_library_pairs(trading_day, bucket)
    return {
        f"arcticdb/{pair.name}": compare_library(
            pair.name, pair.live, pair.shadow, trading_day, rel=rel, absolute=absolute
        )
        for pair in resolved
    }


# ---------------------------------------------------------------------------
# Rewriting the published report (deliverable 2)
# ---------------------------------------------------------------------------


def rewrite_report(report: dict[str, Any], results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Replace every `in_region_only` row this comparator covers with its
    real verdict, keeping the report's schema and re-deriving `met`/`summary`.

    A row this comparator has no result for (a library outside
    `ARCTIC_PARITY_LIBRARIES`, or a key `shadow.parity` never published as
    `in_region_only`) is left untouched — this function only ever narrows the
    set of `in_region_only` rows, never invents or drops one.
    """
    out = dict(report)
    rows = []
    for row in report.get("keys", []):
        result = results.get(row.get("key", "")) if row.get("verdict") == "in_region_only" else None
        if result is None:
            rows.append(row)
            continue
        new_row = {
            "key": row["key"],
            "unit_ids": row["unit_ids"],
            "verdict": result["verdict"],
            "comparator": result["comparator"],
        }
        new_row.update({k: v for k, v in result.items() if k not in {"verdict", "comparator"}})
        rows.append(new_row)
    out["keys"] = rows

    # Every count the published summary already DECLARED stays declared — at
    # zero when no row carries it any more — and the verdict counts are then
    # re-derived from the rows. `settling_bar_keys` is the exception: it is a
    # breakdown OF `match` computed from S3 rows' settling bars (never an
    # ArcticDB row's), so it is carried over as published rather than zeroed.
    # Rebuilding the summary from a fixed list dropped `live_superseded`,
    # `not_applicable` and `settling_bar_keys` on every rewrite, which stopped
    # mattering only while nothing ran this on a scheduled report
    # (alpha-engine-config-I11546).
    previous = report.get("summary") or {}
    summary: dict[str, int] = {
        name: 0
        for name in (
            "match",
            "mismatch",
            "live_missing",
            "shadow_missing",
            "both_missing",
            "unmeasurable",
            "in_region_only",
            *previous,
        )
    }
    summary["total"] = len(rows)
    for row in rows:
        summary[row["verdict"]] = summary.get(row["verdict"], 0) + 1
    if _SETTLING_BAR_KEYS in previous:
        summary[_SETTLING_BAR_KEYS] = int(previous[_SETTLING_BAR_KEYS] or 0)
    out["summary"] = summary
    out["met"] = bool(rows) and all(row["verdict"] in PARITY_PASSING_VERDICTS for row in rows)
    return out


# ---------------------------------------------------------------------------
# The run manifest (deliverable 3) — `nousergon_lib.run_manifest` primitives
# used directly rather than through `run_unit`, whose `unit_id` regex
# (`D[0-9]{2}[A-Z]?`) this comparator's id does not match (see
# `ARCTIC_PARITY_UNIT_ID`'s docstring above).
# ---------------------------------------------------------------------------


def write_run_manifest(
    *,
    trading_day: dt.date,
    status: str,
    reason: str,
    inputs: Iterable[Mapping[str, Any]] = (),
    outputs: Iterable[Mapping[str, Any]] = (),
    started: "dt.datetime | None" = None,
    finished: "dt.datetime | None" = None,
    sink: "Any | None" = None,
    trigger: str = "on_demand",
) -> str:
    """Write one `data_run_manifest.v1` record for this comparator's run.

    `sink` defaults to `nousergon_lib.run_manifest.S3ManifestSink` against
    `alpha-engine-research`. Returns the key written.
    """
    from nousergon_lib import run_manifest as rm

    started = started or dt.datetime.now(dt.timezone.utc)
    finished = finished or started
    run_id = rm.new_run_id(started)
    manifest = {
        "schema_version": rm.SCHEMA_VERSION,
        "run_id": run_id,
        "unit_id": ARCTIC_PARITY_UNIT_ID,
        "trigger": trigger,
        "trading_day": trading_day.isoformat(),
        "calendar_date": started.astimezone(dt.timezone.utc).date().isoformat(),
        "status": status,
        "reason": reason,
        "started": started.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished": finished.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "code_sha": rm.resolve_code_sha(),
        "log_location": "in-region data-spot box, /var/log/data-weekly.log",
        "inputs": [dict(i) for i in inputs],
        "outputs": [dict(o) for o in outputs],
        "rows_in": 0,
        "rows_out": sum(int(o.get("rows_out", 0)) for o in outputs),
        "rows_rejected": [],
        "cost_usd": 0.0,
        "compute": {},
    }
    key = rm.manifest_key(ARCTIC_PARITY_UNIT_ID, trading_day.isoformat(), run_id, prefix=_MANIFEST_PREFIX)
    resolved_sink = sink or rm.S3ManifestSink(bucket="alpha-engine-research")
    resolved_sink.write(key, json.dumps(manifest, sort_keys=True, indent=2).encode("utf-8"))
    log.info("arctic_parity: run manifest written to %s (status=%s)", key, status)
    return key


# ---------------------------------------------------------------------------
# Waiting for v1's own write of the trading day (alpha-engine-config-I11546)
# ---------------------------------------------------------------------------


class LiveUnitNotReady(RuntimeError):
    """v1's run of a unit this comparison depends on has not landed ``ok``."""


def latest_live_manifest(store: Any, unit_id: str, trading_day: dt.date) -> "dict[str, Any] | None":
    """v1's LATEST run manifest for ``unit_id`` on ``trading_day``, or ``None``.

    Read from ``runs/{unit_id}/{trading_day}/`` in ``store`` — the store the
    parity report is published to (``s3://alpha-engine-research/
    data_collection``), which is where every live unit's
    `data_run_manifest.v1` lands. The shadow's own manifests live under
    ``staging/shadow/{day}/`` and can never be picked up here. Latest means
    max ``finished``: a retried unit leaves one manifest per attempt, and the
    last attempt is the one whose writes the live library now holds.
    """
    prefix = f"{_LIVE_RUNS_PREFIX}/{unit_id}/{trading_day.isoformat()}/"
    latest: "dict[str, Any] | None" = None
    for key in sorted(k for k in store.list_keys(prefix) if k.endswith(".json")):
        manifest = json.loads(store.get_bytes(key).decode("utf-8"))
        if not isinstance(manifest, dict) or manifest.get("unit_id") != unit_id:
            continue
        if latest is None or str(manifest.get("finished") or "") >= str(latest.get("finished") or ""):
            latest = manifest
    return latest


def await_live_units(
    store: Any,
    trading_day: dt.date,
    unit_ids: Iterable[str],
    *,
    timeout_seconds: float,
    poll_seconds: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, dict[str, Any]]:
    """Block until v1's run of every ``unit_ids`` member for ``trading_day``
    has a manifest with ``status: ok``; return those manifests by unit.

    **Why this exists.** Every read here is day-bounded, but it is not bounded
    in TIME: unlike the S3 side, which grades each key against the object
    v1's manifest recorded (`alpha-engine-config-I10892`), ArcticDB has no
    manifest-pinned version to read. So a comparison run while v1 is still
    appending the trading day grades a live library that is half-rewritten.
    MEASURED 2026-09-22..24: `shadow-morning` published its report at
    12:29-12:31Z, while v1's `morning-arctic-append` (D18, which rewrites the
    `arcticdb/universe` trading-day row the morning after) ran 12:22-12:55Z under the
    `alpha-engine-weekday` schedule (05:15 PT). A comparator started at the
    end of `shadow-morning` without this wait would grade the universe mid-way
    through v1's own append.

    A latest manifest that is not ``ok`` keeps the wait going rather than
    ending it: v1's recovery path re-runs a failed append, and the re-run's
    manifest is the one the live library ends up reflecting. Raises
    `LiveUnitNotReady` naming what was last seen once ``timeout_seconds``
    passes — never a comparison against a day v1 has not finished writing.
    """
    pending = list(dict.fromkeys(unit_ids))
    found: dict[str, dict[str, Any]] = {}
    last_seen: dict[str, str] = {unit: "no manifest" for unit in pending}
    deadline = clock() + max(0.0, float(timeout_seconds))
    while True:
        for unit in list(pending):
            manifest = latest_live_manifest(store, unit, trading_day)
            if manifest is None:
                continue
            status = str(manifest.get("status") or "")
            last_seen[unit] = f"status={status!r} run_id={manifest.get('run_id')!r}"
            if status == "ok":
                found[unit] = manifest
                pending.remove(unit)
                log.info(
                    "arctic_parity: v1 %s for %s landed ok (run %s, finished %s)",
                    unit, trading_day.isoformat(), manifest.get("run_id"), manifest.get("finished"),
                )
        if not pending:
            return found
        remaining = deadline - clock()
        if remaining <= 0:
            detail = "; ".join(f"{unit}: {last_seen[unit]}" for unit in pending)
            raise LiveUnitNotReady(
                f"v1's run for {trading_day.isoformat()} has not landed ok after "
                f"{float(timeout_seconds):.0f}s ({detail}) under {_LIVE_RUNS_PREFIX}/<unit>/"
                f"{trading_day.isoformat()}/ — refusing to grade a live ArcticDB library "
                "v1 may still be writing (alpha-engine-config-I11546)"
            )
        sleep(min(float(poll_seconds), remaining))


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------


def run_arctic_parity(
    *,
    trading_day: dt.date,
    bucket: str,
    store: Any,
    pairs: list[LibraryPair] | None = None,
    rel_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
    absolute_tolerance: float = DEFAULT_ABSOLUTE_TOLERANCE,
    manifest_sink: "Any | None" = None,
    now: "dt.datetime | None" = None,
) -> dict[str, Any]:
    """Read the published parity report, fill its `in_region_only` rows, and
    publish the result back to the SAME key — the whole point being that
    `shadow.parity`'s report gains real ArcticDB verdicts without a second
    report existing anywhere.

    Raises on any failure (fail loud — a comparator the gate reads as
    evidence has no graceful-degrade carve-out); a run manifest is still
    written on the failure path, same as every other unit in this repo, and
    the exception is re-raised after that write is durable.
    """
    started = now or dt.datetime.now(dt.timezone.utc)
    key = parity_key(trading_day)
    status, reason, updated = "failed", "run did not reach a terminal state", None
    try:
        report = json.loads(store.get_bytes(key).decode("utf-8"))
        results = compare_all(
            trading_day, bucket, pairs=pairs, rel=rel_tolerance, absolute=absolute_tolerance
        )
        updated = rewrite_report(report, results)
        store.put_bytes(key, json.dumps(updated, indent=2, sort_keys=True).encode("utf-8"))
        status, reason = "ok", ""
        return updated
    except BaseException as exc:  # noqa: BLE001 - recorded on the manifest, then re-raised
        reason = f"{type(exc).__name__}: {exc}"[:2000] or type(exc).__name__
        raise
    finally:
        outputs = (
            [{"key": key, "rows_out": len(updated.get("keys", [])), "etag": None, "schema_version": "data_parity_report.v1"}]
            if updated is not None
            else []
        )
        write_run_manifest(
            trading_day=trading_day,
            status=status,
            reason=reason,
            inputs=[{"key": key, "etag": None, "version": None, "schema_version": "data_parity_report.v1"}],
            outputs=outputs,
            started=started,
            finished=dt.datetime.now(dt.timezone.utc),
            sink=manifest_sink,
        )
