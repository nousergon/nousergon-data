"""collectors/arctic_probe.py — in-region ArcticDB probe (data-collector plan
`alpha-engine-config/private-docs/data_collection_plan_260914.md` P-05,
`alpha-engine-config-I10748`).

Why this exists
================

ArcticDB is unreadable from the laptop: the `alpha-engine-data` bucket carries
an explicit S3 Deny that blocks even `ne-admin` on `ListObjectsV2` /
`GetBucketPolicy` (measured 2026-09-01, `alpha-engine-config-I9771`). The
`data_gate` red board (plan §4.1) reads artifacts, never live infrastructure,
so it can never open ArcticDB directly either. This module is the IN-REGION
producer that stands in: it opens the three live ArcticDB libraries
(`universe`, `macro`, `delisted_history`) via `store/arctic_store.py` /
`nousergon_lib.arcticdb`, describes each (row count, symbol count, last index
date — metadata only, via `get_description_batch`, never a full data read),
and writes one JSON record the gate CAN read from the laptop or CI:

    data_collection/probes/arctic/{trading_day}.json

Fail-loud, never a partial record (nousergon-data AGENTS.md producer
discipline; plan §4.1 "How it reads what the laptop cannot"): if ANY declared
library cannot be opened, listed, or described, :class:`ProbeError` is raised
and NOTHING is written for that trading day. This is deliberate — a written
record that mixes ok/not-ok libraries would let the gate read a broken day as
partially green. A day with no probe record renders every ArcticDB-derived
clause UNMEASURABLE on the gate (plan §4.1's table: "Read failed or evidence
source dark" -> UNREPORTED), which is the honest state; it never renders
stale-green.

Where it runs
==============

This is Workload `arctic-probe` in the `data-spot-dispatcher` Lambda
(`infrastructure/lambdas/data-spot-dispatcher/index.py::_WORKLOADS`), wired as
the FINAL workload of both the `data-collection-eod` and `data-collection-morning`
schedule inputs in `infrastructure/cloudformation/nousergon-data-collection.yaml`
— i.e. it runs on the in-region data-spot box, under the box's own
`alpha-engine-executor-profile` instance profile (already grants
`alpha-engine-research/*` read+write — see
`nous-ergon-ops/infrastructure/iam/alpha-engine-executor-role/
alpha-engine-research-bucket-write.json`; no new grant needed).

On-demand invocation (NOT run as part of building/reviewing this module):

    aws lambda invoke --function-name alpha-engine-data-spot-dispatcher \\
      --payload '{"workload":"arctic-probe"}' --cli-binary-format raw-in-base64-out \\
      /tmp/out.json

CLI (in-region only — ArcticDB is unreachable from the laptop):

    python -m collectors.arctic_probe [--bucket BUCKET] [--trading-day YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

#: S3 key template the probe writes under. `{trading_day}` is `YYYY-MM-DD`.
PROBE_KEY_TEMPLATE = "data_collection/probes/arctic/{trading_day}.json"

#: The three live ArcticDB libraries this probe covers (plan P-05). Order is
#: the write order in the record — arbitrary but stable for diffability.
LIBRARIES: tuple[str, ...] = ("universe", "macro", "delisted_history")


class ProbeError(RuntimeError):
    """Raised when any declared ArcticDB library cannot be opened, listed, or
    described cleanly.

    NEVER caught to produce a partial/degraded record — the caller
    (:func:`run_probe`) lets this propagate before anything is written to S3.
    See the module docstring's "Fail-loud, never a partial record" section.
    """


def _open_libraries(bucket: str) -> dict[str, Any]:
    """Open the three live ArcticDB libraries via `store/arctic_store.py`
    (which itself delegates to `nousergon_lib.arcticdb`'s single library-open
    chokepoint). Raises :class:`ProbeError` on the first library that fails
    to open — never opens the rest and returns a partial set."""
    from store.arctic_store import get_delisted_history_lib, get_macro_lib, get_universe_lib

    openers = {
        "universe": lambda: get_universe_lib(bucket),
        "macro": lambda: get_macro_lib(bucket),
        "delisted_history": lambda: get_delisted_history_lib(bucket),
    }
    libs: dict[str, Any] = {}
    for name in LIBRARIES:
        try:
            libs[name] = openers[name]()
        except Exception as exc:
            raise ProbeError(
                f"arctic_probe: library {name!r} open failed on bucket {bucket!r}: {exc}"
            ) from exc
    return libs


def _describe_library(name: str, lib: Any) -> dict[str, Any]:
    """Aggregate `row_count` / `symbol_count` / `last_index_date` for one
    already-opened ArcticDB library.

    Metadata-only: uses `list_symbols()` + `get_description_batch()`, never a
    full `read()` — the probe describes the store, it does not load it.

    `version_id` is always `None`: ArcticDB versions are per-SYMBOL, not
    per-library — there is no single "library version" to report. The field
    stays in the schema (nullable) because the plan names it explicitly; a
    future per-symbol probe variant could populate a per-symbol equivalent.

    Raises :class:`ProbeError` on any listing or description failure —
    including a single symbol's `DataError` inside the batch result. A
    library that opened but cannot be enumerated/described cleanly is not
    distinguishable, for this probe's purposes, from one that could not be
    opened at all: either way nothing about it is trustworthy enough to
    write.
    """
    try:
        symbols = sorted(lib.list_symbols())
    except Exception as exc:
        raise ProbeError(f"arctic_probe: {name}.list_symbols() failed: {exc}") from exc

    if not symbols:
        return {
            "row_count": 0,
            "symbol_count": 0,
            "last_index_date": None,
            "version_id": None,
            "read_ok": True,
        }

    try:
        descriptions = lib.get_description_batch(symbols)
    except Exception as exc:
        raise ProbeError(
            f"arctic_probe: {name}.get_description_batch() failed for "
            f"{len(symbols)} symbols: {exc}"
        ) from exc

    try:
        import arcticdb as adb

        data_error_cls: type | tuple = getattr(adb, "DataError", ())
    except ImportError:  # pragma: no cover - arcticdb is a hard dependency here
        data_error_cls = ()

    row_count = 0
    last_date: str | None = None
    for sym, desc in zip(symbols, descriptions):
        if data_error_cls and isinstance(desc, data_error_cls):
            raise ProbeError(
                f"arctic_probe: {name}: description failed for symbol "
                f"{sym!r}: {getattr(desc, 'exception_string', None) or desc}"
            )
        row_count += int(desc.row_count)
        date_range = getattr(desc, "date_range", None)
        end = date_range[1] if date_range else None
        if end is not None:
            end_date = end.date().isoformat() if hasattr(end, "date") else str(end)[:10]
            if last_date is None or end_date > last_date:
                last_date = end_date

    return {
        "row_count": row_count,
        "symbol_count": len(symbols),
        "last_index_date": last_date,
        "version_id": None,
        "read_ok": True,
    }


def build_probe_record(
    bucket: str,
    *,
    trading_day: str,
    libs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the full probe record for `trading_day`.

    `libs`, when provided, maps library name -> an already-opened
    library-like object (must implement `list_symbols()` and
    `get_description_batch()`) — the injection seam
    `tests/test_arctic_probe.py` uses for a fake Arctic. `None` (the
    production path) opens the three live libraries via
    `store/arctic_store.py`.

    Raises :class:`ProbeError` (never returns a partial dict) if any library
    cannot be opened, listed, or described.
    """
    opened = libs if libs is not None else _open_libraries(bucket)
    missing = [name for name in LIBRARIES if name not in opened]
    if missing:
        raise ProbeError(f"arctic_probe: no library provided for {missing}")

    libraries = {name: _describe_library(name, opened[name]) for name in LIBRARIES}

    return {
        "schema_version": SCHEMA_VERSION,
        "as_of_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "trading_day": trading_day,
        "libraries": libraries,
    }


def write_probe_record(record: dict[str, Any], *, bucket: str, s3_client: Any = None) -> str:
    """Write `record` to `data_collection/probes/arctic/{trading_day}.json`.

    Raises on any S3 failure (fail loud — no swallow; this repo's producer
    discipline has no graceful-degrade carve-out on a writer). Returns the
    key written.
    """
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")
    key = PROBE_KEY_TEMPLATE.format(trading_day=record["trading_day"])
    body = json.dumps(record, indent=2, sort_keys=True).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return key


def run_probe(
    *,
    bucket: str | None = None,
    trading_day: str | None = None,
    libs: dict[str, Any] | None = None,
    s3_client: Any = None,
) -> dict[str, Any]:
    """Build and write the probe record for one trading day. The single entry
    point both the CLI and the dispatcher workload use.

    `trading_day` defaults via `dates.default_run_date()` (the repo-local
    trading-day-axis chokepoint — see `dates.py`), matching every other
    collector's default-date convention. `bucket` defaults to
    `store.arctic_store.DEFAULT_BUCKET` (`alpha-engine-research`).
    """
    from dates import default_run_date
    from store.arctic_store import DEFAULT_BUCKET

    resolved_bucket = bucket or DEFAULT_BUCKET
    resolved_trading_day = trading_day or default_run_date()

    record = build_probe_record(resolved_bucket, trading_day=resolved_trading_day, libs=libs)
    key = write_probe_record(record, bucket=resolved_bucket, s3_client=s3_client)
    logger.info(
        "arctic_probe: wrote s3://%s/%s — %s",
        resolved_bucket,
        key,
        {name: record["libraries"][name]["symbol_count"] for name in LIBRARIES},
    )
    return record


# ── CLI ─────────────────────────────────────────────────────────────────────


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.arctic_probe",
        description=(
            "In-region ArcticDB probe: describe universe/macro/delisted_history "
            "and write data_collection/probes/arctic/{trading_day}.json "
            "(plan P-05, alpha-engine-config-I10748)."
        ),
    )
    parser.add_argument("--bucket", default=None, help="data bucket (default: store.arctic_store.DEFAULT_BUCKET)")
    parser.add_argument(
        "--trading-day",
        default=None,
        help="YYYY-MM-DD to key the record under (default: dates.default_run_date())",
    )
    return parser


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parser().parse_args(argv)
    record = run_probe(bucket=args.bucket, trading_day=args.trading_day)
    print(json.dumps(record, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
