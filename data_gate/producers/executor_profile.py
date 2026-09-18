"""Producer for ``data.phase2.executor_collection_writes_zero`` (`alpha-engine-
config-I11036`).

Writes ``metrics/executor_profile/collection_writes/latest.json`` under
``s3://alpha-engine-research/data_collection``, the key
``data_gate.exit_criteria.read_executor_collection_writes_zero`` already reads.

**Mirrors, does not duplicate, the existing CloudTrail-S3-archive read.**
`data.human_touch.monthly` and `crucible.autonomy` (`crucible/crucible/
autonomy.py::iter_archive_records`, `attribute_pointer_writes`) already read
the fleet's CloudTrail S3 archive rather than the truncated `lookup-events`
API — the exact failure mode `crucible.autonomy`'s own module docstring
names: multi-week windows silently truncate to ~2 days under
`lookup-events`. `crucible` is a separate repo with no dependency edge into
`nousergon-data` (`requirements.txt` pins no `crucible` extra), so this
module re-implements the SAME shape — date-partitioned gzip JSON objects
under the archive bucket/prefix, walked day by day, an uncovered day
reported as a GAP rather than silence — as its own small, independently
tested walker, never a second `lookup-events`-based audit path.

**The archive location is a fleet-wide constant, not operator-configured.**
`nous-ergon-ops/infrastructure/cloudformation/fleet-cloudtrail.yaml` fixes
``BucketName: nousergon-fleet-cloudtrail-archive`` and delivers under
``AWSLogs/<account>/CloudTrail`` (its own ``ArchiveS3Uri`` output). Unlike
``crucible.config.DEFAULT_CLOUDTRAIL_ARCHIVE`` (empty, because that module
predates the stack), this repo's default is the real, live value — override
via ``NOUSERGON_DATA_CLOUDTRAIL_ARCHIVE`` only for a test double or a future
account split.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "COLLECTION_PREFIXES",
    "DEFAULT_ARCHIVE_BUCKET",
    "DEFAULT_ARCHIVE_PREFIX",
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "DEFAULT_OBJECT_BUCKET",
    "EXECUTOR_ROLE_NAME",
    "ARCHIVE_VAR",
    "ArchiveRead",
    "WriteCount",
    "build_metric",
    "count_collection_writes",
    "iter_archive_records",
    "main",
]

logger = logging.getLogger(__name__)

_REGION = "us-east-1"
_ACCOUNT = "711398986525"

#: `fleet-cloudtrail.yaml` Outputs — the fleet-wide CloudTrail archive.
DEFAULT_ARCHIVE_BUCKET = "nousergon-fleet-cloudtrail-archive"
DEFAULT_ARCHIVE_PREFIX = f"AWSLogs/{_ACCOUNT}/CloudTrail"
ARCHIVE_VAR = "NOUSERGON_DATA_CLOUDTRAIL_ARCHIVE"

#: `nous-ergon-ops/infrastructure/iam/alpha-engine-executor-role/` — component
#: 3. A write attributed to this role's assumed-role session (or, degenerate
#: case, the bare role) is a collection write from the executor.
EXECUTOR_ROLE_NAME = "alpha-engine-executor-role"

#: The prefixes `architecture.d/146` rule 1 reserves to component 1 (the
#: collector) — verbatim from the issue.
COLLECTION_PREFIXES: tuple[str, ...] = (
    "market_data/",
    "arcticdb/",
    "staging/",
    "data/",
    "reference/price_cache/",
)

DEFAULT_OBJECT_BUCKET = "alpha-engine-research"
DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_KEY = "data_collection/metrics/executor_profile/collection_writes/latest.json"

_WRITE_EVENTS = frozenset({"PutObject", "DeleteObject", "CompleteMultipartUpload"})


@dataclass(frozen=True)
class ArchiveRead:
    """One day's worth of archive records matching a keep-predicate."""

    records: tuple[dict[str, Any], ...]
    objects_read: int
    records_scanned: int
    covered: bool


def _archive_object_keys(s3: Any, *, bucket: str, prefix: str, region: str, day: dt.date) -> list[str]:
    day_prefix = f"{prefix}/{region}/{day:%Y}/{day:%m}/{day:%d}/"
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=day_prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def _fetch_records(s3: Any, *, bucket: str, key: str) -> list[dict[str, Any]]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    payload = json.loads(gzip.decompress(body))
    records = payload.get("Records")
    return records if isinstance(records, list) else []


def iter_archive_records(
    s3: Any,
    *,
    bucket: str,
    prefix: str,
    region: str,
    day: dt.date,
    keep: Any,
) -> ArchiveRead:
    """Every record on ``day`` for which ``keep(record)`` is true.

    ``covered=False`` means the archive delivered NO objects for the day —
    a trail always delivers, so a day with zero objects is a gap in what we
    can see, never evidence of a quiet day. Distinct from a day that
    delivered objects none of which matched ``keep``, which IS a real zero.
    """
    keys = _archive_object_keys(s3, bucket=bucket, prefix=prefix, region=region, day=day)
    if not keys:
        return ArchiveRead(records=(), objects_read=0, records_scanned=0, covered=False)
    kept: list[dict[str, Any]] = []
    scanned = 0
    for key in keys:
        for record in _fetch_records(s3, bucket=bucket, key=key):
            scanned += 1
            if keep(record):
                kept.append(record)
    return ArchiveRead(records=tuple(kept), objects_read=len(keys), records_scanned=scanned, covered=True)


def _is_executor_identity(record: dict[str, Any]) -> bool:
    identity = record.get("userIdentity") or {}
    issuer_arn = str(((identity.get("sessionContext") or {}).get("sessionIssuer") or {}).get("arn") or "")
    own_arn = str(identity.get("arn") or "")
    needle = f"role/{EXECUTOR_ROLE_NAME}"
    return needle in issuer_arn or needle in own_arn


def _touches_collection_prefix(record: dict[str, Any], *, object_bucket: str) -> bool:
    params = record.get("requestParameters")
    if not isinstance(params, dict):
        return False
    if params.get("bucketName") != object_bucket:
        return False
    key = str(params.get("key") or "").lstrip("/")
    return any(key.startswith(p) for p in COLLECTION_PREFIXES)


def _is_collection_write(record: dict[str, Any], *, object_bucket: str) -> bool:
    if record.get("eventSource") != "s3.amazonaws.com":
        return False
    if record.get("eventName") not in _WRITE_EVENTS:
        return False
    if not _touches_collection_prefix(record, object_bucket=object_bucket):
        return False
    return _is_executor_identity(record)


@dataclass(frozen=True)
class WriteCount:
    collection_writes: int
    days_requested: int
    days_covered: int
    uncovered_days: tuple[str, ...] = field(default_factory=tuple)
    objects_read: int = 0
    records_scanned: int = 0
    write_events: tuple[dict[str, Any], ...] = field(default_factory=tuple)


def count_collection_writes(
    s3: Any,
    *,
    archive_bucket: str,
    archive_prefix: str,
    region: str = _REGION,
    object_bucket: str = DEFAULT_OBJECT_BUCKET,
    start: dt.date,
    end: dt.date,
) -> WriteCount:
    """Executor writes into the collection prefixes over ``[start, end]``.

    Walks day by day so a day the archive did not deliver is visible as a
    named gap in ``uncovered_days`` rather than silently subtracted from the
    count.
    """
    writes: list[dict[str, Any]] = []
    covered_days = 0
    uncovered: list[str] = []
    objects_read = 0
    records_scanned = 0
    day = start
    while day <= end:
        read = iter_archive_records(
            s3,
            bucket=archive_bucket,
            prefix=archive_prefix,
            region=region,
            day=day,
            keep=lambda r: _is_collection_write(r, object_bucket=object_bucket),
        )
        objects_read += read.objects_read
        records_scanned += read.records_scanned
        if read.covered:
            covered_days += 1
            writes.extend(read.records)
        else:
            uncovered.append(day.isoformat())
        day += dt.timedelta(days=1)
    days_requested = (end - start).days + 1
    return WriteCount(
        collection_writes=len(writes),
        days_requested=days_requested,
        days_covered=covered_days,
        uncovered_days=tuple(uncovered),
        objects_read=objects_read,
        records_scanned=records_scanned,
        write_events=tuple(writes),
    )


def build_metric(*, count: WriteCount, as_of: dt.datetime | None = None) -> dict:
    as_of = as_of or dt.datetime.now(dt.timezone.utc)
    return {
        "collection_writes": count.collection_writes,
        # `days_covered` names ONLY the days the archive actually delivered
        # for — a partial window (archive gap) must not silently masquerade
        # as the requested window. `read_executor_collection_writes_zero`
        # requires `days_covered == days` (the requested 7) to read MET.
        "days_covered": count.days_covered,
        "as_of": as_of.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "days_requested": count.days_requested,
        "uncovered_days": list(count.uncovered_days),
        "objects_read": count.objects_read,
        "records_scanned": count.records_scanned,
    }


def _resolve_archive(archive_uri: str | None) -> tuple[str, str]:
    uri = archive_uri or os.environ.get(ARCHIVE_VAR) or f"s3://{DEFAULT_ARCHIVE_BUCKET}/{DEFAULT_ARCHIVE_PREFIX}"
    rest = uri.removeprefix("s3://").strip("/")
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--bucket", default=DEFAULT_BUCKET, help="store bucket to write the metric to")
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--object-bucket", default=DEFAULT_OBJECT_BUCKET, help="bucket the collection prefixes live in")
    ap.add_argument("--archive", default=None, help=f"s3://<bucket>/<prefix> for the CloudTrail archive (default: {ARCHIVE_VAR} or the fleet default)")
    ap.add_argument("--region", default=_REGION)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)

    import boto3  # noqa: PLC0415 - deferred so import stays light for tests

    s3 = boto3.client("s3", region_name=args.region)
    archive_bucket, archive_prefix = _resolve_archive(args.archive)

    end = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)  # yesterday: today's archive is incomplete
    start = end - dt.timedelta(days=args.days - 1)

    count = count_collection_writes(
        s3,
        archive_bucket=archive_bucket,
        archive_prefix=archive_prefix,
        region=args.region,
        object_bucket=args.object_bucket,
        start=start,
        end=end,
    )
    metric = build_metric(count=count)
    print(json.dumps(metric, indent=2, sort_keys=True))

    if not args.no_write:
        s3.put_object(
            Bucket=args.bucket,
            Key=args.key,
            Body=json.dumps(metric, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"WROTE s3://{args.bucket}/{args.key}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
