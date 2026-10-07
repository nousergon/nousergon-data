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

**Objects are fetched concurrently** (`alpha-engine-config-I11781`). The
walk is latency-bound, not CPU-bound: one archive object is ~30 KB gzip and
parses in ~2 ms, but a serial ``GetObject`` costs ~70-110 ms, so a 7-day
window cost ``objects x round-trip``. The window grew from ~10,000 objects
(670 s on 2026-09-28, the last run that finished) to ~15,000 by 2026-10-01
as fleet CloudTrail volume rose, which crossed the job's 15-minute cap: every
run from 2026-09-29 was cancelled mid-walk, before ``main()`` could write
either the metric or an error run record. ``DEFAULT_WORKERS`` bounded
threads overlap the round-trips; the per-day coverage contract, the filter
and every count are unchanged, and records keep archive-key order.

**The same walk also emits the executor's whole write SET**
(`alpha-engine-config-I11063`). The count above answers one yes/no question
(does component 3 still write into the collection prefixes?). I11063 asks a
different one: which keys of ``alpha-engine-research`` does
``alpha-engine-executor-role`` actually write, so its bucket-wide
``PutObject``/``DeleteObject`` grant can be replaced by prefix-scoped
statements derived from evidence rather than from a code grep. The records
needed are exactly the ones this walk already reads, so widening the keep
predicate to every executor write on the object bucket costs no extra
archive fetches; the collection count is the subset under
``COLLECTION_PREFIXES`` and is unchanged. The result goes to
``WRITE_SET_KEY`` beside the metric, grouped by IAM-shaped prefix (see
``key_group``) with per-group event counts, observed days, distinct writer
sessions and the literal keys when there are few enough to grant one by
one. It is an evidence document for a human IAM decision, not a gate input:
no clause reads it, and nothing here edits a grant.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "COLLECTION_PREFIXES",
    "DEFAULT_ARCHIVE_BUCKET",
    "DEFAULT_ARCHIVE_PREFIX",
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "DEFAULT_OBJECT_BUCKET",
    "DEFAULT_WORKERS",
    "EXECUTOR_ROLE_NAME",
    "ARCHIVE_VAR",
    "ArchiveRead",
    "WRITE_SET_KEY",
    "WRITE_SET_SCHEMA",
    "WriteCount",
    "WriteSet",
    "build_metric",
    "build_write_set",
    "count_collection_writes",
    "iter_archive_records",
    "key_group",
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

#: The executor's whole observed write set on ``DEFAULT_OBJECT_BUCKET``
#: (`alpha-engine-config-I11063`). Same writer, same run, same IAM grant
#: (``data_collection/metrics/executor_profile/*``) as the metric above.
WRITE_SET_KEY = "data_collection/metrics/executor_profile/write_set/latest.json"
WRITE_SET_SCHEMA = "executor_write_set.v1"

#: A group lists its literal keys only while it has at most this many: a
#: handful of literals can become literal ARNs, a long tail needs a prefix.
MAX_KEYS_PER_GROUP = 25
#: A top-level prefix whose second level fans out past this (one directory
#: per ticker, say) is reported as one ``<top>/*`` group instead.
MAX_GROUPS_PER_TOP_LEVEL = 40
#: Writer sessions (EC2 instance ids for an instance profile) listed per group.
MAX_SESSIONS_LISTED = 10

#: A path segment that IS a date (``2026-10-01``, ``20261001``, ``2026``,
#: ``date=...``) becomes ``{date}``; a date token INSIDE a segment
#: (``preflight-sweep-20261001T080010Z``, ``shadow_20260918_universe``)
#: becomes ``{date}`` in place; and a run of 10+ digits (ArcticDB's
#: per-library ids, ``universe1775588378382498816``) becomes ``{id}``. So a
#: dated or generated series is ONE group, not one per day or per library —
#: the granularity an IAM prefix grant is written at.
_WHOLE_DATE_SEGMENT = re.compile(r"(?:date|dt|day)=.+|(?:19|20)\d{2}(?:-\d{2})?")
_DATE_TOKEN = re.compile(
    r"(?<!\d)(?:(?:19|20)\d{2}-\d{2}-\d{2}(?:T[\d:.]+Z?)?|(?:19|20)\d{6}(?:T\d{4,6}Z?)?)(?!\d)"
)
_LONG_ID = re.compile(r"\d{10,}")

#: Concurrent ``GetObject`` calls per archive day. The walk is bounded by
#: round-trip latency, so this is what keeps a ~15,000-object window inside
#: the job's timeout. ``main()`` sizes the boto3 connection pool to match —
#: botocore's default pool of 10 would otherwise cap the overlap at 10.
DEFAULT_WORKERS = 32

#: ``DeleteObjects`` (the batch call) is deliberately absent: CloudTrail also
#: emits one ``DeleteObject`` per key it removed, and those are what is
#: counted — counting the batch too would double-count (see
#: ``_object_bucket_and_key``).
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
    workers: int = DEFAULT_WORKERS,
) -> ArchiveRead:
    """Every record on ``day`` for which ``keep(record)`` is true.

    ``covered=False`` means the archive delivered NO objects for the day —
    a trail always delivers, so a day with zero objects is a gap in what we
    can see, never evidence of a quiet day. Distinct from a day that
    delivered objects none of which matched ``keep``, which IS a real zero.

    Objects are fetched on up to ``workers`` threads (boto3 clients are
    thread-safe); results are consumed in archive-key order, so the kept
    records are identical to a serial walk's. A failed fetch propagates.
    """
    keys = _archive_object_keys(s3, bucket=bucket, prefix=prefix, region=region, day=day)
    if not keys:
        return ArchiveRead(records=(), objects_read=0, records_scanned=0, covered=False)

    def _scan(key: str) -> tuple[list[dict[str, Any]], int]:
        records = _fetch_records(s3, bucket=bucket, key=key)
        return [r for r in records if keep(r)], len(records)

    kept: list[dict[str, Any]] = []
    scanned = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for matched, count in pool.map(_scan, keys):
            kept.extend(matched)
            scanned += count
    return ArchiveRead(records=tuple(kept), objects_read=len(keys), records_scanned=scanned, covered=True)


def _is_executor_identity(record: dict[str, Any]) -> bool:
    identity = record.get("userIdentity") or {}
    issuer_arn = str(((identity.get("sessionContext") or {}).get("sessionIssuer") or {}).get("arn") or "")
    own_arn = str(identity.get("arn") or "")
    needle = f"role/{EXECUTOR_ROLE_NAME}"
    return needle in issuer_arn or needle in own_arn


def _object_bucket_and_key(record: dict[str, Any]) -> tuple[str, str]:
    """The ``(bucket, key)`` an S3 data event wrote, or ``("", "")``.

    A single-object call carries both in ``requestParameters``. A batch
    ``DeleteObjects`` does not: CloudTrail logs one ``DeleteObject`` event
    per deleted key with ``requestParameters: null``, and the object
    appears only in ``resources[]`` as an ``AWS::S3::Object`` ARN
    (``additionalEventData.parentRequestID`` ties it to the batch). ArcticDB
    prunes this way, so reading ``requestParameters`` alone made every
    batch-deleted key invisible: 13,400 executor deletes under
    ``arcticdb/`` in the 2026-09-27..10-03 window were uncounted.
    """
    params = record.get("requestParameters")
    if isinstance(params, dict) and params.get("bucketName"):
        return str(params.get("bucketName")), str(params.get("key") or "").lstrip("/")
    for resource in record.get("resources") or []:
        if not isinstance(resource, dict) or resource.get("type") != "AWS::S3::Object":
            continue
        arn = str(resource.get("ARN") or "")
        if not arn.startswith("arn:aws:s3:::"):
            continue
        bucket, _, key = arn.removeprefix("arn:aws:s3:::").partition("/")
        return bucket, key.lstrip("/")
    return "", ""


def _touches_collection_prefix(record: dict[str, Any], *, object_bucket: str) -> bool:
    bucket, key = _object_bucket_and_key(record)
    if bucket != object_bucket:
        return False
    return any(key.startswith(p) for p in COLLECTION_PREFIXES)


def _is_executor_write(record: dict[str, Any], *, object_bucket: str) -> bool:
    """An S3 write by the executor role to ANY key of ``object_bucket``."""
    if record.get("eventSource") != "s3.amazonaws.com":
        return False
    if record.get("eventName") not in _WRITE_EVENTS:
        return False
    bucket, key = _object_bucket_and_key(record)
    if bucket != object_bucket or not key:
        return False
    return _is_executor_identity(record)


def _is_collection_write(record: dict[str, Any], *, object_bucket: str) -> bool:
    if not _is_executor_write(record, object_bucket=object_bucket):
        return False
    return _touches_collection_prefix(record, object_bucket=object_bucket)


def key_group(key: str) -> str:
    """The IAM-shaped prefix a written key is reported under.

    - a top-level object is its own literal key (``research.db``);
    - a file directly under a top-level prefix groups as ``<top>/``
      (``health/daily_data.json`` -> ``health/``), and the group lists the
      literal keys while there are few, which is what a literal-ARN grant
      needs;
    - anything deeper groups by its first two segments, with a date-shaped
      second segment collapsed to ``{date}``
      (``signals/2026-10-01/x.json`` -> ``signals/{date}/``,
      ``arcticdb/universe1775588378382498816/...`` -> ``arcticdb/universe{id}/``).
    """
    parts = key.split("/")
    if len(parts) == 1:
        return key
    top = parts[0]
    if len(parts) == 2:
        return f"{top}/"
    return f"{top}/{_normalize_segment(parts[1])}/"


def _normalize_segment(segment: str) -> str:
    if _WHOLE_DATE_SEGMENT.fullmatch(segment):
        return "{date}"
    return _LONG_ID.sub("{id}", _DATE_TOKEN.sub("{date}", segment))


def _session_name(record: dict[str, Any]) -> str:
    arn = str((record.get("userIdentity") or {}).get("arn") or "")
    return arn.rsplit("/", 1)[-1] if ":assumed-role/" in arn else ""


@dataclass
class _Group:
    events: dict[str, int] = field(default_factory=dict)
    failed: int = 0
    keys: set[str] = field(default_factory=set)
    keys_complete: bool = True
    sessions: set[str] = field(default_factory=set)
    days: set[str] = field(default_factory=set)
    first_seen: str = ""
    last_seen: str = ""
    merged_groups: int = 1

    @property
    def writes(self) -> int:
        return sum(self.events.values())

    def add(self, record: dict[str, Any], key: str) -> None:
        name = str(record.get("eventName"))
        self.events[name] = self.events.get(name, 0) + 1
        if record.get("errorCode"):
            self.failed += 1
        self._add_key(key)
        session = _session_name(record)
        if session:
            self.sessions.add(session)
        when = str(record.get("eventTime") or "")
        if when:
            self.days.add(when[:10])
            self.first_seen = min(self.first_seen, when) if self.first_seen else when
            self.last_seen = max(self.last_seen, when)

    def _add_key(self, key: str) -> None:
        if key in self.keys:
            return
        if len(self.keys) >= MAX_KEYS_PER_GROUP:
            self.keys_complete = False
            return
        self.keys.add(key)

    def merge(self, other: "_Group") -> None:
        for name, n in other.events.items():
            self.events[name] = self.events.get(name, 0) + n
        self.failed += other.failed
        for key in sorted(other.keys):
            self._add_key(key)
        self.keys_complete = self.keys_complete and other.keys_complete
        self.sessions |= other.sessions
        self.days |= other.days
        seen_first = [t for t in (self.first_seen, other.first_seen) if t]
        self.first_seen = min(seen_first) if seen_first else ""
        self.last_seen = max(self.last_seen, other.last_seen)
        self.merged_groups += other.merged_groups


@dataclass
class WriteSet:
    """Every executor write on the object bucket, aggregated by ``key_group``."""

    groups: dict[str, _Group] = field(default_factory=dict)

    def add(self, record: dict[str, Any]) -> None:
        _, key = _object_bucket_and_key(record)
        group = key_group(key)
        self.groups.setdefault(group, _Group()).add(record, key)

    def collapsed(self) -> dict[str, _Group]:
        """``groups`` with any over-wide top level folded into ``<top>/*``."""
        by_top: dict[str, list[str]] = {}
        for name in self.groups:
            if name.count("/") == 2:  # `<top>/<second>/`
                by_top.setdefault(name.split("/", 1)[0], []).append(name)
        out = dict(self.groups)
        for top, names in by_top.items():
            if len(names) <= MAX_GROUPS_PER_TOP_LEVEL:
                continue
            folded = _Group(merged_groups=0)
            for name in sorted(names):
                folded.merge(out.pop(name))
            out[f"{top}/*"] = folded
        return out


@dataclass(frozen=True)
class WriteCount:
    collection_writes: int
    days_requested: int
    days_covered: int
    uncovered_days: tuple[str, ...] = field(default_factory=tuple)
    objects_read: int = 0
    records_scanned: int = 0
    write_events: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    #: Every executor write on the object bucket, collection or not (I11063).
    write_set: WriteSet = field(default_factory=WriteSet)
    window_start: str = ""
    window_end: str = ""


def count_collection_writes(
    s3: Any,
    *,
    archive_bucket: str,
    archive_prefix: str,
    region: str = _REGION,
    object_bucket: str = DEFAULT_OBJECT_BUCKET,
    start: dt.date,
    end: dt.date,
    workers: int = DEFAULT_WORKERS,
) -> WriteCount:
    """Executor writes into the collection prefixes over ``[start, end]``.

    Walks day by day so a day the archive did not deliver is visible as a
    named gap in ``uncovered_days`` rather than silently subtracted from the
    count. The same walk aggregates every executor write on
    ``object_bucket`` into ``write_set`` (I11063); ``collection_writes`` is
    the subset under ``COLLECTION_PREFIXES``.
    """
    writes: list[dict[str, Any]] = []
    write_set = WriteSet()
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
            keep=lambda r: _is_executor_write(r, object_bucket=object_bucket),
            workers=workers,
        )
        objects_read += read.objects_read
        records_scanned += read.records_scanned
        if read.covered:
            covered_days += 1
            for record in read.records:
                write_set.add(record)
                if _touches_collection_prefix(record, object_bucket=object_bucket):
                    writes.append(record)
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
        write_set=write_set,
        window_start=start.isoformat(),
        window_end=end.isoformat(),
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


def build_write_set(
    *,
    count: WriteCount,
    object_bucket: str = DEFAULT_OBJECT_BUCKET,
    as_of: dt.datetime | None = None,
) -> dict:
    """The ``WRITE_SET_KEY`` document: what the executor role was OBSERVED
    to write, never what it is allowed to. Coverage fields are the metric's,
    so a partial window reads as partial here too."""
    as_of = as_of or dt.datetime.now(dt.timezone.utc)
    groups = count.write_set.collapsed()
    by_top: dict[str, int] = {}
    rows = []
    for name in sorted(groups):
        g = groups[name]
        top = name.split("/", 1)[0] + ("/" if "/" in name else "")
        by_top[top] = by_top.get(top, 0) + g.writes
        rows.append(
            {
                "prefix": name,
                "collection": any(name.startswith(p) for p in COLLECTION_PREFIXES),
                "writes": g.writes,
                "events": dict(sorted(g.events.items())),
                "failed": g.failed,
                "keys": sorted(g.keys),
                # False once the group passed MAX_KEYS_PER_GROUP distinct keys:
                # `keys` is then a sample, and only a prefix grant covers it.
                "keys_complete": g.keys_complete,
                "days": sorted(g.days),
                "first_seen": g.first_seen,
                "last_seen": g.last_seen,
                "distinct_sessions": len(g.sessions),
                "sessions": sorted(g.sessions)[:MAX_SESSIONS_LISTED],
                "merged_groups": g.merged_groups,
            }
        )
    total = sum(r["writes"] for r in rows)
    return {
        "schema_version": WRITE_SET_SCHEMA,
        "role": EXECUTOR_ROLE_NAME,
        "object_bucket": object_bucket,
        "as_of": as_of.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "window": {"start": count.window_start, "end": count.window_end},
        "days_requested": count.days_requested,
        "days_covered": count.days_covered,
        "uncovered_days": list(count.uncovered_days),
        "total_writes": total,
        "collection_writes": count.collection_writes,
        "by_top_level": dict(sorted(by_top.items())),
        "groups": rows,
        "limits": {
            "max_keys_per_group": MAX_KEYS_PER_GROUP,
            "max_groups_per_top_level": MAX_GROUPS_PER_TOP_LEVEL,
            "max_sessions_listed": MAX_SESSIONS_LISTED,
        },
        "source": "CloudTrail S3 data events (WriteOnly) in the fleet archive; reads are not logged, so this is a WRITE set only",
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
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="concurrent archive GetObject calls")
    ap.add_argument("--write-set-key", default=WRITE_SET_KEY)
    ap.add_argument(
        "--write-set-file",
        default=None,
        help="also write the write-set document to this LOCAL path (for a read-only operator run with --no-write); never printed",
    )
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)

    import boto3  # noqa: PLC0415 - deferred so import stays light for tests
    from botocore.config import Config  # noqa: PLC0415

    from data_gate.producers._run_record import write_run_record  # noqa: PLC0415

    workers = max(1, args.workers)
    s3 = boto3.client("s3", region_name=args.region, config=Config(max_pool_connections=workers))
    archive_bucket, archive_prefix = _resolve_archive(args.archive)

    started_at = dt.datetime.now(dt.timezone.utc)
    try:
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
            workers=workers,
        )
        metric = build_metric(count=count)
        write_set = build_write_set(count=count, object_bucket=args.object_bucket)
        # Same public-log posture as the sibling producers (alpha-engine-
        # config-I11274): `build_metric` today emits only counts and dates
        # (no principals, ARNs or bucket paths — `WriteCount.write_events`,
        # which DOES carry raw CloudTrail records, is deliberately excluded
        # from it), so this print is not an active leak. Normalized anyway
        # so a future field added to the metric can't silently start being
        # printed to this PUBLIC repo's Actions log without a deliberate
        # decision to widen this line.
        print(f"{args.key}: days_covered={count.days_covered}, status=ok")
        if args.write_set_file:
            # A local file, not stdout: the write set names keys and writer
            # sessions, which stay out of this PUBLIC repo's Actions log.
            with open(args.write_set_file, "w", encoding="utf-8") as fh:
                json.dump(write_set, fh, indent=2, sort_keys=True)
    except Exception as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                s3,
                bucket=args.bucket,
                producer="executor_profile",
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=str(exc),
            )
        raise

    if not args.no_write:
        s3.put_object(
            Bucket=args.bucket,
            Key=args.key,
            Body=json.dumps(metric, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"WROTE s3://{args.bucket}/{args.key}")

        # Second document, after the metric: the clause input lands first,
        # so a failure here can never cost the gate its reading.
        s3.put_object(
            Bucket=args.bucket,
            Key=args.write_set_key,
            Body=json.dumps(write_set, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"WROTE s3://{args.bucket}/{args.write_set_key}")

        write_run_record(
            s3,
            bucket=args.bucket,
            producer="executor_profile",
            status="ok",
            started_at=started_at,
            finished_at=dt.datetime.now(dt.timezone.utc),
            detail={
                "metric_key": args.key,
                "collection_writes": count.collection_writes,
                "days_covered": count.days_covered,
                "days_requested": count.days_requested,
                "write_set_key": args.write_set_key,
                "write_set_total_writes": write_set["total_writes"],
                "write_set_groups": len(write_set["groups"]),
            },
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
