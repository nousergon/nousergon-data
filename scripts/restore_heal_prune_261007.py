#!/usr/bin/env python3
"""Undo the S3 side of the 2026-10-07 daily-heal prune.

Tracked as ``alpha-engine-config-I12115`` (evidence impact on
``alpha-engine-config-I12023``).

**What happened.** The 2026-10-07 09:00Z daily-heal took the backfill path for
the first time (made reachable by nousergon-data-PR2107). That path pruned
previous ArcticDB versions on write (stopped by nousergon-data-PR2109), so the
run issued DeleteObjects against two libraries between 09:03Z and 10:06Z.

**Why it is recoverable.** ``alpha-engine-research`` is versioned. Every delete
left a delete marker over the old object, and the old object is now a
noncurrent version. The bucket rule ``expire-noncurrent-versions-30d`` removes
those for good about 30 days later (~2026-11-06).

**What this does.** It removes exactly those delete markers: the LATEST entry
on its key, created inside the heal's window, and hiding a real object version.
Removing a marker makes the old object current again and stops that clock. It
never deletes an object version. Without ``--apply`` it only prints the plan.

**What it does not do.** Make the pruned versions readable through the ArcticDB
API. The prune also tombstoned them in each symbol's version chain. This keeps
the bytes, which the parity evidence and a faithful replay need; re-exposing
them is a separate decision.

Measured read-only 2026-10-07: 129,105 markers in the window, 86,460 of them
hide an object (4.79 GB). The rest sit on ``cstats`` keys that never existed.

Usage (operator, admin profile)::

    AWS_PROFILE=ne-admin AWS_REGION=us-east-1 python3 scripts/restore_heal_prune_261007.py
    AWS_PROFILE=ne-admin AWS_REGION=us-east-1 python3 scripts/restore_heal_prune_261007.py --apply
"""

from __future__ import annotations

import argparse
import datetime as dt

BUCKET = "alpha-engine-research"
LIBRARY_PREFIXES = (
    "arcticdb/macro1775588378499087360/",
    "arcticdb/universe1775588378382498816/",
)
WINDOW_START = dt.datetime(2026, 10, 7, 9, 0, tzinfo=dt.UTC)
WINDOW_END = dt.datetime(2026, 10, 7, 10, 10, tzinfo=dt.UTC)


def plan_for_prefix(
    s3, bucket: str, prefix: str, start: dt.datetime, end: dt.datetime
) -> tuple[int, list[tuple[str, str]]]:
    """Return (markers in window, [(key, marker_version_id)] that hide an object)."""
    kw = {"Bucket": bucket, "Prefix": prefix}
    markers: list[tuple[str, str]] = []
    has_object: set[str] = set()
    while True:
        page = s3.list_object_versions(**kw)
        for marker in page.get("DeleteMarkers", []):
            if marker["IsLatest"] and start <= marker["LastModified"] <= end:
                markers.append((marker["Key"], marker["VersionId"]))
        for version in page.get("Versions", []):
            has_object.add(version["Key"])
        if not page.get("IsTruncated"):
            break
        kw.update(
            KeyMarker=page["NextKeyMarker"], VersionIdMarker=page["NextVersionIdMarker"]
        )
    return len(markers), [m for m in markers if m[0] in has_object]


def remove_markers(s3, bucket: str, plan: list[tuple[str, str]]) -> int:
    removed = 0
    for i in range(0, len(plan), 1000):
        batch = [{"Key": key, "VersionId": vid} for key, vid in plan[i : i + 1000]]
        out = s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
        if out.get("Errors"):
            raise SystemExit(f"delete_objects errors: {out['Errors'][:5]}")
        removed += len(batch)
    return removed


def main(argv: list[str] | None = None, s3=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="remove the markers (default: print the plan)",
    )
    args = parser.parse_args(argv)
    if s3 is None:
        import boto3

        s3 = boto3.client("s3")
    for prefix in LIBRARY_PREFIXES:
        in_window, plan = plan_for_prefix(s3, BUCKET, prefix, WINDOW_START, WINDOW_END)
        print(f"{prefix}: {in_window} heal-window markers, {len(plan)} hide an object")
        removed = remove_markers(s3, BUCKET, plan) if args.apply else 0
        print(f"  removed={removed} dry_run={not args.apply}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
