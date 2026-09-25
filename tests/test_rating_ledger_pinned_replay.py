"""A shadow replay of D26 builds on the ledger v1 built on, not the one v1 wrote.

`alpha-engine-config-I11231`. `market_data/technicals/rating_history/_manifest.json`
is a read-modify-write key: D26 reads it, adds the trading day as `basis: "live"`,
and writes it back. The same-day shadow run executes after v1's D26, so reading the
CURRENT object found the day already live, and the shadow correctly declined to
rewrite an immutable date. On 09-21, 09-22 and 09-23 that made the key
`not_applicable` on every parity report: honest, but never comparable.

These tests drive the REAL interceptor and the REAL `shadow.pinned_inputs.pin_for`
against a versioned in-memory bucket installed at the botocore boundary, so what is
graded is the path a shadow run actually takes:

* under a replay, the manifest is read at the version current when v1's D26
  STARTED, and the shadow writes its own live date and manifest under the root;
* the pinned read does not trip the interceptor's "unclassified read of a key the
  run also writes" guard, while an unpinned current-object read still does;
* outside a replay nothing changes, and the immutable-date no-op stays an
  auto-skip (`not_applicable`), never a failure.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import boto3
import botocore.client
import pytest
from botocore.exceptions import ClientError

from collectors import metron_market_data as mmd
from collectors import technical_rating_ledger as trl
from features.feature_engineer import RATING_VERSION
from shadow import interceptor
from shadow.root import ShadowGuardViolation, ShadowRoot, activate, deactivate

DAY = "2026-09-23"
PRIOR = "2026-09-22"
ROOT = ShadowRoot(dt.date.fromisoformat(DAY))
BUCKET = "alpha-engine-research"
MANIFEST = trl.RATING_LEDGER_MANIFEST_KEY
UTC = dt.timezone.utc


def _business_days(n: int, end: str) -> list[str]:
    out: list[str] = []
    d = dt.date.fromisoformat(end)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= dt.timedelta(days=1)
    return sorted(out)


class VersionedS3:
    """A versioned bucket, as botocore's ``_make_api_call`` would see it."""

    def __init__(self) -> None:
        # key -> [(last_modified, version_id, body)], oldest first
        self.versions: dict[str, list[tuple[dt.datetime, str, bytes]]] = {}
        self.puts: list[str] = []
        self.reads: list[tuple[str, str, str | None]] = []
        self.operations: list[str] = []
        self._n = 0

    def seed(self, key: str, body: dict, when: str) -> str:
        self._n += 1
        vid = f"v{self._n}"
        stamp = dt.datetime.fromisoformat(when).replace(tzinfo=UTC)
        self.versions.setdefault(key, []).append((stamp, vid, json.dumps(body).encode()))
        return vid

    def current(self, key: str) -> dict | None:
        rows = self.versions.get(key)
        return json.loads(rows[-1][2]) if rows else None

    def __call__(self, client, operation: str, params: dict):  # the _make_api_call signature
        self.operations.append(operation)
        key = params.get("Key")
        if operation == "PutObject":
            body = params.get("Body", b"")
            if hasattr(body, "read"):
                body = body.read()
            if isinstance(body, str):
                body = body.encode()
            self._n += 1
            self.versions.setdefault(key, []).append(
                (dt.datetime(2026, 9, 23, 22, 54, tzinfo=UTC), f"v{self._n}", body)
            )
            self.puts.append(key)
            return {"ETag": '"e"', "VersionId": f"v{self._n}"}
        if operation == "GetObject":
            vid = params.get("VersionId")
            self.reads.append((operation, key, vid))
            rows = self.versions.get(key) or []
            if vid is not None:
                rows = [r for r in rows if r[1] == vid]
            if not rows:
                raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, operation)
            return {"Body": io.BytesIO(rows[-1][2]), "VersionId": rows[-1][1]}
        if operation == "ListObjectsV2":
            keys = sorted(k for k in self.versions if k.startswith(params.get("Prefix", "")))
            return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}
        if operation == "ListObjectVersions":
            prefix = params.get("Prefix", "")
            out = [
                {"Key": k, "LastModified": when, "VersionId": vid}
                for k, rows in self.versions.items() if k.startswith(prefix)
                for when, vid, _ in rows
            ]
            return {"Versions": out, "IsTruncated": False}
        raise AssertionError(f"VersionedS3 does not model {operation}")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 5)


@pytest.fixture
def bucket():
    """The bucket as it stands when the same-day shadow run starts: v1's D26 has
    already added DAY to the manifest (20:15:15Z), after starting at 20:15:06Z."""
    s3 = VersionedS3()
    days = _business_days(40, DAY)
    series = {
        "SPY": [[d, 500.0 + i] for i, d in enumerate(days)],
        "AAPL": [[d, 200.0 + (i % 7)] for i, d in enumerate(days)],
    }
    s3.seed(
        mmd.CONSOLIDATED_CLOSE_HISTORY_KEY,
        {"schema_version": 5, "adjustment_basis": "dividend_adjusted", "series": series,
         "currency": {s: "USD" for s in series}},
        "2026-09-23T20:14:00",
    )
    prior_dates = [d for d in days if d < DAY][-5:]
    assert prior_dates[-1] == PRIOR

    def _entries(dates, live):
        return [
            {"date": d, "basis": "live" if d in live else "backfill", "rating_version": RATING_VERSION}
            for d in dates
        ]

    s3.pre_v1 = s3.seed(
        MANIFEST, {"schema_version": 1, "dates": _entries(prior_dates, {PRIOR})},
        "2026-09-22T20:18:09",
    )
    s3.seed(
        MANIFEST, {"schema_version": 1, "dates": _entries(prior_dates + [DAY], {PRIOR, DAY})},
        "2026-09-23T20:15:15",
    )
    s3.seed(
        f"data_collection/runs/D26/{DAY}/01M37YGPZWD7R5A9556E2XG1EW.json",
        {"unit_id": "D26", "trading_day": DAY, "started": "2026-09-23T20:15:06Z",
         "finished": "2026-09-23T20:15:15Z", "status": "ok", "inputs": []},
        "2026-09-23T20:15:16",
    )
    return s3


@pytest.fixture
def shadow(bucket):
    real = botocore.client.BaseClient._make_api_call
    activate(ROOT)
    interceptor._ORIGINAL = bucket
    try:
        yield bucket
    finally:
        interceptor._ORIGINAL = real
        deactivate()
    assert botocore.client.BaseClient._make_api_call is real


def test_a_same_day_replay_builds_on_the_pre_v1_manifest_and_writes_a_comparable_one(shadow):
    result = trl.collect_rating_ledger(bucket=BUCKET, run_date=DAY, s3_client=boto3.client("s3"))

    assert result["status"] == "ok"
    assert result["live_written"] is True
    assert result["backfill_written"] == 0
    assert "auto_skipped" not in result
    # Read at the version current at v1's D26 START, not the current object.
    assert ("GetObject", MANIFEST, shadow.pre_v1) in shadow.reads
    # Written under the shadow root only; the live manifest is untouched.
    assert shadow.puts == [ROOT.key(f"{trl.RATING_LEDGER_PREFIX}{DAY}.json"), ROOT.key(MANIFEST)]
    assert shadow.current(ROOT.key(MANIFEST)) == shadow.current(MANIFEST), (
        "the shadow's manifest must be the same document v1 wrote, so parity can grade it"
    )


def test_the_pinned_read_is_not_an_input_read_but_a_current_object_read_still_is():
    ledger = interceptor.RunLedger()
    pinned = interceptor.rewrite_params(
        "GetObject", {"Bucket": BUCKET, "Key": MANIFEST, "VersionId": "v1"},
        service="s3", root=ROOT, ledger=ledger,
    )
    assert pinned == {"Bucket": BUCKET, "Key": MANIFEST, "VersionId": "v1"}
    interceptor.rewrite_params(
        "PutObject", {"Bucket": BUCKET, "Key": MANIFEST, "Body": b"{}"},
        service="s3", root=ROOT, ledger=ledger,
    )

    ledger = interceptor.RunLedger()
    interceptor.rewrite_params(
        "GetObject", {"Bucket": BUCKET, "Key": MANIFEST}, service="s3", root=ROOT, ledger=ledger,
    )
    with pytest.raises(ShadowGuardViolation, match="unclassified read of a key the run also writes"):
        interceptor.rewrite_params(
            "PutObject", {"Bucket": BUCKET, "Key": MANIFEST, "Body": b"{}"},
            service="s3", root=ROOT, ledger=ledger,
        )


def test_an_unpinnable_replay_still_records_the_no_op_as_an_auto_skip(shadow):
    """No v1 D26 manifest for the day: nothing to pin to, so the current object is read.
    DAY is already live there, and the result is the immutable-date auto-skip that
    `_phase_collect` records as `not_applicable` -- never `failed`."""
    del shadow.versions[f"data_collection/runs/D26/{DAY}/01M37YGPZWD7R5A9556E2XG1EW.json"]
    result = trl.collect_rating_ledger(bucket=BUCKET, run_date=DAY, s3_client=boto3.client("s3"))
    assert result["live_written"] is False
    assert result["auto_skipped"] is True
    assert result["skip_reason"] == "target date already live-published, immutable"
    assert shadow.puts == []


def test_production_reads_the_current_manifest_and_never_lists_versions(bucket):
    """No shadow root: `pin_for` answers `unpinned` without touching S3."""
    real = botocore.client.BaseClient._make_api_call
    botocore.client.BaseClient._make_api_call = lambda client, op, params: bucket(client, op, params)
    try:
        result = trl.collect_rating_ledger(
            bucket=BUCKET, run_date=DAY, s3_client=boto3.client("s3"),
        )
    finally:
        botocore.client.BaseClient._make_api_call = real
    assert result["auto_skipped"] is True
    assert ("GetObject", MANIFEST, None) in bucket.reads
    assert not any(vid for _, key, vid in bucket.reads if key == MANIFEST)
    assert "ListObjectVersions" not in bucket.operations
    assert "ListObjectsV2" not in bucket.operations
    assert bucket.puts == []


# ---------------------------------------------------------------------------
# The same-day shadow of 2026-09-24 (alpha-engine-config-I11231, again)
# ---------------------------------------------------------------------------
#
# PR1926's pin reached `_current_at` on the same-day shadow of 2026-09-24 (it is
# keyed off the active shadow root, not off a replay-only flag) and died there:
# `alpha-engine-executor-role` holds no `s3:ListBucketVersions`, so the listing
# read AccessDenied, the pin fell back to the CURRENT object -- v1's own
# 20:20:15Z write -- and D26 recorded `not_applicable` again. v1's D26 manifest
# carried `inputs: []`, so the DECLARED pin that needs no listing at all was
# never available either. These pin both halves of that.


class _NoVersionListing(VersionedS3):
    """The live executor role: GetObject yes, ListObjectVersions AccessDenied."""

    def __call__(self, client, operation: str, params: dict):
        if operation == "ListObjectVersions":
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "not authorized to perform: s3:ListBucketVersions"}},
                operation,
            )
        return super().__call__(client, operation, params)


def _production(bucket_model, **kw):
    real = botocore.client.BaseClient._make_api_call
    botocore.client.BaseClient._make_api_call = lambda client, op, params: bucket_model(client, op, params)
    try:
        return trl.collect_rating_ledger(bucket=BUCKET, s3_client=boto3.client("s3"), **kw)
    finally:
        botocore.client.BaseClient._make_api_call = real


def test_production_declares_the_ledger_version_it_built_on(bucket):
    """v1's D26 hands back the manifest version it read, in the closed `InputRef`
    shape, so a later shadow of the day has a DECLARED pin."""
    # Roll the bucket back to the moment v1's D26 starts: DAY not yet recorded.
    bucket.versions[MANIFEST] = bucket.versions[MANIFEST][:1]
    result = _production(bucket, run_date=DAY)
    assert result["live_written"] is True
    assert result["input_refs"] == [
        {"key": f"s3://{BUCKET}/{MANIFEST}", "etag": None, "version": bucket.pre_v1, "schema_version": None},
    ]


def test_the_immutable_no_op_still_names_the_version_it_read(bucket):
    """The no-op is recorded as what it is: the ledger read at a version that
    already carried the day. A shadow that read v1's own write says so."""
    result = _production(bucket, run_date=DAY)
    assert result["auto_skipped"] is True
    (ref,) = result["input_refs"]
    assert ref["key"] == f"s3://{BUCKET}/{MANIFEST}"
    assert ref["version"] == bucket.versions[MANIFEST][-1][1]


@pytest.fixture
def unlistable():
    """The 2026-09-24 same-day shadow's bucket, as the live executor role sees it."""
    s3 = _NoVersionListing()
    days = _business_days(40, DAY)
    series = {
        "SPY": [[d, 500.0 + i] for i, d in enumerate(days)],
        "AAPL": [[d, 200.0 + (i % 7)] for i, d in enumerate(days)],
    }
    s3.seed(
        mmd.CONSOLIDATED_CLOSE_HISTORY_KEY,
        {"schema_version": 5, "adjustment_basis": "dividend_adjusted", "series": series,
         "currency": {s: "USD" for s in series}},
        "2026-09-23T20:14:00",
    )
    prior_dates = [d for d in days if d < DAY][-5:]
    entries = [{"date": d, "basis": "live" if d == PRIOR else "backfill", "rating_version": RATING_VERSION}
               for d in prior_dates]
    s3.pre_v1 = s3.seed(MANIFEST, {"schema_version": 1, "dates": entries}, "2026-09-22T20:18:09")
    return s3


def _v1_manifest_then_shadow(s3):
    """Run v1's D26 in production, write its run manifest as the lib would, then
    run the same-day shadow of the day."""
    v1 = _production(s3, run_date=DAY)
    assert v1["live_written"] is True
    s3.seed(
        f"data_collection/runs/D26/{DAY}/01M3AH6KJ591P3WBR8H08T6MJV.json",
        {"unit_id": "D26", "trading_day": DAY, "started": "2026-09-23T20:15:06Z",
         "finished": "2026-09-23T20:15:15Z", "status": "ok", "inputs": v1["input_refs"]},
        "2026-09-23T20:15:16",
    )
    real = botocore.client.BaseClient._make_api_call
    activate(ROOT)
    interceptor._ORIGINAL = s3
    try:
        return trl.collect_rating_ledger(bucket=BUCKET, run_date=DAY, s3_client=boto3.client("s3"))
    finally:
        interceptor._ORIGINAL = real
        deactivate()


def test_a_same_day_shadow_pins_from_the_declared_version_without_listing_versions(unlistable):
    result = _v1_manifest_then_shadow(unlistable)

    assert "ListObjectVersions" not in unlistable.operations, "a declared pin needs no listing"
    assert result["live_written"] is True, "the shadow builds on the pre-v1 ledger, as v1 did"
    assert "auto_skipped" not in result
    assert ("GetObject", MANIFEST, unlistable.pre_v1) in unlistable.reads
    assert unlistable.current(ROOT.key(MANIFEST)) == unlistable.current(MANIFEST)
