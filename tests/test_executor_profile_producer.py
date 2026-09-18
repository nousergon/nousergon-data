"""Unit tests for data_gate/producers/executor_profile.py (alpha-engine-
config-I11036).

Covers: the archive walk's day-coverage contract (an uncovered day is a gap,
never a silent zero), the write filter (S3 write eventName + collection
prefix + executor-role identity, all three required), and `build_metric`'s
`days_covered` distinguishing a partial window from the full one — the
property `read_executor_collection_writes_zero` keys its MET/UNMET verdict
on.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json

from data_gate.producers import executor_profile as m

UTC = dt.timezone.utc


def _day(y, mo, d):
    return dt.date(y, mo, d)


def _record(event_name, key, role_arn, *, event_source="s3.amazonaws.com", bucket="alpha-engine-research"):
    return {
        "eventSource": event_source,
        "eventName": event_name,
        "requestParameters": {"bucketName": bucket, "key": key},
        "userIdentity": {
            "type": "AssumedRole",
            "arn": f"arn:aws:sts::711398986525:assumed-role/{role_arn.rsplit('/', 1)[-1]}/session",
            "sessionContext": {"sessionIssuer": {"type": "Role", "arn": role_arn}},
        },
    }


_EXECUTOR = "arn:aws:iam::711398986525:role/alpha-engine-executor-role"
_COLLECTOR = "arn:aws:iam::711398986525:role/nousergon-data-collection-box-role"


class _FakeS3:
    """`Bucket/<archive prefix>/<region>/<y>/<m>/<d>/<obj>.json.gz` objects,
    each a gzip'd CloudTrail `{"Records": [...]}` payload."""

    def __init__(self, objects: dict[str, list[dict]]):
        # objects: {key: [record, ...]}
        self._objects = objects

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self._objects

        class _Paginator:
            def paginate(self, Bucket, Prefix):
                keys = [k for k in objects if k.startswith(Prefix)]
                return [{"Contents": [{"Key": k} for k in keys]}]

        return _Paginator()

    def get_object(self, Bucket, Key):
        body = gzip.compress(json.dumps({"Records": self._objects[Key]}).encode())
        return {"Body": _Body(body)}


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


def test_uncovered_day_is_a_gap_not_a_zero():
    s3 = _FakeS3({})  # no archive objects delivered for any day
    result = m.count_collection_writes(
        s3,
        archive_bucket="archive",
        archive_prefix="AWSLogs/711398986525/CloudTrail",
        region="us-east-1",
        start=_day(2026, 9, 1),
        end=_day(2026, 9, 3),
    )
    assert result.days_covered == 0
    assert result.uncovered_days == ("2026-09-01", "2026-09-02", "2026-09-03")
    assert result.collection_writes == 0


def test_only_executor_writes_into_collection_prefixes_count():
    key = "AWSLogs/711398986525/CloudTrail/us-east-1/2026/09/01/obj.json.gz"
    records = [
        _record("PutObject", "market_data/weekly/2026-09-01/bundle.json", _EXECUTOR),
        _record("GetObject", "market_data/weekly/2026-09-01/bundle.json", _EXECUTOR),  # read, not a write
        _record("PutObject", "market_data/weekly/2026-09-01/bundle.json", _COLLECTOR),  # component 1, not the executor
        _record("PutObject", "signals/2026-09-01/signals.json", _EXECUTOR),  # outside collection prefixes
        _record("DeleteObject", "arcticdb/universe/x.parquet", _EXECUTOR),
    ]
    s3 = _FakeS3({key: records})
    result = m.count_collection_writes(
        s3,
        archive_bucket="archive",
        archive_prefix="AWSLogs/711398986525/CloudTrail",
        region="us-east-1",
        start=_day(2026, 9, 1),
        end=_day(2026, 9, 1),
    )
    assert result.days_covered == 1
    assert result.collection_writes == 2  # PutObject market_data + DeleteObject arcticdb, both by the executor


def test_build_metric_days_covered_distinguishes_partial_from_full_window():
    count = m.WriteCount(collection_writes=0, days_requested=7, days_covered=5, uncovered_days=("2026-09-01", "2026-09-02"))
    metric = m.build_metric(count=count, as_of=dt.datetime(2026, 9, 8, tzinfo=UTC))
    assert metric["collection_writes"] == 0
    assert metric["days_covered"] == 5
    assert metric["uncovered_days"] == ["2026-09-01", "2026-09-02"]


def test_resolve_archive_defaults_to_the_fleet_wide_trail(monkeypatch):
    monkeypatch.delenv(m.ARCHIVE_VAR, raising=False)
    bucket, prefix = m._resolve_archive(None)
    assert bucket == m.DEFAULT_ARCHIVE_BUCKET
    assert prefix == m.DEFAULT_ARCHIVE_PREFIX


def test_resolve_archive_honors_env_override(monkeypatch):
    monkeypatch.setenv(m.ARCHIVE_VAR, "s3://test-bucket/test-prefix")
    bucket, prefix = m._resolve_archive(None)
    assert bucket == "test-bucket"
    assert prefix == "test-prefix"


class _PutCapturingS3(_FakeS3):
    def __init__(self, objects):
        super().__init__(objects)
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        return {}


def test_main_writes_the_metric_document(monkeypatch):
    s3 = _PutCapturingS3({})

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None):
            assert name == "s3"
            return s3

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())
    rc = m.main(["--days", "3"])
    assert rc == 0
    assert len(s3.puts) == 1
    body = json.loads(s3.puts[0]["Body"])
    assert body["collection_writes"] == 0
    assert body["days_covered"] == 0  # no archive objects in this fake
    assert s3.puts[0]["Key"] == m.DEFAULT_KEY
