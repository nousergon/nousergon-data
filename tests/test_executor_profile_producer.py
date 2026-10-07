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



def _batch_delete_child(key, role_arn, *, bucket="alpha-engine-research"):
    """The per-key event CloudTrail logs for one key of a ``DeleteObjects``
    batch: ``requestParameters`` is null and the object is only in
    ``resources[]`` (shape copied from a live 2026-09-28 archive record)."""
    record = _record("DeleteObject", key, role_arn, bucket=bucket)
    record["requestParameters"] = None
    record["resources"] = [
        {"accountId": "711398986525", "type": "AWS::S3::Bucket", "ARN": f"arn:aws:s3:::{bucket}"},
        {"type": "AWS::S3::Object", "ARN": f"arn:aws:s3:::{bucket}/{key}"},
    ]
    record["additionalEventData"] = {"parentRequestID": "HA5SRS3HWH35JCJT"}
    return record


def test_batch_delete_child_events_count_from_their_resources():
    key = "AWSLogs/711398986525/CloudTrail/us-east-1/2026/09/28/obj.json.gz"
    batch_parent = _record("DeleteObjects", "", _EXECUTOR)  # the batch call itself: no key, never counted
    batch_parent["requestParameters"] = {"bucketName": "alpha-engine-research", "delete": ""}
    records = [
        batch_parent,
        _batch_delete_child("arcticdb/shadow_universe/sl/a", _EXECUTOR),
        _batch_delete_child("arcticdb/shadow_universe/sl/b", _EXECUTOR),
        _batch_delete_child("arcticdb/shadow_universe/sl/c", _COLLECTOR),  # component 1, not the executor
        _batch_delete_child("signals/x.json", _EXECUTOR),  # outside collection prefixes
        _batch_delete_child("arcticdb/universe/x", _EXECUTOR, bucket="some-other-bucket"),
    ]
    s3 = _FakeS3({key: records})
    result = m.count_collection_writes(
        s3,
        archive_bucket="archive",
        archive_prefix="AWSLogs/711398986525/CloudTrail",
        region="us-east-1",
        start=_day(2026, 9, 28),
        end=_day(2026, 9, 28),
    )
    assert result.collection_writes == 2


def test_a_record_with_neither_params_nor_an_object_resource_is_not_a_write():
    record = _record("DeleteObject", "arcticdb/x", _EXECUTOR)
    record["requestParameters"] = None
    record["resources"] = [{"type": "AWS::S3::Bucket", "ARN": "arn:aws:s3:::alpha-engine-research"}]
    assert not m._is_collection_write(record, object_bucket="alpha-engine-research")

class _LatencyS3(_FakeS3):
    """`_FakeS3` whose every GetObject blocks for a fixed round-trip and
    records how many were in flight at once."""

    def __init__(self, objects, *, latency_s):
        super().__init__(objects)
        import threading

        self._latency_s = latency_s
        self._lock = threading.Lock()
        self._in_flight = 0
        self.peak_in_flight = 0

    def get_object(self, Bucket, Key):
        import time

        with self._lock:
            self._in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self._in_flight)
        try:
            time.sleep(self._latency_s)
            return super().get_object(Bucket, Key)
        finally:
            with self._lock:
                self._in_flight -= 1


def _day_of_objects(n, *, day="2026/09/01"):
    """`n` archive objects for one day; object i carries one executor write
    whose key encodes i, plus one non-matching record."""
    return {
        f"AWSLogs/711398986525/CloudTrail/us-east-1/{day}/obj-{i:04d}.json.gz": [
            _record("PutObject", f"market_data/obj-{i:04d}.json", _EXECUTOR),
            _record("GetObject", f"market_data/obj-{i:04d}.json", _EXECUTOR),
        ]
        for i in range(n)
    }


def test_archive_objects_are_fetched_concurrently():
    """alpha-engine-config-I11781: the walk is round-trip-bound, and a serial
    walk of a ~15,000-object window overran the job's 15-minute cap every
    night from 2026-09-29. The fetches must overlap."""
    s3 = _LatencyS3(_day_of_objects(40), latency_s=0.05)
    result = m.iter_archive_records(
        s3,
        bucket="archive",
        prefix="AWSLogs/711398986525/CloudTrail",
        region="us-east-1",
        day=_day(2026, 9, 1),
        keep=lambda r: r["eventName"] == "PutObject",
        workers=8,
    )
    assert s3.peak_in_flight > 1
    assert s3.peak_in_flight <= 8
    assert result.objects_read == 40
    assert result.records_scanned == 80


def test_concurrent_walk_matches_a_serial_walk_exactly():
    """Same records, same order, same counts with 1 worker or many — the
    concurrency changes how long the walk takes and nothing it reports."""
    objects = _day_of_objects(25)
    kwargs = dict(
        bucket="archive",
        prefix="AWSLogs/711398986525/CloudTrail",
        region="us-east-1",
        day=_day(2026, 9, 1),
        keep=lambda r: r["eventName"] == "PutObject",
    )
    serial = m.iter_archive_records(_LatencyS3(objects, latency_s=0), workers=1, **kwargs)
    parallel = m.iter_archive_records(_LatencyS3(objects, latency_s=0), workers=16, **kwargs)
    assert parallel == serial
    assert [r["requestParameters"]["key"] for r in parallel.records] == [
        f"market_data/obj-{i:04d}.json" for i in range(25)
    ]


def test_a_failed_object_fetch_still_propagates():
    class _OneBadObject(_FakeS3):
        def get_object(self, Bucket, Key):
            if Key.endswith("obj-0003.json.gz"):
                raise RuntimeError("AccessDenied")
            return super().get_object(Bucket, Key)

    import pytest

    with pytest.raises(RuntimeError, match="AccessDenied"):
        m.iter_archive_records(
            _OneBadObject(_day_of_objects(8)),
            bucket="archive",
            prefix="AWSLogs/711398986525/CloudTrail",
            region="us-east-1",
            day=_day(2026, 9, 1),
            keep=lambda r: True,
            workers=4,
        )


def test_main_sizes_the_connection_pool_to_the_worker_count(monkeypatch):
    """botocore's default pool is 10 connections; without matching it the
    extra workers would queue on the pool and the overlap would cap at 10."""
    s3 = _PutCapturingS3({})
    seen = {}

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None, config=None):
            seen["config"] = config
            return s3

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())
    assert m.main(["--days", "1", "--workers", "24"]) == 0
    assert seen["config"].max_pool_connections == 24


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


def test_main_writes_the_metric_document(monkeypatch, capsys):
    s3 = _PutCapturingS3({})

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None, config=None):
            assert name == "s3"
            return s3

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())
    rc = m.main(["--days", "3"])
    assert rc == 0
    # Three PUTs: the metric document, the write set (alpha-engine-config-
    # I11063), then the run record (alpha-engine-config-I11058).
    assert len(s3.puts) == 3
    body = json.loads(s3.puts[0]["Body"])
    assert body["collection_writes"] == 0
    assert body["days_covered"] == 0  # no archive objects in this fake
    assert s3.puts[0]["Key"] == m.DEFAULT_KEY

    write_set = json.loads(s3.puts[1]["Body"])
    assert s3.puts[1]["Key"] == m.WRITE_SET_KEY
    assert write_set["schema_version"] == m.WRITE_SET_SCHEMA
    assert write_set["groups"] == []
    assert write_set["days_covered"] == 0

    run_record = json.loads(s3.puts[2]["Body"])
    assert s3.puts[2]["Key"].startswith("data_collection/runs/executor_profile/")
    assert run_record["producer"] == "executor_profile"
    assert run_record["status"] == "ok"
    assert run_record["error"] is None

    # alpha-engine-config-I11274 (CodeQL: clear-text logging of sensitive
    # information — the sibling finding on cost_monthly.py, same class
    # normalized here even though this producer's own metric carries no
    # ARNs/principals today). Stdout carries the S3 key, the coverage count
    # and a status word only.
    out = capsys.readouterr().out
    assert m.DEFAULT_KEY in out
    assert "days_covered=0" in out


def test_stdout_never_carries_a_cloudtrail_write_event_even_if_the_metric_grew_one(
    monkeypatch, capsys
):
    """Regression guard for the class, not just today's shape:
    `build_metric` deliberately excludes `WriteCount.write_events` (the raw
    CloudTrail records, which carry principal ARNs and object keys). This
    pins the PRINT side independently, so a future change that folds
    `write_events` into the metric dict does not silently start leaking it
    to this public repo's Actions log — it would have to touch this
    assertion too."""
    key = "AWSLogs/711398986525/CloudTrail/us-east-1/2026/09/01/obj.json.gz"
    record = _record("PutObject", "market_data/weekly/2026-09-01/bundle.json", _EXECUTOR)
    s3 = _PutCapturingS3({key: [record]})

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None, config=None):
            return s3

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())
    rc = m.main(["--days", "1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha-engine-executor-role" not in out
    assert "market_data/weekly" not in out
    assert "arn:aws" not in out


def test_main_writes_an_error_run_record_and_still_raises(monkeypatch):
    class _BrokenS3(_PutCapturingS3):
        def get_paginator(self, name):
            raise RuntimeError("boom: archive unreachable")

    s3 = _BrokenS3({})

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None, config=None):
            assert name == "s3"
            return s3

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())

    try:
        m.main(["--days", "3"])
    except RuntimeError as exc:
        assert "boom" in str(exc)
    else:
        raise AssertionError("expected the underlying RuntimeError to propagate")

    assert len(s3.puts) == 1  # only the error run record — never the metric document
    run_record = json.loads(s3.puts[0]["Body"])
    assert s3.puts[0]["Key"].startswith("data_collection/runs/executor_profile/")
    assert run_record["status"] == "error"
    assert "boom" in run_record["error"]


# ── write set (alpha-engine-config-I11063) ──────────────────────────────────


def _timed(record, when, session="i-0abc"):
    record["eventTime"] = when
    role = record["userIdentity"]["sessionContext"]["sessionIssuer"]["arn"].rsplit("/", 1)[-1]
    record["userIdentity"]["arn"] = f"arn:aws:sts::711398986525:assumed-role/{role}/{session}"
    return record


def _count(records, *, day=(2026, 10, 1)):
    key = "AWSLogs/711398986525/CloudTrail/us-east-1/%04d/%02d/%02d/obj.json.gz" % day
    return m.count_collection_writes(
        _FakeS3({key: records}),
        archive_bucket="archive",
        archive_prefix="AWSLogs/711398986525/CloudTrail",
        region="us-east-1",
        start=_day(*day),
        end=_day(*day),
    )


def test_key_group_shapes():
    assert m.key_group("research.db") == "research.db"
    assert m.key_group("health/daily_data.json") == "health/"
    assert m.key_group("trades/eod_pnl.csv") == "trades/"
    assert m.key_group("trades/logs/2026-10-01/daemon.log") == "trades/logs/"
    assert m.key_group("trades/2026-10-01/reconciliation_audit.json") == "trades/{date}/"
    assert m.key_group("signals/20261001/signals.json") == "signals/{date}/"
    assert m.key_group("data/date=2026-10-01/x.parquet") == "data/{date}/"
    assert m.key_group("arcticdb/universe/sym/AAPL/x") == "arcticdb/universe/"
    # Shapes measured in the live archive, 2026-09-28..10-04:
    assert m.key_group("arcticdb/universe1775588378382498816/tdata/x") == "arcticdb/universe{id}/"
    assert (
        m.key_group("arcticdb/shadow_20260928_universe1790636813936307712/x")
        == "arcticdb/shadow_{date}_universe{id}/"
    )
    assert m.key_group("_preflight_sweep/preflight-sweep-20261001T080010Z/a.json") == "_preflight_sweep/preflight-sweep-{date}/"
    assert m.key_group("decision_artifacts/2026/10/01/x.json") == "decision_artifacts/{date}/"
    assert m.key_group("predictor/model_zoo/x.json") == "predictor/model_zoo/"


def test_write_set_covers_every_executor_write_and_the_count_is_its_collection_subset():
    count = _count(
        [
            _timed(_record("PutObject", "trades/eod_pnl.csv", _EXECUTOR), "2026-10-01T21:05:00Z"),
            _timed(_record("PutObject", "trades/trades_full.csv", _EXECUTOR), "2026-10-01T21:05:01Z"),
            _timed(_record("PutObject", "market_data/weekly/2026-10-01/b.json", _EXECUTOR), "2026-10-01T22:00:00Z", "i-0def"),
            _timed(_record("GetObject", "trades/eod_pnl.csv", _EXECUTOR), "2026-10-01T21:06:00Z"),  # read
            _timed(_record("PutObject", "signals/x.json", _COLLECTOR), "2026-10-01T21:00:00Z"),  # other role
            _timed(_record("PutObject", "trades/x.csv", _EXECUTOR, bucket="other-bucket"), "2026-10-01T21:00:00Z"),
            _timed(_batch_delete_child("arcticdb/shadow_universe/sl/a", _EXECUTOR), "2026-10-01T23:00:00Z"),
        ]
    )
    assert count.collection_writes == 2  # market_data + the batch-deleted arcticdb key
    doc = m.build_write_set(count=count, as_of=dt.datetime(2026, 10, 2, tzinfo=UTC))
    assert doc["total_writes"] == 4
    assert doc["collection_writes"] == 2
    assert doc["window"] == {"start": "2026-10-01", "end": "2026-10-01"}
    assert doc["by_top_level"] == {"arcticdb/": 1, "market_data/": 1, "trades/": 2}
    groups = {g["prefix"]: g for g in doc["groups"]}
    assert set(groups) == {"trades/", "market_data/weekly/", "arcticdb/shadow_universe/"}
    trades = groups["trades/"]
    assert trades["keys"] == ["trades/eod_pnl.csv", "trades/trades_full.csv"]
    assert trades["keys_complete"] is True
    assert trades["events"] == {"PutObject": 2}
    assert trades["collection"] is False
    assert trades["days"] == ["2026-10-01"]
    assert trades["first_seen"] == "2026-10-01T21:05:00Z"
    assert trades["last_seen"] == "2026-10-01T21:05:01Z"
    assert trades["sessions"] == ["i-0abc"]
    assert groups["market_data/weekly/"]["collection"] is True
    assert groups["market_data/weekly/"]["sessions"] == ["i-0def"]
    assert groups["arcticdb/shadow_universe/"]["events"] == {"DeleteObject": 1}


def test_a_failed_write_is_counted_and_marked():
    denied = _timed(_record("PutObject", "health/x.json", _EXECUTOR), "2026-10-01T10:00:00Z")
    denied["errorCode"] = "AccessDenied"
    doc = m.build_write_set(count=_count([denied]))
    (group,) = doc["groups"]
    assert group["writes"] == 1
    assert group["failed"] == 1


def test_a_long_tail_of_keys_is_a_sample_not_a_claim():
    records = [
        _timed(_record("PutObject", f"corporate_actions/actions/{i:03d}.json", _EXECUTOR), "2026-10-01T10:00:00Z")
        for i in range(m.MAX_KEYS_PER_GROUP + 5)
    ]
    (group,) = m.build_write_set(count=_count(records))["groups"]
    assert group["prefix"] == "corporate_actions/actions/"
    assert group["writes"] == m.MAX_KEYS_PER_GROUP + 5
    assert len(group["keys"]) == m.MAX_KEYS_PER_GROUP
    assert group["keys_complete"] is False


def test_a_fanned_out_top_level_folds_into_one_group():
    n = m.MAX_GROUPS_PER_TOP_LEVEL + 1
    records = [
        _timed(_record("PutObject", f"prices/T{i:03d}/close.json", _EXECUTOR), "2026-10-01T10:00:00Z")
        for i in range(n)
    ] + [_timed(_record("PutObject", "health/x.json", _EXECUTOR), "2026-10-01T10:00:00Z")]
    doc = m.build_write_set(count=_count(records))
    groups = {g["prefix"]: g for g in doc["groups"]}
    assert set(groups) == {"prices/*", "health/"}
    assert groups["prices/*"]["writes"] == n
    assert groups["prices/*"]["merged_groups"] == n
    assert doc["total_writes"] == n + 1


def test_write_set_carries_the_metric_coverage_so_a_partial_window_reads_partial():
    count = m.WriteCount(collection_writes=0, days_requested=7, days_covered=5, uncovered_days=("2026-09-01", "2026-09-02"))
    doc = m.build_write_set(count=count)
    assert doc["days_requested"] == 7
    assert doc["days_covered"] == 5
    assert doc["uncovered_days"] == ["2026-09-01", "2026-09-02"]


def test_write_set_never_reaches_stdout_and_can_go_to_a_local_file(monkeypatch, capsys, tmp_path):
    key = "AWSLogs/711398986525/CloudTrail/us-east-1/2026/09/01/obj.json.gz"
    record = _timed(_record("PutObject", "trades/eod_pnl.csv", _EXECUTOR), "2026-09-01T21:05:00Z")
    s3 = _PutCapturingS3({key: [record]})

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None, config=None):
            return s3

    import datetime as _dt

    class _FixedDate(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return _dt.datetime(2026, 9, 2, 1, 0, tzinfo=tz)

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())
    monkeypatch.setattr(m.dt, "datetime", _FixedDate)
    out_file = tmp_path / "write_set.json"
    assert m.main(["--days", "1", "--no-write", "--write-set-file", str(out_file)]) == 0
    assert s3.puts == []  # --no-write: nothing goes to S3
    doc = json.loads(out_file.read_text())
    assert doc["total_writes"] == 1
    assert doc["groups"][0]["keys"] == ["trades/eod_pnl.csv"]
    out = capsys.readouterr().out
    assert "trades/eod_pnl.csv" not in out
    assert "i-0abc" not in out
    assert "arn:aws" not in out
