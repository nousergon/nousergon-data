"""Unit tests for data_gate/producers/v1_data_stage.py (alpha-engine-config-I11035).

Covers: `count_executions_since_cutover`'s cutover boundary (stops once an
execution predates the cutover), data-stage detection via
`GetExecutionHistory` (a Choice-routed skip that never enters a data-stage
state is NOT counted), `build_metric`'s shape (exactly the three fields
`read_v1_data_stage_quiet` parses, plus diagnostics), and `main()`'s S3
write.
"""

from __future__ import annotations

import datetime as dt
import json

from data_gate.producers import v1_data_stage as m

UTC = dt.timezone.utc


def _dt(y, mo, d, h=0, mi=0):
    return dt.datetime(y, mo, d, h, mi, tzinfo=UTC)


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return self._pages


class _FakeSFN:
    """`list_executions` returns executions newest-first (as the real API
    does); `get_execution_history` is keyed by executionArn."""

    def __init__(self, executions, histories):
        self._executions = executions
        self._histories = histories

    def get_paginator(self, name):
        if name == "list_executions":
            return _Paginator([{"executions": self._executions}])
        if name == "get_execution_history":
            class _HistPaginator:
                def __init__(self, histories):
                    self._histories = histories

                def paginate(self, executionArn, maxResults=1000):
                    return [{"events": self._histories.get(executionArn, [])}]

            return _HistPaginator(self._histories)
        raise AssertionError(f"unexpected paginator {name!r}")


def _entered(*names):
    return [{"stateEnteredEventDetails": {"name": n}} for n in names]


def test_counts_only_executions_entering_a_data_stage_state():
    cutover = _dt(2026, 9, 1)
    executions = [
        {"executionArn": "e3", "startDate": _dt(2026, 9, 10)},  # ran DataPhase1
        {"executionArn": "e2", "startDate": _dt(2026, 9, 5)},  # skipped (Choice-routed)
        {"executionArn": "e1", "startDate": _dt(2026, 8, 25)},  # BEFORE cutover
    ]
    histories = {
        "e3": _entered("CheckSkipDataPhase1", "DataPhase1"),
        "e2": _entered("CheckSkipDataPhase1"),  # routed around the Task entirely
    }
    sfn = _FakeSFN(executions, histories)
    result = m.count_executions_since_cutover(sfn, cutover=cutover)
    assert result.executions_since_cutover == 1
    assert result.data_stage_executions == ("e3",)
    # e1 predates cutover and is never even queried for its history.
    assert "e1" not in result.data_stage_executions
    assert result.executions_scanned == 2


def test_stops_paging_at_the_cutover_boundary():
    cutover = _dt(2026, 9, 1)
    executions = [
        {"executionArn": f"e{i}", "startDate": _dt(2026, 9, 1 + i)} for i in range(5, -1, -1)
    ]
    sfn = _FakeSFN(executions, {})
    result = m.count_executions_since_cutover(sfn, cutover=cutover)
    # e0 starts exactly at cutover (2026-09-01) and is included; nothing
    # before it is scanned.
    assert result.executions_scanned == 6


def test_zero_since_cutover_is_a_real_zero_not_a_gap():
    cutover = _dt(2026, 9, 1)
    sfn = _FakeSFN([], {})
    result = m.count_executions_since_cutover(sfn, cutover=cutover)
    assert result.executions_since_cutover == 0
    assert result.executions_scanned == 0


def test_build_metric_carries_exactly_the_parsed_shape():
    count = m.ExecutionCount(executions_since_cutover=2, executions_scanned=5, data_stage_executions=("a", "b"))
    metric = m.build_metric(cutover_utc="2026-09-18T00:00:00Z", count=count, as_of=_dt(2026, 9, 19))
    assert metric["cutover_utc"] == "2026-09-18T00:00:00Z"
    assert metric["executions_since_cutover"] == 2
    assert metric["as_of"] == "2026-09-19T00:00:00Z"


def test_scan_cap_raises_rather_than_silently_truncating():
    cutover = _dt(2020, 1, 1)  # far in the past: nothing predates it
    executions = [{"executionArn": f"e{i}", "startDate": _dt(2026, 9, 1)} for i in range(3)]
    sfn = _FakeSFN(executions, {})
    try:
        m.count_executions_since_cutover(sfn, cutover=cutover, scan_cap=1)
    except RuntimeError as exc:
        assert "scan_cap" in str(exc)
    else:
        raise AssertionError("expected RuntimeError on scan_cap exceeded")


class _FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        return {}


def test_main_writes_the_metric_document(monkeypatch, capsys):
    cutover = "2026-09-01T00:00:00Z"
    executions = [{"executionArn": "e1", "startDate": _dt(2026, 9, 5)}]
    histories = {"e1": _entered("DataPhase1")}
    sfn = _FakeSFN(executions, histories)
    s3 = _FakeS3()

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None):
            return {"stepfunctions": sfn, "s3": s3}[name]

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())

    rc = m.main(["--cutover-utc", cutover])
    assert rc == 0
    # Two PUTs: the metric document, then the run record (alpha-engine-config-I11058).
    assert len(s3.puts) == 2
    body = json.loads(s3.puts[0]["Body"])
    assert body["executions_since_cutover"] == 1
    assert s3.puts[0]["Bucket"] == m.DEFAULT_BUCKET
    assert s3.puts[0]["Key"] == m.DEFAULT_KEY

    run_record = json.loads(s3.puts[1]["Body"])
    assert s3.puts[1]["Key"].startswith("data_collection/runs/v1_data_stage/")
    assert run_record["producer"] == "v1_data_stage"
    assert run_record["status"] == "ok"
    assert run_record["error"] is None
    assert run_record["detail"]["executions_since_cutover"] == 1


def test_main_writes_an_error_run_record_and_still_raises(monkeypatch):
    class _BrokenSFN:
        def get_paginator(self, name):
            raise RuntimeError("boom: sfn unreachable")

    s3 = _FakeS3()

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None):
            return {"stepfunctions": _BrokenSFN(), "s3": s3}[name]

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())

    try:
        m.main(["--cutover-utc", "2026-09-01T00:00:00Z"])
    except RuntimeError as exc:
        assert "boom" in str(exc)
    else:
        raise AssertionError("expected the underlying RuntimeError to propagate")

    assert len(s3.puts) == 1  # only the error run record — never the metric document
    run_record = json.loads(s3.puts[0]["Body"])
    assert s3.puts[0]["Key"].startswith("data_collection/runs/v1_data_stage/")
    assert run_record["status"] == "error"
    assert "boom" in run_record["error"]


def test_main_no_write_skips_the_put(monkeypatch):
    sfn = _FakeSFN([], {})
    s3 = _FakeS3()

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None):
            return {"stepfunctions": sfn, "s3": s3}[name]

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())
    rc = m.main(["--no-write"])
    assert rc == 0
    assert s3.puts == []
