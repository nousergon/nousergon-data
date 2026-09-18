"""Unit tests for data_gate/producers/_run_record.py (alpha-engine-config-I11058).

Covers: the key shape (one record per producer per calendar day), the
status validation (never silently coerced), and that ok/error records carry
the fields the fleet telemetry contract's execution signal requires.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from data_gate.producers import _run_record as m

UTC = dt.timezone.utc


class _FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        return {}


def test_run_record_key_is_one_per_producer_per_day():
    key = m.run_record_key("v1_data_stage", dt.date(2026, 9, 18))
    assert key == "data_collection/runs/v1_data_stage/2026-09-18.json"


def test_write_run_record_ok_carries_started_finished_duration_and_detail():
    s3 = _FakeS3()
    started = dt.datetime(2026, 9, 18, 23, 0, 0, tzinfo=UTC)
    finished = dt.datetime(2026, 9, 18, 23, 0, 12, tzinfo=UTC)
    key = m.write_run_record(
        s3,
        bucket="alpha-engine-research",
        producer="v1_data_stage",
        status="ok",
        started_at=started,
        finished_at=finished,
        detail={"executions_since_cutover": 3},
    )
    assert key == "data_collection/runs/v1_data_stage/2026-09-18.json"
    assert len(s3.puts) == 1
    put = s3.puts[0]
    assert put["Bucket"] == "alpha-engine-research"
    assert put["Key"] == key
    record = json.loads(put["Body"])
    assert record["producer"] == "v1_data_stage"
    assert record["status"] == "ok"
    assert record["started_at"] == "2026-09-18T23:00:00Z"
    assert record["finished_at"] == "2026-09-18T23:00:12Z"
    assert record["duration_seconds"] == 12.0
    assert record["error"] is None
    assert record["detail"] == {"executions_since_cutover": 3}


def test_write_run_record_error_carries_the_error_string():
    s3 = _FakeS3()
    started = dt.datetime(2026, 9, 18, 23, 0, 0, tzinfo=UTC)
    finished = dt.datetime(2026, 9, 18, 23, 0, 1, tzinfo=UTC)
    m.write_run_record(
        s3,
        bucket="alpha-engine-research",
        producer="executor_profile",
        status="error",
        started_at=started,
        finished_at=finished,
        error="boom",
    )
    record = json.loads(s3.puts[0]["Body"])
    assert record["status"] == "error"
    assert record["error"] == "boom"
    assert record["detail"] == {}


def test_write_run_record_rejects_an_unknown_status():
    s3 = _FakeS3()
    started = dt.datetime(2026, 9, 18, tzinfo=UTC)
    with pytest.raises(ValueError):
        m.write_run_record(
            s3,
            bucket="alpha-engine-research",
            producer="v1_data_stage",
            status="partial",
            started_at=started,
            finished_at=started,
        )


def test_write_run_record_keys_by_the_finished_date_not_started():
    """A run that starts just before midnight UTC and finishes just after is
    keyed by when it actually FINISHED — the terminal outcome is what a
    reader wants for that day."""
    s3 = _FakeS3()
    started = dt.datetime(2026, 9, 18, 23, 59, 0, tzinfo=UTC)
    finished = dt.datetime(2026, 9, 19, 0, 1, 0, tzinfo=UTC)
    key = m.write_run_record(
        s3,
        bucket="alpha-engine-research",
        producer="v1_data_stage",
        status="ok",
        started_at=started,
        finished_at=finished,
    )
    assert key == "data_collection/runs/v1_data_stage/2026-09-19.json"
