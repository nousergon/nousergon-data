"""The empty-but-fresh guard applies the unit's DECLARED floor
(`alpha-engine-config-I10785`, P-18 deliverable: "applying the declared floor
from each unit's descriptor").

The field is `completeness.rows_out_floor`, the one the completion check
(`data_gate/run_manifest_predicate.py::_rows_out_floor`) already grades a
finished run against. Before this, `_record_phase_lineage` called
`check_empty_fresh` with no floor at all, so a unit that declared one was held
to it after the fact and never at publish time.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from botocore.exceptions import ClientError

import run_units
import weekly_collector
from data_gate import descriptors
from data_gate import run_manifest_predicate as predicate

KEY = "staging/daily_closes/2026-09-14.parquet"


class _PhaseCtx:
    skipped = False
    skip_reason = None

    def record_artifact(self, key: str) -> None:
        pass


class FakeS3:
    def __init__(self, objects: dict[str, int]):
        self.objects = objects
        self.puts: list[tuple[str, dict]] = []

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        self.puts.append((Key, json.loads(Body.decode("utf-8"))))
        return {"ETag": '"abc"'}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": self.objects[Key]}


class FakeRegistry:
    def __init__(self, s3: FakeS3):
        self.date = "2026-09-14"
        self.bucket = "alpha-engine-research"
        self.s3_client = s3
        self.data_mode = "daily"

    @contextmanager
    def phase(self, name, supports_auto_skip=True, **kw):
        yield _PhaseCtx()


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _guard(monkeypatch, floors: dict[str, int], rows: int) -> dict:
    monkeypatch.setattr(run_units, "_rows_out_floors", lambda: floors)
    s3 = FakeS3({KEY: 4096})
    weekly_collector._phase_collect(
        FakeRegistry(s3),
        "daily_closes",
        lambda: {"status": "ok", "tickers_captured": rows},
        artifact_key=KEY,
    )
    (manifest,) = [b for k, b in s3.puts if k.startswith("data_collection/runs/")]
    (guard,) = [g for g in manifest["guards"] if g["guard"] == "data_empty_fresh"]
    return guard


def test_a_count_below_the_declared_floor_reads_below_floor(monkeypatch):
    guard = _guard(monkeypatch, {"D19": 800}, rows=500)
    assert guard["verdict"] == "below_floor"
    assert guard["baseline"] == 800.0
    assert "below its declared floor of 800" in guard["detail"]


def test_a_count_at_the_declared_floor_is_ok(monkeypatch):
    guard = _guard(monkeypatch, {"D19": 800}, rows=800)
    assert guard["verdict"] == "ok"
    assert guard["baseline"] == 800.0


def test_no_declared_floor_leaves_only_the_non_empty_half(monkeypatch):
    assert _guard(monkeypatch, {}, rows=1)["verdict"] == "ok"
    assert _guard(monkeypatch, {}, rows=0)["verdict"] == "empty_fresh"


def test_the_guard_and_the_completion_check_read_one_declaration():
    """Every unit: the guard's floor equals the predicate's `declared` floor, and is
    None exactly where the predicate's floor is not a declaration."""
    run_units._rows_out_floors.cache_clear()
    for unit in descriptors.load_units():
        floor, source = predicate._rows_out_floor(unit.unit_id, unit.raw.get("completeness") or {})
        expected = floor if source == "declared" else None
        assert run_units.rows_out_floor_for(unit.unit_id) == expected, unit.unit_id
