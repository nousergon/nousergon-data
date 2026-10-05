"""The empty-but-fresh + floor guard on the entry-point units (`alpha-engine-config-I10785`, P-18).

`weekly_collector.py::_record_phase_lineage` grades every key a `_phase_collect`
unit publishes. D39, D16 and D46 publish through their own entry points
(`run_units.recorded_entry`), which never pass through that code: D39 filed no
`data_empty_fresh` reading at all, D16 filed one run-level `ok`, and D46 was
recorded as 1 row per parquet whatever the parquet held. Measured 2026-10-05,
38 D46 parquets written 2026-05-13..2026-09-24 hold ZERO rows.

`run_units.grade_published_outputs` is the shared check for those units. These
tests cover the function once, then each unit end to end through its real entry
point, each with an empty-but-fresh case and a below-floor case.
"""

from __future__ import annotations

import dataclasses
import io
import json
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from nousergon_lib.guard_mode import GuardMode

import run_units
from validators import expectations

FAKE_SHA = "b" * 40
BUCKET = "alpha-engine-research"


class FakeSink:
    def __init__(self) -> None:
        self.writes: list[tuple[str, dict]] = []

    def write(self, key: str, payload: bytes):
        self.writes.append((key, json.loads(payload.decode("utf-8"))))

    @property
    def only(self) -> dict:
        assert len(self.writes) == 1, [k for k, _ in self.writes]
        return self.writes[0][1]


@pytest.fixture
def sink(monkeypatch) -> FakeSink:
    fake = FakeSink()
    monkeypatch.setenv("NE_DATA_CODE_SHA", FAKE_SHA)
    monkeypatch.delenv(run_units.TRIGGER_ENV, raising=False)
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: fake)
    return fake


class FakeS3:
    """``objects`` maps a key to ``(body, last_modified)``; HEAD answers from it."""

    def __init__(self, objects: dict[str, tuple[bytes, datetime]] | None = None) -> None:
        self.objects = dict(objects or {})

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key][0])}

    def get_object(self, Bucket, Key):  # noqa: N803
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        return {
            "Contents": [
                {"Key": k, "LastModified": ts} for k, (_, ts) in self.objects.items() if k.startswith(Prefix)
            ],
            "IsTruncated": False,
        }


class RecordingCtx:
    """The three `UnitRun` methods the guard calls."""

    def __init__(self) -> None:
        self.guards: list[dict] = []
        self.metrics: list = []

    def record_guard(self, guard, **kw):
        self.guards.append({"guard": guard, **kw})

    def record_metric(self, record):
        self.metrics.append(record)


def _guards(manifest: dict, *, unit: str | None = None) -> list[dict]:
    out = [g for g in manifest["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name]
    if unit is not None:
        out = [g for g in out if unit in g["detail"]]
    return out


def _metric(manifest: dict, unit: str) -> dict:
    name = f"data.{unit}.guard.empty_fresh"
    found = [m for m in manifest.get("metrics") or [] if m.get("name") == name]
    assert len(found) == 1, f"expected one {name} metric, got {found}"
    return found[0]


def _floor(monkeypatch, **floors: int) -> None:
    real = run_units.rows_out_floor_for
    monkeypatch.setattr(run_units, "rows_out_floor_for", lambda uid: floors.get(uid, real(uid)))


# ───────────────────────── the shared function, once ─────────────────────────

NOW = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def _grade(outputs, s3, **kw):
    ctx = RecordingCtx()
    worst = run_units.grade_published_outputs(
        ctx, "D39", outputs, bucket=BUCKET, s3_client=s3, source_path="test", **kw
    )
    return ctx, worst


def test_a_present_key_with_rows_is_ok():
    s3 = FakeS3({"k.parquet": (b"x" * 100, NOW)})
    ctx, worst = _grade([("k.parquet", 850)], s3)
    assert worst.verdict == "ok"
    assert [g["verdict"] for g in ctx.guards] == ["ok"]
    assert ctx.guards[0]["mode"] == "observe"
    assert ctx.metrics[0].name == "data.D39.guard.empty_fresh"
    assert ctx.metrics[0].status == "GREEN"


@pytest.mark.parametrize(
    "objects,rows,needle",
    [
        ({"k.parquet": (b"", NOW)}, 850, "ZERO-BYTE"),
        ({}, 850, "does not exist"),
        ({"k.parquet": (b"x" * 100, NOW)}, 0, "with 0 rows"),
    ],
    ids=["zero-byte-object", "missing-object", "zero-rows"],
)
def test_empty_but_fresh_is_graded_empty_fresh(objects, rows, needle):
    ctx, worst = _grade([("k.parquet", rows)], FakeS3(objects))
    assert worst.verdict == "empty_fresh"
    assert needle in worst.detail
    assert ctx.metrics[0].status == "RED"


def test_a_run_that_claims_no_key_is_empty_fresh():
    ctx, worst = _grade([], FakeS3())
    assert worst.verdict == "empty_fresh"
    assert "published NO key" in worst.detail
    assert len(ctx.guards) == 1


def test_below_the_declared_floor_is_below_floor(monkeypatch):
    _floor(monkeypatch, D39=1000)
    ctx, worst = _grade([("k.parquet", 850)], FakeS3({"k.parquet": (b"x" * 100, NOW)}))
    assert worst.verdict == "below_floor"
    assert worst.baseline == 1000.0
    assert worst.value == 850.0
    assert "below its declared floor of 1000" in worst.detail
    assert ctx.metrics[0].target == 1000.0


def test_an_unreported_count_is_unmeasurable_never_a_pass():
    _, worst = _grade([("k.parquet", None)], FakeS3({"k.parquet": (b"x" * 100, NOW)}))
    assert worst.verdict == "unmeasurable"
    assert not worst.clean


def test_the_worst_key_is_the_board_metric_not_the_first():
    s3 = FakeS3({"a.json": (b"{}", NOW), "b.parquet": (b"", NOW)})
    ctx, worst = _grade([("a.json", 5), ("b.parquet", 5)], s3)
    assert [g["verdict"] for g in ctx.guards] == ["ok", "empty_fresh"]
    assert worst.key == "b.parquet"
    assert len(ctx.metrics) == 1 and ctx.metrics[0].status == "RED"


def test_observe_mode_does_not_raise_and_enforce_mode_does(monkeypatch):
    s3 = FakeS3({"k.parquet": (b"", NOW)})
    _grade([("k.parquet", 5)], s3)  # observe: recorded, no raise

    enforcing = dataclasses.replace(expectations.EMPTY_FRESH_GUARD, mode=GuardMode.ENFORCE)
    monkeypatch.setattr(expectations, "EMPTY_FRESH_GUARD", enforcing)
    with pytest.raises(run_units.EntryRunFailed, match="D39 empty_fresh guard"):
        _grade([("k.parquet", 5)], s3)


def test_severity_ranking_matches_the_phase_collect_path():
    import weekly_collector

    assert run_units.EMPTY_FRESH_SEVERITY == weekly_collector._EMPTY_FRESH_SEVERITY


# ─────────────────────────── D39 — inst_ownership ───────────────────────────


class _Row:
    quarter = "2026Q1"
    ticker = "AAPL"
    n_funds_holding = 812
    total_shares_held = 1_234_567.0


def _d39_run(monkeypatch, s3: FakeS3, n_rows: int = 850):
    from data.derived import inst_ownership as module

    monkeypatch.setattr(sys, "argv", ["inst_ownership", "--from-membership"])
    monkeypatch.setattr(module, "load_universe_from_membership", lambda **kw: ["AAPL", "MSFT"])  # noqa: ARG005
    monkeypatch.setitem(sys.modules, "boto3", type("_B", (), {"client": staticmethod(lambda *a, **kw: s3)}))
    rows = [_Row() for _ in range(n_rows)]
    monkeypatch.setattr(module, "compute_and_write_inst_ownership", lambda *a, **kw: rows)  # noqa: ARG005
    module.main()


D39_KEYS = ("data/inst_ownership/2026Q1/latest.parquet", "data/inst_ownership/latest.json")


def test_d39_clean_run_grades_both_keys_ok(sink, monkeypatch):
    _d39_run(monkeypatch, FakeS3({k: (b"x" * 200, NOW) for k in D39_KEYS}))
    manifest = sink.only
    assert manifest["status"] == "ok"
    assert {(g["key"], g["verdict"]) for g in _guards(manifest)} == {(k, "ok") for k in D39_KEYS}
    assert _metric(manifest, "D39")["status"] == "GREEN"


def test_d39_empty_but_fresh_parquet_is_graded_on_the_record(sink, monkeypatch):
    """The count says 850; the object that landed is zero bytes."""
    objects = {k: (b"x" * 200, NOW) for k in D39_KEYS}
    objects[D39_KEYS[0]] = (b"", NOW)
    _d39_run(monkeypatch, FakeS3(objects))

    manifest = sink.only
    assert manifest["status"] == "ok"  # observe mode: the exit code and status do not move
    verdicts = {g["key"]: g["verdict"] for g in _guards(manifest)}
    assert verdicts == {D39_KEYS[0]: "empty_fresh", D39_KEYS[1]: "ok"}
    assert _metric(manifest, "D39")["status"] == "RED"


def test_d39_below_its_declared_floor(sink, monkeypatch):
    _floor(monkeypatch, D39=1000)
    _d39_run(monkeypatch, FakeS3({k: (b"x" * 200, NOW) for k in D39_KEYS}), n_rows=850)

    graded = _guards(sink.only)
    assert {g["verdict"] for g in graded} == {"below_floor"}
    assert all(g["value"] == 850.0 and g["baseline"] == 1000.0 for g in graded)
    assert _metric(sink.only, "D39")["status"] == "RED"


# ──────────────────── D16 + D46 — rag-weekly-ingestion ──────────────────────

SINCE = datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc)
TS = SINCE + timedelta(seconds=5)
D46_PARQUET = "data/insider_transactions/2610031212_result.parquet"
D46_LATEST = "data/insider_transactions/latest.json"


def _parquet(n_rows: int) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame({"ticker": ["AAPL"] * n_rows, "shares": [1.0] * n_rows}).to_parquet(buf, index=False)
    return buf.getvalue()


def _d16_objects(*, d46_rows: int | None = 4962, documents: int = 98340) -> dict:
    manifest_body = json.dumps({"totals": {"documents": documents}}).encode()
    objects = {
        "rag/manifest/2026-10-02.json": (manifest_body, TS),
        "rag/manifest/latest.json": (manifest_body, TS),
        "rag/filing_changes/2026-10-02.json": (json.dumps({"n_analyzed": 1649}).encode(), TS),
        "rag/corpus_freshness/latest.json": (json.dumps({"status": "fresh"}).encode(), TS),
        "health/rag_ingestion_progress/2026-10-02.json": (json.dumps({"step": 10}).encode(), TS),
    }
    if d46_rows is not None:
        objects[D46_PARQUET] = (_parquet(d46_rows), TS)
        objects[D46_LATEST] = (json.dumps({"artifact_key": D46_PARQUET, "row_count": d46_rows}).encode(), TS)
    return objects


def _d16_run(monkeypatch, objects: dict) -> dict:
    from rag.pipelines import run_weekly_ingestion_recorded as module

    monkeypatch.setattr(module, "_utcnow", lambda: SINCE)
    monkeypatch.setattr(module, "_run_ingestion_script", lambda dry_run, run_date, yield_dir=None: 0)  # noqa: ARG005
    monkeypatch.setattr(module, "_s3_client", lambda: FakeS3(objects))
    return module.main(["--date", "2026-10-02"])


def test_d16_and_d46_clean_run_grades_every_key_under_its_own_unit(sink, monkeypatch):
    assert _d16_run(monkeypatch, _d16_objects()) == 0
    manifest = sink.only
    keyed = {o["key"]: o["rows_out"] for o in manifest["outputs"]}
    assert keyed[D46_PARQUET] == 4962  # measured from the footer, no longer the singleton 1
    assert keyed[D46_LATEST] == 4962
    graded = {g["key"]: g["verdict"] for g in _guards(manifest)}
    assert graded == {k: "ok" for k in _d16_objects()}
    assert _metric(manifest, "D16")["status"] == "GREEN"
    assert _metric(manifest, "D46")["status"] == "GREEN"


def test_d46_zero_row_parquet_is_empty_but_fresh(sink, monkeypatch):
    """The exact write D46 made 38 times between 2026-05-13 and 2026-09-24."""
    assert _d16_run(monkeypatch, _d16_objects(d46_rows=0)) == 0  # observe: exit code unchanged
    manifest = sink.only
    assert manifest["status"] == "ok"
    keyed = {o["key"]: o["rows_out"] for o in manifest["outputs"]}
    assert keyed[D46_PARQUET] == 0
    d46 = {g["key"]: g["verdict"] for g in _guards(manifest, unit="D46")}
    assert d46 == {D46_PARQUET: "empty_fresh", D46_LATEST: "empty_fresh"}
    assert _metric(manifest, "D46")["status"] == "RED"
    assert _metric(manifest, "D16")["status"] == "GREEN"


def test_d46_writing_nothing_under_its_prefix_is_empty_fresh(sink, monkeypatch):
    assert _d16_run(monkeypatch, _d16_objects(d46_rows=None)) == 0
    d46 = _guards(sink.only, unit="D46")
    assert [g["verdict"] for g in d46] == ["empty_fresh"]
    assert "published NO key" in d46[0]["detail"]
    assert _metric(sink.only, "D46")["status"] == "RED"


def test_d46_below_its_declared_floor(sink, monkeypatch):
    _floor(monkeypatch, D46=5000)
    assert _d16_run(monkeypatch, _d16_objects(d46_rows=4962)) == 0
    d46 = _guards(sink.only, unit="D46")
    assert {g["verdict"] for g in d46} == {"below_floor"}
    assert _metric(sink.only, "D46")["status"] == "RED"
    assert _metric(sink.only, "D16")["status"] == "GREEN"  # D46's floor is D46's, not D16's


def test_d16_key_with_zero_documents_is_empty_but_fresh(sink, monkeypatch):
    """The per-key half D16's descriptor listed as remaining work."""
    assert _d16_run(monkeypatch, _d16_objects(documents=0)) == 0
    d16 = {g["key"]: g["verdict"] for g in _guards(sink.only, unit="D16")}
    assert d16["rag/manifest/latest.json"] == "empty_fresh"
    assert d16["rag/filing_changes/2026-10-02.json"] == "ok"
    assert _metric(sink.only, "D16")["status"] == "RED"


def test_an_unreadable_d46_parquet_is_unmeasurable(sink, monkeypatch):
    objects = _d16_objects()
    objects[D46_PARQUET] = (b"PAR1-not-a-parquet", TS)
    assert _d16_run(monkeypatch, objects) == 0
    graded = {g["key"]: g["verdict"] for g in _guards(sink.only, unit="D46")}
    assert graded[D46_PARQUET] == "unmeasurable"


def test_the_entry_points_reach_the_shared_guard():
    """Every unit that calls the function, by literal id, is the set this PR wires."""
    from tests.test_guard_declarations_read_the_code import _published_graded_units

    assert {"D16", "D39", "D46"} <= _published_graded_units()
