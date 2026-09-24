"""D04 / D09 / D13 must record what they actually published — alpha-engine-config-I11469.

Measured on the 2026-09-23 weekly rehearsal (`rehearsal-2026-09-23-2`): all
three phases ran and wrote data, and all three manifests filed
``status: failed`` / ``EmptyProduction`` with ``outputs: []`` and
``rows_out: 0``:

* D04 (``fred_macro_history``) — ``reference/price_cache/{TWO,HYOAS,BAA10Y}.parquet``
  all landed at 23:05.
* D09 (``signal_returns``) — ``research.db`` and
  ``backups/research_2026-09-23.db`` both landed at 23:26.
* D13 (``arcticdb``) — a 38-minute ArcticDB backfill.

Each phase called ``_phase_collect`` with no ``artifact_key`` and no
``extra_outputs``, so nothing was ever recorded on the run context, and
``alpha-engine-config-I11011`` correctly refused to call an empty record
``ok``. The fix is each phase recording what it wrote (the D03 fix,
alpha-engine-config-I11026, one phase over), and a genuinely empty run of
each still files ``failed``.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from botocore.exceptions import ClientError

import weekly_collector
from collectors import fred_history, signal_returns


# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_prices_manifest_outputs_i11026.py)
# ---------------------------------------------------------------------------


class _PhaseCtx:
    skipped = False
    skip_reason = None

    def __init__(self):
        self.artifacts: list[str] = []

    def record_artifact(self, key: str) -> None:
        self.artifacts.append(key)


class FakeS3:
    def __init__(self, objects: dict[str, int] | None = None):
        self.objects = objects or {}
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
        self.date = "2026-09-23"
        self.bucket = "alpha-engine-research"
        self.s3_client = s3
        self.data_mode = "phase1"

    @contextmanager
    def phase(self, name, supports_auto_skip=True, **kw):
        yield _PhaseCtx()


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _manifest(s3: FakeS3) -> dict:
    (m,) = [body for key, body in s3.puts if key.startswith("data_collection/runs/")]
    return m


def _run(phase: str, result: dict, extra_outputs, objects: dict[str, int] | None = None) -> tuple[dict, FakeS3]:
    s3 = FakeS3(objects)
    weekly_collector._phase_collect(
        FakeRegistry(s3), phase, lambda: result,
        supports_auto_skip=False, extra_outputs=extra_outputs,
    )
    return _manifest(s3), s3


# ---------------------------------------------------------------------------
# D04 — fred_macro_history
# ---------------------------------------------------------------------------

_FRED_PREFIX = "predictor/price_cache/"


def _fred_result(**per_ticker) -> dict:
    return {
        "status": "ok" if all(v.get("status") == "ok" for v in per_ticker.values()) else "partial",
        "refreshed": sum(1 for v in per_ticker.values() if v.get("status") == "ok"),
        "total": len(per_ticker),
        "per_ticker": per_ticker,
        "dry_run": False,
    }


def test_fred_written_keys_reads_only_ok_series_and_nothing_on_a_dry_run():
    result = _fred_result(
        TWO={"status": "ok", "rows": 2600},
        HYOAS={"status": "error", "error": "boom"},
    )
    assert fred_history.written_keys(result, _FRED_PREFIX) == {
        "reference/price_cache/TWO.parquet": 2600
    }
    assert fred_history.written_keys({**result, "dry_run": True}, _FRED_PREFIX) == {}


def test_d04_records_every_series_parquet_it_wrote_and_files_ok():
    keys = {f"reference/price_cache/{t}.parquet": 4096 for t in ("TWO", "HYOAS", "BAA10Y")}
    result = _fred_result(
        TWO={"status": "ok", "rows": 2609},
        HYOAS={"status": "ok", "rows": 2609},
        BAA10Y={"status": "ok", "rows": 2515},
    )
    m, _ = _run(
        "fred_macro_history", result,
        weekly_collector._fred_history_extra_outputs(_FRED_PREFIX), keys,
    )
    assert m["unit_id"] == "D04"
    assert m["status"] == "ok", m.get("reason")
    assert {o["key"]: o["rows_out"] for o in m["outputs"]} == {
        "reference/price_cache/TWO.parquet": 2609,
        "reference/price_cache/HYOAS.parquet": 2609,
        "reference/price_cache/BAA10Y.parquet": 2515,
    }
    assert m["rows_out"] == 2609 + 2609 + 2515


def test_d04_that_wrote_nothing_still_files_failed():
    result = _fred_result()
    m, _ = _run(
        "fred_macro_history", result,
        weekly_collector._fred_history_extra_outputs(_FRED_PREFIX),
    )
    assert m["status"] == "failed"
    assert "EmptyProduction" in m["reason"]
    assert m["outputs"] == []


# ---------------------------------------------------------------------------
# D09 — signal_returns
# ---------------------------------------------------------------------------


def test_signal_returns_collect_reports_the_keys_it_uploaded(monkeypatch, tmp_path):
    """`collect` now returns `db_upload` — the upload helper's own record of
    the two keys it PUT — and an empty dict when it wrote nothing."""
    for step in (
        "_seed_score_performance", "_backfill_score_context", "_backfill_score_returns",
        "_seed_predictor_outcomes", "_seed_shadow_predictor_outcomes",
    ):
        monkeypatch.setattr(signal_returns, step, lambda *a, **k: {"status": "ok", "rows_written": 0})
    monkeypatch.setattr(
        signal_returns, "_backfill_outcome_records",
        lambda *a, **k: {"status": "ok", "rows_written": 3},
    )
    monkeypatch.setattr(
        signal_returns, "_backfill_predictor_returns",
        lambda *a, **k: {"status": "ok", "rows_written": 0},
    )
    monkeypatch.setattr(signal_returns, "_check_outcome_store_coverage", lambda *a, **k: {})
    monkeypatch.setattr(signal_returns, "_emit_context_coverage_metric", lambda *a, **k: {})
    monkeypatch.setattr(signal_returns, "_emit_horizon_grading_lag_metric", lambda *a, **k: {})
    monkeypatch.setattr(signal_returns.boto3, "client", lambda *a, **k: object())
    uploaded = {"pointer_key": "research.db", "backup_key": "backups/research_2026-09-23.db"}
    monkeypatch.setattr(signal_returns, "upload_research_db", lambda *a, **k: dict(uploaded))

    out = signal_returns.collect(
        bucket="b", db_path=str(tmp_path / "r.db"), run_date="2026-09-23",
    )
    assert out["status"] == "ok"
    assert out["total_written"] == 3
    assert out["db_upload"] == uploaded

    monkeypatch.setattr(
        signal_returns, "_backfill_outcome_records",
        lambda *a, **k: {"status": "ok", "rows_written": 0},
    )
    out = signal_returns.collect(
        bucket="b", db_path=str(tmp_path / "r.db"), run_date="2026-09-23",
    )
    assert out["total_written"] == 0
    assert out["db_upload"] == {}


def test_d09_records_the_research_db_keys_it_uploaded_and_files_ok():
    result = {
        "status": "ok",
        "total_written": 41,
        "db_upload": {"pointer_key": "research.db", "backup_key": "backups/research_2026-09-23.db"},
    }
    m, _ = _run(
        "signal_returns", result, weekly_collector._signal_returns_extra_outputs(),
        {"research.db": 444641280, "backups/research_2026-09-23.db": 444641280},
    )
    assert m["unit_id"] == "D09"
    assert m["status"] == "ok", m.get("reason")
    assert {o["key"]: o["rows_out"] for o in m["outputs"]} == {
        "research.db": 41,
        "backups/research_2026-09-23.db": 41,
    }


def test_d09_that_uploaded_nothing_still_files_failed():
    result = {"status": "ok", "total_written": 0, "db_upload": {}}
    m, _ = _run("signal_returns", result, weekly_collector._signal_returns_extra_outputs())
    assert m["status"] == "failed"
    assert m["outputs"] == []


# ---------------------------------------------------------------------------
# D13 — arcticdb backfill
# ---------------------------------------------------------------------------


def test_d13_records_both_arctic_libraries_graded_on_count_not_s3_head():
    """The library references are not S3 keys: grading them with a HEAD would
    read a real 38-minute write as `empty_fresh` ("does not exist")."""
    result = {
        "status": "ok", "tickers_written": 903, "tickers_skipped": 2,
        "tickers_errored": 0, "macro_dates": 2512,
    }
    m, _ = _run("arcticdb", result, weekly_collector._arcticdb_backfill_extra_outputs())
    assert m["unit_id"] == "D13"
    assert m["status"] == "ok", m.get("reason")
    assert {o["key"]: o["rows_out"] for o in m["outputs"]} == {
        "arcticdb/universe": 903,
        "arcticdb/macro": 2512,
    }
    verdicts = {g["key"]: g["verdict"] for g in m["guards"] if g.get("key")}
    assert verdicts == {"arcticdb/universe": "ok", "arcticdb/macro": "ok"}
    (metric,) = [x for x in m["metrics"] if x["name"] == "data.D13.guard.empty_fresh"]
    assert metric["status"] == "GREEN"


def test_d13_that_wrote_nothing_still_files_failed():
    result = {
        "status": "ok", "tickers_written": 0, "tickers_skipped": 0,
        "tickers_errored": 0, "macro_dates": 0,
    }
    m, _ = _run("arcticdb", result, weekly_collector._arcticdb_backfill_extra_outputs())
    assert m["status"] == "failed"
    assert "EmptyProduction" in m["reason"]
    assert m["outputs"] == []


def test_the_three_phase1_call_sites_pass_their_extra_outputs():
    """Wiring guard: the helpers above only help if `_run_phase1` passes them."""
    import inspect

    src = inspect.getsource(weekly_collector)
    for helper in (
        "_fred_history_extra_outputs(",
        "_signal_returns_extra_outputs(",
        "_arcticdb_backfill_extra_outputs(",
    ):
        assert src.count(helper) >= 2, f"{helper} is defined but never passed to _phase_collect"
