"""D03 (prices) must record what it actually published — alpha-engine-config-I11026.

Before this fix, `weekly_collector.py`'s ``prices`` phase declared no
``artifact_key`` (per-ticker parquet writes have no single stable key) and no
``extra_outputs`` either, so a run that refreshed hundreds of tickers still
recorded a manifest with ``rows_out: 0`` / ``outputs: []`` — measured live on
D03's 2026-09-15/16/17 runs (the 09-16 run took 3m38s and wrote real data).
Once ``alpha-engine-config-I11011`` makes an empty-outputs manifest ``failed``
rather than ``ok``, that is a failed manifest on every D03 run, which is the
honest reading, not a regression — the fix is D03 recording what it wrote,
covered here.

Two layers:

1. ``collectors/prices.py::collect``/``written_keys`` — the producer reports
   the per-ticker keys it actually uploaded, with each ticker's OWN row
   count, never a copy of the requested/stale population.
2. ``weekly_collector.py::_prices_extra_outputs`` wired into the ``prices``
   phase (both ``phase1`` and ``daily`` modes) — the manifest wrapper records
   every one of those keys, under the correct per-key row count (the
   dict-shaped ``rows_fn`` extension to the ``extra_outputs`` contract that
   ``_record_phase_lineage`` gained for this issue).
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from botocore.exceptions import ClientError

import weekly_collector
from collectors import prices


# ---------------------------------------------------------------------------
# Layer 1 — collectors/prices.py
# ---------------------------------------------------------------------------


class _FakeS3Upload:
    """Models `_refresh_stale`'s upload path only enough to prove `written`
    is populated from real uploads, not from the requested population."""

    exceptions = type("E", (), {"NoSuchKey": type("NoSuchKey", (Exception,), {})})()

    def __init__(self):
        self.uploaded: list[str] = []

    def get_object(self, Bucket, Key):
        raise self.exceptions.NoSuchKey(Key)

    def upload_file(self, _local, _bucket, key):
        self.uploaded.append(key)


def test_collect_reports_written_keys_and_rows_only_for_uploaded_tickers(monkeypatch):
    """A ticker that fails the refresh must never appear in `written` — only
    what was actually uploaded, matching `refreshed` (the published count)."""
    import pandas as pd

    def _fake_refresh_stale(s3, bucket, s3_prefix, stale, fetch_period, batch_size, *, trading_day):
        # AAPL succeeds (5 rows written); MSFT fails the refresh.
        return 1, ["MSFT"], [("AAPL", 5)], []

    monkeypatch.setattr(prices, "_refresh_stale", _fake_refresh_stale)
    monkeypatch.setattr(
        prices, "_find_stale_fast", lambda *a, **k: ["AAPL", "MSFT"]
    )

    result = prices.collect(
        bucket="alpha-engine-research",
        tickers=["AAPL", "MSFT"],
        s3_prefix="predictor/price_cache/",
        dry_run=False,
        reference_date="2026-09-16",
    )

    assert result["refreshed"] == 1
    assert result["written"] == {"AAPL": 5}
    keys = prices.written_keys(result, "predictor/price_cache/")
    assert keys == {"reference/price_cache/AAPL.parquet": 5}
    # MSFT (failed) and any never-stale ticker must never appear.
    assert "reference/price_cache/MSFT.parquet" not in keys


def test_collect_written_is_empty_when_nothing_was_stale(monkeypatch):
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: [])
    result = prices.collect(
        bucket="b", tickers=["AAPL"], s3_prefix="predictor/price_cache/",
    )
    assert result["status"] == "ok"
    assert result["refreshed"] == 0
    assert result["stale"] == 0
    assert prices.written_keys(result) == {}


def test_written_keys_reads_only_what_was_written_never_the_dry_run_sample(monkeypatch):
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: ["AAPL", "MSFT"])
    result = prices.collect(
        bucket="b", tickers=["AAPL", "MSFT"], s3_prefix="predictor/price_cache/",
        dry_run=True,
    )
    assert result["status"] == "ok_dry_run"
    assert prices.written_keys(result) == {}


# ---------------------------------------------------------------------------
# Layer 2 — weekly_collector.py wiring (manifest wrapper)
# ---------------------------------------------------------------------------


class _PhaseCtx:
    def __init__(self, skipped: bool = False):
        self.skipped = skipped
        self.skip_reason = "prior ok marker" if skipped else None
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
    def __init__(self, s3: FakeS3, mode: str = "phase1"):
        self.date = "2026-09-16"
        self.bucket = "alpha-engine-research"
        self.s3_client = s3
        self.data_mode = mode

    @contextmanager
    def phase(self, name, supports_auto_skip=True, **kw):
        yield _PhaseCtx()


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _manifests(s3: FakeS3) -> list[dict]:
    return [body for key, body in s3.puts if key.startswith("data_collection/runs/")]


def test_prices_phase_records_every_ticker_it_actually_wrote():
    """The measured D03 defect: a real 3m38s run that refreshed tickers used
    to record `rows_out: 0`, `outputs: []`, `status: ok`. It must now record
    every per-ticker key with its own row count, `rows_out > 0`, `status: ok`.
    """
    aapl_key = "reference/price_cache/AAPL.parquet"
    msft_key = "reference/price_cache/MSFT.parquet"
    s3 = FakeS3({aapl_key: 4096, msft_key: 8192})
    reg = FakeRegistry(s3, mode="phase1")

    collector_result = {
        "status": "ok",
        "refreshed": 2,
        "stale": 2,
        "failed": 0,
        "failed_tickers": [],
        "total": 900,
        "written": {"AAPL": 2515, "MSFT": 1200},
    }

    result = weekly_collector._phase_collect(
        reg, "prices",
        lambda: collector_result,
        supports_auto_skip=False,
        extra_outputs=weekly_collector._prices_extra_outputs("predictor/price_cache/"),
    )

    assert result["status"] == "ok"
    m = _manifests(s3)[0]
    assert m["unit_id"] == "D03"
    assert m["status"] == "ok"
    # `rows_out` on the manifest is the SUM over every recorded output (the
    # lib's own aggregate) — 2515 + 1200, not `unit.rows_key`'s scalar count.
    assert m["rows_out"] == 3715
    out_keys = {o["key"]: o["rows_out"] for o in m["outputs"]}
    assert out_keys == {aapl_key: 2515, msft_key: 1200}
    # Each key graded on its OWN row count, never the batch aggregate.
    verdicts = {g["key"]: g["verdict"] for g in m["guards"] if g.get("key")}
    assert verdicts == {aapl_key: "ok", msft_key: "ok"}


def test_prices_phase_that_refreshed_nothing_this_run_still_needs_a_declaration():
    """A run with nothing stale writes no per-ticker output — this is the
    surface alpha-engine-config-I11011 grades: an undeclared unit that
    records no output at all files `failed`, not `ok`."""
    s3 = FakeS3()
    reg = FakeRegistry(s3, mode="daily")

    collector_result = {
        "status": "ok", "refreshed": 0, "stale": 0, "total": 900, "written": {},
    }

    weekly_collector._phase_collect(
        reg, "prices",
        lambda: collector_result,
        supports_auto_skip=False,
        extra_outputs=weekly_collector._prices_extra_outputs("predictor/price_cache/"),
    )

    m = _manifests(s3)[0]
    # D03 declares no `empty_is_valid` — a genuinely empty run correctly
    # fails loud rather than being waved through as `ok`.
    assert m["status"] == "failed"
    assert m["outputs"] == []


def test_extra_outputs_rows_fn_dict_form_grades_each_key_on_its_own_count():
    """`_record_phase_lineage`'s dict-shaped `rows_fn` extension
    (alpha-engine-config-I11026): a mapping return grades each key by ITS OWN
    entry, defaulting an unnamed key to 0 rather than borrowing another key's
    count."""
    a_key = "extra/a.json"
    b_key = "extra/b.json"
    s3 = FakeS3({a_key: 10, b_key: 10})
    reg = FakeRegistry(s3, mode="phase1")

    result = weekly_collector._phase_collect(
        reg, "prices",
        lambda: {"status": "ok", "refreshed": 1, "written": {"a": 1, "b": 1}},
        supports_auto_skip=False,
        extra_outputs=(
            (
                lambda r: [a_key, b_key],
                lambda r: True,
                lambda r: {a_key: 7},  # b_key deliberately absent
            ),
        ),
    )
    assert result["status"] == "ok"
    m = _manifests(s3)[0]
    out_rows = {o["key"]: o["rows_out"] for o in m["outputs"]}
    assert out_rows == {a_key: 7, b_key: 0}
