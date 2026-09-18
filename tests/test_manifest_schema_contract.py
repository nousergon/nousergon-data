"""Consumer-side contract test: a manifest THIS repo produces validates against
`data_run_manifest.v1` (`alpha-engine-config-I10773`, M0 contract discipline —
fleet-root `CLAUDE.md`: "every new cross-repo artifact gets a versioned
schema + producer/consumer contract test at birth").

`nousergon_lib` carries producer-side schema tests for the manifest it lifted
(``run_manifest.py``, ``contracts/data_run_manifest.schema.json``, PRs #414/
#416/#418). This repo has none — every existing test in ``tests/
test_unit_manifests.py`` asserts on SHAPE-SPECIFIC keys (``manifest["status"]
== "ok"``, a particular output key) but never runs the actual JSON Schema over
what a real call site wrote. A schema drift on either side of that boundary
(a field this repo's wrapper stops emitting, a required field the schema
adds) would go undetected by every test that exists today.

Reuses the REAL entry points ``tests/test_unit_manifests.py`` already exercises
(``collectors.daily_news`` for D36, ``collectors.metron_market_data`` for D37)
through ``run_units.recorded_entry``, with a fake sink standing in for S3 —
never a hand-typed manifest dict, which could pass by construction.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("jsonschema")
import jsonschema  # noqa: E402

import run_units  # noqa: E402
from nousergon_lib.contracts import load_schema  # noqa: E402

FAKE_SHA = "a" * 40


class FakeSink:
    """Captures what a real ``S3ManifestSink`` would have PUT — same shape as
    ``tests/test_unit_manifests.py::FakeSink``, duplicated locally rather than
    imported so this file stays independently runnable and does not couple two
    test modules' internals together."""

    bucket = "test-bucket"

    def __init__(self) -> None:
        self.writes: list[tuple[str, dict]] = []

    def write(self, key: str, payload: bytes):
        self.writes.append((key, json.loads(payload.decode("utf-8"))))
        return None

    @property
    def only(self) -> dict:
        assert len(self.writes) == 1, f"expected exactly ONE manifest, got {len(self.writes)}"
        return self.writes[0][1]


@pytest.fixture
def sink(monkeypatch) -> FakeSink:
    fake = FakeSink()
    monkeypatch.setenv("NE_DATA_CODE_SHA", FAKE_SHA)
    monkeypatch.delenv(run_units.TRIGGER_ENV, raising=False)
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: fake)
    return fake


@pytest.fixture(scope="module")
def schema() -> dict:
    return load_schema("data_run_manifest")


def _daily_news_result(status: str = "ok") -> dict:
    return {
        "status": status,
        "tickers": 61,
        "articles": 412,
        "rows": 61,
        "key": "data/news_aggregates_daily/2026-09-14/aggregates.parquet",
        "articles_status": "ok",
        "articles_key": "data/news_articles_daily/2026-09-14/articles.parquet",
        "articles_rows": 389,
        "digest_status": "ok",
        "digest_key": "data/news_digest_daily/latest.json",
        "digest_total": 24,
        "topic_status": "ok",
        "rag_status": "ok",
        "rag_documents_ingested": 389,
        "rag_documents_skipped_exists": 0,
    }


@pytest.fixture
def daily_news(monkeypatch):
    from collectors import daily_news as module

    monkeypatch.setattr(sys, "argv", ["daily_news", "--date", "2026-09-14"])
    return module


@pytest.fixture
def metron(monkeypatch):
    from collectors import metron_market_data as module

    monkeypatch.setattr(module, "collect", lambda **kw: {"status": "ok"})  # noqa: ARG005
    return module


def test_ok_manifest_validates_against_data_run_manifest_v1(sink, schema, daily_news, monkeypatch):
    """D36's normal path — a real ``recorded_entry("D36", ...)`` run through the
    real entry point, with every real key it writes on success."""
    monkeypatch.setattr(daily_news, "collect", lambda *a, **kw: _daily_news_result())  # noqa: ARG005

    assert daily_news.main() == 0
    manifest = sink.only
    assert manifest["status"] == "ok"

    jsonschema.validate(manifest, schema)


def test_failed_manifest_validates_against_data_run_manifest_v1(sink, schema, daily_news, monkeypatch):
    """D36's failure path — the manifest a dying producer writes must be as
    schema-valid as the one a healthy producer writes; a `failed` manifest is
    the one every downstream board reader depends on most for `data_success_
    without_output` / freshness grading, and it is the shape least exercised
    by hand-typed fixtures elsewhere."""
    monkeypatch.setattr(daily_news, "collect", lambda *a, **kw: _daily_news_result("error"))  # noqa: ARG005

    assert daily_news.main() == 1
    manifest = sink.only
    assert manifest["status"] == "failed"

    jsonschema.validate(manifest, schema)


def test_not_applicable_manifest_validates_against_data_run_manifest_v1(sink, schema, metron, monkeypatch):
    """D37's off-session tick — the one reachable ``not_applicable`` path in
    this repo's existing test fixtures (``tests/test_unit_manifests.py::
    test_d37_off_session_tick_is_recorded_as_not_applicable``), reused here
    against the real schema rather than hand-picked keys."""
    monkeypatch.setattr(metron, "collect_intraday", lambda **kw: {  # noqa: ARG005
        "status": "skipped", "reason": "outside US market window",
    })

    assert metron.main(["--only-intraday", "--date", "2026-09-14"]) == 0
    manifest = sink.only
    assert manifest["status"] == "not_applicable"

    jsonschema.validate(manifest, schema)
