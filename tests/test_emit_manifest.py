"""Unit tests for ``rag.pipelines.emit_manifest``.

Mocks ``nousergon_lib.rag.db.execute_query`` so the manifest assembly
runs without a live pgvector connection. Verifies the manifest schema
shape, the per-source rollup math, and the S3 put-object key pattern.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest


# Each query in the module is identified by the substring that uniquely
# distinguishes it. ``_fake_execute_query`` dispatches off these substrings.
_FAKE_BY_SOURCE = [
    {"doc_type": "10-K", "documents": 432, "tickers": 430, "chunks": 12459},
    {"doc_type": "10-Q", "documents": 1287, "tickers": 428, "chunks": 38110},
    {"doc_type": "8-K", "documents": 845, "tickers": 380, "chunks": 5230},
    {"doc_type": "earnings_transcript", "documents": 612, "tickers": 410, "chunks": 18450},
    {"doc_type": "thesis", "documents": 73, "tickers": 22, "chunks": 1850},
]
_FAKE_COVERAGE = [
    {"tickers_with_any_doc": 882, "p25_docs": 8, "p50_docs": 14, "p75_docs": 21}
]
_FAKE_TOTALS = [{"documents": 3249, "chunks": 76099, "tickers": 882}]
_FAKE_INGESTION = [
    {"doc_type": "10-K", "last_ts": datetime(2026, 5, 2, 9, 23, 34, tzinfo=timezone.utc)},
    {"doc_type": "10-Q", "last_ts": datetime(2026, 5, 2, 9, 24, 1, tzinfo=timezone.utc)},
    {"doc_type": "earnings_transcript", "last_ts": datetime(2026, 5, 2, 9, 25, 12, tzinfo=timezone.utc)},
]
_FAKE_BY_DATE_SOURCE = [
    {"ingestion_date": date(2026, 5, 2), "doc_type": "10-K", "documents": 12, "chunks": 348},
    {"ingestion_date": date(2026, 5, 2), "doc_type": "10-Q", "documents": 31, "chunks": 905},
    {"ingestion_date": date(2026, 5, 2), "doc_type": "earnings_transcript", "documents": 18, "chunks": 540},
    {"ingestion_date": date(2026, 4, 25), "doc_type": "10-K", "documents": 9, "chunks": 261},
    {"ingestion_date": date(2026, 4, 25), "doc_type": "8-K", "documents": 22, "chunks": 137},
]


def _fake_execute_query(sql: str, *args, **kwargs):
    # Order matters: the date pivot also contains "GROUP BY d.doc_type" as a
    # substring (it groups by `DATE(d.ingested_at), d.doc_type`), so dispatch
    # the more specific match first.
    if "DATE(d.ingested_at)" in sql:
        return _FAKE_BY_DATE_SOURCE
    if "GROUP BY d.doc_type" in sql:
        return _FAKE_BY_SOURCE
    if "WITHIN GROUP (ORDER BY doc_count)" in sql:
        return _FAKE_COVERAGE
    if "SELECT COUNT(*) FROM rag.documents" in sql.replace("\n", " "):
        return _FAKE_TOTALS
    if "MAX(ingested_at)" in sql:
        return _FAKE_INGESTION
    raise AssertionError(f"unexpected query: {sql[:80]!r}")


@pytest.fixture
def manifest():
    from rag.pipelines import emit_manifest
    with patch.object(emit_manifest, "execute_query", side_effect=_fake_execute_query):
        return emit_manifest.build_manifest()


def test_top_level_keys(manifest):
    assert set(manifest) == {
        "generated_at", "schema_version", "totals", "by_source",
        "by_ticker_coverage", "embedding", "ingestion",
    }
    assert manifest["schema_version"] == "1.1.0"


def test_totals_match_fake(manifest):
    assert manifest["totals"] == {"documents": 3249, "chunks": 76099, "tickers": 882}


def test_by_source_rollup_shape(manifest):
    by_source = manifest["by_source"]
    assert set(by_source) == {"10-K", "10-Q", "8-K", "earnings_transcript", "thesis"}
    assert by_source["10-K"] == {"documents": 432, "tickers": 430, "chunks": 12459}
    # All values must be plain ints (JSON-serializable, no Decimal leakage).
    for entry in by_source.values():
        for v in entry.values():
            assert isinstance(v, int)


def test_coverage_percentiles(manifest):
    cov = manifest["by_ticker_coverage"]
    assert cov == {
        "tickers_with_any_doc": 882,
        "p25_docs_per_ticker": 8,
        "p50_docs_per_ticker": 14,
        "p75_docs_per_ticker": 21,
    }


def test_embedding_metadata(manifest):
    # voyage-3-lite is 512d — matches `embedding vector(512)` in the lib's
    # rag/schema.sql. pgvector enforces dim on INSERT.
    assert manifest["embedding"] == {"model": "voyage-3-lite", "dimension": 512}


def test_ingestion_overall_picks_max(manifest):
    # Overall last_run_ts must be the max across per-source timestamps.
    assert manifest["ingestion"]["last_run_ts"] == "2026-05-02T09:25:12+00:00"
    # Per-source map preserves all reported sources.
    assert set(manifest["ingestion"]["by_source_last_ts"]) == {
        "10-K", "10-Q", "earnings_transcript",
    }


def test_ingestion_by_date_source_pivot_rows(manifest):
    # Powers the dashboard's date×doc_type pivot.
    rows = manifest["ingestion"]["by_date_source"]
    assert len(rows) == 5
    # ISO-format dates so the manifest stays JSON-serializable without
    # default=str fallback.
    assert all(isinstance(r["date"], str) for r in rows)
    assert {r["date"] for r in rows} == {"2026-05-02", "2026-04-25"}
    # Counts surface as plain ints (no Decimal leakage).
    for r in rows:
        assert isinstance(r["documents"], int)
        assert isinstance(r["chunks"], int)
    # Spot-check one cell.
    cell = next(r for r in rows if r["date"] == "2026-05-02" and r["doc_type"] == "10-Q")
    assert cell == {"date": "2026-05-02", "doc_type": "10-Q", "documents": 31, "chunks": 905}


def test_manifest_is_json_serializable(manifest):
    # default=str handles datetime; the build itself should round-trip cleanly.
    payload = json.dumps(manifest, default=str)
    reloaded = json.loads(payload)
    assert reloaded["totals"]["documents"] == 3249


def test_s3_keys_use_dated_path_and_latest_pointer():
    """Verify the CLI writes to both `rag/manifest/{date}.json` and `latest.json`."""
    from rag.pipelines import emit_manifest

    captured = []

    class FakeS3:
        def put_object(self, **kwargs):
            captured.append(kwargs)

    with patch.object(emit_manifest, "execute_query", side_effect=_fake_execute_query):
        with patch("boto3.client", return_value=FakeS3()):
            import sys
            argv_save = sys.argv
            sys.argv = ["emit_manifest", "--output-s3", "--bucket", "test-bucket"]
            try:
                emit_manifest.main()
            finally:
                sys.argv = argv_save

    assert len(captured) == 2
    keys = sorted(c["Key"] for c in captured)
    assert keys[0].startswith("rag/manifest/")
    assert keys[0].endswith(".json")
    assert keys[1] == "rag/manifest/latest.json"
    assert all(c["Bucket"] == "test-bucket" for c in captured)
    assert all(c["ContentType"] == "application/json" for c in captured)
