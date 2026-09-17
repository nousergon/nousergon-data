"""Producer contract test for rag_manifest.schema.json (D16, data-collector plan
P-07, alpha-engine-config-I10873).

Validates the REAL `rag/pipelines/emit_manifest.py::build_manifest` output
(the object written to `rag/manifest/{date}.json` + `latest.json`) against the
schema, with `nousergon_lib.rag.db.execute_query` mocked to deterministic rows
(no live pgvector), plus a hand-built minimal fixture independent of the
producer code.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("jsonschema")

from contracts import validate_rag_manifest
from rag.pipelines import emit_manifest


def _fake_execute_query(query: str, *args, **kwargs):
    q = " ".join(query.split()).lower()
    if "percentile_disc" in q:
        return [{
            "tickers_with_any_doc": 500,
            "p25_docs": 2,
            "p50_docs": 5,
            "p75_docs": 9,
        }]
    if "count(distinct d.id)" in q and "documents" in q and "group by date" in q:
        return [{
            "ingestion_date": __import__("datetime").date(2026, 9, 15),
            "doc_type": "10-K",
            "documents": 10,
            "chunks": 300,
        }]
    if "max(ingested_at)" in q:
        return [{"doc_type": "10-K", "last_ts": __import__("datetime").datetime(2026, 9, 15, 5, 0)}]
    if "group by d.doc_type" in q and "chunks" in q:
        return [
            {"doc_type": "10-K", "documents": 400, "tickers": 380, "chunks": 12000},
            {"doc_type": "10-Q", "documents": 800, "tickers": 380, "chunks": 20000},
        ]
    if "select" in q and "documents" in q and "chunks" in q and "tickers" in q:
        return [{"documents": 1200, "chunks": 32000, "tickers": 500}]
    raise AssertionError(f"unexpected query in rag manifest contract test: {query!r}")


class TestRealProducerOutputValidates:
    def test_build_manifest_validates(self):
        with patch("rag.pipelines.emit_manifest.execute_query", side_effect=_fake_execute_query):
            manifest = emit_manifest.build_manifest()
        errors = validate_rag_manifest(manifest)
        assert errors == [], errors

    def test_build_manifest_has_declared_embedding_model(self):
        with patch("rag.pipelines.emit_manifest.execute_query", side_effect=_fake_execute_query):
            manifest = emit_manifest.build_manifest()
        assert manifest["embedding"]["model"] == "voyage-3-lite"
        assert manifest["embedding"]["dimension"] == 512


class TestHandBuiltFixtureValidates:
    def _payload(self, **overrides) -> dict:
        payload = {
            "generated_at": "2026-09-16T05:00:00+00:00",
            "schema_version": "1.1.0",
            "totals": {"documents": 1200, "chunks": 32000, "tickers": 500},
            "by_source": {
                "10-K": {"documents": 400, "tickers": 380, "chunks": 12000},
            },
            "by_ticker_coverage": {
                "tickers_with_any_doc": 500,
                "p25_docs_per_ticker": 2,
                "p50_docs_per_ticker": 5,
                "p75_docs_per_ticker": 9,
            },
            "embedding": {"model": "voyage-3-lite", "dimension": 512},
            "ingestion": {
                "last_run_ts": "2026-09-15T05:00:00",
                "by_source_last_ts": {"10-K": "2026-09-15T05:00:00"},
                "by_date_source": [
                    {"date": "2026-09-15", "doc_type": "10-K", "documents": 10, "chunks": 300},
                ],
            },
        }
        payload.update(overrides)
        return payload

    def test_full_manifest_validates(self):
        assert validate_rag_manifest(self._payload()) == []

    def test_missing_required_top_level_field_is_rejected(self):
        payload = self._payload()
        del payload["totals"]
        assert validate_rag_manifest(payload) != []

    def test_null_last_run_ts_validates(self):
        payload = self._payload()
        payload["ingestion"]["last_run_ts"] = None
        assert validate_rag_manifest(payload) == []

    def test_by_date_source_entry_missing_field_is_rejected(self):
        payload = self._payload()
        del payload["ingestion"]["by_date_source"][0]["chunks"]
        assert validate_rag_manifest(payload) != []
