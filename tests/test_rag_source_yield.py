"""alpha-engine-config-I11472: a RAG source that returns nothing makes the run DEGRADED.

The 2026-09-23 rehearsal logged ``Total: 0 transcripts ingested for 118
tickers`` and ``Signals thesis ingestion: 0 theses`` and still reported
``COVERED`` with an all-``ok`` completion email. These tests pin:

* the verdict (``rag.pipelines.source_yield``): a source offering 0 documents
  is ``degraded`` and NAMED; a declared-expected 0 is not a gap; a source that
  never reported is degraded; every offered document failing is degraded;
* the Finnhub transcript source records WHY it returned nothing (the non-200
  used to be logged at DEBUG, under the INFO root logger);
* the thesis source declares its 0 expected only when every entry is
  quant-envelope output (no ``thesis_summary`` by design);
* the wiring: the weekly script runs the verdict and the email carries it, and
  the D16 recorded entry point records it as a guard.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from rag.pipelines import source_yield
from rag.pipelines.source_yield import (
    EXPECTED_SOURCES,
    SourceYield,
    assess,
    email_collectors,
    write_yield,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_all_ok(tmp_path, **overrides):
    for name in EXPECTED_SOURCES:
        y = overrides.get(name) or SourceYield(source=name, scope=118, discovered=10, ingested=1, already_held=9)
        write_yield(y, tmp_path)


# ── The verdict ────────────────────────────────────────────────────────


def test_every_source_yielding_is_ok(tmp_path):
    _write_all_ok(tmp_path)
    v = assess(tmp_path)
    assert v["status"] == "ok"
    assert v["degraded_sources"] == []


def test_a_whole_source_returning_zero_is_degraded_and_named(tmp_path):
    _write_all_ok(tmp_path, earnings_transcripts=SourceYield(
        source="earnings_transcripts", scope=118, failures={"http_403": 118},
        detail="Finnhub /stock/transcripts/list HTTP 403: You don't have access",
    ))
    v = assess(tmp_path)
    assert v["status"] == "degraded"
    assert [d["source"] for d in v["degraded_sources"]] == ["earnings_transcripts"]
    reason = v["degraded_sources"][0]["reason"]
    assert "0 documents for 118" in reason and "http_403=118" in reason


def test_nothing_new_is_not_a_zero_source(tmp_path):
    """A filing source whose every document is already held ingests 0 in an
    ordinary week — that is not the degraded condition."""
    _write_all_ok(tmp_path, sec_filings=SourceYield(
        source="sec_filings", scope=118, discovered=900, ingested=0, already_held=900,
    ))
    assert assess(tmp_path)["status"] == "ok"


def test_a_declared_expected_zero_is_not_a_gap(tmp_path):
    _write_all_ok(tmp_path, thesis_history=SourceYield(
        source="thesis_history", scope=1806, expected_empty="quant-envelope output",
    ))
    v = assess(tmp_path)
    assert v["status"] == "ok"
    assert v["sources"]["thesis_history"]["status"] == "expected_empty"


def test_a_source_that_never_reported_is_degraded(tmp_path):
    _write_all_ok(tmp_path)
    (tmp_path / "form4_insider.json").unlink()
    v = assess(tmp_path)
    assert v["status"] == "degraded"
    assert v["degraded_sources"][0]["source"] == "form4_insider"


def test_every_offered_document_failing_is_degraded(tmp_path):
    _write_all_ok(tmp_path, form4_insider=SourceYield(
        source="form4_insider", scope=118, discovered=40, ingested=0, failures={"failed_filings": 40},
    ))
    v = assess(tmp_path)
    assert [d["source"] for d in v["degraded_sources"]] == ["form4_insider"]


def test_an_empty_scope_is_degraded(tmp_path):
    _write_all_ok(tmp_path, sec_filings=SourceYield(source="sec_filings", scope=0))
    assert assess(tmp_path)["status"] == "degraded"


def test_report_always_exits_zero_and_writes_the_verdict(tmp_path, caplog):
    _write_all_ok(tmp_path, earnings_transcripts=SourceYield(source="earnings_transcripts", scope=118))
    with caplog.at_level("WARNING"):
        assert source_yield.main(["--report", "--dir", str(tmp_path)]) == 0
    assert "DEGRADED" in caplog.text and "earnings_transcripts" in caplog.text
    assert source_yield.load_verdict(tmp_path)["status"] == "degraded"


def test_email_overlays_the_verdict():
    base = {"sec_filings": {"status": "ok"}, "earnings_transcripts": {"status": "ok"}}
    verdict = {
        "status": "degraded",
        "sources": {
            "sec_filings": {"status": "ok", "reason": ""},
            "earnings_transcripts": {"status": "degraded", "reason": "source returned 0 documents"},
            "thesis_history": {"status": "expected_empty", "reason": "declared expected"},
        },
    }
    status, collectors = email_collectors(base, verdict)
    assert status == "degraded"
    assert collectors["earnings_transcripts"] == {"status": "degraded", "error": "source returned 0 documents"}
    assert collectors["thesis_history"] == {"status": "expected_empty"}


def test_email_with_no_verdict_is_degraded():
    status, collectors = email_collectors({"sec_filings": {"status": "ok"}}, None)
    assert status == "degraded"
    assert collectors["source_yield"]["status"] == "degraded"


def test_the_email_subject_says_degraded():
    from emailer import _build_email

    subject, _, _ = _build_email("RAG Ingestion", {"status": "degraded", "collectors": {}}, "2026-09-24")
    assert subject.endswith("| DEGRADED")


# ── Finnhub transcripts: the reason for a zero is recorded ─────────────


@pytest.fixture
def _finnhub(monkeypatch):
    from rag.pipelines import ingest_earnings_finnhub as mod

    monkeypatch.setattr(mod, "_get_api_key", lambda: "k")
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    retrieval = ModuleType("nousergon_lib.rag.retrieval")
    retrieval.document_exists = MagicMock(return_value=False)
    retrieval.ingest_document = MagicMock(return_value="doc-id")
    embeddings = ModuleType("nousergon_lib.rag.embeddings")
    embeddings.embed_texts = MagicMock(side_effect=lambda t: [[0.0] for _ in t])
    monkeypatch.setitem(sys.modules, "nousergon_lib.rag.retrieval", retrieval)
    monkeypatch.setitem(sys.modules, "nousergon_lib.rag.embeddings", embeddings)
    return mod


def _resp(status, body):
    r = MagicMock(status_code=status, text=json.dumps(body))
    r.json.return_value = body
    return r


def test_a_premium_gated_list_is_counted_and_logged_once(_finnhub, monkeypatch, caplog):
    body = {"error": "You don't have access to this resource."}
    monkeypatch.setattr(_finnhub.requests, "get", MagicMock(return_value=_resp(403, body)))
    stats = SourceYield(source="earnings_transcripts", scope=3)

    with caplog.at_level("WARNING"):
        for t in ("AAPL", "MSFT", "NVDA"):
            assert _finnhub.ingest_ticker(t, stats=stats) == 0

    assert stats.failures == {"http_403": 3}
    assert "access to this resource" in stats.detail
    assert len([r for r in caplog.records if "transcript list HTTP 403" in r.getMessage()]) == 1
    status, reason = source_yield._source_status(json.loads(json.dumps(stats.__dict__)))
    assert status == "degraded" and "http_403=3" in reason


def test_listed_transcripts_are_counted_as_discovered(_finnhub, monkeypatch):
    listing = {"transcripts": [{"id": "t1", "time": "2026-07-30 16:00:00", "year": 2026, "quarter": 2}]}
    transcript = {"transcript": [
        {"name": "CEO", "role": "executive", "speech": "Prepared remarks " * 20},
        {"name": "Analyst", "role": "analyst", "speech": "What about margins? " * 10},
    ]}

    def get(url, params=None, timeout=None):
        return _resp(200, listing if url.endswith("/list") else transcript)

    monkeypatch.setattr(_finnhub.requests, "get", get)
    stats = SourceYield(source="earnings_transcripts", scope=1)
    assert _finnhub.ingest_ticker("AAPL", stats=stats) == 1
    assert (stats.discovered, stats.ingested, stats.failures) == (1, 1, {})


# ── Theses: zero is declared expected only for an all-quant window ─────


def _thesis_yield(census, **results):
    from rag.pipelines.ingest_theses import signals_theses_yield

    return signals_theses_yield({"signals_theses": 0, "skipped_dedup": 0, **results, "census": census})


def test_an_all_quant_envelope_window_declares_zero_theses_expected():
    y = _thesis_yield({"signals_files": 2, "entries": 1806, "with_thesis": 0,
                       "quant_envelope_no_thesis": 1806, "other_no_thesis": 0, "signals_unreadable": 0})
    assert y.expected_empty and "quant_envelope_producer" in y.expected_empty
    assert source_yield._source_status(y.__dict__)[0] == "expected_empty"


def test_a_narrative_producer_with_no_thesis_is_still_a_gap():
    y = _thesis_yield({"signals_files": 1, "entries": 10, "with_thesis": 0,
                       "quant_envelope_no_thesis": 7, "other_no_thesis": 3, "signals_unreadable": 0})
    assert y.expected_empty is None
    assert source_yield._source_status(y.__dict__)[0] == "degraded"


def test_a_window_with_no_signals_is_a_gap():
    y = _thesis_yield({"signals_files": 0, "entries": 0, "with_thesis": 0,
                       "quant_envelope_no_thesis": 0, "other_no_thesis": 0, "signals_unreadable": 0})
    assert source_yield._source_status(y.__dict__)[0] == "degraded"


def test_the_ingest_census_counts_quant_envelope_entries(monkeypatch):
    """Drives the real ingest_signals_theses over the 2026-09-23 shape."""
    from io import BytesIO

    universe = [{"ticker": t, "thesis_summary": None, "stance_source": "quant_envelope_producer"}
                for t in ("AAPL", "MSFT", "NVDA")]

    class _S3:
        def get_paginator(self, op):
            class _P:
                def paginate(self, **kw):
                    yield {"CommonPrefixes": [{"Prefix": "signals/2026-09-23/"}]}
            return _P()

        def get_object(self, *, Bucket, Key):
            return {"Body": BytesIO(json.dumps({"universe": universe}).encode())}

    boto3_stub = ModuleType("boto3")
    boto3_stub.client = lambda *a, **k: _S3()
    monkeypatch.setitem(sys.modules, "boto3", boto3_stub)
    for name in ("nousergon_lib.rag.retrieval", "nousergon_lib.rag.embeddings"):
        stub = ModuleType(name)
        stub.document_exists = stub.ingest_document = stub.embed_texts = MagicMock()
        monkeypatch.setitem(sys.modules, name, stub)

    from rag.pipelines.ingest_theses import ingest_signals_theses, signals_theses_yield

    y = signals_theses_yield(ingest_signals_theses(dry_run=True))
    assert (y.scope, y.discovered) == (3, 0)
    assert y.expected_empty


# ── Wiring ─────────────────────────────────────────────────────────────


def test_the_weekly_script_runs_the_verdict_after_the_last_step_and_before_the_email():
    src = (REPO_ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh").read_text()
    i_manifest = src.index("rag.pipelines.emit_manifest")
    i_verdict = src.index("-m rag.pipelines.source_yield --report")
    i_email = src.index("send_step_email(")
    assert i_manifest < i_verdict < i_email
    assert "export RAG_SOURCE_YIELD_DIR=" in src
    # The email no longer hardcodes the run's status.
    assert "'status': 'ok',\n    'collectors'" not in src
    assert "email_collectors(" in src


def test_the_verdict_step_cannot_abort_the_run():
    """``--report`` always returns 0, and the script runs it bare under
    ``set -e`` — so an exit other than 0 would be a pipeline failure."""
    src = (REPO_ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh").read_text()
    line = next(ln for ln in src.splitlines() if "rag.pipelines.source_yield --report" in ln)
    assert "||" not in line


@pytest.mark.parametrize("module", [
    "ingest_sec_filings", "ingest_8k_filings", "ingest_earnings_finnhub", "ingest_theses", "ingest_form4",
])
def test_every_expected_source_writes_a_yield(module):
    src = (REPO_ROOT / "rag" / "pipelines" / f"{module}.py").read_text()
    assert re.search(r"write_yield\(", src), f"{module} records no source yield"


def test_expected_sources_match_the_email_collectors():
    src = (REPO_ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh").read_text()
    for name in EXPECTED_SOURCES:
        assert f"'{name}': {{'status': 'ok'}}" in src
