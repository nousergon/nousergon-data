"""Tests for the signals-thesis -> RAG ingest pipeline (``ingest_signals_theses``).

Regression coverage for the config#2938 follow-on failure (2026-07-18): the
weekly RAG-ingestion timeout fix let the pipeline reach Step 4/10 (Thesis
history), which then crashed the *entire* 902-ticker ingestion with
``TypeError: object of type 'NoneType' has no len()``.

Root cause: the expanded universe is produced by the quant-envelope producer
(``stance_source="quant_envelope_producer"``), which emits
``thesis_summary: null`` for every entry -- there is no LLM narrative to embed.
``entry.get("thesis_summary", "")`` returns the ``""`` default only when the
key is ABSENT; an explicit JSON ``null`` returns ``None``, so ``len(thesis)``
blew up. A null/absent thesis must be treated exactly like a too-short one:
skipped, never crash.

The test is hermetic: ``boto3`` and ``nousergon_lib.rag.*`` are stubbed via
``sys.modules`` so it needs neither the AWS SDK nor the heavy lib.
"""

from __future__ import annotations

import json
import sys
from io import BytesIO
from types import ModuleType
from unittest.mock import MagicMock

import pytest


class _InMemoryS3:
    """Minimal S3 client mock: signals/{date}/signals.json objects."""

    def __init__(self, dates_to_payload: dict) -> None:
        self._data = dates_to_payload

    def list_objects_v2(self, *, Bucket, Prefix, Delimiter=None):
        return {"CommonPrefixes": [{"Prefix": f"signals/{d}/"} for d in self._data]}

    def get_object(self, *, Bucket, Key):
        date_str = Key.split("/")[1]  # signals/{date}/signals.json
        return {"Body": BytesIO(json.dumps(self._data[date_str]).encode())}


@pytest.fixture
def _stub_env(monkeypatch):
    """Stub boto3 + nousergon_lib.rag.{embeddings,retrieval} so the pipeline's
    in-function imports resolve without the AWS SDK or the heavy lib.
    document_exists -> False; embed/ingest are unused in dry_run mode."""
    retrieval = ModuleType("nousergon_lib.rag.retrieval")
    retrieval.document_exists = MagicMock(return_value=False)
    retrieval.ingest_document = MagicMock(return_value="doc-id")
    embeddings = ModuleType("nousergon_lib.rag.embeddings")
    embeddings.embed_texts = MagicMock(return_value=[[0.0]])
    monkeypatch.setitem(sys.modules, "nousergon_lib.rag.retrieval", retrieval)
    monkeypatch.setitem(sys.modules, "nousergon_lib.rag.embeddings", embeddings)

    holder = {"s3": None}
    boto3_stub = ModuleType("boto3")
    boto3_stub.client = lambda *a, **k: holder["s3"]
    monkeypatch.setitem(sys.modules, "boto3", boto3_stub)
    return holder


def _entry(ticker, thesis, **extra):
    e = {"ticker": ticker, "signal": "HOLD", "score": 100.0}
    e["thesis_summary"] = thesis  # explicit key so `null` round-trips as-is
    e.update(extra)
    return e


def _run(holder, universe, dry_run=True):
    holder["s3"] = _InMemoryS3({"2026-07-18": {"universe": universe}})
    from rag.pipelines.ingest_theses import ingest_signals_theses

    return ingest_signals_theses(dry_run=dry_run)


def test_null_thesis_summary_skipped_not_crash(_stub_env):
    """The exact 2026-07-18 shape: an all-null quant-envelope universe must be
    skipped cleanly (0 ingested), not raise ``len(None)``."""
    universe = [_entry(t, None, stance_source="quant_envelope_producer")
                for t in ("MNST", "TMHC", "INCY", "EXEL", "ROKU")]
    res = _run(_stub_env, universe)  # must not raise
    assert res["signals_theses"] == 0


def test_null_and_valid_theses_mixed(_stub_env):
    """Null / absent / too-short theses are skipped; only a >=50-char thesis is
    counted -- proving the guard still discriminates after the null fix."""
    universe = [
        _entry("MNST", None),                       # explicit JSON null (the bug)
        {"ticker": "NOKEY", "signal": "BUY"},       # thesis_summary key ABSENT
        _entry("SHORT", "too short"),               # <50 chars
        _entry("BLANK", ""),                         # empty string
        _entry("", "X" * 60),                        # empty ticker
        _entry("AAPL", "A" * 60),                   # valid -> counted
    ]
    res = _run(_stub_env, universe)
    assert res["signals_theses"] == 1  # AAPL only


def test_null_sibling_fields_ingest_without_crash_or_none_pollution(_stub_env):
    """config#2964: a >=50-char thesis paired with explicit-null sub_scores /
    score / signal / conviction / sector must still ingest cleanly -- no
    ``None.get()`` crash, and the embedded text must not contain the literal
    string 'None' for any null field."""
    entry = _entry(
        "QUAL",
        "A" * 60,
        sub_scores=None,
        score=None,
        signal=None,
        conviction=None,
        sector=None,
    )
    from rag.pipelines.ingest_theses import ingest_signals_theses

    _stub_env["s3"] = _InMemoryS3({"2026-07-18": {"universe": [entry]}})
    res = ingest_signals_theses(dry_run=False)  # must not raise

    assert res["signals_theses"] == 1
    ingest_document_mock = sys.modules["nousergon_lib.rag.retrieval"].ingest_document
    _, kwargs = ingest_document_mock.call_args
    assert kwargs["sector"] is None
    embedded_content = kwargs["chunks"][0]["content"]
    assert "None" not in embedded_content
