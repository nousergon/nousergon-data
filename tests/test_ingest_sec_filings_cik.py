"""config#2956 deliverable 5: ``ingest_sec_filings._get_cik`` must delegate
to the shared ``_cik_lookup`` file cache instead of unconditionally
re-downloading ``company_tickers.json`` on every cold in-memory cache.

``ingest_sec_filings.py`` had no prior test file — this is scoped to the
CIK-cache wiring this issue's deliverable touches, not full pipeline
coverage.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rag.pipelines import _cik_lookup, ingest_sec_filings


@pytest.fixture(autouse=True)
def _isolate_cik_state(tmp_path, monkeypatch):
    monkeypatch.setattr(_cik_lookup, "DEFAULT_CACHE_PATH", str(tmp_path / "cik.json"))
    monkeypatch.setattr(ingest_sec_filings, "_CIK_CACHE", {})


def _fake_requests(payload):
    fake = MagicMock()
    resp = MagicMock(status_code=200)
    resp.json.return_value = payload
    fake.get.return_value = resp
    return fake


def test_get_cik_populates_from_shared_loader(monkeypatch):
    fake = _fake_requests({"0": {"ticker": "aapl", "cik_str": 320193}})
    monkeypatch.setattr(ingest_sec_filings, "requests", fake)

    cik = ingest_sec_filings._get_cik("AAPL")

    assert cik == "320193"
    fake.get.assert_called_once()


def test_get_cik_uses_file_cache_across_cold_in_memory_caches(monkeypatch, tmp_path):
    # First "process": populates the shared file cache.
    fake1 = _fake_requests({"0": {"ticker": "aapl", "cik_str": 320193}})
    monkeypatch.setattr(ingest_sec_filings, "requests", fake1)
    assert ingest_sec_filings._get_cik("AAPL") == "320193"
    assert fake1.get.call_count == 1

    # Second "process": cold in-memory _CIK_CACHE, but the file cache
    # (same tmp_path, same DEFAULT_CACHE_PATH) must be reused instead of
    # hitting EDGAR again.
    monkeypatch.setattr(ingest_sec_filings, "_CIK_CACHE", {})
    fake2 = _fake_requests({"0": {"ticker": "aapl", "cik_str": 320193}})
    monkeypatch.setattr(ingest_sec_filings, "requests", fake2)

    assert ingest_sec_filings._get_cik("AAPL") == "320193"
    fake2.get.assert_not_called()


def test_get_cik_returns_none_for_unknown_ticker(monkeypatch):
    fake = _fake_requests({"0": {"ticker": "aapl", "cik_str": 320193}})
    monkeypatch.setattr(ingest_sec_filings, "requests", fake)

    assert ingest_sec_filings._get_cik("ZZZZ") is None
