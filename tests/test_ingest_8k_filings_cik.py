"""config#2956 deliverable 5: ``ingest_8k_filings._get_cik`` must delegate
to the shared ``_cik_lookup`` file cache instead of unconditionally
re-downloading ``company_tickers.json`` on every cold in-memory cache.

``ingest_8k_filings.py`` had no prior test file — this is scoped to the
CIK-cache wiring this issue's deliverable touches, not full pipeline
coverage.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rag.pipelines import _cik_lookup, ingest_8k_filings


@pytest.fixture(autouse=True)
def _isolate_cik_state(tmp_path, monkeypatch):
    monkeypatch.setattr(_cik_lookup, "DEFAULT_CACHE_PATH", str(tmp_path / "cik.json"))
    monkeypatch.setattr(ingest_8k_filings, "_CIK_CACHE", {})


def _fake_requests(payload):
    fake = MagicMock()
    resp = MagicMock(status_code=200)
    resp.json.return_value = payload
    fake.get.return_value = resp
    return fake


def test_get_cik_populates_from_shared_loader(monkeypatch):
    fake = _fake_requests({"0": {"ticker": "msft", "cik_str": 789019}})
    monkeypatch.setattr(ingest_8k_filings, "requests", fake)

    cik = ingest_8k_filings._get_cik("MSFT")

    assert cik == "789019"
    fake.get.assert_called_once()


def test_get_cik_shares_file_cache_with_ingest_sec_filings(monkeypatch, tmp_path):
    """The whole point of deliverable 5: TWO different ingest modules
    (separate ``python -m`` invocations in production) must share ONE
    file cache, so the second module's cold in-memory cache does not
    re-download the map ``ingest_sec_filings`` already fetched this run.
    """
    from rag.pipelines import ingest_sec_filings

    monkeypatch.setattr(ingest_sec_filings, "_CIK_CACHE", {})
    fake1 = _fake_requests({"0": {"ticker": "msft", "cik_str": 789019}})
    monkeypatch.setattr(ingest_sec_filings, "requests", fake1)
    assert ingest_sec_filings._get_cik("MSFT") == "789019"
    assert fake1.get.call_count == 1

    # ingest_8k_filings has its OWN cold _CIK_CACHE (autouse fixture reset
    # it above) but the SAME DEFAULT_CACHE_PATH (isolated to tmp_path) —
    # must read the file cache ingest_sec_filings just wrote.
    fake2 = _fake_requests({"0": {"ticker": "msft", "cik_str": 789019}})
    monkeypatch.setattr(ingest_8k_filings, "requests", fake2)

    assert ingest_8k_filings._get_cik("MSFT") == "789019"
    fake2.get.assert_not_called()


def test_get_cik_returns_none_for_unknown_ticker(monkeypatch):
    fake = _fake_requests({"0": {"ticker": "msft", "cik_str": 789019}})
    monkeypatch.setattr(ingest_8k_filings, "requests", fake)

    assert ingest_8k_filings._get_cik("ZZZZ") is None
