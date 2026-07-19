"""Tests for the shared EDGAR CIK map loader (config#2956 deliverable 5).

Locks the fix for: the ~10k-entry ``company_tickers.json`` map was
re-downloaded 2-3x per weekly-ingestion run because each of
``ingest_sec_filings.py``, ``ingest_8k_filings.py``, and
``ingest_form4.py`` had its OWN process-level ``_CIK_CACHE`` dict, and
each pipeline step is a separate ``python -m`` invocation — so a cold
in-memory cache always re-hit EDGAR. ``load_cik_map`` backs a cold
in-memory cache with a shared ``/tmp`` file cache (mtime TTL), so only
the first pipeline step in a run/day actually downloads.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

from rag.pipelines._cik_lookup import load_cik_map

_PAYLOAD = {
    "0": {"ticker": "aapl", "cik_str": 320193},
    "1": {"ticker": "msft", "cik_str": 789019},
}


def _fake_http(status_code=200, payload=None):
    http = MagicMock()
    resp = MagicMock(status_code=status_code)
    resp.json.return_value = payload if payload is not None else _PAYLOAD
    http.get.return_value = resp
    return http


class TestColdCache:
    def test_downloads_and_uppercases_tickers_on_cold_cache(self, tmp_path):
        cache_path = str(tmp_path / "cik.json")
        http = _fake_http()

        cik_map = load_cik_map(http=http, cache_path=cache_path)

        assert cik_map == {"AAPL": "320193", "MSFT": "789019"}
        http.get.assert_called_once()

    def test_writes_file_cache_after_download(self, tmp_path):
        cache_path = str(tmp_path / "cik.json")
        http = _fake_http()

        load_cik_map(http=http, cache_path=cache_path)

        with open(cache_path) as f:
            payload = json.load(f)
        assert payload["cik_map"] == {"AAPL": "320193", "MSFT": "789019"}

    def test_non_200_returns_empty_and_does_not_write_cache(self, tmp_path):
        cache_path = str(tmp_path / "cik.json")
        http = _fake_http(status_code=503)

        cik_map = load_cik_map(http=http, cache_path=cache_path)

        assert cik_map == {}
        import os
        assert not os.path.exists(cache_path)

    def test_download_exception_returns_empty_not_raises(self, tmp_path):
        cache_path = str(tmp_path / "cik.json")
        http = MagicMock()
        http.get.side_effect = ConnectionError("boom")

        cik_map = load_cik_map(http=http, cache_path=cache_path)

        assert cik_map == {}


class TestFileCacheHit:
    def test_fresh_cache_skips_download(self, tmp_path):
        cache_path = str(tmp_path / "cik.json")
        http = _fake_http()

        # First call populates the cache.
        load_cik_map(http=http, cache_path=cache_path)
        assert http.get.call_count == 1

        # Second call (simulating a NEW process / cold in-memory cache)
        # must read the file cache, not hit EDGAR again.
        http2 = _fake_http()
        cik_map = load_cik_map(http=http2, cache_path=cache_path)

        assert cik_map == {"AAPL": "320193", "MSFT": "789019"}
        http2.get.assert_not_called()

    def test_expired_cache_redownloads(self, tmp_path):
        cache_path = str(tmp_path / "cik.json")
        http = _fake_http()
        load_cik_map(http=http, cache_path=cache_path, ttl_seconds=100)

        # Simulate wall-clock time far past the TTL.
        http2 = _fake_http()
        cik_map = load_cik_map(
            http=http2,
            cache_path=cache_path,
            ttl_seconds=100,
            monotonic_time=lambda: time.time() + 1_000_000,
        )

        assert cik_map == {"AAPL": "320193", "MSFT": "789019"}
        http2.get.assert_called_once()

    def test_missing_cache_file_falls_through_to_download(self, tmp_path):
        cache_path = str(tmp_path / "does_not_exist.json")
        http = _fake_http()

        cik_map = load_cik_map(http=http, cache_path=cache_path)

        assert cik_map == {"AAPL": "320193", "MSFT": "789019"}
        http.get.assert_called_once()

    def test_corrupt_cache_file_falls_through_to_download(self, tmp_path):
        cache_path = tmp_path / "cik.json"
        cache_path.write_text("not valid json{{{")
        http = _fake_http()

        cik_map = load_cik_map(http=http, cache_path=str(cache_path))

        assert cik_map == {"AAPL": "320193", "MSFT": "789019"}
        http.get.assert_called_once()

    def test_cache_missing_cik_map_key_falls_through_to_download(self, tmp_path):
        cache_path = tmp_path / "cik.json"
        cache_path.write_text(json.dumps({"unexpected": "shape"}))
        http = _fake_http()

        cik_map = load_cik_map(http=http, cache_path=str(cache_path))

        assert cik_map == {"AAPL": "320193", "MSFT": "789019"}
        http.get.assert_called_once()


def test_default_cache_path_dynamically_overridable(tmp_path, monkeypatch):
    """``cache_path=None`` (the default every ingestor's ``_get_cik`` uses)
    must resolve the MODULE global ``DEFAULT_CACHE_PATH`` at CALL time,
    not at import/definition time — otherwise
    ``monkeypatch.setattr(_cik_lookup, "DEFAULT_CACHE_PATH", ...)`` in
    test isolation fixtures would silently have no effect."""
    from rag.pipelines import _cik_lookup

    isolated_path = str(tmp_path / "isolated.json")
    monkeypatch.setattr(_cik_lookup, "DEFAULT_CACHE_PATH", isolated_path)

    http = _fake_http()
    cik_map = load_cik_map(http=http)

    assert cik_map == {"AAPL": "320193", "MSFT": "789019"}
    import os
    assert os.path.exists(isolated_path)
