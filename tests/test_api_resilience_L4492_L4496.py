"""Transient external-API resilience + FRED window batching + key-scrub.

Covers the 2026-06-03 weekday-SF hardening trio:

  * L4492 — batch the FRED window fetch into ONE ranged call per series
    (``observation_start``/``observation_end``) instead of one-per-date, so
    the windowed reconciliation stops self-inflicting a 429 storm + the
    30-min MorningEnrich timeout.
  * L4495 (SECURITY) — polygon error strings embed ``apiKey=<live>`` via the
    session querystring; they must be scrubbed before logging / raising.
  * L4496 — polygon 5xx + transient network errors on the grouped-daily
    fetch must retry (bounded backoff + jitter) before the target-date
    hard-fail, then fail loud.
"""

from __future__ import annotations

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors import daily_closes
import polygon_client as pc
from polygon_client import PolygonClient


# ── L4492: FRED window batching ──────────────────────────────────────────────


def test_fred_value_on_or_before_picks_latest_prior():
    series = [("2026-05-28", 17.0), ("2026-05-29", 18.0), ("2026-06-01", 19.0)]
    assert daily_closes._fred_value_on_or_before(series, "2026-05-30") == 18.0
    assert daily_closes._fred_value_on_or_before(series, "2026-06-01") == 19.0
    assert daily_closes._fred_value_on_or_before(series, "2026-06-05") == 19.0
    # Nothing on-or-before the earliest obs → None (no future-dated leak).
    assert daily_closes._fred_value_on_or_before(series, "2026-05-27") is None


def test_fetch_fred_window_one_ranged_call_per_series(monkeypatch):
    monkeypatch.setattr(daily_closes, "get_secret", lambda *a, **k: "fred-key")
    captured: list[dict] = []

    def fake_retry(params):
        captured.append(params)
        resp = MagicMock()
        resp.json.return_value = {
            "observations": [
                {"date": "2026-05-28", "value": "17.0"},
                {"date": "2026-05-29", "value": "."},   # missing → filtered out
                {"date": "2026-06-01", "value": "19.0"},
            ]
        }
        return resp

    monkeypatch.setattr(daily_closes, "_fred_get_with_retry", fake_retry)
    cache = daily_closes._fetch_fred_window(["^VIX"], "2026-05-20", "2026-06-01")

    assert len(captured) == 1  # ONE ranged call for the series (not per-date)
    assert captured[0]["observation_start"] == "2026-05-20"
    assert captured[0]["observation_end"] == "2026-06-01"
    assert captured[0]["sort_order"] == "asc"
    assert "limit" not in captured[0]  # ranged, not last-5
    assert cache["VIX"] == [("2026-05-28", 17.0), ("2026-06-01", 19.0)]


def test_fetch_fred_window_no_key_returns_empty(monkeypatch):
    monkeypatch.setattr(daily_closes, "get_secret", lambda *a, **k: "")
    assert daily_closes._fetch_fred_window(["^VIX"], "2026-05-20", "2026-06-01") == {}


def test_fetch_fred_window_failed_series_absent_from_cache(monkeypatch):
    monkeypatch.setattr(daily_closes, "get_secret", lambda *a, **k: "fred-key")

    def boom(params):
        raise requests.HTTPError("500 for url ...?api_key=SECRET")

    monkeypatch.setattr(daily_closes, "_fred_get_with_retry", boom)
    cache = daily_closes._fetch_fred_window(["^VIX"], "2026-05-20", "2026-06-01")
    assert cache == {}  # failed fetch → absent (per-date emit skips → yf backstop)


def test_fetch_fred_closes_window_cache_no_api(monkeypatch):
    """In window-cache mode the per-date emit must NOT hit the FRED API."""
    def must_not_call(*a, **k):
        raise AssertionError("FRED API must not be hit in window-cache mode")

    monkeypatch.setattr(daily_closes, "_fred_get_with_retry", must_not_call)
    cache = {"VIX": [("2026-05-28", 17.0), ("2026-06-01", 19.0)]}
    records: list[dict] = []
    n = daily_closes._fetch_fred_closes(["^VIX"], "2026-05-30", records, window_cache=cache)

    assert n == 1
    assert records[0] == daily_closes._fred_record("VIX", "2026-05-30", 17.0)


def test_fetch_fred_closes_window_cache_no_prior_obs():
    cache = {"VIX": [("2026-06-01", 19.0)]}
    records: list[dict] = []
    n = daily_closes._fetch_fred_closes(["^VIX"], "2026-05-28", records, window_cache=cache)
    assert n == 0 and records == []


def test_collect_window_prefetches_fred_once(monkeypatch):
    calls = {"window": 0, "per_date_caches": []}

    def fake_window(tickers, start, end):
        calls["window"] += 1
        assert tickers == ["^VIX"]
        return {"VIX": [("2026-05-20", 17.0)]}

    def fake_collect(**kwargs):
        calls["per_date_caches"].append(kwargs.get("fred_window_cache"))
        return {"status": "ok", "tickers_captured": 1, "polygon": 0, "fred": 1, "yfinance": 0}

    monkeypatch.setattr(daily_closes, "_fetch_fred_window", fake_window)
    monkeypatch.setattr(daily_closes, "collect", fake_collect)

    result = daily_closes._collect_window(
        bucket="b", tickers=["^VIX"], run_date="2026-06-01",
        s3_prefix="p/", dry_run=True, source="polygon_only", window_days=3,
    )

    assert calls["window"] == 1                       # ONE prefetch for the window
    assert len(calls["per_date_caches"]) == 3         # but 3 per-date collects
    assert all(c == {"VIX": [("2026-05-20", 17.0)]} for c in calls["per_date_caches"])
    assert result["status"] == "ok"


def test_collect_window_no_fred_tickers_skips_prefetch(monkeypatch):
    calls = {"window": 0, "caches": []}

    def fake_window(*a, **k):
        calls["window"] += 1
        return {}

    def fake_collect(**kwargs):
        calls["caches"].append(kwargs.get("fred_window_cache"))
        return {"status": "ok", "tickers_captured": 1, "polygon": 1, "fred": 0, "yfinance": 0}

    monkeypatch.setattr(daily_closes, "_fetch_fred_window", fake_window)
    monkeypatch.setattr(daily_closes, "collect", fake_collect)

    daily_closes._collect_window(
        bucket="b", tickers=["AAPL"], run_date="2026-06-01",
        s3_prefix="p/", dry_run=True, source="polygon_only", window_days=2,
    )

    assert calls["window"] == 0                        # no FRED tickers → no prefetch
    assert all(c is None for c in calls["caches"])


# ── L4495: polygon apiKey scrub ──────────────────────────────────────────────


def test_daily_closes_scrub_masks_both_styles():
    assert daily_closes._scrub_api_key("x?apiKey=SECRET&y=1") == "x?apiKey=***&y=1"
    assert daily_closes._scrub_api_key("x?api_key=SECRET&y=1") == "x?api_key=***&y=1"


def test_polygon_client_scrub_masks_both_styles():
    assert pc._scrub_api_key("x?apiKey=SECRET&y=1") == "x?apiKey=***&y=1"
    assert pc._scrub_api_key("x?api_key=SECRET&y=1") == "x?api_key=***&y=1"


def test_collect_window_warning_scrubs_polygon_apikey(monkeypatch, caplog):
    """A per-date collect raising an HTTPError carrying ``apiKey`` must be
    scrubbed in both the WARNING log AND the persisted per_date/aggregate
    error (which lands in the S3 morning-enrich log)."""
    leaked = "POLYLEAKED123"

    def fake_collect(**kwargs):
        raise requests.HTTPError(
            "500 Server Error for url: https://api.polygon.io/v2/aggs/grouped/"
            f"locale/us/market/stocks/2026-06-01?adjusted=true&apiKey={leaked}"
        )

    monkeypatch.setattr(daily_closes, "collect", fake_collect)

    with caplog.at_level(logging.WARNING, logger="collectors.daily_closes"):
        result = daily_closes._collect_window(
            bucket="b", tickers=["AAPL"], run_date="2026-06-01",
            s3_prefix="p/", dry_run=True, source="polygon_only", window_days=1,
        )

    combined = "\n".join(r.message for r in caplog.records)
    assert leaked not in combined
    assert "apiKey=***" in combined
    # Persisted surfaces are scrubbed too.
    assert leaked not in result["per_date"]["2026-06-01"]["error"]
    assert leaked not in result.get("error", "")


# ── L4496: polygon 5xx + transient retry ─────────────────────────────────────


def _client() -> PolygonClient:
    # High calls_per_min so the rate limiter never sleeps in tests.
    return PolygonClient(api_key="test-key", calls_per_min=100_000)


def _resp_500():
    resp = MagicMock()
    resp.status_code = 500
    resp.headers = {}
    resp.raise_for_status.side_effect = requests.HTTPError(
        "500 Server Error for url: https://api.polygon.io/v2/aggs/grouped/"
        "locale/us/market/stocks/2026-06-01?adjusted=true&apiKey=POLYLEAKED999"
    )
    return resp


def _resp_200(parsed):
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {}
    resp.raise_for_status.return_value = None
    resp.json.return_value = parsed
    return resp


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    monkeypatch.setattr(pc.time, "sleep", lambda *a, **k: None)


def test_polygon_5xx_retries_then_succeeds():
    client = _client()
    parsed = {"results": [{"T": "AAPL", "o": 1, "h": 2, "l": 0.5, "c": 3, "v": 100, "vw": 1.5}]}
    with patch.object(client._session, "get", side_effect=[_resp_500(), _resp_200(parsed)]) as m:
        out = client.get_grouped_daily("2026-06-01")
    assert m.call_count == 2
    assert out["AAPL"]["close"] == 3


def test_polygon_5xx_exhausted_raises_scrubbed():
    client = _client()
    resps = [_resp_500() for _ in range(pc._POLYGON_MAX_ATTEMPTS)]
    with patch.object(client._session, "get", side_effect=resps):
        with pytest.raises(requests.HTTPError) as ei:
            client.get_grouped_daily("2026-06-01")
    assert "POLYLEAKED999" not in str(ei.value)
    assert "apiKey=***" in str(ei.value)


def test_polygon_transient_timeout_retries_then_succeeds():
    client = _client()
    with patch.object(
        client._session, "get",
        side_effect=[requests.ReadTimeout("read timed out"), _resp_200({"results": []})],
    ) as m:
        out = client.get_grouped_daily("2026-06-01")
    assert m.call_count == 2
    assert out == {}


def test_polygon_transient_exhausted_raises_scrubbed():
    client = _client()
    with patch.object(
        client._session, "get",
        side_effect=requests.ConnectionError("boom for url ...?apiKey=POLYLEAKED999"),
    ):
        with pytest.raises(requests.ConnectionError) as ei:
            client.get_grouped_daily("2026-06-01")
    assert "POLYLEAKED999" not in str(ei.value)
