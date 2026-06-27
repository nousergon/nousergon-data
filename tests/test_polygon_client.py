"""Tests for polygon_client.PolygonClient.

Focus: response caching on get_grouped_daily, which dedup's calendar-date
repeats across overlapping eval_date windows in universe_returns and cuts
the free-tier 5 calls/min rate-limit tax by ~3.5x on backfill runs.

Also covers the 403-raises contract (PolygonForbiddenError) — see
2026-04-23 incident where the prior return-empty-on-403 silently masked
free-tier "before end of day" rejections for stocks for a week.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from polygon_client import PolygonClient, PolygonForbiddenError


def _make_client() -> PolygonClient:
    return PolygonClient(api_key="test-key", calls_per_min=5)


def _fake_response(tickers: list[tuple[str, float]]) -> dict:
    return {
        "results": [
            {"T": t, "o": 1.0, "h": 2.0, "l": 0.5, "c": close, "v": 1000, "vw": 1.5}
            for t, close in tickers
        ],
        "resultsCount": len(tickers),
    }


def test_grouped_daily_caches_identical_dates():
    client = _make_client()
    with patch.object(client, "_get", return_value=_fake_response([("AAPL", 200.0)])) as mock_get:
        first = client.get_grouped_daily("2026-01-05")
        second = client.get_grouped_daily("2026-01-05")
    assert mock_get.call_count == 1
    assert first == second
    assert first["AAPL"]["close"] == 200.0


def test_grouped_daily_distinct_dates_hit_api():
    client = _make_client()
    responses = [
        _fake_response([("AAPL", 200.0)]),
        _fake_response([("AAPL", 201.0)]),
    ]
    with patch.object(client, "_get", side_effect=responses) as mock_get:
        a = client.get_grouped_daily("2026-01-05")
        b = client.get_grouped_daily("2026-01-06")
    assert mock_get.call_count == 2
    assert a["AAPL"]["close"] == 200.0
    assert b["AAPL"]["close"] == 201.0


def test_grouped_daily_caches_empty_response():
    """Non-trading days return empty dicts — cache them too (same URL, same answer)."""
    client = _make_client()
    with patch.object(client, "_get", return_value={"results": [], "resultsCount": 0}) as mock_get:
        first = client.get_grouped_daily("2026-01-03")  # Saturday
        second = client.get_grouped_daily("2026-01-03")
    assert mock_get.call_count == 1
    assert first == {}
    assert second == {}


def test_cache_is_per_instance():
    c1 = _make_client()
    c2 = _make_client()
    with patch.object(c1, "_get", return_value=_fake_response([("AAPL", 200.0)])) as m1:
        c1.get_grouped_daily("2026-01-05")
    with patch.object(c2, "_get", return_value=_fake_response([("AAPL", 201.0)])) as m2:
        c2.get_grouped_daily("2026-01-05")
    assert m1.call_count == 1
    assert m2.call_count == 1


def _make_403_response(message: str = "Attempted to request today's data before end of day. Please upgrade your plan at https://polygon.io/pricing"):
    """Build a fake requests.Response with status 403 + polygon's standard message."""
    resp = MagicMock()
    resp.status_code = 403
    resp.json.return_value = {"message": message, "status": "FORBIDDEN"}
    resp.text = '{"status":"FORBIDDEN","message":"' + message + '"}'
    return resp


def test_403_raises_polygon_forbidden_error():
    """403 must raise PolygonForbiddenError, not silently return empty dict.

    Prior behavior (logged warning + returned {"results": []}) masked the
    2026-04-17 → 2026-04-23 VWAP outage by letting daily_closes.collect
    fall through to yfinance, which writes VWAP=None for every stock.
    """
    client = _make_client()
    with patch.object(client._session, "get", return_value=_make_403_response()):
        with pytest.raises(PolygonForbiddenError) as excinfo:
            client.get_grouped_daily("2026-04-23")
    assert "before end of day" in str(excinfo.value).lower() or "403" in str(excinfo.value)
    assert "/v2/aggs/grouped" in str(excinfo.value)


def test_403_raises_even_when_response_body_is_not_json():
    """Defensive: 403 with malformed/non-JSON body must still raise (not crash on .json())."""
    client = _make_client()
    bad_resp = MagicMock()
    bad_resp.status_code = 403
    bad_resp.json.side_effect = ValueError("not JSON")
    bad_resp.text = "Forbidden"
    with patch.object(client._session, "get", return_value=bad_resp):
        with pytest.raises(PolygonForbiddenError):
            client.get_grouped_daily("2026-04-23")


def test_403_does_not_pollute_grouped_daily_cache():
    """A 403 must not leave a 'cached empty result' that hides the failure on retry."""
    client = _make_client()
    with patch.object(client._session, "get", return_value=_make_403_response()):
        with pytest.raises(PolygonForbiddenError):
            client.get_grouped_daily("2026-04-23")
    # Retry must hit the network again, not a cached empty dict
    with patch.object(client._session, "get", return_value=_make_403_response()) as second:
        with pytest.raises(PolygonForbiddenError):
            client.get_grouped_daily("2026-04-23")
        assert second.called, (
            "Cache must NOT have stored the 403 outcome — every retry must "
            "re-hit the API so an upgraded plan / different time window "
            "succeeds without manual cache busting."
        )


# ── get_splits (data#1298: authoritative split factor source) ────────────────


def _splits_response(events: list[dict]) -> dict:
    """Shape of polygon /v3/reference/splits results."""
    return {
        "results": [
            {
                "execution_date": e["execution_date"],
                "split_from": e["split_from"],
                "split_to": e["split_to"],
                "ticker": e.get("ticker", "DD"),
            }
            for e in events
        ],
        "status": "OK",
    }


def test_get_splits_parses_and_sorts():
    client = _make_client()
    resp = _splits_response(
        [
            {"execution_date": "2026-06-24", "split_from": 3, "split_to": 1},
            {"execution_date": "2019-06-03", "split_from": 1, "split_to": 3},
        ]
    )
    with patch.object(client, "_get", return_value=resp) as mock_get:
        out = client.get_splits("DD")
    assert mock_get.call_count == 1
    # Sorted ascending by execution_date.
    assert [e["execution_date"] for e in out] == ["2019-06-03", "2026-06-24"]
    assert out[1] == {"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}


def test_get_splits_skips_malformed_rows():
    client = _make_client()
    resp = {
        "results": [
            {"execution_date": "2026-06-24", "split_from": 3, "split_to": 1},
            {"execution_date": None, "split_from": 2, "split_to": 1},  # bad date
            {"execution_date": "2025-01-01", "split_from": 0, "split_to": 1},  # bad ratio
        ]
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_splits("DD")
    assert out == [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]


def test_get_splits_forbidden_returns_empty():
    client = _make_client()
    with patch.object(client, "_get", side_effect=PolygonForbiddenError("403")):
        assert client.get_splits("DD") == []
