"""Tests for polygon_client.PolygonClient.

Focus: response caching on get_grouped_daily, which dedup's calendar-date
repeats across overlapping eval_date windows in universe_returns and cuts
the free-tier 5 calls/min rate-limit tax by ~3.5x on backfill runs.

Also covers the 403-raises contract (PolygonForbiddenError) — see
2026-04-23 incident where the prior return-empty-on-403 silently masked
free-tier "before end of day" rejections for stocks for a week.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

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


# ── get_recent_splits (config#717: window-wide corporate-action scan) ────────


def test_get_recent_splits_range_scoped_single_call():
    client = _make_client()
    resp = {
        "results": [
            {"ticker": "AAPL", "execution_date": "2026-05-09",
             "split_from": 1, "split_to": 10},
            {"ticker": "FOO", "execution_date": "2026-05-08",
             "split_from": 2, "split_to": 1},
        ],
        "status": "OK",
    }
    with patch.object(client, "_get", return_value=resp) as mock_get:
        out = client.get_recent_splits("2026-05-01", "2026-05-15")
    assert mock_get.call_count == 1
    # Range filter passed to the splits endpoint (no ticker filter).
    _, kwargs = mock_get.call_args
    params = kwargs["params"]
    assert params["execution_date.gte"] == "2026-05-01"
    assert params["execution_date.lte"] == "2026-05-15"
    assert "ticker" not in params
    # Sorted ascending; carries ticker.
    assert [e["execution_date"] for e in out] == ["2026-05-08", "2026-05-09"]
    assert out[1]["ticker"] == "AAPL"


def test_get_recent_splits_skips_malformed_rows():
    client = _make_client()
    resp = {
        "results": [
            {"ticker": "AAPL", "execution_date": "2026-05-09",
             "split_from": 1, "split_to": 10},
            {"ticker": None, "execution_date": "2026-05-08",
             "split_from": 2, "split_to": 1},  # missing ticker
            {"ticker": "BAR", "execution_date": None,
             "split_from": 2, "split_to": 1},  # missing date
        ]
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_recent_splits("2026-05-01", "2026-05-15")
    assert out == [
        {"ticker": "AAPL", "execution_date": "2026-05-09",
         "split_from": 1, "split_to": 10},
    ]


def test_get_recent_splits_forbidden_returns_empty():
    client = _make_client()
    with patch.object(client, "_get", side_effect=PolygonForbiddenError("403")):
        assert client.get_recent_splits("2026-05-01", "2026-05-15") == []


# ── get_ticker_events (corporate-actions PR6: ticker-rename detection) ─────────


def _events_response(changes: list[dict], *, name: str = "Meta Platforms, Inc.") -> dict:
    """Shape of polygon /vX/reference/tickers/{id}/events results.

    ``changes`` are ``{"date", "ticker"}`` (the ticker the entity changed TO on
    that date) — polygon lists the IPO listing as the earliest ticker_change too.
    """
    return {
        "results": {
            "name": name,
            "figi": "BBG000MM2P62",
            "events": [
                {"type": "ticker_change", "date": c["date"],
                 "ticker_change": {"ticker": c["ticker"]}}
                for c in changes
            ],
        },
        "status": "OK",
    }


def test_get_ticker_events_parses_adjacent_rename_pairs():
    """FB (IPO 2012) -> META (2022): the adjacent transition is the rename pair;
    the earliest listing yields no pair."""
    client = _make_client()
    resp = _events_response([
        {"date": "2022-06-09", "ticker": "META"},
        {"date": "2012-05-18", "ticker": "FB"},
    ])
    with patch.object(client, "_get", return_value=resp) as mock_get:
        out = client.get_ticker_events("FB")
    assert mock_get.call_count == 1
    # Endpoint path carries the queried ticker.
    args, _ = mock_get.call_args
    assert args[0] == "/vX/reference/tickers/FB/events"
    assert out == [
        {"date": "2022-06-09", "old_ticker": "FB", "new_ticker": "META"},
    ]


def test_get_ticker_events_multi_hop_chain():
    """Two renames produce two adjacent old->new pairs, ascending by date."""
    client = _make_client()
    resp = _events_response([
        {"date": "2012-05-18", "ticker": "AAA"},
        {"date": "2018-01-02", "ticker": "BBB"},
        {"date": "2023-03-03", "ticker": "CCC"},
    ])
    with patch.object(client, "_get", return_value=resp):
        out = client.get_ticker_events("AAA")
    assert out == [
        {"date": "2018-01-02", "old_ticker": "AAA", "new_ticker": "BBB"},
        {"date": "2023-03-03", "old_ticker": "BBB", "new_ticker": "CCC"},
    ]


def test_get_ticker_events_single_listing_no_rename():
    """A ticker with only its IPO listing (no later change) has no rename pair —
    the genuine-delist / merger-of-acquired signal."""
    client = _make_client()
    resp = _events_response([{"date": "2012-05-18", "ticker": "XYZ"}])
    with patch.object(client, "_get", return_value=resp):
        assert client.get_ticker_events("XYZ") == []


def test_get_ticker_events_ignores_non_ticker_change_events():
    client = _make_client()
    resp = {
        "results": {
            "name": "X",
            "events": [
                {"type": "ticker_change", "date": "2012-05-18",
                 "ticker_change": {"ticker": "OLD"}},
                {"type": "delisted", "date": "2020-01-01"},  # not a ticker_change
                {"type": "ticker_change", "date": "2022-06-09",
                 "ticker_change": {"ticker": "NEW"}},
            ],
        },
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_ticker_events("OLD")
    assert out == [
        {"date": "2022-06-09", "old_ticker": "OLD", "new_ticker": "NEW"},
    ]


def test_get_ticker_events_forbidden_returns_empty():
    client = _make_client()
    with patch.object(client, "_get", side_effect=PolygonForbiddenError("403")):
        assert client.get_ticker_events("FB") == []


def test_get_ticker_events_empty_results_object():
    """No results / no events key → []."""
    client = _make_client()
    with patch.object(client, "_get", return_value={"status": "OK"}):
        assert client.get_ticker_events("FB") == []


def _http_404(body: dict) -> requests.HTTPError:
    resp = requests.Response()
    resp.status_code = 404
    resp._content = json.dumps(body).encode()
    return requests.HTTPError("404 Client Error", response=resp)


def test_get_ticker_events_404_not_found_returns_empty():
    """config#2812: polygon returns 404 {"status": "NOT_FOUND"} for a ticker
    whose entity has no events record at all — verified live for BLD/JHG after
    their 2026-07-01 delisting (vs. 200 for still-active AAPL/META). This must
    be treated the same as "no renames found" (like the 403 case above), not as
    a detection failure — otherwise prune_delisted_tickers can never clear the
    rename-safety check for a genuinely fully-retired ticker."""
    client = _make_client()
    err = _http_404({"status": "NOT_FOUND", "message": "No events found for given ID"})
    with patch.object(client, "_get", side_effect=err):
        assert client.get_ticker_events("BLD") == []


def test_get_ticker_events_404_unexpected_body_propagates():
    """A 404 that does NOT carry status=NOT_FOUND is an unrecognized shape —
    preserve the history-safety default (propagate, don't silently swallow)."""
    client = _make_client()
    err = _http_404({"status": "SOMETHING_ELSE"})
    with patch.object(client, "_get", side_effect=err):
        with pytest.raises(requests.HTTPError):
            client.get_ticker_events("BLD")


# ── fractional split-ratio fields (2026-07-02 incident class) ────────────────
#
# Polygon publishes FRACTIONAL split_from/split_to for spinoff-style records
# (live 2026-06: CCBC 1:1.2, NRWRF 20.625:21.625, CFRLF 1:1.0517…). The old
# int() cast silently truncated them (1.2 → 1: a corrupted no-op factor) and
# raised on malformed rows, degrading the WHOLE window's split detection to
# empty. Rows now parse fractionally; malformed rows skip per-row with a WARN.


def test_get_recent_splits_preserves_fractional_ratios():
    client = _make_client()
    resp = {
        "results": [
            {"ticker": "CCBC", "execution_date": "2026-06-18",
             "split_from": 1, "split_to": 1.2},
            {"ticker": "NRWRF", "execution_date": "2026-06-18",
             "split_from": 20.625, "split_to": 21.625},
        ]
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_recent_splits("2026-06-15", "2026-06-20")
    assert out[0]["split_to"] == pytest.approx(1.2)
    assert out[1]["split_from"] == pytest.approx(20.625)
    # Integral values stay int (content-addressed action-id stability).
    assert isinstance(out[0]["split_from"], int)


def test_get_recent_splits_skips_malformed_row_keeps_rest():
    client = _make_client()
    resp = {
        "results": [
            {"ticker": "BAD", "execution_date": "2026-06-18",
             "split_from": "not-a-number", "split_to": 1},
            {"ticker": "GOOD", "execution_date": "2026-06-18",
             "split_from": 1, "split_to": 2},
        ]
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_recent_splits("2026-06-15", "2026-06-20")
    assert [r["ticker"] for r in out] == ["GOOD"]


def test_get_splits_preserves_fractional_ratios():
    client = _make_client()
    resp = {
        "results": [
            {"ticker": "HON", "execution_date": "2025-10-30",
             "split_from": 1000, "split_to": 1061},
        ]
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_splits("HON")
    assert out == [
        {"execution_date": "2025-10-30", "split_from": 1000, "split_to": 1061},
    ]
