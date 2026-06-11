"""Tests for the shared Finnhub HTTP client.

Focus: the resilience + secret-hygiene wiring added 2026-06-11 to close the
analyst_consensus gap (#397 / #399). The bounded-backoff retry math itself
lives in (and is tested by) ``alpha_engine_lib.http_retry`` — here we assert
that ``finnhub_get`` *uses* that primitive correctly and handles its outputs:

  * missing key short-circuits to [] (no HTTP),
  * auth rides the ``X-Finnhub-Token`` header, never a ``token=`` query param
    (so the secret can't leak into retry logs / HttpRetryError),
  * the request is routed through ``request_with_retry`` with a bounded
    ``max_attempts`` and a scrub-safe label,
  * a 429 that survives retries returns [] (no raise),
  * a non-2xx that survives retries raises ``requests.HTTPError``.
"""

from unittest.mock import MagicMock

import pytest
import requests

from collectors import finnhub_client


def _fake_response(status_code: int, payload=None) -> MagicMock:
    """A stand-in for the requests.Response that request_with_retry returns."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = payload if payload is not None else {}

    def _raise_for_status():
        if status_code >= 400:
            raise requests.HTTPError(f"{status_code} Error")

    resp.raise_for_status.side_effect = _raise_for_status
    return resp


@pytest.fixture
def _set_key(monkeypatch):
    monkeypatch.setattr(finnhub_client, "get_secret", lambda *a, **k: "SECRET_KEY")


def test_missing_key_returns_empty_without_http(monkeypatch):
    monkeypatch.setattr(finnhub_client, "get_secret", lambda *a, **k: "")
    called = MagicMock()
    monkeypatch.setattr(finnhub_client, "request_with_retry", called)

    assert finnhub_client.finnhub_get("stock/recommendation", {"symbol": "AAPL"}) == []
    called.assert_not_called()


def test_auth_via_header_not_query_param(_set_key, monkeypatch):
    """The token must travel in the X-Finnhub-Token header, never in params/URL."""
    captured = {}

    def _fake_rwr(url, **kwargs):
        captured["url"] = url
        captured["params"] = kwargs.get("params")
        captured["session"] = kwargs.get("session")
        captured["max_attempts"] = kwargs.get("max_attempts")
        captured["label"] = kwargs.get("label")
        return _fake_response(200, {"ok": True})

    monkeypatch.setattr(finnhub_client, "request_with_retry", _fake_rwr)

    out = finnhub_client.finnhub_get("stock/recommendation", {"symbol": "AAPL"})

    assert out == {"ok": True}
    # Secret is in the session header, not the query params or the URL.
    assert captured["session"].headers["X-Finnhub-Token"] == "SECRET_KEY"
    assert "token" not in (captured["params"] or {})
    assert "SECRET_KEY" not in captured["url"]
    assert captured["params"] == {"symbol": "AAPL"}
    # Routed through the bounded-retry primitive with a scrub-safe label.
    assert captured["max_attempts"] == finnhub_client._FINNHUB_MAX_ATTEMPTS
    assert captured["label"] == "finnhub:stock/recommendation"


def test_persistent_429_returns_empty(_set_key, monkeypatch):
    monkeypatch.setattr(
        finnhub_client, "request_with_retry",
        lambda url, **kw: _fake_response(429),
    )
    # No raise — a throttled-out call is no-data, not a hard error.
    assert finnhub_client.finnhub_get("stock/earnings", {"symbol": "AAPL"}) == []


def test_success_returns_parsed_json(_set_key, monkeypatch):
    payload = [{"strongBuy": 5, "buy": 3, "hold": 1, "sell": 0, "strongSell": 0}]
    monkeypatch.setattr(
        finnhub_client, "request_with_retry",
        lambda url, **kw: _fake_response(200, payload),
    )
    assert finnhub_client.finnhub_get("stock/recommendation", {"symbol": "AAPL"}) == payload


def test_non_2xx_survivor_raises(_set_key, monkeypatch):
    """A 5xx that survives retries is returned by the primitive; we raise on it."""
    monkeypatch.setattr(
        finnhub_client, "request_with_retry",
        lambda url, **kw: _fake_response(500),
    )
    with pytest.raises(requests.HTTPError):
        finnhub_client.finnhub_get("stock/metric", {"symbol": "AAPL", "metric": "all"})


def test_no_params_call_routes_empty_params(_set_key, monkeypatch):
    captured = {}

    def _fake_rwr(url, **kwargs):
        captured["params"] = kwargs.get("params")
        return _fake_response(200, {"ok": 1})

    monkeypatch.setattr(finnhub_client, "request_with_retry", _fake_rwr)
    finnhub_client.finnhub_get("stock/recommendation")
    assert captured["params"] == {}
