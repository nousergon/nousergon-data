"""Tests for the analyst sub-collector in ``collectors/alternative.py``.

Contract as of 2026-04-22:

  * Finnhub ``/stock/recommendation`` drives ``rating`` + ``num_analysts``.
  * yfinance ``Ticker.info`` drives ``target_price`` (Finnhub's
    ``/stock/price-target`` and FMP's ``price-target-consensus`` are both
    paid-tier).
  * Failures on either provider must degrade loudly (WARN) but never
    raise — ``_fetch_analyst`` is called per-ticker and a provider outage
    on one ticker must not poison the whole Phase 2 batch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from collectors import alternative


def _finnhub_recommendation_stub(bullish=8, bearish=1, hold=2):
    return [
        {
            "strongBuy": max(bullish - 2, 0),
            "buy": min(bullish, 2),
            "hold": hold,
            "sell": min(bearish, 1),
            "strongSell": max(bearish - 1, 0),
            "period": "2026-04-01",
            "symbol": "AAPL",
        }
    ]


def test_target_price_from_yfinance():
    """When yfinance exposes targetMeanPrice, it must land in target_price."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.return_value.info = {
        "targetMeanPrice": 215.5,
        "numberOfAnalystOpinions": 42,
    }

    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("AAPL")

    assert out["target_price"] == 215.5
    # Finnhub returned num_analysts → yfinance must NOT clobber it
    assert out["num_analysts"] == 11  # 8 bullish + 2 hold + 1 bearish stub totals


def test_num_analysts_backfilled_from_yfinance_when_finnhub_empty():
    """If Finnhub returns nothing, yfinance's count populates num_analysts."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.return_value.info = {
        "targetMeanPrice": 300.0,
        "numberOfAnalystOpinions": 25,
    }

    # Finnhub returns empty list → no rating / no num_analysts from Finnhub.
    # ``_finnhub_get`` is called twice (recommendation, earnings), both empty.
    with patch.object(alternative, "_finnhub_get", return_value=[]), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("NEWCO")

    assert out["target_price"] == 300.0
    assert out["num_analysts"] == 25
    assert out["rating"] is None  # no Finnhub data → no rating classification


def test_yfinance_failure_degrades_loudly_without_raising():
    """yfinance raising inside _fetch_analyst must not bubble up."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.side_effect = RuntimeError("yfinance IP block")

    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("AAPL")

    # Finnhub path still populated rating + num_analysts
    assert out["rating"] in ("Buy", "Hold", "Sell")
    assert out["num_analysts"] == 11
    # yfinance failed → target_price stays None (degraded but observable via WARN)
    assert out["target_price"] is None


def test_missing_target_mean_price_leaves_target_price_none():
    """yfinance sometimes returns info without targetMeanPrice (new/illiquid names)."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.return_value.info = {
        "numberOfAnalystOpinions": 3,
        # no targetMeanPrice
    }

    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("THINLY_COVERED")

    assert out["target_price"] is None
    # Finnhub still authoritative on num_analysts
    assert out["num_analysts"] == 11


# ── yfinance .info bounded-retry resilience (#705 / L4611) ──────────────────
# Sibling intent to the Finnhub analyst-gap fix (#397 / #399, see
# test_finnhub_client.py): a one-off Yahoo throttle/5xx on the retry-LESS
# ``.info`` access used to silently null target_price. _fetch_analyst now
# retries the transient class up to ``_YF_INFO_MAX_ATTEMPTS`` with the shared
# ``backoff_delay`` math, then degrades loudly. yfinance owns its own HTTP, so
# there is no Response to inspect — any raise from ``.info`` is the transient
# signal. We assert the retry recovers, that the cap is bounded (no unbounded
# per-ticker loop that would blow the SSM runtime budget), and that exhaustion
# still WARNs-not-raises.


def test_info_transient_failure_is_retried_then_succeeds():
    """A first-attempt .info failure is retried; the second attempt's value lands."""
    mock_yf_module = MagicMock()
    # First .info access raises (transient Yahoo throttle), second returns data.
    mock_yf_module.Ticker.return_value = MagicMock()
    type(mock_yf_module.Ticker.return_value).info = property(
        MagicMock(side_effect=[
            RuntimeError("429 Too Many Requests"),
            {"targetMeanPrice": 188.25, "numberOfAnalystOpinions": 30},
        ])
    )

    sleep_calls: list[float] = []
    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.object(alternative.time, "sleep", side_effect=sleep_calls.append), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("RETRYME")

    # Retry recovered the value that a single-attempt fetch would have lost.
    assert out["target_price"] == 188.25
    # Exactly one backoff between the two attempts — bounded, not unbounded.
    assert len(sleep_calls) == 1
    # Backoff honors the low runtime cap (full-jitter on top of base=1.0).
    assert 0.0 <= sleep_calls[0] <= alternative._YF_INFO_BACKOFF_CAP


def test_info_retry_is_capped_and_degrades_loudly_without_raising():
    """If every attempt fails, target_price stays None (WARN) and we never raise."""
    info_mock = MagicMock(side_effect=RuntimeError("persistent yfinance IP block"))
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.return_value = MagicMock()
    type(mock_yf_module.Ticker.return_value).info = property(info_mock)

    sleep_calls: list[float] = []
    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.object(alternative.time, "sleep", side_effect=sleep_calls.append), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("DEADNAME")

    # Bounded: exactly _YF_INFO_MAX_ATTEMPTS .info reads, and one fewer sleep.
    assert info_mock.call_count == alternative._YF_INFO_MAX_ATTEMPTS
    assert len(sleep_calls) == alternative._YF_INFO_MAX_ATTEMPTS - 1
    # Degraded but observable: Finnhub half intact, yfinance half None.
    assert out["target_price"] is None
    assert out["rating"] in ("Buy", "Hold", "Sell")
    assert out["num_analysts"] == 11
