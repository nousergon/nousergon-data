"""Producer contract tests for data-collector plan P-07 (alpha-engine-config-I10774):
a versioned JSON Schema + producer test for every plan §3 boundary-table key that
still lacks one — the Metron market-data spine family, intraday `latest.json`,
constituents/universe_classification, and (schema only here; consumer test + pin
live in `crucible`) the ArcticDB `universe` library row shape.

Mirrors the existing SLOT/technical_ratings pattern: every producer write validates
cleanly against its own schema, checked at PR time, plus a hand-built minimal
fixture per schema independent of the producer code so a producer bug that matches
its own (wrong) output can't also pass the contract by construction.

Refs alpha-engine-config-I10774, data_collection_plan_260914.md §3/§4.3.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import constituents as constituents_mod
from collectors import metron_market_data as mmd
from collectors import universe_classification as uc
from contracts import (
    validate_arctic_universe_row,
    validate_constituents,
    validate_metron_analyst,
    validate_metron_close_history,
    validate_metron_closes,
    validate_metron_earnings,
    validate_metron_fundamentals,
    validate_metron_fx,
    validate_metron_fx_history,
    validate_metron_intraday_latest,
    validate_metron_macro,
    validate_metron_sectors,
    validate_metron_security_performance,
    validate_metron_sentiment,
    validate_metron_valuation_medians,
    validate_universe_classification,
)
from tests.test_constituents_sector_map import _make_fake_get
from tests.test_metron_market_data import _UNIVERSE, _puts, _universe_s3


# ── Metron closes + fx ───────────────────────────────────────────────────────

def test_closes_and_fx_producer_write_validates():
    s3 = _universe_s3(_UNIVERSE)
    fx_src = lambda ccys: {c: 1.1 for c in ccys}
    close_src = lambda syms: {s: (100.0, "2026-06-26") for s in syms}
    mmd.collect(bucket="b", run_date="2026-06-26", s3_client=s3,
                close_source=close_src, fx_source=fx_src)
    puts = _puts(s3)
    closes_art = puts[f"{mmd.CLOSES_PREFIX}latest.json"]
    fx_art = puts[f"{mmd.FX_PREFIX}latest.json"]
    assert validate_metron_closes(closes_art) == []
    assert validate_metron_fx(fx_art) == []


def test_closes_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "alpha-engine-data",
           "closes": {"AAPL": {"close": 200.5, "currency": "USD", "bar_date": "2026-06-26"}}}
    assert validate_metron_closes(art) == []


def test_closes_wrong_type_fails():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "alpha-engine-data",
           "closes": {"AAPL": {"close": "not-a-number", "currency": "USD", "bar_date": "2026-06-26"}}}
    assert validate_metron_closes(art) != []


def test_fx_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-26", "base": "USD", "rates": {"EUR": 1.08}}
    assert validate_metron_fx(art) == []


# ── Metron close_history + fx_history ───────────────────────────────────────

def test_close_and_fx_history_producer_write_validates():
    s3 = _universe_s3(_UNIVERSE)
    close_hist_src = lambda syms: {s: [("2026-06-25", 99.0), ("2026-06-26", 100.0)] for s in syms}
    fx_hist_src = lambda ccys: {c: [("2026-06-25", 1.09), ("2026-06-26", 1.1)] for c in ccys}
    mmd.collect_history(bucket="b", s3_client=s3, close_history_source=close_hist_src,
                         fx_history_source=fx_hist_src)
    puts = _puts(s3)
    hist_key = next(k for k in puts if k.startswith(mmd.CLOSE_HISTORY_PREFIX) and k != mmd.CONSOLIDATED_CLOSE_HISTORY_KEY)
    assert validate_metron_close_history(puts[hist_key]) == []
    fx_hist_key = next(k for k in puts if k.startswith(mmd.FX_HISTORY_PREFIX))
    assert validate_metron_fx_history(puts[fx_hist_key]) == []


def test_close_history_hand_built_fixture_validates():
    art = {"schema_version": 1, "yf_symbol": "AAPL", "currency": "USD",
           "adjustment_basis": "dividend_adjusted", "closes": [["2026-06-26", 200.5]]}
    assert validate_metron_close_history(art) == []


def test_close_history_missing_symbol_fails():
    art = {"schema_version": 1, "currency": "USD",
           "adjustment_basis": "dividend_adjusted", "closes": [["2026-06-26", 200.5]]}
    assert validate_metron_close_history(art) != []


# ── Metron sectors + earnings ────────────────────────────────────────────────

def test_reference_producer_write_validates():
    s3 = _universe_s3(_UNIVERSE)
    mmd.collect_reference(
        bucket="b", run_date="2026-06-26", s3_client=s3,
        sector_source=lambda syms: {"AAPL": "Technology"},
        country_source=lambda syms: {"AAPL": "United States"},
        benchmark_source=lambda: {"Technology": 0.30},
        earnings_source=lambda syms, as_of: {"AAPL": "2026-07-30"},
    )
    puts = _puts(s3)
    assert validate_metron_sectors(puts[f"{mmd.SECTORS_PREFIX}latest.json"]) == []
    assert validate_metron_earnings(puts[f"{mmd.EARNINGS_PREFIX}latest.json"]) == []


def test_sectors_hand_built_fixture_validates():
    art = {"schema_version": 2, "as_of": "2026-06-26",
           "sectors": {"AAPL": "Technology"}, "countries": {"AAPL": "United States"},
           "spy_sector_weights": {"Technology": 0.3}}
    assert validate_metron_sectors(art) == []


def test_sectors_out_of_range_weight_fails():
    art = {"schema_version": 2, "as_of": "2026-06-26",
           "sectors": {"AAPL": "Technology"}, "countries": {"AAPL": "United States"},
           "spy_sector_weights": {"Technology": 1.3}}
    assert validate_metron_sectors(art) != []


def test_earnings_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-26", "earnings": {"AAPL": "2026-07-30"}}
    assert validate_metron_earnings(art) == []


# ── Metron macro ─────────────────────────────────────────────────────────────

def test_macro_producer_write_validates():
    s3 = _universe_s3(_UNIVERSE)
    macro_src = lambda ids, as_of: {"FEDFUNDS": [("2026-06-01", 5.33)]}
    rel_src = lambda ids, run_date: (
        {"FEDFUNDS": "2026-07-30"},
        [{"date": "2026-07-30", "kind": "fomc", "series_id": "FEDFUNDS", "label": "FOMC decision"}],
    )
    mmd.collect_macro(bucket="b", run_date="2026-06-11", s3_client=s3,
                       macro_source=macro_src, release_source=rel_src)
    art = _puts(s3)[f"{mmd.MACRO_PREFIX}latest.json"]
    assert validate_metron_macro(art) == []


def test_macro_hand_built_fixture_validates():
    art = {"schema_version": 2, "as_of": "2026-06-11",
           "series": {"FEDFUNDS": [["2026-06-01", 5.33]]},
           "next_release": {"FEDFUNDS": "2026-07-30"},
           "release_events": [{"date": "2026-07-30", "kind": "fomc", "series_id": "FEDFUNDS", "label": "FOMC decision"}]}
    assert validate_metron_macro(art) == []


def test_macro_missing_release_events_field_fails():
    art = {"schema_version": 2, "as_of": "2026-06-11",
           "series": {"FEDFUNDS": [["2026-06-01", 5.33]]}, "next_release": {}}
    assert validate_metron_macro(art) != []


# ── Metron fundamentals ──────────────────────────────────────────────────────

def test_fundamentals_producer_write_validates():
    s3 = _universe_s3(_UNIVERSE)
    src = lambda syms: {s: {"trailingPE": 30.0, "priceToBook": 6.0, "beta": 1.1} for s in syms}
    mmd.collect_fundamentals(bucket="b", run_date="2026-06-12", s3_client=s3, fundamentals_source=src)
    art = _puts(s3)[f"{mmd.FUNDAMENTALS_PREFIX}latest.json"]
    assert validate_metron_fundamentals(art) == []


def test_fundamentals_hand_built_fixture_validates():
    art = {"schema_version": 5, "as_of": "2026-06-12", "source": "yfinance",
           "fundamentals": {"AAPL": {"trailingPE": 30.0, "sector": "Technology"}}}
    assert validate_metron_fundamentals(art) == []


def test_fundamentals_wrong_field_type_fails():
    art = {"schema_version": 5, "as_of": "2026-06-12", "source": "yfinance",
           "fundamentals": {"AAPL": {"trailingPE": "thirty"}}}
    assert validate_metron_fundamentals(art) != []


# ── Metron analyst + sentiment ───────────────────────────────────────────────

def test_analyst_producer_write_validates():
    s3 = _universe_s3(_UNIVERSE)
    src = lambda syms: {s: {"consensus_rating": "buy", "rating_score": 4.2, "mean_target": 250.0,
                             "median_target": 245.0, "num_analysts": 30} for s in syms}
    mmd.collect_analyst(bucket="b", run_date="2026-06-26", s3_client=s3, analyst_source=src)
    art = _puts(s3)[f"{mmd.ANALYST_PREFIX}latest.json"]
    assert validate_metron_analyst(art) == []


def test_analyst_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "yfinance+finnhub",
           "analyst": {"AAPL": {"consensus_rating": "buy", "num_analysts": 30}}}
    assert validate_metron_analyst(art) == []


def test_sentiment_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "news_aggregates_daily(LM)",
           "sentiment": {"AAPL": {"sentiment": 0.2, "n_articles": 5, "as_of": "2026-06-26"}}}
    assert validate_metron_sentiment(art) == []


def test_sentiment_out_of_range_fails():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "news_aggregates_daily(LM)",
           "sentiment": {"AAPL": {"sentiment": 4.0}}}
    assert validate_metron_sentiment(art) != []


# ── Metron security_performance + valuation_medians ─────────────────────────

def test_security_performance_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "computed",
           "performance": {"AAPL": {
               "period_returns": {"1Y": 0.2}, "ytd_pct": 0.1, "ltm_pct": 0.2,
               "volatility": 0.18, "sharpe": 1.2, "sortino": 1.5, "max_drawdown": -0.1,
               "beta_vs_spy": 1.1, "vs_spy_window": 0.03, "vs_spy_1y": 0.05,
               "n_bars": 260, "history_from": "2025-06-26",
           }}}
    assert validate_metron_security_performance(art) == []


def test_security_performance_missing_required_field_fails():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "computed",
           "performance": {"AAPL": {"period_returns": {}, "ytd_pct": 0.1, "ltm_pct": 0.2}}}
    assert validate_metron_security_performance(art) != []


def test_valuation_medians_producer_write_validates():
    universe = ["AAPL", "MSFT", "JPM"]
    rows = {
        "AAPL": {"trailingPE": 30.0, "priceToBook": 6.0, "sector": "Technology", "country": "United States"},
        "MSFT": {"trailingPE": 34.0, "priceToBook": 10.0, "sector": "Technology", "country": "United States"},
        "JPM": {"trailingPE": 12.0, "priceToBook": 1.8, "sector": "Financial Services", "country": "United States"},
    }
    s3 = MagicMock()
    mmd.collect_valuation_medians(bucket="b", run_date="2026-06-26", s3_client=s3,
                                   universe_source=lambda: universe, valuation_source=lambda syms: rows)
    art = _puts(s3)[f"{mmd.VALUATION_MEDIANS_PREFIX}latest.json"]
    assert validate_metron_valuation_medians(art) == []


def test_valuation_medians_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "yfinance",
           "by_sector": {"Technology": {"n": 2, "trailing_pe": 32.0}},
           "by_country": {"United States": {"n": 2, "trailing_pe": 32.0}}}
    assert validate_metron_valuation_medians(art) == []


def test_valuation_medians_missing_n_fails():
    art = {"schema_version": 1, "as_of": "2026-06-26", "source": "yfinance",
           "by_sector": {"Technology": {"trailing_pe": 32.0}}, "by_country": {}}
    assert validate_metron_valuation_medians(art) != []


# ── Metron intraday latest.json ──────────────────────────────────────────────

def _intraday_stub(last: float):
    quote = {"AAPL": {"last": last, "open": last - 1, "prev_close": last - 0.5,
                       "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}}
    index = {s: {"last": 1.0, "open": 1.0, "prev_close": 1.0,
                 "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}
             for s in mmd.INDEX_PROXY_SYMBOLS}
    fund = {s: {"last": 1.0, "open": 1.0, "prev_close": 1.0,
                "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}
            for s in mmd.FUND_PROXY_ETFS}
    merged = {**quote, **index, **fund}
    return lambda syms: {s: dict(merged[s]) for s in syms if s in merged}


def test_intraday_latest_producer_write_validates():
    import datetime as _dt

    s3 = _universe_s3(_UNIVERSE)
    mmd.collect_intraday(bucket="b", s3_client=s3, intraday_source=_intraday_stub(200.0),
                          now=_dt.datetime(2026, 6, 12, 15, 0, tzinfo=_dt.timezone.utc))
    art = _puts(s3)["market_data/intraday/latest.json"]
    assert validate_metron_intraday_latest(art) == []


def test_intraday_latest_hand_built_fixture_validates():
    art = {"schema_version": 3, "as_of_utc": "2026-06-12T15:00:00Z", "source": "yfinance_delayed",
           "quotes": {"AAPL": {"last": 200.0, "open": 199.0, "prev_close": 199.5,
                                "session_date": "2026-06-12", "prev_session_date": "2026-06-11",
                                "currency": "USD", "suspect": False}},
           "indices": {}, "fund_proxies": {}}
    assert validate_metron_intraday_latest(art) == []


def test_intraday_latest_missing_currency_fails():
    art = {"schema_version": 3, "as_of_utc": "2026-06-12T15:00:00Z", "source": "yfinance_delayed",
           "quotes": {"AAPL": {"last": 200.0, "open": 199.0, "prev_close": 199.5,
                                "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}},
           "indices": {}, "fund_proxies": {}}
    assert validate_metron_intraday_latest(art) != []


# ── constituents.json ────────────────────────────────────────────────────────

def test_constituents_producer_write_validates(monkeypatch):
    fake_get = _make_fake_get(
        sp500_tickers=["AAPL", "MSFT"], sp400_tickers=["ABCD"],
    )
    fake_s3 = MagicMock()
    monkeypatch.setattr(constituents_mod, "boto3", MagicMock(client=lambda *a, **k: fake_s3))
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("requests.get", fake_get)
        result = constituents_mod.collect(bucket="test-bucket", run_date="2026-06-26")
    assert result["status"] == "ok"
    put_call = fake_s3.put_object.call_args_list[0]
    art = json.loads(put_call.kwargs["Body"])
    assert validate_constituents(art) == []


def test_constituents_hand_built_fixture_validates():
    art = {
        "date": "2026-06-26", "tickers": ["AAPL", "MSFT"],
        "sector_map": {"AAPL": "Technology", "MSFT": "Technology"},
        "sector_etf_map": {"AAPL": "XLK", "MSFT": "XLK"},
        "sub_industry_map": {"AAPL": "Technology Hardware", "MSFT": "Systems Software"},
        "sub_sector_etf_map": {"AAPL": "XLK", "MSFT": "XLK"},
        "sp500_count": 2, "sp400_count": 0, "total_count": 2,
        "fetched_at": "2026-06-26T00:00:00+00:00",
    }
    assert validate_constituents(art) == []


def test_constituents_missing_sector_map_fails():
    art = {
        "date": "2026-06-26", "tickers": ["AAPL"],
        "sector_etf_map": {"AAPL": "XLK"}, "sub_industry_map": {}, "sub_sector_etf_map": {},
        "sp500_count": 1, "sp400_count": 0, "total_count": 1, "fetched_at": "2026-06-26T00:00:00+00:00",
    }
    assert validate_constituents(art) != []


# ── universe_classification ──────────────────────────────────────────────────

def _make_yf(info_by_ticker: dict[str, dict]) -> MagicMock:
    yf_mock = MagicMock()

    def ticker_factory(t):
        ticker_obj = MagicMock()
        ticker_obj.info = info_by_ticker.get(t, {})
        return ticker_obj

    yf_mock.Ticker.side_effect = ticker_factory
    return yf_mock


def test_universe_classification_producer_write_validates():
    from unittest.mock import patch

    yf_mock = _make_yf({
        "AAPL": {"sector": "Technology", "country": "United States", "industry": "Consumer Electronics"},
    })
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.universe_classification.boto3.client", return_value=fake_s3):
        result = uc.collect(bucket="test-bucket", tickers=["AAPL"], run_date="2026-06-28", inter_request_delay=0.0)
    assert result["status"] == "ok"
    put_call = fake_s3.put_object.call_args_list[0]
    art = json.loads(put_call.kwargs["Body"])
    assert validate_universe_classification(art) == []


def test_universe_classification_hand_built_fixture_validates():
    art = {"schema_version": 1, "as_of": "2026-06-28", "source": "yfinance",
           "ticker_count": 1, "ok_count": 1,
           "data": {"AAPL": {"sector": "Technology", "country": "United States", "industry": "Consumer Electronics"}}}
    assert validate_universe_classification(art) == []


def test_universe_classification_missing_field_fails():
    art = {"schema_version": 1, "as_of": "2026-06-28", "source": "yfinance",
           "ticker_count": 1, "ok_count": 1,
           "data": {"AAPL": {"sector": "Technology", "country": "United States"}}}
    assert validate_universe_classification(art) != []


# ── ArcticDB universe library row (alpha-engine-config-I10828) ──────────────
# atr_14_pct and VWAP: crucible-executor's price_cache.load_atr_14_pct hard-fails
# when atr_14_pct is absent from the frame; VWAP is the raw-price column
# builders/daily_append.py writes alongside OHLCV. Hand-built fixture only --
# no producer-write test here because daily_append's per-ticker Arctic write
# path needs a live/mocked ArcticDB library, out of scope for this contract
# fixture (mirrors the existing schema-only note at the top of this file).

def test_arctic_universe_row_with_atr_and_vwap_validates():
    art = {
        "symbol": "AAPL", "index_date": "2026-06-26",
        "Open": 200.0, "High": 202.0, "Low": 199.0, "Close": 201.0, "Volume": 5_000_000,
        "VWAP": 200.5, "atr_14_pct": 0.0182,
    }
    assert validate_arctic_universe_row(art) == []


def test_arctic_universe_row_with_null_atr_and_vwap_validates():
    # VWAP null on yfinance/FRED-sourced rows; atr_14_pct null during the
    # <14-row ATR warmup window. Both additive/nullable, never required.
    art = {
        "symbol": "AAPL", "index_date": "2026-06-26",
        "Open": 200.0, "High": 202.0, "Low": 199.0, "Close": 201.0, "Volume": 5_000_000,
        "VWAP": None, "atr_14_pct": None,
    }
    assert validate_arctic_universe_row(art) == []


def test_arctic_universe_row_without_atr_and_vwap_still_validates():
    # Rows written before this contract update carry neither column.
    art = {
        "symbol": "AAPL", "index_date": "2026-06-26",
        "Open": 200.0, "High": 202.0, "Low": 199.0, "Close": 201.0, "Volume": 5_000_000,
    }
    assert validate_arctic_universe_row(art) == []


def test_arctic_universe_row_negative_vwap_fails():
    art = {
        "symbol": "AAPL", "index_date": "2026-06-26",
        "Open": 200.0, "High": 202.0, "Low": 199.0, "Close": 201.0, "Volume": 5_000_000,
        "VWAP": -1.0,
    }
    assert validate_arctic_universe_row(art) != []
