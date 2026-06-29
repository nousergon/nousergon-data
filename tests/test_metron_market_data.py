"""Metron market-data producer — EOD closes + FX for Metron's held universe.

`alpha-engine-data` is the system's sole market-data source; Metron consumes these
artifacts. Covers: reading Metron's published universe, building the versioned
closes + FX artifacts, writing dated + ``latest`` keys, omitting unpriceable symbols,
the dry-run no-write path, and the fail-soft empty-universe skip.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest import mock
from unittest.mock import MagicMock

from collectors import metron_market_data as mmd


def _universe_s3(universe: dict | None, heartbeat_ts: str | None = None) -> MagicMock:
    """A MagicMock S3 whose get_object dispatches per key: the Metron universe JSON
    (raises if None) and, when ``heartbeat_ts`` is given, a fresh UI-heartbeat object
    (the intraday demand gate); any other key raises NoSuchKey."""
    s3 = MagicMock()

    def _get(Bucket, Key):
        def _body(obj):
            body = MagicMock()
            body.read.return_value = json.dumps(obj).encode()
            return {"Body": body}
        if Key == "metron/ui_heartbeat.json":
            if heartbeat_ts is None:
                raise Exception("NoSuchKey")
            return _body({"ts": heartbeat_ts})
        if universe is None:
            raise Exception("NoSuchKey")
        return _body(universe)

    s3.get_object.side_effect = _get
    return s3


def _puts(s3: MagicMock) -> dict[str, dict]:
    """Map every put_object call to {key: parsed-json-body}."""
    out = {}
    for call in s3.put_object.call_args_list:
        kw = call.kwargs
        out[kw["Key"]] = json.loads(kw["Body"].decode())
    return out


_UNIVERSE = {
    "schema_version": 1, "as_of": "2026-06-11", "source": "metron",
    "holdings": [
        {"yf_symbol": "AAPL", "currency": "USD"},
        {"yf_symbol": "1299.HK", "currency": "HKD"},
    ],
    "currencies": ["HKD"],
}


def test_builds_and_writes_closes_and_fx_artifacts():
    s3 = _universe_s3(_UNIVERSE)
    closes = lambda syms: {"AAPL": (201.5, "2026-06-11"), "1299.HK": (64.2, "2026-06-11")}
    fx = lambda ccys: {"HKD": 0.1282}

    result = mmd.collect(
        bucket="b", run_date="2026-06-11", s3_client=s3, close_source=closes, fx_source=fx
    )

    assert result["status"] == "ok"
    assert result["universe"] == 2 and result["closes"] == 2 and result["fx"] == 1
    puts = _puts(s3)
    # Dated + latest for both artifacts.
    assert set(puts) == {
        "market_data/eod_closes/2026-06-11.json", "market_data/eod_closes/latest.json",
        "market_data/fx/2026-06-11.json", "market_data/fx/latest.json",
    }
    closes_art = puts["market_data/eod_closes/latest.json"]
    assert closes_art["schema_version"] == mmd.CLOSES_SCHEMA_VERSION
    assert closes_art["source"] == "alpha-engine-data"
    # Currency carried from the universe; foreign listing keyed by yf_symbol.
    assert closes_art["closes"]["1299.HK"] == {"close": 64.2, "currency": "HKD", "bar_date": "2026-06-11"}
    fx_art = puts["market_data/fx/latest.json"]
    assert fx_art["base"] == "USD" and fx_art["rates"] == {"HKD": 0.1282}
    # Dated == latest (same payload written to both).
    assert puts["market_data/eod_closes/2026-06-11.json"] == closes_art


def test_unpriceable_symbol_is_omitted_not_fabricated():
    s3 = _universe_s3(_UNIVERSE)
    closes = lambda syms: {"AAPL": (201.5, "2026-06-11")}  # 1299.HK unpriceable
    result = mmd.collect(bucket="b", run_date="2026-06-11", s3_client=s3, close_source=closes, fx_source=lambda c: {})
    assert result["closes"] == 1
    closes_art = _puts(s3)["market_data/eod_closes/latest.json"]
    assert "1299.HK" not in closes_art["closes"]
    assert "AAPL" in closes_art["closes"]


def test_empty_universe_skips_without_writing():
    s3 = _universe_s3({"holdings": [], "currencies": []})
    result = mmd.collect(bucket="b", run_date="2026-06-11", s3_client=s3,
                         close_source=lambda s: {}, fx_source=lambda c: {})
    assert result["status"] == "skipped"
    s3.put_object.assert_not_called()


def test_missing_universe_object_fail_soft_skips():
    s3 = _universe_s3(None)  # get_object raises
    result = mmd.collect(bucket="b", run_date="2026-06-11", s3_client=s3,
                         close_source=lambda s: {}, fx_source=lambda c: {})
    assert result["status"] == "skipped"
    s3.put_object.assert_not_called()


def test_dry_run_writes_nothing():
    s3 = _universe_s3(_UNIVERSE)
    result = mmd.collect(
        bucket="b", run_date="2026-06-11", dry_run=True, s3_client=s3,
        close_source=lambda s: {"AAPL": (201.5, "2026-06-11")}, fx_source=lambda c: {"HKD": 0.1282},
    )
    assert result["status"] == "ok_dry_run"
    s3.put_object.assert_not_called()


def test_load_universe_parses_holdings_and_currencies():
    s3 = _universe_s3(_UNIVERSE)
    holdings, currencies = mmd.load_metron_universe("b", s3)
    assert {h["yf_symbol"] for h in holdings} == {"AAPL", "1299.HK"}
    assert currencies == ["HKD"]


class TestHistory:
    def test_writes_per_symbol_close_history_and_per_currency_fx_history(self):
        s3 = _universe_s3(_UNIVERSE)
        close_hist = lambda syms: {"AAPL": [("2026-06-10", 200.0), ("2026-06-11", 201.5)], "1299.HK": [("2026-06-11", 64.2)]}
        fx_hist = lambda ccys: {"HKD": [("2026-06-10", 0.128), ("2026-06-11", 0.1282)]}
        result = mmd.collect_history(bucket="b", s3_client=s3, close_history_source=close_hist, fx_history_source=fx_hist)
        assert result["status"] == "ok" and result["close_series"] == 2 and result["fx_series"] == 1
        puts = _puts(s3)
        assert set(puts) == {"market_data/close_history/AAPL.json", "market_data/close_history/1299.HK.json",
                             "market_data/fx_history/HKD.json"}
        aapl = puts["market_data/close_history/AAPL.json"]
        assert aapl["yf_symbol"] == "AAPL" and aapl["currency"] == "USD"
        assert aapl["closes"] == [["2026-06-10", 200.0], ["2026-06-11", 201.5]]
        assert puts["market_data/fx_history/HKD.json"]["rates"][-1] == ["2026-06-11", 0.1282]

    def test_publishes_factor_etf_history_for_risk_attribution(self):
        # The factor/sector ETFs (SPY/MTUM/.../XLK) must be requested + published even
        # though they're not held, or Metron's risk/attribution can't backfill (metron-ops#43).
        s3 = _universe_s3(_UNIVERSE)
        requested: list[str] = []

        def close_hist(syms):
            requested.extend(syms)
            return {s: [("2026-06-11", 100.0)] for s in syms}

        result = mmd.collect_history(bucket="b", s3_client=s3, close_history_source=close_hist, fx_history_source=lambda c: {})
        # Factor/sector ETFs + the index proxies (markets-strip YTD/LTM) + the fund-proxy
        # ETFs (late-fund-NAV reconcile backstop) must all be requested + published.
        assert set(mmd.RISK_FACTOR_ETFS) <= set(requested)  # all factor ETFs requested
        assert set(mmd.INDEX_PROXY_SYMBOLS) <= set(requested)  # QQQ/IWM/ONEQ get close_history → YTD/LTM
        assert set(mmd.FUND_PROXY_ETFS) <= set(requested)  # IXUS published for the fund reconcile
        assert "AAPL" in requested  # held symbols still included
        puts = _puts(s3)
        for etf in ("SPY", "XLK", "MTUM", "QQQ", "IWM", "ONEQ", "IXUS"):
            key = f"market_data/close_history/{etf}.json"
            assert key in puts and puts[key]["currency"] == "USD"
        assert result["close_series"] == len(set(
            ["AAPL", "1299.HK", *mmd.RISK_FACTOR_ETFS, *mmd.INDEX_PROXY_SYMBOLS, *mmd.FUND_PROXY_ETFS]
        ))

    def test_history_dry_run_and_empty_universe(self):
        s3 = _universe_s3(_UNIVERSE)
        r = mmd.collect_history(bucket="b", dry_run=True, s3_client=s3, close_history_source=lambda s: {"AAPL": [("2026-06-11", 201.5)]}, fx_history_source=lambda c: {})
        assert r["status"] == "ok_dry_run"
        s3.put_object.assert_not_called()
        s3b = _universe_s3({"holdings": [], "currencies": []})
        assert mmd.collect_history(bucket="b", s3_client=s3b, close_history_source=lambda s: {}, fx_history_source=lambda c: {})["status"] == "skipped"


class TestMacro:
    def test_writes_macro_series_artifact(self):
        s3 = _universe_s3(_UNIVERSE)  # macro doesn't read the universe; reuse the fake S3
        macro_src = lambda ids: {"FEDFUNDS": [("2026-05-01", 5.33), ("2026-06-01", 5.33)],
                                 "VIXCLS": [("2026-06-10", 14.2), ("2026-06-11", 13.8)]}
        result = mmd.collect_macro(bucket="b", run_date="2026-06-11", s3_client=s3, macro_source=macro_src)
        assert result["status"] == "ok" and result["series"] == 2
        puts = _puts(s3)
        assert set(puts) == {"market_data/macro/latest.json"}
        art = puts["market_data/macro/latest.json"]
        assert art["schema_version"] == mmd.MACRO_SCHEMA_VERSION
        assert art["series"]["FEDFUNDS"] == [["2026-05-01", 5.33], ["2026-06-01", 5.33]]
        assert art["series"]["VIXCLS"][-1] == ["2026-06-11", 13.8]

    def test_macro_dry_run_and_no_series(self):
        s3 = _universe_s3(_UNIVERSE)
        r = mmd.collect_macro(bucket="b", run_date="2026-06-11", dry_run=True, s3_client=s3,
                              macro_source=lambda ids: {"FEDFUNDS": [("2026-06-01", 5.33)]})
        assert r["status"] == "ok_dry_run"
        s3.put_object.assert_not_called()
        assert mmd.collect_macro(bucket="b", s3_client=s3, macro_source=lambda ids: {})["status"] == "skipped"

    def test_macro_publishes_next_release_and_events(self):
        # v2 (metron-ops#49): next_release per series + the macro event calendar, via an
        # injected release_source (no FRED network in tests).
        s3 = _universe_s3(_UNIVERSE)
        macro_src = lambda ids: {"FEDFUNDS": [("2026-06-01", 5.33)], "UNRATE": [("2026-05-01", 4.1)]}
        rel_src = lambda ids, run_date: (
            {"FEDFUNDS": "2026-07-29", "UNRATE": "2026-07-02"},
            [{"date": "2026-07-02", "kind": "release", "series_id": "UNRATE", "label": "Employment Situation"},
             {"date": "2026-07-29", "kind": "fomc", "series_id": "FOMC", "label": "FOMC Meeting"}],
        )
        result = mmd.collect_macro(
            bucket="b", run_date="2026-06-11", s3_client=s3, macro_source=macro_src, release_source=rel_src
        )
        assert result["status"] == "ok" and result["next_release"] == 2 and result["release_events"] == 2
        art = _puts(s3)["market_data/macro/latest.json"]
        assert art["schema_version"] == 2
        assert art["next_release"] == {"FEDFUNDS": "2026-07-29", "UNRATE": "2026-07-02"}
        assert art["release_events"][0]["kind"] == "release" and art["release_events"][1]["kind"] == "fomc"

    def test_macro_without_release_source_defaults_empty(self):
        # Injected macro_source but no release_source + no api_key → release fields empty,
        # series artifact still written (best-effort calendar never blocks the primary data).
        s3 = _universe_s3(_UNIVERSE)
        mmd.collect_macro(bucket="b", run_date="2026-06-11", s3_client=s3,
                          macro_source=lambda ids: {"FEDFUNDS": [("2026-06-01", 5.33)]})
        art = _puts(s3)["market_data/macro/latest.json"]
        assert art["next_release"] == {} and art["release_events"] == []


class TestReference:
    def test_writes_sectors_countries_and_earnings_keyed_by_yf_symbol(self):
        s3 = _universe_s3(_UNIVERSE)
        result = mmd.collect_reference(
            bucket="b", run_date="2026-06-11", s3_client=s3,
            sector_source=lambda syms: {"AAPL": "Technology", "1299.HK": "Financial Services"},
            country_source=lambda syms: {"AAPL": "United States", "1299.HK": "Hong Kong"},
            benchmark_source=lambda: {"Technology": 0.30, "Financial Services": 0.13},
            earnings_source=lambda syms: {"AAPL": "2026-07-30"},
        )
        assert result["status"] == "ok" and result["sectors"] == 2 and result["countries"] == 2 and result["earnings"] == 1
        puts = _puts(s3)
        assert set(puts) == {"market_data/sectors/latest.json", "market_data/earnings/latest.json"}
        sec = puts["market_data/sectors/latest.json"]
        assert sec["schema_version"] == mmd.SECTORS_SCHEMA_VERSION
        assert sec["sectors"] == {"1299.HK": "Financial Services", "AAPL": "Technology"}
        assert sec["countries"] == {"1299.HK": "Hong Kong", "AAPL": "United States"}
        assert sec["spy_sector_weights"]["Technology"] == 0.30
        assert puts["market_data/earnings/latest.json"]["earnings"] == {"AAPL": "2026-07-30"}

    def test_sector_and_country_share_one_info_pass(self):
        """When neither source is injected, the single ``.info`` pass populates both maps."""
        s3 = _universe_s3(_UNIVERSE)
        calls = {"n": 0}

        def fake_classify(yf_symbols):
            calls["n"] += 1
            return ({"AAPL": "Technology"}, {"AAPL": "United States"})

        with mock.patch.object(mmd, "_yfinance_classification", fake_classify), \
             mock.patch.object(mmd, "_yfinance_spy_weights", lambda: {}), \
             mock.patch.object(mmd, "_yfinance_earnings", lambda s: {}):
            result = mmd.collect_reference(bucket="b", run_date="2026-06-11", s3_client=s3)
        assert calls["n"] == 1  # one shared pass, not one per dimension
        assert result["sectors"] == 1 and result["countries"] == 1
        sec = _puts(s3)["market_data/sectors/latest.json"]
        assert sec["countries"] == {"AAPL": "United States"}

    def test_reference_dry_run_and_empty_universe(self):
        s3 = _universe_s3(_UNIVERSE)
        r = mmd.collect_reference(bucket="b", run_date="2026-06-11", dry_run=True, s3_client=s3,
                                  sector_source=lambda s: {"AAPL": "Technology"}, country_source=lambda s: {"AAPL": "United States"},
                                  benchmark_source=lambda: {}, earnings_source=lambda s: {})
        assert r["status"] == "ok_dry_run"
        s3.put_object.assert_not_called()
        s3b = _universe_s3({"holdings": [], "currencies": []})
        assert mmd.collect_reference(bucket="b", s3_client=s3b, sector_source=lambda s: {}, country_source=lambda s: {}, benchmark_source=lambda: {}, earnings_source=lambda s: {})["status"] == "skipped"


class TestFundamentals:
    def test_writes_passthrough_fundamentals_keyed_by_yf_symbol(self):
        s3 = _universe_s3(_UNIVERSE)
        source = lambda syms: {
            "AAPL": {"trailingPE": 31.2, "debtToEquity": 145.0, "sector": "Technology"},
            "1299.HK": {"trailingPE": 12.4, "dividendYield": 0.031},
        }
        result = mmd.collect_fundamentals(
            bucket="b", run_date="2026-06-12", s3_client=s3, fundamentals_source=source
        )
        assert result["status"] == "ok" and result["fundamentals"] == 2
        puts = _puts(s3)
        assert set(puts) == {"market_data/fundamentals/latest.json"}
        art = puts["market_data/fundamentals/latest.json"]
        assert art["schema_version"] == mmd.FUNDAMENTALS_SCHEMA_VERSION
        assert art["source"] == "yfinance"
        # Pass-through: values land exactly as the source returned them (no unit math).
        assert art["fundamentals"]["AAPL"]["debtToEquity"] == 145.0
        assert art["fundamentals"]["1299.HK"]["dividendYield"] == 0.031

    def test_fundamentals_dry_run_and_empty_universe(self):
        s3 = _universe_s3(_UNIVERSE)
        result = mmd.collect_fundamentals(
            bucket="b", s3_client=s3, dry_run=True, fundamentals_source=lambda s: {"AAPL": {"beta": 1.2}}
        )
        assert result["status"] == "ok_dry_run"
        assert not s3.put_object.called
        empty = _universe_s3({"holdings": [], "currencies": []})
        assert mmd.collect_fundamentals(bucket="b", s3_client=empty)["status"] == "skipped"


class TestAnalyst:
    def test_writes_consensus_keyed_by_yf_symbol(self):
        s3 = _universe_s3(_UNIVERSE)
        source = lambda syms: {
            "AAPL": {"consensus_rating": "buy", "rating_score": 0.5,
                     "mean_target": 240.0, "median_target": 238.0, "num_analysts": 38},
            "1299.HK": {"consensus_rating": "strongBuy", "rating_score": 1.0,
                        "mean_target": 95.0, "num_analysts": 12},
        }
        result = mmd.collect_analyst(
            bucket="b", run_date="2026-06-26", s3_client=s3, analyst_source=source
        )
        assert result["status"] == "ok" and result["analyst"] == 2
        puts = _puts(s3)
        assert set(puts) == {"market_data/analyst/latest.json"}
        art = puts["market_data/analyst/latest.json"]
        assert art["schema_version"] == mmd.ANALYST_SCHEMA_VERSION
        assert art["source"] == "yfinance+finnhub"
        # Pass-through: values land exactly as the source returned them.
        assert art["analyst"]["AAPL"]["mean_target"] == 240.0
        assert art["analyst"]["1299.HK"]["rating_score"] == 1.0

    def test_analyst_dry_run_and_empty_universe(self):
        s3 = _universe_s3(_UNIVERSE)
        result = mmd.collect_analyst(
            bucket="b", s3_client=s3, dry_run=True,
            analyst_source=lambda s: {"AAPL": {"consensus_rating": "hold"}},
        )
        assert result["status"] == "ok_dry_run"
        assert not s3.put_object.called
        empty = _universe_s3({"holdings": [], "currencies": []})
        assert mmd.collect_analyst(bucket="b", s3_client=empty)["status"] == "skipped"

    def test_yfinance_analyst_derives_rating_score_and_omits_empty(self, monkeypatch):
        """`_yfinance_analyst` maps the rating ladder → signed score, drops None
        fields, and omits a symbol whose snapshot is entirely empty (coverage gap,
        not zeros)."""
        from types import SimpleNamespace

        snaps = {
            "AAPL": SimpleNamespace(consensus_rating="strongBuy", mean_target=240.0,
                                    median_target=238.0, num_analysts=40),
            "MSFT": SimpleNamespace(consensus_rating="sell", mean_target=None,
                                    median_target=None, num_analysts=5),
            # entirely empty → omitted
            "NOPE": SimpleNamespace(consensus_rating=None, mean_target=None,
                                    median_target=None, num_analysts=None),
            # adapter returns None (fetch miss) → omitted
            "MISS": None,
        }

        class _FakeAdapter:
            def fetch(self, ticker):
                return snaps.get(ticker)

        monkeypatch.setattr(
            "collectors.analyst_sources.YfinanceAnalystAdapter", lambda: _FakeAdapter()
        )
        # No Finnhub key → no rating backfill (secrets via the lib, not os.environ).
        monkeypatch.setattr("alpha_engine_lib.secrets.get_secret", lambda *a, **k: "")
        out = mmd._yfinance_analyst(["AAPL", "MSFT", "NOPE", "MISS"])
        assert set(out) == {"AAPL", "MSFT"}  # empty + miss omitted
        assert out["AAPL"]["rating_score"] == 1.0  # strongBuy
        assert out["MSFT"]["rating_score"] == -0.5  # sell
        assert "mean_target" not in out["MSFT"]  # None dropped


class TestSentiment:
    def test_writes_sentiment_keyed_by_yf_symbol(self):
        s3 = _universe_s3(_UNIVERSE)
        source = lambda syms: {
            "AAPL": {"sentiment": 0.42, "sentiment_mean": 0.30, "n_articles": 12,
                     "event_count": 2, "event_severity_max": 0.6, "as_of": "2026-06-25"},
            "1299.HK": {"sentiment": -0.1, "n_articles": 3, "as_of": "2026-06-24"},
        }
        result = mmd.collect_sentiment(
            bucket="b", run_date="2026-06-26", s3_client=s3, sentiment_source=source
        )
        assert result["status"] == "ok" and result["sentiment"] == 2
        puts = _puts(s3)
        assert set(puts) == {"market_data/sentiment/latest.json"}
        art = puts["market_data/sentiment/latest.json"]
        assert art["schema_version"] == mmd.SENTIMENT_SCHEMA_VERSION
        assert art["source"] == "news_aggregates_daily(LM)"
        assert art["sentiment"]["AAPL"]["sentiment"] == 0.42
        assert art["sentiment"]["AAPL"]["as_of"] == "2026-06-25"

    def test_sentiment_dry_run_and_empty_universe(self):
        s3 = _universe_s3(_UNIVERSE)
        result = mmd.collect_sentiment(
            bucket="b", s3_client=s3, dry_run=True,
            sentiment_source=lambda s: {"AAPL": {"sentiment": 0.1}},
        )
        assert result["status"] == "ok_dry_run"
        assert not s3.put_object.called
        empty = _universe_s3({"holdings": [], "currencies": []})
        assert mmd.collect_sentiment(bucket="b", s3_client=empty)["status"] == "skipped"

    def test_news_sentiment_latest_per_ticker_and_omits_uncovered(self, monkeypatch):
        """`_news_sentiment` picks the most-recent row per ticker, maps
        trusted_mean → `sentiment`, coerces NaN → None, and omits held symbols
        with no news coverage."""
        import pandas as pd

        df = pd.DataFrame([
            # AAPL: two dates → the later (06-25) row must win
            {"ticker": "AAPL", "aggregate_date": "2026-06-24", "lm_sentiment_trusted_mean": 0.10,
             "lm_sentiment_mean": 0.05, "n_articles": 4, "event_count": 0, "event_severity_max": 0.0},
            {"ticker": "AAPL", "aggregate_date": "2026-06-25", "lm_sentiment_trusted_mean": 0.42,
             "lm_sentiment_mean": 0.30, "n_articles": 12, "event_count": 2, "event_severity_max": 0.6},
            # TSLA present in news but NOT in the held universe → filtered out
            {"ticker": "TSLA", "aggregate_date": "2026-06-25", "lm_sentiment_trusted_mean": 0.9,
             "lm_sentiment_mean": 0.9, "n_articles": 50, "event_count": 1, "event_severity_max": 0.2},
        ])
        monkeypatch.setattr("collectors.daily_news.read_daily_news", lambda *a, **k: df)
        out = mmd._news_sentiment(["AAPL", "1299.HK"])  # 1299.HK has no news row → omitted
        assert set(out) == {"AAPL"}
        assert out["AAPL"]["sentiment"] == 0.42  # latest date won
        assert out["AAPL"]["as_of"] == "2026-06-25"
        assert out["AAPL"]["n_articles"] == 12


class TestIntraday:
    _QUOTES = {
        "AAPL": {"last": 202.1, "open": 200.5, "prev_close": 201.5,
                 "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
        "1299.HK": {"last": 64.8, "open": 64.1, "prev_close": 64.2,
                    "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
    }
    _INDEX_QUOTES = {
        "SPY": {"last": 605.2, "open": 603.0, "prev_close": 602.4,
                "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
        "ONEQ": {"last": 101.4, "open": 100.8, "prev_close": 100.5,
                 "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
        "QQQ": {"last": 540.1, "open": 538.5, "prev_close": 537.0,
                "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
        "IWM": {"last": 215.3, "open": 216.0, "prev_close": 216.5,
                "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
    }
    # Fund-proxy quotes — SPY already in _INDEX_QUOTES; IXUS is the intl proxy. The real
    # fetcher is called separately for the fund-proxy set, so the stub must know IXUS too.
    _FUND_PROXY_QUOTES = {
        "IXUS": {"last": 72.4, "open": 72.0, "prev_close": 71.9,
                 "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
    }

    @staticmethod
    def _stub(*maps):
        """An input-aware intraday source: returns only the requested symbols it knows
        (mirrors the real fetcher, which is called separately for held + index symbols)."""
        merged: dict[str, dict] = {}
        for m in maps:
            merged.update(m)
        return lambda syms: {s: dict(merged[s]) for s in syms if s in merged}

    # Friday 2026-06-12 15:00 UTC = 11:00 ET — mid-session (EDT).
    _RTH = datetime(2026, 6, 12, 15, 0, tzinfo=timezone.utc)

    def _s3(self, *, heartbeat_offset_s: int | None = 60, universe: dict | None = _UNIVERSE):
        """Fake S3 with a heartbeat ``offset_s`` seconds BEFORE the test's RTH now."""
        ts = None
        if heartbeat_offset_s is not None:
            ts = (self._RTH - timedelta(seconds=heartbeat_offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return _universe_s3(universe, heartbeat_ts=ts)

    def test_writes_quotes_with_currency_when_open_and_app_active(self):
        s3 = self._s3()
        result = mmd.collect_intraday(
            bucket="b", s3_client=s3,
            intraday_source=self._stub(self._QUOTES, self._INDEX_QUOTES, self._FUND_PROXY_QUOTES),
            now=self._RTH,
        )
        assert result["status"] == "ok" and result["quotes"] == 2 and result["indices"] == 4
        assert result["fund_proxies"] == len(mmd.FUND_PROXY_ETFS)
        puts = _puts(s3)
        assert set(puts) == {"market_data/intraday/latest.json"}
        art = puts["market_data/intraday/latest.json"]
        assert art["schema_version"] == mmd.INTRADAY_SCHEMA_VERSION == 3
        assert art["source"] == "yfinance_delayed"
        assert art["as_of_utc"] == "2026-06-12T15:00:00Z"
        # Currency joined from the universe (the consumer FX-converts the P&L legs).
        assert art["quotes"]["1299.HK"]["currency"] == "HKD"
        # Normal moves carry no suspect flag.
        assert "suspect" not in art["quotes"]["AAPL"]
        # Index proxies land under `indices` (USD), same per-symbol shape as quotes.
        assert set(art["indices"]) == set(mmd.INDEX_PROXY_SYMBOLS)
        assert art["indices"]["SPY"]["currency"] == "USD"
        assert art["indices"]["SPY"]["last"] == 605.2 and art["indices"]["SPY"]["prev_close"] == 602.4
        # Fund proxies land under a DEDICATED `fund_proxies` map (USD), not `indices`.
        assert set(art["fund_proxies"]) == set(mmd.FUND_PROXY_ETFS)
        assert "IXUS" in art["fund_proxies"] and art["fund_proxies"]["IXUS"]["currency"] == "USD"
        assert art["fund_proxies"]["IXUS"]["last"] == 72.4

    def test_index_proxies_fetched_even_with_empty_universe(self):
        """The markets strip is market context — published even when nothing is held."""
        empty = self._s3(universe={"holdings": [], "currencies": []})
        result = mmd.collect_intraday(
            bucket="b", s3_client=empty, intraday_source=self._stub(self._INDEX_QUOTES), now=self._RTH,
        )
        assert result["status"] == "ok" and result["quotes"] == 0 and result["indices"] == 4
        art = _puts(empty)["market_data/intraday/latest.json"]
        assert art["quotes"] == {}
        assert set(art["indices"]) == set(mmd.INDEX_PROXY_SYMBOLS)

    def test_suspect_flag_on_extreme_move_never_dropped(self):
        s3 = self._s3()
        quotes = {"AAPL": {"last": 350.0, "open": 200.5, "prev_close": 201.5,
                           "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}}
        result = mmd.collect_intraday(
            bucket="b", s3_client=s3, intraday_source=self._stub(quotes), now=self._RTH
        )
        assert result["status"] == "ok"
        art = _puts(s3)["market_data/intraday/latest.json"]
        q = art["quotes"]["AAPL"]
        assert q["suspect"] is True
        assert q["last"] == 350.0  # flagged, never clamped/dropped — no fabrication

    def test_default_stays_warm_without_heartbeat(self):
        """Owner build (gate OFF): in-session ticks publish even with no/stale heartbeat,
        so the markets strip never freezes at the morning quote after the app is idle."""
        # No heartbeat key at all → still writes (heartbeat never read).
        absent = self._s3(heartbeat_offset_s=None)
        r1 = mmd.collect_intraday(
            bucket="b", s3_client=absent, intraday_source=self._stub(self._QUOTES, self._INDEX_QUOTES),
            now=self._RTH,
        )
        assert r1["status"] == "ok" and r1["indices"] == 4
        assert absent.put_object.called
        # Stale heartbeat (older than HEARTBEAT_FRESH_SECONDS) → still writes.
        stale = self._s3(heartbeat_offset_s=mmd.HEARTBEAT_FRESH_SECONDS + 60)
        r2 = mmd.collect_intraday(
            bucket="b", s3_client=stale, intraday_source=self._stub(self._QUOTES, self._INDEX_QUOTES),
            now=self._RTH,
        )
        assert r2["status"] == "ok" and _puts(stale)["market_data/intraday/latest.json"]

    def test_require_heartbeat_gate_skips_when_inactive(self):
        """The opt-in demand gate (multi-tenant): Metron closed (no/stale heartbeat) →
        zero quote fetches when require_heartbeat=True."""
        calls = []
        source = lambda s: calls.append(1) or {}
        # No heartbeat key at all.
        absent = self._s3(heartbeat_offset_s=None)
        r1 = mmd.collect_intraday(
            bucket="b", s3_client=absent, intraday_source=source, now=self._RTH, require_heartbeat=True,
        )
        assert r1["status"] == "skipped" and "heartbeat" in r1["reason"]
        # Stale heartbeat (older than HEARTBEAT_FRESH_SECONDS).
        stale = self._s3(heartbeat_offset_s=mmd.HEARTBEAT_FRESH_SECONDS + 60)
        r2 = mmd.collect_intraday(
            bucket="b", s3_client=stale, intraday_source=source, now=self._RTH, require_heartbeat=True,
        )
        assert r2["status"] == "skipped" and "heartbeat" in r2["reason"]
        assert not calls and not absent.put_object.called and not stale.put_object.called
        # A fresh heartbeat opens the gate → writes.
        active = mmd.collect_intraday(
            bucket="b", s3_client=self._s3(), intraday_source=self._stub(self._QUOTES, self._INDEX_QUOTES),
            now=self._RTH, require_heartbeat=True,
        )
        assert active["status"] == "ok"
        # force bypasses BOTH gates even with the gate on (manual/debug runs).
        forced = mmd.collect_intraday(
            bucket="b", s3_client=self._s3(heartbeat_offset_s=None),
            intraday_source=self._stub(self._QUOTES, self._INDEX_QUOTES),
            now=self._RTH, force=True, require_heartbeat=True,
        )
        assert forced["status"] == "ok"

    def test_skips_outside_session_before_heartbeat_read(self):
        calls = []
        source = lambda s: calls.append(1) or {}
        s3 = self._s3()
        # Friday 22:00 UTC = 18:00 ET — after close.
        late = datetime(2026, 6, 12, 22, 0, tzinfo=timezone.utc)
        assert mmd.collect_intraday(bucket="b", s3_client=s3, intraday_source=source, now=late)["status"] == "skipped"
        # Saturday mid-window-hours — weekend.
        sat = datetime(2026, 6, 13, 15, 0, tzinfo=timezone.utc)
        assert mmd.collect_intraday(bucket="b", s3_client=s3, intraday_source=source, now=sat)["status"] == "skipped"
        assert not calls

    def test_market_window_exchange_calendar(self):
        mk = lambda y, mo, d, h, mi: datetime(y, mo, d, h, mi, tzinfo=timezone.utc)
        # EDT regular day (2026-06-12): session 13:30–20:00 UTC, ±5 min margin.
        assert not mmd.in_us_market_window(mk(2026, 6, 12, 13, 24))
        assert mmd.in_us_market_window(mk(2026, 6, 12, 13, 25))
        assert mmd.in_us_market_window(mk(2026, 6, 12, 20, 5))
        assert not mmd.in_us_market_window(mk(2026, 6, 12, 20, 6))
        # EST regular day (2026-12-15): session 14:30–21:00 UTC — the old widened-UTC
        # heuristic would have opened an hour early; the ET evaluation does not.
        assert not mmd.in_us_market_window(mk(2026, 12, 15, 13, 30))
        assert mmd.in_us_market_window(mk(2026, 12, 15, 14, 25))
        assert mmd.in_us_market_window(mk(2026, 12, 15, 21, 5))
        # NYSE holiday (Thanksgiving 2026-11-26) — closed all day.
        assert not mmd.in_us_market_window(mk(2026, 11, 26, 15, 0))
        # Half-day (day after Thanksgiving, EST): closes 13:00 ET = 18:00 UTC.
        assert mmd.in_us_market_window(mk(2026, 11, 27, 17, 0))
        assert mmd.in_us_market_window(mk(2026, 11, 27, 18, 5))
        assert not mmd.in_us_market_window(mk(2026, 11, 27, 19, 0))  # old heuristic said open

    def test_intraday_dry_run_writes_nothing(self):
        s3 = self._s3()
        result = mmd.collect_intraday(
            bucket="b", s3_client=s3, dry_run=True,
            intraday_source=self._stub(self._QUOTES, self._INDEX_QUOTES), now=self._RTH,
        )
        assert result["status"] == "ok_dry_run" and result["quotes"] == 2 and result["indices"] == 4
        assert not s3.put_object.called
