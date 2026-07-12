"""features/metron_supplemental.py — Metron-held/watchlisted tickers outside the
S&P500+400 factor-scoring universe (metron-ops#177). Covers: the uncovered-ticker
diff, the per-ticker compute loop (skip on missing OHLCV / insufficient history /
compute error, sector-ETF resolution via the Yahoo->GICS bridge), and the
supplemental snapshot writer (parquet split + sectors sidecar, empty no-op)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from features import metron_supplemental as ms
from features.feature_engineer import MIN_ROWS_FOR_FEATURES


def _synthetic_ohlcv(rows: int = MIN_ROWS_FOR_FEATURES + 10) -> pd.DataFrame:
    idx = pd.bdate_range("2024-01-02", periods=rows)
    rng = np.random.default_rng(7)
    close = 100.0 + np.cumsum(rng.normal(0, 1, rows))
    return pd.DataFrame(
        {
            "Open": close, "High": close + 1, "Low": close - 1,
            "Close": close, "Volume": rng.integers(1_000, 5_000, rows),
        },
        index=idx,
    )


def _macro() -> dict[str, pd.Series]:
    idx = pd.bdate_range("2024-01-02", periods=MIN_ROWS_FOR_FEATURES + 10)
    rng = np.random.default_rng(3)
    series = lambda base: pd.Series(base + np.cumsum(rng.normal(0, 0.1, len(idx))), index=idx)
    return {
        "SPY": series(450), "VIX": series(15), "TNX": series(4), "IRX": series(5),
        "GLD": series(180), "USO": series(70), "VIX3M": series(16), "HYOAS": series(3.5),
        "XLK": series(200),
    }


def _s3_universe(holdings: list[dict], watchlist: list[dict] | None = None, fundamentals: dict | None = None) -> MagicMock:
    s3 = MagicMock()

    def _get(Bucket, Key):
        def _body(obj):
            body = MagicMock()
            body.read.return_value = json.dumps(obj).encode()
            return {"Body": body}

        if Key == "metron/holdings_universe.json":
            return _body({"holdings": holdings})
        if Key == "metron/watchlist_universe.json":
            if watchlist is None:
                raise Exception("NoSuchKey")
            return _body({"holdings": watchlist})
        if Key == ms.METRON_FUNDAMENTALS_KEY:
            if fundamentals is None:
                raise Exception("NoSuchKey")
            return _body({"fundamentals": fundamentals})
        raise Exception("NoSuchKey")

    s3.get_object.side_effect = _get
    return s3


class TestUncoveredMetronTickers:
    def test_diffs_held_watchlist_against_existing(self):
        s3 = _s3_universe([{"yf_symbol": "MARUY", "currency": "USD"}, {"yf_symbol": "AAPL", "currency": "USD"}])
        uncovered = ms.uncovered_metron_tickers("bucket", s3, existing_tickers={"AAPL", "MSFT"})
        assert uncovered == ["MARUY"]

    def test_empty_when_fully_covered(self):
        s3 = _s3_universe([{"yf_symbol": "AAPL", "currency": "USD"}])
        assert ms.uncovered_metron_tickers("bucket", s3, existing_tickers={"AAPL"}) == []


class TestComputeMetronSupplementalFeatures:
    def test_short_circuits_when_nothing_uncovered(self):
        s3 = _s3_universe([{"yf_symbol": "AAPL", "currency": "USD"}])
        df, sectors = ms.compute_metron_supplemental_features(
            "bucket", s3, existing_tickers={"AAPL"}, macro=_macro(),
        )
        assert df.empty
        assert sectors == {}

    def test_scores_an_uncovered_ticker_with_resolved_sector(self):
        s3 = _s3_universe(
            [{"yf_symbol": "MARUY", "currency": "USD"}],
            fundamentals={"MARUY": {"sector": "Industrials"}},
        )
        df, sectors = ms.compute_metron_supplemental_features(
            "bucket", s3, existing_tickers=set(), macro=_macro(),
            ohlcv_fetcher=lambda ticker, **kw: _synthetic_ohlcv(),
            fundamentals_fetcher=lambda ticker: {"pe_ratio": 12.0, "roe": 0.15},
        )
        assert list(df["ticker"]) == ["MARUY"]
        assert "rsi_14" in df.columns and "pe_ratio" in df.columns
        assert sectors == {"MARUY": "Industrials"}  # Yahoo "Industrials" == GICS "Industrials"

    def test_skips_ticker_with_no_fetchable_ohlcv(self):
        s3 = _s3_universe([{"yf_symbol": "DELISTED", "currency": "USD"}])
        df, sectors = ms.compute_metron_supplemental_features(
            "bucket", s3, existing_tickers=set(), macro=_macro(),
            ohlcv_fetcher=lambda ticker, **kw: None,
        )
        assert df.empty
        assert sectors == {}

    def test_skips_ticker_on_compute_error_without_raising(self):
        s3 = _s3_universe([{"yf_symbol": "BADTICK", "currency": "USD"}])
        bad_df = pd.DataFrame({"Close": [1.0]})  # missing Volume -> compute_features may choke
        df, _ = ms.compute_metron_supplemental_features(
            "bucket", s3, existing_tickers=set(), macro=_macro(),
            ohlcv_fetcher=lambda ticker, **kw: bad_df,
            fundamentals_fetcher=lambda ticker: {},
        )
        assert df.empty  # never raises out of the batch loop

    def test_unmapped_sector_still_scores_ticker(self):
        s3 = _s3_universe(
            [{"yf_symbol": "MYSTERY", "currency": "USD"}],
            fundamentals={"MYSTERY": {"sector": "Some Unmapped Sector"}},
        )
        df, sectors = ms.compute_metron_supplemental_features(
            "bucket", s3, existing_tickers=set(), macro=_macro(),
            ohlcv_fetcher=lambda ticker, **kw: _synthetic_ohlcv(),
            fundamentals_fetcher=lambda ticker: {},
        )
        assert list(df["ticker"]) == ["MYSTERY"]
        assert sectors == {}  # unresolvable sector — no fabricated mapping


class TestYahooToGicsSectorBridge:
    @pytest.mark.parametrize("yahoo,gics,etf", [
        ("Technology", "Information Technology", "XLK"),
        ("Financial Services", "Financials", "XLF"),
        ("Healthcare", "Health Care", "XLV"),
        ("Consumer Cyclical", "Consumer Discretionary", "XLY"),
        ("Consumer Defensive", "Consumer Staples", "XLP"),
        ("Basic Materials", "Materials", "XLB"),
        ("Real Estate", "Real Estate", "XLRE"),
        ("Communication Services", "Communication Services", "XLC"),
    ])
    def test_every_yahoo_sector_resolves_to_a_tradeable_etf(self, yahoo, gics, etf):
        from collectors.constituents import GICS_TO_ETF

        assert ms.YAHOO_TO_GICS_SECTOR[yahoo] == gics
        assert GICS_TO_ETF[gics] == etf


class TestWriteMetronSupplementalSnapshot:
    def test_noop_on_empty_dataframe(self):
        s3 = MagicMock()
        result = ms.write_metron_supplemental_snapshot("2026-07-09", pd.DataFrame(), {}, "bucket", s3_client=s3)
        assert result == {"sectors": 0}
        s3.put_object.assert_not_called()

    def test_writes_parquets_and_sectors_sidecar(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"ticker": "MARUY", "rsi_14": 55.0, "pe_ratio": 12.0}])
        result = ms.write_metron_supplemental_snapshot(
            "2026-07-09", df, {"MARUY": "Industrials"}, "bucket", s3_client=s3,
        )
        put_keys = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
        assert "features/metron_supplemental/2026-07-09/technical.parquet" in put_keys
        assert "features/metron_supplemental/2026-07-09/fundamental.parquet" in put_keys
        sectors_key = "features/metron_supplemental/2026-07-09/sectors.json"
        assert sectors_key in put_keys
        sectors_call = next(c for c in s3.put_object.call_args_list if c.kwargs["Key"] == sectors_key)
        body = json.loads(sectors_call.kwargs["Body"])
        assert body["sectors"] == {"MARUY": "Industrials"}
        assert result["sectors"] == 1
