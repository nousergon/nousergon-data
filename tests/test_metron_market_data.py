"""Metron market-data producer — EOD closes + FX for Metron's held universe.

`alpha-engine-data` is the system's sole market-data source; Metron consumes these
artifacts. Covers: reading Metron's published universe, building the versioned
closes + FX artifacts, writing dated + ``latest`` keys, omitting unpriceable symbols,
the dry-run no-write path, and the fail-soft empty-universe skip.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from collectors import metron_market_data as mmd


def _universe_s3(universe: dict | None) -> MagicMock:
    """A MagicMock S3 whose get_object returns ``universe`` JSON (or raises if None)."""
    s3 = MagicMock()
    if universe is None:
        s3.get_object.side_effect = Exception("NoSuchKey")
    else:
        body = MagicMock()
        body.read.return_value = json.dumps(universe).encode()
        s3.get_object.return_value = {"Body": body}
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


class TestReference:
    def test_writes_sectors_and_earnings_keyed_by_yf_symbol(self):
        s3 = _universe_s3(_UNIVERSE)
        result = mmd.collect_reference(
            bucket="b", run_date="2026-06-11", s3_client=s3,
            sector_source=lambda syms: {"AAPL": "Technology", "1299.HK": "Financial Services"},
            benchmark_source=lambda: {"Technology": 0.30, "Financial Services": 0.13},
            earnings_source=lambda syms: {"AAPL": "2026-07-30"},
        )
        assert result["status"] == "ok" and result["sectors"] == 2 and result["earnings"] == 1
        puts = _puts(s3)
        assert set(puts) == {"market_data/sectors/latest.json", "market_data/earnings/latest.json"}
        sec = puts["market_data/sectors/latest.json"]
        assert sec["schema_version"] == mmd.SECTORS_SCHEMA_VERSION
        assert sec["sectors"] == {"1299.HK": "Financial Services", "AAPL": "Technology"}
        assert sec["spy_sector_weights"]["Technology"] == 0.30
        assert puts["market_data/earnings/latest.json"]["earnings"] == {"AAPL": "2026-07-30"}

    def test_reference_dry_run_and_empty_universe(self):
        s3 = _universe_s3(_UNIVERSE)
        r = mmd.collect_reference(bucket="b", run_date="2026-06-11", dry_run=True, s3_client=s3,
                                  sector_source=lambda s: {"AAPL": "Technology"}, benchmark_source=lambda: {}, earnings_source=lambda s: {})
        assert r["status"] == "ok_dry_run"
        s3.put_object.assert_not_called()
        s3b = _universe_s3({"holdings": [], "currencies": []})
        assert mmd.collect_reference(bucket="b", s3_client=s3b, sector_source=lambda s: {}, benchmark_source=lambda: {}, earnings_source=lambda s: {})["status"] == "skipped"


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


class TestIntraday:
    _QUOTES = {
        "AAPL": {"last": 202.1, "open": 200.5, "prev_close": 201.5,
                 "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
        "1299.HK": {"last": 64.8, "open": 64.1, "prev_close": 64.2,
                    "session_date": "2026-06-12", "prev_session_date": "2026-06-11"},
    }

    def test_writes_quotes_with_currency_inside_market_window(self):
        from datetime import datetime, timezone

        s3 = _universe_s3(_UNIVERSE)
        rth = datetime(2026, 6, 12, 15, 0, tzinfo=timezone.utc)  # Friday 15:00 UTC
        result = mmd.collect_intraday(
            bucket="b", s3_client=s3, intraday_source=lambda s: dict(self._QUOTES), now=rth
        )
        assert result["status"] == "ok" and result["quotes"] == 2
        puts = _puts(s3)
        assert set(puts) == {"market_data/intraday/latest.json"}
        art = puts["market_data/intraday/latest.json"]
        assert art["schema_version"] == mmd.INTRADAY_SCHEMA_VERSION
        assert art["source"] == "yfinance_delayed"
        assert art["as_of_utc"] == "2026-06-12T15:00:00Z"
        # Currency joined from the universe (the consumer FX-converts the P&L legs).
        assert art["quotes"]["1299.HK"]["currency"] == "HKD"
        assert art["quotes"]["AAPL"]["prev_close"] == 201.5

    def test_skips_outside_market_window_without_fetching(self):
        from datetime import datetime, timezone

        s3 = _universe_s3(_UNIVERSE)
        calls = []
        source = lambda s: calls.append(1) or {}
        # Friday 22:00 UTC — after the (DST-widened) close window.
        late = datetime(2026, 6, 12, 22, 0, tzinfo=timezone.utc)
        result = mmd.collect_intraday(bucket="b", s3_client=s3, intraday_source=source, now=late)
        assert result["status"] == "skipped" and "market window" in result["reason"]
        assert not calls and not s3.put_object.called
        # Saturday inside the hour window — still skipped (weekend).
        sat = datetime(2026, 6, 13, 15, 0, tzinfo=timezone.utc)
        assert mmd.collect_intraday(bucket="b", s3_client=s3, intraday_source=source, now=sat)["status"] == "skipped"
        # force=True bypasses the gate (manual backfill/debug runs).
        forced = mmd.collect_intraday(
            bucket="b", s3_client=s3, intraday_source=lambda s: dict(self._QUOTES), now=late, force=True
        )
        assert forced["status"] == "ok"

    def test_market_window_boundaries(self):
        from datetime import datetime, timezone

        mk = lambda h, m: datetime(2026, 6, 12, h, m, tzinfo=timezone.utc)  # a Friday
        assert not mmd.in_us_market_window(mk(13, 24))   # pre-open margin edge
        assert mmd.in_us_market_window(mk(13, 25))       # EDT open margin
        assert mmd.in_us_market_window(mk(20, 0))        # EDT close
        assert mmd.in_us_market_window(mk(21, 10))       # EST close margin
        assert not mmd.in_us_market_window(mk(21, 11))

    def test_intraday_dry_run_and_empty_universe(self):
        from datetime import datetime, timezone

        rth = datetime(2026, 6, 12, 15, 0, tzinfo=timezone.utc)
        s3 = _universe_s3(_UNIVERSE)
        result = mmd.collect_intraday(
            bucket="b", s3_client=s3, dry_run=True, intraday_source=lambda s: dict(self._QUOTES), now=rth
        )
        assert result["status"] == "ok_dry_run"
        assert not s3.put_object.called
        empty = _universe_s3({"holdings": [], "currencies": []})
        assert mmd.collect_intraday(bucket="b", s3_client=empty, now=rth)["status"] == "skipped"
