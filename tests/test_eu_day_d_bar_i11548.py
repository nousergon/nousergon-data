"""alpha-engine-config-I11548 — day D's bar for a non-US listing in D20/D21.

Measured on the 2026-09-21..24 same-day parity reports and the shadow objects:
the standalone D20 (``market_data/eod_closes/*``) and D21
(``market_data/close_history/consolidated.json``) published D-1 as the latest bar
for NOVN.SW, RMS.PA and SU.PA while v1 carried D. v1 fetched at 20:09-20:15 UTC,
the shadow at 22:45-22:52 UTC. On 2026-09-24 the shadow ran with the request end
pushed to D+2 (PR1925) and still got D-1, with no fetch-boundary clip firing: the
vendor's daily series holds nothing dated D for these listings once the exchange's
local date has rolled over, whatever ``end`` asks for.

These tests pin:
  (1) the day-D close is taken from a COMPLETE intraday session whose bars carry
      exchange-local timestamps on D, and only then;
  (2) with a vendor stub shaped like that evidence, D20 and D21 publish day D for a
      .SW and a .PA listing, and leave US listings on the daily path;
  (3) when no day-D session can be proven, bar_date stays D-1 and the run reports
      the symbol as stale, rather than relabelling the D-1 bar;
  (4) the request window is ``history_window``'s again (end = D + 1).
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from collectors import metron_market_data as mmd

D = "2026-09-24"
D_DATE = date(2026, 9, 24)
D_MINUS_1 = "2026-09-23"

EXCHANGE_TZ = {
    "NOVN.SW": "Europe/Zurich",
    "RMS.PA": "Europe/Paris",
    "SU.PA": "Europe/Paris",
    "D05.SI": "Asia/Singapore",
    "AAPL": "America/New_York",
}
EU = ["NOVN.SW", "RMS.PA", "SU.PA"]

#: The shadow's observed D20 write on 2026-09-24.
SHADOW_FETCH = pd.Timestamp("2026-09-24T22:51:43Z")


def _periods(tz: str, day: str, start: str, end: str) -> pd.DataFrame:
    """``tradingPeriods`` as yfinance formats it: one row per session, indexed by
    the session's local midnight, with tz-aware regular ``start``/``end``."""
    s = pd.Timestamp(f"{day} {start}", tz=tz)
    e = pd.Timestamp(f"{day} {end}", tz=tz)
    idx = pd.DatetimeIndex([pd.Timestamp(day, tz=tz)], name="Date")
    return pd.DataFrame({"start": [s], "end": [e]}, index=idx)


def _hourly(tz: str, day: str, first: str, last: str, closes: list[float]) -> pd.DataFrame:
    idx = pd.date_range(pd.Timestamp(f"{day} {first}", tz=tz), pd.Timestamp(f"{day} {last}", tz=tz), freq="1h")
    assert len(idx) == len(closes)
    c = np.array(closes, dtype=float)
    return pd.DataFrame({"Open": c, "High": c, "Low": c, "Close": c, "Volume": np.ones(len(c))}, index=idx)


def _eu_session(sym: str, *, day: str = D, auction: float | None = 118.96, rmt_day: str | None = None):
    """A Paris/Zurich hourly session 09:00-17:00 local, regular end 17:30, and the
    chart meta the same request returns."""
    tz = EXCHANGE_TZ[sym]
    bars = _hourly(tz, day, "09:00", "17:00", [110.0 + i for i in range(9)])
    meta = {
        "exchangeTimezoneName": tz,
        "tradingPeriods": _periods(tz, day, "09:00", "17:30"),
    }
    if auction is not None:
        meta["regularMarketPrice"] = auction
        meta["regularMarketTime"] = pd.Timestamp(f"{rmt_day or day} 17:31:12", tz=tz)
    return bars, meta


# ── (1) the pure session-close rule ──────────────────────────────────────────


def test_completed_session_takes_the_final_print_on_d():
    bars, meta = _eu_session("NOVN.SW")
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="NOVN.SW") == 118.96


def test_final_print_from_another_day_falls_back_to_the_last_bar():
    """``regularMarketTime`` on D+1 is the NEXT session's price: never D's close."""
    bars, meta = _eu_session("RMS.PA", rmt_day="2026-09-25")
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="RMS.PA") == 118.0


def test_no_final_print_uses_the_last_regular_session_bar():
    bars, meta = _eu_session("SU.PA", auction=None)
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="SU.PA") == 118.0


def test_a_session_still_open_is_not_a_close():
    bars, meta = _eu_session("RMS.PA")
    during = pd.Timestamp(f"{D} 12:00", tz="UTC")  # 14:00 in Paris
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, during, label="RMS.PA") is None


def test_no_declared_session_end_is_not_a_close():
    bars, meta = _eu_session("RMS.PA")
    meta["tradingPeriods"] = _periods("Europe/Paris", D_MINUS_1, "09:00", "17:30")
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="RMS.PA") is None
    del meta["tradingPeriods"]
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="RMS.PA") is None


def test_bars_from_another_local_date_are_not_d():
    """An exchange holiday on D: the intraday store's latest session is D-1."""
    bars, meta = _eu_session("NOVN.SW", day=D_MINUS_1)
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="NOVN.SW") is None


def test_the_session_date_is_exchange_local_not_utc():
    """SGX trades 09:00-17:00 SGT = 01:00-09:00 UTC. A bar at 00:00-01:00 UTC on D
    is D in Singapore; one at 23:00 UTC on D-1 is D in Singapore too."""
    tz = "Asia/Singapore"
    bars = _hourly(tz, D, "07:00", "16:00", [77.0 + i / 10 for i in range(10)])
    meta = {"tradingPeriods": _periods(tz, D, "09:00", "17:00")}
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="D05.SI") == 77.9


def test_a_tz_naive_index_is_refused():
    bars, meta = _eu_session("NOVN.SW")
    bars.index = bars.index.tz_localize(None)
    assert mmd._session_close_from_intraday(bars, meta, D_DATE, SHADOW_FETCH, label="NOVN.SW") is None


# ── (2)/(3) the collectors, with a vendor stub shaped like the evidence ──────


def _daily_download(calls: list, *, eu_through: str = D_MINUS_1):
    """``yf.download`` stand-in: the daily series holds D for US/SGX listings and
    stops at ``eu_through`` for the EU ones, as the shadow saw at 22:5x UTC."""

    def _download(tickers, start, end, **kw):
        calls.append({"tickers": tickers, "start": start, "end": end, **kw})
        syms = [tickers] if isinstance(tickers, str) else list(tickers)
        frames = {}
        for sym in syms:
            last = eu_through if sym in EU else D
            idx = pd.bdate_range(start, last)
            closes = np.array([100.0 + i for i in range(len(idx))])
            frames[sym] = pd.DataFrame(
                {"Open": closes, "High": closes, "Low": closes, "Close": closes,
                 "Adj Close": closes, "Volume": np.ones(len(idx))},
                index=idx,
            )
        if len(syms) == 1:
            return frames[syms[0]]
        return pd.concat(frames, axis=1)

    return _download


class _Ticker:
    """``yf.Ticker`` stand-in for the intraday request."""

    requested: list = []
    serve_intraday = True

    def __init__(self, sym):
        self.sym = sym
        self.history_metadata: dict = {}

    def history(self, *, start, end, interval, prepost, auto_adjust):
        _Ticker.requested.append({"sym": self.sym, "start": start, "end": end,
                                  "interval": interval, "prepost": prepost})
        if not _Ticker.serve_intraday or self.sym not in EU:
            return pd.DataFrame()
        bars, meta = _eu_session(self.sym, auction={"NOVN.SW": 118.96, "RMS.PA": 1354.5, "SU.PA": 289.8}[self.sym])
        self.history_metadata = meta
        return bars


@pytest.fixture
def vendor(monkeypatch):
    import yfinance

    calls: list = []
    _Ticker.requested = []
    _Ticker.serve_intraday = True
    monkeypatch.setattr(mmd.time, "sleep", lambda *_: None)
    monkeypatch.setattr(yfinance, "download", _daily_download(calls))
    monkeypatch.setattr(yfinance, "Ticker", _Ticker)
    return calls


def test_d20_publishes_day_d_for_sw_and_pa_from_their_intraday_session(vendor):
    closes = mmd._yfinance_closes(EU + ["AAPL", "D05.SI"], trading_day=D)
    assert {s: closes[s][1] for s in closes} == dict.fromkeys(EU + ["AAPL", "D05.SI"], D)
    # The values are v1's 2026-09-24 closes for these three listings.
    assert {s: closes[s][0] for s in EU} == {"NOVN.SW": 118.96, "RMS.PA": 1354.5, "SU.PA": 289.8}
    # Only the listings whose daily series ended before D were asked intraday,
    # for their own local day D.
    assert sorted(r["sym"] for r in _Ticker.requested) == sorted(EU)
    assert all(
        (r["start"], r["end"], r["interval"], r["prepost"]) == (D, "2026-09-25", "1h", False)
        for r in _Ticker.requested
    )


def test_d21_series_gains_day_d_for_sw_and_pa(vendor):
    series = mmd._yfinance_close_history_dividend_adjusted(EU + ["AAPL"], "10y", trading_day=D)
    assert {s: series[s][-1][0] for s in series} == dict.fromkeys(EU + ["AAPL"], D)
    assert all(series[s][-2][0] == D_MINUS_1 for s in EU)
    assert series["NOVN.SW"][-1][1] == 118.96


def test_fx_history_never_takes_the_intraday_path(vendor):
    mmd._yfinance_fx_history(["CHF"], "10y", trading_day=D)
    assert _Ticker.requested == []


def test_no_intraday_session_keeps_the_truthful_d_minus_1_bar(vendor):
    _Ticker.serve_intraday = False
    closes = mmd._yfinance_closes(EU + ["AAPL"], trading_day=D)
    assert {s: closes[s][1] for s in EU} == dict.fromkeys(EU, D_MINUS_1)
    assert closes["AAPL"][1] == D


def test_the_request_window_ends_at_d_plus_1_again(vendor):
    """PR1925's D+2 end did not change the vendor's answer (2026-09-24 shadow) and
    downgraded the I10893 out-of-contract WARNING to INFO: it is reverted."""
    mmd._yfinance_closes(["AAPL"], trading_day=D)
    mmd._yfinance_close_history_dividend_adjusted(["AAPL"], "10y", trading_day=D)
    assert [c["end"] for c in vendor] == ["2026-09-25", "2026-09-25"]
    assert not hasattr(__import__("dates"), "vendor_request_window")


# ── (3) staleness reaches the run's result, not only a diff ──────────────────


class _S3:
    def __init__(self):
        self.puts: dict = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = Body


def test_d20_reports_a_symbol_that_stays_behind_d(caplog):
    s3 = _S3()
    priced = {"AAPL": (250.0, D), "NOVN.SW": (117.6, D_MINUS_1)}
    import unittest.mock as um

    with um.patch.object(mmd, "load_metron_universe", return_value=(
        [{"yf_symbol": "AAPL", "currency": "USD"}, {"yf_symbol": "NOVN.SW", "currency": "CHF"}], ["CHF"],
    )), caplog.at_level("WARNING"):
        out = mmd.collect(
            run_date=D, dry_run=True, s3_client=s3,
            close_source=lambda syms: priced, fx_source=lambda ccys: {"CHF": 1.2},
        )
    assert out["stale_bars"] == {"NOVN.SW": D_MINUS_1}
    assert "NOVN.SW@2026-09-23" in caplog.text


def test_log_stale_bars_is_empty_when_every_bar_is_d():
    assert mmd._log_stale_bars("closes", {"AAPL": D, "NOVN.SW": D}, D) == {}
