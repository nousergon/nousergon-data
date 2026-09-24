"""alpha-engine-config-I10893 — history fetches are bounded by the run's trading day.

Measured 2026-09-15: a ``weekly_collector --daily --date 2026-09-14`` shadow run
executed on 2026-09-15 (17:02-20:22Z) wrote a pre-close ``2026-09-15`` bar into
``reference/price_cache/*.parquet`` (A: Close 150.279999 vs settled 150.179993) and
``market_data/fx_history/*.json`` (CHF 1.221359 vs settled 1.221195), and the price
cache's 10y window started at ``2016-09-15`` — anchored on wall-clock now. Cause:
``period=``-style yfinance calls with no ``start``/``end``.

These tests pin: (1) every fetch asks for ``[D − window, D]`` via explicit
``start``/``end`` and never ``period=``; (2) a ``--date D`` run on D+1 publishes no
D+1 bar even when the vendor over-answers; (3) the write-site guard RAISES when a
frame bypasses the bound.
"""

from __future__ import annotations

import io
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import collectors.prices as prices
from collectors import fred_history
from collectors import metron_market_data as mmd
from dates import FutureBarError, assert_no_bar_after, clip_to_trading_day, history_window

D = "2026-09-14"
D_PLUS_1 = "2026-09-15"
REPO_ROOT = Path(__file__).resolve().parent.parent


def _ohlcv(start: str, end: str) -> pd.DataFrame:
    idx = pd.bdate_range(start, end)
    n = len(idx)
    return pd.DataFrame(
        {"Open": np.ones(n), "High": np.ones(n), "Low": np.ones(n),
         "Close": np.linspace(10.0, 20.0, n), "Volume": np.ones(n)},
        index=idx,
    )


class _NoSuchKey(Exception):
    pass


class _FakeS3:
    exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})()

    def __init__(self):
        self.uploads: dict[str, bytes] = {}

    def get_object(self, Bucket, Key):
        raise _NoSuchKey(Key)

    def upload_file(self, path, bucket, key):
        with open(path, "rb") as f:
            self.uploads[key] = f.read()


# ── dates.py primitives ──────────────────────────────────────────────────────


def test_history_window_is_a_pure_function_of_the_trading_day():
    assert history_window(D, "10y") == (date(2016, 9, 14), date(2026, 9, 15))
    assert history_window(date(2026, 9, 14), "10d") == (date(2026, 9, 4), date(2026, 9, 15))
    assert history_window(D, "6mo") == (date(2026, 3, 14), date(2026, 9, 15))


def test_history_window_refuses_an_unparseable_period():
    with pytest.raises(ValueError, match="unparseable period"):
        history_window(D, "max")


def test_guard_raises_on_a_bar_after_the_trading_day_and_accepts_d():
    assert_no_bar_after(pd.bdate_range("2026-09-01", D), D, artifact="x")
    assert_no_bar_after([], D, artifact="x")
    with pytest.raises(FutureBarError, match="2026-09-15"):
        assert_no_bar_after(pd.bdate_range("2026-09-01", D_PLUS_1), D, artifact="x")
    with pytest.raises(FutureBarError):
        assert_no_bar_after(["2026-09-11", D_PLUS_1], D, artifact="x")


def test_clip_drops_only_rows_after_d():
    frame = _ohlcv("2026-09-10", D_PLUS_1)
    clipped = clip_to_trading_day(frame, D, label="t")
    assert clipped.index.max() == pd.Timestamp(D)
    assert len(clipped) == len(frame) - 1


# ── reference/price_cache refresh (collectors/prices.py::_refresh_stale) ────


def _recording_download(frame: pd.DataFrame, calls: list[dict]):
    def _download(*_a, **kw):
        calls.append(kw)
        return frame.copy()
    return _download


def test_price_cache_refresh_requests_d_bounded_window_and_publishes_no_d_plus_1(monkeypatch):
    """A --date D run on D+1: the vendor stub answers through D+1 (the partial
    session); the published parquet ends at D and the request never says period=."""
    calls: list[dict] = []
    monkeypatch.setattr(prices.yf, "download",
                        _recording_download(_ohlcv("2016-09-14", D_PLUS_1), calls))
    s3 = _FakeS3()

    refreshed, failed, written = prices._refresh_stale(
        s3, "b", "predictor/price_cache/", ["A"], "10y", 50, trading_day=D,
    )

    assert (refreshed, failed) == (1, [])
    assert len(calls) == 1
    assert "period" not in calls[0]
    assert calls[0]["start"] == "2016-09-14"
    assert calls[0]["end"] == "2026-09-15"  # exclusive
    written = pd.read_parquet(io.BytesIO(s3.uploads["reference/price_cache/A.parquet"]))
    assert written.index.max() == pd.Timestamp(D)
    assert written.index.min() == pd.Timestamp("2016-09-14")


def test_price_cache_refresh_same_day_run_content_is_unchanged(monkeypatch):
    """A run for D executed on D after the close: the vendor answer already ends
    at D, so the bound removes nothing — production content is identical."""
    frame = _ohlcv("2016-09-14", D)
    monkeypatch.setattr(prices.yf, "download", _recording_download(frame, []))
    s3 = _FakeS3()
    prices._refresh_stale(s3, "b", "predictor/price_cache/", ["A"], "10y", 50, trading_day=D)
    written = pd.read_parquet(io.BytesIO(s3.uploads["reference/price_cache/A.parquet"]))
    pd.testing.assert_frame_equal(written, frame, check_freq=False)


def test_price_cache_guard_raises_when_a_frame_bypasses_the_bound(monkeypatch):
    """Disable the fetch-boundary clip: the write-site guard must raise (not
    record a per-ticker failure, not trim) and nothing is uploaded."""
    monkeypatch.setattr(prices.yf, "download",
                        _recording_download(_ohlcv("2016-09-14", D_PLUS_1), []))
    monkeypatch.setattr(prices, "clip_to_trading_day", lambda frame, *_a, **_k: frame)
    s3 = _FakeS3()
    with pytest.raises(FutureBarError, match="A.parquet"):
        prices._refresh_stale(s3, "b", "predictor/price_cache/", ["A"], "10y", 50, trading_day=D)
    assert s3.uploads == {}


def test_price_cache_caret_fred_fetch_is_bounded_by_the_trading_day(monkeypatch):
    seen: dict = {}

    def _fake_fetch(series_id, period_years=10, api_key=None, end_date=None):
        seen["end_date"] = end_date
        return pd.DataFrame({"value": [1.0]}, index=pd.DatetimeIndex([D]))

    monkeypatch.setattr(fred_history, "fetch_fred_history", _fake_fetch)
    prices._fred_ohlcv_for_caret_symbol("VIX3M", "10y", trading_day=D)
    assert seen["end_date"] == D


def test_collect_threads_reference_date_as_the_trading_day(monkeypatch):
    monkeypatch.setattr(prices, "boto3", MagicMock())
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: ["A"])
    captured: dict = {}

    def _fake_refresh(s3, bucket, s3_prefix, stale, fetch_period, batch_size, *, trading_day, short_fetch_retries=None, failure_reasons=None):
        captured["trading_day"] = trading_day
        return 0, list(stale), []

    monkeypatch.setattr(prices, "_refresh_stale", _fake_refresh)
    prices.collect(bucket="b", tickers=["A"], reference_date=D)
    assert captured["trading_day"] == D


# ── FRED price_cache backfill (collectors/fred_history.py) ──────────────────


def test_fred_backfill_passes_end_date_and_raises_on_a_future_bar():
    seen: list = []

    def _fetch(series_id, period_years=10, api_key=None, end_date=None):
        seen.append(end_date)
        return pd.DataFrame({"value": [1.0, 2.0]}, index=pd.DatetimeIndex(["2026-09-11", D_PLUS_1]))

    with patch("collectors.fred_history.fetch_fred_history", side_effect=_fetch):
        with pytest.raises(FutureBarError, match="TWO.parquet"):
            fred_history.backfill_to_s3(bucket="b", tickers=["TWO"], dry_run=True, trading_day=D)
    assert seen == [D]


def test_fetch_fred_history_requests_observation_end_equal_to_d():
    resp = MagicMock()
    resp.json.return_value = {"observations": [{"date": D, "value": "1.0"}]}
    resp.raise_for_status.return_value = None
    with patch("collectors.fred_history.requests.get", return_value=resp) as get:
        fred_history.fetch_fred_history("DGS2", api_key="k", end_date=D)
    params = get.call_args.kwargs["params"]
    assert params["observation_end"] == D


# ── metron close_history / fx_history / closes (collectors/metron_market_data.py) ─


def _yf_frame(end: str) -> pd.DataFrame:
    idx = pd.bdate_range("2026-09-01", end)
    return pd.DataFrame({"Close": np.linspace(1.0, 2.0, len(idx)), "Open": 1.0}, index=idx)


def test_yf_history_fx_is_bounded_and_ends_at_d(monkeypatch):
    import yfinance

    calls: list[dict] = []
    monkeypatch.setattr(yfinance, "download", _recording_download(_yf_frame(D_PLUS_1), calls))
    monkeypatch.setattr(mmd.time, "sleep", lambda *_: None)

    out = mmd._yfinance_fx_history(["CHF"], "10y", trading_day=D)

    assert "period" not in calls[0]
    # The REQUEST reaches D + 2 (alpha-engine-config-I11548); the published series
    # still ends at D because the response is clipped.
    assert (calls[0]["start"], calls[0]["end"]) == ("2016-09-14", "2026-09-16")
    assert out["CHF"][-1][0] == D


def test_yf_history_dividend_adjusted_gap_fill_is_bounded(monkeypatch):
    import yfinance

    calls: list[dict] = []
    monkeypatch.setattr(yfinance, "download", _recording_download(_yf_frame(D_PLUS_1), calls))
    out = mmd._yfinance_close_history_dividend_adjusted(["A"], "10y", trading_day=D)
    assert calls[0]["auto_adjust"] is True and "period" not in calls[0]
    assert out["A"][-1][0] == D


def test_latest_closes_and_fx_are_bounded(monkeypatch):
    import yfinance

    calls: list[dict] = []
    monkeypatch.setattr(yfinance, "download", _recording_download(_yf_frame(D_PLUS_1), calls))
    closes = mmd._yfinance_closes(["A"], trading_day=D)
    fx = mmd._yfinance_fx(["CHF"], trading_day=D)
    # Request reaches D + 2 (alpha-engine-config-I11548); the answer is still D's bar.
    assert all("period" not in c and c["end"] == "2026-09-16" for c in calls)
    assert closes["A"][1] == D
    assert fx["CHF"] == round(float(_yf_frame(D_PLUS_1)["Close"].loc[D]), 6)


def _history_s3() -> MagicMock:
    s3 = MagicMock()
    universe = {"holdings": [{"yf_symbol": "A", "currency": "CHF"}], "currencies": ["CHF"]}

    def _get(Bucket, Key):
        if Key == mmd.HOLDINGS_UNIVERSE_KEY:
            body = MagicMock()
            body.read.return_value = json.dumps(universe).encode()
            return {"Body": body}
        raise Exception("NoSuchKey")

    s3.get_object.side_effect = _get
    return s3


def test_collect_history_raises_before_writing_when_a_series_passes_d(monkeypatch):
    monkeypatch.setattr(mmd, "_load_sp1500_symbols", lambda bucket: set())
    s3 = _history_s3()
    with pytest.raises(FutureBarError, match="fx_history/CHF.json"):
        mmd.collect_history(
            bucket="b", run_date=D, s3_client=s3,
            close_history_source=lambda syms: {s: [("2026-09-11", 1.0), (D, 1.1)] for s in syms},
            fx_history_source=lambda ccys: {"CHF": [(D, 1.2211), (D_PLUS_1, 1.221359)]},
        )
    s3.put_object.assert_not_called()


def test_collect_history_default_sources_are_bounded_by_run_date(monkeypatch):
    monkeypatch.setattr(mmd, "_load_sp1500_symbols", lambda bucket: set())
    seen: dict = {}

    def _fake_cache_source(s3_client, bucket, period, *, reference_day=None):
        seen["reference_day"] = reference_day
        return lambda syms: {s: [(D, 1.0)] for s in syms}

    def _fake_fx(currencies, period="10y", *, trading_day):
        seen["fx_trading_day"] = trading_day
        return {c: [(D, 1.2)] for c in currencies}

    monkeypatch.setattr(mmd, "_price_cache_close_history", _fake_cache_source)
    monkeypatch.setattr(mmd, "_yfinance_fx_history", _fake_fx)
    s3 = _history_s3()
    result = mmd.collect_history(bucket="b", run_date=D, s3_client=s3)
    assert result["status"] == "ok"
    assert seen == {"reference_day": date(2026, 9, 14), "fx_trading_day": date(2026, 9, 14)}


def test_price_cache_close_series_excludes_bars_after_the_reference_day(monkeypatch):
    frame = _ohlcv("2016-09-14", D_PLUS_1)
    monkeypatch.setattr("store.parquet_loader.load_parquet_from_s3", lambda *a, **k: frame)
    series = mmd._price_cache_close_series(MagicMock(), "b", "A", "10y", reference_day=date(2026, 9, 14))
    assert series[-1][0] == D


def test_collect_latest_closes_raises_on_a_bar_after_run_date(monkeypatch):
    monkeypatch.setattr(mmd, "load_metron_universe",
                        lambda bucket, s3: ([{"yf_symbol": "A", "currency": "USD"}], []))
    s3 = MagicMock()
    with pytest.raises(FutureBarError):
        mmd.collect(bucket="b", run_date=D, s3_client=s3,
                    close_source=lambda syms: {"A": (150.28, D_PLUS_1)},
                    fx_source=lambda ccys: {})
    s3.put_object.assert_not_called()


# ── weekly_collector wiring ─────────────────────────────────────────────────


def test_weekly_collector_threads_run_date_into_every_bounded_writer():
    src = (REPO_ROOT / "weekly_collector.py").read_text()
    assert "metron_market_data.collect_history(bucket=bucket, run_date=run_date" in src
    assert 'tickers=["TWO", "HYOAS", "BAA10Y"], trading_day=run_date' in src
    assert "_assert_no_bar_after(combined_pcache.index, target_date" in src
