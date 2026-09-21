"""alpha-engine-config-I9286 — route the caret index tickers' 10y history
refresh through FRED as well as yfinance, taking whichever answers longer.

Measured 2026-08-29: ``yf.download('^VIX3M', period='10y')`` returned 2484
rows from a laptop and 1 row from EC2 host ``i-00b4403ae4eb894cc`` (the box
that runs the weekly collector), minutes apart. FRED served 2518 rows from
the same EC2 host in the same run. This is the SOURCE defect behind
``alpha-engine-config-I9256``'s truncating write and part of
``alpha-engine-config-I9324``'s intermittent macro outage.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest

import collectors.prices as _prices


def _ohlcv(n: int, start: str = "2016-08-19") -> pd.DataFrame:
    idx = pd.bdate_range(start, periods=n)
    return pd.DataFrame(
        {
            "Open": np.ones(n), "High": np.ones(n), "Low": np.ones(n),
            "Close": np.linspace(10.0, 20.0, n), "Volume": np.zeros(n),
        },
        index=idx,
    )


class _NoSuchKey(Exception):
    pass


class _FakeS3:
    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects = dict(objects or {})
        self.uploads: list[str] = []
        self.exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})()

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, path, bucket, key):
        with open(path, "rb") as f:
            self.uploads.append((key, f.read()))


def _patch_yf_download(monkeypatch, frame: pd.DataFrame):
    monkeypatch.setattr(_prices.yf, "download", lambda *a, **k: frame, raising=True)


def test_fred_wins_over_a_short_yfinance_answer_for_vix3m(monkeypatch):
    """The exact measured shape: yfinance answers 1 row, FRED answers long."""
    s3 = _FakeS3()
    _patch_yf_download(monkeypatch, _ohlcv(1))
    monkeypatch.setattr(
        _prices, "_fred_ohlcv_for_caret_symbol",
        lambda ticker, period, **_kw: _ohlcv(2518),
    )

    refreshed, failed, written, insufficient = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["VIX3M"], "10y", 50, trading_day="2026-09-14",
    )

    assert refreshed == 1
    assert failed == []
    assert len(s3.uploads) == 1
    key, body = s3.uploads[0]
    assert key == "reference/price_cache/VIX3M.parquet"
    written = pd.read_parquet(io.BytesIO(body))
    assert len(written) == 2518, "the FRED (longer) answer must be what gets written"


def test_yfinance_wins_when_it_answers_longer(monkeypatch):
    s3 = _FakeS3()
    _patch_yf_download(monkeypatch, _ohlcv(2500))
    monkeypatch.setattr(
        _prices, "_fred_ohlcv_for_caret_symbol",
        lambda ticker, period, **_kw: _ohlcv(2000),
    )

    refreshed, failed, written, insufficient = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["VIX3M"], "10y", 50, trading_day="2026-09-14",
    )
    assert refreshed == 1
    key, body = s3.uploads[0]
    written = pd.read_parquet(io.BytesIO(body))
    assert len(written) == 2500, "yfinance's longer answer must win"


def test_fred_outage_degrades_to_yfinance_not_to_nothing(monkeypatch):
    """A FRED failure must never turn into a total miss — deliverable 2 of
    alpha-engine-config-I9286 (union-no-shrink / longest-of, never a hard
    cutover). ``_fred_ohlcv_for_caret_symbol`` itself never raises (it catches
    every fetch failure internally and returns None) — this pins that
    contract at the ``_refresh_stale`` call site."""
    s3 = _FakeS3()
    _patch_yf_download(monkeypatch, _ohlcv(2484))
    monkeypatch.setattr(
        _prices, "_fred_ohlcv_for_caret_symbol", lambda ticker, period, **_kw: None,
    )

    refreshed, failed, written, insufficient = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["VIX3M"], "10y", 50, trading_day="2026-09-14",
    )
    assert refreshed == 1
    assert failed == []
    key, body = s3.uploads[0]
    written = pd.read_parquet(io.BytesIO(body))
    assert len(written) == 2484, "yfinance answer must still be used when FRED errors"


def test_non_caret_tickers_never_consult_fred(monkeypatch):
    """A plain equity ticker must not trigger a FRED lookup at all."""
    s3 = _FakeS3()
    _patch_yf_download(monkeypatch, _ohlcv(2500))
    called = []
    monkeypatch.setattr(
        _prices, "_fred_ohlcv_for_caret_symbol",
        lambda ticker, period, **_kw: called.append(ticker) or None,
    )

    refreshed, failed, written, insufficient = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["AAPL"], "10y", 50, trading_day="2026-09-14",
    )
    assert refreshed == 1
    assert called == [], "a non-caret ticker must never reach the FRED path"


def test_longest_of_raises_when_every_candidate_is_empty():
    with pytest.raises(RuntimeError, match="no usable candidate"):
        _prices._longest_of([("yfinance", pd.DataFrame()), ("fred", None)])


def test_longest_of_picks_the_longer_named_candidate():
    short = _ohlcv(5)
    long = _ohlcv(50)
    name, chosen = _prices._longest_of([("yfinance", short), ("fred", long)])
    assert name == "fred"
    assert len(chosen) == 50


def test_fred_ohlcv_helper_never_raises_on_fetch_failure(monkeypatch):
    """Unit-level pin of the contract the outage test above relies on:
    ``_fred_ohlcv_for_caret_symbol`` catches a FRED fetch failure internally."""
    import collectors.fred_history as _fh

    def _raise(series_id, period_years):
        raise RuntimeError("FRED_API_KEY not set")

    monkeypatch.setattr(_fh, "fetch_fred_history", _raise)
    assert _prices._fred_ohlcv_for_caret_symbol("VIX3M", "10y") is None


def test_fred_ohlcv_helper_returns_none_for_a_non_fred_symbol():
    assert _prices._fred_ohlcv_for_caret_symbol("NOTAREALSYM", "10y") is None
