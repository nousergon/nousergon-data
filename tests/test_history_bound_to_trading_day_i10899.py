"""alpha-engine-config-I10899 — residual `period=` history fetches bounded to
the run's trading day.

PR1760 (I10893) bounded the PRIMARY history fetches (`collectors/prices.py`,
`collectors/fred_history.py`) to `[D - window, D]`. This file pins the three
residual sites named in I10899 to the same contract: every fetch asks for an
explicit `start`/`end` (never `period=`), a vendor over-answer is clipped at
the fetch boundary, and a bar that still reaches the publish boundary after D
raises `dates.FutureBarError` rather than being silently published or folded
into a per-ticker miss.

  * `collectors/macro.py::_fetch_market_prices` — commodity/index closes +
    30d returns feeding `market_data/weekly/{D}/macro.json`.
  * `collectors/alternative.py::_fetch_options` — options price-fallback and
    IV-rank realized-vol history feeding the per-ticker `alternative/*.json`
    artifact.
  * `features/metron_supplemental.py::_fetch_ticker_ohlcv` — OHLCV for
    Metron-held tickers outside the S&P500+400 universe, feeding
    `features/metron_supplemental/{D}/*.parquet`.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from dates import FutureBarError

D = "2026-09-14"
D_PLUS_1 = "2026-09-15"


def _ohlcv(start: str, end: str) -> pd.DataFrame:
    idx = pd.bdate_range(start, end)
    n = len(idx)
    return pd.DataFrame(
        {"Open": np.ones(n), "High": np.ones(n), "Low": np.ones(n),
         "Close": np.linspace(10.0, 20.0, n), "Volume": np.ones(n)},
        index=idx,
    )


def _multi_ticker_ohlcv(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """A ``group_by="ticker"`` yfinance-shaped frame: MultiIndex columns
    ``(ticker, field)``, matching what ``macro._fetch_market_prices`` reads
    via ``df[ticker]["Close"]``."""
    frames = {t: _ohlcv(start, end) for t in tickers}
    return pd.concat(frames, axis=1)


# ── collectors/macro.py::_fetch_market_prices ───────────────────────────────


_MACRO_TICKERS = ["CL=F", "GC=F", "HG=F", "SPY", "QQQ", "IWM"]


def test_macro_market_prices_requests_d_bounded_window_never_period(monkeypatch):
    from collectors import macro

    calls: list[dict] = []

    def _download(*_a, **kw):
        calls.append(kw)
        return _multi_ticker_ohlcv(_MACRO_TICKERS, "2026-08-01", D_PLUS_1)

    monkeypatch.setattr(macro.yf, "download", _download)

    result = macro._fetch_market_prices(D)

    assert len(calls) == 1
    assert "period" not in calls[0]
    assert calls[0]["start"] == "2026-08-10"  # D - 35d
    assert calls[0]["end"] == "2026-09-15"  # D + 1, exclusive
    # The frame is clipped at the fetch boundary before any field is derived —
    # sp500_close must reflect D's bar, not D+1's.
    full = _ohlcv("2026-08-01", D_PLUS_1)
    clipped_last_close = round(float(full[full.index <= pd.Timestamp(D)]["Close"].iloc[-1]), 2)
    assert result["sp500_close"] == pytest.approx(clipped_last_close)


def test_macro_market_prices_guard_raises_when_clip_bypassed(monkeypatch):
    from collectors import macro

    monkeypatch.setattr(
        macro.yf, "download",
        lambda *_a, **_kw: _multi_ticker_ohlcv(_MACRO_TICKERS, "2026-08-01", D_PLUS_1),
    )
    # Patch clip_to_trading_day at the `dates` module level (macro imports it
    # locally inside the function) so it becomes a no-op identity function —
    # the vendor over-answer then reaches assert_no_bar_after unclipped.
    import dates

    monkeypatch.setattr(dates, "clip_to_trading_day", lambda frame, *_a, **_k: frame)

    with pytest.raises(FutureBarError, match=D_PLUS_1):
        macro._fetch_market_prices(D)


def test_macro_market_prices_same_day_run_is_unaffected(monkeypatch):
    """A run for D executed on D (vendor answer already ends at D) — the
    bound removes nothing and every field is computed as before."""
    from collectors import macro

    frame = _multi_ticker_ohlcv(_MACRO_TICKERS, "2026-08-01", D)
    monkeypatch.setattr(macro.yf, "download", lambda *_a, **_kw: frame.copy())

    result = macro._fetch_market_prices(D)

    assert result["sp500_close"] == pytest.approx(20.0)


# ── collectors/alternative.py::_fetch_options ───────────────────────────────


class _FakeChain:
    def __init__(self):
        self.calls = pd.DataFrame(
            {"openInterest": [100], "strike": [150.0], "impliedVolatility": [0.30]}
        )
        self.puts = pd.DataFrame(
            {"openInterest": [80], "strike": [150.0], "impliedVolatility": [0.28]}
        )


class _FakeTicker:
    """Records every `.history()` call so the test can assert start/end are
    threaded and `period=` is never passed."""

    history_calls: list[dict] = []
    history_frame: pd.DataFrame | None = None

    def __init__(self, _ticker):
        self.options = ["2026-10-15"]

    @property
    def info(self):
        return {}  # no regularMarketPrice/previousClose -> forces history() fallback

    def option_chain(self, _exp):
        return _FakeChain()

    def history(self, **kw):
        type(self).history_calls.append(kw)
        return type(self).history_frame.copy()


def _install_fake_yfinance(monkeypatch, history_frame: pd.DataFrame):
    _FakeTicker.history_calls = []
    _FakeTicker.history_frame = history_frame
    fake_module = SimpleNamespace(Ticker=_FakeTicker)
    monkeypatch.setitem(sys.modules, "yfinance", fake_module)


def test_options_price_fallback_and_iv_rank_request_d_bounded_windows(monkeypatch):
    from collectors import alternative

    _install_fake_yfinance(monkeypatch, _ohlcv("2024-09-01", D))

    result = alternative._fetch_options("AAPL", D)

    assert len(_FakeTicker.history_calls) == 2
    for kw in _FakeTicker.history_calls:
        assert "period" not in kw
        assert kw["end"] == D_PLUS_1  # D + 1, exclusive
    # price fallback: [D - 5d, D]; iv_rank: [D - 1y, D]
    assert _FakeTicker.history_calls[0]["start"] == "2026-09-09"
    assert _FakeTicker.history_calls[1]["start"] == "2025-09-14"
    assert result["put_call_ratio"] == pytest.approx(80 / 100, rel=1e-3)


def test_options_guard_raises_when_clip_bypassed(monkeypatch):
    from collectors import alternative

    _install_fake_yfinance(monkeypatch, _ohlcv("2024-09-01", D_PLUS_1))
    monkeypatch.setattr(alternative, "clip_to_trading_day", lambda frame, *_a, **_k: frame)

    with pytest.raises(FutureBarError, match=D_PLUS_1):
        alternative._fetch_options("AAPL", D)


def test_options_fetch_reraises_future_bar_through_process_one_ticker(monkeypatch):
    """I10899: a future bar reaching `_fetch_options` must propagate out of
    `_process_one_ticker`'s broad `except Exception` — never fold into an
    ordinary per-ticker `status: error`, which the caller's ThreadPoolExecutor
    loop would otherwise treat as isolated to one name among ~900."""
    from collectors import alternative

    _install_fake_yfinance(monkeypatch, _ohlcv("2024-09-01", D_PLUS_1))
    monkeypatch.setattr(alternative, "clip_to_trading_day", lambda frame, *_a, **_k: frame)
    monkeypatch.setattr(alternative, "_fetch_analyst", lambda ticker: {})
    monkeypatch.setattr(alternative, "_fetch_revisions", lambda ticker, bucket, run_date: {})
    monkeypatch.setattr(alternative, "_fetch_insider", lambda ticker, run_date: {})

    with pytest.raises(FutureBarError):
        alternative._process_one_ticker(
            "AAPL", D, "test-bucket", [], s3=None, s3_prefix="market_data/",
        )


# ── features/metron_supplemental.py::_fetch_ticker_ohlcv ────────────────────


def test_metron_supplemental_ohlcv_requests_d_bounded_window(monkeypatch):
    from features import metron_supplemental as ms

    calls: list[dict] = []

    def _download(*_a, **kw):
        calls.append(kw)
        return _ohlcv("2024-09-14", D_PLUS_1)

    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(download=_download))

    df = ms._fetch_ticker_ohlcv("MARUY", D, period="2y")

    assert len(calls) == 1
    assert "period" not in calls[0]
    # The REQUEST reaches D + 2 so a held EU/Asian listing's exchange-local end
    # cannot drop D's bar (alpha-engine-config-I11548); the frame is still clipped to D.
    assert calls[0]["end"] == "2026-09-16"
    assert df is not None
    assert df.index.max() == pd.Timestamp(D)  # clipped at the fetch boundary


def test_metron_supplemental_ohlcv_guard_raises_when_clip_bypassed(monkeypatch):
    from features import metron_supplemental as ms

    monkeypatch.setitem(
        sys.modules, "yfinance",
        SimpleNamespace(download=lambda *_a, **_kw: _ohlcv("2024-09-14", D_PLUS_1)),
    )
    monkeypatch.setattr(ms, "clip_to_trading_day", lambda frame, *_a, **_k: frame)

    with pytest.raises(FutureBarError, match=D_PLUS_1):
        ms._fetch_ticker_ohlcv("MARUY", D, period="2y")
