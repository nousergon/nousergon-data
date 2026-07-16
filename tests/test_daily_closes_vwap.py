"""Regression tests for the daily_closes VWAP semantics.

Contract as of 2026-04-17 (Phase 7 VWAP centralization):

  * Polygon grouped-daily (`vw` field) → true volume-weighted VWAP is written.
  * yfinance fallback → VWAP is None. yfinance does not expose true VWAP and
    the previous `(H+L+C)/3` typical-price proxy silently misrepresented
    arithmetic typical price as VWAP, which contaminated the ArcticDB
    universe column once Phase 7 migration started materializing VWAP there.
  * FRED fallback → VWAP is None. FRED returns a single daily close value;
    there is no trade distribution to weight, so passing Close off as VWAP
    would be misrepresentation.

The previous contract (introduced after the 2026-04-10 incident) populated
VWAP with the proxy on yfinance fallback to prevent the executor from logging
"no VWAP column — skipping" for up to 5 consecutive days during polygon
outages. Per the 2026-04-17 decision, we accept that consequence rather than
perpetuate the proxy. `executor/price_cache.py::load_daily_vwap` already
looks back up to 5 prior trading days for a populated VWAP, which covers the
rare polygon-outage-on-multiple-consecutive-days scenario.
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from collectors import daily_closes


def _make_yf_frame(rows):
    """Build a yfinance-shaped DataFrame from (date, open, high, low, close, volume) tuples."""
    index = pd.DatetimeIndex([r[0] for r in rows])
    df = pd.DataFrame(
        {
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Adj Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
        },
        index=index,
    )
    return df


def test_yfinance_fallback_writes_none_vwap():
    """yfinance fallback must write VWAP=None, not a (H+L+C)/3 proxy."""
    records = []
    fake_frame = _make_yf_frame([
        ("2026-04-10", 100.0, 105.0, 99.0, 103.0, 1_000_000),
    ])

    mock_yf = MagicMock()
    mock_yf.download.return_value = fake_frame

    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        count = daily_closes._fetch_yfinance_closes(
            ["AAPL"], "2026-04-10", records
        )

    assert count == 1
    assert len(records) == 1
    row = records[0]
    assert row["ticker"] == "AAPL"
    assert row["VWAP"] is None, (
        "yfinance fallback must write VWAP=None. Writing (H+L+C)/3 as VWAP "
        "silently misrepresents arithmetic typical price as volume-weighted "
        "VWAP — see 2026-04-17 Phase 7 VWAP centralization decision."
    )


def test_yfinance_fallback_multi_ticker_vwap_none():
    """VWAP must be None for every row produced by the fallback."""
    records = []
    index = pd.DatetimeIndex(["2026-04-10"])
    multi = pd.DataFrame(
        {
            ("AAPL", "Open"): [100.0],
            ("AAPL", "High"): [105.0],
            ("AAPL", "Low"): [99.0],
            ("AAPL", "Close"): [103.0],
            ("AAPL", "Adj Close"): [103.0],
            ("AAPL", "Volume"): [1_000_000],
            ("MSFT", "Open"): [200.0],
            ("MSFT", "High"): [210.0],
            ("MSFT", "Low"): [198.0],
            ("MSFT", "Close"): [206.0],
            ("MSFT", "Adj Close"): [206.0],
            ("MSFT", "Volume"): [2_000_000],
        },
        index=index,
    )
    multi.columns = pd.MultiIndex.from_tuples(multi.columns)

    mock_yf = MagicMock()
    mock_yf.download.return_value = multi

    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        count = daily_closes._fetch_yfinance_closes(
            ["AAPL", "MSFT"], "2026-04-10", records
        )

    assert count == 2
    for row in records:
        assert row["VWAP"] is None, f"VWAP should be None (not proxy) for {row['ticker']}"


def test_yfinance_fetch_returns_requested_date_not_latest_bar():
    """Regression for alpha-engine-config#2475.

    ``_fetch_yfinance_closes`` must return the close ON-OR-BEFORE the
    requested ``date_str``, not yfinance's most-recent bar. The old
    ``period="5d"`` + ``iloc[-1]`` implementation always returned the latest
    available close regardless of what date was requested — the same defect
    already fixed for FRED in ``_fetch_fred_closes``/``_fetch_fred_window``
    (2026-05, L4492). This mislabeled a later close with an earlier date,
    which made the L1 cross-source observer (config#1277) falsely QUARANTINE
    SPY on nearly every historical date in its rolling window.
    """
    records = []
    fake_frame = _make_yf_frame([
        ("2026-06-29", 100.0, 101.0, 99.0, 100.5, 1_000_000),
        ("2026-06-30", 102.0, 103.0, 101.0, 102.5, 1_200_000),
        ("2026-07-01", 200.0, 201.0, 199.0, 200.5, 900_000),  # a later "latest" bar
    ])

    mock_yf = MagicMock()
    mock_yf.download.return_value = fake_frame

    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        count = daily_closes._fetch_yfinance_closes(["SPY"], "2026-06-30", records)

    assert count == 1
    assert records[0]["Close"] == 102.5, (
        "must return the close for the REQUESTED date (2026-06-30), not "
        "yfinance's latest available bar (2026-07-01 close=200.5)"
    )

    call_kwargs = mock_yf.download.call_args.kwargs
    assert "period" not in call_kwargs, (
        "must not use period= (anchored to wall-clock now, ignores the "
        "requested date) — use a start/end window bounded by date_str"
    )
    assert call_kwargs.get("start") == "2026-06-21"
    assert call_kwargs.get("end") == "2026-07-01"


def test_yfinance_fetch_falls_back_to_prior_trading_day():
    """A requested date with no exact yfinance bar (weekend/holiday) resolves
    to the nearest prior trading day, mirroring FRED's on-or-before semantics
    — not skipped, and not a later bar."""
    records = []
    fake_frame = _make_yf_frame([
        ("2026-07-02", 300.0, 301.0, 299.0, 300.5, 1_000_000),  # Thursday
        ("2026-07-06", 305.0, 306.0, 304.0, 305.5, 1_100_000),  # next Monday
    ])

    mock_yf = MagicMock()
    mock_yf.download.return_value = fake_frame

    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        # 2026-07-04 (Saturday) has no bar — should resolve to 07-02, not 07-06.
        count = daily_closes._fetch_yfinance_closes(["SPY"], "2026-07-04", records)

    assert count == 1
    assert records[0]["Close"] == 300.5
