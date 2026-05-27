"""Regression tests for `builders.backfill.backfill` ticker_filter error split.

The single "no data or in skip list" error message previously emitted by
the ticker_filter sad path masked the 2026-05-27 PSTG case (ticker
dropped from constituents but still in chronic_polygon_gaps allowlist)
by collapsing it into a "no data" framing. The flow-doctor alert
``Ticker PSTG not found in universe (no data or in skip list)`` led to
~15 minutes of investigation before the constituents-drift root cause
surfaced.

After the split, each disqualification reason returns its own ``error``
code so an operator (or future CW alarm) can tell at a glance whether
the failure is:
  - ``ticker_in_skip_list``  — config issue (ticker is in _SKIP_TICKERS)
  - ``ticker_is_sector_etf`` — config issue (ticker matches a sector
                               ETF prefix)
  - ``ticker_no_data``       — transient (parquet missing or empty)
  - ``ticker_not_in_constituents`` — config drift (chronic_polygon_gaps
                               or other allowlist lags an S&P remove)
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd


def _ohlcv(start: str, n: int = 5) -> pd.DataFrame:
    idx = pd.date_range(start, periods=n, freq="B")
    return pd.DataFrame(
        {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0, "Volume": 1_000_000},
        index=idx,
    )


def _stub_backfill_deps(price_data: dict, constituents_set: set):
    """Build the patch context all four tests need: stub the four loader
    helpers + the daily-delta merge + the S3 client. The ticker_filter
    error path returns BEFORE any ArcticDB write, so get_universe_lib /
    get_macro_lib don't need to be stubbed.
    """
    from builders import backfill as _bf

    patches = [
        patch.object(_bf, "_load_full_cache", return_value=price_data),
        patch.object(
            _bf, "_apply_daily_delta",
            return_value=(price_data, []),
        ),
        patch.object(_bf, "_extract_macro_series", return_value={}),
        patch.object(_bf, "_load_sector_map", return_value={}),
        patch.object(_bf, "_load_cached_fundamentals", return_value={}),
        patch.object(_bf, "_load_cached_alternative", return_value={}),
        patch.object(
            _bf, "_load_current_constituents",
            return_value=set(constituents_set),
        ),
        patch.object(_bf, "boto3", MagicMock()),
    ]
    return patches


def _enter_all(patches):
    """Enter every patch and return the list of mock objects in order."""
    return [p.__enter__() for p in patches]


def _exit_all(patches):
    for p in reversed(patches):
        p.__exit__(None, None, None)


def test_ticker_filter_returns_in_skip_list_error_for_skip_ticker():
    """A ticker in _SKIP_TICKERS (and not promoted via _UNIVERSE_EXTRA)
    is rejected as a config error, not silently as 'no data'. VIX is
    in _SKIP_TICKERS but not _UNIVERSE_EXTRA."""
    from builders.backfill import backfill

    price_data = {"AAPL": _ohlcv("2026-04-01"), "VIX": _ohlcv("2026-04-01")}
    patches = _stub_backfill_deps(
        price_data=price_data,
        constituents_set={"AAPL", "VIX"},  # even if "in constituents", skip wins
    )
    _enter_all(patches)
    try:
        result = backfill(
            bucket="test-bucket",
            ticker_filter="VIX",
            dry_run=False,
        )
    finally:
        _exit_all(patches)

    assert result["status"] == "error"
    assert result["error"] == "ticker_in_skip_list: VIX"


def test_ticker_filter_returns_sector_etf_error_for_sector_etf():
    """A ticker matching the _SECTOR_ETF_PREFIXES (XL*) is rejected with
    its own dedicated error code."""
    from builders.backfill import backfill

    price_data = {"AAPL": _ohlcv("2026-04-01"), "XLF": _ohlcv("2026-04-01")}
    patches = _stub_backfill_deps(
        price_data=price_data,
        constituents_set={"AAPL", "XLF"},
    )
    _enter_all(patches)
    try:
        result = backfill(
            bucket="test-bucket",
            ticker_filter="XLF",
            dry_run=False,
        )
    finally:
        _exit_all(patches)

    assert result["status"] == "error"
    assert result["error"] == "ticker_is_sector_etf: XLF"


def test_ticker_filter_returns_no_data_error_when_parquet_missing():
    """A ticker that has no row in price_data (parquet absent / empty)
    returns ticker_no_data — distinguishes a transient S3/data fault
    from a config-drift fault."""
    from builders.backfill import backfill

    price_data = {"AAPL": _ohlcv("2026-04-01")}  # NVDA missing
    patches = _stub_backfill_deps(
        price_data=price_data,
        constituents_set={"AAPL", "NVDA"},
    )
    _enter_all(patches)
    try:
        result = backfill(
            bucket="test-bucket",
            ticker_filter="NVDA",
            dry_run=False,
        )
    finally:
        _exit_all(patches)

    assert result["status"] == "error"
    assert result["error"] == "ticker_no_data: NVDA"


def test_ticker_filter_returns_not_in_constituents_error_for_pstg_case():
    """The 2026-05-27 PSTG case verbatim: PSTG has parquet data (the
    chronic-gap self-heal JUST wrote it) but PSTG is no longer in the
    current constituents set. New ticker_not_in_constituents code names
    the config-drift cause so an operator sees it directly."""
    from builders.backfill import backfill

    price_data = {
        "AAPL": _ohlcv("2026-04-01"),
        "PSTG": _ohlcv("2026-04-01"),
    }
    patches = _stub_backfill_deps(
        price_data=price_data,
        constituents_set={"AAPL"},  # PSTG NOT in constituents — the bug case
    )
    _enter_all(patches)
    try:
        result = backfill(
            bucket="test-bucket",
            ticker_filter="PSTG",
            dry_run=False,
        )
    finally:
        _exit_all(patches)

    assert result["status"] == "error"
    assert result["error"] == "ticker_not_in_constituents: PSTG"
