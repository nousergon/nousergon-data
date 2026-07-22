"""Trading-day-exact staleness for collectors/prices.py (config#2756).

Background: ``_find_stale_fast`` used to gate staleness on a calendar-day
delta with a fixed "+2 days for weekends" buffer, calibrated for the
Saturday-only weekly cadence. Invoking the same collector on a Mon-Fri daily
cadence (config#2756's daily price_cache refresh) under that check still only
refreshed a ticker every ~3-4 calendar days regardless of the configured
threshold — the buffer throttles refresh frequency independent of invocation
cadence. These tests pin the trading-day-exact replacement
(``nousergon_lib.dates.is_fresh_in_trading_days``), which keeps
"N trading sessions stale" meaning the same thing at any cadence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from collectors import prices


def _make_s3(last_modified_by_ticker: dict[str, datetime]) -> MagicMock:
    contents = [
        {"Key": f"predictor/price_cache/{ticker}.parquet", "LastModified": last_mod}
        for ticker, last_mod in last_modified_by_ticker.items()
    ]

    class _Paginator:
        def paginate(self, *, Bucket: str, Prefix: str):
            yield {"Contents": contents}

    s3 = MagicMock()
    s3.get_paginator.return_value = _Paginator()
    return s3


def test_friday_close_is_fresh_on_saturday_and_sunday():
    """A Friday-dated parquet must NOT be stale when checked over the
    weekend — trading-day arithmetic (not calendar days) is what makes
    Sat/Sun freshness checks holiday/weekend-safe."""
    s3 = _make_s3({"AAPL": datetime(2026, 5, 22, tzinfo=timezone.utc)})  # Friday
    stale = prices._find_stale_fast(
        s3, "bucket", "predictor/price_cache/", ["AAPL"],
        staleness_threshold_days=1, reference_date="2026-05-24",  # Sunday
    )
    assert stale == []


def test_friday_close_is_stale_by_tuesday_at_one_day_threshold():
    """Two missed trading sessions (Mon + Tue) must trip a max_stale=1 gate —
    this is the exact Tue-Fri regression config#2756 exists to fix: under the
    daily EOD cadence, a Friday-dated cache entry must be recognized as stale
    once Monday's close exists and is missing."""
    s3 = _make_s3({"AAPL": datetime(2026, 5, 15, tzinfo=timezone.utc)})  # Friday
    stale = prices._find_stale_fast(
        s3, "bucket", "predictor/price_cache/", ["AAPL"],
        staleness_threshold_days=1, reference_date="2026-05-19",  # Tuesday (no holiday that week)
    )
    assert stale == ["AAPL"]


def test_missing_ticker_is_always_stale():
    s3 = _make_s3({})
    stale = prices._find_stale_fast(
        s3, "bucket", "predictor/price_cache/", ["ZZZZ"],
        staleness_threshold_days=3, reference_date="2026-05-26",
    )
    assert stale == ["ZZZZ"]


def test_reference_date_defaults_to_today_when_omitted():
    """Back-compat: an omitted reference_date must not raise or silently
    treat everything as stale/fresh — it falls back to today's UTC date,
    same as the pre-config#2756 behavior anchored on ``datetime.now()``."""
    s3 = _make_s3({"AAPL": datetime.now(timezone.utc)})
    stale = prices._find_stale_fast(
        s3, "bucket", "predictor/price_cache/", ["AAPL"],
        staleness_threshold_days=3,
    )
    assert stale == []


def test_collect_threads_reference_date_into_staleness_check(monkeypatch):
    """``collect()``'s ``reference_date`` kwarg must reach ``_find_stale_fast``
    — without this wiring, weekly_collector's per-call ``run_date`` threading
    is a no-op and staleness silently reverts to "today", breaking
    deterministic re-runs against a fixed ``--date``."""
    captured = {}

    def _fake_find_stale(s3, bucket, prefix, all_tickers, staleness_threshold_days, reference_date=None):
        captured["reference_date"] = reference_date
        return []

    monkeypatch.setattr(prices, "_find_stale_fast", _fake_find_stale)
    monkeypatch.setattr(prices, "boto3", MagicMock())

    prices.collect(
        bucket="b", tickers=["AAPL"], reference_date="2026-05-26",
    )
    assert captured["reference_date"] == "2026-05-26"
