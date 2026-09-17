"""Regression tests for alpha-engine-config-I9288 — stray ``^``-prefixed
price-cache keys (``^VIX``, ``^TNX``, ``^VIX3M``) entering the refresh
population and failing the daily/weekly collector run.

Measured mechanism (I9288, 2026-09-15 comment):
  1. ``_find_stale_fast`` enumerates ``reference/price_cache/`` basenames to
     build its staleness map, so a stray ``^VIX3M.parquet`` key (or a
     caret-embedded literal reaching ``all_tickers`` from upstream, e.g.
     ``weekly_collector.py``'s ``_MACRO_DAILY_TICKERS``) entered the refresh
     population.
  2. ``^VIX3M`` (with the caret already embedded in the ticker string) does
     not match any BARE entry in ``_CARET_SYMBOLS``, so it skipped the I9286
     FRED longest-of selection and went to yfinance alone.
  3. yfinance answered 1 row from EC2, hit the I9256 short-fetch refusal, and
     the whole ``prices`` collector step reported ``status=partial`` — which
     failed the whole weekly/daily run.
  4. ``_refresh_stale`` is the writer: it formats
     ``f"{prefix}{ticker}.parquet"`` with whatever ``ticker`` string it was
     handed, so a caret-embedded ticker produces the stray key itself.

These tests pin two independent defenses:
  * Population-level (``_find_stale_fast`` / ``_reject_caret_tickers``):
    a ``^``-prefixed basename or ticker literal is DROPPED with a WARNING,
    never entering the staleness map or the refresh population. A stray key
    already in S3 must not halt the collector — but the drop is never
    silent.
  * Write-time chokepoint
    (``builders._price_cache_writeboth.assert_valid_price_cache_ticker``):
    any writer handed a ``^``-containing ticker RAISES. This is defense in
    depth for a bug upstream of the population filter — a producer repo
    fails loud on its own writers rather than emit a corrupt key.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from builders._price_cache_writeboth import assert_valid_price_cache_ticker
from collectors import CaretTickerError, prices


def _make_s3(contents: list[dict]) -> MagicMock:
    class _Paginator:
        def paginate(self, *, Bucket: str, Prefix: str):
            yield {"Contents": contents}

    s3 = MagicMock()
    s3.get_paginator.return_value = _Paginator()
    return s3


# ---------------------------------------------------------------------------
# assert_valid_price_cache_ticker — the write-time chokepoint
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ticker", ["^VIX3M", "^VIX", "^TNX", "^IRX", "AA^PL"])
def test_chokepoint_raises_on_any_caret_ticker(ticker):
    with pytest.raises(ValueError, match=r"\^"):
        assert_valid_price_cache_ticker(ticker)


@pytest.mark.parametrize("ticker", ["VIX3M", "AAPL", "BRK.B", "TWO"])
def test_chokepoint_accepts_bare_tickers(ticker):
    assert_valid_price_cache_ticker(ticker) is None


def test_prices_refresh_stale_refuses_caret_ticker_write(monkeypatch, tmp_path):
    """Even if a caret-embedded ticker reaches ``_refresh_stale`` (bypassing
    the population filter — the exact bug this issue closes), the write-time
    chokepoint raises instead of uploading a stray key.

    alpha-engine-config-I10904: this call site's per-ticker isolation
    (``except FutureBarError: raise`` / ``except Exception``) used to catch
    the guard's bare ``ValueError`` in the broad handler and record the
    ticker as an ordinary per-ticker ``failed`` — indistinguishable from a
    transient yfinance miss. It now propagates as ``CaretTickerError`` out
    of ``_refresh_stale`` (a population-contract violation, not a per-ticker
    one), before any upload."""
    idx = pd.bdate_range("2016-08-19", periods=2600)
    fake_df = pd.DataFrame(
        {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0},
        index=idx,
    )
    monkeypatch.setattr(prices.yf, "download", lambda **_kw: fake_df.copy())

    class _NoSuchKey(Exception):
        pass

    class _RecordingS3:
        exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})()

        def __init__(self):
            self.uploads: list[str] = []

        def get_object(self, Bucket, Key):
            raise _NoSuchKey(Key)

        def upload_file(self, _local, _bucket, key):
            self.uploads.append(key)

    s3 = _RecordingS3()
    with pytest.raises(CaretTickerError, match=r"\^VIX3M"):
        prices._refresh_stale(
            s3=s3,
            bucket="test-bucket",
            s3_prefix="predictor/price_cache/",
            stale=["^VIX3M"],
            fetch_period="10y",
            batch_size=10,
            trading_day="2026-09-14",
        )

    assert s3.uploads == [], "chokepoint must refuse the write, not just log"


# ---------------------------------------------------------------------------
# _find_stale_fast / _reject_caret_tickers — the population-level filter
# ---------------------------------------------------------------------------


def test_stray_caret_basename_excluded_from_staleness_map():
    """A stray ``^VIX3M.parquet`` key in the S3 listing must never become a
    ticker in the staleness map — it is dropped, not treated as either
    fresh or stale for a ticker named ``^VIX3M``."""
    s3 = _make_s3([
        {"Key": "reference/price_cache/^VIX3M.parquet",
         "LastModified": datetime(2026, 9, 14, 20, 8, 43, tzinfo=timezone.utc)},
        {"Key": "reference/price_cache/^VIX.parquet",
         "LastModified": datetime(2026, 9, 14, 20, 8, 43, tzinfo=timezone.utc)},
        {"Key": "reference/price_cache/^TNX.parquet",
         "LastModified": datetime(2026, 9, 14, 20, 8, 43, tzinfo=timezone.utc)},
    ])
    stale = prices._find_stale_fast(
        s3, "bucket", "reference/price_cache/", [],
        staleness_threshold_days=1, reference_date="2026-09-15",
    )
    # Nothing requested (all_tickers=[]), so nothing in `stale` regardless —
    # this asserts the enumeration doesn't error/crash on the stray keys.
    assert stale == []


def test_caret_literal_in_requested_tickers_never_enters_stale_population():
    """The production bug: ``all_tickers`` itself contains a caret-embedded
    literal (from an upstream caller like ``_MACRO_DAILY_TICKERS``). It must
    be dropped before the staleness check, never appended to ``stale``, even
    though its matching S3 key exists and is fresh."""
    s3 = _make_s3([
        {"Key": "reference/price_cache/^VIX3M.parquet",
         "LastModified": datetime(2026, 9, 14, 20, 8, 43, tzinfo=timezone.utc)},
        {"Key": "reference/price_cache/VIX3M.parquet",
         "LastModified": datetime(2026, 9, 14, 20, 8, 44, tzinfo=timezone.utc)},
    ])
    stale = prices._find_stale_fast(
        s3, "bucket", "reference/price_cache/", ["VIX3M", "^VIX3M"],
        staleness_threshold_days=3, reference_date="2026-09-15",
    )
    assert "^VIX3M" not in stale
    assert stale == []  # bare VIX3M is fresh (1 day old, threshold=3)


def test_bare_vix3m_still_goes_through_normal_staleness_check():
    """The fix must not collaterally break the legitimate bare ``VIX3M``
    ticker's staleness handling — only caret-prefixed entries are dropped."""
    s3 = _make_s3([
        {"Key": "reference/price_cache/VIX3M.parquet",
         "LastModified": datetime(2026, 8, 1, tzinfo=timezone.utc)},
    ])
    stale = prices._find_stale_fast(
        s3, "bucket", "reference/price_cache/", ["VIX3M"],
        staleness_threshold_days=1, reference_date="2026-09-15",
    )
    assert stale == ["VIX3M"]


def test_reject_caret_tickers_drops_and_warns(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="collectors.prices"):
        out = prices._reject_caret_tickers(["AAPL", "^VIX3M", "VIX3M"], "test-ctx")

    assert out == ["AAPL", "VIX3M"]
    assert any("^VIX3M" in rec.message for rec in caplog.records)
    assert any("test-ctx" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# End-to-end: collect() must not go "partial" because of a stray caret key
# ---------------------------------------------------------------------------


def test_collect_does_not_mark_partial_from_stray_caret_key(monkeypatch):
    """Reproduces the live failure end-to-end: a stray ``^VIX3M.parquet``
    key sits in S3 (residual from the pre-fix bug) and the caller's ticker
    list (mirroring ``_MACRO_DAILY_TICKERS``) also carries the caret literal.
    Before the fix, ``^VIX3M`` entered ``stale``, was refreshed via
    yfinance-only (bypassing FRED), got a 1-row short answer, hit the I9256
    refusal, and flipped the whole ``prices`` result to ``status=partial``.
    After the fix, ``^VIX3M`` never reaches ``_refresh_stale`` at all."""

    def _fake_find_stale(s3, bucket, prefix, all_tickers, staleness_threshold_days, reference_date=None):
        # Delegate to the real implementation so the population filter is
        # exercised, but keep the S3 double local to this test.
        return prices._find_stale_fast(
            s3, bucket, prefix, all_tickers, staleness_threshold_days, reference_date,
        )

    s3 = _make_s3([
        {"Key": "predictor/price_cache/^VIX3M.parquet",
         "LastModified": datetime(2026, 9, 14, 20, 8, 43, tzinfo=timezone.utc)},
    ])
    monkeypatch.setattr(prices, "boto3", MagicMock(client=lambda *_a, **_kw: s3))

    refresh_calls: list[list[str]] = []

    def _fake_refresh_stale(s3, bucket, s3_prefix, stale, fetch_period, batch_size, **_kw):
        refresh_calls.append(list(stale))
        # Simulate every requested ticker refreshing cleanly — the point of
        # this test is that ^VIX3M never reaches this call at all.
        return len(stale), []

    monkeypatch.setattr(prices, "_refresh_stale", _fake_refresh_stale)

    result = prices.collect(
        bucket="test-bucket",
        tickers=["SPY", "^VIX", "^VIX3M", "^TNX", "^IRX"],
        s3_prefix="predictor/price_cache/",
        staleness_threshold_days=1,
        reference_date="2026-09-15",
    )

    assert result["status"] == "ok"
    assert result["failed"] == 0
    for call in refresh_calls:
        assert not any(t.startswith("^") for t in call), call
