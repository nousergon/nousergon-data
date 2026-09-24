"""Regression tests for alpha-engine-config-I11467 — a full-length refresh that
is missing the run's last session must not replace a cache that has it.

Measured on the 2026-09-23 weekly rehearsal (execution ``2b6ab316``,
trading_day 2026-09-22), launched 00:00 UTC = 20:00 ET on 2026-09-22:
yfinance answered ``end=2026-09-23`` with ~2,512-row series ending 2026-09-21,
and ``reference/price_cache/AAPL.parquet`` went from a version ending
2026-09-22 (written 2026-09-22T20:09Z) to versions ending 2026-09-21
(00:10Z and 01:44Z on 2026-09-23). The I9256 short-fetch guard only compares
row counts against a 400-row threshold, so it never looked.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest

import collectors.prices as _prices

_KEY = "reference/price_cache/AAPL.parquet"


def _ending(last: str, n: int = 2512) -> pd.DataFrame:
    idx = pd.bdate_range(end=last, periods=n)
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
    def __init__(self, objects: dict[str, bytes]):
        self.objects = dict(objects)
        self.uploads: list[str] = []
        self.gets: list[str] = []
        self.exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})()

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, path, bucket, key):
        self.uploads.append(key)


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy")
    return buf.getvalue()


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch):
    monkeypatch.setattr(_prices, "_sleep_seconds", lambda seconds: None)


def _refresh(monkeypatch, s3, fetched: pd.DataFrame, trading_day: str):
    monkeypatch.setattr(_prices.yf, "download", lambda *a, **k: fetched, raising=True)
    return _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["AAPL"], "10y", 50,
        trading_day=trading_day,
    )


def test_fetch_ending_before_the_cached_last_bar_is_not_written(monkeypatch):
    """The measured rehearsal case: cache ends 09-22, fetch ends 09-21."""
    s3 = _FakeS3({_KEY: _parquet_bytes(_ending("2026-09-22"))})

    refreshed, failed, written = _refresh(
        monkeypatch, s3, _ending("2026-09-21"), trading_day="2026-09-22",
    )

    assert s3.uploads == [], "a refresh that moves the last bar backwards must not upload"
    assert refreshed == 0
    assert written == []
    assert failed == ["AAPL"], "the refusal must degrade the run, never be silent"


def test_current_fetch_uploads_without_reading_the_cache(monkeypatch):
    """A fetch that ends on the expected session pays no extra S3 GET."""
    s3 = _FakeS3({_KEY: _parquet_bytes(_ending("2026-09-21"))})

    refreshed, failed, _ = _refresh(
        monkeypatch, s3, _ending("2026-09-22"), trading_day="2026-09-22",
    )

    assert (refreshed, failed) == (1, [])
    assert s3.uploads == [_KEY]
    assert s3.gets == []


def test_expected_last_bar_is_the_prior_session_on_a_weekend_run(monkeypatch):
    """A Saturday run expects Friday's bar; ending on Friday is current."""
    s3 = _FakeS3({_KEY: _parquet_bytes(_ending("2026-09-25"))})

    refreshed, failed, _ = _refresh(
        monkeypatch, s3, _ending("2026-09-25"), trading_day="2026-09-26",
    )

    assert (refreshed, failed) == (1, [])
    assert s3.gets == []


def test_lagging_fetch_that_does_not_regress_the_cache_still_uploads(monkeypatch):
    """Vendor lag with a cache that is equally behind is not a regression."""
    s3 = _FakeS3({_KEY: _parquet_bytes(_ending("2026-09-21"))})

    refreshed, failed, _ = _refresh(
        monkeypatch, s3, _ending("2026-09-21"), trading_day="2026-09-22",
    )

    assert (refreshed, failed) == (1, [])
    assert s3.uploads == [_KEY]
    assert s3.gets == [_KEY]


def test_lagging_fetch_for_a_ticker_with_no_cache_uploads(monkeypatch):
    s3 = _FakeS3({})

    refreshed, failed, _ = _refresh(
        monkeypatch, s3, _ending("2026-09-21"), trading_day="2026-09-22",
    )

    assert (refreshed, failed) == (1, [])
    assert s3.uploads == [_KEY]


def test_unreadable_cache_on_a_lagging_fetch_fails_rather_than_overwriting(monkeypatch):
    class _BrokenS3(_FakeS3):
        def get_object(self, Bucket, Key):
            raise RuntimeError("s3 throttled")

    s3 = _BrokenS3({})

    refreshed, failed, _ = _refresh(
        monkeypatch, s3, _ending("2026-09-21"), trading_day="2026-09-22",
    )

    assert s3.uploads == []
    assert (refreshed, failed) == (0, ["AAPL"])


def test_expected_last_bar_anchors_on_the_nyse_calendar():
    assert _prices._expected_last_bar("2026-09-22").isoformat() == "2026-09-22"
    assert _prices._expected_last_bar("2026-09-26").isoformat() == "2026-09-25"
    assert _prices._expected_last_bar("2026-09-27").isoformat() == "2026-09-25"
