"""Regression test for alpha-engine-config-I9256 — the ORIGIN of the macro
truncation: ``collectors/prices.py`` uploaded whatever yfinance returned.

Measured 2026-08-29: ``reference/price_cache/VIX3M.parquet`` was 4206 bytes /
ONE row, written 02:44:23 UTC in the same refresh batch that wrote a full
2515-row ``VIX.parquet``. A direct ``yf.download('^VIX3M', period='10y')`` run
minutes later returned 2485 rows, so the short answer was transient — and it
still destroyed the 10y cache, because the upload is unconditional.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest

import collectors.prices as _prices


def _ohlcv(n: int) -> pd.DataFrame:
    idx = pd.bdate_range("2016-08-19", periods=n)
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
        self.exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})()

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, path, bucket, key):
        self.uploads.append(key)


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy")
    return buf.getvalue()


def _patch_download(monkeypatch, frame: pd.DataFrame):
    monkeypatch.setattr(
        _prices.yf, "download", lambda *a, **k: frame, raising=True,
    )


def test_short_fetch_does_not_overwrite_a_full_price_cache(monkeypatch):
    """The measured VIX3M case: 1-row answer vs a 2515-row parquet."""
    full = _ohlcv(2515)
    s3 = _FakeS3({"reference/price_cache/VIX3M.parquet": _parquet_bytes(full)})
    _patch_download(monkeypatch, _ohlcv(1))

    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["VIX3M"], "10y", 50, trading_day="2026-09-14",
    )

    assert s3.uploads == [], "a shrinking refresh must not be uploaded"
    assert refreshed == 0
    assert failed == ["VIX3M"], "the ticker must be reported as failed, not silently skipped"


def test_full_fetch_still_uploads(monkeypatch):
    s3 = _FakeS3({"reference/price_cache/VIX.parquet": _parquet_bytes(_ohlcv(2500))})
    _patch_download(monkeypatch, _ohlcv(2515))

    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["VIX"], "10y", 50, trading_day="2026-09-14",
    )
    assert refreshed == 1
    assert failed == []
    assert s3.uploads == ["reference/price_cache/VIX.parquet"]


def test_short_fetch_for_a_brand_new_ticker_is_allowed(monkeypatch):
    """A genuinely new listing has no parquet to regress."""
    s3 = _FakeS3({})
    _patch_download(monkeypatch, _ohlcv(26))

    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["NEWCO"], "10y", 50, trading_day="2026-09-14",
    )
    assert refreshed == 1
    assert failed == []
    assert s3.uploads == ["reference/price_cache/NEWCO.parquet"]


def test_short_fetch_on_a_young_ticker_is_still_failed_never_excused(monkeypatch):
    """alpha-engine-config-I11230 follow-up (measured 2026-09-21, reverted
    commit 8aa1d7e6): a prior revision of this fix excused a shrinking
    refresh whenever the ticker's OWN existing history was also small
    (< `_SHORT_FETCH_ROW_THRESHOLD`), reasoning that a recently-listed
    ticker has "no growth margin". That was wrong on the guard's own
    condition — `len(new_df) < existing_rows` is a real regression
    regardless of the ticker's age, and excusing it for young tickers
    re-introduced exactly the defect I11230 exists to remove, just scoped to
    the youngest tickers. A refusal is `partial`, full stop: a young ticker
    (81 existing rows, the measured FDXF shape) whose fetch comes back
    SHORTER (10 rows) is a genuine loss and must be reported as `failed`,
    identically to a mature ticker's regression."""
    young = _ohlcv(81)
    s3 = _FakeS3({"reference/price_cache/FDXF.parquet": _parquet_bytes(young)})
    _patch_download(monkeypatch, _ohlcv(10))

    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["FDXF"], "10y", 50, trading_day="2026-09-14",
    )

    assert s3.uploads == [], "the existing (larger) history must be preserved"
    assert refreshed == 0
    assert failed == ["FDXF"], "a young ticker's regression is still a failure, not excused"


def test_unreadable_existing_parquet_raises_rather_than_overwriting(monkeypatch):
    class _BrokenS3(_FakeS3):
        def get_object(self, Bucket, Key):
            raise RuntimeError("s3 throttled")

    s3 = _BrokenS3({})
    with pytest.raises(RuntimeError, match="could not read the existing price-cache parquet"):
        _prices._existing_parquet_rows(
            s3, "alpha-engine-research", "predictor/price_cache/", "VIX3M",
        )
