"""Regression tests for alpha-engine-config-I11576 — the interior-session hole
filler (I11553) must not make the short-fetch guard refuse the fetch that
follows it.

Measured 2026-09-24 (read-only, ``reference/price_cache/HONA.parquet``
versions): the 20:07Z/20:12Z refreshes wrote 70 rows ending 2026-09-24 with no
2026-09-22; the 22:18Z refresh, the first with the filler, wrote 71 — the same
series plus 2026-09-22 at D19's polygon close (167.32). Every later fetch that
session still lacked 09-22 (70 rows), which the short-fetch guard (only under
400 rows) read as one row SHORTER than the cache and refused, three retries
each: HONA 70 vs 71, Q 228 vs 229, FDXF 83 vs 84, SOLS 233 vs 234 at 22:47Z.
They are the only cached tickers under the threshold. The guard had compared
the vendor's RAW answer with a cache the filler had grown; it now judges the
frame that would be published (fill first, then guard).
"""

from __future__ import annotations

import io
import logging

import numpy as np
import pandas as pd
import pytest

import collectors.prices as _prices

_BUCKET = "alpha-engine-research"
_TICKER = "HONA"
_CACHE_KEY = f"reference/price_cache/{_TICKER}.parquet"
_HOLE = "2026-09-22"
_TRADING_DAY = "2026-09-24"


def _sessions(start: str, end: str) -> pd.DatetimeIndex:
    """Weekdays minus 2026 Labor Day: exact NYSE sessions for June-Sept 2026
    apart from Juneteenth/July 3, which are simply extra rows here (extra rows
    are never holes)."""
    idx = pd.bdate_range(start, end)
    return idx[idx != pd.Timestamp("2026-09-07")]


def _series(*, start: str = "2026-06-15", end: str = _TRADING_DAY,
            drop: tuple[str, ...] = (_HOLE,)) -> pd.DataFrame:
    """A HONA-shaped young listing (~70 rows, under the 400-row threshold)."""
    idx = _sessions(start, end)
    close = np.linspace(150.0, 160.0, len(idx))
    df = pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99, "Close": close,
         "Volume": np.full(len(idx), 1_000.0)},
        index=idx,
    )
    return df.drop(index=[pd.Timestamp(d) for d in drop if pd.Timestamp(d) in df.index])


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy")
    return buf.getvalue()


def _daily_closes(session: str, close: float) -> bytes:
    df = pd.DataFrame(
        {"date": [session], "Open": [close], "High": [close], "Low": [close],
         "Close": [close], "Adj_Close": [close], "Volume": [3_873_796], "source": ["polygon"]},
        index=pd.Index([_TICKER], name="ticker"),
    )
    return _parquet_bytes(df)


class _NoSuchKey(Exception):
    pass


class _FakeS3:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = dict(objects)
        self.uploaded: dict[str, pd.DataFrame] = {}
        self.exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})()

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, path, bucket, key):
        with open(path, "rb") as fh:
            self.objects[key] = fh.read()
        self.uploaded[key] = pd.read_parquet(path)


def _staging(fetched: pd.DataFrame, hole_close: float) -> dict[str, bytes]:
    """D19's polygon rows for the hole and both neighbours, on the fetch's basis."""
    return {
        f"staging/daily_closes/{d}.parquet": _daily_closes(
            d, hole_close if d == _HOLE else float(fetched.loc[pd.Timestamp(d), "Close"]),
        )
        for d in ("2026-09-21", _HOLE, "2026-09-23")
    }


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch):
    monkeypatch.setattr(_prices, "_sleep_seconds", lambda seconds: None)


def _refresh(monkeypatch, s3, answer: pd.DataFrame, *, calls: list | None = None):
    """One ``_refresh_stale`` run where every vendor call (batch and each
    short-fetch retry) answers ``answer``."""
    def _download(*a, **k):
        if calls is not None:
            calls.append(k.get("tickers"))
        return answer.copy()

    monkeypatch.setattr(_prices.yf, "download", _download, raising=True)
    failure_reasons: dict[str, str] = {}
    refreshed, failed, written = _prices._refresh_stale(
        s3, _BUCKET, "reference/price_cache/", [_TICKER], "10y", 50,
        trading_day=_TRADING_DAY, failure_reasons=failure_reasons,
    )
    return refreshed, failed, written, failure_reasons


def test_the_measured_sequence_fill_then_same_session_refetch_is_not_refused(monkeypatch):
    """The exact 2026-09-24 sequence: the filler writes 09-22 into a young
    ticker's cache, then the next fetch of the same session still lacks it."""
    vendor = _series()  # 70 rows, no 2026-09-22 — what yfinance kept answering
    assert len(vendor) < _prices._SHORT_FETCH_ROW_THRESHOLD
    s3 = _FakeS3(_staging(vendor, hole_close=167.32))

    # Run 1 (22:18Z): cache holds the vendor's own 09-22-less series, so the
    # guard passes and the filler adds 2026-09-22 from D19.
    s3.objects[_CACHE_KEY] = _parquet_bytes(vendor)
    first = _refresh(monkeypatch, s3, vendor)
    assert first[:2] == (1, [])
    cached = pd.read_parquet(io.BytesIO(s3.objects[_CACHE_KEY]))
    assert len(cached) == len(vendor) + 1
    assert float(cached.loc[pd.Timestamp(_HOLE), "Close"]) == pytest.approx(167.32)

    # Run 2 (22:42Z): same answer, cache now one row longer.
    calls: list = []
    refreshed, failed, written, reasons = _refresh(monkeypatch, s3, vendor, calls=calls)

    assert (refreshed, failed, reasons) == (1, [], {}), (
        "a fetch missing only a session the filler supplies is not short"
    )
    assert calls == [_TICKER], "no short-fetch retries for an answer that is not short"
    published = s3.uploaded[_CACHE_KEY]
    assert len(published) == len(cached)
    assert float(published.loc[pd.Timestamp(_HOLE), "Close"]) == pytest.approx(167.32)
    assert written == [(_TICKER, len(cached))]


def test_after_staging_expires_the_cache_alone_keeps_the_refetch_accepted(monkeypatch):
    """Past the 7-day ``staging/`` lifecycle the only source is the cache's own
    filled bar; the fetch must still be accepted, and the bar carried forward."""
    vendor = _series()
    cached = _series(drop=())
    cached.loc[pd.Timestamp(_HOLE), "Close"] = 167.32
    s3 = _FakeS3({_CACHE_KEY: _parquet_bytes(cached)})

    refreshed, failed, _, _ = _refresh(monkeypatch, s3, vendor)

    assert (refreshed, failed) == (1, [])
    assert float(s3.uploaded[_CACHE_KEY].loc[pd.Timestamp(_HOLE), "Close"]) == pytest.approx(167.32)


def test_a_genuinely_truncated_fetch_with_a_hole_is_still_refused(monkeypatch, caplog):
    """The guard's purpose survives: an answer missing the START of the history
    is short however many interior sessions the filler supplies."""
    vendor = _series(start="2026-08-17")  # ~27 rows, 09-22 still missing
    cached = _series(drop=())
    cached.loc[pd.Timestamp(_HOLE), "Close"] = 167.32
    s3 = _FakeS3({_CACHE_KEY: _parquet_bytes(cached), **_staging(vendor, hole_close=167.32)})

    with caplog.at_level(logging.WARNING):
        refreshed, failed, _, reasons = _refresh(monkeypatch, s3, vendor)

    assert (refreshed, failed) == (0, [_TICKER])
    assert reasons == {_TICKER: _prices.FAIL_SHORT_FETCH}
    assert _CACHE_KEY not in s3.uploaded, "existing history preserved"
    assert not any("filled before upload" in r.getMessage() for r in caplog.records), (
        "a refused ticker published nothing; its fill must not be reported as published"
    )


def test_an_unfillable_hole_the_cache_carries_is_still_refused(monkeypatch):
    """When the filler cannot supply the session (a corporate action beside the
    hole), the published frame would lose a cached bar — still short."""
    vendor = _series()
    cached = _series(drop=())
    # The cache's neighbours sit on a different basis on each side of the hole,
    # so its factors disagree and the fill is refused; no staging exists.
    cached.loc[pd.Timestamp("2026-09-23"):, ["Open", "High", "Low", "Close"]] *= 0.9
    s3 = _FakeS3({_CACHE_KEY: _parquet_bytes(cached)})

    refreshed, failed, _, reasons = _refresh(monkeypatch, s3, vendor)

    assert (refreshed, failed) == (0, [_TICKER])
    assert reasons == {_TICKER: _prices.FAIL_SHORT_FETCH}
    assert _CACHE_KEY not in s3.uploaded
