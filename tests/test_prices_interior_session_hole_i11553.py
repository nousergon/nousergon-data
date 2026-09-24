"""Regression tests for alpha-engine-config-I11553 — a price-cache refresh that
is current at both ends but missing a session in the middle must not publish
the hole.

Measured 2026-09-24: from the 2026-09-23T20:07Z refresh on, yfinance answered
``[..., 2026-09-21, 2026-09-23, 2026-09-24]`` for 837 of ~930 cached tickers
(A.parquet: 2512 rows, ABT.parquet: 2512 rows; AAPL, one of the 95 complete
answers: 2513). The short-fetch guard reads only row counts under 400 and the
behind-fetch guard (I11467) only the last bar, so both passed it, and D21
rebuilt ``market_data/close_history/consolidated.json`` with 2026-09-22 for 95
of 955 symbols. D19's ``staging/daily_closes/2026-09-22.parquet`` held 932.
"""

from __future__ import annotations

import io
import logging

import numpy as np
import pandas as pd
import pytest

import collectors.prices as _prices
from collectors.price_cache_holes import HOLE_FILL_FACTOR_AGREEMENT, SessionHoleFiller

_BUCKET = "alpha-engine-research"
_CACHE_KEY = "reference/price_cache/A.parquet"
_HOLE = "2026-09-22"


def _sessions(start: str, end: str) -> pd.DatetimeIndex:
    """Weekdays minus 2026 Labor Day: a superset of NYSE sessions before it, exact in
    September 2026 where the holes under test sit (extra rows are never holes)."""
    idx = pd.bdate_range(start, end)
    return idx[idx != pd.Timestamp("2026-09-07")]


def _frame(index: pd.DatetimeIndex, close: np.ndarray, volume: float = 1_000.0) -> pd.DataFrame:
    return pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99, "Close": close,
         "Volume": np.full(len(index), volume)},
        index=index,
    )


def _fetched(*, drop: tuple[str, ...] = (_HOLE,), end: str = "2026-09-24",
             scale: float = 1.0) -> pd.DataFrame:
    idx = _sessions("2024-06-03", end)
    close = np.linspace(100.0, 130.0, len(idx)) * scale
    df = _frame(idx, close)
    return df.drop(index=[pd.Timestamp(d) for d in drop])


def _daily_closes(session: str, close: float, ticker: str = "A") -> bytes:
    df = pd.DataFrame(
        {"date": [session], "Open": [close], "High": [close], "Low": [close],
         "Close": [close], "Adj_Close": [close], "Volume": [4242], "source": ["polygon"]},
        index=pd.Index([ticker], name="ticker"),
    )
    return _parquet_bytes(df)


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy")
    return buf.getvalue()


def _close_on(df: pd.DataFrame, day: str) -> float:
    return float(df.loc[pd.Timestamp(day), "Close"])


class _NoSuchKey(Exception):
    pass


class _FakeS3:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = dict(objects)
        self.gets: list[str] = []
        self.uploaded: dict[str, pd.DataFrame] = {}
        self.exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})()

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, path, bucket, key):
        self.uploaded[key] = pd.read_parquet(path)


def _staging_for(fetched: pd.DataFrame, hole_close: float, *, factor: float = 1.0) -> dict[str, bytes]:
    """D19 rows for the hole and its neighbours, ``factor`` off the fetched basis."""
    return {
        "staging/daily_closes/2026-09-21.parquet": _daily_closes("2026-09-21", _close_on(fetched, "2026-09-21") / factor),
        "staging/daily_closes/2026-09-22.parquet": _daily_closes(_HOLE, hole_close),
        "staging/daily_closes/2026-09-23.parquet": _daily_closes("2026-09-23", _close_on(fetched, "2026-09-23") / factor),
    }


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch):
    monkeypatch.setattr(_prices, "_sleep_seconds", lambda seconds: None)


def _refresh(monkeypatch, s3, fetched: pd.DataFrame, ticker: str = "A", trading_day: str = "2026-09-24"):
    monkeypatch.setattr(_prices.yf, "download", lambda *a, **k: fetched, raising=True)
    return _prices._refresh_stale(
        s3, _BUCKET, "reference/price_cache/", [ticker], "10y", 50,
        trading_day=trading_day,
    )


def test_the_measured_hole_is_filled_from_d19_before_upload(monkeypatch):
    """The 2026-09-24 shape: current at both ends, 2026-09-22 missing."""
    fetched = _fetched()
    s3 = _FakeS3(_staging_for(fetched, hole_close=167.26))

    refreshed, failed, written = _refresh(monkeypatch, s3, fetched)

    assert (refreshed, failed) == (1, [])
    published = s3.uploaded[_CACHE_KEY]
    assert pd.Timestamp(_HOLE) in published.index, "the interior session must be published"
    assert _close_on(published, _HOLE) == pytest.approx(167.26)
    assert published.loc[pd.Timestamp(_HOLE), "Volume"] == 4242
    assert published.index.is_monotonic_increasing
    assert written == [("A", len(fetched) + 1)]


def test_a_complete_answer_reads_nothing_extra(monkeypatch):
    s3 = _FakeS3({})

    refreshed, failed, _ = _refresh(monkeypatch, s3, _fetched(drop=()))

    assert (refreshed, failed) == (1, [])
    assert s3.gets == [], "hole detection is calendar-only; no hole, no S3 read"


def test_the_fill_is_rescaled_onto_the_fetched_dividend_basis(monkeypatch):
    """A dividend that went ex after the hole moves the whole adjusted series."""
    fetched = _fetched()
    s3 = _FakeS3(_staging_for(fetched, hole_close=167.26, factor=0.99))

    _refresh(monkeypatch, s3, fetched)

    assert _close_on(s3.uploaded[_CACHE_KEY], _HOLE) == pytest.approx(167.26 * 0.99)


def test_after_staging_expires_the_cache_carries_the_filled_bar_forward(monkeypatch):
    """``staging/`` expires after 7 days; a vendor that never restores the
    session must not re-open the hole the day the D19 file goes."""
    fetched = _fetched()
    cached = _fetched(drop=())
    cached.loc[pd.Timestamp(_HOLE), "Close"] = 111.0
    s3 = _FakeS3({_CACHE_KEY: _parquet_bytes(cached)})

    refreshed, failed, _ = _refresh(monkeypatch, s3, fetched)

    assert (refreshed, failed) == (1, [])
    assert _close_on(s3.uploaded[_CACHE_KEY], _HOLE) == pytest.approx(111.0)


def test_a_corporate_action_beside_the_hole_is_refused_and_reported(monkeypatch, caplog):
    """Neighbour factors that disagree mean one factor would be wrong on one side."""
    fetched = _fetched()
    objects = _staging_for(fetched, hole_close=167.26)
    objects["staging/daily_closes/2026-09-23.parquet"] = _daily_closes(
        "2026-09-23", _close_on(fetched, "2026-09-23") / (1 + 10 * HOLE_FILL_FACTOR_AGREEMENT),
    )
    s3 = _FakeS3(objects)

    with caplog.at_level(logging.WARNING):
        refreshed, failed, _ = _refresh(monkeypatch, s3, fetched)

    assert (refreshed, failed) == (1, []), "an unfilled hole never blocks the upload"
    assert pd.Timestamp(_HOLE) not in s3.uploaded[_CACHE_KEY].index
    errors = [r for r in caplog.records if r.levelno == logging.ERROR and "could NOT be filled" in r.getMessage()]
    assert len(errors) == 1, "one aggregated ERROR per run, never silent"
    assert "2026-09-22" in errors[0].getMessage()


def test_a_long_standing_gap_is_recorded_below_error(monkeypatch, caplog):
    """A hole older than the D19 staging retention (e.g. a listing transfer)
    has no source; it is recorded, but does not page every run."""
    fetched = _fetched(drop=("2026-08-12",))
    s3 = _FakeS3({})

    with caplog.at_level(logging.WARNING):
        _refresh(monkeypatch, s3, fetched)

    unfilled = [r for r in caplog.records if "could NOT be filled" in r.getMessage()]
    assert [r.levelno for r in unfilled] == [logging.WARNING]


def test_caret_macro_series_are_not_scanned(monkeypatch):
    """VIX/TNX/IRX/VIX3M carry their own FRED-vs-yfinance source selection."""
    filler = SessionHoleFiller(
        _FakeS3({}), _BUCKET,
        window_start=pd.Timestamp("2026-08-03").date(),
        expected_last=pd.Timestamp("2026-09-24").date(),
        skip={"VIX"},
    )
    fetched = _fetched()

    assert filler.fill("VIX", fetched) is fetched
    assert filler.unfilled == {}


def test_holes_are_interior_only():
    """A missing last bar is the behind-fetch guard's case, not a hole."""
    filler = SessionHoleFiller(
        _FakeS3({}), _BUCKET,
        window_start=pd.Timestamp("2026-08-03").date(),
        expected_last=pd.Timestamp("2026-09-24").date(),
    )
    assert filler.holes(_fetched(drop=(), end="2026-09-23").index) == []
    assert [d.isoformat() for d in filler.holes(_fetched().index)] == [_HOLE]
