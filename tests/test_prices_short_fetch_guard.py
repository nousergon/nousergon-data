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


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch):
    """The I11287 bounded retry sleeps between attempts in production; every
    test in this file exercises the guard synchronously, so backoff is
    zeroed here rather than each test eating up to 14s of real sleep."""
    monkeypatch.setattr(_prices, "_sleep_seconds", lambda seconds: None)


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


# ── alpha-engine-config-I11287: bounded single-ticker retry ────────────────
# A short answer is frequently transient (measured: this guard fired for
# ^VIX3M twice in 30 days — traced to a stray caret-embedded ticker literal
# fixed same-day by I9288, but the guard itself is a general defense the
# retry below applies to regardless of WHY a given answer came back short).


def _patch_download_sequence(monkeypatch, frames: list[pd.DataFrame]):
    """Each call to ``yf.download`` returns the next frame in ``frames``;
    the last frame repeats once the sequence is exhausted."""
    calls: list[dict] = []

    def _fake(*args, **kwargs):
        calls.append(kwargs)
        idx = min(len(calls) - 1, len(frames) - 1)
        return frames[idx]

    monkeypatch.setattr(_prices.yf, "download", _fake, raising=True)
    return calls


def test_short_fetch_recovers_after_a_transient_retry(monkeypatch):
    """Batch fetch answers short once; the SECOND call (first dedicated
    retry) answers full — the ticker must be refreshed, not failed, and the
    retry count recorded for visibility."""
    full = _ohlcv(2515)
    s3 = _FakeS3({"reference/price_cache/VIX3M.parquet": _parquet_bytes(full)})
    calls = _patch_download_sequence(monkeypatch, [_ohlcv(1), _ohlcv(2520)])

    retries: dict[str, int] = {}
    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["VIX3M"], "10y", 50,
        trading_day="2026-09-14", short_fetch_retries=retries,
    )

    assert refreshed == 1
    assert failed == []
    assert s3.uploads == ["reference/price_cache/VIX3M.parquet"]
    assert retries == {"VIX3M": 1}, "recovered on the first dedicated retry attempt"
    assert len(calls) == 2, "one batch call + exactly one retry call, no more"


def test_short_fetch_retry_exhausted_still_reads_partial_with_count_recorded(monkeypatch):
    """A persistently short answer (every attempt, batch + all 3 retries)
    must still end up `failed` exactly as before this change — the retry
    is insurance against a TRANSIENT glitch, never an excuse."""
    full = _ohlcv(2515)
    s3 = _FakeS3({"reference/price_cache/VIX3M.parquet": _parquet_bytes(full)})
    calls = _patch_download_sequence(monkeypatch, [_ohlcv(1)])  # always short

    retries: dict[str, int] = {}
    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["VIX3M"], "10y", 50,
        trading_day="2026-09-14", short_fetch_retries=retries,
    )

    assert s3.uploads == []
    assert refreshed == 0
    assert failed == ["VIX3M"]
    assert retries == {"VIX3M": _prices._SHORT_FETCH_RETRY_ATTEMPTS}
    assert len(calls) == 1 + _prices._SHORT_FETCH_RETRY_ATTEMPTS


def _as_yfinance_single_ticker(frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """yfinance >= 0.2.48's default single-ticker shape: (Price, Ticker) columns."""
    out = frame.copy()
    out.columns = pd.MultiIndex.from_product([out.columns, [ticker]], names=["Price", "Ticker"])
    return out


def test_retry_recovers_on_yfinance_multiindex_default(monkeypatch):
    """alpha-engine-config-I11445: the 2026-09-23 rehearsal's HONA/Q/FDXF/SOLS.

    The retry's single-ticker ``yf.download`` gets (Price, Ticker) MultiIndex
    columns unless it passes ``multi_level_index=False``; on that frame
    ``dropna(subset=["Close"])`` raised KeyError(['Close']) out of a helper
    documented as never raising. This fake behaves like real yfinance: flat
    only when asked to be."""
    existing = _ohlcv(69)
    s3 = _FakeS3({"reference/price_cache/HONA.parquet": _parquet_bytes(existing)})
    calls: list[dict] = []

    def _fake(*args, **kwargs):
        calls.append(kwargs)
        frame = _ohlcv(68) if len(calls) == 1 else _ohlcv(69)
        if kwargs.get("group_by") == "ticker":  # the batch call: (Ticker, Price)
            out = frame.copy()
            out.columns = pd.MultiIndex.from_product([["HONA"], out.columns])
            return out
        if kwargs.get("multi_level_index", True):
            return _as_yfinance_single_ticker(frame, "HONA")
        return frame

    monkeypatch.setattr(_prices.yf, "download", _fake, raising=True)

    retries: dict[str, int] = {}
    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", ["HONA"], "10y", 50,
        trading_day="2026-09-14", short_fetch_retries=retries,
    )

    assert failed == []
    assert refreshed == 1
    assert written == [("HONA", 69)]
    # A young listing never reaches the helper's 400-row early exit, so it
    # spends every attempt and keeps the longest answer.
    assert retries == {"HONA": _prices._SHORT_FETCH_RETRY_ATTEMPTS}


def test_retry_helper_never_raises_on_a_multiindex_frame(monkeypatch):
    """Even if a caller's yfinance ignores ``multi_level_index`` the helper
    flattens the frame rather than raising."""
    monkeypatch.setattr(
        _prices.yf, "download",
        lambda *a, **k: _as_yfinance_single_ticker(_ohlcv(500), "Q"), raising=True,
    )
    import datetime as _dt

    df, attempts = _prices._retry_short_fetch_ticker(
        "Q", "Q", _dt.date(2016, 8, 19), _dt.date(2026, 9, 15), "2026-09-14",
    )

    assert attempts == 1
    assert df is not None and len(df) == 500
    assert "Close" in df.columns and not isinstance(df.columns, pd.MultiIndex)


def test_retry_budget_is_bounded_across_the_whole_run(monkeypatch):
    """More refusing tickers than the run-level retry budget must NOT retry
    them all — bounding total added time regardless of how many tickers
    the guard fires on (a per-call cap that says nothing about call count
    is the defect class this fleet has already paid for)."""
    n_tickers = _prices._SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN + 5
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    objects = {
        f"reference/price_cache/{t}.parquet": _parquet_bytes(_ohlcv(500)) for t in tickers
    }
    s3 = _FakeS3(objects)
    _patch_download(monkeypatch, _ohlcv(1))  # every ticker's batch answer is short

    retries: dict[str, int] = {}
    refreshed, failed, written = _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", tickers, "10y", 50,
        trading_day="2026-09-14", short_fetch_retries=retries,
    )

    assert refreshed == 0
    assert sorted(failed) == sorted(tickers), "every ticker still ends up failed, budget or not"
    retried = [t for t, n in retries.items() if n > 0]
    assert len(retried) == _prices._SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN, (
        "only the budgeted number of DISTINCT tickers may enter the retry path"
    )


def test_worst_case_added_sleep_time_is_bounded(monkeypatch):
    """Names the number the PR body cites: worst case is
    MAX_TICKERS_PER_RUN tickers each exhausting every backoff step."""
    slept: list[float] = []
    monkeypatch.setattr(_prices, "_sleep_seconds", lambda seconds: slept.append(seconds))

    n_tickers = _prices._SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN + 3
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    objects = {
        f"reference/price_cache/{t}.parquet": _parquet_bytes(_ohlcv(500)) for t in tickers
    }
    s3 = _FakeS3(objects)
    _patch_download(monkeypatch, _ohlcv(1))

    _prices._refresh_stale(
        s3, "alpha-engine-research", "predictor/price_cache/", tickers, "10y", 50,
        trading_day="2026-09-14",
    )

    worst_case_backoff_seconds = sum(_prices._SHORT_FETCH_RETRY_BACKOFF_SECONDS)
    # +25% jitter ceiling per call, per the retry helper's `uniform(0.75, 1.25)`
    bound = _prices._SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN * worst_case_backoff_seconds * 1.25
    assert sum(slept) <= bound, "total sleep must stay within the documented worst case"
    assert len(slept) <= (
        _prices._SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN * _prices._SHORT_FETCH_RETRY_ATTEMPTS
    )


def test_a_non_transient_caret_ticker_error_is_never_retried(monkeypatch):
    """`CaretTickerError` (alpha-engine-config-I10904) is a run-level
    contract violation, not a per-ticker miss — it must propagate exactly
    as before this change, and the retry helper must never be invoked for
    it (retrying a malformed ticker literal cannot make it valid)."""
    called = []
    monkeypatch.setattr(
        _prices, "_retry_short_fetch_ticker",
        lambda *a, **k: called.append(1) or (None, 0),
    )
    _patch_download(monkeypatch, _ohlcv(2500))  # long enough to clear the guard

    s3 = _FakeS3({})
    with pytest.raises(_prices.CaretTickerError):
        _prices._refresh_stale(
            s3, "alpha-engine-research", "predictor/price_cache/", ["^BADTICK"], "10y", 50,
            trading_day="2026-09-14",
        )
    assert called == [], "a non-transient contract violation must never enter the retry path"
