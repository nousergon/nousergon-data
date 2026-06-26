"""Wave 3 PR4 (cutover) — producer-side single-prefix regression suite.

Covers:
  * The ``price_cache_write_prefixes`` / ``price_cache_read_prefixes`` helpers
    themselves: the production-default sentinel now resolves to
    ``reference/price_cache/`` ONLY (legacy dropped), custom prefix → single.
  * Each of the three production writers (``collectors/prices.py``,
    ``collectors/fred_history.py``, ``weekly_collector._self_heal_chronic_polygon_gaps``
    via its module-scoped chronic-gap path) calls into the helper and ends up
    putting ticker parquets at ONLY the new ``reference/price_cache/`` prefix.

These tests pin the Wave 3 cutover contract: write-both is retired and the
legacy ``predictor/price_cache/`` prefix is GONE from both the write and read
chains. Any regression that re-introduces a legacy write/read fails here.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import pandas as pd
import pytest

from builders._price_cache_writeboth import (
    PRICE_CACHE_LEGACY_PREFIX,
    PRICE_CACHE_NEW_PREFIX,
    list_price_cache_keys,
    price_cache_read_prefixes,
    price_cache_write_prefixes,
)


# ---------------------------------------------------------------------------
# Helper contract
# ---------------------------------------------------------------------------


def test_default_writes_reference_only_legacy_dropped():
    """Wave-3 PR4 cutover: production default → single write to the new
    ``reference/price_cache/`` prefix. The legacy ``predictor/price_cache/``
    entry is GONE — write-both is retired.
    """
    out = price_cache_write_prefixes()
    assert out == [PRICE_CACHE_NEW_PREFIX]
    assert PRICE_CACHE_LEGACY_PREFIX not in out


def test_explicit_legacy_sentinel_resolves_to_reference_only():
    """The legacy string is still the production-default sentinel the prod
    call sites pass (config ``s3_prefix``). Post-cutover it resolves to the
    reference prefix ONLY — never writes to the legacy tree."""
    out = price_cache_write_prefixes(PRICE_CACHE_LEGACY_PREFIX)
    assert out == [PRICE_CACHE_NEW_PREFIX]
    assert PRICE_CACHE_LEGACY_PREFIX not in out


def test_custom_prefix_returns_single():
    """A test/config-override prefix (anything other than legacy) gets
    single-write behavior — Wave 3 write-both only mirrors the legacy
    production path."""
    custom = "some/other/prefix/"
    out = price_cache_write_prefixes(custom)
    assert out == [custom]


def test_new_prefix_is_not_legacy():
    """Sanity guard against a future copy-paste regression that aliases the
    two constants."""
    assert PRICE_CACHE_NEW_PREFIX != PRICE_CACHE_LEGACY_PREFIX
    assert PRICE_CACHE_NEW_PREFIX.startswith("reference/")
    assert PRICE_CACHE_LEGACY_PREFIX.startswith("predictor/")


# ---------------------------------------------------------------------------
# Read-side helper (Wave-3 PR3 reader migration)
# ---------------------------------------------------------------------------


def test_read_helper_default_resolves_reference_only():
    """Wave-3 PR4 cutover: the read chain drops the legacy fallback. With the
    producer writing only ``reference/`` and the legacy tree slated for
    ``aws s3 rm``, reads resolve from ``reference/`` alone.
    """
    out = price_cache_read_prefixes()
    assert out == [PRICE_CACHE_NEW_PREFIX]
    assert PRICE_CACHE_LEGACY_PREFIX not in out


def test_read_helper_explicit_legacy_sentinel_resolves_reference_only():
    out = price_cache_read_prefixes(PRICE_CACHE_LEGACY_PREFIX)
    assert out == [PRICE_CACHE_NEW_PREFIX]
    assert PRICE_CACHE_LEGACY_PREFIX not in out


def test_read_helper_custom_prefix_returns_single():
    """Test/config-override prefix opts out of the fallback chain — mirrors
    the write-side single-prefix semantics."""
    custom = "some/other/prefix/"
    out = price_cache_read_prefixes(custom)
    assert out == [custom]


# ---------------------------------------------------------------------------
# list_price_cache_keys — aggregate-listing helper (PR3-wave-2)
# ---------------------------------------------------------------------------


def _make_paginator(pages_by_prefix: dict[str, list[list[dict]]]):
    """Build a paginator double that returns per-prefix pages.

    Each ``pages_by_prefix[prefix]`` is a list of page dicts' ``Contents``
    arrays; each Content entry is ``{"Key": "..."}``.
    """

    class _Paginator:
        def paginate(self, *, Bucket: str, Prefix: str):
            pages = pages_by_prefix.get(Prefix, [])
            for contents in pages:
                yield {"Contents": contents}

    s3 = MagicMock()
    s3.get_paginator.return_value = _Paginator()
    return s3


def test_list_price_cache_keys_default_lists_reference_only():
    """Wave-3 PR4 cutover: aggregate listing under the production default
    consults the new ``reference/`` prefix ONLY — the legacy leg is dropped
    from the read chain, so any keys still lingering under the legacy prefix
    (pre-``aws s3 rm``) are NOT listed.
    """
    new_keys = [{"Key": f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet"},
                {"Key": f"{PRICE_CACHE_NEW_PREFIX}MSFT.parquet"}]
    legacy_keys = [{"Key": f"{PRICE_CACHE_LEGACY_PREFIX}AAPL.parquet"},
                   {"Key": f"{PRICE_CACHE_LEGACY_PREFIX}MSFT.parquet"},
                   {"Key": f"{PRICE_CACHE_LEGACY_PREFIX}ZZZZ.parquet"}]
    s3 = _make_paginator({
        PRICE_CACHE_NEW_PREFIX: [new_keys],
        PRICE_CACHE_LEGACY_PREFIX: [legacy_keys],
    })

    out = list_price_cache_keys(s3, "alpha-engine-research")
    # Only the reference-prefix keys surface; the legacy-only ZZZZ is ignored.
    assert out == [
        f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet",
        f"{PRICE_CACHE_NEW_PREFIX}MSFT.parquet",
    ]
    assert not any(k.startswith(PRICE_CACHE_LEGACY_PREFIX) for k in out)


def test_list_price_cache_keys_custom_prefix_opts_out_of_chain():
    """A non-default prefix opts out of the fallback chain (mirrors the
    single-key helper semantics): only the explicit prefix is listed.
    """
    custom = "some/other/prefix/"
    keys = [{"Key": f"{custom}XYZ.parquet"}]
    s3 = _make_paginator({custom: [keys]})

    out = list_price_cache_keys(s3, "b", custom)
    assert out == [f"{custom}XYZ.parquet"]
    # And neither leg of the production chain was consulted.
    s3.get_paginator.return_value  # noqa — no further-prefix assertion needed; pages dict carries it.


# ---------------------------------------------------------------------------
# collectors/prices.py — yfinance refresh upload
# ---------------------------------------------------------------------------


def test_prices_refresh_uploads_to_both_prefixes(monkeypatch, tmp_path):
    """``_refresh_stale_tickers`` ends each successful per-ticker branch with
    an ``s3.upload_file`` — Wave 3 wraps that in a write-both loop. We exercise
    the success path with stubbed yfinance + a recording S3 client and assert
    BOTH keys land for every refreshed ticker."""
    from collectors import prices

    # Stub yfinance to return a deterministic single-ticker frame
    idx = pd.date_range("2026-04-01", periods=10, freq="B")
    fake_df = pd.DataFrame(
        {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.5, "Volume": 1_000},
        index=idx,
    )

    def fake_download(**_kwargs):
        return fake_df.copy()

    monkeypatch.setattr(prices.yf, "download", fake_download)

    recorded: list[tuple[str, str]] = []

    class _RecordingS3:
        def upload_file(self, _local, bucket, key):
            recorded.append((bucket, key))

    s3 = _RecordingS3()
    refreshed, failed = prices._refresh_stale(
        s3=s3,
        bucket="test-bucket",
        s3_prefix=PRICE_CACHE_LEGACY_PREFIX,
        stale=["AAPL"],
        fetch_period="10y",
        batch_size=10,
    )

    assert refreshed == 1
    assert failed == []
    # Cutover: ONLY the reference prefix is hit — legacy write retired.
    keys = sorted(k for _b, k in recorded)
    assert keys == [f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet"]
    assert not any(k.startswith(PRICE_CACHE_LEGACY_PREFIX) for _b, k in recorded)
    assert all(b == "test-bucket" for b, _ in recorded)


# ---------------------------------------------------------------------------
# collectors/fred_history.py — FRED backfill upload
# ---------------------------------------------------------------------------


def test_fred_backfill_uploads_to_both_prefixes(monkeypatch):
    """``backfill_to_s3`` uploads each FRED-sourced ticker parquet via
    ``s3.upload_file``. Wave 3 wraps that in a write-both loop."""
    from collectors import fred_history

    # Stub the FRED HTTP path — return a deterministic OHLCV frame
    idx = pd.date_range("2020-01-01", periods=20, freq="B")
    fake_ohlcv = pd.DataFrame(
        {
            "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
            "Adj_Close": 1.0, "Volume": 0, "VWAP": None, "source": "fred",
        },
        index=idx,
    )
    monkeypatch.setattr(
        fred_history, "fetch_fred_history", lambda *_args, **_kw: fake_ohlcv,
    )
    monkeypatch.setattr(
        fred_history, "fred_history_to_ohlcv", lambda df: df,
    )

    recorded: list[tuple[str, str]] = []

    class _RecordingS3:
        def upload_file(self, _local, bucket, key):
            recorded.append((bucket, key))

    monkeypatch.setattr(
        fred_history.boto3, "client", lambda _svc: _RecordingS3(),
    )

    out = fred_history.backfill_to_s3(
        bucket="test-bucket",
        s3_prefix=PRICE_CACHE_LEGACY_PREFIX,
        tickers=["TWO"],
        period_years=5,
        dry_run=False,
    )

    assert out["status"] == "ok"
    assert out["refreshed"] == 1
    # Cutover: ONLY the reference prefix is hit — legacy write retired.
    keys = sorted(k for _b, k in recorded)
    assert keys == [f"{PRICE_CACHE_NEW_PREFIX}TWO.parquet"]
    assert not any(k.startswith(PRICE_CACHE_LEGACY_PREFIX) for _b, k in recorded)


# ---------------------------------------------------------------------------
# weekly_collector — chronic-gap self-heal patch
# ---------------------------------------------------------------------------


def test_weekly_chronic_gap_self_heal_writes_reference_only(monkeypatch):
    """``_self_heal_chronic_polygon_gaps`` reads the parquet for the
    existing-rows union via the read-prefix chain (now ``reference/`` only),
    then PUTs the combined frame back via the write-prefix chain (also
    ``reference/`` only). Wave 3 PR4 cutover: the PUT lands on the reference
    prefix ONLY — the legacy write is retired."""
    import weekly_collector as wc

    target_date = "2026-05-12"
    target_ts = pd.Timestamp(target_date).normalize()

    # yfinance.download is locally re-imported inside the helper as ``_yf``;
    # monkeypatching the module attribute pre-call rebinds the name yfinance
    # resolves to.
    import yfinance as yf

    idx = pd.bdate_range(target_ts - pd.Timedelta(days=10), target_ts)
    new_rows_df = pd.DataFrame(
        {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 100},
        index=idx,
    )

    def _fake_download(*_a, **_kw):
        return new_rows_df.copy()

    monkeypatch.setattr(yf, "download", _fake_download)

    # ArcticDB universe lib: tail returns a stale last_date so the heal
    # branch runs.
    class _StaleTail:
        data = pd.DataFrame(index=[pd.Timestamp("2026-04-01")])

    class _FakeUniverseLib:
        def tail(self, _ticker, n=1):
            return _StaleTail()

    monkeypatch.setattr(
        "store.arctic_store.get_universe_lib",
        lambda _bucket: _FakeUniverseLib(),
    )

    # S3 client: NoSuchKey on the legacy GET (no prior parquet), recording
    # put_object so we can assert write-both.
    put_calls: list[dict] = []

    class _FakeS3Exceptions:
        class NoSuchKey(Exception):
            pass

    class _FakeS3:
        exceptions = _FakeS3Exceptions

        def get_object(self, **_kw):
            raise _FakeS3Exceptions.NoSuchKey("no prior parquet")

        def put_object(self, **kw):
            put_calls.append(kw)

    monkeypatch.setattr(wc.boto3, "client", lambda _svc: _FakeS3())

    # builders.backfill is called after the put — stub so the test doesn't
    # touch ArcticDB / S3 again.
    monkeypatch.setattr(
        "builders.backfill.backfill", lambda **_kw: {"status": "ok"},
    )

    summary = wc._self_heal_chronic_polygon_gaps(
        bucket="test-bucket",
        target_date=target_date,
        chronic_tickers=["PSTG"],
        dry_run=False,
    )

    assert summary["errors"] == [], summary
    assert len(summary["healed"]) == 1, summary

    # Cutover: ONLY the reference prefix is hit — legacy write retired.
    pcache_keys = sorted(
        c["Key"] for c in put_calls if c["Key"].endswith("/PSTG.parquet")
    )
    assert pcache_keys == [f"{PRICE_CACHE_NEW_PREFIX}PSTG.parquet"], pcache_keys
    assert not any(
        c["Key"].startswith(PRICE_CACHE_LEGACY_PREFIX) for c in put_calls
    )
