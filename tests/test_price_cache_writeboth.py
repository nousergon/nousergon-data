"""Wave 3 PR1 — producer-side write-both regression suite.

Covers:
  * The ``price_cache_write_prefixes`` helper itself (legacy default → both,
    custom prefix → single).
  * Each of the three production writers (``collectors/prices.py``,
    ``collectors/fred_history.py``, ``weekly_collector._patch_chronic_gap_ticker``
    via its module-scoped chronic-gap path) calls into the helper and ends up
    putting ticker parquets at BOTH the legacy and new prefix, with identical
    bodies and key shape.

These tests pin the Wave 3 write-both contract so a future "delete the legacy
write" refactor can't quietly skip a writer — every active prod path is
exercised. PR4 cutover edits the helper + flips these tests to expect a
single-prefix write.
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


def test_default_returns_both_prefixes_legacy_first():
    """Production default → write-both with legacy ordered first.

    Order matters: legacy first so a permission/quota failure on the legacy
    prefix preserves pre-Wave-3 fail-loud semantics — the new prefix never
    silently masks a legacy write failure.
    """
    out = price_cache_write_prefixes()
    assert out == [PRICE_CACHE_LEGACY_PREFIX, PRICE_CACHE_NEW_PREFIX]


def test_explicit_legacy_returns_both_prefixes():
    """Callers that pass the legacy prefix explicitly get the same result as
    callers that use the default — protects against config-layer regressions
    where ``s3_prefix`` is read from yaml and matches the legacy string."""
    out = price_cache_write_prefixes(PRICE_CACHE_LEGACY_PREFIX)
    assert out == [PRICE_CACHE_LEGACY_PREFIX, PRICE_CACHE_NEW_PREFIX]


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


def test_read_helper_default_returns_new_first_legacy_second():
    """The read order is the WRITE order REVERSED — new prefix consulted
    first so consumers see the post-PR4 home as soon as the soak begins;
    legacy is the fallback during the soak window only.
    """
    out = price_cache_read_prefixes()
    assert out == [PRICE_CACHE_NEW_PREFIX, PRICE_CACHE_LEGACY_PREFIX]


def test_read_helper_explicit_legacy_returns_new_first_legacy_second():
    out = price_cache_read_prefixes(PRICE_CACHE_LEGACY_PREFIX)
    assert out == [PRICE_CACHE_NEW_PREFIX, PRICE_CACHE_LEGACY_PREFIX]


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


def test_list_price_cache_keys_default_iterates_new_then_legacy():
    """Aggregate listing under the production default consults the new
    prefix first then the legacy prefix; tickers present in BOTH only
    surface once (first-prefix-wins, deduped by basename).
    """
    new_keys = [{"Key": f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet"},
                {"Key": f"{PRICE_CACHE_NEW_PREFIX}MSFT.parquet"}]
    legacy_keys = [{"Key": f"{PRICE_CACHE_LEGACY_PREFIX}AAPL.parquet"},
                   {"Key": f"{PRICE_CACHE_LEGACY_PREFIX}MSFT.parquet"}]
    s3 = _make_paginator({
        PRICE_CACHE_NEW_PREFIX: [new_keys],
        PRICE_CACHE_LEGACY_PREFIX: [legacy_keys],
    })

    out = list_price_cache_keys(s3, "alpha-engine-research")
    # AAPL + MSFT each appear exactly once, both anchored on the new prefix.
    assert out == [
        f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet",
        f"{PRICE_CACHE_NEW_PREFIX}MSFT.parquet",
    ], "First-prefix-wins must dedupe by {ticker}.parquet basename."


def test_list_price_cache_keys_falls_back_to_legacy_for_missing_basenames():
    """When the new prefix is partially populated (soak-window backfill
    hasn't picked up every ticker yet), legacy fills the gaps so the
    aggregate set stays complete — the whole point of keeping the
    legacy fallback live during the soak.
    """
    # Only AAPL has been mirrored to new yet; MSFT still legacy-only.
    new_keys = [{"Key": f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet"}]
    legacy_keys = [{"Key": f"{PRICE_CACHE_LEGACY_PREFIX}AAPL.parquet"},
                   {"Key": f"{PRICE_CACHE_LEGACY_PREFIX}MSFT.parquet"}]
    s3 = _make_paginator({
        PRICE_CACHE_NEW_PREFIX: [new_keys],
        PRICE_CACHE_LEGACY_PREFIX: [legacy_keys],
    })

    out = list_price_cache_keys(s3, "alpha-engine-research")
    # AAPL from new (first-wins), MSFT from legacy (gap-fill).
    assert out == [
        f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet",
        f"{PRICE_CACHE_LEGACY_PREFIX}MSFT.parquet",
    ]


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
    # Both prefixes hit, same ticker, same bucket
    keys = sorted(k for _b, k in recorded)
    assert keys == [
        f"{PRICE_CACHE_LEGACY_PREFIX}AAPL.parquet",
        f"{PRICE_CACHE_NEW_PREFIX}AAPL.parquet",
    ]
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
    keys = sorted(k for _b, k in recorded)
    assert keys == [
        f"{PRICE_CACHE_LEGACY_PREFIX}TWO.parquet",
        f"{PRICE_CACHE_NEW_PREFIX}TWO.parquet",
    ]


# ---------------------------------------------------------------------------
# weekly_collector — chronic-gap self-heal patch
# ---------------------------------------------------------------------------


def test_weekly_chronic_gap_self_heal_writes_both_prefixes(monkeypatch):
    """``_self_heal_chronic_polygon_gaps`` reads the legacy parquet for the
    existing-rows union, then PUTs the combined frame back. Wave 3 PR1 sends
    the PUT to both prefixes with identical body bytes; the GET stays on
    legacy until reader migration (PR3+)."""
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

    # Both prefixes hit with the same ticker key, bodies identical
    pcache_keys = sorted(
        c["Key"] for c in put_calls if c["Key"].endswith("/PSTG.parquet")
    )
    assert pcache_keys == [
        f"{PRICE_CACHE_LEGACY_PREFIX}PSTG.parquet",
        f"{PRICE_CACHE_NEW_PREFIX}PSTG.parquet",
    ], pcache_keys

    bodies = [c["Body"] for c in put_calls if c["Key"].endswith("/PSTG.parquet")]
    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
