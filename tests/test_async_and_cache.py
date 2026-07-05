"""Tests for the async aggregator + S3 TTL cache (Wave 1 PR D).

Covers:
  - S3TtlCache: get/set round-trip, expired returns None, JSON helpers,
    metadata stamping, idempotent overwrite, get_json malformed → None
  - cached_call + cached_acall: cache hit skips compute; cache miss
    calls compute + writes
  - AsyncNewsAggregator: parallel fan-in / cache hit-miss / retry
    on transient failure / final-failure isolated / semaphore caps
    concurrency
"""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock

import anyio
import pytest

from nousergon_lib.sources import NewsArticle

from collectors.news_aggregator_async import (
    AsyncNewsAggregator,
    _cache_key,
)
from data.cache import S3TtlCache, _hash_key


# ── In-memory S3 mock with metadata support ────────────────────────────


class _InMemoryS3:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], dict] = {}

    def put_object(self, *, Bucket, Key, Body, Metadata=None, ContentType=None):
        self.store[(Bucket, Key)] = {
            "Body": Body,
            "Metadata": dict(Metadata or {}),
        }
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        entry = self.store.get((Bucket, Key))
        if entry is None:
            raise RuntimeError("NoSuchKey")
        return {
            "Body": BytesIO(entry["Body"]),
            "Metadata": entry["Metadata"],
        }


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Cache helpers ──────────────────────────────────────────────────────


class TestCacheHelpers:
    def test_hash_key_deterministic(self):
        assert _hash_key("foo") == _hash_key("foo")
        assert _hash_key("foo") != _hash_key("bar")

    def test_hash_key_handles_special_chars(self):
        # Slashes, spaces, query strings — all valid input
        h = _hash_key("news/polygon/AAPL,MSFT?hours=48")
        assert isinstance(h, str)
        assert "/" not in h  # output is hex


# ── S3TtlCache ─────────────────────────────────────────────────────────


class TestS3TtlCache:
    def test_set_then_get_round_trip(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        cache.set("k1", b"hello world", ttl_seconds=60)
        assert cache.get("k1") == b"hello world"

    def test_get_missing_returns_none(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        assert cache.get("missing") is None

    def test_expired_returns_none(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        # Manually inject an expired entry
        past = (_now() - timedelta(seconds=10))
        s3.put_object(
            Bucket="b",
            Key=f"cache/{_hash_key('k')}.bin",
            Body=b"x",
            Metadata={
                "cache-key": "k",
                "cache-cached-at": (past - timedelta(seconds=60)).isoformat().replace("+00:00", "Z"),
                "cache-ttl-seconds": "30",
                "cache-expires-at": past.isoformat().replace("+00:00", "Z"),
            },
        )
        assert cache.get("k") is None

    def test_entry_without_metadata_treated_as_expired(self):
        """Old entries (or external writes) without our metadata stamp
        are treated as expired so we don't return stale-or-garbled
        content."""
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        s3.put_object(
            Bucket="b", Key=f"cache/{_hash_key('k')}.bin",
            Body=b"orphan", Metadata={},
        )
        assert cache.get("k") is None

    def test_overwrite_extends_ttl(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        cache.set("k", b"v1", ttl_seconds=10)
        cache.set("k", b"v2", ttl_seconds=100)
        assert cache.get("k") == b"v2"

    def test_default_ttl_applied(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(
            s3, bucket="b", prefix="cache", default_ttl_seconds=300,
        )
        cache.set("k", b"v")
        # Inspect the stored metadata
        entry = s3.store[("b", f"cache/{_hash_key('k')}.bin")]
        assert entry["Metadata"]["cache-ttl-seconds"] == "300"

    def test_get_json_roundtrip(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        cache.set_json("k", {"foo": "bar", "n": 42}, ttl_seconds=60)
        assert cache.get_json("k") == {"foo": "bar", "n": 42}

    def test_get_json_malformed_returns_none(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        cache.set("k", b"not-valid-json{", ttl_seconds=60)
        assert cache.get_json("k") is None


class TestCachedCall:
    def test_miss_calls_compute_and_caches(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        call_count = {"n": 0}

        def compute():
            call_count["n"] += 1
            return b"computed value"

        # First call → miss → compute called
        v1 = cache.cached_call("k", compute_fn=compute, ttl_seconds=60)
        assert v1 == b"computed value"
        assert call_count["n"] == 1

        # Second call → hit → compute NOT called
        v2 = cache.cached_call("k", compute_fn=compute, ttl_seconds=60)
        assert v2 == b"computed value"
        assert call_count["n"] == 1  # unchanged

    def test_acall_miss_calls_compute_and_caches(self):
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        call_count = {"n": 0}

        async def acompute():
            call_count["n"] += 1
            return b"async value"

        async def run():
            v1 = await cache.cached_acall(
                "ak", async_compute_fn=acompute, ttl_seconds=60,
            )
            v2 = await cache.cached_acall(
                "ak", async_compute_fn=acompute, ttl_seconds=60,
            )
            return v1, v2

        v1, v2 = asyncio.run(run())
        assert v1 == v2 == b"async value"
        assert call_count["n"] == 1


# ── Cache key generation ──────────────────────────────────────────────


class TestAsyncCacheKey:
    def test_stable_under_ticker_reorder(self):
        k1 = _cache_key("polygon", ["AAPL", "MSFT"], 48)
        k2 = _cache_key("polygon", ["MSFT", "AAPL"], 48)
        assert k1 == k2

    def test_changes_with_hours(self):
        assert (
            _cache_key("polygon", ["AAPL"], 48)
            != _cache_key("polygon", ["AAPL"], 24)
        )

    def test_changes_with_vendor(self):
        assert (
            _cache_key("polygon", ["AAPL"], 48)
            != _cache_key("gdelt", ["AAPL"], 48)
        )


# ── AsyncNewsAggregator ───────────────────────────────────────────────


def _article(source: str = "polygon", title: str = "T") -> NewsArticle:
    return NewsArticle(
        tickers=("AAPL",),
        title=title,
        body_excerpt="body",
        url=f"https://x.com/{source}/{title}",
        published_at=_now(),
        source=source,
        fetched_at=_now(),
    )


class _StaticSource:
    """Sync adapter conforming to NewsSource Protocol."""
    def __init__(self, name: str, articles: list[NewsArticle]) -> None:
        self.name = name
        self._articles = articles
        self.call_count = 0
        self.last_kwargs: dict | None = None

    def fetch(self, tickers, *, hours=48):
        self.call_count += 1
        self.last_kwargs = {"tickers": tickers, "hours": hours}
        return list(self._articles)


class _TransientFailingSource:
    """Adapter that raises N times then succeeds."""
    def __init__(self, name: str, fail_n_times: int, then_return: list) -> None:
        self.name = name
        self._fail_remaining = fail_n_times
        self._then = then_return
        self.call_count = 0

    def fetch(self, tickers, *, hours=48):
        self.call_count += 1
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise RuntimeError("transient")
        return list(self._then)


class TestAsyncAggregatorFanIn:
    def test_parallel_fan_in_combines_sources(self):
        polygon = _StaticSource("polygon", [_article("polygon", "A")])
        gdelt = _StaticSource("gdelt", [_article("gdelt", "B")])
        agg = AsyncNewsAggregator(sources=[polygon, gdelt])

        async def run():
            return await agg.fetch(["AAPL"])

        out = asyncio.run(run())
        assert {a.canonical_title for a in out} == {"A", "B"}
        assert polygon.call_count == 1
        assert gdelt.call_count == 1

    def test_per_source_failure_isolated(self):
        good = _StaticSource("polygon", [_article("polygon", "A")])

        class _AlwaysBroken:
            name = "broken"
            call_count = 0

            def fetch(self, tickers, *, hours=48):
                _AlwaysBroken.call_count += 1
                raise RuntimeError("permanent")

        agg = AsyncNewsAggregator(
            sources=[good, _AlwaysBroken()],
            # Use 1 retry attempt to keep the test fast
            max_retry_attempts=1,
            retry_initial_wait=0.0,
        )

        async def run():
            return await agg.fetch(["AAPL"])

        out = asyncio.run(run())
        # Good source's article still surfaces
        assert len(out) == 1
        assert out[0].canonical_title == "A"

    def test_retry_recovers_from_transient_failure(self):
        # Fail 2 times, succeed on 3rd attempt
        flaky = _TransientFailingSource(
            "polygon", fail_n_times=2,
            then_return=[_article("polygon", "OK")],
        )
        agg = AsyncNewsAggregator(
            sources=[flaky],
            max_retry_attempts=3,
            retry_initial_wait=0.01,  # fast for tests
        )

        async def run():
            return await agg.fetch(["AAPL"])

        out = asyncio.run(run())
        assert len(out) == 1
        assert out[0].canonical_title == "OK"
        assert flaky.call_count == 3  # 2 failures + 1 success

    def test_retry_exhausted_returns_empty(self):
        flaky = _TransientFailingSource(
            "polygon", fail_n_times=100,  # never succeeds
            then_return=[],
        )
        agg = AsyncNewsAggregator(
            sources=[flaky], max_retry_attempts=2,
            retry_initial_wait=0.01,
        )

        async def run():
            return await agg.fetch(["AAPL"])

        out = asyncio.run(run())
        assert out == []
        assert flaky.call_count == 2  # exactly max_retry_attempts


class TestAsyncAggregatorCaching:
    def test_cache_hit_skips_adapter_call(self):
        polygon = _StaticSource("polygon", [_article("polygon", "A")])
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        agg = AsyncNewsAggregator(
            sources=[polygon], cache=cache,
        )

        async def run():
            await agg.fetch(["AAPL"])  # cold — calls adapter
            await agg.fetch(["AAPL"])  # warm — cache hit
            return polygon.call_count

        n = asyncio.run(run())
        assert n == 1  # adapter called once across two fan-ins

    def test_cache_miss_after_expiry_re_runs_adapter(self):
        polygon = _StaticSource("polygon", [_article("polygon", "A")])
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        # Override per-source TTL to 0 so cache always expires
        agg = AsyncNewsAggregator(
            sources=[polygon], cache=cache,
            per_source_ttl_seconds={"polygon": 0},
        )

        async def run():
            await agg.fetch(["AAPL"])
            await agg.fetch(["AAPL"])
            return polygon.call_count

        n = asyncio.run(run())
        assert n == 2  # both fan-ins hit the adapter

    def test_cache_isolated_per_tickers(self):
        polygon = _StaticSource("polygon", [_article("polygon", "A")])
        s3 = _InMemoryS3()
        cache = S3TtlCache(s3, bucket="b", prefix="cache")
        agg = AsyncNewsAggregator(sources=[polygon], cache=cache)

        async def run():
            await agg.fetch(["AAPL"])
            await agg.fetch(["MSFT"])  # different tickers — separate key
            return polygon.call_count

        assert asyncio.run(run()) == 2


class TestSemaphoreConcurrency:
    def test_semaphore_caps_in_flight_calls(self):
        """With concurrency=1, two ticker-batches against the same
        source serialize. Track 'currently in flight' via a counter
        and assert it never exceeds 1."""
        in_flight = {"n": 0, "max": 0}
        lock = threading.Lock()

        class _SlowSource:
            name = "polygon"

            def fetch(self, tickers, *, hours=48):
                with lock:
                    in_flight["n"] += 1
                    in_flight["max"] = max(in_flight["max"], in_flight["n"])
                import time
                time.sleep(0.05)
                with lock:
                    in_flight["n"] -= 1
                return [_article("polygon", "T")]

        # Two adapters sharing the same `name` so they share a semaphore
        s1, s2 = _SlowSource(), _SlowSource()
        agg = AsyncNewsAggregator(
            sources=[s1, s2],
            per_source_concurrency={"polygon": 1},
        )

        async def run():
            return await agg.fetch(["AAPL"])

        asyncio.run(run())
        # Even though 2 adapters fan out in parallel, semaphore caps
        # concurrent in-flight calls to 1
        assert in_flight["max"] == 1
