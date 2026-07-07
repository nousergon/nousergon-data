"""Async news aggregator — anyio-based parallel fan-in with per-vendor
rate limits + tenacity retry.

Wave 1 PR D of the institutional data-revamp arc.

Wraps the sync :class:`NewsSource` adapters (defined in alpha-engine-lib
sources Protocols, implemented in ``collectors/news_sources/``) in an
async fan-in pattern using ``anyio.to_thread.run_sync`` — adapters stay
synchronous (existing tests + Lambda dispatch unchanged); aggregation
becomes concurrent.

Production design:

  - **Per-vendor concurrency limit** via ``anyio.Semaphore``. Polygon's
    free tier is 5 req/min; setting concurrency=2 keeps us comfortably
    under the limit. GDELT has no documented rate cap but we hold
    concurrency=1 + adapter-side 1s sleep per ticker. Per-vendor knobs
    in the constructor so each vendor's posture is independent.

  - **Tenacity retry per vendor** with exponential backoff. Wraps the
    individual ``adapter.fetch(tickers, hours)`` call. Up to 3 attempts
    with 2s/4s/8s backoff. Permanent failures (auth, config) raise on
    final attempt; transient (timeouts, 5xx) absorbed.

  - **Optional cache** via :class:`data.cache.S3TtlCache`. When passed,
    each adapter's output is cached under
    ``cache_key="news/{vendor}/{sorted-tickers-hash}/{hours}"`` for a
    configurable TTL. Default 1h — agents calling within the same
    session see the same fan-in result without redundant vendor hits.

  - **Reuses sync aggregator's dedup + trust weighting** —
    :class:`NewsAggregator._dedup` and ``DEFAULT_TRUST_WEIGHTS`` are
    composed verbatim. Same canonical output shape
    (``AggregatedNewsArticle``); same downstream consumers.

Reuse pattern: the existing sync :class:`NewsAggregator.fetch` stays
in place for callers that don't have an event loop. The async variant
is what the production fetch_data wiring (PR F) will call.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Iterable

import anyio
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from nousergon_lib.sources import NewsArticle, NewsSource

from collectors.news_aggregator import (
    AggregatedNewsArticle,
    DEFAULT_TRUST_WEIGHTS,
    NewsAggregator,
)
from data.cache import S3TtlCache

logger = logging.getLogger(__name__)


# Default concurrency per source — conservative free-tier safe defaults.
# Override per-deployment via the ``per_source_concurrency`` constructor
# arg.
DEFAULT_PER_SOURCE_CONCURRENCY: dict[str, int] = {
    "polygon": 2,     # Polygon free tier: 5 req/min — concurrency=2
                      # holds us under the limit alongside the inter-
                      # call rate limiter on PolygonClient
    "gdelt": 1,       # GDELT "be polite" guidance + 1s adapter sleep
    "yahoo_rss": 4,   # Yahoo RSS has no documented hard limit
    "edgar_press": 2, # EDGAR is 10 req/sec; concurrency=2 is safe
    "benzinga": 4,    # Paid tier when wired — assume higher quota
    "ravenpack": 4,   # Paid tier
    "bloomberg": 4,   # Paid tier
}


# Default per-source cache TTLs (seconds). News is fresh-sensitive so
# 1h is the typical default; some sources update less often.
DEFAULT_PER_SOURCE_TTL_SECONDS: dict[str, int] = {
    "polygon": 1_800,     # 30 min — Polygon news churns fast
    "gdelt": 3_600,       # 1h — GDELT batches ~15 min behind wall-clock
    "yahoo_rss": 3_600,   # 1h
    "edgar_press": 14_400,  # 4h — EDGAR is daily
    "benzinga": 1_800,
    "ravenpack": 1_800,
    "bloomberg": 1_800,
}


def _cache_key(vendor: str, tickers: list[str], hours: int) -> str:
    """Stable cache key for one (vendor, tickers, hours) fan-in call."""
    sorted_tickers = sorted(tickers)
    fingerprint = ",".join(sorted_tickers)
    h = hashlib.sha1(
        f"news|{vendor}|{fingerprint}|{hours}".encode("utf-8"),
    ).hexdigest()[:16]
    return f"news/{vendor}/{h}"


class AsyncNewsAggregator:
    """Async fan-in across NewsSource adapters with rate limits, retry,
    and optional cache.

    Args:
        sources: enabled NewsSource adapters.
        trust_weights: per-vendor trust weights (composes with the
                       sync aggregator's defaults).
        per_source_concurrency: max parallel ``fetch()`` calls per
                                vendor name. Defaults to
                                :data:`DEFAULT_PER_SOURCE_CONCURRENCY`.
        cache: optional :class:`S3TtlCache`. When set, per-vendor
               outputs are cached.
        per_source_ttl_seconds: TTL per vendor when caching. Defaults
                                to :data:`DEFAULT_PER_SOURCE_TTL_SECONDS`.
        max_retry_attempts: total adapter call attempts (1 = no retry).
                            Default 3.
        retry_initial_wait: seconds for first retry backoff (doubled
                            each subsequent attempt). Default 2.
    """

    def __init__(
        self,
        sources: Iterable[NewsSource],
        *,
        trust_weights: dict[str, float] | None = None,
        per_source_concurrency: dict[str, int] | None = None,
        cache: S3TtlCache | None = None,
        per_source_ttl_seconds: dict[str, int] | None = None,
        max_retry_attempts: int = 3,
        retry_initial_wait: float = 2.0,
    ) -> None:
        self._sources = tuple(sources)
        # Wrap a sync NewsAggregator so dedup + canonical-pick logic is
        # reused verbatim. Same output contract as the sync path.
        self._sync_aggregator = NewsAggregator(
            sources=[], trust_weights=trust_weights or DEFAULT_TRUST_WEIGHTS,
        )
        self._concurrency = dict(
            per_source_concurrency or DEFAULT_PER_SOURCE_CONCURRENCY,
        )
        self._semaphores: dict[str, anyio.Semaphore] = {}
        self._cache = cache
        self._ttl = dict(
            per_source_ttl_seconds or DEFAULT_PER_SOURCE_TTL_SECONDS,
        )
        self._max_retry_attempts = max_retry_attempts
        self._retry_initial_wait = retry_initial_wait

    @property
    def source_names(self) -> tuple[str, ...]:
        return tuple(s.name for s in self._sources)

    def trust_weight(self, source_name: str) -> float:
        """Per-source trust weight — delegates to the wrapped sync aggregator.

        Makes this class a drop-in for ``aggregate_and_write(aggregator=...)``
        (which looks up per-source trust weights when building the aggregates
        DataFrame). Same weights, same 0.5 fallback + warning as the sync path.
        """
        return self._sync_aggregator.trust_weight(source_name)

    def _semaphore_for(self, vendor: str) -> anyio.Semaphore:
        if vendor not in self._semaphores:
            limit = self._concurrency.get(vendor, 2)
            self._semaphores[vendor] = anyio.Semaphore(limit)
        return self._semaphores[vendor]

    async def fetch(
        self, tickers: list[str], *, hours: int = 48,
    ) -> list[AggregatedNewsArticle]:
        """Async fan-in across all enabled adapters. Returns the
        deduped + trust-weighted aggregated list.

        Per-adapter:
          1. Check cache (if configured)
          2. Run with semaphore + tenacity retry
          3. Write to cache (if configured)

        One adapter raising on final retry attempt logs + continues
        with the rest — defense-in-depth matching the sync path.
        """
        all_articles: list[NewsArticle] = []
        results: dict[str, list[NewsArticle]] = {}

        async def _run_one(source: NewsSource) -> None:
            try:
                articles = await self._fetch_one(source, tickers, hours)
            except Exception as e:
                logger.exception(
                    "[async_news_aggregator] %s raised on final retry: %s",
                    source.name, e,
                )
                articles = []
            results[source.name] = articles

        async with anyio.create_task_group() as tg:
            for source in self._sources:
                tg.start_soon(_run_one, source)

        for source_name in self.source_names:
            batch = results.get(source_name, [])
            logger.info(
                "[async_news_aggregator] %s returned %d articles",
                source_name, len(batch),
            )
            all_articles.extend(batch)

        if not all_articles:
            return []
        return self._sync_aggregator._dedup(all_articles)

    async def _fetch_one(
        self, source: NewsSource, tickers: list[str], hours: int,
    ) -> list[NewsArticle]:
        """Cache-aware, semaphore-gated, retry-wrapped per-source fetch."""
        cache_key = _cache_key(source.name, tickers, hours)
        ttl = self._ttl.get(source.name)

        # Cache check (sync — S3 get is fast)
        if self._cache is not None:
            cached_bytes = self._cache.get(cache_key)
            if cached_bytes is not None:
                try:
                    return [
                        NewsArticle(**row)
                        for row in json.loads(cached_bytes)
                    ]
                except Exception as e:
                    logger.warning(
                        "[async_news_aggregator] cache hit but "
                        "deserialization failed for %s: %s",
                        source.name, e,
                    )

        sem = self._semaphore_for(source.name)

        async def _call():
            async with sem:
                return await anyio.to_thread.run_sync(
                    source.fetch, tickers, lambda: hours,
                )

        # tenacity AsyncRetrying — wraps the call with exp backoff.
        # We retry on any non-RetryError exception. Authentication
        # errors propagate after final attempt (loud failure).
        articles: list[NewsArticle]
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self._max_retry_attempts),
                wait=wait_exponential(
                    multiplier=self._retry_initial_wait, min=1, max=30,
                ),
                retry=retry_if_exception_type(Exception),
                reraise=True,
            ):
                with attempt:
                    articles = await self._invoke_adapter(
                        source, tickers, hours, sem,
                    )
        except RetryError:
            return []

        # Cache write
        if self._cache is not None and articles:
            try:
                serialized = json.dumps(
                    [a.model_dump(mode="json") for a in articles],
                    default=str,
                ).encode("utf-8")
                self._cache.set(cache_key, serialized, ttl_seconds=ttl)
            except Exception as e:
                logger.warning(
                    "[async_news_aggregator] cache write failed for "
                    "%s: %s", source.name, e,
                )

        return articles

    async def _invoke_adapter(
        self,
        source: NewsSource,
        tickers: list[str],
        hours: int,
        sem: anyio.Semaphore,
    ) -> list[NewsArticle]:
        """Run one adapter under the per-vendor semaphore. Adapters are
        sync; offload to thread via anyio."""
        async with sem:
            def _call_with_hours():
                return source.fetch(tickers, hours=hours)
            return await anyio.to_thread.run_sync(_call_with_hours)
