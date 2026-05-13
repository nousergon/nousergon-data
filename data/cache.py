"""S3-backed TTL cache for producer-side fetchers.

Wave 1 PR D of the institutional data-revamp arc (plan doc:
``~/Development/alpha-engine-docs/private/data-revamp-260513.md``).

Generic cache utility used by news / analyst / filings fetchers to
avoid re-hitting upstream vendor APIs within a session. Idempotent:
the same key + within-TTL window returns the cached body without
calling the upstream.

Storage layout::

    s3://{bucket}/{prefix}/{key_hash}.bin

Each cached object carries S3 metadata:

    cache-key            : original cache key (for debugging)
    cache-cached-at      : ISO-8601 UTC timestamp at write time
    cache-ttl-seconds    : integer TTL applied
    cache-expires-at     : ISO-8601 UTC timestamp at which the entry expires

The key is sha1-hashed for S3-key-safe encoding; the original key is
retained in metadata for grep-from-AWS-console debugging.

Why S3-backed (vs in-memory):

- Process boundary — Lambda invocations + spot-instance restarts both
  pick up the cache from a fresh process. In-memory dict caches lose
  on cold start.
- Cross-runtime — the news Lambda + the Saturday SF spot can share a
  cache for the same ticker's news without independent reaches.
- Cheap: S3 PUT/GET on tiny cache bodies dominates over the avoided
  vendor API call.

Configurable per-source TTL: news fetches typically 1-6h; analyst
24h; filings 24h. Caller passes the TTL at .get/.set time, so a
single cache instance serves multiple source types.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/_cache"
DEFAULT_TTL_SECONDS = 3_600  # 1 hour


@dataclass(frozen=True)
class _CacheMetadata:
    cache_key: str
    cached_at: datetime
    ttl_seconds: int
    expires_at: datetime

    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at


def _hash_key(key: str) -> str:
    """Hash the user-facing cache key to an S3-safe filename.

    SHA1 is fine here — collision resistance isn't security-critical;
    we just need deterministic filename safety for arbitrary keys
    (which may contain '/', spaces, query strings, etc.)."""
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


class S3TtlCache:
    """S3-backed TTL cache.

    Args:
        s3_client: boto3 S3 client (or a test mock with put_object /
                   get_object / head_object).
        bucket: S3 bucket name.
        prefix: S3 key prefix; one cache instance per logical scope
                (one for news, one for analyst, etc.) keeps caches
                isolated for cheaper LIST + simpler eviction.
        default_ttl_seconds: TTL applied when callers don't specify.
    """

    def __init__(
        self,
        s3_client: Any,
        *,
        bucket: str = DEFAULT_S3_BUCKET,
        prefix: str = DEFAULT_S3_PREFIX,
        default_ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._default_ttl = default_ttl_seconds

    def _key_path(self, cache_key: str) -> str:
        return f"{self._prefix}/{_hash_key(cache_key)}.bin"

    def get(self, cache_key: str) -> bytes | None:
        """Return the cached body, or ``None`` if missing or expired.

        Expired entries are NOT deleted on read — eviction is lazy
        (next overwrite). Saves cost on read-heavy workloads where the
        expired marker simply isn't returned.
        """
        s3_key = self._key_path(cache_key)
        try:
            obj = self._s3.get_object(Bucket=self._bucket, Key=s3_key)
        except Exception:
            return None
        metadata = obj.get("Metadata") or {}
        meta = _parse_metadata(metadata)
        if meta is None or meta.is_expired:
            return None
        body = obj["Body"].read()
        return body

    def set(
        self,
        cache_key: str,
        value: bytes,
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        """Write a cache entry. Overwrites any prior entry for the same
        key — idempotent."""
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        cached_at = _now_utc()
        expires_at = cached_at + timedelta(seconds=ttl)
        # S3 metadata values must be strings; constrained to ASCII.
        metadata = {
            "cache-key": cache_key[:1024],  # safety truncation
            "cache-cached-at": _iso(cached_at),
            "cache-ttl-seconds": str(ttl),
            "cache-expires-at": _iso(expires_at),
        }
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._key_path(cache_key),
            Body=value,
            Metadata=metadata,
            ContentType="application/octet-stream",
        )

    def cached_call(
        self,
        cache_key: str,
        *,
        compute_fn: Callable[[], bytes],
        ttl_seconds: int | None = None,
    ) -> bytes:
        """Fetch-or-compute pattern. Returns cached body if fresh;
        otherwise calls ``compute_fn`` and caches the result.

        ``compute_fn`` is only called on cache miss / expired entry,
        so passing an expensive synchronous fetch as the compute fn
        is safe. Caller must serialize their payload to bytes — the
        cache is content-agnostic.
        """
        cached = self.get(cache_key)
        if cached is not None:
            return cached
        value = compute_fn()
        self.set(cache_key, value, ttl_seconds=ttl_seconds)
        return value

    async def cached_acall(
        self,
        cache_key: str,
        *,
        async_compute_fn: Callable[[], Awaitable[bytes]],
        ttl_seconds: int | None = None,
    ) -> bytes:
        """Async variant of :meth:`cached_call`. Useful from
        :mod:`collectors.news_aggregator_async` where each adapter
        call returns a coroutine.
        """
        cached = self.get(cache_key)
        if cached is not None:
            return cached
        value = await async_compute_fn()
        self.set(cache_key, value, ttl_seconds=ttl_seconds)
        return value

    # ── JSON convenience wrappers ─────────────────────────────────────

    def get_json(self, cache_key: str) -> Any | None:
        body = self.get(cache_key)
        if body is None:
            return None
        try:
            return json.loads(body)
        except Exception as e:
            logger.warning(
                "[s3_ttl_cache] failed to deserialize JSON for %s: %s",
                cache_key, e,
            )
            return None

    def set_json(
        self,
        cache_key: str,
        value: Any,
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        self.set(
            cache_key,
            json.dumps(value, default=str).encode("utf-8"),
            ttl_seconds=ttl_seconds,
        )


def _parse_metadata(metadata: dict[str, str]) -> _CacheMetadata | None:
    cache_key = metadata.get("cache-key") or ""
    cached_at = _parse_iso(metadata.get("cache-cached-at"))
    expires_at = _parse_iso(metadata.get("cache-expires-at"))
    ttl = metadata.get("cache-ttl-seconds") or "0"
    if cached_at is None or expires_at is None:
        # Entry without our metadata stamp — treat as expired (safer
        # than treating as fresh; will overwrite on next set).
        return None
    try:
        ttl_int = int(ttl)
    except ValueError:
        ttl_int = 0
    return _CacheMetadata(
        cache_key=cache_key,
        cached_at=cached_at,
        ttl_seconds=ttl_int,
        expires_at=expires_at,
    )
