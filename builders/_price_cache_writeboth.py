"""Wave 3 PR1 — additive write-both helper for the price_cache prefix migration.

ROADMAP P1 "`predictor/` S3 namespace rationalization Wave 3": migrate the 10y
``predictor/price_cache/*.parquet`` tree to ``reference/price_cache/`` so the
``predictor/`` namespace ends up owning ONLY predictor-module outputs
(``weights/``, ``predictions/``, ``metrics/``) and ``reference/`` collects
long-lived data-module references. Mirrors the Wave 1 ``predictor/daily_closes/``
→ ``staging/daily_closes/`` arc but uses write-both + soak (instead of Wave 1's
hard cutover) because this writer rewrites only STALE tickers — a hard cut
would leave fresh tickers in legacy and the new prefix incomplete for an
entire yfinance refresh cycle. CLAUDE.md S3 Contract Safety mandates the
write-both for any path change.

Soak contract:
    Wave 3 PR1 (this PR) → both prefixes written, every reader stays on legacy.
    ≥1 week soak after PR1's first Saturday SF lands writes to both prefixes.
    Wave 3 PR3+ → reader migrations with legacy fallback in 4 repos.
    Wave 3 PR4 (cutover) → swap primary to ``reference/``, drop legacy from this
        helper's return list, retire fallbacks, ``aws s3 rm`` legacy prefix.

The helper is the single chokepoint for write-both — three writers call into
it (``collectors/prices.py`` yfinance refresh, ``collectors/fred_history.py``
FRED backfill, ``weekly_collector.py`` chronic-gap self-heal patch). Adding
a future writer requires no per-call-site discipline beyond wrapping the
upload in ``for prefix in price_cache_write_prefixes(s3_prefix): ...``.
"""

from __future__ import annotations

from typing import Any

# Wave 3 PR1 — legacy primary, new mirror. The cutover PR (Wave 3 PR4) flips
# ``PRICE_CACHE_NEW_PREFIX`` to first position and removes the legacy entry
# from ``price_cache_write_prefixes`` (one-line edit at that time).
PRICE_CACHE_LEGACY_PREFIX = "predictor/price_cache/"
PRICE_CACHE_NEW_PREFIX = "reference/price_cache/"


def price_cache_write_prefixes(primary: str = PRICE_CACHE_LEGACY_PREFIX) -> list[str]:
    """Return the active write prefixes for ticker parquet uploads.

    During the Wave 3 write-both soak (this PR through Wave 3 PR4 cutover):

      * ``primary == "predictor/price_cache/"`` (the production default) →
        ``["predictor/price_cache/", "reference/price_cache/"]`` — every
        ticker-parquet write hits both prefixes byte-for-byte.
      * ``primary`` is any other string (custom prefix from a test or a
        config override) → ``[primary]`` — single-write fallback. Tests
        that need to assert a single-prefix write pass an explicit custom
        prefix; production paths use the default and get write-both.

    Callers wrap their upload in::

        for prefix in price_cache_write_prefixes(s3_prefix):
            s3.upload_file(local_path, bucket, f"{prefix}{ticker}.parquet")

    The order is deterministic (legacy first, new second) so a fail-fast
    upload error on the legacy prefix preserves the existing pre-Wave-3
    failure semantics — the new prefix never silently masks a legacy
    write failure.
    """
    if primary == PRICE_CACHE_LEGACY_PREFIX:
        return [PRICE_CACHE_LEGACY_PREFIX, PRICE_CACHE_NEW_PREFIX]
    return [primary]


def price_cache_read_prefixes(primary: str = PRICE_CACHE_LEGACY_PREFIX) -> list[str]:
    """Return active READ prefixes in fallback order.

    Companion to :func:`price_cache_write_prefixes` — the Wave-3 reader
    migration (PR3+) iterates this in fallback order so the new prefix
    is consulted first and the legacy prefix is the safety net during
    the soak window.

      * ``primary == "predictor/price_cache/"`` (the production default) →
        ``["reference/price_cache/", "predictor/price_cache/"]``: try
        the new prefix first (post-PR4 it's the sole survivor), fall
        back to legacy on miss for the soak period.
      * ``primary`` is any other string → ``[primary]`` (single-read
        fallback, mirrors the test-friendly write-side semantics).

    Callers wrap their reads in::

        for prefix in price_cache_read_prefixes(s3_prefix):
            try:
                return s3.get_object(Bucket=bucket, Key=f"{prefix}{name}")
            except ClientError:
                continue

    At Wave-3 PR4 cutover the legacy entry is removed here in the same
    edit that flips the write helper — one-line change in each.
    """
    if primary == PRICE_CACHE_LEGACY_PREFIX:
        return [PRICE_CACHE_NEW_PREFIX, PRICE_CACHE_LEGACY_PREFIX]
    return [primary]


def list_price_cache_keys(
    s3: Any, bucket: str, primary: str = PRICE_CACHE_LEGACY_PREFIX,
) -> list[str]:
    """List per-ticker parquet keys across the active read prefixes.

    Companion to :func:`price_cache_read_prefixes` for aggregate-read
    sites (``builders/backfill._load_full_cache``,
    ``collectors/slim_cache.collect``). Iterates the fallback
    chain in read order and returns deduplicated keys by
    ``{ticker}.parquet`` basename — first prefix wins, so the new
    prefix takes precedence during the Wave-3 soak and is the sole
    survivor post-PR4 cutover. Legacy fills any reference-side gaps.

    Keys are returned with their full prefix attached (callers pass
    them straight to ``s3.get_object`` / ``download_file``). Order is
    deterministic: all unique keys discovered under the first prefix
    in :func:`price_cache_read_prefixes` order, then any new keys
    from the second prefix.

    The single-key fallback path (one ticker, two candidate prefixes)
    is inlined at the few call sites that need it — the predictor's
    ``regime/features._read_parquet_close`` chokepoint and the data
    repo's ``_load_parquet_warmup`` — mirroring the
    :func:`price_cache_read_prefixes` semantics without a second
    helper signature.
    """
    if primary != PRICE_CACHE_LEGACY_PREFIX:
        # Custom prefix opts out of the fallback chain, matching the
        # write/read-helper conventions for tests + config overrides.
        prefixes: list[str] = [primary]
    else:
        prefixes = price_cache_read_prefixes(primary)

    seen_basenames: set[str] = set()
    out: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                basename = key.rsplit("/", 1)[-1]
                if basename in seen_basenames:
                    continue
                seen_basenames.add(basename)
                out.append(key)
    return out


__all__ = [
    "PRICE_CACHE_LEGACY_PREFIX",
    "PRICE_CACHE_NEW_PREFIX",
    "price_cache_write_prefixes",
    "price_cache_read_prefixes",
    "list_price_cache_keys",
]
