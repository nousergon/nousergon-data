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

# Wave 3 PR4 (cutover, DONE) — ``reference/price_cache/`` is the sole write +
# read home. ``PRICE_CACHE_LEGACY_PREFIX`` is retained only as the
# production-default *sentinel* the prod call sites still pass (the helper
# translates it to the reference prefix) and so the constant remains importable
# for any historical reference; the legacy tree itself is removed live via
# ``aws s3 rm --recursive s3://alpha-engine-research/predictor/price_cache/``.
PRICE_CACHE_LEGACY_PREFIX = "predictor/price_cache/"
PRICE_CACHE_NEW_PREFIX = "reference/price_cache/"


def price_cache_write_prefixes(primary: str = PRICE_CACHE_LEGACY_PREFIX) -> list[str]:
    """Return the active write prefixes for ticker parquet uploads.

    Wave 3 PR4 cutover (this edit) retired the write-both soak: ticker
    parquets now write to ``reference/price_cache/`` ONLY. The legacy
    ``predictor/price_cache/`` tree is no longer written and is removed
    live via ``aws s3 rm --recursive`` once this lands.

      * ``primary == "predictor/price_cache/"`` (the production-default
        sentinel that prod call sites still pass) → ``["reference/price_cache/"]``
        — the sole surviving home.
      * ``primary`` is any other string (custom prefix from a test or a
        config override) → ``[primary]`` — single-write, prefix honored
        verbatim.

    Callers wrap their upload in::

        for prefix in price_cache_write_prefixes(s3_prefix):
            s3.upload_file(local_path, bucket, f"{prefix}{ticker}.parquet")

    The default-sentinel argument is kept (rather than retargeting the
    call sites) so the helper stays the single chokepoint for the prefix
    contract — call sites pass their config-derived ``s3_prefix`` and the
    helper owns the legacy→reference translation.
    """
    if primary == PRICE_CACHE_LEGACY_PREFIX:
        return [PRICE_CACHE_NEW_PREFIX]
    return [primary]


def price_cache_read_prefixes(primary: str = PRICE_CACHE_LEGACY_PREFIX) -> list[str]:
    """Return active READ prefixes in fallback order.

    Companion to :func:`price_cache_write_prefixes`. Wave-3 PR4 cutover
    (this edit) dropped the legacy ``predictor/price_cache/`` fallback in
    the same change that retired write-both: with the producer writing
    only ``reference/price_cache/`` and the legacy tree slated for
    ``aws s3 rm``, reads resolve from ``reference/`` alone.

      * ``primary == "predictor/price_cache/"`` (the production-default
        sentinel) → ``["reference/price_cache/"]``: the sole surviving home.
      * ``primary`` is any other string → ``[primary]`` (single-read,
        prefix honored verbatim — mirrors the write-side semantics).

    Callers wrap their reads in::

        for prefix in price_cache_read_prefixes(s3_prefix):
            try:
                return s3.get_object(Bucket=bucket, Key=f"{prefix}{name}")
            except ClientError:
                continue
    """
    if primary == PRICE_CACHE_LEGACY_PREFIX:
        return [PRICE_CACHE_NEW_PREFIX]
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
