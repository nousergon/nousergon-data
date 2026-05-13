"""Multi-source news aggregator — fan-in, dedup, trust weighting.

Wave 1 PR β of the institutional data revamp (plan doc:
``~/Development/alpha-engine-docs/private/data-revamp-260513.md``).

Architecture::

    [Polygon] ──┐
    [GDELT]   ──┼──→ NewsAggregator.fetch(tickers, hours)
    [Yahoo]   ──┤        │
    [Benzinga]──┘        ▼
                  fan-in concatenate
                         │
                         ▼
                   dedup pass (URL hash → title-similarity)
                         │
                         ▼
                  apply trust weights (config-driven)
                         │
                         ▼
                  list[AggregatedNewsArticle]
                  (with full source-provenance preserved)

The output preserves all source variants of a deduped story so
downstream NLP can weight contributions by source trust (an article
indexed by 3 sources gets stronger sentiment-signal weight than one
indexed by 1). Source-provenance also enables audit + future
counterfactual eval ("what if Polygon went down today — what does the
aggregate look like with only GDELT + Yahoo?").

Why this layer (vs the consumer calling each adapter directly):

- One place to change dedup heuristics
- One place to thread async fetching (PR D — concurrent fan-in)
- One place to compute per-source coverage metrics for CW
- Consumers don't need to know which adapters are enabled
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from alpha_engine_lib.sources import NewsArticle, NewsSource

logger = logging.getLogger(__name__)


# ── Aggregated shape ───────────────────────────────────────────────────


@dataclass(frozen=True)
class AggregatedNewsArticle:
    """One canonical news event with all source variants preserved.

    ``variants`` is the list of (source-specific) ``NewsArticle``
    records that the deduper matched together. Downstream NLP weights
    by trust to compute final sentiment / event-flag aggregates.
    """

    canonical_title: str
    canonical_url: str
    tickers: tuple[str, ...]
    earliest_published_at: datetime
    variants: tuple[NewsArticle, ...]
    canonical_fingerprint: str = field(default="", compare=False)

    @property
    def n_sources(self) -> int:
        return len({v.source for v in self.variants})


# ── Trust weights ──────────────────────────────────────────────────────


# Default trust weights — overridden per-deployment via config layer
# (the aggregator constructor accepts a custom mapping). Conservative
# defaults: paid vendors weighted ≥0.95; free tier 0.5-0.9 by quality.
DEFAULT_TRUST_WEIGHTS: dict[str, float] = {
    # Paid (when wired)
    "bloomberg": 1.0,
    "ravenpack": 1.0,
    "benzinga": 0.95,
    # Free
    "polygon": 0.9,        # aggregates Benzinga + Zacks + MT — high quality on free tier
    "gdelt": 0.85,         # academic-grade, breadth > depth
    "edgar_press": 0.95,   # primary source
    "yahoo_rss": 0.5,      # headlines-only, consumer-grade fallback
}


# ── Dedup helpers ──────────────────────────────────────────────────────


_TITLE_NORMALIZE_RE = re.compile(r"[^a-z0-9\s]+")
_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_title(title: str) -> str:
    """Lowercase + strip punctuation + collapse whitespace.

    Wire stories from different vendors often differ only in
    capitalization, punctuation, or stop-word reordering. This is the
    cheapest dedup signal that catches the obvious cases. PR β
    intentionally uses this (vs full MinHash LSH from datasketch) —
    keep deps lean; upgrade if recall metrics show miss rate.
    """
    return _WHITESPACE_RE.sub(
        " ", _TITLE_NORMALIZE_RE.sub(" ", title.lower())
    ).strip()


def _url_fingerprint(url: str) -> str:
    """SHA-1 of the URL stripped to scheme+host+path (no query)."""
    if "?" in url:
        url = url.split("?", 1)[0]
    if "#" in url:
        url = url.split("#", 1)[0]
    return hashlib.sha1(url.lower().encode("utf-8")).hexdigest()


def _article_fingerprint(article: NewsArticle) -> str:
    """Composite fingerprint — normalized title + URL host+path.

    Two articles fingerprinting the same are treated as the same wire
    story. Distinct titles on the same URL (rare; defensive) still
    collide on URL hash.
    """
    title_norm = _normalize_title(article.title)
    url_fp = _url_fingerprint(article.url)
    return hashlib.sha1(
        f"{title_norm}|{url_fp}".encode("utf-8")
    ).hexdigest()


# ── Aggregator ─────────────────────────────────────────────────────────


class NewsAggregator:
    """Fan-in across ``NewsSource`` adapters → dedup → trust weights.

    ``sources`` are the enabled adapters. Order doesn't matter — dedup
    is symmetric. Sources can be added/removed at runtime without
    rewiring consumers.

    ``trust_weights`` maps adapter ``name`` → weight in ``[0, 1]``.
    Unmapped sources default to 0.5 (conservative — flag in logs).
    """

    def __init__(
        self,
        sources: Iterable[NewsSource],
        trust_weights: dict[str, float] | None = None,
    ) -> None:
        self._sources: tuple[NewsSource, ...] = tuple(sources)
        self._trust_weights: dict[str, float] = dict(trust_weights or DEFAULT_TRUST_WEIGHTS)

    @property
    def source_names(self) -> tuple[str, ...]:
        return tuple(s.name for s in self._sources)

    def trust_weight(self, source_name: str) -> float:
        if source_name not in self._trust_weights:
            logger.warning(
                "[news_aggregator] no trust weight configured for source %r — "
                "defaulting to 0.5 (treat with care + add to config)",
                source_name,
            )
        return self._trust_weights.get(source_name, 0.5)

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[AggregatedNewsArticle]:
        """Fan-in across all enabled sources, dedup, return aggregated set.

        Sequential per-source today; PR D adds async parallel fan-in.
        Each source's own retry/rate-limit policy applies inside its
        ``fetch()`` — aggregator is transport-agnostic.
        """
        all_articles: list[NewsArticle] = []
        for source in self._sources:
            try:
                batch = source.fetch(tickers, hours=hours)
            except Exception as e:
                # Per the Protocol contract sources shouldn't raise on
                # transient failures, but defense in depth: one bad
                # adapter can't take down the whole aggregation.
                logger.exception(
                    "[news_aggregator] source %r raised: %s — continuing with "
                    "remaining sources", source.name, e
                )
                continue
            all_articles.extend(batch)
            logger.info(
                "[news_aggregator] %s returned %d articles", source.name, len(batch)
            )

        if not all_articles:
            return []

        return self._dedup(all_articles)

    def _dedup(
        self, articles: list[NewsArticle]
    ) -> list[AggregatedNewsArticle]:
        """Group by composite fingerprint; pick canonical per group."""
        groups: dict[str, list[NewsArticle]] = defaultdict(list)
        for a in articles:
            fp = _article_fingerprint(a)
            groups[fp].append(a)

        aggregated: list[AggregatedNewsArticle] = []
        for fp, variants in groups.items():
            # Canonical title: longest title across variants (more info
            # almost always = more useful for downstream NLP). Canonical
            # URL: highest-trust source's URL. Tickers: union across
            # variants.
            sorted_by_title_len = sorted(
                variants, key=lambda v: len(v.title or ""), reverse=True
            )
            canonical_title = sorted_by_title_len[0].title or ""
            sorted_by_trust = sorted(
                variants,
                key=lambda v: self.trust_weight(v.source),
                reverse=True,
            )
            canonical_url = sorted_by_trust[0].url
            ticker_union = tuple(sorted({
                t for v in variants for t in v.tickers
            }))
            earliest = min(v.published_at for v in variants)
            aggregated.append(AggregatedNewsArticle(
                canonical_title=canonical_title,
                canonical_url=canonical_url,
                tickers=ticker_union,
                earliest_published_at=earliest,
                variants=tuple(variants),
                canonical_fingerprint=fp,
            ))

        # Sort by earliest_published_at desc so consumers see the
        # freshest stories first.
        aggregated.sort(
            key=lambda a: a.earliest_published_at, reverse=True
        )
        return aggregated
