"""Yahoo Finance RSS news adapter — fallback / cross-validation source.

Pure feedparser-based; matches the inline pattern in
``collectors/alternative.py::_fetch_news`` but normalized into the
canonical ``NewsArticle`` shape via the adapter Protocol.

Trust weight in config should be low (~0.5) — RSS is consumer-grade,
headlines-only, frequent dupes of wire stories Polygon already indexed.
Kept as a fallback / coverage-expansion source, not a primary.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from alpha_engine_lib.sources import NewsArticle

logger = logging.getLogger(__name__)


_YAHOO_RSS_URL = (
    "https://feeds.finance.yahoo.com/rss/2.0/headline"
    "?s={ticker}&region=US&lang=en-US"
)


class YahooRssNewsAdapter:
    """Yahoo Finance RSS adapter. Implements ``NewsSource``.

    ``feedparser_module`` is injectable for testing — production usage
    leaves it None and the adapter lazy-imports the package."""

    name = "yahoo_rss"

    def __init__(self, feedparser_module=None) -> None:
        self._feedparser = feedparser_module

    def _get_feedparser(self):
        if self._feedparser is None:
            import feedparser
            self._feedparser = feedparser
        return self._feedparser

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[NewsArticle]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        articles: list[NewsArticle] = []
        for ticker in tickers:
            try:
                feed = self._get_feedparser().parse(
                    _YAHOO_RSS_URL.format(ticker=ticker)
                )
            except Exception as e:
                logger.warning(
                    "[yahoo_rss] fetch failed for %s: %s", ticker, e
                )
                continue
            for entry in (feed.entries or [])[:50]:
                article = _to_article(entry, ticker=ticker, cutoff=cutoff)
                if article is not None:
                    articles.append(article)
        return articles


def _to_article(entry, *, ticker: str, cutoff: datetime) -> NewsArticle | None:
    """Map one feedparser entry to canonical ``NewsArticle``."""
    try:
        url = entry.get("link") or ""
        if not url:
            return None
        pub = entry.get("published_parsed") or entry.get("updated_parsed")
        if pub:
            published_dt = datetime(*pub[:6], tzinfo=timezone.utc)
        else:
            published_dt = datetime.now(timezone.utc)
        if published_dt < cutoff:
            return None
        # Vendor source tag (e.g. "Reuters" via Yahoo's wire)
        source_attr = entry.get("source")
        if isinstance(source_attr, dict):
            vendor_source = source_attr.get("title") or "Yahoo Finance"
        else:
            vendor_source = "Yahoo Finance"
        return NewsArticle(
            tickers=(ticker,),
            title=(entry.get("title") or "").strip(),
            body_excerpt=(entry.get("summary") or "").strip(),
            url=url,
            published_at=published_dt,
            source="yahoo_rss",
            vendor_article_id=entry.get("id"),
            fetched_at=datetime.now(timezone.utc),
            headline_authors=None,
            tags=(vendor_source,) if vendor_source else (),
        )
    except Exception as e:
        logger.warning("[yahoo_rss] schema drift on entry: %s", e)
        return None
