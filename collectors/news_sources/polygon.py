"""Polygon news adapter. Free tier on our existing Polygon API key.

Endpoint: GET /v2/reference/news?ticker={t}&published_utc.gte={iso}

Polygon's free tier limits us to 5 req/min — same per-key shared limit
as the price endpoints (see ``polygon_client.PolygonClient``). We reuse
that client's rate limiter rather than maintaining a second one.

Quality: Polygon aggregates Benzinga, Zacks, MT Newswires, etc. —
genuinely institutional-grade headlines on a free tier. Article body
text is typically just the lead paragraph (full body lives on the
original publisher's site).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from alpha_engine_lib.sources import NewsArticle

from polygon_client import polygon_client

logger = logging.getLogger(__name__)


class PolygonNewsAdapter:
    """Polygon news adapter. Implements ``NewsSource``."""

    name = "polygon"

    def __init__(self, client: Any = None) -> None:
        self._client = client  # default: lazy via polygon_client() factory

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = polygon_client()
        return self._client

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[NewsArticle]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        articles: list[NewsArticle] = []
        for ticker in tickers:
            try:
                payload = self._get_client()._get(
                    "/v2/reference/news",
                    params={
                        "ticker": ticker,
                        "published_utc.gte": cutoff_iso,
                        "order": "desc",
                        "limit": 50,
                        "sort": "published_utc",
                    },
                )
            except Exception as e:
                # Transient vendor failure — return what we have for
                # other tickers rather than failing the whole batch.
                logger.warning(
                    "[polygon_news] fetch failed for %s: %s", ticker, e
                )
                continue
            for item in payload.get("results", []) or []:
                article = _to_article(item, ticker=ticker)
                if article is not None:
                    articles.append(article)
        return articles


def _to_article(item: dict, *, ticker: str) -> NewsArticle | None:
    """Map a Polygon ``results[]`` entry to the canonical ``NewsArticle``.

    Returns ``None`` if the entry is missing required fields — vendor
    schema drift surfaces as a log warning, not a crash.
    """
    try:
        published = item.get("published_utc")
        if not published:
            return None
        if isinstance(published, str):
            published_dt = datetime.fromisoformat(
                published.replace("Z", "+00:00")
            )
        else:
            return None
        # Polygon attaches a `tickers` list — keep the cross-mention
        # context so the aggregator can match the article against
        # multiple tickers in one call.
        article_tickers = tuple(item.get("tickers") or (ticker,))
        return NewsArticle(
            tickers=article_tickers,
            title=item.get("title") or "",
            body_excerpt=item.get("description") or "",
            url=item.get("article_url") or item.get("amp_url") or "",
            published_at=published_dt,
            source="polygon",
            vendor_article_id=item.get("id"),
            fetched_at=datetime.now(timezone.utc),
            headline_authors=tuple(item.get("author") or [])
                if isinstance(item.get("author"), list)
                else (item["author"],) if item.get("author")
                else None,
            tags=tuple(item.get("keywords") or []),
        )
    except Exception as e:
        logger.warning("[polygon_news] schema drift on item: %s", e)
        return None
