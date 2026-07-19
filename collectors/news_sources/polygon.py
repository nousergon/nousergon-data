"""Polygon news adapter. Free tier on our existing Polygon API key.

Endpoint: GET /v2/reference/news?ticker={t}&published_utc.gte={iso}

Polygon's free tier limits us to 5 req/min — same per-key shared limit
as the price endpoints (see ``polygon_client.PolygonClient``). We reuse
that client's rate limiter rather than maintaining a second one.

Quality: Polygon aggregates Benzinga, Zacks, MT Newswires, etc. —
genuinely institutional-grade headlines on a free tier. Article body
text is typically just the lead paragraph (full body lives on the
original publisher's site).

Resilience posture (alpha-engine-config#2938, 2026-07-18): Polygon's free
tier is hard-capped at 5 req/min (12s/ticker), so sweeping a large universe
is inherently slow — ~944 tickers ⇒ ~3.15h. GDELT got a wall-clock time
budget from config#2813, but Polygon was left uncovered: an unbounded
per-ticker loop over an ~9x-grown universe (plus a sustained 429 storm) blew
the caller's outer deadline with ZERO output — killing the daily digest AND
SIGKILLing the weekly Saturday RAGIngestion step (two 1h ExecutionTimedOut).
This adapter now enforces its OWN hard time budget (below), mirroring
``GdeltNewsAdapter``: on the DAILY path a tight budget bails early with a
partial digest (acceptable — ``partial: true``); on the WEEKLY path the
caller sizes the budget from the live universe to COMPLETE the full sweep
(config#2938 ruling 1), and the guard is only a SIGKILL backstop so a
pathological throttle degrades coverage instead of taking the pipeline down.
Budgets are derived in ``collectors.news_sources.fetch_budget``.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from nousergon_lib.sources import NewsArticle

from polygon_client import polygon_client

logger = logging.getLogger(__name__)


# Hard wall-clock budget for one ``fetch()`` call across the ENTIRE ticker
# list (config#2938), mirroring ``GdeltNewsAdapter``. The default is the tight
# DAILY bail-early value; the WEEKLY caller passes a larger, universe-derived
# budget (see ``collectors.news_sources.fetch_budget``). Checked before each
# ticker (not just once), so a slow-but-not-yet-exhausted run still stops
# promptly at the boundary and lets the rest of the pull (other sources,
# aggregation, digest/RAG write) run within the caller's overall deadline.
_DEFAULT_MAX_FETCH_SECONDS = 1_200.0  # 20 min


class PolygonNewsAdapter:
    """Polygon news adapter. Implements ``NewsSource``."""

    name = "polygon"

    def __init__(
        self,
        client: Any = None,
        *,
        max_fetch_seconds: float = _DEFAULT_MAX_FETCH_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client  # default: lazy via polygon_client() factory
        self._max_fetch_seconds = max_fetch_seconds
        self._monotonic = monotonic

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
        deadline = self._monotonic() + self._max_fetch_seconds
        budget_exhausted_at: int | None = None
        for i, ticker in enumerate(tickers):
            # Never checked before the FIRST ticker — even a zero/negative
            # budget attempts one fetch; the guard only skips the remainder.
            if i > 0 and self._monotonic() >= deadline:
                budget_exhausted_at = i
                break
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

        if budget_exhausted_at is not None:
            skipped = len(tickers) - budget_exhausted_at
            logger.warning(
                "[polygon_news] time budget (%.0fs) exhausted after %d/%d "
                "tickers — skipping remaining %d so the rest of the pull "
                "(other sources, aggregation, digest/RAG write) still gets to "
                "run within the caller's overall deadline",
                self._max_fetch_seconds, budget_exhausted_at,
                len(tickers), skipped,
            )
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
