"""GDELT news adapter. Free + no API key.

GDELT (Global Database of Events, Language, and Tone) is THE academic
free news source for finance research. The 2.0 DOC API exposes a
real-time stream of articles indexed across thousands of sources with
event extraction, named-entity recognition, and tone scoring already
applied vendor-side.

Endpoint: GET https://api.gdeltproject.org/api/v2/doc/doc

Key params for our use:
  query="(company name OR ticker) sourcecountry:US"
  mode=ArtList
  format=json
  maxrecords=50
  timespan=2d (or hours-based via startdatetime/enddatetime)

Notes:

- GDELT indexes articles ~15 min after publication; not real-time but
  fresh enough for daily research cadence.
- "Company name" recognition is approximate — for accuracy, pair with a
  ticker→name map and post-filter on tickers we expect.
- Free tier: no documented hard rate limit, but be polite (we cap at
  one request per ticker per call, ~1 sec delay between batches).
- Trust weight in config should be moderate (~0.85) — academic-grade
  but breadth > depth; cross-validate with Polygon when both have hits.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from nousergon_lib.sources import NewsArticle

import requests

logger = logging.getLogger(__name__)


_GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"


# Conservative inter-request delay. GDELT doesn't publish a hard limit
# but historic guidance is "be reasonable" — 1 req/sec is safe.
_INTER_REQUEST_SLEEP_SECONDS = 1.0


# GDELT periodically returns HTTP 429 under load (it has no documented hard
# rate cap, so the 1s inter-request spacing is heuristic). Without honoring
# Retry-After, every 429 dropped that ticker's coverage on the daily pull
# (the fail-soft `continue` path) — config#663. On a 429 we wait the
# server-advertised Retry-After (clamped) and retry the ticker ONCE before
# giving up, recovering breadth without stalling the whole pull.
_RETRY_AFTER_FALLBACK_SECONDS = 2.0
_RETRY_AFTER_MAX_SECONDS = 30.0


class GdeltNewsAdapter:
    """GDELT 2.0 DOC API adapter. Implements ``NewsSource``.

    ``ticker_name_map`` is required: GDELT's recognition works on
    company names, not exchange tickers. Pass a {ticker: company_name}
    dict (e.g. from S&P 500 constituents) so the adapter can build the
    right query.
    """

    name = "gdelt"

    def __init__(
        self,
        ticker_name_map: dict[str, str] | None = None,
        *,
        http: Any = None,
        inter_request_sleep: float = _INTER_REQUEST_SLEEP_SECONDS,
    ) -> None:
        self._ticker_name_map = ticker_name_map or {}
        self._http = http or requests
        self._inter_request_sleep = inter_request_sleep

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[NewsArticle]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)
        articles: list[NewsArticle] = []
        for i, ticker in enumerate(tickers):
            if i > 0:
                time.sleep(self._inter_request_sleep)
            company_name = self._ticker_name_map.get(ticker, ticker)
            query = _build_query(ticker, company_name)
            params = {
                "query": query,
                "mode": "ArtList",
                "format": "json",
                "maxrecords": 50,
                "startdatetime": start.strftime("%Y%m%d%H%M%S"),
                "enddatetime": end.strftime("%Y%m%d%H%M%S"),
                "sort": "DateDesc",
            }
            payload = self._fetch_one(ticker, params)
            if payload is None:
                continue
            for item in payload.get("articles", []) or []:
                article = _to_article(item, ticker=ticker)
                if article is not None:
                    articles.append(article)
        return articles

    def _fetch_one(self, ticker: str, params: dict) -> dict | None:
        """Fetch one ticker's articles, honoring a 429 ``Retry-After`` with a
        single bounded retry. Returns the parsed JSON payload, or ``None`` on
        terminal failure (the caller skips the ticker — fail-soft)."""
        for attempt in range(2):  # one initial + one retry-after-429
            try:
                resp = self._http.get(
                    _GDELT_DOC_API,
                    params=params,
                    timeout=20,
                )
            except Exception as e:
                logger.warning(
                    "[gdelt_news] fetch failed for %s: %s", ticker, e
                )
                return None

            if getattr(resp, "status_code", None) == 429 and attempt == 0:
                wait = _retry_after_seconds(resp)
                logger.warning(
                    "[gdelt_news] 429 for %s — honoring Retry-After=%.1fs "
                    "then retrying once", ticker, wait,
                )
                time.sleep(wait)
                continue

            try:
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(
                    "[gdelt_news] fetch failed for %s: %s", ticker, e
                )
                return None
        return None


def _retry_after_seconds(resp: Any) -> float:
    """Parse a 429 response's ``Retry-After`` header into a clamped sleep.

    Honors the integer-seconds form (the only form GDELT/CDN proxies emit in
    practice). Falls back to a small fixed wait when the header is missing or
    unparseable, and clamps to ``_RETRY_AFTER_MAX_SECONDS`` so a hostile or
    buggy header can't stall the whole daily pull."""
    headers = getattr(resp, "headers", None) or {}
    raw = headers.get("Retry-After")
    wait = _RETRY_AFTER_FALLBACK_SECONDS
    if raw is not None:
        try:
            wait = float(int(str(raw).strip()))
        except (TypeError, ValueError):
            wait = _RETRY_AFTER_FALLBACK_SECONDS
    if wait < 0:
        wait = _RETRY_AFTER_FALLBACK_SECONDS
    return min(wait, _RETRY_AFTER_MAX_SECONDS)


def _build_query(ticker: str, company_name: str) -> str:
    """Build the GDELT query expression. We OR ticker + company name to
    catch both ('NVDA' headlines + 'Nvidia' headlines) and gate on US
    source country to filter noise."""
    name_quoted = f'"{company_name}"' if " " in company_name else company_name
    return f'({ticker} OR {name_quoted}) sourcecountry:US'


def _to_article(item: dict, *, ticker: str) -> NewsArticle | None:
    """Map one GDELT ``articles[]`` entry to ``NewsArticle``."""
    try:
        seendate = item.get("seendate")
        if not seendate:
            return None
        # GDELT seendate format: '20260513T120000Z'
        published_dt = datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
        url = item.get("url") or ""
        if not url:
            return None
        return NewsArticle(
            tickers=(ticker,),
            title=item.get("title") or "",
            body_excerpt="",  # GDELT DOC API doesn't expose body
            url=url,
            published_at=published_dt,
            source="gdelt",
            vendor_article_id=None,  # GDELT DOC doesn't expose record IDs
            fetched_at=datetime.now(timezone.utc),
            headline_authors=None,
            tags=tuple(filter(None, [
                item.get("sourcecountry"),
                item.get("language"),
                item.get("domain"),
            ])),
        )
    except Exception as e:
        logger.warning("[gdelt_news] schema drift on item: %s", e)
        return None
