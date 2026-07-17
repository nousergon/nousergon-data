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

Resilience posture (alpha-engine-config#2813, 2026-07-17): GDELT throttling
escalated from "occasional 429" (config#663, judged non-blocking) to a
sustained ~100% 429 rate across an entire run, which made the *unbounded*
per-ticker loop below run long enough to blow the caller's wall-clock
budget (``daily-news.service``'s ``TimeoutStartSec``) with zero output —
no digest at all, cascading into a missed morning-signal episode. This
adapter now enforces its own hard time budget (below) so a bad GDELT day
degrades *this adapter's* coverage instead of starving the whole pipeline
of a chance to run its later steps (aggregation, digest write). Polygon +
Yahoo RSS are unaffected by GDELT's throttling and reliably complete on
their own, so a capped/degraded GDELT contribution still yields a real,
usable digest.
"""

from __future__ import annotations

import logging
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

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


# Hard wall-clock budget for one ``fetch()`` call across the ENTIRE ticker
# list (config#2813). Sized well under the standalone box's 40-min systemd
# TimeoutStartSec so Polygon + Yahoo RSS (unaffected by GDELT throttling)
# plus NLP/aggregation/digest-write always get to run regardless of how
# badly GDELT is behaving. Checked before each ticker, not just once, so a
# slow-but-not-yet-exhausted run still stops promptly at the boundary.
_DEFAULT_MAX_FETCH_SECONDS = 1_200.0  # 20 min

# Adaptive retry abandonment (config#2813): the single Retry-After-honoring
# retry (config#663) helps when 429s are occasional, but on 2026-07-17's
# sustained ~100% throttle rate, the retry attempt ALSO 429'd on every single
# ticker observed — i.e. it bought zero recovered coverage while still
# paying the full Retry-After wait + a second round trip. Once we've seen
# enough retry attempts to trust the signal and NONE of them recovered a
# ticker, stop paying for the retry pass for the rest of this run — this
# roughly halves per-ticker latency under sustained throttling, letting
# strictly more tickers fit inside the time budget above. If retries ARE
# sometimes working (occasional-429 regime), this never engages and behavior
# is identical to before.
_RETRY_ABANDON_SAMPLE_SIZE = 8

# GDELT's company-name search can never meaningfully match a bond CUSIP
# (a 9-character alphanumeric identifier, never a company/ticker name) —
# every such query is a guaranteed-zero-value round trip that still counts
# against the rate limit and the time budget above. Skipped up front.
_CUSIP_RE = re.compile(r"^[A-Z0-9]{9}$")


def _looks_like_cusip(ticker: str) -> bool:
    """True for a 9-char alphanumeric identifier containing at least one
    digit — CUSIPs are always 9 chars and mix letters+digits; equity/ETF
    tickers are essentially never 9 characters. Conservative by design:
    only skips identifiers that could never be a real ticker, never a
    judgment call about whether a *valid* ticker is "worth" a GDELT query."""
    return bool(_CUSIP_RE.match(ticker)) and any(c.isdigit() for c in ticker)


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
        max_fetch_seconds: float = _DEFAULT_MAX_FETCH_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ticker_name_map = ticker_name_map or {}
        self._http = http or requests
        self._inter_request_sleep = inter_request_sleep
        self._max_fetch_seconds = max_fetch_seconds
        self._monotonic = monotonic
        # Per-call adaptive state — reset at the top of every fetch() so one
        # day's throttling regime never leaks into the next day's adapter
        # instance (a fresh one is constructed per collect() run anyway).
        self._retry_outcomes: deque[bool] = deque(maxlen=_RETRY_ABANDON_SAMPLE_SIZE)
        self._retries_abandoned = False

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[NewsArticle]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)
        articles: list[NewsArticle] = []
        self._retry_outcomes.clear()
        self._retries_abandoned = False

        fetch_tickers = []
        cusip_skipped = 0
        for ticker in tickers:
            if _looks_like_cusip(ticker):
                cusip_skipped += 1
            else:
                fetch_tickers.append(ticker)
        if cusip_skipped:
            logger.info(
                "[gdelt_news] skipping %d CUSIP-shaped identifier(s) — "
                "GDELT company-name search never matches a bond CUSIP",
                cusip_skipped,
            )

        deadline = self._monotonic() + self._max_fetch_seconds
        budget_exhausted_at: int | None = None
        for i, ticker in enumerate(fetch_tickers):
            if i > 0:
                if self._monotonic() >= deadline:
                    budget_exhausted_at = i
                    break
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

        if budget_exhausted_at is not None:
            skipped = len(fetch_tickers) - budget_exhausted_at
            logger.warning(
                "[gdelt_news] time budget (%.0fs) exhausted after %d/%d "
                "tickers — skipping remaining %d so the rest of the pull "
                "(other sources, aggregation, digest write) still gets to "
                "run within the caller's overall deadline",
                self._max_fetch_seconds, budget_exhausted_at,
                len(fetch_tickers), skipped,
            )
        return articles

    def _fetch_one(self, ticker: str, params: dict) -> dict | None:
        """Fetch one ticker's articles, honoring a 429 ``Retry-After`` with a
        single bounded retry — UNLESS retries have been adaptively abandoned
        for the rest of this run (see ``_RETRY_ABANDON_SAMPLE_SIZE`` above).
        Returns the parsed JSON payload, or ``None`` on terminal failure (the
        caller skips the ticker — fail-soft)."""
        max_attempts = 1 if self._retries_abandoned else 2
        for attempt in range(max_attempts):
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

            if getattr(resp, "status_code", None) == 429 and attempt == 0 and max_attempts > 1:
                wait = _retry_after_seconds(resp)
                logger.warning(
                    "[gdelt_news] 429 for %s — honoring Retry-After=%.1fs "
                    "then retrying once", ticker, wait,
                )
                time.sleep(wait)
                continue

            try:
                resp.raise_for_status()
                if attempt > 0:
                    self._record_retry_outcome(recovered=True)
                return resp.json()
            except Exception as e:
                if attempt > 0:
                    self._record_retry_outcome(recovered=False)
                logger.warning(
                    "[gdelt_news] fetch failed for %s: %s", ticker, e
                )
                return None
        return None

    def _record_retry_outcome(self, *, recovered: bool) -> None:
        """Track whether the bounded retry pass recovers coverage. Once a
        full window of retries has recovered NOTHING, abandon retrying for
        the rest of this run (see module docstring / config#2813) — cuts
        per-ticker latency under sustained throttling without ever reducing
        breadth in the occasional-429 regime the retry was built for."""
        if self._retries_abandoned:
            return
        self._retry_outcomes.append(recovered)
        if (
            len(self._retry_outcomes) >= _RETRY_ABANDON_SAMPLE_SIZE
            and not any(self._retry_outcomes)
        ):
            self._retries_abandoned = True
            logger.warning(
                "[gdelt_news] last %d retries recovered zero coverage — "
                "abandoning the retry pass for the rest of this run "
                "(sustained-throttle regime, not the occasional-429 case "
                "the retry was built for)",
                _RETRY_ABANDON_SAMPLE_SIZE,
            )


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
