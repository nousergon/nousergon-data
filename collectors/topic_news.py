"""Topic news fetcher — curated free RSS feeds for ``macro`` + ``tech``.

The per-ticker daily news path (``collectors.daily_news``) answers "what is
happening to the names we hold/track". This module answers the complementary
"what is happening in the world" — broad macro/markets and tech headlines —
from a small, hand-curated set of stable, free RSS feeds. It is the second
input to the podcast-ready daily *digest* (see ``collectors.daily_news`` →
``data.derived.news_digest``).

Design (mirrors ``collectors/news_sources/yahoo_rss.py``):

- Pure ``feedparser`` — no API keys, no LLM, deterministic.
- Fail-soft PER FEED: a single dead/garbled feed logs a WARN and is skipped;
  it never kills the topic (mirrors the per-source fail-soft in the ticker
  path). A topic with every feed down simply returns ``[]``.
- Recency filter: drop entries older than ``hours`` (default 24) by published
  date; entries with no parseable date are kept (treated as "just now") so a
  feed that omits timestamps still contributes.
- Dedupe by normalized URL then title (first occurrence wins, so the
  highest-priority feed in the list keeps the canonical entry).
- Cap per topic to ``per_topic_cap`` (default 15) by recency descending — the
  digest stays a bounded, podcast-readable size.

Returns ``{"macro": [Article, ...], "tech": [Article, ...]}`` where each
Article is a small dict ``{"title","source","published","excerpt","url"}``
(``published`` is UTC ISO-8601 with a trailing ``Z``, or ``""`` when the feed
omitted a date).

The feed URLs below were verified live (HTTP 200 + non-empty feedparser parse)
on 2026-06-15. They are all free, no-auth, broadly stable publisher feeds.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ── Curated feeds ──────────────────────────────────────────────────────
#
# (display_source_name, feed_url). Order matters: on a URL/title dedupe the
# EARLIER feed wins, so list higher-signal publishers first. All verified
# live 2026-06-15 (HTTP 200 + feedparser parse with timestamped entries).
TOPIC_FEEDS: dict[str, list[tuple[str, str]]] = {
    "macro": [
        # CNBC "Top News" — high-frequency business/markets/economy headlines
        # (~29 fresh items within 24h). Chosen over CNBC "Economy" (id
        # 20910258) and MarketWatch MarketPulse, which are LOW-FREQUENCY
        # topical feeds whose newest items lag 3+ days — outside the 24h
        # lookback (verified 2026-06-15). NOTE: CNBC rejects feedparser's
        # default UA (returns 0); _BROWSER_UA below is required.
        ("CNBC", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
        # CNBC "Finance" — markets/finance depth.
        ("CNBC Finance", "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
        # NPR "Business/Economy" — clean macro supplement.
        ("NPR", "https://feeds.npr.org/1006/rss.xml"),
    ],
    "tech": [
        ("TechCrunch", "https://techcrunch.com/feed/"),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/index"),
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
    ],
}

DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_PER_TOPIC_CAP = 15

# Cap raw entries pulled per feed BEFORE filtering — bound the work even if a
# feed is unusually large. Mirrors yahoo_rss.py's per-feed [:50] slice.
_PER_FEED_ENTRY_CAP = 50

# Some feeds (notably CNBC) reject feedparser's default User-Agent and return
# an empty body — passing a browser-like UA is required or the feed silently
# yields 0 entries (the CNBC-Economy feed returned nothing in production until
# this was set). Harmless for feeds that don't care.
_BROWSER_UA = "Mozilla/5.0 (compatible; alpha-engine-news/1.0; +https://nousergon.ai)"


def _iso_z(dt: datetime) -> str:
    """UTC ISO-8601 with a trailing ``Z`` (matches the artifact sidecars)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_url(url: str) -> str:
    """Cheap dedupe key — strip a trailing slash + lowercase the scheme/host
    portion is overkill here; we only need stable equality across the same
    feed re-listing a story, so a strip + lower is sufficient."""
    return url.strip().rstrip("/").lower()


def _parse_entry(
    entry: Any,
    *,
    source: str,
    cutoff: datetime,
) -> dict[str, str] | None:
    """Map one feedparser entry to the small Article dict, applying the
    recency filter. Returns ``None`` when the entry is too old or malformed.

    Entries with NO parseable published date are KEPT (published="") — a feed
    that omits timestamps should still contribute rather than be silently
    dropped.
    """
    try:
        url = (entry.get("link") or "").strip()
        title = (entry.get("title") or "").strip()
        if not url or not title:
            return None

        pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if pub_struct:
            published_dt = datetime(*pub_struct[:6], tzinfo=timezone.utc)
            if published_dt < cutoff:
                return None
            published = _iso_z(published_dt)
        else:
            # No date → keep, but mark unknown (sorts last in recency order).
            published = ""

        excerpt = (entry.get("summary") or entry.get("description") or "").strip()
        return {
            "title": title,
            "source": source,
            "published": published,
            "excerpt": excerpt,
            "url": url,
        }
    except Exception as e:  # noqa: BLE001 — schema drift on one entry must not kill the feed
        logger.warning("[topic_news] schema drift on entry from %s: %s", source, e)
        return None


def _fetch_feed(
    feedparser_module: Any,
    *,
    source: str,
    url: str,
    cutoff: datetime,
) -> list[dict[str, str]]:
    """Fetch + parse a single feed. Fail-soft: any error → WARN + ``[]`` so a
    dead feed never kills the topic."""
    try:
        # agent= sends a browser-like UA; some feeds (CNBC) return an empty
        # body to feedparser's default UA. feedparser accepts `agent` kwarg.
        feed = feedparser_module.parse(url, agent=_BROWSER_UA)
    except Exception as e:  # noqa: BLE001 — network/parse failure is per-feed fail-soft
        logger.warning("[topic_news] feed fetch failed for %s (%s): %s", source, url, e)
        return []

    out: list[dict[str, str]] = []
    for entry in (getattr(feed, "entries", None) or [])[:_PER_FEED_ENTRY_CAP]:
        article = _parse_entry(entry, source=source, cutoff=cutoff)
        if article is not None:
            out.append(article)
    logger.info("[topic_news] %s: %d entries within %s", source, len(out), url)
    return out


def _dedupe(articles: list[dict[str, str]]) -> list[dict[str, str]]:
    """Dedupe by normalized URL, then by title (first occurrence wins)."""
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    out: list[dict[str, str]] = []
    for a in articles:
        ukey = _normalize_url(a["url"])
        tkey = a["title"].strip().lower()
        if ukey in seen_urls or tkey in seen_titles:
            continue
        seen_urls.add(ukey)
        seen_titles.add(tkey)
        out.append(a)
    return out


def _by_recency_desc(articles: list[dict[str, str]]) -> list[dict[str, str]]:
    """Sort newest-first. Empty ``published`` (unknown date) sorts last."""
    return sorted(articles, key=lambda a: a.get("published") or "", reverse=True)


def fetch_topic(
    topic: str,
    *,
    hours: int = DEFAULT_LOOKBACK_HOURS,
    per_topic_cap: int = DEFAULT_PER_TOPIC_CAP,
    feedparser_module: Any = None,
) -> list[dict[str, str]]:
    """Fetch one topic's curated feeds → deduped, recency-capped Article list.

    ``feedparser_module`` is injectable for testing — production leaves it
    None and the module is lazy-imported.
    """
    feeds = TOPIC_FEEDS.get(topic)
    if not feeds:
        logger.warning("[topic_news] unknown topic %r — no feeds configured", topic)
        return []

    if feedparser_module is None:
        import feedparser

        feedparser_module = feedparser

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    collected: list[dict[str, str]] = []
    for source, url in feeds:
        collected.extend(
            _fetch_feed(feedparser_module, source=source, url=url, cutoff=cutoff)
        )

    deduped = _dedupe(collected)
    ordered = _by_recency_desc(deduped)
    capped = ordered[:per_topic_cap]
    logger.info(
        "[topic_news] topic=%s collected=%d deduped=%d capped=%d",
        topic, len(collected), len(deduped), len(capped),
    )
    return capped


def fetch_topics(
    topics: list[str] | None = None,
    *,
    hours: int = DEFAULT_LOOKBACK_HOURS,
    per_topic_cap: int = DEFAULT_PER_TOPIC_CAP,
    feedparser_module: Any = None,
) -> dict[str, list[dict[str, str]]]:
    """Fetch all requested topics (default: ``macro`` + ``tech``).

    Each topic is independently fail-soft: a topic whose feeds are all down
    returns ``[]`` for that key without affecting the others. The whole call
    never raises on network/parse failure — the worst case is an all-empty
    result, which the digest builder treats as "no topic news today".
    """
    if topics is None:
        topics = ["macro", "tech"]
    return {
        topic: fetch_topic(
            topic,
            hours=hours,
            per_topic_cap=per_topic_cap,
            feedparser_module=feedparser_module,
        )
        for topic in topics
    }
