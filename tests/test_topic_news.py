"""Tests for collectors/topic_news.py — curated macro/tech RSS topic fetcher.

No live network: a fake feedparser module is injected. Covers parsing of the
small Article dict, the recency filter, URL/title dedupe, the per-topic cap,
and per-feed fail-soft (one dead feed must not kill the topic).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from collectors import topic_news


def _struct(dt: datetime) -> time.struct_time:
    """A feedparser-style ``*_parsed`` value (UTC struct_time)."""
    return dt.astimezone(timezone.utc).timetuple()


class _Entry(dict):
    """feedparser entries support both attribute and .get() access; the code
    under test uses .get(), so a plain dict suffices."""


def _entry(
    *,
    title="A headline",
    link="https://example.com/a",
    summary="An excerpt.",
    published: datetime | None = None,
    no_date: bool = False,
) -> dict:
    e = {"title": title, "link": link, "summary": summary}
    if not no_date and published is not None:
        e["published_parsed"] = _struct(published)
    return e


class _FakeFeed:
    def __init__(self, entries):
        self.entries = entries
        self.bozo = False


class _FakeFeedparser:
    """Maps URL → list of entries (or an exception to simulate a dead feed)."""

    def __init__(self, by_url):
        self._by_url = by_url
        self.calls: list[str] = []

    def parse(self, url):
        self.calls.append(url)
        val = self._by_url.get(url)
        if isinstance(val, Exception):
            raise val
        return _FakeFeed(val or [])


def _macro_urls() -> list[str]:
    return [url for _, url in topic_news.TOPIC_FEEDS["macro"]]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_fetch_topic_parses_article_dict_shape():
    urls = _macro_urls()
    fp = _FakeFeedparser({
        urls[0]: [_entry(title="Fed holds rates", link="https://x/fed",
                         summary="No change.", published=_now())],
    })
    out = topic_news.fetch_topic("macro", feedparser_module=fp)
    assert len(out) == 1
    a = out[0]
    assert set(a.keys()) == {"title", "source", "published", "excerpt", "url"}
    assert a["title"] == "Fed holds rates"
    assert a["source"] == "CNBC"  # first macro feed's display name
    assert a["url"] == "https://x/fed"
    assert a["excerpt"] == "No change."
    assert a["published"].endswith("Z")


def test_recency_filter_drops_old_entries():
    urls = _macro_urls()
    old = _now() - timedelta(hours=48)
    fresh = _now() - timedelta(hours=1)
    fp = _FakeFeedparser({
        urls[0]: [
            _entry(title="old", link="https://x/old", published=old),
            _entry(title="fresh", link="https://x/fresh", published=fresh),
        ],
    })
    out = topic_news.fetch_topic("macro", hours=24, feedparser_module=fp)
    titles = [a["title"] for a in out]
    assert titles == ["fresh"]


def test_entry_without_date_is_kept_with_empty_published():
    urls = _macro_urls()
    fp = _FakeFeedparser({
        urls[0]: [_entry(title="undated", link="https://x/u", no_date=True)],
    })
    out = topic_news.fetch_topic("macro", feedparser_module=fp)
    assert len(out) == 1
    assert out[0]["published"] == ""


def test_dedupe_by_url_first_feed_wins():
    urls = _macro_urls()
    # Same URL appears in two feeds; the earlier feed's entry wins.
    fp = _FakeFeedparser({
        urls[0]: [_entry(title="from CNBC", link="https://x/dup", published=_now())],
        urls[1]: [_entry(title="from MW", link="https://x/dup/", published=_now())],
    })
    out = topic_news.fetch_topic("macro", feedparser_module=fp)
    assert len(out) == 1
    assert out[0]["source"] == "CNBC"


def test_dedupe_by_title():
    urls = _macro_urls()
    fp = _FakeFeedparser({
        urls[0]: [_entry(title="Same Story", link="https://x/1", published=_now())],
        urls[1]: [_entry(title="same story", link="https://x/2", published=_now())],
    })
    out = topic_news.fetch_topic("macro", feedparser_module=fp)
    assert len(out) == 1


def test_per_topic_cap_keeps_most_recent():
    urls = _macro_urls()
    base = _now()
    entries = [
        _entry(title=f"h{i}", link=f"https://x/{i}", published=base - timedelta(minutes=i))
        for i in range(20)
    ]
    fp = _FakeFeedparser({urls[0]: entries})
    out = topic_news.fetch_topic("macro", per_topic_cap=5, feedparser_module=fp)
    assert len(out) == 5
    # Newest-first: h0 (most recent) … h4
    assert [a["title"] for a in out] == ["h0", "h1", "h2", "h3", "h4"]


def test_dead_feed_is_fail_soft_does_not_kill_topic():
    urls = _macro_urls()
    fp = _FakeFeedparser({
        urls[0]: RuntimeError("network down"),
        urls[1]: [_entry(title="survivor", link="https://x/s", published=_now())],
    })
    out = topic_news.fetch_topic("macro", feedparser_module=fp)
    # The dead feed[0] was skipped; feed[1]'s entry survived.
    assert [a["title"] for a in out] == ["survivor"]
    assert urls[0] in fp.calls and urls[1] in fp.calls


def test_entry_missing_url_or_title_skipped():
    urls = _macro_urls()
    fp = _FakeFeedparser({
        urls[0]: [
            {"title": "no link", "summary": "s"},          # no link
            {"link": "https://x/notitle", "summary": "s"},  # no title
            _entry(title="ok", link="https://x/ok", published=_now()),
        ],
    })
    out = topic_news.fetch_topic("macro", feedparser_module=fp)
    assert [a["title"] for a in out] == ["ok"]


def test_unknown_topic_returns_empty():
    fp = _FakeFeedparser({})
    assert topic_news.fetch_topic("sports", feedparser_module=fp) == []
    assert fp.calls == []  # no feeds configured → no fetch


def test_fetch_topics_returns_both_topics():
    macro_urls = [u for _, u in topic_news.TOPIC_FEEDS["macro"]]
    tech_urls = [u for _, u in topic_news.TOPIC_FEEDS["tech"]]
    fp = _FakeFeedparser({
        macro_urls[0]: [_entry(title="macro1", link="https://x/m1", published=_now())],
        tech_urls[0]: [_entry(title="tech1", link="https://x/t1", published=_now())],
    })
    out = topic_news.fetch_topics(feedparser_module=fp)
    assert set(out.keys()) == {"macro", "tech"}
    assert [a["title"] for a in out["macro"]] == ["macro1"]
    assert [a["title"] for a in out["tech"]] == ["tech1"]


def test_fetch_topics_topic_with_all_feeds_down_is_empty_not_raising():
    macro_urls = [u for _, u in topic_news.TOPIC_FEEDS["macro"]]
    tech_urls = [u for _, u in topic_news.TOPIC_FEEDS["tech"]]
    by_url = {u: RuntimeError("down") for u in macro_urls}
    by_url[tech_urls[0]] = [_entry(title="tech survives", link="https://x/t", published=_now())]
    fp = _FakeFeedparser(by_url)
    out = topic_news.fetch_topics(feedparser_module=fp)
    assert out["macro"] == []                       # all macro feeds down → empty
    assert [a["title"] for a in out["tech"]] == ["tech survives"]


def test_curated_feeds_are_https_and_nonempty():
    """Lightweight guard on the hardcoded feed list — every topic has feeds,
    each is an (name, https-url) pair."""
    assert set(topic_news.TOPIC_FEEDS.keys()) == {"macro", "tech"}
    for topic, feeds in topic_news.TOPIC_FEEDS.items():
        assert feeds, f"{topic} has no feeds"
        for name, url in feeds:
            assert name and isinstance(name, str)
            assert url.startswith("https://"), f"{topic} feed {url} not https"
