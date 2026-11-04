"""Tests for data/derived/news_digest.py — the podcast-ready combined digest.

Asserts the digest CONTRACT shape exactly, the portfolio-section derivation
+ newsworthiness selection + cap, fail-soft empty topics, and the
S3 write (dated history object + full latest.json) round-trip.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from io import BytesIO

import pandas as pd
import pytest

from data.derived.news_articles import build_news_articles_df
from data.derived.news_digest import (
    DEFAULT_S3_PREFIX,
    SCHEMA_VERSION,
    build_digest,
    read_digest,
    write_digest,
)

from collectors.news_aggregator import AggregatedNewsArticle, NewsAggregator
from collectors.nlp.pipeline import NewsNLPOutput
from collectors.nlp.protocols import SentimentScore

from nousergon_lib.sources import NewsArticle


# ── helpers (mirror tests/test_news_articles.py) ───────────────────────


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _variant(source: str, *, body="body", url="https://x/1", title="t") -> NewsArticle:
    return NewsArticle(
        tickers=("AAPL",), title=title, body_excerpt=body, url=url,
        published_at=_now(), source=source, fetched_at=_now(),
        headline_authors=None, tags=(),
    )


def _aggregated(*, fingerprint, tickers=("AAPL",), sources=("polygon",),
                title="Story", url="https://x.com/a", published_at=None):
    variants = tuple(_variant(s, title=title) for s in sources)
    return AggregatedNewsArticle(
        canonical_title=title, canonical_url=url, tickers=tickers,
        earliest_published_at=published_at or _now(),
        variants=variants, canonical_fingerprint=fingerprint,
    )


def _lm(fp, *, composite):
    return SentimentScore(
        scorer="loughran_mcdonald", article_fingerprint=fp, composite=composite,
        positive_word_count=0, negative_word_count=0,
        uncertainty_word_count=0, total_token_count=10,
    )


def _articles_df(articles, nlp=None):
    return build_news_articles_df(
        articles=articles, nlp_output=nlp or NewsNLPOutput(),
        aggregate_date=date(2026, 5, 13), aggregator=NewsAggregator(sources=[]),
    )


_TOPICS = {
    "macro": [{"title": "Fed holds", "source": "CNBC", "published": "2026-05-13T12:00:00Z",
               "excerpt": "No change.", "url": "https://x/fed"}],
    "tech": [{"title": "New chip", "source": "TechCrunch", "published": "2026-05-13T13:00:00Z",
              "excerpt": "Fast.", "url": "https://x/chip"}],
}


# ── contract shape ─────────────────────────────────────────────────────


class TestBuildDigestContract:
    def test_top_level_contract_keys(self):
        df = _articles_df([_aggregated(fingerprint="a")])
        d = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        assert d["schema_version"] == SCHEMA_VERSION
        assert d["date"] == "2026-05-13"
        assert d["generated_at"].endswith("Z")
        assert set(d["sections"].keys()) == {"portfolio", "macro", "tech"}

    def test_portfolio_entry_shape(self):
        df = _articles_df(
            [_aggregated(fingerprint="a", tickers=("AAPL",))],
            nlp=NewsNLPOutput(sentiment_scores=[_lm("a", composite=-0.12)]),
        )
        d = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        p = d["sections"]["portfolio"]
        assert len(p) == 1
        assert set(p[0].keys()) == {
            "id", "ticker", "title", "source", "published", "excerpt", "sentiment", "url",
        }
        assert p[0]["ticker"] == "AAPL"
        assert p[0]["sentiment"] == pytest.approx(-0.12)

    def test_macro_tech_sections_passthrough(self):
        df = _articles_df([_aggregated(fingerprint="a")])
        d = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        for section in ("macro", "tech"):
            got = [{k: v for k, v in e.items() if k != "id"} for e in d["sections"][section]]
            assert got == _TOPICS[section]
        # Topic entries must NOT carry a ticker/sentiment field.
        assert set(d["sections"]["macro"][0].keys()) == {
            "id", "title", "source", "published", "excerpt", "url",
        }
        # The caller's topic dicts are not mutated.
        assert "id" not in _TOPICS["macro"][0]

    def test_multi_ticker_story_expands_per_ticker(self):
        df = _articles_df([_aggregated(fingerprint="a", tickers=("AAPL", "MSFT"))])
        d = build_digest(articles_df=df, topics={}, digest_date=date(2026, 5, 13))
        tickers = {e["ticker"] for e in d["sections"]["portfolio"]}
        assert tickers == {"AAPL", "MSFT"}


class TestItemIds:
    """Every item carries an ``id`` unique within its section, safe in a URL
    path, and stable across re-runs (the console lists one row per item)."""

    def test_ids_are_unique_even_when_stories_repeat(self):
        same = {"title": "Same", "source": "CNBC", "published": "2026-05-13T12:00:00Z",
                "excerpt": "x", "url": "https://x/a?b=1#c"}
        topics = {"macro": [same, dict(same), dict(same, title="Other")], "tech": [same]}
        df = _articles_df([_aggregated(fingerprint="a", tickers=("AAPL", "MSFT"))])
        d = build_digest(articles_df=df, topics=topics, digest_date=date(2026, 5, 13))
        for section, items in d["sections"].items():
            ids = [e["id"] for e in items]
            assert len(ids) == len(set(ids)), section
            for i in ids:
                assert re.fullmatch(r"[0-9a-f]{12}(-\d+)?", i), i
        macro = [e["id"] for e in d["sections"]["macro"]]
        assert macro[1] == f"{macro[0]}-2"

    def test_ids_are_stable_across_runs_and_differ_by_section(self):
        df = _articles_df([_aggregated(fingerprint="a")])
        one = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        two = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        assert one["sections"] == two["sections"]
        topics = {"macro": _TOPICS["macro"], "tech": _TOPICS["macro"]}
        d = build_digest(articles_df=df, topics=topics, digest_date=date(2026, 5, 13))
        assert d["sections"]["macro"][0]["id"] != d["sections"]["tech"][0]["id"]


class TestPortfolioSelection:
    def test_ranked_by_abs_sentiment_then_recency(self):
        arts = [
            _aggregated(fingerprint="neutral", title="neutral", tickers=("A",)),
            _aggregated(fingerprint="bad", title="bad", tickers=("B",)),
            _aggregated(fingerprint="good", title="good", tickers=("C",)),
        ]
        nlp = NewsNLPOutput(sentiment_scores=[
            _lm("neutral", composite=0.0),
            _lm("bad", composite=-0.9),
            _lm("good", composite=0.5),
        ])
        df = _articles_df(arts, nlp=nlp)
        d = build_digest(articles_df=df, topics={}, digest_date=date(2026, 5, 13))
        titles = [e["title"] for e in d["sections"]["portfolio"]]
        # |−0.9| > |0.5| > |0.0|
        assert titles == ["bad", "good", "neutral"]

    def test_portfolio_cap(self):
        arts = [
            _aggregated(fingerprint=f"fp{i}", title=f"t{i}", tickers=(f"T{i}",),
                        url=f"https://x/{i}")
            for i in range(10)
        ]
        df = _articles_df(arts)
        d = build_digest(articles_df=df, topics={}, digest_date=date(2026, 5, 13),
                         portfolio_cap=4)
        assert len(d["sections"]["portfolio"]) == 4

    def test_portfolio_excludes_gdelt_source(self):
        # GDELT keyword-matches tickers → false positives; the portfolio section
        # must use only ticker-accurate sources (Polygon/Yahoo), never GDELT.
        df = _articles_df([
            _aggregated(fingerprint="g", sources=("gdelt",),
                        title="GDELT false positive", url="https://x/g"),
            _aggregated(fingerprint="p", sources=("polygon",),
                        title="Polygon real", url="https://x/p"),
        ])
        d = build_digest(articles_df=df, topics={}, digest_date=date(2026, 5, 13))
        titles = [e["title"] for e in d["sections"]["portfolio"]]
        assert "Polygon real" in titles
        assert "GDELT false positive" not in titles
        assert all(e["source"].lower() != "gdelt" for e in d["sections"]["portfolio"])

    def test_portfolio_all_gdelt_yields_empty(self):
        df = _articles_df([
            _aggregated(fingerprint="g1", sources=("gdelt",), title="noise1", url="https://x/g1"),
            _aggregated(fingerprint="g2", sources=("gdelt",), title="noise2", url="https://x/g2"),
        ])
        d = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        assert d["sections"]["portfolio"] == []
        assert len(d["sections"]["macro"]) == 1  # topics unaffected

    def test_empty_articles_empty_portfolio(self):
        df = _articles_df([])
        d = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        assert d["sections"]["portfolio"] == []
        assert len(d["sections"]["macro"]) == 1  # topics still present


class TestTopicFailSoft:
    def test_none_topics_yields_empty_macro_tech_but_full_portfolio(self):
        df = _articles_df([_aggregated(fingerprint="a", tickers=("AAPL",))])
        d = build_digest(articles_df=df, topics=None, digest_date=date(2026, 5, 13))
        assert d["sections"]["macro"] == []
        assert d["sections"]["tech"] == []
        assert len(d["sections"]["portfolio"]) == 1

    def test_partial_topics_missing_key_is_empty(self):
        df = _articles_df([_aggregated(fingerprint="a")])
        d = build_digest(articles_df=df, topics={"macro": _TOPICS["macro"]},
                         digest_date=date(2026, 5, 13))
        assert len(d["sections"]["macro"]) == 1
        assert d["sections"]["tech"] == []


# ── S3 round-trip (in-memory mock; no moto dep) ────────────────────────


class _InMemoryS3:
    def __init__(self):
        self._store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self._store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise KeyError(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": BytesIO(self._store[(Bucket, Key)])}


class TestWriteReadDigest:
    def test_write_both_dated_and_latest_full_digest(self):
        s3 = _InMemoryS3()
        df = _articles_df([_aggregated(fingerprint="a")])
        digest = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        key = write_digest(digest, s3_client=s3, run_id="2605131934")

        assert key == "data/news_digest_daily/2605131934_digest.json"
        latest_key = "data/news_digest_daily/latest.json"
        assert ("alpha-engine-research", key) in s3._store
        assert ("alpha-engine-research", latest_key) in s3._store

        # latest.json holds the FULL digest, not a pointer.
        latest = json.loads(s3._store[("alpha-engine-research", latest_key)])
        assert latest["schema_version"] == SCHEMA_VERSION
        assert "sections" in latest
        assert latest["sections"]["macro"] == digest["sections"]["macro"]
        assert [e["title"] for e in latest["sections"]["macro"]] == [e["title"] for e in _TOPICS["macro"]]
        # Dated history object is byte-identical to latest.
        dated = json.loads(s3._store[("alpha-engine-research", key)])
        assert dated == latest

    def test_read_digest_returns_full_payload(self):
        s3 = _InMemoryS3()
        df = _articles_df([_aggregated(fingerprint="a", tickers=("AAPL",))])
        digest = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        write_digest(digest, s3_client=s3, run_id="2605131934")

        out = read_digest(s3_client=s3)
        assert out["date"] == "2026-05-13"
        assert out["sections"]["portfolio"][0]["ticker"] == "AAPL"

    def test_read_missing_digest_returns_empty_schema(self):
        s3 = _InMemoryS3()
        out = read_digest(s3_client=s3)
        assert out["schema_version"] == SCHEMA_VERSION
        assert out["sections"] == {"portfolio": [], "macro": [], "tech": []}
        assert out["date"] is None


def test_default_prefix_is_digest_daily():
    assert DEFAULT_S3_PREFIX == "data/news_digest_daily"


def test_schema_version_pinned():
    assert SCHEMA_VERSION == 1
