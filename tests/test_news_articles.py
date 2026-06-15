"""Tests for the raw per-article daily news writer (companion to the
per-(ticker, date) aggregate).

Covers:
  - One row per canonical (deduped) story
  - Multi-ticker story stays a single row carrying its full ticker tuple
  - Primary source = highest-trust variant; sources/tags unioned
  - Per-article LM sentiment + event rollup (top description by severity)
  - body_excerpt falls back across variants
  - Newest-first ordering
  - Empty input → well-formed empty DataFrame
  - S3 parquet write + read round-trip + missing-key empty schema
  - Schema version pinned to column
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from io import BytesIO

import pytest

from alpha_engine_lib.sources import NewsArticle

from collectors.news_aggregator import (
    AggregatedNewsArticle,
    DEFAULT_TRUST_WEIGHTS,
    NewsAggregator,
)
from collectors.nlp.pipeline import NewsNLPOutput
from collectors.nlp.protocols import EventFlag, SentimentScore

from data.derived.news_articles import (
    SCHEMA_VERSION,
    NewsArticleRecord,
    articles_build_and_write,
    build_news_articles_df,
    read_news_articles_parquet,
    write_news_articles_parquet,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_variant(
    source: str,
    *,
    title: str = "t",
    url: str = "https://x/1",
    body: str = "body",
    published_at: datetime | None = None,
    tags: tuple[str, ...] = (),
    authors: tuple[str, ...] | None = None,
) -> NewsArticle:
    return NewsArticle(
        tickers=("AAPL",),
        title=title,
        body_excerpt=body,
        url=url,
        published_at=published_at or _now(),
        source=source,
        fetched_at=_now(),
        headline_authors=authors,
        tags=tags,
    )


def _make_aggregated(
    *,
    fingerprint: str,
    tickers: tuple[str, ...] = ("AAPL",),
    sources: tuple[str, ...] = ("polygon",),
    title: str = "Story",
    url: str = "https://x.com/a",
    published_at: datetime | None = None,
    variant_kwargs: dict | None = None,
) -> AggregatedNewsArticle:
    vk = variant_kwargs or {}
    variants = tuple(_make_variant(s, title=title, **vk) for s in sources)
    return AggregatedNewsArticle(
        canonical_title=title,
        canonical_url=url,
        tickers=tickers,
        earliest_published_at=published_at or _now(),
        variants=variants,
        canonical_fingerprint=fingerprint,
    )


def _lm_score(fp: str, *, composite: float, pos: int = 0, neg: int = 0, total: int = 10) -> SentimentScore:
    return SentimentScore(
        scorer="loughran_mcdonald",
        article_fingerprint=fp,
        composite=composite,
        positive_word_count=pos,
        negative_word_count=neg,
        uncertainty_word_count=0,
        total_token_count=total,
    )


def _event(fp: str, *, category="earnings_release", description="Q4 results.", severity=0.5) -> EventFlag:
    return EventFlag(
        extractor="rule_based",
        article_fingerprint=fp,
        category=category,
        description=description,
        tickers=(),
        severity=severity,
        extracted_at=_now(),
    )


class TestBuildNewsArticlesDf:
    def test_one_story_one_row(self):
        articles = [_make_aggregated(fingerprint="fp1", tickers=("AAPL",))]
        out = NewsNLPOutput(
            sentiment_scores=[_lm_score("fp1", composite=0.5, pos=3)],
            event_flags=[_event("fp1", severity=0.7, description="Big news.")],
        )
        df = build_news_articles_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        assert len(df) == 1
        row = df.iloc[0]
        assert row["title"] == "Story"
        assert row["url"] == "https://x.com/a"
        assert row["aggregate_date"] == date(2026, 5, 13)
        assert row["schema_version"] == SCHEMA_VERSION
        assert json.loads(row["tickers_json"]) == ["AAPL"]
        assert row["n_tickers"] == 1
        assert row["lm_sentiment"] == pytest.approx(0.5)
        assert row["lm_positive_words"] == 3
        assert row["event_count"] == 1
        assert row["event_severity_max"] == 0.7
        assert row["top_event_description"] == "Big news."

    def test_multi_ticker_story_is_single_row(self):
        articles = [_make_aggregated(
            fingerprint="fp1", tickers=("AAPL", "MSFT", "GOOGL"),
        )]
        df = build_news_articles_df(
            articles=articles, nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        assert len(df) == 1
        assert json.loads(df.iloc[0]["tickers_json"]) == ["AAPL", "MSFT", "GOOGL"]
        assert df.iloc[0]["n_tickers"] == 3

    def test_primary_source_is_highest_trust(self):
        # polygon (0.9) outranks yahoo_rss (0.5)
        articles = [_make_aggregated(
            fingerprint="a", sources=("yahoo_rss", "polygon"),
        )]
        agg = NewsAggregator(sources=[])
        df = build_news_articles_df(
            articles=articles, nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13), aggregator=agg,
        )
        row = df.iloc[0]
        assert row["primary_source"] == "polygon"
        assert json.loads(row["sources_json"]) == ["polygon", "yahoo_rss"]
        assert row["n_sources"] == 2
        assert row["trust_weight_max"] == pytest.approx(DEFAULT_TRUST_WEIGHTS["polygon"])

    def test_tags_unioned_sorted(self):
        articles = [_make_aggregated(
            fingerprint="a", sources=("polygon", "gdelt"),
            variant_kwargs={"tags": ("earnings", "guidance")},
        )]
        df = build_news_articles_df(
            articles=articles, nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        assert json.loads(df.iloc[0]["tags_json"]) == ["earnings", "guidance"]

    def test_body_excerpt_falls_back_to_nonempty_variant(self):
        # Highest-trust variant has empty body; lower-trust has text.
        v_hi = _make_variant("polygon", body="   ")
        v_lo = _make_variant("yahoo_rss", body="real summary text")
        art = AggregatedNewsArticle(
            canonical_title="t", canonical_url="https://x/1",
            tickers=("AAPL",), earliest_published_at=_now(),
            variants=(v_hi, v_lo), canonical_fingerprint="a",
        )
        agg = NewsAggregator(sources=[])
        df = build_news_articles_df(
            articles=[art], nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13), aggregator=agg,
        )
        assert df.iloc[0]["body_excerpt"] == "real summary text"

    def test_top_event_description_is_highest_severity(self):
        articles = [_make_aggregated(fingerprint="a")]
        out = NewsNLPOutput(event_flags=[
            _event("a", description="minor", severity=0.2),
            _event("a", description="major", severity=0.9),
        ])
        df = build_news_articles_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        row = df.iloc[0]
        assert row["event_count"] == 2
        assert row["top_event_description"] == "major"
        assert row["event_severity_max"] == 0.9

    def test_newest_first_ordering(self):
        old = _make_aggregated(
            fingerprint="old", title="old",
            published_at=datetime(2026, 5, 13, 9, 0, tzinfo=timezone.utc),
        )
        new = _make_aggregated(
            fingerprint="new", title="new",
            published_at=datetime(2026, 5, 13, 15, 0, tzinfo=timezone.utc),
        )
        df = build_news_articles_df(
            articles=[old, new], nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        assert df.iloc[0]["title"] == "new"
        assert df.iloc[1]["title"] == "old"

    def test_published_at_is_iso_z(self):
        art = _make_aggregated(
            fingerprint="a",
            published_at=datetime(2026, 5, 13, 15, 30, tzinfo=timezone.utc),
        )
        df = build_news_articles_df(
            articles=[art], nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        assert df.iloc[0]["published_at"] == "2026-05-13T15:30:00Z"

    def test_no_sentiment_defaults_zero(self):
        df = build_news_articles_df(
            articles=[_make_aggregated(fingerprint="a")],
            nlp_output=NewsNLPOutput(), aggregate_date=date(2026, 5, 13),
        )
        assert df.iloc[0]["lm_sentiment"] == 0.0
        assert df.iloc[0]["event_count"] == 0
        assert df.iloc[0]["top_event_description"] == ""

    def test_empty_articles_produces_empty_df_with_schema(self):
        df = build_news_articles_df(
            articles=[], nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        assert len(df) == 0
        for col in NewsArticleRecord.__dataclass_fields__:
            assert col in df.columns


# ── S3 round-trip (in-memory mock; no moto dep) ────────────────────────


class _InMemoryS3:
    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self._store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise KeyError(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": BytesIO(self._store[(Bucket, Key)])}


class TestS3ParquetRoundTrip:
    def test_write_then_read_preserves_rows(self):
        s3 = _InMemoryS3()
        articles = [_make_aggregated(fingerprint="a", tickers=("AAPL", "MSFT"))]
        out = NewsNLPOutput(sentiment_scores=[_lm_score("a", composite=0.42)])
        df_in = build_news_articles_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        key = write_news_articles_parquet(
            df_in, aggregate_date=date(2026, 5, 13), s3_client=s3,
            run_id="2605131934",
        )
        assert key == "data/news_articles_daily/2605131934_articles.parquet"
        assert ("alpha-engine-research", "data/news_articles_daily/latest.json") in s3._store

        df_out = read_news_articles_parquet(s3_client=s3)
        assert len(df_out) == 1
        assert json.loads(df_out.iloc[0]["tickers_json"]) == ["AAPL", "MSFT"]
        assert df_out.iloc[0]["lm_sentiment"] == pytest.approx(0.42)

    def test_missing_parquet_returns_empty_schema_df(self):
        s3 = _InMemoryS3()
        df = read_news_articles_parquet(s3_client=s3)
        assert len(df) == 0
        for col in NewsArticleRecord.__dataclass_fields__:
            assert col in df.columns

    def test_sidecar_payload_shape(self):
        s3 = _InMemoryS3()
        df_in = build_news_articles_df(
            articles=[_make_aggregated(fingerprint="a")],
            nlp_output=NewsNLPOutput(), aggregate_date=date(2026, 5, 13),
        )
        write_news_articles_parquet(
            df_in, aggregate_date=date(2026, 5, 13), s3_client=s3,
            run_id="2605131934",
        )
        sidecar = json.loads(
            s3._store[("alpha-engine-research", "data/news_articles_daily/latest.json")]
        )
        assert sidecar["run_id"] == "2605131934"
        assert sidecar["artifact_key"] == "data/news_articles_daily/2605131934_articles.parquet"
        assert sidecar["aggregate_date"] == "2026-05-13"
        assert sidecar["schema_version"] == SCHEMA_VERSION
        assert sidecar["row_count"] == 1


class TestArticlesBuildAndWrite:
    def test_end_to_end_returns_key_and_df(self):
        s3 = _InMemoryS3()
        articles = [_make_aggregated(fingerprint="a")]
        agg = NewsAggregator(sources=[])
        key, df = articles_build_and_write(
            articles=articles, nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13), aggregator=agg, s3_client=s3,
        )
        assert key.startswith("data/news_articles_daily/")
        assert key.endswith("_articles.parquet")
        assert len(df) == 1

    def test_accepts_datetime_for_aggregate_date(self):
        s3 = _InMemoryS3()
        key, _ = articles_build_and_write(
            articles=[], nlp_output=NewsNLPOutput(),
            aggregate_date=datetime(2026, 5, 13, 12, tzinfo=timezone.utc),
            aggregator=None, s3_client=s3,
        )
        assert key.startswith("data/news_articles_daily/")


def test_schema_version_pinned():
    assert isinstance(SCHEMA_VERSION, int)
    assert SCHEMA_VERSION == 1
