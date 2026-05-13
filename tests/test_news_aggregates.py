"""Tests for the news structured-aggregates writer (Wave 1 PR A.2).

Covers:
  - Per-ticker aggregation from NewsNLPOutput streams
  - Trust-weighted sentiment mean
  - Event filtering by ticker (event.tickers set vs empty)
  - Multi-ticker article counted under each ticker
  - Top-N event descriptions sorted by severity desc
  - Empty aggregation produces well-formed empty DataFrame
  - S3 parquet write + read round-trip (in-memory moto-style mock)
  - Schema version pinned to column
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from io import BytesIO

import pandas as pd
import pytest

from alpha_engine_lib.sources import NewsArticle

from collectors.news_aggregator import (
    AggregatedNewsArticle,
    DEFAULT_TRUST_WEIGHTS,
    NewsAggregator,
)
from collectors.nlp.pipeline import NewsNLPOutput
from collectors.nlp.protocols import EventFlag, SentimentScore

from data.derived.news_aggregates import (
    DEFAULT_S3_PREFIX,
    SCHEMA_VERSION,
    TOP_EVENT_DESCRIPTIONS_N,
    NewsTickerDailyAggregate,
    aggregate_and_write,
    build_news_aggregates_df,
    read_news_aggregates_parquet,
    write_news_aggregates_parquet,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_variant(source: str, title: str = "t", url: str = "https://x/1") -> NewsArticle:
    return NewsArticle(
        tickers=("AAPL",),
        title=title,
        body_excerpt="body",
        url=url,
        published_at=_now(),
        source=source,
        fetched_at=_now(),
    )


def _make_aggregated(
    *,
    fingerprint: str,
    tickers: tuple[str, ...] = ("AAPL",),
    sources: tuple[str, ...] = ("polygon",),
    title: str = "Story",
) -> AggregatedNewsArticle:
    variants = tuple(_make_variant(s, title=title) for s in sources)
    return AggregatedNewsArticle(
        canonical_title=title,
        canonical_url="https://x.com/a",
        tickers=tickers,
        earliest_published_at=_now(),
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


def _event(
    fp: str,
    *,
    category: str = "earnings_release",
    description: str = "Q4 results released.",
    tickers: tuple[str, ...] = (),
    severity: float = 0.5,
) -> EventFlag:
    return EventFlag(
        extractor="anthropic_haiku",
        article_fingerprint=fp,
        category=category,
        description=description,
        tickers=tickers,
        severity=severity,
        extracted_at=_now(),
    )


# ── Per-ticker aggregation ─────────────────────────────────────────────


class TestBuildNewsAggregatesDf:
    def test_one_article_one_ticker_one_row(self):
        articles = [_make_aggregated(fingerprint="fp1", tickers=("AAPL",))]
        out = NewsNLPOutput(
            sentiment_scores=[_lm_score("fp1", composite=0.5, pos=3, total=10)],
            event_flags=[_event("fp1", severity=0.7)],
        )
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        assert len(df) == 1
        row = df.iloc[0]
        assert row["ticker"] == "AAPL"
        assert row["aggregate_date"] == date(2026, 5, 13)
        assert row["schema_version"] == SCHEMA_VERSION
        assert row["n_articles"] == 1
        assert row["lm_sentiment_mean"] == pytest.approx(0.5)
        assert row["lm_positive_words_total"] == 3
        assert row["event_count"] == 1
        assert row["event_severity_max"] == 0.7

    def test_multi_ticker_article_emits_one_row_per_ticker(self):
        articles = [_make_aggregated(
            fingerprint="fp1", tickers=("AAPL", "MSFT", "GOOGL"),
        )]
        out = NewsNLPOutput(
            sentiment_scores=[_lm_score("fp1", composite=0.3)],
        )
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        assert len(df) == 3
        tickers = sorted(df["ticker"].tolist())
        assert tickers == ["AAPL", "GOOGL", "MSFT"]
        # All three rows reference the same article — sentiment mirrored
        for _, row in df.iterrows():
            assert row["n_articles"] == 1
            assert row["lm_sentiment_mean"] == pytest.approx(0.3)

    def test_multiple_articles_per_ticker_aggregate(self):
        articles = [
            _make_aggregated(fingerprint="a", tickers=("AAPL",)),
            _make_aggregated(fingerprint="b", tickers=("AAPL",)),
            _make_aggregated(fingerprint="c", tickers=("AAPL",)),
        ]
        out = NewsNLPOutput(
            sentiment_scores=[
                _lm_score("a", composite=0.6),
                _lm_score("b", composite=0.0),
                _lm_score("c", composite=-0.3),
            ],
        )
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        assert len(df) == 1
        row = df.iloc[0]
        assert row["n_articles"] == 3
        assert row["lm_sentiment_mean"] == pytest.approx(0.1)
        assert row["lm_sentiment_max"] == pytest.approx(0.6)
        assert row["lm_sentiment_min"] == pytest.approx(-0.3)

    def test_empty_articles_produces_empty_df_with_schema(self):
        df = build_news_aggregates_df(
            articles=[],
            nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        assert len(df) == 0
        # Empty DF still has canonical columns so consumers don't break
        for col in NewsTickerDailyAggregate.__dataclass_fields__:
            assert col in df.columns


class TestTrustWeighting:
    def test_trusted_mean_weights_by_highest_source_trust(self):
        # Two articles: one from polygon (high trust), one from yahoo_rss
        # (low trust). Same sentiment magnitudes — trusted_mean should
        # lean toward the polygon article.
        articles = [
            _make_aggregated(fingerprint="a", tickers=("AAPL",), sources=("polygon",)),
            _make_aggregated(fingerprint="b", tickers=("AAPL",), sources=("yahoo_rss",)),
        ]
        out = NewsNLPOutput(sentiment_scores=[
            _lm_score("a", composite=0.8),
            _lm_score("b", composite=-0.4),
        ])
        agg = NewsAggregator(sources=[])
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out,
            aggregate_date=date(2026, 5, 13), aggregator=agg,
        )
        row = df.iloc[0]
        # Raw mean = (0.8 + (-0.4)) / 2 = 0.2
        assert row["lm_sentiment_mean"] == pytest.approx(0.2)
        # Trusted mean = (0.8 * polygon_trust + -0.4 * yahoo_trust) /
        # (polygon_trust + yahoo_trust)
        polygon_w = DEFAULT_TRUST_WEIGHTS["polygon"]  # 0.9
        yahoo_w = DEFAULT_TRUST_WEIGHTS["yahoo_rss"]  # 0.5
        expected = (0.8 * polygon_w + -0.4 * yahoo_w) / (polygon_w + yahoo_w)
        assert row["lm_sentiment_trusted_mean"] == pytest.approx(expected, rel=1e-3)

    def test_n_articles_trusted_weighted_counts_variants(self):
        # Article 'a' has 2 source variants (polygon + gdelt); the
        # trusted-weighted count should sum BOTH variants' trust weights.
        articles = [_make_aggregated(
            fingerprint="a", tickers=("AAPL",),
            sources=("polygon", "gdelt"),
        )]
        out = NewsNLPOutput()
        agg = NewsAggregator(sources=[])
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out,
            aggregate_date=date(2026, 5, 13), aggregator=agg,
        )
        row = df.iloc[0]
        expected = DEFAULT_TRUST_WEIGHTS["polygon"] + DEFAULT_TRUST_WEIGHTS["gdelt"]
        assert row["n_articles_trusted_weighted"] == pytest.approx(expected)

    def test_source_counts_json_per_variant(self):
        articles = [_make_aggregated(
            fingerprint="a", tickers=("AAPL",),
            sources=("polygon", "gdelt", "gdelt"),  # 1 polygon + 2 gdelt variants
        )]
        df = build_news_aggregates_df(
            articles=articles, nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        counts = json.loads(df.iloc[0]["n_articles_by_source_json"])
        assert counts == {"gdelt": 2, "polygon": 1}


class TestEventAggregation:
    def test_event_with_tickers_set_only_attaches_to_those_tickers(self):
        # Article concerns AAPL + MSFT; event tags only AAPL.
        articles = [_make_aggregated(
            fingerprint="a", tickers=("AAPL", "MSFT"),
        )]
        out = NewsNLPOutput(event_flags=[
            _event("a", tickers=("AAPL",), severity=0.8),
        ])
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        by_ticker = {r["ticker"]: r for _, r in df.iterrows()}
        assert by_ticker["AAPL"]["event_count"] == 1
        assert by_ticker["MSFT"]["event_count"] == 0

    def test_event_without_tickers_inherits_from_article(self):
        # Empty event.tickers means "ungated; applies to whatever
        # article(s) it appeared in".
        articles = [_make_aggregated(
            fingerprint="a", tickers=("AAPL", "MSFT"),
        )]
        out = NewsNLPOutput(event_flags=[_event("a", tickers=(), severity=0.5)])
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        # Both tickers see the event since extractor didn't gate
        for _, row in df.iterrows():
            assert row["event_count"] == 1

    def test_top_event_descriptions_sorted_by_severity_desc(self):
        articles = [_make_aggregated(fingerprint="a", tickers=("AAPL",))]
        out = NewsNLPOutput(event_flags=[
            _event("a", description="low-severity event", severity=0.2),
            _event("a", description="high-severity event", severity=0.95),
            _event("a", description="mid-severity event", severity=0.5),
            _event("a", description="another mid event", severity=0.4),
        ])
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        descs = df.iloc[0]["top_event_descriptions"]
        # Top-N = 3, separated by " | "
        parts = descs.split(" | ")
        assert len(parts) == TOP_EVENT_DESCRIPTIONS_N
        assert parts[0] == "high-severity event"  # highest severity first
        assert "low-severity event" not in descs  # dropped (4th place)

    def test_event_categories_comma_joined_sorted(self):
        articles = [_make_aggregated(fingerprint="a", tickers=("AAPL",))]
        out = NewsNLPOutput(event_flags=[
            _event("a", category="merger_or_acquisition"),
            _event("a", category="earnings_release"),
            _event("a", category="earnings_release"),  # dedup
        ])
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        cats = df.iloc[0]["event_categories"]
        # Sorted + deduped
        assert cats == "earnings_release,merger_or_acquisition"

    def test_event_severity_mean_computed(self):
        articles = [_make_aggregated(fingerprint="a", tickers=("AAPL",))]
        out = NewsNLPOutput(event_flags=[
            _event("a", severity=0.2),
            _event("a", severity=0.8),
        ])
        df = build_news_aggregates_df(
            articles=articles, nlp_output=out, aggregate_date=date(2026, 5, 13),
        )
        assert df.iloc[0]["event_severity_mean"] == pytest.approx(0.5)


# ── S3 parquet round-trip (in-memory mock; no moto dep) ────────────────


class _InMemoryS3:
    """Minimal in-memory S3 mock supporting put_object + get_object.

    Avoids adding moto as a test dep. Sufficient for the round-trip
    + missing-key + overwrite tests we run here.
    """

    class _NoSuchKey(Exception):
        pass

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self._store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise self._NoSuchKey(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": BytesIO(self._store[(Bucket, Key)])}


class TestS3ParquetRoundTrip:
    def test_write_then_read_preserves_rows(self):
        s3 = _InMemoryS3()
        articles = [_make_aggregated(fingerprint="a", tickers=("AAPL",))]
        out = NewsNLPOutput(sentiment_scores=[
            _lm_score("a", composite=0.42, pos=2, neg=0, total=10),
        ])
        df_in = build_news_aggregates_df(
            articles=articles, nlp_output=out,
            aggregate_date=date(2026, 5, 13),
        )
        key = write_news_aggregates_parquet(
            df_in, aggregate_date=date(2026, 5, 13), s3_client=s3,
            run_id="2605131934",  # pin the run_id for deterministic key
        )
        # Canonical shape: YYMMDDHHMM-encoded artifact key
        assert key == "data/news_aggregates/2605131934_result.parquet"
        # latest.json sidecar points at it
        assert ("alpha-engine-research", "data/news_aggregates/latest.json") in s3._store

        df_out = read_news_aggregates_parquet(
            aggregate_date=date(2026, 5, 13), s3_client=s3,
        )
        assert len(df_out) == 1
        assert df_out.iloc[0]["ticker"] == "AAPL"
        assert df_out.iloc[0]["lm_sentiment_mean"] == pytest.approx(0.42)

    def test_missing_parquet_returns_empty_schema_df(self):
        s3 = _InMemoryS3()
        df = read_news_aggregates_parquet(
            aggregate_date=date(2026, 1, 1), s3_client=s3,
        )
        assert len(df) == 0
        for col in NewsTickerDailyAggregate.__dataclass_fields__:
            assert col in df.columns

    def test_overwrite_existing_parquet(self):
        s3 = _InMemoryS3()
        # v1: 1 row
        articles_v1 = [_make_aggregated(fingerprint="a", tickers=("AAPL",))]
        df_v1 = build_news_aggregates_df(
            articles=articles_v1, nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        write_news_aggregates_parquet(
            df_v1, aggregate_date=date(2026, 5, 13), s3_client=s3,
        )
        # v2: 2 rows (overwrite)
        articles_v2 = [
            _make_aggregated(fingerprint="a", tickers=("AAPL",)),
            _make_aggregated(fingerprint="b", tickers=("MSFT",)),
        ]
        df_v2 = build_news_aggregates_df(
            articles=articles_v2, nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
        )
        write_news_aggregates_parquet(
            df_v2, aggregate_date=date(2026, 5, 13), s3_client=s3,
        )
        df_read = read_news_aggregates_parquet(
            aggregate_date=date(2026, 5, 13), s3_client=s3,
        )
        assert len(df_read) == 2

    def test_canonical_artifact_and_latest_keys(self):
        """Canonical shape after migration: YYMMDDHHMM-encoded
        artifact key + latest.json sidecar (both in eval_artifacts lib).
        """
        from alpha_engine_lib.eval_artifacts import (
            eval_artifact_key, eval_latest_key, new_eval_run_id,
        )
        run_id = new_eval_run_id()
        assert len(run_id) == 10  # YYMMDDHHMM
        ak = eval_artifact_key(
            "data/news_aggregates", run_id, basename="result.parquet",
        )
        assert ak.startswith("data/news_aggregates/")
        assert ak.endswith("_result.parquet")
        lk = eval_latest_key("data/news_aggregates")
        assert lk == "data/news_aggregates/latest.json"


# ── End-to-end orchestrator helper ─────────────────────────────────────


class TestAggregateAndWrite:
    def test_end_to_end_returns_key_and_df(self):
        s3 = _InMemoryS3()
        articles = [_make_aggregated(fingerprint="a", tickers=("AAPL",))]
        out = NewsNLPOutput(sentiment_scores=[
            _lm_score("a", composite=0.3),
        ])
        agg = NewsAggregator(sources=[])
        key, df = aggregate_and_write(
            articles=articles, nlp_output=out,
            aggregate_date=date(2026, 5, 13),
            aggregator=agg, s3_client=s3,
        )
        assert key.endswith("_result.parquet")
        assert key.startswith("data/news_aggregates/")
        assert len(df) == 1

    def test_accepts_datetime_for_aggregate_date(self):
        s3 = _InMemoryS3()
        key, _ = aggregate_and_write(
            articles=[], nlp_output=NewsNLPOutput(),
            aggregate_date=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
            aggregator=None, s3_client=s3,
        )
        # Run_id is YYMMDDHHMM (time-of-write); date encoded in row + sidecar
        assert key.startswith("data/news_aggregates/")
        assert key.endswith("_result.parquet")


# ── Schema version pin ────────────────────────────────────────────────


def test_schema_version_pinned_to_int_constant():
    assert isinstance(SCHEMA_VERSION, int)
    # Pin value so consumers checking for v1 don't break on accidental bump
    assert SCHEMA_VERSION == 1


def test_schema_version_on_every_row():
    articles = [
        _make_aggregated(fingerprint="a", tickers=("AAPL",)),
        _make_aggregated(fingerprint="b", tickers=("MSFT",)),
    ]
    df = build_news_aggregates_df(
        articles=articles, nlp_output=NewsNLPOutput(),
        aggregate_date=date(2026, 5, 13),
    )
    assert (df["schema_version"] == SCHEMA_VERSION).all()
