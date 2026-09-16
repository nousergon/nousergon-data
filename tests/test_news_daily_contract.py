"""Producer contract tests for D36 (daily-news, data-collector plan P-07,
alpha-engine-config-I10870).

Three published keys, three schemas, mirroring the D38/D39 pattern (PR1747):

  - ``contracts/news_digest_daily.schema.json`` — data/news_digest_daily/latest.json.
    Consumer (HARD requirement): morning-signal ``news_context.py::load_news_context``
    (pinned copy + reader-exercising test in that repo, separate PR). The digest's
    own module docstring (``data/derived/news_digest.py`` CONTRACT block) already
    specified this shape; this schema formalizes it as a versioned JSON Schema.
  - ``contracts/news_article_row.schema.json`` — one row of
    data/news_articles_daily/{run_id}_articles.parquet. Consumer: crucible-dashboard
    ``views/Daily_News.py`` (pinned copy + test, separate PR).
  - ``contracts/news_aggregate_row.schema.json`` — one row of
    data/news_aggregates_daily/{run_id}.parquet. Producer side only here: the live
    consumer (crucible-research ``thinktank/context.py``) is v1 and out of scope for
    a consumer pin (plan §3 still calls for the producer schema regardless, since a
    surviving consumer exists).

Reuses the real builders (``build_digest``, ``build_news_articles_df``,
``build_news_aggregates_df``) via the same fixtures as
tests/test_news_digest.py / tests/test_news_articles.py / tests/test_news_aggregates.py
— not hand-typed toy dicts alone — plus a hand-built minimal fixture per schema so a
producer bug can't pass by construction.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

pytest.importorskip("jsonschema")

from collectors.news_aggregator import AggregatedNewsArticle, NewsAggregator
from collectors.nlp.pipeline import NewsNLPOutput
from collectors.nlp.protocols import SentimentScore
from contracts import (
    validate_news_aggregate_row,
    validate_news_article_row,
    validate_news_digest_daily,
)
from data.derived.news_aggregates import build_news_aggregates_df
from data.derived.news_articles import build_news_articles_df
from data.derived.news_digest import build_digest
from nousergon_lib.sources import NewsArticle

import json
from pathlib import Path

_CONTRACTS_DIR = Path(__file__).parent.parent / "contracts"


def _schema(name: str) -> dict:
    return json.loads((_CONTRACTS_DIR / f"{name}.schema.json").read_text())


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


class TestSchemasAreValid:
    @pytest.mark.parametrize("name", ["news_digest_daily", "news_article_row", "news_aggregate_row"])
    def test_schema_parses_as_json_schema(self, name):
        import jsonschema

        jsonschema.Draft202012Validator.check_schema(_schema(name))


class TestDigestValidatesAgainstRealBuild:
    def test_real_build_digest_output_validates(self):
        df = _articles_df(
            [_aggregated(fingerprint="a", tickers=("AAPL",))],
            nlp=NewsNLPOutput(sentiment_scores=[_lm("a", composite=-0.12)]),
        )
        d = build_digest(articles_df=df, topics=_TOPICS, digest_date=date(2026, 5, 13))
        assert validate_news_digest_daily(d) == []

    def test_empty_sections_still_validate(self):
        d = build_digest(articles_df=_articles_df([]), topics={}, digest_date=date(2026, 5, 13))
        assert validate_news_digest_daily(d) == []

    def test_hand_built_fixture_missing_sections_is_rejected(self):
        bad = {"schema_version": 1, "date": "2026-05-13", "generated_at": "2026-05-13T00:00:00Z"}
        assert validate_news_digest_daily(bad) != []


class TestArticleRowValidatesAgainstRealBuild:
    def test_real_build_articles_row_validates(self):
        df = _articles_df(
            [_aggregated(fingerprint="a", tickers=("AAPL",))],
            nlp=NewsNLPOutput(sentiment_scores=[_lm("a", composite=0.2)]),
        )
        row = df.iloc[0].to_dict()
        # aggregate_date is a datetime.date object pre-parquet; the contract
        # validates the post-parquet-round-trip shape (str), matching what
        # the dashboard's pd.read_parquet + to_dict('records') actually sees.
        row["aggregate_date"] = str(row["aggregate_date"])
        assert validate_news_article_row(row) == []

    def test_row_missing_required_field_is_rejected(self):
        df = _articles_df([_aggregated(fingerprint="a")])
        row = df.iloc[0].to_dict()
        del row["lm_sentiment"]
        assert validate_news_article_row(row) != []


class TestAggregateRowValidatesAgainstRealBuild:
    def test_real_build_aggregate_row_validates(self):
        df = build_news_aggregates_df(
            articles=[_aggregated(fingerprint="a", tickers=("AAPL",))],
            nlp_output=NewsNLPOutput(sentiment_scores=[_lm("a", composite=0.2)]),
            aggregate_date=date(2026, 5, 13),
            aggregator=NewsAggregator(sources=[]),
        )
        row = df.iloc[0].to_dict()
        row["aggregate_date"] = str(row["aggregate_date"])
        assert validate_news_aggregate_row(row) == []

    def test_row_missing_required_field_is_rejected(self):
        df = build_news_aggregates_df(
            articles=[_aggregated(fingerprint="a")],
            nlp_output=NewsNLPOutput(),
            aggregate_date=date(2026, 5, 13),
            aggregator=NewsAggregator(sources=[]),
        )
        row = df.iloc[0].to_dict()
        del row["n_articles"]
        assert validate_news_aggregate_row(row) != []
