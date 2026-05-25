"""News NLP pipeline orchestrator — composes scorers + extractors.

Reads a list of ``AggregatedNewsArticle`` records from the aggregator
and produces three parallel output streams:

  sentiment_scores:  one per (article, scorer)
  entity_mentions:   list (flat) across all articles + extractors
  event_flags:       list (flat) across all articles + extractors

The orchestrator runs scorers/extractors independently per article;
each article's failure (transient LLM error, malformed text) drops
that article's contribution from the affected stream but doesn't fail
the batch — matches the producer-side "graceful degrade" policy.

Output is structured-ready for the PR A.2 parquet writer (one row per
record per stream) and the PR A.3 RAG ingest pass.

Cost telemetry: LLM-based components (event_extraction) emit cost
records under their own agent_id; the pipeline itself doesn't add
overhead. Run-level aggregate cost is summed from the cost telemetry
stream downstream.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Sequence

from collectors.news_aggregator import AggregatedNewsArticle
from collectors.nlp.protocols import (
    EntityExtractor,
    EntityMention,
    EventExtractor,
    EventFlag,
    SentimentScore,
    SentimentScorer,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NewsNLPOutput:
    """Aggregate output across all articles in one pipeline run.

    Each stream is a flat list — downstream parquet writer pivots to
    long-form rows; RAG-ingest path joins back to articles via the
    ``article_fingerprint`` foreign key.
    """

    sentiment_scores: list[SentimentScore] = field(default_factory=list)
    entity_mentions: list[EntityMention] = field(default_factory=list)
    event_flags: list[EventFlag] = field(default_factory=list)
    n_articles_processed: int = 0
    n_articles_failed: int = 0


class NewsNLPPipeline:
    """Compose sentiment scorers + entity extractors + event extractors.

    Multiple scorers may be wired (e.g. LoughranMcDonald + FinBERT for
    ensemble) — each emits a per-article SentimentScore tagged with
    its ``name``. Downstream aggregation chooses how to combine them
    (or keeps them separate per scorer).

    Empty extractor lists are valid — the pipeline only runs the
    components it's given.
    """

    def __init__(
        self,
        *,
        sentiment_scorers: Sequence[SentimentScorer] = (),
        entity_extractors: Sequence[EntityExtractor] = (),
        event_extractors: Sequence[EventExtractor] = (),
    ) -> None:
        self._sentiment_scorers = tuple(sentiment_scorers)
        self._entity_extractors = tuple(entity_extractors)
        self._event_extractors = tuple(event_extractors)

    def process(
        self, articles: Sequence[AggregatedNewsArticle],
    ) -> NewsNLPOutput:
        sentiment: list[SentimentScore] = []
        entities: list[EntityMention] = []
        events: list[EventFlag] = []
        n_processed = 0
        n_failed = 0

        for article in articles:
            text = _article_text(article)
            if not text.strip():
                n_failed += 1
                continue
            fp = article.canonical_fingerprint

            for scorer in self._sentiment_scorers:
                try:
                    sentiment.append(scorer.score(
                        text=text, article_fingerprint=fp,
                    ))
                except Exception as e:
                    logger.warning(
                        "[nlp_pipeline] %s sentiment failed on %s: %s",
                        scorer.name, fp, e,
                    )

            for extractor in self._entity_extractors:
                try:
                    entities.extend(extractor.extract(
                        text=text, article_fingerprint=fp,
                    ))
                except Exception as e:
                    logger.warning(
                        "[nlp_pipeline] %s entity-extract failed on %s: %s",
                        extractor.name, fp, e,
                    )

            # Union vendor tags across all variants — Polygon keywords +
            # GDELT event codes + Benzinga channels for the same wire
            # story. Rule-based extractors use this as the primary
            # classification signal; LLM extractors (if any reactivated)
            # ignore the kwarg via the EventExtractor Protocol default.
            article_tags: tuple[str, ...] = tuple({
                t for v in article.variants for t in v.tags
            })
            for extractor in self._event_extractors:
                try:
                    events.extend(extractor.extract(
                        text=text, article_fingerprint=fp,
                        article_tickers=article.tickers,
                        article_tags=article_tags,
                    ))
                except Exception as e:
                    logger.warning(
                        "[nlp_pipeline] %s event-extract failed on %s: %s",
                        extractor.name, fp, e,
                    )

            n_processed += 1

        logger.info(
            "[nlp_pipeline] processed=%d failed=%d "
            "sentiment_scores=%d entity_mentions=%d event_flags=%d",
            n_processed, n_failed,
            len(sentiment), len(entities), len(events),
        )

        return NewsNLPOutput(
            sentiment_scores=sentiment,
            entity_mentions=entities,
            event_flags=events,
            n_articles_processed=n_processed,
            n_articles_failed=n_failed,
        )


def _article_text(article: AggregatedNewsArticle) -> str:
    """Concatenate canonical title + the longest body_excerpt across
    variants. Multiple vendors syndicating the same wire story can
    contribute different excerpt lengths; we use the longest available
    so the scorers/extractors have maximum text to work with."""
    longest_excerpt = ""
    for v in article.variants:
        excerpt = v.body_excerpt or ""
        if len(excerpt) > len(longest_excerpt):
            longest_excerpt = excerpt
    pieces = [article.canonical_title or "", longest_excerpt]
    return "\n\n".join(p for p in pieces if p)
