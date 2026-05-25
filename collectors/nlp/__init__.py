"""News NLP pipeline — sentiment, entities, structured events.

Wave 1 PR A.1 of the institutional data-revamp arc (plan doc:
``~/Development/alpha-engine-docs/private/data-revamp-260513.md``).

Runs over ``AggregatedNewsArticle`` output from
``collectors/news_aggregator.py`` to produce structured per-(ticker,
date) aggregates that feed both the snapshot (PR A.2 — S3 parquet
writer) and the RAG corpus chunks (PR A.3).

Three independent NLP components, each implementing a Protocol so
upgrade paths (FinBERT, spaCy transformer NER, larger LLMs) drop in
without changing the pipeline orchestrator:

  SentimentScorer:  AggregatedNewsArticle → SentimentScore
  EntityExtractor:  AggregatedNewsArticle → list[EntityMention]
  EventExtractor:   AggregatedNewsArticle → list[EventFlag]

Today's free-tier implementations:

  loughran_mcdonald.LoughranMcDonaldScorer            — finance-domain
                                                        dictionary sentiment
                                                        (academic standard)
  rule_based_event_extraction.RuleBasedEventExtractor — deterministic event
                                                        classification from
                                                        vendor tags (Polygon
                                                        keywords, GDELT codes)
                                                        + title-keyword regex.
                                                        Replaced the Haiku-
                                                        backed
                                                        AnthropicEventExtractor
                                                        2026-05-25 per the
                                                        "LLM calls confined
                                                        to research module"
                                                        architectural rule.

Heavier free upgrades that drop in as new adapter classes (Phase 3+):

  finbert.FinBERTScorer        — HF yiyanghkust/finbert-tone
  spacy_ner.SpacyEntityExtractor — en_core_web_sm or larger
"""

from collectors.nlp.loughran_mcdonald import (
    LoughranMcDonaldScorer,
    load_lm_master_dict,
)
from collectors.nlp.pipeline import NewsNLPPipeline
from collectors.nlp.protocols import (
    EntityExtractor,
    EntityMention,
    EventExtractor,
    EventFlag,
    SentimentScore,
    SentimentScorer,
)
from collectors.nlp.rule_based_event_extraction import (
    DEFAULT_EVENT_CATEGORIES,
    RuleBasedEventExtractor,
)

__all__ = [
    "EntityMention",
    "EntityExtractor",
    "EventFlag",
    "EventExtractor",
    "SentimentScore",
    "SentimentScorer",
    "LoughranMcDonaldScorer",
    "load_lm_master_dict",
    "RuleBasedEventExtractor",
    "DEFAULT_EVENT_CATEGORIES",
    "NewsNLPPipeline",
]
