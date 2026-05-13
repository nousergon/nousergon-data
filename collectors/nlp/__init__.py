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

  loughran_mcdonald.LoughranMcDonaldScorer — finance-domain dictionary
                                              sentiment, the academic
                                              gold standard
  event_extraction.AnthropicEventExtractor  — Haiku-tier structured
                                              event flag extraction
                                              (we already pay Anthropic)

Heavier free upgrades that drop in as new adapter classes (Phase 3+):

  finbert.FinBERTScorer        — HF yiyanghkust/finbert-tone
  spacy_ner.SpacyEntityExtractor — en_core_web_sm or larger
"""

from collectors.nlp.event_extraction import (
    AnthropicEventExtractor,
    DEFAULT_EVENT_CATEGORIES,
)
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

__all__ = [
    "EntityMention",
    "EntityExtractor",
    "EventFlag",
    "EventExtractor",
    "SentimentScore",
    "SentimentScorer",
    "LoughranMcDonaldScorer",
    "load_lm_master_dict",
    "AnthropicEventExtractor",
    "DEFAULT_EVENT_CATEGORIES",
    "NewsNLPPipeline",
]
