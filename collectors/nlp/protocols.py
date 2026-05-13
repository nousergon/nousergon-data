"""NLP pipeline Protocols + Pydantic output shapes.

Component pattern: each NLP analysis dimension (sentiment, entities,
events) is a Protocol with one or more concrete implementations. The
pipeline orchestrator (``pipeline.NewsNLPPipeline``) composes them
without knowing which concrete classes are wired — upgrade paths
(FinBERT for sentiment, transformer NER for entities, larger LLMs for
events) drop in as new classes without touching the orchestrator or
the consumer side.

Shapes are Pydantic with ``frozen=True`` + ``extra='forbid'`` so a
downstream Pandas/Parquet writer (PR A.2) can rely on a fixed column
schema.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


# ── Sentiment ──────────────────────────────────────────────────────────


class SentimentScore(BaseModel):
    """One sentiment-scorer's output for one article.

    Loughran-McDonald output populates ``positive``/``negative`` raw
    counts + the normalized ``composite`` in [-1, +1]. Transformer-
    based scorers (FinBERT) leave the raw counts None and fill
    ``composite`` from the model's softmax.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    scorer: str = Field(
        description="Scorer slug: 'loughran_mcdonald' | 'finbert' | "
                    "'vader'. Joins onto per-scorer aggregation rules "
                    "downstream — different scorers carry different "
                    "calibration."
    )
    article_fingerprint: str = Field(
        description="The aggregated article's canonical fingerprint "
                    "(from NewsAggregator) — joins back to the article "
                    "set without re-fingerprinting."
    )
    composite: float = Field(
        description="Normalized score in [-1, +1]. -1 = maximally "
                    "negative; +1 = maximally positive. 0 = neutral / "
                    "no signal."
    )
    positive_word_count: int | None = Field(
        default=None,
        description="Loughran-McDonald-style raw positive-word count. "
                    "None for transformer scorers that don't expose it.",
    )
    negative_word_count: int | None = Field(
        default=None,
        description="LM-style raw negative-word count.",
    )
    uncertainty_word_count: int | None = Field(
        default=None,
        description="LM 'uncertainty' category — distinct from sentiment "
                    "polarity; useful for risk gating.",
    )
    total_token_count: int | None = Field(
        default=None,
        description="Total tokens analyzed (denominator for word-count "
                    "ratios).",
    )


@runtime_checkable
class SentimentScorer(Protocol):
    """Sentiment-scorer Protocol.

    Implementations: lexicon (Loughran-McDonald, VADER), transformer
    (FinBERT, FinBERT-tone), LLM (Anthropic Haiku via structured-
    output). Pipeline can compose multiple scorers for ensemble.
    """

    name: str

    def score(self, *, text: str, article_fingerprint: str) -> SentimentScore: ...


# ── Entities ───────────────────────────────────────────────────────────


class EntityMention(BaseModel):
    """One entity surfaced from an article."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    extractor: str = Field(
        description="Extractor slug: 'regex_ticker' | 'spacy_en' | "
                    "'spacy_trf' | 'llm_haiku'."
    )
    article_fingerprint: str
    text: str = Field(description="Surface form as it appeared in the article.")
    label: str = Field(
        description="Entity type: 'ORG' | 'PERSON' | 'GPE' | 'MONEY' | "
                    "'PERCENT' | 'TICKER' | 'PRODUCT' | etc."
    )
    canonical_ticker: str | None = Field(
        default=None,
        description="When the entity resolves to a tradeable ticker via "
                    "the ticker→name map, the canonical exchange symbol. "
                    "Otherwise None.",
    )


@runtime_checkable
class EntityExtractor(Protocol):
    name: str

    def extract(
        self, *, text: str, article_fingerprint: str,
    ) -> list[EntityMention]: ...


# ── Events ─────────────────────────────────────────────────────────────


class EventFlag(BaseModel):
    """One structured event flagged from the article."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    extractor: str = Field(
        description="Extractor slug: 'anthropic_haiku' | "
                    "'gdelt_native' (when we use vendor-native event "
                    "codes) | 'rule_based'."
    )
    article_fingerprint: str
    category: str = Field(
        description="Event category from a closed taxonomy (see "
                    "event_extraction.DEFAULT_EVENT_CATEGORIES). "
                    "Categorical for downstream aggregation by type."
    )
    description: str = Field(
        description="Free-text 1-sentence description of the specific "
                    "event ('SEC investigation announced into accounting "
                    "practices'). Indexable into the RAG corpus for "
                    "downstream agent retrieval.",
    )
    tickers: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Tickers this event directly concerns. May be a "
                    "subset of the article's tickers — e.g. an M&A "
                    "article between A and B may flag only A as the "
                    "acquirer."
    )
    severity: float = Field(
        default=0.5,
        description="Subjective importance in [0, 1]. Used by downstream "
                    "alerting + thesis_update trigger weights.",
    )
    extracted_at: datetime = Field(
        description="UTC timestamp when the extractor produced this flag."
    )


@runtime_checkable
class EventExtractor(Protocol):
    name: str

    def extract(
        self, *, text: str, article_fingerprint: str,
        article_tickers: tuple[str, ...],
    ) -> list[EventFlag]: ...
