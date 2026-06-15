"""Raw per-article daily news records — the human-readable companion to
``news_aggregates`` (the per-(ticker, date) rollup).

Where ``news_aggregates.py`` collapses a day's articles into one
sentiment/event row per ticker (the machine-readable feature substrate an
LLM brief or the agents read), this module preserves the underlying
deduped articles themselves — one row per canonical story — so a
human-facing surface (the dashboard "Daily News" console page) can render
a reverse-chronological feed of linked headlines, sources, and excerpts.

Both artifacts are written from the SAME already-fetched + already-parsed
article set in ``collectors.daily_news.collect`` — the raw-article write
is purely additive (one extra S3 PUT) and adds NO API calls and NO LLM
spend (the producer stays deterministic). The aggregate remains the
primary artifact; this is a secondary, richer-grained companion.

Schema design mirrors ``news_aggregates``:

- ``schema_version`` column on every row for consumer shimming.
- list-valued fields (``tickers``, ``sources``, ``tags``, ``authors``)
  are JSON-serialized strings — portable across the parquet round-trip
  without pyarrow struct/list fiddliness, and trivially ``json.loads``-ed
  by the dashboard before exploding per-ticker.
- One row per canonical (deduped) story, NOT per (story, ticker): a
  multi-ticker piece stays a single row carrying its full ticker tuple;
  the consumer explodes when it wants a per-ticker view.

Eval-artifacts shape (flat layout + YYMMDDHHMM run_id + ``latest.json``
sidecar) is identical to the aggregate so the dashboard reuses the same
listing/read conventions.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Iterable, Sequence

import pandas as pd

from collectors.news_aggregator import AggregatedNewsArticle, NewsAggregator
from collectors.nlp.pipeline import NewsNLPOutput
from collectors.nlp.protocols import EventFlag, SentimentScore

# Reuse the aggregate module's trust-weight resolution so the two
# artifacts agree on "which source is most trusted" for a given story.
from data.derived.news_aggregates import _make_trust_weight_fn

logger = logging.getLogger(__name__)


SCHEMA_VERSION = 1
"""Bump on any breaking change to the per-article row schema. Additive
columns bump this comment's minor note but not the lock; consumers gate
on the ``schema_version`` column."""


# ── Row shape ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NewsArticleRecord:
    """One row in the raw-article parquet — a single canonical story.

    - ``tickers_json``: JSON list of every ticker this story concerns
      (union across deduped source variants).
    - ``primary_source`` / ``sources_json``: the highest-trust source
      slug + the sorted set of all source slugs that carried the story.
    - ``body_excerpt``: lead paragraph / summary from the highest-trust
      variant (full body lives only in the RAG corpus, not here).
    - ``lm_sentiment``: Loughran-McDonald composite in [-1, +1] for THIS
      story (mean across LM scores for its fingerprint; 0.0 if unscored).
    - ``top_event_description``: highest-severity rule-based event
      description for the story ("" when no event flagged).
    """

    article_fingerprint: str
    aggregate_date: Date
    schema_version: int
    title: str
    url: str
    tickers_json: str
    n_tickers: int
    primary_source: str
    sources_json: str
    n_sources: int
    trust_weight_max: float
    published_at: str
    body_excerpt: str
    authors_json: str
    tags_json: str
    lm_sentiment: float
    lm_positive_words: int
    lm_negative_words: int
    lm_uncertainty_words: int
    event_count: int
    event_severity_max: float
    event_categories: str
    top_event_description: str


# ── Builder ────────────────────────────────────────────────────────────


def build_news_articles_df(
    articles: Sequence[AggregatedNewsArticle],
    nlp_output: NewsNLPOutput,
    *,
    aggregate_date: Date,
    aggregator: NewsAggregator | None = None,
    trust_weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Produce a DataFrame with one row per canonical (deduped) article.

    ``trust_weights`` (or ``aggregator.trust_weights`` if provided) drive
    the ``primary_source`` / ``trust_weight_max`` selection. Falls back to
    a flat-1.0 weighting if neither is passed (ties broken by sort order).
    """
    weight_fn = _make_trust_weight_fn(aggregator, trust_weights)

    # Index NLP streams by article fingerprint for O(1) per-article lookup.
    sentiment_by_fp: dict[str, list[SentimentScore]] = {}
    for s in nlp_output.sentiment_scores:
        sentiment_by_fp.setdefault(s.article_fingerprint, []).append(s)

    events_by_fp: dict[str, list[EventFlag]] = {}
    for e in nlp_output.event_flags:
        events_by_fp.setdefault(e.article_fingerprint, []).append(e)

    rows: list[NewsArticleRecord] = []
    for article in articles:
        rows.append(_build_article_row(
            article=article,
            sentiment_by_fp=sentiment_by_fp,
            events_by_fp=events_by_fp,
            aggregate_date=aggregate_date,
            weight_fn=weight_fn,
        ))

    if not rows:
        return _empty_df()
    # Newest first — the parquet is already in feed order for consumers.
    df = pd.DataFrame([r.__dict__ for r in rows])
    return df.sort_values("published_at", ascending=False).reset_index(drop=True)


def _build_article_row(
    *,
    article: AggregatedNewsArticle,
    sentiment_by_fp: dict[str, list[SentimentScore]],
    events_by_fp: dict[str, list[EventFlag]],
    aggregate_date: Date,
    weight_fn,
) -> NewsArticleRecord:
    fp = article.canonical_fingerprint
    variants = list(article.variants)

    # Source provenance — sorted unique slugs + the most-trusted variant.
    sources = sorted({v.source for v in variants})
    primary_variant = max(
        variants,
        key=lambda v: weight_fn(v.source),
        default=None,
    )
    primary_source = primary_variant.source if primary_variant else ""
    trust_weight_max = max((weight_fn(v.source) for v in variants), default=0.0)

    # Body excerpt: prefer the highest-trust variant; fall back to the
    # first non-empty excerpt across variants (some feeds are headline-only).
    body_excerpt = ""
    if primary_variant and (primary_variant.body_excerpt or "").strip():
        body_excerpt = primary_variant.body_excerpt
    else:
        for v in variants:
            if (v.body_excerpt or "").strip():
                body_excerpt = v.body_excerpt
                break

    # Authors from the primary variant (wire feeds often expose none).
    authors = list(primary_variant.headline_authors) if (
        primary_variant and primary_variant.headline_authors
    ) else []

    # Vendor tags unioned across variants.
    tags: set[str] = set()
    for v in variants:
        tags.update(v.tags or ())

    # Sentiment — LM composite mean for this fingerprint.
    lm_scores: list[float] = []
    pos = neg = unc = 0
    for s in sentiment_by_fp.get(fp, []):
        if s.scorer != "loughran_mcdonald":
            continue
        lm_scores.append(s.composite)
        pos += s.positive_word_count or 0
        neg += s.negative_word_count or 0
        unc += s.uncertainty_word_count or 0
    lm_sentiment = sum(lm_scores) / len(lm_scores) if lm_scores else 0.0

    # Events for this story.
    events = events_by_fp.get(fp, [])
    if events:
        event_count = len(events)
        severities = [e.severity for e in events]
        event_severity_max = max(severities)
        categories_str = ",".join(sorted({e.category for e in events}))
        top = max(events, key=lambda e: e.severity)
        top_event_description = top.description
    else:
        event_count = 0
        event_severity_max = 0.0
        categories_str = ""
        top_event_description = ""

    return NewsArticleRecord(
        article_fingerprint=fp,
        aggregate_date=aggregate_date,
        schema_version=SCHEMA_VERSION,
        title=article.canonical_title,
        url=article.canonical_url,
        tickers_json=json.dumps(list(article.tickers), separators=(",", ":")),
        n_tickers=len(article.tickers),
        primary_source=primary_source,
        sources_json=json.dumps(sources, separators=(",", ":")),
        n_sources=len(sources),
        trust_weight_max=round(trust_weight_max, 4),
        published_at=_iso(article.earliest_published_at),
        body_excerpt=body_excerpt,
        authors_json=json.dumps(authors, separators=(",", ":")),
        tags_json=json.dumps(sorted(tags), separators=(",", ":")),
        lm_sentiment=round(lm_sentiment, 6),
        lm_positive_words=pos,
        lm_negative_words=neg,
        lm_uncertainty_words=unc,
        event_count=event_count,
        event_severity_max=round(event_severity_max, 4),
        event_categories=categories_str,
        top_event_description=top_event_description,
    )


def _iso(dt: datetime) -> str:
    """UTC ISO-8601 with a trailing ``Z`` (consistent with the sidecar)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _empty_df() -> pd.DataFrame:
    """Empty DataFrame with the canonical column order so consumers
    reading an empty-day parquet don't crash on missing columns."""
    cols = list(NewsArticleRecord.__dataclass_fields__.keys())
    return pd.DataFrame(columns=cols)


# ── S3 parquet writer ──────────────────────────────────────────────────


DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/news_articles_daily"


def write_news_articles_parquet(
    df: pd.DataFrame,
    *,
    aggregate_date: Date,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    run_id: str | None = None,
) -> str:
    """Write the per-article DataFrame to S3 as parquet using the canonical
    ``alpha_engine_lib.eval_artifacts`` shape: flat layout + YYMMDDHHMM
    run_id + ``latest.json`` sidecar. Returns the artifact S3 key.

    Mirrors ``news_aggregates.write_news_aggregates_parquet`` so the
    dashboard reuses the same sidecar→artifact resolution.
    """
    from alpha_engine_lib.eval_artifacts import (
        eval_artifact_key,
        eval_latest_key,
        new_eval_run_id,
    )

    run_id = run_id or new_eval_run_id()
    artifact_key = eval_artifact_key(prefix, run_id, basename="articles.parquet")
    latest_key = eval_latest_key(prefix)

    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=artifact_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    latest_payload = {
        "run_id": run_id,
        "artifact_key": artifact_key,
        "aggregate_date": aggregate_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "row_count": int(len(df)),
        "written_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=latest_key,
        Body=json.dumps(latest_payload).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        "[news_articles] wrote %d rows to s3://%s/%s (latest=%s)",
        len(df), bucket, artifact_key, latest_key,
    )
    return artifact_key


def read_news_articles_parquet(
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> pd.DataFrame:
    """Consumer-side read. Resolves the canonical artifact via the
    ``latest.json`` sidecar. Returns an empty canonical-schema DataFrame
    when no artifact exists."""
    from alpha_engine_lib.eval_artifacts import eval_latest_key

    latest_key = eval_latest_key(prefix)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=latest_key)
        sidecar = json.loads(obj["Body"].read())
        artifact_key = sidecar.get("artifact_key")
        if artifact_key:
            body = s3_client.get_object(Bucket=bucket, Key=artifact_key)
            return pd.read_parquet(BytesIO(body["Body"].read()), engine="pyarrow")
    except Exception as e:
        logger.info(
            "[news_articles] canonical sidecar read failed for %s (%s)",
            latest_key, type(e).__name__,
        )
    return _empty_df()


# ── Orchestrator-friendly helper ──────────────────────────────────────


def articles_build_and_write(
    articles: Iterable[AggregatedNewsArticle],
    nlp_output: NewsNLPOutput,
    *,
    aggregate_date: Date | datetime,
    aggregator: NewsAggregator | None,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> tuple[str, pd.DataFrame]:
    """End-to-end: build the per-article DataFrame + write it to S3.

    Returns ``(s3_key, dataframe)``. The DataFrame is returned for
    immediate downstream use (logging row counts, status dict) so the
    caller doesn't re-read from S3.
    """
    if isinstance(aggregate_date, datetime):
        aggregate_date = aggregate_date.date()
    df = build_news_articles_df(
        articles=list(articles),
        nlp_output=nlp_output,
        aggregate_date=aggregate_date,
        aggregator=aggregator,
    )
    key = write_news_articles_parquet(
        df,
        aggregate_date=aggregate_date,
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
    )
    return key, df
