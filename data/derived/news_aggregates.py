"""News aggregates — per-(ticker, date) sentiment + event + entity rollup.

Wave 1 PR A.2 of the institutional data-revamp arc (plan doc:
``~/Development/alpha-engine-docs/private/data-revamp-260513.md``).

Joins the three NLP-pipeline output streams (sentiment_scores,
entity_mentions, event_flags) with the aggregator's source-provenance
information and produces one row per (ticker, aggregate_date). This is
the canonical structured signal that downstream consumers read into
``input_data_snapshot`` for thesis_update, sector_quant, and
sector_qual agents.

Schema design notes:

- ``schema_version`` column on every row so consumers can shim through
  schema evolution. Bump on any breaking change. Additive changes
  bump the minor version comment but not the column.
- Source-provenance preserved as ``n_articles_by_source`` (JSON-
  serialized dict, since parquet handles dicts via pyarrow struct but
  pandas DataFrame.to_parquet flattens awkwardly — JSON string is
  more portable).
- Numerical aggregates use trust-weighted means as the primary
  "headline" metric (``lm_sentiment_trusted_mean``); raw mean kept as
  ``lm_sentiment_mean`` for audit + diagnostics.
- Top event descriptions kept as a list-string so the snapshot's
  glance-text panel can render them without retrieving each event flag
  separately. Capped at 3 by severity descending.

Why one parquet file per date (vs per ticker):

- ~25 held tickers × ~50 universe tickers ≈ 75 rows/day. Pandas
  reads a 75-row parquet faster than 75 separate files.
- Single S3 object per day = cheaper LIST + atomic write (overwrite
  is atomic at the object level).
- Schema evolution is easier — one schema migration per file, not 75.

Idempotent: re-running for the same date overwrites the parquet. Each
write is a complete snapshot of that date's aggregates; no
append-merge logic needed.
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

logger = logging.getLogger(__name__)


SCHEMA_VERSION = 1
"""Bump on any breaking change to the row schema. Consumers should
gate on this column."""


# Top-N event descriptions to keep on the per-(ticker, date) row.
# Sorted by severity desc; ties broken by appearance order.
TOP_EVENT_DESCRIPTIONS_N = 3


# ── Row shape ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NewsTickerDailyAggregate:
    """One row in the structured aggregates parquet.

    Field semantics mirror what consumers join into the snapshot:

    - ``n_articles``: raw count of unique aggregated articles
      mentioning this ticker.
    - ``n_articles_trusted_weighted``: sum of source trust weights
      (one per variant) across this ticker's articles. Higher = more
      institutional-quality coverage. Cross-source-deduped.
    - ``lm_sentiment_*``: Loughran-McDonald composite stats. Mean is
      simple mean across articles; ``trusted_mean`` weights each
      article's sentiment by its highest-trust source variant.
    - ``event_count``: total event flags surfaced across all articles
      mentioning this ticker.
    - ``event_severity_max``: max severity in [0,1] across events.
    - ``event_categories``: comma-joined sorted list of unique
      categories surfaced for this ticker on this date.
    - ``top_event_descriptions``: top-N event descriptions by
      severity desc (1-sentence summaries from the LLM extractor).
      Useful for glance-text in the snapshot.
    """

    ticker: str
    aggregate_date: Date
    schema_version: int
    n_articles: int
    n_articles_trusted_weighted: float
    n_articles_by_source_json: str
    lm_sentiment_mean: float
    lm_sentiment_max: float
    lm_sentiment_min: float
    lm_sentiment_trusted_mean: float
    lm_positive_words_total: int
    lm_negative_words_total: int
    lm_uncertainty_words_total: int
    lm_total_tokens: int
    event_count: int
    event_severity_max: float
    event_severity_mean: float
    event_categories: str
    top_event_descriptions: str
    entity_mentions_count: int


# ── Aggregation ────────────────────────────────────────────────────────


def build_news_aggregates_df(
    articles: Sequence[AggregatedNewsArticle],
    nlp_output: NewsNLPOutput,
    *,
    aggregate_date: Date,
    aggregator: NewsAggregator | None = None,
    trust_weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Produce a DataFrame with one row per ticker mentioned across the
    article set.

    ``trust_weights`` (or ``aggregator.trust_weights`` if provided) is
    used to compute trusted-mean sentiment + n_articles_trusted_weighted.
    Falls back to a flat-1.0 weighting if neither is passed.
    """
    weight_fn = _make_trust_weight_fn(aggregator, trust_weights)

    # Index NLP output streams by article fingerprint for O(1) lookups
    sentiment_by_fp: dict[str, list[SentimentScore]] = {}
    for s in nlp_output.sentiment_scores:
        sentiment_by_fp.setdefault(s.article_fingerprint, []).append(s)

    events_by_fp: dict[str, list[EventFlag]] = {}
    for e in nlp_output.event_flags:
        events_by_fp.setdefault(e.article_fingerprint, []).append(e)

    entities_by_fp: dict[str, int] = {}
    for em in nlp_output.entity_mentions:
        entities_by_fp[em.article_fingerprint] = (
            entities_by_fp.get(em.article_fingerprint, 0) + 1
        )

    # Group articles by ticker; one ticker may appear in many articles,
    # and one article may concern many tickers (multi-mention pieces).
    per_ticker_articles: dict[str, list[AggregatedNewsArticle]] = {}
    for article in articles:
        for ticker in article.tickers:
            per_ticker_articles.setdefault(ticker, []).append(article)

    rows: list[NewsTickerDailyAggregate] = []
    for ticker in sorted(per_ticker_articles):
        ticker_articles = per_ticker_articles[ticker]
        rows.append(_build_row(
            ticker=ticker,
            ticker_articles=ticker_articles,
            sentiment_by_fp=sentiment_by_fp,
            events_by_fp=events_by_fp,
            entities_by_fp=entities_by_fp,
            aggregate_date=aggregate_date,
            weight_fn=weight_fn,
        ))

    if not rows:
        return _empty_df()
    return pd.DataFrame([r.__dict__ for r in rows])


def _make_trust_weight_fn(
    aggregator: NewsAggregator | None,
    trust_weights: dict[str, float] | None,
):
    if aggregator is not None:
        return aggregator.trust_weight
    if trust_weights is not None:
        return lambda src: trust_weights.get(src, 0.5)
    return lambda src: 1.0


def _build_row(
    *,
    ticker: str,
    ticker_articles: list[AggregatedNewsArticle],
    sentiment_by_fp: dict[str, list[SentimentScore]],
    events_by_fp: dict[str, list[EventFlag]],
    entities_by_fp: dict[str, int],
    aggregate_date: Date,
    weight_fn,
) -> NewsTickerDailyAggregate:
    n_articles = len(ticker_articles)

    # Per-source coverage + trust-weighted article count
    source_counts: dict[str, int] = {}
    trusted_sum = 0.0
    for art in ticker_articles:
        for variant in art.variants:
            source_counts[variant.source] = source_counts.get(variant.source, 0) + 1
            trusted_sum += weight_fn(variant.source)

    # Sentiment aggregates — use LM scorer's output where present
    lm_scores: list[float] = []
    lm_trusted_scores: list[tuple[float, float]] = []  # (score, weight)
    pos_total = neg_total = unc_total = tok_total = 0
    for art in ticker_articles:
        fp = art.canonical_fingerprint
        for s in sentiment_by_fp.get(fp, []):
            if s.scorer != "loughran_mcdonald":
                continue
            lm_scores.append(s.composite)
            # Weight by highest-trust source for this article
            article_trust = max(
                (weight_fn(v.source) for v in art.variants),
                default=0.5,
            )
            lm_trusted_scores.append((s.composite, article_trust))
            pos_total += s.positive_word_count or 0
            neg_total += s.negative_word_count or 0
            unc_total += s.uncertainty_word_count or 0
            tok_total += s.total_token_count or 0

    if lm_scores:
        lm_mean = sum(lm_scores) / len(lm_scores)
        lm_max = max(lm_scores)
        lm_min = min(lm_scores)
    else:
        lm_mean = lm_max = lm_min = 0.0

    if lm_trusted_scores:
        total_w = sum(w for _, w in lm_trusted_scores)
        lm_trusted_mean = (
            sum(s * w for s, w in lm_trusted_scores) / total_w
            if total_w > 0 else lm_mean
        )
    else:
        lm_trusted_mean = 0.0

    # Event aggregates — only events whose tickers list contains this ticker
    # (when tickers list is non-empty); empty-tickers events fall back to
    # "article concerns this ticker" semantics.
    relevant_events: list[EventFlag] = []
    for art in ticker_articles:
        fp = art.canonical_fingerprint
        for e in events_by_fp.get(fp, []):
            if e.tickers:
                if ticker in e.tickers:
                    relevant_events.append(e)
            else:
                # Event extractor didn't tag tickers; inherit from article
                relevant_events.append(e)

    if relevant_events:
        event_count = len(relevant_events)
        severities = [e.severity for e in relevant_events]
        event_severity_max = max(severities)
        event_severity_mean = sum(severities) / len(severities)
        categories = sorted({e.category for e in relevant_events})
        # Top-N descriptions by severity desc
        top_descs = sorted(
            relevant_events, key=lambda e: -e.severity
        )[:TOP_EVENT_DESCRIPTIONS_N]
        top_descriptions = " | ".join(e.description for e in top_descs)
        categories_str = ",".join(categories)
    else:
        event_count = 0
        event_severity_max = 0.0
        event_severity_mean = 0.0
        categories_str = ""
        top_descriptions = ""

    # Entity mentions count
    entity_count = sum(
        entities_by_fp.get(art.canonical_fingerprint, 0)
        for art in ticker_articles
    )

    return NewsTickerDailyAggregate(
        ticker=ticker,
        aggregate_date=aggregate_date,
        schema_version=SCHEMA_VERSION,
        n_articles=n_articles,
        n_articles_trusted_weighted=round(trusted_sum, 4),
        n_articles_by_source_json=json.dumps(
            dict(sorted(source_counts.items())), separators=(",", ":")
        ),
        lm_sentiment_mean=round(lm_mean, 6),
        lm_sentiment_max=round(lm_max, 6),
        lm_sentiment_min=round(lm_min, 6),
        lm_sentiment_trusted_mean=round(lm_trusted_mean, 6),
        lm_positive_words_total=pos_total,
        lm_negative_words_total=neg_total,
        lm_uncertainty_words_total=unc_total,
        lm_total_tokens=tok_total,
        event_count=event_count,
        event_severity_max=round(event_severity_max, 4),
        event_severity_mean=round(event_severity_mean, 4),
        event_categories=categories_str,
        top_event_descriptions=top_descriptions,
        entity_mentions_count=entity_count,
    )


def _empty_df() -> pd.DataFrame:
    """Empty DataFrame with the canonical column order so consumers
    reading an empty-day parquet don't crash on missing columns."""
    cols = list(NewsTickerDailyAggregate.__dataclass_fields__.keys())
    return pd.DataFrame(columns=cols)


# ── S3 parquet writer ──────────────────────────────────────────────────


DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/news_aggregates"


def write_news_aggregates_parquet(
    df: pd.DataFrame,
    *,
    aggregate_date: Date,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    run_id: str | None = None,
) -> str:
    """Write the aggregates DataFrame to S3 as parquet using the
    canonical ``nousergon_lib.eval_artifacts`` shape: flat layout +
    YYMMDDHHMM-encoded run_id + ``latest.json`` sidecar.

    Returns the artifact S3 key.

    Re-runs on the same calendar day produce distinct artifacts
    (different YYMMDDHHMM run_ids) and update ``latest.json`` to
    point at the newest one. Audit-friendly: every Saturday SF
    invocation preserved + latest is always discoverable.

    ``aggregate_date`` is stamped onto every row of the parquet
    (already a column in ``NewsTickerDailyAggregate``) so consumers
    can filter by the canonical date without parsing the run_id.
    """
    from nousergon_lib.eval_artifacts import (
        eval_artifact_key,
        eval_latest_key,
        new_eval_run_id,
    )

    run_id = run_id or new_eval_run_id()
    artifact_key = eval_artifact_key(prefix, run_id, basename="result.parquet")
    latest_key = eval_latest_key(prefix)

    # Write parquet body
    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=artifact_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    # Write latest.json sidecar pointing to the artifact run_id
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
        "[news_aggregates] wrote %d rows to s3://%s/%s (latest=%s)",
        len(df), bucket, artifact_key, latest_key,
    )
    return artifact_key


def read_news_aggregates_parquet(
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> pd.DataFrame:
    """Consumer-side read. Resolves the canonical artifact via the
    ``latest.json`` sidecar.

    ``latest.json`` always points at the most recent run regardless of
    date; the parquet itself carries ``aggregate_date`` per row so any
    date filtering happens at the DataFrame layer.

    Returns an empty DataFrame with the canonical schema when no
    artifact exists.
    """
    from nousergon_lib.eval_artifacts import eval_latest_key

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
            "[news_aggregates] canonical sidecar read failed for %s (%s)",
            latest_key, type(e).__name__,
        )

    return _empty_df()


# ── Orchestrator-friendly helper ──────────────────────────────────────


def aggregate_and_write(
    articles: Iterable[AggregatedNewsArticle],
    nlp_output: NewsNLPOutput,
    *,
    aggregate_date: Date | datetime,
    aggregator: NewsAggregator | None,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> tuple[str, pd.DataFrame]:
    """End-to-end: build aggregates DataFrame + write to S3.

    Returns ``(s3_key, dataframe)``. The DataFrame is returned for
    immediate downstream use (e.g. logging row counts, emitting CW
    metrics) so the caller doesn't need to re-read from S3.
    """
    if isinstance(aggregate_date, datetime):
        aggregate_date = aggregate_date.date()
    df = build_news_aggregates_df(
        articles=list(articles),
        nlp_output=nlp_output,
        aggregate_date=aggregate_date,
        aggregator=aggregator,
    )
    key = write_news_aggregates_parquet(
        df,
        aggregate_date=aggregate_date,
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
    )
    return key, df
