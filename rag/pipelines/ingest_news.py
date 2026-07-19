"""Ingest aggregated news articles into the RAG vector store.

Wave 1 PR A.3 of the institutional data-revamp arc (plan doc:
``~/Development/alpha-engine-docs/private/data-revamp-260513.md``).

Pairs with:
- PR β (#226) — news source adapters
- PR A.1 (#227) — NLP pipeline
- PR A.2 (#228) — structured aggregates writer

The structured aggregates parquet (PR A.2) carries per-(ticker, date)
numerical signals. This module carries the **full narrative text** of
each news article into the existing RAG corpus, indexed alongside SEC
filings + theses. Consumer agents (thesis_update, sector_quant/qual)
retrieve relevant news at inference time via the hybrid-retrieval API
that the qual analyst's ``query_filings`` tool already uses.

Architecture:

    [Polygon/GDELT/Yahoo adapters] ──→ NewsAggregator (PR β)
                                              │
                                              ▼
                              AggregatedNewsArticle list
                                              │
                       ┌──────────────────────┼──────────────────────┐
                       ▼                      ▼                      ▼
              [NLP pipeline]         [aggregates parquet]      [RAG ingest]   ← THIS PR
              (PR A.1)               (PR A.2)                  (PR A.3)
                       │                      │                      │
                       ▼                      ▼                      ▼
              streams                 S3 parquet              pgvector (with
              (sentiment +            per-(ticker, date)      SEC filings)
              events + entities)

Idempotency: pre-checked via the lib's ``document_exists`` — re-runs
of the SAME article (matched via ``external_id``, the aggregator's
composite fingerprint) skip the embedding call entirely. Distinct
articles for one (ticker, source, day) are no longer collapsed onto a
single row (config#2957) — ``external_id`` is what makes per-article
dedup possible instead of per-(ticker, source, day).

Chunking: news articles are short (title + body excerpt = typically
<500 tokens). We emit ONE chunk per article per ticker. For longer
syndicated bodies we'd split, but the current Polygon/GDELT/Yahoo
adapters only return excerpts so single-chunk is the right shape.

Multi-ticker articles: indexed once per ticker (the RAG schema is
ticker-keyed; the qual agent's ``query_filings`` tool gates by ticker
so a sector-wide piece needs to surface for each constituent).
"""

from __future__ import annotations

import logging
from datetime import date as Date
from typing import Any, Sequence

from collectors.news_aggregator import AggregatedNewsArticle

logger = logging.getLogger(__name__)


# Source slug → canonical RAG `source` field. Joins onto the same
# source values used by the structured aggregates writer + downstream
# retrieval gating. We prefix with "news_" so consumer queries can
# filter "news only" vs "filings only" by source-prefix without
# enumerating vendors.
_RAG_SOURCE_PREFIX = "news_"


def _rag_source(news_article_source: str) -> str:
    """Map ``NewsArticle.source`` (vendor slug) → RAG ``source`` field."""
    return f"{_RAG_SOURCE_PREFIX}{news_article_source}"


def _chunk_text(article: AggregatedNewsArticle) -> str:
    """Build the chunk body text for one aggregated article.

    Composes canonical title + longest variant body excerpt. The
    longer the input text, the better the embedding's semantic
    fidelity — pick the longest body across variants the aggregator
    grouped together.
    """
    longest_excerpt = ""
    for v in article.variants:
        excerpt = v.body_excerpt or ""
        if len(excerpt) > len(longest_excerpt):
            longest_excerpt = excerpt
    pieces = [article.canonical_title or "", longest_excerpt]
    return "\n\n".join(p for p in pieces if p).strip()


def _canonical_source(article: AggregatedNewsArticle) -> str:
    """Pick the canonical vendor slug for the aggregated article.

    Prefer the highest-trust variant; ties broken by vendor name
    alphabetically so the choice is deterministic across re-runs
    (same article ingested twice produces same document).
    """
    if not article.variants:
        return "unknown"
    # We don't have access to trust weights here (no aggregator
    # passed); pick by variant ordering. NewsAggregator's _dedup keeps
    # variants in the order they were inserted; that's "all sources
    # for this fingerprint". For deterministic source selection across
    # runs we pick alphabetically — re-ingests produce the same source.
    sources = sorted({v.source for v in article.variants})
    return sources[0] if sources else "unknown"


def ingest_articles(
    articles: Sequence[AggregatedNewsArticle],
    *,
    filed_date: Date,
    ticker_to_sector: dict[str, str] | None = None,
    embed_texts_fn=None,
    document_exists_fn=None,
    ingest_document_fn=None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest aggregated news articles into the RAG corpus.

    One document per (ticker, article) pair — multi-ticker articles
    are indexed once per ticker so the ticker-keyed RAG schema
    surfaces them when the qual agent queries by any constituent.

    Returns a stats dict::

        {
            "n_articles_input": int,
            "n_documents_attempted": int,
            "n_documents_skipped_exists": int,
            "n_documents_skipped_empty_text": int,
            "n_documents_ingested": int,
            "n_failures": int,
        }

    Args:
        articles: aggregated news articles (from NewsAggregator).
        filed_date: the canonical filed_date stamped on every document
            this run. For news, all articles in one ingest batch share a
            filed_date — typically the calendar date the batch was
            fetched. Per-article dedup within that shared filed_date is
            keyed on each article's ``external_id`` (config#2957), not
            filed_date alone.
        ticker_to_sector: optional ticker → GICS sector map. Sector is
            an optional column on the RAG documents table; omit to
            skip sector tagging.
        embed_texts_fn / document_exists_fn / ingest_document_fn:
            injectable for testing. Production callers pass None and
            we lazy-import from ``nousergon_lib.rag``.
        dry_run: log the would-be ingest without calling the embedder
            or the DB writer. Useful for new-batch sanity checks.
    """
    if embed_texts_fn is None:
        from nousergon_lib.rag import embed_texts
        embed_texts_fn = embed_texts
    if document_exists_fn is None:
        from nousergon_lib.rag import document_exists
        document_exists_fn = document_exists
    if ingest_document_fn is None:
        from nousergon_lib.rag import ingest_document
        ingest_document_fn = ingest_document
    ticker_to_sector = ticker_to_sector or {}

    stats = {
        "n_articles_input": len(articles),
        "n_documents_attempted": 0,
        "n_documents_skipped_exists": 0,
        "n_documents_skipped_empty_text": 0,
        "n_documents_ingested": 0,
        "n_failures": 0,
    }

    # Pass 1: resolve dedup + dry-run/empty-text skips and build the list
    # of documents that actually need an embedding. config#2956
    # deliverable 3 — accumulate ALL pending chunk bodies across every
    # article/ticker in this run and call ``embed_texts_fn`` ONCE (it
    # already batches internally in blocks of up to 128, see
    # ``nousergon_lib.rag.embeddings.embed_texts``) instead of once PER
    # article, which was the previous N-embedding-calls-per-run shape.
    pending: list[dict[str, Any]] = []
    for article in articles:
        body = _chunk_text(article)
        if not body or len(body) < 20:
            stats["n_documents_skipped_empty_text"] += len(article.tickers)
            continue

        rag_source = _rag_source(_canonical_source(article))

        # Stable per-article identity for dedup (config#2957): the
        # aggregator's own composite fingerprint (normalized title +
        # URL host+path, alpha-engine-data collectors/news_aggregator.py
        # ``_article_fingerprint``) already groups source variants of the
        # same real-world story, so re-ingesting the SAME article (even
        # from a later run / different day's fetch) yields the SAME
        # external_id. Without this, rag.documents' dedup key collapses
        # every article for one (ticker, source, day) onto a single row.
        external_id = article.canonical_fingerprint or None

        for ticker in article.tickers:
            stats["n_documents_attempted"] += 1
            if document_exists_fn(ticker, "news", filed_date, rag_source, external_id):
                stats["n_documents_skipped_exists"] += 1
                continue
            if dry_run:
                logger.info(
                    "[DRY RUN] Would ingest %s news %s (%s): "
                    "title=%r url=%s",
                    ticker, filed_date, rag_source,
                    article.canonical_title[:80], article.canonical_url,
                )
                stats["n_documents_ingested"] += 1
                continue
            # One chunk per news article — short bodies. If the excerpt
            # grows in a future adapter (e.g. Benzinga full body), split
            # here at ~400-token windows mirroring
            # ingest_8k_filings._chunk_text.
            section_label = (
                article.canonical_title[:100] if article.canonical_title
                else "news"
            )
            pending.append({
                "article": article,
                "ticker": ticker,
                "rag_source": rag_source,
                "external_id": external_id,
                "chunk": {"content": body, "section_label": section_label},
            })

    if pending:
        try:
            embeddings = embed_texts_fn([item["chunk"]["content"] for item in pending])
        except Exception as e:
            # A batch-level embedding failure (e.g. Voyage API outage)
            # fails every pending document this run — isolated per-run,
            # not per-document, since there is now one shared API call.
            # Still fail soft: log and count, don't crash the caller.
            stats["n_failures"] += len(pending)
            logger.warning(
                "[news_ingest] batch embedding failed for %d pending "
                "document(s): %s", len(pending), e,
            )
            pending = []
        else:
            for item, embedding in zip(pending, embeddings):
                item["chunk"]["embedding"] = embedding

    # Pass 2: ingest each document, isolating per-document failures so
    # one bad write doesn't drop the rest of the batch.
    for item in pending:
        article = item["article"]
        ticker = item["ticker"]
        rag_source = item["rag_source"]
        try:
            doc_id = ingest_document_fn(
                ticker=ticker,
                sector=ticker_to_sector.get(ticker),
                doc_type="news",
                source=rag_source,
                filed_date=filed_date,
                title=article.canonical_title or None,
                url=article.canonical_url or None,
                chunks=[item["chunk"]],
                external_id=item["external_id"],
            )
            if doc_id:
                stats["n_documents_ingested"] += 1
                logger.info(
                    "Ingested news for %s on %s (%s): %s",
                    ticker, filed_date, rag_source,
                    article.canonical_title[:80],
                )
            else:
                stats["n_failures"] += 1
        except Exception as e:
            stats["n_failures"] += 1
            logger.warning(
                "[news_ingest] failed for %s %s: %s",
                ticker, article.canonical_url, e,
            )

    logger.info(
        "[news_ingest] complete: %s", stats,
    )
    return stats
