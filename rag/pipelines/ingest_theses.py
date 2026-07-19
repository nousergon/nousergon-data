"""Ingest Alpha Engine research history into the RAG vector store.

Embeds agent reports (news briefs, research briefs, macro reports) and
investment thesis summaries from research.db. Enables semantic search over
prior analysis (e.g., "What were the risks for AAPL last month?").

Data sources:
    - agent_reports: per-ticker news/research markdown briefs from LLM agents
    - investment_thesis: composite scores, ratings, thesis summaries

Usage:
    # Ingest all records from research.db
    python -m rag.pipelines.ingest_theses --db-path /path/to/research.db

    # Ingest only new records since last run
    python -m rag.pipelines.ingest_theses --db-path /path/to/research.db --since 2026-03-01

    # v2 signals theses, restricted to the corpus scope (holdings ∪ active
    # candidates ∪ top-60 signals board — config#2943)
    python -m rag.pipelines.ingest_theses --signals --scope holdings+candidates+board60 --since 2026-07-05
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import date

from rag.pipelines._corpus_scope import (
    DEFAULT_BUCKET,
    add_scope_arg,
    resolve_corpus_scope,
)

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 400
_CHUNK_OVERLAP = 50


def _chunk_text(text: str, chunk_size: int = _CHUNK_SIZE, overlap: int = _CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks by approximate token count."""
    words = text.split()
    words_per_chunk = int(chunk_size / 1.3)
    overlap_words = int(overlap / 1.3)

    chunks = []
    start = 0
    while start < len(words):
        end = start + words_per_chunk
        chunk = " ".join(words[start:end])
        if chunk.strip():
            chunks.append(chunk)
        start = end - overlap_words
        if start >= len(words):
            break
    return chunks


def _load_agent_reports(db_path: str, since: str | None = None) -> list[dict]:
    """Load agent reports from research.db."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    sql = "SELECT symbol, date, agent_type, report_md, word_count FROM agent_reports WHERE length(report_md) > 100"
    params = []
    if since:
        sql += " AND date >= ?"
        params.append(since)
    sql += " ORDER BY date DESC"

    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows


def _load_investment_theses(db_path: str, since: str | None = None) -> list[dict]:
    """Load investment thesis records from research.db."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    sql = """SELECT symbol, date, rating, score, thesis_summary, conviction, signal,
                    technical_score, news_score, research_score
             FROM investment_thesis WHERE length(thesis_summary) > 50"""
    params = []
    if since:
        sql += " AND date >= ?"
        params.append(since)
    sql += " ORDER BY date DESC"

    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows


def _field(entry: dict, key: str, default):
    """Read an entry field, normalising both an ABSENT key and an explicit JSON `null` to `default`.

    `dict.get(key, default)` only substitutes `default` when `key` is absent; an
    explicit JSON `null` (which producers like quant_envelope_producer write for
    several signals.json entry fields, not just thesis_summary) yields `None`
    straight through, which then blows up downstream `len()`/`.get()` calls
    (config#2938 follow-on).
    """
    value = entry.get(key)
    return default if value is None else value


def _agent_type_to_section(agent_type: str) -> str:
    """Map agent_type to a section label for retrieval filtering."""
    return {
        "news": "news_brief",
        "research": "research_brief",
        "macro": "macro_report",
        "consolidator": "weekly_consolidation",
    }.get(agent_type, agent_type)


def ingest_theses(
    db_path: str,
    since: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Ingest agent reports and investment theses into RAG store.

    Returns summary dict with counts.
    """
    from nousergon_lib.rag.embeddings import embed_texts
    from nousergon_lib.rag.retrieval import ingest_document, document_exists

    results = {"agent_reports": 0, "investment_theses": 0, "skipped_dedup": 0, "chunks_total": 0}

    # ── Agent reports (news/research/macro briefs) ───────────────────────────
    reports = _load_agent_reports(db_path, since)
    logger.info("Loaded %d agent reports from %s", len(reports), db_path)

    for report in reports:
        ticker = report.get("symbol", "")
        date_str = report.get("date", "")
        agent_type = report.get("agent_type", "")
        content = report.get("report_md", "")

        if not ticker or not date_str or not content:
            continue

        try:
            filed_date = date.fromisoformat(date_str[:10])
        except ValueError:
            continue

        # Use agent_type as part of doc_type for dedup granularity
        doc_type = f"thesis_{agent_type}"
        source = "alpha_engine"

        if document_exists(ticker, doc_type, filed_date, source):
            results["skipped_dedup"] += 1
            continue

        if dry_run:
            logger.info("[DRY RUN] Would ingest %s %s %s", ticker, doc_type, filed_date)
            results["agent_reports"] += 1
            continue

        # Chunk the markdown report
        text_chunks = _chunk_text(content)
        if not text_chunks:
            continue

        section_label = _agent_type_to_section(agent_type)
        all_chunks = [{"content": c, "section_label": section_label} for c in text_chunks]

        # Embed
        embeddings = embed_texts([c["content"] for c in all_chunks])
        for chunk, emb in zip(all_chunks, embeddings):
            chunk["embedding"] = emb

        doc_id = ingest_document(
            ticker=ticker,
            sector=None,
            doc_type=doc_type,
            source=source,
            filed_date=filed_date,
            title=f"{ticker} {agent_type} brief — {filed_date}",
            url=None,
            chunks=all_chunks,
        )
        if doc_id:
            results["agent_reports"] += 1
            results["chunks_total"] += len(all_chunks)

    # ── Investment thesis summaries ──────────────────────────────────────────
    theses = _load_investment_theses(db_path, since)
    logger.info("Loaded %d investment thesis records from %s", len(theses), db_path)

    for thesis in theses:
        ticker = thesis.get("symbol", "")
        date_str = thesis.get("date", "")
        summary = thesis.get("thesis_summary", "")

        if not ticker or not date_str or len(summary) < 50:
            continue

        try:
            filed_date = date.fromisoformat(date_str[:10])
        except ValueError:
            continue

        doc_type = "thesis_score"
        source = "alpha_engine"

        if document_exists(ticker, doc_type, filed_date, source):
            results["skipped_dedup"] += 1
            continue

        if dry_run:
            results["investment_theses"] += 1
            continue

        # Build enriched content with score context
        rating = thesis.get("rating", "")
        score = thesis.get("score", "")
        conviction = thesis.get("conviction", "")
        signal = thesis.get("signal", "")
        enriched = (
            f"[{ticker} | {filed_date} | Rating: {rating} | Score: {score} | "
            f"Conviction: {conviction} | Signal: {signal}]\n{summary}"
        )

        chunks = [{"content": enriched, "section_label": "thesis_summary"}]
        embeddings = embed_texts([c["content"] for c in chunks])
        for chunk, emb in zip(chunks, embeddings):
            chunk["embedding"] = emb

        doc_id = ingest_document(
            ticker=ticker,
            sector=None,
            doc_type=doc_type,
            source=source,
            filed_date=filed_date,
            title=f"{ticker} thesis {rating} ({score}) — {filed_date}",
            url=None,
            chunks=chunks,
        )
        if doc_id:
            results["investment_theses"] += 1
            results["chunks_total"] += len(chunks)

    logger.info(
        "Ingestion complete: %d agent reports, %d theses, %d chunks, %d dedup skipped",
        results["agent_reports"], results["investment_theses"],
        results["chunks_total"], results["skipped_dedup"],
    )
    return results


def ingest_signals_theses(
    bucket: str = "alpha-engine-research",
    since: str | None = None,
    dry_run: bool = False,
    scope: set[str] | None = None,
) -> dict:
    """Ingest thesis summaries from v2 signals.json files on S3.

    The v2 sector-team architecture (quant + qual sub-scores) writes thesis
    content to signals/{date}/signals.json rather than SQLite agent_reports.

    Args:
        scope: config#2943 — restrict ingestion to this ticker set (holdings
            ∪ active candidates ∪ top-60 signals board, see
            ``rag.pipelines._corpus_scope.resolve_corpus_scope``). ``None``
            means no filtering (legacy full-universe behavior) — callers
            should always pass the resolved scope in production; ``None``
            is retained only so existing direct-call tests don't have to
            thread a scope through.

    Returns summary dict with counts.
    """
    import boto3
    from nousergon_lib.rag.embeddings import embed_texts
    from nousergon_lib.rag.retrieval import ingest_document, document_exists

    s3 = boto3.client("s3")
    results = {"signals_theses": 0, "skipped_dedup": 0, "chunks_total": 0, "skipped_out_of_scope": 0}

    # List signal dates
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="signals/", Delimiter="/")
    prefixes = sorted([p["Prefix"] for p in resp.get("CommonPrefixes", [])])

    for prefix in prefixes:
        date_str = prefix.strip("/").split("/")[-1]
        if since and date_str < since:
            continue

        try:
            filed_date = date.fromisoformat(date_str)
        except ValueError:
            continue

        # Load signals.json
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"{prefix}signals.json")
            data = json.loads(obj["Body"].read())
        except Exception:
            continue

        universe = data.get("universe", [])
        market_regime = data.get("market_regime", "")

        for entry in universe:
            ticker = _field(entry, "ticker", "")
            if scope is not None and ticker.upper() not in scope:
                results["skipped_out_of_scope"] += 1
                continue
            # Quant-envelope signals (stance_source="quant_envelope_producer") carry
            # thesis_summary=null by design — no LLM narrative to embed — so a null
            # thesis normalises to "" and the <50-char guard skips them cleanly
            # instead of raising `len(None)` (config#2938 follow-on, 2026-07-18).
            thesis = _field(entry, "thesis_summary", "")
            if not ticker or len(thesis) < 50:
                continue

            doc_type = "thesis_signal"
            if document_exists(ticker, doc_type, filed_date, "alpha_engine"):
                results["skipped_dedup"] += 1
                continue

            if dry_run:
                results["signals_theses"] += 1
                continue

            # Sibling fields may also carry explicit null from the same producer;
            # route every read through `_field` so a null anywhere in the entry
            # can't crash ingestion or silently embed the literal string 'None'.
            score = _field(entry, "score", "")
            signal = _field(entry, "signal", "")
            conviction = _field(entry, "conviction", "")
            sub_scores = _field(entry, "sub_scores", {})
            quant = _field(sub_scores, "quant", "")
            qual = _field(sub_scores, "qual", "")
            sector = _field(entry, "sector", None)

            enriched = (
                f"[{ticker} | {filed_date} | Signal: {signal} | Score: {score} | "
                f"Conviction: {conviction} | Quant: {quant} | Qual: {qual} | "
                f"Regime: {market_regime}]\n{thesis}"
            )

            chunks = [{"content": enriched, "section_label": "thesis_quant_qual"}]
            embeddings = embed_texts([c["content"] for c in chunks])
            for chunk, emb in zip(chunks, embeddings):
                chunk["embedding"] = emb

            doc_id = ingest_document(
                ticker=ticker,
                sector=sector,
                doc_type=doc_type,
                source="alpha_engine",
                filed_date=filed_date,
                title=f"{ticker} {signal} ({score}) quant={quant} qual={qual} — {filed_date}",
                url=None,
                chunks=chunks,
            )
            if doc_id:
                results["signals_theses"] += 1
                results["chunks_total"] += 1

    logger.info(
        "Signals thesis ingestion: %d theses, %d chunks, %d dedup skipped",
        results["signals_theses"], results["chunks_total"], results["skipped_dedup"],
    )
    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingest thesis history into RAG store")
    parser.add_argument("--db-path", type=str, help="Path to research.db (for legacy agent_reports)")
    parser.add_argument("--signals", action="store_true", help="Ingest v2 theses from signals.json on S3")
    # Shared --scope flag definition (config#2943) — this ingestor has no
    # --tickers (it's driven by iterating signals.json's universe, not a
    # ticker list), so it uses add_scope_arg directly rather than the
    # resolve_tickers_from_args wrapper the other 5 ingestors use.
    add_scope_arg(parser)
    parser.add_argument("--bucket", type=str, default=DEFAULT_BUCKET)
    parser.add_argument("--since", type=str, help="Only ingest records from this date forward (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.signals:
        scope = resolve_corpus_scope(bucket=args.bucket) if args.scope else None
        results = ingest_signals_theses(bucket=args.bucket, since=args.since, dry_run=args.dry_run, scope=scope)
        print(json.dumps(results, indent=2))
    elif args.db_path:
        results = ingest_theses(args.db_path, since=args.since, dry_run=args.dry_run)
        print(json.dumps(results, indent=2))
    else:
        parser.error("Provide --db-path (legacy) or --signals (v2)")


if __name__ == "__main__":
    main()
