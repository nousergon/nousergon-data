"""RAG corpus manifest emitter.

Aggregates the live pgvector corpus into a single JSON snapshot and writes
it to S3 as the public-safe RAG inventory artifact. The presentation layer
(public Knowledge Base panel + private dashboard inventory page) reads from
this manifest — it never queries pgvector directly. Per Decision 11 of the
presentation revamp plan, presentation surfaces are *views* of upstream
outputs, not new measurement layers.

What lands in the manifest:

- ``by_source``: per ``doc_type`` rollup (10-K, 10-Q, 8-K,
  earnings_transcript, thesis) — document count, ticker count, chunk count
- ``by_ticker_coverage``: how many tickers are covered + per-ticker depth
  percentiles (p25 / p50 / p75)
- ``totals``: documents, chunks, tickers
- ``embedding``: model name + dimension + chunk vector dimension from the
  ``rag.chunks.embedding`` column
- ``ingestion``: latest ``ingested_at`` overall + per ``doc_type``, plus
  per-(date, doc_type) document/chunk counts so the dashboard can render
  the inventory as a date×doc_type pivot

What is intentionally *not* in the manifest (disclosure boundary):
per-ticker doc lists, individual document titles, chunk content. Those
stay private and only surface on dashboard.nousergon.ai under Cloudflare
Access during interview screenshare.

Usage::

    python -m rag.pipelines.emit_manifest --output-s3
    python -m rag.pipelines.emit_manifest --output-local /tmp/manifest.json
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from nousergon_lib.rag.db import execute_query

logger = logging.getLogger(__name__)

# Hardcoded; ``rag.embeddings.embed_*`` defaults to voyage-3-lite (512d,
# matches the ``embedding vector(512)`` column in ``rag/schema.sql``).
# Surfaced in the manifest so consumers don't have to re-derive it.
_EMBEDDING_MODEL = "voyage-3-lite"
_EMBEDDING_DIMENSION = 512


def _by_source() -> dict[str, dict[str, int]]:
    """Per ``doc_type``: documents, distinct tickers, chunks."""
    rows = execute_query(
        """
        SELECT
            d.doc_type,
            COUNT(DISTINCT d.id)        AS documents,
            COUNT(DISTINCT d.ticker)    AS tickers,
            COUNT(c.id)                 AS chunks
        FROM rag.documents d
        LEFT JOIN rag.chunks c ON c.document_id = d.id
        GROUP BY d.doc_type
        ORDER BY d.doc_type
        """
    )
    return {
        r["doc_type"]: {
            "documents": int(r["documents"]),
            "tickers": int(r["tickers"]),
            "chunks": int(r["chunks"]),
        }
        for r in rows
    }


def _by_ticker_coverage() -> dict[str, Any]:
    """Universe coverage rollup: how many tickers, depth percentiles."""
    rows = execute_query(
        """
        WITH per_ticker AS (
            SELECT ticker, COUNT(*) AS doc_count
            FROM rag.documents
            GROUP BY ticker
        )
        SELECT
            COUNT(*)                                                       AS tickers_with_any_doc,
            PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY doc_count)        AS p25_docs,
            PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY doc_count)        AS p50_docs,
            PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY doc_count)        AS p75_docs
        FROM per_ticker
        """
    )
    if not rows:
        return {
            "tickers_with_any_doc": 0,
            "p25_docs_per_ticker": 0,
            "p50_docs_per_ticker": 0,
            "p75_docs_per_ticker": 0,
        }
    r = rows[0]
    return {
        "tickers_with_any_doc": int(r["tickers_with_any_doc"] or 0),
        "p25_docs_per_ticker": int(r["p25_docs"] or 0),
        "p50_docs_per_ticker": int(r["p50_docs"] or 0),
        "p75_docs_per_ticker": int(r["p75_docs"] or 0),
    }


def _totals() -> dict[str, int]:
    rows = execute_query(
        """
        SELECT
            (SELECT COUNT(*) FROM rag.documents)                AS documents,
            (SELECT COUNT(*) FROM rag.chunks)                   AS chunks,
            (SELECT COUNT(DISTINCT ticker) FROM rag.documents)  AS tickers
        """
    )
    r = rows[0] if rows else {"documents": 0, "chunks": 0, "tickers": 0}
    return {
        "documents": int(r["documents"] or 0),
        "chunks": int(r["chunks"] or 0),
        "tickers": int(r["tickers"] or 0),
    }


def _by_date_source() -> list[dict[str, Any]]:
    """Per (ingestion calendar date, doc_type): documents + chunks.

    Powers the dashboard's date×doc_type pivot. Aggregates only — no
    titles, no per-ticker breakdown — so it stays inside the public-safe
    disclosure boundary alongside the rest of the manifest.
    """
    rows = execute_query(
        """
        SELECT
            DATE(d.ingested_at)         AS ingestion_date,
            d.doc_type                  AS doc_type,
            COUNT(DISTINCT d.id)        AS documents,
            COUNT(c.id)                 AS chunks
        FROM rag.documents d
        LEFT JOIN rag.chunks c ON c.document_id = d.id
        WHERE d.ingested_at IS NOT NULL
        GROUP BY DATE(d.ingested_at), d.doc_type
        ORDER BY ingestion_date DESC, doc_type
        """
    )
    out = []
    for r in rows:
        d = r["ingestion_date"]
        out.append({
            "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
            "doc_type": r["doc_type"],
            "documents": int(r["documents"] or 0),
            "chunks": int(r["chunks"] or 0),
        })
    return out


def _ingestion() -> dict[str, Any]:
    rows = execute_query(
        """
        SELECT doc_type, MAX(ingested_at) AS last_ts
        FROM rag.documents
        GROUP BY doc_type
        ORDER BY doc_type
        """
    )
    by_source_last_ts = {r["doc_type"]: r["last_ts"].isoformat() for r in rows if r["last_ts"]}
    overall = max(by_source_last_ts.values(), default=None)
    return {
        "last_run_ts": overall,
        "by_source_last_ts": by_source_last_ts,
        "by_date_source": _by_date_source(),
    }


def build_manifest() -> dict[str, Any]:
    """Assemble the manifest dict by querying pgvector."""
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.1.0",
        "totals": _totals(),
        "by_source": _by_source(),
        "by_ticker_coverage": _by_ticker_coverage(),
        "embedding": {
            "model": _EMBEDDING_MODEL,
            "dimension": _EMBEDDING_DIMENSION,
        },
        "ingestion": _ingestion(),
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Emit RAG corpus manifest")
    parser.add_argument("--output-s3", action="store_true", help="Write manifest to S3 (date + latest pointer)")
    parser.add_argument("--output-local", type=str, help="Write manifest to local file")
    parser.add_argument("--bucket", type=str, default="alpha-engine-research")
    args = parser.parse_args()

    manifest = build_manifest()

    if args.output_local:
        with open(args.output_local, "w") as f:
            json.dump(manifest, f, indent=2, default=str)
        logger.info("Written to %s", args.output_local)

    if args.output_s3:
        import boto3
        s3 = boto3.client("s3")
        body = json.dumps(manifest, indent=2, default=str).encode()
        dated_key = f"rag/manifest/{date.today().isoformat()}.json"
        s3.put_object(
            Bucket=args.bucket, Key=dated_key,
            Body=body, ContentType="application/json",
        )
        s3.put_object(
            Bucket=args.bucket, Key="rag/manifest/latest.json",
            Body=body, ContentType="application/json",
        )
        logger.info("Written to s3://%s/%s (+ latest)", args.bucket, dated_key)


if __name__ == "__main__":
    main()
