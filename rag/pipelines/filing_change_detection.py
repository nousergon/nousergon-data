"""Filing change detection — "Lazy Prices" signal.

Compares consecutive 10-K filings per ticker using embedding centroid similarity
and section-level text diffs. Low change scores indicate "lazy" management that
may underperform (Cohen, Malloy & Nguyen 2020).

Outputs filing_changes.json to S3 for downstream consumption by the research
scoring pipeline.

Usage:
    python -m rag.pipelines.filing_change_detection --output-s3
    python -m rag.pipelines.filing_change_detection --output-local /tmp/filing_changes.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import date

import numpy as np

logger = logging.getLogger(__name__)


def _load_filing_embeddings() -> dict[str, list[dict]]:
    """Load all 10-K and 10-Q filing embeddings grouped by ticker.

    Returns {ticker: [{filed_date, doc_type, embeddings: [np.array], sections: {label: [np.array]}}]}

    pgvector ``vector`` columns are normalized via
    ``nousergon_lib.rag.coerce_embedding`` — the owned chokepoint that makes
    the ndarray guarantee representation-agnostic (config#2221). The former
    local ``_embedding_to_f32`` was the call-site-only fix (nousergon-data
    PR #747) for the 2026-07-11 weekly-freshness break; it was lifted into
    nousergon-lib so no future consumer of ``c.embedding`` can reintroduce the
    ``float() ... not 'Vector'`` crash.
    """
    from nousergon_lib.rag import coerce_embedding, get_connection

    sql = """
        SELECT d.ticker, d.doc_type, d.filed_date, c.section_label, c.embedding
        FROM rag.documents d
        JOIN rag.chunks c ON c.document_id = d.id
        WHERE d.doc_type IN ('10-K', '10-Q')
        ORDER BY d.ticker, d.filed_date, c.chunk_index
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    # Group by (ticker, doc_type, filed_date)
    grouped: dict[tuple, dict] = {}
    for ticker, doc_type, filed_date, section_label, embedding in rows:
        key = (ticker, doc_type, str(filed_date))
        if key not in grouped:
            grouped[key] = {
                "ticker": ticker,
                "doc_type": doc_type,
                "filed_date": str(filed_date),
                "embeddings": [],
                "sections": defaultdict(list),
            }
        if embedding is not None:
            vec = coerce_embedding(embedding)
            grouped[key]["embeddings"].append(vec)
            if section_label:
                grouped[key]["sections"][section_label].append(vec)

    # Reorganize by ticker
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for entry in grouped.values():
        by_ticker[entry["ticker"]].append(entry)

    # Sort each ticker's filings by date
    for ticker in by_ticker:
        by_ticker[ticker].sort(key=lambda x: x["filed_date"])

    return dict(by_ticker)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two vectors."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def _centroid(vectors: list[np.ndarray]) -> np.ndarray | None:
    """Compute the mean (centroid) of a list of vectors."""
    if not vectors:
        return None
    return np.mean(np.stack(vectors), axis=0)


def compute_filing_changes(min_filings: int = 2) -> list[dict]:
    """Compute filing change scores for all tickers with consecutive filings.

    For each ticker with 2+ same-type filings, computes:
    - overall_similarity: cosine similarity between consecutive filing centroids
    - section_similarities: per-section centroid cosine similarity
    - change_score: 1 - overall_similarity (0 = identical, 1 = completely different)

    Returns list of per-ticker change records.
    """
    by_ticker = _load_filing_embeddings()
    results = []

    for ticker, filings in by_ticker.items():
        # Group by doc_type (compare 10-K to 10-K, 10-Q to 10-Q)
        by_type: dict[str, list[dict]] = defaultdict(list)
        for f in filings:
            by_type[f["doc_type"]].append(f)

        for doc_type, type_filings in by_type.items():
            if len(type_filings) < min_filings:
                continue

            # Compare consecutive filings (most recent pair)
            prev = type_filings[-2]
            curr = type_filings[-1]

            prev_centroid = _centroid(prev["embeddings"])
            curr_centroid = _centroid(curr["embeddings"])

            if prev_centroid is None or curr_centroid is None:
                continue

            overall_sim = _cosine_similarity(prev_centroid, curr_centroid)

            # Section-level similarities
            section_sims = {}
            all_sections = set(prev["sections"].keys()) | set(curr["sections"].keys())
            for section in all_sections:
                prev_sec = _centroid(prev["sections"].get(section, []))
                curr_sec = _centroid(curr["sections"].get(section, []))
                if prev_sec is not None and curr_sec is not None:
                    section_sims[section] = round(_cosine_similarity(prev_sec, curr_sec), 4)

            change_score = round(1.0 - overall_sim, 4)

            record = {
                "ticker": ticker,
                "doc_type": doc_type,
                "prev_date": prev["filed_date"],
                "curr_date": curr["filed_date"],
                "overall_similarity": round(overall_sim, 4),
                "change_score": change_score,
                "section_similarities": section_sims,
                "n_prev_chunks": len(prev["embeddings"]),
                "n_curr_chunks": len(curr["embeddings"]),
            }

            # Flag "lazy" filings: very high similarity = minimal changes
            if change_score < 0.05:
                record["lazy_flag"] = True
                logger.info(
                    "LAZY filing detected: %s %s change_score=%.4f (%s→%s)",
                    ticker, doc_type, change_score, prev["filed_date"], curr["filed_date"],
                )

            # Flag high risk factor changes
            rf_sim = section_sims.get("Risk Factors")
            if rf_sim is not None and rf_sim < 0.90:
                record["risk_factor_change_flag"] = True
                logger.info(
                    "Risk factor change: %s %s RF_similarity=%.4f",
                    ticker, doc_type, rf_sim,
                )

            results.append(record)

    # Sort by change_score ascending (laziest first)
    results.sort(key=lambda x: x["change_score"])

    logger.info(
        "Filing change detection: %d ticker-filing pairs analyzed, %d lazy flags, %d risk factor flags",
        len(results),
        sum(1 for r in results if r.get("lazy_flag")),
        sum(1 for r in results if r.get("risk_factor_change_flag")),
    )
    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Detect filing changes (Lazy Prices signal)")
    parser.add_argument("--output-s3", action="store_true", help="Write results to S3")
    parser.add_argument("--output-local", type=str, help="Write results to local file")
    parser.add_argument("--bucket", type=str, default="alpha-engine-research")
    args = parser.parse_args()

    results = compute_filing_changes()

    output = {
        "date": date.today().isoformat(),
        "n_analyzed": len(results),
        "n_lazy": sum(1 for r in results if r.get("lazy_flag")),
        "n_risk_factor_changes": sum(1 for r in results if r.get("risk_factor_change_flag")),
        "filings": results,
    }

    if args.output_local:
        with open(args.output_local, "w") as f:
            json.dump(output, f, indent=2)
        logger.info("Written to %s", args.output_local)

    if args.output_s3:
        import boto3
        s3 = boto3.client("s3")
        key = f"rag/filing_changes/{date.today().isoformat()}.json"
        s3.put_object(
            Bucket=args.bucket, Key=key,
            Body=json.dumps(output, indent=2).encode(),
            ContentType="application/json",
        )
        # Also write latest pointer
        s3.put_object(
            Bucket=args.bucket, Key="rag/filing_changes/latest.json",
            Body=json.dumps(output, indent=2).encode(),
            ContentType="application/json",
        )
        logger.info("Written to s3://%s/%s (+ latest)", args.bucket, key)

    # Print summary
    print(f"\n{'='*60}")
    print(f"Filing Change Detection — {date.today()}")
    print(f"{'='*60}")
    print(f"Tickers analyzed: {len(set(r['ticker'] for r in results))}")
    print(f"Filing pairs: {len(results)}")
    print(f"Lazy flags (change < 5%): {output['n_lazy']}")
    print(f"Risk factor changes: {output['n_risk_factor_changes']}")

    if results:
        print(f"\nLaziest filings:")
        for r in results[:5]:
            print(f"  {r['ticker']} {r['doc_type']}: change={r['change_score']:.4f} "
                  f"({r['prev_date']}→{r['curr_date']})")

        changed = [r for r in results if r.get("risk_factor_change_flag")]
        if changed:
            print(f"\nRisk factor changes:")
            for r in changed[:5]:
                rf_sim = r["section_similarities"].get("Risk Factors", "N/A")
                print(f"  {r['ticker']} {r['doc_type']}: RF_sim={rf_sim} "
                      f"({r['prev_date']}→{r['curr_date']})")


if __name__ == "__main__":
    main()
