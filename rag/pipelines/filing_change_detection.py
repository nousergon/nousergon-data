"""Filing change detection — "Lazy Prices" signal.

Compares consecutive 10-K filings per ticker using embedding centroid similarity
and section-level text diffs. Low change scores indicate "lazy" management that
may underperform (Cohen, Malloy & Nguyen 2020).

Outputs filing_changes.json to S3 for downstream consumption by the research
scoring pipeline.

Egress contract (config-I2780): centroid aggregation is pushed down to
Postgres — this module must NEVER select raw ``rag.chunks.embedding`` rows
in bulk. A full-corpus embedding read is ~150-250MB of Neon data-transfer
per run and, amplified by per-PR canary replays, exhausted the project's
monthly quota on 2026-07-16 (hard connect lockout for every RAG consumer).
Canary/CI invocations must additionally pass ``--sample-tickers N``.

Usage:
    python -m rag.pipelines.filing_change_detection --output-s3
    python -m rag.pipelines.filing_change_detection --output-local /tmp/filing_changes.json
    python -m rag.pipelines.filing_change_detection --sample-tickers 5 --output-local /tmp/probe.json
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


# Server-side centroid aggregation (config-I2780 / config-I2753 / config-I2781).
#
# The original query here SELECTed the raw ``c.embedding`` column for EVERY
# 10-K/10-Q chunk in the corpus on every invocation (~25k chunks × ~5KB of
# pgvector text wire format ≈ 150-250MB of Neon egress per run), then reduced
# it all client-side to one centroid per filing (+ one per section) for only
# the latest two filings per (ticker, doc_type). That single query shape —
# amplified by the per-PR canary replay re-running it on every push
# (config#2246) — is what exhausted the Neon project's 5GB/month data-transfer
# quota on 2026-07-16 and hard-locked every RAG consumer out of the DB.
#
# The reduction is associative, so it is pushed down to Postgres: pgvector's
# ``AVG(vector)`` computes the element-wise mean (identical semantics to the
# ``np.mean(np.stack(...))`` it replaces), GROUPING SETS emits both the
# per-filing overall centroid and the per-(filing, section) centroids in one
# pass, and DENSE_RANK restricts the scan to the filings actually compared.
# Wire payload drops from the full corpus to ~one vector per (filing ×
# section) actually analyzed — ≈95%+ egress reduction — with the client-side
# similarity/flag logic unchanged.
#
# ``GROUPING(c.section_label)`` disambiguates the overall row (=1) from a
# per-section row whose label is genuinely NULL (=0); unlabeled chunks
# contribute to the overall centroid but not to any section, exactly as the
# old client-side grouping behaved.
_CENTROID_SQL = """
    WITH filings AS (
        SELECT id, ticker, doc_type, filed_date,
               DENSE_RANK() OVER (
                   PARTITION BY ticker, doc_type ORDER BY filed_date DESC
               ) AS date_rank
        FROM rag.documents
        WHERE doc_type IN ('10-K', '10-Q'){ticker_filter}
    )
    SELECT f.ticker,
           f.doc_type,
           f.filed_date,
           c.section_label,
           GROUPING(c.section_label) AS is_overall,
           AVG(c.embedding)          AS centroid,
           COUNT(*)                  AS n_chunks
    FROM filings f
    JOIN rag.chunks c ON c.document_id = f.id
    WHERE f.date_rank <= %s AND c.embedding IS NOT NULL
    GROUP BY GROUPING SETS (
        (f.ticker, f.doc_type, f.filed_date),
        (f.ticker, f.doc_type, f.filed_date, c.section_label)
    )
    ORDER BY f.ticker, f.doc_type, f.filed_date
"""

# Canary/CI sampling (config-I2780 fix b): restrict the scan to the first N
# tickers (deterministic order) so a probe run proves the code path end-to-end
# for kilobytes of egress instead of replaying full production load.
_TICKER_SAMPLE_FILTER = """
          AND ticker IN (
              SELECT DISTINCT ticker FROM rag.documents
              WHERE doc_type IN ('10-K', '10-Q')
              ORDER BY ticker
              LIMIT %s
          )"""


def _load_filing_centroids(
    min_filings: int = 2, sample_tickers: int | None = None
) -> dict[str, list[dict]]:
    """Load per-filing embedding centroids for the latest filings per ticker.

    Aggregation happens server-side (see ``_CENTROID_SQL``); only centroids
    cross the wire. Fetches the latest ``max(2, min_filings)`` distinct
    filing dates per (ticker, doc_type): two is all the consecutive-pair
    comparison reads, but ``min_filings > 2`` callers still need to OBSERVE
    that many filings exist to preserve the original threshold semantics.

    DENSE_RANK (not ROW_NUMBER) ranks by distinct ``filed_date``, so a filing
    ingested from two sources on the same date collapses into one centroid
    group — matching the old client-side grouping by (ticker, doc_type, date).

    Args:
        min_filings: Same threshold ``compute_filing_changes`` applies.
        sample_tickers: When set (canary/CI probes), restrict to the first N
            tickers so the probe never replays full production load
            (config-I2780).

    Returns:
        {ticker: [{ticker, doc_type, filed_date, centroid: np.ndarray,
                   sections: {label: np.ndarray}, n_chunks: int}, ...]}
        with each ticker's filings sorted by filed_date ascending.

    pgvector ``vector`` columns (the centroid + per-section aggregates
    returned by ``_CENTROID_SQL``) are normalized via
    ``nousergon_lib.rag.coerce_embedding`` — the owned chokepoint that makes
    the ndarray guarantee representation-agnostic (config#2221). The former
    local ``_embedding_to_f32`` was the call-site-only fix (nousergon-data
    PR #747) for the 2026-07-11 weekly-freshness break; it was lifted into
    nousergon-lib so no future consumer of a ``vector`` column can
    reintroduce the ``float() ... not 'Vector'`` crash.
    """
    from nousergon_lib.rag import coerce_embedding
    from nousergon_lib.rag.db import get_connection

    params: list = []
    if sample_tickers is not None:
        if sample_tickers < 1:
            raise ValueError(f"sample_tickers must be >= 1; got {sample_tickers}")
        ticker_filter = _TICKER_SAMPLE_FILTER
        params.append(sample_tickers)
    else:
        ticker_filter = ""
    sql = _CENTROID_SQL.format(ticker_filter=ticker_filter)
    params.append(max(2, min_filings))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    grouped: dict[tuple, dict] = {}
    for ticker, doc_type, filed_date, section_label, is_overall, centroid, n_chunks in rows:
        key = (ticker, doc_type, str(filed_date))
        if key not in grouped:
            grouped[key] = {
                "ticker": ticker,
                "doc_type": doc_type,
                "filed_date": str(filed_date),
                "centroid": None,
                "sections": {},
                "n_chunks": 0,
            }
        if is_overall:
            grouped[key]["centroid"] = coerce_embedding(centroid)
            grouped[key]["n_chunks"] = int(n_chunks)
        elif section_label:
            grouped[key]["sections"][section_label] = coerce_embedding(centroid)

    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for entry in grouped.values():
        by_ticker[entry["ticker"]].append(entry)

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


def compute_filing_changes(
    min_filings: int = 2, sample_tickers: int | None = None
) -> list[dict]:
    """Compute filing change scores for all tickers with consecutive filings.

    For each ticker with 2+ same-type filings, computes:
    - overall_similarity: cosine similarity between consecutive filing centroids
    - section_similarities: per-section centroid cosine similarity
    - change_score: 1 - overall_similarity (0 = identical, 1 = completely different)

    Args:
        min_filings: Minimum same-type filings a ticker needs to be analyzed.
        sample_tickers: When set, analyze only the first N tickers — the
            canary/CI probe knob (config-I2780); production runs leave it None.

    Returns list of per-ticker change records.
    """
    by_ticker = _load_filing_centroids(
        min_filings=min_filings, sample_tickers=sample_tickers
    )
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

            prev_centroid = prev["centroid"]
            curr_centroid = curr["centroid"]

            if prev_centroid is None or curr_centroid is None:
                continue

            overall_sim = _cosine_similarity(prev_centroid, curr_centroid)

            # Section-level similarities (only sections present in BOTH filings)
            section_sims = {}
            all_sections = set(prev["sections"].keys()) | set(curr["sections"].keys())
            for section in all_sections:
                prev_sec = prev["sections"].get(section)
                curr_sec = curr["sections"].get(section)
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
                "n_prev_chunks": prev["n_chunks"],
                "n_curr_chunks": curr["n_chunks"],
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
    parser.add_argument(
        "--sample-tickers",
        type=int,
        default=None,
        help=(
            "Restrict the analysis to the first N tickers (deterministic "
            "order). Canary/CI probe knob (config-I2780): proves the "
            "DB-to-S3 code path end-to-end for kilobytes of Neon egress "
            "instead of replaying full production load. Production runs "
            "omit it."
        ),
    )
    parser.add_argument(
        "--key-prefix",
        type=str,
        default="",
        help=(
            "Prefix the dated S3 key with this (e.g. 'canary/{run_id}/') and "
            "never touch the real 'latest.json' pointer. Used by the "
            "Saturday-replay canary (alpha-engine-config#2246) to exercise "
            "this pipeline against the live pgvector corpus without "
            "clobbering the production pointer."
        ),
    )
    args = parser.parse_args()

    results = compute_filing_changes(sample_tickers=args.sample_tickers)

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
        key = f"rag/filing_changes/{args.key_prefix}{date.today().isoformat()}.json"
        s3.put_object(
            Bucket=args.bucket, Key=key,
            Body=json.dumps(output, indent=2).encode(),
            ContentType="application/json",
        )
        if args.key_prefix:
            # Canary/staging run — never touch the real pointer consumers read.
            logger.info("Written to s3://%s/%s (key-prefix set, latest.json untouched)", args.bucket, key)
        else:
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

    # Grep-able single-line summary for orchestrating shell scripts (the
    # Saturday-replay canary's spot bootstrap in particular — see
    # alpha-engine-config#2246).
    print("RESULT_JSON=" + json.dumps({
        "status": "PASS",  # canary aggregator contract: literal PASS (config-I2748)
        "n_analyzed": output["n_analyzed"],
        "n_lazy": output["n_lazy"],
        "n_risk_factor_changes": output["n_risk_factor_changes"],
    }))


if __name__ == "__main__":
    main()
