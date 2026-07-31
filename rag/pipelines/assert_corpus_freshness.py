"""Corpus freshness assertion — the weekly pipeline READS, it does not FILL.

``rag-corpus-policy.md`` §2.3: *ingestion runs on its own cadence, in its own
process, off every decision pipeline's critical path. A decision pipeline may
VERIFY corpus freshness; it may never FILL the corpus.*

WHAT THIS REPLACES
------------------
``run_weekly_ingestion.sh`` step 5 used to run the full news fetch inline —
the ~3.1h Polygon sweep that dominated ``RAGIngestion``'s 6h budget and was
the stage in flight when the 2026-07-29 weekly pipeline died
(``alpha-engine-config-I5695``). ``collectors/daily_news.py`` now ingests the
same articles into the corpus at the point it already fetches them every
weekday (config-I5702), so by Saturday the corpus is warm and this step only
has to answer one question:

    is the corpus current enough for this run?

THE NON-NEGOTIABLE
------------------
**This never fetches.** A stale corpus sets a visible degraded flag and the
run proceeds on what is held. It does not stop to fill. §2.3's own warning:
*"an assertion that blocks or backfills inline reintroduces the exact coupling
this removes."* If the assertion fails, something OFF the critical path
repairs the corpus — that is the whole point.

Exit code is ALWAYS 0. Staleness is a degraded flag, not a pipeline failure:
a retrieval-time miss degrades one answer, while failing the weekly pipeline
over stale news degrades the belief set the whole trading week depends on.
The degraded state is reported by the artifact and the log, never by a
non-zero exit — which is exactly the honest-degradation shape
``weekly-sf-policy.md`` §2.3 requires.

THE SOURCE OF TRUTH
-------------------
The watermark store (config-I5701) already records the last CONFIRMED ingest
per ``(ticker, doc_type)``. Freshness is therefore *derived from what actually
landed*, not from an artifact's mtime — an artifact can be rewritten by a run
that ingested nothing, but a watermark only advances on a confirmed store.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

FRESHNESS_KEY = "rag/corpus_freshness/latest.json"

# Weekdays only, so a Saturday run legitimately sees Friday-close data. The
# weekly SF fires one day after the week's last trading session, so ~2 days of
# slack covers the normal case; anything beyond that means the daily job has
# actually stopped.
DEFAULT_MAX_AGE_HOURS = 60

# Below this share of the scope covered, the corpus is not merely stale, it is
# patchy — a distinct condition worth naming separately in the flag.
DEFAULT_MIN_COVERAGE = 0.80


def assess(
    *,
    bucket: str,
    run_date: str | None = None,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    s3_client=None,
    now: datetime | None = None,
) -> dict:
    """Return the freshness verdict for the current decision set.

    Never raises on a stale or patchy corpus — that is the reported outcome,
    not an error. Only an unresolvable SCOPE raises, because without a scope
    there is no question to answer.
    """
    from rag.pipelines._rag_scope import load_rag_scope
    from rag.pipelines._watermarks import (
        WatermarkStore,
        _utcnow,
    )
    from rag.pipelines.run_news_pipeline import NEWS_DOC_TYPE, NEWS_SOURCE

    now = now or _utcnow()
    scope = load_rag_scope(bucket=bucket, s3_client=s3_client, run_date=run_date)
    tickers = scope["tickers"]

    store = WatermarkStore(NEWS_SOURCE, bucket=bucket, s3_client=s3_client)
    fresh, stale, missing = [], [], []
    oldest_age_h = 0.0
    for t in tickers:
        mark = store.get(t, NEWS_DOC_TYPE)
        if mark is None:
            missing.append(t)
            continue
        age_h = (now - mark).total_seconds() / 3600.0
        oldest_age_h = max(oldest_age_h, age_h)
        (fresh if age_h <= max_age_hours else stale).append(t)

    covered = len(fresh)
    coverage = (covered / len(tickers)) if tickers else 0.0

    reasons = []
    if missing:
        reasons.append(f"{len(missing)} ticker(s) never ingested")
    if stale:
        reasons.append(
            f"{len(stale)} ticker(s) older than {max_age_hours}h "
            f"(oldest {oldest_age_h:.1f}h)"
        )
    if coverage < min_coverage:
        reasons.append(
            f"coverage {coverage:.0%} below the {min_coverage:.0%} floor"
        )

    return {
        "schema_version": 1,
        "checked_at": now.isoformat().replace("+00:00", "Z"),
        "run_date": scope.get("run_date"),
        "source": NEWS_SOURCE,
        "doc_type": NEWS_DOC_TYPE,
        "scope": len(tickers),
        "fresh": covered,
        "stale": len(stale),
        "missing": len(missing),
        "coverage": round(coverage, 4),
        "oldest_age_hours": round(oldest_age_h, 2),
        "max_age_hours": max_age_hours,
        "min_coverage": min_coverage,
        "degraded": bool(reasons),
        "degraded_reasons": reasons,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", default="alpha-engine-research")
    p.add_argument("--run-date", default=None)
    p.add_argument("--max-age-hours", type=int, default=DEFAULT_MAX_AGE_HOURS)
    p.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE)
    p.add_argument(
        "--no-write", action="store_true",
        help="Assess and log without publishing the freshness artifact.",
    )
    args = p.parse_args()

    try:
        verdict = assess(
            bucket=args.bucket,
            run_date=args.run_date,
            max_age_hours=args.max_age_hours,
            min_coverage=args.min_coverage,
        )
    except Exception as e:
        # An unresolvable scope is a real upstream failure, but it must still
        # not take the weekly pipeline down over NEWS. Report maximally
        # degraded and exit 0 — the scope failure itself is already loud in
        # every other stage that needs it.
        logger.error("[corpus_freshness] assessment FAILED (%s: %s) — "
                     "reporting DEGRADED and continuing", type(e).__name__, e)
        print(json.dumps({"degraded": True, "degraded_reasons": [str(e)],
                          "schema_version": 1}))
        return 0

    if verdict["degraded"]:
        logger.warning(
            "[corpus_freshness] DEGRADED — %s | scope=%d fresh=%d stale=%d "
            "missing=%d coverage=%.0f%%",
            "; ".join(verdict["degraded_reasons"]), verdict["scope"],
            verdict["fresh"], verdict["stale"], verdict["missing"],
            verdict["coverage"] * 100,
        )
    else:
        logger.info(
            "[corpus_freshness] OK — scope=%d fresh=%d coverage=%.0f%% "
            "oldest=%.1fh",
            verdict["scope"], verdict["fresh"], verdict["coverage"] * 100,
            verdict["oldest_age_hours"],
        )

    if not args.no_write:
        try:
            import boto3
            boto3.client("s3").put_object(
                Bucket=args.bucket, Key=FRESHNESS_KEY,
                Body=json.dumps(verdict).encode(),
                ContentType="application/json",
            )
            logger.info("[corpus_freshness] published s3://%s/%s",
                        args.bucket, FRESHNESS_KEY)
        except Exception as e:
            logger.warning("[corpus_freshness] publish failed (%s) — the "
                           "verdict is still on stdout and in this log", e)

    # ALWAYS 0. See the module docstring: staleness is a degraded flag, never a
    # pipeline failure.
    return 0


if __name__ == "__main__":
    sys.exit(main())
