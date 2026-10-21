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

# Upper bound on how many tickers the verdict names per list. The scope is the
# decision set (~110 names), so this never truncates in practice; it exists so
# a scope regression back toward the ~900-name board cannot balloon the
# artifact. A truncation is recorded, never silent.
MAX_NAMED_TICKERS = 200

# Why the not-fresh tickers are not fresh (alpha-engine-config-I12468). The
# verdict used to carry only counts, so "8 of 108 stale" could not be told
# apart from a dead feed without recomputing it by hand. Derived from the
# watermark store itself: the news ingest advances EVERY ticker in its fetch
# set on a confirmed run, so when the newest mark in the store is fresh, any
# ticker that is not fresh was outside that run's fetch set.
CAUSE_NO_INGEST_RECORDED = "no_ingest_recorded"
CAUSE_INGEST_STALE = "ingest_stale"
CAUSE_SCOPE_CHANGED_AFTER_LAST_INGEST = "scope_changed_after_last_ingest"
CAUSE_OUTSIDE_LAST_FETCH_SET = "outside_last_fetch_set"

_CAUSE_TEXT = {
    CAUSE_NO_INGEST_RECORDED: "the watermark store records no news ingest at all",
    CAUSE_INGEST_STALE: "the news ingest itself has not confirmed a run within the window",
    CAUSE_SCOPE_CHANGED_AFTER_LAST_INGEST: (
        "the scope was published after the last ingest, so these names entered "
        "it before any ingest could fetch them"
    ),
    CAUSE_OUTSIDE_LAST_FETCH_SET: (
        "the ingest ran after this scope was published but did not fetch these "
        "names — its fetch set differs from this scope"
    ),
}


def _parse_ts(raw) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


def _last_ingest(store, doc_type: str) -> datetime | None:
    """Newest confirmed mark for ``doc_type`` across the whole store.

    That is the last time the news ingest confirmed a run, whatever its fetch
    set was. ``None`` when the store is empty or unreadable.
    """
    if store.unreadable:
        return None
    suffix = f"|{doc_type}"
    marks = (_parse_ts(v) for k, v in store.load().items() if k.endswith(suffix))
    return max((m for m in marks if m is not None), default=None)


def _not_fresh_cause(
    *,
    last_ingest: datetime | None,
    scope_generated_at: datetime | None,
    now: datetime,
    max_age_hours: int,
) -> str:
    if last_ingest is None:
        return CAUSE_NO_INGEST_RECORDED
    if (now - last_ingest).total_seconds() / 3600.0 > max_age_hours:
        return CAUSE_INGEST_STALE
    if scope_generated_at is not None and scope_generated_at > last_ingest:
        return CAUSE_SCOPE_CHANGED_AFTER_LAST_INGEST
    return CAUSE_OUTSIDE_LAST_FETCH_SET


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
    stale_detail: list[dict] = []
    oldest_age_h = 0.0
    for t in tickers:
        mark = store.get(t, NEWS_DOC_TYPE)
        if mark is None:
            missing.append(t)
            continue
        age_h = (now - mark).total_seconds() / 3600.0
        oldest_age_h = max(oldest_age_h, age_h)
        if age_h <= max_age_hours:
            fresh.append(t)
        else:
            stale.append(t)
            stale_detail.append({
                "ticker": t,
                "last_ingested_at": _iso(mark),
                "age_hours": round(age_h, 2),
            })
    # Oldest first: the name that has gone longest without news leads.
    stale_detail.sort(key=lambda d: (-d["age_hours"], d["ticker"]))

    last_ingest = _last_ingest(store, NEWS_DOC_TYPE)
    scope_generated_at = _parse_ts(scope.get("generated_at"))
    cause = (
        _not_fresh_cause(
            last_ingest=last_ingest,
            scope_generated_at=scope_generated_at,
            now=now,
            max_age_hours=max_age_hours,
        )
        if (stale or missing) else None
    )

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
    if cause is not None:
        reasons.append(f"cause: {cause} — {_CAUSE_TEXT[cause]}")

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
        # Named, not just counted (alpha-engine-config-I12468): a verdict that
        # says "8 stale" without saying which cannot be acted on.
        "stale_tickers": stale_detail[:MAX_NAMED_TICKERS],
        "stale_tickers_truncated": max(0, len(stale_detail) - MAX_NAMED_TICKERS),
        "missing_tickers": sorted(missing)[:MAX_NAMED_TICKERS],
        "missing_tickers_truncated": max(0, len(missing) - MAX_NAMED_TICKERS),
        "not_fresh_cause": cause,
        "last_ingest_at": _iso(last_ingest),
        "last_ingest_age_hours": (
            round((now - last_ingest).total_seconds() / 3600.0, 2)
            if last_ingest else None
        ),
        "scope_generated_at": _iso(scope_generated_at),
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
            "missing=%d coverage=%.0f%% | stale=%s missing=%s",
            "; ".join(verdict["degraded_reasons"]), verdict["scope"],
            verdict["fresh"], verdict["stale"], verdict["missing"],
            verdict["coverage"] * 100,
            [d["ticker"] for d in verdict.get("stale_tickers", [])],
            verdict.get("missing_tickers", []),
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
