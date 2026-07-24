"""Persisted corpus-scope state (config#2943 deliverable 2b — ticker churn).

The daily corpus delta needs to know, each day, which tickers are NEW to
the scope since yesterday's pass — those get the full 2yr-filings backfill
folded into today's delta (the ruling's "sized fine at ~1-5 tickers/day of
turnover"), while tickers already in scope only need a short incremental
lookback (new filings/articles since the last pass).

State is a small JSON pointer on S3 (``rag_corpus/scope_state/latest.json``)
recording the resolved scope as of the last successful daily/Saturday pass.
Deliberately NOT ArcticDB/Postgres — this is a tiny (~150-ticker) set, S3
JSON is the simplest thing that works and matches the "pointer + dated
sidecar" convention already used by ``daily_news.py``'s
``data/news_aggregates_daily/latest.json``.
"""

from __future__ import annotations

import json
import logging
from datetime import date as Date
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
SCOPE_STATE_KEY = "rag_corpus/scope_state/latest.json"


def load_prior_scope(bucket: str = DEFAULT_BUCKET, s3_client: Any = None) -> set[str]:
    """Read the scope recorded by the last successful delta pass.

    Fail-soft: a missing/unreadable pointer (first-ever run, or a
    transient S3 blip) returns an empty set — the caller then treats
    EVERY resolved ticker as "new" and folds the full 2yr backfill for
    all of them into that run. Safe (if slower) default: never silently
    skips a legitimate backfill because the pointer was unreadable.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=SCOPE_STATE_KEY)
        data = json.loads(obj["Body"].read())
        tickers = {str(t).strip().upper() for t in data.get("tickers", []) if str(t).strip()}
        logger.info("[corpus_scope_state] loaded prior scope: %d tickers (as_of=%s)",
                    len(tickers), data.get("as_of"))
        return tickers
    except Exception as e:
        logger.warning("[corpus_scope_state] no readable prior scope (%s) — "
                        "treating every resolved ticker as new-to-scope", e)
        return set()


def write_scope_state(
    tickers: set[str],
    as_of: Date | None = None,
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
) -> str:
    """Persist today's resolved scope as the new pointer.

    Called only after a delta pass completes (success or partial-with-
    logged-failures) — never on a hard abort, so a bad run doesn't
    poison tomorrow's churn detection with a truncated scope.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    as_of = as_of or Date.today()
    payload = {
        "as_of": as_of.isoformat(),
        "tickers": sorted(tickers),
        "count": len(tickers),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=SCOPE_STATE_KEY,
        Body=json.dumps(payload, indent=2).encode(),
        ContentType="application/json",
    )
    logger.info("[corpus_scope_state] wrote scope state: %d tickers (as_of=%s)",
                len(tickers), as_of)
    return SCOPE_STATE_KEY


def diff_scope(current: set[str], prior: set[str]) -> tuple[set[str], set[str]]:
    """Return ``(new_to_scope, dropped_from_scope)``.

    ``dropped_from_scope`` is informational only (config#2943 ruling: rows
    for out-of-scope tickers are RETAINED, never deleted — no caller should
    act on this by removing anything). ``new_to_scope`` drives the 2yr
    backfill fold-in.
    """
    return (current - prior, prior - current)


# Beyond this many days without a successful daily-delta write, the Saturday
# top-up's short lookback windows (14d filings/8-K, 48h news) are no longer a
# safe assumption — a full week with zero daily passes (first deploy of
# config#2943, or a sustained daily-delta outage) means the corpus may be
# missing up to a week of filings/news that the short top-up window would
# silently skip. 7 days = one full week, matching the daily delta's own
# weekday cadence (5 attempts/week; even with 2 misses this stays fresh).
STALE_COVERAGE_THRESHOLD_DAYS = 7


def needs_wide_topup(bucket: str = DEFAULT_BUCKET, s3_client: Any = None) -> bool:
    """True if the Saturday top-up should widen its lookback windows back to
    full-coverage (config#2943 cold-start / missed-week guard).

    Reads the scope-state pointer's own ``as_of`` date (written ONLY by a
    successful ``run_daily_corpus_delta.sh`` pass). Returns True (widen)
    when:
      - the pointer doesn't exist yet (cold start — no daily delta has ever
        run, e.g. the first Saturday/Thu/Fri after this PR ships), or
      - the pointer is unreadable (S3 error, malformed JSON), or
      - the pointer is older than ``STALE_COVERAGE_THRESHOLD_DAYS`` (the
        daily delta has stopped running — a sustained outage).

    Fail-safe by construction: every failure mode defaults to True (widen),
    never to False (stay thin) — a wide top-up costs more runtime but never
    silently produces an incomplete corpus with a clean exit code.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=SCOPE_STATE_KEY)
        data = json.loads(obj["Body"].read())
        as_of = Date.fromisoformat(data["as_of"])
        age_days = (Date.today() - as_of).days
        if age_days > STALE_COVERAGE_THRESHOLD_DAYS:
            logger.warning(
                "[corpus_scope_state] scope_state as_of=%s is %dd old (>%dd) — "
                "daily corpus delta appears to have stopped running. "
                "Widening Saturday top-up to full-coverage windows.",
                as_of, age_days, STALE_COVERAGE_THRESHOLD_DAYS,
            )
            return True
        logger.info(
            "[corpus_scope_state] scope_state as_of=%s (%dd old) — daily "
            "coverage looks current; using short delta-only top-up windows.",
            as_of, age_days,
        )
        return False
    except Exception as e:
        logger.warning(
            "[corpus_scope_state] no readable scope_state pointer (%s) — "
            "treating as cold-start (no daily delta has run yet). "
            "Widening Saturday top-up to full-coverage windows.", e,
        )
        return True
