"""The RAG corpus's fetch set — one resolver, one definition (config-I5700).

``rag-corpus-policy.md`` §2.1: *the fetch set is the decision set, never the
universe.* This module is the single place that decides which tickers the
corpus fills for.

WHAT CHANGED AND WHY
--------------------
Every ingestion pipeline used to resolve its tickers from
``signals/{date}/signals.json``'s ``universe`` array. That array is a **sizing
envelope**, not a scope: ``crucible-research/scoring/signals_envelope.py``
emits one row per name on the whole scanner board — measured 2026-07-29 at
**903 rows, all HOLD** — so the executor can size and exit the names it holds.
It was never a statement about what the system is deciding on.

Reading it as a scope cost ~3.1h of Polygon crawl per weekly run at the
account-wide 5 req/min (12.5 s/ticker), was roughly half of ``RAGIngestion``'s
6h budget, and was the stage in flight when the 2026-07-29 weekly pipeline died
(``alpha-engine-config-I5695``).

The decision set is already published, canonically, by the scanner:

    universe_membership/{date}/membership.json :: cuts.scanner_candidates
        {"basis": "scanner_gate", "size": 60, "tickers": [...],
         "source": "candidates/{date}/candidates.json::scanner_tickers"}

``champion-challenger-policy.md`` §2 names that artifact as the universe-cut
registry. Reading it here means the corpus scope is **arm-independent**: it
does not change when the selection-producer champion is promoted, demoted, or
swapped, because it is upstream of every producer arm.

Held names are unioned in from Metron's holdings artifact — a position needs
evidence whether or not it ranks this week, and an EXIT still needs a
rationale (§2.1).

WHY NOT THE TOP-20 PREDICTOR CUT. ``predictor_universe_cut`` resolves to
``attractiveness_top_20`` today, but the Think Tank challenger arm consumes the
**top 60** and outputs 20 — RAG evidence is that arm's input. Scoping the
corpus to 20 would starve the challenger and make the champion/challenger
comparison unfair on breadth (``champion-challenger-policy.md`` §4). 60 is the
correct width; Brian ruled the same on 2026-07-30.

FAIL LOUD. A missing or empty cut raises. There is deliberately NO fallback to
``signals.json::universe`` — that fallback IS the defect this module exists to
remove, and it would be invisible until the next timeout.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# An equity ticker every downstream vendor can actually resolve: 1-5 letters,
# optionally a share-class suffix (BRK.B, BF-B). Deliberately strict.
#
# Why this exists: the held-position artifact is Metron's, and Metron holds
# more than equities. Measured 2026-07-30, the live held set contained
# ``912828YK0`` — a US Treasury CUSIP. Every ingestion source in this package
# is equity-only (EDGAR by CIK, Finnhub earnings, Polygon news), so a CUSIP is
# a guaranteed wasted request PER SOURCE, PER RUN, forever, and lands in the
# corpus as a permanent gap that no watermark can ever close.
_TICKER_RE = re.compile(r"^[A-Z]{1,5}([.-][A-Z]{1,2})?$")

DEFAULT_BUCKET = "alpha-engine-research"

# O(1) pointer objects. The prior implementation paged `list_objects_v2` with
# `Delimiter="/"` and took `sorted(prefixes)[-1]`, which silently returns the
# 1000th-OLDEST date once the partition count crosses the 1000-CommonPrefixes
# page limit (48 partitions as of 2026-07-30, so latent rather than live).
# Reading a pointer removes the failure mode instead of raising its threshold.
MEMBERSHIP_LATEST_KEY = "universe_membership/latest.json"
MEMBERSHIP_DATED_TPL = "universe_membership/{date}/membership.json"

# Metron's held-position artifact — the same key collectors/daily_news.py
# already reads, so the daily and weekly paths resolve one identical set
# (rag-corpus-policy.md §2.3, one fetch one corpus).
HOLDINGS_UNIVERSE_KEY = "metron/holdings_universe.json"

# The cut that defines the corpus width. See "WHY NOT THE TOP-20" above.
SCOPE_CUT = "scanner_candidates"


class RagScopeUnavailable(RuntimeError):
    """The decision set could not be resolved.

    Raised rather than degraded-to-wider: a corpus that quietly fills for the
    whole board is exactly the 903-ticker regression config-I5700 removed, and
    it surfaces only as a vendor bill or a timeout hours later.
    """


def _get_json(s3_client: Any, bucket: str, key: str) -> dict | None:
    """Return a parsed S3 JSON object, or None when absent/unparseable.

    Callers decide the fail-loud policy — this helper deliberately does not,
    because the two artifacts it serves have different criticality.
    """
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.warning("[rag_scope] could not read s3://%s/%s: %s", bucket, key, e)
        return None
    try:
        return json.loads(obj["Body"].read())
    except Exception as e:
        logger.warning("[rag_scope] unparseable JSON at s3://%s/%s: %s", bucket, key, e)
        return None


def _load_holdings(s3_client: Any, bucket: str) -> list[str]:
    """Held tickers from Metron's holdings artifact.

    NON-FATAL on absence, matching collectors/daily_news.py's posture: a
    missing Metron artifact narrows coverage (held names lose fresh evidence)
    but cannot corrupt it, and blocking the entire corpus fill on a
    cross-product artifact is the worse failure. The degradation is logged at
    WARNING naming exactly what is not covered, never swallowed silently.
    """
    data = _get_json(s3_client, bucket, HOLDINGS_UNIVERSE_KEY)
    if not data:
        logger.warning(
            "[rag_scope] holdings artifact s3://%s/%s unavailable — HELD NAMES "
            "WILL NOT BE COVERED this run (scanner cut only). Not fatal, but "
            "positions go without fresh evidence until it returns.",
            bucket, HOLDINGS_UNIVERSE_KEY,
        )
        return []
    tickers = [str(t).strip().upper() for t in (data.get("tickers") or []) if t]
    logger.info("[rag_scope] holdings: %d held ticker(s) (as_of=%s)",
                len(tickers), data.get("as_of"))
    return tickers


def load_rag_scope(
    *,
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
    run_date: str | None = None,
) -> dict:
    """Resolve the corpus fetch set. Returns ``{tickers, counts, source}``.

    ``run_date`` pins the dated membership artifact; omitted reads the
    ``latest.json`` pointer. Pin it whenever the caller is part of a dated
    pipeline run, so a corpus fill and the run it serves cannot disagree about
    which week they are in.

    Raises :class:`RagScopeUnavailable` when the scanner cut is missing or
    empty — never widens.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    key = (
        MEMBERSHIP_DATED_TPL.format(date=run_date) if run_date
        else MEMBERSHIP_LATEST_KEY
    )
    membership = _get_json(s3_client, bucket, key)
    if not membership:
        raise RagScopeUnavailable(
            f"universe membership artifact s3://{bucket}/{key} is missing or "
            f"unparseable. The scanner writes it every run upstream of every "
            f"producer arm, so absence is a real upstream failure. Refusing to "
            f"fall back to signals.json::universe — that is the 903-ticker "
            f"path config-I5700 removed (rag-corpus-policy.md §2.1)."
        )

    cut = (membership.get("cuts") or {}).get(SCOPE_CUT) or {}
    cut_tickers = [str(t).strip().upper() for t in (cut.get("tickers") or []) if t]
    if not cut_tickers:
        raise RagScopeUnavailable(
            f"membership artifact s3://{bucket}/{key} carries no non-empty "
            f"cuts.{SCOPE_CUT}. Available cuts: "
            f"{sorted((membership.get('cuts') or {}).keys())}. Refusing to "
            f"widen (rag-corpus-policy.md §2.1)."
        )

    held = _load_holdings(s3_client, bucket)
    candidates = sorted(set(cut_tickers) | set(held))

    # Drop anything no equity source can resolve (see _TICKER_RE). Named in the
    # log rather than silently dropped — a non-equity holding is a real
    # position that will carry no corpus evidence, which a reader of the
    # coverage numbers has to be able to account for.
    tickers = [t for t in candidates if _TICKER_RE.match(t)]
    rejected = [t for t in candidates if not _TICKER_RE.match(t)]
    if rejected:
        logger.warning(
            "[rag_scope] dropped %d non-equity identifier(s) no ingestion "
            "source can resolve: %s — these carry NO corpus evidence",
            len(rejected), rejected,
        )

    logger.info(
        "[rag_scope] resolved %d ticker(s) for run_date=%s — scanner cut %r %d "
        "∪ held %d (source: %s)",
        len(tickers), membership.get("run_date"), SCOPE_CUT, len(set(cut_tickers)),
        len(set(held)), cut.get("source") or key,
    )
    return {
        "tickers": tickers,
        "counts": {
            "total": len(tickers),
            SCOPE_CUT: len(set(cut_tickers)),
            "held": len(set(held)),
            "rejected_non_equity": len(rejected),
        },
        "run_date": membership.get("run_date"),
        "source": cut.get("source") or key,
    }


def load_rag_scope_tickers(
    *,
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
    run_date: str | None = None,
) -> list[str]:
    """Ticker-list convenience wrapper over :func:`load_rag_scope`.

    Every ``rag/pipelines`` entry point and ``collectors/daily_news.py`` call
    this. It replaces ``_signals_universe.load_signals_tickers`` AND the three
    inline copies of the same loader that survived the 2026-05-13 lift
    (``ingest_sec_filings``, ``ingest_8k_filings``, ``ingest_earnings_finnhub``
    each carried their own ``list_objects_v2`` + ``universe`` read).
    """
    return load_rag_scope(
        bucket=bucket, s3_client=s3_client, run_date=run_date,
    )["tickers"]
