"""Daily news producer — weekday news pull for the held + tracked universe.

Mirrors the Saturday ``run_news_pipeline`` chain (news aggregator →
``NewsNLPPipeline`` → ``aggregate_and_write``) but on a WEEKDAY cadence over a
small, high-value universe:

    robodashboard holdings  ∪  alpha-engine signals universe (tracked + recs)

and writes to a SEPARATE eval-artifact prefix (``data/news_aggregates_daily/``)
with a ``latest.json`` sidecar, so the robodashboard morning brief gets a stable
pointer to the freshest pull without knowing the trading day. The Saturday
``data/news_aggregates/`` artifact (full signals universe, 168h, RAG-ingested)
is untouched — this is an additive daily companion.

Deterministic: APIs (Polygon/GDELT/Yahoo) + dictionary NLP (Loughran-McDonald +
rule-based events). No LLM, no API spend — honors
``[[preference_llm_calls_confined_to_research_module]]``.

Why a small universe (vs. all ~900 constituents): the only daily consumers are
robodashboard's brief (the held names) and AE's own tracked set — and Polygon's
free tier is 5 req/min, so a per-ticker pull over 900 names is infeasible in any
morning window. The full universe stays on the weekly Saturday cadence.

NOTE on scheduling: a per-ticker pull over ~50-70 names is bounded by Polygon's
free-tier 5 req/min (~12-14 min). We fan the three sources in CONCURRENTLY via
:class:`AsyncNewsAggregator` (per-vendor rate limits + tenacity retry), so wall
time ≈ the Polygon-bound ~12-14 min rather than the SUM of the three sources.
Run this as its own decoupled weekday SSM step (``python -m collectors.daily_news``),
not inside ``_run_daily``; the ``RunDailyNews`` SF step's ``executionTimeout`` is
sized with headroom over that floor. (Before 2026-06-09 this used the *sync*
aggregator, which summed the sources sequentially and ``TimedOut`` every weekday
at the 1200s ceiling, silently producing nothing — see ROADMAP L4573.)
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date as Date
from datetime import datetime, timezone
from typing import Any

from collectors.nlp.loughran_mcdonald import (
    LmDictUnavailable,
    ensure_lm_master_dict,
)

logger = logging.getLogger(__name__)

DAILY_PREFIX = "data/news_aggregates_daily"
ARTICLES_PREFIX = "data/news_articles_daily"
DIGEST_PREFIX = "data/news_digest_daily"
HOLDINGS_UNIVERSE_KEY = "robodashboard/holdings_universe.json"
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_BUCKET = "alpha-engine-research"


def _load_holdings_universe(bucket: str, s3_client: Any) -> list[str]:
    """Read robodashboard's published held-ticker symbols (fail-soft → [])."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=HOLDINGS_UNIVERSE_KEY)
        data = json.loads(obj["Body"].read())
        tickers = [
            str(t).strip().upper() for t in data.get("tickers", []) if str(t).strip()
        ]
        logger.info("[daily_news] loaded %d robodashboard holdings tickers", len(tickers))
        return tickers
    except Exception as e:  # missing object, no creds, parse error, etc.
        logger.warning(
            "[daily_news] holdings_universe unavailable (%s) — proceeding with AE universe only",
            e,
        )
        return []


def assemble_universe(bucket: str, s3_client: Any) -> list[str]:
    """Union robodashboard holdings ∪ AE signals universe (tracked + recs).

    Each slice is fail-soft: a missing holdings file or signals.json degrades
    to whatever is available rather than aborting the pull.
    """
    from rag.pipelines._signals_universe import load_signals_tickers

    holdings = _load_holdings_universe(bucket, s3_client)
    try:
        ae = load_signals_tickers(bucket=bucket, s3_client=s3_client)
    except Exception as e:
        logger.warning("[daily_news] signals universe unavailable (%s)", e)
        ae = []
    universe = sorted(set(holdings) | set(ae))
    logger.info(
        "[daily_news] universe = %d holdings ∪ %d AE-signals = %d unique",
        len(holdings),
        len(ae),
        len(universe),
    )
    return universe


def _build_aggregator():
    """Construct the default multi-source ASYNC aggregator.

    Uses :class:`AsyncNewsAggregator` (concurrent fan-in + per-vendor rate
    limits + tenacity retry) so the three sources overlap instead of summing
    sequentially — the fix for the 1200s SSM timeout (ROADMAP L4573). Isolated
    as a seam so the daily orchestrator can be unit-tested without the adapter
    constructors or the SEC company-name fetch touching the network.
    """
    from collectors.news_aggregator_async import AsyncNewsAggregator
    from collectors.news_sources.gdelt import GdeltNewsAdapter
    from collectors.news_sources.polygon import PolygonNewsAdapter
    from collectors.news_sources.yahoo_rss import YahooRssNewsAdapter
    from rag.pipelines.run_news_pipeline import _load_ticker_name_map

    return AsyncNewsAggregator(
        sources=[
            PolygonNewsAdapter(),
            GdeltNewsAdapter(ticker_name_map=_load_ticker_name_map()),
            YahooRssNewsAdapter(),
        ]
    )


def _build_nlp_pipeline():
    """Construct the default rule-based NLP pipeline (no LLM)."""
    from collectors.nlp.loughran_mcdonald import LoughranMcDonaldScorer
    from collectors.nlp.pipeline import NewsNLPPipeline
    from collectors.nlp.rule_based_event_extraction import RuleBasedEventExtractor

    return NewsNLPPipeline(
        sentiment_scorers=[LoughranMcDonaldScorer()],
        event_extractors=[RuleBasedEventExtractor()],
    )


def collect(
    bucket: str = DEFAULT_BUCKET,
    *,
    run_date: str | None = None,
    hours: int = DEFAULT_LOOKBACK_HOURS,
    dry_run: bool = False,
    s3_client: Any = None,
) -> dict:
    """Pull daily news for the held + tracked universe and write the daily
    aggregates parquet (``data/news_aggregates_daily/``).

    Returns a status dict. This is a SECONDARY artifact — callers should treat
    a non-``ok`` status as a soft degrade (no news that day), never a hard
    failure of any primary pipeline.
    """
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")

    agg_date = (
        Date.fromisoformat(run_date)
        if run_date
        else datetime.now(timezone.utc).date()
    )

    universe = assemble_universe(bucket, s3_client)
    if not universe:
        logger.warning("[daily_news] empty universe — skipping news pull")
        return {"status": "skipped", "reason": "empty_universe", "tickers": 0}

    # ── Ensure the LM sentiment dict is present (self-heal from S3) ──────────
    # A missing dict makes the scorer silently return all-zero sentiment; rather
    # than write that degraded artifact, fail loud here (L4575 /
    # [[feedback_no_silent_fails]]). Checked BEFORE the ~17-min news pull so a
    # dict outage fails fast. ensure_lm_master_dict self-heals any host that
    # never ran the install script by fetching the canonical CSV from S3.
    try:
        ensure_lm_master_dict(s3_client=s3_client)
    except LmDictUnavailable as e:
        logger.error(
            "[daily_news] %s — refusing to write an all-zero-sentiment artifact",
            e,
        )
        return {
            "status": "error",
            "reason": "lm_dict_unavailable",
            "tickers": len(universe),
        }

    # ── Fetch (concurrent multi-source fan-in; deterministic) ────────────────
    # AsyncNewsAggregator.fetch is a coroutine — drive it from this sync entry
    # point with anyio.run so the three vendors overlap (≈ Polygon-bound wall
    # time) instead of summing sequentially past the SSM timeout (L4573).
    import functools

    import anyio

    aggregator = _build_aggregator()
    articles = anyio.run(functools.partial(aggregator.fetch, universe, hours=hours))
    logger.info(
        "[daily_news] fetched %d aggregated articles for %d tickers",
        len(articles),
        len(universe),
    )

    # ── NLP (rule-based; no LLM) ─────────────────────────────────────────────
    nlp_output = _build_nlp_pipeline().process(articles)

    if dry_run:
        logger.info(
            "[daily_news] dry-run — skipping parquet write (%d articles, %d tickers)",
            len(articles),
            len(universe),
        )
        return {
            "status": "ok_dry_run",
            "tickers": len(universe),
            "articles": len(articles),
        }

    # ── Write to the DAILY prefix (separate from Saturday's) ─────────────────
    from data.derived.news_aggregates import aggregate_and_write

    key, df = aggregate_and_write(
        articles=articles,
        nlp_output=nlp_output,
        aggregate_date=agg_date,
        aggregator=aggregator,
        s3_client=s3_client,
        bucket=bucket,
        prefix=DAILY_PREFIX,
    )
    logger.info("[daily_news] wrote %d rows to s3://%s/%s", len(df), bucket, key)

    # ── Additive raw-article companion (human-readable feed substrate) ────────
    # The aggregate above is the PRIMARY artifact (consumers + ARTIFACT_REGISTRY
    # gate on it). This second write preserves the underlying deduped articles
    # (headline/url/source/excerpt/per-article sentiment) for the dashboard
    # "Daily News" page — built from the SAME already-fetched data, so no extra
    # API calls and no LLM spend.
    #
    # FAIL-SOFT, deliberately (per [[feedback_no_silent_fails]] — acceptable
    # category: secondary artifact hung off a path whose PRIMARY deliverable
    # already succeeded). (a) Failure mode swallowed: the raw-article parquet/
    # sidecar write fails (S3 hiccup, schema bug). (b) Primary survives: the
    # aggregate already landed above; consumers are unaffected. (c) Recording
    # surface: a WARN log here AND the ``articles_status`` field on the returned
    # status dict (which the SF/logs capture). It must never abort the weekday
    # SF — the whole daily_news producer is already secondary/fail-soft.
    articles_status = "ok"
    articles_key = None
    articles_rows = 0
    articles_df = None
    try:
        from data.derived.news_articles import articles_build_and_write

        articles_key, articles_df = articles_build_and_write(
            articles=articles,
            nlp_output=nlp_output,
            aggregate_date=agg_date,
            aggregator=aggregator,
            s3_client=s3_client,
            bucket=bucket,
            prefix=ARTICLES_PREFIX,
        )
        articles_rows = int(len(articles_df))
        logger.info(
            "[daily_news] wrote %d article rows to s3://%s/%s",
            articles_rows, bucket, articles_key,
        )
    except Exception as e:  # noqa: BLE001 — fail-soft secondary artifact (see above)
        articles_status = "error"
        logger.warning(
            "[daily_news] raw-article companion write FAILED (%s: %s) — "
            "aggregate artifact already landed, continuing",
            type(e).__name__, e,
        )

    # ── Podcast-ready combined digest (portfolio + macro + tech) ─────────────
    # Combines the per-ticker article records ALREADY in memory (portfolio
    # section) with curated macro/tech RSS headlines (topic_news) into a single
    # small JSON the daily-brief / podcast consumer reads from latest.json in
    # one GET. Same fail-soft posture as the article companion above (per
    # [[feedback_no_silent_fails]] — acceptable category: secondary artifact
    # hung off a path whose PRIMARY deliverable already landed):
    #   (a) Failure modes swallowed: topic RSS fetch is down/garbled (→ empty
    #       macro/tech, digest still written); OR the digest build/write itself
    #       fails (S3 hiccup, schema bug).
    #   (b) Primary survives: the aggregate (and article companion) already
    #       landed above; consumers of those are unaffected.
    #   (c) Recording surface: WARN logs here AND ``digest_status`` /
    #       ``topic_status`` on the returned status dict (SF/logs capture it).
    # Topic-fetch failure must NEVER block the digest: we still write it with
    # the portfolio section populated and empty macro/tech.
    digest_status = "ok"
    digest_key = None
    topic_status = "ok"
    try:
        topics: dict = {}
        try:
            from collectors.topic_news import fetch_topics

            topics = fetch_topics(["macro", "tech"], hours=hours)
        except Exception as e:  # noqa: BLE001 — topic fetch is best-effort; degrade to empty
            topic_status = "error"
            topics = {}
            logger.warning(
                "[daily_news] topic-news fetch FAILED (%s: %s) — "
                "writing digest with empty macro/tech",
                type(e).__name__, e,
            )

        from data.derived.news_digest import build_digest, write_digest

        digest = build_digest(
            articles_df=articles_df,
            topics=topics,
            digest_date=agg_date,
        )
        digest_key = write_digest(
            digest,
            s3_client=s3_client,
            bucket=bucket,
            prefix=DIGEST_PREFIX,
        )
        logger.info(
            "[daily_news] wrote digest to s3://%s/%s (portfolio=%d macro=%d tech=%d)",
            bucket, digest_key,
            len(digest["sections"]["portfolio"]),
            len(digest["sections"]["macro"]),
            len(digest["sections"]["tech"]),
        )
    except Exception as e:  # noqa: BLE001 — fail-soft secondary artifact (see above)
        digest_status = "error"
        logger.warning(
            "[daily_news] digest build/write FAILED (%s: %s) — "
            "aggregate + article artifacts already landed, continuing",
            type(e).__name__, e,
        )

    return {
        "status": "ok",
        "tickers": len(universe),
        "articles": len(articles),
        "rows": int(len(df)),
        "key": key,
        "articles_status": articles_status,
        "articles_key": articles_key,
        "articles_rows": articles_rows,
        "digest_status": digest_status,
        "digest_key": digest_key,
        "topic_status": topic_status,
    }


def read_daily_news(*, bucket: str = DEFAULT_BUCKET, s3_client: Any = None):
    """Consumer-side read of the latest daily news aggregates (via the
    ``latest.json`` sidecar). Returns an empty canonical-schema DataFrame when
    no daily artifact exists yet."""
    from data.derived.news_aggregates import read_news_aggregates_parquet

    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")
    return read_news_aggregates_parquet(
        s3_client=s3_client, bucket=bucket, prefix=DAILY_PREFIX
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", type=str, default=DEFAULT_BUCKET)
    parser.add_argument(
        "--hours",
        type=int,
        default=DEFAULT_LOOKBACK_HOURS,
        help="Lookback window in hours (default 24 = overnight + pre-market).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Aggregate date stamp (default: today UTC).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch + NLP but don't write the parquet.",
    )
    args = parser.parse_args()
    result = collect(
        args.bucket, run_date=args.date, hours=args.hours, dry_run=args.dry_run
    )
    logger.info("[daily_news] complete: %s", result)
    return 0 if result.get("status", "").startswith("ok") or result["status"] == "skipped" else 1


if __name__ == "__main__":
    raise SystemExit(main())
