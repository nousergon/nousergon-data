"""Daily news *digest* — the podcast-ready combined artifact.

Where ``news_aggregates`` (per-ticker rollup) and ``news_articles`` (raw
per-article feed) are the machine/human substrates for the HELD + TRACKED
universe, the digest is a single small JSON that combines three sections for
a daily-brief / podcast consumer:

    sections.portfolio  ← the most-newsworthy per-ticker stories (derived from
                          the ``NewsArticleRecord`` rows ``daily_news.collect``
                          already produced — no extra fetch)
    sections.macro      ← curated macro/markets RSS headlines (topic_news)
    sections.tech       ← curated tech RSS headlines (topic_news)

It is written to ``data/news_digest_daily/`` with BOTH a dated history object
(``{run_id}.json``) AND a ``latest.json`` that holds the FULL digest (not a
pointer) so the consumer reads it in a single GET.

CONTRACT (``schema_version: 1``):

    {
      "schema_version": 1,
      "date": "YYYY-MM-DD",
      "generated_at": "<ISO8601 UTC>",
      "sections": {
        "portfolio": [{"ticker","title","source","published","excerpt","sentiment","url"}],
        "macro":     [{"title","source","published","excerpt","url"}],
        "tech":      [{"title","source","published","excerpt","url"}]
      }
    }

Deterministic, no LLM, no API spend (topic_news is plain feedparser; the
portfolio section is derived from already-fetched records).
"""

from __future__ import annotations

import json
import logging
from datetime import date as Date
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


SCHEMA_VERSION = 1
"""Bump on any breaking change to the digest contract. Consumers gate on
the top-level ``schema_version`` field."""

DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/news_digest_daily"

# Cap the portfolio section so the digest stays a bounded, podcast-readable
# size. Selected by newsworthiness (see ``_select_portfolio``).
DEFAULT_PORTFOLIO_CAP = 30


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _select_portfolio(
    articles_df: pd.DataFrame,
    *,
    cap: int = DEFAULT_PORTFOLIO_CAP,
) -> list[dict[str, Any]]:
    """Pick the most-newsworthy per-ticker portfolio stories from the raw
    ``NewsArticleRecord`` DataFrame.

    Each row of ``articles_df`` is one canonical (deduped) story carrying a
    ``tickers_json`` list. The digest's portfolio section is a flat
    per-(ticker, story) list, so a multi-ticker story expands into one entry
    per ticker.

    "Newsworthy" ranking, descending:
      1. absolute LM sentiment magnitude (a strongly +/- story matters more
         than a neutral one),
      2. recency (``published_at``).

    The DataFrame is the raw-article companion shape from
    ``data.derived.news_articles`` (columns: ``title``, ``url``,
    ``tickers_json``, ``primary_source``, ``published_at``, ``body_excerpt``,
    ``lm_sentiment``). Missing columns degrade gracefully to empty/zero.
    """
    if articles_df is None or len(articles_df) == 0:
        return []

    df = articles_df.copy()
    # Defensive: ensure the columns we read exist.
    for col, default in (
        ("title", ""),
        ("url", ""),
        ("tickers_json", "[]"),
        ("primary_source", ""),
        ("published_at", ""),
        ("body_excerpt", ""),
        ("lm_sentiment", 0.0),
    ):
        if col not in df.columns:
            df[col] = default

    # GDELT keyword-matches ticker symbols/fragments ("meta-analysis" → META,
    # "MDT" → unrelated articles), polluting the portfolio section with false
    # positives (verified 2026-06-15: 19/30 portfolio items were GDELT noise).
    # Polygon (API-tagged) + Yahoo (per-ticker RSS) are ticker-accurate, so the
    # portfolio digest uses only those. GDELT remains valid for the macro/tech
    # topic sections (keyword/topic news by design) — this filter is
    # portfolio-only.
    df = df[df["primary_source"].astype(str).str.lower() != "gdelt"]
    if len(df) == 0:
        return []

    df["_abs_sentiment"] = df["lm_sentiment"].fillna(0.0).abs()
    df = df.sort_values(
        ["_abs_sentiment", "published_at"], ascending=[False, False]
    )

    entries: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        try:
            tickers = json.loads(row["tickers_json"]) or []
        except (TypeError, ValueError):
            tickers = []
        for ticker in tickers:
            entries.append(
                {
                    "ticker": str(ticker),
                    "title": str(row["title"]),
                    "source": str(row["primary_source"]),
                    "published": str(row["published_at"]),
                    "excerpt": str(row["body_excerpt"]),
                    "sentiment": round(float(row["lm_sentiment"] or 0.0), 6),
                    "url": str(row["url"]),
                }
            )
            if len(entries) >= cap:
                return entries
    return entries


def build_digest(
    *,
    articles_df: pd.DataFrame,
    topics: dict[str, list[dict[str, Any]]] | None,
    digest_date: Date,
    portfolio_cap: int = DEFAULT_PORTFOLIO_CAP,
) -> dict[str, Any]:
    """Assemble the digest dict from the portfolio article records + the
    macro/tech topic results.

    ``topics`` may be ``None`` or partial (a topic fetch that failed) — the
    corresponding section is emitted as an empty list. The portfolio section
    is always populated from ``articles_df`` regardless of topic state.
    """
    topics = topics or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "date": digest_date.isoformat(),
        "generated_at": _iso_now(),
        "sections": {
            "portfolio": _select_portfolio(articles_df, cap=portfolio_cap),
            "macro": list(topics.get("macro") or []),
            "tech": list(topics.get("tech") or []),
        },
    }


def write_digest(
    digest: dict[str, Any],
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    run_id: str | None = None,
) -> str:
    """Write the digest JSON to S3 as BOTH a dated history object
    (``{prefix}/{run_id}.json``) AND ``{prefix}/latest.json`` (the FULL
    digest — consumers read it directly in one GET).

    Uses the canonical ``nousergon_lib.eval_artifacts`` run_id +
    artifact/latest key helpers so the digest history shares the listing
    conventions of the sibling daily artifacts. Returns the dated artifact
    key.
    """
    from nousergon_lib.eval_artifacts import (
        eval_artifact_key,
        eval_latest_key,
        new_eval_run_id,
    )

    run_id = run_id or new_eval_run_id()
    artifact_key = eval_artifact_key(prefix, run_id, basename="digest.json")
    latest_key = eval_latest_key(prefix)

    body = json.dumps(digest, separators=(",", ":")).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket, Key=artifact_key, Body=body, ContentType="application/json"
    )
    # latest.json carries the FULL digest, not a pointer — one-GET consumer.
    s3_client.put_object(
        Bucket=bucket, Key=latest_key, Body=body, ContentType="application/json"
    )
    n = digest.get("sections", {})
    logger.info(
        "[news_digest] wrote digest to s3://%s/%s (portfolio=%d macro=%d tech=%d; latest=%s)",
        bucket, artifact_key,
        len(n.get("portfolio", [])), len(n.get("macro", [])), len(n.get("tech", [])),
        latest_key,
    )
    return artifact_key


def read_digest(
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> dict[str, Any]:
    """Consumer-side read of the latest digest (one GET on ``latest.json``).
    Returns an empty-schema digest dict when no artifact exists."""
    from nousergon_lib.eval_artifacts import eval_latest_key

    latest_key = eval_latest_key(prefix)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=latest_key)
        return json.loads(obj["Body"].read())
    except Exception as e:  # noqa: BLE001 — missing/garbled artifact → empty schema
        logger.info(
            "[news_digest] latest digest read failed for %s (%s)",
            latest_key, type(e).__name__,
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "date": None,
            "generated_at": None,
            "sections": {"portfolio": [], "macro": [], "tech": []},
        }
