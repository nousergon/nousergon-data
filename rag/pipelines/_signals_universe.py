"""Shared helper for `--from-signals` flag across rag/pipelines/.

Loads the held-population ticker set from the latest signals.json on
S3. Pre-2026-05-13 each pipeline (ingest_8k_filings, ingest_sec_filings,
ingest_earnings_finnhub) duplicated this loader inline. Gate A
(institutional data-revamp Wave 1) added 3 more pipelines that need
the same load shape, so the helper lifts to a single module.

The signals.json producer is the Research Lambda's archive writer
(alpha-engine-research). It writes one `signals/{run_date}/signals.json`
per Saturday SF firing.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


DEFAULT_BUCKET = "alpha-engine-research"


def load_signals_tickers(
    *,
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
) -> list[str]:
    """Return the ticker list from the most recent signals.json on S3.

    Returns an empty list when no signals.json has been produced yet
    (logged at ERROR — caller should typically fail loud rather than
    silently process zero tickers).

    Reads the `universe` array (each entry is a dict with `ticker` key);
    falls back to the deprecated flat-ticker format if present.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    resp = s3_client.list_objects_v2(
        Bucket=bucket, Prefix="signals/", Delimiter="/",
    )
    prefixes = sorted(
        [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    )
    if not prefixes:
        logger.error(
            "[signals_universe] no signals/ prefix found on s3://%s",
            bucket,
        )
        return []
    latest_prefix = prefixes[-1]
    key = f"{latest_prefix}signals.json"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.error(
            "[signals_universe] failed to read s3://%s/%s: %s",
            bucket, key, e,
        )
        return []
    try:
        data = json.loads(obj["Body"].read())
    except Exception as e:
        logger.error(
            "[signals_universe] failed to parse signals.json: %s", e,
        )
        return []

    tickers: list[str] = []
    universe = data.get("universe")
    if isinstance(universe, list):
        for entry in universe:
            if isinstance(entry, dict):
                t = entry.get("ticker")
            elif isinstance(entry, str):
                t = entry
            else:
                continue
            if t:
                tickers.append(t.strip().upper())
    logger.info(
        "[signals_universe] loaded %d tickers from %s",
        len(tickers), key,
    )
    return tickers
