"""Daily analyst-consensus snapshotter.

Wave 1 PR C of the institutional data-revamp arc. Writes one JSON
per (ticker, date) capturing today's analyst consensus + price target
across multiple sources. The matching ``data/derived/analyst_revisions.py``
reads the time series and computes self-derived 7d/30d revision deltas
without needing a paid revisions feed.

S3 layout::

    s3://alpha-engine-research/data/analyst_snapshots/{ticker}/{YYYY-MM-DD}.json

Per-ticker subfolder keeps a coherent per-ticker time series readable
in O(prefix-list) — important since downstream revisions computation
reads multiple dates per ticker.

Each daily snapshot is a small JSON document containing one
``AnalystSnapshot`` record per source. Multi-source preservation
matters because (a) sources disagree on coverage; (b) some metrics
require cross-source merge (yfinance has mean_target, finnhub has
rating bucket counts — neither alone is sufficient).

Idempotent: re-writing the same date overwrites.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date as Date
from datetime import datetime, timezone
from typing import Any, Sequence

from nousergon_lib.sources import AnalystSnapshot, AnalystSource
from nousergon_lib.yfinance_quiet import quiet_yfinance

logger = logging.getLogger(__name__)


SCHEMA_VERSION = 1
DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/analyst_snapshots"


# ── On-disk shape ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class AnalystSnapshotDocument:
    """Daily JSON document layout — one per (ticker, snapshot_date).

    ``snapshots_by_source`` keeps every adapter's full record. The
    derived-revisions module computes deltas by reading multiple days'
    documents and pivoting on ``snapshots_by_source[source].mean_target``
    (or any other field of interest).
    """

    ticker: str
    snapshot_date: Date
    schema_version: int
    snapshots_by_source: dict[str, dict]
    fetched_at: datetime


# ── Snapshot the universe ──────────────────────────────────────────────


def snapshot_one_ticker(
    ticker: str,
    sources: Sequence[AnalystSource],
) -> dict[str, AnalystSnapshot]:
    """Run each adapter for one ticker. Returns mapping source.name → snapshot.

    Source failures (transient API hiccup, ticker not covered) skip
    that source rather than crash the batch — matches the Protocol
    contract.
    """
    out: dict[str, AnalystSnapshot] = {}
    for source in sources:
        try:
            snap = source.fetch(ticker)
        except Exception as e:
            logger.warning(
                "[analyst_snapshotter] %s adapter raised for %s: %s",
                source.name, ticker, e,
            )
            continue
        if snap is not None:
            out[source.name] = snap
    return out


def write_snapshot_document(
    *,
    ticker: str,
    snapshot_date: Date,
    snapshots: dict[str, AnalystSnapshot],
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    run_id: str | None = None,
) -> str:
    """Write the per-ticker snapshot document under canonical
    eval-artifacts shape:
      artifact:   ``{prefix}/{ticker}/{run_id}_result.json``
      latest:     ``{prefix}/{ticker}/latest.json``

    Each ticker has its own ``latest.json`` since the revisions reader
    walks per-ticker time series. ``snapshot_date`` stays in the body
    payload so the revisions module can index by date when reading
    multiple snapshots back.

    Empty ``snapshots`` still writes a document — that's signal (no
    adapter covered the ticker that day)."""
    from nousergon_lib.eval_artifacts import (
        eval_artifact_key, eval_latest_key, new_eval_run_id,
    )

    run_id = run_id or new_eval_run_id()
    per_ticker_prefix = f"{prefix}/{ticker.upper()}"
    artifact_key = eval_artifact_key(
        per_ticker_prefix, run_id, basename="result.json",
    )
    latest_key = eval_latest_key(per_ticker_prefix)

    body = {
        "ticker": ticker,
        "snapshot_date": snapshot_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "snapshots_by_source": {
            name: _serialize_snapshot(snap)
            for name, snap in snapshots.items()
        },
    }
    s3_client.put_object(
        Bucket=bucket, Key=artifact_key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    s3_client.put_object(
        Bucket=bucket, Key=latest_key,
        Body=json.dumps({
            "run_id": run_id,
            "artifact_key": artifact_key,
            "ticker": ticker,
            "snapshot_date": snapshot_date.isoformat(),
            "schema_version": SCHEMA_VERSION,
            "written_at": body["fetched_at"],
        }).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        "[analyst_snapshotter] wrote %s [%d sources] to s3://%s/%s",
        ticker, len(snapshots), bucket, artifact_key,
    )
    return artifact_key


def snapshot_universe(
    tickers: Sequence[str],
    sources: Sequence[AnalystSource],
    *,
    snapshot_date: Date,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    dry_run: bool = False,
) -> dict[str, int]:
    """End-to-end: for each ticker, run each adapter, write per-ticker
    snapshot document to S3. Returns stats dict.
    """
    stats = {
        "n_tickers": len(tickers),
        "n_documents_written": 0,
        "n_source_calls_attempted": 0,
        "n_source_calls_succeeded": 0,
        "n_tickers_with_zero_coverage": 0,
    }
    # ``quiet_yfinance`` is pure-stdlib and demotes only the "yfinance"
    # logger, so wrapping here is safe regardless of which adapters are in
    # ``sources``. Without it, a delisted/unpriceable ticker anywhere in the
    # universe makes yfinance log its own per-symbol ERROR — the same storm
    # bug class fixed in ``collectors/prices.py`` (nousergon-data#455) and
    # ``collectors/metron_market_data.py`` (config#1029); ``stats`` above
    # (returned to the caller to log) is the aggregated coverage surface.
    with quiet_yfinance():
        for ticker in tickers:
            snapshots = snapshot_one_ticker(ticker, sources)
            stats["n_source_calls_attempted"] += len(sources)
            stats["n_source_calls_succeeded"] += len(snapshots)
            if not snapshots:
                stats["n_tickers_with_zero_coverage"] += 1
            if dry_run:
                logger.info(
                    "[DRY RUN] would snapshot %s with %d sources",
                    ticker, len(snapshots),
                )
                stats["n_documents_written"] += 1
                continue
            write_snapshot_document(
                ticker=ticker,
                snapshot_date=snapshot_date,
                snapshots=snapshots,
                s3_client=s3_client,
                bucket=bucket,
                prefix=prefix,
            )
            stats["n_documents_written"] += 1
    return stats


# ── S3 key + serialization helpers ────────────────────────────────────


def _serialize_snapshot(snap: AnalystSnapshot) -> dict:
    """Pydantic AnalystSnapshot → JSON-safe dict.

    Uses model_dump(mode='json') so datetime/date/tuple fields encode
    consistently across Python versions and the round-trip back to
    Pydantic during read is loss-free.
    """
    return snap.model_dump(mode="json")


def read_snapshot_document(
    ticker: str,
    snapshot_date: Date,
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> dict | None:
    """Read one snapshot document for (ticker, date).

    Canonical shape: lists ``{prefix}/{ticker}/`` and finds artifacts
    whose YYMMDDHHMM run_id starts with the date's YYMMDD prefix.
    Picks the most recent intra-day run when multiple exist.

    Returns the raw JSON dict or None if no document exists.
    """
    # Canonical: list prefix + find by run_id date prefix.
    # Filenames are `{prefix}/{ticker}/{YYMMDDHHMM}.json` (lib's
    # eval_artifact_key shortcuts basename='result.json' → just .json).
    per_ticker_prefix = f"{prefix}/{ticker.upper()}"
    yymmdd = snapshot_date.strftime("%y%m%d")
    try:
        resp = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=f"{per_ticker_prefix}/{yymmdd}",
        )
        candidates = sorted(
            obj["Key"] for obj in (resp.get("Contents") or [])
            if obj["Key"].endswith(".json")
            and not obj["Key"].endswith("/latest.json")
        )
        if candidates:
            # Most-recent intra-day run (sort puts latest YYMMDDHHMM last)
            obj = s3_client.get_object(Bucket=bucket, Key=candidates[-1])
            return json.loads(obj["Body"].read())
    except Exception as e:
        logger.debug(
            "[analyst_snapshotter] canonical list failed for %s/%s (%s)",
            ticker, snapshot_date, type(e).__name__,
        )
    return None
