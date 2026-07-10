"""Self-derived analyst-estimate revisions.

Wave 1 PR C of the institutional data-revamp arc. Reads the daily
analyst-consensus snapshot time series (written by
``data/snapshotter/analyst_daily.py``) and computes
revisions deltas:

  - ``mean_target_delta_7d``    — mean target now minus 7 trailing days
  - ``mean_target_delta_30d``   — mean target now minus 30 trailing days
  - ``num_analysts_delta_30d``  — coverage change (additions vs drops)
  - ``rating_changed_30d``      — boolean: consensus_rating different
                                  from 30d-ago snapshot

Why self-derived (vs a paid IBES revisions feed):

- IBES is a Phase 4 paid subscription. We own the time series via
  the snapshotter — sufficient for 7/30-day deltas at no extra cost.
- Composes with the IbesAnalystAdapter stub when subscription lands:
  this module's outputs add to (don't conflict with) IBES's native
  revision history.

Acceptance: ≥14 days of accumulated daily snapshots produce useful
revisions output. Earlier days emit None deltas (degraded gracefully).

S3 layout::

    s3://alpha-engine-research/data/analyst_revisions/{YYYY-MM-DD}.parquet

One parquet per as-of-date holds all tickers (similar to news_aggregates).
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any, Iterable

import pandas as pd

logger = logging.getLogger(__name__)


SCHEMA_VERSION = 1
DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/analyst_revisions"


@dataclass(frozen=True)
class AnalystRevisionRow:
    """Per-(ticker, as_of_date) revision row.

    None values mean "insufficient history" — common in the first 30
    days of snapshotter operation. Downstream consumers should treat
    None as "no signal yet" rather than "signal is zero".
    """

    ticker: str
    as_of_date: Date
    schema_version: int
    primary_source: str
    """The source ``mean_target`` was read from (yfinance preferred;
    falls back to first available)."""
    mean_target_current: float | None
    mean_target_7d_ago: float | None
    mean_target_30d_ago: float | None
    mean_target_delta_7d: float | None
    mean_target_delta_30d: float | None
    mean_target_pct_change_30d: float | None
    num_analysts_current: int | None
    num_analysts_30d_ago: int | None
    num_analysts_delta_30d: int | None
    consensus_rating_current: str | None
    consensus_rating_30d_ago: str | None
    rating_changed_30d: bool
    n_snapshot_days_observed: int
    """How many distinct snapshot dates contributed to this row.
    Useful for downstream confidence weighting."""


# ── Source preference ─────────────────────────────────────────────────


# Source preference order for mean_target. yfinance is the most reliable
# free source for the headline consensus number. Finnhub free doesn't
# expose target; IBES / Visible Alpha would override when wired.
_SOURCE_PREFERENCE_FOR_TARGET = ("ibes", "visible_alpha", "yfinance", "finnhub")
_SOURCE_PREFERENCE_FOR_RATING = ("ibes", "visible_alpha", "finnhub", "yfinance")


def _pick_field(
    snapshots_by_source: dict, *, sources: tuple[str, ...], field: str,
) -> tuple[Any, str | None]:
    """Look up the first non-None ``field`` across ``sources`` in
    preference order. Returns (value, source_name) or (None, None)."""
    for src in sources:
        snap = snapshots_by_source.get(src)
        if not snap:
            continue
        value = snap.get(field)
        if value is not None:
            return value, src
    return None, None


# ── Build revision rows ───────────────────────────────────────────────


def build_revision_row(
    *,
    ticker: str,
    as_of_date: Date,
    snapshot_documents_by_date: dict[Date, dict],
) -> AnalystRevisionRow:
    """Compute one revision row from a ticker's snapshot time series.

    ``snapshot_documents_by_date`` maps Date → the JSON document the
    snapshotter wrote that day. Missing dates are tolerated (callers
    pass whatever the S3 listing returned). Deltas degrade to None
    when the historical anchor doesn't exist.
    """
    today_doc = snapshot_documents_by_date.get(as_of_date)
    today_snapshots = (today_doc or {}).get("snapshots_by_source") or {}

    target_today, target_source = _pick_field(
        today_snapshots,
        sources=_SOURCE_PREFERENCE_FOR_TARGET, field="mean_target",
    )
    num_analysts_today, _ = _pick_field(
        today_snapshots,
        sources=_SOURCE_PREFERENCE_FOR_TARGET, field="num_analysts",
    )
    rating_today, _ = _pick_field(
        today_snapshots,
        sources=_SOURCE_PREFERENCE_FOR_RATING, field="consensus_rating",
    )

    target_7d = _historical_target(
        snapshot_documents_by_date, as_of_date, days_back=7,
    )
    target_30d, num_analysts_30d, rating_30d = _historical_at(
        snapshot_documents_by_date, as_of_date, days_back=30,
    )

    delta_7d = _safe_subtract(target_today, target_7d)
    delta_30d = _safe_subtract(target_today, target_30d)
    pct_30d = _safe_pct_change(target_today, target_30d)

    num_delta_30d = _safe_subtract_int(num_analysts_today, num_analysts_30d)
    rating_changed_30d = (
        rating_today is not None
        and rating_30d is not None
        and rating_today != rating_30d
    )

    return AnalystRevisionRow(
        ticker=ticker,
        as_of_date=as_of_date,
        schema_version=SCHEMA_VERSION,
        primary_source=target_source or "",
        mean_target_current=target_today,
        mean_target_7d_ago=target_7d,
        mean_target_30d_ago=target_30d,
        mean_target_delta_7d=delta_7d,
        mean_target_delta_30d=delta_30d,
        mean_target_pct_change_30d=pct_30d,
        num_analysts_current=num_analysts_today,
        num_analysts_30d_ago=num_analysts_30d,
        num_analysts_delta_30d=num_delta_30d,
        consensus_rating_current=rating_today,
        consensus_rating_30d_ago=rating_30d,
        rating_changed_30d=rating_changed_30d,
        n_snapshot_days_observed=len(snapshot_documents_by_date),
    )


def _historical_target(
    docs_by_date: dict[Date, dict],
    as_of: Date,
    *,
    days_back: int,
) -> float | None:
    """Look up mean_target on (as_of - days_back). Tolerates missing
    exact-day match by walking back a few days (handles weekends /
    holidays / snapshotter outages)."""
    for offset in range(days_back, days_back + 7):
        d = as_of - timedelta(days=offset)
        doc = docs_by_date.get(d)
        if not doc:
            continue
        target, _ = _pick_field(
            doc.get("snapshots_by_source") or {},
            sources=_SOURCE_PREFERENCE_FOR_TARGET, field="mean_target",
        )
        if target is not None:
            return float(target)
    return None


def _historical_at(
    docs_by_date: dict[Date, dict],
    as_of: Date,
    *,
    days_back: int,
) -> tuple[float | None, int | None, str | None]:
    """Like _historical_target but also returns num_analysts + rating
    from the SAME historical snapshot (so all three deltas are
    apples-to-apples). Walks back a few days for weekend/holiday gaps."""
    for offset in range(days_back, days_back + 7):
        d = as_of - timedelta(days=offset)
        doc = docs_by_date.get(d)
        if not doc:
            continue
        snaps = doc.get("snapshots_by_source") or {}
        target, _ = _pick_field(
            snaps, sources=_SOURCE_PREFERENCE_FOR_TARGET, field="mean_target",
        )
        num, _ = _pick_field(
            snaps, sources=_SOURCE_PREFERENCE_FOR_TARGET, field="num_analysts",
        )
        rating, _ = _pick_field(
            snaps, sources=_SOURCE_PREFERENCE_FOR_RATING, field="consensus_rating",
        )
        if target is not None or rating is not None or num is not None:
            return (
                float(target) if target is not None else None,
                int(num) if num is not None else None,
                rating,
            )
    return None, None, None


def _safe_subtract(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return round(float(a) - float(b), 4)


def _safe_subtract_int(a: int | None, b: int | None) -> int | None:
    if a is None or b is None:
        return None
    return int(a) - int(b)


def _safe_pct_change(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or float(b) == 0:
        return None
    return round((float(a) - float(b)) / float(b), 6)


# ── Time-series loader from S3 ────────────────────────────────────────


def load_snapshot_time_series(
    ticker: str,
    *,
    as_of_date: Date,
    days_back: int = 30,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    snapshot_prefix: str = "data/analyst_snapshots",
) -> dict[Date, dict]:
    """Load all snapshot documents for ``ticker`` in the window
    [as_of - days_back, as_of]. Returns a mapping Date → JSON document.

    Canonical shape: list ``{prefix}/{ticker}/`` once + parse each
    artifact's body to extract ``snapshot_date``; index by that date.
    Cheaper than per-date GETs since one LIST + small body reads
    cover the entire window.
    """
    import json as _json
    out: dict[Date, dict] = {}
    cutoff_earliest = as_of_date - timedelta(days=days_back + 7)

    # Canonical: list the per-ticker prefix + filter to the window
    per_ticker_prefix = f"{snapshot_prefix}/{ticker.upper()}/"
    try:
        resp = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=per_ticker_prefix,
        )
        keys = [
            obj["Key"] for obj in (resp.get("Contents") or [])
            if obj["Key"].endswith(".json")
            and not obj["Key"].endswith("/latest.json")
        ]
    except Exception:
        keys = []

    for key in keys:
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            doc = _json.loads(obj["Body"].read())
        except Exception as e:
            logger.warning(
                "[analyst_revisions] failed to parse %s: %s", key, e,
            )
            continue
        try:
            snap_date = Date.fromisoformat(doc.get("snapshot_date", "")[:10])
        except (ValueError, TypeError):
            continue
        if cutoff_earliest <= snap_date <= as_of_date:
            # If multiple runs landed for the same date, keep the
            # one with the most-recent fetched_at timestamp.
            existing = out.get(snap_date)
            if existing is None or (
                doc.get("fetched_at", "") > existing.get("fetched_at", "")
            ):
                out[snap_date] = doc
    return out


# ── Parquet writer ─────────────────────────────────────────────────────


def rows_to_dataframe(rows: Iterable[AnalystRevisionRow]) -> pd.DataFrame:
    rows_list = list(rows)
    if not rows_list:
        cols = list(AnalystRevisionRow.__dataclass_fields__.keys())
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([asdict(r) for r in rows_list])


def write_revisions_parquet(
    rows: Iterable[AnalystRevisionRow],
    *,
    as_of_date: Date,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    run_id: str | None = None,
) -> str:
    """Canonical eval-artifacts shape (YYMMDDHHMM + latest.json sidecar)."""
    import json as _json
    from nousergon_lib.eval_artifacts import (
        eval_artifact_key, eval_latest_key, new_eval_run_id,
    )

    df = rows_to_dataframe(rows)
    run_id = run_id or new_eval_run_id()
    artifact_key = eval_artifact_key(prefix, run_id, basename="result.parquet")
    latest_key = eval_latest_key(prefix)

    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3_client.put_object(
        Bucket=bucket, Key=artifact_key, Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    s3_client.put_object(
        Bucket=bucket, Key=latest_key,
        Body=_json.dumps({
            "run_id": run_id,
            "artifact_key": artifact_key,
            "as_of_date": as_of_date.isoformat(),
            "schema_version": SCHEMA_VERSION,
            "row_count": int(len(df)),
            "written_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        "[analyst_revisions] wrote %d rows to s3://%s/%s",
        len(df), bucket, artifact_key,
    )
    return artifact_key


# ── End-to-end orchestrator ────────────────────────────────────────────


def compute_and_write_revisions(
    tickers: list[str],
    *,
    as_of_date: Date,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    snapshot_prefix: str = "data/analyst_snapshots",
    revisions_prefix: str = DEFAULT_S3_PREFIX,
) -> tuple[str, list[AnalystRevisionRow]]:
    """Load time-series for each ticker, build revision rows, write
    parquet. Returns (s3_key, rows)."""
    rows: list[AnalystRevisionRow] = []
    for ticker in tickers:
        docs = load_snapshot_time_series(
            ticker,
            as_of_date=as_of_date,
            s3_client=s3_client,
            bucket=bucket,
            snapshot_prefix=snapshot_prefix,
        )
        rows.append(build_revision_row(
            ticker=ticker,
            as_of_date=as_of_date,
            snapshot_documents_by_date=docs,
        ))
    key = write_revisions_parquet(
        rows, as_of_date=as_of_date,
        s3_client=s3_client, bucket=bucket, prefix=revisions_prefix,
    )
    return key, rows
