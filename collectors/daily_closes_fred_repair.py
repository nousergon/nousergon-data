"""collectors/daily_closes_fred_repair.py — one-shot repair tool for the
2026-05-12 FRED-clobber regression.

The windowed-reconciliation cutover (alpha-engine-data PRs #199/#200/#201
+ alpha-engine-config commit flipping ``data/config.yaml`` to
``daily_closes: { window_days: 14, skip_if_canonical: true }``, activated
2026-05-11) amplified a pre-existing latent bug in
``collectors/daily_closes._fetch_fred_closes``: that fetcher queried FRED
with ``sort_order=desc, limit=5`` and no upper-bound, so per-date calls
across the 14-BDay rolling window all returned today's most-recent
observation. Every historical date's ``staging/daily_closes/{date}.parquet``
got today's VIX/VIX3M/TNX/IRX/TWO/HYOAS/BAA10Y written onto it,
clobbering the correct historical values.

FlowDoctor surfaced the regression on 2026-05-12 ~13:01 / ~13:04 UTC
with paired alerts for VIX @ 2026-04-22 and VIX @ 2026-04-28 both
showing identical pre-fix (18.36) and post-fix (17.19) closes — the
signature of "every per-date stamp got today's latest".

Companion fix: ``_fetch_fred_closes`` now sends
``observation_end=date_str`` so per-date calls return that date's
actual FRED value (or most-recent on-or-before for the same-day case
where FRED hasn't published yet).

This script re-fetches the correct FRED values across an operator-
specified date window and rewrites the FRED-ticker rows of each affected
parquet under ``staging/daily_closes/``. Polygon-sourced stock rows are
left untouched — polygon's grouped-daily endpoint takes a date parameter
and was always per-date-correct, so only FRED rows need repair.

Usage::

    python -m collectors.daily_closes_fred_repair \
        --bucket alpha-engine-research \
        --start 2026-04-22 --end 2026-05-12

    # dry-run: read + plan only, no S3 writes
    python -m collectors.daily_closes_fred_repair \
        --bucket alpha-engine-research \
        --start 2026-04-22 --end 2026-05-12 --dry-run

Idempotent — re-running on already-repaired parquets is a no-op (writes
the same correct values back).
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

from nousergon_lib.secrets import get_secret

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Import the ticker → FRED series map from the canonical home so this
# script never drifts from the live fetcher's coverage set.
from collectors.daily_closes import (
    _FRED_BASE,
    _FRED_INDEX_MAP,
    _FRED_TIMEOUT,
    _scrub_api_key,
)


def _business_days(start: str, end: str) -> list[str]:
    """Return business-day YYYY-MM-DD strings in ``[start, end]`` inclusive."""
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    if s > e:
        raise ValueError(f"start {start} must be <= end {end}")
    out: list[str] = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:
            out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def _fetch_fred_range(
    series_id: str,
    start: str,
    end: str,
    api_key: str,
) -> dict[str, float]:
    """Single FRED call returning ``{YYYY-MM-DD: value}`` for the range.

    Missing observations (FRED's ``"."``) are dropped. ``end`` is padded
    out a few days so the per-date most-recent-on-or-before lookup can
    fall back across calendar gaps.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end,
        "sort_order": "asc",
    }
    last_err: Exception | None = None
    obs: list[dict] = []
    for attempt in range(1, 4):
        try:
            resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
            resp.raise_for_status()
            obs = resp.json().get("observations", [])
            break
        except requests.exceptions.RequestException as e:
            last_err = e
            if attempt < 3:
                logger.warning(
                    "FRED %s range attempt %d failed: %s — retrying in %ds",
                    series_id, attempt, _scrub_api_key(e), attempt * 3,
                )
                time.sleep(attempt * 3)
            else:
                logger.error(
                    "FRED %s range failed after 3 attempts: %s",
                    series_id, _scrub_api_key(e),
                )
                raise RuntimeError(
                    f"FRED range fetch failed for {series_id} after retries: "
                    f"{_scrub_api_key(last_err)}"
                ) from None
    out: dict[str, float] = {}
    for o in obs:
        val = o.get("value", ".")
        if val == "." or val is None:
            continue
        date = o.get("date")
        try:
            out[date] = float(val)
        except (TypeError, ValueError):
            continue
    return out


def _value_on_or_before(
    fred_values: dict[str, float],
    date_str: str,
    sorted_dates: list[str],
) -> Optional[tuple[str, float]]:
    """Return ``(obs_date, value)`` for the most recent FRED observation on
    or before ``date_str``, or None if none exists. Matches the live fetcher's
    semantic so the repair output is byte-identical to a fresh windowed run."""
    # sorted_dates is ascending. Binary search would be marginally faster
    # but the windows we care about are <30 dates — linear scan is fine.
    candidate: Optional[tuple[str, float]] = None
    for d in sorted_dates:
        if d > date_str:
            break
        candidate = (d, fred_values[d])
    return candidate


def _read_parquet(s3, bucket: str, key: str) -> Optional[pd.DataFrame]:
    """Return the parquet at ``s3://bucket/key`` as a DataFrame, or None on 404."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey"):
            return None
        raise
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")


def _write_parquet(s3, bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    buf.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )


def repair(
    bucket: str,
    start: str,
    end: str,
    s3_prefix: str = "staging/daily_closes/",
    tickers: Optional[list[str]] = None,
    dry_run: bool = False,
) -> dict:
    """Re-fetch correct FRED values for ``[start, end]`` and overwrite the
    FRED-ticker rows of each daily_closes parquet in the window.

    Returns a per-date summary dict.
    """
    api_key = get_secret("FRED_API_KEY", required=False, default="")
    if not api_key:
        raise RuntimeError("FRED_API_KEY not set — cannot fetch FRED values")

    if tickers is None:
        tickers = sorted(_FRED_INDEX_MAP.keys())
    unknown = [t for t in tickers if t not in _FRED_INDEX_MAP]
    if unknown:
        raise ValueError(
            f"Unknown FRED tickers {unknown}. Known: {sorted(_FRED_INDEX_MAP.keys())}"
        )

    # Pad the FRED query backward so per-date lookups can fall back across
    # weekend / holiday gaps without hitting the start-of-range cliff.
    pad_start = (
        datetime.strptime(start, "%Y-%m-%d").date() - timedelta(days=14)
    ).isoformat()

    logger.info(
        "Fetching FRED ranges %s → %s for %d tickers (padded start %s)",
        start, end, len(tickers), pad_start,
    )
    fred_data: dict[str, dict[str, float]] = {}
    fred_sorted: dict[str, list[str]] = {}
    for ticker in tickers:
        series_id = _FRED_INDEX_MAP[ticker]
        vals = _fetch_fred_range(series_id, pad_start, end, api_key)
        if not vals:
            logger.warning(
                "FRED %s → %s: no observations in [%s, %s] — skipping ticker",
                ticker, series_id, pad_start, end,
            )
            continue
        fred_data[ticker] = vals
        fred_sorted[ticker] = sorted(vals.keys())
        logger.info(
            "FRED %s: %d observations from %s to %s",
            ticker, len(vals), fred_sorted[ticker][0], fred_sorted[ticker][-1],
        )

    if not fred_data:
        return {"status": "error", "error": "no FRED data retrieved"}

    s3 = boto3.client("s3")
    bdays = _business_days(start, end)
    per_date: dict[str, dict] = {}
    n_parquets_missing = 0
    n_parquets_rewritten = 0
    n_rows_repaired = 0

    for d in bdays:
        key = f"{s3_prefix}{d}.parquet"
        df = _read_parquet(s3, bucket, key)
        if df is None:
            per_date[d] = {"status": "missing", "rewritten": False}
            n_parquets_missing += 1
            continue

        repaired_rows: list[str] = []
        changes: dict[str, tuple[float, float]] = {}
        for ticker in tickers:
            if ticker not in fred_data:
                continue
            if ticker not in df.index:
                continue
            lookup = _value_on_or_before(fred_data[ticker], d, fred_sorted[ticker])
            if lookup is None:
                logger.warning(
                    "No FRED %s observation on or before %s — leaving row unchanged",
                    ticker, d,
                )
                continue
            obs_date, value = lookup
            current_close = float(df.at[ticker, "Close"]) if "Close" in df.columns else None
            value = round(value, 4)
            if current_close is not None and abs(current_close - value) < 1e-4:
                # Already correct — idempotent skip.
                continue
            # Overwrite OHLC + Adj_Close with the correct FRED single-value
            # close. Volume / VWAP / source columns are preserved (already
            # the FRED defaults 0 / None / "fred" from the prior write).
            for col in ("Open", "High", "Low", "Close", "Adj_Close"):
                if col in df.columns:
                    df.at[ticker, col] = value
            if "source" in df.columns:
                df.at[ticker, "source"] = "fred"
            if "VWAP" in df.columns:
                df.at[ticker, "VWAP"] = None
            if "Volume" in df.columns:
                df.at[ticker, "Volume"] = 0
            repaired_rows.append(ticker)
            changes[ticker] = (current_close if current_close is not None else float("nan"), value)
            logger.info(
                "Repair %s @ %s: %s → %s (FRED obs date %s)",
                ticker, d,
                f"{current_close:.4f}" if current_close is not None else "?",
                f"{value:.4f}", obs_date,
            )

        if not repaired_rows:
            per_date[d] = {"status": "noop", "rewritten": False}
            continue

        if dry_run:
            per_date[d] = {
                "status": "would_rewrite",
                "rewritten": False,
                "tickers": repaired_rows,
                "changes": {t: {"old": old, "new": new} for t, (old, new) in changes.items()},
            }
            continue

        _write_parquet(s3, bucket, key, df)
        per_date[d] = {
            "status": "rewritten",
            "rewritten": True,
            "tickers": repaired_rows,
            "changes": {t: {"old": old, "new": new} for t, (old, new) in changes.items()},
        }
        n_parquets_rewritten += 1
        n_rows_repaired += len(repaired_rows)

    return {
        "status": "ok",
        "bucket": bucket,
        "prefix": s3_prefix,
        "start": start,
        "end": end,
        "tickers": tickers,
        "dry_run": dry_run,
        "business_days_in_window": len(bdays),
        "parquets_missing": n_parquets_missing,
        "parquets_rewritten": n_parquets_rewritten,
        "rows_repaired": n_rows_repaired,
        "per_date": per_date,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bucket", default="alpha-engine-research")
    parser.add_argument("--prefix", default="staging/daily_closes/")
    parser.add_argument(
        "--start", required=True,
        help="YYYY-MM-DD inclusive start of repair window",
    )
    parser.add_argument(
        "--end", required=True,
        help="YYYY-MM-DD inclusive end of repair window",
    )
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help=f"Subset of FRED tickers to repair. Default: all of {sorted(_FRED_INDEX_MAP.keys())}",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Plan + log changes without writing to S3",
    )
    args = parser.parse_args()

    result = repair(
        bucket=args.bucket,
        start=args.start,
        end=args.end,
        s3_prefix=args.prefix,
        tickers=args.tickers,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, default=str))
    sys.exit(0 if result.get("status") == "ok" else 2)


if __name__ == "__main__":
    main()
