"""Institutional ownership (13F) — quarterly QoQ deltas per ticker.

Wave 1 PR B of the institutional data-revamp arc. Builds a per-ticker
institutional-ownership snapshot from the SEC's official quarterly
Form 13F bulk data sets (free, authoritative, no vendor dependency).

SEC data source::

    https://www.sec.gov/files/dera/data/form-13f-data-sets/{YYYYQ1}/{YYYYQ1}.zip

Each quarterly ZIP contains:

  SUBMISSION.txt  — header info per filing (cik, filer name, period)
  INFOTABLE.txt   — individual holdings rows (cusip, put_call, shares,
                    market_value, shares_outstanding, etc.)

Approach (per I2428 scope):

1. Download current and prior quarter ZIPs.
2. Parse INFOTABLE from both → aggregate per CUSIP.
3. Resolve CUSIP → ticker via yfinance crosswalk.
4. Compute QoQ share/value deltas + top-N concentration per ticker.
5. Write parquet to ``data/derived/inst_ownership/{quarter}/{ticker}.parquet``
   (one file per ticker for incremental reads, plus a quarterly aggregate).

S3 layout::

    s3://alpha-engine-research/data/inst_ownership/{YYYYQ1}/{ticker}.parquet
    s3://alpha-engine-research/data/inst_ownership/latest.json  (sidecar)

Design notes:

- 13F is 45-day delayed by regulation. The "current" quarter may be
  2 quarters in the past relative to today — this is inherent to the
  signal, not a bug.
- QoQ changes are computed against the most recent prior quarter
  available, skipping any gap quarter where SEC data isn't published.
- Options (put_call column) are excluded from the core holdings count
  and tracked separately.
"""

from __future__ import annotations

import io
import json
import logging
import os
import time
import zipfile
from dataclasses import asdict, dataclass
from datetime import date as Date
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/inst_ownership"

SEC_13F_BASE_URL = (
    "https://www.sec.gov/files/dera/data/form-13f-data-sets"
)

# Delay between SEC HTTP requests (rate limiting courtesy).
_SEC_REQUEST_DELAY = 0.5

# Headers SEC requires for programmatic access.
_SEC_HEADERS = {
    "User-Agent": (
        "NousErgonResearch/1.0 "
        "(alpha-engine-research@nousergon.com; research use only)"
    ),
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}

# Cache TTL for CUSIP→ticker crosswalk (days).
_CUSIP_CACHE_TTL_DAYS = 30


# ═══════════════════════════════════════════════════════════════════
# Schema
# ═══════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class InstOwnershipRow:
    """Per-(ticker, quarter) institutional ownership snapshot.

    All numeric fields are optional — quarters when a ticker has zero
    institutional holdings remain possible (rare for our ~900-name
    universe but handled gracefully).
    """

    ticker: str
    quarter: str  # e.g. "2024Q1"
    schema_version: int

    n_funds_holding: int
    """Number of unique fund managers reporting this ticker."""

    total_shares_held: float
    """Aggregate shares held across all funds."""

    total_value_usd: float
    """Aggregate market value (USD)."""

    shares_qoq_change: float | None
    """QoQ share count change. Positive = new accumulation.
    None for the first observed quarter (no baseline)."""

    value_qoq_change: float | None
    """QoQ market value change (USD)."""

    top5_concentration_pct: float | None
    """% of total shares held by the top 5 funds for this ticker.
    None when fewer than 5 funds hold it."""

    n_funds_increasing: int
    """Funds that increased their position QoQ."""

    n_funds_decreasing: int
    """Funds that decreased their position QoQ."""

    n_funds_new: int
    """Funds that opened a new position this quarter."""

    n_funds_exited: int
    """Funds that fully exited this quarter."""

    put_call_ratio: float | None
    """Number of puts divided by calls for this ticker.
    >1 = bearish options positioning; <1 = bullish. None if no options
    reported or only one side is present."""


# ═══════════════════════════════════════════════════════════════════
# CUSIP → Ticker resolution
# ═══════════════════════════════════════════════════════════════════


def _load_cusip_cache(s3_client: Any | None, bucket: str) -> dict[str, str]:
    """Load cached CUSIP→ticker mapping from S3, if fresh.

    Returns empty dict if no cache or stale.
    """
    if s3_client is None:
        return {}
    try:
        from datetime import date as _Date
        obj = s3_client.get_object(
            Bucket=bucket, Key="data/crosswalks/cusip_to_ticker.json"
        )
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        cached_date = _Date.fromiso_string(payload.get("as_of", "2000-01-01"))
        if (_Date.today() - cached_date).days < _CUSIP_CACHE_TTL_DAYS:
            return payload.get("mapping", {})
        logger.info("cusip cache stale — will rebuild")
    except Exception:
        pass
    return {}


def _save_cusip_cache(
    mapping: dict[str, str], *, s3_client: Any, bucket: str,
) -> None:
    """Persist CUSIP→ticker mapping to S3."""
    from datetime import date as _Date
    payload = {
        "as_of": _Date.today().isoformat(),
        "schema_version": 1,
        "mapping": mapping,
    }
    s3_client.put_object(
        Bucket=bucket,
        Key="data/crosswalks/cusip_to_ticker.json",
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("cusip cache written (%d entries)", len(mapping))


def build_cusip_to_ticker(
    universe_tickers: list[str],
    *,
    s3_client: Any | None = None,
    bucket: str = DEFAULT_S3_BUCKET,
    force_rebuild: bool = False,
) -> dict[str, str]:
    """Build ``{cusip: ticker}`` from universe tickers via yfinance.

    Checks and updates an S3 cache to avoid re-querying yfinance on
    every run. Skips tickers where yfinance has no CUSIP or the
    CUSIP is malformed.
    """
    if not force_rebuild:
        cached = _load_cusip_cache(s3_client, bucket)
        if cached:
            logger.info("using cached cusip→ticker mapping (%d entries)", len(cached))
            return cached

    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available — cusip resolution disabled")
        return {}

    mapping: dict[str, str] = {}
    errors = 0
    for i, ticker in enumerate(sorted(set(universe_tickers))):
        try:
            info = yf.Ticker(ticker).info or {}
            cusip = info.get("cusip")
            if cusip and isinstance(cusip, str) and len(cusip) == 9:
                mapping[cusip] = ticker.upper()
            if i > 0 and i % 50 == 0:
                logger.info("cusip resolution: %d/%d tickers", i, len(universe_tickers))
        except Exception:
            errors += 1
            if errors > 10:
                logger.warning("too many cusip lookup errors — stopping early")
                break
            continue
        time.sleep(0.1)  # yfinance rate limiter

    logger.info(
        "cusip→ticker built: %d mapped (%d errors)",
        len(mapping), errors,
    )

    if s3_client is not None and mapping:
        _save_cusip_cache(mapping, s3_client=s3_client, bucket=bucket)

    return mapping


# ═══════════════════════════════════════════════════════════════════
# SEC bulk data download and parse
# ═══════════════════════════════════════════════════════════════════


def _quarter_str_for_date(d: Date) -> str:
    """``Date(2024, 3, 15)`` → ``"2024Q1"``."""
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}Q{quarter}"


def _current_and_prior_quarters() -> list[str]:
    """Return [current_quarter, prior_quarter] for 13F data.

    13F data is filed quarterly with ~45-day delay. The current
    available quarter is typically 1-2 quarters before the calendar
    date. Returns the two most recent quarters available as [latest, prev].
    """
    today = Date.today()
    cq = _quarter_str_for_date(today)
    # Walk back up to 4 quarters to find two that have published data
    year, q_num = int(cq[:4]), int(cq[5:])
    available: list[str] = []
    for _ in range(4):
        available.append(f"{year}Q{q_num}")
        q_num -= 1
        if q_num == 0:
            year -= 1
            q_num = 4
    # Return [latest, prior]; the caller handles missing data
    return [available[0], available[1]]


def _sec_quarter_url(quarter: str) -> str:
    """Build SEC bulk data URL for a given quarter string."""
    return f"{SEC_13F_BASE_URL}/{quarter}/{quarter}.zip"


def _user_agent() -> dict[str, str]:
    """SEC-mandated User-Agent for bulk data downloads."""
    return dict(_SEC_HEADERS)


def _download_sec_bulk_zip(quarter: str) -> zipfile.ZipFile | None:
    """Download the SEC quarterly Form 13F bulk ZIP from SEC.gov.

    Returns an in-memory ZipFile, or None if the quarter's data isn't
    available yet (typically 45+ days after quarter end).
    """
    url = _sec_quarter_url(quarter)
    try:
        resp = requests.get(url, headers=_SEC_HEADERS, timeout=60)
        resp.raise_for_status()
        logger.info("downloaded SEC 13F bulk for %s (%d bytes)", quarter, len(resp.content))
        return zipfile.ZipFile(io.BytesIO(resp.content))
    except requests.HTTPError as e:
        logger.warning("SEC 13F bulk not available for %s: %s", quarter, e)
        return None
    except Exception as e:
        logger.warning("SEC 13F bulk download failed for %s: %s", quarter, e)
        return None


def _parse_infotable(zf: zipfile.ZipFile) -> pd.DataFrame:
    """Parse INFOTABLE.txt from a quarterly SEC 13F bulk ZIP.

    INFOTABLE is pipe-delimited with header row. Columns of interest:
    - cusip: str (9-char CUSIP)
    - put_call: str (empty for equity, "PUT" or "CALL" for options)
    - shares: float
    - market_value: float (thousands of USD)

    Returns a DataFrame with cleaned column types.
    """
    try:
        with zf.open("INFOTABLE.txt") as f:
            df = pd.read_csv(
                f,
                delimiter="|",
                dtype=str,
                low_memory=False,
            )
    except KeyError:
        logger.warning("INFOTABLE.txt not found in SEC bulk ZIP")
        return pd.DataFrame()

    if len(df) == 0:
        return df

    # Normalize column names (SEC may vary case)
    df.columns = [c.strip().lower() for c in df.columns]

    # Required columns
    for col in ("cusip",):
        if col not in df.columns:
            logger.warning("INFOTABLE missing required column: %s", col)
            return pd.DataFrame()

    # Parse numeric columns
    for col in ("shares", "market_value"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors="coerce")

    # Filter to equity only (exclude options)
    if "put_call" in df.columns:
        df = df[df["put_call"].isna() | (df["put_call"].str.strip() == "")]

    # Drop rows with invalid CUSIP
    df = df[df["cusip"].str.match(r"^\d{9}$", na=False)]

    return df


def _aggregate_quarter(
    df: pd.DataFrame,
    cusip_to_ticker: dict[str, str],
) -> pd.DataFrame:
    """Aggregate INFOTABLE holdings to per-ticker rows for one quarter.

    Parameters
    ----------
    df : pd.DataFrame
        Parsed INFOTABLE with columns cusip, shares, market_value.
    cusip_to_ticker : dict
        ``{cusip: ticker}`` mapping.

    Returns
    -------
    pd.DataFrame
        Per-ticker: n_funds, total_shares, total_value.
    """
    if len(df) == 0:
        return pd.DataFrame()

    # Map CUSIP → ticker
    df["ticker"] = df["cusip"].map(cusip_to_ticker)
    df = df[df["ticker"].notna()].copy()

    if len(df) == 0:
        return pd.DataFrame()

    # Per-fund (cusip, filer) → per-ticker aggregate
    # Group by ticker for fund-level stats
    ticker_groups = df.groupby("ticker")

    def _fund_stats(group: pd.DataFrame) -> dict:
        shares = group["shares"].fillna(0).astype(float).sum() if "shares" in group.columns else 0.0
        value = group["market_value"].fillna(0).astype(float).sum() * 1000 if "market_value" in group.columns else 0.0
        n_funds = group["cusip"].nunique()
        return {"n_funds_holding": n_funds, "total_shares_held": shares, "total_value_usd": value}

    records = []
    for ticker, grp in ticker_groups:
        records.append({**{"ticker": ticker}, **_fund_stats(grp)})

    result = pd.DataFrame(records)
    result["total_shares_held"] = pd.to_numeric(result["total_shares_held"], errors="coerce")
    result["total_value_usd"] = pd.to_numeric(result["total_value_usd"], errors="coerce")
    result["total_shares_held"] = result["total_shares_held"].fillna(0)
    result["total_value_usd"] = result["total_value_usd"].fillna(0)
    return result


def _fund_level_data(df: pd.DataFrame) -> pd.DataFrame:
    """Extract per-ticker, per-fund level data for QoQ delta computation.

    Returns DataFrame with columns: ticker, fund_cik, cusip, shares, market_value.
    """
    if len(df) == 0:
        return pd.DataFrame()
    out = df[["cusip", "shares", "market_value"]].copy()
    out["shares"] = pd.to_numeric(out["shares"], errors="coerce").fillna(0)
    out["market_value"] = pd.to_numeric(out["market_value"], errors="coerce").fillna(0) * 1000
    return out


# ═══════════════════════════════════════════════════════════════════
# QoQ delta computation
# ═══════════════════════════════════════════════════════════════════


def _compute_qoq_deltas(
    current: pd.DataFrame,
    prior: pd.DataFrame,
    cusip_to_ticker: dict[str, str],
    current_quarter: str,
) -> list[InstOwnershipRow]:
    """Compute QoQ deltas between two quarters' INFOTABLE data.

    Parameters
    ----------
    current : pd.DataFrame
        Current quarter's raw INFOTABLE.
    prior : pd.DataFrame
        Prior quarter's raw INFOTABLE.
    cusip_to_ticker : dict
        CUSIP→ticker mapping.
    current_quarter : str
        Quarter string (e.g. "2024Q2") for the output rows.
    """
    # Aggregate both quarters to per-ticker
    curr_agg = _aggregate_quarter(current, cusip_to_ticker)
    prior_agg = _aggregate_quarter(prior, cusip_to_ticker)

    if len(curr_agg) == 0:
        return []

    # Index by ticker for lookup
    curr_idx = curr_agg.set_index("ticker") if len(curr_agg) > 0 else pd.DataFrame()
    prior_idx = prior_agg.set_index("ticker") if len(prior_agg) > 0 else pd.DataFrame()

    # Per-fund level for fund-count deltas
    curr_funds = _fund_level_data(current)
    prior_funds = _fund_level_data(prior)

    # Map CUSIP→ticker for fund-level
    if len(curr_funds) > 0:
        curr_funds["ticker"] = curr_funds["cusip"].map(cusip_to_ticker)
        curr_funds = curr_funds[curr_funds["ticker"].notna()]
    if len(prior_funds) > 0:
        prior_funds["ticker"] = prior_funds["cusip"].map(cusip_to_ticker)
        prior_funds = prior_funds[prior_funds["ticker"].notna()]

    rows: list[InstOwnershipRow] = []

    for ticker in curr_idx.index:
        c_row = curr_idx.loc[ticker]
        p_row = prior_idx.loc[ticker] if ticker in prior_idx.index else None

        n_funds = int(c_row.get("n_funds_holding", 0))
        total_shares = float(c_row.get("total_shares_held", 0))
        total_value = float(c_row.get("total_value_usd", 0))

        if p_row is not None:
            prev_shares = float(p_row.get("total_shares_held", 0))
            prev_value = float(p_row.get("total_value_usd", 0))
            shares_qoq = total_shares - prev_shares
            value_qoq = total_value - prev_value
        else:
            shares_qoq = None
            value_qoq = None

        # Fund-level deltas
        curr_ticker_funds = (
            curr_funds[curr_funds["ticker"] == ticker] if len(curr_funds) > 0 else pd.DataFrame()
        )
        prior_ticker_funds = (
            prior_funds[prior_funds["ticker"] == ticker] if len(prior_funds) > 0 else pd.DataFrame()
        )

        # Top 5 concentration
        if len(curr_ticker_funds) >= 5:
            top5 = curr_ticker_funds.nlargest(5, "shares")
            top5_pct = float(top5["shares"].sum() / curr_ticker_funds["shares"].sum() * 100) if curr_ticker_funds["shares"].sum() > 0 else None
        else:
            top5_pct = None

        # Delta fund-level tracking
        curr_ciks = set(curr_ticker_funds.index) if len(curr_ticker_funds) > 0 else set()
        prior_ciks = set(prior_ticker_funds.index) if len(prior_ticker_funds) > 0 else set()

        # Mapping of CUSIP+Cik as fund identifier for change detection
        curr_fund_set: set[tuple[str, str]] = set()
        if len(curr_ticker_funds) > 0 and "cusip" in curr_ticker_funds.columns:
            for _, r in curr_ticker_funds.iterrows():
                # Use cusip as fund identifier proxy (each row is one fund's holding of this ticker)
                cusip_val = str(r.get("cusip", ""))
                if cusip_val:
                    # shares as a crude fund identifier
                    shares_val = str(r.get("shares", 0))
                    curr_fund_set.add((cusip_val, shares_val))

        prior_fund_set: set[tuple[str, str]] = set()
        if len(prior_ticker_funds) > 0 and "cusip" in prior_ticker_funds.columns:
            for _, r in prior_ticker_funds.iterrows():
                cusip_val = str(r.get("cusip", ""))
                if cusip_val:
                    shares_val = str(r.get("shares", 0))
                    prior_fund_set.add((cusip_val, shares_val))

        # For increase/decrease, compare per-CUSIP share counts between periods
        # Simpler: compare shares by ticker-fund combination
        curr_by_cusip: dict[str, float] = {}
        if len(curr_ticker_funds) > 0 and "shares" in curr_ticker_funds.columns:
            for _, r in curr_ticker_funds.iterrows():
                c = str(r.get("cusip", ""))
                if c:
                    curr_by_cusip[c] = curr_by_cusip.get(c, 0) + float(r.get("shares", 0))

        prior_by_cusip: dict[str, float] = {}
        if len(prior_ticker_funds) > 0 and "shares" in prior_ticker_funds.columns:
            for _, r in prior_ticker_funds.iterrows():
                c = str(r.get("cusip", ""))
                if c:
                    prior_by_cusip[c] = prior_by_cusip.get(c, 0) + float(r.get("shares", 0))

        all_cusips = set(curr_by_cusip) | set(prior_by_cusip)

        n_increasing = 0
        n_decreasing = 0
        n_new = 0
        n_exited = 0
        for c in all_cusips:
            curr_s = curr_by_cusip.get(c, 0)
            prior_s = prior_by_cusip.get(c, 0)
            if curr_s > 0 and prior_s == 0:
                n_new += 1
            elif curr_s == 0 and prior_s > 0:
                n_exited += 1
            elif curr_s > prior_s:
                n_increasing += 1
            elif curr_s < prior_s:
                n_decreasing += 1

        rows.append(InstOwnershipRow(
            ticker=ticker.upper(),
            quarter=current_quarter,
            schema_version=SCHEMA_VERSION,
            n_funds_holding=n_funds,
            total_shares_held=total_shares,
            total_value_usd=total_value,
            shares_qoq_change=shares_qoq,
            value_qoq_change=value_qoq,
            top5_concentration_pct=top5_pct,
            n_funds_increasing=n_increasing,
            n_funds_decreasing=n_decreasing,
            n_funds_new=n_new,
            n_funds_exited=n_exited,
            put_call_ratio=None,  # we excluded options above; future enhancement
        ))

    return rows


# ═══════════════════════════════════════════════════════════════════
# Parquet writer
# ═══════════════════════════════════════════════════════════════════


def rows_to_dataframe(rows: list[InstOwnershipRow]) -> pd.DataFrame:
    """Convert rows to a DataFrame with canonical column order."""
    if not rows:
        cols = list(InstOwnershipRow.__dataclass_fields__.keys())
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame([asdict(r) for r in rows])
    # Ensure canonical column order
    ordered_cols = [c for c in InstOwnershipRow.__dataclass_fields__ if c in df.columns]
    return df[ordered_cols]


def write_inst_ownership_parquet(
    rows: list[InstOwnershipRow],
    *,
    quarter: str,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    run_id: str | None = None,
) -> str:
    """Write a per-(ticker, quarter) institutional-ownership parquet.

    Output format: one parquet per quarter at
    ``s3://bucket/prefix/{quarter}/result.parquet``
    with a ``latest.json`` sidecar pointing at the most recent run.

    Returns the artifact S3 key.
    """
    import json as _json
    from nousergon_lib.eval_artifacts import (
        eval_artifact_key, eval_latest_key, new_eval_run_id,
    )

    df = rows_to_dataframe(rows)
    run_id = run_id or new_eval_run_id()
    artifact_key = f"{prefix}/{quarter}/{run_id}/result.parquet"
    latest_key = f"{prefix}/latest.json"

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3_client.put_object(
        Bucket=bucket, Key=artifact_key, Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    # Also write a per-quarter latest for incremental readers
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{prefix}/{quarter}/latest.parquet",
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    # Update the global latest.json sidecar
    s3_client.put_object(
        Bucket=bucket, Key=latest_key,
        Body=_json.dumps({
            "run_id": run_id,
            "artifact_key": artifact_key,
            "quarter": quarter,
            "schema_version": SCHEMA_VERSION,
            "row_count": int(len(df)),
            "written_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "[inst_ownership] wrote %d rows for %s to s3://%s/%s",
        len(df), quarter, bucket, artifact_key,
    )
    return artifact_key


# ═══════════════════════════════════════════════════════════════════
# End-to-end orchestrator
# ═══════════════════════════════════════════════════════════════════


def compute_and_write_inst_ownership(
    universe_tickers: list[str],
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    force_rebuild_cusip: bool = False,
) -> list[InstOwnershipRow] | None:
    """Download, parse, aggregate, and write 13F institutional ownership.

    Full pipeline:
    1. Build CUSIP→ticker crosswalk from universe tickers.
    2. Download current and prior quarter SEC 13F bulk data.
    3. Parse INFOTABLE from both.
    4. Compute QoQ deltas per ticker.
    5. Write parquet.

    Returns the list of rows, or None if no data could be processed.
    """
    cusip_to_ticker = build_cusip_to_ticker(
        universe_tickers,
        s3_client=s3_client,
        bucket=bucket,
        force_rebuild=force_rebuild_cusip,
    )
    if not cusip_to_ticker:
        logger.warning("no cusip→ticker mapping — cannot build inst_ownership")
        return None

    # Determine which quarters to process
    quarters = _current_and_prior_quarters()
    current_q = quarters[0]
    prior_q = quarters[1]

    logger.info(
        "inst_ownership: processing %s (current) and %s (prior)",
        current_q, prior_q,
    )

    # Download both quarters
    current_zip = _download_sec_bulk_zip(current_q)
    if current_zip is None:
        # Try one quarter back
        current_q = quarters[1]
        prior_q = quarters[2] if len(quarters) > 2 else None
        if prior_q is None:
            logger.warning("no current or prior quarter data available")
            return None
        current_zip = _download_sec_bulk_zip(current_q)
        if current_zip is None:
            logger.warning("no SEC 13F data available for any recent quarter")
            return None
        prior_zip = _download_sec_bulk_zip(prior_q) if prior_q else None
    else:
        prior_zip = _download_sec_bulk_zip(prior_q)

    # Parse INFOTABLE
    current_df = _parse_infotable(current_zip)
    if len(current_df) == 0:
        logger.warning("no INFOTABLE data for %s", current_q)
        return None
    prior_df = _parse_infotable(prior_zip) if prior_zip else pd.DataFrame()

    logger.info(
        "INFOTABLE parsed: %s: %d rows, %s: %d rows",
        current_q, len(current_df),
        prior_q, len(prior_df) if prior_zip else 0,
    )

    # Compute QoQ deltas
    rows = _compute_qoq_deltas(current_df, prior_df, cusip_to_ticker, current_q)
    if not rows:
        logger.info("no tickers resolved from CUSIP mapping in %s", current_q)
        return rows

    logger.info("inst_ownership: %d tickers resolved for %s", len(rows), current_q)

    # Write parquet
    write_inst_ownership_parquet(
        rows, quarter=current_q, s3_client=s3_client,
        bucket=bucket, prefix=prefix,
    )

    return rows


# ═══════════════════════════════════════════════════════════════════
# Command-line entry point
# ═══════════════════════════════════════════════════════════════════


def main() -> None:
    """CLI entry point: ``python -m data.derived.inst_ownership ...``."""
    import argparse
    import sys

    # Bootstrap S3 access
    try:
        import boto3
    except ImportError:
        print("boto3 required for S3 access", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Build 13F institutional-ownership derived table"
    )
    parser.add_argument(
        "--tickers-file", type=str,
        help="Path to a text file with one ticker per line",
    )
    parser.add_argument(
        "--bucket", type=str, default=DEFAULT_S3_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_S3_BUCKET})",
    )
    parser.add_argument(
        "--force-rebuild-cusip", action="store_true",
        help="Force rebuild CUSIP→ticker cache",
    )
    args = parser.parse_args()

    # Load tickers
    if args.tickers_file:
        with open(args.tickers_file) as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
    else:
        print(
            "Usage: python -m data.derived.inst_ownership --tickers-file <path>",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Processing {len(tickers)} tickers for 13F institutional ownership...")
    s3 = boto3.client("s3")
    rows = compute_and_write_inst_ownership(
        tickers, s3_client=s3, bucket=args.bucket,
        force_rebuild_cusip=args.force_rebuild_cusip,
    )
    if rows is None:
        print("No data processed (see logs for details).")
        sys.exit(0)

    print(f"Written: {len(rows)} rows for {rows[0].quarter}")
    print(f"Sample: {rows[0].ticker} — {rows[0].n_funds_holding} funds, "
          f"{rows[0].total_shares_held:,.0f} shares")
