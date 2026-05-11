"""
constituents.py — Fetch S&P 500 + S&P 400 constituent tickers from Wikipedia.

Writes constituents.json to S3 with:
  - tickers: deduplicated list of ~900 symbols
  - sector_map: {ticker: GICS_sector_name}
  - sector_etf_map: {ticker: sector_ETF_symbol}
  - sp500_count, sp400_count, total_count, fetched_at

Falls back to a local CSV cache if Wikipedia is unreachable.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# GICS sector name → sector ETF symbol
GICS_TO_ETF: dict[str, str] = {
    "Information Technology": "XLK",
    "Financials": "XLF",
    "Health Care": "XLV",
    "Consumer Discretionary": "XLY",
    "Industrials": "XLI",
    "Communication Services": "XLC",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Materials": "XLB",
}

_CACHE_PATH = Path(__file__).parent.parent / "data" / "constituents_cache.csv"

_URLS = {
    "S&P 500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
    "S&P 400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
}

_HEADERS = {"User-Agent": "alpha-engine-data/1.0 (weekly-collector)"}


def collect(
    bucket: str,
    s3_prefix: str = "market_data/",
    run_date: str | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Fetch S&P 500+400 constituents from Wikipedia and write to S3.

    Returns dict with status, counts, and any errors.
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    tickers, sector_map, sector_etf_map, sp500_count, sp400_count = _fetch_constituents()

    if not tickers:
        return {"status": "error", "error": "No tickers fetched"}

    unmapped = [t for t in tickers if t not in sector_map]
    if unmapped:
        raise RuntimeError(
            f"Sector mapping incomplete: {len(unmapped)} of {len(tickers)} tickers "
            f"missing GICS sector. Sample: {unmapped[:10]}. EOD reconcile sector "
            f"attribution depends on full coverage; aborting before write."
        )

    result = {
        "date": run_date,
        "tickers": tickers,
        "sector_map": sector_map,
        "sector_etf_map": sector_etf_map,
        "sp500_count": sp500_count,
        "sp400_count": sp400_count,
        "total_count": len(tickers),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        logger.info(
            "[dry-run] constituents: %d tickers (%d S&P500, %d S&P400), %d sector mappings",
            len(tickers), sp500_count, sp400_count, len(sector_etf_map),
        )
        return {
            "status": "ok_dry_run",
            "count": len(tickers),
            "tickers": tickers,
        }

    # Write to S3
    s3 = boto3.client("s3")
    key = f"{s3_prefix}weekly/{run_date}/constituents.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(result, indent=2),
        ContentType="application/json",
    )
    logger.info("Wrote constituents.json to s3://%s/%s (%d tickers)", bucket, key, len(tickers))

    # Write sector_map.json to canonical data path + legacy predictor path
    sector_map_body = json.dumps(sector_etf_map, indent=2, sort_keys=True)
    for sector_map_key in ["data/sector_map.json", "predictor/price_cache/sector_map.json"]:
        s3.put_object(
            Bucket=bucket, Key=sector_map_key,
            Body=sector_map_body, ContentType="application/json",
        )
    logger.info("Wrote sector_map.json to data/ and predictor/ paths")

    # tickers is included in the return so callers don't need an S3 round-trip
    # to re-read what they just wrote. Pre-MorningEnrich preflight (PR #134)
    # consumes this directly to feed prune_delisted_tickers' constituents_override
    # and to populate the daily_closes request list for the same run. Existing
    # _run_phase1 caller (line 156) just stores the dict — the extra key is
    # additive, no breakage.
    return {
        "status": "ok",
        "count": len(tickers),
        "tickers": tickers,
    }


def _select_constituents_table(tables: list[pd.DataFrame], index_name: str) -> pd.DataFrame:
    """Pick the constituents DataFrame from pd.read_html output.

    Wikipedia inserts banner/disambiguation tables ahead of the constituents
    table without notice (S&P 400 page added one ~2026-05; the prior
    `tables[0]` heuristic returned a 2-col warning banner with integer column
    names instead of the 400-row constituents table). Find by columns
    instead of by position: must have a ticker/symbol column AND a GICS
    sector (not sub-industry) column. Returns the first matching table —
    on Wikipedia constituent pages this is the live roster; the second-such
    table (recent additions/removals) lacks a GICS Sector column.
    """
    candidates: list[pd.DataFrame] = []
    for df in tables:
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]
        cols_lower = [str(c).lower() for c in df.columns]
        has_ticker = any("symbol" in c or "ticker" in c for c in cols_lower)
        has_gics_sector = any(
            "gics" in c and "sector" in c and "sub" not in c for c in cols_lower
        )
        if has_ticker and has_gics_sector:
            candidates.append(df)
    if not candidates:
        raise RuntimeError(
            f"No constituents table found in {index_name} Wikipedia page "
            f"(scanned {len(tables)} tables; need columns matching symbol/ticker "
            f"AND GICS sector). Wikipedia layout drift — extractor needs update."
        )
    return max(candidates, key=len)


def _fetch_constituents() -> tuple[list[str], dict[str, str], dict[str, str], int, int]:
    """
    Fetch constituent tickers and sector mappings from Wikipedia.

    Returns:
        (tickers, sector_map, sector_etf_map, sp500_count, sp400_count)
        - sector_map: {ticker: GICS_sector_name}
        - sector_etf_map: {ticker: sector_ETF_symbol}
    """
    tickers: list[str] = []
    sector_map: dict[str, str] = {}
    sector_etf_map: dict[str, str] = {}
    sp500_count = 0
    sp400_count = 0

    try:
        for index_name, url in _URLS.items():
            resp = requests.get(url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            tables = pd.read_html(StringIO(resp.text))
            df = _select_constituents_table(tables, index_name)

            col = next(
                (c for c in df.columns if "symbol" in str(c).lower() or "ticker" in str(c).lower()),
                df.columns[0],
            )
            batch = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(".", "-", regex=False)  # BRK.B → BRK-B for yfinance
                .tolist()
            )
            batch = [t for t in batch if t and t != "nan" and len(t) <= 6]
            tickers.extend(batch)
            logger.info("Fetched %d tickers from %s", len(batch), index_name)

            if index_name == "S&P 500":
                sp500_count = len(batch)
            else:
                sp400_count = len(batch)

            sector_col = next(
                (c for c in df.columns if "gics" in str(c).lower() and "sector" in str(c).lower()
                 and "sub" not in str(c).lower()),
                None,
            )
            if sector_col is None:
                raise RuntimeError(
                    f"GICS sector column missing from {index_name} Wikipedia table "
                    f"(columns: {list(df.columns)}). Column-name drift — extractor needs update."
                )
            for ticker, sector in zip(batch, df[sector_col].astype(str).tolist()):
                sector_name = sector.strip()
                sector_map[ticker] = sector_name
                etf = GICS_TO_ETF.get(sector_name)
                if etf:
                    sector_etf_map[ticker] = etf
            logger.info(
                "[%s] Sector map: %d added (running total: %d sectors, %d ETFs)",
                index_name, len(batch), len(sector_map), len(sector_etf_map),
            )

        tickers = list(dict.fromkeys(tickers))  # deduplicate, preserve order

        # Update local cache
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"ticker": tickers}).to_csv(_CACHE_PATH, index=False)

        return tickers, sector_map, sector_etf_map, sp500_count, sp400_count

    except Exception as e:
        logger.warning("Wikipedia fetch failed (%s); trying local cache...", e)
        if _CACHE_PATH.exists():
            cached = pd.read_csv(_CACHE_PATH)["ticker"].tolist()
            logger.info("Loaded %d tickers from cache", len(cached))
            return cached, {}, {}, 0, 0
        logger.error("No cache found — cannot build universe")
        return [], {}, {}, 0, 0


def load_from_s3(bucket: str, s3_prefix: str = "market_data/") -> dict | None:
    """Load the latest constituents.json from S3. Returns None if not found."""
    s3 = boto3.client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=f"{s3_prefix}latest_weekly.json")
        pointer = json.loads(resp["Body"].read())
        date = pointer.get("date")
        if not date:
            return None
        resp = s3.get_object(Bucket=bucket, Key=f"{s3_prefix}weekly/{date}/constituents.json")
        return json.loads(resp["Body"].read())
    except Exception:
        return None
