"""
macro.py — Fetch macro data from FRED and yfinance, compute market breadth.

Extracted from alpha-engine-research/data/fetchers/macro_fetcher.py.

Writes macro.json to S3 with:
  - FRED series: fed funds, treasuries, VIX, unemployment, CPI, sentiment, claims, HY spread
  - Market prices: SPY, QQQ, IWM closes + 30d returns, commodities (oil, gold, copper)
  - Yield curve slope (10yr - 2yr in bps)
  - Market breadth: % above 50d/200d MA, advance/decline ratio
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
import numpy as np

from alpha_engine_lib.secrets import get_secret
import pandas as pd
import requests
import yfinance as yf

from store.parquet_loader import load_slim_cache

logger = logging.getLogger(__name__)

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_FRED_TIMEOUT = 15

_FRED_SERIES = {
    "fed_funds_rate": "FEDFUNDS",
    "treasury_2yr": "DGS2",
    "treasury_10yr": "DGS10",
    "vix": "VIXCLS",
    "unemployment": "UNRATE",
    "consumer_sentiment": "UMCSENT",
    "initial_claims": "ICSA",
    "hy_credit_spread_oas": "BAMLH0A0HYM2",
}


def collect(
    bucket: str,
    s3_prefix: str = "market_data/",
    run_date: str | None = None,
    price_data: dict[str, pd.DataFrame] | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Fetch macro data and write macro.json to S3.

    Args:
        bucket: S3 bucket name
        s3_prefix: S3 prefix for market_data
        run_date: date string for S3 path
        price_data: optional pre-loaded price data for breadth computation
        dry_run: if True, fetch but don't write to S3

    Returns:
        dict with status and any errors
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    macro = _fetch_fred()
    market = _fetch_market_prices()
    macro.update(market)

    # Compute breadth. If the caller did not pass in price_data, load the
    # slim cache that Phase 1 just wrote to S3 so downstream Research still
    # gets a real breadth reading. If that load fails for any reason, we
    # OMIT the "breadth" key entirely rather than writing null — Research
    # has its own fallback and macro_agent reads macro_data.get("breadth", {}),
    # which only honors the default when the key is missing.
    if price_data is None:
        try:
            s3_read = boto3.client("s3")
            price_data = load_slim_cache(s3_read, bucket)
        except Exception as exc:
            logger.warning("Failed to load slim cache for breadth: %s", exc)
            price_data = None

    if price_data:
        macro["breadth"] = _compute_market_breadth(price_data)
    else:
        logger.warning(
            "No price data available for breadth — omitting breadth key "
            "(research will fall back to its own computation)"
        )

    macro["fetched_at"] = datetime.now(timezone.utc).isoformat()

    if dry_run:
        logger.info("[dry-run] macro: %d fields fetched", len(macro))
        return {"status": "ok_dry_run", "fields": len(macro)}

    # Write to S3
    s3 = boto3.client("s3")
    key = f"{s3_prefix}weekly/{run_date}/macro.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(macro, indent=2, default=str),
        ContentType="application/json",
    )
    logger.info("Wrote macro.json to s3://%s/%s", bucket, key)

    return {"status": "ok", "fields": len(macro)}


def _fetch_fred() -> dict:
    """Fetch all FRED series + compute derived metrics."""
    api_key = get_secret("FRED_API_KEY", required=False, default="")
    if not api_key:
        logger.warning("FRED_API_KEY not set — skipping FRED data")
        return {k: None for k in _FRED_SERIES}

    macro = {}
    for key, series_id in _FRED_SERIES.items():
        macro[key] = _fred_latest(series_id, api_key)

    # Yield curve slope (10yr - 2yr in bps)
    if macro.get("treasury_10yr") and macro.get("treasury_2yr"):
        macro["yield_curve_slope"] = round(
            (macro["treasury_10yr"] - macro["treasury_2yr"]) * 100, 1
        )
    else:
        macro["yield_curve_slope"] = None

    # CPI YoY
    macro["cpi_yoy"] = _fred_cpi_yoy(api_key)

    return macro


def _fred_latest(series_id: str, api_key: str) -> Optional[float]:
    """Fetch the most recent observation for a FRED series (with retry)."""
    for attempt in range(1, 3):
        try:
            params = {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 5,
            }
            resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
            resp.raise_for_status()
            obs = resp.json().get("observations", [])
            for o in obs:
                val = o.get("value", ".")
                if val != ".":
                    return float(val)
            return None
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                logger.warning("FRED %s attempt %d failed: %s — retrying", series_id, attempt, e)
                time.sleep(3)
            else:
                logger.warning("FRED %s failed after 2 attempts: %s", series_id, e)
        except Exception as e:
            logger.warning("FRED %s failed: %s", series_id, e)
            return None
    return None


def _fred_cpi_yoy(api_key: str) -> Optional[float]:
    """Compute CPI YoY% by comparing latest vs 12 months prior."""
    try:
        params = {
            "series_id": "CPIAUCSL",
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 15,
        }
        resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
        resp.raise_for_status()
        obs = [o for o in resp.json().get("observations", []) if o["value"] != "."]
        if len(obs) < 13:
            return None
        latest = float(obs[0]["value"])
        year_ago = float(obs[12]["value"])
        return round((latest / year_ago - 1) * 100, 2)
    except Exception as e:
        logger.warning("CPI YoY computation failed: %s", e)
        return None


def _fetch_market_prices() -> dict:
    """Fetch commodity and index prices via yfinance."""
    commodity_tickers = ["CL=F", "GC=F", "HG=F"]
    index_tickers = ["SPY", "QQQ", "IWM"]
    all_tickers = commodity_tickers + index_tickers

    result: dict = {}
    try:
        df = yf.download(
            all_tickers,
            period="35d",
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=True,
        )

        def _last_close(ticker: str) -> Optional[float]:
            try:
                s = df[ticker]["Close"].dropna()
                return round(float(s.iloc[-1]), 2) if not s.empty else None
            except Exception:
                return None

        def _return_30d(ticker: str) -> Optional[float]:
            try:
                s = df[ticker]["Close"].dropna()
                if len(s) >= 20:
                    return round(((s.iloc[-1] / s.iloc[-20]) - 1) * 100, 2)
            except Exception:
                pass
            return None

        result["oil_wti"] = _last_close("CL=F")
        result["gold"] = _last_close("GC=F")
        result["copper"] = _last_close("HG=F")
        result["sp500_close"] = _last_close("SPY")
        result["sp500_30d_return"] = _return_30d("SPY")
        result["qqq_30d_return"] = _return_30d("QQQ")
        result["iwm_30d_return"] = _return_30d("IWM")

    except Exception as e:
        logger.warning("yfinance macro download failed: %s", e)
        for k in ["oil_wti", "gold", "copper", "sp500_close",
                   "sp500_30d_return", "qqq_30d_return", "iwm_30d_return"]:
            result.setdefault(k, None)

    return result


def _compute_market_breadth(price_data: dict[str, pd.DataFrame]) -> dict:
    """
    Compute equity breadth metrics from ~900 stocks.

    Returns: pct_above_50d_ma, pct_above_200d_ma, advance_decline_ratio, n_stocks
    """
    above_50d = 0
    total_50d = 0
    above_200d = 0
    total_200d = 0
    advancers = 0
    decliners = 0

    for ticker, df in price_data.items():
        if df is None or df.empty or len(df) < 10:
            continue
        close = df["Close"]
        current = float(close.iloc[-1])

        if len(close) >= 50:
            ma50 = float(close.rolling(50).mean().iloc[-1])
            total_50d += 1
            if current > ma50:
                above_50d += 1

        if len(close) >= 200:
            ma200 = float(close.rolling(200).mean().iloc[-1])
            total_200d += 1
            if current > ma200:
                above_200d += 1

        if len(close) >= 6:
            five_day_return = current / float(close.iloc[-6]) - 1
            if five_day_return > 0:
                advancers += 1
            elif five_day_return < 0:
                decliners += 1

    result = {
        "pct_above_50d_ma": round(above_50d / total_50d * 100, 1) if total_50d > 0 else None,
        "pct_above_200d_ma": round(above_200d / total_200d * 100, 1) if total_200d > 0 else None,
        "advance_decline_ratio": round(advancers / max(decliners, 1), 2),
        "n_stocks": max(total_50d, total_200d),
    }
    logger.info(
        "[breadth] above_50dMA=%.1f%% above_200dMA=%.1f%% A/D=%.2f n=%d",
        result["pct_above_50d_ma"] or 0,
        result["pct_above_200d_ma"] or 0,
        result["advance_decline_ratio"],
        result["n_stocks"],
    )
    return result


def load_from_s3(bucket: str, s3_prefix: str = "market_data/") -> dict | None:
    """Load the latest macro.json from S3. Returns None if the pointer is missing; raises on unexpected errors."""
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=f"{s3_prefix}latest_weekly.json")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return None
        raise
    pointer = json.loads(resp["Body"].read())
    date = pointer.get("date")
    if not date:
        return None
    resp = s3.get_object(Bucket=bucket, Key=f"{s3_prefix}weekly/{date}/macro.json")
    return json.loads(resp["Body"].read())
