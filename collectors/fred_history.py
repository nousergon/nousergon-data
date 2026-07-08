"""
collectors/fred_history.py — FRED date-range time-series fetcher.

Stage 2.5b of the regime-conditioning rebuild (plan doc:
alpha-engine-docs/private/regime-conditioning-260510.md). Provides the
historical-fetch counterpart to ``collectors/daily_closes._fetch_fred_closes``
(which is single-latest-only) so FRED-only macro symbols (TWO, HYOAS,
and any future FRED series) can populate the 10-year price-cache
parquets the predictor reads.

The yfinance refresh path in ``collectors/prices.py`` covers symbols
yfinance carries (^VIX/^VIX3M/^TNX/^IRX); this module covers the FRED-
only ones (DGS2 → TWO, BAMLH0A0HYM2 → HYOAS).

Output schema matches what ``predictor`` reads from ``predictor/price_cache/``:
DatetimeIndex, columns = [Open, High, Low, Close, Adj_Close, Volume,
VWAP, source]. FRED publishes a single value per date; we replicate it
to OHLC and emit Volume=0 + VWAP=None to match the existing per-day
parquet contract.
"""

from __future__ import annotations

import logging
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from nousergon_lib.secrets import get_secret
from typing import Optional

import boto3
import pandas as pd
import requests

from builders._price_cache_writeboth import price_cache_write_prefixes

logger = logging.getLogger(__name__)

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_FRED_TIMEOUT = 30  # longer than _fred_latest's 15s — date-range responses are bigger

# FRED-only symbols that need historical backfill via this module. Map
# our parquet/ArcticDB ticker key → FRED series id. Mirrors the
# Stage 2.5 entries in ``collectors/daily_closes._FRED_INDEX_MAP`` for
# the symbols not on yfinance. Kept separate so changes here don't
# impact daily_closes' single-latest fetch behaviour.
FRED_HISTORY_MAP: dict[str, str] = {
    "TWO": "DGS2",
    # ICE BofA US HY Index OAS — only 3y of FRED public history (2023+),
    # license-restricted. Forward observation grows; recent walk-forward
    # folds get HY-specific regime conditioning.
    "HYOAS": "BAMLH0A0HYM2",
    # Moody's BAA Corporate Bond Yield Relative to 10Y Treasury — full
    # 40y FRED history (1986+), daily, percent. Captures the credit-
    # regime signal across the full predictor training corpus that
    # HYOAS can't (BAMLH0A0HYM2 is licence-gated to 2023+ on FRED).
    # BBB-rated spread vs HY's below-BBB; both belong in the institutional
    # credit-regime feature set per AQR/Two Sigma factor models.
    "BAA10Y": "BAA10Y",
}


def fetch_fred_history(
    series_id: str,
    period_years: int = 10,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch a multi-year date-range time series from FRED.

    Args:
        series_id: FRED series identifier (e.g., ``"DGS2"`` or
            ``"BAMLH0A0HYM2"``).
        period_years: trailing window in years. Default 10 to match the
            yfinance refresh ``period="10y"``.
        api_key: optional override; defaults to ``FRED_API_KEY`` env var.

    Returns:
        DataFrame indexed by date (DatetimeIndex, ascending) with a
        single ``value`` column of floats. Missing values (FRED's "."
        marker) are dropped. Raises ``RuntimeError`` if no API key is
        available or the request fails after retries.
    """
    if api_key is None:
        api_key = get_secret("FRED_API_KEY", required=False, default="")
    if not api_key:
        raise RuntimeError(
            "FRED_API_KEY not set — cannot fetch historical FRED series. "
            "Set the env var or pass api_key explicitly."
        )

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=int(period_years * 365.25) + 7)

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
        "sort_order": "asc",
    }

    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
            obs = payload.get("observations", [])
            if not obs:
                raise RuntimeError(
                    f"FRED returned no observations for {series_id} "
                    f"in [{start_date}, {end_date}]"
                )

            rows = []
            for o in obs:
                val = o.get("value", ".")
                if val == "." or val is None:
                    continue
                try:
                    rows.append((pd.Timestamp(o["date"]), float(val)))
                except (KeyError, ValueError):
                    continue

            if not rows:
                raise RuntimeError(
                    f"FRED {series_id}: every observation in window was "
                    f"missing or unparseable"
                )

            df = pd.DataFrame(rows, columns=["date", "value"]).set_index("date")
            df = df.sort_index()
            logger.info(
                "FRED history %s: %d observations from %s to %s",
                series_id, len(df), df.index.min().date(), df.index.max().date(),
            )
            return df

        except requests.exceptions.RequestException as e:
            last_err = e
            if attempt < 3:
                logger.warning(
                    "FRED %s history attempt %d failed: %s — retrying in %ds",
                    series_id, attempt, e, attempt * 3,
                )
                time.sleep(attempt * 3)
            else:
                logger.error(
                    "FRED %s history failed after 3 attempts: %s",
                    series_id, e,
                )
    raise RuntimeError(
        f"FRED history fetch failed for {series_id} after retries: {last_err}"
    )


def fred_history_to_ohlcv(
    df_fred: pd.DataFrame,
) -> pd.DataFrame:
    """Convert a FRED single-value time series to the OHLCV-shape parquet.

    FRED publishes a single value per date; the predictor's ``compute_features``
    + downstream readers expect parquets with ``[Open, High, Low, Close,
    Adj_Close, Volume, VWAP, source]`` columns matching what yfinance produces.
    This helper replicates the value to OHLC and emits Volume=0 + VWAP=None
    so the schema matches.

    Args:
        df_fred: output of ``fetch_fred_history`` — DatetimeIndex with a
            single ``value`` column.

    Returns:
        OHLCV-shape DataFrame with the same DatetimeIndex.
    """
    if "value" not in df_fred.columns:
        raise ValueError(
            f"Expected 'value' column in FRED DataFrame, got {list(df_fred.columns)}"
        )
    val = df_fred["value"].astype(float)
    out = pd.DataFrame(
        {
            "Open": val,
            "High": val,
            "Low": val,
            "Close": val,
            "Adj_Close": val,
            "Volume": 0,
            "VWAP": None,
            "source": "fred",
        },
        index=df_fred.index,
    )
    return out


def backfill_to_s3(
    bucket: str,
    s3_prefix: str = "predictor/price_cache/",
    tickers: list[str] | None = None,
    period_years: int = 10,
    dry_run: bool = False,
) -> dict:
    """Backfill TWO + HYOAS (or any subset of ``FRED_HISTORY_MAP``) to S3.

    One-shot operator step. Run after Stage 2.5 ships and before Stage 2c-full
    consumes the new parquets. Idempotent — full rewrite each call (matches
    the yfinance ``auto_adjust=True`` rewrite semantics).

    Args:
        bucket: S3 bucket name (typically ``alpha-engine-research``).
        s3_prefix: S3 key prefix; default matches yfinance refresh path.
        tickers: subset of ``FRED_HISTORY_MAP`` keys; default = all.
        period_years: trailing history window.
        dry_run: if True, fetch but skip S3 upload.

    Returns:
        dict with status, refreshed count, and per-ticker row counts.
    """
    if tickers is None:
        tickers = sorted(FRED_HISTORY_MAP.keys())

    unknown = [t for t in tickers if t not in FRED_HISTORY_MAP]
    if unknown:
        raise ValueError(
            f"Unknown FRED-history tickers {unknown}. "
            f"Known: {sorted(FRED_HISTORY_MAP.keys())}"
        )

    s3 = boto3.client("s3") if not dry_run else None
    results: dict[str, dict] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        local_dir = Path(tmpdir)

        for ticker in tickers:
            series_id = FRED_HISTORY_MAP[ticker]
            try:
                fred_df = fetch_fred_history(series_id, period_years=period_years)
                ohlcv = fred_history_to_ohlcv(fred_df)
                parquet_path = local_dir / f"{ticker}.parquet"
                ohlcv.to_parquet(parquet_path, engine="pyarrow", compression="snappy")
                results[ticker] = {
                    "status": "ok",
                    "rows": len(ohlcv),
                    "first_date": ohlcv.index.min().date().isoformat(),
                    "last_date": ohlcv.index.max().date().isoformat(),
                }
                if not dry_run:
                    # Wave 3 PR1: write-both to legacy ``predictor/price_cache/``
                    # + new ``reference/price_cache/`` (see
                    # builders/_price_cache_writeboth.py for soak contract)
                    for prefix in price_cache_write_prefixes(s3_prefix):
                        s3.upload_file(
                            str(parquet_path),
                            bucket,
                            f"{prefix}{ticker}.parquet",
                        )
                        logger.info(
                            "Uploaded s3://%s/%s%s.parquet (%d rows, %s → %s)",
                            bucket, prefix, ticker, len(ohlcv),
                            results[ticker]["first_date"], results[ticker]["last_date"],
                        )
            except Exception as e:
                logger.error("Backfill failed for %s (%s): %s", ticker, series_id, e)
                results[ticker] = {"status": "error", "error": str(e)}

    n_ok = sum(1 for r in results.values() if r["status"] == "ok")
    return {
        "status": "ok" if n_ok == len(tickers) else "partial",
        "refreshed": n_ok,
        "total": len(tickers),
        "per_ticker": results,
        "dry_run": dry_run,
    }


def main():
    """CLI entry point — one-shot backfill of all FRED_HISTORY_MAP tickers.

    Usage:
        python -m collectors.fred_history --bucket alpha-engine-research
        python -m collectors.fred_history --dry-run
        python -m collectors.fred_history --tickers TWO HYOAS --period-years 10
    """
    import argparse
    import json
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--bucket", default="alpha-engine-research",
        help="S3 bucket name. Default: alpha-engine-research",
    )
    parser.add_argument(
        "--prefix", default="predictor/price_cache/",
        help="S3 prefix. Default: predictor/price_cache/",
    )
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help="Subset of FRED_HISTORY_MAP keys. Default: all.",
    )
    parser.add_argument(
        "--period-years", type=int, default=10,
        help="Trailing history window. Default 10 (matches yfinance).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch but skip S3 upload.",
    )
    args = parser.parse_args()

    result = backfill_to_s3(
        bucket=args.bucket,
        s3_prefix=args.prefix,
        tickers=args.tickers,
        period_years=args.period_years,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, default=str))
    sys.exit(0 if result["status"] == "ok" else 2)


if __name__ == "__main__":
    main()
