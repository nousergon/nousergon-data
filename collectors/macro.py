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

import io
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import boto3
import numpy as np

from alpha_engine_lib.secrets import get_secret
import pandas as pd
import requests
import yfinance as yf

from alpha_engine_lib.arcticdb import load_universe_ohlcv

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

# ── Historical macro time series ──────────────────────────────────────────────
# A standalone, dashboard-facing artifact (NOT the per-ticker feature store):
# full FRED observation history per series, refreshed weekly and OVERWRITTEN each
# run (FRED returns the entire series, so the artifact is idempotent + self-
# healing — no week-by-week accumulation). Consumed by robodashboard's Macro page
# via market_data/macro_history.parquet. See ARTIFACT_REGISTRY.yaml.
_MACRO_HISTORY_KEY = "macro_history.parquet"
_MACRO_HISTORY_START = "2000-01-01"  # ~25y of history is plenty for a dashboard

# series_id → (label, units, frequency). Mirrors _FRED_SERIES plus the native
# 10Y-2Y spread series and the raw CPI index (CPI YoY is derived below).
_FRED_HISTORY_SERIES = {
    "FEDFUNDS": ("Fed Funds Rate", "percent", "monthly"),
    "DGS2": ("2Y Treasury", "percent", "daily"),
    "DGS10": ("10Y Treasury", "percent", "daily"),
    "T10Y2Y": ("Yield Curve Slope (10Y-2Y)", "percent", "daily"),
    "VIXCLS": ("VIX", "index", "daily"),
    "UNRATE": ("Unemployment Rate", "percent", "monthly"),
    "UMCSENT": ("Consumer Sentiment", "index", "monthly"),
    "ICSA": ("Initial Jobless Claims", "count", "weekly"),
    "BAMLH0A0HYM2": ("High-Yield Credit Spread (OAS)", "percent", "daily"),
    "CPIAUCSL": ("CPI (Index)", "index", "monthly"),
}

# Derived series (computed from a raw series above), appended to the artifact so
# the dashboard consumes them directly rather than re-deriving.
_CPI_YOY = ("CPI_YOY", "Inflation (CPI YoY)", "percent", "monthly")


def _load_breadth_prices(bucket: str) -> Optional[dict]:
    """Load the ~900-ticker price set for breadth from the ArcticDB
    universe library.

    Wave-4 terminal state: ``predictor/price_cache_slim/`` is deleted;
    ArcticDB (via the lib ``load_universe_ohlcv`` reader) is the sole
    source. Removal of the slim fallback + dual-read was gated on the
    6-week consumer-side ArcticDB-primary soak since the 2026-04-14
    cutover (see PR #269 body for the full rationale).

    Returns ``None`` if the ArcticDB read fails (caller then omits the
    breadth key — the existing no-null contract; Research has its own
    fallback). This matches the pre-Wave-4 behaviour when the single
    price source was unavailable.
    """
    try:
        return load_universe_ohlcv(bucket) or None
    except Exception as exc:  # noqa: BLE001 - omit breadth, don't write null
        logger.warning("ArcticDB universe read for breadth failed: %s", exc)
        return None


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
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()

    macro = _fetch_fred()
    market = _fetch_market_prices()
    macro.update(market)

    # Compute breadth. If the caller did not pass in price_data, load the
    # ~900-ticker price set (ArcticDB primary, slim-cache fallback — see
    # _load_breadth_prices) so downstream Research still gets a real breadth
    # reading. If both sources fail we OMIT the "breadth" key entirely rather
    # than writing null — Research has its own fallback and macro_agent reads
    # macro_data.get("breadth", {}), which only honors the default when the
    # key is missing.
    if price_data is None:
        price_data = _load_breadth_prices(bucket)

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

    # Historical macro time series — a SECONDARY, dashboard-only artifact hung off
    # the primary macro.json write. Guarded so a FRED-history failure (the swallowed
    # mode) cannot mask the macro.json success that the trading pipeline depends on
    # (why primary survives); the failure is recorded in the WARN log + the returned
    # ``macro_history`` status field (the recording surfaces). Not raised here.
    history_status: dict = {"status": "skipped_empty", "rows": 0}
    try:
        history_status = write_macro_history(bucket=bucket, s3_prefix=s3_prefix, dry_run=dry_run)
    except Exception as e:  # noqa: BLE001 - secondary artifact; never fail macro.json
        logger.warning("macro_history write failed (macro.json unaffected): %s", e)
        history_status = {"status": "error", "error": str(e)}

    # Macro release calendar — a SECOND dashboard-only artifact, guarded the same
    # way (a calendar failure must never mask the macro.json success the trading
    # pipeline depends on; the failure is recorded in the WARN log + the returned
    # ``release_calendar`` status field).
    release_status: dict = {"status": "skipped_empty", "rows": 0}
    try:
        release_status = write_release_calendar(bucket=bucket, s3_prefix=s3_prefix, dry_run=dry_run)
    except Exception as e:  # noqa: BLE001 - secondary artifact; never fail macro.json
        logger.warning("release_calendar write failed (macro.json unaffected): %s", e)
        release_status = {"status": "error", "error": str(e)}

    return {
        "status": "ok",
        "fields": len(macro),
        "macro_history": history_status,
        "release_calendar": release_status,
    }


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


def _fred_history(series_id: str, api_key: str, start: str = _MACRO_HISTORY_START) -> list[tuple[str, float]]:
    """Fetch the full observation history for a FRED series (with retry).

    Returns ``[(date, value), ...]`` ascending by date, missing values ('.')
    dropped. Returns ``[]`` on persistent failure (the caller omits the series
    rather than writing partial/None rows).
    """
    for attempt in range(1, 3):
        try:
            params = {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "observation_start": start,
                "sort_order": "asc",
            }
            resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
            resp.raise_for_status()
            obs = resp.json().get("observations", [])
            out: list[tuple[str, float]] = []
            for o in obs:
                val = o.get("value", ".")
                if val != ".":
                    out.append((o["date"], float(val)))
            return out
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                logger.warning("FRED history %s attempt %d failed: %s — retrying", series_id, attempt, e)
                time.sleep(3)
            else:
                logger.warning("FRED history %s failed after 2 attempts: %s", series_id, e)
        except Exception as e:
            logger.warning("FRED history %s failed: %s", series_id, e)
            return []
    return []


def _cpi_yoy_rows(cpi_obs: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """Derive CPI YoY% from the raw monthly CPI index history.

    For each month with an observation 12 entries prior, YoY = (v / v_12mo − 1) * 100.
    ``cpi_obs`` must be ascending by date (as ``_fred_history`` returns).
    """
    rows: list[tuple[str, float]] = []
    for i in range(12, len(cpi_obs)):
        prior = cpi_obs[i - 12][1]
        if prior:
            rows.append((cpi_obs[i][0], round((cpi_obs[i][1] / prior - 1) * 100, 2)))
    return rows


def build_macro_history(api_key: str | None = None) -> pd.DataFrame:
    """Build the long-format macro history DataFrame.

    Columns: ``date, series_id, label, value, units, frequency``. One row per
    (series, date). Includes the raw FRED series in ``_FRED_HISTORY_SERIES`` plus
    the derived CPI YoY series. Returns an empty frame (correct columns) when no
    FRED key is configured, so callers can skip the write cleanly.
    """
    cols = ["date", "series_id", "label", "value", "units", "frequency"]
    if api_key is None:
        api_key = get_secret("FRED_API_KEY", required=False, default="")
    if not api_key:
        logger.warning("FRED_API_KEY not set — skipping macro history")
        return pd.DataFrame(columns=cols)

    records: list[dict] = []
    cpi_obs: list[tuple[str, float]] = []
    for series_id, (label, units, frequency) in _FRED_HISTORY_SERIES.items():
        obs = _fred_history(series_id, api_key)
        if series_id == "CPIAUCSL":
            cpi_obs = obs
        for date, value in obs:
            records.append(
                {"date": date, "series_id": series_id, "label": label,
                 "value": value, "units": units, "frequency": frequency}
            )

    # Derived: CPI YoY (inflation) from the raw CPI index.
    yoy_id, yoy_label, yoy_units, yoy_freq = _CPI_YOY
    for date, value in _cpi_yoy_rows(cpi_obs):
        records.append(
            {"date": date, "series_id": yoy_id, "label": yoy_label,
             "value": value, "units": yoy_units, "frequency": yoy_freq}
        )

    df = pd.DataFrame(records, columns=cols)
    logger.info("Built macro history: %d rows across %d series", len(df), df["series_id"].nunique() if not df.empty else 0)
    return df


def write_macro_history(bucket: str, s3_prefix: str = "market_data/", dry_run: bool = False) -> dict:
    """Build + write the macro history parquet to ``market_data/macro_history.parquet``.

    OVERWRITES the single fixed key each run (idempotent — FRED returns full
    history). Returns a status dict; raises on the S3 write failure so a hard
    producer fault surfaces, but an empty build (no FRED key / all fetches failed)
    is a no-op rather than writing an empty artifact over a good one.
    """
    df = build_macro_history()
    if df.empty:
        logger.warning("macro history empty — skipping write (no FRED key or all series failed)")
        return {"status": "skipped_empty", "rows": 0}

    if dry_run:
        logger.info("[dry-run] macro_history: %d rows across %d series", len(df), df["series_id"].nunique())
        return {"status": "ok_dry_run", "rows": len(df), "series": int(df["series_id"].nunique())}

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    buf.seek(0)
    key = f"{s3_prefix}{_MACRO_HISTORY_KEY}"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info("Wrote macro history to s3://%s/%s (%d rows)", bucket, key, len(df))
    return {"status": "ok", "rows": len(df), "series": int(df["series_id"].nunique())}


# ── Macro release calendar ────────────────────────────────────────────────────
# A second dashboard-facing artifact: forward-looking macro EVENT dates (FRED
# data releases + scheduled FOMC meetings) for robodashboard's Calendar page.
# Unlike macro_history (full observation history), this carries the NEXT release
# date per indicator. Overwritten weekly. See ARTIFACT_REGISTRY.yaml.
_RELEASE_CALENDAR_KEY = "macro_release_calendar.parquet"
_RELEASE_HORIZON_DAYS = 180
_FRED_RELEASE_DATES_BASE = "https://api.stlouisfed.org/fred/release/dates"
_FRED_SERIES_RELEASE_BASE = "https://api.stlouisfed.org/fred/series/release"
_RELEASE_CALENDAR_COLS = ["date", "kind", "series_id", "label", "release_name"]

# FRED series → calendar label, restricted to indicators with a clean monthly /
# weekly release schedule. FEDFUNDS and the daily series (DGS2/DGS10/T10Y2Y/
# VIXCLS/HY-OAS) are intentionally absent: a daily release is noise on a
# calendar, and the meaningful fed-funds event is the FOMC meeting (emitted
# separately below), not the daily H.15 print.
_RELEASE_CALENDAR_SERIES = {
    "CPIAUCSL": "CPI release",
    "UNRATE": "Employment Situation (Unemployment)",
    "ICSA": "Initial Jobless Claims",
    "UMCSENT": "Consumer Sentiment",
}

# Scheduled 2026 FOMC meeting decision days — the SECOND day of each two-day
# meeting, when the statement the market reacts to is released. Source:
# federalreserve.gov FOMC calendar. REFRESH ANNUALLY (append 2027 dates when the
# Fed publishes them; stale years simply drop off via the >= today filter).
_FOMC_MEETINGS = (
    "2026-01-28",
    "2026-03-18",
    "2026-04-29",
    "2026-06-17",
    "2026-07-29",
    "2026-09-16",
    "2026-10-28",
    "2026-12-09",
)


def _fred_release_id(series_id: str, api_key: str) -> tuple[int, str] | None:
    """Return ``(release_id, release_name)`` for a FRED series, or None on failure."""
    try:
        params = {"series_id": series_id, "api_key": api_key, "file_type": "json"}
        resp = requests.get(_FRED_SERIES_RELEASE_BASE, params=params, timeout=_FRED_TIMEOUT)
        resp.raise_for_status()
        releases = resp.json().get("releases", [])
        if not releases:
            return None
        r = releases[0]
        return int(r["id"]), r.get("name", "")
    except Exception as e:  # noqa: BLE001 - omit this series, don't fail the artifact
        logger.warning("FRED series/release %s failed: %s", series_id, e)
        return None


def _fred_release_dates(release_id: int, api_key: str) -> list[str]:
    """Return scheduled release dates (ISO strings) for a FRED release.

    ``include_release_dates_with_no_data=true`` makes FRED include FUTURE
    scheduled dates (which have no data yet). Returns ``[]`` on failure; the
    caller filters to the forward horizon.

    ``sort_order=desc`` returns the furthest-future scheduled dates first, so the
    limit must exceed the count of ALL future-scheduled dates or the NEAREST ones
    get truncated off the bottom — that silently dropped near-term weekly claims
    (a ~7-week gap) at limit=24. 130 comfortably covers a year-plus of any
    cadence (weekly ≈ 52/yr) while the caller still trims to the 180d horizon.
    """
    try:
        params = {
            "release_id": release_id,
            "api_key": api_key,
            "file_type": "json",
            "include_release_dates_with_no_data": "true",
            "sort_order": "desc",
            "limit": 130,
        }
        resp = requests.get(_FRED_RELEASE_DATES_BASE, params=params, timeout=_FRED_TIMEOUT)
        resp.raise_for_status()
        return [d["date"] for d in resp.json().get("release_dates", []) if d.get("date")]
    except Exception as e:  # noqa: BLE001 - omit this series, don't fail the artifact
        logger.warning("FRED release/dates %s failed: %s", release_id, e)
        return []


def build_release_calendar(api_key: str | None = None, today: date | None = None) -> pd.DataFrame:
    """Build the forward macro event calendar (FRED releases + FOMC meetings).

    Columns: ``date, kind ('release'|'fomc'), series_id, label, release_name``.
    One row per upcoming FRED release in ``[today, today+_RELEASE_HORIZON_DAYS]``
    plus every future scheduled FOMC meeting. FRED fetches are best-effort per
    series (a failure omits that series, not the artifact), so a FRED hiccup
    still yields a FOMC-only calendar. Returns an empty frame (right columns)
    only when there's no FRED key — matching ``build_macro_history`` so the
    caller skips the write cleanly.
    """
    if api_key is None:
        api_key = get_secret("FRED_API_KEY", required=False, default="")
    if not api_key:
        logger.warning("FRED_API_KEY not set — skipping release calendar")
        return pd.DataFrame(columns=_RELEASE_CALENDAR_COLS)
    if today is None:
        today = datetime.now(timezone.utc).date()
    horizon = today + timedelta(days=_RELEASE_HORIZON_DAYS)

    records: list[dict] = []
    for series_id, label in _RELEASE_CALENDAR_SERIES.items():
        rel = _fred_release_id(series_id, api_key)
        if rel is None:
            continue
        release_id, release_name = rel
        for d in _fred_release_dates(release_id, api_key):
            try:
                dd = datetime.strptime(d, "%Y-%m-%d").date()
            except ValueError:
                continue
            if today <= dd <= horizon:
                records.append(
                    {"date": d, "kind": "release", "series_id": series_id,
                     "label": label, "release_name": release_name}
                )

    for d in _FOMC_MEETINGS:
        if datetime.strptime(d, "%Y-%m-%d").date() >= today:
            records.append(
                {"date": d, "kind": "fomc", "series_id": "FOMC",
                 "label": "FOMC Meeting", "release_name": "Federal Open Market Committee"}
            )

    df = pd.DataFrame(records, columns=_RELEASE_CALENDAR_COLS)
    if not df.empty:
        df.sort_values("date", inplace=True, kind="stable")
        df.reset_index(drop=True, inplace=True)
    logger.info("Built release calendar: %d events", len(df))
    return df


def write_release_calendar(bucket: str, s3_prefix: str = "market_data/", dry_run: bool = False) -> dict:
    """Build + write the release calendar to ``market_data/macro_release_calendar.parquet``.

    OVERWRITES the single fixed key each run (idempotent — the build is fully
    derived from FRED's schedule + the FOMC constant). An empty build (no FRED
    key) is a no-op rather than clobbering a good artifact with nothing.
    """
    df = build_release_calendar()
    if df.empty:
        logger.warning("release calendar empty — skipping write (no FRED key)")
        return {"status": "skipped_empty", "rows": 0}

    if dry_run:
        logger.info("[dry-run] release_calendar: %d events", len(df))
        return {"status": "ok_dry_run", "rows": len(df)}

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    buf.seek(0)
    key = f"{s3_prefix}{_RELEASE_CALENDAR_KEY}"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info("Wrote release calendar to s3://%s/%s (%d events)", bucket, key, len(df))
    return {"status": "ok", "rows": len(df)}


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
