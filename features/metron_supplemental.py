"""features/metron_supplemental.py — supplemental factor-scoring inputs for
Metron-held/watchlisted tickers outside the S&P500+400 factor-scoring universe
(metron-ops#164).

crucible-research's weekly Attractiveness pipeline (``scoring/factor_scoring.py``)
only scores tickers present in ``features/{date}/technical.parquet`` /
``fundamental.parquet``, which this repo's ``compute_and_write`` builds strictly
over the ~903-name S&P500+400 ArcticDB ``universe`` library. A Metron-held ticker
outside that set (e.g. an international ADR like MARUY) never gets a row there —
not a bug, a coverage boundary (see mnemon
reference_metron_attractiveness_sp1500_scanner_universe_gate_260709).

This module computes the SAME factor inputs for just that small uncovered
delta and writes them to a SEPARATE, clearly-namespaced parquet pair under
``features/metron_supplemental/{date}/`` — deliberately NOT merged into the
core ``features/{date}/`` snapshot that Predictor trains on and Executor's
factor-loading risk matrix (``apply_factor_zscores``) consumes. That keeps this
purely a display-coverage extension for Metron's Attractiveness score, with zero
change to the ML training/risk universe. ``scoring/factor_scoring.py`` reads this
supplemental snapshot as an OPTIONAL extra source and concatenates it in before
computing composites — Metron's own ``attractiveness.py`` needs no change at all,
since it already just does a dict lookup against whatever tickers show up in
``factors/profiles/latest.json``.

Quant-only, like the main pipeline: no LLM/agent calls, so cost scales with the
handful of extra tickers, not with any research-agent budget.
"""

from __future__ import annotations

import json
import logging

import pandas as pd

from collectors.constituents import GICS_TO_ETF
from collectors.fundamentals import _fetch_single_ticker
from collectors.metron_market_data import load_metron_universe
from features.feature_engineer import FEATURES, MIN_ROWS_FOR_FEATURES, compute_features
from features.writer import write_feature_snapshot
from nousergon_lib.yfinance_quiet import yf_quiet

logger = logging.getLogger(__name__)

SUPPLEMENTAL_PREFIX = "features/metron_supplemental/"
# Metron's own held+watchlist fundamentals artifact (collectors/metron_market_data.py
# collect_fundamentals) — reused here purely as a sector source so this module doesn't
# need a second yfinance round-trip per ticker just to learn its GICS sector.
METRON_FUNDAMENTALS_KEY = "market_data/fundamentals/latest.json"

# yfinance's Ticker.info['sector'] taxonomy (Title Case) -> GICS sector name — the
# key space GICS_TO_ETF (collectors/constituents.py) and crucible-research's own
# sector_map are both keyed on. Yahoo's 11 sectors are a 1:1 relabeling of GICS's 11,
# not a different partition.
YAHOO_TO_GICS_SECTOR: dict[str, str] = {
    "Technology": "Information Technology",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Energy": "Energy",
    "Industrials": "Industrials",
    "Basic Materials": "Materials",
    "Utilities": "Utilities",
    "Real Estate": "Real Estate",
    "Communication Services": "Communication Services",
}


def uncovered_metron_tickers(bucket: str, s3_client, existing_tickers: set[str]) -> list[str]:
    """Metron held+watchlist tickers not already present in the caller's
    technical/fundamental snapshot (i.e. not in the S&P500+400 universe that
    snapshot was built over)."""
    holdings, _ = load_metron_universe(bucket, s3_client)
    held_watchlist = {h["yf_symbol"] for h in holdings}
    return sorted(held_watchlist - set(existing_tickers))


@yf_quiet
def _fetch_ticker_ohlcv(ticker: str, period: str = "2y") -> pd.DataFrame | None:
    """Full OHLCV for one arbitrary ticker. Held/watchlist tickers outside the
    ArcticDB ``universe`` lib have no cached price history, so this fetches on
    demand — unlike the main pipeline, which reads a pre-populated cache.
    Returns None on fetch failure or insufficient history (never fabricates)."""
    import yfinance as yf

    try:
        df = yf.download(ticker, period=period, interval="1d", auto_adjust=True, progress=False)
    except Exception as exc:  # noqa: BLE001 - one bad ticker must not abort the batch
        logger.warning("[metron_supplemental] OHLCV fetch failed for %s: %s", ticker, exc)
        return None
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):  # yf.download can nest columns even for one ticker
        df.columns = df.columns.get_level_values(0)
    if len(df) < MIN_ROWS_FOR_FEATURES:
        logger.info(
            "[metron_supplemental] %s has %d rows (<%d minimum) — skipped",
            ticker, len(df), MIN_ROWS_FOR_FEATURES,
        )
        return None
    return df


def _load_metron_sector(bucket: str, s3_client, ticker: str, _cache: dict = {}) -> str | None:
    """Metron's own fundamentals artifact already carries yfinance's raw
    ``Ticker.info['sector']`` for every held/watchlist ticker — reuse it rather
    than a second per-ticker yfinance call just for sector. ``_cache`` is a
    mutable-default used deliberately as a process-lifetime memo (this module
    is invoked once per weekly run, not long-lived)."""
    if "fundamentals" not in _cache:
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=METRON_FUNDAMENTALS_KEY)
            _cache["fundamentals"] = json.loads(obj["Body"].read()).get("fundamentals", {})
        except Exception as exc:  # noqa: BLE001 - sector becomes Unknown, not fatal
            logger.warning("[metron_supplemental] fundamentals artifact unavailable: %s", exc)
            _cache["fundamentals"] = {}
    return _cache["fundamentals"].get(ticker, {}).get("sector")


def compute_metron_supplemental_features(
    bucket: str,
    s3_client,
    existing_tickers: set[str],
    macro: dict[str, pd.Series],
    ohlcv_fetcher=_fetch_ticker_ohlcv,
    fundamentals_fetcher=_fetch_single_ticker,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Compute technical + fundamental factor columns for Metron-held/watchlisted
    tickers outside the S&P500+400 factor-scoring universe.

    Same ``compute_features`` math the main ~903-name pipeline runs — pure quant,
    no LLM calls. Never fabricates: a ticker with no fetchable OHLCV, insufficient
    history, or a compute error is skipped (logged), not backfilled with a guess.

    Returns (features_df, sector_map) — ``features_df`` has the same column shape
    ``compute_and_write``'s main loop produces (ticker + every ``FEATURES`` column),
    ready for ``write_feature_snapshot``. ``sector_map`` is {ticker: GICS_sector_name}
    for the caller (crucible-research) to union into its own sector_map — this repo
    has no sector_map contract of its own beyond internal sector-ETF selection.
    """
    uncovered = uncovered_metron_tickers(bucket, s3_client, existing_tickers)
    if not uncovered:
        return pd.DataFrame(), {}

    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")
    hyoas_series = macro.get("HYOAS")

    rows: list[dict] = []
    sector_map: dict[str, str] = {}

    for ticker in uncovered:
        df = ohlcv_fetcher(ticker)
        if df is None:
            continue

        yahoo_sector = _load_metron_sector(bucket, s3_client, ticker)
        gics_sector = YAHOO_TO_GICS_SECTOR.get(yahoo_sector) if yahoo_sector else None
        sector_etf = GICS_TO_ETF.get(gics_sector) if gics_sector else None
        sector_etf_series = macro.get(sector_etf) if sector_etf else None
        if gics_sector:
            sector_map[ticker] = gics_sector

        try:
            fundamental_data = fundamentals_fetcher(ticker)
        except Exception as exc:  # noqa: BLE001 - one ticker's Finnhub failure isn't fatal
            logger.warning("[metron_supplemental] fundamentals fetch failed for %s: %s", ticker, exc)
            fundamental_data = None

        try:
            featured_df = compute_features(
                df,
                spy_series=spy_series,
                vix_series=vix_series,
                sector_etf_series=sector_etf_series,
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                hyoas_series=hyoas_series,
                fundamental_data=fundamental_data,
            )
        except Exception as exc:  # noqa: BLE001 - one ticker's compute failure isn't fatal
            logger.warning("[metron_supplemental] feature compute failed for %s: %s", ticker, exc)
            continue

        if featured_df.empty:
            continue

        latest = featured_df.iloc[-1]
        row = {"ticker": ticker}
        for f in FEATURES:
            val = latest[f] if f in latest.index else 0.0
            row[f] = float(val) if pd.notna(val) else 0.0
        rows.append(row)

    logger.info(
        "[metron_supplemental] %d/%d uncovered tickers scored (%d skipped: no OHLCV / "
        "insufficient history / compute error)",
        len(rows), len(uncovered), len(uncovered) - len(rows),
    )
    return pd.DataFrame(rows), sector_map


def write_metron_supplemental_snapshot(
    date_str: str,
    features_df: pd.DataFrame,
    sector_map: dict[str, str],
    bucket: str,
    s3_client=None,
) -> dict:
    """Write the supplemental snapshot: technical/fundamental parquets (reusing
    the SAME per-group writer + registry as the main snapshot, so the schema is
    byte-for-byte identical to what ``factor_scoring.py`` already reads) plus a
    small sectors sidecar crucible-research unions into its own sector_map."""
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    if features_df.empty:
        logger.info("[metron_supplemental] nothing to write for %s — no uncovered tickers scored", date_str)
        return {"sectors": 0}

    written = write_feature_snapshot(date_str, features_df, bucket, prefix=SUPPLEMENTAL_PREFIX, s3_client=s3_client)

    sectors_key = f"{SUPPLEMENTAL_PREFIX}{date_str}/sectors.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=sectors_key,
        Body=json.dumps({"schema_version": 1, "date": date_str, "sectors": sector_map}, indent=2, sort_keys=True),
        ContentType="application/json",
    )
    written["sectors"] = len(sector_map)
    logger.info("[metron_supplemental] wrote snapshot for %s: %s", date_str, written)
    return written
