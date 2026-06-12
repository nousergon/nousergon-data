"""Metron market-data producer — EOD closes + FX for Metron's held-ticker universe.

`alpha-engine-data` is the single market-data ground truth for the whole Nous Ergon
system. Metron publishes its held-ticker universe to
``s3://<bucket>/metron/holdings_universe.json`` (yf_symbols + the non-USD currencies it
holds); this producer reads it and writes two artifacts the Metron app consumes — so
Metron makes NO direct market-data API calls of its own:

    market_data/eod_closes/{date}.json   + market_data/eod_closes/latest.json
    market_data/fx/{date}.json           + market_data/fx/latest.json
    market_data/fundamentals/latest.json   (daily — tearsheet multiples/ratios)
    market_data/intraday/latest.json       (every 15 min during US RTH — Today view)

Closes cover the held union — including foreign listings (``1299.HK``, ``RMS.PA``), OTC
(``GTBIF``), and funds (``FNILX``) that the ~903-name SP1500 constituent cache refuses.
FX covers the held non-USD currencies (``{CCY}USD=X``).

Artifact schemas (versioned — Metron's consumer pins on ``schema_version``):

    closes: {schema_version, as_of, source, closes: {yf_symbol: {close, currency, bar_date}}}
    fx:     {schema_version, as_of, base: "USD", rates: {CCY: rate}}

Runs each weekday in ``weekly_collector._run_daily``. Best-effort per the module posture:
the universe read fail-softs to an empty pull (logged), and a fetch/​write error returns
an ``error`` status so the phase registry records it without aborting the daily run.

Entry point: ``python -m collectors.metron_market_data [--date YYYY-MM-DD] [--dry-run]``
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
# Metron publishes its held universe here (see metron api/services/data_spine.py).
HOLDINGS_UNIVERSE_KEY = "metron/holdings_universe.json"
CLOSES_PREFIX = "market_data/eod_closes/"
FX_PREFIX = "market_data/fx/"
# History artifacts (per-symbol / per-currency) — power Metron's Performance NAV
# reconstruction (close series) + as-of-date realized/dividend FX conversion.
CLOSE_HISTORY_PREFIX = "market_data/close_history/"
FX_HISTORY_PREFIX = "market_data/fx_history/"
# Reference data — GICS sector per held symbol + SPY's sector weights (Brinson
# attribution), and each held symbol's next earnings date (Calendar page). Moves
# Metron's last yfinance fetches to the spine so Metron reads ALL external data here.
SECTORS_PREFIX = "market_data/sectors/"
EARNINGS_PREFIX = "market_data/earnings/"
# Macro indicators for Metron's Macro page (FRED observation series) — Metron's last
# direct external fetch. The data macro.json artifact is latest-values-only / weekly /
# missing T10YIE+T10Y2Y, so this publishes the exact series Metron needs as history.
MACRO_PREFIX = "market_data/macro/"
# FRED series ids Metron's Macro page renders (mirrors metron INDICATORS).
METRON_MACRO_SERIES = ["FEDFUNDS", "UNRATE", "T10YIE", "DGS10", "DGS2", "T10Y2Y", "VIXCLS"]
# Fundamentals — per-holding valuation multiples + balance-sheet ratios for Metron's
# tearsheets (config#1022). Values are passed through EXACTLY as yfinance Ticker.info
# returns them (no unit conversion at the producer — no fabrication; the consumer owns
# display semantics and pins schema_version). NOTE: the weekly Finnhub
# collectors/fundamentals.py covers the ~903-name SYSTEM universe with a predictor
# feature-set; this family covers Metron's HELD universe (incl. foreign/OTC/funds
# Finnhub free tier can't price) with the wider tearsheet field set.
FUNDAMENTALS_PREFIX = "market_data/fundamentals/"
# yfinance Ticker.info keys published per symbol (artifact field == info key).
FUNDAMENTALS_INFO_KEYS = [
    "trailingPE", "forwardPE", "trailingPegRatio", "enterpriseToEbitda",
    "earningsGrowth", "revenueGrowth", "debtToEquity", "currentRatio", "quickRatio",
    "returnOnEquity", "returnOnAssets", "grossMargins", "operatingMargins",
    "beta", "dividendYield", "marketCap", "sector", "industry",
]
# Intraday — last/open/prior-close per held symbol, refreshed every 15 min during US
# regular trading hours by a systemd timer on the trading box (config#1023). Single
# `latest.json` key (no dated files — 26 writes/day would litter the prefix).
INTRADAY_PREFIX = "market_data/intraday/"
CLOSES_SCHEMA_VERSION = 1
FX_SCHEMA_VERSION = 1
CLOSE_HISTORY_SCHEMA_VERSION = 1
FX_HISTORY_SCHEMA_VERSION = 1
SECTORS_SCHEMA_VERSION = 1
EARNINGS_SCHEMA_VERSION = 1
MACRO_SCHEMA_VERSION = 1
FUNDAMENTALS_SCHEMA_VERSION = 1
INTRADAY_SCHEMA_VERSION = 1
BASE_CURRENCY = "USD"
DEFAULT_HISTORY_PERIOD = "10y"  # mirrors the predictor price_cache 10y convention
BENCHMARK = "SPY"  # the attribution benchmark whose GICS sector weights we publish

# yfinance ``funds_data.sector_weightings`` snake_case keys → canonical GICS label
# (the Title-Case labels ``Ticker.info['sector']`` returns). Stable GICS reference;
# mirrored from metron portfolio_analytics/sectors.
_FUNDS_SECTOR_KEY = {
    "technology": "Technology", "financial_services": "Financial Services",
    "healthcare": "Healthcare", "consumer_cyclical": "Consumer Cyclical",
    "consumer_defensive": "Consumer Defensive", "energy": "Energy",
    "industrials": "Industrials", "basic_materials": "Basic Materials",
    "utilities": "Utilities", "realestate": "Real Estate",
    "communication_services": "Communication Services",
}

_YFINANCE_BATCH_SIZE = 100
_YFINANCE_BATCH_DELAY = 2  # seconds between batches (rate-limit courtesy)

# A close source maps yf_symbols → {yf_symbol: (close, bar_date_iso)}. Default is
# yfinance; tests inject their own. Mirrors the price-source seam in the Metron consumer.
CloseSource = Callable[[list[str]], dict[str, tuple[float, str]]]
# An FX source maps currencies → {currency: rate} (base per 1 unit of currency).
FxSource = Callable[[list[str]], dict[str, float]]
# History sources map symbols/currencies → {key: [(date_iso, value), …]} ascending.
CloseHistorySource = Callable[[list[str]], dict[str, list[tuple[str, float]]]]
FxHistorySource = Callable[[list[str]], dict[str, list[tuple[str, float]]]]
# Reference sources: sectors maps yf_symbols → {yf_symbol: gics}; benchmark weights is a
# 0-arg → {sector: weight}; earnings maps yf_symbols → {yf_symbol: date_iso}.
SectorSource = Callable[[list[str]], dict[str, str]]
BenchmarkWeightsSource = Callable[[], dict[str, float]]
EarningsSource = Callable[[list[str]], dict[str, str]]
# A macro source maps FRED series ids → {series_id: [(date_iso, value), …]} ascending.
MacroSource = Callable[[list[str]], dict[str, list[tuple[str, float]]]]
# A fundamentals source maps yf_symbols → {yf_symbol: {info_key: value}}.
FundamentalsSource = Callable[[list[str]], dict[str, dict]]
# An intraday source maps yf_symbols → {yf_symbol: quote dict} (see _yfinance_intraday).
IntradaySource = Callable[[list[str]], dict[str, dict]]


# ── Universe read ───────────────────────────────────────────────────────────


def load_metron_universe(bucket: str, s3_client: Any) -> tuple[list[dict], list[str]]:
    """Read Metron's published held universe → ``(holdings, currencies)``.

    ``holdings`` = ``[{"yf_symbol", "currency"}, …]``; ``currencies`` = distinct non-USD
    currencies held. Fail-soft: a missing object / no creds / parse error → ``([], [])``
    (logged) so the daily run proceeds rather than aborting."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=HOLDINGS_UNIVERSE_KEY)
        data = json.loads(obj["Body"].read())
        holdings = [
            {"yf_symbol": str(h["yf_symbol"]).strip(), "currency": str(h.get("currency", "USD")).strip()}
            for h in data.get("holdings", [])
            if str(h.get("yf_symbol", "")).strip()
        ]
        currencies = [str(c).strip().upper() for c in data.get("currencies", []) if str(c).strip()]
        logger.info("[metron_market_data] universe: %d instruments, %d non-USD currencies",
                    len(holdings), len(currencies))
        return holdings, currencies
    except Exception as e:  # missing object, no creds, parse error, etc.
        logger.warning("[metron_market_data] metron universe unavailable (%s) — empty pull", e)
        return [], []


# ── yfinance fetchers (default sources) ─────────────────────────────────────


def _yfinance_closes(yf_symbols: list[str]) -> dict[str, tuple[float, str]]:
    """Latest daily close per yf_symbol via yfinance → ``{yf_symbol: (close, bar_date)}``.
    Foreign listings (``.HK``/``.PA``/…) resolve natively. Unpriceable symbols omitted."""
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover - yfinance/pandas are prod deps
        logger.warning("[metron_market_data] yfinance/pandas unavailable")
        return {}

    out: dict[str, tuple[float, str]] = {}
    batches = [yf_symbols[i:i + _YFINANCE_BATCH_SIZE] for i in range(0, len(yf_symbols), _YFINANCE_BATCH_SIZE)]
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(_YFINANCE_BATCH_DELAY)
        try:
            raw = yf.download(
                tickers=batch[0] if len(batch) == 1 else batch,
                period="5d", interval="1d", auto_adjust=False,
                progress=False, group_by="ticker", threads=True,
            )
            is_multi = isinstance(raw.columns, pd.MultiIndex)
            for sym in batch:
                try:
                    df = (raw[sym] if is_multi else raw).copy()
                    df.index = pd.to_datetime(df.index)
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue
                    last = df.iloc[-1]
                    bar_date = df.index[-1].date().isoformat()
                    out[sym] = (round(float(last["Close"]), 4), bar_date)
                except Exception as e:
                    logger.warning("[metron_market_data] close extract failed for %s: %s", sym, e)
        except Exception as e:
            logger.warning("[metron_market_data] yfinance close batch failed: %s", e)
    logger.info("[metron_market_data] closes: %d/%d symbols priced", len(out), len(yf_symbols))
    return out


def _yfinance_fx(currencies: list[str], base: str = BASE_CURRENCY) -> dict[str, float]:
    """Latest FX rate per currency via yfinance ``{CCY}{BASE}=X`` → ``{CCY: rate}``
    (``base`` per 1 unit of ``CCY``). Unresolvable pairs omitted — no fabrication."""
    if not currencies:
        return {}
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover
        logger.warning("[metron_market_data] yfinance/pandas unavailable for FX")
        return {}

    pairs = {f"{c}{base}=X": c for c in currencies if c and c != base}
    if not pairs:
        return {}
    out: dict[str, float] = {}
    try:
        raw = yf.download(
            tickers=list(pairs) if len(pairs) > 1 else next(iter(pairs)),
            period="5d", interval="1d", auto_adjust=False,
            progress=False, group_by="ticker", threads=True,
        )
        is_multi = isinstance(raw.columns, pd.MultiIndex)
        for pair, ccy in pairs.items():
            try:
                df = (raw[pair] if is_multi else raw).copy()
                df = df.dropna(subset=["Close"])
                if df.empty:
                    continue
                out[ccy] = round(float(df.iloc[-1]["Close"]), 6)
            except Exception as e:
                logger.warning("[metron_market_data] FX extract failed for %s: %s", pair, e)
    except Exception as e:
        logger.warning("[metron_market_data] yfinance FX batch failed: %s", e)
    logger.info("[metron_market_data] fx: %d/%d currencies resolved", len(out), len(pairs))
    return out


def _yf_history(symbols: list[str], period: str, *, is_fx: bool = False, base: str = BASE_CURRENCY) -> dict[str, list[tuple[str, float]]]:
    """Daily close series per symbol via yfinance over ``period`` →
    ``{key: [(bar_date, close), …]}`` ascending. ``is_fx`` maps a currency to the
    ``{CCY}{BASE}=X`` pair and keys the result by the bare currency. Empty series omitted."""
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover
        logger.warning("[metron_market_data] yfinance/pandas unavailable for history")
        return {}

    targets = {f"{c}{base}=X": c for c in symbols if c and c != base} if is_fx else {s: s for s in symbols if s}
    if not targets:
        return {}
    out: dict[str, list[tuple[str, float]]] = {}
    keys = list(targets)
    batches = [keys[i:i + _YFINANCE_BATCH_SIZE] for i in range(0, len(keys), _YFINANCE_BATCH_SIZE)]
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(_YFINANCE_BATCH_DELAY)
        try:
            raw = yf.download(tickers=batch[0] if len(batch) == 1 else batch, period=period,
                              interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=True)
            is_multi = isinstance(raw.columns, pd.MultiIndex)
            for key in batch:
                try:
                    df = (raw[key] if is_multi else raw).copy()
                    df.index = pd.to_datetime(df.index)
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue
                    out[targets[key]] = [(d.date().isoformat(), round(float(c), 6)) for d, c in df["Close"].items()]
                except Exception as e:
                    logger.warning("[metron_market_data] history extract failed for %s: %s", key, e)
        except Exception as e:
            logger.warning("[metron_market_data] yfinance history batch failed: %s", e)
    logger.info("[metron_market_data] history: %d/%d series captured", len(out), len(targets))
    return out


def _yfinance_close_history(yf_symbols: list[str], period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, list[tuple[str, float]]]:
    return _yf_history(yf_symbols, period, is_fx=False)


def _yfinance_fx_history(currencies: list[str], period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, list[tuple[str, float]]]:
    return _yf_history(currencies, period, is_fx=True)


def _yfinance_sectors(yf_symbols: list[str]) -> dict[str, str]:
    """Canonical GICS sector per held symbol via ``yf.Ticker(sym).info['sector']``.
    Fail-soft: an unclassifiable symbol is omitted (Metron shows a coverage gap)."""
    try:
        import yfinance as yf
    except ImportError:  # pragma: no cover
        return {}
    out: dict[str, str] = {}
    for sym in yf_symbols:
        try:
            sector = (yf.Ticker(sym).info or {}).get("sector")
            if sector:
                out[sym] = str(sector)
        except Exception as e:
            logger.warning("[metron_market_data] sector fetch failed for %s: %s", sym, e)
    logger.info("[metron_market_data] sectors: %d/%d classified", len(out), len(yf_symbols))
    return out


def _yfinance_spy_weights() -> dict[str, float]:
    """SPY's live GICS sector weights (canonical label → fraction) via
    ``funds_data.sector_weightings`` (snake_case → canonical). ``{}`` on failure."""
    try:
        import yfinance as yf
    except ImportError:  # pragma: no cover
        return {}
    try:
        raw = yf.Ticker(BENCHMARK).funds_data.sector_weightings or {}
    except Exception as e:
        logger.warning("[metron_market_data] SPY sector weights fetch failed: %s", e)
        return {}
    return {_FUNDS_SECTOR_KEY[k]: float(v) for k, v in raw.items() if k in _FUNDS_SECTOR_KEY}


def _yfinance_earnings(yf_symbols: list[str]) -> dict[str, str]:
    """Next (earliest upcoming) earnings date per held symbol via yfinance →
    ``{yf_symbol: date_iso}``. Fail-soft: no resolvable date → omitted."""
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover
        return {}
    out: dict[str, str] = {}
    for sym in yf_symbols:
        try:
            df = yf.Ticker(sym).get_earnings_dates(limit=8)
            if df is None or df.empty:
                continue
            idx = pd.to_datetime(df.index)
            today = pd.Timestamp.utcnow().tz_localize(None)
            future = sorted(d for d in idx.tz_localize(None) if d >= today)
            if future:
                out[sym] = future[0].date().isoformat()
        except Exception as e:
            logger.warning("[metron_market_data] earnings fetch failed for %s: %s", sym, e)
    logger.info("[metron_market_data] earnings: %d/%d dated", len(out), len(yf_symbols))
    return out


def _yfinance_fundamentals(yf_symbols: list[str]) -> dict[str, dict]:
    """Tearsheet fundamentals per held symbol via ``yf.Ticker(sym).info`` →
    ``{yf_symbol: {info_key: value}}`` over ``FUNDAMENTALS_INFO_KEYS``.

    Values pass through exactly as yfinance returns them (units documented at the
    consumer; no producer-side conversion = no fabricated units). Fail-soft per
    symbol; a symbol with no resolvable info is omitted (coverage gap, not zeros).
    """
    try:
        import yfinance as yf
    except ImportError:  # pragma: no cover
        return {}
    out: dict[str, dict] = {}
    for sym in yf_symbols:
        try:
            info = yf.Ticker(sym).info or {}
        except Exception as e:
            logger.warning("[metron_market_data] fundamentals fetch failed for %s: %s", sym, e)
            continue
        fields = {k: info[k] for k in FUNDAMENTALS_INFO_KEYS if info.get(k) is not None}
        if fields:
            out[sym] = fields
    logger.info("[metron_market_data] fundamentals: %d/%d symbols covered", len(out), len(yf_symbols))
    return out


def _yfinance_intraday(yf_symbols: list[str]) -> dict[str, dict]:
    """Latest (~15-min delayed) quote + session context per symbol, one batched
    2-day daily-bar download → ``{yf_symbol: quote}`` where quote =
    ``{last, open, prev_close, session_date, prev_session_date}``.

    During a symbol's session the latest daily bar's Close IS the delayed last
    price; outside it (e.g. a HK listing during US RTH) it is that exchange's
    last completed session — ``session_date`` lets the consumer tell which.
    Fail-soft per symbol; a symbol with no two valid bars is omitted.
    """
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover
        logger.warning("[metron_market_data] yfinance/pandas unavailable for intraday")
        return {}
    out: dict[str, dict] = {}
    batches = [yf_symbols[i:i + _YFINANCE_BATCH_SIZE] for i in range(0, len(yf_symbols), _YFINANCE_BATCH_SIZE)]
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(_YFINANCE_BATCH_DELAY)
        try:
            raw = yf.download(
                tickers=batch[0] if len(batch) == 1 else batch,
                period="5d", interval="1d", auto_adjust=False,
                progress=False, group_by="ticker", threads=True,
            )
            is_multi = isinstance(raw.columns, pd.MultiIndex)
            for sym in batch:
                try:
                    df = (raw[sym] if is_multi else raw).copy()
                    df.index = pd.to_datetime(df.index)
                    df = df.dropna(subset=["Close"])
                    if len(df) < 2:
                        continue  # need prior close + current session — no fabrication
                    cur, prev = df.iloc[-1], df.iloc[-2]
                    out[sym] = {
                        "last": round(float(cur["Close"]), 4),
                        "open": round(float(cur["Open"]), 4),
                        "prev_close": round(float(prev["Close"]), 4),
                        "session_date": df.index[-1].date().isoformat(),
                        "prev_session_date": df.index[-2].date().isoformat(),
                    }
                except Exception as e:
                    logger.warning("[metron_market_data] intraday extract failed for %s: %s", sym, e)
        except Exception as e:
            logger.warning("[metron_market_data] yfinance intraday batch failed: %s", e)
    logger.info("[metron_market_data] intraday: %d/%d symbols quoted", len(out), len(yf_symbols))
    return out


def _fred_series_history(series_ids: list[str], api_key: str, *, lookback_years: int = 2) -> dict[str, list[tuple[str, float]]]:
    """~``lookback_years`` of daily/monthly observations per FRED series id →
    ``{series_id: [(date, value), …]}`` ascending. stdlib urllib; fail-soft per series."""
    if not api_key:
        logger.warning("[metron_market_data] FRED_API_KEY unset — skipping macro")
        return {}
    import urllib.parse
    import urllib.request
    from datetime import date as _date
    today = datetime.now(timezone.utc).date()
    try:
        start = today.replace(year=today.year - lookback_years).isoformat()
    except ValueError:  # Feb 29
        start = today.replace(year=today.year - lookback_years, day=28).isoformat()
    out: dict[str, list[tuple[str, float]]] = {}
    for sid in series_ids:
        params = urllib.parse.urlencode({"series_id": sid, "api_key": api_key, "file_type": "json",
                                         "sort_order": "asc", "observation_start": start})
        try:
            with urllib.request.urlopen(f"https://api.stlouisfed.org/fred/series/observations?{params}", timeout=15) as resp:
                payload = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("[metron_market_data] FRED fetch failed for %s: %s", sid, e)
            continue
        obs: list[tuple[str, float]] = []
        for row in payload.get("observations", []):
            raw = row.get("value")
            if raw in (None, "", "."):  # FRED's missing-value marker
                continue
            try:
                _date.fromisoformat(row["date"])
                obs.append((row["date"], float(raw)))
            except (ValueError, KeyError):
                continue
        if obs:
            out[sid] = obs
    logger.info("[metron_market_data] macro: %d/%d series fetched", len(out), len(series_ids))
    return out


# ── S3 write (the single put-object site for this file) ──────────────────────


def _write_json(s3_client: Any, bucket: str, key: str, obj: dict) -> None:
    """Write ``obj`` as compact JSON to ``s3://bucket/key``. The ONE put_object site in
    this module — every artifact (dated + latest) routes through here, so the
    artifact-registry coverage guard pins a single count."""
    s3_client.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


# ── Orchestration ────────────────────────────────────────────────────────────


def collect(
    *,
    bucket: str = DEFAULT_BUCKET,
    run_date: str | None = None,
    dry_run: bool = False,
    s3_client: Any = None,
    close_source: CloseSource | None = None,
    fx_source: FxSource | None = None,
) -> dict:
    """Read Metron's held universe → fetch EOD closes + FX → write the two artifacts
    (dated + ``latest``). Returns a status dict. ``close_source``/``fx_source`` inject
    fetchers for tests; ``s3_client`` injects a fake S3."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    holdings, currencies = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}

    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in holdings}
    yf_symbols = sorted(ccy_by_yf)

    fetch_closes = close_source or _yfinance_closes
    fetch_fx = fx_source or _yfinance_fx
    priced = fetch_closes(yf_symbols)
    rates = fetch_fx(currencies)

    closes = {
        yf: {"close": close, "currency": ccy_by_yf.get(yf, "USD"), "bar_date": bar_date}
        for yf, (close, bar_date) in sorted(priced.items())
    }
    closes_artifact = {
        "schema_version": CLOSES_SCHEMA_VERSION, "as_of": run_date,
        "source": "alpha-engine-data", "closes": closes,
    }
    fx_artifact = {
        "schema_version": FX_SCHEMA_VERSION, "as_of": run_date,
        "base": BASE_CURRENCY, "rates": dict(sorted(rates.items())),
    }

    closes_key = f"{CLOSES_PREFIX}{run_date}.json"
    fx_key = f"{FX_PREFIX}{run_date}.json"
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN: %d closes, %d fx (not written)", len(closes), len(rates))
        return {"status": "ok_dry_run", "universe": len(holdings),
                "closes": len(closes), "fx": len(rates)}

    try:
        _write_json(s3_client, bucket, closes_key, closes_artifact)
        _write_json(s3_client, bucket, f"{CLOSES_PREFIX}latest.json", closes_artifact)
        _write_json(s3_client, bucket, fx_key, fx_artifact)
        _write_json(s3_client, bucket, f"{FX_PREFIX}latest.json", fx_artifact)
    except Exception as e:  # fail loud to the phase registry — never a silent producer
        logger.error("[metron_market_data] artifact write failed: %s", e)
        return {"status": "error", "error": str(e)}

    logger.info("[metron_market_data] wrote %d closes + %d fx → s3://%s/%s{,latest}",
                len(closes), len(rates), bucket, CLOSES_PREFIX)
    return {
        "status": "ok", "universe": len(holdings),
        "closes": len(closes), "fx": len(rates),
        "closes_key": closes_key, "fx_key": fx_key,
    }


def collect_history(
    *, bucket: str = DEFAULT_BUCKET, dry_run: bool = False, s3_client: Any = None,
    period: str = DEFAULT_HISTORY_PERIOD,
    close_history_source: CloseHistorySource | None = None,
    fx_history_source: FxHistorySource | None = None,
) -> dict:
    """Write per-symbol close-history + per-currency FX-history artifacts for Metron's
    held universe (Performance NAV reconstruction + as-of-date realized/dividend FX):

        market_data/close_history/{yf_symbol}.json  {schema_version, yf_symbol, currency, closes: [[date, close], …]}
        market_data/fx_history/{CCY}.json           {schema_version, currency, base, rates: [[date, rate], …]}

    Idempotent (full-series overwrite each run). Injectable sources/S3 for tests."""
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, currencies = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}
    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in holdings}
    closes = (close_history_source or _yfinance_close_history)(sorted(ccy_by_yf))
    fx = (fx_history_source or _yfinance_fx_history)(currencies)
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN history: %d close series, %d fx series", len(closes), len(fx))
        return {"status": "ok_dry_run", "close_series": len(closes), "fx_series": len(fx)}
    try:
        for yf_sym, series in sorted(closes.items()):
            _write_json(s3_client, bucket, f"{CLOSE_HISTORY_PREFIX}{yf_sym}.json", {
                "schema_version": CLOSE_HISTORY_SCHEMA_VERSION, "yf_symbol": yf_sym,
                "currency": ccy_by_yf.get(yf_sym, "USD"), "closes": [list(p) for p in series]})
        for ccy, series in sorted(fx.items()):
            _write_json(s3_client, bucket, f"{FX_HISTORY_PREFIX}{ccy}.json", {
                "schema_version": FX_HISTORY_SCHEMA_VERSION, "currency": ccy,
                "base": BASE_CURRENCY, "rates": [list(p) for p in series]})
    except Exception as e:  # fail loud to the phase registry
        logger.error("[metron_market_data] history write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote %d close-history + %d fx-history series", len(closes), len(fx))
    return {"status": "ok", "close_series": len(closes), "fx_series": len(fx)}


def collect_reference(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None, sector_source: SectorSource | None = None,
    benchmark_source: BenchmarkWeightsSource | None = None, earnings_source: EarningsSource | None = None,
) -> dict:
    """Write the GICS-sectors + earnings reference artifacts for Metron's held universe —
    moving Metron's last external (yfinance) fetches to the spine:

        market_data/sectors/latest.json   {schema_version, as_of, sectors: {yf_symbol: gics}, spy_sector_weights: {sector: weight}}
        market_data/earnings/latest.json   {schema_version, as_of, earnings: {yf_symbol: date_iso}}

    Keyed by yf_symbol (consistent with the closes artifact). Injectable sources/S3."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}
    yf_symbols = sorted({h["yf_symbol"] for h in holdings})
    sectors = (sector_source or _yfinance_sectors)(yf_symbols)
    spy_weights = (benchmark_source or _yfinance_spy_weights)()
    earnings = (earnings_source or _yfinance_earnings)(yf_symbols)
    sectors_artifact = {"schema_version": SECTORS_SCHEMA_VERSION, "as_of": run_date,
                        "sectors": dict(sorted(sectors.items())), "spy_sector_weights": dict(sorted(spy_weights.items()))}
    earnings_artifact = {"schema_version": EARNINGS_SCHEMA_VERSION, "as_of": run_date,
                         "earnings": dict(sorted(earnings.items()))}
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN reference: %d sectors, %d spy-weights, %d earnings",
                    len(sectors), len(spy_weights), len(earnings))
        return {"status": "ok_dry_run", "sectors": len(sectors), "earnings": len(earnings)}
    try:
        _write_json(s3_client, bucket, f"{SECTORS_PREFIX}latest.json", sectors_artifact)
        _write_json(s3_client, bucket, f"{EARNINGS_PREFIX}latest.json", earnings_artifact)
    except Exception as e:  # fail loud
        logger.error("[metron_market_data] reference write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote %d sectors + %d spy-weights + %d earnings",
                len(sectors), len(spy_weights), len(earnings))
    return {"status": "ok", "sectors": len(sectors), "spy_weights": len(spy_weights), "earnings": len(earnings)}


def collect_macro(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None, api_key: str | None = None, macro_source: MacroSource | None = None,
) -> dict:
    """Write the macro-indicator artifact for Metron's Macro page — Metron's LAST direct
    external fetch (FRED) moved to the spine:

        market_data/macro/latest.json   {schema_version, as_of, series: {series_id: [[date, value], …]}}

    Fetches the 7 FRED series Metron renders as ~2y observation history. Injectable
    source/S3 for tests; FRED key from ``alpha_engine_lib.secrets`` when not injected."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    if macro_source is None:
        if api_key is None:
            from alpha_engine_lib.secrets import get_secret
            api_key = get_secret("FRED_API_KEY", required=False, default="")
        series = _fred_series_history(METRON_MACRO_SERIES, api_key)
    else:
        series = macro_source(METRON_MACRO_SERIES)
    if not series:
        return {"status": "skipped", "reason": "no macro series (FRED key unset or fetch failed)"}
    artifact = {"schema_version": MACRO_SCHEMA_VERSION, "as_of": run_date,
                "series": {sid: [list(p) for p in obs] for sid, obs in sorted(series.items())}}
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN macro: %d series (not written)", len(series))
        return {"status": "ok_dry_run", "series": len(series)}
    try:
        _write_json(s3_client, bucket, f"{MACRO_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud
        logger.error("[metron_market_data] macro write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote %d macro series", len(series))
    return {"status": "ok", "series": len(series)}


def collect_fundamentals(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None, fundamentals_source: FundamentalsSource | None = None,
) -> dict:
    """Write the tearsheet-fundamentals artifact for Metron's held universe
    (config#1022 — multiples + balance-sheet ratios, consumed by metron-ops#22):

        market_data/fundamentals/latest.json
            {schema_version, as_of, source: "yfinance", fundamentals: {yf_symbol: {info_key: value}}}

    Field values are yfinance ``Ticker.info`` pass-throughs over
    ``FUNDAMENTALS_INFO_KEYS`` — the consumer owns display/unit semantics and pins
    ``schema_version``. Daily cadence (fundamentals move quarterly; daily is plenty).
    Injectable source/S3 for tests."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}
    yf_symbols = sorted({h["yf_symbol"] for h in holdings})
    fundamentals = (fundamentals_source or _yfinance_fundamentals)(yf_symbols)
    artifact = {
        "schema_version": FUNDAMENTALS_SCHEMA_VERSION, "as_of": run_date,
        "source": "yfinance", "fundamentals": dict(sorted(fundamentals.items())),
    }
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN fundamentals: %d symbols (not written)", len(fundamentals))
        return {"status": "ok_dry_run", "fundamentals": len(fundamentals)}
    try:
        _write_json(s3_client, bucket, f"{FUNDAMENTALS_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud to the phase registry
        logger.error("[metron_market_data] fundamentals write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote fundamentals for %d/%d symbols", len(fundamentals), len(yf_symbols))
    return {"status": "ok", "universe": len(holdings), "fundamentals": len(fundamentals)}


# US regular trading hours in UTC, with margin: 13:25 (pre-13:30 open) → 20:10
# (post-20:00 close). EDT-based; in EST (winter) the 14:30–21:00 session shifts an
# hour later, so the window below is widened to cover BOTH offsets — the cost is a
# few harmless extra runs, never a missed session segment.
_RTH_UTC_START = (13, 25)
_RTH_UTC_END = (21, 10)


def in_us_market_window(now: datetime | None = None) -> bool:
    """True on a weekday inside the (DST-widened) US RTH UTC window. NYSE holidays
    are NOT checked here — on a holiday the trading box is the scheduler and the
    quotes are simply unchanged; the artifact's ``session_date`` keeps it honest."""
    now = now or datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    hm = (now.hour, now.minute)
    return _RTH_UTC_START <= hm <= _RTH_UTC_END


def collect_intraday(
    *, bucket: str = DEFAULT_BUCKET, dry_run: bool = False, s3_client: Any = None,
    intraday_source: IntradaySource | None = None, force: bool = False,
    now: datetime | None = None,
) -> dict:
    """Write the intraday-quotes artifact for Metron's held universe (config#1023 —
    the 15-minute Today-view feed, consumed by metron-ops#23):

        market_data/intraday/latest.json
            {schema_version, as_of_utc, source: "yfinance_delayed",
             quotes: {yf_symbol: {last, open, prev_close, session_date, prev_session_date, currency}}}

    ``last`` is ~15-min delayed. Runs every 15 min during US RTH via a systemd timer
    on the trading box (infrastructure/systemd/metron-intraday.timer); outside the
    market window it returns ``skipped`` without fetching (``force`` overrides, for
    manual runs). Single ``latest.json`` key — consumers see staleness via
    ``as_of_utc``. Injectable source/S3 for tests."""
    if not force and not in_us_market_window(now):
        return {"status": "skipped", "reason": "outside US market window"}
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}
    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in holdings}
    quotes = (intraday_source or _yfinance_intraday)(sorted(ccy_by_yf))
    for sym, q in quotes.items():
        q["currency"] = ccy_by_yf.get(sym, "USD")
    artifact = {
        "schema_version": INTRADAY_SCHEMA_VERSION,
        "as_of_utc": (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "yfinance_delayed",
        "quotes": dict(sorted(quotes.items())),
    }
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN intraday: %d quotes (not written)", len(quotes))
        return {"status": "ok_dry_run", "quotes": len(quotes)}
    try:
        _write_json(s3_client, bucket, f"{INTRADAY_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud — the timer unit's journal + freshness scan record it
        logger.error("[metron_market_data] intraday write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote %d intraday quotes", len(quotes))
    return {"status": "ok", "universe": len(holdings), "quotes": len(quotes)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m collectors.metron_market_data", description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--date", default=None, help="run date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--history", action="store_true", help="also write close/FX history artifacts")
    parser.add_argument("--reference", action="store_true", help="also write sectors/earnings artifacts")
    parser.add_argument("--macro", action="store_true", help="also write the macro-indicators artifact")
    parser.add_argument("--fundamentals", action="store_true", help="also write the tearsheet-fundamentals artifact")
    parser.add_argument("--only-intraday", action="store_true",
                        help="write ONLY the intraday-quotes artifact (the 15-min timer entry; "
                             "no-op outside the US market window unless --force)")
    parser.add_argument("--force", action="store_true", help="bypass the market-window gate for --only-intraday")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.only_intraday:
        intra = collect_intraday(bucket=args.bucket, dry_run=args.dry_run, force=args.force)
        logger.info("[metron_market_data] intraday done: %s", intra)
        return 0 if intra.get("status") in ("ok", "ok_dry_run", "skipped") else 1
    result = collect(bucket=args.bucket, run_date=args.date, dry_run=args.dry_run)
    logger.info("[metron_market_data] latest done: %s", result)
    ok = result.get("status") in ("ok", "ok_dry_run", "skipped")
    if args.history:
        hist = collect_history(bucket=args.bucket, dry_run=args.dry_run)
        logger.info("[metron_market_data] history done: %s", hist)
        ok = ok and hist.get("status") in ("ok", "ok_dry_run", "skipped")
    if args.reference:
        ref = collect_reference(bucket=args.bucket, run_date=args.date, dry_run=args.dry_run)
        logger.info("[metron_market_data] reference done: %s", ref)
        ok = ok and ref.get("status") in ("ok", "ok_dry_run", "skipped")
    if args.macro:
        mac = collect_macro(bucket=args.bucket, run_date=args.date, dry_run=args.dry_run)
        logger.info("[metron_market_data] macro done: %s", mac)
        ok = ok and mac.get("status") in ("ok", "ok_dry_run", "skipped")
    if args.fundamentals:
        fund = collect_fundamentals(bucket=args.bucket, run_date=args.date, dry_run=args.dry_run)
        logger.info("[metron_market_data] fundamentals done: %s", fund)
        ok = ok and fund.get("status") in ("ok", "ok_dry_run", "skipped")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
