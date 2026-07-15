"""Metron market-data producer — EOD closes + FX for Metron's held + watchlist universe.

`alpha-engine-data` is the single market-data ground truth for the whole Nous Ergon
system. Metron publishes its held-ticker universe to
``s3://<bucket>/metron/holdings_universe.json`` (yf_symbols + the non-USD currencies it
holds) and its watchlist-only-ticker universe (tracked but never bought,
metron-ops#42/#121) to ``s3://<bucket>/metron/watchlist_universe.json`` (metron-ops#132)
— ``load_metron_universe()`` reads and UNIONS both, so every producer below (and every
Metron per-ticker consumer: fundamentals/technicals/analyst/sentiment/tearsheet) treats a
watched-but-not-held ticker identically to a held one. This producer writes two artifacts
the Metron app consumes — so Metron makes NO direct market-data API calls of its own:

    market_data/eod_closes/{date}.json   + market_data/eod_closes/latest.json
    market_data/fx/{date}.json           + market_data/fx/latest.json
    market_data/fundamentals/latest.json   (daily — tearsheet multiples/ratios)
    market_data/intraday/latest.json       (every 5 min while NYSE open AND Metron in use)

Closes cover the held+watchlist union — including foreign listings (``1299.HK``,
``RMS.PA``), OTC (``GTBIF``), and funds (``FNILX``) that the ~903-name SP1500 constituent
cache refuses. FX covers the held+watchlist non-USD currencies (``{CCY}USD=X``).

Artifact schemas (versioned — Metron's consumer pins on ``schema_version``):

    closes: {schema_version, as_of, source, closes: {yf_symbol: {close, currency, bar_date}}}
    fx:     {schema_version, as_of, base: "USD", rates: {CCY: rate}}
    technicals: {schema_version, as_of, source: "computed", technicals: {yf_symbol:
                 {rsi_14, macd_hist, ma_50, ma_200, pct_to_ma_50, pct_to_ma_200,
                  high_52w, low_52w, pct_in_52w_range, mom_20d, mom_60d}}}   (daily; derived
                 from close_history — no new fetch)
    valuation_medians: {schema_version, as_of, source: "yfinance", by_sector / by_country:
                 {group: {trailing_pe, forward_pe, price_to_book, price_to_sales, ev_ebitda,
                  dividend_yield, n}}}   (weekly; SP1500-broad peer benchmark for Holdings)

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
from datetime import date, datetime, timezone
from typing import Any, Callable

from nousergon_lib.yfinance_quiet import log_yf_coverage, quiet_yfinance, yf_quiet

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
# Metron publishes its held universe here (see metron api/services/data_spine.py).
HOLDINGS_UNIVERSE_KEY = "metron/holdings_universe.json"
# Metron also publishes a WATCHLIST-only-ticker universe (tracked but never bought,
# metron-ops#42/#121) — unioned into load_metron_universe() below so a watchlist-only
# ticker gets the SAME fundamentals/technicals/analyst/sentiment/price-history coverage a
# held position does (metron-ops#132: Brian added MU to his watchlist and it showed no
# data — every per-ticker collector fetches strictly over the held union above, so a
# never-held ticker was never in scope for any of them).
WATCHLIST_UNIVERSE_KEY = "metron/watchlist_universe.json"
CLOSES_PREFIX = "market_data/eod_closes/"
FX_PREFIX = "market_data/fx/"
# History artifacts (per-symbol / per-currency) — power Metron's Performance NAV
# reconstruction (close series) + as-of-date realized/dividend FX conversion.
CLOSE_HISTORY_PREFIX = "market_data/close_history/"
FX_HISTORY_PREFIX = "market_data/fx_history/"
# Factor/sector ETFs whose close-history Metron's RISK (factor model) + ATTRIBUTION
# (Brinson sector) need. These are NOT in any held universe, so unless their history is
# published here the spine has no close_history for them and risk/attribution can NEVER
# compute — metron-ops#43: the daily refresh logged `risk=False, attribution=False` with
# `NoSuchKey` reading market_data/close_history/{XLP,XLV,...}.json. Mirrors metron
# api/services/risk.py (SPY + STYLE_ETF) + portfolio_analytics.sectors.SECTOR_ETF; all USD.
RISK_FACTOR_ETFS = [
    "SPY", "MTUM", "QUAL", "USMV", "VLUE", "SIZE",  # market + iShares MSCI USA style factors
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",  # GICS sector SPDRs
]
# Tracking-proxy ETFs for late-striking mutual funds (Metron fund-NAV estimate). A mutual
# fund's NAV strikes hours after the EOD run, so its same-day move is estimated from a proxy
# ETF tracking the same exposure (metron api/services/fund_proxy.py: FNILX→SPY large-cap,
# FZILX/FTIHX→IXUS total-intl-ex-US). The FULL distinct proxy set — these are NOT held, so
# their close_history (reconcile fallback) + a dedicated intraday `fund_proxies` quote map
# (the same-day estimate) must be published here or the spine has no proxy data. SPY's
# close_history already comes via RISK_FACTOR_ETFS; the union just guarantees coverage and
# gives the live estimate a single source for every proxy. All USD.
FUND_PROXY_ETFS = ["SPY", "IXUS"]
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
# v2 (metron Holdings metrics): added priceToBook + priceToSalesTrailing12Months so the
# Holdings table can show P/B and P/S alongside the existing P/E family.
# v3 (metron Holdings balance-sheet band): added the absolute balance-sheet fields
# (totalDebt / totalCash / ebitda / freeCashflow) the Holdings "Balance Sheet" columns
# need — cash balance, debt balance, net debt, and net-debt/EBITDA leverage.
# v4 (metron-ops#163): added trailingEps/forwardEps so the Holdings Valuation band can
# show raw EPS alongside P/E, not just the ratio.
# v5 (metron-ops#178): added bookValue/revenuePerShare/enterpriseValue — every Valuation
# multiple now has its raw input(s) in the same band (P/B -> book value/share, P/S ->
# revenue/share, EV/EBITDA -> enterprise value).
FUNDAMENTALS_INFO_KEYS = [
    "trailingPE", "forwardPE", "trailingPegRatio", "enterpriseToEbitda",
    "priceToBook", "priceToSalesTrailing12Months",
    "earningsGrowth", "revenueGrowth", "debtToEquity", "currentRatio", "quickRatio",
    "returnOnEquity", "returnOnAssets", "grossMargins", "operatingMargins",
    "totalDebt", "totalCash", "ebitda", "freeCashflow",
    "beta", "dividendYield", "marketCap", "sector", "industry",
    "trailingEps", "forwardEps",
    "bookValue", "revenuePerShare", "enterpriseValue",
]
# Technicals — per-held-symbol indicators computed from the close_history this module
# already publishes daily (zero new fetches). Slow-moving (RSI14 / 50d-200d MA / 52w
# range / momentum) so a daily refresh off the close series is plenty.
TECHNICALS_PREFIX = "market_data/technicals/"
# Valuation medians — SP1500-broad sector & country median multiples, the peer benchmark
# the Holdings "by sector → country" view bands each holding against. A small DERIVED
# aggregate (medians + member counts, no per-ticker rows). Weekly cadence (multiples move
# quarterly; the median of ~900 names is stable week to week).
VALUATION_MEDIANS_PREFIX = "market_data/valuation_medians/"
# Analyst consensus — per-held-symbol consensus rating + price targets + #analysts, the
# "consensus research" inputs to the Holdings Sentiment/Consensus band + the per-holding
# attractiveness score (metron-ops#105). FREE sources only (yfinance recommendationKey +
# targetMean/MedianPrice + numberOfAnalystOpinions, optionally Finnhub rating buckets via
# the canonical analyst_sources adapters). Forward consensus EPS/revenue ESTIMATES are a
# PAID feed (IBES/Visible Alpha) — NOT emitted here; the consumer scaffolds those columns
# as "N/A · paid feed" and they populate when metron-ops#107 wires the paid source.
ANALYST_PREFIX = "market_data/analyst/"
# News sentiment — per-held-symbol latest LM (Loughran-McDonald) sentiment + event
# rollup, the "sentiment" input to the Holdings Sentiment/Consensus band + attractiveness
# score (metron-ops#105). A JSON PROJECTION of the held-universe latest slice of the
# upstream `data/news_aggregates_daily/` parquet (per-(ticker,date) time series — correctly
# parquet for its analytical role). Projecting to JSON here keeps Metron's spine readers
# uniform (no pyarrow in the API) — right format at each layer.
SENTIMENT_PREFIX = "market_data/sentiment/"
# yfinance Ticker.info keys the valuation-medians pass reads per universe symbol. Mirrors
# the multiple subset of FUNDAMENTALS_INFO_KEYS (same source + semantics as the per-holding
# fundamentals → the band and the row are directly comparable) plus sector/country to group.
VALUATION_MEDIAN_KEYS = [
    "trailingPE", "forwardPE", "priceToBook",
    "priceToSalesTrailing12Months", "enterpriseToEbitda", "dividendYield",
]
# Intraday — last/open/prior-close per held symbol, refreshed every 15 min during US
# regular trading hours by a systemd timer on the trading box (config#1023). Single
# `latest.json` key (no dated files — 26 writes/day would litter the prefix). The same
# artifact also carries the major-index proxies (see INDEX_PROXY_SYMBOLS).
INTRADAY_PREFIX = "market_data/intraday/"
# Major US-index ETF proxies for Metron's Overview "markets" strip — refreshed in the
# same intraday run (market context, fetched independent of the held universe). The
# index VALUES (^GSPC / ^IXIC / ^NDX / ^RUT) carry a separate index license; the tradeable
# ETF prices are ordinary equity trades, so the ETF is published as the index proxy and
# Metron maps each symbol to the index it tracks. ONEQ (Nasdaq Composite) and QQQ
# (Nasdaq-100) are both carried because the broad Composite and the mega-cap-100 routinely
# diverge intraday — the headline "Nasdaq" most readers see is the Composite. Quotes land
# under the artifact's `indices` key (same per-symbol shape as held `quotes`).
INDEX_PROXY_SYMBOLS = ["SPY", "ONEQ", "QQQ", "IWM"]
CLOSES_SCHEMA_VERSION = 1
FX_SCHEMA_VERSION = 1
CLOSE_HISTORY_SCHEMA_VERSION = 1
# Additive per-artifact provenance field (config#1865) documenting close_history's price
# basis now that it's primarily sourced from the dividend-adjusted price_cache rather than
# an independent split-only-adjusted yfinance fetch — see _price_cache_close_history's
# docstring for the full basis-change rationale. Old consumers pinned only on
# schema_version/yf_symbol/currency/closes ignore the extra key (no shape break).
CLOSE_HISTORY_ADJUSTMENT_BASIS = "dividend_adjusted"
FX_HISTORY_SCHEMA_VERSION = 1
SECTORS_SCHEMA_VERSION = 2  # v2: additive `countries` map (yf_symbol → country of domicile)
EARNINGS_SCHEMA_VERSION = 1
MACRO_SCHEMA_VERSION = 2  # v2: added next_release (per series) + release_events (metron-ops#49)
FUNDAMENTALS_SCHEMA_VERSION = 5  # v5: + bookValue/revenuePerShare/enterpriseValue (metron-ops#178)
INTRADAY_SCHEMA_VERSION = 3  # v3: additive `fund_proxies` map (mutual-fund tracking-proxy ETF quotes)
TECHNICALS_SCHEMA_VERSION = 2  # v2: + pct_from_52wk_high (tearsheet parity with Holdings)
SECURITY_PERFORMANCE_SCHEMA_VERSION = 1
SECURITY_PERFORMANCE_PREFIX = "market_data/security_performance/"
VALUATION_MEDIANS_SCHEMA_VERSION = 1
ANALYST_SCHEMA_VERSION = 1  # consensus rating + price targets + #analysts (free sources)
SENTIMENT_SCHEMA_VERSION = 1  # LM news sentiment + event rollup (held-universe latest slice)
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
# Reference sources: sectors maps yf_symbols → {yf_symbol: gics}; countries maps
# yf_symbols → {yf_symbol: country-of-domicile}; benchmark weights is a
# 0-arg → {sector: weight}; earnings maps yf_symbols → {yf_symbol: date_iso}.
SectorSource = Callable[[list[str]], dict[str, str]]
CountrySource = Callable[[list[str]], dict[str, str]]
BenchmarkWeightsSource = Callable[[], dict[str, float]]
EarningsSource = Callable[[list[str]], dict[str, str]]
# A macro source maps FRED series ids → {series_id: [(date_iso, value), …]} ascending.
MacroSource = Callable[[list[str]], dict[str, list[tuple[str, float]]]]
# A fundamentals source maps yf_symbols → {yf_symbol: {info_key: value}}.
FundamentalsSource = Callable[[list[str]], dict[str, dict]]
# An analyst source maps yf_symbols → {yf_symbol: {consensus_rating, mean_target, …}}.
AnalystSource = Callable[[list[str]], dict[str, dict]]
# A sentiment source maps yf_symbols → {yf_symbol: {sentiment, n_articles, …}}.
SentimentSource = Callable[[list[str]], dict[str, dict]]
# An intraday source maps yf_symbols → {yf_symbol: quote dict} (see _yfinance_intraday).
IntradaySource = Callable[[list[str]], dict[str, dict]]
# A valuation source maps yf_symbols → {yf_symbol: {multiple_key|sector|country: value}}
# (the multiples + classification the medians pass groups on; see _yfinance_valuation).
ValuationSource = Callable[[list[str]], dict[str, dict]]
# A medians universe source returns the broad (SP1500 ∪ held) yf_symbol list to benchmark.
MediansUniverseSource = Callable[[], list[str]]


# ── Universe read ───────────────────────────────────────────────────────────


def _read_metron_universe_holdings(bucket: str, s3_client: Any, key: str) -> list[dict]:
    """Read one Metron universe artifact's ``holdings`` list. Fail-soft per artifact: a
    missing object / no creds / parse error contributes nothing (logged) rather than
    aborting the caller's union."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read())
        return [
            {"yf_symbol": str(h["yf_symbol"]).strip(), "currency": str(h.get("currency", "USD")).strip()}
            for h in data.get("holdings", [])
            if str(h.get("yf_symbol", "")).strip()
        ]
    except Exception as e:  # missing object, no creds, parse error, etc.
        logger.warning("[metron_market_data] %s unavailable (%s) — no contribution", key, e)
        return []


def load_metron_universe(bucket: str, s3_client: Any) -> tuple[list[dict], list[str]]:
    """Read Metron's published universe → ``(holdings, currencies)`` — the UNION of the
    held-ticker universe and the watchlist-only-ticker universe (metron-ops#132), so a
    ticker Brian is only watching (never bought) gets the same per-ticker coverage a held
    position does everywhere this feeds: fundamentals/technicals/analyst/sentiment,
    close/FX history, and the valuation-medians benchmark set.

    ``holdings`` = ``[{"yf_symbol", "currency"}, …]``, deduped by yf_symbol — a symbol in
    BOTH universes keeps the held-universe currency (it's the economically authoritative
    one for a real position). ``currencies`` = distinct non-USD currencies across both.
    Fail-soft per artifact (see ``_read_metron_universe_holdings``): either missing / no
    creds / parse error contributes nothing rather than aborting the daily run."""
    watched = _read_metron_universe_holdings(bucket, s3_client, WATCHLIST_UNIVERSE_KEY)
    held = _read_metron_universe_holdings(bucket, s3_client, HOLDINGS_UNIVERSE_KEY)
    by_yf: dict[str, str] = {h["yf_symbol"]: h["currency"] for h in watched}
    by_yf.update({h["yf_symbol"]: h["currency"] for h in held})  # held wins on conflict
    holdings = [{"yf_symbol": yf, "currency": ccy} for yf, ccy in sorted(by_yf.items())]
    currencies = sorted({ccy for ccy in by_yf.values() if ccy and ccy != "USD"})
    watchlist_only = {h["yf_symbol"] for h in watched} - {h["yf_symbol"] for h in held}
    logger.info(
        "[metron_market_data] universe: %d instruments (%d held, %d watchlist-only), %d non-USD currencies",
        len(holdings), len(held), len(watchlist_only), len(currencies),
    )
    return holdings, currencies


def _load_sp1500_symbols(bucket: str) -> set[str]:
    """S&P 500 + 400 yf_symbols from the fleet constituents artifact. Fail-soft → ∅."""
    symbols: set[str] = set()
    try:
        from collectors import constituents

        art = constituents.load_from_s3(bucket)
        for t in (art or {}).get("tickers", []) or []:
            if str(t).strip():
                symbols.add(str(t).strip())
    except Exception as e:
        logger.warning("[metron_market_data] constituents universe unavailable (%s)", e)
    return symbols


def load_price_derived_universe(bucket: str, s3_client: Any) -> tuple[list[dict], list[str]]:
    """Universe for price-derived spine artifacts: **SP1500 ∪ Metron held ∪ watchlist**.

    One daily pass covers the research scanner universe plus Metron's foreign/OTC/fund
    delta so overlap tickers (AAPL, MU, …) share a single close_history + derived-metrics
    spine. SP1500-only symbols default to USD; held-universe currency wins on conflict.
    Fundamentals/analyst/sentiment stay on ``load_metron_universe()`` (held-scoped)."""
    metron_holdings, _ = load_metron_universe(bucket, s3_client)
    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in metron_holdings}
    sp1500 = _load_sp1500_symbols(bucket)
    all_yf = sorted(sp1500 | set(ccy_by_yf))
    if not all_yf:
        return [], []
    holdings = [{"yf_symbol": yf, "currency": ccy_by_yf.get(yf, "USD")} for yf in all_yf]
    currencies = sorted({
        ccy for yf in all_yf for ccy in [ccy_by_yf.get(yf, "USD")] if ccy and ccy != "USD"
    })
    metron_only = set(ccy_by_yf) - sp1500
    logger.info(
        "[metron_market_data] price-derived universe: %d symbols (%d SP1500, %d metron-only)",
        len(holdings), len(sp1500), len(metron_only),
    )
    return holdings, currencies


# ── yfinance fetchers (default sources) ─────────────────────────────────────
#
# The yfinance log-noise chokepoint (quiet_yfinance / yf_quiet / log_yf_coverage)
# now lives in the cross-repo source of truth nousergon_lib.yfinance_quiet (krepis),
# lifted out of the former in-repo collectors/yfinance_quiet.py once the same bug
# class recurred through collectors/prices.py (2026-06-19, config#1029 follow-up;
# config#1161). The underscored names below are kept as thin backward-compat
# aliases so existing call sites + tests read unchanged.

_quiet_yfinance = quiet_yfinance
_yf_quiet = yf_quiet

# Metron-specific coverage context: many holdings are non-listed instruments
# (e.g. 401(k) CITs) yfinance can never price — they're broker-snapshot-priced
# and belong out of the published universe (config#1029).
_METRON_COVERAGE_NOTE = (
    "non-listed instruments (e.g. 401(k) CITs) are broker-snapshot-priced in "
    "Metron and belong out of the published universe (config#1029)"
)


def _log_yf_coverage(
    kind: str,
    requested: list[str],
    covered: dict | set,
    *,
    error_on_empty: bool = False,
) -> None:
    """Metron wrapper over :func:`nousergon_lib.yfinance_quiet.log_yf_coverage` (config#1029)."""
    log_yf_coverage(
        logger, kind, requested, covered,
        error_on_empty=error_on_empty, note=_METRON_COVERAGE_NOTE,
    )


@_yf_quiet
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
    _log_yf_coverage("closes", yf_symbols, out, error_on_empty=True)
    return out


@_yf_quiet
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
    _log_yf_coverage("fx", list(pairs.values()), out)
    return out


@_yf_quiet
def _yf_history(
    symbols: list[str], period: str, *, is_fx: bool = False, base: str = BASE_CURRENCY,
    auto_adjust: bool = False,
) -> dict[str, list[tuple[str, float]]]:
    """Daily close series per symbol via yfinance over ``period`` →
    ``{key: [(bar_date, close), …]}`` ascending. ``is_fx`` maps a currency to the
    ``{CCY}{BASE}=X`` pair and keys the result by the bare currency. Empty series omitted.
    ``auto_adjust`` selects the basis (config#1865): ``False`` (default) is split-adjusted-
    only; ``True`` is dividend-adjusted, matching the price_cache basis (see
    ``_yfinance_close_history_dividend_adjusted``, the gap-fill fallback used by
    ``_price_cache_close_history``)."""
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
                              interval="1d", auto_adjust=auto_adjust, progress=False, group_by="ticker", threads=True)
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
    _log_yf_coverage("fx_history" if is_fx else "close_history", list(targets.values()), out)
    return out


def _yfinance_close_history(yf_symbols: list[str], period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, list[tuple[str, float]]]:
    """Legacy split-only-adjusted (``auto_adjust=False``) yfinance fetch. No longer
    ``collect_history``'s default source (config#1865: superseded by
    ``_price_cache_close_history``, which reads the dividend-adjusted price_cache and
    gap-fills via ``_yfinance_close_history_dividend_adjusted``) — kept for direct callers/
    tests that want the pre-#1865 split-only basis explicitly."""
    return _yf_history(yf_symbols, period, is_fx=False)


def _yfinance_close_history_dividend_adjusted(
    yf_symbols: list[str], period: str = DEFAULT_HISTORY_PERIOD,
) -> dict[str, list[tuple[str, float]]]:
    """Dividend-adjusted (``auto_adjust=True``) yfinance fetch — same basis as
    ``reference/price_cache/`` (see ``collectors/prices.py``). Used by
    ``_price_cache_close_history`` to gap-fill symbols price_cache doesn't cover, so a
    published close_history series is never a split-only/dividend-adjusted chimera
    (config#1865)."""
    return _yf_history(yf_symbols, period, is_fx=False, auto_adjust=True)


def _period_to_timedelta(period: str) -> Any:
    """Best-effort ``"10y"``/``"5d"``/``"6mo"``-style yfinance period string → ``pd.Timedelta``.
    Unrecognized shapes fall back to the ``DEFAULT_HISTORY_PERIOD`` (10y) window rather than
    raising — a period-string typo should degrade to "trim less aggressively", never abort
    the price_cache read."""
    import pandas as pd

    try:
        if period.endswith("mo"):
            return pd.Timedelta(days=30 * int(period[:-2]))
        if period.endswith("y"):
            return pd.Timedelta(days=365 * int(period[:-1]))
        if period.endswith("d"):
            return pd.Timedelta(days=int(period[:-1]))
    except (ValueError, AttributeError):
        pass
    return pd.Timedelta(days=365 * 10)


# config#1865-followup (Brian ruling, 2026-07-15 triage): reference/price_cache/ only
# refreshes weekly (collectors/prices.py), so "the parquet exists" is NOT the same claim
# as "the parquet is current" — a covered symbol's cached bar can be several trading days
# stale relative to the daily-fresh yfinance fetch it replaced. Concrete regression this
# guards against: CRWD's cached snapshot was 4 trading days stale and missed a +12.58%
# earnings move, producing a -19.9pp security_performance diff with nothing to do with the
# dividend-adjustment basis change. A symbol whose latest cached bar is stale beyond this
# many trading days is treated as NOT covered by price_cache for this run and routed
# through the SAME yfinance gap-fill path as a genuine no-coverage symbol, rather than
# silently publishing a week-old price. Start conservative at 1 (tolerate the ordinary T+0/
# T+1 refresh-timing lag, nothing more); revisit only with an explicit written rationale.
PRICE_CACHE_MAX_STALE_TRADING_DAYS = 1


def _price_cache_close_series(
    s3_client: Any, bucket: str, yf_symbol: str, period: str, *, reference_day: date | None = None,
) -> list[tuple[str, float]] | None:
    """Read one symbol's dividend-adjusted close series from the price_cache parquet
    (``reference/price_cache/{ticker}.parquet`` — ``builders/_price_cache_writeboth.py``
    owns the read-prefix fallback chain), trimmed to ``period`` lookback. Returns ``None``
    when the parquet is absent in every active read prefix (genuine no-coverage — e.g. a
    foreign/OTC/fund symbol outside price_cache's SP1500-overlap universe), unreadable/
    malformed, OR when the cached snapshot's latest bar is more than
    ``PRICE_CACHE_MAX_STALE_TRADING_DAYS`` NYSE trading sessions behind ``reference_day``
    (config#1865-followup — see the constant's docstring for the CRWD regression this
    closes). All three cases route the caller to the yfinance gap-fill fallback rather than
    aborting the run or silently publishing a stale price (module posture: best-effort, one
    symbol's irregularity never blocks the rest). ``reference_day`` defaults to
    ``nousergon_lib.dates.last_closed_trading_day()`` — injectable for tests/determinism."""
    from botocore.exceptions import ClientError

    from builders._price_cache_writeboth import PRICE_CACHE_LEGACY_PREFIX, price_cache_read_prefixes
    from nousergon_lib.dates import is_fresh_in_trading_days, last_closed_trading_day, trading_days_stale
    from store.parquet_loader import load_parquet_from_s3

    df = None
    for prefix in price_cache_read_prefixes(PRICE_CACHE_LEGACY_PREFIX):
        key = f"{prefix}{yf_symbol}.parquet"
        try:
            df = load_parquet_from_s3(s3_client, bucket, key)
            break
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                continue
            logger.warning("[metron_market_data] price_cache read failed for %s (%s): %s", yf_symbol, key, exc)
            return None
        except Exception as e:
            logger.warning("[metron_market_data] price_cache parse failed for %s (%s): %s", yf_symbol, key, e)
            return None

    if df is None or df.empty or "Close" not in df.columns:
        return None

    import pandas as pd

    cutoff = pd.Timestamp.now(tz="UTC").tz_localize(None) - _period_to_timedelta(period)
    trimmed = df.loc[df.index >= cutoff, "Close"].dropna()
    if trimmed.empty:
        return None

    latest_date = trimmed.index.max().date()
    ref_day = reference_day or last_closed_trading_day()
    if not is_fresh_in_trading_days(latest_date, ref_day, max_stale=PRICE_CACHE_MAX_STALE_TRADING_DAYS):
        logger.info(
            "[metron_market_data] price_cache stale for %s: latest cached bar %s is %d "
            "trading day(s) behind %s (max=%d) — routing to yfinance gap-fill instead of "
            "publishing a stale snapshot",
            yf_symbol, latest_date.isoformat(), trading_days_stale(latest_date, ref_day),
            ref_day.isoformat(), PRICE_CACHE_MAX_STALE_TRADING_DAYS,
        )
        return None

    return [(d.date().isoformat(), round(float(c), 6)) for d, c in trimmed.items()]


def _price_cache_close_history(
    s3_client: Any, bucket: str, period: str = DEFAULT_HISTORY_PERIOD, *, reference_day: date | None = None,
) -> "CloseHistorySource":
    """Build ``collect_history``'s default ``close_history_source`` (config#1865): prefer
    the already-refreshed ``reference/price_cache/`` parquet (``collectors/prices.py``'s
    weekly ~903-symbol SP1500-overlap refresh) over an independent yfinance fetch, cutting
    the duplicate yfinance fan-out ``collect_history`` used to make for every symbol
    price_cache already covers. yfinance is only called for the gap: symbols price_cache
    doesn't carry at all, AND (config#1865-followup) symbols price_cache carries but whose
    cached bar has gone stale (see ``PRICE_CACHE_MAX_STALE_TRADING_DAYS``) — both are
    "not usably covered this run" and share one fallback path.

    Basis change (Operator decision 2026-07-08, config#1865, resolving the ask in
    https://github.com/nousergon/alpha-engine-config/issues/1865#issuecomment-4912376677):
    price_cache is fetched ``auto_adjust=True`` (dividend-adjusted Close). Pre-#1865,
    close_history was independently fetched ``auto_adjust=False`` (split-adjusted only,
    NOT dividend-adjusted — see the now-superseded ``_yfinance_close_history``). Sourcing
    from price_cache means close_history is now dividend-adjusted — a deliberate,
    documented basis change, not a silent one: downstream consumers (security_performance /
    risk / attribution) will see return/YTD/volatility/drawdown figures shift on
    dividend-paying names. The gap-fill fallback uses
    ``_yfinance_close_history_dividend_adjusted`` (``auto_adjust=True``) — the SAME basis —
    so a published close_history series is never a split-only/dividend-adjusted chimera.

    Staleness fallback (config#1865-followup, Brian ruling 2026-07-15): price_cache's
    weekly refresh means a "covered" symbol can silently carry up to a week-old close —
    unlike the daily-fresh yfinance fetch it replaced. Every symbol's cached bar is checked
    against ``reference_day`` (defaults to ``nousergon_lib.dates.last_closed_trading_day()``,
    computed once per call so every symbol in the run is judged against the same session —
    injectable for tests) and routed to the yfinance gap-fill fallback when stale beyond
    ``PRICE_CACHE_MAX_STALE_TRADING_DAYS`` trading days, rather than accepting a stale
    price_cache value with no signal that it happened."""
    from nousergon_lib.dates import last_closed_trading_day

    ref_day = reference_day or last_closed_trading_day()

    def _source(yf_symbols: list[str]) -> dict[str, list[tuple[str, float]]]:
        from_cache: dict[str, list[tuple[str, float]]] = {}
        gaps: list[str] = []
        for sym in yf_symbols:
            series = _price_cache_close_series(s3_client, bucket, sym, period, reference_day=ref_day)
            if series:
                from_cache[sym] = series
            else:
                gaps.append(sym)
        gap_filled = _yfinance_close_history_dividend_adjusted(gaps, period) if gaps else {}
        logger.info(
            "[metron_market_data] close_history: %d/%d symbols from price_cache, "
            "%d yfinance gap-fill (dividend-adjusted basis, config#1865; includes "
            "staleness-triggered fallback beyond %d trading day(s), config#1865-followup)",
            len(from_cache), len(yf_symbols), len(gap_filled), PRICE_CACHE_MAX_STALE_TRADING_DAYS,
        )
        return {**from_cache, **gap_filled}

    return _source


def _yfinance_fx_history(currencies: list[str], period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, list[tuple[str, float]]]:
    return _yf_history(currencies, period, is_fx=True)


@_yf_quiet
def _yfinance_classification(yf_symbols: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """Canonical GICS sector AND country-of-domicile per held symbol from a SINGLE
    ``yf.Ticker(sym).info`` pass (``info['sector']`` + ``info['country']``) — country is
    a zero-extra-API-cost addition to the existing sector fetch. Returns
    ``(sectors, countries)``. Fail-soft per symbol: an unclassifiable symbol is omitted
    from the respective map (Metron shows a coverage gap, never a guessed value)."""
    try:
        import yfinance as yf
    except ImportError:  # pragma: no cover
        return {}, {}
    sectors: dict[str, str] = {}
    countries: dict[str, str] = {}
    for sym in yf_symbols:
        try:
            info = yf.Ticker(sym).info or {}
            sector = info.get("sector")
            if sector:
                sectors[sym] = str(sector)
            country = info.get("country")
            if country:
                countries[sym] = str(country)
        except Exception as e:
            logger.warning("[metron_market_data] classification fetch failed for %s: %s", sym, e)
    logger.info("[metron_market_data] sectors: %d/%d classified, countries: %d/%d domiciled",
                len(sectors), len(yf_symbols), len(countries), len(yf_symbols))
    _log_yf_coverage("sectors", yf_symbols, sectors)
    _log_yf_coverage("countries", yf_symbols, countries)
    return sectors, countries


@_yf_quiet
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


@_yf_quiet
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
    _log_yf_coverage("earnings", yf_symbols, out)
    return out


@_yf_quiet
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
    _log_yf_coverage("fundamentals", yf_symbols, out)
    return out


# Consensus-rating ladder → a signed numeric score in [-1, +1] so the consumer can
# average / band it without re-deriving the ordering. strongBuy=+1 … strongSell=-1.
_RATING_SCORE = {
    "strongBuy": 1.0, "buy": 0.5, "hold": 0.0, "sell": -0.5, "strongSell": -1.0,
}


@_yf_quiet
def _yfinance_analyst(yf_symbols: list[str]) -> dict[str, dict]:
    """Consensus research per held symbol via the canonical free analyst adapter(s)
    → ``{yf_symbol: {consensus_rating, rating_score, mean_target, median_target,
    num_analysts}}``.

    FREE sources only: ``YfinanceAnalystAdapter`` (recommendationKey + target
    mean/median + #analysts) is primary; if ``FINNHUB_API_KEY`` is set, the
    ``FinnhubAnalystAdapter`` rating buckets backfill a missing consensus_rating
    (Finnhub's price target is paid-tier, so targets stay from yfinance). Forward
    consensus EPS/revenue estimates are a PAID feed and are intentionally NOT
    fetched here. Fail-soft per symbol; a symbol with no resolvable snapshot is
    omitted (coverage gap, not zeros)."""
    from collectors.analyst_sources import YfinanceAnalystAdapter
    from nousergon_lib.secrets import get_secret  # secrets via the lib, never os.environ
    # (nousergon_lib, not nousergon_lib: this repo pins a pre-rename lib (<0.60.0)
    # where only the nousergon_lib name exists — matches finnhub_client.py.)

    yf_adapter = YfinanceAnalystAdapter()
    finnhub_adapter = None
    if get_secret("FINNHUB_API_KEY", required=False, default=""):
        try:
            from collectors.analyst_sources import FinnhubAnalystAdapter
            finnhub_adapter = FinnhubAnalystAdapter()
        except Exception as e:  # pragma: no cover - optional backfill
            logger.warning("[metron_market_data] Finnhub analyst adapter unavailable: %s", e)

    out: dict[str, dict] = {}
    for sym in yf_symbols:
        snap = yf_adapter.fetch(sym)
        rating = snap.consensus_rating if snap else None
        # Finnhub backfills ONLY a missing rating (targets stay from yfinance).
        if rating is None and finnhub_adapter is not None:
            fh = finnhub_adapter.fetch(sym)
            if fh is not None and fh.consensus_rating is not None:
                rating = fh.consensus_rating
                if snap is None:
                    snap = fh
        if snap is None:
            continue
        fields = {
            "consensus_rating": rating,
            "rating_score": _RATING_SCORE.get(rating) if rating else None,
            "mean_target": snap.mean_target,
            "median_target": snap.median_target,
            "num_analysts": snap.num_analysts,
        }
        # Omit a symbol that resolved to an empty snapshot (all None) — coverage gap.
        if any(v is not None for v in fields.values()):
            out[sym] = {k: v for k, v in fields.items() if v is not None}
    logger.info("[metron_market_data] analyst: %d/%d symbols covered", len(out), len(yf_symbols))
    _log_yf_coverage("analyst", yf_symbols, out)
    return out


@_yf_quiet
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
    _log_yf_coverage("intraday", yf_symbols, out)
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


def _read_json(s3_client: Any, bucket: str, key: str) -> dict | None:
    """Read+parse ``s3://bucket/key`` → dict, or ``None`` on any miss (NoSuchKey, parse
    error, no creds). Fail-soft so a derived collector (technicals reads back close_history;
    medians reads back the universe) degrades to a coverage gap rather than aborting."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


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
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()
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

        market_data/close_history/{yf_symbol}.json  {schema_version, yf_symbol, currency,
            adjustment_basis, closes: [[date, close], …]}
        market_data/fx_history/{CCY}.json           {schema_version, currency, base, rates: [[date, rate], …]}

    close_history basis (config#1865): sourced primarily from ``reference/price_cache/``
    (dividend-adjusted — ``auto_adjust=True``), gap-filled via yfinance for symbols
    price_cache doesn't cover, OR whose cached bar has gone stale beyond
    ``PRICE_CACHE_MAX_STALE_TRADING_DAYS`` trading days (config#1865-followup — price_cache
    only refreshes weekly, so "covered" alone is not "current"; a stale symbol is routed to
    the same gap-fill path as a genuinely uncovered one rather than silently publishing a
    week-old close). Both use the same dividend-adjusted basis, so the series is never a
    split-only/dividend-adjusted chimera. This dedups the independent yfinance fetch
    ``collect_history`` used to make for every symbol against the SP1500-overlap universe
    ``collectors/prices.py`` already refreshes weekly. Pre-#1865 this was an independent
    yfinance fetch at ``auto_adjust=False`` (split-only) — see
    ``_price_cache_close_history``'s docstring for the full basis-change and
    staleness-fallback rationale (Operator decision 2026-07-08 + Brian ruling 2026-07-15).

    Idempotent (full-series overwrite each run). Injectable sources/S3 for tests."""
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, currencies = load_price_derived_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty price-derived universe", "universe": 0}
    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in holdings}
    # Held symbols + the factor/sector ETFs Metron's risk/attribution need (metron-ops#43),
    # + the major-index ETF proxies (so the Overview markets strip resolves YTD/LTM for
    # QQQ/IWM/ONEQ — not just SPY, which only worked because it's in RISK_FACTOR_ETFS),
    # + the fund-proxy ETFs (close_history backstop for the late-fund-NAV reconcile). All
    # USD, independent of holdings — without them the spine has no close_history for these.
    hist_symbols = sorted(
        set(ccy_by_yf) | set(RISK_FACTOR_ETFS) | set(INDEX_PROXY_SYMBOLS) | set(FUND_PROXY_ETFS)
    )
    closes = (close_history_source or _price_cache_close_history(s3_client, bucket, period))(hist_symbols)
    fx = (fx_history_source or _yfinance_fx_history)(currencies)
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN history: %d close series, %d fx series", len(closes), len(fx))
        return {"status": "ok_dry_run", "close_series": len(closes), "fx_series": len(fx)}
    try:
        for yf_sym, series in sorted(closes.items()):
            _write_json(s3_client, bucket, f"{CLOSE_HISTORY_PREFIX}{yf_sym}.json", {
                "schema_version": CLOSE_HISTORY_SCHEMA_VERSION, "yf_symbol": yf_sym,
                "currency": ccy_by_yf.get(yf_sym, "USD"),
                "adjustment_basis": CLOSE_HISTORY_ADJUSTMENT_BASIS,
                "closes": [list(p) for p in series]})
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
    country_source: CountrySource | None = None,
    benchmark_source: BenchmarkWeightsSource | None = None, earnings_source: EarningsSource | None = None,
) -> dict:
    """Write the GICS-sectors + earnings reference artifacts for Metron's held universe —
    moving Metron's last external (yfinance) fetches to the spine:

        market_data/sectors/latest.json   {schema_version, as_of, sectors: {yf_symbol: gics}, countries: {yf_symbol: country}, spy_sector_weights: {sector: weight}}
        market_data/earnings/latest.json   {schema_version, as_of, earnings: {yf_symbol: date_iso}}

    Keyed by yf_symbol (consistent with the closes artifact). Injectable sources/S3.
    Sector + country share a single ``.info`` pass when neither is injected."""
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}
    yf_symbols = sorted({h["yf_symbol"] for h in holdings})
    # Sector + country come from one ``.info`` pass; an injected source overrides its
    # dimension (tests), and the shared fetch only runs if a real fetch is still needed.
    fetched_sectors: dict[str, str] | None = None
    fetched_countries: dict[str, str] | None = None
    if sector_source is None or country_source is None:
        fetched_sectors, fetched_countries = _yfinance_classification(yf_symbols)
    sectors = sector_source(yf_symbols) if sector_source else (fetched_sectors or {})
    countries = country_source(yf_symbols) if country_source else (fetched_countries or {})
    spy_weights = (benchmark_source or _yfinance_spy_weights)()
    earnings = (earnings_source or _yfinance_earnings)(yf_symbols)
    sectors_artifact = {"schema_version": SECTORS_SCHEMA_VERSION, "as_of": run_date,
                        "sectors": dict(sorted(sectors.items())),
                        "countries": dict(sorted(countries.items())),
                        "spy_sector_weights": dict(sorted(spy_weights.items()))}
    earnings_artifact = {"schema_version": EARNINGS_SCHEMA_VERSION, "as_of": run_date,
                         "earnings": dict(sorted(earnings.items()))}
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN reference: %d sectors, %d countries, %d spy-weights, %d earnings",
                    len(sectors), len(countries), len(spy_weights), len(earnings))
        return {"status": "ok_dry_run", "sectors": len(sectors), "countries": len(countries), "earnings": len(earnings)}
    try:
        _write_json(s3_client, bucket, f"{SECTORS_PREFIX}latest.json", sectors_artifact)
        _write_json(s3_client, bucket, f"{EARNINGS_PREFIX}latest.json", earnings_artifact)
    except Exception as e:  # fail loud
        logger.error("[metron_market_data] reference write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote %d sectors + %d countries + %d spy-weights + %d earnings",
                len(sectors), len(countries), len(spy_weights), len(earnings))
    return {"status": "ok", "sectors": len(sectors), "countries": len(countries),
            "spy_weights": len(spy_weights), "earnings": len(earnings)}


def collect_macro(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None, api_key: str | None = None, macro_source: MacroSource | None = None,
    release_source: Callable[[list[str], str], tuple[dict, list]] | None = None,
) -> dict:
    """Write the macro-indicator artifact for Metron's Macro page — Metron's LAST direct
    external fetch (FRED) moved to the spine:

        market_data/macro/latest.json
            {schema_version, as_of, series: {series_id: [[date, value], …]},
             next_release: {series_id: "YYYY-MM-DD"},
             release_events: [{date, kind, series_id, label}, …]}

    Fetches the 7 FRED series Metron renders as ~2y observation history, plus (v2) each
    series' next scheduled release date and the forward macro event calendar (FOMC +
    curated CPI/employment/claims releases) for the Macro "Next expected" column + the
    Calendar page (metron-ops#49/#13). Injectable source(s)/S3 for tests; FRED key from
    ``nousergon_lib.secrets`` when not injected."""
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    if macro_source is None:
        if api_key is None:
            from nousergon_lib.secrets import get_secret
            api_key = get_secret("FRED_API_KEY", required=False, default="")
        series = _fred_series_history(METRON_MACRO_SERIES, api_key)
    else:
        series = macro_source(METRON_MACRO_SERIES)
    if not series:
        return {"status": "skipped", "reason": "no macro series (FRED key unset or fetch failed)"}
    # v2: next-release dates + the macro event calendar. Best-effort — a FRED hiccup leaves
    # them empty (the consumer degrades to "next-release not wired") and never costs the
    # series artifact, which is the primary deliverable.
    next_release: dict[str, str] = {}
    release_events: list[dict] = []
    if release_source is not None:
        next_release, release_events = release_source(METRON_MACRO_SERIES, run_date)
    elif api_key:
        try:
            next_release, release_events = _fred_macro_releases(METRON_MACRO_SERIES, api_key, run_date)
        except Exception as e:  # noqa: BLE001 - secondary calendar data; never fail the series artifact
            logger.warning("[metron_market_data] macro release info failed (non-fatal): %s", e)
    artifact = {"schema_version": MACRO_SCHEMA_VERSION, "as_of": run_date,
                "series": {sid: [list(p) for p in obs] for sid, obs in sorted(series.items())},
                "next_release": dict(sorted(next_release.items())),
                "release_events": release_events}
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN macro: %d series (not written)", len(series))
        return {"status": "ok_dry_run", "series": len(series)}
    try:
        _write_json(s3_client, bucket, f"{MACRO_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud
        logger.error("[metron_market_data] macro write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote %d macro series, %d next-release, %d events",
                len(series), len(next_release), len(release_events))
    return {"status": "ok", "series": len(series),
            "next_release": len(next_release), "release_events": len(release_events)}


def _fred_macro_releases(series_ids: list[str], api_key: str, run_date: str) -> tuple[dict, list]:
    """``(next_release, release_events)`` for the Macro page — the next scheduled release
    date per series (the "Next expected" column) + the forward macro event calendar (FOMC +
    curated CPI/employment/claims releases for the Calendar page). Reuses the FRED
    release-schedule helpers in ``collectors.macro``. Per-series failures are omitted (the
    helpers already fail-soft); the whole call is best-effort at the caller."""
    from datetime import date as _date

    from collectors.macro import _fred_release_dates, _fred_release_id, build_release_calendar

    next_release: dict[str, str] = {}
    for sid in series_ids:
        rel = _fred_release_id(sid, api_key)
        if rel is None:
            continue
        future = sorted(d for d in _fred_release_dates(rel[0], api_key) if d >= run_date)
        if future:
            next_release[sid] = future[0]  # earliest scheduled date on/after run_date

    try:
        today = _date.fromisoformat(run_date)
    except ValueError:
        today = datetime.now(timezone.utc).date()
    df = build_release_calendar(api_key, today)
    events = (
        []
        if df.empty
        else [
            {"date": r["date"], "kind": r["kind"], "series_id": r["series_id"], "label": r["label"]}
            for r in df.to_dict("records")
        ]
    )
    return next_release, events


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
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()
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


def collect_analyst(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None, analyst_source: AnalystSource | None = None,
) -> dict:
    """Write the consensus-research artifact for Metron's held universe
    (metron-ops#105 — Holdings Sentiment/Consensus band + attractiveness score):

        market_data/analyst/latest.json
            {schema_version, as_of, source: "yfinance+finnhub", analyst:
                {yf_symbol: {consensus_rating, rating_score, mean_target,
                             median_target, num_analysts}}}

    FREE sources only (see ``_yfinance_analyst``). The consumer derives
    price-target upside vs the live price and owns display semantics; forward
    consensus EPS/revenue ESTIMATES are a paid feed scaffolded N/A downstream.
    Daily cadence (ratings/targets move slowly; daily is plenty). Injectable
    source/S3 for tests. Mirrors ``collect_fundamentals``."""
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}
    yf_symbols = sorted({h["yf_symbol"] for h in holdings})
    analyst = (analyst_source or _yfinance_analyst)(yf_symbols)
    artifact = {
        "schema_version": ANALYST_SCHEMA_VERSION, "as_of": run_date,
        "source": "yfinance+finnhub", "analyst": dict(sorted(analyst.items())),
    }
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN analyst: %d symbols (not written)", len(analyst))
        return {"status": "ok_dry_run", "analyst": len(analyst)}
    try:
        _write_json(s3_client, bucket, f"{ANALYST_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud to the phase registry
        logger.error("[metron_market_data] analyst write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote analyst for %d/%d symbols", len(analyst), len(yf_symbols))
    return {"status": "ok", "universe": len(holdings), "analyst": len(analyst)}


def _news_sentiment(yf_symbols: list[str]) -> dict[str, dict]:
    """Held-universe latest news-sentiment slice projected from the upstream
    ``data/news_aggregates_daily/`` parquet → ``{yf_symbol: {sentiment,
    sentiment_mean, n_articles, event_count, event_severity_max, as_of}}``.

    ``sentiment`` is the source-trust-weighted LM composite
    (``lm_sentiment_trusted_mean`` ∈ [-1, +1]) — the headline metric; the raw
    ``lm_sentiment_mean`` is kept for audit. The news universe keys by plain
    ticker, so matching is on ``yf_symbol`` (US names line up; foreign/fund
    symbols with no coverage are omitted — a gap, not a zero). If the parquet
    spans multiple dates, the most recent row per ticker wins; ``as_of`` carries
    that row's date so the consumer can show sentiment staleness honestly."""
    from collectors.daily_news import read_daily_news

    try:
        df = read_daily_news()
    except Exception as e:  # fail-soft: degrade to "sentiment unavailable"
        logger.warning("[metron_market_data] news-aggregates read failed: %s", e)
        return {}
    if df is None or getattr(df, "empty", True):
        return {}

    want = set(yf_symbols)
    out: dict[str, dict] = {}
    sub = df[df["ticker"].isin(want)]
    for sym, rows in sub.groupby("ticker"):
        row = rows.sort_values("aggregate_date").iloc[-1]  # most recent date wins
        def _num(col):
            v = row.get(col)
            try:
                f = float(v)
                return round(f, 4) if f == f else None  # NaN → None
            except (TypeError, ValueError):
                return None
        fields = {
            "sentiment": _num("lm_sentiment_trusted_mean"),
            "sentiment_mean": _num("lm_sentiment_mean"),
            "n_articles": int(row["n_articles"]) if row.get("n_articles") is not None else None,
            "event_count": int(row["event_count"]) if row.get("event_count") is not None else None,
            "event_severity_max": _num("event_severity_max"),
            "as_of": str(row["aggregate_date"])[:10] if row.get("aggregate_date") is not None else None,
        }
        if any(v is not None for v in (fields["sentiment"], fields["n_articles"])):
            out[str(sym)] = {k: v for k, v in fields.items() if v is not None}
    logger.info("[metron_market_data] sentiment: %d/%d symbols covered", len(out), len(yf_symbols))
    return out


def collect_sentiment(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None, sentiment_source: SentimentSource | None = None,
) -> dict:
    """Write the news-sentiment artifact for Metron's held universe
    (metron-ops#105 — Holdings Sentiment/Consensus band + attractiveness score):

        market_data/sentiment/latest.json
            {schema_version, as_of, source: "news_aggregates_daily(LM)", sentiment:
                {yf_symbol: {sentiment, sentiment_mean, n_articles, event_count,
                             event_severity_max, as_of}}}

    A JSON projection of the held-universe latest slice of the upstream
    `data/news_aggregates_daily/` parquet (see ``_news_sentiment``) — keeps
    Metron's spine readers uniform (no pyarrow in the API). Injectable source/S3
    for tests. Mirrors ``collect_analyst``."""
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}
    yf_symbols = sorted({h["yf_symbol"] for h in holdings})
    sentiment = (sentiment_source or _news_sentiment)(yf_symbols)
    artifact = {
        "schema_version": SENTIMENT_SCHEMA_VERSION, "as_of": run_date,
        "source": "news_aggregates_daily(LM)", "sentiment": dict(sorted(sentiment.items())),
    }
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN sentiment: %d symbols (not written)", len(sentiment))
        return {"status": "ok_dry_run", "sentiment": len(sentiment)}
    try:
        _write_json(s3_client, bucket, f"{SENTIMENT_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud to the phase registry
        logger.error("[metron_market_data] sentiment write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote sentiment for %d/%d symbols", len(sentiment), len(yf_symbols))
    return {"status": "ok", "universe": len(holdings), "sentiment": len(sentiment)}


# ── Technicals (derived from close_history — no new fetch) ────────────────────

# Minimum close observations needed before any indicator is emitted for a symbol. The
# 200-day MA is the deepest window; below it we emit only the indicators a shorter series
# supports (never a fabricated MA on too little data).
_TECH_MIN_OBS = 30
_SESSIONS_PER_YEAR = 252


def _compute_technicals(closes: list[list]) -> dict | None:
    """Indicators from an ascending ``[[date_iso, close], …]`` series → flat dict, or
    ``None`` if the series is too short / unusable. Reuses the fleet-canonical Wilder RSI +
    MACD from ``features.feature_engineer`` so the dashboard and the predictor agree on the
    definitions (per the SOTA mirror-don't-reinvent rule). Each field is independently
    gated on having enough history — a 120-day series gets RSI/50d-MA but null 200d-MA."""
    import pandas as pd

    from features.feature_engineer import _compute_macd, _compute_rsi

    vals = [c for c in closes if isinstance(c, (list, tuple)) and len(c) == 2 and c[1] is not None]
    if len(vals) < _TECH_MIN_OBS:
        return None
    s = pd.Series([float(v[1]) for v in vals], dtype="float64")
    s = s[s > 0]
    if len(s) < _TECH_MIN_OBS:
        return None
    last = float(s.iloc[-1])

    def _round(x: float | None, n: int = 4) -> float | None:
        if x is None:
            return None
        try:
            x = float(x)
        except (TypeError, ValueError):
            return None
        return round(x, n) if x == x and x not in (float("inf"), float("-inf")) else None

    rsi = _compute_rsi(s)
    macd_line, signal_line = _compute_macd(s)
    rsi_14 = _round(rsi.iloc[-1], 2) if len(rsi) else None
    macd_hist = (
        _round(float(macd_line.iloc[-1]) - float(signal_line.iloc[-1]))
        if len(macd_line) and len(signal_line) else None
    )
    ma_50 = float(s.iloc[-50:].mean()) if len(s) >= 50 else None
    ma_200 = float(s.iloc[-200:].mean()) if len(s) >= 200 else None
    window_52w = s.iloc[-_SESSIONS_PER_YEAR:]
    high_52w = float(window_52w.max())
    low_52w = float(window_52w.min())
    rng = high_52w - low_52w
    out = {
        "rsi_14": rsi_14,
        "macd_hist": macd_hist,
        "ma_50": _round(ma_50, 4),
        "ma_200": _round(ma_200, 4),
        "pct_to_ma_50": _round(last / ma_50 - 1.0) if ma_50 else None,
        "pct_to_ma_200": _round(last / ma_200 - 1.0) if ma_200 else None,
        "high_52w": _round(high_52w, 4),
        "low_52w": _round(low_52w, 4),
        "pct_in_52w_range": _round((last - low_52w) / rng) if rng > 0 else None,
        "pct_from_52wk_high": _round(last / high_52w - 1.0) if high_52w > 0 else None,
        "mom_20d": _round(last / float(s.iloc[-21]) - 1.0) if len(s) >= 21 else None,
        "mom_60d": _round(last / float(s.iloc[-61]) - 1.0) if len(s) >= 61 else None,
    }
    # All-null → no usable indicator; treat as a coverage gap (no fabricated row).
    return out if any(v is not None for v in out.values()) else None


def collect_technicals(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None,
) -> dict:
    """Write per-held-symbol technical indicators for Metron's Holdings table, computed
    from the ``close_history`` artifacts this module already publishes (read back from S3 —
    **no new market-data fetch**):

        market_data/technicals/latest.json
            {schema_version, as_of, technicals: {yf_symbol: {rsi_14, macd_hist, ma_50,
             ma_200, pct_to_ma_50, pct_to_ma_200, high_52w, low_52w, pct_in_52w_range,
             mom_20d, mom_60d}}}

    Daily cadence (close_history refreshes daily). Universe = SP1500 ∪ Metron held/watchlist.
    Fail-soft per symbol; a symbol with no close_history / too short a series is omitted."""
    if run_date is None:
        from dates import default_run_date

        run_date = default_run_date()
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_price_derived_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty price-derived universe", "universe": 0}
    yf_symbols = sorted({h["yf_symbol"] for h in holdings})
    technicals: dict[str, dict] = {}
    for yf_sym in yf_symbols:
        hist = _read_json(s3_client, bucket, f"{CLOSE_HISTORY_PREFIX}{yf_sym}.json")
        if not hist or not hist.get("closes"):
            continue
        try:
            tech = _compute_technicals(hist["closes"])
        except Exception as e:  # one bad series must not sink the whole artifact
            logger.warning("[metron_market_data] technicals compute failed for %s: %s", yf_sym, e)
            continue
        if tech is not None:
            technicals[yf_sym] = tech
    artifact = {
        "schema_version": TECHNICALS_SCHEMA_VERSION, "as_of": run_date,
        "source": "computed", "technicals": dict(sorted(technicals.items())),
    }
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN technicals: %d/%d symbols (not written)",
                    len(technicals), len(yf_symbols))
        return {"status": "ok_dry_run", "technicals": len(technicals), "universe": len(yf_symbols)}
    try:
        _write_json(s3_client, bucket, f"{TECHNICALS_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud to the phase registry
        logger.error("[metron_market_data] technicals write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote technicals for %d/%d symbols", len(technicals), len(yf_symbols))
    return {"status": "ok", "universe": len(yf_symbols), "technicals": len(technicals)}


# ── Security performance (period returns + risk stats — derived from close_history) ──

_MIN_RISK_BARS = 60  # ~3 months of daily closes before annualized risk stats mean anything


def _closes_to_series(closes: list[list]) -> list[tuple[date, float]]:
    """Ascending (bar_date, close) from artifact ``[[date_iso, close], …]`` rows."""
    out: list[tuple[date, float]] = []
    for row in closes:
        if not isinstance(row, (list, tuple)) or len(row) != 2 or row[1] is None:
            continue
        try:
            d, c = date.fromisoformat(str(row[0])), float(row[1])
        except (TypeError, ValueError):
            continue
        if c > 0:
            out.append((d, c))
    return sorted(out, key=lambda t: t[0])


def _sp_daily_returns(closes: list[float]) -> list[float]:
    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            rets.append(closes[i] / closes[i - 1] - 1.0)
    return rets


def _sp_period_returns(series: list[tuple[date, float]], as_of: date) -> dict[str, float]:
    if len(series) < 2:
        return {}
    last_close = series[-1][1]
    out: dict[str, float] = {}
    for years, label in ((1, "1Y"), (3, "3Y"), (5, "5Y"), (10, "10Y")):
        try:
            start = as_of.replace(year=as_of.year - years)
        except ValueError:
            start = as_of.replace(year=as_of.year - years, day=28)
        ref = next((c for d, c in series if d >= start), None)
        if ref is not None and ref > 0 and series[0][0] <= start:
            out[label] = round(last_close / ref - 1.0, 6)
    return out


def _sp_window_return(series: list[tuple[date, float]], start: date) -> float | None:
    if len(series) < 2 or series[0][0] > start:
        return None
    ref = next((c for d, c in series if d >= start), None)
    last = series[-1][1]
    if ref is None or ref <= 0:
        return None
    return round(last / ref - 1.0, 6)


def _sp_year_ago(as_of: date) -> date:
    try:
        return as_of.replace(year=as_of.year - 1)
    except ValueError:
        return as_of.replace(year=as_of.year - 1, day=28)


def _sp_beta_and_vs_spy(
    ticker_series: list[tuple[date, float]], spy_series: list[tuple[date, float]]
) -> tuple[float | None, float | None]:
    spy_by_date = dict(spy_series)
    common = [(d, c, spy_by_date[d]) for d, c in ticker_series if d in spy_by_date]
    if len(common) < _MIN_RISK_BARS:
        return None, None
    t_closes = [c for _, c, _ in common]
    s_closes = [s for _, _, s in common]
    t_rets, s_rets = _sp_daily_returns(t_closes), _sp_daily_returns(s_closes)
    n = min(len(t_rets), len(s_rets))
    if n < _MIN_RISK_BARS or not s_rets:
        return None, None
    t_rets, s_rets = t_rets[:n], s_rets[:n]
    s_mean = sum(s_rets) / n
    t_mean = sum(t_rets) / n
    var = sum((s - s_mean) ** 2 for s in s_rets) / n
    if var <= 0:
        return None, None
    cov = sum((t_rets[i] - t_mean) * (s_rets[i] - s_mean) for i in range(n)) / n
    beta = round(cov / var, 4)
    vs_window = round(
        (t_closes[-1] / t_closes[0] - 1.0) - (s_closes[-1] / s_closes[0] - 1.0), 6
    )
    return beta, vs_window


def _compute_security_performance(
    closes: list[list], *, spy_closes: list[list] | None, as_of: date
) -> dict | None:
    """Period returns + risk stats + beta vs SPY from a close_history series.

    Canonical producer for Metron tearsheet / Holdings LTM — consumers read
    ``market_data/security_performance/latest.json`` and never recompute."""
    from nousergon_lib.quant.riskstats import max_drawdown, sharpe_ratio, sortino_ratio, volatility

    series = _closes_to_series(closes)
    if len(series) < 2:
        return None
    spy_series = _closes_to_series(spy_closes or [])
    period_returns = _sp_period_returns(series, as_of)
    out: dict = {
        "period_returns": period_returns,
        "ytd_pct": _sp_window_return(series, date(as_of.year, 1, 1)),
        "ltm_pct": _sp_window_return(series, _sp_year_ago(as_of)),
        "n_bars": len(series),
        "history_from": series[0][0].isoformat(),
    }
    closes_only = [c for _, c in series]
    rets = _sp_daily_returns(closes_only)
    if len(rets) >= _MIN_RISK_BARS:
        span_days = (series[-1][0] - series[0][0]).days or 1
        ppy = len(rets) / (span_days / 365.25)
        vol = volatility(rets, periods_per_year=ppy)
        if vol is not None:
            out["volatility"] = round(vol, 6)
        sharpe = sharpe_ratio(rets, periods_per_year=ppy)
        if sharpe is not None:
            out["sharpe"] = round(sharpe, 4)
        sortino = sortino_ratio(rets, periods_per_year=ppy)
        if sortino is not None:
            out["sortino"] = round(sortino, 4)
        index = [1.0]
        for r in rets:
            index.append(index[-1] * (1.0 + r))
        mdd = max_drawdown(index)
        if mdd is not None:
            out["max_drawdown"] = round(mdd, 6)
        beta, vs_window = _sp_beta_and_vs_spy(series, spy_series)
        out["beta_vs_spy"] = beta
        out["vs_spy_window"] = vs_window
    if spy_series:
        spy_periods = _sp_period_returns(spy_series, as_of)
        if period_returns.get("1Y") is not None and spy_periods.get("1Y") is not None:
            out["vs_spy_1y"] = round(period_returns["1Y"] - spy_periods["1Y"], 6)
    return out


def collect_security_performance(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None,
) -> dict:
    """Write per-symbol performance metrics for Metron (tearsheet + Holdings LTM + markets
    strip), computed from ``close_history`` artifacts already on the spine — **no new fetch**:

        market_data/security_performance/latest.json
            {schema_version, as_of, performance: {yf_symbol: {period_returns, ytd_pct,
             ltm_pct, volatility, sharpe, sortino, max_drawdown, beta_vs_spy,
             vs_spy_1y, vs_spy_window, n_bars, history_from}}}

    Daily cadence; runs after ``collect_history``. Universe = SP1500 ∪ Metron held/watchlist.
    Fail-soft per symbol."""
    if run_date is None:
        from dates import default_run_date

        run_date = default_run_date()
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    holdings, _ = load_price_derived_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty price-derived universe", "universe": 0}
    yf_symbols = sorted({h["yf_symbol"] for h in holdings})
    as_of = date.fromisoformat(str(run_date)[:10])
    spy_hist = _read_json(s3_client, bucket, f"{CLOSE_HISTORY_PREFIX}{BENCHMARK}.json")
    spy_closes = (spy_hist or {}).get("closes")
    performance: dict[str, dict] = {}
    for yf_sym in yf_symbols:
        hist = _read_json(s3_client, bucket, f"{CLOSE_HISTORY_PREFIX}{yf_sym}.json")
        if not hist or not hist.get("closes"):
            continue
        try:
            perf = _compute_security_performance(hist["closes"], spy_closes=spy_closes, as_of=as_of)
        except Exception as e:
            logger.warning("[metron_market_data] security_performance compute failed for %s: %s", yf_sym, e)
            continue
        if perf is not None:
            performance[yf_sym] = perf
    artifact = {
        "schema_version": SECURITY_PERFORMANCE_SCHEMA_VERSION,
        "as_of": run_date,
        "source": "computed",
        "performance": dict(sorted(performance.items())),
    }
    if dry_run:
        logger.info(
            "[metron_market_data] DRY-RUN security_performance: %d/%d symbols (not written)",
            len(performance), len(yf_symbols),
        )
        return {"status": "ok_dry_run", "performance": len(performance), "universe": len(yf_symbols)}
    try:
        _write_json(s3_client, bucket, f"{SECURITY_PERFORMANCE_PREFIX}latest.json", artifact)
    except Exception as e:
        logger.error("[metron_market_data] security_performance write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info(
        "[metron_market_data] wrote security_performance for %d/%d symbols",
        len(performance), len(yf_symbols),
    )
    return {"status": "ok", "universe": len(yf_symbols), "performance": len(performance)}


# ── Valuation medians (SP1500-broad sector & country peer benchmark) ──────────

# camelCase yfinance .info key → snake_case median output field (the consumer maps these
# 1:1 to the per-holding multiple, so band and row are directly comparable).
_MEDIAN_OUT_NAME = {
    "trailingPE": "trailing_pe", "forwardPE": "forward_pe", "priceToBook": "price_to_book",
    "priceToSalesTrailing12Months": "price_to_sales", "enterpriseToEbitda": "ev_ebitda",
    "dividendYield": "dividend_yield",
}
# A sector/country bucket below this many members is statistically too thin to be a
# benchmark — it's still emitted (with its honest `n`) so the consumer can choose to
# suppress it, never silently dropped.
_MEDIAN_MIN_BUCKET = 3


@_yf_quiet
def _yfinance_valuation(yf_symbols: list[str]) -> dict[str, dict]:
    """Per-symbol valuation multiples + GICS sector + country of domicile via
    ``yf.Ticker(sym).info`` → ``{yf_symbol: {key: value, …, "sector": …, "country": …}}``
    over ``VALUATION_MEDIAN_KEYS``. Same source + units as ``_yfinance_fundamentals`` so the
    median band and the per-holding row are apples-to-apples. Fail-soft per symbol."""
    try:
        import yfinance as yf
    except ImportError:  # pragma: no cover
        return {}
    out: dict[str, dict] = {}
    for i, sym in enumerate(yf_symbols):
        if i > 0 and i % _YFINANCE_BATCH_SIZE == 0:
            time.sleep(_YFINANCE_BATCH_DELAY)  # rate-limit courtesy on the ~900-name pass
        try:
            info = yf.Ticker(sym).info or {}
        except Exception as e:
            logger.warning("[metron_market_data] valuation fetch failed for %s: %s", sym, e)
            continue
        row = {k: info[k] for k in VALUATION_MEDIAN_KEYS if info.get(k) is not None}
        sector, country = info.get("sector"), info.get("country")
        if sector:
            row["sector"] = sector
        if country:
            row["country"] = country
        if row:
            out[sym] = row
    logger.info("[metron_market_data] valuation: %d/%d symbols covered", len(out), len(yf_symbols))
    _log_yf_coverage("valuation", yf_symbols, out)
    return out


def _default_medians_universe(bucket: str, s3_client: Any) -> list[str]:
    """The broad benchmark universe = SP1500 ∪ Metron held/watchlist (``load_price_derived_universe``)."""
    holdings, _ = load_price_derived_universe(bucket, s3_client)
    return [h["yf_symbol"] for h in holdings]


def _grouped_medians(rows: list[dict], group_key: str) -> dict[str, dict]:
    """Median of each multiple per ``group_key`` (``"sector"`` | ``"country"``) value, plus
    the member count ``n``. A multiple's median uses only finite, > 0 samples (a negative /
    zero P/E is meaningless, not a data point); a bucket missing a multiple omits that field
    rather than emitting a fabricated value."""
    import statistics

    buckets: dict[str, list[dict]] = {}
    for r in rows:
        g = r.get(group_key)
        if g:
            buckets.setdefault(str(g), []).append(r)
    out: dict[str, dict] = {}
    for g, members in buckets.items():
        entry: dict = {"n": len(members)}
        for in_key, out_key in _MEDIAN_OUT_NAME.items():
            samples = []
            for m in members:
                v = m.get(in_key)
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    continue
                if v == v and v not in (float("inf"), float("-inf")) and v > 0:
                    samples.append(v)
            if samples:
                entry[out_key] = round(statistics.median(samples), 4)
        out[g] = entry
    return dict(sorted(out.items()))


def collect_valuation_medians(
    *, bucket: str = DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None, valuation_source: ValuationSource | None = None,
    universe_source: MediansUniverseSource | None = None,
) -> dict:
    """Write the SP1500-broad sector & country median-multiple benchmark for Metron's
    Holdings "by sector → country" bands (metron Holdings metrics):

        market_data/valuation_medians/latest.json
            {schema_version, as_of, source: "yfinance",
             by_sector:  {sector:  {trailing_pe, forward_pe, price_to_book, price_to_sales,
                                    ev_ebitda, dividend_yield, n}},
             by_country: {country: {…same…}}}

    Weekly cadence (medians of ~900 names move quarterly). A derived AGGREGATE — emits only
    medians + member counts, never per-ticker rows. Injectable sources/S3 for tests; hard-
    fails (no silent zero artifact) if the universe pass returns nothing usable."""
    if run_date is None:
        from dates import default_run_date

        run_date = default_run_date()
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    universe = (
        universe_source() if universe_source is not None
        else _default_medians_universe(bucket, s3_client)
    )
    if not universe:
        return {"status": "skipped", "reason": "empty medians universe", "universe": 0}
    rows_by_sym = (valuation_source or _yfinance_valuation)(universe)
    rows = list(rows_by_sym.values())
    if not rows:  # no-silent-fails: an empty pass is an error, not a blank artifact
        logger.error("[metron_market_data] valuation medians: 0/%d symbols covered", len(universe))
        return {"status": "error", "error": "valuation pass returned no usable rows",
                "universe": len(universe)}
    by_sector = _grouped_medians(rows, "sector")
    by_country = _grouped_medians(rows, "country")
    artifact = {
        "schema_version": VALUATION_MEDIANS_SCHEMA_VERSION, "as_of": run_date,
        "source": "yfinance", "by_sector": by_sector, "by_country": by_country,
    }
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN valuation medians: %d sectors, %d countries "
                    "from %d/%d symbols (not written)",
                    len(by_sector), len(by_country), len(rows), len(universe))
        return {"status": "ok_dry_run", "sectors": len(by_sector),
                "countries": len(by_country), "covered": len(rows), "universe": len(universe)}
    try:
        _write_json(s3_client, bucket, f"{VALUATION_MEDIANS_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud to the phase registry
        logger.error("[metron_market_data] valuation medians write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote valuation medians: %d sectors + %d countries "
                "from %d/%d symbols", len(by_sector), len(by_country), len(rows), len(universe))
    return {"status": "ok", "universe": len(universe), "covered": len(rows),
            "sectors": len(by_sector), "countries": len(by_country)}


# NYSE early-close days (1:00 PM ET close): day after Thanksgiving, and Christmas
# Eve / July 3 when they fall on a weekday. Source: nyse.com/markets/hours-calendars.
# Holidays themselves come from nousergon_lib.trading_calendar.NYSE_HOLIDAYS (the
# fleet-canonical set, maintained through 2030). Lift this early-close set into
# nousergon_lib.trading_calendar on second adoption per the
# lift-invariants-after-second-recurrence rule.
NYSE_EARLY_CLOSES: set = {
    # 2026
    date(2026, 11, 27),  # day after Thanksgiving
    date(2026, 12, 24),  # Christmas Eve (Thursday)
    # 2027
    date(2027, 11, 26),  # day after Thanksgiving
    date(2027, 12, 23),  # day before observed Christmas (Dec 24 = observed holiday)
    # 2028
    date(2028, 7, 3),    # day before Independence Day (Monday)
    date(2028, 11, 24),  # day after Thanksgiving
    # 2029
    date(2029, 7, 3),    # day before Independence Day (Tuesday)
    date(2029, 11, 23),  # day after Thanksgiving
    date(2029, 12, 24),  # Christmas Eve (Monday)
    # 2030
    date(2030, 7, 3),    # day before Independence Day (Wednesday)
    date(2030, 11, 29),  # day after Thanksgiving
    date(2030, 12, 24),  # Christmas Eve (Tuesday)
}
_SESSION_MARGIN_MIN = 5  # minutes of slack either side of the official session


def in_us_market_window(now: datetime | None = None) -> bool:
    """True inside the actual NYSE session (±5 min margin), in exchange time.

    Exchange-calendar gating: weekends and NYSE holidays via the fleet-canonical
    ``nousergon_lib.trading_calendar.is_trading_day``; the session is 9:30 ET →
    16:00 ET (13:00 ET on ``NYSE_EARLY_CLOSES`` half-days), evaluated in
    America/New_York so DST is handled exactly — no widened-UTC heuristic."""
    from zoneinfo import ZoneInfo

    from nousergon_lib.trading_calendar import is_trading_day

    now = now or datetime.now(timezone.utc)
    et = now.astimezone(ZoneInfo("America/New_York"))
    if not is_trading_day(et.date()):
        return False
    close_hm = (13, 0) if et.date() in NYSE_EARLY_CLOSES else (16, 0)
    open_min = 9 * 60 + 30 - _SESSION_MARGIN_MIN
    close_min = close_hm[0] * 60 + close_hm[1] + _SESSION_MARGIN_MIN
    return open_min <= et.hour * 60 + et.minute <= close_min


# The (opt-in) demand gate: Metron's web layer touches this key while the app is
# actively being used (throttled heartbeat — see metron api/services/data_spine.py).
# When the gate is ON (``require_heartbeat=True``, set for a multi-tenant deployment via
# the ``--require-heartbeat`` flag) the producer fetches ONLY while the heartbeat is fresh,
# so a closed app costs zero quote fetches. The gate is OFF by default: on the single-tenant
# owner build the strip is 4 globally-shared ETF symbols + a small held universe — one fetch
# serves everyone — so we keep it warm every session tick rather than inflict a multi-minute
# cold-start (a frozen morning quote) on the owner every time the app has been idle >10 min.
# Re-enable before multi-tenant by adding ``--require-heartbeat`` to the systemd unit.
UI_HEARTBEAT_KEY = "metron/ui_heartbeat.json"
HEARTBEAT_FRESH_SECONDS = 600  # 10 min — two missed 5-min ticks ends the session


def metron_app_active(bucket: str, s3_client: Any, now: datetime | None = None) -> bool:
    """True when Metron's UI heartbeat exists and is fresh (see ``UI_HEARTBEAT_KEY``).
    Fail-soft on read errors → inactive (the artifact just goes stale; the consumer
    shows staleness honestly via ``as_of_utc``)."""
    now = now or datetime.now(timezone.utc)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=UI_HEARTBEAT_KEY)
        ts = json.loads(obj["Body"].read()).get("ts", "")
        beat = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return (now - beat).total_seconds() <= HEARTBEAT_FRESH_SECONDS
    except Exception as e:  # missing key, parse error, no creds — all mean "not active"
        logger.info("[metron_market_data] no fresh UI heartbeat (%s) — app inactive", e)
        return False


def _flag_suspect_quotes(quotes: dict[str, dict]) -> int:
    """Flag (never drop) any quote moving >40% vs prior close — almost always a bad
    scrape or an unadjusted corporate action, not a real move. Marks ``suspect: true``
    in place (no fabrication: the consumer renders a warning, not a wild P&L leg) and
    returns the count flagged."""
    n_suspect = 0
    for q in quotes.values():
        prev = q.get("prev_close") or 0.0
        if prev and abs(q.get("last", prev) / prev - 1.0) > 0.40:
            q["suspect"] = True
            n_suspect += 1
    return n_suspect


# Cross-source scale-coherence bounds (metron-ops#159 — MARUY intraday quote landed at
# 30.17 against a settled EOD close of 308.40, a 10.2:1 ADR-ratio-scale divergence). The
# >40% move guard above compares a quote only to ITS OWN prior tick from the SAME
# yfinance intraday pull — so when yfinance's live quote and its own recent-session bars
# both silently shift onto a new scale together (an ADR ratio change the live feed picked
# up that the settled-close run, fetched at a different time, hasn't), `last` and
# `prev_close` agree with EACH OTHER and the move guard sees nothing wrong. Mirrors
# metron's consumer-side guard (api/services/intraday.py `_COHERENCE_RATIO_BOUNDS`) but
# runs at the SOURCE against THIS producer's own settled ``eod_closes`` artifact, so a
# wrong-scale quote is flagged for every consumer of the spine, not just Metron.
_SCALE_COHERENCE_RATIO_BOUNDS = (0.5, 2.0)


def _flag_scale_incoherent_quotes(quotes: dict[str, dict], eod_closes: dict[str, dict]) -> int:
    """Flag (never drop) any quote whose implied move vs THIS producer's own settled EOD
    close (``eod_closes``, the ``market_data/eod_closes/latest.json`` artifact this module
    also publishes) falls outside ``_SCALE_COHERENCE_RATIO_BOUNDS`` — a real single-session
    move that large is rare enough that it's overwhelmingly a wrong-scale quote (ADR ratio
    change, pence-vs-pounds, symbol collision). Symbols with no settled close on file are
    skipped (nothing to cross-check against). Marks ``suspect: true`` in place and returns
    the count newly flagged (a quote already suspect from the move guard isn't double
    counted)."""
    n_suspect = 0
    lo, hi = _SCALE_COHERENCE_RATIO_BOUNDS
    for sym, q in quotes.items():
        eod = eod_closes.get(sym)
        settled = (eod or {}).get("close")
        last = q.get("last")
        if not settled or last is None:
            continue
        ratio = last / settled
        if not (lo <= ratio <= hi):
            already = q.get("suspect", False)
            q["suspect"] = True
            if not already:
                n_suspect += 1
    return n_suspect


def collect_intraday(
    *, bucket: str = DEFAULT_BUCKET, dry_run: bool = False, s3_client: Any = None,
    intraday_source: IntradaySource | None = None, force: bool = False,
    now: datetime | None = None, require_heartbeat: bool = False,
) -> dict:
    """Write the intraday-quotes artifact for Metron's held universe + the major-index
    proxies (config#1023 — the 15-minute Today-view feed + the Overview markets strip,
    consumed by metron-ops#23):

        market_data/intraday/latest.json
            {schema_version, as_of_utc, source: "yfinance_delayed",
             quotes:       {yf_symbol: {last, open, prev_close, session_date, prev_session_date, currency}},
             indices:      {etf_symbol: {last, open, prev_close, session_date, prev_session_date, currency}},
             fund_proxies: {etf_symbol: {last, open, prev_close, session_date, prev_session_date, currency}}}

    ``last`` is ~15-min delayed. Runs every 5 min via a systemd timer on the trading box
    (infrastructure/systemd/metron-intraday.timer), gated on the NYSE session window
    (exchange calendar incl. half-days). When ``require_heartbeat`` (the multi-tenant
    demand gate — OFF by default, see ``UI_HEARTBEAT_KEY``) it ALSO gates on a fresh Metron
    UI heartbeat so a closed app costs zero fetches; OFF, it stays warm every session tick
    so the owner never sees a frozen morning quote after the app was idle. Outside the
    market window it returns ``skipped`` without fetching (``force`` overrides every gate,
    for manual runs). The major-index ETF proxies (``INDEX_PROXY_SYMBOLS``) are market
    context — fetched every run regardless of the held universe, so a brand-new account
    still gets the markets strip. A quote moving >40% vs prior close carries
    ``suspect: true`` (flagged, never dropped); a held quote whose scale disagrees with
    this producer's own settled EOD close (metron-ops#159) is flagged the same way, even
    when the move guard alone would miss it (see ``_flag_scale_incoherent_quotes``).
    Single ``latest.json`` key — consumers see staleness via ``as_of_utc``. Injectable
    source/S3 for tests."""
    if not force and not in_us_market_window(now):
        return {"status": "skipped", "reason": "outside US market window"}
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    if require_heartbeat and not force and not metron_app_active(bucket, s3_client, now):
        return {"status": "skipped", "reason": "metron app inactive (no fresh UI heartbeat)"}
    fetch = intraday_source or _yfinance_intraday

    # Held-universe quotes (may be empty — a brand-new account still gets the index strip).
    holdings, _ = load_metron_universe(bucket, s3_client)
    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in holdings}
    quotes = fetch(sorted(ccy_by_yf)) if ccy_by_yf else {}
    for sym, q in quotes.items():
        q["currency"] = ccy_by_yf.get(sym, "USD")

    # Major-index ETF proxies — market context, always fetched (independent of holdings).
    indices = fetch(list(INDEX_PROXY_SYMBOLS))
    for q in indices.values():
        q["currency"] = BASE_CURRENCY

    # Fund-proxy ETF quotes — the same-day move that estimates a late-striking mutual fund's
    # return (metron fund_proxy.py). Kept in a DEDICATED map (not `indices`, which is the 4
    # headline-index proxies) so the consumer has one clean source for every proxy and IXUS
    # never reads as a headline index. Always fetched, independent of holdings.
    fund_proxies = fetch(list(FUND_PROXY_ETFS))
    for q in fund_proxies.values():
        q["currency"] = BASE_CURRENCY

    n_suspect = _flag_suspect_quotes(quotes) + _flag_suspect_quotes(indices) + _flag_suspect_quotes(fund_proxies)
    if n_suspect:
        logger.warning("[metron_market_data] %d intraday quote(s) flagged suspect (>40%% vs prev close)", n_suspect)

    # Cross-source scale-coherence check (metron-ops#159): compare each held quote against
    # THIS producer's own settled EOD close (already published, so no extra fetch). Fail-soft
    # — a missing/unreadable eod_closes artifact just skips the check (nothing to flag yet).
    eod_closes = (_read_json(s3_client, bucket, f"{CLOSES_PREFIX}latest.json") or {}).get("closes", {})
    n_scale_suspect = _flag_scale_incoherent_quotes(quotes, eod_closes)
    if n_scale_suspect:
        logger.warning(
            "[metron_market_data] %d intraday quote(s) flagged suspect (scale-incoherent vs settled close)",
            n_scale_suspect,
        )
    artifact = {
        "schema_version": INTRADAY_SCHEMA_VERSION,
        "as_of_utc": (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "yfinance_delayed",
        "quotes": dict(sorted(quotes.items())),
        "indices": dict(sorted(indices.items())),
        "fund_proxies": dict(sorted(fund_proxies.items())),
    }
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN intraday: %d quotes, %d indices, %d fund-proxies (not written)",
                    len(quotes), len(indices), len(fund_proxies))
        return {"status": "ok_dry_run", "quotes": len(quotes), "indices": len(indices),
                "fund_proxies": len(fund_proxies)}
    try:
        _write_json(s3_client, bucket, f"{INTRADAY_PREFIX}latest.json", artifact)
    except Exception as e:  # fail loud — the timer unit's journal + freshness scan record it
        logger.error("[metron_market_data] intraday write failed: %s", e)
        return {"status": "error", "error": str(e)}
    logger.info("[metron_market_data] wrote %d intraday quotes + %d indices + %d fund-proxies",
                len(quotes), len(indices), len(fund_proxies))
    return {"status": "ok", "universe": len(holdings), "quotes": len(quotes),
            "indices": len(indices), "fund_proxies": len(fund_proxies)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m collectors.metron_market_data", description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--date", default=None, help="run date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--history", action="store_true", help="also write close/FX history artifacts")
    parser.add_argument("--reference", action="store_true", help="also write sectors/earnings artifacts")
    parser.add_argument("--macro", action="store_true", help="also write the macro-indicators artifact")
    parser.add_argument("--fundamentals", action="store_true", help="also write the tearsheet-fundamentals artifact")
    parser.add_argument("--analyst", action="store_true", help="also write the consensus-research artifact (free sources)")
    parser.add_argument("--sentiment", action="store_true", help="also write the news-sentiment artifact (held-universe slice of news_aggregates_daily)")
    parser.add_argument("--only-intraday", action="store_true",
                        help="write ONLY the intraday-quotes artifact (the 5-min timer entry; "
                             "no-op outside the NYSE session unless --force)")
    parser.add_argument("--require-heartbeat", action="store_true",
                        help="ALSO gate --only-intraday on a fresh Metron UI heartbeat (the "
                             "multi-tenant demand gate — OFF by default so the single-tenant owner "
                             "build stays warm every session tick)")
    parser.add_argument("--force", action="store_true",
                        help="bypass the market-window AND app-heartbeat gates for --only-intraday")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.only_intraday:
        intra = collect_intraday(bucket=args.bucket, dry_run=args.dry_run, force=args.force,
                                 require_heartbeat=args.require_heartbeat)
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
    if args.analyst:
        ana = collect_analyst(bucket=args.bucket, run_date=args.date, dry_run=args.dry_run)
        logger.info("[metron_market_data] analyst done: %s", ana)
        ok = ok and ana.get("status") in ("ok", "ok_dry_run", "skipped")
    if args.sentiment:
        sent = collect_sentiment(bucket=args.bucket, run_date=args.date, dry_run=args.dry_run)
        logger.info("[metron_market_data] sentiment done: %s", sent)
        ok = ok and sent.get("status") in ("ok", "ok_dry_run", "skipped")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
