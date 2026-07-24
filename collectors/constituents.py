"""
constituents.py — Fetch S&P 500 + S&P 400 constituent membership from the
SSGA SPDR ETFs' daily holdings files (SPY / MDY), with GICS sector + GICS
sub-industry classification from Wikipedia.

Writes constituents.json to S3 with:
  - tickers: deduplicated list of ~900 symbols
  - sector_map: {ticker: GICS_sector_name}
  - sector_etf_map: {ticker: sector_ETF_symbol}
  - sub_industry_map: {ticker: GICS_sub_industry_name}
  - sp500_count, sp400_count, total_count, fetched_at

Falls back to a local CSV cache if either source is unreachable.

MEMBERSHIP SOURCE (config#2812, replaces Wikipedia-as-membership-source):
SPY and MDY are full-replication S&P 500 / S&P 400 index funds — the fund
manager (State Street/SSGA) is contractually required to hold the ACTUAL
current index constituents, and both publish their full holdings as a daily
xlsx, no auth. This is standard free/practical index-membership tracking
(the alternative — a licensed S&P Dow Jones Indices data feed — is the true
gold standard but a paid commercial subscription, overkill here). Verified
live 2026-07-17: JHG and BLD (delisted 2026-07-01 via take-private mergers)
had already dropped from BOTH SPY's and MDY's holdings, while Wikipedia's
community-edited constituents pages still listed both 17+ days later —
Wikipedia-membership-lag was the root cause of alpha-engine-config-I2703/
I2812 (the daily preopen pipeline's ArcticDB freshness gate hard-failing
every day on two tickers a Wikipedia-driven auto-prune could never catch,
since it requires the ticker to be ABSENT from the Wikipedia page first).

SECTOR SOURCE (unchanged): SPY/MDY's own "Sector" holdings column is NOT
usable GICS classification (verified live: >98% of SPY rows carry a literal
"-" placeholder, not a sector name) — Wikipedia's constituents tables remain
the sector/sub-industry source, keyed by ticker and looked up against the
SSGA-sourced membership list. A membership ticker absent from Wikipedia's
sector map still hard-fails in ``collect()`` (unchanged behavior) — this is
now a *useful* freshness signal in the opposite direction (Wikipedia lagging
on a brand-new ADDITION, which is much rarer and lower-impact than the
removal-lag this fix addresses, and surfaces loudly rather than silently).

``sub_industry_map`` (config#934 narrow slice, 2026-07-09): the Wikipedia
constituents tables already scraped here carry a "GICS Sub-Industry" column
alongside "GICS Sector" (that's *why* ``_select_constituents_table``'s sector
matcher has to exclude "sub" — both columns exist on the same table). This is
purely additive collector-side capture: best-effort, non-blocking (missing/
unmapped sub-industry does NOT raise, unlike the sector map's hard
completeness gate — sub-industry is not yet consumed by anything downstream).
The full cross-repo ask (sub-sector benchmark definitions, crucible-predictor
feature wiring, retrain) is separate, unstarted follow-on scope — see #934.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from io import BytesIO, StringIO
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

# GICS sub-industry name → sub-sector benchmark ETF symbol (config#934).
#
# INTENTIONALLY PARTIAL + EXTENSIBLE. This map only covers the well-known,
# liquid sub-industry ETF proxies where a sub-sector benchmark meaningfully
# differs from the parent GICS-sector ETF (e.g. Semiconductors → SMH is a
# far tighter benchmark for NVDA than the whole-Tech XLK). Any sub-industry
# NOT listed here falls back to the ticker's existing GICS-sector ETF (see
# _build_sub_sector_etf_map below), so the downstream sub-sector-relative
# feature is ALWAYS defined and, for an unmapped sub-industry, gracefully
# equals the sector-relative value. Add rows here (with a liquid ETF proxy)
# to make a sub-industry benchmark-distinct — no other code change needed.
#
# Keys must be the EXACT GICS sub-industry spellings that appear in the
# Wikipedia "GICS Sub-Industry" column captured by _fetch_constituents
# (which stores them verbatim into sub_industry_map). Getting a spelling
# wrong just means that sub-industry silently falls back to its sector ETF.
GICS_SUBINDUSTRY_TO_ETF: dict[str, str] = {
    "Semiconductors": "SMH",
    "Semiconductor Materials & Equipment": "SMH",
    "Application Software": "IGV",
    "Systems Software": "IGV",
    "Biotechnology": "XBI",
    "Pharmaceuticals": "PPH",
    "Oil & Gas Exploration & Production": "XOP",
    "Regional Banks": "KRE",
    "Aerospace & Defense": "ITA",
    "Gold": "GDX",
}

_CACHE_PATH = Path(__file__).parent.parent / "data" / "constituents_cache.csv"

# Membership ground truth: SSGA SPDR full-replication index funds' daily
# holdings (config#2812). Both hosted by the same provider with an identical
# schema (Name/Ticker/Identifier/SEDOL/Weight/Sector/Shares Held), no auth.
_SSGA_HOLDINGS_URLS = {
    "S&P 500": "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
    "S&P 400": "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx",
}

# Sector/sub-industry classification source (unchanged from pre-config#2812).
_WIKIPEDIA_URLS = {
    "S&P 500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
    "S&P 400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
}

_HEADERS = {"User-Agent": "alpha-engine-data/1.0 (weekly-collector)"}

# A real US equity ticker in the SSGA holdings file: 1-6 uppercase letters,
# optional single-letter share class suffix (e.g. BRK.A). Excludes the file's
# non-equity rows: cash positions ("-"/"999USDZ92", "CASH_USD"), tiny
# settlement/contra placeholder rows (CUSIP-shaped "ticker" values), and the
# trailing legal-disclaimer text block (NaN ticker).
_SSGA_TICKER_RE = re.compile(r"^[A-Z]{1,6}(\.[A-Z])?$")

# Sector-classification is now sourced independently of membership
# (config#2812), so a small number of SSGA-confirmed-current members can
# legitimately have no Wikipedia sector row yet — Wikipedia lags brand-new
# index ADDITIONS the same way it lagged JHG/BLD's REMOVAL, just verified
# live to be a much smaller/quieter gap (2 tickers, TOST + IESC, both
# recent legitimate additions, on the first live run of this fix). Warn
# loudly and proceed for a small gap; a gap this large signals a genuine
# parse/layout failure and still hard-fails, mirroring daily_append's
# missing-from-closes convention (small-N tolerated + alerted, not silently
# dropped, per feedback_no_silent_fails).
_UNMAPPED_SECTOR_HARD_FAIL_THRESHOLD = 10


def collect(
    bucket: str,
    s3_prefix: str = "market_data/",
    run_date: str | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Fetch S&P 500+400 membership from SSGA (SPY/MDY holdings) + GICS sector
    classification from Wikipedia, and write to S3.

    Returns dict with status, counts, and any errors.
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    tickers, sector_map, sector_etf_map, sub_industry_map, sp500_count, sp400_count = (
        _fetch_constituents()
    )

    if not tickers:
        return {"status": "error", "error": "No tickers fetched"}

    unmapped = [t for t in tickers if t not in sector_map]
    if len(unmapped) > _UNMAPPED_SECTOR_HARD_FAIL_THRESHOLD:
        raise RuntimeError(
            f"Sector mapping incomplete: {len(unmapped)} of {len(tickers)} tickers "
            f"missing GICS sector (exceeds the {_UNMAPPED_SECTOR_HARD_FAIL_THRESHOLD}-ticker "
            f"tolerance for Wikipedia addition-lag). Sample: {unmapped[:10]}. EOD reconcile "
            f"sector attribution depends on full coverage; aborting before write."
        )
    if unmapped:
        logger.warning(
            "Sector mapping: %d of %d tickers missing GICS sector (within the "
            "%d-ticker Wikipedia addition-lag tolerance) — likely recent index "
            "additions Wikipedia hasn't classified yet: %s",
            len(unmapped), len(tickers), _UNMAPPED_SECTOR_HARD_FAIL_THRESHOLD, unmapped,
        )
    # Sub-industry is additive/best-effort — NOT a hard gate like sector
    # above. Nothing downstream consumes it yet (config#934 narrow slice),
    # so a partial or empty sub_industry_map must not block the weekly
    # constituents write the way a missing sector would.

    # sub_sector_etf_map (config#934 forward step): ticker → sub-sector
    # benchmark ETF, defaulting to the ticker's sector ETF where the
    # sub-industry has no liquid proxy. Additive/best-effort like
    # sub_industry_map — derived purely from the two maps above, so it
    # cannot fail independently and never blocks the write.
    sub_sector_etf_map = _build_sub_sector_etf_map(
        tickers, sector_etf_map, sub_industry_map
    )

    result = {
        "date": run_date,
        "tickers": tickers,
        "sector_map": sector_map,
        "sector_etf_map": sector_etf_map,
        "sub_industry_map": sub_industry_map,
        "sub_sector_etf_map": sub_sector_etf_map,
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

    # Write sector_map.json to canonical data path + Wave-3 reference/
    # path. PR4 (config#780) retired the legacy predictor/price_cache/
    # write: the ticker-parquet side already writes reference/ only via
    # _price_cache_write_prefixes(), and this collector's own legacy
    # write was the one straggler still recreating the deleted prefix
    # on every weekly run.
    sector_map_body = json.dumps(sector_etf_map, indent=2, sort_keys=True)
    for sector_map_key in (
        "data/sector_map.json",
        "reference/price_cache/sector_map.json",
    ):
        s3.put_object(
            Bucket=bucket, Key=sector_map_key,
            Body=sector_map_body, ContentType="application/json",
        )
    logger.info(
        "Wrote sector_map.json to data/ and reference/ paths",
    )

    # Write sub_industry_map.json alongside sector_map.json (config#934
    # narrow slice) — same dual-path convention as above, so a future
    # consumer can pick it up from either location. Purely additive: no
    # reader exists yet (nothing in this repo or crucible-predictor is
    # wired to it), so this write cannot change any existing behavior.
    sub_industry_map_body = json.dumps(sub_industry_map, indent=2, sort_keys=True)
    for sub_industry_map_key in (
        "data/sub_industry_map.json",
        "reference/price_cache/sub_industry_map.json",
    ):
        s3.put_object(
            Bucket=bucket, Key=sub_industry_map_key,
            Body=sub_industry_map_body, ContentType="application/json",
        )
    logger.info(
        "Wrote sub_industry_map.json to data/ and reference/ paths",
    )

    # Write sub_sector_etf_map.json (config#934 forward step) — same
    # dual-path convention as sector_map.json / sub_industry_map.json above.
    # This IS consumed downstream (features/feature_engineer's
    # sub_sector_vs_benchmark_* + builders/daily_append), unlike the raw
    # sub_industry_map. Additive: written non-blocking (an empty map on a
    # Wikipedia layout drift degrades the sub-sector features to their
    # neutral default rather than failing the weekly write). The two new S3
    # paths need an ARTIFACT_REGISTRY.yaml grandfather (companion config PR,
    # same as config#2020 did for sub_industry_map).
    sub_sector_etf_map_body = json.dumps(sub_sector_etf_map, indent=2, sort_keys=True)
    for sub_sector_etf_map_key in (
        "data/sub_sector_etf_map.json",
        "reference/price_cache/sub_sector_etf_map.json",
    ):
        s3.put_object(
            Bucket=bucket, Key=sub_sector_etf_map_key,
            Body=sub_sector_etf_map_body, ContentType="application/json",
        )
    logger.info(
        "Wrote sub_sector_etf_map.json to data/ and reference/ paths",
    )

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


def _fetch_ssga_membership() -> tuple[list[str], int, int]:
    """Fetch current S&P 500 + S&P 400 membership from SPY/MDY's daily
    holdings files (config#2812 — see module docstring for why this replaced
    Wikipedia as the membership source).

    Returns (tickers, sp500_count, sp400_count). Raises on any fetch/parse
    failure — caller falls back to the local cache.
    """
    tickers: list[str] = []
    sp500_count = 0
    sp400_count = 0
    for index_name, url in _SSGA_HOLDINGS_URLS.items():
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        # SSGA's holdings sheet has a 4-row banner (fund name/date/disclaimer)
        # above the real header row.
        df = pd.read_excel(BytesIO(resp.content), skiprows=4, engine="openpyxl")
        if "Ticker" not in df.columns:
            raise RuntimeError(
                f"SSGA holdings file for {index_name} missing 'Ticker' column "
                f"(columns: {list(df.columns)}). Layout drift — extractor needs update."
            )
        raw_tickers = df["Ticker"].astype(str).str.strip()
        batch = [t for t in raw_tickers if _SSGA_TICKER_RE.match(t)]
        # BRK.A/BRK.B style share-class dot → hyphen, matching the yfinance
        # convention the rest of the pipeline expects (was also done for the
        # Wikipedia source).
        batch = [t.replace(".", "-") for t in batch]
        if not batch:
            raise RuntimeError(
                f"SSGA holdings file for {index_name} yielded zero valid tickers "
                f"after filtering ({len(raw_tickers)} raw rows) — parse likely broken."
            )
        tickers.extend(batch)
        logger.info("Fetched %d tickers from %s (SSGA %s holdings)",
                    len(batch), index_name, "SPY" if index_name == "S&P 500" else "MDY")
        if index_name == "S&P 500":
            sp500_count = len(batch)
        else:
            sp400_count = len(batch)
    return list(dict.fromkeys(tickers)), sp500_count, sp400_count  # dedupe, preserve order


def _fetch_wikipedia_sectors() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Fetch GICS sector + sub-industry classification from Wikipedia's
    constituents tables, keyed by ticker (config#2812 — Wikipedia is now
    classification-only; see module docstring). Returns maps for every
    ticker Wikipedia currently lists, regardless of SSGA membership; the
    caller filters to the SSGA-sourced membership list.

    Returns (sector_map, sector_etf_map, sub_industry_map). Raises on any
    fetch/parse failure or missing sector column — caller falls back to the
    local cache.
    """
    sector_map: dict[str, str] = {}
    sector_etf_map: dict[str, str] = {}
    sub_industry_map: dict[str, str] = {}

    for index_name, url in _WIKIPEDIA_URLS.items():
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
        logger.info("Fetched %d tickers from %s (Wikipedia sector classification)",
                    len(batch), index_name)

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

        # GICS Sub-Industry column (config#934 narrow slice) — same
        # table, one level finer than sector (e.g. "Semiconductors" /
        # "Application Software" vs. the parent "Information
        # Technology" sector). Best-effort: unlike sector above, a
        # missing sub-industry column does NOT raise — nothing
        # downstream depends on this yet, so a Wikipedia layout
        # change here should degrade gracefully rather than block
        # the weekly constituents write.
        sub_industry_col = next(
            (c for c in df.columns if "gics" in str(c).lower() and "sub" in str(c).lower()
             and "industry" in str(c).lower()),
            None,
        )
        if sub_industry_col is not None:
            for ticker, sub_industry in zip(
                batch, df[sub_industry_col].astype(str).tolist()
            ):
                sub_industry_name = sub_industry.strip()
                if sub_industry_name and sub_industry_name.lower() != "nan":
                    sub_industry_map[ticker] = sub_industry_name
            logger.info(
                "[%s] Sub-industry map: running total %d",
                index_name, len(sub_industry_map),
            )
        else:
            logger.warning(
                "[%s] GICS Sub-Industry column missing (columns: %s) — "
                "sub_industry_map will be incomplete for this index.",
                index_name, list(df.columns),
            )

    return sector_map, sector_etf_map, sub_industry_map


def _fetch_constituents() -> tuple[
    list[str], dict[str, str], dict[str, str], dict[str, str], int, int
]:
    """
    Fetch constituent membership from SSGA (SPY/MDY holdings) and GICS
    sector/sub-industry classification from Wikipedia (config#2812).

    Returns:
        (tickers, sector_map, sector_etf_map, sub_industry_map, sp500_count, sp400_count)
        - tickers: SSGA-sourced S&P 500 + S&P 400 membership (ground truth)
        - sector_map: {ticker: GICS_sector_name}, filtered to ``tickers``
        - sector_etf_map: {ticker: sector_ETF_symbol}, filtered to ``tickers``
        - sub_industry_map: {ticker: GICS_sub_industry_name} (best-effort,
          additive — a ticker missing here does not block collect()).
    """
    try:
        tickers, sp500_count, sp400_count = _fetch_ssga_membership()
        wiki_sector_map, wiki_sector_etf_map, wiki_sub_industry_map = _fetch_wikipedia_sectors()

        # Filter the Wikipedia-derived maps down to SSGA's membership list —
        # a ticker Wikipedia still lists but SSGA has already dropped (the
        # exact I2703/I2812 failure mode) must not leak into the output.
        member_set = set(tickers)
        sector_map = {t: s for t, s in wiki_sector_map.items() if t in member_set}
        sector_etf_map = {t: e for t, e in wiki_sector_etf_map.items() if t in member_set}
        sub_industry_map = {t: s for t, s in wiki_sub_industry_map.items() if t in member_set}

        # Update local cache with full sector mapping so a future source
        # outage doesn't dead-end on the empty-sector-map raise in collect().
        # Prior cache stored only ticker symbols; the 2026-05-11 partial
        # outage exposed that gap (S&P 500 fetch succeeded, S&P 400 failed,
        # fallback returned 903 symbols with zero sector data → raise).
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "ticker": tickers,
            "gics_sector": [sector_map.get(t, "") for t in tickers],
            "sector_etf": [sector_etf_map.get(t, "") for t in tickers],
            "gics_sub_industry": [sub_industry_map.get(t, "") for t in tickers],
        }).to_csv(_CACHE_PATH, index=False)

        return tickers, sector_map, sector_etf_map, sub_industry_map, sp500_count, sp400_count

    except Exception as e:
        logger.warning("Constituents fetch failed (%s); trying local cache...", e)
        return _load_from_cache()


def _load_from_cache() -> tuple[
    list[str], dict[str, str], dict[str, str], dict[str, str], int, int
]:
    """Read the local cache and reconstruct ticker list + sector maps.

    Backwards-compatible with the legacy ticker-only cache schema: missing
    gics_sector / sector_etf / gics_sub_industry columns return empty dicts
    (missing gics_sector then trips collect()'s `Sector mapping incomplete`
    raise — failing loud rather than writing constituents.json with missing
    sector data; a missing/empty sub_industry_map does NOT raise, since it's
    additive and not yet consumed downstream).
    """
    if not _CACHE_PATH.exists():
        logger.error("No cache found — cannot build universe")
        return [], {}, {}, {}, 0, 0
    df = pd.read_csv(_CACHE_PATH)
    tickers = df["ticker"].astype(str).tolist()
    sector_map: dict[str, str] = {}
    sector_etf_map: dict[str, str] = {}
    sub_industry_map: dict[str, str] = {}
    if "gics_sector" in df.columns:
        for ticker, sector in zip(tickers, df["gics_sector"].astype(str).tolist()):
            sector = sector.strip()
            if sector and sector.lower() != "nan":
                sector_map[ticker] = sector
    if "sector_etf" in df.columns:
        for ticker, etf in zip(tickers, df["sector_etf"].astype(str).tolist()):
            etf = etf.strip()
            if etf and etf.lower() != "nan":
                sector_etf_map[ticker] = etf
    if "gics_sub_industry" in df.columns:
        for ticker, sub_industry in zip(tickers, df["gics_sub_industry"].astype(str).tolist()):
            sub_industry = sub_industry.strip()
            if sub_industry and sub_industry.lower() != "nan":
                sub_industry_map[ticker] = sub_industry
    logger.info(
        "Loaded %d tickers from cache (sector_map=%d, sector_etf_map=%d, sub_industry_map=%d)",
        len(tickers), len(sector_map), len(sector_etf_map), len(sub_industry_map),
    )
    return tickers, sector_map, sector_etf_map, sub_industry_map, 0, 0


def _build_sub_sector_etf_map(
    tickers: list[str],
    sector_etf_map: dict[str, str],
    sub_industry_map: dict[str, str],
) -> dict[str, str]:
    """Build {ticker: sub-sector ETF symbol} (config#934).

    For each ticker, pick a sub-sector benchmark ETF from its GICS
    sub-industry via ``GICS_SUBINDUSTRY_TO_ETF``. When the sub-industry
    has no liquid ETF proxy (unmapped, or the ticker has no sub-industry
    captured at all), FALL BACK to the ticker's existing sector ETF from
    ``sector_etf_map`` — so the map is always defined for any ticker that
    has a sector ETF, and an unmapped sub-industry gracefully resolves to
    the same benchmark the sector-relative feature already uses.

    Best-effort/additive, mirroring ``sub_industry_map``: a ticker with no
    sector ETF (rare — sector coverage is a hard gate in ``collect``) and
    no sub-industry proxy is simply omitted rather than raising.
    """
    sub_sector_etf_map: dict[str, str] = {}
    for ticker in tickers:
        sub_industry = sub_industry_map.get(ticker)
        etf = GICS_SUBINDUSTRY_TO_ETF.get(sub_industry) if sub_industry else None
        if not etf:
            etf = sector_etf_map.get(ticker)
        if etf:
            sub_sector_etf_map[ticker] = etf
    return sub_sector_etf_map


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
