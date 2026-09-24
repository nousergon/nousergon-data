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
SSGA-sourced membership list.

SECTOR FALLBACK + COMPLETENESS GATE (alpha-engine-config-I11468): Wikipedia
lags brand-new index ADDITIONS the same way it lagged JHG/BLD's removal. Until
I11468 ``collect()`` tolerated up to 10 such members with a log warning and
published them WITHOUT a ``sector_map`` entry, so the 2026-09-23 S&P 400 adds
(AGNC, CORT, EAT, HUBS) reached signals as sector "Unknown" and
ChallengerShadow refused the write. Now a member with no Wikipedia GICS row is
classified from yfinance ``Ticker.info`` (sector + industry, mapped onto GICS
by ``_YF_SECTOR_TO_GICS`` / ``_YF_INDUSTRY_TO_GICS``), with each fallback
recorded in the published ``sector_fallback`` field. Any member that is STILL
unclassified after that raises ``SectorCoverageIncomplete`` before anything is
written. There is no tolerance: every published constituent has a sector.

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
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO, StringIO
from pathlib import Path

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)


class ConstituentsUnavailable(RuntimeError):
    """Neither the live constituent feed nor the local cache could be read.

    A distinct type, not a bare ``RuntimeError``, so a caller that genuinely
    wants to soft-fail can catch exactly this and nothing else — the fleet's
    fail-loud default stays the default, and opting out is explicit and
    narrow rather than an ``except Exception`` that also swallows a schema
    change or a permissions error (alpha-engine-config-I7435).
    """


# Weight units in the SSGA holdings file are PERCENT (e.g. 7.12 for 7.12%),
# not fractions. Nothing in the file declares that, so units are DETECTED from
# the raw sum: a units flip upstream would otherwise be silently renormalised
# away and every downstream contribution would be off by 100x while still
# summing to 1.0 (alpha-engine-config-I11295).
#
# Detection is by BAND, not by a single floor, and a sum in NEITHER band
# raises. A floor alone (">= 50 means percent, else fractions") reads a
# broken percent file summing to 30 as fractions summing to 30 — nonsense in
# either unit, accepted as one of them. There is no third reading of a weight
# column worth guessing at.
#
# The bands are not centred on 1.0 / 100.0 because an EQUITY-only roster
# never sums to the whole index: cash, futures and settlement rows carry real
# index weight and are dropped by _SSGA_TICKER_RE, so ~97-99.5% is the normal
# observed sum. Below the lower bound the parse or the filter is wrong.
_WEIGHT_SUM_BANDS: tuple[tuple[str, float, float], ...] = (
    ("percent", 90.0, 101.0),
    ("fraction", 0.90, 1.01),
)


@dataclass(frozen=True)
class SsgaWeights:
    """Per-constituent index weight, as published by the fund's own holdings file.

    ``weight_map`` values are FRACTIONS normalised to sum to 1.0 **within each
    index**, so a ticker's weight is relative to its own index and not to the
    combined S&P 500 + S&P 400 roster. ``index_of`` names that index, which is
    what makes the normalisation interpretable — a consumer renormalising or
    filtering to one index must not have to re-derive membership from counts
    (the ordering-contract failure mode of alpha-engine-config-I6946).

    ``raw_sum_by_index`` is the PRE-normalisation sum in the file's own units,
    kept so a units flip or a layout drift is visible rather than absorbed.

    ``method`` records provenance: ``ssga_holdings_file`` when weights came
    from the live file, ``cache_no_weights`` when the local cache was served
    and carries none. A consumer must never read an absent weight as zero.
    """

    weight_map: dict[str, float] = field(default_factory=dict)
    index_of: dict[str, str] = field(default_factory=dict)
    raw_sum_by_index: dict[str, float] = field(default_factory=dict)
    method: str = "cache_no_weights"


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

class SectorCoverageIncomplete(RuntimeError):
    """A constituent has no GICS sector after every classification source.

    alpha-engine-config-I11468. Raised BEFORE any S3 write. A constituent
    published without a sector is not a smaller correct answer: it reaches
    signals as sector "Unknown", which the executor's sector caps cannot size
    against, and ChallengerShadow refuses the whole write over it.
    """


# yfinance ``info['sector']`` → GICS sector name (alpha-engine-config-I11468).
# yfinance's sector taxonomy is Morningstar-derived, not GICS: the names
# differ for six of the eleven sectors, so the mapping is spelled out and a
# yfinance sector NOT listed here is treated as unclassified (raise), never
# passed through or guessed. Measured 2026-09-24 against the 898 constituents
# that carry both a Wikipedia GICS sector and a yfinance sector: this table
# plus the industry overrides below agree with GICS for ~96% of names.
_YF_SECTOR_TO_GICS: dict[str, str] = {
    "Technology": "Information Technology",
    "Healthcare": "Health Care",
    "Financial Services": "Financials",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Communication Services": "Communication Services",
    "Industrials": "Industrials",
    "Energy": "Energy",
    "Utilities": "Utilities",
    "Real Estate": "Real Estate",
    "Basic Materials": "Materials",
}

# yfinance ``info['industry']`` values whose GICS sector differs from the
# sector-level mapping above. Each was UNANIMOUS in the 2026-09-24 measurement:
# "REIT - Mortgage" (yfinance: Real Estate) is Financials under GICS since the
# 2023 reclassification (2 of 2, and AGNC's case), and "Packaging &
# Containers" (yfinance: Consumer Cyclical) is GICS Materials (11 of 11).
_YF_INDUSTRY_TO_GICS: dict[str, str] = {
    "REIT - Mortgage": "Financials",
    "Packaging & Containers": "Materials",
}

# Pause between the fallback's per-ticker yfinance calls. Same value and
# rationale as collectors/universe_classification.py (avoids HTTP 429). The
# fallback only runs for members Wikipedia has not classified yet — a
# handful in a week with index adds, zero otherwise.
_YF_FALLBACK_DELAY_SECS = 0.4


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

    (
        tickers, sector_map, sector_etf_map, sub_industry_map,
        sp500_count, sp400_count, weights,
    ) = _fetch_constituents()

    if not tickers:
        return {"status": "error", "error": "No tickers fetched"}

    # alpha-engine-config-I11468: members Wikipedia has not classified yet
    # (addition-lag) get a sector from yfinance; then EVERY member must have
    # both a GICS sector and a sector ETF, or nothing is written.
    sector_fallback = _fill_missing_sectors(tickers, sector_map, sector_etf_map)
    _assert_full_sector_coverage(tickers, sector_map, sector_etf_map, sector_fallback)

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

    # Explicit per-index rosters (alpha-engine-config-I6946). `tickers` is the
    # SPY batch followed by the MDY batch, so the S&P 500 slice was recoverable
    # only as `tickers[:sp500_count]` — an ORDERING contract that nothing
    # declared and no test held. historical_constituents now diffs these
    # snapshots to derive index membership changes, which makes a silent
    # reordering here a burst of fabricated index churn there. Naming the two
    # lists retires the dependency for every snapshot written from now on;
    # `_sp500_roster` falls back to the prefix for the ones already on S3.
    #
    # The slice is only sound when the counts account for the whole list —
    # `tickers` is deduped, so an overlap between the two funds would shorten
    # it without moving either count. When they disagree the keys are OMITTED
    # rather than written wrong: a reader that finds them absent falls back to
    # the same guarded prefix, where a reader that finds them WRONG has no way
    # to tell.
    per_index: dict[str, list[str]] = {}
    if sp500_count + sp400_count == len(tickers):
        per_index = {
            "sp500_tickers": tickers[:sp500_count],
            "sp400_tickers": tickers[sp500_count:],
        }
    else:
        logger.warning(
            "constituents: sp500_count(%d) + sp400_count(%d) != len(tickers)(%d) "
            "— omitting sp500_tickers/sp400_tickers rather than slicing on a "
            "count that does not describe this list (config-I6946)",
            sp500_count, sp400_count, len(tickers),
        )

    result = {
        "date": run_date,
        "tickers": tickers,
        **per_index,
        "sector_map": sector_map,
        "sector_etf_map": sector_etf_map,
        "sub_industry_map": sub_industry_map,
        "sub_sector_etf_map": sub_sector_etf_map,
        # Members whose sector came from the yfinance fallback rather than
        # Wikipedia's GICS table, with the raw yfinance evidence
        # (alpha-engine-config-I11468). Empty in a week with no index adds.
        "sector_fallback": sector_fallback,
        "sp500_count": sp500_count,
        "sp400_count": sp400_count,
        "total_count": len(tickers),
        # Per-constituent index weight (alpha-engine-config-I11295). Fractions
        # normalised WITHIN each index, so `index_of` is what makes a value
        # interpretable and travels with it. `weight_method` is provenance a
        # consumer must honour rather than infer: on a cache-served run it
        # reads `cache_no_weights` and `weight_map` is empty, which means
        # UNKNOWN weight, never zero weight.
        "weight_map": weights.weight_map,
        "index_of": weights.index_of,
        "weight_method": weights.method,
        "weight_sum_raw_sp500": weights.raw_sum_by_index.get("S&P 500"),
        "weight_sum_raw_sp400": weights.raw_sum_by_index.get("S&P 400"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        logger.info(
            "[dry-run] constituents: %d tickers (%d S&P500, %d S&P400), "
            "%d sector mappings, %d weights (%s)",
            len(tickers), sp500_count, sp400_count, len(sector_etf_map),
            len(weights.weight_map), weights.method,
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
    # alpha-engine-config-I10898: the three dual-path maps above are six of
    # D01's eight published keys, and the run manifest has to record every one
    # of them (I10855) or the dispatcher's completion check fails the unit for
    # a key it declares and does not report. Their sizes are returned here so
    # the caller's `extra_outputs` can record a real `rows_out` rather than a
    # zero that the empty-but-fresh guard would then read as a fresh empty
    # write.
    return {
        "status": "ok",
        "count": len(tickers),
        "tickers": tickers,
        "sector_map_count": len(sector_etf_map),
        "sector_fallback_count": len(sector_fallback),
        "sub_industry_map_count": len(sub_industry_map),
        "sub_sector_etf_map_count": len(sub_sector_etf_map),
        "weight_map_count": len(weights.weight_map),
        "weight_method": weights.method,
    }


def _yfinance_classification(tickers: list[str]) -> dict[str, dict[str, str]]:
    """Fetch ``{ticker: {"sector", "industry"} | {"error"}}`` from yfinance.

    One ``Ticker.info`` call per ticker. A failure is RECORDED per ticker as
    ``{"error": ...}`` — never dropped — so ``_assert_full_sector_coverage``
    can name why a member stayed unclassified. SSGA spells share classes with
    a dot (``BRK.B``); yfinance wants a dash (``BRK-B``).
    """
    from nousergon_lib.yfinance_quiet import quiet_yfinance

    try:
        import yfinance as yf
    except ImportError as exc:
        return {t: {"error": f"yfinance not importable: {exc}"} for t in tickers}

    out: dict[str, dict[str, str]] = {}
    with quiet_yfinance():
        for i, ticker in enumerate(tickers):
            if i > 0 and _YF_FALLBACK_DELAY_SECS > 0:
                time.sleep(_YF_FALLBACK_DELAY_SECS)
            try:
                info = yf.Ticker(ticker.replace(".", "-")).info or {}
            except Exception as exc:
                out[ticker] = {"error": f"{type(exc).__name__}: {exc}"}
                continue
            out[ticker] = {
                "sector": str(info.get("sector") or "").strip(),
                "industry": str(info.get("industry") or "").strip(),
            }
    return out


def _yf_to_gics(sector: str, industry: str) -> str | None:
    """Map a yfinance sector/industry pair onto a GICS sector, or None.

    Industry overrides win; an unknown or empty sector is None (unclassified),
    never passed through — a non-GICS name would have no sector ETF and would
    be a new bucket the executor's sector caps do not know.
    """
    if industry in _YF_INDUSTRY_TO_GICS:
        return _YF_INDUSTRY_TO_GICS[industry]
    return _YF_SECTOR_TO_GICS.get(sector)


def _fill_missing_sectors(
    tickers: list[str],
    sector_map: dict[str, str],
    sector_etf_map: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Classify members missing from the Wikipedia pass via yfinance, in place.

    alpha-engine-config-I11468. Mutates ``sector_map`` / ``sector_etf_map``
    for every member yfinance can classify onto a GICS sector, and returns
    ``{ticker: evidence}`` for every member it TRIED: on success
    ``{"source": "yfinance", "sector", "yf_sector", "yf_industry"}``, on
    failure ``{"source": "yfinance", "error"}``. Only members lacking a
    sector are looked up, so a normal week makes no yfinance call at all.
    """
    missing = [t for t in tickers if t not in sector_map]
    if not missing:
        return {}
    logger.warning(
        "Sector mapping: %d of %d members have no Wikipedia GICS row (likely "
        "recent index additions) — classifying via yfinance fallback: %s",
        len(missing), len(tickers), missing,
    )
    evidence: dict[str, dict[str, str]] = {}
    for ticker, row in _yfinance_classification(missing).items():
        if "error" in row:
            evidence[ticker] = {"source": "yfinance", "error": row["error"]}
            continue
        gics = _yf_to_gics(row.get("sector", ""), row.get("industry", ""))
        if gics is None:
            evidence[ticker] = {
                "source": "yfinance",
                "error": (
                    f"no GICS mapping for yfinance sector={row.get('sector')!r} "
                    f"industry={row.get('industry')!r}"
                ),
            }
            continue
        sector_map[ticker] = gics
        sector_etf_map[ticker] = GICS_TO_ETF[gics]
        evidence[ticker] = {
            "source": "yfinance",
            "sector": gics,
            "yf_sector": row.get("sector", ""),
            "yf_industry": row.get("industry", ""),
        }
    logger.warning(
        "Sector fallback: classified %d of %d via yfinance: %s",
        sum("sector" in e for e in evidence.values()), len(missing),
        {t: e.get("sector") or e.get("error") for t, e in evidence.items()},
    )
    return evidence


def _assert_full_sector_coverage(
    tickers: list[str],
    sector_map: dict[str, str],
    sector_etf_map: dict[str, str],
    sector_fallback: dict[str, dict[str, str]],
) -> None:
    """Raise unless every member has a GICS sector AND a sector ETF.

    alpha-engine-config-I11468: ``len(sector_map) == len(tickers)`` is the
    producer's invariant, with no tolerance. Both maps are checked because
    they are published separately — ``sector_map`` inside constituents.json,
    ``sector_etf_map`` as ``data/sector_map.json`` for the feature store — and
    a member missing from either is the same "Unknown" sector downstream.
    """
    unmapped = [t for t in tickers if t not in sector_map or t not in sector_etf_map]
    if unmapped:
        reasons = {
            t: (sector_fallback.get(t) or {}).get("error", "no sector ETF for its GICS sector")
            for t in unmapped
        }
        raise SectorCoverageIncomplete(
            f"Sector mapping incomplete: {len(unmapped)} of {len(tickers)} "
            f"constituents have no GICS sector after the Wikipedia pass and the "
            f"yfinance fallback — refusing to publish constituents with an "
            f"'Unknown' sector (alpha-engine-config-I11468). Reasons (first 10): "
            f"{dict(list(reasons.items())[:10])}"
        )


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


def _fetch_ssga_membership() -> tuple[list[str], int, int, SsgaWeights]:
    """Fetch current S&P 500 + S&P 400 membership AND per-constituent weight
    from SPY/MDY's daily holdings files (config#2812 — see module docstring for
    why this replaced Wikipedia as the membership source).

    Returns (tickers, sp500_count, sp400_count, weights). Raises on any
    fetch/parse failure — caller falls back to the local cache.

    The ``Weight`` column has always been in the bytes this function downloads
    (see the schema comment on ``_SSGA_HOLDINGS_URLS``); it was read and
    discarded until alpha-engine-config-I11295. Contribution to an index's move
    is ``weight_at_prior_close x return``, so weight was the only absent term
    in any index-relative attribution — the returns side already exists
    full-population in ``collectors/daily_closes.py``.
    """
    tickers: list[str] = []
    sp500_count = 0
    sp400_count = 0
    weight_map: dict[str, float] = {}
    index_of: dict[str, str] = {}
    raw_sum_by_index: dict[str, float] = {}
    for index_name, url in _SSGA_HOLDINGS_URLS.items():
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        # SSGA's holdings sheet has a 4-row banner (fund name/date/disclaimer)
        # above the real header row.
        df = pd.read_excel(BytesIO(resp.content), skiprows=4, engine="openpyxl")
        for required in ("Ticker", "Weight"):
            if required not in df.columns:
                raise RuntimeError(
                    f"SSGA holdings file for {index_name} missing {required!r} column "
                    f"(columns: {list(df.columns)}). Layout drift — extractor needs update."
                )
        raw_tickers = df["Ticker"].astype(str).str.strip()
        raw_weights = pd.to_numeric(df["Weight"], errors="coerce")
        # Filter and weight-key in ONE pass over the same rows, so a ticker and
        # its weight can never come from different rows. Share-class dot →
        # hyphen matches the yfinance convention the rest of the pipeline
        # expects (was also done for the Wikipedia source).
        batch: list[str] = []
        batch_weights: dict[str, float] = {}
        for raw_ticker, raw_weight in zip(raw_tickers, raw_weights):
            if not _SSGA_TICKER_RE.match(raw_ticker):
                continue
            ticker = raw_ticker.replace(".", "-")
            batch.append(ticker)
            if pd.notna(raw_weight):
                # A ticker appearing twice in one fund's file (share classes are
                # distinct tickers, so this would be a genuine duplicate row)
                # accumulates rather than overwrites.
                batch_weights[ticker] = batch_weights.get(ticker, 0.0) + float(raw_weight)
        if not batch:
            raise RuntimeError(
                f"SSGA holdings file for {index_name} yielded zero valid tickers "
                f"after filtering ({len(raw_tickers)} raw rows) — parse likely broken."
            )
        missing_weight = [t for t in batch if t not in batch_weights]
        if missing_weight:
            raise RuntimeError(
                f"SSGA holdings file for {index_name}: {len(missing_weight)} of "
                f"{len(batch)} member rows carry no numeric Weight "
                f"(sample: {missing_weight[:10]}). A member with no weight cannot "
                f"be read as zero weight — refusing to publish a partial roster."
            )
        raw_sum = sum(batch_weights.values())
        # Units are DETECTED, never assumed — see _WEIGHT_SUM_BANDS.
        units = next(
            (name for name, lo, hi in _WEIGHT_SUM_BANDS if lo <= raw_sum <= hi),
            None,
        )
        if units is None:
            raise RuntimeError(
                f"SSGA holdings file for {index_name}: equity weights sum to "
                f"{raw_sum!r}, which is neither percent "
                f"({_WEIGHT_SUM_BANDS[0][1]}-{_WEIGHT_SUM_BANDS[0][2]}) nor "
                f"fractions ({_WEIGHT_SUM_BANDS[1][1]}-{_WEIGHT_SUM_BANDS[1][2]}) "
                f"over {len(batch_weights)} equity rows. Refusing to guess the "
                f"units of a weight column — the parse, the equity filter or the "
                f"file's own units have changed."
            )
        # Normalise WITHIN the index. Cross-index normalisation would make a
        # ticker's weight depend on the other fund's roster, which is not what
        # 'weight in the S&P 500' means.
        normalised = {t: w / raw_sum for t, w in batch_weights.items()}
        tickers.extend(batch)
        weight_map.update(normalised)
        index_of.update({t: index_name for t in batch})
        raw_sum_by_index[index_name] = raw_sum
        logger.info(
            "Fetched %d tickers from %s (SSGA %s holdings), weights raw sum "
            "%.4f (detected units: %s)",
            len(batch), index_name, "SPY" if index_name == "S&P 500" else "MDY",
            raw_sum, units,
        )
        if index_name == "S&P 500":
            sp500_count = len(batch)
        else:
            sp400_count = len(batch)
    weights = SsgaWeights(
        weight_map=weight_map,
        index_of=index_of,
        raw_sum_by_index=raw_sum_by_index,
        method="ssga_holdings_file",
    )
    # dedupe, preserve order
    return list(dict.fromkeys(tickers)), sp500_count, sp400_count, weights


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
    list[str], dict[str, str], dict[str, str], dict[str, str], int, int, SsgaWeights
]:
    """
    Fetch constituent membership from SSGA (SPY/MDY holdings) and GICS
    sector/sub-industry classification from Wikipedia (config#2812).

    Returns:
        (tickers, sector_map, sector_etf_map, sub_industry_map, sp500_count,
         sp400_count, weights)
        - tickers: SSGA-sourced S&P 500 + S&P 400 membership (ground truth)
        - sector_map: {ticker: GICS_sector_name}, filtered to ``tickers``
        - sector_etf_map: {ticker: sector_ETF_symbol}, filtered to ``tickers``
        - sub_industry_map: {ticker: GICS_sub_industry_name} (best-effort,
          additive — a ticker missing here does not block collect()).
        - weights: SsgaWeights — per-constituent index weight, normalised
          within each index (alpha-engine-config-I11295). The 7th element was
          APPENDED rather than folded into an existing map, so every in-repo
          unpack site fails loudly on the shape change instead of silently
          binding the wrong value.
    """
    try:
        tickers, sp500_count, sp400_count, weights = _fetch_ssga_membership()
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
            # Cached weights are deliberately served as `cache_no_weights` on
            # read (see _load_from_cache): a stale weight presented as current
            # is worse than an absent one, because a consumer cannot tell.
            # They are written anyway so a cache-served run can still be
            # diagnosed after the fact.
            "index_weight": [weights.weight_map.get(t, "") for t in tickers],
            "index_name": [weights.index_of.get(t, "") for t in tickers],
        }).to_csv(_CACHE_PATH, index=False)

        return (
            tickers, sector_map, sector_etf_map, sub_industry_map,
            sp500_count, sp400_count, weights,
        )

    except Exception as e:
        logger.warning("Constituents fetch failed (%s); trying local cache...", e)
        try:
            return _load_from_cache()
        except ConstituentsUnavailable as cache_exc:
            # BOTH sources are gone. Chaining is the whole point: raising the
            # cache miss alone would name the symptom and bury the trigger,
            # and on 2026-08-15 the trigger was a missing `openpyxl` — an
            # environment defect that reads nothing like "no cache found"
            # (alpha-engine-config-I7435).
            raise ConstituentsUnavailable(
                f"constituents unavailable: live fetch failed ({e!r}) AND the "
                f"local cache fallback failed ({cache_exc})"
            ) from e


def _load_from_cache() -> tuple[
    list[str], dict[str, str], dict[str, str], dict[str, str], int, int, SsgaWeights
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
        # An empty universe is never a legitimate return value here. Returning
        # one made a total outage indistinguishable from a real result: on
        # 2026-08-15 the caller logged "Wikipedia constituents: 0 tickers" at
        # INFO, and a drift check comparing 0 against anything either passes
        # vacuously or reports drift whose real cause is a missing dependency
        # (alpha-engine-config-I7435).
        raise ConstituentsUnavailable(
            f"no local constituents cache at {_CACHE_PATH} — cannot build "
            "universe, and an empty universe is not a result"
        )
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
    # Weights are NOT served from the cache. The cache exists for a source
    # outage, and a weight is only meaningful as of a date: contribution is
    # `weight_at_prior_close x return`, so yesterday's weight presented as
    # today's is a wrong answer wearing a right one's clothes. An absent
    # weight is declarable and a consumer can refuse; a stale one cannot be
    # detected downstream. `cache_no_weights` says so out loud.
    return (
        tickers, sector_map, sector_etf_map, sub_industry_map, 0, 0,
        SsgaWeights(method="cache_no_weights"),
    )


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
