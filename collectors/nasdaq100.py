"""
nasdaq100.py — Fetch Nasdaq-100 (NDX) membership and per-constituent weight
(alpha-engine-config-I11296). Sibling artifact to ``constituents.py`` (S&P
500 / S&P 400), not folded into it — see module docstring below for why.

Writes ``market_data/index_constituents/NDX.json`` (the consumer-facing
"latest" path `alpha-engine-config-I11297` reads) AND a dated snapshot at
``market_data/weekly/{date}/NDX.json`` (point-in-time membership history,
mirroring ``constituents.py``'s dated-snapshot convention without touching
``constituents.json`` itself or its S&P PIT replay in
``historical_constituents.py``).

SOURCE LADDER (recorded per run in ``weight_method``):

1. Invesco QQQ's own daily holdings file — the full-replication fund's own
   published holdings, the exact analogue of the SSGA/SPY approach already
   trusted for S&P 500 (config#2812; ``constituents.py``'s module
   docstring). MEASURED 2026-09-21: the Invesco host returns HTTP 406 on
   every holdings/product-detail/product-page URL tried, with a current
   desktop-Chrome User-Agent, `Accept: */*`, a referer from the product
   page and `Accept-Language` set — host-level bot protection, not a wrong
   path or a missing header. Per alpha-engine-config-I11296, this is not
   escalated to headless-browser scraping; rung 2 is used instead and the
   skip is recorded in the payload as ``invesco_status``.
2. Nasdaq's own membership endpoint (``api.nasdaq.com/api/quote/list-type/
   nasdaq100``, verified live 2026-09-21: HTTP 200, 101 rows — NDX holds
   101 securities, dual share classes (e.g. GOOG/GOOGL) accounting for the
   101st) for MEMBERSHIP ONLY. Weights are computed from ``market_cap_raw``
   in this fleet's own licensed Finnhub fundamentals archive
   (``archive/fundamentals/{date}.json``, written by
   ``collectors/fundamentals.py::collect``) rather than from Nasdaq's
   scraped, comma-formatted ``marketCap`` string — deliberately, so the
   weight leg never depends on an unstable web-JSON numeric. Recorded as
   ``weight_method: "modified_cap_approx"``: NDX's real weighting scheme is
   MODIFIED market-cap (an annual reconstitution plus special rebalances
   that cap mega-cap concentration), which plain proportional cap-weighting
   does not reproduce — this is a genuine approximation, not the index's
   true methodology, and travels with the data via ``weight_method`` rather
   than being absorbed silently (see `alpha-engine-config-I11296` gotchas;
   the licensed-feed target state is `metron-ops-I24`, not bought).

   COVERAGE GAP: the fundamentals archive's universe is the fleet's
   ~900-ticker S&P 500 + S&P 400 roster. NDX-only names outside that roster
   (e.g. PDD, ARM) have no market-cap row there. Per I11296, that gap is
   MEASURED and DECLARED — never filled with a zero. Such tickers appear in
   ``tickers``/``index_of`` (they ARE members) but are omitted from
   ``weight_map`` and listed in ``unweighted_tickers``; ``weight_map`` sums
   to 1.0 only over the *weighted* subset, and ``raw_sum`` is that subset's
   pre-normalization market-cap total.
3. Local cache fallback (last-known membership only, no weights) —
   ``weight_method: "cache_no_weights"``, mirroring the fallback contract
   `alpha-engine-config-I11295` established for the S&P side.

DO NOT SOURCE THE INDEX VALUE. ``collectors/metron_market_data.py:175``
records that ``^NDX`` (like ``^GSPC``/``^IXIC``/``^RUT``) carries a separate
index license — this module only ever reads MEMBERSHIP lists (Nasdaq's own
JSON, Invesco's holdings file), never the index level itself.

A run yielding a constituent count outside [100, 102] raises rather than
publishing — 101 is normal (dual share class), but a hard 100 assumption
would break on it, and no plausible parse of NDX should yield <100 or >102.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import requests

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

_INVESCO_HOLDINGS_URL = (
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/"
    "holdings/0?audienceType=Investor&action=download&ticker=QQQ"
)
_INVESCO_PRODUCT_PAGE = (
    "https://www.invesco.com/us/financial-products/etfs/product-detail"
    "?productId=QQQ"
)
_NASDAQ100_URL = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"

_CACHE_PATH = Path(__file__).parent.parent / "data" / "nasdaq100_cache.csv"

_MIN_CONSTITUENT_COUNT = 100
_MAX_CONSTITUENT_COUNT = 102

_FUNDAMENTALS_ARCHIVE_PREFIX = "archive/fundamentals/"


class Nasdaq100Unavailable(RuntimeError):
    """Neither a live membership source nor the local cache could be read.

    A distinct type (mirroring ``constituents.ConstituentsUnavailable``) so
    a caller that genuinely wants to soft-fail can catch exactly this and
    nothing else — the fleet's fail-loud default stays the default.
    """


@dataclass(frozen=True)
class Nasdaq100Weights:
    """Per-constituent weight contract for NDX, mirroring the shape
    `alpha-engine-config-I11295` established for S&P: a normalised
    ``weight_map`` (fractions summing to 1.0 WITHIN the weighted subset),
    ``raw_sum`` recorded pre-normalisation, and a ``method`` provenance
    string. Unlike the S&P contract (full weight coverage expected), a
    member with no weight here does NOT raise — the fundamentals-archive
    coverage gap is a declared, measured limitation of rung 2 (see module
    docstring), not a data-integrity failure; such tickers are named in
    ``unweighted_tickers`` instead.
    """

    index_of: dict[str, str]
    weight_map: dict[str, float]
    raw_sum: float
    method: str
    unweighted_tickers: tuple[str, ...]


def _validate_membership_count(tickers: list[str]) -> None:
    n = len(tickers)
    if not (_MIN_CONSTITUENT_COUNT <= n <= _MAX_CONSTITUENT_COUNT):
        raise RuntimeError(
            f"Nasdaq-100 membership count {n} outside the expected "
            f"[{_MIN_CONSTITUENT_COUNT}, {_MAX_CONSTITUENT_COUNT}] band "
            "(101 is normal — dual share class; anything else is parse "
            "drift or a truncated response) — refusing to publish."
        )


def parse_nasdaq100_response(payload: dict) -> list[str]:
    """Parse ``api.nasdaq.com/api/quote/list-type/nasdaq100``'s JSON body
    into a sorted, deduplicated ticker list. Pure — no network.

    Raises ``RuntimeError`` on any structural drift (missing ``rows``,
    a row with no ``symbol``) or a member count outside [100, 102].
    """
    try:
        rows = payload["data"]["data"]["rows"]
    except (KeyError, TypeError) as e:
        raise RuntimeError(
            f"Nasdaq-100 endpoint response missing data.data.rows "
            f"(keys: {list(payload.keys()) if isinstance(payload, dict) else type(payload)}) "
            "— layout drift, extractor needs update."
        ) from e
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(
            "Nasdaq-100 endpoint response's data.data.rows is empty or not "
            "a list — layout drift or a truncated response."
        )
    tickers: list[str] = []
    for i, row in enumerate(rows):
        symbol = row.get("symbol") if isinstance(row, dict) else None
        if not symbol or not isinstance(symbol, str):
            raise RuntimeError(
                f"Nasdaq-100 endpoint row {i} missing a valid 'symbol' field "
                f"(row: {row!r}) — layout drift, extractor needs update."
            )
        tickers.append(symbol.strip().replace(".", "-"))
    tickers = sorted(dict.fromkeys(tickers))  # dedupe, stable order
    _validate_membership_count(tickers)
    return tickers


def _fetch_nasdaq100_membership() -> list[str]:
    """Fetch live NDX membership from Nasdaq's own endpoint (rung 2).
    Raises on any fetch/parse failure — caller falls back to cache."""
    resp = requests.get(_NASDAQ100_URL, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    tickers = parse_nasdaq100_response(payload)
    logger.info("Fetched %d tickers from Nasdaq-100 endpoint", len(tickers))
    return tickers


def _try_invesco_holdings() -> list[str] | None:
    """Rung 1: attempt Invesco QQQ's own daily holdings file, once, with a
    full browser header set + a session cookie picked up from the product
    page (per I11296: try once legitimately, never escalate to headless-
    browser scraping). Returns None (not raises) on refusal — this is an
    EXPECTED skip per the measured 2026-09-21 406, not a transient error;
    the caller records the outcome in the payload and moves to rung 2.
    """
    headers = dict(_HEADERS)
    headers["Accept"] = "*/*"
    headers["Referer"] = _INVESCO_PRODUCT_PAGE
    headers["Accept-Language"] = "en-US,en;q=0.9"
    try:
        session = requests.Session()
        session.headers.update(headers)
        session.get(_INVESCO_PRODUCT_PAGE, timeout=15)  # pick up any session cookie
        resp = session.get(_INVESCO_HOLDINGS_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.info(
            "Invesco QQQ holdings file refused (%s) — measured host-level bot "
            "protection (alpha-engine-config-I11296), falling to rung 2, not "
            "escalating to headless-browser scraping.",
            e,
        )
        return None
    # Reachable this run where it was not on 2026-09-21 — still parse
    # defensively rather than assume a stable schema we've never seen 200 for.
    raise NotImplementedError(
        "Invesco QQQ holdings file returned a non-error response for the "
        "first time since alpha-engine-config-I11296 was measured (406) — "
        "the rung-1 parser was never built against a live response. "
        "Investigate the response body and implement the parser before "
        "trusting this rung; do not silently fall through to rung 2 while "
        "leaving this unimplemented."
    )


def _latest_fundamentals_archive_key(bucket: str, s3_client: Any, run_date: str) -> str | None:
    """Find the most recent ``archive/fundamentals/{date}.json`` key at or
    before ``run_date`` (ISO dates sort lexically). ``fundamentals.py``
    writes no 'latest' pointer, so this lists the prefix rather than
    guessing a key. Returns None if the prefix is empty or unreachable.
    """
    try:
        resp = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=_FUNDAMENTALS_ARCHIVE_PREFIX
        )
    except Exception as e:
        logger.warning("Could not list %s: %s", _FUNDAMENTALS_ARCHIVE_PREFIX, e)
        return None
    keys = [
        obj["Key"] for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]
    eligible = [k for k in keys if k.rsplit("/", 1)[-1][:10] <= run_date]
    if not eligible:
        return None
    return max(eligible)


def _load_market_caps(bucket: str, s3_client: Any, run_date: str) -> tuple[dict[str, float], str | None]:
    """Load {ticker: market_cap_raw} from the latest fundamentals archive
    at or before ``run_date``. Returns ({}, None) if unreachable — the
    caller treats that as zero coverage, not a raise (membership is still
    valid rung-2 data even with no weight leg available)."""
    key = _latest_fundamentals_archive_key(bucket, s3_client, run_date)
    if key is None:
        return {}, None
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        results = json.loads(resp["Body"].read())
    except Exception as e:
        logger.warning("Could not read %s: %s", key, e)
        return {}, None
    market_caps = {
        ticker: float(rec["market_cap_raw"])
        for ticker, rec in results.items()
        if isinstance(rec, dict) and rec.get("market_cap_raw", 0) > 0
    }
    return market_caps, key


def compute_weights(
    tickers: list[str], market_caps: dict[str, float], method: str
) -> Nasdaq100Weights:
    """Normalise {ticker: raw_market_cap} to a weight_map summing to 1.0
    over the WEIGHTED subset. Pure — no network. Members with no market
    cap are named in ``unweighted_tickers`` rather than zero-filled (the
    coverage gap this module's docstring measures and declares)."""
    weighted = {t: market_caps[t] for t in tickers if t in market_caps}
    unweighted = tuple(t for t in tickers if t not in market_caps)
    raw_sum = sum(weighted.values())
    if raw_sum > 0:
        weight_map = {t: cap / raw_sum for t, cap in weighted.items()}
    else:
        weight_map = {}
    return Nasdaq100Weights(
        index_of={t: "NDX" for t in tickers},
        weight_map=weight_map,
        raw_sum=raw_sum,
        method=method,
        unweighted_tickers=unweighted,
    )


def _load_from_cache() -> list[str]:
    """Read the local membership-only cache (rung 3). Raises
    ``Nasdaq100Unavailable`` if no cache exists — an empty universe is
    never a legitimate return value (mirrors ``constituents._load_from_cache``:
    a total outage must not read as a valid zero-member result)."""
    if not _CACHE_PATH.exists():
        raise Nasdaq100Unavailable(
            f"no local NDX membership cache at {_CACHE_PATH} — cannot "
            "build fallback universe, and an empty universe is not a result"
        )
    import csv

    with open(_CACHE_PATH, newline="", encoding="utf-8") as f:
        tickers = [row["ticker"] for row in csv.DictReader(f)]
    if not tickers:
        raise Nasdaq100Unavailable(
            f"local NDX membership cache at {_CACHE_PATH} is empty"
        )
    logger.info("Loaded %d tickers from NDX membership cache", len(tickers))
    return tickers


def _save_to_cache(tickers: list[str]) -> None:
    import csv

    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_CACHE_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker"])
        writer.writeheader()
        for t in tickers:
            writer.writerow({"ticker": t})


def collect(
    bucket: str,
    s3_prefix: str = "market_data/",
    run_date: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Fetch Nasdaq-100 membership + per-constituent weight and write to S3.

    Returns a dict with status, counts, and (on success) the payload
    written. Ladder: Invesco holdings (rung 1, measured-refused) -> Nasdaq
    membership endpoint + fundamentals-archive weights (rung 2) -> local
    cache, no weights (rung 3).
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    s3 = boto3.client("s3")

    try:
        invesco_tickers = _try_invesco_holdings()
    except NotImplementedError as e:
        # Rung 1 returned a non-error response for the first time since
        # I11296 was measured (406) but no parser exists for it yet.
        # Surfaced loud rather than silently falling through to rung 2 —
        # see _try_invesco_holdings' docstring.
        return {"status": "error", "error": str(e)}
    invesco_status = "unavailable_406" if invesco_tickers is None else "ok"
    assert invesco_tickers is None, (
        "_try_invesco_holdings returned tickers without raising — its "
        "contract is None-or-raise until a rung-1 parser exists"
    )

    weights: Nasdaq100Weights | None = None
    method: str
    tickers: list[str]
    market_cap_source: str | None = None

    try:
        tickers = _fetch_nasdaq100_membership()
        market_caps, market_cap_source = _load_market_caps(bucket, s3, run_date)
        method = "modified_cap_approx"
        weights = compute_weights(tickers, market_caps, method)
        _save_to_cache(tickers)
    except Exception as e:
        logger.warning("Nasdaq-100 rung-2 fetch failed (%s); trying local cache...", e)
        try:
            tickers = _load_from_cache()
        except Nasdaq100Unavailable as cache_exc:
            raise Nasdaq100Unavailable(
                f"NDX membership unavailable: live fetch failed ({e!r}) AND "
                f"the local cache fallback failed ({cache_exc})"
            ) from e
        method = "cache_no_weights"
        weights = Nasdaq100Weights(
            index_of={t: "NDX" for t in tickers},
            weight_map={},
            raw_sum=0.0,
            method=method,
            unweighted_tickers=tuple(tickers),
        )

    _validate_membership_count(tickers)

    result = {
        "date": run_date,
        "index": "NDX",
        "tickers": tickers,
        "index_of": weights.index_of,
        "weight_map": weights.weight_map,
        "raw_sum": weights.raw_sum,
        "weight_method": weights.method,
        "constituent_count": len(tickers),
        "weighted_count": len(weights.weight_map),
        "unweighted_tickers": list(weights.unweighted_tickers),
        "market_cap_source": market_cap_source,
        "invesco_status": invesco_status,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        logger.info(
            "[dry-run] NDX: %d tickers, %d weighted (method=%s)",
            len(tickers), len(weights.weight_map), method,
        )
        return {
            "status": "ok_dry_run",
            "count": len(tickers),
            "weighted_count": len(weights.weight_map),
            "weight_method": method,
        }

    latest_key = f"{s3_prefix}index_constituents/NDX.json"
    dated_key = f"{s3_prefix}weekly/{run_date}/NDX.json"
    body = json.dumps(result, indent=2)
    for key in (latest_key, dated_key):
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    logger.info(
        "Wrote NDX.json to s3://%s/%s and s3://%s/%s (%d tickers, %d weighted, method=%s)",
        bucket, latest_key, bucket, dated_key, len(tickers), len(weights.weight_map), method,
    )

    return {
        "status": "ok",
        "count": len(tickers),
        "weighted_count": len(weights.weight_map),
        "weight_method": method,
        "s3_key": latest_key,
        "dated_s3_key": dated_key,
    }


def load_from_s3(bucket: str, s3_prefix: str = "market_data/") -> dict | None:
    """Load the latest NDX.json from S3. Returns None if not found."""
    s3 = boto3.client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=f"{s3_prefix}index_constituents/NDX.json")
        return json.loads(resp["Body"].read())
    except Exception:
        return None
