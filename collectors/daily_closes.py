"""
collectors/daily_closes.py — Daily OHLCV archive for all tracked tickers.

Writes one parquet per trading day at staging/daily_closes/{date}.parquet.
The parquet is the in-flight checkpoint between the API fetch (polygon /
FRED / yfinance) and ArcticDB ingest by ``builders/daily_append.py``.
Lives under ``staging/`` (not ``predictor/``) because it is intermediate
state, not authoritative storage — the canonical home for daily OHLCV
is ArcticDB universe library. S3 lifecycle policy on ``staging/`` expires
parquets after 7 days; the parquet's only role is restartability when
daily_append fails after the upstream fetch succeeded.

Two collection modes (selected via the ``source`` parameter):

  * ``yfinance_only`` (EOD pass, ~1:05 PM PT) — same-day OHLCV via yfinance
    for stocks + FRED for the 4 index tickers (VIX/VIX3M/TNX/IRX). Polygon
    is skipped entirely because free tier returns 403 "before end of day"
    for same-day grouped-daily. ``VWAP`` writes as ``None`` for everything;
    the morning enrichment fills it. Hard-fails on yfinance failure.

  * ``polygon_only`` (morning pass, ~5:30 AM PT next trading day) —
    polygon.io grouped-daily for stocks (with VWAP) + FRED for indices.
    ``PolygonForbiddenError`` propagates loudly — no yfinance fallback
    masks the failure. When an existing parquet is being overwritten
    (the yfinance EOD pass wrote first), per-ticker Close discrepancy
    is logged so corporate-action drift / data-quality issues are visible.
    With ``skip_if_canonical=True`` (window mode) the polygon side skips
    dates already fully polygon-canonical EXCEPT those a recent split
    restated — see ``collect``'s ``skip_if_canonical`` doc (config#717).

  * ``auto`` (default, legacy) — historical behavior: polygon → FRED →
    yfinance fallback chain. Kept for backfill and one-shot scripts that
    don't care about the source distinction. Per-PR-1 design decision:
    operational pipelines (EOD SF, morning SF) MUST specify a mode
    explicitly so the failure semantics are deterministic.

Schema: index=ticker (str), columns=[date, Open, High, Low, Close, Adj_Close, Volume, VWAP]
"""

from __future__ import annotations

import io
import logging
import random
import re
import time
from datetime import datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import requests

from nousergon_lib.secrets import get_secret
from nousergon_lib.yfinance_quiet import log_yf_coverage, yf_quiet

logger = logging.getLogger(__name__)

# Matches BOTH FRED's ``api_key=`` (snake) and polygon's ``apiKey=`` (camel)
# querystring fragments. L4495: polygon error strings embed ``apiKey=<live>``
# (confirmed 2026-06-03 from a grouped-daily 500 WARNING) and the original
# FRED-only pattern let the polygon key through.
_API_KEY_RE = re.compile(r"(?:api_key|apiKey)=[^&\s]+")
# Back-compat alias — referenced by name in some tests.
_FRED_API_KEY_RE = _API_KEY_RE


def _scrub_api_key(msg: object) -> str:
    """Mask the ``api_key=...`` / ``apiKey=...`` querystring in any string.

    ``requests.exceptions.HTTPError`` (FRED and polygon) embeds the full
    request URL — including the key querystring — in its ``str()``
    representation. Logging that to CloudWatch leaks the credential. Always
    pass FRED/polygon-fetch exceptions through this scrubber before logging.
    """
    return _API_KEY_RE.sub(lambda m: m.group(0).split("=", 1)[0] + "=***", str(msg))

_NYSE_TZ = ZoneInfo("America/New_York")

_YFINANCE_BATCH_SIZE = 100
_YFINANCE_BATCH_DELAY = 2  # seconds between batches

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_FRED_TIMEOUT = 15
# L4480: bounded retry with exponential backoff + full jitter for FRED.
# The windowed reconciliation fires ~N×(window) FRED calls in a tight burst;
# without spacing FRED returns 429 storms (the 2026-06-01 TNX failure). #354
# made us resilient to a missed value; this stops the storm at the source.
# Honors a server `Retry-After` when present, else exponential backoff + jitter.
_FRED_MAX_ATTEMPTS = 3
_FRED_BACKOFF_BASE = 1.0   # seconds; wait ≈ base * 2**attempt + U(0, base)
_FRED_BACKOFF_CAP = 30.0   # seconds; never wait longer than this between tries


def _fred_get_with_retry(params: dict) -> requests.Response:
    """GET a FRED observation with bounded backoff + jitter on transient errors.

    Retries on 429 / 5xx / timeout / connection error (the recoverable class);
    a 4xx other than 429 (e.g. a malformed series_id) raises immediately — no
    point retrying a deterministic client error. Raises the last exception (or
    an HTTPError via ``raise_for_status``) after ``_FRED_MAX_ATTEMPTS``.
    """
    last_exc: Exception | None = None
    for attempt in range(_FRED_MAX_ATTEMPTS):
        try:
            resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
            if resp.status_code == 429 or resp.status_code >= 500:
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = _FRED_BACKOFF_BASE * (2 ** attempt)
                else:
                    wait = _FRED_BACKOFF_BASE * (2 ** attempt)
                wait = min(wait + random.uniform(0, _FRED_BACKOFF_BASE), _FRED_BACKOFF_CAP)
                if attempt < _FRED_MAX_ATTEMPTS - 1:
                    logger.warning(
                        "FRED %s — backing off %.1fs (attempt %d/%d)",
                        resp.status_code, wait, attempt + 1, _FRED_MAX_ATTEMPTS,
                    )
                    time.sleep(wait)
                    continue
            resp.raise_for_status()
            return resp
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt < _FRED_MAX_ATTEMPTS - 1:
                wait = min(
                    _FRED_BACKOFF_BASE * (2 ** attempt)
                    + random.uniform(0, _FRED_BACKOFF_BASE),
                    _FRED_BACKOFF_CAP,
                )
                logger.warning(
                    "FRED transient %s — backing off %.1fs (attempt %d/%d)",
                    type(exc).__name__, wait, attempt + 1, _FRED_MAX_ATTEMPTS,
                )
                time.sleep(wait)
                continue
            raise
    if last_exc is not None:
        raise last_exc
    # All attempts were 429/5xx that fell through the loop — surface the last.
    resp.raise_for_status()
    return resp

# Map our ArcticDB ticker key (after stripping ^) to FRED series id.
# Both yfinance (^VIX, ^TNX, ...) and FRED (VIXCLS, DGS10, ...) publish
# these in the same scale (raw index level for VIX/VIX3M, percent for
# TNX/IRX/TWO/HYOAS), so no conversion is needed before appending to
# ArcticDB.
#
# TWO + HYOAS added 2026-05-10 (Stage 2.5 of regime-conditioning rebuild
# — plan doc: alpha-engine-docs/private/regime-conditioning-260510.md).
# Both are FRED-only (no yfinance proxy), so they only flow through the
# FRED fallback path. Historical backfill is gated on a follow-up PR
# adding a FRED history fetcher; this PR begins forward-only collection.
_FRED_INDEX_MAP = {
    "VIX": "VIXCLS",
    "VIX3M": "VXVCLS",
    "TNX": "DGS10",
    "IRX": "DTB3",
    # 2Y treasury — enables 10Y-2Y curve slope (recession-focused canonical)
    # alongside the existing 10Y-3M (TNX-IRX cyclical).
    "TWO": "DGS2",
    # ICE BofA US High Yield Index Option-Adjusted Spread, percent.
    # Major regime indicator that VIX misses — credit widens before vol
    # spikes in many cycles, and stays wide during recoveries when vol
    # has already calmed. Institutional risk-factor models include it.
    "HYOAS": "BAMLH0A0HYM2",
    # Moody's BAA Corporate Bond Yield Relative to 10Y Treasury, percent.
    # Full 40y FRED history (1986+) — the credit-regime signal HYOAS can't
    # provide across the full predictor training corpus (HYOAS is license-
    # gated to 2023+ on FRED). BBB-rated spread vs HY's below-BBB; both
    # belong in the institutional credit-regime feature set.
    "BAA10Y": "BAA10Y",
}


def _coalesce_by_source_priority(
    new_records: list[dict],
    existing_rows: list[dict],
    run_date: str,
) -> tuple[list[dict], dict]:
    """Merge this-run records with the prior parquet by source priority.

    Institutional source-of-record waterfall (see ``_SOURCE_PRIORITY``). For
    each ticker, keep the row from the highest-priority source across {prior
    parquet, this run}:

    * **retain-on-empty** — a ticker the live pass could not refresh this run
      (absent from ``new_records``) keeps its prior row instead of being
      dropped. A populated cell can never regress to absent. This is the bug
      that halted the 2026-06-01 weekday pipeline: a transient FRED 429 on
      ``TNX`` let a wholesale overwrite blank a value the prior parquet held.
    * **restatement wins** — a ticker present from the SAME-or-higher priority
      source this run overwrites the prior value (ties resolve to the fresh
      row), so polygon's corporate-action-adjusted close still lands.
    * **no source-downgrade** — a strictly lower-priority fresh value (e.g. a
      yfinance backstop) cannot clobber a higher-priority existing value (e.g.
      a prior polygon close + true VWAP), preventing the 2026-04-17
      ``VWAP=None`` contamination class.

    A row whose ``Close`` is null/NaN is treated as missing (priority below any
    real value) so it neither wins a merge nor gets written as an empty cell.

    Returns ``(merged_records, stats)`` — stats counts retained / overwritten /
    new_only / downgrade_blocked tickers for loud observability.
    """
    def _prio(row: dict | None) -> int:
        if row is None:
            return -1
        close = row.get("Close")
        if close is None or pd.isna(close):
            return 0
        return _SOURCE_PRIORITY.get(row.get("source"), _UNKNOWN_SOURCE_PRIORITY)

    new_by_ticker = {r["ticker"]: r for r in new_records}
    existing_by_ticker = {r["ticker"]: r for r in existing_rows}

    merged: dict[str, dict] = {}
    stats = {"overwritten": 0, "retained": 0, "new_only": 0, "downgrade_blocked": 0}

    for ticker in set(new_by_ticker) | set(existing_by_ticker):
        new_row = new_by_ticker.get(ticker)
        old_row = existing_by_ticker.get(ticker)
        new_p, old_p = _prio(new_row), _prio(old_row)

        # Nothing usable from either side — drop (don't write a null cell).
        if max(new_p, old_p) <= 0:
            continue

        if old_row is None:
            merged[ticker] = new_row
            stats["new_only"] += 1
        elif new_p < 0:  # ticker absent from this run → retain prior
            merged[ticker] = old_row
            stats["retained"] += 1
        elif new_p >= old_p:  # equal-or-higher source wins (tie → restatement)
            merged[ticker] = new_row
            stats["overwritten"] += 1
        else:  # fresh value is strictly lower-quality — keep the better existing
            merged[ticker] = old_row
            stats["downgrade_blocked"] += 1

    return list(merged.values()), stats


def _is_post_close_write(last_modified: datetime, run_date: str) -> bool:
    """Return True if ``last_modified`` is at or after the NYSE close for ``run_date``.

    NYSE closes at 16:00 America/New_York. ``zoneinfo`` resolves EST/EDT
    automatically so this is correct year-round without explicit DST logic.
    """
    run_day = datetime.strptime(run_date, "%Y-%m-%d").date()
    close_et = datetime.combine(run_day, dtime(16, 0), tzinfo=_NYSE_TZ)
    return last_modified >= close_et


_VALID_SOURCES = ("auto", "yfinance_only", "polygon_only")
_YFINANCE_MIN_COVERAGE = 0.95   # below this, yfinance_only mode hard-fails
_POLYGON_MIN_COVERAGE = 0.95    # below this, polygon_only mode hard-fails
_DISCREPANCY_WARN_PCT = 0.01    # |polygon_close - yfinance_close| / yfinance_close
_DISCREPANCY_ERROR_PCT = 0.05

# Source-of-record priority for the coalescing merge (institutional waterfall).
# A cell is replaced only by an equal-or-higher-priority source; a lower-priority
# or *missing* value never clobbers a higher-quality existing value. This is the
# structural form of Brian's 2026-05-10 decision ("a cell is only updated if the
# data exists in [the authoritative source], else the prior datapoint is
# retained") — generalized so data can never regress to a less-informative value.
# polygon (adjusted close + true VWAP) and fred (sole source for its index
# series) are co-primary over DISJOINT ticker domains (equities vs ^indices), so
# they never compete for the same cell; yfinance is the backstop tier.
_SOURCE_PRIORITY = {"polygon": 3, "fred": 3, "yfinance": 1}
# Prior parquet rows written before the `source` column existed: treat as
# backstop-tier so a fresh polygon/fred value wins but a missing fresh value
# still retains them (never blanked).
_UNKNOWN_SOURCE_PRIORITY = 1

# Share-class symbol convention bridge (Yahoo/our-universe dash → polygon dot).
#
# Our universe + ArcticDB key class shares with a dash + single class
# letter (BRK-B, BF-B, MOG-A — the Yahoo convention). Polygon serves the
# *same security* under the dot convention (BRK.B, BF.B, MOG.A). This is
# a pure symbol-format mismatch, NOT a data gap or a delay: polygon's
# grouped-daily bulk call we already make every morning ALREADY contains
# these rows under the dot key (live-verified 2026-05-19 for 2026-05-18:
# BRK.B/BF.B/MOG.A all present same-day; BRK-B/BF-B/MOG-A all absent).
#
# Before this bridge, `grouped.get("BRK-B")` missed, then the rate-limited
# (5 calls/min) per-ticker fallback re-queried "BRK-B" and also missed
# (recovered 0/N) on every one of the 14 window dates — ~12 min of pure
# wasted retries that pushed weekday MorningEnrich past its 30-min SSM
# timeout (2026-05-19 SIGKILL/137 → whole weekday pipeline FAILED).
#
# The pattern is anchored to exactly the US class-share convention — a
# 1–5 char root, one hyphen, one uppercase class letter. It cannot
# misfire on a normal ticker, an index/^ ticker, a sector ETF, or a
# FRED-mapped symbol (none match `^[A-Z]{1,5}-[A-Z]$`). Future S&P
# class shares are handled automatically — no per-ticker config upkeep
# and no new chronic-gap entries.
_SHARE_CLASS_RE = re.compile(r"^[A-Z]{1,5}-[A-Z]$")


def _polygon_symbol(store_ticker: str) -> str:
    """Map our dash store-key to polygon's symbol for query/lookup.

    Returns the dot form for class-share tickers (``BRK-B`` → ``BRK.B``),
    else the input unchanged. The return value is ONLY ever used to talk
    to polygon (grouped-daily key lookup + the per-ticker endpoint path);
    the stored record always keeps the original dash ``store_ticker`` so
    ArcticDB / universe / downstream keys are unaffected.
    """
    if _SHARE_CLASS_RE.match(store_ticker):
        return store_ticker.replace("-", ".")
    return store_ticker


def _previous_business_days(run_date: str, n: int) -> list[str]:
    """Return ``n`` NYSE TRADING days ending at-or-before ``run_date``,
    newest first. ``n=1`` returns the most-recent trading day at or
    before ``run_date``.

    Used by :func:`collect` in window-scan mode to enumerate the dates
    each pass will reconcile. Polygon's free-tier rate-limit is honored
    by the caller — one ``grouped-daily`` call per date in the returned
    list, total ``n`` polygon calls regardless of universe size.

    HOLIDAY-AWARE since 2026-07-02 (config#1572): this helper was
    weekday-only, on the documented assumption that "holidays return zero
    rows from polygon and an empty yfinance batch" downstream. That
    assumption was FALSE in practice — the first post-Juneteenth
    yfinance-mode window enumerated 2026-06-19, the batch fetch returned
    data anyway, and a fabricated 924-row parquet for a closed market day
    entered the archive (and, via the Saturday backfill delta, the
    ArcticDB training store universe-wide). Non-trading days must never be
    enumerated for collection; ``nousergon_lib.trading_calendar`` is
    the same source of truth the Step Function's CheckTradingDay gate uses.
    """
    from nousergon_lib.trading_calendar import is_trading_day

    if n < 1:
        raise ValueError(f"window n must be >= 1, got {n}")
    cur = datetime.strptime(run_date, "%Y-%m-%d").date()
    # Runaway guard: a broken calendar must fail loud, not spin backwards.
    max_steps = n * 3 + 30
    steps = 0
    # Normalize the starting point to a trading day.
    while not is_trading_day(cur):
        cur = cur - timedelta(days=1)
        steps += 1
        if steps > max_steps:
            raise RuntimeError(
                f"_previous_business_days: no trading day within {max_steps} "
                f"calendar days before {run_date} — trading_calendar appears broken"
            )
    dates: list[str] = [cur.isoformat()]
    for _ in range(n - 1):
        cur = cur - timedelta(days=1)
        steps += 1
        while not is_trading_day(cur):
            cur = cur - timedelta(days=1)
            steps += 1
            if steps > max_steps:
                raise RuntimeError(
                    f"_previous_business_days: exhausted {max_steps} steps "
                    f"walking {n} trading days back from {run_date} — "
                    f"trading_calendar appears broken"
                )
        dates.append(cur.isoformat())
    return dates


def _polygon_date_fully_canonical(
    existing_df: pd.DataFrame,
    tickers: list[str],
) -> bool:
    """True iff every STOCK ticker in ``tickers`` is already polygon-canonical
    in ``existing_df`` — i.e. present with ``source="polygon"`` and a non-null
    ``Close`` (config#717).

    Only equities are considered: the FRED-index macro tickers
    (^TNX/^VIX/^IRX/^VIX3M) never come from polygon, so they neither block a
    polygon skip nor get demoted by one. If the parquet predates the ``source``
    column (legacy write), it can't be proven canonical → returns False so the
    legacy always-fetch path is preserved. An empty stock universe is vacuously
    canonical (nothing for polygon to fetch).
    """
    if "source" not in existing_df.columns:
        return False
    stock_tickers = [t for t in tickers if t.lstrip("^") not in _FRED_INDEX_MAP]
    if not stock_tickers:
        return True
    index = set(str(t) for t in existing_df.index)
    for t in stock_tickers:
        store_key = t.lstrip("^")
        if store_key not in index:
            return False
        row = existing_df.loc[store_key]
        if row.get("source") != "polygon" or pd.isna(row.get("Close")):
            return False
    return True


# config#717: a split with execution date E retroactively restates the polygon
# *adjusted* close of every date STRICTLY BEFORE E. So the split-aware polygon
# skip-canonical optimization must re-fetch any window date that a recently
# executed split would have restated, even if that date's parquet is already
# fully polygon-canonical. We only consider splits whose execution date is
# recent enough to plausibly touch a trailing window — anything older than the
# window's oldest date can't restate a date inside the window that isn't already
# correct (the date that needed restating was healed when the split first
# landed). A small forward buffer covers a split announced with a near-future
# effective date that polygon already lists.
_SPLIT_LOOKAHEAD_DAYS = 7


def _recent_split_events(
    window_dates: list[str],
    *,
    client=None,
) -> list[dict]:
    """Fetch ALL polygon split events executing over ``window_dates``' span
    (plus a small forward buffer) in ONE call (config#717).

    Extracted so the window scan can derive BOTH the
    ``_fetch_recent_split_dates`` skip-set AND the ``corporate_actions``
    detected-record set from a SINGLE polygon ``get_recent_splits`` call — no
    second API hit (the whole point of the config#717 one-call-per-window
    budget). Each event is ``{"ticker", "execution_date", "split_from",
    "split_to"}``. On any failure (client construction, 403, network) DEGRADES
    GRACEFULLY to ``[]`` (apiKey scrubbed from logs) — a corporate-action miss
    must never hard-fail the window.
    """
    if not window_dates:
        return []
    if client is None:
        try:
            from polygon_client import polygon_client

            client = polygon_client()
        except Exception as exc:  # import / construction failure — degrade
            logger.warning(
                "config#717: could not obtain polygon client for split scan "
                "(%s) — proceeding without corporate-action skip protection",
                _scrub_api_key(exc),
            )
            return []
    oldest = min(window_dates)
    newest = max(window_dates)
    lookahead = (
        datetime.strptime(newest, "%Y-%m-%d") + timedelta(days=_SPLIT_LOOKAHEAD_DAYS)
    ).strftime("%Y-%m-%d")
    try:
        return client.get_recent_splits(oldest, lookahead)
    except Exception as exc:
        logger.warning(
            "config#717: polygon split scan failed (%s) — proceeding without "
            "corporate-action skip protection (canonical-only skip still applies)",
            _scrub_api_key(exc),
        )
        return []


def _touched_dates_from_split_events(
    window_dates: list[str],
    splits: list[dict],
) -> set[str]:
    """Mark every window date strictly before any split's execution date as
    "touched" (must be re-fetched — its adjusted close was restated)."""
    if not splits:
        return set()
    touched: set[str] = set()
    for ev in splits:
        e = ev.get("execution_date")
        if not e:
            continue
        for d in window_dates:
            if d < e:  # ISO dates compare lexicographically == chronologically
                touched.add(d)
    return touched


def _fetch_recent_split_dates(
    window_dates: list[str],
    *,
    client=None,
) -> set[str]:
    """Return the subset of ``window_dates`` whose polygon adjusted close a
    recently executed split has retroactively restated (config#717).

    A split executing on date E divides/multiplies the adjusted close of every
    date strictly before E. We query polygon for ALL splits executing in the
    window's span (plus a small forward buffer) in ONE call, then mark every
    window date that falls strictly before any such split's execution date as
    "touched" — those must be re-fetched so the stored adjusted close stays on
    the current scale; the rest are safe to skip if already canonical.

    On any failure (403, network, empty) returns an empty set — i.e. NOTHING is
    marked touched. The caller's skip decision then degrades safely: a date is
    skipped only when it is BOTH fully canonical AND not touched, so an empty
    "touched" set just means the canonical-only check governs. (If a split is
    silently missed, the existing per-fetch discrepancy logging on the dates
    that ARE fetched, plus the next pass's coverage, remain the backstop.)
    """
    if not window_dates:
        return set()
    splits = _recent_split_events(window_dates, client=client)
    touched = _touched_dates_from_split_events(window_dates, splits)
    oldest = min(window_dates)
    newest = max(window_dates)
    lookahead = (
        datetime.strptime(newest, "%Y-%m-%d") + timedelta(days=_SPLIT_LOOKAHEAD_DAYS)
    ).strftime("%Y-%m-%d")
    if touched:
        logger.info(
            "config#717: %d split event(s) in [%s..%s] restate %d window date(s) "
            "— those will be re-fetched (not skipped): %s",
            len(splits), oldest, lookahead, len(touched),
            ", ".join(sorted(touched)),
        )
    return touched


# Sentinel distinguishing "caller passed no registry, build one if applicable"
# (a genuine standalone single-date call) from "window orchestrator explicitly
# passed a registry (possibly None)" — so a per-date collect() driven by
# _collect_window never builds its own registry (one per window, not one per
# date). See collect()/_collect_window.
_REGISTRY_UNSET = object()


def _build_corporate_action_registry(
    window_dates: list[str],
    bucket: str,
    *,
    dry_run: bool,
    run_id: str,
    need_touched: bool,
):
    """Detect splits over ``window_dates`` in ONE polygon call and (when live)
    build a ``CorporateActionRegistry`` with the detected splits recorded.

    Returns ``(registry_or_None, split_touched_dates_or_None)``. The single
    ``_recent_split_events`` fetch feeds BOTH the config#717 skip-set (when
    ``need_touched``) AND the corporate-actions detected records — no second
    polygon call. Registry construction / recording is best-effort: any failure
    WARNs (apiKey scrubbed) and degrades to ``registry=None`` so the discrepancy
    classifier falls back to the text ``_split_ratio_hint`` rather than
    hard-failing the collection run.
    """
    if not window_dates:
        return None, (set() if need_touched else None)
    events = _recent_split_events(window_dates)
    touched: set[str] | None = None
    if need_touched:
        touched = _touched_dates_from_split_events(window_dates, events)
        if touched:
            oldest, newest = min(window_dates), max(window_dates)
            lookahead = (
                datetime.strptime(newest, "%Y-%m-%d")
                + timedelta(days=_SPLIT_LOOKAHEAD_DAYS)
            ).strftime("%Y-%m-%d")
            logger.info(
                "config#717: %d split event(s) in [%s..%s] restate %d window "
                "date(s) — those will be re-fetched (not skipped): %s",
                len(events), oldest, lookahead, len(touched),
                ", ".join(sorted(touched)),
            )

    registry = None
    detected_actions: list = []
    if not dry_run and bucket:
        try:
            import corporate_actions

            registry = corporate_actions.CorporateActionRegistry(
                boto3.client("s3"), bucket
            )
            actions = corporate_actions.splits_from_events(events)
            detected_actions = actions
            n_new = 0
            for action in actions:
                try:
                    if registry.record_detected(action, run_id=run_id):
                        n_new += 1
                except Exception as exc:
                    # Per-action record failure must not lose the others or the
                    # run — WARN (recording surface) and continue.
                    logger.warning(
                        "corporate_actions: record_detected failed for %s @ %s "
                        "(%s)",
                        action.ticker, action.ex_date, _scrub_api_key(exc),
                    )
            if actions:
                logger.info(
                    "corporate_actions: %d split action(s) detected over "
                    "[%s..%s] (run_id=%s, %d newly recorded)",
                    len(actions), min(window_dates), max(window_dates),
                    run_id, n_new,
                )
        except Exception as exc:
            logger.warning(
                "corporate_actions: registry unavailable (%s) — discrepancy "
                "classification falls back to the text split-ratio hint",
                _scrub_api_key(exc),
            )
            registry = None
            detected_actions = []
    # The detected actions are returned so the window orchestrator can hand them
    # (with the registry) to ``corporate_actions.sync`` WITHOUT a second polygon
    # call (PR4, config#1433) — the single ``_recent_split_events`` fetch above
    # is reused for detection, the config#717 skip-set, AND the sync restatement.
    return registry, touched, detected_actions


def _send_corporate_action_email(actions: list, run_date: str) -> None:
    """Send ONE informational email summarizing confirmed corporate actions that
    restated adjusted history this run (best-effort, NEVER raises).

    These are EXPECTED restatements (a detected split), not anomalies — the
    email exists so the operator sees "system saw HON's 1-for-2 reverse split
    and the >5% adjusted-close jump it caused is accounted for", rather than the
    silence of a suppressed ERROR. A send failure WARNs (recording surface per
    the no-silent-fails rule) but must not fail the collection run.
    """
    if not actions:
        return
    n = len({a.action_id for a in actions})
    subject = f"📋 Corporate action(s) detected & restated — {n} ticker(s)"
    lines = [
        "The data pipeline detected the following corporate action(s); Polygon's",
        "adjusted history has restated the affected dates. This is EXPECTED — the",
        "resulting >5% adjusted-close change is accounted for, no action needed.",
        "",
    ]
    seen: set[str] = set()
    for action in actions:
        if action.action_id in seen:
            continue
        seen.add(action.action_id)
        lines.append(
            f"  • {action.ticker}: {action.human()} (ex-date {action.ex_date})"
        )
    lines += [
        "",
        "Polygon adjusted history restated; expected — no action needed.",
    ]
    body = "\n".join(lines)
    try:
        from emailer import send_email

        send_email(subject, body)
        logger.info(
            "corporate_actions: informational email sent for %d action(s) "
            "(run_date=%s)",
            n, run_date,
        )
    except Exception as exc:
        logger.warning(
            "corporate_actions: informational email send failed (%s) — "
            "restatement still logged at WARN (corporate_action_restatement)",
            _scrub_api_key(exc),
        )


def collect(
    bucket: str,
    tickers: list[str],
    run_date: str | None = None,
    s3_prefix: str = "staging/daily_closes/",
    dry_run: bool = False,
    source: str = "auto",
    window_days: int = 1,
    skip_if_canonical: bool = False,
    fred_window_cache: dict[str, list[tuple[str, float]]] | None = None,
    split_touched_dates: set[str] | None = None,
    equities_source: str = "polygon",
    index_source: str = "fred",
    fallback_source: str = "yfinance",
    registry=_REGISTRY_UNSET,
    _emit_ca_email: bool = True,
    _defer_unexplained_errors: bool = False,
) -> dict:
    """
    Fetch OHLCV for all tickers and write to S3.

    Args:
        bucket: S3 bucket name
        tickers: list of ticker symbols to capture
        run_date: YYYY-MM-DD (defaults to today)
        s3_prefix: S3 key prefix for daily closes
        dry_run: if True, fetch but don't write to S3
        source: ``yfinance_only`` (EOD pass — polygon skipped, no VWAP),
                ``polygon_only`` (morning pass — polygon required, FRED for indices,
                no yfinance fallback), or ``auto`` (legacy chain). See module
                docstring for full rationale.
        window_days: int = 1
                Number of business days to scan, ending on ``run_date``
                inclusive. Default 1 preserves single-date legacy behavior.
                When > 1: iterate from oldest → newest over the window
                (i.e. ``run_date - (window_days - 1) BDays`` → ``run_date``)
                and call this collector once per date. Polygon stays bounded
                at ``window_days`` ``grouped-daily`` calls in total — one
                per date — which is the only way to honor the free-tier
                rate limit. Window-mode callers are also expected to set
                ``skip_if_canonical=True`` so steady-state yfinance batch
                cost stays near zero (most cells already have an
                authoritative source from prior pass days).
        skip_if_canonical: bool = False
                When True, the per-date fetch reads the existing
                ``staging/daily_closes/{date}.parquet`` (if any), extracts
                the set of "canonical" tickers (rows where
                ``source ∈ {"yfinance", "polygon"}`` AND ``Close`` is not
                null), and skips fetching those tickers from yfinance.
                Existing canonical rows are then merged into the output
                parquet via ``_coalesce_by_source_priority`` (config#720 —
                the same source-priority-waterfall primitive every mode
                uses; a fresh yfinance/auto fetch is equal-or-higher
                priority than a canonical prior row of the same tier, so it
                always wins the merge, matching the pre-#720 behavior).
                Implements the source-precedence-ladder skip-set
                semantic from the windowed-data-reconciliation arc:

                  - ``yfinance_only`` mode: skips canonical tickers (any
                    source already populated), so yfinance only fetches
                    cells that are NaN. Coverage gate evaluates the
                    merged-output denominator (existing canonical rows
                    contribute as if freshly fetched).
                  - ``polygon_only`` mode (config#717): the flag is now
                    HONORED, split-aware. A date is skipped (no polygon
                    grouped-daily call) iff its parquet is already fully
                    polygon-canonical (every stock ticker present with
                    ``source="polygon"`` + non-null ``Close``) AND no
                    recently executed corporate action restated it. A
                    split with execution date E retroactively divides /
                    multiplies the adjusted close of every date BEFORE E,
                    so those dates are re-fetched even when canonical;
                    ``split_touched_dates`` carries the set of such dates
                    (detected via the polygon splits endpoint, one
                    range-scoped call per window). The discrepancy-logging
                    / coalesce path stays intact for the dates that ARE
                    fetched. Previously (option a) polygon blanket-ignored
                    the flag and re-fetched every date — the 2026-06-03
                    30-min-timeout root cause.
                  - ``auto`` mode: flag applied to the yfinance step
                    only; polygon step always runs.

                Default False preserves legacy single-date overwrite
                semantics for non-window callers.
        split_touched_dates: set[str] | None = None
                config#717. The set of dates a recently executed corporate
                action (split) restated — these are re-fetched by the
                polygon_only skip-canonical path even when their parquet is
                already canonical. Computed once per window by
                ``_collect_window`` (one range-scoped polygon splits call)
                and threaded into each per-date ``collect``. A standalone
                single-date polygon_only + skip_if_canonical caller computes
                it inline. ``None`` ⇒ treated as "nothing touched" (the
                canonical-only check then governs).

    Returns:
        Single-date mode (``window_days=1``): dict with ``status``,
        ``tickers_captured``, ``polygon``/``fred``/``yfinance`` counts,
        ``source``.

        Window mode (``window_days > 1``): dict with **target-driven**
        ``status`` — ``"ok"`` iff the TARGET date (``target_date``, the
        newest date in the window = the date downstream reads)
        succeeded, regardless of non-target *historical backfill* date
        failures; ``"error"`` only if the target date itself failed
        (caller escalates that to a hard stop). Non-target backfill
        failures are recorded in ``per_date`` and listed in
        ``backfill_failed_dates`` (best-effort, NON-fatal — surfaced via
        a WARNING for the surveillance channel, never halts the
        pipeline). Also: aggregated ``tickers_captured`` / ``polygon`` /
        ``fred`` / ``yfinance`` counters, ``source``, ``window_days``,
        ``target_date``, ``per_date`` (date → per-date result dict),
        ``skipped_dates`` (post-close already-written dates).

    Raises:
        ValueError: invalid ``source`` or ``window_days < 1``
        PolygonForbiddenError: polygon 403 in ``polygon_only`` or ``auto`` mode
        RuntimeError: per-mode coverage threshold breached
    """
    if source not in _VALID_SOURCES:
        raise ValueError(
            f"Invalid source={source!r}. Must be one of {_VALID_SOURCES}."
        )
    if window_days < 1:
        raise ValueError(f"window_days must be >= 1, got {window_days}")

    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()

    # NYSE-calendar gate (config#1572): a daily-closes parquet for a
    # non-trading day is fabricated data by definition — polygon and yfinance
    # have no session to serve, and whatever a batch fetch returns anyway gets
    # persisted as a phantom day (2026-06-19 Juneteenth: 924 fabricated
    # yfinance rows entered the archive and, via the Saturday backfill delta,
    # the ArcticDB training store universe-wide). Window mode normalizes its
    # anchor to a trading day (weekend/holiday SF fire-times are legitimate);
    # a SINGLE-date call for a non-trading day is always a caller error and
    # fails loud.
    from nousergon_lib.trading_calendar import is_trading_day as _is_td

    _run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    if window_days == 1 and not _is_td(_run_dt):
        raise ValueError(
            f"collect: run_date={run_date} is not an NYSE trading day — "
            f"refusing to write a phantom daily-closes parquet (config#1572). "
            f"If polygon/NYSE calendars diverged, fix "
            f"nousergon_lib.trading_calendar, not this guard."
        )

    if window_days > 1:
        return _collect_window(
            bucket=bucket,
            tickers=tickers,
            run_date=run_date,
            s3_prefix=s3_prefix,
            dry_run=dry_run,
            source=source,
            window_days=window_days,
            skip_if_canonical=skip_if_canonical,
            equities_source=equities_source,
            index_source=index_source,
            fallback_source=fallback_source,
        )

    # Single-date split-aware skip protection + corporate-action registry: when
    # a window caller drives per-date collect()s it passes both
    # ``split_touched_dates`` and an explicit ``registry`` down (computed/built
    # once for the whole window). A standalone single-date polygon_only caller
    # (rare) builds them here for just this date — ONE polygon split fetch feeds
    # both — so neither the config#717 skip guard nor the registry-aware
    # discrepancy classification is bypassed.
    if registry is _REGISTRY_UNSET:
        registry = None
        # Only when this standalone call would have scanned splits anyway
        # (polygon_only + skip_if_canonical + no pre-threaded touched set): ONE
        # _recent_split_events fetch feeds BOTH the config#717 skip-set AND the
        # registry. When split_touched_dates was threaded in (window-driven) or
        # skip_if_canonical is off (legacy overwrite), we neither scan nor build
        # a registry — preserving the no-extra-call contract of those paths
        # (registry stays None ⇒ discrepancy logging falls back to the text
        # split-ratio hint, the pre-config#1431 behavior).
        if (
            source == "polygon_only"
            and window_days == 1
            and skip_if_canonical
            and split_touched_dates is None
        ):
            registry, split_touched_dates, _detected_actions = (
                _build_corporate_action_registry(
                    [run_date],
                    bucket,
                    dry_run=dry_run,
                    run_id=run_date,
                    need_touched=True,
                )
            )

    s3 = boto3.client("s3")
    key = f"{s3_prefix}{run_date}.parquet"

    # Existing-parquet inspection — mode-aware. Runs in both dry_run and live
    # mode so dry_run can surface "this would skip" / "this would overwrite
    # with these discrepancies" before the user commits to a real write.
    #
    # Skip-on-exists short-circuit (yfinance_only + auto only) returns inside
    # the live branch since dry_run by definition isn't going to write.
    existing_close_for_discrepancy: dict[str, float] | None = None
    # Full prior-parquet rows (with ``source``) fed to the single
    # ``_coalesce_by_source_priority`` merge (config#720: the one preservation
    # primitive for every mode) so a transient live-fetch gap retains the prior
    # value instead of blanking it. Empty unless an existing parquet is read.
    #
    # ``polygon_only`` populates this with EVERY prior row (any source) — the
    # coalesce's own priority waterfall decides retain/overwrite/downgrade-block.
    # ``yfinance_only``/``auto`` + ``skip_if_canonical=True`` populate it with
    # only the rows already carrying an authoritative source (yfinance/polygon)
    # and a non-null Close — mirrors the pre-config#720 ``canonical_existing_rows``
    # filter exactly, so those modes' coalesce input (and therefore output) is
    # unchanged; yfinance/auto's own fresh fetch is equal-or-higher priority
    # than a canonical prior row of the same tier, so it naturally lands in the
    # coalesce's "restatement wins"/"new_only" branches — the "equal-priority
    # case" the consolidation issue (config#720) describes.
    existing_rows_for_merge: list[dict] = []
    canonical_skip_set: set[str] = set()
    from botocore.exceptions import ClientError
    head = None
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        err_code = exc.response.get("Error", {}).get("Code")
        if err_code not in ("404", "NoSuchKey"):
            # Auth failure, throttling, or network — not "file doesn't exist".
            # Don't silently paper over it.
            raise
        # 404/NoSuchKey: expected case — file doesn't exist, proceed to write.

    if head is not None:
        last_modified = head["LastModified"]
        if source == "polygon_only":
            # Read existing rows for (a) Close-discrepancy logging and (b) the
            # source-priority coalesce merge before write — so a cell the live
            # pass can't refresh this run is RETAINED, never blanked. Failures
            # here are non-fatal for discrepancy logging, but losing the prior
            # rows means we fall back to the legacy destructive overwrite, so
            # warn loudly. The coverage gate still runs on the FRESH fetch, so
            # a real polygon outage is not masked by retained rows.
            #
            # config#717: split-aware skip-canonical. When skip_if_canonical=True
            # the polygon side ALSO skips a date whose parquet is already fully
            # polygon-canonical (every stock ticker has source="polygon" + a
            # non-null Close) — EXCEPT dates a recent corporate action restated
            # (``split_touched_dates``), which must be re-fetched so the stored
            # adjusted close stays on the current scale. Mirrors the yfinance
            # side's canonical-skip structure for consistency.
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                existing_df = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
                existing_close_for_discrepancy = {
                    str(t): float(existing_df.loc[t, "Close"])
                    for t in existing_df.index
                    if pd.notna(existing_df.loc[t, "Close"])
                }
                for t in existing_df.index:
                    row = {"ticker": str(t)}
                    row.update(existing_df.loc[t].to_dict())
                    existing_rows_for_merge.append(row)
                logger.info(
                    "polygon_only: found existing parquet (last_modified=%s, %d tickers) — "
                    "will coalesce (retain-on-empty, priority-ranked) and log Close discrepancies",
                    last_modified.isoformat(), len(existing_close_for_discrepancy),
                )
                if skip_if_canonical and not dry_run:
                    touched = split_touched_dates or set()
                    is_touched = run_date in touched
                    fully_canonical = _polygon_date_fully_canonical(
                        existing_df, tickers,
                    )
                    if fully_canonical and not is_touched:
                        logger.info(
                            "[skip_if_canonical] polygon_only %s: parquet fully "
                            "polygon-canonical and no recent corporate action "
                            "restates it — skipping polygon re-fetch (saves one "
                            "grouped-daily call)",
                            run_date,
                        )
                        return {
                            "status": "ok",
                            "tickers_captured": 0,
                            "skipped": True,
                            "skipped_reason": "polygon_canonical",
                            "source": source,
                        }
                    logger.info(
                        "[skip_if_canonical] polygon_only %s: re-fetching "
                        "(fully_canonical=%s, corporate_action_touched=%s)",
                        run_date, fully_canonical, is_touched,
                    )
            except Exception as exc:
                logger.warning(
                    "polygon_only: failed to read existing parquet for coalesce/discrepancy "
                    "(%s) — proceeding with destructive overwrite (no retain-on-empty this run)",
                    exc,
                )
        elif skip_if_canonical:
            # yfinance_only / auto + skip_if_canonical=True: read the full
            # parquet, extract canonical rows so they survive into the merged
            # output via the shared ``_coalesce_by_source_priority`` step
            # below (config#720 — same primitive polygon_only uses). Bypass
            # the post-close-skip short-circuit below — the whole point of
            # windowed reconciliation is to fill NaN cells in older dates
            # that legacy logic would skip.
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                existing_df = pd.read_parquet(
                    io.BytesIO(obj["Body"].read()), engine="pyarrow",
                )
                if "source" in existing_df.columns:
                    for t in existing_df.index:
                        row = existing_df.loc[t]
                        row_source = row.get("source")
                        row_close = row.get("Close")
                        if (
                            row_source in ("yfinance", "polygon")
                            and pd.notna(row_close)
                        ):
                            canonical_skip_set.add(str(t))
                            preserved = {"ticker": str(t)}
                            preserved.update(row.to_dict())
                            existing_rows_for_merge.append(preserved)
                logger.info(
                    "[skip_if_canonical] %s: %d existing canonical "
                    "tickers will be preserved; yfinance fetch reduced "
                    "to %d non-canonical",
                    run_date, len(canonical_skip_set),
                    len(tickers) - len(canonical_skip_set),
                )
            except Exception as exc:
                # Read failure → fall back to legacy overwrite (existing
                # parquet is opaque to us; safer to refetch than to lose
                # data preservation invariant silently).
                logger.warning(
                    "[skip_if_canonical] %s: failed to read existing "
                    "parquet (%s) — falling back to legacy refetch + "
                    "overwrite for this date",
                    run_date, exc,
                )
                canonical_skip_set = set()
                existing_rows_for_merge = []
        elif not dry_run and _is_post_close_write(last_modified, run_date):
            logger.info(
                "Daily closes already exist for %s (post-close at %s, source=%s) — skipping",
                run_date, last_modified.isoformat(), source,
            )
            return {"status": "ok", "tickers_captured": 0, "skipped": True, "source": source}
        elif not dry_run:
            logger.warning(
                "Existing %s was written pre-close at %s — refusing to skip; "
                "re-collecting authoritative post-close data",
                key, last_modified.isoformat(),
            )
            # fall through to re-fetch + overwrite

    if not tickers:
        return {"status": "error", "error": "no tickers provided", "source": source}

    records: list[dict] = []

    # ── Step 1: polygon.io grouped-daily ─────────────────────────────────────
    # Skipped in yfinance_only mode (free tier returns 403 same-day; deferring
    # to the morning polygon_only enrichment is the canonical path).
    #
    # L4482: a TRANSIENT polygon NETWORK failure (read-timeout / connection
    # error / rate-limit-exhausted) must NOT abort the whole date — the
    # FRED-index macro tickers (^TNX/^VIX/^IRX/^VIX3M) and their yfinance
    # backstop NEVER come from polygon, yet a raise here skips Steps 2 & 3 and
    # leaves the critical macro keys unfilled (the exact gap that failed
    # recovery re-run #1 on 2026-06-01 despite #354 — a polygon read-timeout).
    # So in polygon_only mode we catch ONLY the transient network class, log
    # loudly, and fall through to FRED + the macro backstop. A REAL polygon
    # outage is still surfaced: zero equity records → the equity coverage gate
    # below hard-fails, so the catch cannot mask an equity-data failure.
    # NARROW BY DESIGN: `PolygonForbiddenError` (structural 403) and
    # `_fetch_polygon_closes`'s deliberate "0 tickers" empty-data RuntimeError
    # still propagate with their own clear messages — only network transients
    # are downgraded to "continue to the macro backstop".
    from polygon_client import PolygonRateLimitError
    # Lazy import: ``sources`` adapters import ``collectors.daily_closes`` at
    # module load, so importing it here (at call time, when this module is fully
    # initialized) avoids a circular import. The registry makes the source per
    # role config-/param-selectable — swapping polygon→databento is one param.
    from sources import get_adapter
    polygon_count = 0
    if source != "yfinance_only":
        try:
            polygon_count = get_adapter(equities_source).fetch_into(
                records, tickers, run_date, strict=(source == "polygon_only"),
            )
        except (requests.Timeout, requests.ConnectionError,
                PolygonRateLimitError) as exc:
            if source != "polygon_only":
                raise  # auto mode already owns its fallback inside the fetch
            logger.warning(
                "L4482: polygon grouped-daily failed transiently for %s (%s: %s) "
                "— proceeding to FRED (Step 2) + macro yfinance backstop (Step 3) "
                "so the FRED-index macro keys still fill. A real equity outage is "
                "caught by the coverage gate (0 equity records → hard-fail).",
                run_date, type(exc).__name__, exc,
            )

    # ── Step 2: FRED for the 4 indices polygon never serves ──────────────────
    # VIX/VIX3M/TNX/IRX are not on polygon free tier (and won't be on paid either
    # for the index symbols we use). FRED has same-scale equivalents
    # (VIXCLS/VXVCLS/DGS10/DTB3) that publish T-1 values reliably. Runs in
    # every mode — these tickers have no other source.
    captured_tickers = {r["ticker"] for r in records}
    fred_missing = [
        t for t in tickers
        if t.lstrip("^") not in captured_tickers and t.lstrip("^") in _FRED_INDEX_MAP
    ]
    fred_count = 0
    if fred_missing:
        # L4492: in window mode the caller prefetches the whole window's FRED
        # series in one ranged call each and passes the cache down; per-date
        # emit then reads from it (no per-date FRED I/O). None → legacy path.
        fred_count = get_adapter(index_source).fetch_into(
            records, fred_missing, run_date, window_cache=fred_window_cache,
        )

    # ── Step 3: yfinance ─────────────────────────────────────────────────────
    # polygon_only refuses yfinance fallback for the EQUITY universe per
    # feedback_no_silent_fails: a silent yfinance fill would hide a polygon
    # outage and re-introduce the 2026-04-17 → 2026-04-23 VWAP=None
    # contamination. That refusal is equity-specific — it does NOT apply to the
    # FRED-index macro tickers (^TNX/^VIX/^IRX/^VIX3M), which polygon never
    # serves and which carry no VWAP. For those, FRED → yfinance is the
    # legitimate primary chain, so a FRED 429 (the 2026-06-01 TNX failure)
    # falls through to yfinance LOUDLY rather than leaving a critical macro key
    # absent. Equities still refuse yfinance in polygon_only.
    captured_tickers = {r["ticker"] for r in records}
    missing = [t for t in tickers if t.lstrip("^") not in captured_tickers]
    # When skip_if_canonical=True (yfinance_only / auto window mode), drop
    # tickers that already have an authoritative source in the existing
    # parquet — those rows will be merged back via the source-priority
    # coalesce below, so refetching them would just churn API budget.
    if canonical_skip_set:
        before = len(missing)
        missing = [t for t in missing if t.lstrip("^") not in canonical_skip_set]
        logger.info(
            "[skip_if_canonical] %s: yfinance fetch list %d → %d "
            "(skipped %d canonical)",
            run_date, before, len(missing), before - len(missing),
        )
    yfinance_count = 0
    if missing:
        if source == "polygon_only":
            # Equity universe stays refused; macro FRED-index tickers get the
            # loud yfinance backstop.
            macro_missing = [t for t in missing if t.lstrip("^") in _FRED_INDEX_MAP]
            if macro_missing:
                logger.warning(
                    "polygon_only: FRED did not supply macro ticker(s) %s for %s — "
                    "falling back to yfinance (loud backstop; FRED likely rate-limited). "
                    "Equity universe still refuses yfinance per feedback_no_silent_fails.",
                    macro_missing, run_date,
                )
                yfinance_count = get_adapter(fallback_source).fetch_into(
                    records, macro_missing, run_date,
                )
        else:
            yfinance_count = get_adapter(fallback_source).fetch_into(
                records, missing, run_date,
            )

    # ── Source-priority coalesce (yfinance_only / auto) — config#720 ─────────
    # Merge preserved canonical rows from the existing parquet into the
    # records list via the SAME ``_coalesce_by_source_priority`` primitive
    # ``polygon_only`` uses below (config#720: unify the two preservation
    # mechanisms). ``existing_rows_for_merge`` here holds only the rows
    # already carrying an authoritative source (yfinance/polygon) + non-null
    # Close (populated above, identical filter to the pre-#720
    # ``canonical_existing_rows``), and yfinance/auto's own fresh fetch is
    # equal-or-higher priority than a canonical prior row of the same
    # tier — so this fresh fetch always lands in the coalesce's
    # "restatement wins" / "new_only" branches, the "equal-priority case"
    # the consolidation subsumes; a ticker this run couldn't (re)capture
    # retains its prior canonical row exactly as before.
    #
    # Deliberately runs BEFORE the coverage gate below (unlike the
    # polygon_only coalesce, which runs after) — preserved canonical rows
    # must count toward the coverage denominator here, matching the
    # documented ``skip_if_canonical`` contract ("coverage gate evaluates
    # the merged-output denominator"). polygon_only has
    # ``existing_rows_for_merge`` empty at this point (only populated in its
    # own branch above), so this is a no-op for that mode.
    if source != "polygon_only" and existing_rows_for_merge:
        records, canon_merge_stats = _coalesce_by_source_priority(
            records, existing_rows_for_merge, run_date,
        )
        logger.info(
            "[skip_if_canonical] %s: coalesce retained %d preserved canonical "
            "row(s), overwrote %d, new %d (total %d)",
            run_date, canon_merge_stats["retained"],
            canon_merge_stats["overwritten"], canon_merge_stats["new_only"],
            len(records),
        )

    # ── Coverage gates — per-mode hard-fails ─────────────────────────────────
    n_stock_tickers = sum(1 for t in tickers if t.lstrip("^") not in _FRED_INDEX_MAP)
    n_stock_records = sum(
        1 for r in records if r["ticker"] not in _FRED_INDEX_MAP
    )
    stock_coverage = (n_stock_records / n_stock_tickers) if n_stock_tickers else 1.0

    if source == "yfinance_only" and stock_coverage < _YFINANCE_MIN_COVERAGE:
        raise RuntimeError(
            f"yfinance_only mode for {run_date}: stock coverage {stock_coverage:.1%} "
            f"below {_YFINANCE_MIN_COVERAGE:.0%} threshold ({n_stock_records}/{n_stock_tickers}). "
            f"yfinance batch download must be failing — investigate before letting "
            f"the EOD pipeline write a sparse parquet that EOD reconcile + tomorrow's "
            f"morning enrichment will both have to compensate for."
        )
    if source == "polygon_only" and stock_coverage < _POLYGON_MIN_COVERAGE:
        raise RuntimeError(
            f"polygon_only mode for {run_date}: stock coverage {stock_coverage:.1%} "
            f"below {_POLYGON_MIN_COVERAGE:.0%} threshold ({n_stock_records}/{n_stock_tickers}). "
            f"Polygon grouped-daily returned fewer tickers than expected — check "
            f"polygon API status / quota / date validity. NOT falling back to yfinance "
            f"by design (per feedback_no_silent_fails)."
        )

    if not records:
        logger.warning("No closes captured for %s (source=%s)", run_date, source)
        return {"status": "error", "error": "no data fetched", "tickers_captured": 0, "source": source}

    # ── Source-priority coalesce (polygon_only) ──────────────────────────────
    # Merge the fresh fetch with the prior parquet so a cell the live pass could
    # not refresh this run RETAINS its prior value instead of being blanked,
    # while polygon restatements still win and a lower-tier fresh value can't
    # downgrade a higher-tier existing cell. Runs AFTER the coverage gate, so a
    # genuine polygon outage still hard-fails on the FRESH fetch before any
    # retained rows could mask it. (yfinance_only / auto already ran the SAME
    # ``_coalesce_by_source_priority`` primitive above, before the coverage
    # gate — config#720 unified both modes onto this one function; only the
    # gate-ordering and the existing-rows population differ per mode.)
    if source == "polygon_only" and existing_rows_for_merge:
        records, merge_stats = _coalesce_by_source_priority(
            records, existing_rows_for_merge, run_date,
        )
        if merge_stats["retained"] or merge_stats["downgrade_blocked"]:
            logger.warning(
                "polygon_only coalesce for %s: retained %d prior cell(s) the live pass "
                "could not refresh, blocked %d source-downgrade(s); overwrote %d, new %d "
                "(total %d).",
                run_date, merge_stats["retained"], merge_stats["downgrade_blocked"],
                merge_stats["overwritten"], merge_stats["new_only"], len(records),
            )
        else:
            logger.info(
                "polygon_only coalesce for %s: overwrote %d, new %d, total %d "
                "(no retain/downgrade events).",
                run_date, merge_stats["overwritten"], merge_stats["new_only"], len(records),
            )

    closes_df = pd.DataFrame(records).set_index("ticker")
    logger.info(
        "Daily closes: %d tickers for %s source=%s (polygon=%d, fred=%d, yfinance=%d)",
        len(closes_df), run_date, source, polygon_count, fred_count, yfinance_count,
    )

    # Discrepancy logging (polygon_only mode, when overwriting an existing parquet)
    explained_actions: list = []
    unexplained_discrepancies: list = []
    if existing_close_for_discrepancy and polygon_count > 0:
        explained_actions, unexplained_discrepancies = _log_close_discrepancies(
            closes_df, existing_close_for_discrepancy, run_date, registry=registry,
            defer_unexplained=_defer_unexplained_errors,
        )

    # ONE informational email per run for confirmed corporate-action
    # restatements — but only when THIS collect() owns the email (standalone
    # single-date call). When driven by _collect_window, the email is deferred
    # (``_emit_ca_email=False``) and sent ONCE at the window level over all
    # dates. Never in dry_run (registry is None there, so no explained actions).
    if _emit_ca_email and not dry_run and explained_actions:
        _send_corporate_action_email(explained_actions, run_date)

    if dry_run:
        return {
            "status": "ok_dry_run",
            "tickers_captured": len(closes_df),
            "polygon": polygon_count,
            "fred": fred_count,
            "yfinance": yfinance_count,
            "source": source,
            "corporate_actions": explained_actions,
            "unexplained_discrepancies": unexplained_discrepancies,
        }

    # ── Step 4: Write to S3 ──────────────────────────────────────────────────
    try:
        buf = io.BytesIO()
        closes_df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
        buf.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info(
            "Written to s3://%s/%s (%d tickers, source=%s)",
            bucket, key, len(closes_df), source,
        )
        return {
            "status": "ok",
            "tickers_captured": len(closes_df),
            "polygon": polygon_count,
            "fred": fred_count,
            "yfinance": yfinance_count,
            "source": source,
            "corporate_actions": explained_actions,
            "unexplained_discrepancies": unexplained_discrepancies,
        }
    except Exception as e:
        logger.error("Failed to write daily closes: %s", e)
        return {
            "status": "error",
            "error": str(e),
            "tickers_captured": len(closes_df),
            "source": source,
        }


def _collect_window(
    bucket: str,
    tickers: list[str],
    run_date: str,
    s3_prefix: str,
    dry_run: bool,
    source: str,
    window_days: int,
    skip_if_canonical: bool = False,
    equities_source: str = "polygon",
    index_source: str = "fred",
    fallback_source: str = "yfinance",
) -> dict:
    """Iterate ``collect`` over a backward-looking business-day window.

    Calls :func:`collect` with ``window_days=1`` per date so the
    per-date branch reuses the existing fetch / coverage-gate / write
    pipeline unchanged. Iterates oldest → newest so the most recent
    date's parquet is the last one written; idempotent on re-run since
    each per-date call goes through the same skip-on-exists path the
    legacy single-date flow uses.

    Polygon call rate is bounded at ``window_days`` ``grouped-daily``
    calls total — one per date — which is the contract the free-tier
    rate limit requires.

    ``skip_if_canonical=True`` propagates to every per-date call so the
    yfinance side skips tickers that already have an authoritative
    source in the existing parquet — keeps steady-state yfinance batch
    cost near zero across the window. The polygon side now honors the
    flag too (config#717): a date whose parquet is already fully
    polygon-canonical is skipped (no grouped-daily call) UNLESS a
    recently executed split restated its adjusted close. Those
    corporate-action-touched dates are detected once for the whole
    window via a single range-scoped polygon splits call
    (``_fetch_recent_split_dates``) and threaded into each per-date
    ``collect`` as ``split_touched_dates`` so the per-date polygon cost
    drops to "only the dates that actually changed".

    Returns an aggregate dict; see ``collect`` docstring's "Window mode"
    return-shape section for the schema.
    """
    window_dates = _previous_business_days(run_date, n=window_days)
    # ── L4492: prefetch FRED once per series over the whole window ──────────
    # Each per-date ``collect`` below previously re-fetched all FRED index
    # series for its date (window_days × len(series) calls → 429 storm + the
    # 2026-06-03 30-min timeout). Fetch each series once over the window range
    # (padded a few calendar days so the oldest date's on-or-before lookup
    # resolves) and hand the cache to every per-date call. Only prefetch when
    # the universe actually contains FRED-index tickers — keeps non-macro
    # window callers (and tests) on a zero-FRED-I/O path.
    fred_tickers = [t for t in tickers if t.lstrip("^") in _FRED_INDEX_MAP]
    fred_window_cache: dict[str, list[tuple[str, float]]] | None = None
    if fred_tickers:
        _oldest = window_dates[-1]  # _previous_business_days returns newest-first
        _start = (
            datetime.strptime(_oldest, "%Y-%m-%d") - timedelta(days=10)
        ).strftime("%Y-%m-%d")
        fred_window_cache = _fetch_fred_window(fred_tickers, _start, window_dates[0])
    # ── config#717: scan corporate actions once for the whole window ─────────
    # polygon_only + skip_if_canonical skips already-canonical dates to save the
    # per-date grouped-daily call (the 2026-06-03 30-min-timeout root cause),
    # but a split retroactively restates the adjusted close of every date before
    # its execution date — those MUST be re-fetched. One range-scoped splits
    # call covers the whole window (no per-date corporate-action I/O). Only run
    # when the optimization is actually active.
    # config#1431 (corporate-actions program): ONE polygon split scan for the
    # whole window feeds BOTH the config#717 skip-set AND the
    # CorporateActionRegistry (detected-records + the registry-aware discrepancy
    # classifier). Built once here and threaded into every per-date collect() as
    # an EXPLICIT ``registry`` so no per-date call builds its own (one fetch per
    # window, not per date). Built for polygon_only (where discrepancy logging
    # runs) regardless of skip_if_canonical; the touched-date skip-set is only
    # derived when skip_if_canonical is on.
    # Gated identically to the pre-config#1431 config#717 split scan
    # (polygon_only + skip_if_canonical + live): that path ALREADY made one
    # ``get_recent_splits`` call per window — we now route it through
    # ``_recent_split_events`` so the SAME single call feeds both the skip-set
    # and the registry (no extra polygon call). A polygon_only window without
    # skip_if_canonical neither scans nor builds a registry (registry stays None
    # ⇒ discrepancy logging keeps the text split-ratio hint — no regression).
    split_touched_dates: set[str] | None = None
    registry = None
    if source == "polygon_only" and skip_if_canonical and not dry_run:
        registry, split_touched_dates, detected_actions = (
            _build_corporate_action_registry(
                window_dates,
                bucket,
                dry_run=dry_run,
                run_id=window_dates[0],  # target date identifies the window run
                need_touched=True,
            )
        )
        # ── PR4 (config#1433): unified corporate-action sync, ONCE at the START ─
        # Restate ALL stores (the daily_closes archive parquets + the ArcticDB
        # universe) for every detected split BEFORE the per-date collect loop
        # (re-)writes parquets and BEFORE downstream daily_append reads the
        # universe — so the split-boundary discontinuity is flattened up front
        # instead of re-forming mid-week between Saturday backfills. Reuses the
        # registry + actions already detected above (NO extra polygon call), and
        # is scoped to the requested universe. Best-effort: a sync failure WARNs
        # and the morning collection proceeds (the per-date discrepancy logging,
        # the morning polygon re-fetch, and the blocking Saturday backfill audit
        # all remain in place).
        if registry is not None and detected_actions:
            try:
                import corporate_actions

                sync_result = corporate_actions.sync(
                    boto3.client("s3"),
                    bucket,
                    min(window_dates),
                    max(window_dates),
                    stores=[
                        corporate_actions.STORE_DAILY_CLOSES_ARCHIVE,
                        corporate_actions.STORE_ARCTICDB_UNIVERSE,
                    ],
                    run_id=window_dates[0],
                    tickers=tickers,
                    registry=registry,
                    actions=detected_actions,
                )
                n_applied = sum(
                    1
                    for store_results in sync_result.applied.values()
                    for r in store_results
                    if r.get("status") == "applied" and r.get("n_rows_adjusted", 0) > 0
                )
                logger.info(
                    "corporate_actions.sync: %d split(s) detected, %d "
                    "restatement(s) applied across %d store(s), %d dividend(s) "
                    "recorded (CRSP-separate, no price restate, no email) over "
                    "[%s..%s] (run_id=%s)",
                    len(sync_result.detected), n_applied,
                    len(sync_result.applied), len(sync_result.dividends),
                    min(window_dates), max(window_dates), window_dates[0],
                )
            except Exception as exc:
                logger.warning(
                    "corporate_actions.sync failed (%s) — proceeding with the "
                    "morning collection; per-date re-fetch + Saturday backfill "
                    "audit remain the heal", _scrub_api_key(exc),
                )
    # The newest date in the window is the TARGET date — the one
    # downstream (predictor inference / eod_reconcile) actually reads.
    # The older dates are best-effort *historical backfill*: a per-date
    # polygon/coverage hiccup on them (same-day-403, polygon free-tier
    # quota/rate-limit exhaustion, a transient non-overlapping
    # grouped-daily) must NOT hard-fail the run — that would block the
    # whole weekly/EOD pipeline on a recoverable backfill miss while the
    # target date is perfectly good. Same best-effort discipline as the
    # chronic-gap self-heal step in weekly_collector.py. Fatality is
    # decided AFTER the loop, from the TARGET date only.
    #
    # 2026-05-15 incident: a non-target backfill date failed on a
    # same-day recovery re-run; the old "any per-date error → aggregate
    # 'partial'" → strict caller roll-up (weekly_collector.py:1076,
    # `not in ("ok","ok_dry_run")` ⇒ "failed") escalated it to
    # SystemExit(1), halting an otherwise-healthy MorningEnrich. The
    # static "point-in-time coverage-gate" theory was falsified by a
    # dry-run repro (coverage held ~99%); the real defect is this
    # best-effort-vs-strict-rollup contradiction. See plan doc
    # morningenrich-coverage-gate-260515.md §8/§9.
    target_date = window_dates[0]
    aggregate: dict = {
        "status": "ok",
        "source": source,
        "window_days": window_days,
        "target_date": target_date,
        "per_date": {},
        "tickers_captured": 0,
        "polygon": 0,
        "fred": 0,
        "yfinance": 0,
        "skipped_dates": [],
        "backfill_failed_dates": [],
        "corporate_actions": [],
        "unexplained_discrepancies": [],
    }
    # Iterate oldest → newest so the most recent date's parquet is the
    # last one written. Matches the operator mental model "the latest
    # date's data is the freshest on disk."
    for d in reversed(window_dates):
        try:
            result = collect(
                bucket=bucket,
                tickers=tickers,
                run_date=d,
                s3_prefix=s3_prefix,
                dry_run=dry_run,
                source=source,
                window_days=1,
                skip_if_canonical=skip_if_canonical,
                fred_window_cache=fred_window_cache,  # L4492: 1 ranged call/series
                split_touched_dates=split_touched_dates,  # config#717
                registry=registry,  # config#1431: one registry per window
                _emit_ca_email=False,  # email sent ONCE at window level below
                _defer_unexplained_errors=True,  # aggregated ONCE below (2026-07-02)
                equities_source=equities_source,
                index_source=index_source,
                fallback_source=fallback_source,
            )
        except Exception as exc:
            # Record + continue. Fatality is target-driven (decided
            # after the loop) — a non-target backfill miss is recoverable
            # and must not halt the pipeline.
            logger.warning(
                "[daily_closes window] date=%s source=%s failed: %s — "
                "recording and continuing window",
                d, source, _scrub_api_key(exc),  # L4495: exc may carry polygon apiKey
            )
            aggregate["per_date"][d] = {
                "status": "error",
                "error": _scrub_api_key(exc),  # L4495: never persist the key to S3 logs
                "source": source,
            }
            continue
        aggregate["per_date"][d] = result
        for k in ("tickers_captured", "polygon", "fred", "yfinance"):
            if k in result and isinstance(result[k], int):
                aggregate[k] += result[k]
        if result.get("skipped"):
            aggregate["skipped_dates"].append(d)
        # config#1431: accumulate confirmed corporate-action restatements across
        # the window for the single informational email sent below.
        if result.get("corporate_actions"):
            aggregate["corporate_actions"].extend(result["corporate_actions"])
        if result.get("unexplained_discrepancies"):
            aggregate["unexplained_discrepancies"].extend(
                result["unexplained_discrepancies"]
            )

    # config#1431: ONE informational email per window run for confirmed
    # corporate-action restatements (deduped by action_id in the email layer).
    # Skipped in dry_run (registry is None → no explained actions accumulate).
    if not dry_run and aggregate["corporate_actions"]:
        _send_corporate_action_email(aggregate["corporate_actions"], target_date)

    # 2026-07-02: window-level roll-up of the UNEXPLAINED >5% overwrites the
    # per-date calls deferred. A corporate-action restatement touches every
    # window date with ONE uniform ratio — six per-date ERROR emails for one
    # HON event was misrouting. Uniform-ratio groups (≥2 dates, same ticker,
    # ratio agreeing within tolerance) page ONCE with the inferred factor;
    # singletons keep the original per-date ERROR semantics.
    _emit_window_unexplained_discrepancies(
        aggregate["unexplained_discrepancies"], target_date,
    )

    # ── Fatality is TARGET-driven, not "any per-date error" ─────────────
    _target = aggregate["per_date"].get(target_date)
    _target_ok = _target is not None and _target.get("status") in (
        "ok", "ok_dry_run",
    )
    aggregate["backfill_failed_dates"] = sorted(
        d for d, r in aggregate["per_date"].items()
        if r.get("status") == "error" and d != target_date
    )
    if not _target_ok:
        # Target date itself failed (or produced no result) — FATAL. The
        # caller's strict roll-up escalates this to SystemExit(1), which
        # is correct: downstream must not read a bad/absent target row.
        aggregate["status"] = "error"
        aggregate["error"] = (
            f"target date {target_date} failed: "
            f"{(_target or {}).get('error', 'no result produced')}"
        )
    else:
        # Target good. Non-target backfill misses (if any) are surfaced
        # loudly for the surveillance / Flow-Doctor channel but are
        # non-fatal (mirrors chronic-gap self-heal best-effort policy).
        if aggregate["backfill_failed_dates"]:
            logger.warning(
                "[daily_closes window] target %s OK; %d non-target "
                "backfill date(s) failed — best-effort, NON-FATAL: %s",
                target_date, len(aggregate["backfill_failed_dates"]),
                ", ".join(aggregate["backfill_failed_dates"]),
            )
        aggregate["status"] = "ok"
    return aggregate


def _fetch_polygon_closes(
    tickers: list[str],
    run_date: str,
    records: list[dict],
    source: str,
) -> int:
    """Fetch OHLCV+VWAP from polygon grouped-daily.

    In ``polygon_only`` mode, ``PolygonForbiddenError`` propagates — caller
    must handle (no silent yfinance fallback).

    In ``auto`` mode, polygon failures are caught and logged so the legacy
    chain (FRED → yfinance) can fill the gap. Note: this is the historical
    silent-fall-through behavior. New operational code paths should use
    ``polygon_only`` or ``yfinance_only`` to make failure semantics explicit.
    """
    from polygon_client import polygon_client, PolygonForbiddenError

    try:
        grouped = polygon_client().get_grouped_daily(run_date)
    except PolygonForbiddenError:
        if source == "polygon_only":
            raise
        logger.warning(
            "Polygon 403 in auto mode — falling back to FRED+yfinance "
            "(this is the historical silent-fallback path; new pipelines "
            "should use --source polygon_only to surface the failure)"
        )
        return 0
    except Exception as e:
        if source == "polygon_only":
            raise
        logger.warning(
            "Polygon grouped-daily failed in auto mode: %s — falling back",
            _scrub_api_key(e),  # L4495: exc may carry polygon apiKey
        )
        return 0

    if not grouped:
        if source == "polygon_only":
            raise RuntimeError(
                f"polygon grouped-daily returned 0 tickers for {run_date} — "
                f"likely a non-trading day or polygon API outage. polygon_only "
                f"mode refuses to fall through (see feedback_no_silent_fails)."
            )
        return 0

    polygon_count = 0
    for ticker in tickers:
        store_ticker = ticker.lstrip("^")
        # Look up by polygon's symbol (dot form for class shares), but
        # keep ``store_ticker`` (dash) as the persisted record key.
        g = grouped.get(_polygon_symbol(store_ticker))
        if g:
            records.append({
                "ticker": store_ticker,
                "date": run_date,
                "Open": round(g["open"], 4),
                "High": round(g["high"], 4),
                "Low": round(g["low"], 4),
                "Close": round(g["close"], 4),
                "Adj_Close": round(g["close"], 4),
                "Volume": int(g["volume"]),
                "VWAP": round(g["vwap"], 4) if g.get("vwap") else None,
                "source": "polygon",
            })
            polygon_count += 1
    logger.info("Polygon grouped-daily: %d/%d tickers", polygon_count, len(tickers))

    # ── Per-ticker fallback for tickers polygon's bulk endpoint dropped ────
    # Polygon's grouped-daily endpoint returns inconsistent ticker sets across
    # calls (observed 2026-05-02: two calls 4h apart returned 913-ticker
    # subsets that differed by 8 tickers — all real S&P 500/400 names). Hit
    # the per-ticker /aggs/ticker endpoint for the gaps so a transient bulk
    # miss doesn't tip MorningEnrich's missing-from-closes hard-fail. Stays
    # within polygon source — no silent yfinance fallback.
    captured = {r["ticker"] for r in records}
    missing_stocks = [
        t for t in tickers
        if t.lstrip("^") not in captured and t.lstrip("^") not in _FRED_INDEX_MAP
    ]
    if missing_stocks:
        recovered = _fetch_polygon_closes_per_ticker(missing_stocks, run_date, records)
        polygon_count += recovered
        logger.info(
            "Polygon per-ticker fallback: recovered %d/%d (still missing %d): %s",
            recovered, len(missing_stocks),
            len(missing_stocks) - recovered,
            [t for t in missing_stocks if t.lstrip("^") not in {r["ticker"] for r in records}][:10],
        )
    return polygon_count


def _fetch_polygon_closes_per_ticker(
    tickers: list[str],
    run_date: str,
    records: list[dict],
) -> int:
    """Per-ticker polygon single-day fetch — fallback for tickers missing
    from the grouped-daily response. Same source (polygon), same schema,
    so no silent-fallback risk per feedback_no_silent_fails.

    Each ticker is one rate-limited polygon call. With the default 5
    calls/min and ~10-15 misses on a typical bulk-endpoint flake, this
    adds 2-3 minutes to MorningEnrich runtime — well under the SF's
    DataPhase1 budget. Class-share tickers are now recovered for free
    by the grouped-daily dot-key lookup (see ``_polygon_symbol``) so
    they no longer reach this fallback; ``_polygon_symbol`` is still
    applied here for the rare case a class share is genuinely dropped
    by the bulk endpoint on a given date.
    """
    from polygon_client import polygon_client

    recovered = 0
    for ticker in tickers:
        store_ticker = ticker.lstrip("^")
        try:
            bar = polygon_client().get_single_day_bar(
                _polygon_symbol(store_ticker), run_date
            )
        except Exception as exc:
            logger.warning(
                "Polygon per-ticker fallback failed for %s @ %s: %s",
                store_ticker, run_date, _scrub_api_key(exc),  # L4495
            )
            continue
        if not bar:
            continue
        records.append({
            "ticker": store_ticker,
            "date": run_date,
            "Open": round(bar["open"], 4),
            "High": round(bar["high"], 4),
            "Low": round(bar["low"], 4),
            "Close": round(bar["close"], 4),
            "Adj_Close": round(bar["close"], 4),
            "Volume": int(bar["volume"]),
            "VWAP": round(bar["vwap"], 4) if bar.get("vwap") else None,
            "source": "polygon",
        })
        recovered += 1
    return recovered


_SPLIT_RATIO_MAX = 50  # largest forward/reverse split factor worth hinting (covers 20:1 NVDA-class splits)
_SPLIT_RATIO_TOL = 0.005  # adjusted closes restate by the EXACT factor; 0.5% absorbs feed rounding


def _split_ratio_hint(prior: float, new: float) -> str:
    """Corporate-action hint when prior/new sits on a clean split ratio, else "".

    A forward N-for-1 split divides the adjusted close by exactly N (a reverse
    1-for-N multiplies by N), so a cross-source overwrite whose ratio is within
    ``_SPLIT_RATIO_TOL`` of an integer 2..``_SPLIT_RATIO_MAX`` is far more likely
    a split restatement than a code bug. Surfacing the ratio in the ERROR message
    hands the strongest evidence to whoever diagnoses it — the KLAC 10:1 split
    (2026-06-10) was auto-diagnosed as a producer decimal-shift bug because the
    message only said "90.00% diff" (data#417-419, config#1030).
    """
    if prior <= 0 or new <= 0:
        return ""
    for big, small, template in (
        (prior, new, "%d-for-1 forward stock split"),
        (new, prior, "1-for-%d reverse stock split"),
    ):
        ratio = big / small
        n = round(ratio)
        if 2 <= n <= _SPLIT_RATIO_MAX and abs(ratio - n) / n <= _SPLIT_RATIO_TOL:
            return (
                " [ratio = %d:1 — consistent with a %s restating adjusted history "
                "(corporate action), check the split calendar before suspecting a code bug]"
                % (n, template % n)
            )
    return ""


# Two window dates' unexplained overwrite ratios "agree" (same corporate-action
# restatement) when they differ by less than this relative tolerance.
_UNIFORM_RATIO_REL_TOL = 0.02


def _emit_window_unexplained_discrepancies(rows: list, target_date: str) -> None:
    """Window-level roll-up of unexplained >5% overwrite discrepancies.

    Groups rows by ticker; a group whose ratios all agree within
    ``_UNIFORM_RATIO_REL_TOL`` across ≥2 dates is ONE event (a corporate-action
    restatement sweeping the window — the 2026-07-02 HON case produced six
    per-date ERROR emails for one 2:1 separation) and pages ONCE with the
    inferred factor. Non-uniform groups and singletons page per row, preserving
    the original per-date ERROR semantics.
    """
    if not rows:
        return
    by_ticker: dict[str, list] = {}
    for r in rows:
        by_ticker.setdefault(r["ticker"], []).append(r)
    for ticker, group in sorted(by_ticker.items()):
        ratios = [g["ratio"] for g in group]
        mean_ratio = sum(ratios) / len(ratios)
        uniform = len(group) >= 2 and all(
            abs(r - mean_ratio) <= _UNIFORM_RATIO_REL_TOL * mean_ratio
            for r in ratios
        )
        if uniform:
            dates = ", ".join(sorted(g["date"] for g in group))
            logger.error(
                "polygon_only OVERWRITE %s: UNIFORM ×%.4f restatement across "
                "%d window date(s) [%s]%s — one event, consistent with a "
                "corporate action restating adjusted history that has NO "
                "matching registry/feed record (or an inverted one); check "
                "the split calendar and the corporate-action registry before "
                "downstream consumers re-read",
                ticker, mean_ratio, len(group), dates,
                _split_ratio_hint(group[0]["prior"], group[0]["new"]),
            )
        else:
            for g in group:
                pct = abs(g["new"] - g["prior"]) / g["prior"] * 100
                logger.error(
                    "polygon_only OVERWRITE %s @ %s: Close %.4f → %.4f "
                    "(%.2f%% diff vs prior parquet)%s — investigate before "
                    "downstream consumers re-read",
                    g["ticker"], g["date"], g["prior"], g["new"], pct,
                    _split_ratio_hint(g["prior"], g["new"]),
                )


def _log_close_discrepancies(
    new_df: pd.DataFrame,
    prior_close: dict[str, float],
    run_date: str,
    *,
    registry=None,
    defer_unexplained: bool = False,
) -> "tuple[list, list]":
    """Log per-ticker Close discrepancy when polygon overwrites yfinance.

    A small drift (<1%) is normal — different feeds, slight tick-time offsets,
    consolidated tape coverage variance. Larger drifts (>1% WARN, >5% ERROR)
    typically indicate corporate-action timing differences or one-source data
    quality issues worth a human eyeball.

    L4486: the >5% ERROR band is for genuine cross-source EQUITY drift (polygon
    overwriting a different-source equity close — a data-quality flag). The
    FRED-index macro tickers (^TNX/^VIX/^IRX/^VIX3M, …) are a different class:
    the windowed reconciliation predictably RESTATES them toward the
    authoritative FRED value — healing a cell clobbered by a transient 429 (the
    5/14 VIX case) or correcting a stale T-1 edge cell (the reconciliation runs
    before FRED publishes the prior session's value). Those self-heals jump >5%
    on volatile-VIX days but are DESIRABLE, not anomalies, so they log at WARN
    (`fred_restatement`) and are excluded from the flow-doctor ERROR filter. The
    recording surface stays (per feedback_no_silent_fails) — just at the right
    severity. Pattern observed twice (2026-05-12, 2026-06-02).

    config#1431 (corporate-actions program): when a ``registry`` is supplied, a
    >5% equity jump that the registry can attribute to a DETECTED corporate
    action (a confirmed split whose ``expected_factor`` matches ``new/prior``)
    is the EXPECTED adjusted-history restatement, NOT an anomaly — it logs at
    WARN (`corporate_action_restatement`) and is excluded from the flow-doctor
    ERROR filter (fixing the HON 1-for-2 reverse-split false-ERROR). The
    authoritative registry takes precedence over the secondary
    ``_split_ratio_hint`` text heuristic (which stays, now only annotating the
    UNEXPLAINED ERROR branch).

    2026-07-02 (six-ERROR-email incident): with ``defer_unexplained=True`` the
    unexplained >5% rows are NOT logged at ERROR here — they are returned for
    the WINDOW caller to aggregate (a corporate-action restatement touches
    every window date with ONE uniform ratio; six per-date ERROR emails for
    one event is misrouting, and a genuinely unexplained uniform-ratio group
    should page ONCE with the inferred factor). Per-date WARN still records
    each row (``feedback_no_silent_fails`` — the surface stays, severity moves
    to the aggregate).

    Returns ``(explained_actions, unexplained_rows)`` where each unexplained
    row is ``{"ticker", "date", "prior", "new", "ratio"}``.
    """
    n_compared = 0
    n_warn = 0
    n_error = 0
    n_restatement = 0
    n_corporate_action = 0
    explained_actions: list = []
    unexplained_rows: list = []
    biggest: tuple[str, float] = ("", 0.0)
    for ticker in new_df.index:
        prior = prior_close.get(str(ticker))
        new_close = new_df.loc[ticker, "Close"]
        if prior is None or pd.isna(new_close) or prior == 0:
            continue
        n_compared += 1
        is_fred_index = str(ticker).lstrip("^") in _FRED_INDEX_MAP
        pct_diff = abs(float(new_close) - prior) / prior
        if pct_diff > _DISCREPANCY_ERROR_PCT and is_fred_index:
            # FRED-index restatement toward the authoritative value — expected
            # self-heal, NOT an equity data-quality anomaly. WARN, not ERROR.
            logger.warning(
                "fred_restatement %s @ %s: Close %.4f → %.4f (%.2f%% diff vs prior parquet) — "
                "windowed reconciliation healed toward authoritative FRED (expected)",
                ticker, run_date, prior, float(new_close), pct_diff * 100,
            )
            n_restatement += 1
        elif (
            pct_diff > _DISCREPANCY_ERROR_PCT
            and registry is not None
            and (
                action := registry.explains_discrepancy(
                    str(ticker), run_date, prior, float(new_close)
                )
            )
            is not None
        ):
            # config#1431: confirmed corporate-action restatement (the registry
            # is AUTHORITATIVE) — expected adjusted-history restatement, WARN not
            # ERROR, excluded from the flow-doctor ERROR filter.
            logger.warning(
                "corporate_action_restatement %s @ %s: Close %.4f → %.4f "
                "(%.2f%% diff vs prior parquet) — confirmed %s (registry %s), "
                "adjusted history restated (expected, no action needed)",
                ticker, run_date, prior, float(new_close), pct_diff * 100,
                action.human(), action.action_id,
            )
            n_corporate_action += 1
            explained_actions.append(action)
        elif pct_diff > _DISCREPANCY_ERROR_PCT:
            unexplained_rows.append({
                "ticker": str(ticker),
                "date": run_date,
                "prior": float(prior),
                "new": float(new_close),
                "ratio": float(new_close) / float(prior),
            })
            if defer_unexplained:
                # Window caller aggregates uniform-ratio groups into ONE
                # ERROR; the per-row surface stays at WARN so nothing is
                # silently dropped (feedback_no_silent_fails).
                logger.warning(
                    "polygon_only OVERWRITE %s @ %s: Close %.4f → %.4f "
                    "(%.2f%% diff vs prior parquet)%s — deferred to the "
                    "window-level unexplained-discrepancy aggregation",
                    ticker, run_date, prior, float(new_close), pct_diff * 100,
                    _split_ratio_hint(prior, float(new_close)),
                )
            else:
                logger.error(
                    "polygon_only OVERWRITE %s @ %s: Close %.4f → %.4f (%.2f%% diff vs prior parquet)%s — "
                    "investigate before downstream consumers re-read",
                    ticker, run_date, prior, float(new_close), pct_diff * 100,
                    _split_ratio_hint(prior, float(new_close)),
                )
            n_error += 1
        elif pct_diff > _DISCREPANCY_WARN_PCT:
            logger.warning(
                "polygon_only OVERWRITE %s @ %s: Close %.4f → %.4f (%.2f%% diff vs prior parquet)",
                ticker, run_date, prior, float(new_close), pct_diff * 100,
            )
            n_warn += 1
        if pct_diff > biggest[1]:
            biggest = (str(ticker), pct_diff)
    logger.info(
        "polygon_only discrepancy summary for %s: compared=%d warn(>1%%)=%d error(>5%%)=%d "
        "fred_restatement(>5%%)=%d corporate_action(>5%%)=%d biggest=%s@%.2f%%",
        run_date, n_compared, n_warn, n_error, n_restatement, n_corporate_action,
        biggest[0] or "n/a", biggest[1] * 100,
    )
    return explained_actions, unexplained_rows


def _fred_record(store_ticker: str, date_str: str, close: float) -> dict:
    """Build a FRED daily-close record (OHLC all = close; no volume/VWAP).

    VWAP only meaningful from polygon grouped-daily (volume-weighted across
    trades). FRED single-value closes give us no distribution to VWAP, so
    None rather than passing Close off as VWAP.
    """
    return {
        "ticker": store_ticker,
        "date": date_str,
        "Open": round(close, 4),
        "High": round(close, 4),
        "Low": round(close, 4),
        "Close": round(close, 4),
        "Adj_Close": round(close, 4),
        "Volume": 0,
        "VWAP": None,
        "source": "fred",
    }


def _fred_value_on_or_before(
    series: list[tuple[str, float]], date_str: str,
) -> float | None:
    """Value of the most-recent observation dated on-or-before ``date_str``.

    ``series`` is ascending-sorted ``(date, value)``. Mirrors the per-date
    API semantics (``observation_end=date_str``, newest non-missing) against
    the prefetched window cache (L4492) — a future-dated value is never
    returned, matching the legacy path's defensive ``obs_date > date_str``
    guard.
    """
    candidate: float | None = None
    for d, v in series:
        if d <= date_str:
            candidate = v
        else:
            break
    return candidate


def _fetch_fred_window(
    tickers: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, list[tuple[str, float]]]:
    """Fetch each FRED index series ONCE over ``[start_date, end_date]`` (L4492).

    The windowed-reconciliation loop previously hit FRED once PER DATE per
    series (``window_days × len(series)`` calls — 56 at window=14), firing a
    self-inflicted 429 storm and burning most of MorningEnrich's runtime (the
    2026-06-03 30-min SSM timeout). The FRED ``observations`` endpoint takes
    ``observation_start``/``observation_end``, so the whole window's values
    for a series come back in ONE ranged call; the per-date emit then indexes
    this cache via :func:`_fred_value_on_or_before` (no further I/O). This
    *supersedes* L4480's backoff-on-429 (which only tolerated the storm
    slowly) by removing the storm at the source.

    Returns ``{store_ticker: [(obs_date, value), ...]}`` ascending by date,
    non-missing only. A series whose fetch fails is simply absent — the
    per-date caller skips it and the loud yfinance macro backstop fills the
    gap (same degradation as a per-date FRED miss). ``start_date`` should be
    padded a few calendar days before the oldest window date so the oldest
    date's on-or-before lookup can resolve to a prior observation (FRED daily
    series skip weekends/holidays).
    """
    api_key = get_secret("FRED_API_KEY", required=False, default="")
    if not api_key:
        logger.warning(
            "FRED_API_KEY not set — skipping FRED window prefetch for %d tickers",
            len(tickers),
        )
        return {}

    cache: dict[str, list[tuple[str, float]]] = {}
    for ticker in tickers:
        store_ticker = ticker.lstrip("^")
        series_id = _FRED_INDEX_MAP.get(store_ticker)
        if not series_id:
            continue
        try:
            params = {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "observation_start": start_date,
                "observation_end": end_date,
                "sort_order": "asc",
            }
            resp = _fred_get_with_retry(params)  # L4480: backoff + jitter
            obs = resp.json().get("observations", [])
            series = [
                (o["date"], float(o["value"]))
                for o in obs
                if o.get("value", ".") != "." and o.get("date")
            ]
            cache[store_ticker] = series
            logger.info(
                "FRED window %s → %s: %d obs over [%s, %s] (1 ranged call)",
                store_ticker, series_id, len(series), start_date, end_date,
            )
        except Exception as e:
            logger.warning(
                "FRED window fetch failed for %s (%s): %s — per-date emit will "
                "skip it (yfinance macro backstop fills the gap)",
                store_ticker, series_id, _scrub_api_key(e),
            )
    return cache


def _fetch_fred_closes(
    tickers: list[str],
    date_str: str,
    records: list[dict],
    window_cache: dict[str, list[tuple[str, float]]] | None = None,
) -> int:
    """Fetch FRED close on-or-before ``date_str`` for index tickers.

    Serves the index/macro symbols not on polygon free tier
    (VIX, VIX3M, TNX, IRX, TWO, HYOAS, BAA10Y).

    The query is bounded by ``observation_end=date_str`` so per-date calls
    from the windowed-reconciliation loop return that date's actual FRED
    value rather than today's "most recent" — the original unbounded
    ``sort_order=desc, limit=5`` shape predated window mode and clobbered
    every historical date in the rolling window with today's latest close
    (FlowDoctor `polygon_only OVERWRITE VIX` ERROR alerts 2026-05-12 surfaced
    the regression; the prior parquet's correct historical VIX was
    overwritten with today's value on every MorningEnrich pass since the
    2026-05-11 ``window_days: 14`` cutover).

    Same-day morning call (FRED publishes T-1): observation_end=today still
    returns the most-recent-on-or-before-today observation (typically T-1),
    so the legacy "today's parquet carries yesterday's FRED value" semantic
    is preserved for the current-day case.

    ``window_cache`` (L4492): when provided (window mode), the per-date value
    is read from the prefetched ranged-call cache (:func:`_fetch_fred_window`)
    instead of hitting FRED — so the windowed-reconciliation loop makes
    ``len(series)`` ranged calls total, not ``window_days × len(series)``
    per-date calls. ``None`` (the default; single-date callers) preserves the
    legacy per-date API path.
    """
    if window_cache is not None:
        count = 0
        for ticker in tickers:
            store_ticker = ticker.lstrip("^")
            series = window_cache.get(store_ticker)
            if not series:
                # Series absent (not a FRED index, or its window fetch failed)
                # — skip; the yfinance macro backstop handles the gap loudly.
                continue
            close = _fred_value_on_or_before(series, date_str)
            if close is None:
                logger.warning(
                    "FRED %s: no cached observation on or before %s",
                    store_ticker, date_str,
                )
                continue
            records.append(_fred_record(store_ticker, date_str, close))
            count += 1
        logger.info(
            "FRED window-cache: %d/%d index tickers captured for %s",
            count, len(tickers), date_str,
        )
        return count

    api_key = get_secret("FRED_API_KEY", required=False, default="")
    if not api_key:
        logger.warning("FRED_API_KEY not set — skipping FRED fallback for %d tickers", len(tickers))
        return 0

    count = 0
    for ticker in tickers:
        store_ticker = ticker.lstrip("^")
        series_id = _FRED_INDEX_MAP.get(store_ticker)
        if not series_id:
            continue
        try:
            params = {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "observation_end": date_str,
                "sort_order": "desc",
                "limit": 5,
            }
            resp = _fred_get_with_retry(params)  # L4480: backoff + jitter
            obs = resp.json().get("observations", [])
            latest = next((o for o in obs if o.get("value", ".") != "."), None)
            if latest is None:
                logger.warning(
                    "FRED %s → %s: no non-missing observation on or before %s",
                    store_ticker, series_id, date_str,
                )
                continue
            obs_date = latest.get("date")
            if obs_date and obs_date > date_str:
                # Defensive: refuse to stamp a future-dated FRED value onto
                # date_str's parquet even if FRED somehow ignored observation_end.
                logger.error(
                    "FRED %s observation date %s > requested %s — refusing to "
                    "write future value (likely upstream API behavior change)",
                    store_ticker, obs_date, date_str,
                )
                continue
            close = float(latest["value"])
            records.append(_fred_record(store_ticker, date_str, close))
            count += 1
        except Exception as e:
            logger.warning(
                "FRED fetch failed for %s (%s): %s",
                store_ticker, series_id, _scrub_api_key(e),
            )

    logger.info("FRED fallback: %d/%d index tickers captured", count, len(tickers))
    return count


@yf_quiet
def _fetch_yfinance_closes(
    tickers: list[str],
    date_str: str,
    records: list[dict],
) -> int:
    """Fetch closes from yfinance for tickers not covered by polygon.

    Runs under ``yf_quiet`` (nousergon_lib.yfinance_quiet): a delisted/renamed
    ticker (e.g. JHG, BLD 2026-07-10) makes yfinance log its own per-symbol
    "possibly delisted" ERROR, which Flow Doctor turns into one report per
    symbol per worded variant — the same recurring bug class already fixed in
    ``collectors/prices.py`` (nousergon-data#455) and
    ``collectors/metron_market_data.py`` (config#1029). The replacement
    recording surface is the aggregated ``log_yf_coverage`` call below.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available for daily closes fallback")
        return 0

    count = 0
    covered: set[str] = set()
    batches = [tickers[i:i + _YFINANCE_BATCH_SIZE]
               for i in range(0, len(tickers), _YFINANCE_BATCH_SIZE)]

    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(_YFINANCE_BATCH_DELAY)
        try:
            tickers_arg = batch[0] if len(batch) == 1 else batch
            raw = yf.download(
                tickers=tickers_arg,
                period="5d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=True,
            )
            is_multi = isinstance(raw.columns, pd.MultiIndex)

            for ticker in batch:
                try:
                    df = (raw[ticker] if is_multi else raw).copy()
                    df.index = pd.to_datetime(df.index)
                    if df.index.tz is not None:
                        df.index = df.index.tz_convert("UTC").tz_localize(None)
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue

                    last = df.iloc[-1]
                    store_ticker = ticker.lstrip("^")
                    adj_close = float(last["Adj Close"]) if "Adj Close" in df.columns else float(last["Close"])
                    high = float(last["High"])
                    low = float(last["Low"])
                    close = float(last["Close"])
                    # VWAP is None on yfinance fallback. yfinance does not expose
                    # true volume-weighted VWAP; the previous (H+L+C)/3 proxy
                    # misrepresented proxy values as VWAP and contaminated the
                    # ArcticDB universe column once Phase 7 migration started
                    # materializing VWAP there. Per 2026-04-17 decision: only
                    # polygon-sourced true VWAP is written. See ROADMAP "VWAP
                    # centralization". Executor `load_daily_vwap` already handles
                    # None by looking back up to 5 prior trading days.
                    records.append({
                        "ticker": store_ticker,
                        "date": date_str,
                        "Open": round(float(last["Open"]), 4),
                        "High": round(high, 4),
                        "Low": round(low, 4),
                        "Close": round(close, 4),
                        "Adj_Close": round(adj_close, 4),
                        "Volume": int(last["Volume"]) if pd.notna(last.get("Volume")) else 0,
                        "VWAP": None,
                        "source": "yfinance",
                    })
                    count += 1
                    covered.add(ticker)
                except Exception as e:
                    logger.warning("yfinance close extract failed for %s: %s", ticker, e)
        except Exception as e:
            logger.warning("yfinance batch failed: %s", e)

    logger.info("yfinance fallback: %d/%d tickers captured", count, len(tickers))
    log_yf_coverage(logger, "daily_closes", tickers, covered)
    return count
