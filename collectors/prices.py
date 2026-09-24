"""
prices.py — Refresh stale price cache parquets and upload to S3.

Two-phase staleness check:
  1. Fast: list the LIVE ``reference/price_cache/`` tree (via
     ``price_cache_read_prefixes`` — never the legacy ``predictor/price_cache/``
     tree frozen 2026-06-19, alpha-engine-config-I11518) and compare each
     parquet's last-modified time, on the trading-day axis, with the run's
     trading day. A split guard adds any fresh ticker whose history does not
     reflect a split executed in the last 30 days (polygon split scan).
  2. Refresh: yfinance batch download for stale tickers only (10y full rewrite).

Why yfinance for refresh (not polygon): polygon free tier only has ~2 years
of historical data. The price cache needs 10y for GBM training.

Why full replace (not append): yfinance auto_adjust=True retroactively adjusts
the entire price history on splits/dividends. Appending creates a discontinuity
at the splice point. Full rewrite guarantees internal consistency.

Index tickers (VIX, TNX, IRX): not available on polygon free tier — always
fetched via yfinance with ^ prefix.

Staleness is trading-day-exact (nousergon_lib.dates.is_fresh_in_trading_days),
not a calendar-day delta with a fixed weekend buffer (config#2756). The prior
calendar-day check was calibrated for the weekly-only Saturday cadence; a
fixed "+2 days for weekends" buffer throttles refresh frequency independent of
the caller's actual invocation cadence, so calling ``collect()`` daily under
that check still only refreshed tickers every 3-4 calendar days. Trading-day
arithmetic keeps "stale" meaning the same thing (more than N NYSE sessions
behind) whether ``collect()`` runs weekly (full-universe rebuild) or daily
(only the handful of tickers that missed a session get the full 10y rewrite).
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
import yfinance as yf

from builders._price_cache_writeboth import (
    assert_valid_price_cache_ticker,
    price_cache_read_prefixes,
    price_cache_write_prefixes,
)
from collectors import CaretTickerError
from collectors.price_cache_holes import SessionHoleFiller
from dates import (
    FutureBarError,
    as_trading_day,
    assert_no_bar_after,
    bar_settlement_guard_entry,
    clip_to_trading_day,
    default_run_date,
    history_window,
)
from nousergon_lib.yfinance_quiet import log_yf_coverage, yf_quiet
from shadow.interceptor import guard_baseline_reads
from shadow.root import ShadowGuardViolation

logger = logging.getLogger(__name__)

# Tickers that require a leading caret in yfinance (not available on polygon)
_CARET_SYMBOLS = {"VIX", "VIX3M", "TNX", "IRX"}

# Always-download tickers (benchmarks, macro, sector ETFs)
# config#934: the sub-sector benchmark ETFs (SMH/IGV/XBI/PPH/XOP/KRE/ITA/GDX)
# are appended so the sub_sector_vs_benchmark_* features have a maintained
# price history — these are the distinct non-XL* symbols in
# collectors.constituents.GICS_SUBINDUSTRY_TO_ETF. Kept as a literal list here
# (matching the sector-ETF convention on the line above) rather than imported
# from constituents to avoid a collector→collector import at module load.
_SUB_SECTOR_ETFS = ["SMH", "IGV", "XBI", "PPH", "XOP", "KRE", "ITA", "GDX"]

# alpha-engine-config-I10704: every member of
# ``features.compute.UNIVERSE_BENCHMARK_PROXIES`` MUST appear here, or the
# proxy has no price-cache parquet and the universe write has nothing to write
# from. IWM is the member the pre-I10704 list was missing (the XL* proxies
# were already collected for their Close-only `macro` rows). The relationship
# is enforced by tests/test_benchmark_proxies_i10704.py rather than by a
# runtime import, deliberately: this module is imported by collectors at
# process start and must not pull in the feature-compute import graph.
_ALWAYS_DOWNLOAD = [
    "SPY", "IWM", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC",
    *_SUB_SECTOR_ETFS,
]


# ── Why a ticker was not written (alpha-engine-config-I11547) ───────────────
# Every ticker ``_refresh_stale`` does not upload is counted under exactly ONE
# of these reasons, and ``collect()`` reports each count under its own result
# key (declared in ``run_units.PHASE_UNITS`` as D03's ``rejected_keys``). Until
# I11547 every one of them was recorded as ``short_fetch_guard_refused``,
# because the manifest only saw the total — so on 2026-09-21..23 the shadow
# D03 manifests blamed the short-fetch guard for four tickers it had ACCEPTED,
# and the fetch window was investigated for a defect that sat in the shadow
# interceptor. A reason is a claim about the cause; it is recorded only where
# that cause was observed.
FAIL_SHORT_FETCH = "short_fetch_guard_refused"
FAIL_BEHIND_FETCH = "behind_fetch_guard_refused"
FAIL_NO_DATA = "vendor_no_data"
FAIL_BATCH_ERROR = "batch_fetch_error"
FAIL_REFRESH_ERROR = "refresh_error"

#: reason -> the ``collect()`` result key carrying its count. Mirrored as
#: literals in ``run_units.PHASE_UNITS`` (D03) and in the result dict below;
#: ``tests/test_prices_shadow_recent_listings_i11547.py`` pins the three together.
FAILURE_RESULT_KEYS: dict[str, str] = {
    FAIL_SHORT_FETCH: "failed_short_fetch_refused",
    FAIL_BEHIND_FETCH: "failed_behind_fetch_refused",
    FAIL_NO_DATA: "failed_vendor_no_data",
    FAIL_BATCH_ERROR: "failed_batch_fetch_error",
    FAIL_REFRESH_ERROR: "failed_refresh_error",
}

#: The per-key refusal record (``result["guards"]``) that lets a reader of the
#: manifest — ``shadow/parity.py`` — tell WHICH keys a failed run refused,
#: rather than attributing every absent key to the run's failure (the
#: 2026-09-22 parity report blamed AGNC/CORT/EAT/HUBS on a failure that named
#: FDXF/HONA/Q/SOLS). One keyed entry per refused key, for at most
#: ``_REFUSED_KEYS_RECORD_CAP`` tickers, plus one unkeyed summary entry whose
#: verdict says whether the keyed list is complete.
REFUSED_KEYS_GUARD = "write_refused"
REFUSED_KEYS_COMPLETE = "complete"
REFUSED_KEYS_TRUNCATED = "truncated"
_REFUSED_KEYS_RECORD_CAP = 50


def refused_keys_guard_entries(failure_reasons: "dict[str, str]", s3_prefix: str) -> list[dict]:
    """``result["guards"]`` entries naming every key this run did not write.

    Empty when nothing failed. Keys are addressed under the same write
    prefix(es) the upload uses, so they compare directly with the manifest's
    ``outputs`` and with the live key a parity row is graded on.
    """
    if not failure_reasons:
        return []
    listed = list(failure_reasons.items())[:_REFUSED_KEYS_RECORD_CAP]
    complete = len(listed) == len(failure_reasons)
    entries: list[dict] = [
        {
            "guard": REFUSED_KEYS_GUARD,
            "mode": "enforce",
            "verdict": REFUSED_KEYS_COMPLETE if complete else REFUSED_KEYS_TRUNCATED,
            "detail": (
                f"{len(failure_reasons)} ticker(s) not written this run; "
                + (
                    f"every one is listed in this manifest's keyed {REFUSED_KEYS_GUARD!r} entries"
                    if complete
                    else f"only the first {len(listed)} are listed (cap {_REFUSED_KEYS_RECORD_CAP}), "
                         "so an unlisted key may still have been refused"
                )
            ),
            "key": None,
            "value": float(len(failure_reasons)),
            "baseline": None,
        }
    ]
    for ticker, reason in listed:
        for prefix in price_cache_write_prefixes(s3_prefix):
            entries.append(
                {
                    "guard": REFUSED_KEYS_GUARD,
                    "mode": "enforce",
                    "verdict": "refused",
                    "detail": f"{ticker}: {reason}",
                    "key": f"{prefix}{ticker}.parquet",
                    "value": None,
                    "baseline": None,
                }
            )
    return entries


def collect(
    bucket: str,
    tickers: list[str],
    s3_prefix: str = "predictor/price_cache/",
    fetch_period: str = "10y",
    staleness_threshold_days: int = 3,
    batch_size: int = 50,
    dry_run: bool = False,
    reference_date: str | date | None = None,
) -> dict:
    """
    Identify stale tickers and refresh their price cache parquets.

    Uses polygon grouped-daily for fast staleness check (1 API call),
    then yfinance batch download for the actual 10y refresh.

    Args:
        bucket: S3 bucket name
        tickers: full universe of tickers to maintain
        s3_prefix: S3 key prefix for price cache parquets
        fetch_period: yfinance period string for full refresh
        staleness_threshold_days: NYSE trading sessions before a parquet is stale
        batch_size: tickers per yfinance batch download
        dry_run: if True, identify stale tickers but don't fetch/upload
        reference_date: trading day staleness is measured against (ISO string
            or ``date``). Defaults to today's UTC calendar date — pass the
            caller's ``run_date`` explicitly so a re-run against a fixed date
            is deterministic across weekly and daily invocations.

    Returns:
        dict with status, refreshed count, errors
    """
    s3 = boto3.client("s3")
    all_tickers = list(dict.fromkeys(tickers + _ALWAYS_DOWNLOAD))

    # ── Fast staleness check via S3 metadata ─────────────────────────────────
    # Instead of downloading all parquets, list the LIVE prefix and check
    # last-modified (alpha-engine-config-I11518), plus the split guard.
    split_forced: dict[str, str] = {}
    stale = _find_stale_fast(
        s3, bucket, s3_prefix, all_tickers, staleness_threshold_days, reference_date,
        forced_refresh=split_forced,
    )

    if not stale:
        logger.info("Price cache is current — no refresh needed (%d tickers checked)", len(all_tickers))
        return {"status": "ok", "refreshed": 0, "stale": 0, "total": len(all_tickers)}

    logger.info("%d / %d tickers are stale or missing", len(stale), len(all_tickers))

    if dry_run:
        return {
            "status": "ok_dry_run",
            "stale": len(stale),
            "stale_sample": stale[:20],
            "total": len(all_tickers),
        }

    # ── Refresh stale tickers via yfinance ───────────────────────────────────
    # alpha-engine-config-I10893: the refresh window is anchored on the run's
    # trading day, never on wall-clock time — a ``--date D`` run executed on
    # D+1 must not fetch (or publish) the partial D+1 session.
    trading_day = str(reference_date) if reference_date is not None else default_run_date()
    # alpha-engine-config-I11354: the moment this run's vendor fetch opens.
    # Taken HERE rather than inside the batch loop because the whole refresh is
    # one fetch window and its OPENING edge is the conservative one — a run that
    # starts before the bar settles does not become settled because it ran long.
    fetch_started_at = datetime.now(timezone.utc)
    short_fetch_retries: dict[str, int] = {}
    failure_reasons: dict[str, str] = {}
    refreshed, failed_tickers, written = _refresh_stale(
        s3, bucket, s3_prefix, stale, fetch_period, batch_size,
        trading_day=trading_day, short_fetch_retries=short_fetch_retries,
        failure_reasons=failure_reasons,
    )
    # A failed ticker with no observed cause is an error of the refresh, never
    # a guard refusal — so the per-cause counts always sum to `failed`.
    for ticker in failed_tickers:
        failure_reasons.setdefault(ticker, FAIL_REFRESH_ERROR)
    failure_counts = {reason: 0 for reason in FAILURE_RESULT_KEYS}
    for reason in failure_reasons.values():
        failure_counts[reason] += 1

    # ── Validate refreshed tickers ─────────────────────────────────────────
    validation = {}
    if refreshed > 0:
        try:
            from validators.price_validator import validate_refreshed
            refreshed_tickers = [t for t in stale if t not in failed_tickers]
            validation = validate_refreshed(s3, bucket, s3_prefix, refreshed_tickers)
        except Exception as e:
            logger.warning("Price validation failed (non-fatal): %s", e)

    result = {
        "status": "ok" if not failed_tickers else "partial",
        "refreshed": refreshed,
        "stale": len(stale),
        "failed": len(failed_tickers),
        "failed_tickers": failed_tickers[:20],
        # alpha-engine-config-I11547: the same `failed` total, split by the
        # cause actually observed (the five always sum to `failed`). D03's
        # `rejected_keys` reads these, never the undifferentiated total.
        "failed_short_fetch_refused": failure_counts[FAIL_SHORT_FETCH],
        "failed_behind_fetch_refused": failure_counts[FAIL_BEHIND_FETCH],
        "failed_vendor_no_data": failure_counts[FAIL_NO_DATA],
        "failed_batch_fetch_error": failure_counts[FAIL_BATCH_ERROR],
        "failed_refresh_error": failure_counts[FAIL_REFRESH_ERROR],
        "total": len(all_tickers),
        # alpha-engine-config-I11026: the per-ticker keys + row counts this
        # run actually uploaded — never a copy of `stale` (attempted, not
        # written), never the batch's aggregate `refreshed` count standing in
        # for every key's row count. `written_keys()` below turns this into
        # the manifest's `extra_outputs` callable form.
        "written": dict(written),
        # alpha-engine-config-I11354: grade THIS run's bar on the settlement
        # clock and carry the verdict on D03's manifest. Observe mode — the
        # reading never moves the exit code; it is what a promotion to enforce
        # (and Brian's ruling on the 16:45 ET `data-collection-eod` schedule)
        # will be argued from. `_record_collector_guards` folds this on.
        "guards": [
            bar_settlement_guard_entry(
                fetch_started_at, trading_day, key=f"{s3_prefix}*.parquet",
            ),
            *refused_keys_guard_entries(failure_reasons, s3_prefix),
        ],
    }
    if split_forced:
        # alpha-engine-config-I11518: every ticker the split guard pulled into
        # the refresh although it was fresh by age, with the reason (bounded:
        # a split scan failure names the whole fresh set, so cap the sample).
        result["split_forced_refresh"] = len(split_forced)
        result["split_forced_sample"] = dict(list(split_forced.items())[:20])
    if short_fetch_retries:
        # alpha-engine-config-I11287: never silent — every ticker that
        # entered the short-fetch guard's bounded retry is named here with
        # its attempt count, whether or not the retry recovered it.
        result["short_fetch_retries"] = dict(short_fetch_retries)
    if failed_tickers:
        # alpha-engine-config-I11230 deliverable 2: `_DegradedRun` (the
        # manifest-level handler for `status="partial"`) reads
        # `error`/`detail`/`reason` for the failure it records on D03's
        # manifest — without this key it falls back to "no detail reported",
        # which is exactly the "carrying neither the failed tickers nor a
        # reason" gap the issue names. Bounded to the same 20-ticker sample as
        # `failed_tickers` above (never the full ~900-ticker universe —
        # alpha-engine-config-I10941).
        _sample = ", ".join(failed_tickers[:20])
        _more = f" (+{len(failed_tickers) - 20} more)" if len(failed_tickers) > 20 else ""
        _by_reason = ", ".join(f"{reason}={count}" for reason, count in failure_counts.items() if count)
        result["reason"] = (
            f"{len(failed_tickers)} of {len(all_tickers)} tickers failed to refresh "
            f"({_by_reason}): {_sample}{_more}"
        )
        # Bounded like `failed_tickers` (alpha-engine-config-I10941).
        result["failure_reasons"] = dict(list(failure_reasons.items())[:20])
    if validation:
        result["validation"] = validation
    return result


def written_keys(result: dict, s3_prefix: str = "predictor/price_cache/") -> dict[str, int]:
    """``{s3_key: row_count}`` for every ticker parquet ``collect()`` actually
    uploaded this run, addressed under the SAME write prefix(es) the upload
    itself used (``price_cache_write_prefixes`` — post-cutover, exactly one:
    ``reference/price_cache/``). Reads only ``result["written"]`` (the record
    of what was written), never the requested ticker population, so a ticker
    that failed or was never stale never appears here
    (alpha-engine-config-I11026)."""
    out: dict[str, int] = {}
    for ticker, rows in (result.get("written") or {}).items():
        for prefix in price_cache_write_prefixes(s3_prefix):
            out[f"{prefix}{ticker}.parquet"] = int(rows or 0)
    return out


def _reject_caret_tickers(tickers: list[str], context: str) -> list[str]:
    """Drop any ``^``-prefixed entry from ``tickers``, one WARNING per stray
    (alpha-engine-config-I9288).

    Price-cache tickers are bare names everywhere in this module except the
    yfinance-request boundary inside ``_refresh_stale``, which prepends
    ``^`` itself for members of ``_CARET_SYMBOLS``. A caret-prefixed literal
    reaching a population built here is always a stray — either a caller
    upstream already embedded the caret (measured root cause:
    ``weekly_collector.py``'s ``_MACRO_DAILY_TICKERS``, which
    ``daily_closes.collect`` wants caret-prefixed but which is not this
    collector's contract), or a residual key from a prior run of the bug
    this closes (``^VIX``, ``^TNX``, ``^VIX3M`` observed live in
    ``reference/price_cache/``).

    Dropping (not raising) here is deliberate: a stray key already sitting
    in S3, or a caller passing one, must not halt a producer run — but it
    must not be silent either, so every drop is a named WARNING a human or
    Flow Doctor can act on. Contrast with
    ``builders._price_cache_writeboth.assert_valid_price_cache_ticker``,
    which RAISES — that is the write-time chokepoint of last resort, this
    is the population-level filter that keeps the collector running.
    """
    clean: list[str] = []
    for t in tickers:
        if t.startswith("^"):
            logger.warning(
                "%s: dropping stray caret-prefixed ticker %r from the price-cache "
                "population — price-cache tickers are bare names; caret-prefixing "
                "is internal to the yfinance-request boundary only "
                "(_CARET_SYMBOLS). See alpha-engine-config-I9288.",
                context, t,
            )
            continue
        clean.append(t)
    return clean


def _implied_last_bar(last_modified: datetime) -> date:
    """The newest NYSE session whose close had settled when an object was
    written — the most a parquet written at ``last_modified`` can hold.

    ``LastModified``'s UTC calendar date overstates that: the daily EOD refresh
    writes at ~23:xx UTC, and a run that crosses midnight (the 2026-09-23
    rehearsal launched at 00:00 UTC — alpha-engine-config-I11467) stamps D+1 on
    a parquet whose last bar is D, which let a ``max_stale=1`` gate tolerate a
    TWO-session lag. A write during a session likewise cannot hold that
    session. Mapping the write instant onto the trading-day axis is never less
    conservative than the calendar date: it can only make a parquet read older.
    """
    from nousergon_lib.dates import last_closed_trading_day

    ts = last_modified
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return last_closed_trading_day(ts)


def _list_live_cache(s3, bucket: str, prefix: str) -> dict[str, tuple[str, datetime]]:
    """``{ticker: (key, LastModified)}`` for every per-ticker parquet under the
    LIVE read prefixes (``price_cache_read_prefixes(prefix)``), first prefix
    wins per ticker.

    alpha-engine-config-I11518: this used to list ``prefix`` verbatim, and every
    production caller passes the retired ``predictor/price_cache/`` sentinel,
    whose tree froze on 2026-06-19 (939 objects). Every ticker therefore read
    stale on every run and was re-fetched for 10 years. The sentinel now
    resolves through the same chokepoint the refresh WRITES through, so the scan
    sees what the writer produced. Only direct children of a prefix count — a
    nested key cannot stand in for a missing ticker.
    """
    existing: dict[str, tuple[str, datetime]] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for read_prefix in price_cache_read_prefixes(prefix):
        n = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=read_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet") or not key.startswith(read_prefix):
                    continue
                rest = key[len(read_prefix):]
                if "/" in rest:
                    continue
                ticker = rest[: -len(".parquet")]
                if ticker.startswith("^"):
                    logger.warning(
                        "_find_stale_fast: stray caret-prefixed price-cache key %s "
                        "excluded from the staleness map — bare names are the "
                        "contract for this listing. See alpha-engine-config-I9288.",
                        key,
                    )
                    continue
                n += 1
                existing.setdefault(ticker, (key, obj["LastModified"]))
        logger.info("S3 cache: %d parquets under s3://%s/%s", n, bucket, read_prefix)
    return existing


def _find_stale_fast(
    s3,
    bucket: str,
    prefix: str,
    all_tickers: list[str],
    staleness_threshold_days: int,
    reference_date: str | date | None = None,
    *,
    split_scan=None,
    forced_refresh: "dict[str, str] | None" = None,
) -> list[str]:
    """
    Fast staleness check using S3 object metadata (no downloads).

    Lists the per-ticker parquets under the LIVE read prefixes
    (:func:`_list_live_cache` — never the retired ``predictor/price_cache/``
    tree, alpha-engine-config-I11518) and maps each object's ``LastModified``
    onto the NYSE trading-day axis (:func:`_implied_last_bar`), then checks it
    against ``reference_date`` with nousergon_lib.dates.is_fresh_in_trading_days
    — holiday/weekend-aware, so the same ``staleness_threshold_days`` value
    means "N trading sessions behind" whether this runs weekly or daily.

    A ticker is stale when it has NO object under the live prefixes (a full
    fetch, never a silent skip), when its parquet is more than
    ``staleness_threshold_days`` sessions behind, or when the split guard
    (:func:`_split_guard`) finds a recent split the parquet's history does not
    reflect. ``split_scan(start, end) -> list[CorporateAction]`` defaults to the
    polygon whole-market split scan; tests inject one. ``forced_refresh``, if
    given, is filled in place with ``{ticker: reason}`` for every ticker the
    split guard added.

    A ``^``-prefixed basename must never become a ticker here (alpha-engine-
    config-I9288): neither a stray S3 key discovered by listing nor a
    caret-embedded literal in ``all_tickers`` enters the staleness map or
    the refresh population — both are dropped with a WARNING via
    :func:`_reject_caret_tickers`.
    """
    from nousergon_lib.dates import is_fresh_in_trading_days

    reference = as_trading_day(
        reference_date if reference_date is not None else datetime.now(timezone.utc).date()
    )

    all_tickers = _reject_caret_tickers(all_tickers, "_find_stale_fast: requested tickers")

    existing = _list_live_cache(s3, bucket, prefix)

    stale: list[str] = []
    fresh: dict[str, tuple[str, datetime]] = {}
    n_missing = 0
    for ticker in all_tickers:
        found = existing.get(ticker)
        if found is None:
            n_missing += 1
            stale.append(ticker)
        elif not is_fresh_in_trading_days(
            _implied_last_bar(found[1]), reference, max_stale=staleness_threshold_days,
        ):
            stale.append(ticker)
        else:
            fresh[ticker] = found

    forced = _split_guard(s3, bucket, fresh, reference, split_scan=split_scan)
    if forced_refresh is not None:
        forced_refresh.update(forced)
    if forced:
        already = set(stale)
        stale = [t for t in all_tickers if t in already or t in forced]

    logger.info(
        "Staleness (threshold=%d sessions, reference=%s): %d requested, %d missing "
        "under the live prefix, %d aged out, %d forced by the split guard, %d fresh",
        staleness_threshold_days, reference.isoformat(), len(all_tickers), n_missing,
        len(stale) - n_missing - len(forced), len(forced), len(fresh) - len(forced),
    )
    return stale


# ── Split guard (alpha-engine-config-I11518) ────────────────────────────────
# Before I11518 the scan read the frozen legacy tree, so every ticker was
# re-fetched (10y, ``auto_adjust=True``) on every run and any split was folded
# into the whole history within a day, whatever else had touched the parquet.
# Once the scan reads the live tree a fresh parquet is SKIPPED, and two things
# can then leave its history on the wrong scale:
#   1. it was written before a split's ex_date — the whole history sits on the
#      pre-split basis until the ticker next ages out;
#   2. the chronic-gap self-heal (``weekly_collector._self_heal_chronic_polygon_gaps``)
#      APPENDED post-split adjusted rows onto pre-split history. That write
#      bumps LastModified, so the append itself makes the seam look fresh.
# Either way the fix is the full re-fetch this module already does, so the
# guard only decides WHICH fresh tickers get one. Splits come from polygon's
# whole-market split scan (one call per run); a ticker's parquet is read only
# when a split executed after it was written could still be un-flattened in it.
# Window over which executed splits are considered. Covers any fresh parquet's
# age (a few sessions at every configured threshold) with room for a self-heal
# seam written weeks after the ticker's last full rewrite.
_SPLIT_GUARD_LOOKBACK_DAYS = 30


def _polygon_split_scan(start: str, end: str) -> list:
    """Whole-market splits executed in ``[start, end]`` as CorporateActions.

    RAISES on any client or fetch failure (unlike ``corporate_actions.
    detect_splits``, which degrades to ``[]``): the guard must be able to tell
    "no splits" from "could not look", because the second one has to fall back
    to a full refresh rather than skip tickers blind.
    """
    from corporate_actions import splits_from_events
    from polygon_client import polygon_client

    return splits_from_events(polygon_client().get_recent_splits(start, end))


def _scrubbed(exc: Exception) -> str:
    """``exc`` named by its type only.

    The split scan's request URL carries the polygon apiKey, and an HTTP
    exception's text echoes that URL. A regex scrub is not a sanitiser CodeQL
    (or a reviewer) can verify, so the message is never logged at all — the
    type is enough to tell a 429 from a timeout from a parse error here.
    """
    return type(exc).__name__


def _cache_ticker(polygon_ticker: str) -> str:
    """Polygon class-share tickers use ``.`` (``BRK.B``); the cache uses ``-``."""
    return str(polygon_ticker).replace(".", "-")


def _split_seam_reason(df: pd.DataFrame, action, implied_last: date) -> "str | None":
    """Why ``df`` (a fresh parquet) does not reflect ``action``, or None if it does."""
    from corporate_actions import _ORIENTATION_MIN_SEPARATION, expected_factor, price_evidence_orientation

    ex = as_trading_day(action.ex_date)
    label = f"split {action.split_from}:{action.split_to} ex {ex.isoformat()}"
    last_bar = _last_bar_date(df.index) if not df.empty else None
    if last_bar is None or last_bar < ex:
        return (
            f"{label}: parquet ends {last_bar.isoformat() if last_bar else 'empty'}, "
            "before the ex_date, so its whole history is on the pre-split basis"
        )
    try:
        factor = expected_factor(action)
    except Exception:  # noqa: BLE001 - malformed ratio: nothing to test the seam against
        return None
    if 1.0 / _ORIENTATION_MIN_SEPARATION < factor < _ORIENTATION_MIN_SEPARATION:
        # A near-1 record (polygon's 1000:1061 spinoff-style ratios) cannot be
        # told apart from an ordinary daily move, so only the ex_date/last-bar
        # checks above apply to it.
        return None
    if "Close" not in df.columns:
        return f"{label}: parquet has no Close column to check the boundary"
    close = df["Close"].copy()
    idx = pd.to_datetime(close.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    close.index = idx
    verdict = price_evidence_orientation(close.dropna(), action)
    if verdict in ("direct", "inverse", "ambiguous"):
        return (
            f"{label}: the adjusted Close still jumps by the split factor at the "
            f"boundary ({verdict}) — pre-split rows were never re-adjusted"
        )
    return None


def _split_guard(
    s3,
    bucket: str,
    fresh: dict[str, tuple[str, datetime]],
    reference: date,
    *,
    split_scan=None,
) -> dict[str, str]:
    """``{ticker: reason}`` for every FRESH ticker whose parquet does not
    reflect a split executed in the last ``_SPLIT_GUARD_LOOKBACK_DAYS``.

    * Written before the ex_date (by :func:`_implied_last_bar`) → re-fetch,
      no read needed.
    * Written on/after it → read the parquet: a last bar before the ex_date, or
      a boundary move matching the split factor (the self-heal append seam),
      → re-fetch. A read failure → re-fetch (the refresh has its own guards;
      skipping blind is the failure this exists to stop).

    If the split scan itself fails, EVERY fresh ticker is returned: that is the
    pre-I11518 full refresh, never a skip made without looking.
    """
    if not fresh:
        return {}
    scan = split_scan if split_scan is not None else _polygon_split_scan
    start = reference - timedelta(days=_SPLIT_GUARD_LOOKBACK_DAYS)
    try:
        actions = scan(start.isoformat(), reference.isoformat())
    except Exception as exc:  # noqa: BLE001 - degrade to a full refresh, never a blind skip
        logger.warning(
            "Split guard: the polygon split scan failed (%s) — refreshing all %d "
            "fresh tickers this run rather than skipping them without knowing "
            "whether a split restated their history (alpha-engine-config-I11518).",
            _scrubbed(exc), len(fresh),
        )
        return {t: "split scan unavailable this run" for t in fresh}

    by_ticker: dict[str, list] = {}
    for action in actions or []:
        if getattr(action, "type", "split") != "split":
            continue
        try:
            ex = as_trading_day(action.ex_date)
        except Exception:  # noqa: BLE001 - unparseable ex_date is not a candidate
            continue
        if ex > reference:
            continue
        ticker = _cache_ticker(action.ticker)
        if ticker in fresh:
            by_ticker.setdefault(ticker, []).append(action)

    import io as _io

    forced: dict[str, str] = {}
    for ticker, ticker_actions in by_ticker.items():
        key, last_modified = fresh[ticker]
        implied_last = _implied_last_bar(last_modified)
        reason = None
        df = None
        for action in sorted(ticker_actions, key=lambda a: a.ex_date):
            ex = as_trading_day(action.ex_date)
            if implied_last < ex:
                reason = (
                    f"split {action.split_from}:{action.split_to} ex {ex.isoformat()}: "
                    f"parquet written {last_modified.isoformat()} (holds at most "
                    f"{implied_last.isoformat()}), before the ex_date"
                )
                break
            if df is None:
                try:
                    # A write guard's baseline, not an input: it decides
                    # whether to re-fetch and is never published
                    # (alpha-engine-config-I11547).
                    with guard_baseline_reads():
                        obj = s3.get_object(Bucket=bucket, Key=key)
                    df = pd.read_parquet(_io.BytesIO(obj["Body"].read()))
                except Exception as exc:  # noqa: BLE001 - unverifiable → re-fetch, logged below
                    reason = f"could not read s3://{bucket}/{key} to check the split: {exc}"
                    break
            reason = _split_seam_reason(df, action, implied_last)
            if reason:
                break
        if reason:
            forced[ticker] = reason
            logger.warning(
                "Split guard: %s is fresh by age but will be re-fetched — %s "
                "(alpha-engine-config-I11518).", ticker, reason,
            )
    return forced


# Row count below which a "full period" yfinance refresh is treated as
# SUSPECT and checked against what the price cache already holds.
# ~400 trading days is ~18 months — far under the 10y (~2500 row) fetch every
# maintained ticker returns, and comfortably over a genuinely new listing that
# has no existing parquet to regress. Only frames under this threshold pay the
# extra S3 GET, so the guard is free on the ~900-ticker happy path.
_SHORT_FETCH_ROW_THRESHOLD = 400


# ── Short-fetch guard: bounded single-ticker retry (alpha-engine-config-
# I11287) ─────────────────────────────────────────────────────────────────
# A short answer is frequently a transient vendor glitch, not a real
# regression (measured: ^VIX3M's two 2026-09-15 guard hits were traced to a
# stray caret-embedded ticker literal fixed same-day by I9288 — but the
# guard itself is a general defense against ANY vendor's intermittent short
# reply, caret or not, and that class stays live). Today the only recovery
# path is the weekly SF re-running the ENTIRE ~5,474s DataPhase1 stage
# (`infrastructure/step_function.json::DataPhase1RetryGate`) for one bad
# ticker out of ~900. A few extra seconds re-fetching just that ticker here
# is far cheaper than redoing 91 minutes of work — but "retry the flaky
# call" must itself be bounded on BOTH axes (attempts per ticker, and
# tickers-per-run), never just the first: a per-call cap that says nothing
# about call count is the defect class this fleet has already paid for
# (bugclass_a_per_call_cap_that_says_nothing_about_call_count_260921).
_SHORT_FETCH_RETRY_ATTEMPTS = 3
# Backoff applied before each of the 3 dedicated re-fetch attempts, with
# +/-25% jitter at call time (avoids every retried ticker hammering the
# vendor on the same cadence). Fixed schedule, not a config knob: this
# guard fired twice in 30 days fleet-wide, so a tunable is over-engineering
# for the observed rate.
_SHORT_FETCH_RETRY_BACKOFF_SECONDS = (2.0, 4.0, 8.0)
# Hard cap on how many DISTINCT tickers may enter the retry path in one
# run. Bounds total added time even if a systemic vendor issue makes the
# guard fire broadly instead of on 1-2 tickers: worst case is
# `_SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN` tickers each exhausting all 3
# attempts — (2+4+8)s backoff + ~3 single-symbol `yf.download` calls
# (bounded by the vendor's own per-call latency, typically low single-digit
# seconds) per ticker, i.e. roughly 10 * 30s ~= 5 minutes added, against a
# ~5,474s DataPhase1 stage. A ticker that exhausts the run-level budget is
# refused immediately, exactly as before this change, with a WARNING
# naming that the budget (not the ticker's own attempts) was the limiter.
_SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN = 10


def _sleep_seconds(seconds: float) -> None:
    """Thin wrapper around ``time.sleep`` so tests can monkeypatch retry
    backoff to zero without changing the production delay."""
    import time

    time.sleep(seconds)


def _retry_short_fetch_ticker(
    ticker: str,
    yf_sym: str,
    window_start,
    window_end_excl,
    trading_day: "str | date",
) -> tuple["pd.DataFrame | None", int]:
    """Re-fetch a SINGLE ticker up to ``_SHORT_FETCH_RETRY_ATTEMPTS`` times
    after the batch fetch answered short.

    Never raises: a download error on a retry attempt is logged and treated
    as "this attempt found nothing", identically to a short/empty answer —
    the caller (the short-fetch guard) reports the persistent refusal
    exactly as it did before this change if every attempt fails.

    Returns ``(best_df, attempts_made)``. ``best_df`` is the LONGEST clean
    frame seen across attempts (never worse than giving up after one try),
    or ``None`` if every attempt errored or came back empty.
    """
    import random

    best_df: "pd.DataFrame | None" = None
    attempts_made = 0
    for attempt_idx, base_delay in enumerate(_SHORT_FETCH_RETRY_BACKOFF_SECONDS, start=1):
        attempts_made = attempt_idx
        _sleep_seconds(base_delay * random.uniform(0.75, 1.25))
        try:
            raw = yf.download(
                tickers=yf_sym,
                start=window_start.isoformat(),
                end=window_end_excl.isoformat(),
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=False,
                # yfinance >= 0.2.48 answers even a single ticker with
                # (Price, Ticker) MultiIndex columns by default. `"Close" in
                # columns` is still True on that frame but
                # `dropna(subset=["Close"])` raises KeyError(['Close']), so
                # before this every retry died on its first answer and the
                # caller logged "Refresh failed for <T>: ['Close']" — HONA, Q,
                # FDXF and SOLS on the 2026-09-23 rehearsal
                # (alpha-engine-config-I11445).
                multi_level_index=False,
            )
        except Exception as exc:  # noqa: BLE001 - a failed attempt is not fatal, logged and retried
            logger.warning(
                "Short-fetch retry %d/%d for %s failed: %s",
                attempt_idx, _SHORT_FETCH_RETRY_ATTEMPTS, ticker, exc,
            )
            continue

        try:
            if raw is None or raw.empty:
                continue
            if isinstance(raw.columns, pd.MultiIndex):
                raw = raw.copy()
                raw.columns = raw.columns.get_level_values(0)
            if "Close" not in raw.columns:
                continue
            df = raw.dropna(subset=["Close"])
            if df.empty:
                continue

            idx = pd.to_datetime(df.index)
            if idx.tz is not None:
                idx = idx.tz_convert("UTC").tz_localize(None)
            df.index = idx
            df = clip_to_trading_day(
                df.sort_index(), trading_day, label=f"price_cache_refresh_retry[{ticker}]",
            )
        except Exception as exc:  # noqa: BLE001 - the docstring's never-raises contract
            logger.warning(
                "Short-fetch retry %d/%d for %s returned an unusable frame: %s",
                attempt_idx, _SHORT_FETCH_RETRY_ATTEMPTS, ticker, exc,
            )
            continue
        if best_df is None or len(df) > len(best_df):
            best_df = df
        if len(best_df) >= _SHORT_FETCH_ROW_THRESHOLD:
            break

    return best_df, attempts_made


def _is_missing_object(s3, exc: Exception) -> bool:
    """True when ``exc`` means "this S3 key does not exist" (and nothing else).

    Deliberately narrow: every other failure (throttle, 403, transient network)
    must NOT be read as "no existing parquet", or a short fetch would overwrite
    a full history on a blip. Matches both the botocore ``ClientError`` code and
    the modelled ``s3.exceptions.NoSuchKey`` class name so it holds for real
    clients and for test doubles alike.
    """
    code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
    if code in {"NoSuchKey", "NoSuchBucket", "404"}:
        return True
    modelled = getattr(getattr(s3, "exceptions", None), "NoSuchKey", None)
    if isinstance(modelled, type) and isinstance(exc, modelled):
        return True
    return type(exc).__name__.lstrip("_") in {"NoSuchKey", "NoSuchBucketError"}


def _read_existing_parquet(
    s3, bucket: str, s3_prefix: str, ticker: str, *, purpose: str,
) -> "pd.DataFrame | None":
    """The ticker's current price-cache parquet, or None if absent.

    Any read failure other than "the object does not exist" RAISES — treating an
    unreadable parquet as absent would let a bad fetch overwrite a good history
    on a transient S3 error, which is the same silent-degrade the write guards
    below exist to stop. ``purpose`` names the guard in that error.

    Every caller is a WRITE GUARD comparing the fetch with what it would
    overwrite: the frame read here decides whether to publish and is never
    published. Under a shadow root it is therefore a guard baseline
    (``shadow.interceptor.guard_baseline_reads``) — read live, not recorded as
    an input. Recorded as an input, it made the upload of the same key raise,
    which is why every same-day shadow run refused FDXF, HONA, Q and SOLS while
    v1 wrote them (alpha-engine-config-I11547).
    """
    import io as _io

    last_exc: Exception | None = None
    for prefix in price_cache_read_prefixes(s3_prefix):
        try:
            with guard_baseline_reads():
                obj = s3.get_object(Bucket=bucket, Key=f"{prefix}{ticker}.parquet")
        except Exception as exc:  # noqa: BLE001 - classified below, never swallowed
            if _is_missing_object(s3, exc):
                continue
            last_exc = exc
            continue
        return pd.read_parquet(_io.BytesIO(obj["Body"].read()))
    if last_exc is not None:
        raise RuntimeError(
            f"{purpose}: could not read the existing price-cache parquet "
            f"for {ticker}: {last_exc}"
        ) from last_exc
    return None


def _existing_parquet_rows(s3, bucket: str, s3_prefix: str, ticker: str) -> int | None:
    """Row count of the ticker's current price-cache parquet, or None if absent.

    Raises on any read failure other than "the object does not exist" (see
    :func:`_read_existing_parquet`).
    """
    df = _read_existing_parquet(
        s3, bucket, s3_prefix, ticker,
        purpose="Short-fetch guard (checking for truncation)",
    )
    return None if df is None else len(df)


def _last_bar_date(index) -> "date | None":
    """The calendar date of the newest bar in ``index`` (UTC-normalized), or
    None for an empty index."""
    if len(index) == 0:
        return None
    idx = pd.to_datetime(pd.Index(index))
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    last = idx.max()
    return None if pd.isna(last) else last.date()


def _existing_parquet_last_bar(s3, bucket: str, s3_prefix: str, ticker: str) -> "date | None":
    """Date of the newest bar in the ticker's current price-cache parquet, or
    None if there is no parquet. Raises on any read failure other than "the
    object does not exist" (see :func:`_read_existing_parquet`)."""
    df = _read_existing_parquet(
        s3, bucket, s3_prefix, ticker,
        purpose="Behind-fetch guard (checking the cached last bar)",
    )
    return None if df is None else _last_bar_date(df.index)


def _expected_last_bar(trading_day: "str | date") -> date:
    """The newest bar a history fetch serving ``trading_day`` should end on.

    ``nousergon_lib.dates.expected_last_close`` on the NYSE calendar: D itself
    when D is a session, else the session before it (a Saturday run expects
    Friday). Anchored on the run's trading day, never on the box's UTC clock.
    Falls back to D's own calendar date if the lib lookup raises — that is the
    conservative direction (it can only make the behind-fetch guard look at the
    cache MORE often, never less).
    """
    d = as_trading_day(trading_day)
    try:
        from nousergon_lib.dates import expected_last_close

        return expected_last_close(d)
    except Exception:  # noqa: BLE001 - a calendar miss must not block the refresh
        logger.warning(
            "_expected_last_bar: expected_last_close(%s) failed; using the "
            "calendar date as the expected last bar", d.isoformat(), exc_info=True,
        )
        return d


def _longest_of(candidates: list[tuple[str, pd.DataFrame]]) -> tuple[str, pd.DataFrame]:
    """Pick the longest of several candidate history frames, by row count.

    Deliberate rather than a preference order (mirrors
    ``builders/repair_macro_series.py::fetch_full_history``): the failure this
    guards against is a SOURCE answering short, so the selection can't assume
    any one source is healthy. Raises if every candidate is empty/None.
    """
    named = [(name, df) for name, df in candidates if df is not None and not df.empty]
    if not named:
        raise RuntimeError("_longest_of: no usable candidate frames")
    return max(named, key=lambda kv: len(kv[1]))


def _fred_ohlcv_for_caret_symbol(
    ticker: str, period: str, *, trading_day: "str | date | None" = None,
) -> "pd.DataFrame | None":
    """Fetch ``ticker``'s FRED-sourced history, reshaped to the yfinance OHLCV
    column set. Returns ``None`` (never raises) on any failure — the caller
    falls back to whatever yfinance answered, since FRED being unavailable
    must degrade to yfinance rather than to nothing (alpha-engine-config-I9286
    deliverable 2). ``trading_day`` bounds the FRED observation window's end
    (alpha-engine-config-I10893); ``None`` keeps the lookup-only callers that
    never publish (tests of the unmapped-symbol path) working.
    """
    from collectors.fred_history import FRED_HISTORY_MAP, fetch_fred_history, fred_history_to_ohlcv

    series_id = FRED_HISTORY_MAP.get(ticker)
    if series_id is None:
        return None
    years = int(period.rstrip("y")) if period.endswith("y") and period[:-1].isdigit() else 10
    try:
        fred_df = fetch_fred_history(series_id, period_years=years, end_date=trading_day)
        out = fred_history_to_ohlcv(fred_df)
    except Exception as exc:
        logger.warning(
            "FRED history fetch failed for %s (%s): %s — falling back to yfinance",
            ticker, series_id, exc,
        )
        return None
    idx = pd.to_datetime(out.index)
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    out.index = idx.normalize()
    # Match yfinance's minimal OHLCV column set — the FRED shape carries
    # Adj_Close/VWAP/source too, and letting those vary week-to-week on the
    # SAME ticker's parquet (yfinance-shaped one week, FRED-shaped the next)
    # is schema churn the parquet contract doesn't need here.
    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in out.columns]
    return out[keep]


@yf_quiet
def _refresh_stale(
    s3,
    bucket: str,
    s3_prefix: str,
    stale: list[str],
    fetch_period: str,
    batch_size: int,
    *,
    trading_day: "str | date",
    short_fetch_retries: "dict[str, int] | None" = None,
    failure_reasons: "dict[str, str] | None" = None,
) -> tuple[int, list[str], list[tuple[str, int]]]:
    """Batch-fetch stale tickers from yfinance and upload to S3.

    ``failure_reasons`` (alpha-engine-config-I11547), if given, is populated in
    place with ``{ticker: reason}`` — one of the ``FAIL_*`` constants — for
    every ticker in ``failed_tickers``, naming the cause actually observed.
    Same out-parameter shape as ``short_fetch_retries``, for the same reason.

    A :class:`shadow.root.ShadowGuardViolation` propagates rather than becoming
    a per-ticker failure: its own contract is "always fatal", and folding it
    into a per-ticker miss is what let the I11547 violation read as four
    short-fetch refusals on three consecutive shadow manifests.

    ``short_fetch_retries`` (alpha-engine-config-I11287), if given, is
    populated in place with ``{ticker: attempts_made}`` for every ticker
    that entered the short-fetch guard's bounded retry path this run —
    whether the retry recovered the ticker or it still reads failed. Kept
    as an optional out-parameter rather than a new return value so the
    ``(refreshed, failed_tickers, written)`` 3-tuple every existing caller
    and test unpacks stays unchanged.

    ``trading_day`` (required, alpha-engine-config-I10893) bounds every fetch
    to ``[trading_day − fetch_period, trading_day]`` via explicit
    ``start``/``end`` — never ``period=``, which ends at vendor "now" and so
    published a partial D+1 session on a ``--date D`` rerun. Each frame is
    checked by :func:`dates.assert_no_bar_after` immediately before upload;
    a :class:`dates.FutureBarError` propagates (not a per-ticker failure).

    Runs under ``yf_quiet`` (nousergon_lib.yfinance_quiet): yfinance's
    per-symbol "possibly delisted" ERROR spray is demoted so one transient/
    unpriceable ticker can't storm Flow Doctor with a report per worded
    variant (the 2026-06-19 PCAR recurrence of the config#1029 PCKM storm).
    The replacement recording surface is the aggregated ``log_yf_coverage``
    record emitted before returning.

    Returns ``(refreshed, failed_tickers, written)`` — ``written`` is the
    ``[(ticker, row_count)]`` list for every ticker actually uploaded this
    run, appended ONLY after ``s3.upload_file`` succeeds (never a copy of
    ``stale``, never a count of attempts). ``alpha-engine-config-I11026``:
    the per-symbol key set D03 publishes is knowable only from what this run
    wrote, so the caller (``collect()``) hands this straight to the run
    manifest via the ``extra_outputs`` callable form rather than the
    descriptor's declared (and here nonexistent) fixed key list.
    """
    import time

    window_start, window_end_excl = history_window(trading_day, fetch_period)
    expected_last = _expected_last_bar(trading_day)
    # alpha-engine-config-I11553: an answer can be current at both ends and
    # still miss a session in the middle — see collectors/price_cache_holes.py.
    hole_filler = SessionHoleFiller(
        s3, bucket,
        window_start=window_start, expected_last=expected_last,
        cache_keys=lambda t: [f"{p}{t}.parquet" for p in price_cache_read_prefixes(s3_prefix)],
        skip=_CARET_SYMBOLS,
    )
    logger.info(
        "Refreshing %d stale tickers (window=%s: start=%s, end=%s exclusive, "
        "expected last bar %s) ...",
        len(stale), fetch_period, window_start.isoformat(), window_end_excl.isoformat(),
        expected_last.isoformat(),
    )

    refreshed = 0
    failed_tickers: list[str] = []
    written: list[tuple[str, int]] = []
    _retry_counts: "dict[str, int]" = short_fetch_retries if short_fetch_retries is not None else {}
    _retry_budget_remaining = _SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN
    _reasons: "dict[str, str]" = failure_reasons if failure_reasons is not None else {}

    def _fail(ticker: str, reason: str) -> None:
        failed_tickers.append(ticker)
        _reasons[ticker] = reason

    with tempfile.TemporaryDirectory() as tmpdir:
        local_dir = Path(tmpdir)

        for batch_start in range(0, len(stale), batch_size):
            batch = stale[batch_start: batch_start + batch_size]
            yf_symbols = [f"^{t}" if t in _CARET_SYMBOLS else t for t in batch]

            if batch_start > 0:
                time.sleep(2)  # rate limit between batches

            try:
                tickers_arg = yf_symbols[0] if len(yf_symbols) == 1 else yf_symbols
                raw = yf.download(
                    tickers=tickers_arg,
                    start=window_start.isoformat(),
                    end=window_end_excl.isoformat(),
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
                is_multi = isinstance(raw.columns, pd.MultiIndex)
            except Exception as e:
                logger.warning("yfinance batch failed for %s...: %s", batch[:3], e)
                for ticker in batch:
                    _fail(ticker, FAIL_BATCH_ERROR)
                continue

            for ticker in batch:
                yf_sym = f"^{ticker}" if ticker in _CARET_SYMBOLS else ticker
                try:
                    new_df = (raw[yf_sym] if is_multi else raw).copy()
                    if "Close" not in new_df.columns or new_df.empty:
                        _fail(ticker, FAIL_NO_DATA)
                        continue
                    new_df = new_df.dropna(subset=["Close"])
                    if new_df.empty:
                        _fail(ticker, FAIL_NO_DATA)
                        continue

                    # Normalize index
                    idx = pd.to_datetime(new_df.index)
                    if idx.tz is not None:
                        idx = idx.tz_convert("UTC").tz_localize(None)
                    new_df.index = idx
                    new_df = clip_to_trading_day(
                        new_df.sort_index(), trading_day,
                        label=f"price_cache_refresh[{ticker}]",
                    )

                    # ── FRED longest-of selection for caret index tickers ───
                    # (alpha-engine-config-I9286). yfinance answers ``^VIX3M``
                    # with 1 row of history from the EC2 host that runs the
                    # weekly collector on SOME Saturdays and a full answer on
                    # others (measured 2026-08-29) — the intermittency behind
                    # alpha-engine-config-I9324's dead-then-healthy champion
                    # vintages. FRED already serves these four reliably (it's
                    # what daily_closes.py's single-latest fallback uses), but
                    # a hard cutover would make a FRED outage total instead of
                    # degrading to yfinance — so take whichever answers longer,
                    # every run, and log which source won.
                    if ticker in _CARET_SYMBOLS:
                        fred_df = _fred_ohlcv_for_caret_symbol(
                            ticker, fetch_period, trading_day=trading_day,
                        )
                        if fred_df is not None:
                            source, new_df = _longest_of(
                                [("yfinance", new_df), ("fred", fred_df)]
                            )
                            logger.info(
                                "%s: source selection -> %s (%d rows)",
                                ticker, source, len(new_df),
                            )
                        else:
                            logger.info(
                                "%s: FRED unavailable this run, using yfinance (%d rows)",
                                ticker, len(new_df),
                            )

                    # ── Short-fetch guard (alpha-engine-config-I9256) ───────
                    # yfinance intermittently answers a full-period request with
                    # a handful of rows (measured 2026-08-29: a 1-row
                    # ``reference/price_cache/VIX3M.parquet`` written at
                    # 02:44:23 UTC in the same batch that wrote a full 2515-row
                    # VIX.parquet). Uploading that wholesale destroys the 10y
                    # cache, and the Saturday backfill then rewrites ArcticDB
                    # ``macro/VIX3M`` from it — 2509 rows -> 16, undetected for
                    # two weeks. A refresh that would SHRINK the cache is a
                    # failed refresh, not a new truth: skip the upload, count the
                    # ticker as failed so ``status`` degrades to "partial", and
                    # leave the good parquet in place.
                    if len(new_df) < _SHORT_FETCH_ROW_THRESHOLD:
                        existing_rows = _existing_parquet_rows(
                            s3, bucket, s3_prefix, ticker
                        )
                        if existing_rows is not None and len(new_df) < existing_rows:
                            original_len = len(new_df)
                            attempts_made = 0
                            # alpha-engine-config-I11287: absorb a transient
                            # short answer here, bounded on BOTH axes — a
                            # fixed attempt count per ticker AND a run-level
                            # cap on how many DIFFERENT tickers may retry at
                            # all, so a systemic vendor issue cannot turn
                            # into an unbounded loop across ~900 tickers.
                            if _retry_budget_remaining > 0:
                                _retry_budget_remaining -= 1
                                retried_df, attempts_made = _retry_short_fetch_ticker(
                                    ticker, yf_sym, window_start, window_end_excl, trading_day,
                                )
                                _retry_counts[ticker] = attempts_made
                                if retried_df is not None and len(retried_df) >= existing_rows:
                                    logger.info(
                                        "Short-fetch guard: %s recovered after %d retr%s "
                                        "(%d rows, was %d) — uploading.",
                                        ticker, attempts_made,
                                        "y" if attempts_made == 1 else "ies",
                                        len(retried_df), original_len,
                                    )
                                    new_df = retried_df
                                else:
                                    recovered_len = len(retried_df) if retried_df is not None else original_len
                                    logger.error(
                                        "Short-fetch REFUSED for %s after %d retr%s: "
                                        "yfinance returned %d rows (best of %d/%d attempts) "
                                        "for period=%s but the existing price-cache parquet "
                                        "has %d — not uploading (existing history "
                                        "preserved). See alpha-engine-config-I9256, "
                                        "alpha-engine-config-I11287.",
                                        ticker, attempts_made,
                                        "y" if attempts_made == 1 else "ies",
                                        recovered_len, attempts_made, _SHORT_FETCH_RETRY_ATTEMPTS,
                                        fetch_period, existing_rows,
                                    )
                                    _fail(ticker, FAIL_SHORT_FETCH)
                                    continue
                            else:
                                _retry_counts[ticker] = 0
                                logger.error(
                                    "Short-fetch REFUSED for %s: yfinance returned %d rows "
                                    "for period=%s but the existing price-cache parquet has "
                                    "%d — not uploading (existing history preserved). "
                                    "Retry budget for this run (%d tickers) already "
                                    "exhausted. See alpha-engine-config-I9256, "
                                    "alpha-engine-config-I11287.",
                                    ticker, original_len, fetch_period, existing_rows,
                                    _SHORT_FETCH_RETRY_MAX_TICKERS_PER_RUN,
                                )
                                _fail(ticker, FAIL_SHORT_FETCH)
                                continue

                    # ── Behind-fetch guard (alpha-engine-config-I11467) ─────
                    # A full-length answer can still be missing the run's last
                    # session. Measured on the 2026-09-23 rehearsal: launched
                    # 00:00 UTC (20:00 ET on 2026-09-22) for trading_day
                    # 2026-09-22, yfinance answered ``end=2026-09-23`` with
                    # series ending 2026-09-21 for every ticker, and the
                    # refresh overwrote a cache that already held the
                    # 2026-09-22 bar (AAPL: version written 2026-09-22T20:09Z
                    # ends 09-22; the 00:10Z and 01:44Z rewrites end 09-21).
                    # The short-fetch guard above cannot see this — 2,512 rows
                    # is not short. A refresh that moves the cache's last bar
                    # BACKWARDS is a failed refresh, not a new truth: keep the
                    # existing parquet and count the ticker as failed, exactly
                    # like a shrinking refresh. The existing parquet is only
                    # read when the fetch ends before the run's expected
                    # session, so a current answer pays no extra S3 GET.
                    fetched_last = _last_bar_date(new_df.index)
                    if fetched_last is not None and fetched_last < expected_last:
                        cached_last = _existing_parquet_last_bar(
                            s3, bucket, s3_prefix, ticker,
                        )
                        if cached_last is not None and fetched_last < cached_last:
                            logger.error(
                                "Behind-fetch REFUSED for %s: the fetched series ends "
                                "%s but the existing price-cache parquet already ends "
                                "%s (expected last bar for trading_day %s is %s) — not "
                                "uploading (existing history preserved). See "
                                "alpha-engine-config-I11467.",
                                ticker, fetched_last.isoformat(), cached_last.isoformat(),
                                str(trading_day), expected_last.isoformat(),
                            )
                            _fail(ticker, FAIL_BEHIND_FETCH)
                            continue
                        logger.warning(
                            "%s: fetched series ends %s, before the expected last bar "
                            "%s for trading_day %s (cached parquet ends %s) — "
                            "uploading, since it does not move the cache backwards.",
                            ticker, fetched_last.isoformat(), expected_last.isoformat(),
                            str(trading_day),
                            cached_last.isoformat() if cached_last else "absent",
                        )

                    # ── Interior-session hole fill (alpha-engine-config-I11553) ──
                    # Neither guard above reads the middle of the series; a
                    # 2026-09-22 bar dropped by the vendor for 837 tickers
                    # reached D21's close_history this way.
                    new_df = hole_filler.fill(ticker, new_df)

                    # Write locally and upload (Wave 3 PR1: write-both to legacy
                    # ``predictor/price_cache/`` + new ``reference/price_cache/``;
                    # see builders/_price_cache_writeboth.py for soak contract)
                    try:
                        assert_valid_price_cache_ticker(ticker)
                    except ValueError as _caret_exc:
                        # I10904: the guard's own ValueError was being caught
                        # by the broad `except Exception` below and folded
                        # into an ordinary per-ticker miss. Re-raise as the
                        # dedicated type so the `except CaretTickerError:
                        # raise` clause ahead of that handler catches only
                        # this failure — an unrelated ValueError elsewhere in
                        # this block still degrades to a per-ticker failure.
                        raise CaretTickerError(str(_caret_exc)) from _caret_exc
                    assert_no_bar_after(
                        new_df.index, trading_day,
                        artifact=f"{s3_prefix}{ticker}.parquet",
                    )
                    parquet_path = local_dir / f"{ticker}.parquet"
                    new_df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")
                    for prefix in price_cache_write_prefixes(s3_prefix):
                        s3.upload_file(str(parquet_path), bucket, f"{prefix}{ticker}.parquet")
                    refreshed += 1
                    written.append((ticker, len(new_df)))

                except FutureBarError:
                    raise  # run-level contract violation, never a per-ticker miss
                except CaretTickerError:
                    raise  # run-level contract violation, never a per-ticker miss (I10904)
                except ShadowGuardViolation:
                    raise  # always fatal by its own contract, never a per-ticker miss (I11547)
                except Exception as e:
                    logger.warning("Refresh failed for %s: %s", ticker, e)
                    _fail(ticker, FAIL_REFRESH_ERROR)

            pct = 100 * min(batch_start + batch_size, len(stale)) / len(stale)
            logger.info(
                "Batch %d/%d — %d refreshed so far (%.0f%%)",
                batch_start // batch_size + 1,
                -(-len(stale) // batch_size),
                refreshed, pct,
            )

    logger.info("Price cache refresh complete: %d / %d tickers updated", refreshed, len(stale))

    # Single aggregated record per run — the named recording surface that
    # replaces yfinance's suppressed per-symbol ERROR spray. error_on_empty:
    # the 10y price cache is load-bearing (GBM training reads it), so a total
    # miss escalates to one loud ERROR (provider outage); a partial miss is one
    # WARN naming the unpriceable tickers (transient/rate-limit this run, or
    # persistent delisting/rename candidates for universe pruning).
    covered = set(stale) - set(failed_tickers)
    log_yf_coverage(
        logger, "price_cache_refresh", stale, covered, error_on_empty=True,
        note="stale tickers with no yfinance data this run — transient/rate-limit "
             "misses retry next refresh; persistent misses are delisting/rename "
             "candidates for universe pruning",
    )
    hole_filler.report(logger)
    return refreshed, failed_tickers, written
