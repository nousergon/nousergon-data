"""
prices.py — Refresh stale price cache parquets and upload to S3.

Two-phase staleness check:
  1. Fast: polygon grouped-daily (1 API call) gets latest close for all US stocks.
     Compare against S3 parquet last-modified dates to find stale tickers.
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
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import yfinance as yf

from builders._price_cache_writeboth import (
    price_cache_read_prefixes,
    price_cache_write_prefixes,
)
from nousergon_lib.yfinance_quiet import log_yf_coverage, yf_quiet

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
    # Instead of downloading all parquets, just list them and check last-modified
    stale = _find_stale_fast(
        s3, bucket, s3_prefix, all_tickers, staleness_threshold_days, reference_date,
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
    refreshed, failed_tickers = _refresh_stale(
        s3, bucket, s3_prefix, stale, fetch_period, batch_size,
    )

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
        "total": len(all_tickers),
    }
    if validation:
        result["validation"] = validation
    return result


def _find_stale_fast(
    s3,
    bucket: str,
    prefix: str,
    all_tickers: list[str],
    staleness_threshold_days: int,
    reference_date: str | date | None = None,
) -> list[str]:
    """
    Fast staleness check using S3 object metadata (no downloads).

    Lists all parquets in the cache, checks LastModified timestamp against
    ``reference_date`` on the NYSE trading-day axis (nousergon_lib.dates.
    is_fresh_in_trading_days) — holiday/weekend-aware, so the same
    ``staleness_threshold_days`` value means "N trading sessions behind"
    whether this runs weekly or daily. Any ticker with no parquet, or a
    parquet more than ``staleness_threshold_days`` sessions stale, is stale.
    """
    from nousergon_lib.dates import is_fresh_in_trading_days

    reference = reference_date if reference_date is not None else datetime.now(timezone.utc).date()

    # Build map of ticker -> last modified from S3 listing
    existing: dict[str, datetime] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            ticker = key.split("/")[-1].replace(".parquet", "")
            existing[ticker] = obj["LastModified"]

    logger.info("S3 cache: %d parquets found", len(existing))

    stale: list[str] = []
    for ticker in all_tickers:
        last_mod = existing.get(ticker)
        if last_mod is None:
            stale.append(ticker)
        elif not is_fresh_in_trading_days(
            last_mod.date(), reference, max_stale=staleness_threshold_days,
        ):
            stale.append(ticker)

    return stale


# Row count below which a "full period" yfinance refresh is treated as
# SUSPECT and checked against what the price cache already holds.
# ~400 trading days is ~18 months — far under the 10y (~2500 row) fetch every
# maintained ticker returns, and comfortably over a genuinely new listing that
# has no existing parquet to regress. Only frames under this threshold pay the
# extra S3 GET, so the guard is free on the ~900-ticker happy path.
_SHORT_FETCH_ROW_THRESHOLD = 400


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


def _existing_parquet_rows(s3, bucket: str, s3_prefix: str, ticker: str) -> int | None:
    """Row count of the ticker's current price-cache parquet, or None if absent.

    Any read failure other than "the object does not exist" RAISES — treating an
    unreadable parquet as absent would let a short fetch overwrite a full history
    on a transient S3 error, which is the same silent-degrade this guard exists
    to stop.
    """
    import io as _io

    last_exc: Exception | None = None
    for prefix in price_cache_read_prefixes(s3_prefix):
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"{prefix}{ticker}.parquet")
        except Exception as exc:  # noqa: BLE001 - classified below, never swallowed
            if _is_missing_object(s3, exc):
                continue
            last_exc = exc
            continue
        return len(pd.read_parquet(_io.BytesIO(obj["Body"].read())))
    if last_exc is not None:
        raise RuntimeError(
            f"Short-fetch guard: could not read the existing price-cache parquet "
            f"for {ticker} to check for truncation: {last_exc}"
        ) from last_exc
    return None


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


def _fred_ohlcv_for_caret_symbol(ticker: str, period: str) -> "pd.DataFrame | None":
    """Fetch ``ticker``'s FRED-sourced history, reshaped to the yfinance OHLCV
    column set. Returns ``None`` (never raises) on any failure — the caller
    falls back to whatever yfinance answered, since FRED being unavailable
    must degrade to yfinance rather than to nothing (alpha-engine-config-I9286
    deliverable 2).
    """
    from collectors.fred_history import FRED_HISTORY_MAP, fetch_fred_history, fred_history_to_ohlcv

    series_id = FRED_HISTORY_MAP.get(ticker)
    if series_id is None:
        return None
    years = int(period.rstrip("y")) if period.endswith("y") and period[:-1].isdigit() else 10
    try:
        fred_df = fetch_fred_history(series_id, period_years=years)
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
) -> tuple[int, list[str]]:
    """Batch-fetch stale tickers from yfinance and upload to S3.

    Runs under ``yf_quiet`` (nousergon_lib.yfinance_quiet): yfinance's
    per-symbol "possibly delisted" ERROR spray is demoted so one transient/
    unpriceable ticker can't storm Flow Doctor with a report per worded
    variant (the 2026-06-19 PCAR recurrence of the config#1029 PCKM storm).
    The replacement recording surface is the aggregated ``log_yf_coverage``
    record emitted before returning.
    """
    import time

    logger.info("Refreshing %d stale tickers (period=%s) ...", len(stale), fetch_period)

    refreshed = 0
    failed_tickers: list[str] = []

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
                    period=fetch_period,
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
                is_multi = isinstance(raw.columns, pd.MultiIndex)
            except Exception as e:
                logger.warning("yfinance batch failed for %s...: %s", batch[:3], e)
                failed_tickers.extend(batch)
                continue

            for ticker in batch:
                yf_sym = f"^{ticker}" if ticker in _CARET_SYMBOLS else ticker
                try:
                    new_df = (raw[yf_sym] if is_multi else raw).copy()
                    if "Close" not in new_df.columns or new_df.empty:
                        failed_tickers.append(ticker)
                        continue
                    new_df = new_df.dropna(subset=["Close"])
                    if new_df.empty:
                        failed_tickers.append(ticker)
                        continue

                    # Normalize index
                    idx = pd.to_datetime(new_df.index)
                    if idx.tz is not None:
                        idx = idx.tz_convert("UTC").tz_localize(None)
                    new_df.index = idx
                    new_df = new_df.sort_index()

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
                        fred_df = _fred_ohlcv_for_caret_symbol(ticker, fetch_period)
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
                            logger.error(
                                "Short-fetch REFUSED for %s: yfinance returned %d rows "
                                "for period=%s but the existing price-cache parquet has "
                                "%d — not uploading (existing history preserved). "
                                "See alpha-engine-config-I9256.",
                                ticker, len(new_df), fetch_period, existing_rows,
                            )
                            failed_tickers.append(ticker)
                            continue

                    # Write locally and upload (Wave 3 PR1: write-both to legacy
                    # ``predictor/price_cache/`` + new ``reference/price_cache/``;
                    # see builders/_price_cache_writeboth.py for soak contract)
                    parquet_path = local_dir / f"{ticker}.parquet"
                    new_df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")
                    for prefix in price_cache_write_prefixes(s3_prefix):
                        s3.upload_file(str(parquet_path), bucket, f"{prefix}{ticker}.parquet")
                    refreshed += 1

                except Exception as e:
                    logger.warning("Refresh failed for %s: %s", ticker, e)
                    failed_tickers.append(ticker)

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
    return refreshed, failed_tickers
