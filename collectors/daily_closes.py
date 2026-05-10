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
import os
import time
from datetime import datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

_NYSE_TZ = ZoneInfo("America/New_York")

_YFINANCE_BATCH_SIZE = 100
_YFINANCE_BATCH_DELAY = 2  # seconds between batches

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_FRED_TIMEOUT = 15

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
}


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


def _previous_business_days(run_date: str, n: int) -> list[str]:
    """Return ``n`` business days ending on ``run_date`` (inclusive),
    newest first. ``n=1`` returns the most-recent business day at or
    before ``run_date``.

    Used by :func:`collect` in window-scan mode to enumerate the dates
    each pass will reconcile. Polygon's free-tier rate-limit is honored
    by the caller — one ``grouped-daily`` call per date in the returned
    list, total ``n`` polygon calls regardless of universe size.

    Saturday / Sunday ``run_date`` walks back to the prior Friday before
    starting the window, so a Sat SF firing at 02:00 PT doesn't burn a
    slot on a non-trading day. NYSE holiday handling lives downstream —
    holidays return zero rows from polygon and an empty yfinance batch,
    which the per-date skip logic handles gracefully.
    """
    if n < 1:
        raise ValueError(f"window n must be >= 1, got {n}")
    cur = datetime.strptime(run_date, "%Y-%m-%d").date()
    # Normalize the starting point to a business day.
    while cur.weekday() >= 5:  # Sat=5, Sun=6
        cur = cur - timedelta(days=1)
    dates: list[str] = [cur.isoformat()]
    for _ in range(n - 1):
        cur = cur - timedelta(days=1)
        while cur.weekday() >= 5:
            cur = cur - timedelta(days=1)
        dates.append(cur.isoformat())
    return dates


def collect(
    bucket: str,
    tickers: list[str],
    run_date: str | None = None,
    s3_prefix: str = "staging/daily_closes/",
    dry_run: bool = False,
    source: str = "auto",
    window_days: int = 1,
    skip_if_canonical: bool = False,
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
                parquet. Implements the source-precedence-ladder skip-set
                semantic from the windowed-data-reconciliation arc:

                  - ``yfinance_only`` mode: skips canonical tickers (any
                    source already populated), so yfinance only fetches
                    cells that are NaN. Coverage gate evaluates the
                    merged-output denominator (existing canonical rows
                    contribute as if freshly fetched).
                  - ``polygon_only`` mode: flag is *ignored*. Per
                    2026-05-10 design decision (option a) polygon always
                    re-overwrites within the window — this catches
                    corporate-action backfills that retroactively shift
                    polygon's adjusted close. The 14/day grouped-daily
                    contract still holds because polygon makes one call
                    per date in the window regardless of skip behavior.
                  - ``auto`` mode: flag applied to the yfinance step
                    only; polygon step always runs.

                Default False preserves legacy single-date overwrite
                semantics for non-window callers.

    Returns:
        Single-date mode (``window_days=1``): dict with ``status``,
        ``tickers_captured``, ``polygon``/``fred``/``yfinance`` counts,
        ``source``.

        Window mode (``window_days > 1``): dict with ``status``
        (``"ok"`` if every date succeeded, ``"partial"`` if any date
        errored), aggregated ``tickers_captured`` / ``polygon`` /
        ``fred`` / ``yfinance`` counters across the window, ``source``,
        ``window_days``, ``per_date`` (date → per-date result dict),
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

    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

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
    # When skip_if_canonical=True, ``canonical_existing_rows`` carries
    # the records dicts for tickers in the existing parquet that already
    # have an authoritative source (yfinance / polygon) and a non-null
    # Close. They get merged into the output records before write so
    # the parquet preserves prior canonical state across the window
    # scan. Empty when skip_if_canonical=False (legacy overwrite path).
    canonical_existing_rows: list[dict] = []
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
            # Read existing rows so we can log Close-discrepancy after the
            # polygon overwrite. Failures here are non-fatal — discrepancy
            # logging is observability, not a write blocker.
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                existing_df = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
                existing_close_for_discrepancy = {
                    str(t): float(existing_df.loc[t, "Close"])
                    for t in existing_df.index
                    if pd.notna(existing_df.loc[t, "Close"])
                }
                logger.info(
                    "polygon_only: found existing parquet (last_modified=%s, %d tickers) — "
                    "will overwrite and log Close discrepancies",
                    last_modified.isoformat(), len(existing_close_for_discrepancy),
                )
            except Exception as exc:
                logger.warning(
                    "polygon_only: failed to read existing parquet for discrepancy logging "
                    "(%s) — proceeding with overwrite without discrepancy comparison",
                    exc,
                )
        elif skip_if_canonical:
            # yfinance_only / auto + skip_if_canonical=True: read the
            # full parquet, extract canonical rows so they survive into
            # the merged output. Bypass the post-close-skip short-circuit
            # below — the whole point of windowed reconciliation is to
            # fill NaN cells in older dates that legacy logic would skip.
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
                            canonical_existing_rows.append(preserved)
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
                canonical_existing_rows = []
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
    polygon_count = 0
    if source != "yfinance_only":
        polygon_count = _fetch_polygon_closes(tickers, run_date, records, source=source)

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
        fred_count = _fetch_fred_closes(fred_missing, run_date, records)

    # ── Step 3: yfinance — only in auto + yfinance_only modes ────────────────
    # polygon_only refuses yfinance fallback per feedback_no_silent_fails: a
    # silent yfinance fill would hide polygon outages and re-introduce the
    # 2026-04-17 → 2026-04-23 VWAP=None contamination.
    captured_tickers = {r["ticker"] for r in records}
    missing = [t for t in tickers if t.lstrip("^") not in captured_tickers]
    # When skip_if_canonical=True (yfinance_only / auto window mode), drop
    # tickers that already have an authoritative source in the existing
    # parquet — those rows will be merged from ``canonical_existing_rows``
    # before write, so refetching them would just churn API budget.
    if canonical_skip_set:
        before = len(missing)
        missing = [t for t in missing if t.lstrip("^") not in canonical_skip_set]
        logger.info(
            "[skip_if_canonical] %s: yfinance fetch list %d → %d "
            "(skipped %d canonical)",
            run_date, before, len(missing), before - len(missing),
        )
    yfinance_count = 0
    if missing and source != "polygon_only":
        yfinance_count = _fetch_yfinance_closes(missing, run_date, records)

    # Merge preserved canonical rows from the existing parquet into the
    # records list. These are tickers we deliberately skipped fetching
    # — they survive into the output unchanged. Polygon-only mode has
    # ``canonical_existing_rows`` empty by construction (skip flag
    # ignored per option (a)), so the legacy overwrite path is preserved.
    if canonical_existing_rows:
        already_captured = {r["ticker"] for r in records}
        merged_in = 0
        for row in canonical_existing_rows:
            if row["ticker"] not in already_captured:
                records.append(row)
                merged_in += 1
        logger.info(
            "[skip_if_canonical] %s: merged %d preserved canonical rows "
            "into output records",
            run_date, merged_in,
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

    closes_df = pd.DataFrame(records).set_index("ticker")
    logger.info(
        "Daily closes: %d tickers for %s source=%s (polygon=%d, fred=%d, yfinance=%d)",
        len(closes_df), run_date, source, polygon_count, fred_count, yfinance_count,
    )

    # Discrepancy logging (polygon_only mode, when overwriting an existing parquet)
    if existing_close_for_discrepancy and polygon_count > 0:
        _log_close_discrepancies(closes_df, existing_close_for_discrepancy, run_date)

    if dry_run:
        return {
            "status": "ok_dry_run",
            "tickers_captured": len(closes_df),
            "polygon": polygon_count,
            "fred": fred_count,
            "yfinance": yfinance_count,
            "source": source,
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
    cost near zero across the window. Polygon side ignores the flag
    (option a, always overwrites).

    Returns an aggregate dict; see ``collect`` docstring's "Window mode"
    return-shape section for the schema.
    """
    window_dates = _previous_business_days(run_date, n=window_days)
    aggregate: dict = {
        "status": "ok",
        "source": source,
        "window_days": window_days,
        "per_date": {},
        "tickers_captured": 0,
        "polygon": 0,
        "fred": 0,
        "yfinance": 0,
        "skipped_dates": [],
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
            )
        except Exception as exc:
            # Per-date failures don't kill the rest of the window; record
            # the failure and continue. This matches the existing single-
            # date semantics where a coverage-gate / API failure on one
            # day doesn't block subsequent days when re-run.
            logger.warning(
                "[daily_closes window] date=%s source=%s failed: %s — "
                "recording and continuing window",
                d, source, exc,
            )
            aggregate["per_date"][d] = {
                "status": "error",
                "error": str(exc),
                "source": source,
            }
            aggregate["status"] = "partial"
            continue
        aggregate["per_date"][d] = result
        for k in ("tickers_captured", "polygon", "fred", "yfinance"):
            if k in result and isinstance(result[k], int):
                aggregate[k] += result[k]
        if result.get("status") == "error":
            aggregate["status"] = "partial"
        if result.get("skipped"):
            aggregate["skipped_dates"].append(d)
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
        logger.warning("Polygon grouped-daily failed in auto mode: %s — falling back", e)
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
        g = grouped.get(store_ticker)
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
    DataPhase1 budget.
    """
    from polygon_client import polygon_client

    recovered = 0
    for ticker in tickers:
        store_ticker = ticker.lstrip("^")
        try:
            bar = polygon_client().get_single_day_bar(store_ticker, run_date)
        except Exception as exc:
            logger.warning(
                "Polygon per-ticker fallback failed for %s @ %s: %s",
                store_ticker, run_date, exc,
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


def _log_close_discrepancies(
    new_df: pd.DataFrame,
    prior_close: dict[str, float],
    run_date: str,
) -> None:
    """Log per-ticker Close discrepancy when polygon overwrites yfinance.

    A small drift (<1%) is normal — different feeds, slight tick-time offsets,
    consolidated tape coverage variance. Larger drifts (>1% WARN, >5% ERROR)
    typically indicate corporate-action timing differences or one-source data
    quality issues worth a human eyeball.
    """
    n_compared = 0
    n_warn = 0
    n_error = 0
    biggest: tuple[str, float] = ("", 0.0)
    for ticker in new_df.index:
        prior = prior_close.get(str(ticker))
        new_close = new_df.loc[ticker, "Close"]
        if prior is None or pd.isna(new_close) or prior == 0:
            continue
        n_compared += 1
        pct_diff = abs(float(new_close) - prior) / prior
        if pct_diff > _DISCREPANCY_ERROR_PCT:
            logger.error(
                "polygon_only OVERWRITE %s @ %s: Close %.4f → %.4f (%.2f%% diff vs prior parquet) — "
                "investigate before downstream consumers re-read",
                ticker, run_date, prior, float(new_close), pct_diff * 100,
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
        "biggest=%s@%.2f%%",
        run_date, n_compared, n_warn, n_error, biggest[0] or "n/a", biggest[1] * 100,
    )


def _fetch_fred_closes(
    tickers: list[str],
    date_str: str,
    records: list[dict],
) -> int:
    """Fetch the latest close for index tickers from FRED.

    Serves the 4 index symbols not on polygon free tier (VIX, VIX3M, TNX, IRX).
    Takes the most recent non-missing observation for each series — typically
    T-1 when the daily pipeline runs at 6:05 AM PT.
    """
    api_key = os.environ.get("FRED_API_KEY", "")
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
                "sort_order": "desc",
                "limit": 5,
            }
            resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
            resp.raise_for_status()
            obs = resp.json().get("observations", [])
            latest = next((o for o in obs if o.get("value", ".") != "."), None)
            if latest is None:
                logger.warning("FRED %s → %s: no non-missing observation", store_ticker, series_id)
                continue
            close = float(latest["value"])
            records.append({
                "ticker": store_ticker,
                "date": date_str,
                "Open": round(close, 4),
                "High": round(close, 4),
                "Low": round(close, 4),
                "Close": round(close, 4),
                "Adj_Close": round(close, 4),
                "Volume": 0,
                # VWAP only meaningful from polygon grouped-daily (volume-weighted
                # across trades). FRED single-value closes give us no distribution
                # to VWAP, so None rather than passing Close off as VWAP.
                "VWAP": None,
                "source": "fred",
            })
            count += 1
        except Exception as e:
            logger.warning("FRED fetch failed for %s (%s): %s", store_ticker, series_id, e)

    logger.info("FRED fallback: %d/%d index tickers captured", count, len(tickers))
    return count


def _fetch_yfinance_closes(
    tickers: list[str],
    date_str: str,
    records: list[dict],
) -> int:
    """Fetch closes from yfinance for tickers not covered by polygon."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available for daily closes fallback")
        return 0

    count = 0
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
                except Exception as e:
                    logger.warning("yfinance close extract failed for %s: %s", ticker, e)
        except Exception as e:
            logger.warning("yfinance batch failed: %s", e)

    logger.info("yfinance fallback: %d/%d tickers captured", count, len(tickers))
    return count
