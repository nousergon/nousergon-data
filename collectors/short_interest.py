"""
short_interest.py — Collect short interest metrics from yfinance and write to S3.

FINRA reports short interest bi-monthly (15th + end-of-month), so a Saturday
weekly cadence captures every refresh with one cycle of buffer. yfinance
``Ticker.info`` exposes the three fields research's scoring layer cares about:
``shortPercentOfFloat`` (the squeeze indicator), ``shortRatio`` (days-to-cover),
and ``sharesShort`` (raw shares short).

This collector replaces the orphaned ``fetch_short_interest`` in
``alpha-engine-research/data/fetchers/price_fetcher.py``. Per the
"research is a pure consumer of alpha-engine-data" principle established
in Phase 7c, research will read from ``market_data/<date>/short_interest.json``
instead of calling yfinance inline. The integration on the research side
ships in a follow-up PR (see ROADMAP).

Output: ``s3://<bucket>/market_data/weekly/<run_date>/short_interest.json``

Schema:
  {
    "date": "YYYY-MM-DD",
    "fetched_at": "ISO-8601 UTC",
    "ticker_count": int,
    "ok_count": int,        # tickers with at least one populated field
    "data": {
      "<TICKER>": {
        "short_pct_float": float | null,   # percent (5.0 = 5%)
        "short_ratio":     float | null,   # days to cover
        "shares_short":    int | null,     # raw shares short
      },
      ...
    }
  }

Failure semantics: per-ticker errors are logged and recorded as a row with
all-null fields. The collector returns ``status="ok"`` as long as at least
``_MIN_OK_RATIO`` of tickers produce some populated data (default 50%) —
yfinance's ``Ticker.info`` is rate-limit-sensitive and partial coverage is
expected. Below that threshold, returns ``status="error"`` so the
``hard_fail_until_stable`` rule in ``weekly_collector.main`` aborts the run.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


# Per-ticker delay between yfinance ``Ticker.info`` calls. yfinance has no
# documented rate limit; empirically 0.3-0.5s/call avoids HTTP 429 storms
# while keeping the full-universe collection under ~10 min on the Saturday
# spot. Adjustable via ``inter_request_delay`` arg.
_DEFAULT_DELAY_SECS = 0.4

# Minimum fraction of requested tickers that must produce *some* populated
# field for the run to be considered OK. Below this we suspect a yfinance
# outage / IP block rather than the bi-monthly FINRA refresh just being
# old, and hard-fail.
_MIN_OK_RATIO = 0.50


def collect(
    bucket: str,
    tickers: list[str],
    s3_prefix: str = "market_data/",
    run_date: str | None = None,
    inter_request_delay: float = _DEFAULT_DELAY_SECS,
    dry_run: bool = False,
) -> dict:
    """
    Collect short interest for ``tickers`` and write to S3.

    Parameters
    ----------
    bucket : str
        S3 bucket for the JSON output.
    tickers : list[str]
        Symbols to fetch. Typically the full S&P 500 + S&P 400 universe
        produced by the constituents collector earlier in the same run.
    s3_prefix : str
        Market-data S3 prefix (typically ``"market_data/"``).
    run_date : str
        YYYY-MM-DD stamp; defaults to today (UTC).
    inter_request_delay : float
        Seconds to sleep between per-ticker yfinance calls.
    dry_run : bool
        If True, fetch ~5 tickers and skip the S3 write.

    Returns
    -------
    dict
        ``{status, ticker_count, ok_count, ...}``. ``status="ok"`` if
        ok_count >= MIN_OK_RATIO * ticker_count, else ``"error"``.
    """
    if run_date is None:
        from dates import default_run_date  # config#1014: trading-day axis

        run_date = default_run_date()

    if not tickers:
        return {
            "status": "error",
            "error": "no tickers provided — short interest needs the constituents list",
        }

    # In dry-run, sample the first few tickers to validate yfinance is reachable
    # without paying the full ~10 min collection cost.
    fetch_list = tickers[:5] if dry_run else tickers

    try:
        import yfinance as yf
    except ImportError as exc:
        return {
            "status": "error",
            "error": f"yfinance not importable: {exc}",
        }

    payload: dict[str, dict] = {}
    ok_count = 0
    err_count = 0
    started = time.time()

    for i, ticker in enumerate(fetch_list):
        if i > 0 and inter_request_delay > 0:
            time.sleep(inter_request_delay)
        row = {
            "short_pct_float": None,
            "short_ratio": None,
            "shares_short": None,
        }
        try:
            info = yf.Ticker(ticker).info
            short_pct = info.get("shortPercentOfFloat")
            short_ratio = info.get("shortRatio")
            shares_short = info.get("sharesShort")

            # yfinance exposes shortPercentOfFloat as a 0-1 ratio; convert
            # to percent for the executor / scoring layer's threshold semantics.
            if short_pct is not None:
                row["short_pct_float"] = round(float(short_pct) * 100, 2)
            if short_ratio is not None:
                row["short_ratio"] = round(float(short_ratio), 2)
            if shares_short is not None:
                row["shares_short"] = int(shares_short)

            if any(v is not None for v in row.values()):
                ok_count += 1
        except Exception as exc:
            err_count += 1
            logger.debug("short interest fetch failed for %s: %s", ticker, exc)

        payload[ticker] = row

        if (i + 1) % 100 == 0:
            elapsed = time.time() - started
            logger.info(
                "short interest progress: %d/%d (%d ok, %d err) in %.0fs",
                i + 1, len(fetch_list), ok_count, err_count, elapsed,
            )

    elapsed = time.time() - started
    ok_ratio = ok_count / max(len(fetch_list), 1)
    logger.info(
        "short interest done: %d/%d ok (%.1f%%), %d errors, %.0fs",
        ok_count, len(fetch_list), ok_ratio * 100, err_count, elapsed,
    )

    result = {
        "date": run_date,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "ticker_count": len(fetch_list),
        "ok_count": ok_count,
        "data": payload,
    }

    if dry_run:
        return {
            "status": "ok_dry_run",
            "ticker_count": len(fetch_list),
            "ok_count": ok_count,
            "duration_seconds": round(elapsed, 1),
        }

    if ok_ratio < _MIN_OK_RATIO:
        return {
            "status": "error",
            "error": (
                f"only {ok_count}/{len(fetch_list)} tickers ({ok_ratio:.1%}) had populated "
                f"short-interest data — below {_MIN_OK_RATIO:.0%} threshold. yfinance "
                f"outage or IP block suspected; not writing partial output."
            ),
            "ticker_count": len(fetch_list),
            "ok_count": ok_count,
        }

    s3 = boto3.client("s3")
    key = f"{s3_prefix}weekly/{run_date}/short_interest.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(result, indent=2, default=str),
        ContentType="application/json",
    )
    logger.info(
        "Wrote short_interest.json to s3://%s/%s (%d tickers, %d populated)",
        bucket, key, len(fetch_list), ok_count,
    )

    return {
        "status": "ok",
        "ticker_count": len(fetch_list),
        "ok_count": ok_count,
        "duration_seconds": round(elapsed, 1),
    }
