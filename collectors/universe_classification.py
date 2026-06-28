"""
universe_classification.py — Sector / country-of-domicile / industry per ticker
for the full S&P 500 + S&P 400 universe, written to S3.

The system already gets GICS *sector* for the ~900-name universe from the
Wikipedia constituents pass (``collectors/constituents.py``), and Metron gets
sector+country for its HELD universe from ``collectors/metron_market_data.py``
(``market_data/sectors/latest.json``, v2). What was missing was a
**country-of-domicile + industry classification for the whole research
universe** — the dimension the new ~900-stock universe scoreboard
(crucible-research ``scoring/universe_board.py`` →
``scanner/universe/{date}/universe.json``) filters on alongside sector and the
factor/valuation metrics. This producer fills that gap from the same
``yfinance Ticker.info`` source Metron's classification pass already uses
(``info['sector']`` + ``info['country']`` + ``info['industry']``) — a
zero-new-dependency reuse of a proven production path (FMP's ``/stable``
profile endpoint caps at 250 calls/day and can't cover 900, so yfinance is
both the cheaper and the already-trusted source for this field).

Country/industry domicile is near-static (it changes only on a redomicile or
reclassification), so the weekly Saturday cadence is ample and the artifact is
a single ``latest.json`` (plus a dated copy for provenance).

Output:
  ``s3://<bucket>/market_data/universe_classification/{run_date}.json``
  ``s3://<bucket>/market_data/universe_classification/latest.json``

Schema (versioned — consumers pin on ``schema_version``):
  {
    "schema_version": 1,
    "as_of": "YYYY-MM-DD",
    "source": "yfinance",
    "ticker_count": int,        # tickers requested
    "ok_count": int,            # tickers with at least one populated field
    "data": {
      "<TICKER>": {
        "sector":   str | null,   # GICS sector (info['sector'])
        "country":  str | null,   # country of domicile (info['country'])
        "industry": str | null,   # GICS sub-industry (info['industry'])
      },
      ...
    }
  }

Failure semantics mirror ``collectors/short_interest.py``: per-ticker errors
are logged and recorded as an all-null row (a coverage gap, never a guessed
value); the collector returns ``status="ok"`` as long as at least
``_MIN_OK_RATIO`` of tickers produce some populated field, else
``status="error"`` (no partial write) so ``hard_fail_until_stable`` in
``weekly_collector.main`` aborts the run rather than publishing a thin
artifact.

Entry point: ``python -m collectors.universe_classification [--date YYYY-MM-DD] [--dry-run]``
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
UNIVERSE_CLASSIFICATION_PREFIX = "market_data/universe_classification/"
UNIVERSE_CLASSIFICATION_SCHEMA_VERSION = 1

# Per-ticker delay between yfinance ``Ticker.info`` calls. Same rationale +
# value as short_interest.py — empirically ~0.4s/call avoids HTTP 429 storms
# while keeping the full-universe pass under ~10 min on the Saturday spot.
_DEFAULT_DELAY_SECS = 0.4

# Minimum fraction of requested tickers that must produce *some* populated
# field for the run to count as OK. Below this we suspect a yfinance outage /
# IP block rather than genuine missing classification, and hard-fail instead
# of publishing a thin artifact (no-silent-fails).
_MIN_OK_RATIO = 0.50

# info keys → artifact field. Country/industry are the value-add over the
# Wikipedia sector pass; sector is carried too so a downstream consumer that
# wants one classification artifact need not also read constituents.json.
_INFO_FIELDS: dict[str, str] = {
    "sector": "sector",
    "country": "country",
    "industry": "industry",
}


def collect(
    bucket: str,
    tickers: list[str],
    s3_prefix: str = "market_data/",
    run_date: str | None = None,
    inter_request_delay: float = _DEFAULT_DELAY_SECS,
    dry_run: bool = False,
) -> dict:
    """
    Collect sector/country/industry classification for ``tickers`` → S3.

    Parameters
    ----------
    bucket : str
        S3 bucket for the JSON output.
    tickers : list[str]
        Symbols to classify. Typically the full S&P 500 + S&P 400 universe
        produced by the constituents collector earlier in the same run.
    s3_prefix : str
        Market-data S3 prefix (typically ``"market_data/"``). The artifact
        lands at ``{s3_prefix}universe_classification/{run_date}.json`` plus a
        ``latest.json`` sidecar.
    run_date : str
        YYYY-MM-DD stamp; defaults to today (trading-day axis).
    inter_request_delay : float
        Seconds to sleep between per-ticker yfinance calls.
    dry_run : bool
        If True, classify ~5 tickers and skip the S3 write.

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
            "error": "no tickers provided — universe classification needs the constituents list",
        }

    # In dry-run, sample the first few tickers to validate yfinance is
    # reachable without paying the full ~10 min collection cost.
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
        row = {field: None for field in _INFO_FIELDS.values()}
        try:
            info = yf.Ticker(ticker).info or {}
            for info_key, field in _INFO_FIELDS.items():
                v = info.get(info_key)
                if v:
                    row[field] = str(v)
            if any(v is not None for v in row.values()):
                ok_count += 1
        except Exception as exc:
            err_count += 1
            logger.debug("classification fetch failed for %s: %s", ticker, exc)

        payload[ticker] = row

        if (i + 1) % 100 == 0:
            elapsed = time.time() - started
            logger.info(
                "universe classification progress: %d/%d (%d ok, %d err) in %.0fs",
                i + 1, len(fetch_list), ok_count, err_count, elapsed,
            )

    elapsed = time.time() - started
    ok_ratio = ok_count / max(len(fetch_list), 1)
    logger.info(
        "universe classification done: %d/%d ok (%.1f%%), %d errors, %.0fs",
        ok_count, len(fetch_list), ok_ratio * 100, err_count, elapsed,
    )

    artifact = {
        "schema_version": UNIVERSE_CLASSIFICATION_SCHEMA_VERSION,
        "as_of": run_date,
        "source": "yfinance",
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
                f"classification data — below {_MIN_OK_RATIO:.0%} threshold. yfinance "
                f"outage or IP block suspected; not writing partial output."
            ),
            "ticker_count": len(fetch_list),
            "ok_count": ok_count,
        }

    s3 = boto3.client("s3")
    body = json.dumps(artifact, indent=2, default=str)
    dated_key = f"{s3_prefix}universe_classification/{run_date}.json"
    latest_key = f"{s3_prefix}universe_classification/latest.json"
    _put(s3, bucket, dated_key, body)
    _put(s3, bucket, latest_key, body)
    logger.info(
        "Wrote universe_classification to s3://%s/{%s,%s} (%d tickers, %d populated)",
        bucket, dated_key, latest_key, len(fetch_list), ok_count,
    )

    return {
        "status": "ok",
        "ticker_count": len(fetch_list),
        "ok_count": ok_count,
        "duration_seconds": round(elapsed, 1),
    }


def _put(s3_client, bucket: str, key: str, body: str) -> None:
    """The ONE put_object site in this module — dated + latest both route here
    so the artifact-registry coverage guard pins a single count."""
    s3_client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.universe_classification", description=__doc__,
    )
    parser.add_argument("--date", default=None, help="YYYY-MM-DD run date (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Sample 5 tickers, skip S3 write")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="S3 bucket")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from collectors import constituents

    existing = constituents.load_from_s3(args.bucket, "market_data/") or {}
    tickers = existing.get("tickers", [])
    if not tickers:
        logger.error("No constituents available in S3 — run the constituents collector first")
        return 1

    result = collect(
        bucket=args.bucket,
        tickers=tickers,
        run_date=args.date,
        dry_run=args.dry_run,
    )
    logger.info("universe_classification result: %s", result)
    return 0 if result.get("status", "").startswith("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
