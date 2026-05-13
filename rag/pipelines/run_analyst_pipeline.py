"""Gate A — analyst pipeline orchestrator CLI.

Runs the full Wave 1 analyst producer chain on Saturday SF:

  1. Snapshot consensus + price targets per ticker via
     ``data.snapshotter.analyst_daily.snapshot_universe`` — writes
     one JSON per (ticker, snapshot_date) to S3.
  2. Compute self-derived 7d/30d revisions deltas from the
     accumulated time series via
     ``data.derived.analyst_revisions.compute_and_write_revisions`` —
     writes one parquet per as-of-date to S3.

Cadence: weekly on Saturday SF. Revisions become meaningful after
~4 weekly snapshots accumulate (Gate B in the ROADMAP).

Usage::

    python -m rag.pipelines.run_analyst_pipeline --from-signals

    # Ad-hoc replay
    python -m rag.pipelines.run_analyst_pipeline --tickers AAPL,MSFT \\
        --snapshot-date 2026-05-17

    # Skip revisions (snapshot-only — first run before time series exists)
    python -m rag.pipelines.run_analyst_pipeline --from-signals --skip-revisions
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--tickers", type=str,
        help="Comma-separated ticker list.",
    )
    grp.add_argument(
        "--from-signals", action="store_true",
        help="Load tickers from latest signals.json on S3.",
    )
    parser.add_argument(
        "--snapshot-date", type=str, default=None,
        help="Date stamp for the snapshot (default: today UTC).",
    )
    parser.add_argument(
        "--bucket", type=str, default="alpha-engine-research",
    )
    parser.add_argument(
        "--skip-revisions", action="store_true",
        help="Snapshot only; skip the revisions computation step. "
             "Use when the time series hasn't accumulated yet.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch but don't write to S3.",
    )
    args = parser.parse_args()

    # Resolve tickers
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        from rag.pipelines._signals_universe import load_signals_tickers
        tickers = load_signals_tickers(bucket=args.bucket)
    if not tickers:
        logger.error("[run_analyst_pipeline] no tickers — aborting")
        return 1
    logger.info("[run_analyst_pipeline] running for %d tickers", len(tickers))

    if args.snapshot_date:
        snap_date = date.fromisoformat(args.snapshot_date)
    else:
        snap_date = datetime.now(timezone.utc).date()

    import boto3
    s3 = boto3.client("s3")

    # ── Step 1: snapshot ─────────────────────────────────────────
    logger.info(
        "[run_analyst_pipeline] step 1/2 — snapshot consensus + price targets",
    )
    from collectors.analyst_sources.finnhub import FinnhubAnalystAdapter
    from collectors.analyst_sources.yfinance import YfinanceAnalystAdapter
    from data.snapshotter.analyst_daily import snapshot_universe

    sources = [
        YfinanceAnalystAdapter(),
        FinnhubAnalystAdapter(),
    ]
    snap_stats = snapshot_universe(
        tickers, sources,
        snapshot_date=snap_date, s3_client=s3, bucket=args.bucket,
        dry_run=args.dry_run,
    )
    logger.info("[run_analyst_pipeline] step 1 — %s", snap_stats)

    # ── Step 2: revisions ────────────────────────────────────────
    if args.skip_revisions or args.dry_run:
        logger.info(
            "[run_analyst_pipeline] step 2/2 — SKIPPED "
            "(--skip-revisions or --dry-run)",
        )
    else:
        logger.info("[run_analyst_pipeline] step 2/2 — compute revisions")
        from data.derived.analyst_revisions import compute_and_write_revisions
        key, rows = compute_and_write_revisions(
            tickers, as_of_date=snap_date,
            s3_client=s3, bucket=args.bucket,
        )
        n_signal = sum(
            1 for r in rows if r.mean_target_delta_30d is not None
        )
        logger.info(
            "[run_analyst_pipeline] step 2 — wrote %d rows to s3://%s/%s "
            "(%d with non-null 30d delta)",
            len(rows), args.bucket, key, n_signal,
        )

    logger.info("[run_analyst_pipeline] complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
