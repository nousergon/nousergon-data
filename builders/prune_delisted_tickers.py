"""builders/prune_delisted_tickers.py — orchestrated delisted-ticker cleanup.

Removes ArcticDB universe symbols that meet BOTH conditions:

  (A) Absent from the latest ``market_data/weekly/{date}/constituents.json``
      ``tickers`` list (S&P 500 + 400, ~903 names). Wikipedia is the
      authoritative source there; if a ticker has been removed, the
      weekly constituents fetch reflects it.

  (B) ArcticDB ``last_date`` for the symbol is older than ``--absent-days``
      (default 14 = 2 weeks). Confirms the symbol is genuinely
      stale on the data side too — daily_closes upstream stops emitting
      a ticker shortly after delisting.

Both conditions together prevent flapping:
  - Constituents-only check would over-prune on a Wikipedia parsing
    hiccup (legitimate ticker temporarily missing from the JSON).
  - last_date-only check would over-prune on a multi-week daily_closes
    outage (e.g. polygon free-tier 403 streak from 2026-04-23).

Composes with ``daily_append`` PR #101's missing-from-closes hard-fail:
the named ticker list in that error message is the operator's
investigation surface; this builder is the auto-triage tool that closes
the loop on legitimate delistings (so the threshold doesn't have to keep
getting bumped or the symbol manually deleted).

Usage:
    python -m builders.prune_delisted_tickers                   # dry-run
    python -m builders.prune_delisted_tickers --apply           # actually prune
    python -m builders.prune_delisted_tickers --absent-days 7   # tighter window
    python -m builders.prune_delisted_tickers --apply --tickers HOLX,RACE  # one-off
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

from builders._constituents_loader import load_constituents_for_run_date
from features.compute import DEFAULT_BUCKET, _SKIP_TICKERS, _is_sector_etf
from store.arctic_store import get_universe_lib

log = logging.getLogger(__name__)

DEFAULT_ABSENT_DAYS = 14
AUDIT_PREFIX = "builders/prune_audit/"


def _load_latest_constituents(
    s3, bucket: str, run_date: str | None = None,
) -> tuple[set[str], str]:
    """Return ``(tickers_set, weekly_date_str)`` from the current
    constituents.json.

    Thin wrapper around
    :func:`builders._constituents_loader.load_constituents_for_run_date`.
    When ``run_date`` is provided (Phase-1 happy path), reads directly
    from ``market_data/weekly/{run_date}/constituents.json``; otherwise
    falls back to the ``latest_weekly.json`` pointer.

    Lifted 2026-05-24 (ROADMAP L1397). The pre-lift implementation
    always followed the pointer — which, during Phase-1, points at the
    PRIOR week's partition (the constituents collector writes the new
    weekly file first; ``_write_manifest`` advances the pointer at
    end-of-Phase-1). Calling prune mid-Phase-1 with the stale pointer
    pruned tickers REMOVED last week instead of this week (BK/FLO/PSTG
    for the 5/23 cycle would have stayed in arctic for one extra week
    before the next prune run cleared them). Same defect class as the
    L1316 backfill TOCTOU.
    """
    return load_constituents_for_run_date(s3, bucket, run_date=run_date)


def _read_last_date(universe_lib, ticker: str) -> pd.Timestamp | None:
    """Return the most recent index date for a symbol, or None if unreadable.

    Uses ``tail(1)`` to avoid pulling the full series — every read is a
    separate S3 round-trip and we may scan dozens of stale candidates.
    """
    try:
        df = universe_lib.tail(ticker, 1).data
    except Exception as exc:
        log.warning(
            "Could not read tail(1) for %s — assuming readable but treating "
            "as not-stale-enough-to-prune (refuse to delete data we can't "
            "verify): %s",
            ticker, exc,
        )
        return None
    if df.empty:
        return None
    return pd.Timestamp(df.index[-1]).normalize()


def prune_delisted_tickers(
    *,
    bucket: str = DEFAULT_BUCKET,
    absent_days: int = DEFAULT_ABSENT_DAYS,
    apply: bool = False,
    tickers_override: list[str] | None = None,
    constituents_override: "set[str] | list[str] | None" = None,
    run_date: str | None = None,
    today: pd.Timestamp | None = None,
) -> dict:
    """Prune ArcticDB universe symbols that are confirmed delistings.

    Parameters
    ----------
    bucket
        S3 bucket holding both ArcticDB and constituents.json.
    absent_days
        Minimum days since last_date before a candidate is pruned. Pairs
        with the constituents-absence check to prevent flapping.
    apply
        If True, actually delete from ArcticDB. Default False (dry-run).
    tickers_override
        Skip the constituents-diff and target a specific list. Still
        gated on the last_date staleness check — operator can't blow
        up a fresh symbol via a typo.
    constituents_override
        Skip the ``latest_weekly.json`` pointer read and use this set as
        the authoritative current-constituents reference for the diff.
        Lets a caller that just refreshed constituents in-process (e.g.
        the pre-MorningEnrich preflight) prune against the freshest
        membership without needing to update the public pointer first
        (which has cross-module read fan-out — alternative/macro/
        features/compute all depend on it). Mutually exclusive with
        ``tickers_override``.
    run_date
        ``YYYY-MM-DD`` of the current Phase-1 work date. When set, the
        constituents read goes directly to
        ``market_data/weekly/{run_date}/constituents.json`` rather than
        following the ``latest_weekly.json`` pointer — required for
        in-Phase-1 callers (``weekly_collector._run_phase1``) because
        the pointer isn't advanced until ``_write_manifest`` at end-of-
        Phase-1. Without ``run_date``, a Phase-1 prune call sees LAST
        week's constituents and fails to prune this-week's REMOVALS
        (BK/FLO/PSTG for the 5/23 cycle). Mutually exclusive with
        ``constituents_override``; ignored when ``tickers_override`` is
        set. Closes ROADMAP L1397 (same TOCTOU defect class as L1316
        ``backfill`` fix in data #294).
    today
        Override the staleness reference date for testing. Defaults to
        UTC midnight today.

    Returns
    -------
    summary dict with the action plan and outcome.
    """
    s3 = boto3.client("s3")
    universe_lib = get_universe_lib(bucket)
    today = today or pd.Timestamp(datetime.now(timezone.utc).date())
    threshold_date = today - timedelta(days=absent_days)

    arctic_symbols = set(universe_lib.list_symbols())
    log.info("ArcticDB universe holds %d symbols", len(arctic_symbols))

    if tickers_override is not None and constituents_override is not None:
        raise ValueError(
            "tickers_override and constituents_override are mutually exclusive — "
            "the former targets a specific delete list, the latter swaps the "
            "freshness reference. Pass at most one."
        )

    if tickers_override is not None:
        candidates = sorted(set(tickers_override) & arctic_symbols)
        ignored = sorted(set(tickers_override) - arctic_symbols)
        if ignored:
            log.warning(
                "Skipping %d tickers from --tickers override that aren't in "
                "ArcticDB: %s",
                len(ignored), ignored,
            )
        weekly_date = "(override)"
    else:
        if constituents_override is not None:
            constituents = set(constituents_override)
            weekly_date = "(in-process override)"
            log.info(
                "Constituents from in-process override: %d tickers",
                len(constituents),
            )
        else:
            constituents, weekly_date = _load_latest_constituents(
                s3, bucket, run_date=run_date,
            )
            log.info(
                "Latest constituents (date=%s, source=%s): %d tickers",
                weekly_date,
                "run_date direct" if run_date else "latest_weekly pointer",
                len(constituents),
            )
        # Only stocks can be pruned — never touch macro/index series or
        # sector ETFs (those aren't constituents-tracked but are still
        # required by daily_append's macro-load path).
        candidates = sorted(
            t for t in arctic_symbols
            if t not in constituents
            and t not in _SKIP_TICKERS
            and not _is_sector_etf(t)
        )
        log.info(
            "Constituents-absent candidates (before last_date check): %d",
            len(candidates),
        )

    pruned: list[dict] = []
    skipped_recent: list[dict] = []
    skipped_unreadable: list[str] = []

    for ticker in candidates:
        last_date = _read_last_date(universe_lib, ticker)
        if last_date is None:
            # Read failed or empty series — refuse to delete what we
            # can't verify. Operator must investigate manually.
            skipped_unreadable.append(ticker)
            continue
        if last_date > threshold_date:
            skipped_recent.append({
                "ticker": ticker,
                "last_date": last_date.strftime("%Y-%m-%d"),
                "threshold": threshold_date.strftime("%Y-%m-%d"),
            })
            continue
        # Both conditions met — prune.
        record = {
            "ticker": ticker,
            "last_date": last_date.strftime("%Y-%m-%d"),
            "days_stale": int((today - last_date).days),
            "constituents_date": weekly_date,
        }
        if apply:
            try:
                universe_lib.delete(ticker)
            except Exception as exc:
                # Fail loudly — we don't want a half-pruned universe
                # silently passing as "done".
                raise RuntimeError(
                    f"Failed to delete {ticker} from ArcticDB universe "
                    f"(others already pruned: {[p['ticker'] for p in pruned]}): "
                    f"{exc}"
                ) from exc
            log.warning(
                "PRUNED ticker=%s last_date=%s days_stale=%d",
                ticker, record["last_date"], record["days_stale"],
            )
        else:
            log.info(
                "DRY-RUN would prune ticker=%s last_date=%s days_stale=%d",
                ticker, record["last_date"], record["days_stale"],
            )
        pruned.append(record)

    summary = {
        "status": "ok",
        "applied": apply,
        "today": today.strftime("%Y-%m-%d"),
        "absent_days_threshold": absent_days,
        "constituents_date": weekly_date,
        "arctic_universe_size_before": len(arctic_symbols),
        "candidates_count": len(candidates),
        "pruned_count": len(pruned),
        "skipped_recent_count": len(skipped_recent),
        "skipped_unreadable_count": len(skipped_unreadable),
        "pruned": pruned,
        "skipped_recent": skipped_recent,
        "skipped_unreadable": skipped_unreadable,
    }

    log.info(
        "prune_delisted_tickers: applied=%s candidates=%d pruned=%d "
        "skipped_recent=%d skipped_unreadable=%d",
        apply, len(candidates), len(pruned),
        len(skipped_recent), len(skipped_unreadable),
    )

    # Always write the audit, even on dry-run + zero-prune — gives Sat
    # SF reviewers a per-week artifact they can grep across runs.
    _write_audit(s3, bucket, summary)

    return summary


def _write_audit(s3, bucket: str, summary: dict) -> None:
    """Write a per-run audit JSON to S3 for forensic review."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    suffix = "apply" if summary["applied"] else "dryrun"
    key = f"{AUDIT_PREFIX}{summary['today']}-{ts}-{suffix}.json"
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(summary, indent=2).encode(),
            ContentType="application/json",
        )
        log.info("Audit written: s3://%s/%s", bucket, key)
    except Exception as exc:
        log.warning(
            "Failed to write audit to s3://%s/%s: %s. "
            "Pruning result still authoritative — audit is observability.",
            bucket, key, exc,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually delete from ArcticDB. Default is dry-run.",
    )
    parser.add_argument(
        "--absent-days", type=int, default=DEFAULT_ABSENT_DAYS,
        help=f"Min days since last_date for prune candidacy "
             f"(default {DEFAULT_ABSENT_DAYS}).",
    )
    parser.add_argument(
        "--tickers", type=str, default=None,
        help="Comma-separated ticker override (skip constituents diff). "
             "Last-date staleness check still applies.",
    )
    parser.add_argument(
        "--bucket", default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET}).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    tickers_override = None
    if args.tickers:
        tickers_override = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    summary = prune_delisted_tickers(
        bucket=args.bucket,
        absent_days=args.absent_days,
        apply=args.apply,
        tickers_override=tickers_override,
    )

    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
