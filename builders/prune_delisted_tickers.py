"""builders/prune_delisted_tickers.py — orchestrated delisted-ticker cleanup.

SURVIVORSHIP-FREE RETENTION (config#1943, Leg 3): a confirmed delisting is NO
LONGER hard-deleted. Its FULL OHLCV history + as-of-membership metadata is first
MOVED into the separate ``delisted_history`` ArcticDB library (retention store),
and ONLY on a successful retention write is the symbol removed from the live
``universe`` library. This stops the destruction of delisted price history that
was the dominant survivorship-bias term (~1-4%/yr overstatement) — a
point-in-time backtest universe can now be reconstructed from the retention
store. If the retention write FAILS, the symbol is NOT deleted from ``universe``
this pass (fail-safe: never lose data). Retention is idempotent — re-running the
pruner overwrites the same ``delisted_history`` record in place rather than
duplicating it. The separate-library choice (over an in-``universe`` "delisted"
flag) keeps every live-trading consumer of ``universe`` unchanged: they still see
only tradable names and never learn a flag. See
``store.arctic_store.get_delisted_history_lib`` for the record schema/contract.
Backfilling already-pruned names and the backtester's consumption of this store
are SEPARATE follow-on legs (config#1942 / #1943 dependents) — this builder only
establishes the retention store and stops destroying history going forward.

Before any deletion, RENAME-triggered migration runs (corporate-actions PR6,
config#1433): a candidate going missing may be a 1:1 ticker RENAME, not a
delisting. Each candidate is checked against polygon's ticker-events API; a
``ticker_change`` where the candidate is the OLD ticker MIGRATES its ArcticDB
history to the new symbol key (predictor continuity) and removes it from the
prune set. Mergers of the acquired symbol have no ticker_change and prune as a
delisting (NOT spliced). HISTORY-SAFETY: a candidate whose rename detection /
migration FAILS is SKIPPED this pass — never pruned on a detection failure, so
a polygon outage cannot delete a renamed symbol's history before it migrates.

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

import corporate_actions as ca
from builders._constituents_loader import load_constituents_for_run_date
from features.compute import DEFAULT_BUCKET, _SKIP_TICKERS, _is_sector_etf
from polygon_client import polygon_client
from store.arctic_store import get_delisted_history_lib, get_universe_lib

log = logging.getLogger(__name__)

DEFAULT_ABSENT_DAYS = 14
AUDIT_PREFIX = "builders/prune_audit/"

# Schema version stamped on every delisted_history record's metadata. Bump
# only on a breaking layout change; readers gate on it. Kept next to the
# producer so the contract version lives beside the code that writes it.
DELISTED_HISTORY_SCHEMA_VERSION = 1


def _build_registry(s3, bucket: str):
    """Construct a ``CorporateActionRegistry`` from an S3 client + bucket, or
    ``None`` when no usable client is available (mirrors
    ``features.compute._build_registry``). Used by the rename-migration phase to
    record renames + enforce exactly-once ArcticDB symbol migration."""
    if s3 is None:
        return None
    try:
        return ca.CorporateActionRegistry(s3, bucket)
    except Exception as exc:  # noqa: BLE001 - degrade, never hard-fail the prune
        log.warning(
            "could not build corporate-action registry (%s) — rename "
            "migration degraded this pass", exc,
        )
        return None


def _build_rename_client():
    """Construct a polygon client for rename detection, or ``None`` on failure.

    Indirected through a module-level seam (``builders.prune_delisted_tickers.
    polygon_client``) so tests patch it without a live API call. A construction
    failure degrades to ``None`` → the caller treats EVERY candidate as a
    detection failure and prunes none of them this pass (history-safety)."""
    try:
        return polygon_client()
    except Exception as exc:  # noqa: BLE001 - degrade, never hard-fail the prune
        log.warning(
            "could not construct polygon client for rename detection (%s) — "
            "no rename detection this pass; refusing to prune on an "
            "unverifiable candidate set (history-safety)", exc,
        )
        return None


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


def _retain_delisted(
    universe_lib,
    delisted_lib,
    ticker: str,
    *,
    today: pd.Timestamp,
    constituents_date: str,
) -> dict:
    """MOVE a confirmed-delisted ticker's history into ``delisted_history``.

    Reads ``ticker``'s FULL frame from the universe library and writes it
    VERBATIM (re-keyed, NOT re-projected — same identity-preserving discipline
    as ``corporate_actions.migrate_symbol``) into ``delisted_lib`` under the
    same ticker key, carrying the as-of-membership metadata contract defined in
    ``store.arctic_store.get_delisted_history_lib``.

    Idempotent: writes with ``prune_previous_versions=True`` so a re-run over an
    already-retained (but not-yet-deleted) ticker overwrites the single record
    in place — no duplication, no version pile-up.

    This is the RETENTION half of the fail-safe: the caller only deletes from
    the live universe AFTER this returns successfully. Any read/empty/write
    failure RAISES here so the caller can abort the delete for this ticker and
    preserve the data (never lose history on a retention failure).

    Returns the record's metadata dict (also embedded in the audit summary).
    """
    df = universe_lib.read(ticker).data
    if df is None or df.empty:
        # An empty frame means there's nothing to retain — refuse to proceed to
        # a delete on a symbol we couldn't actually read (defensive; the
        # staleness gate already read a non-empty tail(1), so this is a
        # concurrent-mutation / read-inconsistency guard).
        raise RuntimeError(
            f"refusing to retain+delete {ticker}: universe read returned an "
            f"empty frame (nothing to preserve in delisted_history)"
        )

    first_active = pd.Timestamp(df.index[0]).strftime("%Y-%m-%d")
    last_active = pd.Timestamp(df.index[-1]).strftime("%Y-%m-%d")
    metadata = {
        "schema_version": DELISTED_HISTORY_SCHEMA_VERSION,
        "symbol": ticker,
        "delisted_detected_on": today.strftime("%Y-%m-%d"),
        "first_active_date": first_active,
        "last_active_date": last_active,
        "rows": int(len(df)),
        "constituents_date": constituents_date,
        "retained_at": datetime.now(timezone.utc).isoformat(),
        "source": "prune_delisted_tickers",
    }
    # Write-then-(caller-deletes): the retention record lands BEFORE the live
    # universe delete, so a failure here leaves the universe untouched.
    delisted_lib.write(
        ticker, df, metadata=metadata, prune_previous_versions=True,
    )
    log.warning(
        "RETAINED ticker=%s -> delisted_history rows=%d window=%s..%s "
        "detected_on=%s", ticker, metadata["rows"], first_active, last_active,
        metadata["delisted_detected_on"],
    )
    return metadata


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
    # Survivorship-free retention store (config#1943, Leg 3). Opened lazily via
    # a module-level seam so tests can patch it; only actually touched on the
    # apply-path deletion loop below.
    delisted_lib = None
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

    # ── PR6: rename-triggered ArcticDB migration BEFORE the prune deletion ───
    # A prune candidate going missing from constituents/daily_closes may be a
    # 1:1 ticker RENAME (e.g. FB -> META), NOT a delisting. Query polygon
    # ticker-events for every candidate FIRST: a ``ticker_change`` where the
    # candidate is the OLD ticker means it was renamed → MIGRATE its full
    # ArcticDB history to the new key (predictor continuity) and REMOVE it from
    # the prune set. A genuine delist / merger-OF-THE-ACQUIRED has no
    # ticker_change and prunes as today (mergers are NOT spliced).
    #
    # Ordering is load-bearing: migration MUST run before the deletion loop, or
    # the prune would delete the orphaned old-symbol history before it can be
    # carried over.
    #
    # HISTORY-SAFETY: a candidate whose rename DETECTION (or migration) FAILS
    # is SKIPPED this pass — never pruned on a failure. A polygon outage must
    # not cause a renamed symbol's history to be wrongly deleted; the candidate
    # is re-examined next pass once detection succeeds.
    registry = _build_registry(s3, bucket)
    rename_client = _build_rename_client()
    run_id = f"prune-{today.strftime('%Y-%m-%d')}"
    migrated: list[dict] = []
    skipped_rename_detect_failed: list[str] = []
    skip_prune: set[str] = set()

    if candidates and (registry is None or rename_client is None):
        # Cannot run rename detection (no registry or no polygon client) → we
        # cannot confirm any candidate is a delist vs a rename. Refuse to prune
        # blind this pass (history-safety).
        log.warning(
            "prune: rename detection unavailable (registry=%s polygon=%s) — "
            "refusing to prune %d candidate(s) this pass (history-safety)",
            registry is not None, rename_client is not None, len(candidates),
        )
        skipped_rename_detect_failed = list(candidates)
        skip_prune = set(candidates)
    elif candidates:
        detection = ca.detect_renames(candidates, client=rename_client)
        for action in detection.renames:
            old_t, new_t = action.old_ticker, action.new_ticker
            record = {
                "old_ticker": old_t,
                "new_ticker": new_t,
                "ex_date": action.ex_date,
                "action_id": action.action_id,
                "migrated": False,
            }
            if apply:
                try:
                    did = ca.migrate_symbol(
                        universe_lib, old_t, new_t,
                        registry=registry, run_id=run_id, ex_date=action.ex_date,
                    )
                    record["migrated"] = bool(did)
                    skip_prune.add(old_t)  # migrated (or already-applied) — not a delist
                    log.warning(
                        "RENAME-MIGRATED ticker=%s -> %s ex_date=%s applied=%s",
                        old_t, new_t, action.ex_date, did,
                    )
                except Exception as exc:  # noqa: BLE001 - skip, never prune the rename
                    log.warning(
                        "migrate_symbol failed for %s -> %s (%s) — NOT pruning "
                        "%s this pass (history-safety)", old_t, new_t, exc, old_t,
                    )
                    record["error"] = str(exc)
                    skip_prune.add(old_t)
                    skipped_rename_detect_failed.append(old_t)
            else:
                log.info(
                    "DRY-RUN would migrate ticker=%s -> %s ex_date=%s",
                    old_t, new_t, action.ex_date,
                )
                skip_prune.add(old_t)  # in dry-run, exclude from the would-prune set too
            migrated.append(record)
        # Candidates whose ticker-events query RAISED — skip pruning this pass.
        for cand in sorted(detection.failed_candidates):
            skip_prune.add(cand)
            skipped_rename_detect_failed.append(cand)

    if skip_prune:
        candidates = [c for c in candidates if c not in skip_prune]

    pruned: list[dict] = []
    skipped_recent: list[dict] = []
    skipped_unreadable: list[str] = []
    retained: list[dict] = []
    skipped_retention_failed: list[str] = []

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
            # RETENTION-BEFORE-DELETE (config#1943, Leg 3, fail-safe): move the
            # full OHLCV history + as-of-membership metadata into the separate
            # delisted_history library FIRST. Only on a successful retention
            # write do we delete from the live universe. A retention failure
            # SKIPS the delete for this ticker (never lose data) and is retried
            # next pass — it does NOT abort the whole run, so one bad symbol
            # can't strand the rest of the prune.
            if delisted_lib is None:
                delisted_lib = get_delisted_history_lib(bucket)
            try:
                meta = _retain_delisted(
                    universe_lib, delisted_lib, ticker,
                    today=today, constituents_date=weekly_date,
                )
            except Exception as exc:  # noqa: BLE001 - fail safe, never delete
                log.warning(
                    "RETENTION FAILED for %s (%s) — NOT deleting from universe "
                    "this pass (history-safety); will retry next run", ticker, exc,
                )
                skipped_retention_failed.append(ticker)
                continue
            record["retained"] = True
            record["retained_rows"] = meta["rows"]
            retained.append({"ticker": ticker, **meta})
            try:
                universe_lib.delete(ticker)
            except Exception as exc:
                # Fail loudly — we don't want a half-pruned universe
                # silently passing as "done". The history is already safe in
                # delisted_history at this point, so no data is lost; the
                # operator investigates the delete failure.
                raise RuntimeError(
                    f"Failed to delete {ticker} from ArcticDB universe "
                    f"(history already retained in delisted_history; others "
                    f"already pruned: {[p['ticker'] for p in pruned]}): {exc}"
                ) from exc
            log.warning(
                "PRUNED ticker=%s last_date=%s days_stale=%d (history retained "
                "in delisted_history)",
                ticker, record["last_date"], record["days_stale"],
            )
        else:
            log.info(
                "DRY-RUN would retain+prune ticker=%s last_date=%s days_stale=%d",
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
        "retained_count": len(retained),
        "skipped_recent_count": len(skipped_recent),
        "skipped_unreadable_count": len(skipped_unreadable),
        "skipped_retention_failed_count": len(skipped_retention_failed),
        "migrated_count": len([m for m in migrated if m.get("migrated")]),
        "skipped_rename_detect_failed_count": len(skipped_rename_detect_failed),
        "pruned": pruned,
        "retained": retained,
        "skipped_recent": skipped_recent,
        "skipped_unreadable": skipped_unreadable,
        "skipped_retention_failed": sorted(set(skipped_retention_failed)),
        "migrated": migrated,
        "skipped_rename_detect_failed": sorted(set(skipped_rename_detect_failed)),
    }

    log.info(
        "prune_delisted_tickers: applied=%s pruned=%d retained=%d migrated=%d "
        "skipped_recent=%d skipped_unreadable=%d skipped_retention_failed=%d "
        "skipped_rename_detect_failed=%d",
        apply, len(pruned), len(retained), summary["migrated_count"],
        len(skipped_recent), len(skipped_unreadable),
        len(skipped_retention_failed), len(skipped_rename_detect_failed),
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
