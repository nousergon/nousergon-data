"""builders/splice_rebase.py — Option B: flatten a SPLICED universe series by
re-basing at the stored discontinuity, then register the corporate action at
its TRUE (SEC-confirmed) ex-date.

config#2219 (MLI): ``register_and_restate.py`` (Option A) requires
``corporate_actions.apply``'s price-evidence gate to confirm a split at its
true ex-date. MLI's stored ``arcticdb_universe`` series is a SPLICE — rows
before 2026-06-12 sit on the un-split-adjusted (~2x) basis, rows from
2026-06-12 onward are already on the split-adjusted basis, and there is no
price discontinuity at the true SEC ex-date (2026-07-01) for ``apply`` to
confirm (verified live: ``dry_run_canary_not_cleared``, orientation=none,
canary 0.233 vs 0.18 threshold). ``apply`` correctly refuses — an in-place
restate anchored at the true ex-date would double-adjust the already-split
06-12..06-30 window.

This module re-bases directly at the STORED discontinuity (06-12) using the
same reusable multiplicative-factor primitive
(``corporate_actions.restate_series_for_splits`` /
``corporate_actions._split_math.cumulative_factor``) split.apply() calls
internally — it just supplies the boundary date explicitly instead of
deriving it from price evidence, since a spliced series has no ex-date
evidence to derive it from. The CorporateAction registered in the registry
still carries the TRUE SEC ex-date (2026-07-01) — the registry is a record
of what actually happened in the world; the math event is purely an
implementation detail of how the ALREADY-partially-restated stored series
gets flattened onto one consistent basis.

Usage (dry-run preview, no writes):
    python -m builders.splice_rebase --ticker MLI --splice-date 2026-06-12 \
        --true-ex-date 2026-07-01 --split-from 1 --split-to 2
Apply (writes prod ArcticDB + registers the action — data box only):
    python -m builders.splice_rebase --ticker MLI --splice-date 2026-06-12 \
        --true-ex-date 2026-07-01 --split-from 1 --split-to 2 --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd

from corporate_actions import CorporateAction, CorporateActionRegistry, STORE_ARCTICDB_UNIVERSE
from corporate_actions._split_math import restate_series_for_splits
from features.compute import _ACTION_JUMP_SCREEN_THRESHOLD
from store.arctic_store import DEFAULT_BUCKET

log = logging.getLogger(__name__)

_CANARY_THRESHOLD = _ACTION_JUMP_SCREEN_THRESHOLD


def _max_abs_daily_move(df: pd.DataFrame) -> float:
    if df is None or df.empty or "Close" not in df.columns:
        return float("nan")
    moves = df["Close"].pct_change().abs().dropna()
    return float(moves.max()) if not moves.empty else float("nan")


def splice_rebase(
    ticker: str,
    splice_date: str,
    true_ex_date: str,
    split_from: "int | float",
    split_to: "int | float",
    *,
    bucket: str = DEFAULT_BUCKET,
    s3=None,
    dry_run: bool = True,
    run_id: str | None = None,
) -> dict:
    """Flatten ``ticker``'s spliced universe series at ``splice_date`` and
    register the split at ``true_ex_date``. Returns a JSON-serializable
    summary; touches only ``ticker``."""
    if s3 is None:
        s3 = boto3.client("s3")
    if run_id is None:
        run_id = "splice-rebase-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Lazy import (mirrors builders.register_and_restate /
    # corporate_actions._sync_arcticdb_universe) so a single monkeypatch of
    # store.arctic_store.get_universe_lib drives this read AND the write below
    # — a top-level `from store.arctic_store import get_universe_lib` binds
    # the original function object at import time and silently ignores a
    # test's patch, reading real production data instead of the fixture.
    from store.arctic_store import get_universe_lib, to_arctic_canonical

    lib = get_universe_lib(bucket)
    try:
        before = lib.read(ticker).data
    except Exception as exc:  # noqa: BLE001 - symbol absent => nothing to rebase
        return {
            "status": "no_such_symbol",
            "ticker": ticker,
            "detail": f"{ticker} not present in the universe ArcticDB lib ({exc})",
        }

    canary_before = _max_abs_daily_move(before)

    action = CorporateAction.from_split(
        ticker=ticker,
        ex_date=true_ex_date,
        split_from=split_from,
        split_to=split_to,
        source="splice_rebase",
    )

    # is_applied MUST be checked BEFORE computing the restate math:
    # restate_series_for_splits is not idempotent (it always applies the FULL
    # cumulative factor per-call, mirroring corporate_actions.apply's own
    # contract) — re-running it against an already-flattened series
    # double-adjusts. An already-applied action is a pure noop, full stop.
    registry = CorporateActionRegistry(s3, bucket)
    if not dry_run and registry.is_applied(STORE_ARCTICDB_UNIVERSE, action.action_id):
        return {
            "ticker": ticker,
            "action_id": action.action_id,
            "split": f"{split_from}:{split_to}",
            "splice_date": splice_date,
            "true_ex_date": true_ex_date,
            "canary_before": round(canary_before, 6) if pd.notna(canary_before) else None,
            "dry_run": dry_run,
            "status": "noop_already_applied",
        }

    # The math event: anchor the multiplicative factor at the STORED
    # discontinuity, not the true ex-date — restate_series_for_splits
    # multiplies price cols by split_from/split_to and divides volume by the
    # same factor for every row STRICTLY BEFORE this date.
    math_event = {
        "execution_date": splice_date,
        "split_from": split_from,
        "split_to": split_to,
    }
    restated = restate_series_for_splits(before, [math_event])
    canary_after = _max_abs_daily_move(restated)
    cleared = pd.notna(canary_after) and canary_after < _CANARY_THRESHOLD
    n_adjusted = int(
        (restated["Close"] != before["Close"]).sum()
        if "Close" in before.columns and "Close" in restated.columns
        else 0
    )

    summary: dict = {
        "ticker": ticker,
        "action_id": action.action_id,
        "split": f"{split_from}:{split_to}",
        "splice_date": splice_date,
        "true_ex_date": true_ex_date,
        "canary_threshold": _CANARY_THRESHOLD,
        "canary_before": round(canary_before, 6) if pd.notna(canary_before) else None,
        "canary_after": round(canary_after, 6) if pd.notna(canary_after) else None,
        "canary_cleared": bool(cleared),
        "n_rows_changed": n_adjusted,
        "dry_run": dry_run,
    }

    if dry_run:
        summary["status"] = "dry_run_ok" if cleared else "dry_run_canary_not_cleared"
        return summary

    if not cleared:
        raise RuntimeError(
            f"splice_rebase: {ticker} re-based at {splice_date} but canary NOT "
            f"cleared (max |daily move|={canary_after} >= {_CANARY_THRESHOLD}). "
            f"Refusing to write or register — the splice boundary date is "
            f"likely wrong for this ticker. NOT reporting a clean restatement."
        )

    # WRITE-THEN-MARK (mirrors corporate_actions._sync_arcticdb_universe): the
    # registry applied-marker is the contract daily_append trusts to mean "this
    # history is on the restated scale" — never mark before the write lands.
    lib.write(ticker, to_arctic_canonical(restated), prune_previous_versions=True)
    registry.record_detected(action, run_id=run_id)
    registry.mark_applied(action, STORE_ARCTICDB_UNIVERSE, run_id=run_id)

    after = lib.read(ticker).data
    canary_reread = _max_abs_daily_move(after)
    summary["canary_after_reread"] = (
        round(canary_reread, 6) if pd.notna(canary_reread) else None
    )
    summary["status"] = "applied"
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flatten a spliced universe series at its stored "
        "discontinuity and register the split at its true ex-date "
        "(dry-run by default; --apply writes prod ArcticDB)."
    )
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--splice-date", required=True,
                        help="Date of the STORED discontinuity (YYYY-MM-DD)")
    parser.add_argument("--true-ex-date", required=True,
                        help="SEC-confirmed true ex-date (YYYY-MM-DD)")
    parser.add_argument("--split-from", type=float, required=True)
    parser.add_argument("--split-to", type=float, required=True)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--apply", action="store_true",
                        help="WRITE the registry + prod ArcticDB (default: dry-run)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    def _norm(x: float) -> "int | float":
        return int(x) if float(x).is_integer() else x

    result = splice_rebase(
        args.ticker, args.splice_date, args.true_ex_date,
        _norm(args.split_from), _norm(args.split_to),
        bucket=args.bucket, dry_run=not args.apply,
    )
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") in ("dry_run_canary_not_cleared", "no_such_symbol"):
        sys.exit(1)


if __name__ == "__main__":
    main()
