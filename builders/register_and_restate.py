"""builders/register_and_restate.py — register a corporate split and re-restate
one ticker's stored universe history onto a single split-adjusted basis, end to
end, with a BLOCKING corruption-canary and a dry-run-by-default posture.

This turns the previously bespoke, hand-scripted split remediation — the
CRWD/HON/DD 2026-07-02 incident response (nousergon-data#588), which called
``CorporateAction.from_split`` / ``registry.record_detected`` / ``backfill
--ticker`` by hand against live production ArcticDB with no dry-run — into a
tested CLI over the EPIC config#1433 primitives:

    CorporateAction.from_split
      → CorporateActionRegistry.record_detected       (register)
      → corporate_actions.sync(STORE_ARCTICDB_UNIVERSE) (restate + write + mark)
      → features.compute.audit_action_jumps            (residual-jump canary, BLOCKING)

Operator-ratified path (config#2219 Option A, 2026-07-11): register the split
and re-restate the ticker's universe history, verifying
``Close.pct_change().abs().max()`` below the audit screen floor post-restate.

IMPORTANT — this composes the price-evidence-gated ``corporate_actions.apply``
(config#1455): it restates a split ONLY when the raw close shows the
corroborating boundary move at the ex-date. A ticker whose stored series is a
**splice** (partially adjusted — e.g. a yfinance-total-return recent window
grafted onto an un-split-adjusted older window, so the discontinuity sits at the
splice point, not the true ex-date) has NO ex-date boundary for ``apply`` to
confirm, so ``apply`` refuses (``status="unconfirmed"``) and the canary stays
red. This CLI then reports the refusal and, in ``--apply`` mode, RAISES rather
than writing a half-fixed series — the operator must first re-source that
ticker onto one consistent basis (Option A's "polygon raw" step) before the
restate can land. Fail-loud, never a silent bad write.

Usage (dry-run preview, no writes):
    python -m builders.register_and_restate --ticker MLI \
        --split-from 1 --split-to 2 --ex-date 2026-07-01
Apply (registers + writes prod ArcticDB — data box only):
    python -m builders.register_and_restate --ticker MLI \
        --split-from 1 --split-to 2 --ex-date 2026-07-01 --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd

import corporate_actions as ca
from corporate_actions import CorporateActionRegistry
from features.compute import _ACTION_JUMP_SCREEN_THRESHOLD, audit_action_jumps
from store.arctic_store import DEFAULT_BUCKET

log = logging.getLogger(__name__)

# The post-restatement corruption canary: a properly flattened split series has
# no residual daily move above the live diagnostic screen floor (config#1433).
# Sourced from features.compute so this tracks the canonical threshold rather
# than pinning a literal that can drift out of sync with the audit.
_CANARY_THRESHOLD = _ACTION_JUMP_SCREEN_THRESHOLD


def _max_abs_daily_move(df: pd.DataFrame) -> float:
    """Largest absolute daily Close pct-change in ``df`` (the canary metric).
    ``nan`` for a frame with <2 priced rows."""
    if df is None or df.empty or "Close" not in df.columns:
        return float("nan")
    moves = df["Close"].pct_change().abs().dropna()
    return float(moves.max()) if not moves.empty else float("nan")


def register_and_restate(
    ticker: str,
    split_from: "int | float",
    split_to: "int | float",
    ex_date: str,
    *,
    bucket: str = DEFAULT_BUCKET,
    s3=None,
    dry_run: bool = True,
    run_id: str | None = None,
) -> dict:
    """Register a split for ``ticker`` and restate its universe history.

    ``dry_run`` (default) previews with ZERO side effects: it reads the current
    universe series, restates in-memory via the price-evidence-gated
    ``corporate_actions.apply``, and reports the pre/post canary — it does NOT
    write the registry or ArcticDB. ``dry_run=False`` (the CLI ``--apply``)
    registers the split and runs ``corporate_actions.sync`` (which restates,
    writes back, and marks applied), then RE-READS and RAISES if the canary is
    still red (a refused / incomplete restatement must never be reported clean).

    Returns a JSON-serializable summary; never merges or touches other tickers.
    """
    if s3 is None:
        s3 = boto3.client("s3")
    if run_id is None:
        run_id = "register-and-restate-" + datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )
    action = ca.CorporateAction.from_split(
        ticker=ticker,
        ex_date=ex_date,
        split_from=split_from,
        split_to=split_to,
        source="register_and_restate",
    )
    registry = CorporateActionRegistry(s3, bucket)

    # Lazy import (mirrors corporate_actions._sync_arcticdb_universe) so a single
    # monkeypatch of store.arctic_store.get_universe_lib drives both this read
    # and sync's internal restatement write.
    from store.arctic_store import get_universe_lib

    lib = get_universe_lib(bucket)
    try:
        before = lib.read(ticker).data
    except Exception as exc:  # noqa: BLE001 - symbol absent ⇒ nothing to restate
        return {
            "status": "no_such_symbol",
            "ticker": ticker,
            "action_id": action.action_id,
            "detail": f"{ticker} not present in the universe ArcticDB lib ({exc})",
        }

    canary_before = _max_abs_daily_move(before)
    summary: dict = {
        "ticker": ticker,
        "action_id": action.action_id,
        "split": f"{split_from}:{split_to}",
        "ex_date": ex_date,
        "human": action.human(),
        "canary_threshold": _CANARY_THRESHOLD,
        "canary_before": round(canary_before, 6) if pd.notna(canary_before) else None,
        "dry_run": dry_run,
    }

    if dry_run:
        # In-memory restatement preview via the price-evidence-gated apply();
        # registry=None so nothing is marked/written.
        restated, applied_math = ca.apply(
            before, [action], store=ca.STORE_ARCTICDB_UNIVERSE, registry=None,
            run_id=run_id,
        )
        canary_after = _max_abs_daily_move(restated)
        n_adjusted = sum(int(r.get("n_rows_adjusted", 0)) for r in applied_math)
        statuses = sorted({r.get("status") for r in applied_math})
        cleared = pd.notna(canary_after) and canary_after < _CANARY_THRESHOLD
        summary.update(
            status="dry_run_ok" if cleared else "dry_run_canary_not_cleared",
            apply_statuses=statuses,
            n_rows_would_adjust=n_adjusted,
            canary_after=round(canary_after, 6) if pd.notna(canary_after) else None,
            canary_cleared=bool(cleared),
            note=(
                "Restatement would flatten the series."
                if cleared
                else "apply() did not clear the canary — the stored series is not a "
                "single-basis series this split can flatten in place (e.g. a splice, "
                "or a refused price-evidence orientation). Re-source the ticker onto "
                "one consistent basis before restating; NO write in --apply mode "
                "until the canary clears."
            ),
        )
        return summary

    # --apply: register, then restate+write+mark via sync (dividend scan skipped).
    registry.record_detected(action, run_id=run_id)
    sync_result = ca.sync(
        s3, bucket, ex_date, ex_date,
        stores=[ca.STORE_ARCTICDB_UNIVERSE],
        run_id=run_id, tickers=[ticker], registry=registry,
        actions=[action], dividend_actions=[],
    )
    applied = sync_result.applied.get(ca.STORE_ARCTICDB_UNIVERSE, [])
    after = lib.read(ticker).data
    canary_after = _max_abs_daily_move(after)
    audit = audit_action_jumps({ticker: after}, registry)
    cleared = pd.notna(canary_after) and canary_after < _CANARY_THRESHOLD
    summary.update(
        status="applied" if cleared else "canary_not_cleared",
        apply_statuses=sorted({r.get("status") for r in applied}),
        n_rows_adjusted=sum(int(r.get("n_rows_adjusted", 0)) for r in applied),
        canary_after=round(canary_after, 6) if pd.notna(canary_after) else None,
        canary_cleared=bool(cleared),
        audit_missed=audit.missed,
    )
    if not cleared:
        # A registered-but-un-flattened split is the data#1298 corruption class:
        # refuse to report success, so the operator falls back to re-sourcing.
        raise RuntimeError(
            f"register_and_restate: {ticker} split registered but canary NOT "
            f"cleared (max |daily move|={canary_after} ≥ {_CANARY_THRESHOLD}); "
            f"apply statuses={summary['apply_statuses']}. The stored series is "
            f"not single-basis (likely a splice) — re-source onto one consistent "
            f"basis and re-run. NOT reporting a clean restatement."
        )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register a corporate split + restate a ticker's universe "
        "history (dry-run by default; --apply writes prod ArcticDB)."
    )
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--split-from", type=float, required=True,
                        help="Ratio numerator (e.g. 1 for a 2-for-1 forward split)")
    parser.add_argument("--split-to", type=float, required=True,
                        help="Ratio denominator (e.g. 2 for a 2-for-1 forward split)")
    parser.add_argument("--ex-date", required=True, help="Ex-date YYYY-MM-DD")
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

    result = register_and_restate(
        args.ticker, _norm(args.split_from), _norm(args.split_to), args.ex_date,
        bucket=args.bucket, dry_run=not args.apply,
    )
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") in ("dry_run_canary_not_cleared", "canary_not_cleared",
                                "no_such_symbol"):
        sys.exit(1)


if __name__ == "__main__":
    main()
