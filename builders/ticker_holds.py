"""Machine-written per-ticker holds for an unexplained split-like break.

alpha-engine-config-I13044 (prevention for nous-ergon-ops-I2044, the CTVA
2026-10-01 spin-off). ``daily_append._splice_basis_guard`` WRITES a pure-append
bar whose ratio to the prior stored close is split-like when no registered
corporate action explains it, because a genuine crash or pop looks the same
(CAR -48%, KD -55%). It pages at ERROR. What it did not do is keep the name out
of picks while the series is suspect: a person added that hold by hand three
days later (crucible-research#840).

This module is the machine-written form of that hold. When the guard returns
``"unexplained"``, ``daily_append`` opens a hold at
``s3://{bucket}/data_quality/ticker_holds/{TICKER}.json``. The crucible-research
scanner reads every ``status: active`` hold and removes the name from its
universe before the quant filter, beside its own declared holds. The page
stays; the hold is in addition to it.

A hold clears in one of two ways, and the object stays as the record:

* automatically, when the stored series no longer prints the break: the close
  stored for ``break_date`` over the last stored close before it is back inside
  the guard's soft band. That happens when the vendor re-adjusted the history,
  or a declared action was applied and restated it. It is evidence, checked
  on the series itself, the same way ``corporate_actions.apply_declared``
  decides a restatement is already in place;
* by an operator, with a reason: ``python -m builders.ticker_holds clear
  TICKER --reason "..." --apply``. That is the path for a break that was a
  genuine move.

There is no expiry. A suspect series stays held until evidence or a person
says otherwise; the ERROR page is the operator's cue.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import pandas as pd
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

HOLDS_PREFIX = "data_quality/ticker_holds/"
HOLD_ISSUE = "alpha-engine-config-I13044"
STATUS_ACTIVE = "active"
STATUS_CLEARED = "cleared"
SCHEMA_VERSION = 1


def hold_key(ticker: str) -> str:
    return f"{HOLDS_PREFIX}{ticker}.json"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _put(s3, bucket: str, body: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=hold_key(body["ticker"]),
        Body=json.dumps(body, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def read_hold(s3, bucket: str, ticker: str) -> dict | None:
    """The stored hold for ``ticker``, or None when there is none."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=hold_key(ticker))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(obj["Body"].read())


def load_active_holds(s3, bucket: str) -> dict[str, dict]:
    """Every ``status: active`` hold, by ticker. Raises on any read failure."""
    holds: dict[str, dict] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=HOLDS_PREFIX):
        for item in page.get("Contents", []) or []:
            key = item["Key"]
            if not key.endswith(".json"):
                continue
            body = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
            if body.get("status") == STATUS_ACTIVE:
                holds[body["ticker"]] = body
    return holds


def open_hold(
    s3,
    bucket: str,
    *,
    ticker: str,
    break_date: str,
    prior_date: str,
    prior_close: float,
    break_close: float,
    ratio: float,
) -> dict | None:
    """Open a hold for an unexplained split-like break.

    Returns the written body, or None when no new hold was opened:

    * an active hold already exists. Its first break is never overwritten, and
      a further unexplained break on another date is appended to
      ``additional_breaks``, so the auto-clear check must see EVERY held break
      restated before it releases the name (a restated first break with a
      later one still printed is not a clean series);
    * the stored hold was cleared by an operator and this ``break_date`` is one
      of its held breaks (the first or any of ``additional_breaks``).
      Replaying ``daily_append`` for that date re-detects the move the
      operator already judged genuine; reopening it would undo the clearance
      (codex 14f010ed1b164061: only the first break used to count).

    Any other cleared hold is replaced: the name broke again.
    """
    existing = read_hold(s3, bucket, ticker)
    if existing is not None and existing.get("status") == STATUS_ACTIVE:
        if break_date not in _held_dates(existing):
            body = dict(existing)
            body["additional_breaks"] = list(existing.get("additional_breaks", []) or []) + [{
                "break_date": break_date,
                "prior_date": prior_date,
                "prior_close": round(float(prior_close), 6),
                "break_close": round(float(break_close), 6),
                "ratio": round(float(ratio), 6),
                "recorded_utc": _now_utc(),
            }]
            _put(s3, bucket, body)
            log.error(
                "ticker_holds: %s already held since %s (break %s); recorded the "
                "further unexplained break on %s (ratio %.4f) - the hold clears "
                "only once every held break is restated",
                ticker, existing.get("opened_utc"), existing.get("break_date"),
                break_date, ratio,
            )
        else:
            log.warning(
                "ticker_holds: %s already held since %s (break %s) - keeping that hold",
                ticker, existing.get("opened_utc"), existing.get("break_date"),
            )
        return None
    if (
        existing is not None
        and existing.get("status") == STATUS_CLEARED
        and break_date in _held_dates(existing)
        and not str(existing.get("cleared_by", "")).startswith("auto:")
    ):
        log.warning(
            "ticker_holds: %s break %s was cleared by %s (%s) - not reopening it on a replay",
            ticker, break_date, existing.get("cleared_by"), existing.get("clear_reason"),
        )
        return None
    body = {
        "schema_version": SCHEMA_VERSION,
        "ticker": ticker,
        "status": STATUS_ACTIVE,
        "break_date": break_date,
        "prior_date": prior_date,
        "prior_close": round(float(prior_close), 6),
        "break_close": round(float(break_close), 6),
        "ratio": round(float(ratio), 6),
        "opened_utc": _now_utc(),
        "opened_by": "builders.daily_append._splice_basis_guard",
        "why": (
            "pure-append close %.4f on %s vs prior stored close %.4f on %s "
            "(ratio %.4f) is split-like and no registered corporate action "
            "explains it; held out of picks until the stored series no longer "
            "prints the break or an operator clears it"
            % (break_close, break_date, prior_close, prior_date, ratio)
        ),
        "issue": HOLD_ISSUE,
    }
    _put(s3, bucket, body)
    log.error(
        "ticker_holds: OPENED hold on %s - %s (s3://%s/%s). Clears when the "
        "stored series is restated; a genuine move is cleared by an operator: "
        "python -m builders.ticker_holds clear %s --reason '...' --apply",
        ticker, body["why"], bucket, hold_key(ticker), ticker,
    )
    return body


def _held_dates(hold: dict) -> set:
    """The first ``break_date`` and every ``additional_breaks`` date of a hold."""
    return {hold.get("break_date")} | {
        b.get("break_date")
        for b in hold.get("additional_breaks", []) or []
        if isinstance(b, dict)
    }


def break_restated(hold: dict, hist: pd.DataFrame, soft_ratio: float) -> tuple[bool, float | None]:
    """Whether the stored series no longer prints any held break.

    ``(True, ratio)`` only on positive evidence for EVERY held break (the
    first ``break_date`` and each of ``additional_breaks``): a stored close for
    that date and a stored close before it whose ratio is inside
    ``(1/soft_ratio, soft_ratio)``. A missing row is not evidence. ``ratio`` is
    the first break's current ratio.
    """
    if hist is None or hist.empty or "Close" not in hist.columns:
        return False, None
    first_ok, first_ratio = _one_break_restated(hold.get("break_date"), hist, soft_ratio)
    if not first_ok:
        return False, first_ratio
    for extra in hold.get("additional_breaks", []) or []:
        if not isinstance(extra, dict) or not _one_break_restated(
            extra.get("break_date"), hist, soft_ratio
        )[0]:
            return False, first_ratio
    return True, first_ratio


def _one_break_restated(break_date, hist: pd.DataFrame, soft_ratio: float) -> tuple[bool, float | None]:
    try:
        break_ts = pd.Timestamp(break_date).normalize()
    except Exception:  # noqa: BLE001 - malformed hold, never auto-clear it
        return False, None
    if pd.isna(break_ts):
        return False, None
    closes = hist["Close"].dropna()
    if break_ts not in closes.index:
        return False, None
    prior = closes[closes.index < break_ts]
    if prior.empty:
        return False, None
    after, before = float(closes.loc[break_ts]), float(prior.iloc[-1])
    if not (after > 0 and before > 0):
        return False, None
    ratio = after / before
    return (1.0 / soft_ratio < ratio < soft_ratio), ratio


def clear_hold(s3, bucket: str, hold: dict, *, reason: str, cleared_by: str) -> dict:
    """Mark ``hold`` cleared. The object stays as the record."""
    body = dict(hold)
    body.update(
        status=STATUS_CLEARED,
        cleared_utc=_now_utc(),
        cleared_by=cleared_by,
        clear_reason=reason,
    )
    _put(s3, bucket, body)
    log.warning(
        "ticker_holds: CLEARED hold on %s (break %s) by %s: %s",
        hold["ticker"], hold.get("break_date"), cleared_by, reason,
    )
    return body


def main(argv: list[str] | None = None) -> int:
    """Operator CLI: ``list``, or ``clear TICKER --reason ... [--apply]``."""
    import boto3

    p = argparse.ArgumentParser(prog="python -m builders.ticker_holds")
    p.add_argument("--bucket", default="alpha-engine-research")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    c = sub.add_parser("clear")
    c.add_argument("ticker")
    c.add_argument("--reason", required=True)
    c.add_argument("--apply", action="store_true", help="write; default is a dry run")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    s3 = boto3.client("s3")
    if args.cmd == "list":
        for ticker, hold in sorted(load_active_holds(s3, args.bucket).items()):
            print(f"{ticker}\tbreak {hold.get('break_date')}\topened {hold.get('opened_utc')}\t{hold.get('why')}")
        return 0
    if not args.reason.strip():
        print("--reason must say why the break is not a data defect", file=sys.stderr)
        return 2
    hold = read_hold(s3, args.bucket, args.ticker)
    if hold is None or hold.get("status") != STATUS_ACTIVE:
        print(f"{args.ticker}: no active hold", file=sys.stderr)
        return 1
    if not args.apply:
        print(f"DRY RUN: would clear {args.ticker} (break {hold.get('break_date')}): {args.reason}")
        return 0
    clear_hold(s3, args.bucket, hold, reason=args.reason, cleared_by="operator")
    return 0


if __name__ == "__main__":
    sys.exit(main())
