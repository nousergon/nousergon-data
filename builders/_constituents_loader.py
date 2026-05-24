"""Shared constituents.json reader — single chokepoint for both
``builders/backfill.py`` and ``builders/prune_delisted_tickers.py``.

Lifted 2026-05-24 to close ROADMAP L1397 + 5/23-SF P0 sweep follow-on
per [[feedback_lift_invariants_to_chokepoint_after_second_recurrence]]:
the same ``latest_weekly.json`` pointer-vs-direct-read TOCTOU defect class
that bit ``backfill()`` (data #294) also lives in
``prune_delisted_tickers._load_latest_constituents``. Both consumers
live in the same repo, so the institutional move is an in-repo helper
chokepoint — not cross-repo lib lift. (A third reader in a different
repo would justify the cross-repo lift; until then, in-repo helper is
right-sized scope per ``~/Development/CLAUDE.md`` SOTA rule.)

Failure mode the helper closes (verbatim from 2026-05-23 SF L1316):

  - Constituents collector writes ``weekly/2026-05-23/constituents.json``
    at T0 (903 tickers including BNY/P/SN).
  - ``_write_manifest`` advances ``latest_weekly.json`` pointer to
    2026-05-23 at T0 + N min (end of Phase 1).
  - Any reader running between T0 and T0+N reading the pointer sees the
    PRIOR weekly's constituents — BNY/P/SN missing on the way IN to
    ArcticDB writes (backfill); BK/FLO/PSTG still pruning-protected on
    the way OUT (prune). Both sides have the same root cause.

Fix: callers that know their ``run_date`` (the Phase-1 work date)
read directly from ``weekly/{run_date}/constituents.json``. Callers
that don't (CLI, ad-hoc recovery) fall back to the pointer — which
is then safely advanced after the work completes.
"""

from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)


def load_constituents_for_run_date(
    s3,
    bucket: str,
    run_date: str | None = None,
) -> tuple[set[str], str]:
    """Load the current S&P 500 + 400 constituents set.

    When ``run_date`` is provided (Phase-1 happy path), reads directly
    from ``market_data/weekly/{run_date}/constituents.json`` — the file
    the constituents collector wrote earlier in this same Phase-1
    invocation. Otherwise falls back to ``market_data/latest_weekly.json``
    for ad-hoc callers (per-ticker recovery, dry-runs, post-Phase-1
    intraday recovery scripts).

    Parameters
    ----------
    s3
        boto3 S3 client.
    bucket
        S3 bucket holding the weekly constituents partitions.
    run_date
        ``YYYY-MM-DD`` of the current Phase-1 work date, OR ``None`` to
        follow the pointer.

    Returns
    -------
    tuple
        ``(tickers_set, weekly_date_str)`` — ``tickers_set`` is the set
        of constituent symbols; ``weekly_date_str`` is the resolved
        weekly partition's date (the value of ``run_date`` when passed,
        or the pointer's ``date`` field when not).

    Raises
    ------
    RuntimeError
        If ``constituents.json`` is reachable but missing/empty the
        ``tickers`` field. Filtering against an empty set would write
        zero tickers to ArcticDB OR delete every symbol on the prune
        path — both are catastrophic. Fail-loud per
        ``[[feedback_no_silent_fails]]``.
    """
    if run_date:
        key = f"market_data/weekly/{run_date}/constituents.json"
        weekly_date = run_date
        source = f"run_date={run_date} (direct)"
    else:
        pointer_obj = s3.get_object(Bucket=bucket, Key="market_data/latest_weekly.json")
        pointer = json.loads(pointer_obj["Body"].read())
        weekly_date = pointer["date"]
        prefix = pointer["s3_prefix"].rstrip("/")
        key = f"{prefix}/constituents.json"
        source = f"pointer→{weekly_date}"
    cons_obj = s3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(cons_obj["Body"].read())
    tickers = payload.get("tickers")
    if not tickers:
        raise RuntimeError(
            f"constituents.json at s3://{bucket}/{key} ({source}) has no "
            f"`tickers` field — refusing to operate against an empty "
            f"constituents set (would either write zero tickers to arctic "
            f"universe OR delete every symbol on the prune path)."
        )
    return set(tickers), weekly_date
