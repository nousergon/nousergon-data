"""builders/migrate_universe_feature_order.py — normalize feature column order in ArcticDB universe.

Background (2026-05-21 EOD blackout, PR #279 column-order regression):
----------------------------------------------------------------------
PR #279 (Growth + Stewardship pillar substrate, merged 2026-05-20 23:07Z)
inserted five new fundamental fields — ``revenue_growth_3y``,
``eps_growth_3y``, ``payout_ratio``, ``dividend_yield``, ``capex_growth_5y``
— into ``features.feature_engineer.FEATURES`` *between* the v3.0
fundamentals block and the v3.1 return block.

The follow-on weekday-SF MorningEnrich (2026-05-21 ~12:46Z) ran
``daily_append`` against ArcticDB symbols that still carried the
pre-PR-#279 72-column schema. For the 891 tickers whose ArcticDB
``last_date`` matched the morning target_date, daily_append routed
through the WRITE branch (``builders/daily_append.py``: backfill
splice — full rewrite via ``pd.concat([hist, today_row])``). Pandas'
default outer-join preserved ``hist``'s 72-column order and *appended*
the five new pillar columns at the end — so 891 symbols ended up with::

    [...old 72 cols..., revenue_growth_3y, eps_growth_3y, payout_ratio,
     dividend_yield, capex_growth_5y]   # pillars at end

instead of the canonical FEATURES-order layout that ``today_row`` ships
with::

    [..., gross_margin, roe, current_ratio,
     revenue_growth_3y, eps_growth_3y, payout_ratio, dividend_yield,
     capex_growth_5y,                          # pillars in middle
     return_60d, return_120d, ..., realized_vol_63d]

The same-day EOD's ``daily_append`` then routed through the UPDATE
branch (``target_ts > hist.max()``) with ``today_row`` in canonical
order and tripped ArcticDB's ``StreamDescriptorMismatch`` on every
affected symbol — 905/905 error rate, pipeline halt before EOD
Reconcile + StopTradingInstance.

The WRITE-path defect has been closed in the same PR as this script
(``builders/daily_append.py`` now re-projects ``combined`` to canonical
order before WritePayload). This script repairs the 891 symbols whose
schema was scrambled by the earlier WRITE path so the next EOD's
UPDATE path can find a matching layout.

Mirrors the design of ``builders/migrate_universe_vwap.py`` (same class
of repair: a one-off canonical-order normalization), with the canonical
order extended to::

    OHLCV_COLS + [PROVENANCE_COL] + FEATURES

(matching what ``daily_append`` writes via ``today_row``). Feature
columns that are present in the symbol but not in ``FEATURES`` (e.g.
deprecated experimental features that haven't been pruned yet) are
preserved at the end of the row in their existing relative order, so
the migration never drops data.

Idempotent — symbols already in canonical order are skipped.

Usage::

    python -m builders.migrate_universe_feature_order            # dry-run
    python -m builders.migrate_universe_feature_order --apply    # actually write
    python -m builders.migrate_universe_feature_order --apply --tickers AAPL,MMM
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

from builders.daily_append import OHLCV_COLS, PROVENANCE_COL
from features.compute import DEFAULT_BUCKET
from features.feature_engineer import FEATURES
from store.arctic_store import get_universe_lib

log = logging.getLogger(__name__)

AUDIT_PREFIX = "builders/migrate_universe_feature_order_audit/"
DEFAULT_WORKERS = 16


def _canonical_column_order(existing_cols: list[str]) -> list[str]:
    """Return the canonical column ordering for a universe symbol.

    ``OHLCV_COLS + [PROVENANCE_COL] + FEATURES`` first, then any existing
    columns that fall outside all three sets — preserved in their current
    relative order at the end so deprecated/experimental fields aren't
    silently dropped.
    """
    ohlcv_set = set(OHLCV_COLS)
    features_set = set(FEATURES)
    head: list[str] = []
    head.extend(c for c in OHLCV_COLS if c in existing_cols)
    if PROVENANCE_COL in existing_cols:
        head.append(PROVENANCE_COL)
    head.extend(f for f in FEATURES if f in existing_cols)
    accounted = set(head)
    tail = [
        c for c in existing_cols
        if c not in accounted
        and c not in ohlcv_set
        and c not in features_set
        and c != PROVENANCE_COL
    ]
    return head + tail


def _is_canonical(existing_cols: list[str]) -> bool:
    """True iff existing column order already matches the canonical layout."""
    return list(existing_cols) == _canonical_column_order(list(existing_cols))


def _write_audit(s3, bucket: str, summary: dict) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{AUDIT_PREFIX}{ts}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(summary, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("Wrote audit to s3://%s/%s", bucket, key)


def migrate_universe_feature_order(
    *,
    bucket: str = DEFAULT_BUCKET,
    apply: bool = False,
    tickers_override: list[str] | None = None,
) -> dict:
    """Normalize universe symbols to canonical FEATURES-order column layout.

    Parameters
    ----------
    bucket
        S3 bucket holding ArcticDB.
    apply
        If True, actually write the reordered frames. Default False
        (dry-run; counts + per-ticker diff log only).
    tickers_override
        Subset of symbols to migrate (rest are left alone). Useful for
        canary runs and one-off repairs. ``None`` = every symbol in the
        universe library.

    Returns
    -------
    summary dict with the action plan and outcome.
    """
    s3 = boto3.client("s3")
    universe_lib = get_universe_lib(bucket)

    arctic_symbols = sorted(universe_lib.list_symbols())
    log.info("ArcticDB universe holds %d symbols", len(arctic_symbols))

    if tickers_override is not None:
        targets = sorted(set(tickers_override) & set(arctic_symbols))
        ignored = sorted(set(tickers_override) - set(arctic_symbols))
        if ignored:
            log.warning(
                "Skipping %d tickers from --tickers override that aren't in "
                "ArcticDB: %s",
                len(ignored), ignored,
            )
    else:
        targets = arctic_symbols

    migrated: list[dict] = []
    already_canonical: list[str] = []
    errors: list[dict] = []

    workers = int(
        os.environ.get("MIGRATE_UNIVERSE_FEATURE_ORDER_WORKERS", str(DEFAULT_WORKERS))
    )

    def _migrate_one(ticker: str) -> dict:
        try:
            df = universe_lib.read(ticker).data
        except Exception as exc:
            return {"ticker": ticker, "outcome": "read_error", "error": str(exc)}

        existing_cols = list(df.columns)
        if _is_canonical(existing_cols):
            return {"ticker": ticker, "outcome": "already_canonical"}

        canonical = _canonical_column_order(existing_cols)
        df = df[canonical]

        # Per-ticker diff: log the first 3 positional mismatches so the
        # audit shows the actual scrambling, not just "reordered".
        diffs = [
            (i, existing_cols[i], canonical[i])
            for i in range(min(len(existing_cols), len(canonical)))
            if existing_cols[i] != canonical[i]
        ][:3]

        record = {
            "ticker": ticker,
            "outcome": "migrated",
            "rows": len(df),
            "n_cols": len(canonical),
            "first_diffs": diffs,
        }
        if apply:
            try:
                universe_lib.write(ticker, df, prune_previous_versions=True)
            except Exception as exc:
                return {"ticker": ticker, "outcome": "write_error", "error": str(exc)}
        return record

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_migrate_one, targets))
    elapsed = time.time() - t0
    log.info(
        "Threadpooled migration: %d targets in %.1fs (workers=%d)",
        len(targets), elapsed, workers,
    )

    for r in results:
        outcome = r["outcome"]
        if outcome == "already_canonical":
            already_canonical.append(r["ticker"])
        elif outcome == "migrated":
            log_prefix = "MIGRATED" if apply else "DRY-RUN would migrate"
            log.info(
                "%s ticker=%s rows=%d n_cols=%d first_diffs=%s",
                log_prefix, r["ticker"], r["rows"], r["n_cols"], r["first_diffs"],
            )
            migrated.append(r)
        elif outcome == "read_error":
            log.error("Could not read %s: %s", r["ticker"], r["error"])
            errors.append({"ticker": r["ticker"], "stage": "read", "error": r["error"]})
        elif outcome == "write_error":
            log.error("Failed to write %s: %s", r["ticker"], r["error"])
            errors.append({"ticker": r["ticker"], "stage": "write", "error": r["error"]})
        else:
            raise RuntimeError(f"unexpected outcome={outcome!r} for {r['ticker']}")

    summary = {
        "status": "ok" if not errors else "partial",
        "applied": apply,
        "arctic_universe_size": len(arctic_symbols),
        "targets_count": len(targets),
        "migrated_count": len(migrated),
        "already_canonical_count": len(already_canonical),
        "errors_count": len(errors),
        "elapsed_seconds": round(elapsed, 1),
        "workers": workers,
        "migrated": migrated,
        "already_canonical": already_canonical,
        "errors": errors,
    }

    log.info(
        "migrate_universe_feature_order: applied=%s targets=%d migrated=%d "
        "already_canonical=%d errors=%d elapsed=%.1fs workers=%d",
        apply, len(targets), len(migrated), len(already_canonical),
        len(errors), elapsed, workers,
    )

    _write_audit(s3, bucket, summary)

    return summary


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually rewrite. Default dry-run.",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated subset of tickers to migrate (default: all).",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET})",
    )
    args = parser.parse_args()

    tickers_override = (
        [t.strip() for t in args.tickers.split(",") if t.strip()]
        if args.tickers
        else None
    )

    result = migrate_universe_feature_order(
        bucket=args.bucket,
        apply=args.apply,
        tickers_override=tickers_override,
    )
    print(json.dumps(result, indent=2, default=str))
    if result["errors_count"] > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
