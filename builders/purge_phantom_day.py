"""purge_phantom_day — remove a fabricated non-trading-day row fleet-wide.

config#1572: 2026-06-19 (Juneteenth, NYSE closed) entered the daily-closes
archive as a fabricated 924-row yfinance-sourced parquet — the weekday-only
window enumeration in ``collectors.daily_closes._previous_business_days``
included the holiday, the yfinance batch returned data anyway, and the
Saturday backfill's daily delta then propagated a 2026-06-19 row into every
ArcticDB universe/macro symbol. A phantom session distorts every
rolling-window feature that crosses it and, around a corporate action, can
carry a wrong-basis value that later corroborates a bad restatement (the
2026-07-02 HON incident chain).

This is the one-shot inverse: for a NAMED non-trading date, drop that row
from every universe + macro symbol and delete the staging parquet. Dry-run by
default; ``--apply`` performs the writes. Symbols without the row are
untouched (no rewrite, no version churn).

Usage (Mac: import arcticdb before boto3 — see the CRSP recipe):
    python -c "import arcticdb; import sys; \
        sys.argv=['p','--date','2026-06-19','--apply']; \
        from builders.purge_phantom_day import main; main()"
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date as _date

import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
_PARQUET_KEY_TEMPLATE = "staging/daily_closes/{date}.parquet"


def _purge_library(lib, target_ts: pd.Timestamp, apply: bool) -> dict:
    """Drop ``target_ts``'s row from every symbol in ``lib`` that has it.

    Full-series read → drop → write (with prune) per affected symbol —
    ArcticDB's update() cannot delete a row, so the rewrite is the correct
    primitive (same one daily_append's backfill branch uses). Returns
    ``{"affected": [...], "clean": int, "errors": [(symbol, err), ...]}``.
    """
    affected: list[str] = []
    errors: list[tuple[str, str]] = []
    clean = 0
    symbols = lib.list_symbols()
    for i, symbol in enumerate(symbols):
        try:
            df = lib.read(symbol).data
            if target_ts not in df.index:
                clean += 1
                continue
            affected.append(symbol)
            if apply:
                out = df.drop(index=target_ts)
                lib.write(symbol, out, prune_previous_versions=True)
        except Exception as exc:  # noqa: BLE001 - collected + raised at exit
            errors.append((symbol, str(exc)))
            log.error("purge_phantom_day: %s failed: %s", symbol, exc)
        if (i + 1) % 100 == 0:
            log.info(
                "purge_phantom_day: %d/%d symbols scanned (%d affected so far)",
                i + 1, len(symbols), len(affected),
            )
    return {"affected": affected, "clean": clean, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove a fabricated non-trading-day row from ArcticDB + the archive"
    )
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (must be a NON-trading day)")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--apply", action="store_true",
        help="Perform the writes/deletes (default: dry-run report only)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    from nousergon_lib.trading_calendar import is_trading_day

    target = _date.fromisoformat(args.date)
    # Inverse sanity gate: purging a REAL session would destroy good data.
    if is_trading_day(target):
        raise SystemExit(
            f"{args.date} IS an NYSE trading day — refusing to purge a real "
            f"session. This tool only removes fabricated non-trading-day rows "
            f"(config#1572)."
        )

    import boto3

    from store.arctic_store import get_macro_lib, get_universe_lib

    target_ts = pd.Timestamp(args.date)
    mode = "APPLY" if args.apply else "DRY-RUN"
    log.info("purge_phantom_day %s: date=%s bucket=%s", mode, args.date, args.bucket)

    results = {}
    for name, lib in (
        ("universe", get_universe_lib(args.bucket)),
        ("macro", get_macro_lib(args.bucket)),
    ):
        res = _purge_library(lib, target_ts, apply=args.apply)
        results[name] = res
        log.info(
            "purge_phantom_day %s [%s]: %d affected, %d clean, %d errors%s",
            mode, name, len(res["affected"]), res["clean"], len(res["errors"]),
            "" if args.apply or not res["affected"] else
            f" — would purge: {', '.join(res['affected'][:8])}"
            f"{'…' if len(res['affected']) > 8 else ''}",
        )

    key = _PARQUET_KEY_TEMPLATE.format(date=args.date)
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=args.bucket, Key=key)
        parquet_exists = True
    except Exception:  # noqa: BLE001 - 404 ⇒ nothing to delete
        parquet_exists = False
    if parquet_exists and args.apply:
        s3.delete_object(Bucket=args.bucket, Key=key)
        log.info("purge_phantom_day: deleted s3://%s/%s", args.bucket, key)
    elif parquet_exists:
        log.info("purge_phantom_day DRY-RUN: would delete s3://%s/%s", args.bucket, key)
    else:
        log.info("purge_phantom_day: no parquet at s3://%s/%s", args.bucket, key)

    all_errors = [e for res in results.values() for e in res["errors"]]
    if all_errors:
        raise SystemExit(
            f"purge_phantom_day: {len(all_errors)} symbol(s) failed — re-run "
            f"to converge (idempotent); first: {all_errors[0]}"
        )
    total_affected = sum(len(res["affected"]) for res in results.values())
    print(
        f"purge_phantom_day {mode} complete: {total_affected} symbol row(s) "
        f"{'purged' if args.apply else 'would be purged'} for {args.date}; "
        f"parquet {'deleted' if (parquet_exists and args.apply) else ('present (dry-run)' if parquet_exists else 'absent')}"
    )


if __name__ == "__main__":
    sys.exit(main())
