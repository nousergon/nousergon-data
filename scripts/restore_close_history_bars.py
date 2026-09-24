#!/usr/bin/env python3
"""scripts/restore_close_history_bars.py — put back close_history bars a refetch dropped.

Origin: alpha-engine-config-I11556. D21 (``collectors/metron_market_data.py::
collect_history``) rebuilt every series from its sources on each run, so when
yfinance's store lost the 2026-09-22 session, the next publish dropped it. The
collector now carries such bars forward itself; this script puts back bars that
were dropped BEFORE that fix, from an older S3 version of the consolidated
artifact.

It invents no merge rule of its own. It runs the collector's
:func:`carry_forward_interior_bars` with the LIVE series as the fresh side and
the chosen source version as the previous publish, so a restored bar obeys the
rules the collector applies every day: interior sessions only, a live bar is
never overwritten, and each carried close is rescaled onto the live basis (a
carry is refused when a corporate action makes that ambiguous).

What an ``--apply`` run writes, through the collector's own put site:

  1. ``market_data/close_history/consolidated.json`` — the live document with
     the named symbols' series replaced, conditional on the ETag it read, so a
     D21 publish that lands in between is never overwritten;
  2. ``market_data/close_history/{SYM}.json`` for every symbol that gained a
     bar, with only ``closes`` replaced.

It then reads the consolidated artifact back and fails unless every
``--expect-date`` is present for every named symbol.

Without ``--apply`` it is read-only and prints the plan.

Exit codes: 0 = done (or, dry run, the plan is complete); 1 = a carry was
refused, a named symbol is absent from either version, or an ``--expect-date``
would still be missing — nothing is written in that case; 2 = the read-back
after writing did not show the restored bars.

Usage::

    python -m scripts.restore_close_history_bars \\
        --source-version-id .i1Erw0YTjMmfyFlRxfKOOTXq5bO0m8e \\
        --symbols VOO,ASML,MELI --expect-date 2026-09-22 [--apply]

Do not run it while D21 is publishing (weekdays ~20:05-20:30 UTC). The ETag
condition refuses the write in that case rather than losing either side, and
re-running afterwards is safe: a bar already present is never carried again.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from collectors import metron_market_data as mmd

log = logging.getLogger(__name__)


def _series_of(doc: dict, sym: str) -> list[tuple[str, float]]:
    return [(str(d), float(c)) for d, c in doc["series"][sym]]


def plan_restore(
    live_doc: dict, source_doc: dict, symbols: list[str], expect_dates: list[str],
) -> dict:
    """Pure: what the restore would write, and whether it may write it."""
    missing_live = [s for s in symbols if s not in live_doc["series"]]
    missing_source = [s for s in symbols if s not in source_doc["series"]]
    present = [s for s in symbols if s not in missing_live and s not in missing_source]
    fresh = {s: _series_of(live_doc, s) for s in present}
    previous = {s: source_doc["series"][s] for s in present}
    merged, carried, refused = mmd.carry_forward_interior_bars(fresh, previous)
    still_missing = [
        {"symbol": s, "date": d}
        for s in present
        for d in expect_dates
        if d not in {bar_date for bar_date, _c in merged[s]}
    ]
    changed = sorted({c["symbol"] for c in carried})
    return {
        "carried": carried,
        "refused": refused,
        "missing_from_live": missing_live,
        "missing_from_source": missing_source,
        "still_missing_expected": still_missing,
        "changed_symbols": changed,
        "merged": {s: merged[s] for s in changed},
        "ok": not (refused or missing_live or missing_source or still_missing),
    }


def _get(s3: Any, bucket: str, key: str, version_id: str | None = None) -> tuple[dict, str, str]:
    kwargs = {"Bucket": bucket, "Key": key}
    if version_id:
        kwargs["VersionId"] = version_id
    obj = s3.get_object(**kwargs)
    return json.loads(obj["Body"].read()), obj.get("ETag", ""), obj.get("VersionId", "")


def apply_restore(s3: Any, bucket: str, live_doc: dict, live_etag: str, plan: dict) -> None:
    """Write the consolidated artifact (ETag-conditional), then each changed per-symbol file."""
    doc = dict(live_doc)
    doc["series"] = dict(live_doc["series"])
    for sym, series in plan["merged"].items():
        doc["series"][sym] = [list(p) for p in series]
    mmd._write_json(s3, bucket, mmd.CONSOLIDATED_CLOSE_HISTORY_KEY, doc, if_match=live_etag)
    for sym, series in plan["merged"].items():
        key = f"{mmd.CLOSE_HISTORY_PREFIX}{sym}.json"
        per_symbol, _etag, _vid = _get(s3, bucket, key)
        per_symbol["closes"] = [list(p) for p in series]
        mmd._write_json(s3, bucket, key, per_symbol)


def verify(s3: Any, bucket: str, symbols: list[str], expect_dates: list[str]) -> list[dict]:
    """Read back the consolidated artifact; return every expected bar still absent."""
    doc, _etag, _vid = _get(s3, bucket, mmd.CONSOLIDATED_CLOSE_HISTORY_KEY)
    return [
        {"symbol": s, "date": d}
        for s in symbols
        for d in expect_dates
        if d not in {str(bar[0]) for bar in doc["series"].get(s, [])}
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.restore_close_history_bars", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--bucket", default=mmd.DEFAULT_BUCKET)
    parser.add_argument("--source-version-id", required=True,
                        help="S3 VersionId of consolidated.json to restore bars from")
    parser.add_argument("--symbols", required=True, help="comma-separated yf symbols")
    parser.add_argument("--expect-date", action="append", default=[],
                        help="a session every named symbol must carry afterwards (repeatable)")
    parser.add_argument("--apply", action="store_true", help="write; without it the run is read-only")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    symbols = sorted({s.strip() for s in args.symbols.split(",") if s.strip()})
    import boto3

    s3 = boto3.client("s3")
    live_doc, live_etag, live_vid = _get(s3, args.bucket, mmd.CONSOLIDATED_CLOSE_HISTORY_KEY)
    source_doc, _etag, _vid = _get(
        s3, args.bucket, mmd.CONSOLIDATED_CLOSE_HISTORY_KEY, args.source_version_id,
    )
    plan = plan_restore(live_doc, source_doc, symbols, args.expect_date)
    report = {k: v for k, v in plan.items() if k != "merged"}
    report.update({"live_version_id": live_vid, "source_version_id": args.source_version_id,
                   "apply": args.apply})
    print(json.dumps(report, indent=2, default=str))
    if not plan["ok"]:
        log.error("plan is incomplete (see refused / missing_* / still_missing_expected) — nothing written")
        return 1
    if not args.apply:
        log.info("dry run: %d bar(s) across %d symbol(s) would be restored; re-run with --apply",
                 len(plan["carried"]), len(plan["changed_symbols"]))
        return 0
    apply_restore(s3, args.bucket, live_doc, live_etag, plan)
    absent = verify(s3, args.bucket, symbols, args.expect_date)
    if absent:
        log.error("read-back after the write is still missing: %s", absent)
        return 2
    log.info("restored %d bar(s) across %d symbol(s); read-back shows every expected bar",
             len(plan["carried"]), len(plan["changed_symbols"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
