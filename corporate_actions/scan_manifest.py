"""Daily corporate-action scan manifest (alpha-engine-config-I11076, option A).

WHY THIS EXISTS:
    The trader's reconciler checks the broker's cash and positions against what
    the day's corporate actions should have done to them. To do that it needs a
    list of the actions that touch one trading day, and it needs to know that
    the list is COMPLETE. An empty list from a feed that did not answer and an
    empty list from a quiet day look the same, and the first one would let a
    missed dividend or split pass as reconciled.

    The registry ``sync`` path (``corporate_actions.detect_*``) degrades to
    ``[]`` on a failed or forbidden query by design, and reads only the first
    page, so it cannot carry that promise. This module can: every query is
    paginated to the end and every error raises, and the manifest is written
    ONLY after all of them succeed. The object's presence is the completeness
    claim. Its absence means "unknown", and the reader treats every held
    instrument as unchecked.

    Brian's ruling 2026-10-22: Polygon is the single source for prices and
    corporate actions, IB is the source for holdings and cash.

WHAT ONE MANIFEST COVERS:
    Trading day ``D``, over the calendar window ``(previous trading day, D]``,
    so an action dated on a weekend or holiday lands on the next session, which
    is the day the broker books it.

    * ``splits``: every split with ``execution_date`` in the window.
    * ``dividends_paid``: every cash dividend with ``pay_date`` in the window.
      This is the day cash arrives, for shares held at the record date.
    * ``dividends_ex``: every cash dividend going ex in the window. This is the
      day the price drops by the dividend, which matters for P&L attribution
      and for re-striking marks.
    * ``declared``: reviewed in-repo actions the vendor does not publish
      (``corporate_actions/declared.py``), such as spin-offs.

    Tickers are Polygon symbols (``BRK.B``). Mapping to broker symbols is the
    reader's job.

WHERE AND WHEN:
    ``s3://alpha-engine-research/corporate_actions/scans/{D}.json``. Written by
    MorningEnrich (~05:30 PT) for today's session and for the session it
    enriches, so the post-close reconcile of day ``D`` reads a scan taken that
    morning and re-taken the next. Overwriting is safe: a later scan of the same
    window only sees more of the vendor's record. Also runnable by hand::

        python -m corporate_actions.scan_manifest --date 2026-10-22 [--dry-run]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)

SCHEMA = "corporate_action_scan.v1"
KEY_TEMPLATE = "corporate_actions/scans/{day}.json"
DEFAULT_BUCKET = "alpha-engine-research"

__all__ = [
    "KEY_TEMPLATE",
    "SCHEMA",
    "build_scan",
    "morning_scan",
    "run",
    "run_for_sessions",
    "scan_key",
    "window_start",
    "write_scan",
]


def scan_key(day: str) -> str:
    return KEY_TEMPLATE.format(day=day)


def _as_date(day: str | date) -> date:
    return day if isinstance(day, date) else date.fromisoformat(str(day))


def window_start(day: str | date, *, is_trading_day=None) -> str:
    """First calendar day of ``D``'s window: the day after the prior session."""
    if is_trading_day is None:
        from nousergon_lib.trading_calendar import is_trading_day
    d = _as_date(day)
    candidate = d - timedelta(days=1)
    for _ in range(15):
        if is_trading_day(candidate):
            return (candidate + timedelta(days=1)).isoformat()
        candidate -= timedelta(days=1)
    raise RuntimeError(f"no trading day within 15 days before {d}")


def _declared_in_window(start: str, end: str) -> list[dict]:
    from corporate_actions.declared import DECLARED_SPINOFFS

    rows = []
    for d in DECLARED_SPINOFFS:
        if start <= d.ex_date <= end:
            rows.append(
                {
                    "kind": "spinoff",
                    "ticker": d.ticker,
                    "spun_ticker": d.spun_ticker,
                    "ex_date": d.ex_date,
                    "distribution_ratio": d.distribution_ratio,
                    "price_factor": d.price_factor,
                    "sources": list(d.sources),
                }
            )
    return rows


def build_scan(client, day: str, *, start: str | None = None) -> dict:
    """Query Polygon for ``day``'s window. Raises on ANY query failure."""
    day = _as_date(day).isoformat()
    start = start or window_start(day)
    splits = client.get_all_splits_between(start, day)
    paid = client.get_all_dividends_between(start, day, date_field="pay_date")
    ex = client.get_all_dividends_between(start, day, date_field="ex_dividend_date")
    body = {
        "schema": SCHEMA,
        "trading_day": day,
        "window_start": start,
        "window_end": day,
        "source": "polygon",
        "complete": True,
        "splits": splits,
        "dividends_paid": paid,
        "dividends_ex": ex,
        "declared": _declared_in_window(start, day),
    }
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    body["content_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
    body["generated_at"] = datetime.now(timezone.utc).isoformat()
    return body


def write_scan(s3, bucket: str, scan: dict) -> str:
    key = scan_key(scan["trading_day"])
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(scan, sort_keys=True, indent=1, default=str).encode(),
        ContentType="application/json",
    )
    return key


def run(day: str, *, bucket: str = DEFAULT_BUCKET, client=None, s3=None,
        dry_run: bool = False) -> dict:
    """Build and write one day's scan. Raises if any query fails."""
    if client is None:
        from polygon_client import polygon_client

        client = polygon_client()
    scan = build_scan(client, day)
    summary = {
        "trading_day": scan["trading_day"],
        "window": [scan["window_start"], scan["window_end"]],
        "splits": len(scan["splits"]),
        "dividends_paid": len(scan["dividends_paid"]),
        "dividends_ex": len(scan["dividends_ex"]),
        "declared": len(scan["declared"]),
        "content_sha256": scan["content_sha256"],
    }
    if dry_run:
        summary["status"] = "ok_dry_run"
        return summary
    if s3 is None:
        import boto3

        s3 = boto3.client("s3")
    summary["key"] = write_scan(s3, bucket, scan)
    summary["status"] = "ok"
    logger.info("corporate-action scan written: %s", summary)
    return summary


def run_for_sessions(days, *, bucket: str = DEFAULT_BUCKET, dry_run: bool = False,
                     client=None, s3=None) -> dict:
    """Scan each session in ``days`` independently, for MorningEnrich.

    One day's failure does not stop the other, and nothing is written for a day
    whose queries did not all succeed. Each failure is logged at ERROR and
    returned with its error text, so the caller can record it. A missing scan is
    surfaced downstream by the reader, which marks every holding unchecked.
    """
    out: dict = {}
    for day in sorted({_as_date(d).isoformat() for d in days if d}):
        try:
            out[day] = run(day, bucket=bucket, client=client, s3=s3, dry_run=dry_run)
        except Exception as exc:  # noqa: BLE001 - recorded loud, per day
            logger.error(
                "corporate-action scan for %s FAILED, no manifest written: %s",
                day, exc, exc_info=True,
            )
            out[day] = {"status": "error", "error": str(exc)}
    return out


def morning_scan(bucket: str, enrich_target: str | None, *, dry_run: bool = False,
                 now: datetime | None = None, is_trading_day=None) -> dict:
    """MorningEnrich's hook: scan today's session (if any) and the enriched one.

    Today's scan is what the same evening's post-close reconcile reads; the
    enriched session's scan is a second look at yesterday with one more day of
    vendor publication behind it. Never raises.
    """
    from zoneinfo import ZoneInfo

    try:
        if is_trading_day is None:
            from nousergon_lib.trading_calendar import is_trading_day
        now = now or datetime.now(timezone.utc)
        today = now.astimezone(ZoneInfo("America/New_York")).date()
        days = [enrich_target]
        if is_trading_day(today):
            days.append(today.isoformat())
    except Exception as exc:  # noqa: BLE001 - recorded loud
        logger.error("corporate-action scan: could not resolve sessions: %s", exc,
                     exc_info=True)
        return {"status": "error", "error": str(exc)}
    per_day = run_for_sessions(days, bucket=bucket, dry_run=dry_run)
    ok = all(r.get("status") in ("ok", "ok_dry_run") for r in per_day.values())
    return {"status": "ok" if ok else "error", "per_day": per_day}


def _main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--date", required=True, help="trading day YYYY-MM-DD")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    summary = run(args.date, bucket=args.bucket, dry_run=args.dry_run)
    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
