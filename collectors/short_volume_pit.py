"""Point-in-time daily short-sale volume from FINRA's Reg SHO daily files.

Why this exists: the model zoo's W7 family (short-side pressure) stalled
because FINRA's free short-INTEREST history only starts 2021-06, which leaves
window A (2018-2021) unscoreable. FINRA's Reg SHO daily short-sale VOLUME files
go back to 2009 and are free.
This is a different quantity: the share of each day's TRF/ADF-reported volume
that was a short sale, not the open short position. The consumer must not
read it as short interest.

Source: one pipe-delimited text file per trading day on FINRA's CDN.
- Preferred: the consolidated file, CNMSshvol{YYYYMMDD}.txt. It combines the
  Nasdaq TRF Carteret (Q), Nasdaq TRF Chicago (B), NYSE TRF (N) and ADF (D).
  Its `Market` column holds that day's facility union, such as "B,Q,N".
- Fallback, for days where the consolidated file 404s: the per-facility files
  (`FACILITY_FILES`), summed per symbol. The run record says which source
  served each day.

Layout (FINRA "Daily Short Sale Volume File Layout"):
`Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market`. A header line
comes first and a record-count trailer comes last. A day with no data still
ships, with only the header and trailer. Since 2026-02-23 NMS quantities can
carry six decimals, so volumes are floats.

Knowledge rule: FINRA publishes a trade date's file by 6 pm ET that day. A
consumer may use a row for session S only when `trade_date < S`, the same
strict inequality as the other PIT datasets.

Output, one parquet per calendar month, rewritten whole:

    fundamentals_pit/short_volume/v1/months/{YYYY-MM}.parquet
    fundamentals_pit/short_volume/v1/runs/{run_date}/{run_id}.json  (+ runs/latest.json)

Missing days: a trading day (NYSE calendar) with no file at any URL is
recorded under `dates_missing` and never filled. A month missing more than
`MAX_MISSING_DAYS_PER_MONTH` trading days stops the run. That many 404s is a
URL or layout change, not a gap.

PUBLIC REPO: logs carry counts only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import logging
import os
import uuid
from collections.abc import Iterable, Sequence
from typing import Any

import pandas as pd

from collectors.edgar_pit_fundamentals import S3ObjectStore, SecClient, SecFetchError

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/short_volume/v1"
MONTHS_PREFIX = f"{DATASET_PREFIX}/months/"
RUNS_PREFIX = f"{DATASET_PREFIX}/runs/"
RUN_LATEST_KEY = f"{RUNS_PREFIX}latest.json"
DEFAULT_BUCKET = "alpha-engine-research"

BASE_URL = "https://cdn.finra.org/equity/regsho/daily/{code}shvol{ymd}.txt"
CONSOLIDATED = "CNMS"
#: TRF / ADF facility files, summed when the consolidated file is absent.
#: The ORF (OTC) file is left out, as it is from the consolidated file.
FACILITY_FILES: tuple[str, ...] = ("FNSQ", "FNQC", "FNYX", "FNRA")

HEADER = ("Date", "Symbol", "ShortVolume", "ShortExemptVolume", "TotalVolume", "Market")
#: The pre-2011 layout had no ShortExemptVolume column.
HEADER_NO_EXEMPT = ("Date", "Symbol", "ShortVolume", "TotalVolume", "Market")

MAX_MISSING_DAYS_PER_MONTH = 3
#: Lines that are neither the header, data nor the trailer, as a share of a
#: file's data lines, before the file is refused as a format change.
MAX_MALFORMED_FRACTION = 0.001

ROW_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "symbol",
    "short_volume",
    "short_exempt_volume",
    "total_volume",
    "markets",
    "source",
    "schema_version",
)


class SourceFormatError(RuntimeError):
    """FINRA's file is not the layout this collector reads."""


# -- parsing -------------------------------------------------------------------


def _is_trailer(line: str) -> bool:
    return "|" not in line and line.replace(",", "").strip().isdigit()


def parse_file(text: str, trade_date: dt.date) -> tuple[pd.DataFrame, int]:
    """(rows, malformed line count) for one facility or consolidated file."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        raise SourceFormatError(f"{trade_date}: empty file")
    header = tuple(field.strip() for field in lines[0].split("|"))
    if header not in (HEADER, HEADER_NO_EXEMPT):
        raise SourceFormatError(f"{trade_date}: unexpected header {lines[0][:120]!r}")
    width = len(header)
    body = lines[1:]
    if body and _is_trailer(body[-1]):
        body = body[:-1]
    records, malformed = [], 0
    expected = trade_date.strftime("%Y%m%d")
    for line in body:
        fields = [field.strip() for field in line.split("|")]
        if len(fields) != width:
            malformed += 1
            continue
        if fields[0] != expected:
            raise SourceFormatError(
                f"{trade_date}: a row is dated {fields[0]!r}, not {expected}"
            )
        records.append(fields)
    if body and malformed > max(1, MAX_MALFORMED_FRACTION * len(body)):
        raise SourceFormatError(
            f"{trade_date}: {malformed} of {len(body)} lines do not split into {width} fields"
        )
    frame = pd.DataFrame(records, columns=list(header))
    if "ShortExemptVolume" not in frame:
        frame["ShortExemptVolume"] = None
    out = pd.DataFrame(
        {
            "symbol": frame["Symbol"].astype("string"),
            "short_volume": pd.to_numeric(frame["ShortVolume"], errors="coerce"),
            "short_exempt_volume": pd.to_numeric(
                frame["ShortExemptVolume"], errors="coerce"
            ),
            "total_volume": pd.to_numeric(frame["TotalVolume"], errors="coerce"),
            "markets": frame["Market"].astype("string"),
        }
    )
    bad = out["short_volume"].isna() | out["total_volume"].isna()
    if bad.sum() > max(1, MAX_MALFORMED_FRACTION * max(len(out), 1)):
        raise SourceFormatError(
            f"{trade_date}: {int(bad.sum())} rows have a non-numeric volume"
        )
    return out[~bad].reset_index(drop=True), malformed + int(bad.sum())


def combine_facilities(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Sum per-facility rows into one row per symbol, joining the market codes."""
    stacked = pd.concat(frames.values(), ignore_index=True)
    grouped = stacked.groupby("symbol", sort=True)
    out = grouped[["short_volume", "short_exempt_volume", "total_volume"]].sum(
        min_count=1
    )
    out["markets"] = grouped["markets"].agg(lambda m: ",".join(sorted(set(m))))
    return out.reset_index()


# -- fetching ------------------------------------------------------------------


def _get_text(client: Any, url: str) -> str | None:
    """The file body, or None on a 404. Any other failure raises."""
    try:
        response = client.get(url, timeout=120.0)
    except SecFetchError as error:
        if error.status in (403, 404):
            # The CDN answers 403 for a key that does not exist.
            return None
        raise
    return response.content.decode("latin-1")


def fetch_day(client: Any, trade_date: dt.date) -> tuple[pd.DataFrame, str, int] | None:
    """(rows, source label, malformed count) for one day, or None if no file exists."""
    ymd = trade_date.strftime("%Y%m%d")
    text = _get_text(client, BASE_URL.format(code=CONSOLIDATED, ymd=ymd))
    if text is not None:
        frame, malformed = parse_file(text, trade_date)
        return frame, CONSOLIDATED, malformed
    frames: dict[str, pd.DataFrame] = {}
    malformed = 0
    for code in FACILITY_FILES:
        text = _get_text(client, BASE_URL.format(code=code, ymd=ymd))
        if text is None:
            continue
        frame, bad = parse_file(text, trade_date)
        frames[code] = frame
        malformed += bad
    if not frames:
        return None
    return combine_facilities(frames), "+".join(sorted(frames)), malformed


# -- calendar ------------------------------------------------------------------


def trading_days(first: dt.date, last: dt.date) -> list[dt.date]:
    from nousergon_lib.trading_calendar import is_trading_day

    days, day = [], first
    while day <= last:
        if is_trading_day(day):
            days.append(day)
        day += dt.timedelta(days=1)
    return days


def month_label(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def month_key(year: int, month: int) -> str:
    return f"{MONTHS_PREFIX}{month_label(year, month)}.parquet"


def months_between(
    first: tuple[int, int], last: tuple[int, int]
) -> list[tuple[int, int]]:
    out, (year, month) = [], first
    while (year, month) <= last:
        out.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def parse_month(text: str) -> tuple[int, int]:
    try:
        year, month = (int(part) for part in text.strip().split("-"))
    except ValueError:
        raise ValueError(f"{text!r} is not YYYY-MM") from None
    if not 1 <= month <= 12:
        raise ValueError(f"{text!r} is not YYYY-MM")
    return year, month


def month_bounds(year: int, month: int) -> tuple[dt.date, dt.date]:
    first = dt.date(year, month, 1)
    nxt = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
    return first, nxt - dt.timedelta(days=1)


# -- run -----------------------------------------------------------------------


def collect_month(
    client: Any, year: int, month: int, *, through: dt.date
) -> tuple[pd.DataFrame, dict[str, Any]]:
    first, last = month_bounds(year, month)
    days = trading_days(first, min(last, through))
    frames, sources, missing, malformed = [], {}, [], 0
    for day in days:
        fetched = fetch_day(client, day)
        if fetched is None:
            missing.append(day.isoformat())
            continue
        frame, source, bad = fetched
        frame.insert(0, "trade_date", day)
        frame["source"] = source
        frames.append(frame)
        sources[source] = sources.get(source, 0) + 1
        malformed += bad
    label = month_label(year, month)
    if len(missing) > MAX_MISSING_DAYS_PER_MONTH:
        raise SourceFormatError(
            f"{label}: {len(missing)} of {len(days)} trading days have no file at any URL"
        )
    if not frames:
        raise SourceFormatError(f"{label}: no rows")
    out = pd.concat(frames, ignore_index=True)
    out["schema_version"] = SCHEMA_VERSION
    out["source"] = out["source"].astype("string")
    out = out.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    stats = {
        "trading_days": len(days),
        "days_written": len(frames),
        "dates_missing": missing,
        "sources": sources,
        "rows": len(out),
        "lines_malformed": malformed,
    }
    return out[list(ROW_COLUMNS)], stats


def _to_parquet(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, compression="zstd")
    return buffer.getvalue()


def run(
    store: Any,
    client: Any,
    months: Iterable[tuple[int, int]],
    *,
    run_date: dt.date,
    through: dt.date,
    overwrite: bool,
    run_id: str | None = None,
) -> dict[str, Any]:
    months = list(months)
    run_id = run_id or uuid.uuid4().hex[:12]
    existing = set(store.list_keys(MONTHS_PREFIX))
    summary: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "through": through.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "months": {},
        "months_written": 0,
        "months_skipped_existing": 0,
        "rows_written": 0,
    }
    for year, month in months:
        key = month_key(year, month)
        if key in existing and not overwrite:
            summary["months_skipped_existing"] += 1
            continue
        frame, stats = collect_month(client, year, month, through=through)
        store.put_bytes(key, _to_parquet(frame), "application/octet-stream")
        label = month_label(year, month)
        summary["months"][label] = stats
        summary["months_written"] += 1
        summary["rows_written"] += stats["rows"]
        logger.info(
            "%s: %d rows over %d day(s), %d missing",
            label,
            stats["rows"],
            stats["days_written"],
            len(stats["dates_missing"]),
        )
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    body = json.dumps(summary, indent=2, sort_keys=True).encode()
    store.put_bytes(
        f"{RUNS_PREFIX}{run_date.isoformat()}/{run_id}.json", body, "application/json"
    )
    store.put_bytes(RUN_LATEST_KEY, body, "application/json")
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.short_volume_pit",
        description="Point-in-time daily short-sale volume from FINRA's Reg SHO files.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    backfill = sub.add_parser(
        "backfill", help="every month from --start to the last complete one"
    )
    backfill.add_argument("--start", required=True, help="first month, e.g. 2016-01")
    backfill.add_argument("--end", help="last month (default: the last complete month)")
    backfill.add_argument("--overwrite", action="store_true")
    sub.add_parser(
        "incremental",
        help="the previous and current month, rewritten through yesterday",
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("NE_RESEARCH_BUCKET", DEFAULT_BUCKET)
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parser().parse_args(argv)
    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)
    this_month = (today.year, today.month)
    previous = (
        (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    )
    if args.mode == "backfill":
        end = parse_month(args.end) if args.end else previous
        months, overwrite = months_between(parse_month(args.start), end), args.overwrite
    else:
        months, overwrite = [previous, this_month], True

    import boto3

    summary = run(
        S3ObjectStore(boto3.client("s3"), args.bucket),
        SecClient(),
        months,
        run_date=today,
        through=yesterday,
        overwrite=overwrite,
    )
    logger.info(
        "run %s: %d month(s) written, %d skipped, %d rows",
        summary["run_id"],
        summary["months_written"],
        summary["months_skipped_existing"],
        summary["rows_written"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
