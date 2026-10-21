"""Point-in-time fails-to-deliver from the SEC's half-monthly CNS fails files.

Why this exists: this is the second free short-side signal for the model zoo's
W7 family, next to FINRA's daily short-sale volume
(`collectors/short_volume_pit.py`). FINRA's free short-interest history only
starts 2021-06. The SEC's fails-to-deliver files go back to 2004.

Source: one zip per half month, published by the SEC's Division of Trading and
Markets and served from `DATASET_URLS`, tried in order with a 404 moving on to
the next. The `a` file covers settlement dates on the 1st to the 15th and the
`b` file the 16th to month end. Each zip holds one pipe-delimited text file.
Some zips name it with no `.txt` extension, so any single member is read.

Layout: `SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE`,
latin-1 text. The header comes first and trailer lines may follow. A PRICE of
"." means unavailable and is kept as null. The quantity is the CUMULATIVE
outstanding fails on that settlement date, not new fails that day.

Knowledge rule: the files carry no publication time. The SEC says the first
half of a month is available at the end of the month, and the second half
around the 15th of the next month. Each row therefore carries a conservative
`known_at`: the 7th of the next month for an `a` file, and the 22nd of the
next month for a `b` file, both a week past the SEC's stated schedule. A
consumer may use a row for session S only when `known_at < S`. The zip's HTTP
Last-Modified is recorded in the run record as an observation. It is not used
as `known_at`, because a re-upload would make it later than the real
publication and push history out of the windows it belongs to.

Output, one parquet per half month, rewritten whole:

    fundamentals_pit/fails_to_deliver/v1/periods/{YYYYMM}{a|b}.parquet
    fundamentals_pit/fails_to_deliver/v1/runs/{run_date}/{run_id}.json  (+ runs/latest.json)

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
import zipfile
from collections.abc import Iterable, Sequence
from typing import Any

import pandas as pd

from collectors.edgar_pit_fundamentals import S3ObjectStore, SecClient, SecFetchError

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/fails_to_deliver/v1"
PERIODS_PREFIX = f"{DATASET_PREFIX}/periods/"
RUNS_PREFIX = f"{DATASET_PREFIX}/runs/"
RUN_LATEST_KEY = f"{RUNS_PREFIX}latest.json"
DEFAULT_BUCKET = "alpha-engine-research"

DATASET_URLS: tuple[str, ...] = (
    "https://www.sec.gov/files/data/fails-deliver-data/cnsfails{yyyymm}{half}.zip",
    "https://www.sec.gov/files/data/other/fails-deliver-data/cnsfails{yyyymm}{half}.zip",
)

HEADER = (
    "SETTLEMENT DATE",
    "CUSIP",
    "SYMBOL",
    "QUANTITY (FAILS)",
    "DESCRIPTION",
    "PRICE",
)
#: Lines that are not data (other than the header and trailer), as a share of
#: the file's data lines, before the file is refused as a format change.
#: Descriptions are known to carry a stray "|" (e.g. "BAM| ENTMT INC"), so a
#: line with SEVEN fields is repaired by re-joining the description.
MAX_MALFORMED_FRACTION = 0.001
#: Days after the SEC's stated schedule before a half month counts as known.
KNOWN_AT_DAY = {"a": 7, "b": 22}

ROW_COLUMNS: tuple[str, ...] = (
    "settlement_date",
    "cusip",
    "symbol",
    "quantity",
    "description",
    "price",
    "period",
    "known_at",
    "schema_version",
)


class SourceFormatError(RuntimeError):
    """The SEC's file is not the layout this collector reads."""


Period = tuple[int, int, str]  # (year, month, "a" | "b")


def period_label(period: Period) -> str:
    year, month, half = period
    return f"{year:04d}{month:02d}{half}"


def period_key(period: Period) -> str:
    return f"{PERIODS_PREFIX}{period_label(period)}.parquet"


def periods_between(first: Period, last: Period) -> list[Period]:
    out, (year, month, half) = [], first
    while (year, month, half) <= last:
        out.append((year, month, half))
        if half == "a":
            half = "b"
        else:
            half = "a"
            year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def parse_period(text: str) -> Period:
    text = text.strip().lower()
    if len(text) != 7 or text[-1] not in "ab" or not text[:6].isdigit():
        raise ValueError(f"{text!r} is not YYYYMMa or YYYYMMb")
    year, month = int(text[:4]), int(text[4:6])
    if not 1 <= month <= 12:
        raise ValueError(f"{text!r} is not YYYYMMa or YYYYMMb")
    return year, month, text[-1]


def known_at(period: Period) -> dt.date:
    year, month, half = period
    year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return dt.date(year, month, KNOWN_AT_DAY[half])


def period_bounds(period: Period) -> tuple[dt.date, dt.date]:
    year, month, half = period
    if half == "a":
        return dt.date(year, month, 1), dt.date(year, month, 15)
    nxt = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
    return dt.date(year, month, 16), nxt - dt.timedelta(days=1)


def last_known_period(today: dt.date) -> Period:
    """The newest half month whose `known_at` is before today."""
    year, month = today.year, today.month
    candidates: list[Period] = []
    for back in range(0, 3):
        y, m = year, month - back
        while m < 1:
            y, m = y - 1, m + 12
        candidates += [(y, m, "a"), (y, m, "b")]
    return max(p for p in candidates if known_at(p) < today)


# -- parsing -------------------------------------------------------------------


def _member_text(payload: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [n for n in archive.namelist() if not n.endswith("/")]
        if len(members) != 1:
            raise SourceFormatError(f"zip holds {len(members)} members, expected 1")
        return archive.read(members[0]).decode("latin-1")


def parse_file(text: str, period: Period) -> tuple[pd.DataFrame, int]:
    lines = [line.rstrip("\r") for line in text.splitlines() if line.strip()]
    if not lines:
        raise SourceFormatError(f"{period_label(period)}: empty file")
    header = tuple(field.strip() for field in lines[0].split("|"))
    if header != HEADER:
        raise SourceFormatError(
            f"{period_label(period)}: unexpected header {lines[0][:120]!r}"
        )
    records, malformed = [], 0
    for line in lines[1:]:
        raw = line.split("|")
        if len(raw) == 7:
            raw = raw[:4] + [raw[4] + "|" + raw[5]] + raw[6:]
        fields = [field.strip() for field in raw]
        if len(fields) != 6 or not fields[0].isdigit():
            # Trailer lines ("Trailer record count ...") land here too.
            malformed += 1
            continue
        records.append(fields)
    if malformed > max(2, MAX_MALFORMED_FRACTION * len(records)):
        raise SourceFormatError(
            f"{period_label(period)}: {malformed} of {len(lines) - 1} lines are not data"
        )
    frame = pd.DataFrame(records, columns=list(HEADER))
    settlement = pd.to_datetime(
        frame["SETTLEMENT DATE"], format="%Y%m%d", errors="coerce"
    )
    quantity = pd.to_numeric(frame["QUANTITY (FAILS)"], errors="coerce")
    bad = settlement.isna() | quantity.isna()
    if bad.sum() > max(2, MAX_MALFORMED_FRACTION * len(frame)):
        raise SourceFormatError(
            f"{period_label(period)}: {int(bad.sum())} rows have an unreadable date or quantity"
        )
    first, last = period_bounds(period)
    out = pd.DataFrame(
        {
            "settlement_date": settlement.dt.date,
            "cusip": frame["CUSIP"].astype("string"),
            "symbol": frame["SYMBOL"].str.upper().astype("string"),
            "quantity": quantity.astype("float64"),
            "description": frame["DESCRIPTION"].astype("string"),
            "price": pd.to_numeric(frame["PRICE"].replace(".", None), errors="coerce"),
            "period": period_label(period),
            "known_at": known_at(period),
            "schema_version": SCHEMA_VERSION,
        }
    )[~bad.to_numpy()]
    outside = out[(out["settlement_date"] < first) | (out["settlement_date"] > last)]
    if not outside.empty:
        raise SourceFormatError(
            f"{period_label(period)}: {len(outside)} row(s) settle outside {first}..{last}"
        )
    out = out.sort_values(["settlement_date", "cusip"]).reset_index(drop=True)
    return out[list(ROW_COLUMNS)], malformed + int(bad.sum())


# -- run -----------------------------------------------------------------------


def fetch_period(client: Any, period: Period) -> tuple[str, bytes, str | None]:
    """(serving URL, zip bytes, Last-Modified) from the first URL that does not 404."""
    year, month, half = period
    missing: SecFetchError | None = None
    for template in DATASET_URLS:
        url = template.format(yyyymm=f"{year:04d}{month:02d}", half=half)
        try:
            response = client.get(url, timeout=300.0)
        except SecFetchError as error:
            if error.status != 404:
                raise
            missing = error
            continue
        headers = getattr(response, "headers", {}) or {}
        return url, response.content, headers.get("Last-Modified")
    assert missing is not None
    raise SecFetchError(
        f"{period_label(period)}: HTTP 404 from every URL ({missing})", status=404
    ) from missing


def _to_parquet(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, compression="zstd")
    return buffer.getvalue()


def run(
    store: Any,
    client: Any,
    periods: Iterable[Period],
    *,
    run_date: dt.date,
    overwrite: bool,
    newest_may_be_unpublished: bool,
    run_id: str | None = None,
) -> dict[str, Any]:
    periods = list(periods)
    run_id = run_id or uuid.uuid4().hex[:12]
    existing = set(store.list_keys(PERIODS_PREFIX))
    summary: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "periods": {},
        "periods_written": 0,
        "periods_skipped_existing": 0,
        "periods_unpublished": [],
        "rows_written": 0,
    }
    for period in periods:
        label, key = period_label(period), period_key(period)
        if key in existing and not overwrite:
            summary["periods_skipped_existing"] += 1
            continue
        try:
            url, payload, last_modified = fetch_period(client, period)
        except SecFetchError as error:
            if (
                newest_may_be_unpublished
                and period == periods[-1]
                and error.status == 404
            ):
                logger.info("%s is not published yet; skipped", label)
                summary["periods_unpublished"].append(label)
                continue
            raise
        frame, malformed = parse_file(_member_text(payload), period)
        store.put_bytes(key, _to_parquet(frame), "application/octet-stream")
        summary["periods"][label] = {
            "rows": len(frame),
            "lines_not_data": malformed,
            "source_url": url,
            "last_modified": last_modified,
            "known_at": known_at(period).isoformat(),
        }
        summary["periods_written"] += 1
        summary["rows_written"] += len(frame)
        logger.info("%s: %d rows", label, len(frame))
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    body = json.dumps(summary, indent=2, sort_keys=True).encode()
    store.put_bytes(
        f"{RUNS_PREFIX}{run_date.isoformat()}/{run_id}.json", body, "application/json"
    )
    store.put_bytes(RUN_LATEST_KEY, body, "application/json")
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.fails_to_deliver_pit",
        description="Point-in-time fails-to-deliver from the SEC's half-monthly files.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    backfill = sub.add_parser(
        "backfill", help="every half month from --start to the newest known one"
    )
    backfill.add_argument(
        "--start", required=True, help="first half month, e.g. 201601a"
    )
    backfill.add_argument(
        "--end", help="last half month (default: the newest known one)"
    )
    backfill.add_argument("--overwrite", action="store_true")
    sub.add_parser("incremental", help="the newest three half months, rewritten")
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
    newest = last_known_period(today)
    if args.mode == "backfill":
        end = parse_period(args.end) if args.end else newest
        periods = periods_between(parse_period(args.start), end)
        overwrite, lenient = args.overwrite, args.end is None
    else:
        periods = periods_between((newest[0] - 1, 1, "a"), newest)[-3:]
        overwrite, lenient = True, True

    import boto3

    summary = run(
        S3ObjectStore(boto3.client("s3"), args.bucket),
        SecClient(),
        periods,
        run_date=today,
        overwrite=overwrite,
        newest_may_be_unpublished=lenient,
    )
    logger.info(
        "run %s: %d period(s) written, %d skipped, %d unpublished, %d rows",
        summary["run_id"],
        summary["periods_written"],
        summary["periods_skipped_existing"],
        len(summary["periods_unpublished"]),
        summary["rows_written"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
