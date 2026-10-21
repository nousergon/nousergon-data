"""Point-in-time institutional holdings from the SEC's Form 13F data sets.

Why this exists: the model zoo asked for institutional-ownership history
(breadth, changes, crowding) with a known-at date and coverage from 2017. The
fleet's earlier 13F path (`collectors/alternative.py`, per-ticker edgartools
calls) was deprecated 2026-07-13 and never kept history.

Source: the SEC's "Form 13F Data Sets" -- every 13F-HR / 13F-NT (and
amendment) EDGAR accepted in a window, flattened to TSV and zipped. The
windows were calendar quarters (`2023q4_form13f.zip`) until 2024 and became
rolling three-month windows (`01dec2024-28feb2025_form13f.zip`) after. The
collector does not guess the names: it reads the SEC's own listing page
(`LISTING_URL`) and takes every `*_form13f.zip` link on it.

Rows: one per information-table line (one security held by one filer in one
report), joined to its submission and cover page:
    accession, filing_date, period_of_report, filer_cik, submission_type,
    is_amendment, amendment_type, cusip, issuer_name, title_of_class,
    value_usd, shares, shares_type, put_call, investment_discretion
`value_usd` is normalised. Filings made before 2023-01-03 report VALUE in
thousands of dollars and later ones in dollars (the SEC's 13F amendments), so
earlier values are multiplied by 1,000. The raw value is kept as `value_raw`.

Knowledge rule: a row is known as of its EDGAR `filing_date`. Use it for
session S only when `filing_date < S`. The report's `period_of_report` is
usually 45 days earlier and must never be used as the knowledge time.
Amendments are kept as their own rows with their own filing date. A consumer
that wants the "as known on S" holding takes, per (filer, period), the newest
filing filed before S, and must read `amendment_type` ("RESTATEMENT" replaces,
"NEW HOLDINGS" adds).

Output, one parquet per SEC archive, rewritten whole:

    fundamentals_pit/holdings_13f/v1/archives/{label}.parquet
    fundamentals_pit/holdings_13f/v1/runs/{run_date}/{run_id}.json  (+ runs/latest.json)

Rows with an unparseable FILING_DATE or PERIOD_OF_REPORT are left out and
counted in the run record. More than `MAX_BAD_DATE_FRACTION` of an archive
stops the run as a format change.

PUBLIC REPO: logs carry counts only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import logging
import os
import re
import uuid
import zipfile
from collections.abc import Iterable, Sequence
from typing import Any

import pandas as pd

from collectors.edgar_pit_fundamentals import S3ObjectStore, SecClient

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/holdings_13f/v1"
ARCHIVES_PREFIX = f"{DATASET_PREFIX}/archives/"
RUNS_PREFIX = f"{DATASET_PREFIX}/runs/"
RUN_LATEST_KEY = f"{RUNS_PREFIX}latest.json"
DEFAULT_BUCKET = "alpha-engine-research"

LISTING_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
SEC_ROOT = "https://www.sec.gov"
_LINK = re.compile(r'href="([^"]*?([0-9a-z\-]+)_form13f\.zip)"', re.IGNORECASE)
_QUARTER = re.compile(r"^(\d{4})q([1-4])$")
_WINDOW = re.compile(r"^(\d{2}[a-z]{3}\d{4})-(\d{2}[a-z]{3}\d{4})$")

#: VALUE switched from thousands of dollars to dollars for filings made on or
#: after this date.
VALUE_IN_DOLLARS_FROM = dt.date(2023, 1, 3)
MAX_BAD_DATE_FRACTION = 0.001

REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "SUBMISSION.tsv": (
        "ACCESSION_NUMBER",
        "FILING_DATE",
        "SUBMISSIONTYPE",
        "CIK",
        "PERIODOFREPORT",
    ),
    "COVERPAGE.tsv": ("ACCESSION_NUMBER", "ISAMENDMENT", "AMENDMENTTYPE"),
    "INFOTABLE.tsv": (
        "ACCESSION_NUMBER",
        "NAMEOFISSUER",
        "TITLEOFCLASS",
        "CUSIP",
        "VALUE",
        "SSHPRNAMT",
        "SSHPRNAMTTYPE",
        "PUTCALL",
        "INVESTMENTDISCRETION",
    ),
}

ROW_COLUMNS: tuple[str, ...] = (
    "accession",
    "filing_date",
    "period_of_report",
    "filer_cik",
    "submission_type",
    "is_amendment",
    "amendment_type",
    "cusip",
    "issuer_name",
    "title_of_class",
    "value_usd",
    "value_raw",
    "shares",
    "shares_type",
    "put_call",
    "investment_discretion",
    "archive",
    "schema_version",
)


class SourceFormatError(RuntimeError):
    """The SEC's archive is not the layout this collector reads."""


# -- listing -------------------------------------------------------------------


def archive_span(label: str) -> tuple[dt.date, dt.date]:
    """The filing-date window an archive label names."""
    match = _QUARTER.match(label)
    if match:
        year, quarter = int(match.group(1)), int(match.group(2))
        first = dt.date(year, 3 * quarter - 2, 1)
        nxt = (
            dt.date(year + 1, 1, 1)
            if quarter == 4
            else dt.date(year, 3 * quarter + 1, 1)
        )
        return first, nxt - dt.timedelta(days=1)
    match = _WINDOW.match(label)
    if match:
        first, last = (dt.datetime.strptime(g, "%d%b%Y").date() for g in match.groups())
        return first, last
    raise SourceFormatError(f"unrecognised 13F archive label {label!r}")


def list_archives(html: str) -> list[tuple[str, str]]:
    """[(label, absolute URL)] for every 13F archive on the listing page, oldest first."""
    found: dict[str, str] = {}
    for href, label in _LINK.findall(html):
        label = label.lower()
        found[label] = href if href.startswith("http") else SEC_ROOT + href
    if not found:
        raise SourceFormatError("the 13F listing page links no *_form13f.zip archive")
    return sorted(found.items(), key=lambda item: archive_span(item[0]))


# -- parsing -------------------------------------------------------------------


def _read_member(archive: zipfile.ZipFile, name: str) -> pd.DataFrame:
    members = {
        member.rsplit("/", 1)[-1].upper(): member for member in archive.namelist()
    }
    if name.upper() not in members:
        raise SourceFormatError(f"archive has no {name} (members: {sorted(members)})")
    with archive.open(members[name.upper()]) as handle:
        # Only the needed columns: an INFOTABLE runs to millions of rows, and
        # the box has 4 GB.
        wanted = set(REQUIRED_COLUMNS[name])
        frame = pd.read_csv(
            handle,
            sep="\t",
            dtype=str,
            keep_default_na=False,
            na_values=[""],
            quoting=3,
            usecols=lambda column: column.strip().upper() in wanted,
        )
    frame.columns = [c.strip().upper() for c in frame.columns]
    missing = [c for c in REQUIRED_COLUMNS[name] if c not in frame.columns]
    if missing:
        raise SourceFormatError(f"{name} is missing required columns {missing}")
    return frame[list(REQUIRED_COLUMNS[name])]


def _dates(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.strip()
    parsed = pd.to_datetime(text, format="%d-%b-%Y", errors="coerce")
    iso = parsed.isna() & text.notna()
    if iso.any():
        parsed = parsed.where(
            ~iso, pd.to_datetime(text.where(iso), format="%Y-%m-%d", errors="coerce")
        )
    return parsed.dt.date


def parse_archive(payload: bytes, label: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        submission = _read_member(archive, "SUBMISSION.tsv")
        cover = _read_member(archive, "COVERPAGE.tsv")
        info = _read_member(archive, "INFOTABLE.tsv")
    submission = submission.assign(
        filing_date=_dates(submission["FILING_DATE"]),
        period_of_report=_dates(submission["PERIODOFREPORT"]),
    )
    bad = submission["filing_date"].isna() | submission["period_of_report"].isna()
    if bad.sum() > max(1, MAX_BAD_DATE_FRACTION * len(submission)):
        raise SourceFormatError(
            f"{label}: {int(bad.sum())} submissions have an unreadable date"
        )
    submission = submission[~bad]
    merged = info.merge(submission, on="ACCESSION_NUMBER", how="inner").merge(
        cover.drop_duplicates("ACCESSION_NUMBER"), on="ACCESSION_NUMBER", how="left"
    )
    value_raw = pd.to_numeric(merged["VALUE"], errors="coerce")
    in_thousands = merged["filing_date"] < VALUE_IN_DOLLARS_FROM
    out = pd.DataFrame(
        {
            "accession": merged["ACCESSION_NUMBER"].astype("string"),
            "filing_date": merged["filing_date"],
            "period_of_report": merged["period_of_report"],
            "filer_cik": pd.to_numeric(merged["CIK"], errors="coerce").astype("Int64"),
            "submission_type": merged["SUBMISSIONTYPE"]
            .str.strip()
            .str.upper()
            .astype("string"),
            "is_amendment": merged["ISAMENDMENT"]
            .fillna("")
            .str.strip()
            .str.upper()
            .eq("Y"),
            "amendment_type": merged["AMENDMENTTYPE"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "cusip": merged["CUSIP"].str.strip().str.upper().astype("string"),
            "issuer_name": merged["NAMEOFISSUER"].astype("string"),
            "title_of_class": merged["TITLEOFCLASS"].astype("string"),
            "value_usd": (
                value_raw * in_thousands.map({True: 1000.0, False: 1.0})
            ).astype("float64"),
            "value_raw": value_raw.astype("float64"),
            "shares": pd.to_numeric(merged["SSHPRNAMT"], errors="coerce").astype(
                "float64"
            ),
            "shares_type": merged["SSHPRNAMTTYPE"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "put_call": merged["PUTCALL"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "investment_discretion": merged["INVESTMENTDISCRETION"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "archive": label,
            "schema_version": SCHEMA_VERSION,
        }
    )
    first, last = archive_span(label)
    outside = out[(out["filing_date"] < first) | (out["filing_date"] > last)]
    if len(outside) > MAX_BAD_DATE_FRACTION * max(len(out), 1):
        raise SourceFormatError(
            f"{label}: {len(outside)} rows filed outside {first}..{last}"
        )
    out = out.sort_values(["filing_date", "accession", "cusip"]).reset_index(drop=True)
    stats = {
        "submissions": int(len(submission)),
        "submissions_bad_date": int(bad.sum()),
        "infotable_rows": int(len(info)),
        "rows": int(len(out)),
        "rows_unmatched": int(len(info) - len(out)),
        "rows_outside_window": int(len(outside)),
    }
    return out[list(ROW_COLUMNS)], stats


# -- run -----------------------------------------------------------------------


def archive_key(label: str) -> str:
    return f"{ARCHIVES_PREFIX}{label}.parquet"


def _to_parquet(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, compression="zstd")
    return buffer.getvalue()


def run(
    store: Any,
    client: Any,
    archives: Iterable[tuple[str, str]],
    *,
    run_date: dt.date,
    overwrite: bool,
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or uuid.uuid4().hex[:12]
    existing = set(store.list_keys(ARCHIVES_PREFIX))
    summary: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "archives": {},
        "archives_written": 0,
        "archives_skipped_existing": 0,
        "rows_written": 0,
    }
    for label, url in archives:
        key = archive_key(label)
        if key in existing and not overwrite:
            summary["archives_skipped_existing"] += 1
            continue
        frame, stats = parse_archive(client.get(url, timeout=900.0).content, label)
        store.put_bytes(key, _to_parquet(frame), "application/octet-stream")
        stats["source_url"] = url
        summary["archives"][label] = stats
        summary["archives_written"] += 1
        summary["rows_written"] += stats["rows"]
        logger.info(
            "%s: %d holdings rows from %d submissions",
            label,
            stats["rows"],
            stats["submissions"],
        )
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    body = json.dumps(summary, indent=2, sort_keys=True).encode()
    store.put_bytes(
        f"{RUNS_PREFIX}{run_date.isoformat()}/{run_id}.json", body, "application/json"
    )
    store.put_bytes(RUN_LATEST_KEY, body, "application/json")
    return summary


def select_archives(
    listed: list[tuple[str, str]], start: dt.date
) -> list[tuple[str, str]]:
    return [(label, url) for label, url in listed if archive_span(label)[1] >= start]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.holdings_13f_pit",
        description="Point-in-time 13F holdings from the SEC's Form 13F data sets.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    backfill = sub.add_parser(
        "backfill", help="every listed archive whose window ends on or after --start"
    )
    backfill.add_argument(
        "--start", required=True, help="first filing date, e.g. 2016-01-01"
    )
    backfill.add_argument("--overwrite", action="store_true")
    sub.add_parser("incremental", help="the newest two listed archives, rewritten")
    parser.add_argument(
        "--bucket", default=os.environ.get("NE_RESEARCH_BUCKET", DEFAULT_BUCKET)
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parser().parse_args(argv)
    client = SecClient()
    listed = list_archives(client.get(LISTING_URL).text)
    if args.mode == "backfill":
        archives = select_archives(listed, dt.date.fromisoformat(args.start))
        overwrite = args.overwrite
    else:
        archives, overwrite = listed[-2:], True

    import boto3

    summary = run(
        S3ObjectStore(boto3.client("s3"), args.bucket),
        client,
        archives,
        run_date=dt.date.today(),
        overwrite=overwrite,
    )
    logger.info(
        "run %s: %d archive(s) written, %d skipped, %d rows",
        summary["run_id"],
        summary["archives_written"],
        summary["archives_skipped_existing"],
        summary["rows_written"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
