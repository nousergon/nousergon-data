"""Point-in-time Form 4 insider transactions from SEC's quarterly Form 3/4/5 data sets.

Why this exists (alpha-engine-config-I12461): the model zoo's W4 family scores
insider buying/selling over 2018 onward, and the only Form 4 data in the fleet
is D46 (`rag/pipelines/ingest_form4.py`), which fetches a 90-day lookback per
ticker each week. That cannot reconstruct history, and per-filing XML scraping
of eight years would be ~10^6 SEC requests.

Source: SEC DERA "Insider Transactions Data Sets" -- one zip per calendar
quarter, every Form 3/4/5 (and amendment) EDGAR accepted in that quarter,
already flattened to TSV. No API key; SEC requires a declared User-Agent
(`SecClient`, shared with `edgar_pit_fundamentals`). One request per quarter.

Knowledge rule: a row is known as of its EDGAR `filing_date`. Like the EDGAR
fundamentals sessions, a consumer may use it for session S only when
`filing_date < S` -- the data sets carry the filing date, not the acceptance
time, so a same-day filing after the close cannot be told from one before the
open, and the strict inequality is the only safe reading.

Output, one parquet per quarter, rewritten whole (a quarter's data set is
final once published; SEC re-publishes only to correct):

    fundamentals_pit/insider/v1/quarters/{YYYY}q{N}.parquet
    fundamentals_pit/insider/v1/runs/{run_date}/{run_id}.json  (+ runs/latest.json)

Rows are non-derivative transactions (Table I) from Form 4 and 4/A only --
open-market buys and sells, grants, exercises, tax withholding all carry their
own `trans_code`, and the consumer chooses which codes are signal. Derivative
rows (Table II) and Forms 3/5 are out of scope until a family asks for them.

PUBLIC REPO: logs carry counts only. The data is SEC public record, but the
convention is kept so this module never becomes the exception.
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

from collectors.edgar_pit_fundamentals import S3ObjectStore, SecClient, SecFetchError

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/insider/v1"
QUARTERS_PREFIX = f"{DATASET_PREFIX}/quarters/"
RUNS_PREFIX = f"{DATASET_PREFIX}/runs/"
RUN_LATEST_KEY = f"{RUNS_PREFIX}latest.json"
DEFAULT_BUCKET = "alpha-engine-research"

#: Where SEC serves a quarter's zip, tried in order; a 404 falls through to
#: the next template, any other failure stops the run. SEC's path segment is
#: `insider-transactions-data-sets` (data.gov catalog; the 2026-10-20 backfill
#: died on `form-345-data-sets`, which never existed). Quarters from 2026q2 on
#: are reported to live under `datastandardsinnovation/` with the old path
#: 404ing for them -- one third-party report, not live-verified, hence the
#: fallback rather than a cut-over. The verified path goes first: it serves
#: every quarter from 2006q1 through 2026q1.
DATASET_URLS: tuple[str, ...] = (
    "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{year}q{quarter}_form345.zip",
    "https://www.sec.gov/files/datastandardsinnovation/data/insider-transactions-data-sets/{year}q{quarter}_form345.zip",
)

#: Forms whose Table I rows are admitted.
FORMS = frozenset({"4", "4/A"})

#: Columns each member must carry; a data set missing any is a source-format
#: change and stops the run rather than writing a partial table.
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "SUBMISSION.tsv": (
        "ACCESSION_NUMBER",
        "FILING_DATE",
        "PERIOD_OF_REPORT",
        "DOCUMENT_TYPE",
        "ISSUERCIK",
        "ISSUERTRADINGSYMBOL",
    ),
    "REPORTINGOWNER.tsv": (
        "ACCESSION_NUMBER",
        "RPTOWNERCIK",
        "RPTOWNER_RELATIONSHIP",
        "RPTOWNER_TITLE",
    ),
    "NONDERIV_TRANS.tsv": (
        "ACCESSION_NUMBER",
        "NONDERIV_TRANS_SK",
        "SECURITY_TITLE",
        "TRANS_DATE",
        "TRANS_CODE",
        "TRANS_SHARES",
        "TRANS_PRICEPERSHARE",
        "TRANS_ACQUIRED_DISP_CD",
        "SHRS_OWND_FOLWNG_TRANS",
        "DIRECT_INDIRECT_OWNERSHIP",
    ),
}

ROW_COLUMNS: tuple[str, ...] = (
    "accession",
    "transaction_sk",
    "filing_date",
    "period_of_report",
    "form",
    "issuer_cik",
    "issuer_symbol",
    "owner_ciks",
    "owner_count",
    "is_director",
    "is_officer",
    "is_ten_percent_owner",
    "is_other",
    "owner_titles",
    "security_title",
    "trans_date",
    "trans_code",
    "acquired_disposed",
    "shares",
    "price_per_share",
    "shares_owned_after",
    "direct_indirect",
    "schema_version",
)

_QUARTER_RE = re.compile(r"^(\d{4})q([1-4])$")


class SourceFormatError(RuntimeError):
    """The published data set does not have the shape this module reads."""


# -- quarters ----------------------------------------------------------------------


def parse_quarter(text: str) -> tuple[int, int]:
    match = _QUARTER_RE.match(text.strip().lower())
    if not match:
        raise ValueError(f"{text!r} is not a quarter like 2018q1")
    return int(match.group(1)), int(match.group(2))


def quarter_of(day: dt.date) -> tuple[int, int]:
    return day.year, (day.month - 1) // 3 + 1


def quarters_between(
    start: tuple[int, int], end: tuple[int, int]
) -> list[tuple[int, int]]:
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    out: list[tuple[int, int]] = []
    year, quarter = start
    while (year, quarter) <= end:
        out.append((year, quarter))
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return out


def last_complete_quarter(today: dt.date) -> tuple[int, int]:
    year, quarter = quarter_of(today)
    return (year - 1, 4) if quarter == 1 else (year, quarter - 1)


def quarter_label(year: int, quarter: int) -> str:
    return f"{year}q{quarter}"


def quarter_key(year: int, quarter: int) -> str:
    return f"{QUARTERS_PREFIX}{quarter_label(year, quarter)}.parquet"


# -- parsing -----------------------------------------------------------------------


def _read_member(archive: zipfile.ZipFile, name: str) -> pd.DataFrame:
    members = {member.rsplit("/", 1)[-1]: member for member in archive.namelist()}
    if name not in members:
        raise SourceFormatError(f"data set has no {name} (members: {sorted(members)})")
    with archive.open(members[name]) as handle:
        frame = pd.read_csv(
            handle,
            sep="\t",
            dtype=str,
            keep_default_na=False,
            na_values=[""],
            quoting=3,
        )
    missing = [
        column for column in REQUIRED_COLUMNS[name] if column not in frame.columns
    ]
    if missing:
        raise SourceFormatError(f"{name} is missing required columns {missing}")
    return frame


def _parse_sec_dates(values: pd.Series, *, column: str) -> pd.Series:
    """SEC writes `DD-MON-YYYY` (e.g. 05-JAN-2024); ISO is accepted too."""
    text = values.astype("string").str.strip()
    parsed = pd.to_datetime(text, format="%d-%b-%Y", errors="coerce")
    iso = parsed.isna() & text.notna()
    if iso.any():
        parsed = parsed.where(
            ~iso, pd.to_datetime(text.where(iso), format="%Y-%m-%d", errors="coerce")
        )
    bad = parsed.isna() & text.notna()
    if bad.any():
        raise SourceFormatError(f"{int(bad.sum())} unparseable {column} value(s)")
    return parsed.dt.date


def _owner_flags(owners: pd.DataFrame) -> pd.DataFrame:
    relationship = owners["RPTOWNER_RELATIONSHIP"].fillna("").str.lower()
    frame = pd.DataFrame(
        {
            "ACCESSION_NUMBER": owners["ACCESSION_NUMBER"],
            "owner_cik": owners["RPTOWNERCIK"].fillna("").str.strip(),
            "is_director": relationship.str.contains("director"),
            "is_officer": relationship.str.contains("officer"),
            "is_ten_percent_owner": relationship.str.contains(
                "tenpercentowner|10%", regex=True
            ),
            "is_other": relationship.str.contains("other"),
            "title": owners["RPTOWNER_TITLE"].fillna("").str.strip(),
        }
    )
    grouped = frame.groupby("ACCESSION_NUMBER", sort=False)
    return pd.DataFrame(
        {
            "owner_ciks": grouped["owner_cik"].agg(
                lambda s: ";".join(sorted({v for v in s if v}))
            ),
            "owner_count": grouped["owner_cik"].agg(lambda s: len({v for v in s if v})),
            "is_director": grouped["is_director"].any(),
            "is_officer": grouped["is_officer"].any(),
            "is_ten_percent_owner": grouped["is_ten_percent_owner"].any(),
            "is_other": grouped["is_other"].any(),
            "owner_titles": grouped["title"].agg(
                lambda s: ";".join(sorted({v for v in s if v}))
            ),
        }
    ).reset_index()


def parse_dataset(payload: bytes) -> tuple[pd.DataFrame, dict[str, int]]:
    """One quarterly zip -> (Table I rows from Form 4 / 4/A, counts)."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        submissions = _read_member(archive, "SUBMISSION.tsv")
        owners = _read_member(archive, "REPORTINGOWNER.tsv")
        transactions = _read_member(archive, "NONDERIV_TRANS.tsv")

    forms = submissions["DOCUMENT_TYPE"].fillna("").str.strip().str.upper()
    kept = submissions[forms.isin(FORMS)].copy()
    stats = {
        "submissions": len(submissions),
        "form4_submissions": len(kept),
        "nonderiv_rows": len(transactions),
    }
    rows = transactions.merge(
        kept[list(REQUIRED_COLUMNS["SUBMISSION.tsv"])],
        on="ACCESSION_NUMBER",
        how="inner",
    )
    rows = rows.merge(_owner_flags(owners), on="ACCESSION_NUMBER", how="left")
    stats["rows_without_owner"] = int(rows["owner_ciks"].isna().sum())

    out = pd.DataFrame(
        {
            "accession": rows["ACCESSION_NUMBER"].astype("string"),
            "transaction_sk": pd.to_numeric(
                rows["NONDERIV_TRANS_SK"], errors="raise"
            ).astype("int64"),
            "filing_date": _parse_sec_dates(rows["FILING_DATE"], column="FILING_DATE"),
            "period_of_report": _parse_sec_dates(
                rows["PERIOD_OF_REPORT"], column="PERIOD_OF_REPORT"
            ),
            "form": rows["DOCUMENT_TYPE"].str.strip().str.upper().astype("string"),
            "issuer_cik": pd.to_numeric(rows["ISSUERCIK"], errors="raise").astype(
                "int64"
            ),
            "issuer_symbol": rows["ISSUERTRADINGSYMBOL"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "owner_ciks": rows["owner_ciks"].fillna("").astype("string"),
            "owner_count": rows["owner_count"].fillna(0).astype("int64"),
            "is_director": rows["is_director"].fillna(False).astype(bool),
            "is_officer": rows["is_officer"].fillna(False).astype(bool),
            "is_ten_percent_owner": rows["is_ten_percent_owner"]
            .fillna(False)
            .astype(bool),
            "is_other": rows["is_other"].fillna(False).astype(bool),
            "owner_titles": rows["owner_titles"].fillna("").astype("string"),
            "security_title": rows["SECURITY_TITLE"].fillna("").astype("string"),
            "trans_date": _parse_sec_dates(rows["TRANS_DATE"], column="TRANS_DATE"),
            "trans_code": rows["TRANS_CODE"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "acquired_disposed": rows["TRANS_ACQUIRED_DISP_CD"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "shares": pd.to_numeric(rows["TRANS_SHARES"], errors="coerce").astype(
                "float64"
            ),
            "price_per_share": pd.to_numeric(
                rows["TRANS_PRICEPERSHARE"], errors="coerce"
            ).astype("float64"),
            "shares_owned_after": pd.to_numeric(
                rows["SHRS_OWND_FOLWNG_TRANS"], errors="coerce"
            ).astype("float64"),
            "direct_indirect": rows["DIRECT_INDIRECT_OWNERSHIP"]
            .fillna("")
            .str.strip()
            .str.upper()
            .astype("string"),
            "schema_version": SCHEMA_VERSION,
        }
    )
    if out["filing_date"].isna().any():
        raise SourceFormatError(
            "a Form 4 row has no FILING_DATE; its knowledge time is unknown"
        )
    out = out.sort_values(["filing_date", "accession", "transaction_sk"]).reset_index(
        drop=True
    )
    stats["rows"] = len(out)
    return out[list(ROW_COLUMNS)], stats


def check_quarter_bounds(frame: pd.DataFrame, year: int, quarter: int) -> None:
    """Every filing date must fall inside the data set's own quarter."""
    if frame.empty:
        raise SourceFormatError(
            f"{quarter_label(year, quarter)} has no Form 4 Table I rows"
        )
    first = dt.date(year, 3 * quarter - 2, 1)
    last = (
        dt.date(year + 1, 1, 1) if quarter == 4 else dt.date(year, 3 * quarter + 1, 1)
    ) - dt.timedelta(days=1)
    outside = frame[(frame["filing_date"] < first) | (frame["filing_date"] > last)]
    if not outside.empty:
        raise SourceFormatError(
            f"{quarter_label(year, quarter)}: {len(outside)} row(s) filed outside {first}..{last}"
        )


# -- run ---------------------------------------------------------------------------


def fetch_dataset(sec: Any, year: int, quarter: int) -> tuple[str, bytes]:
    """(serving URL, zip bytes) from the first template that does not 404.

    A quarter every template 404s raises the LAST template's error with
    status 404, so the caller's newest-quarter leniency still applies and a
    gap anywhere else stays fatal.
    """
    missing: SecFetchError | None = None
    for template in DATASET_URLS:
        url = template.format(year=year, quarter=quarter)
        try:
            return url, sec.get(url, timeout=900.0).content
        except SecFetchError as error:
            if error.status != 404:
                raise
            missing = error
    assert missing is not None
    raise SecFetchError(
        f"{quarter_label(year, quarter)}: HTTP 404 from every data set URL ({missing})",
        status=404,
    ) from missing


def _to_parquet(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def run(
    store: Any,
    sec: Any,
    quarters: Iterable[tuple[int, int]],
    *,
    run_date: dt.date,
    overwrite: bool,
    newest_may_be_unpublished: bool,
    run_id: str | None = None,
) -> dict[str, Any]:
    quarters = list(quarters)
    run_id = run_id or uuid.uuid4().hex[:12]
    existing = set(store.list_keys(QUARTERS_PREFIX))
    summary: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "quarters": {},
        "quarters_written": 0,
        "quarters_skipped_existing": 0,
        "quarters_unpublished": [],
        "rows_written": 0,
    }
    for year, quarter in quarters:
        label, key = quarter_label(year, quarter), quarter_key(year, quarter)
        if key in existing and not overwrite:
            summary["quarters_skipped_existing"] += 1
            continue
        try:
            url, payload = fetch_dataset(sec, year, quarter)
        except SecFetchError as error:
            if (
                newest_may_be_unpublished
                and (year, quarter) == quarters[-1]
                and error.status == 404
            ):
                logger.info("%s is not published yet; skipped", label)
                summary["quarters_unpublished"].append(label)
                continue
            raise
        frame, stats = parse_dataset(payload)
        stats["source_url"] = url
        check_quarter_bounds(frame, year, quarter)
        store.put_bytes(key, _to_parquet(frame), "application/octet-stream")
        summary["quarters"][label] = stats
        summary["quarters_written"] += 1
        summary["rows_written"] += stats["rows"]
        logger.info(
            "%s: %d rows written (%d Form 4 filings)",
            label,
            stats["rows"],
            stats["form4_submissions"],
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
        prog="python -m collectors.insider_transactions_pit",
        description="Point-in-time Form 4 insider transactions from SEC's quarterly data sets.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    backfill = sub.add_parser(
        "backfill", help="every quarter from --start to the last complete one"
    )
    backfill.add_argument("--start", required=True, help="first quarter, e.g. 2018q1")
    backfill.add_argument(
        "--end", help="last quarter (default: the last complete quarter)"
    )
    backfill.add_argument("--overwrite", action="store_true")
    sub.add_parser("incremental", help="the last two complete quarters, rewritten")
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
    last = last_complete_quarter(today)
    if args.mode == "backfill":
        end = parse_quarter(args.end) if args.end else last
        quarters = quarters_between(parse_quarter(args.start), end)
        overwrite, lenient = args.overwrite, args.end is None
    else:
        previous = last_complete_quarter(dt.date(last[0], 3 * last[1] - 2, 1))
        quarters, overwrite, lenient = [previous, last], True, True

    import boto3

    summary = run(
        S3ObjectStore(boto3.client("s3"), args.bucket),
        SecClient(),
        quarters,
        run_date=today,
        overwrite=overwrite,
        newest_may_be_unpublished=lenient,
    )
    logger.info(
        "run %s: %d quarter(s) written, %d skipped, %d unpublished, %d rows",
        summary["run_id"],
        summary["quarters_written"],
        summary["quarters_skipped_existing"],
        len(summary["quarters_unpublished"]),
        summary["rows_written"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
