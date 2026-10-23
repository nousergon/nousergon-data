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

Memory: each archive is parsed, written and freed in its OWN child process
(`process_in_child`), so the backfill's footprint is one archive's, however
many archives it walks. Inside the child, INFOTABLE is read in chunks, each
chunk becomes an arrow table, the chunks are concatenated without a copy, and
the parquet is written one row group at a time from a sort index rather than
from a sorted copy of the whole archive. See `process_archive`.

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
import subprocess
import sys
import uuid
import zipfile
from collections.abc import Callable, Iterator, Iterable, Sequence
from functools import partial
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

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


#: The arrow schema of every v1 archive parquet, pinned. It is what the v1
#: archives written 2026-10-22/23 carry (checked on 01mar2026-31may2026):
#: pandas' string[pyarrow] is large_string, the object date columns date32.
ARROW_SCHEMA = pa.schema(
    [
        ("accession", pa.large_string()),
        ("filing_date", pa.date32()),
        ("period_of_report", pa.date32()),
        ("filer_cik", pa.int64()),
        ("submission_type", pa.large_string()),
        ("is_amendment", pa.bool_()),
        ("amendment_type", pa.large_string()),
        ("cusip", pa.large_string()),
        ("issuer_name", pa.large_string()),
        ("title_of_class", pa.large_string()),
        ("value_usd", pa.float64()),
        ("value_raw", pa.float64()),
        ("shares", pa.float64()),
        ("shares_type", pa.large_string()),
        ("put_call", pa.large_string()),
        ("investment_discretion", pa.large_string()),
        ("archive", pa.large_string()),
        ("schema_version", pa.int64()),
    ]
)
SORT_KEYS: tuple[tuple[str, str], ...] = (
    ("filing_date", "ascending"),
    ("accession", "ascending"),
    ("cusip", "ascending"),
)
#: pyarrow's default row-group ceiling, which the v1 archives were written with.
ROW_GROUP_ROWS = 1_048_576


class SourceFormatError(RuntimeError):
    """The SEC's archive is not the layout this collector reads."""


class ArchiveProcessError(RuntimeError):
    """The child process that handles one archive did not finish it."""


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


def _normalise(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    frame.columns = [c.strip().upper() for c in frame.columns]
    missing = [c for c in REQUIRED_COLUMNS[name] if c not in frame.columns]
    if missing:
        raise SourceFormatError(f"{name} is missing required columns {missing}")
    return frame[list(REQUIRED_COLUMNS[name])]


def _csv_options(name: str) -> dict[str, Any]:
    wanted = set(REQUIRED_COLUMNS[name])
    return {
        "sep": "\t",
        "dtype": str,
        "keep_default_na": False,
        "na_values": [""],
        "quoting": 3,
        "usecols": lambda column: column.strip().upper() in wanted,
    }


def _member(archive: zipfile.ZipFile, name: str) -> str:
    members = {
        member.rsplit("/", 1)[-1].upper(): member for member in archive.namelist()
    }
    if name.upper() not in members:
        raise SourceFormatError(f"archive has no {name} (members: {sorted(members)})")
    return members[name.upper()]


def _read_member(archive: zipfile.ZipFile, name: str) -> pd.DataFrame:
    with archive.open(_member(archive, name)) as handle:
        return _normalise(pd.read_csv(handle, **_csv_options(name)), name)


def _iter_member(
    archive: zipfile.ZipFile, name: str, chunk_rows: int
) -> Iterator[pd.DataFrame]:
    """The member in chunks. An INFOTABLE runs to millions of rows, and the box
    has 4 GB: read whole, as object strings, it was OOM-killed on 2017q3
    (2026-10-23, exit 137)."""
    with archive.open(_member(archive, name)) as handle:
        for chunk in pd.read_csv(handle, chunksize=chunk_rows, **_csv_options(name)):
            yield _normalise(chunk, name)


def _dates(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.strip()
    parsed = pd.to_datetime(text, format="%d-%b-%Y", errors="coerce")
    iso = parsed.isna() & text.notna()
    if iso.any():
        parsed = parsed.where(
            ~iso, pd.to_datetime(text.where(iso), format="%Y-%m-%d", errors="coerce")
        )
    return parsed.dt.date


_TEXT = "string[pyarrow]"
CHUNK_ROWS = 250_000


def _text(values: pd.Series, *, upper: bool = False) -> pd.Series:
    out = values.fillna("").str.strip() if upper else values
    if upper:
        out = out.str.upper()
    return out.astype(_TEXT)


def _holdings(merged: pd.DataFrame, label: str) -> pd.DataFrame:
    value_raw = pd.to_numeric(merged["VALUE"], errors="coerce")
    in_thousands = merged["filing_date"] < VALUE_IN_DOLLARS_FROM
    return pd.DataFrame(
        {
            "accession": merged["ACCESSION_NUMBER"].astype(_TEXT),
            "filing_date": merged["filing_date"],
            "period_of_report": merged["period_of_report"],
            "filer_cik": pd.to_numeric(merged["CIK"], errors="coerce").astype("Int64"),
            "submission_type": _text(merged["SUBMISSIONTYPE"], upper=True),
            "is_amendment": merged["ISAMENDMENT"]
            .fillna("")
            .str.strip()
            .str.upper()
            .eq("Y"),
            "amendment_type": _text(merged["AMENDMENTTYPE"], upper=True),
            "cusip": _text(merged["CUSIP"], upper=True),
            "issuer_name": merged["NAMEOFISSUER"].astype(_TEXT),
            "title_of_class": merged["TITLEOFCLASS"].astype(_TEXT),
            "value_usd": (
                value_raw * in_thousands.map({True: 1000.0, False: 1.0})
            ).astype("float64"),
            "value_raw": value_raw.astype("float64"),
            "shares": pd.to_numeric(merged["SSHPRNAMT"], errors="coerce").astype(
                "float64"
            ),
            "shares_type": _text(merged["SSHPRNAMTTYPE"], upper=True),
            "put_call": _text(merged["PUTCALL"], upper=True),
            "investment_discretion": _text(merged["INVESTMENTDISCRETION"], upper=True),
            "archive": pd.Series(label, index=merged.index, dtype=_TEXT),
            "schema_version": SCHEMA_VERSION,
        }
    )


def _chunk_table(merged: pd.DataFrame, label: str) -> pa.Table:
    frame = _holdings(merged, label)[list(ROW_COLUMNS)]
    return pa.Table.from_pandas(frame, schema=ARROW_SCHEMA, preserve_index=False)


def parse_archive_table(
    payload: bytes, label: str, *, chunk_rows: int = CHUNK_ROWS
) -> tuple[pa.Table, dict[str, Any]]:
    """The archive's rows as one arrow table, UNSORTED (`archive_parquet` sorts).

    Each INFOTABLE chunk is joined, normalised and turned into an arrow table
    while it is still small; the chunk tables are then concatenated, which
    copies nothing. The pandas path this replaced held every chunk frame, a
    concatenated copy and a sorted copy at once.
    """
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        submission = _read_member(archive, "SUBMISSION.tsv")
        cover = _read_member(archive, "COVERPAGE.tsv")
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
        cover = cover.drop_duplicates("ACCESSION_NUMBER")
        parts: list[pa.Table] = []
        info_rows = 0
        for info in _iter_member(archive, "INFOTABLE.tsv", chunk_rows):
            info_rows += len(info)
            merged = info.merge(submission, on="ACCESSION_NUMBER", how="inner").merge(
                cover, on="ACCESSION_NUMBER", how="left"
            )
            parts.append(_chunk_table(merged, label))
            del info, merged
    if not parts:
        raise SourceFormatError(f"{label}: INFOTABLE has no rows")
    table = pa.concat_tables(parts)
    del parts
    first, last = archive_span(label)
    filed = table["filing_date"]
    outside = int(
        pc.sum(
            pc.or_(
                pc.less(filed, pa.scalar(first, pa.date32())),
                pc.greater(filed, pa.scalar(last, pa.date32())),
            )
        ).as_py()
        or 0
    )
    if outside > MAX_BAD_DATE_FRACTION * max(table.num_rows, 1):
        raise SourceFormatError(
            f"{label}: {outside} rows filed outside {first}..{last}"
        )
    stats = {
        "submissions": int(len(submission)),
        "submissions_bad_date": int(bad.sum()),
        "infotable_rows": int(info_rows),
        "rows": int(table.num_rows),
        "rows_unmatched": int(info_rows - table.num_rows),
        "rows_outside_window": outside,
    }
    return table, stats


def _sort_indices(table: pa.Table) -> pa.Array:
    return pc.sort_indices(table, sort_keys=list(SORT_KEYS))


def archive_parquet(table: pa.Table, *, row_group_rows: int = ROW_GROUP_ROWS) -> bytes:
    """The archive's parquet, sorted by `SORT_KEYS`, written one row group at a time.

    Only one row group's sorted copy exists at once, never a sorted copy of
    the whole archive.
    """
    order = _sort_indices(table)
    sink = pa.BufferOutputStream()
    with pq.ParquetWriter(sink, table.schema, compression="zstd") as writer:
        for start in range(0, table.num_rows, row_group_rows):
            group = table.take(order.slice(start, row_group_rows))
            writer.write_table(group, row_group_size=row_group_rows)
            del group
    return sink.getvalue().to_pybytes()


def parse_archive(
    payload: bytes, label: str, *, chunk_rows: int = CHUNK_ROWS
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """The archive's rows as a sorted DataFrame (for inspection and tests; the
    writer path is `process_archive`, which never builds one)."""
    table, stats = parse_archive_table(payload, label, chunk_rows=chunk_rows)
    frame = table.take(_sort_indices(table)).to_pandas()
    return frame[list(ROW_COLUMNS)], stats


# -- run -----------------------------------------------------------------------


def archive_key(label: str) -> str:
    return f"{ARCHIVES_PREFIX}{label}.parquet"


def process_archive(store: Any, client: Any, label: str, url: str) -> dict[str, Any]:
    """Fetch, parse and write ONE archive; its stats for the run record."""
    payload = client.get(url, timeout=900.0).content
    table, stats = parse_archive_table(payload, label)
    del payload
    body = archive_parquet(table)
    del table
    store.put_bytes(archive_key(label), body, "application/octet-stream")
    stats["source_url"] = url
    return stats


_REPO_ROOT = Path(__file__).resolve().parents[1]


def child_command(label: str, url: str, bucket: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "collectors.holdings_13f_pit",
        "--bucket",
        bucket,
        "archive",
        "--label",
        label,
        "--url",
        url,
    ]


def process_in_child(label: str, url: str, *, bucket: str) -> dict[str, Any]:
    """`process_archive` in a fresh interpreter, so the memory it peaks at is
    handed back to the OS when the archive is done.

    Why: the 2026-10-23 backfills were OOM-killed (exit 137) on the 4 GB box
    three times: after 3 archives twice with whole-INFOTABLE reads, then after
    36 with chunked reads (data#2260). Each death came after archives the same
    process had already finished, and the archive killed in the first run was
    written fine as the FIRST archive of the next one. Memory a Python process
    has freed is largely kept by the allocator rather than returned to the OS,
    so what one archive peaked at stayed resident under the next. A child per
    archive bounds the run at one archive's peak. The child's stderr (its
    logging) is the parent's; its stdout carries only its stats as JSON.
    """
    done = subprocess.run(
        child_command(label, url, bucket),
        cwd=_REPO_ROOT,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if done.returncode != 0:
        how = (
            f"was killed by signal {-done.returncode}"
            + (" (SIGKILL: the kernel's OOM killer)" if done.returncode == -9 else "")
            if done.returncode < 0
            else f"exited {done.returncode}"
        )
        raise ArchiveProcessError(f"{label}: the archive's child process {how}")
    lines = [line for line in done.stdout.splitlines() if line.strip()]
    if not lines:
        raise ArchiveProcessError(f"{label}: the child process printed no stats")
    return json.loads(lines[-1])


def _write_run_record(store: Any, summary: dict[str, Any], run_date: dt.date) -> None:
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    body = json.dumps(summary, indent=2, sort_keys=True).encode()
    store.put_bytes(
        f"{RUNS_PREFIX}{run_date.isoformat()}/{summary['run_id']}.json",
        body,
        "application/json",
    )
    store.put_bytes(RUN_LATEST_KEY, body, "application/json")


def run(
    store: Any,
    client: Any,
    archives: Iterable[tuple[str, str]],
    *,
    run_date: dt.date,
    overwrite: bool,
    run_id: str | None = None,
    process: Callable[[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Every archive not already written (unless `overwrite`), one at a time.

    `process(label, url)` writes one archive and returns its stats; the
    default runs it in this process, `main` passes `process_in_child`. An
    archive already under `ARCHIVES_PREFIX` is skipped, so a re-dispatch
    resumes. A failed archive still leaves a run record naming it.
    """
    if process is None:
        process = partial(process_archive, store, client)
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
        try:
            stats = process(label, url)
        except Exception as error:
            summary["failed_archive"] = label
            summary["failure"] = type(error).__name__
            _write_run_record(store, summary, run_date)
            raise
        summary["archives"][label] = stats
        summary["archives_written"] += 1
        summary["rows_written"] += stats["rows"]
        logger.info(
            "%s: %d holdings rows from %d submissions",
            label,
            stats["rows"],
            stats["submissions"],
        )
    _write_run_record(store, summary, run_date)
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
    one = sub.add_parser(
        "archive",
        help="one archive, in this process (what backfill and incremental run "
        "in a child per archive); prints its stats as JSON",
    )
    one.add_argument("--label", required=True)
    one.add_argument("--url", required=True)
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

    import boto3

    store = S3ObjectStore(boto3.client("s3"), args.bucket)
    if args.mode == "archive":
        stats = process_archive(store, client, args.label.lower(), args.url)
        print(json.dumps(stats, sort_keys=True), flush=True)
        return 0
    listed = list_archives(client.get(LISTING_URL).text)
    if args.mode == "backfill":
        archives = select_archives(listed, dt.date.fromisoformat(args.start))
        overwrite = args.overwrite
    else:
        archives, overwrite = listed[-2:], True

    summary = run(
        store,
        client,
        archives,
        run_date=dt.date.today(),
        overwrite=overwrite,
        process=partial(process_in_child, bucket=args.bucket),
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
