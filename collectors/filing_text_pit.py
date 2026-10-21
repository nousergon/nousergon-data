"""Point-in-time 10-K / 10-Q text, one parquet per issuer.

Why this exists (alpha-engine-config-I12463): the model zoo's W3 family
("lazy prices", Cohen, Malloy & Nguyen 2020) scores how much a company's
filing text changes from one report to the next, 2018 onward. The RAG
ingestion (`rag/pipelines/ingest_sec_filings.py`) reads a two-year lookback
into chunks for retrieval; it keeps no comparable per-filing text history.

What is stored, per filing: the whole primary document as plain text, the
four narrative sections the RAG splitter already finds (Risk Factors, MD&A,
Business, market risk) WITHOUT the RAG's 50k-character cap, and a SHA-256 of
each so a consumer can tell "unchanged" without reading the text.

Discovery is the EDGAR submissions API (`data.sec.gov/submissions`), which
names each filing's primary document; its `filings.files` pages carry the
filings older than the ~1,000 most recent, so a heavy filer's 2016 10-K is
still found. Download is the EDGAR Archives, paced by `SecClient`.

Knowledge rule: a filing is known as of its EDGAR `filing_date`; a consumer
may use it for session S only when `filing_date < S` (the same strict rule as
the EDGAR fundamentals sessions, for the same reason: no acceptance time).

Issuers: the (ticker, CIK) pairs of the newest EDGAR PIT facts run, or
`--tickers`. Names delisted before that run are NOT covered (they have no
row in SEC's current ticker map either); see the issue.

Output: `fundamentals_pit/filing_text/v1/issuers/{cik}.parquet`, rewritten
whole per issuer, plus `runs/{run_date}/{run_id}.json` and `runs/latest.json`.
An issuer with any failed download is NOT written, so the next run retries
it instead of leaving a silent hole; the run fails if more than
`MAX_FAILED_ISSUER_SHARE` of issuers failed.

PUBLIC REPO: logs carry counts only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import os
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from collectors.edgar_pit_fundamentals import (
    RUN_LATEST_KEY as EDGAR_RUN_LATEST_KEY,
)
from collectors.edgar_pit_fundamentals import (
    S3ObjectStore,
    SecClient,
    SecFetchError,
)

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/filing_text/v1"
ISSUERS_PREFIX = f"{DATASET_PREFIX}/issuers/"
RUNS_PREFIX = f"{DATASET_PREFIX}/runs/"
RUN_LATEST_KEY = f"{RUNS_PREFIX}latest.json"
DEFAULT_BUCKET = "alpha-engine-research"

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
SUBMISSIONS_PAGE_URL = "https://data.sec.gov/submissions/{name}"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"

#: Original reports only: an amendment restates part of a filing, and
#: comparing it to the full report before it would read as a large change.
FORMS = ("10-K", "10-Q")

#: 2016 so the first 2018 filing has a same-quarter filing two years back.
DEFAULT_START = dt.date(2016, 1, 1)

MAX_FAILED_ISSUER_SHARE = 0.05

#: Below this a primary document is a cover page or a wrapper, not a report.
MIN_DOCUMENT_CHARS = 5_000

SECTION_COLUMNS = {
    "Risk Factors": "risk_factors",
    "Management's Discussion and Analysis": "mdna",
    "Business": "business",
    "Quantitative and Qualitative Disclosures About Market Risk": "market_risk",
}

ROW_COLUMNS: tuple[str, ...] = (
    "cik",
    "ticker",
    "accession",
    "form",
    "filing_date",
    "report_date",
    "primary_document",
    "document_chars",
    "document_sha256",
    "document_text",
    *(f"{name}_text" for name in SECTION_COLUMNS.values()),
    *(f"{name}_sha256" for name in SECTION_COLUMNS.values()),
    "schema_version",
)


class FilingTextError(RuntimeError):
    pass


class ShortDocumentError(FilingTextError):
    """A filing's primary document is too short to be the report (a cover page or
    wrapper whose body sits in an exhibit). The filing is skipped and recorded;
    the issuer's other filings are still written."""


@dataclass(frozen=True)
class Filing:
    cik: int
    accession: str
    form: str
    filing_date: dt.date
    report_date: dt.date | None
    primary_document: str

    @property
    def url(self) -> str:
        return ARCHIVE_URL.format(
            cik=self.cik,
            accession=self.accession.replace("-", ""),
            document=self.primary_document,
        )


def issuer_key(cik: int) -> str:
    return f"{ISSUERS_PREFIX}{cik}.parquet"


# -- discovery ---------------------------------------------------------------------


def _date_or_none(text: Any) -> dt.date | None:
    if not text:
        return None
    try:
        return dt.date.fromisoformat(str(text))
    except ValueError:
        return None


def filings_in_block(
    cik: int, block: Mapping[str, Any], *, start: dt.date, end: dt.date
) -> list[Filing]:
    """One columnar block (`filings.recent`, or a `files` page) -> admitted filings."""
    forms = block.get("form") or []
    columns = ("accessionNumber", "filingDate", "reportDate", "primaryDocument")
    for column in columns:
        if len(block.get(column) or []) != len(forms):
            raise FilingTextError(
                f"CIK {cik}: submissions column {column} does not match form"
            )
    out: list[Filing] = []
    for i, form in enumerate(forms):
        form = str(form).strip().upper()
        filed = _date_or_none(block["filingDate"][i])
        document = str(block["primaryDocument"][i] or "").strip()
        if (
            form not in FORMS
            or filed is None
            or not start <= filed <= end
            or not document
        ):
            continue
        out.append(
            Filing(
                cik=cik,
                accession=str(block["accessionNumber"][i]),
                form=form,
                filing_date=filed,
                report_date=_date_or_none(block["reportDate"][i]),
                primary_document=document,
            )
        )
    return out


def discover(sec: Any, cik: int, *, start: dt.date, end: dt.date) -> list[Filing]:
    payload = sec.get_json(SUBMISSIONS_URL.format(cik=cik))
    filings = payload.get("filings") or {}
    found = filings_in_block(cik, filings.get("recent") or {}, start=start, end=end)
    for page in filings.get("files") or []:
        page_to = _date_or_none(page.get("filingTo"))
        if page_to is not None and page_to < start:
            continue
        found += filings_in_block(
            cik,
            sec.get_json(SUBMISSIONS_PAGE_URL.format(name=page["name"])),
            start=start,
            end=end,
        )
    unique = {f.accession: f for f in found}
    return sorted(unique.values(), key=lambda f: (f.filing_date, f.accession))


# -- text --------------------------------------------------------------------------


def _sha(text: str | None) -> str | None:
    return hashlib.sha256(text.encode()).hexdigest() if text else None


def filing_row(filing: Filing, ticker: str, html: str) -> dict[str, Any]:
    from rag.pipelines.ingest_sec_filings import _extract_sections, _html_to_text

    text = _html_to_text(html)
    if len(text) < MIN_DOCUMENT_CHARS:
        raise ShortDocumentError(
            f"{filing.accession}: primary document is {len(text)} chars, under {MIN_DOCUMENT_CHARS}"
        )
    sections = _extract_sections(html, form_type=filing.form, max_chars=None, text=text)
    row: dict[str, Any] = {
        "cik": filing.cik,
        "ticker": ticker,
        "accession": filing.accession,
        "form": filing.form,
        "filing_date": filing.filing_date,
        "report_date": filing.report_date,
        "primary_document": filing.primary_document,
        "document_chars": len(text),
        "document_sha256": _sha(text),
        "document_text": text,
        "schema_version": SCHEMA_VERSION,
    }
    for label, name in SECTION_COLUMNS.items():
        section = sections.get(label)
        row[f"{name}_text"] = section
        row[f"{name}_sha256"] = _sha(section)
    return row


def issuer_frame(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(list(rows), columns=list(ROW_COLUMNS))
    frame["cik"] = frame["cik"].astype("int64")
    frame["document_chars"] = frame["document_chars"].astype("int64")
    frame["schema_version"] = frame["schema_version"].astype("int64")
    return frame.sort_values(["filing_date", "accession"]).reset_index(drop=True)


def _to_parquet(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, compression="zstd")
    return buffer.getvalue()


# -- run ---------------------------------------------------------------------------


def run(
    store: Any,
    sec: Any,
    issuers: Sequence[tuple[str, int]],
    *,
    start: dt.date,
    end: dt.date,
    run_date: dt.date,
    overwrite: bool,
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or uuid.uuid4().hex[:12]
    existing = set(store.list_keys(ISSUERS_PREFIX))
    summary: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "issuers_requested": len(issuers),
        "issuers_written": 0,
        "issuers_skipped_existing": 0,
        "issuers_without_filings": 0,
        "issuers_failed": [],
        "filings_written": 0,
        "filings_skipped_short": [],
        "sections_found": dict.fromkeys(SECTION_COLUMNS.values(), 0),
    }
    for ticker, cik in issuers:
        if issuer_key(cik) in existing and not overwrite:
            summary["issuers_skipped_existing"] += 1
            continue
        try:
            filings = discover(sec, cik, start=start, end=end)
            rows = []
            for filing in filings:
                try:
                    rows.append(filing_row(filing, ticker, sec.get(filing.url).text))
                except ShortDocumentError:
                    summary["filings_skipped_short"].append(
                        f"{ticker}:{filing.accession}"
                    )
        except (SecFetchError, FilingTextError) as error:
            logger.warning(
                "%s (CIK %d) not written: %s", ticker, cik, type(error).__name__
            )
            summary["issuers_failed"].append(ticker)
            continue
        if not rows:
            summary["issuers_without_filings"] += 1
            continue
        store.put_bytes(
            issuer_key(cik), _to_parquet(issuer_frame(rows)), "application/octet-stream"
        )
        summary["issuers_written"] += 1
        summary["filings_written"] += len(rows)
        for name in SECTION_COLUMNS.values():
            summary["sections_found"][name] += sum(1 for r in rows if r[f"{name}_text"])
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    attempted = len(issuers) - summary["issuers_skipped_existing"]
    failed_share = len(summary["issuers_failed"]) / attempted if attempted else 0.0
    summary["failed_issuer_share"] = failed_share
    body = json.dumps(summary, indent=2, sort_keys=True).encode()
    store.put_bytes(
        f"{RUNS_PREFIX}{run_date.isoformat()}/{run_id}.json", body, "application/json"
    )
    store.put_bytes(RUN_LATEST_KEY, body, "application/json")
    if failed_share > MAX_FAILED_ISSUER_SHARE:
        raise FilingTextError(
            f"{len(summary['issuers_failed'])} of {attempted} issuers failed "
            f"({failed_share:.1%} > {MAX_FAILED_ISSUER_SHARE:.0%}); rerun retries them"
        )
    return summary


def issuers_from_edgar_facts(store: Any) -> list[tuple[str, int]]:
    """(ticker, cik) pairs of the newest EDGAR PIT facts run."""
    latest = json.loads(store.get_bytes(EDGAR_RUN_LATEST_KEY))
    facts = pd.read_parquet(
        io.BytesIO(store.get_bytes(latest["facts_key"])), columns=["ticker", "cik"]
    )
    pairs = facts.drop_duplicates().sort_values("ticker")
    out = [(str(t), int(c)) for t, c in zip(pairs["ticker"], pairs["cik"], strict=True)]
    if not out:
        raise FilingTextError(f"{latest['facts_key']} names no issuers")
    return out


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.filing_text_pit",
        description="Point-in-time 10-K / 10-Q text per issuer.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    backfill = sub.add_parser(
        "backfill", help="every issuer, filings from --start; write-if-absent"
    )
    backfill.add_argument("--start", type=dt.date.fromisoformat, default=DEFAULT_START)
    backfill.add_argument("--overwrite", action="store_true")
    sub.add_parser(
        "refresh", help="every issuer rewritten with its filings from the default start"
    )
    parser.add_argument(
        "--tickers", help="TICKER:CIK,... instead of the EDGAR facts issuers"
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

    import boto3

    store = S3ObjectStore(boto3.client("s3"), args.bucket)
    if args.tickers:
        issuers = [
            (pair.split(":")[0].strip().upper(), int(pair.split(":")[1]))
            for pair in args.tickers.split(",")
            if pair.strip()
        ]
    else:
        issuers = issuers_from_edgar_facts(store)
    start = args.start if args.mode == "backfill" else DEFAULT_START
    overwrite = args.overwrite if args.mode == "backfill" else True
    today = dt.datetime.now(dt.UTC).date()
    summary = run(
        store,
        SecClient(),
        issuers,
        start=start,
        end=today,
        run_date=today,
        overwrite=overwrite,
    )
    logger.info(
        "run %s: %d issuers written (%d filings), %d skipped, %d failed",
        summary["run_id"],
        summary["issuers_written"],
        summary["filings_written"],
        summary["issuers_skipped_existing"],
        len(summary["issuers_failed"]),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
