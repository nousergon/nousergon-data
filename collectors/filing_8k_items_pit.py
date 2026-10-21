"""Point-in-time 8-K item codes per filing, from EDGAR's submissions API.

Why this exists: the model zoo wants event types (earnings releases 2.02,
officer changes 5.02, material agreements 1.01, impairments 2.06, and so on)
as features, with a known-at date and history from 2017. It needs no filing
text, only the item codes.

Source: EDGAR's per-company submissions JSON
(`data.sec.gov/submissions/CIK##########.json` and its older `files` pages).
Every filing there carries its `items` list, so one request per issuer, plus
one per older page, covers its whole 8-K history. This is the same discovery
`collectors/filing_text_pit.py` uses. Reading the item codes from the full
index plus each filing's header would cost one request per 8-K, about 10^6
requests for ten years.

Issuers: the newest EDGAR PIT facts run (`issuers_from_edgar_facts`), the same
universe as the filing-text dataset. It holds current names only, so delisted
issuers are absent. The run record says so, and widening it is a separate
change.

Rows: one per (filing, item code), forms 8-K and 8-K/A:
    cik, ticker, accession, form, filing_date, acceptance_datetime,
    report_date, item, items_all
`item` is one code (e.g. "2.02"). `items_all` is the filing's full
comma-joined list.

Knowledge rule: use a row for session S only when `filing_date < S`.
`acceptance_datetime` is kept as EDGAR reports it, for a consumer that wants
intraday precision, but it is not the rule.

Output, one parquet per calendar quarter of filing date, rewritten whole on
every run:

    fundamentals_pit/filing_8k_items/v1/quarters/{YYYY}q{N}.parquet
    fundamentals_pit/filing_8k_items/v1/runs/{run_date}/{run_id}.json  (+ runs/latest.json)

An issuer whose submissions fail to download stops the run. A partial
universe would read as "no events" for the missing names.

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
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from collectors.edgar_pit_fundamentals import S3ObjectStore, SecClient
from collectors.filing_text_pit import (
    SUBMISSIONS_PAGE_URL,
    SUBMISSIONS_URL,
    issuers_from_edgar_facts,
)

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/filing_8k_items/v1"
QUARTERS_PREFIX = f"{DATASET_PREFIX}/quarters/"
RUNS_PREFIX = f"{DATASET_PREFIX}/runs/"
RUN_LATEST_KEY = f"{RUNS_PREFIX}latest.json"
DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_START = dt.date(2016, 1, 1)
FORMS = frozenset({"8-K", "8-K/A"})

ROW_COLUMNS: tuple[str, ...] = (
    "cik",
    "ticker",
    "accession",
    "form",
    "filing_date",
    "acceptance_datetime",
    "report_date",
    "item",
    "items_all",
    "schema_version",
)


class SourceFormatError(RuntimeError):
    """EDGAR's submissions JSON is not the shape this collector reads."""


def _date(text: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(text)) if text else None
    except ValueError:
        return None


def rows_in_block(
    cik: int, ticker: str, block: Mapping[str, Any], *, start: dt.date
) -> list[dict[str, Any]]:
    forms = block.get("form") or []
    columns = (
        "accessionNumber",
        "filingDate",
        "reportDate",
        "acceptanceDateTime",
        "items",
    )
    for column in columns:
        if len(block.get(column) or []) != len(forms):
            raise SourceFormatError(
                f"CIK {cik}: submissions column {column} does not match form"
            )
    out: list[dict[str, Any]] = []
    for i, form in enumerate(forms):
        form = str(form).strip().upper()
        filed = _date(block["filingDate"][i])
        if form not in FORMS or filed is None or filed < start:
            continue
        codes = [
            c.strip() for c in str(block["items"][i] or "").split(",") if c.strip()
        ]
        for code in codes or [""]:
            out.append(
                {
                    "cik": cik,
                    "ticker": ticker,
                    "accession": str(block["accessionNumber"][i]),
                    "form": form,
                    "filing_date": filed,
                    "acceptance_datetime": str(block["acceptanceDateTime"][i] or ""),
                    "report_date": _date(block["reportDate"][i]),
                    "item": code,
                    "items_all": ",".join(codes),
                }
            )
    return out


def issuer_rows(
    sec: Any, cik: int, ticker: str, *, start: dt.date
) -> list[dict[str, Any]]:
    payload = sec.get_json(SUBMISSIONS_URL.format(cik=cik))
    filings = payload.get("filings") or {}
    rows = rows_in_block(cik, ticker, filings.get("recent") or {}, start=start)
    for page in filings.get("files") or []:
        page_to = _date(page.get("filingTo"))
        if page_to is not None and page_to < start:
            continue
        rows += rows_in_block(
            cik,
            ticker,
            sec.get_json(SUBMISSIONS_PAGE_URL.format(name=page["name"])),
            start=start,
        )
    unique = {(r["accession"], r["item"]): r for r in rows}
    return list(unique.values())


def quarter_label(day: dt.date) -> str:
    return f"{day.year}q{(day.month - 1) // 3 + 1}"


def _to_parquet(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, compression="zstd")
    return buffer.getvalue()


def run(
    store: Any,
    sec: Any,
    issuers: Sequence[tuple[str, int]],
    *,
    start: dt.date,
    run_date: dt.date,
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or uuid.uuid4().hex[:12]
    rows: list[dict[str, Any]] = []
    for ticker, cik in issuers:
        rows += issuer_rows(sec, cik, ticker, start=start)
    if not rows:
        raise SourceFormatError("no 8-K rows for any issuer")
    frame = pd.DataFrame(rows)
    frame["schema_version"] = SCHEMA_VERSION
    for column in (
        "ticker",
        "accession",
        "form",
        "acceptance_datetime",
        "item",
        "items_all",
    ):
        frame[column] = frame[column].astype("string")
    frame = frame.sort_values(["filing_date", "accession", "item"]).reset_index(
        drop=True
    )
    frame["_quarter"] = frame["filing_date"].map(quarter_label)
    summary: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "start": start.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "issuers": len(issuers),
        "issuers_with_8k": int(frame["cik"].nunique()),
        "universe": "newest EDGAR PIT facts run; current names only, no delisted issuers",
        "filings": int(frame["accession"].nunique()),
        "rows": len(frame),
        "quarters": {},
    }
    for label, part in frame.groupby("_quarter", sort=True):
        part = part.drop(columns="_quarter")[list(ROW_COLUMNS)]
        store.put_bytes(
            f"{QUARTERS_PREFIX}{label}.parquet",
            _to_parquet(part),
            "application/octet-stream",
        )
        summary["quarters"][label] = {
            "rows": len(part),
            "filings": int(part["accession"].nunique()),
        }
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    body = json.dumps(summary, indent=2, sort_keys=True).encode()
    store.put_bytes(
        f"{RUNS_PREFIX}{run_date.isoformat()}/{run_id}.json", body, "application/json"
    )
    store.put_bytes(RUN_LATEST_KEY, body, "application/json")
    logger.info(
        "run %s: %d rows, %d filings, %d of %d issuers with an 8-K, %d quarters",
        run_id,
        summary["rows"],
        summary["filings"],
        summary["issuers_with_8k"],
        summary["issuers"],
        len(summary["quarters"]),
    )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        prog="python -m collectors.filing_8k_items_pit",
        description="Point-in-time 8-K item codes per filing.",
    )
    parser.add_argument("--start", type=dt.date.fromisoformat, default=DEFAULT_START)
    parser.add_argument(
        "--bucket", default=os.environ.get("NE_RESEARCH_BUCKET", DEFAULT_BUCKET)
    )
    args = parser.parse_args(argv)

    import boto3

    store = S3ObjectStore(boto3.client("s3"), args.bucket)
    run(
        store,
        SecClient(),
        issuers_from_edgar_facts(store),
        start=args.start,
        run_date=dt.date.today(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
