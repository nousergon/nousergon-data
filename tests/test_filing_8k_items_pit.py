"""collectors/filing_8k_items_pit.py — 8-K item codes from EDGAR submissions."""

from __future__ import annotations

import datetime as dt
import io
import json
from pathlib import Path

import pandas as pd
import pytest

from collectors import filing_8k_items_pit as m
from collectors.filing_text_pit import SUBMISSIONS_PAGE_URL, SUBMISSIONS_URL

_REPO = Path(__file__).resolve().parents[1]


def _block(rows):
    keys = (
        "form",
        "accessionNumber",
        "filingDate",
        "reportDate",
        "acceptanceDateTime",
        "items",
    )
    return {k: [r[i] for r in rows] for i, k in enumerate(keys)}


_RECENT = _block(
    [
        (
            "8-K",
            "a-1",
            "2024-02-01",
            "2024-01-31",
            "2024-02-01T16:05:00.000Z",
            "2.02,9.01",
        ),
        ("10-Q", "a-2", "2024-05-01", "2024-03-31", "2024-05-01T16:05:00.000Z", ""),
        ("8-K/A", "a-3", "2024-03-01", "2024-02-28", "2024-03-01T09:00:00.000Z", ""),
    ]
)
_OLD = _block(
    [
        ("8-K", "o-1", "2017-06-01", "2017-05-30", "2017-06-01T08:00:00.000Z", "5.02"),
        ("8-K", "o-0", "2015-06-01", "2015-05-30", "2015-06-01T08:00:00.000Z", "1.01"),
        (
            "8-K",
            "a-1",
            "2024-02-01",
            "2024-01-31",
            "2024-02-01T16:05:00.000Z",
            "2.02,9.01",
        ),
    ]
)


class _Sec:
    def __init__(self, payloads) -> None:
        self.payloads, self.urls = payloads, []

    def get_json(self, url):
        self.urls.append(url)
        return self.payloads[url]


def _sec(cik=320193, pages=None):
    pages = (
        pages if pages is not None else [{"name": "p1.json", "filingTo": "2018-01-01"}]
    )
    return _Sec(
        {
            SUBMISSIONS_URL.format(cik=cik): {
                "filings": {"recent": _RECENT, "files": pages}
            },
            SUBMISSIONS_PAGE_URL.format(name="p1.json"): _OLD,
            SUBMISSIONS_PAGE_URL.format(name="ancient.json"): _OLD,
        }
    )


class _Store:
    def __init__(self) -> None:
        self.objects = {}

    def put_bytes(self, key, body, content_type):
        self.objects[key] = body


def test_one_row_per_item_only_8k_forms_and_start_filter() -> None:
    rows = m.issuer_rows(_sec(), 320193, "AAPL", start=dt.date(2016, 1, 1))
    got = sorted((r["accession"], r["item"]) for r in rows)
    assert got == [("a-1", "2.02"), ("a-1", "9.01"), ("a-3", ""), ("o-1", "5.02")]
    a1 = next(r for r in rows if r["accession"] == "a-1")
    assert a1["items_all"] == "2.02,9.01" and a1["filing_date"] == dt.date(2024, 2, 1)
    assert a1["report_date"] == dt.date(2024, 1, 31)


def test_pages_ending_before_start_are_not_fetched() -> None:
    sec = _sec(pages=[{"name": "ancient.json", "filingTo": "2015-12-31"}])
    m.issuer_rows(sec, 320193, "AAPL", start=dt.date(2016, 1, 1))
    assert SUBMISSIONS_PAGE_URL.format(name="ancient.json") not in sec.urls


def test_a_ragged_block_stops_the_run() -> None:
    bad = dict(_RECENT, items=["2.02"])
    with pytest.raises(m.SourceFormatError, match="items"):
        m.rows_in_block(1, "X", bad, start=dt.date(2016, 1, 1))


def test_run_writes_quarters_and_run_record() -> None:
    store = _Store()
    summary = m.run(
        store,
        _sec(),
        [("AAPL", 320193)],
        start=dt.date(2016, 1, 1),
        run_date=dt.date(2026, 10, 21),
        run_id="r1",
    )
    assert set(summary["quarters"]) == {"2017q2", "2024q1"}
    q = pd.read_parquet(io.BytesIO(store.objects[f"{m.QUARTERS_PREFIX}2024q1.parquet"]))
    assert tuple(q.columns) == m.ROW_COLUMNS and len(q) == 3
    assert json.loads(store.objects[m.RUN_LATEST_KEY])["filings"] == 3


def test_no_rows_at_all_stops_the_run() -> None:
    sec = _Sec(
        {
            SUBMISSIONS_URL.format(cik=1): {
                "filings": {"recent": _block([]), "files": []}
            }
        }
    )
    with pytest.raises(m.SourceFormatError, match="no 8-K"):
        m.run(
            _Store(),
            sec,
            [("X", 1)],
            start=dt.date(2016, 1, 1),
            run_date=dt.date(2026, 10, 21),
        )


def test_the_dispatcher_exposes_the_backfill_workload() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"filing-event-items-pit-backfill"' in source
    assert "python -m collectors.filing_8k_items_pit" in source
