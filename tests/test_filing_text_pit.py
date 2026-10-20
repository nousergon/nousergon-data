"""collectors/filing_text_pit.py — per-issuer 10-K / 10-Q text history."""

from __future__ import annotations

import datetime as dt
import io
import json
from pathlib import Path

import pandas as pd
import pytest

from collectors import filing_text_pit as m
from collectors.edgar_pit_fundamentals import SecFetchError

_REPO = Path(__file__).resolve().parents[1]


def _para(topic: str, n: int) -> str:
    return (
        "<p>"
        + f"This paragraph discusses {topic} in enough detail to be a real section. "
        * n
        + "</p>"
    )


_TEN_K = f"""<html><body>
<div>Item 1. Business</div>{_para("our products", 20)}
<div>Item 1A. Risk Factors</div>{_para("the risks to our business", 1200)}
<div>Item 1B. Unresolved Staff Comments</div><p>None.</p>
<div>Item 7. Management's Discussion and Analysis of Financial Condition</div>{_para("results", 30)}
<div>Item 8. Financial Statements</div>{_para("the balance sheet", 20)}
</body></html>"""


def _block(rows):
    keys = ("accessionNumber", "filingDate", "reportDate", "form", "primaryDocument")
    return {key: [row[i] for row in rows] for i, key in enumerate(keys)}


_RECENT = _block(
    [
        ("0000320193-24-000001", "2024-11-01", "2024-09-28", "10-K", "aapl-2024.htm"),
        ("0000320193-24-000002", "2024-08-02", "2024-06-29", "10-Q", "aapl-q3.htm"),
        ("0000320193-24-000003", "2024-08-03", "2024-08-01", "4", "form4.xml"),
        ("0000320193-24-000004", "2024-08-04", "2024-06-29", "10-Q/A", "aapl-q3a.htm"),
    ]
)
_OLD_PAGE = _block(
    [
        ("0000320193-17-000001", "2017-11-03", "2017-09-30", "10-K", "aapl-2017.htm"),
        ("0000320193-14-000001", "2014-10-27", "2014-09-27", "10-K", "aapl-2014.htm"),
    ]
)


class _Response:
    def __init__(self, text: str) -> None:
        self.text = text


class _Sec:
    def __init__(self, *, missing: set[str] = frozenset(), pages=None) -> None:
        self.missing = missing
        self.pages = (
            pages
            if pages is not None
            else [
                {
                    "name": "CIK0000320193-submissions-001.json",
                    "filingFrom": "2010-01-01",
                    "filingTo": "2018-01-01",
                },
                {
                    "name": "CIK0000320193-submissions-002.json",
                    "filingFrom": "2001-01-01",
                    "filingTo": "2009-12-31",
                },
            ]
        )
        self.urls: list[str] = []

    def get_json(self, url: str):
        self.urls.append(url)
        if url == m.SUBMISSIONS_URL.format(cik=320193):
            return {"filings": {"recent": _RECENT, "files": self.pages}}
        if url.endswith("submissions-001.json"):
            return _OLD_PAGE
        raise AssertionError(f"unexpected {url}")

    def get(self, url: str, **_):
        self.urls.append(url)
        if any(url.endswith(doc) for doc in self.missing):
            raise SecFetchError(f"GET {url} -> HTTP 404", status=404)
        return _Response(_TEN_K)


class _Store:
    def __init__(self, keys=()) -> None:
        self.objects: dict[str, bytes] = {key: b"" for key in keys}

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def put_bytes(self, key, body, content_type):
        self.objects[key] = body

    def get_bytes(self, key):
        return self.objects[key]


_START, _END = dt.date(2016, 1, 1), dt.date(2026, 10, 19)


class TestDiscover:
    def test_reads_older_pages_and_keeps_original_reports_in_range(self) -> None:
        sec = _Sec()
        filings = m.discover(sec, 320193, start=_START, end=_END)
        assert [f.accession for f in filings] == [
            "0000320193-17-000001",
            "0000320193-24-000002",
            "0000320193-24-000001",
        ], "Form 4, the 10-Q/A and the 2014 10-K are out"
        assert not any(u.endswith("submissions-002.json") for u in sec.urls), (
            "a page that ends before --start is not fetched"
        )
        assert filings[-1].url.endswith("/320193/000032019324000001/aapl-2024.htm")

    def test_ragged_columns_are_refused(self) -> None:
        bad = dict(_RECENT, primaryDocument=["a.htm"])
        with pytest.raises(m.FilingTextError, match="primaryDocument"):
            m.filings_in_block(320193, bad, start=_START, end=_END)


class TestRun:
    def test_writes_one_parquet_per_issuer_with_untruncated_sections(self) -> None:
        store = _Store()
        summary = m.run(
            store,
            _Sec(),
            [("AAPL", 320193)],
            start=_START,
            end=_END,
            run_date=_END,
            overwrite=False,
            run_id="r1",
        )
        assert summary["issuers_written"] == 1 and summary["filings_written"] == 3
        frame = pd.read_parquet(io.BytesIO(store.objects[m.issuer_key(320193)]))
        assert tuple(frame.columns) == m.ROW_COLUMNS
        assert list(frame["filing_date"]) == sorted(frame["filing_date"])
        row = frame.iloc[0]
        assert row["ticker"] == "AAPL" and row["form"] == "10-K"
        assert len(row["risk_factors_text"]) > 50_000, (
            "the RAG's 50k cap is not applied"
        )
        assert row["mdna_text"] and row["business_text"]
        assert row["market_risk_text"] is None and row["market_risk_sha256"] is None
        assert len(row["document_sha256"]) == 64
        assert json.loads(store.objects[m.RUN_LATEST_KEY])["run_id"] == "r1"

    def test_an_issuer_with_a_failed_download_is_not_written(self) -> None:
        store = _Store()
        issuers = [("AAPL", 320193)] + [(f"X{i}", 320193) for i in range(25)]
        sec = _Sec(missing={"aapl-q3.htm"})
        with pytest.raises(m.FilingTextError, match="issuers failed"):
            m.run(
                store,
                sec,
                issuers,
                start=_START,
                end=_END,
                run_date=_END,
                overwrite=True,
            )
        assert m.issuer_key(320193) not in store.objects
        assert (
            json.loads(store.objects[m.RUN_LATEST_KEY])["issuers_failed"][0] == "AAPL"
        )

    def test_existing_issuers_are_skipped_unless_overwriting(self) -> None:
        store, sec = _Store([m.issuer_key(320193)]), _Sec()
        summary = m.run(
            store,
            sec,
            [("AAPL", 320193)],
            start=_START,
            end=_END,
            run_date=_END,
            overwrite=False,
        )
        assert summary["issuers_skipped_existing"] == 1 and sec.urls == []

    def test_a_cover_page_is_not_a_report(self) -> None:
        filing = m.Filing(1, "a", "10-K", _END, None, "x.htm")
        with pytest.raises(m.FilingTextError, match="chars"):
            m.filing_row(filing, "T", "<html><body><p>Cover page.</p></body></html>")


def test_issuers_come_from_the_newest_edgar_facts_run() -> None:
    store = _Store()
    facts = pd.DataFrame({"ticker": ["B", "A", "A"], "cik": [2, 1, 1]})
    buffer = io.BytesIO()
    facts.to_parquet(buffer, index=False)
    store.objects["facts.parquet"] = buffer.getvalue()
    store.objects[m.EDGAR_RUN_LATEST_KEY] = json.dumps(
        {"facts_key": "facts.parquet"}
    ).encode()
    assert m.issuers_from_edgar_facts(store) == [("A", 1), ("B", 2)]


def test_the_dispatcher_exposes_the_backfill_workload() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"filing-text-pit-backfill"' in source
    assert "python -m collectors.filing_text_pit backfill" in source


def test_each_filing_is_parsed_once(monkeypatch) -> None:
    """Parsing the HTML is nearly all of the backfill's time, so do it once."""
    from rag.pipelines import ingest_sec_filings as rag

    calls = []
    real = rag._html_to_text
    monkeypatch.setattr(
        rag, "_html_to_text", lambda html: calls.append(1) or real(html)
    )
    row = m.filing_row(m.Filing(1, "a", "10-K", _END, None, "x.htm"), "T", _TEN_K)
    assert len(calls) == 1
    assert row["risk_factors_text"] and row["mdna_text"]


def test_the_backfill_declares_a_runtime_cap_that_fits_it() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"filing-text-pit-backfill": 43200,' in source
