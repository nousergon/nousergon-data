"""collectors/insider_transactions_pit.py — SEC quarterly Form 3/4/5 data sets."""

from __future__ import annotations

import datetime as dt
import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from collectors import insider_transactions_pit as m
from collectors.edgar_pit_fundamentals import SecFetchError

_REPO = Path(__file__).resolve().parents[1]

_SUBMISSION = [
    (
        "ACCESSION_NUMBER",
        "FILING_DATE",
        "PERIOD_OF_REPORT",
        "DATE_OF_FILING_DATE_CHANGE",
        "DOCUMENT_TYPE",
        "ISSUERCIK",
        "ISSUERNAME",
        "ISSUERTRADINGSYMBOL",
    ),
    (
        "0001-24-000001",
        "05-FEB-2024",
        "01-FEB-2024",
        "",
        "4",
        "320193",
        "Apple Inc.",
        "aapl",
    ),
    (
        "0001-24-000002",
        "06-FEB-2024",
        "02-FEB-2024",
        "",
        "4/A",
        "1045810",
        "NVIDIA CORP",
        "NVDA",
    ),
    (
        "0001-24-000003",
        "07-FEB-2024",
        "07-FEB-2024",
        "",
        "3",
        "320193",
        "Apple Inc.",
        "AAPL",
    ),
]
_OWNERS = [
    (
        "ACCESSION_NUMBER",
        "RPTOWNERCIK",
        "RPTOWNERNAME",
        "RPTOWNER_RELATIONSHIP",
        "RPTOWNER_TITLE",
    ),
    ("0001-24-000001", "111", "Cook Timothy", "Director,Officer", "CEO"),
    ("0001-24-000002", "222", "Fund A", "TenPercentOwner", ""),
    ("0001-24-000002", "333", "Fund B", "TenPercentOwner", ""),
    ("0001-24-000003", "444", "New Director", "Director", ""),
]
_TRANS = [
    (
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
    (
        "0001-24-000001",
        "10",
        "Common Stock",
        "01-FEB-2024",
        "S",
        "1000",
        "185.5",
        "D",
        "3000000",
        "D",
    ),
    (
        "0001-24-000001",
        "11",
        "Common Stock",
        "01-FEB-2024",
        "M",
        "500",
        "",
        "A",
        "3000500",
        "D",
    ),
    (
        "0001-24-000002",
        "20",
        "Common Stock",
        "02-FEB-2024",
        "P",
        "200",
        "650",
        "A",
        "9000",
        "I",
    ),
    (
        "0001-24-000003",
        "30",
        "Common Stock",
        "07-FEB-2024",
        "J",
        "1",
        "0",
        "A",
        "1",
        "D",
    ),
]


def _tsv(rows) -> str:
    return "\n".join("\t".join(row) for row in rows) + "\n"


def _zip(
    submission=_SUBMISSION, owners=_OWNERS, trans=_TRANS, prefix="2024q1_form345/"
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{prefix}SUBMISSION.tsv", _tsv(submission))
        archive.writestr(f"{prefix}REPORTINGOWNER.tsv", _tsv(owners))
        archive.writestr(f"{prefix}NONDERIV_TRANS.tsv", _tsv(trans))
        archive.writestr(f"{prefix}FORM_345_metadata.json", "{}")
    return buffer.getvalue()


class _Store:
    def __init__(self, keys=()) -> None:
        self.objects: dict[str, bytes] = {key: b"" for key in keys}

    def list_keys(self, prefix: str) -> list[str]:
        return [key for key in self.objects if key.startswith(prefix)]

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None:
        self.objects[key] = body


class _Response:
    def __init__(self, content: bytes) -> None:
        self.content = content


class _Sec:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.urls: list[str] = []

    def get(self, url: str, *, timeout: float = 0) -> _Response:
        self.urls.append(url)
        if url not in self.payloads:
            raise SecFetchError(f"GET {url} -> HTTP 404", status=404)
        return _Response(self.payloads[url])


def _url(year: int, quarter: int, template: int = 0) -> str:
    return m.DATASET_URLS[template].format(year=year, quarter=quarter)


def _bulk_trans(n: int, *, accession: str = "0001-24-000001", bad=None) -> list:
    """`n` Table I rows on one filing; `bad` maps a row index to the
    (column, raw value) that replaces its well-formed value."""
    header = _TRANS[0]
    rows = [header]
    for i in range(n):
        row = dict(zip(header, _TRANS[1]))
        row["ACCESSION_NUMBER"] = accession
        row["NONDERIV_TRANS_SK"] = str(1000 + i)
        if bad and i in bad:
            column, value = bad[i]
            row[column] = value
        rows.append(tuple(row[c] for c in header))
    return rows


class TestQuarters:
    def test_range_and_last_complete(self) -> None:
        assert m.quarters_between((2018, 3), (2019, 2)) == [
            (2018, 3),
            (2018, 4),
            (2019, 1),
            (2019, 2),
        ]
        assert m.last_complete_quarter(dt.date(2026, 10, 19)) == (2026, 3)
        assert m.last_complete_quarter(dt.date(2026, 2, 1)) == (2025, 4)
        assert m.parse_quarter("2018Q1") == (2018, 1)
        with pytest.raises(ValueError):
            m.parse_quarter("2018-01")


class TestParse:
    def test_keeps_form4_table_i_rows_with_owner_roles(self) -> None:
        frame, stats, quarantined = m.parse_dataset(_zip())
        assert quarantined.empty and stats["rows_quarantined"] == 0
        assert tuple(frame.columns) == m.ROW_COLUMNS
        assert stats["form4_submissions"] == 2
        assert list(frame["transaction_sk"]) == [10, 11, 20], (
            "the Form 3 row is out of scope"
        )
        apple = frame[frame["transaction_sk"] == 10].iloc[0]
        assert apple["filing_date"] == dt.date(2024, 2, 5)
        assert apple["issuer_symbol"] == "AAPL"
        assert (
            apple["is_director"]
            and apple["is_officer"]
            and not apple["is_ten_percent_owner"]
        )
        assert apple["shares"] == 1000 and apple["price_per_share"] == pytest.approx(
            185.5
        )
        assert pd.isna(frame[frame["transaction_sk"] == 11].iloc[0]["price_per_share"])
        nvidia = frame[frame["transaction_sk"] == 20].iloc[0]
        assert nvidia["form"] == "4/A"
        assert nvidia["owner_count"] == 2 and nvidia["owner_ciks"] == "222;333"
        assert nvidia["is_ten_percent_owner"]

    def test_a_missing_column_stops_the_run(self) -> None:
        trans = [tuple(c for c in row[:-1]) for row in _TRANS]
        with pytest.raises(m.SourceFormatError, match="DIRECT_INDIRECT_OWNERSHIP"):
            m.parse_dataset(_zip(trans=trans))

    def test_an_unparseable_filing_date_above_the_ceiling_stops_the_run(
        self,
    ) -> None:
        # 2 of 3 rows is far above QUARANTINE_MAX_FRACTION: a format change.
        submission = list(_SUBMISSION)
        submission[1] = ("0001-24-000001", "2024/02/05", *submission[1][2:])
        with pytest.raises(m.SourceFormatError, match="FILING_DATE=2"):
            m.parse_dataset(_zip(submission=submission))

    def test_one_bad_trans_date_in_a_large_quarter_is_quarantined(self) -> None:
        # alpha-engine-config-I12461: 2014q3 carried ONE unparseable TRANS_DATE
        # and the strict parser aborted ~50 quarters of backfill on it.
        trans = _bulk_trans(2000, bad={7: ("TRANS_DATE", "31-JUN-2O24")})
        frame, stats, quarantined = m.parse_dataset(_zip(trans=trans))
        assert len(frame) == 1999 and 1007 not in set(frame["transaction_sk"])
        assert stats["rows_quarantined"] == 1
        assert stats["quarantined_by_column"] == {
            "FILING_DATE": 0,
            "PERIOD_OF_REPORT": 0,
            "TRANS_DATE": 1,
        }
        assert quarantined.to_dict(orient="records") == [
            {
                "accession": "0001-24-000001",
                "transaction_sk": "1007",
                "column": "TRANS_DATE",
                "raw_value": "31-JUN-2O24",
            }
        ]
        assert frame["trans_date"].notna().all()

    def test_a_bad_period_of_report_is_quarantined_like_trans_date(self) -> None:
        submission = list(_SUBMISSION)
        submission[1] = (*submission[1][:2], "1-FEBR-2024", *submission[1][3:])
        trans = _bulk_trans(2000, accession="0001-24-000002")
        trans.append(_TRANS[1])
        frame, stats, quarantined = m.parse_dataset(
            _zip(submission=submission, trans=trans)
        )
        assert stats["rows_quarantined"] == 1 and len(frame) == 2000
        assert list(quarantined["column"]) == ["PERIOD_OF_REPORT"]
        assert "0001-24-000001" not in set(frame["accession"])

    def test_a_whole_unparseable_column_still_stops_the_run(self) -> None:
        bad = {i: ("TRANS_DATE", "2024.02.01") for i in range(2000)}
        with pytest.raises(m.SourceFormatError, match="TRANS_DATE=2000"):
            m.parse_dataset(_zip(trans=_bulk_trans(2000, bad=bad)))

    def test_just_above_the_ceiling_stops_the_run(self) -> None:
        # 3 of 2000 = 0.15% > 0.1%; 2 of 2000 = 0.1% is allowed.
        bad = {i: ("TRANS_DATE", "??") for i in range(3)}
        with pytest.raises(m.SourceFormatError, match="quarantine ceiling"):
            m.parse_dataset(_zip(trans=_bulk_trans(2000, bad=bad)))
        bad = {i: ("TRANS_DATE", "??") for i in range(2)}
        _, stats, _ = m.parse_dataset(_zip(trans=_bulk_trans(2000, bad=bad)))
        assert stats["rows_quarantined"] == 2

    def test_a_blank_trans_date_is_not_quarantined(self) -> None:
        bad = {0: ("TRANS_DATE", " ")}
        frame, stats, _ = m.parse_dataset(_zip(trans=_bulk_trans(10, bad=bad)))
        assert stats["rows_quarantined"] == 0 and len(frame) == 10

    def test_rows_filed_outside_the_quarter_are_refused(self) -> None:
        frame, _, _ = m.parse_dataset(_zip())
        m.check_quarter_bounds(frame, 2024, 1)
        with pytest.raises(m.SourceFormatError, match="outside"):
            m.check_quarter_bounds(frame, 2024, 2)


class TestRun:
    def test_writes_each_quarter_and_a_run_record(self) -> None:
        store, sec = _Store(), _Sec({_url(2024, 1): _zip()})
        summary = m.run(
            store,
            sec,
            [(2024, 1)],
            run_date=dt.date(2026, 10, 19),
            overwrite=False,
            newest_may_be_unpublished=False,
            run_id="r1",
        )
        assert summary["quarters_written"] == 1 and summary["rows_written"] == 3
        frame = pd.read_parquet(io.BytesIO(store.objects[m.quarter_key(2024, 1)]))
        assert len(frame) == 3
        assert json.loads(store.objects[m.RUN_LATEST_KEY])["run_id"] == "r1"
        assert m.RUNS_PREFIX + "2026-10-19/r1.json" in store.objects

    def test_quarantine_is_written_before_the_parquet_and_counted(self) -> None:
        trans = _bulk_trans(2000, bad={3: ("TRANS_DATE", "00-JAN-2024")})
        store, sec = _Store(), _Sec({_url(2024, 1): _zip(trans=trans)})
        writes: list[str] = []
        put = store.put_bytes
        store.put_bytes = lambda k, b, c: (writes.append(k), put(k, b, c))
        summary = m.run(
            store,
            sec,
            [(2024, 1)],
            run_date=dt.date(2026, 10, 21),
            overwrite=False,
            newest_may_be_unpublished=False,
            run_id="r2",
        )
        qkey = m.quarantine_key(2024, 1)
        assert writes.index(qkey) < writes.index(m.quarter_key(2024, 1))
        record = json.loads(store.objects[qkey])
        assert record["rows_quarantined"] == 1 and record["rows_written"] == 1999
        assert record["records"][0]["raw_value"] == "00-JAN-2024"
        assert summary["rows_quarantined"] == 1 and summary["rows_written"] == 1999
        latest = json.loads(store.objects[m.RUN_LATEST_KEY])
        assert latest["quarters"]["2024q1"]["quarantine_key"] == qkey

    def test_a_clean_quarter_writes_no_quarantine_record(self) -> None:
        store = _Store()
        m.run(
            store,
            _Sec({_url(2024, 1): _zip()}),
            [(2024, 1)],
            run_date=dt.date(2026, 10, 21),
            overwrite=False,
            newest_may_be_unpublished=False,
        )
        assert store.list_keys(m.QUARANTINE_PREFIX) == []
        assert json.loads(store.objects[m.RUN_LATEST_KEY])["rows_quarantined"] == 0

    def test_a_rerun_resumes_past_quarters_already_written(self) -> None:
        # The 10-20 23:24Z run left 2014q1/q2 and no manifest; a rerun without
        # --overwrite must skip what exists and carry on from the next quarter.
        store = _Store([m.quarter_key(2023, 4)])
        sec = _Sec({_url(2024, 1): _zip()})
        summary = m.run(
            store,
            sec,
            [(2023, 4), (2024, 1)],
            run_date=dt.date(2026, 10, 21),
            overwrite=False,
            newest_may_be_unpublished=False,
        )
        assert sec.urls == [_url(2024, 1)]
        assert summary["quarters_skipped_existing"] == 1
        assert summary["quarters_written"] == 1

    def test_existing_quarters_are_skipped_unless_overwriting(self) -> None:
        store = _Store([m.quarter_key(2024, 1)])
        sec = _Sec({_url(2024, 1): _zip()})
        summary = m.run(
            store,
            sec,
            [(2024, 1)],
            run_date=dt.date(2026, 10, 19),
            overwrite=False,
            newest_may_be_unpublished=False,
        )
        assert summary["quarters_skipped_existing"] == 1 and sec.urls == []

    def test_only_the_newest_quarter_may_be_unpublished(self) -> None:
        sec = _Sec({_url(2024, 1): _zip()})
        summary = m.run(
            _Store(),
            sec,
            [(2024, 1), (2024, 2)],
            run_date=dt.date(2026, 10, 19),
            overwrite=True,
            newest_may_be_unpublished=True,
        )
        assert summary["quarters_unpublished"] == ["2024q2"]
        with pytest.raises(SecFetchError):
            m.run(
                _Store(),
                _Sec({}),
                [(2024, 1), (2024, 2)],
                run_date=dt.date(2026, 10, 19),
                overwrite=True,
                newest_may_be_unpublished=True,
            )


class TestDatasetUrls:
    def test_the_url_names_sec_s_insider_transactions_data_sets_path(self) -> None:
        # The 2026-10-20 backfill 404'd on `form-345-data-sets`, a path SEC
        # never served; the zip lives under `insider-transactions-data-sets`.
        assert _url(2014, 1) == (
            "https://www.sec.gov/files/structureddata/data/"
            "insider-transactions-data-sets/2014q1_form345.zip"
        )
        assert all("form-345-data-sets" not in u for u in m.DATASET_URLS)

    def test_a_404_falls_through_to_the_next_template(self) -> None:
        store = _Store()
        sec = _Sec({_url(2024, 1, template=1): _zip()})
        summary = m.run(
            store,
            sec,
            [(2024, 1)],
            run_date=dt.date(2026, 10, 19),
            overwrite=False,
            newest_may_be_unpublished=False,
        )
        assert sec.urls == [_url(2024, 1, 0), _url(2024, 1, 1)]
        assert summary["quarters"]["2024q1"]["source_url"] == _url(2024, 1, 1)
        assert m.quarter_key(2024, 1) in store.objects

    def test_a_non_404_failure_does_not_fall_through(self) -> None:
        class _Down(_Sec):
            def get(self, url: str, *, timeout: float = 0) -> _Response:
                self.urls.append(url)
                raise SecFetchError(f"GET {url} -> HTTP 503", status=503)

        sec = _Down({})
        with pytest.raises(SecFetchError) as raised:
            m.fetch_dataset(sec, 2024, 1)
        assert raised.value.status == 503 and sec.urls == [_url(2024, 1, 0)]

    def test_a_quarter_missing_everywhere_mid_range_is_fatal(self) -> None:
        sec = _Sec({_url(2024, 1): _zip(), _url(2024, 3): _zip()})
        with pytest.raises(SecFetchError) as raised:
            m.run(
                _Store(),
                sec,
                [(2024, 1), (2024, 2), (2024, 3)],
                run_date=dt.date(2026, 10, 19),
                overwrite=True,
                newest_may_be_unpublished=True,
            )
        assert raised.value.status == 404 and "2024q2" in str(raised.value)


def test_the_dispatcher_exposes_the_backfill_workload() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"insider-transactions-pit-backfill"' in source
    assert (
        "python -m collectors.insider_transactions_pit backfill --start 2014q1"
        in source
    )
