"""collectors/holdings_13f_pit.py — SEC Form 13F data sets."""

from __future__ import annotations

import datetime as dt
import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from collectors import holdings_13f_pit as m

_REPO = Path(__file__).resolve().parents[1]

_LISTING = """
<a href="/files/structureddata/data/form-13f-data-sets/2015q4_form13f.zip">2015 Q4</a>
<a href="/files/structureddata/data/form-13f-data-sets/2022q4_form13f.zip">2022 Q4</a>
<a href="https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01dec2024-28feb2025_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2023q1_form13f.zip">2023 Q1</a>
"""


def _tsv(rows) -> str:
    return "\n".join("\t".join(r) for r in rows) + "\n"


def _zip(filing_dates=("15-NOV-2022", "10-FEB-2023"), label="x") -> bytes:
    submission = [
        (
            "ACCESSION_NUMBER",
            "FILING_DATE",
            "SUBMISSIONTYPE",
            "CIK",
            "PERIODOFREPORT",
            "EXTRA",
        ),
        ("0001-22-1", filing_dates[0], "13F-HR", "1000", "30-SEP-2022", "z"),
        ("0001-23-1", filing_dates[1], "13F-HR/A", "2000", "31-DEC-2022", "z"),
    ]
    cover = [
        ("ACCESSION_NUMBER", "ISAMENDMENT", "AMENDMENTTYPE", "FILINGMANAGER_NAME"),
        ("0001-22-1", "N", "", "Fund A"),
        ("0001-23-1", "Y", "RESTATEMENT", "Fund B"),
    ]
    info = [
        (
            "ACCESSION_NUMBER",
            "INFOTABLE_SK",
            "NAMEOFISSUER",
            "TITLEOFCLASS",
            "CUSIP",
            "VALUE",
            "SSHPRNAMT",
            "SSHPRNAMTTYPE",
            "PUTCALL",
            "INVESTMENTDISCRETION",
            "VOTING_AUTH_SOLE",
        ),
        (
            "0001-22-1",
            "1",
            "APPLE INC",
            "COM",
            "037833100",
            "500",
            "3000",
            "SH",
            "",
            "SOLE",
            "1",
        ),
        (
            "0001-23-1",
            "2",
            "APPLE INC",
            "COM",
            "037833100",
            "600000",
            "4000",
            "SH",
            "",
            "DFND",
            "0",
        ),
        (
            "0001-23-1",
            "3",
            "APPLE INC",
            "COM",
            "037833100",
            "10000",
            "100",
            "SH",
            "Put",
            "SOLE",
            "0",
        ),
    ]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{label}/SUBMISSION.tsv", _tsv(submission))
        archive.writestr(f"{label}/COVERPAGE.tsv", _tsv(cover))
        archive.writestr(f"{label}/INFOTABLE.tsv", _tsv(info))
    return buffer.getvalue()


class TestListing:
    def test_both_label_styles_are_listed_oldest_first(self) -> None:
        listed = m.list_archives(_LISTING)
        assert [label for label, _ in listed] == [
            "2015q4",
            "2022q4",
            "2023q1",
            "01dec2024-28feb2025",
        ]
        assert listed[0][1].startswith("https://www.sec.gov/files/")
        assert m.archive_span("01dec2024-28feb2025") == (
            dt.date(2024, 12, 1),
            dt.date(2025, 2, 28),
        )
        assert m.archive_span("2023q1") == (dt.date(2023, 1, 1), dt.date(2023, 3, 31))

    def test_start_filters_by_window_end(self) -> None:
        kept = m.select_archives(m.list_archives(_LISTING), dt.date(2016, 1, 1))
        assert [label for label, _ in kept][0] == "2022q4"

    def test_an_unknown_label_or_empty_page_stops_the_run(self) -> None:
        with pytest.raises(m.SourceFormatError):
            m.archive_span("2024-annual")
        with pytest.raises(m.SourceFormatError, match="links no"):
            m.list_archives("<html></html>")


class TestParse:
    def test_joins_and_normalises_value_across_the_2023_unit_change(self) -> None:
        frame, stats = m.parse_archive(_zip(), "01oct2022-28feb2023")
        assert tuple(frame.columns) == m.ROW_COLUMNS
        assert stats["rows"] == 3 and stats["rows_outside_window"] == 0
        old = frame[frame["accession"] == "0001-22-1"].iloc[0]
        assert old["value_usd"] == 500_000.0, "pre-2023-01-03 VALUE is in thousands"
        new = frame[frame["accession"] == "0001-23-1"]
        assert set(new["value_usd"]) == {600_000.0, 10_000.0}
        assert new["is_amendment"].all() and set(new["amendment_type"]) == {
            "RESTATEMENT"
        }
        assert set(new["put_call"]) == {"", "PUT"}
        assert old["period_of_report"] == dt.date(2022, 9, 30)

    def test_many_rows_outside_the_window_stop_the_run(self) -> None:
        with pytest.raises(m.SourceFormatError, match="outside"):
            m.parse_archive(_zip(), "2015q4")

    def test_an_unreadable_filing_date_stops_the_run(self) -> None:
        with pytest.raises(m.SourceFormatError, match="unreadable"):
            m.parse_archive(
                _zip(filing_dates=("2022/11/15", "bad")), "01oct2022-28feb2023"
            )


class _Response:
    def __init__(self, content: bytes) -> None:
        self.content = content


class _Client:
    def __init__(self, payloads) -> None:
        self.payloads, self.urls = payloads, []

    def get(self, url, **_):
        self.urls.append(url)
        return _Response(self.payloads[url])


class _Store:
    def __init__(self, keys=()) -> None:
        self.objects = {k: b"" for k in keys}

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def put_bytes(self, key, body, content_type):
        self.objects[key] = body


def test_run_writes_one_parquet_per_archive_and_skips_existing() -> None:
    store = _Store()
    summary = m.run(
        store,
        _Client({"u": _zip()}),
        [("01oct2022-28feb2023", "u")],
        run_date=dt.date(2026, 10, 21),
        overwrite=False,
        run_id="r1",
    )
    assert summary["archives_written"] == 1
    assert (
        len(
            pd.read_parquet(
                io.BytesIO(store.objects[m.archive_key("01oct2022-28feb2023")])
            )
        )
        == 3
    )
    assert json.loads(store.objects[m.RUN_LATEST_KEY])["run_id"] == "r1"
    client = _Client({})
    again = m.run(
        store,
        client,
        [("01oct2022-28feb2023", "u")],
        run_date=dt.date(2026, 10, 21),
        overwrite=False,
    )
    assert again["archives_skipped_existing"] == 1 and client.urls == []


def test_the_dispatcher_exposes_the_backfill_workload() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"institutional-holdings-pit-backfill"' in source
    assert "python -m collectors.holdings_13f_pit backfill --start 2016-01-01" in source


def test_chunked_reading_gives_the_same_rows() -> None:
    whole, _ = m.parse_archive(_zip(), "01oct2022-28feb2023")
    chunked, stats = m.parse_archive(_zip(), "01oct2022-28feb2023", chunk_rows=1)
    assert stats["infotable_rows"] == 3
    pd.testing.assert_frame_equal(whole, chunked)


# -- memory: one archive per child process (2026-10-23 OOM, exit 137) ----------


def test_the_parquet_is_the_v1_schema_sorted_one_row_group_at_a_time() -> None:
    import pyarrow.parquet as pq

    table, _ = m.parse_archive_table(_zip(), "01oct2022-28feb2023")
    body = m.archive_parquet(table, row_group_rows=2)
    parquet = pq.ParquetFile(io.BytesIO(body))
    assert parquet.schema_arrow.remove_metadata().equals(m.ARROW_SCHEMA)
    assert parquet.metadata.num_row_groups == 2
    frame = pd.read_parquet(io.BytesIO(body))
    keys = ["filing_date", "accession", "cusip"]
    assert frame[keys].equals(frame.sort_values(keys)[keys].reset_index(drop=True))
    whole, _ = m.parse_archive(_zip(), "01oct2022-28feb2023")
    pd.testing.assert_frame_equal(
        frame.astype(str), whole.astype(str).reset_index(drop=True)
    )


class _Done:
    def __init__(self, returncode: int, stdout: str = "") -> None:
        self.returncode, self.stdout = returncode, stdout


def test_a_child_runs_this_module_on_one_archive_and_returns_its_stats(
    monkeypatch,
) -> None:
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return _Done(0, 'progress\n{"rows": 7, "submissions": 2}\n')

    monkeypatch.setattr(m.subprocess, "run", fake_run)
    stats = m.process_in_child("2023q1", "https://x/2023q1_form13f.zip", bucket="b")
    assert stats == {"rows": 7, "submissions": 2}
    (cmd, kwargs), = calls
    assert cmd[1:] == [
        "-m",
        "collectors.holdings_13f_pit",
        "--bucket",
        "b",
        "archive",
        "--label",
        "2023q1",
        "--url",
        "https://x/2023q1_form13f.zip",
    ]
    assert kwargs["cwd"] == _REPO and "stderr" not in kwargs, "child logs reach the box log"


def test_the_child_command_line_is_one_this_module_accepts() -> None:
    import subprocess
    import sys

    cmd = m.child_command("2023q1", "https://x/2023q1_form13f.zip", "b")
    done = subprocess.run(
        [*cmd, "--help"], cwd=_REPO, capture_output=True, text=True, check=False
    )
    assert done.returncode == 0, done.stderr
    assert "--label" in done.stdout and cmd[0] == sys.executable


def test_an_oom_killed_child_stops_the_run_and_leaves_a_run_record(
    monkeypatch,
) -> None:
    monkeypatch.setattr(m.subprocess, "run", lambda cmd, **_: _Done(-9))
    store = _Store()
    with pytest.raises(m.ArchiveProcessError, match="signal 9.*OOM"):
        m.run(
            store,
            _Client({}),
            [("2023q1", "u")],
            run_date=dt.date(2026, 10, 23),
            overwrite=False,
            run_id="r9",
            process=lambda label, url: m.process_in_child(label, url, bucket="b"),
        )
    record = json.loads(store.objects[m.RUN_LATEST_KEY])
    assert record["failed_archive"] == "2023q1" and record["run_id"] == "r9"
    assert record["failure"] == "ArchiveProcessError"


def test_backfill_resumes_and_hands_each_archive_to_its_own_child(
    monkeypatch,
) -> None:
    """The parent lists, skips what is written, and never downloads an archive."""

    class _Listing:
        def __init__(self) -> None:
            self.urls = []

        def get(self, url, **_):
            self.urls.append(url)
            response = _Response(b"")
            response.text = _LISTING
            return response

    sec = _Listing()
    store = _Store(keys=[m.archive_key("2022q4")])
    children = []

    def fake_child(label, url, *, bucket):
        children.append((label, bucket))
        return {"rows": 1, "submissions": 1}

    monkeypatch.setattr(m, "SecClient", lambda: sec)
    monkeypatch.setattr(m, "S3ObjectStore", lambda client, bucket: store)
    monkeypatch.setattr("boto3.client", lambda *a, **k: None)
    monkeypatch.setattr(m, "process_in_child", fake_child)
    assert m.main(["--bucket", "bk", "backfill", "--start", "2016-01-01"]) == 0
    assert children == [("2023q1", "bk"), ("01dec2024-28feb2025", "bk")]
    assert sec.urls == [m.LISTING_URL], "archives are fetched by the child only"
    record = json.loads(store.objects[m.RUN_LATEST_KEY])
    assert record["archives_skipped_existing"] == 1
    assert record["archives_written"] == 2
