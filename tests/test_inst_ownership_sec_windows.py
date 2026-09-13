"""Tests for the SEC 13F filing-window discovery and parsing path
(alpha-engine-config-I10529).

The old ``SEC_13F_BASE_URL`` scheme
(``.../dera/data/form-13f-data-sets/{YYYYq1}/{YYYYq1}.zip``) 404s for
every quarter — SEC moved this data set under ``structureddata`` and,
from 2024 on, publishes three-month FILING WINDOWS (not calendar
quarters) that mix report periods. Covers:

1. Deterministic window-filename generation, including a year boundary
   and a leap year (measured against the real SEC naming, 2026-09-13).
2. Index-page discovery from a saved HTML excerpt.
3. PERIODOFREPORT selection with an amendment superseding the original.
4. INFOTABLE/SUBMISSION parsing against a small synthetic TSV in the
   measured 2024+ header format (real headers measured against
   01mar2026-31may2026_form13f.zip, downloaded 2026-09-13).
"""

from __future__ import annotations

import io
import zipfile
from datetime import date as Date
from pathlib import Path

import pandas as pd
import pytest

from data.derived.inst_ownership import (
    _candidate_window_filenames,
    _dedupe_amendments,
    _download_recent_windows,
    _is_leap_year,
    _parse_infotable,
    _parse_submission,
    _prior_window,
    _quarter_str_for_date,
    _recent_window_filenames,
    _select_report_periods,
    _window_end_date,
    _window_filename,
    _window_index_for_date,
    discover_window_filenames_from_html,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Real INFOTABLE.tsv header measured 2026-09-13 against the SEC's
# 01mar2026-31may2026_form13f.zip (see module docstring).
_INFOTABLE_HEADER = (
    "ACCESSION_NUMBER\tINFOTABLE_SK\tNAMEOFISSUER\tTITLEOFCLASS\tCUSIP\tFIGI"
    "\tVALUE\tSSHPRNAMT\tSSHPRNAMTTYPE\tPUTCALL\tINVESTMENTDISCRETION"
    "\tOTHERMANAGER\tVOTING_AUTH_SOLE\tVOTING_AUTH_SHARED\tVOTING_AUTH_NONE"
)
# Real SUBMISSION.tsv header measured the same way.
_SUBMISSION_HEADER = "ACCESSION_NUMBER\tFILING_DATE\tSUBMISSIONTYPE\tCIK\tPERIODOFREPORT"


def _make_zip(files: dict[str, str]) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    buf.seek(0)
    return zipfile.ZipFile(buf)


# ═══════════════════════════════════════════════════════════════════
# Window-filename generation
# ═══════════════════════════════════════════════════════════════════


class TestWindowFilenameGeneration:
    def test_matches_measured_sec_naming(self):
        # Measured live 2026-09-13: newest published window.
        assert _window_filename(2026, 1) == "01mar2026-31may2026_form13f.zip"
        assert _window_filename(2026, 2) == "01jun2026-31aug2026_form13f.zip"
        assert _window_filename(2026, 3) == "01sep2026-30nov2026_form13f.zip"

    def test_year_boundary_dec_feb_window_spans_two_years(self):
        # The Dec-Feb window's start year is (end year - 1).
        assert _window_filename(2026, 0) == "01dec2025-28feb2026_form13f.zip"

    def test_leap_year_dec_feb_window_ends_feb_29(self):
        assert _is_leap_year(2024) is True
        assert _window_filename(2024, 0) == "01dec2023-29feb2024_form13f.zip"

    def test_non_leap_year_dec_feb_window_ends_feb_28(self):
        assert _is_leap_year(2023) is False
        assert _window_filename(2023, 0) == "01dec2022-28feb2023_form13f.zip"

    def test_invalid_index_raises(self):
        with pytest.raises(ValueError):
            _window_filename(2026, 4)

    @pytest.mark.parametrize(
        "d, expected_year, expected_idx",
        [
            (Date(2026, 9, 13), 2026, 3),  # Sep -> window 3 (Sep-Nov), same year
            (Date(2026, 12, 1), 2027, 0),  # Dec -> window 0, END year is next year
            (Date(2026, 1, 15), 2026, 0),  # Jan -> window 0 (Dec[y-1]-Feb[y])
            (Date(2026, 6, 30), 2026, 2),  # Jun -> window 2 (Jun-Aug)
        ],
    )
    def test_window_index_for_date(self, d, expected_year, expected_idx):
        assert _window_index_for_date(d) == (expected_year, expected_idx)

    def test_prior_window_wraps_year(self):
        assert _prior_window(2026, 0) == (2025, 3)
        assert _prior_window(2026, 2) == (2026, 1)

    def test_recent_window_filenames_walks_backward_across_year_boundary(self):
        # Today in the Sep-Nov 2026 window (unpublished); walking back
        # crosses into the published Jun-Aug and Mar-May windows, then
        # back across the year boundary into 2025.
        names = _recent_window_filenames(Date(2026, 9, 13), 6)
        assert names == [
            "01sep2026-30nov2026_form13f.zip",
            "01jun2026-31aug2026_form13f.zip",
            "01mar2026-31may2026_form13f.zip",
            "01dec2025-28feb2026_form13f.zip",
            "01sep2025-30nov2025_form13f.zip",
            "01jun2025-31aug2025_form13f.zip",
        ]


# ═══════════════════════════════════════════════════════════════════
# Index-page discovery
# ═══════════════════════════════════════════════════════════════════


class TestIndexPageDiscovery:
    def test_discovers_and_sorts_most_recent_first(self):
        html = (FIXTURES_DIR / "sec_13f_index_page_excerpt.html").read_text()
        names = discover_window_filenames_from_html(html)
        assert names[0] == "01mar2026-31may2026_form13f.zip"
        assert names == [
            "01mar2026-31may2026_form13f.zip",
            "01dec2025-28feb2026_form13f.zip",
            "01sep2025-30nov2025_form13f.zip",
            "01jan2024-29feb2024_form13f.zip",
            "2023q4_form13f.zip",
            "2023q1_form13f.zip",
        ]

    def test_skips_non_zip_and_unrecognized_links(self):
        html = (FIXTURES_DIR / "sec_13f_index_page_excerpt.html").read_text()
        names = discover_window_filenames_from_html(html)
        assert "README.htm" not in " ".join(names)

    def test_empty_html_yields_no_candidates(self):
        assert discover_window_filenames_from_html("<html></html>") == []

    def test_window_end_date_legacy_quarter(self):
        assert _window_end_date("2023q4_form13f.zip") == Date(2023, 12, 31)
        assert _window_end_date("2023q1_form13f.zip") == Date(2023, 3, 31)

    def test_window_end_date_unrecognized_returns_none(self):
        assert _window_end_date("not_a_window.zip") is None

    def test_candidate_window_filenames_falls_back_when_index_unreachable(self, monkeypatch):
        monkeypatch.setattr(
            "data.derived.inst_ownership._fetch_sec_index_html", lambda: None
        )
        names = _candidate_window_filenames(today=Date(2026, 9, 13), count=3)
        assert names == _recent_window_filenames(Date(2026, 9, 13), 3)

    def test_candidate_window_filenames_uses_discovery_when_available(self, monkeypatch):
        html = (FIXTURES_DIR / "sec_13f_index_page_excerpt.html").read_text()
        monkeypatch.setattr(
            "data.derived.inst_ownership._fetch_sec_index_html", lambda: html
        )
        names = _candidate_window_filenames(count=3)
        assert names == [
            "01mar2026-31may2026_form13f.zip",
            "01dec2025-28feb2026_form13f.zip",
            "01sep2025-30nov2025_form13f.zip",
        ]


# ═══════════════════════════════════════════════════════════════════
# _download_recent_windows: skip 404s, stop at `count` successes
# ═══════════════════════════════════════════════════════════════════


class TestDownloadRecentWindows:
    def test_skips_unpublished_candidates_and_stops_at_count(self, monkeypatch):
        monkeypatch.setattr(
            "data.derived.inst_ownership._candidate_window_filenames",
            lambda count: ["future.zip", "published_a.zip", "published_b.zip", "published_c.zip"],
        )

        def fake_download(name):
            if name == "future.zip":
                return None  # unpublished (404)
            return _make_zip({"marker.txt": name})

        monkeypatch.setattr(
            "data.derived.inst_ownership._download_sec_bulk_zip", fake_download
        )
        monkeypatch.setattr("time.sleep", lambda _s: None)

        result = _download_recent_windows(count=2, max_candidates=4)
        assert [name for name, _zf in result] == ["published_a.zip", "published_b.zip"]

    def test_returns_empty_when_nothing_published(self, monkeypatch):
        monkeypatch.setattr(
            "data.derived.inst_ownership._candidate_window_filenames",
            lambda count: ["a.zip", "b.zip"],
        )
        monkeypatch.setattr(
            "data.derived.inst_ownership._download_sec_bulk_zip", lambda name: None
        )
        assert _download_recent_windows(count=2, max_candidates=2) == []


# ═══════════════════════════════════════════════════════════════════
# PERIODOFREPORT selection with amendment supersession
# ═══════════════════════════════════════════════════════════════════


class TestPeriodSelectionAndAmendments:
    def _submissions(self, rows: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        df["FILING_DATE"] = pd.to_datetime(df["FILING_DATE"])
        df["PERIODOFREPORT"] = pd.to_datetime(df["PERIODOFREPORT"])
        return df

    def test_amendment_supersedes_original_for_same_filer_and_period(self):
        submissions = self._submissions([
            {
                "ACCESSION_NUMBER": "0001-original",
                "FILING_DATE": "2026-05-15",
                "SUBMISSIONTYPE": "13F-HR",
                "CIK": "0000000001",
                "PERIODOFREPORT": "2026-03-31",
            },
            {
                "ACCESSION_NUMBER": "0001-amended",
                "FILING_DATE": "2026-06-01",
                "SUBMISSIONTYPE": "13F-HR/A",
                "CIK": "0000000001",
                "PERIODOFREPORT": "2026-03-31",
            },
            {
                "ACCESSION_NUMBER": "0002-other-filer",
                "FILING_DATE": "2026-05-15",
                "SUBMISSIONTYPE": "13F-HR",
                "CIK": "0000000002",
                "PERIODOFREPORT": "2025-12-31",
            },
        ])
        winners = _dedupe_amendments(submissions)
        assert len(winners) == 2
        cik1_accession = winners.loc[
            winners["CIK"] == "0000000001", "ACCESSION_NUMBER"
        ].iloc[0]
        assert cik1_accession == "0001-amended"

    def test_same_day_amendment_breaks_tie_over_original(self):
        submissions = self._submissions([
            {
                "ACCESSION_NUMBER": "orig",
                "FILING_DATE": "2026-05-15",
                "SUBMISSIONTYPE": "13F-HR",
                "CIK": "0000000001",
                "PERIODOFREPORT": "2026-03-31",
            },
            {
                "ACCESSION_NUMBER": "amend",
                "FILING_DATE": "2026-05-15",
                "SUBMISSIONTYPE": "13F-HR/A",
                "CIK": "0000000001",
                "PERIODOFREPORT": "2026-03-31",
            },
        ])
        winners = _dedupe_amendments(submissions)
        assert len(winners) == 1
        assert winners["ACCESSION_NUMBER"].iloc[0] == "amend"

    def test_select_report_periods_returns_most_recent_first(self):
        submissions = self._submissions([
            {
                "ACCESSION_NUMBER": f"acc-{i}",
                "FILING_DATE": "2026-05-15",
                "SUBMISSIONTYPE": "13F-HR",
                "CIK": f"{i:010d}",
                "PERIODOFREPORT": period,
            }
            for i, period in enumerate(
                ["2026-03-31", "2025-12-31", "2025-09-30"]
            )
        ])
        winners = _dedupe_amendments(submissions)
        periods = _select_report_periods(winners, count=2)
        assert [p.date() if hasattr(p, "date") else p for p in periods] == [
            Date(2026, 3, 31),
            Date(2025, 12, 31),
        ]

    def test_no_submissions_yields_no_periods(self):
        empty = pd.DataFrame(
            columns=["ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"]
        )
        assert _select_report_periods(_dedupe_amendments(empty)) == []


# ═══════════════════════════════════════════════════════════════════
# Parser against the measured 2024+ TSV format
# ═══════════════════════════════════════════════════════════════════


class TestParsersAgainstMeasuredFormat:
    def test_parse_infotable_measured_header(self):
        body = (
            _INFOTABLE_HEADER + "\n"
            "0001566307-26-000071\t129575969\tCOSTAR GROUP INC\tCOM\t22160N109\t"
            "\t291134\t7217\tSH\t\tSOLE\t\t0\t0\t7217\n"
            "0001566307-26-000071\t129575970\tEBAY INC.\tCOM\t278642103\t"
            "\t417782\t4590\tSH\t\tSOLE\t\t0\t0\t4590\n"
            # PUT option row — must be excluded from equity holdings.
            "0001566307-26-000071\t129575971\tSOME OPTION\tCOM\t000000000\t"
            "\t1000\t100\tSH\tPUT\tSOLE\t\t0\t0\t0\n"
            # Malformed CUSIP — must be dropped.
            "0001566307-26-000071\t129575972\tBAD CUSIP\tCOM\tBAD\t"
            "\t100\t10\tSH\t\tSOLE\t\t0\t0\t0\n"
        )
        zf = _make_zip({"INFOTABLE.tsv": body})
        df = _parse_infotable(zf)
        assert {"accession_number", "cusip", "put_call", "shares", "market_value"} <= set(
            df.columns
        )
        assert len(df) == 2  # option row + malformed CUSIP row dropped
        assert set(df["cusip"]) == {"22160N109", "278642103"}
        # VALUE is USD (not thousands) since 2023-01-03 — no *1000 scaling.
        row = df[df["cusip"] == "22160N109"].iloc[0]
        assert row["market_value"] == 291134
        assert row["shares"] == 7217

    def test_parse_infotable_missing_file_returns_empty(self):
        zf = _make_zip({"OTHER.tsv": "x\n"})
        assert len(_parse_infotable(zf)) == 0

    def test_parse_submission_measured_header(self):
        body = (
            _SUBMISSION_HEADER + "\n"
            "0000278331-26-000010\t31-MAR-2026\t13F-NT/A\t0000278331\t30-SEP-2025\n"
            "0002113426-26-000002\t31-MAR-2026\t13F-HR\t0002113426\t31-MAR-2026\n"
        )
        zf = _make_zip({"SUBMISSION.tsv": body})
        df = _parse_submission(zf)
        assert list(df.columns) == [
            "ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT",
        ]
        assert len(df) == 2
        assert df["PERIODOFREPORT"].dt.date.tolist() == [Date(2025, 9, 30), Date(2026, 3, 31)]
        assert df["FILING_DATE"].dt.date.tolist() == [Date(2026, 3, 31), Date(2026, 3, 31)]

    def test_parse_submission_missing_file_returns_empty(self):
        zf = _make_zip({"OTHER.tsv": "x\n"})
        assert len(_parse_submission(zf)) == 0


def test_quarter_str_for_date_accepts_timestamp():
    assert _quarter_str_for_date(pd.Timestamp("2026-03-31")) == "2026Q1"
    assert _quarter_str_for_date(Date(2025, 12, 31)) == "2025Q4"
