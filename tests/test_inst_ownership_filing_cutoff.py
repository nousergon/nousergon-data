"""Tests for the 13F filing-date cutoff, VALUE thousands-scaling, and the
historical ``--report-period`` backfill mode (alpha-engine-config-I10733).

No network, no S3 (an in-memory S3 stub mirrors the existing
``test_inst_ownership_reader.py`` / ``test_inst_ownership_cusip_mapping.py``
pattern). SEC bulk ZIPs are built in-process from real 2024+-format
TSV headers (measured against ``01mar2026-31may2026_form13f.zip``,
2026-09-13 — see ``test_inst_ownership_sec_windows.py``).

Covers:

1. The filing-date cutoff (17 CFR 240.13f-1(a), 45 days) excludes a late
   filer and a late amendment; an on-time amendment still supersedes its
   on-time original.
2. VALUE is scaled x1000 for submissions filed before 2023-01-03 and left
   alone on/after it.
3. Historical window selection by filing-date-range overlap, both for a
   legacy calendar-quarter period and a 2024+ windowed period.
4. An end-to-end historical backfill: writes the per-quarter artifact,
   never touches the global freshness sidecar.
5. Fail-loud outcomes: an unpublished required window, a non-quarter-end
   ``report_period``, and a period with no joinable rows.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date as Date
from io import BytesIO

import pandas as pd
import pytest

from data.derived.inst_ownership import (
    InstOwnershipPeriodUnavailable,
    SEC_THIRTEEN_F_THOUSANDS_CUTOFF,
    THIRTEEN_F_FILING_DEADLINE_DAYS,
    _apply_filing_cutoff,
    _apply_value_scaling,
    _dedupe_amendments,
    _download_windows,
    _find_zip_member,
    _is_quarter_end,
    _parse_infotable,
    _parse_submission,
    _previous_quarter_end,
    _windows_needed_for_period,
    compute_and_write_inst_ownership,
)

# Real headers measured 2026-09-13 against the SEC's
# 01mar2026-31may2026_form13f.zip (see test_inst_ownership_sec_windows.py).
_INFOTABLE_HEADER = (
    "ACCESSION_NUMBER\tINFOTABLE_SK\tNAMEOFISSUER\tTITLEOFCLASS\tCUSIP\tFIGI"
    "\tVALUE\tSSHPRNAMT\tSSHPRNAMTTYPE\tPUTCALL\tINVESTMENTDISCRETION"
    "\tOTHERMANAGER\tVOTING_AUTH_SOLE\tVOTING_AUTH_SHARED\tVOTING_AUTH_NONE"
)
_SUBMISSION_HEADER = "ACCESSION_NUMBER\tFILING_DATE\tSUBMISSIONTYPE\tCIK\tPERIODOFREPORT"


def _make_zip(files: dict[str, str]) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    buf.seek(0)
    return zipfile.ZipFile(buf)


def _sec_date(d: str) -> str:
    """``"2026-05-10"`` -> ``"10-MAY-2026"`` (SEC's SUBMISSION.tsv date format)."""
    y, m, day = d.split("-")
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    return f"{day}-{months[int(m) - 1]}-{y}"


def _submission_row(accession: str, filed: str, kind: str, cik: str, period: str) -> str:
    return f"{accession}\t{_sec_date(filed)}\t{kind}\t{cik}\t{_sec_date(period)}\n"


def _infotable_row(accession: str, cusip: str, value: int, shares: int) -> str:
    return (
        f"{accession}\t1\tSOME ISSUER\tCOM\t{cusip}\t\t{value}\t{shares}\tSH\t\tSOLE\t\t0\t0\t{shares}\n"
    )


class _InMemoryS3:
    """Mirrors test_inst_ownership_reader.py's stub, plus seed_cache from
    test_inst_ownership_cusip_mapping.py so cusip resolution needs no
    network in the end-to-end tests."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self._store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise Exception(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": BytesIO(self._store[(Bucket, Key)])}

    def has(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self._store

    def seed_cusip_cache(self, bucket: str, mapping: dict[str, str]) -> None:
        payload = {"as_of": "2026-09-13", "schema_version": 1, "mapping": mapping}
        self._store[(bucket, "data/crosswalks/cusip_to_ticker.json")] = json.dumps(payload).encode("utf-8")


# ═══════════════════════════════════════════════════════════════════
# Filing-date cutoff
# ═══════════════════════════════════════════════════════════════════


def _submissions(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["FILING_DATE"] = pd.to_datetime(df["FILING_DATE"])
    df["PERIODOFREPORT"] = pd.to_datetime(df["PERIODOFREPORT"])
    return df


class TestFilingDateCutoff:
    def test_excludes_late_filer(self):
        # Period 2026-03-31, deadline 2026-05-15. Filed 2026-06-01: late.
        submissions = _submissions([
            {"ACCESSION_NUMBER": "on-time", "FILING_DATE": "2026-05-10",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000001", "PERIODOFREPORT": "2026-03-31"},
            {"ACCESSION_NUMBER": "late", "FILING_DATE": "2026-06-01",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000002", "PERIODOFREPORT": "2026-03-31"},
        ])
        admitted = _apply_filing_cutoff(submissions)
        assert set(admitted["ACCESSION_NUMBER"]) == {"on-time"}

    def test_boundary_exactly_45_days_is_admitted(self):
        submissions = _submissions([
            {"ACCESSION_NUMBER": "boundary", "FILING_DATE": "2026-05-15",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000001", "PERIODOFREPORT": "2026-03-31"},
        ])
        assert THIRTEEN_F_FILING_DEADLINE_DAYS == 45
        admitted = _apply_filing_cutoff(submissions)
        assert len(admitted) == 1

    def test_on_time_amendment_still_supersedes_original(self):
        submissions = _submissions([
            {"ACCESSION_NUMBER": "orig", "FILING_DATE": "2026-05-01",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000001", "PERIODOFREPORT": "2026-03-31"},
            {"ACCESSION_NUMBER": "amend-on-time", "FILING_DATE": "2026-05-10",
             "SUBMISSIONTYPE": "13F-HR/A", "CIK": "0000000001", "PERIODOFREPORT": "2026-03-31"},
        ])
        admitted = _apply_filing_cutoff(submissions)
        winners = _dedupe_amendments(admitted)
        assert list(winners["ACCESSION_NUMBER"]) == ["amend-on-time"]

    def test_late_amendment_excluded_on_time_original_stands(self):
        submissions = _submissions([
            {"ACCESSION_NUMBER": "orig-on-time", "FILING_DATE": "2026-05-01",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000001", "PERIODOFREPORT": "2026-03-31"},
            {"ACCESSION_NUMBER": "amend-late", "FILING_DATE": "2026-06-10",
             "SUBMISSIONTYPE": "13F-HR/A", "CIK": "0000000001", "PERIODOFREPORT": "2026-03-31"},
        ])
        admitted = _apply_filing_cutoff(submissions)
        # The late amendment must never even reach dedupe.
        assert set(admitted["ACCESSION_NUMBER"]) == {"orig-on-time"}
        winners = _dedupe_amendments(admitted)
        assert list(winners["ACCESSION_NUMBER"]) == ["orig-on-time"]

    def test_empty_input_returns_empty(self):
        empty = pd.DataFrame(
            columns=["ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"]
        )
        assert len(_apply_filing_cutoff(empty)) == 0


# ═══════════════════════════════════════════════════════════════════
# VALUE thousands-scaling
# ═══════════════════════════════════════════════════════════════════


class TestValueScaling:
    def _winners(self) -> pd.DataFrame:
        return _submissions([
            {"ACCESSION_NUMBER": "legacy", "FILING_DATE": "2022-05-10",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000001", "PERIODOFREPORT": "2022-03-31"},
            {"ACCESSION_NUMBER": "boundary-day", "FILING_DATE": "2023-01-02",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000002", "PERIODOFREPORT": "2022-12-31"},
            {"ACCESSION_NUMBER": "modern", "FILING_DATE": "2023-01-03",
             "SUBMISSIONTYPE": "13F-HR", "CIK": "0000000003", "PERIODOFREPORT": "2022-12-31"},
        ])

    def test_scales_legacy_filings_x1000(self):
        assert SEC_THIRTEEN_F_THOUSANDS_CUTOFF == Date(2023, 1, 3)
        infotable = pd.DataFrame([
            {"accession_number": "legacy", "cusip": "AAA000001", "market_value": 100.0},
            {"accession_number": "boundary-day", "cusip": "AAA000001", "market_value": 200.0},
            {"accession_number": "modern", "cusip": "AAA000001", "market_value": 300.0},
        ])
        out = _apply_value_scaling(infotable, self._winners())
        vals = dict(zip(out["accession_number"], out["market_value"]))
        assert vals["legacy"] == 100_000.0
        assert vals["boundary-day"] == 200_000.0  # day before cutoff, still legacy
        assert vals["modern"] == 300.0  # cutoff day itself, whole-USD already

    def test_accession_missing_from_winners_left_unscaled(self):
        infotable = pd.DataFrame([
            {"accession_number": "unknown", "cusip": "AAA000001", "market_value": 50.0},
        ])
        out = _apply_value_scaling(infotable, self._winners())
        assert out["market_value"].iloc[0] == 50.0

    def test_empty_infotable_returns_empty(self):
        empty = pd.DataFrame(columns=["accession_number", "cusip", "market_value"])
        assert len(_apply_value_scaling(empty, self._winners())) == 0


# ═══════════════════════════════════════════════════════════════════
# Quarter-end helpers
# ═══════════════════════════════════════════════════════════════════


class TestQuarterEndHelpers:
    @pytest.mark.parametrize("d, expected", [
        (Date(2026, 3, 31), True), (Date(2026, 6, 30), True),
        (Date(2026, 9, 30), True), (Date(2026, 12, 31), True),
        (Date(2026, 3, 30), False), (Date(2026, 6, 15), False),
        (Date(2024, 2, 29), False),
    ])
    def test_is_quarter_end(self, d, expected):
        assert _is_quarter_end(d) is expected

    def test_previous_quarter_end_within_year(self):
        assert _previous_quarter_end(Date(2026, 6, 30)) == Date(2026, 3, 31)
        assert _previous_quarter_end(Date(2026, 12, 31)) == Date(2026, 9, 30)

    def test_previous_quarter_end_crosses_year(self):
        assert _previous_quarter_end(Date(2026, 3, 31)) == Date(2025, 12, 31)


# ═══════════════════════════════════════════════════════════════════
# Historical window selection by filing-date-range overlap
# ═══════════════════════════════════════════════════════════════════


_LEGACY_INDEX_HTML = """<html><body>
<a href="/files/structureddata/data/form-13f-data-sets/2021q3_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2021q4_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2022q1_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2022q2_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2022q3_form13f.zip">x</a>
</body></html>"""

_WINDOWED_INDEX_HTML = """<html><body>
<a href="/files/structureddata/data/form-13f-data-sets/01mar2024-31may2024_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/01jun2024-31aug2024_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/01sep2024-30nov2024_form13f.zip">x</a>
</body></html>"""


class TestHistoricalWindowSelection:
    def test_legacy_quarter_period_selects_three_legacy_windows(self):
        # P=2022-03-31 (prior 2021-12-31): matches the deliverable's worked
        # example — 2021q4/2022q1/2022q2.
        names = _windows_needed_for_period(
            Date(2022, 3, 31), Date(2021, 12, 31), index_html=_LEGACY_INDEX_HTML,
        )
        assert names == ["2021q4_form13f.zip", "2022q1_form13f.zip", "2022q2_form13f.zip"]

    def test_2024_plus_windowed_period_selects_two_windows(self):
        # P=2024-06-30 (prior 2024-03-31): Mar-May covers Q1's deadline,
        # Jun-Aug covers Q2's.
        names = _windows_needed_for_period(
            Date(2024, 6, 30), Date(2024, 3, 31), index_html=_WINDOWED_INDEX_HTML,
        )
        assert names == ["01jun2024-31aug2024_form13f.zip", "01mar2024-31may2024_form13f.zip"]

    def test_unreachable_index_raises(self, monkeypatch):
        monkeypatch.setattr("data.derived.inst_ownership._fetch_sec_index_html", lambda: None)
        with pytest.raises(RuntimeError, match="index page unreachable"):
            _windows_needed_for_period(Date(2024, 6, 30), Date(2024, 3, 31))

    def test_no_window_covers_range_raises(self):
        with pytest.raises(RuntimeError, match="no published SEC 13F window"):
            _windows_needed_for_period(
                Date(2030, 6, 30), Date(2030, 3, 31), index_html=_WINDOWED_INDEX_HTML,
            )


class TestDownloadWindows:
    def test_unpublished_required_window_raises(self, monkeypatch):
        monkeypatch.setattr(
            "data.derived.inst_ownership._download_sec_bulk_zip", lambda name: None
        )
        monkeypatch.setattr("time.sleep", lambda _s: None)
        with pytest.raises(RuntimeError, match="not published"):
            _download_windows(["missing.zip"], cache={})

    def test_cache_avoids_redownload(self, monkeypatch):
        calls = []

        def fake_download(name):
            calls.append(name)
            return _make_zip({"marker.txt": name})

        monkeypatch.setattr("data.derived.inst_ownership._download_sec_bulk_zip", fake_download)
        monkeypatch.setattr("time.sleep", lambda _s: None)
        cache: dict = {}
        _download_windows(["a.zip"], cache=cache)
        _download_windows(["a.zip", "b.zip"], cache=cache)
        assert calls == ["a.zip", "b.zip"]  # a.zip fetched only once


# ═══════════════════════════════════════════════════════════════════
# End-to-end historical backfill
# ═══════════════════════════════════════════════════════════════════


class TestHistoricalBackfillEndToEnd:
    def _windows(self) -> dict[str, zipfile.ZipFile]:
        # Mar-May 2024 window: Q1 2024 report period, on-time filing.
        mar_may = _make_zip({
            "SUBMISSION.tsv": (
                _SUBMISSION_HEADER + "\n"
                + _submission_row("acc-q1", "2024-05-10", "13F-HR", "0000000001", "2024-03-31")
            ),
            "INFOTABLE.tsv": (
                _INFOTABLE_HEADER + "\n"
                + _infotable_row("acc-q1", "22160N109", 291134, 7217)
            ),
        })
        # Jun-Aug 2024 window: Q2 2024 report period, on-time filing —
        # same fund increases its position.
        jun_aug = _make_zip({
            "SUBMISSION.tsv": (
                _SUBMISSION_HEADER + "\n"
                + _submission_row("acc-q2", "2024-08-01", "13F-HR", "0000000001", "2024-06-30")
            ),
            "INFOTABLE.tsv": (
                _INFOTABLE_HEADER + "\n"
                + _infotable_row("acc-q2", "22160N109", 400000, 9000)
            ),
        })
        # Sep-Nov 2024 window: Q3 2024 report period, on-time filing.
        sep_nov = _make_zip({
            "SUBMISSION.tsv": (
                _SUBMISSION_HEADER + "\n"
                + _submission_row("acc-q3", "2024-11-01", "13F-HR", "0000000001", "2024-09-30")
            ),
            "INFOTABLE.tsv": (
                _INFOTABLE_HEADER + "\n"
                + _infotable_row("acc-q3", "22160N109", 450000, 9500)
            ),
        })
        return {
            "01mar2024-31may2024_form13f.zip": mar_may,
            "01jun2024-31aug2024_form13f.zip": jun_aug,
            "01sep2024-30nov2024_form13f.zip": sep_nov,
        }

    def _patch_download(self, monkeypatch):
        windows = self._windows()
        monkeypatch.setattr(
            "data.derived.inst_ownership._download_sec_bulk_zip",
            lambda name: windows.get(name),
        )
        monkeypatch.setattr("time.sleep", lambda _s: None)

    def test_writes_quarter_artifact_never_touches_global_sidecar(self, monkeypatch):
        self._patch_download(monkeypatch)
        s3 = _InMemoryS3()
        s3.seed_cusip_cache("alpha-engine-research", {"22160N109": "COST"})

        rows = compute_and_write_inst_ownership(
            ["COST"], s3_client=s3, bucket="alpha-engine-research",
            report_period=Date(2024, 6, 30), update_global_sidecar=False,
            _index_html=_WINDOWED_INDEX_HTML,
        )

        assert rows is not None and len(rows) == 1
        assert rows[0].ticker == "COST"
        assert rows[0].quarter == "2024Q2"
        assert rows[0].shares_qoq_change == 9000 - 7217

        assert s3.has("alpha-engine-research", "data/inst_ownership/2024Q2/latest.parquet")
        assert not s3.has("alpha-engine-research", "data/inst_ownership/latest.json")

    def test_shared_window_cache_across_adjacent_periods(self, monkeypatch):
        """Q3's 'prior' window (Jun-Aug 2024) is Q2's own 'current' window —
        a second period in the same backfill invocation must not re-download
        it (I10733 caching requirement)."""
        windows = self._windows()
        calls = []

        def fake_download(name):
            calls.append(name)
            return windows.get(name)

        monkeypatch.setattr("data.derived.inst_ownership._download_sec_bulk_zip", fake_download)
        monkeypatch.setattr("time.sleep", lambda _s: None)

        s3 = _InMemoryS3()
        s3.seed_cusip_cache("alpha-engine-research", {"22160N109": "COST"})
        cache: dict = {}

        compute_and_write_inst_ownership(
            ["COST"], s3_client=s3, bucket="alpha-engine-research",
            report_period=Date(2024, 6, 30), update_global_sidecar=False,
            _index_html=_WINDOWED_INDEX_HTML, _window_cache=cache,
        )
        compute_and_write_inst_ownership(
            ["COST"], s3_client=s3, bucket="alpha-engine-research",
            report_period=Date(2024, 9, 30), update_global_sidecar=False,
            _index_html=_WINDOWED_INDEX_HTML, _window_cache=cache,
        )
        assert calls.count("01jun2024-31aug2024_form13f.zip") == 1

    def test_non_quarter_end_report_period_refused(self):
        s3 = _InMemoryS3()
        with pytest.raises(ValueError, match="not a calendar quarter-end date"):
            compute_and_write_inst_ownership(
                ["COST"], s3_client=s3, bucket="alpha-engine-research",
                report_period=Date(2024, 6, 15),
            )

    def test_period_with_no_joinable_rows_raises(self, monkeypatch):
        # Windows download fine but carry no submission for the requested
        # PERIODOFREPORT at all (e.g. a quarter that genuinely has no
        # filings in the covering window(s), or the window covers only the
        # neighboring period) — must raise, never silently skip.
        stray = _make_zip({
            "SUBMISSION.tsv": (
                _SUBMISSION_HEADER + "\n"
                + _submission_row("acc-other", "2024-05-10", "13F-HR", "0000000009", "2023-12-31")
            ),
            "INFOTABLE.tsv": (
                _INFOTABLE_HEADER + "\n"
                + _infotable_row("acc-other", "22160N109", 100, 10)
            ),
        })
        monkeypatch.setattr(
            "data.derived.inst_ownership._download_sec_bulk_zip",
            lambda name: stray,
        )
        monkeypatch.setattr("time.sleep", lambda _s: None)
        s3 = _InMemoryS3()

        with pytest.raises(InstOwnershipPeriodUnavailable, match="no on-time"):
            compute_and_write_inst_ownership(
                ["COST"], s3_client=s3, bucket="alpha-engine-research",
                report_period=Date(2024, 6, 30), update_global_sidecar=False,
                _index_html=_WINDOWED_INDEX_HTML,
            )


# ═══════════════════════════════════════════════════════════════════
# Nested-subdirectory SEC ZIP layout (alpha-engine-config-I10763)
# ═══════════════════════════════════════════════════════════════════
#
# MEASURED 2026-09-14 directly against SEC.gov: every 13F window ZIP
# inspected (01mar2025-31may2025, 01sep2025-30nov2025, 01dec2025-28feb2026,
# 01mar2026-31may2026, ...) has its 9 members at the archive root. Exactly
# one — 01jun2025-31aug2025_form13f.zip, published 2025-09-02 — packs all
# 9 members under a top-level ``01JUN2025-31AUG2025_form13f/`` directory
# instead. That is the ONLY window whose downloaded on-time filings for
# PERIODOFREPORT 2025-06-30 exist, so the flat ``zf.open("SUBMISSION.tsv")``
# lookup KeyErroring on it (silently treated as "0 submissions in this
# window") is exactly why the 2025Q2 backfill reported "no on-time
# submission for this PERIODOFREPORT" — a parse failure read as a real
# absence.
_2025Q2_WINDOWED_INDEX_HTML = """<html><body>
<a href="/files/structureddata/data/form-13f-data-sets/01mar2025-31may2025_form13f.zip">x</a>
<a href="/files/structureddata/data/form-13f-data-sets/01jun2025-31aug2025_form13f.zip">x</a>
</body></html>"""


def _nested_zip(top_dir: str, files: dict[str, str]) -> zipfile.ZipFile:
    """Like ``_make_zip`` but every entry is written under ``top_dir/`` —
    reproduces the real, measured layout of
    ``01jun2025-31aug2025_form13f.zip``."""
    return _make_zip({f"{top_dir}/{name}": content for name, content in files.items()})


class TestNestedZipMemberLookup:
    def test_flat_zip_matches_exact_name(self):
        zf = _make_zip({"SUBMISSION.tsv": "x"})
        assert _find_zip_member(zf, "SUBMISSION.tsv") == "SUBMISSION.tsv"

    def test_nested_zip_matches_by_basename(self):
        zf = _nested_zip("01JUN2025-31AUG2025_form13f", {"SUBMISSION.tsv": "x"})
        assert _find_zip_member(zf, "SUBMISSION.tsv") == (
            "01JUN2025-31AUG2025_form13f/SUBMISSION.tsv"
        )

    def test_missing_member_returns_none(self):
        zf = _make_zip({"OTHER.tsv": "x"})
        assert _find_zip_member(zf, "SUBMISSION.tsv") is None

    def test_parse_submission_reads_nested_member(self):
        zf = _nested_zip("01JUN2025-31AUG2025_form13f", {
            "SUBMISSION.tsv": (
                _SUBMISSION_HEADER + "\n"
                + _submission_row("acc-1", "2025-08-01", "13F-HR", "0000000001", "2025-06-30")
            ),
        })
        df = _parse_submission(zf)
        assert len(df) == 1
        assert df.iloc[0]["ACCESSION_NUMBER"] == "acc-1"

    def test_parse_infotable_reads_nested_member(self):
        zf = _nested_zip("01JUN2025-31AUG2025_form13f", {
            "INFOTABLE.tsv": (
                _INFOTABLE_HEADER + "\n"
                + _infotable_row("acc-1", "22160N109", 400000, 9000)
            ),
        })
        df = _parse_infotable(zf)
        assert len(df) == 1
        assert df.iloc[0]["cusip"] == "22160N109"


class TestRecordedFixture2025Q2Backfill:
    """Recorded fixture reproducing alpha-engine-config-I10763's exact
    failure: report period 2025-06-30, windows
    ['01jun2025-31aug2025_form13f.zip', '01mar2025-31may2025_form13f.zip'],
    where the Jun-Aug window (the ONLY one carrying 2025-06-30's on-time
    filings) is nested. No live network — the two window ZIPs are built
    in-process with the measured real layout."""

    def _windows(self) -> dict[str, zipfile.ZipFile]:
        mar_may = _make_zip({
            "SUBMISSION.tsv": (
                _SUBMISSION_HEADER + "\n"
                + _submission_row("acc-q1", "2025-05-10", "13F-HR", "0000000001", "2025-03-31")
            ),
            "INFOTABLE.tsv": (
                _INFOTABLE_HEADER + "\n"
                + _infotable_row("acc-q1", "22160N109", 291134, 7217)
            ),
        })
        # Nested layout — measured live against the real published ZIP.
        jun_aug = _nested_zip("01JUN2025-31AUG2025_form13f", {
            "SUBMISSION.tsv": (
                _SUBMISSION_HEADER + "\n"
                + _submission_row("acc-q2", "2025-08-01", "13F-HR", "0000000001", "2025-06-30")
            ),
            "INFOTABLE.tsv": (
                _INFOTABLE_HEADER + "\n"
                + _infotable_row("acc-q2", "22160N109", 400000, 9000)
            ),
        })
        return {
            "01mar2025-31may2025_form13f.zip": mar_may,
            "01jun2025-31aug2025_form13f.zip": jun_aug,
        }

    def _patch_download(self, monkeypatch):
        windows = self._windows()
        monkeypatch.setattr(
            "data.derived.inst_ownership._download_sec_bulk_zip",
            lambda name: windows.get(name),
        )
        monkeypatch.setattr("time.sleep", lambda _s: None)

    def test_2025q2_backfill_succeeds_with_nested_window(self, monkeypatch):
        self._patch_download(monkeypatch)
        s3 = _InMemoryS3()
        s3.seed_cusip_cache("alpha-engine-research", {"22160N109": "COST"})

        rows = compute_and_write_inst_ownership(
            ["COST"], s3_client=s3, bucket="alpha-engine-research",
            report_period=Date(2025, 6, 30), update_global_sidecar=False,
            _index_html=_2025Q2_WINDOWED_INDEX_HTML,
        )

        assert rows is not None and len(rows) == 1
        assert rows[0].ticker == "COST"
        assert rows[0].quarter == "2025Q2"
        assert rows[0].shares_qoq_change == 9000 - 7217
        assert s3.has("alpha-engine-research", "data/inst_ownership/2025Q2/latest.parquet")
        assert not s3.has("alpha-engine-research", "data/inst_ownership/latest.json")
