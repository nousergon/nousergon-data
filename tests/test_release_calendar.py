"""Tests for the macro release-calendar artifact (collectors.macro).

The release calendar (market_data/macro_release_calendar.parquet) is a second
dashboard-facing artifact: forward-looking macro EVENT dates — FRED data
releases (CPI / unemployment / claims / sentiment) plus scheduled FOMC meetings
— consumed by robodashboard's Calendar page. These tests keep the FRED fetch +
S3 write offline via monkeypatch and pin ``today`` for determinism.
"""

from __future__ import annotations

import io
from datetime import date

import pandas as pd

from collectors import macro

_CAL_COLS = ["date", "kind", "series_id", "label", "release_name"]

_TODAY = date(2026, 6, 4)


def _fake_release_id(series_id, api_key):
    # Each calendar series maps to a distinct fake release id + name.
    names = {
        "CPIAUCSL": "Consumer Price Index",
        "UNRATE": "Employment Situation",
        "ICSA": "Unemployment Insurance Weekly Claims",
        "UMCSENT": "Surveys of Consumers",
    }
    ids = {"CPIAUCSL": 10, "UNRATE": 50, "ICSA": 180, "UMCSENT": 91}
    return (ids[series_id], names[series_id])


def _fake_release_dates(release_id, api_key):
    # One past date (filtered out), one in-window future, one far-future (out of
    # the 180d horizon). Same shape for every release id.
    return ["2026-05-13", "2026-06-11", "2027-01-15"]


def _patch_fred(monkeypatch):
    monkeypatch.setattr(macro, "_fred_release_id", _fake_release_id)
    monkeypatch.setattr(macro, "_fred_release_dates", _fake_release_dates)


def test_build_release_calendar_shape_and_kinds(monkeypatch):
    _patch_fred(monkeypatch)
    df = macro.build_release_calendar(api_key="fake-key", today=_TODAY)
    assert list(df.columns) == _CAL_COLS
    assert set(df["kind"]) == {"release", "fomc"}
    # All four configured release series appear (each contributes the in-window date).
    assert set(df[df["kind"] == "release"]["series_id"]) == set(macro._RELEASE_CALENDAR_SERIES)


def test_build_release_calendar_future_only_and_horizon(monkeypatch):
    _patch_fred(monkeypatch)
    df = macro.build_release_calendar(api_key="fake-key", today=_TODAY)
    rel_dates = set(df[df["kind"] == "release"]["date"])
    assert "2026-05-13" not in rel_dates  # past → dropped
    assert "2026-06-11" in rel_dates  # in-window future → kept
    assert "2027-01-15" not in rel_dates  # beyond 180d horizon → dropped


def test_build_release_calendar_fomc_future_only(monkeypatch):
    _patch_fred(monkeypatch)
    df = macro.build_release_calendar(api_key="fake-key", today=_TODAY)
    fomc = df[df["kind"] == "fomc"]
    assert list(fomc["series_id"].unique()) == ["FOMC"]
    fomc_dates = set(fomc["date"])
    # 2026-06-17 onward are future as of 2026-06-04; the Jan/Mar/Apr meetings are past.
    assert "2026-06-17" in fomc_dates
    assert "2026-12-09" in fomc_dates
    assert "2026-04-29" not in fomc_dates


def test_build_release_calendar_sorted_by_date(monkeypatch):
    _patch_fred(monkeypatch)
    df = macro.build_release_calendar(api_key="fake-key", today=_TODAY)
    assert list(df["date"]) == sorted(df["date"])


def test_build_release_calendar_fomc_only_when_fred_down(monkeypatch):
    # A FRED outage (no release id) must still yield a FOMC-only calendar, not
    # an empty artifact — best-effort per series.
    monkeypatch.setattr(macro, "_fred_release_id", lambda *a, **k: None)
    df = macro.build_release_calendar(api_key="fake-key", today=_TODAY)
    assert not df.empty
    assert set(df["kind"]) == {"fomc"}


def test_build_release_calendar_empty_without_key(monkeypatch):
    monkeypatch.setattr(macro, "get_secret", lambda *a, **k: "")
    df = macro.build_release_calendar()
    assert df.empty
    assert list(df.columns) == _CAL_COLS


def test_write_release_calendar_puts_parquet(monkeypatch):
    df = pd.DataFrame(
        [{"date": "2026-06-11", "kind": "release", "series_id": "CPIAUCSL",
          "label": "CPI release", "release_name": "Consumer Price Index"}],
        columns=_CAL_COLS,
    )
    monkeypatch.setattr(macro, "build_release_calendar", lambda *a, **k: df)

    captured = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())

    result = macro.write_release_calendar(bucket="test-bucket")
    assert result["status"] == "ok"
    assert result["rows"] == 1
    assert captured["Key"] == "market_data/macro_release_calendar.parquet"
    back = pd.read_parquet(io.BytesIO(captured["Body"]), engine="pyarrow")
    assert list(back.columns) == _CAL_COLS
    assert back.iloc[0]["kind"] == "release"


def test_write_release_calendar_skips_empty(monkeypatch):
    monkeypatch.setattr(macro, "build_release_calendar", lambda *a, **k: pd.DataFrame(columns=_CAL_COLS))

    calls = []

    class _FakeS3:
        def put_object(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())

    result = macro.write_release_calendar(bucket="test-bucket")
    assert result["status"] == "skipped_empty"
    assert calls == []
