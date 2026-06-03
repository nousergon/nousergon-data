"""Tests for the macro history artifact (collectors.macro.build_macro_history etc.).

The macro history parquet (market_data/macro_history.parquet) is a standalone,
dashboard-facing artifact: full FRED observation history per series in long
format, overwritten weekly. Consumed by robodashboard's Macro page. These tests
keep the FRED fetch + S3 write offline via monkeypatch.
"""

from __future__ import annotations

import io

import pandas as pd

from collectors import macro

_HISTORY_COLS = ["date", "series_id", "label", "value", "units", "frequency"]


def test_cpi_yoy_rows_derives_year_over_year():
    """YoY% = (value / value_12_months_prior − 1) * 100, aligned to the later month."""
    # 14 monthly points; index rising 1%/mo from 100 → only entries with a
    # 12-prior partner produce a YoY value (entries 12 and 13).
    obs = [(f"2020-{m:02d}-01", 100.0 + i) for i, m in enumerate(range(1, 13))]
    obs += [("2021-01-01", 112.0), ("2021-02-01", 113.0)]
    rows = macro._cpi_yoy_rows(obs)
    assert [d for d, _ in rows] == ["2021-01-01", "2021-02-01"]
    # 112 / 100 - 1 = 12.0%
    assert rows[0][1] == 12.0
    # 113 / 101 - 1 = 11.88%
    assert rows[1][1] == 11.88


def test_build_macro_history_long_format_and_derived_cpi(monkeypatch):
    """build_macro_history returns long-format rows for each series + derived CPI YoY."""

    def _fake_history(series_id, api_key, start=macro._MACRO_HISTORY_START):
        if series_id == "CPIAUCSL":
            # 13 months so exactly one YoY row is derivable.
            return [(f"2020-{m:02d}-01", 100.0 + m) for m in range(1, 13)] + [("2021-01-01", 113.0)]
        return [("2020-01-01", 1.0), ("2020-02-01", 2.0)]

    monkeypatch.setattr(macro, "_fred_history", _fake_history)

    df = macro.build_macro_history(api_key="fake-key")
    assert list(df.columns) == _HISTORY_COLS
    # Every configured raw series is present...
    for series_id in macro._FRED_HISTORY_SERIES:
        assert series_id in df["series_id"].values
    # ...plus the derived inflation series.
    assert "CPI_YOY" in df["series_id"].values
    cpi_yoy = df[df["series_id"] == "CPI_YOY"]
    assert len(cpi_yoy) == 1
    assert cpi_yoy.iloc[0]["units"] == "percent"
    assert cpi_yoy.iloc[0]["label"] == "Inflation (CPI YoY)"
    # 113 / 101 - 1 = 11.88%
    assert cpi_yoy.iloc[0]["value"] == round((113.0 / 101.0 - 1) * 100, 2)


def test_build_macro_history_empty_without_key(monkeypatch):
    """No FRED key → empty frame with the right columns (caller skips the write)."""
    monkeypatch.setattr(macro, "get_secret", lambda *a, **k: "")
    df = macro.build_macro_history()
    assert df.empty
    assert list(df.columns) == _HISTORY_COLS


def test_write_macro_history_puts_parquet(monkeypatch):
    """A non-empty build writes one parquet PUT to market_data/macro_history.parquet."""
    df = pd.DataFrame(
        [{"date": "2020-01-01", "series_id": "FEDFUNDS", "label": "Fed Funds Rate",
          "value": 1.5, "units": "percent", "frequency": "monthly"}],
        columns=_HISTORY_COLS,
    )
    monkeypatch.setattr(macro, "build_macro_history", lambda *a, **k: df)

    captured = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())

    result = macro.write_macro_history(bucket="test-bucket")
    assert result["status"] == "ok"
    assert result["rows"] == 1
    assert captured["Key"] == "market_data/macro_history.parquet"
    # Body is a real parquet round-tripping to the same frame.
    back = pd.read_parquet(io.BytesIO(captured["Body"]), engine="pyarrow")
    assert list(back.columns) == _HISTORY_COLS
    assert back.iloc[0]["series_id"] == "FEDFUNDS"


def test_write_macro_history_skips_empty(monkeypatch):
    """An empty build is a no-op — never overwrite a good artifact with nothing."""
    monkeypatch.setattr(macro, "build_macro_history", lambda *a, **k: pd.DataFrame(columns=_HISTORY_COLS))

    calls = []

    class _FakeS3:
        def put_object(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())

    result = macro.write_macro_history(bucket="test-bucket")
    assert result["status"] == "skipped_empty"
    assert calls == []
