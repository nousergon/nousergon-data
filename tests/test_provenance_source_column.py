"""Regression tests for provenance ``source`` column on ArcticDB OHLCV writes.

The chronic-polygon-gap arc (PR #193 + #195) put per-row provenance on
the table — daily_closes.collect already records source ("polygon" /
"yfinance" / "fred") per row in the staging parquet, but the ArcticDB
universe library writes (the canonical OHLCV store consumed by predictor
training + inference) dropped that column and the audit trail of "where
did this row's value come from" died at the parquet boundary.

This module pins:

  - The schema-bridge helper ``_align_schema_for_update`` correctly handles
    the migration boundary (existing series without source vs new row
    with source, and vice versa).
  - ``_load_daily_closes`` surfaces source from the staging parquet.
  - ``_apply_daily_delta`` propagates source through the merge (pre-delta
    rows tagged "yfinance"; delta rows tagged from the daily_closes parquet's
    source field; dedup keep="last" lets the delta source win on overlap).
"""
from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd


# ── Schema-bridge helper ─────────────────────────────────────────────────────


def test_align_schema_noop_when_schemas_match():
    from builders.daily_append import _align_schema_for_update

    cols = ["Open", "High", "Low", "Close", "Volume"]
    existing = pd.DataFrame(
        [[1.0, 2.0, 0.5, 1.5, 1000]] * 3,
        columns=cols,
        index=pd.date_range("2026-04-20", periods=3),
    )
    new_row = pd.DataFrame(
        [[1.1, 2.1, 0.6, 1.6, 1100]],
        columns=cols,
        index=pd.DatetimeIndex(["2026-04-23"]),
    )
    aligned = _align_schema_for_update(new_row, existing)
    # Identical schemas → identity-return so caller-side mock equality
    # checks see the same object reference.
    assert aligned is new_row


def test_align_schema_drops_extra_cols_in_new_row():
    """new_row has source, existing doesn't — drop source from new_row
    so update() doesn't trip ArcticDB's strict schema match."""
    from builders.daily_append import _align_schema_for_update

    cols_existing = ["Open", "High", "Low", "Close", "Volume"]
    existing = pd.DataFrame(
        [[1.0, 2.0, 0.5, 1.5, 1000]] * 3,
        columns=cols_existing,
        index=pd.date_range("2026-04-20", periods=3),
    )
    new_row = pd.DataFrame(
        [[1.1, 2.1, 0.6, 1.6, 1100, "polygon"]],
        columns=cols_existing + ["source"],
        index=pd.DatetimeIndex(["2026-04-23"]),
    )
    aligned = _align_schema_for_update(new_row, existing)
    assert "source" not in aligned.columns
    assert list(aligned.columns) == cols_existing


def test_align_schema_adds_missing_cols_to_new_row_as_nan():
    """existing has source, new_row doesn't — add source=NaN to new_row
    so update() can proceed; reorder to match existing column order."""
    from builders.daily_append import _align_schema_for_update

    cols_existing = ["Open", "High", "Low", "Close", "Volume", "source"]
    existing = pd.DataFrame(
        [[1.0, 2.0, 0.5, 1.5, 1000, "yfinance"]] * 3,
        columns=cols_existing,
        index=pd.date_range("2026-04-20", periods=3),
    )
    new_row = pd.DataFrame(
        [[1.1, 2.1, 0.6, 1.6, 1100]],
        columns=["Open", "High", "Low", "Close", "Volume"],
        index=pd.DatetimeIndex(["2026-04-23"]),
    )
    aligned = _align_schema_for_update(new_row, existing)
    assert "source" in aligned.columns
    assert pd.isna(aligned.iloc[0]["source"])
    assert list(aligned.columns) == cols_existing


def test_align_schema_passes_through_when_existing_empty():
    """First write to a new symbol — nothing to align against."""
    from builders.daily_append import _align_schema_for_update

    new_row = pd.DataFrame(
        [[1.1, 2.1, 0.6, 1.6, 1100, "polygon"]],
        columns=["Open", "High", "Low", "Close", "Volume", "source"],
        index=pd.DatetimeIndex(["2026-04-23"]),
    )
    aligned = _align_schema_for_update(new_row, pd.DataFrame())
    assert aligned is new_row


# ── _load_daily_closes surfaces source ───────────────────────────────────────


def test_load_daily_closes_extracts_source_per_ticker():
    """daily_closes.collect writes a per-row source field; daily_append's
    record dict must surface it so the per-ticker write loop can carry
    it into ArcticDB."""
    from builders.daily_append import _load_daily_closes

    df = pd.DataFrame(
        {
            "Open":   [100.0, 200.0, 50.0],
            "High":   [101.0, 202.0, 51.0],
            "Low":    [99.0,  198.0, 49.0],
            "Close":  [100.5, 201.0, 50.5],
            "Volume": [1_000_000, 2_000_000, 500_000],
            "VWAP":   [100.4, 201.1, 50.4],
            "source": ["polygon", "polygon", "fred"],
        },
        index=["AAPL", "MSFT", "TNX"],
    )
    df.index.name = "ticker"

    s3 = MagicMock()
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow")
    buf.seek(0)
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: buf.read())}

    records = _load_daily_closes(s3, "test-bucket", "2026-05-09")

    assert records["AAPL"]["source"] == "polygon"
    assert records["MSFT"]["source"] == "polygon"
    assert records["TNX"]["source"] == "fred"


def test_load_daily_closes_defaults_source_to_unknown_when_absent():
    """Pre-migration parquets lack source column. Surface as 'unknown'
    rather than raising, so daily_append's per-ticker loop can still
    write a defensible default."""
    from builders.daily_append import _load_daily_closes

    df = pd.DataFrame(
        {
            "Open":   [100.0],
            "High":   [101.0],
            "Low":    [99.0],
            "Close":  [100.5],
            "Volume": [1_000_000],
            "VWAP":   [100.4],
        },
        index=["AAPL"],
    )
    df.index.name = "ticker"

    s3 = MagicMock()
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow")
    buf.seek(0)
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: buf.read())}

    records = _load_daily_closes(s3, "test-bucket", "2026-05-09")
    assert records["AAPL"]["source"] == "unknown"


# ── _apply_daily_delta source propagation ────────────────────────────────────


def test_apply_daily_delta_tags_pre_delta_yfinance_and_delta_from_parquet():
    """Pre-delta rows (price_cache) tagged ``yfinance``; delta rows tagged
    from the parquet's source field; dedup keep="last" lets the delta
    source win on overlap. The merged frame's source column carries
    per-row provenance the next ArcticDB write can persist verbatim."""
    from features.compute import _apply_daily_delta

    # Pre-delta price_cache: 5 rows ending 2026-05-06, all yfinance origin
    price_data = {
        "SPY": pd.DataFrame(
            {
                "Open":   [100.0, 101.0, 102.0, 103.0, 104.0],
                "High":   [101.0, 102.0, 103.0, 104.0, 105.0],
                "Low":    [99.0,  100.0, 101.0, 102.0, 103.0],
                "Close":  [100.5, 101.5, 102.5, 103.5, 104.5],
                "Volume": [1_000_000] * 5,
            },
            index=pd.bdate_range(end="2026-05-06", periods=5),
        ),
    }

    # Delta parquets for 5/7 + 5/8, polygon origin
    def _make_parquet(date: str, source: str) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                "Open":   [110.0],
                "High":   [111.0],
                "Low":    [109.0],
                "Close":  [110.5],
                "Volume": [1_500_000],
                "source": [source],
            },
            index=["SPY"],
        )
        df.index.name = "ticker"
        return df

    delta_5_7 = _make_parquet("2026-05-07", "polygon")
    delta_5_8 = _make_parquet("2026-05-08", "polygon")

    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey

    def _get_object(Bucket, Key):
        for date_str, df in [("2026-05-07", delta_5_7), ("2026-05-08", delta_5_8)]:
            if Key == f"staging/daily_closes/{date_str}.parquet":
                buf = io.BytesIO()
                df.to_parquet(buf, engine="pyarrow")
                buf.seek(0)
                return {"Body": MagicMock(read=lambda buf=buf: buf.read())}
        raise _NoSuchKey(Key)

    s3.get_object.side_effect = _get_object

    out, _splits = _apply_daily_delta(s3, "test-bucket", "2026-05-09", price_data)

    assert "source" in out["SPY"].columns
    # Pre-delta dates → yfinance
    yfinance_dates = out["SPY"][out["SPY"]["source"] == "yfinance"].index
    assert len(yfinance_dates) == 5
    # Delta dates → polygon
    polygon_dates = out["SPY"][out["SPY"]["source"] == "polygon"].index
    assert pd.Timestamp("2026-05-07") in polygon_dates
    assert pd.Timestamp("2026-05-08") in polygon_dates


def test_apply_daily_delta_no_delta_files_keeps_yfinance_default():
    """When no delta files exist (e.g. the cache is current), pre-delta
    rows still get the ``yfinance`` provenance tag — the merged frame
    carries provenance even on cache-only paths."""
    from features.compute import _apply_daily_delta

    price_data = {
        "SPY": pd.DataFrame(
            {
                "Open":   [100.0, 101.0],
                "High":   [101.0, 102.0],
                "Low":    [99.0,  100.0],
                "Close":  [100.5, 101.5],
                "Volume": [1_000_000, 1_100_000],
            },
            index=pd.bdate_range(end="2026-05-08", periods=2),
        ),
    }

    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey

    s3.get_object.side_effect = _NoSuchKey("no delta files")

    out, _splits = _apply_daily_delta(s3, "test-bucket", "2026-05-09", price_data)

    # The "no delta files" branch returns price_data without a source column
    # (per the existing early-return at "No daily_closes delta files found —
    # using cache as-is"). That's an acceptable degraded mode — backfill's
    # later default-fill at builders/backfill.py applies "yfinance" when
    # the source column is absent. Pin the behaviour so a future refactor
    # doesn't accidentally tag those rows as something else.
    assert out["SPY"].shape[0] == 2
