"""Tests for the ``skip_if_canonical`` parameter on
collectors.daily_closes.collect.

PR 2 of the windowed-data-reconciliation arc (plan doc:
``alpha-engine-docs/private/windowed-data-reconciliation-260510.md``).

When ``skip_if_canonical=True``:
- ``yfinance_only`` / ``auto`` modes read the existing parquet, identify
  tickers with ``source ∈ {"yfinance", "polygon"}`` AND non-null Close
  ("canonical"), skip yfinance fetch for those, and merge the preserved
  canonical rows into the output parquet.
- ``polygon_only`` mode IGNORES the flag — polygon always re-overwrites
  within the window per option (a) so corporate-action backfills are
  picked up. ``grouped-daily`` call rate stays at 1 per date regardless.
- Legacy post-close-skip short-circuit is bypassed in skip-aware mode
  so older window dates can still have NaN cells filled.
- If reading the existing parquet fails (e.g. corrupt), the per-date
  call falls back to the legacy refetch+overwrite path so a single
  bad parquet doesn't take down the window.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from collectors import daily_closes


def _existing_parquet_with_sources(
    rows: dict[str, dict],
    *,
    last_modified: datetime | None = None,
) -> MagicMock:
    """Build an S3 mock whose head_object returns the parquet metadata
    and get_object returns a parquet payload with per-ticker source +
    Close fields.

    ``rows`` maps ticker → field dict like ``{"Close": 100.0, "source": "yfinance"}``.
    Default Close=100, default source=None (NaN cell), Volume=0, VWAP=None.
    """
    s3 = MagicMock()
    s3.head_object.return_value = {
        "LastModified": last_modified or datetime(2026, 5, 7, 21, 0, 0, tzinfo=timezone.utc),
        "ContentLength": 12345,
        "ContentType": "application/octet-stream",
    }
    df_rows = []
    for ticker, fields in rows.items():
        df_rows.append({
            "Open": fields.get("Open", 100.0),
            "High": fields.get("High", 100.0),
            "Low": fields.get("Low", 100.0),
            "Close": fields.get("Close", 100.0),
            "Adj_Close": fields.get("Adj_Close", 100.0),
            "Volume": fields.get("Volume", 0),
            "VWAP": fields.get("VWAP"),
            "source": fields.get("source"),
        })
    df = pd.DataFrame(df_rows, index=pd.Index(list(rows.keys()), name="ticker"))
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    body_bytes = buf.getvalue()

    def _get_object(*args, **kwargs):
        return {"Body": MagicMock(read=lambda: body_bytes)}
    s3.get_object.side_effect = _get_object
    s3.put_object.return_value = {"ETag": '"abc"'}
    return s3


# ── skip_if_canonical=True with yfinance_only ───────────────────────────────


class TestSkipCanonicalYfinanceOnly:
    """yfinance-side skip semantics."""

    def test_skips_canonical_yfinance_tickers(self):
        """A ticker with source='yfinance' + non-null Close in the
        existing parquet should be skipped from the yfinance fetch."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 150.0, "source": "yfinance"},
            "MSFT": {"Close": 300.0, "source": "yfinance"},
        })
        captured_yf_missing: list = []

        def _yf_side(missing, run_date, records):
            captured_yf_missing.extend(t.lstrip("^") for t in missing)
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
                    "Adj_Close": 1.0, "Volume": 1, "VWAP": None,
                    "source": "yfinance",
                })
            return len(missing)

        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=_yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL", "MSFT", "GOOGL"],
                    run_date="2026-05-08",
                    source="yfinance_only", skip_if_canonical=True,
                )
        # AAPL + MSFT skipped (canonical); only GOOGL fetched.
        assert captured_yf_missing == ["GOOGL"]
        # Output should still include AAPL + MSFT (preserved from existing
        # parquet) plus GOOGL (fresh fetch).
        # We can verify via the put_object call's payload.
        put_call = s3.put_object.call_args
        body = put_call.kwargs["Body"]
        out_df = pd.read_parquet(io.BytesIO(body), engine="pyarrow")
        assert set(out_df.index) == {"AAPL", "MSFT", "GOOGL"}
        assert out_df.loc["AAPL", "Close"] == 150.0  # preserved
        assert out_df.loc["MSFT", "Close"] == 300.0  # preserved
        assert out_df.loc["GOOGL", "Close"] == 1.0   # freshly fetched

    def test_skips_canonical_polygon_tickers(self):
        """A ticker with source='polygon' should also be skipped — polygon
        is a higher-authority source than yfinance, never demote."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 150.0, "source": "polygon"},
        })
        captured_yf_missing: list = []

        def _yf_side(missing, run_date, records):
            captured_yf_missing.extend(t.lstrip("^") for t in missing)
            return 0

        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=_yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                daily_closes.collect(
                    bucket="b", tickers=["AAPL"],
                    run_date="2026-05-08",
                    source="yfinance_only", skip_if_canonical=True,
                )
        assert captured_yf_missing == []  # AAPL was skipped

    def test_does_not_skip_nan_close_tickers(self):
        """A ticker with source='yfinance' but Close=NaN is not canonical
        — yfinance should retry to fill the NaN."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": float("nan"), "source": "yfinance"},
        })
        captured_yf_missing: list = []

        def _yf_side(missing, run_date, records):
            captured_yf_missing.extend(t.lstrip("^") for t in missing)
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
                    "Adj_Close": 1.0, "Volume": 1, "VWAP": None,
                    "source": "yfinance",
                })
            return len(missing)

        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=_yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                daily_closes.collect(
                    bucket="b", tickers=["AAPL"],
                    run_date="2026-05-08",
                    source="yfinance_only", skip_if_canonical=True,
                )
        assert captured_yf_missing == ["AAPL"]  # NaN cell, refetched

    def test_does_not_skip_when_source_missing(self):
        """An existing parquet with no ``source`` column at all (legacy
        write before data #196) should not skip anything — falls back
        to legacy refetch."""
        s3 = MagicMock()
        s3.head_object.return_value = {
            "LastModified": datetime(2026, 5, 7, 21, 0, 0, tzinfo=timezone.utc),
            "ContentLength": 100,
            "ContentType": "application/octet-stream",
        }
        df = pd.DataFrame(
            [{"Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0,
              "Adj_Close": 100.0, "Volume": 0, "VWAP": None}],
            index=pd.Index(["AAPL"], name="ticker"),
        )
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: buf.getvalue())}
        s3.put_object.return_value = {"ETag": '"abc"'}

        captured_yf_missing: list = []

        def _yf_side(missing, run_date, records):
            captured_yf_missing.extend(t.lstrip("^") for t in missing)
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
                    "Adj_Close": 1.0, "Volume": 1, "VWAP": None,
                    "source": "yfinance",
                })
            return len(missing)

        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=_yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                daily_closes.collect(
                    bucket="b", tickers=["AAPL"],
                    run_date="2026-05-08",
                    source="yfinance_only", skip_if_canonical=True,
                )
        assert captured_yf_missing == ["AAPL"]  # no source col → no skip

    def test_bypasses_post_close_skip_short_circuit(self):
        """skip_if_canonical=True must bypass the legacy post-close skip
        because the whole point is to look INSIDE the existing parquet
        for NaN cells to fill, not skip the date entirely."""
        # Existing parquet last-modified post-close on 5/8 → legacy logic
        # would short-circuit. Set up: AAPL has NaN Close (needs refetch).
        s3 = _existing_parquet_with_sources(
            {"AAPL": {"Close": float("nan"), "source": "yfinance"}},
            last_modified=datetime(2026, 5, 8, 22, 0, 0, tzinfo=timezone.utc),
        )
        captured_yf_missing: list = []

        def _yf_side(missing, run_date, records):
            captured_yf_missing.extend(t.lstrip("^") for t in missing)
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
                    "Adj_Close": 1.0, "Volume": 1, "VWAP": None,
                    "source": "yfinance",
                })
            return len(missing)

        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=_yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL"],
                    run_date="2026-05-08",
                    source="yfinance_only", skip_if_canonical=True,
                )
        # Yfinance was invoked (legacy short-circuit bypassed) and the
        # NaN cell is now filled.
        assert captured_yf_missing == ["AAPL"]
        assert result.get("status") == "ok"


# ── skip_if_canonical=True with polygon_only (option a — flag ignored) ──────


class TestSkipCanonicalPolygonOnlyIgnoresFlag:
    """polygon_only mode ignores skip_if_canonical per option (a) —
    polygon always overwrites within the window so corporate-action
    backfills are absorbed. The 14/day grouped-daily contract still
    holds because polygon makes one call per date regardless of
    skip behavior."""

    def test_polygon_only_does_not_skip_canonical_polygon_tickers(self):
        """Existing parquet has polygon-source rows — polygon_only mode
        should still fetch every date in the window, ignoring the
        skip flag."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 150.0, "source": "polygon"},
            "MSFT": {"Close": 300.0, "source": "polygon"},
        })
        polygon_calls: list = []

        def _polygon_side(tickers, run_date, records, source):
            polygon_calls.append(run_date)
            for t in tickers:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 99.0, "High": 99.0, "Low": 99.0, "Close": 99.0,
                    "Adj_Close": 99.0, "Volume": 1, "VWAP": 99.0,
                    "source": "polygon",
                })
            return len(tickers)

        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", side_effect=_polygon_side,
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                daily_closes.collect(
                    bucket="b", tickers=["AAPL", "MSFT"],
                    run_date="2026-05-08",
                    source="polygon_only", skip_if_canonical=True,
                )
        # Polygon was called once (one grouped-daily for the date) and
        # fetched both tickers regardless of their canonical state.
        assert len(polygon_calls) == 1
        # Verify the output overwrote the existing rows with fresh polygon data.
        put_call = s3.put_object.call_args
        out_df = pd.read_parquet(io.BytesIO(put_call.kwargs["Body"]), engine="pyarrow")
        # Fresh polygon data has Close=99 (vs existing 150/300).
        assert out_df.loc["AAPL", "Close"] == 99.0
        assert out_df.loc["MSFT", "Close"] == 99.0


# ── Read-failure fallback ───────────────────────────────────────────────────


class TestSkipCanonicalReadFailureFallback:
    """If reading the existing parquet fails (corrupt / network), fall
    back to legacy refetch+overwrite for the date — don't take down
    the whole window because of one unreadable parquet."""

    def test_corrupt_parquet_falls_back_to_legacy_refetch(self):
        s3 = MagicMock()
        s3.head_object.return_value = {
            "LastModified": datetime(2026, 5, 7, 21, 0, 0, tzinfo=timezone.utc),
            "ContentLength": 12345,
            "ContentType": "application/octet-stream",
        }
        # Simulate a corrupt parquet: get_object succeeds but pandas can't
        # parse the body.
        s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b"not actually parquet bytes")
        }
        s3.put_object.return_value = {"ETag": '"abc"'}
        captured_yf_missing: list = []

        def _yf_side(missing, run_date, records):
            captured_yf_missing.extend(t.lstrip("^") for t in missing)
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
                    "Adj_Close": 1.0, "Volume": 1, "VWAP": None,
                    "source": "yfinance",
                })
            return len(missing)

        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=_yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL"],
                    run_date="2026-05-08",
                    source="yfinance_only", skip_if_canonical=True,
                )
        # Yfinance refetched (no canonical detected because parquet read failed).
        assert captured_yf_missing == ["AAPL"]
        assert result.get("status") == "ok"


# ── Default skip_if_canonical=False preserves legacy behavior ───────────────


class TestSkipCanonicalDefaultsFalse:
    def test_default_does_not_read_existing_parquet_for_canonical_extraction(self):
        """When skip_if_canonical=False (default), the existing-parquet
        canonical-extraction code path must not run. yfinance_only mode
        should still hit the legacy post-close skip-on-exists return.
        """
        s3 = _existing_parquet_with_sources(
            {"AAPL": {"Close": 150.0, "source": "yfinance"}},
            last_modified=datetime(2026, 5, 8, 22, 0, 0, tzinfo=timezone.utc),
        )
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ) as yf_mock, patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                    source="yfinance_only",
                )
        # Legacy short-circuit: no yfinance call, returns "skipped".
        yf_mock.assert_not_called()
        assert result.get("skipped") is True
