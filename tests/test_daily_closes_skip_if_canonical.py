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


# ── skip_if_canonical=True with polygon_only (config#717: split-aware skip) ──


def _polygon_side_factory(polygon_calls: list, close: float = 99.0):
    """A ``_fetch_polygon_closes`` stand-in that records each call's date and
    appends a fresh polygon row per ticker."""

    def _polygon_side(tickers, run_date, records, source):
        polygon_calls.append(run_date)
        for t in tickers:
            records.append({
                "ticker": t.lstrip("^"), "date": run_date,
                "Open": close, "High": close, "Low": close, "Close": close,
                "Adj_Close": close, "Volume": 1, "VWAP": close,
                "source": "polygon",
            })
        return len(tickers)

    return _polygon_side


class TestSkipCanonicalPolygonOnlySplitAware:
    """config#717: polygon_only + skip_if_canonical now skips a date whose
    parquet is already fully polygon-canonical, EXCEPT dates a recently
    executed split restated (those re-fetch so the adjusted close stays on
    the current scale). Root-cause fix for the 2026-06-03 30-min timeout —
    polygon previously blanket-ignored the flag and re-fetched every date."""

    def test_skips_clean_canonical_polygon_date(self):
        """All stock tickers polygon-canonical + no recent split → skip the
        polygon grouped-daily call entirely."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 150.0, "source": "polygon"},
            "MSFT": {"Close": 300.0, "source": "polygon"},
        })
        polygon_calls: list = []
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes",
                side_effect=_polygon_side_factory(polygon_calls),
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_recent_split_dates", return_value=set(),
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL", "MSFT"],
                    run_date="2026-05-08",
                    source="polygon_only", skip_if_canonical=True,
                )
        # Skipped: no polygon fetch, no S3 write, status ok+skipped.
        assert polygon_calls == []
        s3.put_object.assert_not_called()
        assert result.get("skipped") is True
        assert result.get("skipped_reason") == "polygon_canonical"

    def test_refetches_date_touched_by_split(self):
        """A split restating this date (date < execution_date) forces a
        re-fetch even though the parquet is fully canonical."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 1500.0, "source": "polygon"},  # pre-split price
            "MSFT": {"Close": 300.0, "source": "polygon"},
        })
        polygon_calls: list = []
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes",
                side_effect=_polygon_side_factory(polygon_calls),
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_recent_split_dates",
                return_value={"2026-05-08"},  # AAPL 10:1 executed after this date
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL", "MSFT"],
                    run_date="2026-05-08",
                    source="polygon_only", skip_if_canonical=True,
                )
        # Re-fetched: one grouped-daily call, parquet rewritten with split-
        # adjusted close.
        assert polygon_calls == ["2026-05-08"]
        assert result.get("skipped") is not True
        out_df = pd.read_parquet(
            io.BytesIO(s3.put_object.call_args.kwargs["Body"]), engine="pyarrow",
        )
        assert out_df.loc["AAPL", "Close"] == 99.0  # fresh restated value

    def test_refetches_when_split_touched_dates_passed_directly(self):
        """The window path threads ``split_touched_dates`` down — verify the
        per-date collect honors it (and does not re-scan splits itself)."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 1500.0, "source": "polygon"},
        })
        polygon_calls: list = []
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes",
                side_effect=_polygon_side_factory(polygon_calls),
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_recent_split_dates",
            ) as scan_mock:
                daily_closes.collect(
                    bucket="b", tickers=["AAPL"],
                    run_date="2026-05-08",
                    source="polygon_only", skip_if_canonical=True,
                    split_touched_dates={"2026-05-08"},
                )
        # split_touched_dates supplied → no inline split scan, and re-fetched.
        scan_mock.assert_not_called()
        assert polygon_calls == ["2026-05-08"]

    def test_refetches_non_canonical_date(self):
        """A ticker missing polygon source (legacy yfinance row) means the
        date is NOT fully polygon-canonical → re-fetch even with no split."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 150.0, "source": "polygon"},
            "MSFT": {"Close": 300.0, "source": "yfinance"},  # not polygon
        })
        polygon_calls: list = []
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes",
                side_effect=_polygon_side_factory(polygon_calls),
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_recent_split_dates", return_value=set(),
            ):
                daily_closes.collect(
                    bucket="b", tickers=["AAPL", "MSFT"],
                    run_date="2026-05-08",
                    source="polygon_only", skip_if_canonical=True,
                )
        assert polygon_calls == ["2026-05-08"]

    def test_first_fetch_no_existing_parquet_still_works(self):
        """No existing parquet (first fetch) → polygon must fetch + write,
        skip logic never triggers."""
        s3 = MagicMock()
        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject",
        )
        s3.put_object.return_value = {"ETag": '"abc"'}
        polygon_calls: list = []
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes",
                side_effect=_polygon_side_factory(polygon_calls),
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_recent_split_dates", return_value=set(),
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL", "MSFT"],
                    run_date="2026-05-08",
                    source="polygon_only", skip_if_canonical=True,
                )
        assert polygon_calls == ["2026-05-08"]
        assert result.get("status") == "ok"
        assert result.get("skipped") is not True

    def test_legacy_default_polygon_overwrites_without_flag(self):
        """skip_if_canonical=False (default) preserves the legacy overwrite —
        polygon always re-fetches, no split scan."""
        s3 = _existing_parquet_with_sources({
            "AAPL": {"Close": 150.0, "source": "polygon"},
        }, last_modified=datetime(2026, 5, 7, 21, 0, 0, tzinfo=timezone.utc))
        polygon_calls: list = []
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes",
                side_effect=_polygon_side_factory(polygon_calls),
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_recent_split_dates",
            ) as scan_mock:
                daily_closes.collect(
                    bucket="b", tickers=["AAPL"],
                    run_date="2026-05-08",
                    source="polygon_only",  # skip_if_canonical defaults False
                )
        assert polygon_calls == ["2026-05-08"]
        scan_mock.assert_not_called()


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


# ── config#717 helper unit tests ────────────────────────────────────────────


class TestPolygonDateFullyCanonical:
    def test_all_polygon_canonical_returns_true(self):
        df = pd.DataFrame(
            [{"Close": 1.0, "source": "polygon"},
             {"Close": 2.0, "source": "polygon"}],
            index=pd.Index(["AAPL", "MSFT"], name="ticker"),
        )
        assert daily_closes._polygon_date_fully_canonical(df, ["AAPL", "MSFT"]) is True

    def test_missing_ticker_returns_false(self):
        df = pd.DataFrame(
            [{"Close": 1.0, "source": "polygon"}],
            index=pd.Index(["AAPL"], name="ticker"),
        )
        assert daily_closes._polygon_date_fully_canonical(df, ["AAPL", "MSFT"]) is False

    def test_non_polygon_source_returns_false(self):
        df = pd.DataFrame(
            [{"Close": 1.0, "source": "yfinance"}],
            index=pd.Index(["AAPL"], name="ticker"),
        )
        assert daily_closes._polygon_date_fully_canonical(df, ["AAPL"]) is False

    def test_nan_close_returns_false(self):
        df = pd.DataFrame(
            [{"Close": float("nan"), "source": "polygon"}],
            index=pd.Index(["AAPL"], name="ticker"),
        )
        assert daily_closes._polygon_date_fully_canonical(df, ["AAPL"]) is False

    def test_no_source_column_returns_false(self):
        df = pd.DataFrame(
            [{"Close": 1.0}], index=pd.Index(["AAPL"], name="ticker"),
        )
        assert daily_closes._polygon_date_fully_canonical(df, ["AAPL"]) is False

    def test_index_tickers_do_not_block_skip(self):
        """FRED-index macro tickers never come from polygon — they must not
        block a polygon skip nor be required to have source='polygon'."""
        df = pd.DataFrame(
            [{"Close": 1.0, "source": "polygon"},
             {"Close": 20.0, "source": "fred"}],
            index=pd.Index(["AAPL", "VIX"], name="ticker"),
        )
        # ^VIX is a FRED-index ticker; only AAPL (equity) must be polygon.
        assert daily_closes._polygon_date_fully_canonical(df, ["AAPL", "^VIX"]) is True

    def test_empty_stock_universe_vacuously_canonical(self):
        df = pd.DataFrame(
            [{"Close": 20.0, "source": "fred"}],
            index=pd.Index(["VIX"], name="ticker"),
        )
        assert daily_closes._polygon_date_fully_canonical(df, ["^VIX"]) is True


class TestFetchRecentSplitDates:
    def test_marks_dates_before_split_execution(self):
        client = MagicMock()
        client.get_recent_splits.return_value = [
            {"ticker": "AAPL", "execution_date": "2026-05-09",
             "split_from": 1, "split_to": 10},
        ]
        window = ["2026-05-11", "2026-05-08", "2026-05-07"]  # newest-first
        touched = daily_closes._fetch_recent_split_dates(window, client=client)
        # Dates strictly before 2026-05-09 are restated.
        assert touched == {"2026-05-08", "2026-05-07"}
        # One range-scoped call covering [oldest .. newest+lookahead].
        assert client.get_recent_splits.call_count == 1
        args = client.get_recent_splits.call_args.args
        assert args[0] == "2026-05-07"  # oldest

    def test_no_splits_returns_empty(self):
        client = MagicMock()
        client.get_recent_splits.return_value = []
        touched = daily_closes._fetch_recent_split_dates(
            ["2026-05-08"], client=client,
        )
        assert touched == set()

    def test_split_after_whole_window_touches_all(self):
        client = MagicMock()
        client.get_recent_splits.return_value = [
            {"ticker": "X", "execution_date": "2026-06-01",
             "split_from": 1, "split_to": 2},
        ]
        window = ["2026-05-09", "2026-05-08"]
        touched = daily_closes._fetch_recent_split_dates(window, client=client)
        assert touched == {"2026-05-09", "2026-05-08"}

    def test_split_predating_window_touches_nothing(self):
        client = MagicMock()
        client.get_recent_splits.return_value = [
            {"ticker": "X", "execution_date": "2026-05-07",
             "split_from": 1, "split_to": 2},
        ]
        window = ["2026-05-09", "2026-05-08"]
        touched = daily_closes._fetch_recent_split_dates(window, client=client)
        assert touched == set()

    def test_scan_failure_degrades_to_empty(self):
        client = MagicMock()
        client.get_recent_splits.side_effect = RuntimeError("polygon down")
        touched = daily_closes._fetch_recent_split_dates(
            ["2026-05-08"], client=client,
        )
        assert touched == set()

    def test_empty_window_returns_empty(self):
        assert daily_closes._fetch_recent_split_dates([]) == set()
