"""Tests for the ``window_days`` parameter on collectors.daily_closes.collect.

PR 1 of the windowed-data-reconciliation arc (plan doc:
``alpha-engine-docs/private/windowed-data-reconciliation-260510.md``).
This PR adds the structural orchestration only — default
``window_days=1`` preserves all existing single-date behavior.
Per-cell skip-if-canonical optimization lands in PR 2.

These tests pin:

1. ``window_days=1`` is byte-identical to the legacy single-date code
   path (no orchestration overhead, no result-shape change).
2. ``window_days > 1`` fans out to ``window_days`` per-date calls,
   newest written last. Polygon's ``grouped-daily`` is invoked at most
   once per date (the free-tier rate-limit contract).
3. The per-date result dicts aggregate into a stable window-mode
   return shape with ``per_date`` keyed by ISO date.
4. Per-date failures don't take down the rest of the window — the
   aggregate's ``status`` flips to ``"partial"`` but every other date
   completes.
5. ``_previous_business_days`` skips weekends and respects ``n``.
6. Invalid ``window_days`` (< 1) raises ``ValueError`` early.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from collectors import daily_closes


# ── Helper: minimal polygon stub ────────────────────────────────────────────


def _polygon_grouped_records(date: str, ticker: str, close: float) -> dict:
    return {
        "open": close, "high": close, "low": close, "close": close,
        "vwap": close, "volume": 1000,
    }


def _no_existing_parquet_s3():
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadObject",
    )
    return s3


# ── _previous_business_days helper ──────────────────────────────────────────


class TestPreviousBusinessDays:
    def test_n_eq_1_returns_run_date_only(self):
        # 2026-05-08 is a Friday.
        assert daily_closes._previous_business_days("2026-05-08", n=1) == [
            "2026-05-08"
        ]

    def test_n_eq_5_walks_back_through_weekends(self):
        # Friday 5/8 → Thu 5/7 → Wed 5/6 → Tue 5/5 → Mon 5/4 (5 BDays).
        assert daily_closes._previous_business_days("2026-05-08", n=5) == [
            "2026-05-08", "2026-05-07", "2026-05-06",
            "2026-05-05", "2026-05-04",
        ]

    def test_walks_across_weekend(self):
        # Monday 5/11 → Fri 5/8 → Thu 5/7 (3 BDays).
        assert daily_closes._previous_business_days("2026-05-11", n=3) == [
            "2026-05-11", "2026-05-08", "2026-05-07",
        ]

    def test_n_lt_1_raises(self):
        with pytest.raises(ValueError, match="n must be >= 1"):
            daily_closes._previous_business_days("2026-05-08", n=0)

    def test_14_bday_window(self):
        """Production default — ROADMAP-spec'd 14 BDays. Pin shape."""
        # 2026-05-08 (Friday) is a representative weekday run_date.
        dates = daily_closes._previous_business_days("2026-05-08", n=14)
        assert len(dates) == 14
        assert dates[0] == "2026-05-08"  # newest first
        assert dates[-1] == "2026-04-21"  # 14 BDays back from Fri 5/8
        # All dates are weekdays.
        for d in dates:
            assert datetime.strptime(d, "%Y-%m-%d").weekday() < 5

    def test_saturday_run_date_normalizes_to_friday(self):
        """Saturday SF firing at 02:00 PT shouldn't burn a slot on the
        non-trading Saturday; the function walks back to Friday."""
        # 2026-05-09 is a Saturday → first window date should be Fri 5/8.
        dates = daily_closes._previous_business_days("2026-05-09", n=3)
        assert dates == ["2026-05-08", "2026-05-07", "2026-05-06"]

    def test_sunday_run_date_normalizes_to_friday(self):
        # 2026-05-10 is a Sunday → first window date should also be Fri 5/8.
        dates = daily_closes._previous_business_days("2026-05-10", n=3)
        assert dates == ["2026-05-08", "2026-05-07", "2026-05-06"]


# ── window_days < 1 validation ──────────────────────────────────────────────


class TestWindowDaysValidation:
    def test_window_days_lt_1_raises(self):
        with pytest.raises(ValueError, match="window_days must be >= 1"):
            daily_closes.collect(
                bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                source="yfinance_only", window_days=0,
            )

    def test_window_days_negative_raises(self):
        with pytest.raises(ValueError, match="window_days must be >= 1"):
            daily_closes.collect(
                bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                source="yfinance_only", window_days=-3,
            )


# ── window_days=1: legacy parity ────────────────────────────────────────────


class TestWindowDays1Parity:
    """Default ``window_days=1`` must produce a byte-identical result to
    the legacy single-date call path. This is the no-behavior-change
    guarantee for PR 1.
    """

    def test_window_days_1_returns_single_date_shape(self):
        s3 = _no_existing_parquet_s3()
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=1
            ) as yf_mock, patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                # Make yfinance "fetch" populate the records list.
                def _yf_side(missing, run_date, records):
                    records.append({
                        "ticker": "AAPL", "date": run_date,
                        "Open": 100.0, "High": 100.0, "Low": 100.0,
                        "Close": 100.0, "Adj_Close": 100.0,
                        "Volume": 1000, "VWAP": None, "source": "yfinance",
                    })
                    return 1
                yf_mock.side_effect = _yf_side
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                    source="yfinance_only", window_days=1,
                )
        # Single-date shape — no per_date, no window_days, no skipped_dates.
        assert "per_date" not in result
        assert "window_days" not in result
        assert "skipped_dates" not in result
        assert result["status"] == "ok"
        assert result["source"] == "yfinance_only"
        assert result["tickers_captured"] == 1

    def test_window_days_default_is_1(self):
        """Existing call sites (no ``window_days`` argument) must continue
        to behave as single-date.
        """
        s3 = _no_existing_parquet_s3()
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes",
            ) as yf_mock, patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                def _yf_side(missing, run_date, records):
                    records.append({
                        "ticker": "AAPL", "date": run_date,
                        "Open": 100.0, "High": 100.0, "Low": 100.0,
                        "Close": 100.0, "Adj_Close": 100.0,
                        "Volume": 1000, "VWAP": None, "source": "yfinance",
                    })
                    return 1
                yf_mock.side_effect = _yf_side
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                    source="yfinance_only",
                )
        assert "per_date" not in result  # default = single-date


# ── window_days > 1: orchestration ──────────────────────────────────────────


class TestWindowOrchestration:
    """Window mode fans out to ``window_days`` per-date calls."""

    def _stub_fetches_each_date(self, captured_calls: list):
        """Build patches so each per-date call appends one record + tracks
        which run_date was passed."""
        def _yf_side(missing, run_date, records):
            captured_calls.append(("yfinance", run_date))
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 100.0, "High": 100.0, "Low": 100.0,
                    "Close": 100.0, "Adj_Close": 100.0,
                    "Volume": 1000, "VWAP": None, "source": "yfinance",
                })
            return len(missing)

        def _polygon_side(tickers, run_date, records, source):
            captured_calls.append(("polygon_grouped_daily", run_date))
            for t in tickers:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 100.0, "High": 100.0, "Low": 100.0,
                    "Close": 100.0, "Adj_Close": 100.0,
                    "Volume": 1000, "VWAP": 100.0, "source": "polygon",
                })
            return len(tickers)
        return _yf_side, _polygon_side

    def test_window_days_3_yfinance_calls_3_dates(self):
        """yfinance_only window mode triggers exactly window_days
        per-date calls, oldest → newest."""
        captured = []
        yf_side, _ = self._stub_fetches_each_date(captured)
        s3 = _no_existing_parquet_s3()
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                    source="yfinance_only", window_days=3,
                )
        # 3 yfinance per-date fetches, oldest first.
        yf_dates = [d for kind, d in captured if kind == "yfinance"]
        assert yf_dates == ["2026-05-06", "2026-05-07", "2026-05-08"]
        # Aggregate result shape.
        assert result["status"] == "ok"
        assert result["window_days"] == 3
        assert set(result["per_date"].keys()) == {
            "2026-05-06", "2026-05-07", "2026-05-08",
        }
        assert result["tickers_captured"] == 3  # 1 ticker × 3 dates
        assert result["source"] == "yfinance_only"
        assert result["yfinance"] == 3

    def test_polygon_only_window_makes_one_grouped_daily_per_date(self):
        """Polygon free-tier rate-limit contract: ``window_days``
        ``grouped-daily`` calls in total, one per date — the only way
        to honor 14/day at the free tier."""
        captured = []
        _, polygon_side = self._stub_fetches_each_date(captured)
        s3 = _no_existing_parquet_s3()
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", side_effect=polygon_side,
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                result = daily_closes.collect(
                    bucket="b", tickers=["AAPL", "MSFT"],
                    run_date="2026-05-08",
                    source="polygon_only", window_days=14,
                )
        polygon_dates = [d for kind, d in captured if kind == "polygon_grouped_daily"]
        # Exactly window_days grouped-daily calls — the free-tier ceiling.
        assert len(polygon_dates) == 14
        assert result["window_days"] == 14
        assert result["polygon"] == 14 * 2  # 2 tickers × 14 dates
        assert len(result["per_date"]) == 14

    def test_window_mode_propagates_skip_if_canonical(self):
        """Default window-mode call sets skip_if_canonical=True per the
        windowed-data-reconciliation arc design.
        """
        captured = []

        def _yf_side(missing, run_date, records):
            captured.append(("yfinance", run_date, len(missing)))
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 100.0, "High": 100.0, "Low": 100.0,
                    "Close": 100.0, "Adj_Close": 100.0,
                    "Volume": 1000, "VWAP": None, "source": "yfinance",
                })
            return len(missing)

        s3 = _no_existing_parquet_s3()
        with patch("collectors.daily_closes.boto3.client", return_value=s3):
            with patch.object(
                daily_closes, "_fetch_polygon_closes", return_value=0
            ), patch.object(
                daily_closes, "_fetch_yfinance_closes", side_effect=_yf_side,
            ), patch.object(
                daily_closes, "_fetch_fred_closes", return_value=0
            ):
                daily_closes.collect(
                    bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                    source="yfinance_only", window_days=2,
                    skip_if_canonical=True,
                )
        # Both per-date calls fired (skip flag propagates but no canonical
        # rows in the parquet stub → no skip happens; just verify the
        # fan-out works).
        assert len(captured) == 2

    def test_per_date_failure_does_not_block_window(self):
        """If one date errors (e.g. a coverage-gate trip on a non-trading
        day), the rest of the window completes. Aggregate status flips
        to ``partial``."""
        captured = []
        s3 = _no_existing_parquet_s3()

        def _yf_side(missing, run_date, records):
            captured.append(run_date)
            if run_date == "2026-05-07":
                # Simulate yfinance returning nothing on this date — the
                # coverage gate should fire and the date's call raises.
                return 0
            for t in missing:
                records.append({
                    "ticker": t.lstrip("^"), "date": run_date,
                    "Open": 100.0, "High": 100.0, "Low": 100.0,
                    "Close": 100.0, "Adj_Close": 100.0,
                    "Volume": 1000, "VWAP": None, "source": "yfinance",
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
                    bucket="b", tickers=["AAPL"], run_date="2026-05-08",
                    source="yfinance_only", window_days=3,
                )
        # All 3 dates were attempted.
        assert sorted(captured) == ["2026-05-06", "2026-05-07", "2026-05-08"]
        # Aggregate flagged partial because 5/7 hit the coverage gate.
        assert result["status"] == "partial"
        assert "2026-05-07" in result["per_date"]
        assert result["per_date"]["2026-05-07"]["status"] == "error"
        # Other dates still succeeded.
        assert result["per_date"]["2026-05-06"]["status"] == "ok"
        assert result["per_date"]["2026-05-08"]["status"] == "ok"
