"""Tests for collectors/universe_classification.py.

Mocks yfinance ``Ticker.info`` to exercise the collector without hitting the
network. Locks the invariants that mirror collectors/short_interest.py:
  1. A well-populated info dict produces correctly-shaped sector/country/industry.
  2. The artifact carries schema_version + the dated AND latest keys are both
     written from a single PUT helper.
  3. Per-ticker exceptions don't crash the run; the row gets all-null fields
     (a coverage gap, never a guessed value).
  4. Below-threshold ok_ratio returns status=error rather than writing a partial
     artifact to S3.
  5. Empty tickers / dry-run short-circuit without an S3 write.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from collectors import universe_classification as uc


def _make_yf(info_by_ticker: dict[str, dict]) -> MagicMock:
    """Build a yfinance mock where Ticker(t).info returns info_by_ticker[t]."""
    yf_mock = MagicMock()

    def ticker_factory(t):
        ticker_obj = MagicMock()
        ticker_obj.info = info_by_ticker.get(t, {})
        return ticker_obj

    yf_mock.Ticker.side_effect = ticker_factory
    return yf_mock


def test_well_populated_info_produces_typed_output_and_both_keys():
    yf_mock = _make_yf({
        "AAPL": {"sector": "Technology", "country": "United States", "industry": "Consumer Electronics"},
        "LIN": {"sector": "Materials", "country": "Ireland", "industry": "Specialty Chemicals"},
    })
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.universe_classification.boto3.client", return_value=fake_s3):
        result = uc.collect(
            bucket="test-bucket",
            tickers=["AAPL", "LIN"],
            run_date="2026-06-28",
            inter_request_delay=0.0,
        )

    assert result["status"] == "ok"
    assert result["ok_count"] == 2

    # Dated + latest keys both written from the single PUT helper.
    written_keys = {c.kwargs["Key"] for c in fake_s3.put_object.call_args_list}
    assert written_keys == {
        "market_data/universe_classification/2026-06-28.json",
        "market_data/universe_classification/latest.json",
    }

    body = json.loads(fake_s3.put_object.call_args_list[0].kwargs["Body"])
    assert body["schema_version"] == uc.UNIVERSE_CLASSIFICATION_SCHEMA_VERSION
    assert body["as_of"] == "2026-06-28"
    assert body["data"]["LIN"] == {
        "sector": "Materials", "country": "Ireland", "industry": "Specialty Chemicals",
    }


def test_partial_info_keeps_populated_fields():
    """A ticker missing industry still contributes sector+country (no fabrication)."""
    yf_mock = _make_yf({"MSFT": {"sector": "Technology", "country": "United States"}})
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.universe_classification.boto3.client", return_value=fake_s3):
        result = uc.collect(
            bucket="test-bucket", tickers=["MSFT"], run_date="2026-06-28", inter_request_delay=0.0,
        )
    assert result["status"] == "ok"
    body = json.loads(fake_s3.put_object.call_args_list[0].kwargs["Body"])
    assert body["data"]["MSFT"] == {
        "sector": "Technology", "country": "United States", "industry": None,
    }


def test_per_ticker_exception_does_not_crash():
    yf_mock = MagicMock()

    def ticker_factory(t):
        if t == "BAD":
            raise RuntimeError("yfinance internal error")
        ticker_obj = MagicMock()
        ticker_obj.info = {"sector": "Technology", "country": "United States", "industry": "Software"}
        return ticker_obj

    yf_mock.Ticker.side_effect = ticker_factory
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.universe_classification.boto3.client", return_value=fake_s3):
        result = uc.collect(
            bucket="test-bucket",
            tickers=["GOOD1", "BAD", "GOOD2"],
            run_date="2026-06-28",
            inter_request_delay=0.0,
        )

    assert result["status"] == "ok"
    assert result["ok_count"] == 2  # GOOD1 + GOOD2
    body = json.loads(fake_s3.put_object.call_args_list[0].kwargs["Body"])
    assert body["data"]["BAD"] == {"sector": None, "country": None, "industry": None}


def test_below_threshold_returns_error_no_s3_write():
    """If <50% of tickers populate any field, status=error and no S3 write."""
    yf_mock = MagicMock()

    def ticker_factory(t):
        ticker_obj = MagicMock()
        ticker_obj.info = (
            {"sector": "Technology", "country": "United States", "industry": "Software"}
            if t == "AAPL" else {}
        )
        return ticker_obj

    yf_mock.Ticker.side_effect = ticker_factory
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.universe_classification.boto3.client", return_value=fake_s3):
        result = uc.collect(
            bucket="test-bucket",
            tickers=["AAPL", "MSFT", "GOOG", "AMZN", "META"],
            run_date="2026-06-28",
            inter_request_delay=0.0,
        )

    assert result["status"] == "error"
    assert "below 50% threshold" in result["error"].lower()
    fake_s3.put_object.assert_not_called()


def test_dry_run_samples_first_five_no_s3_write():
    yf_mock = _make_yf({
        t: {"sector": "Technology", "country": "United States", "industry": "Software"}
        for t in ["A", "B", "C", "D", "E", "F", "G"]
    })
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.universe_classification.boto3.client", return_value=fake_s3):
        result = uc.collect(
            bucket="test-bucket",
            tickers=["A", "B", "C", "D", "E", "F", "G"],
            run_date="2026-06-28",
            inter_request_delay=0.0,
            dry_run=True,
        )

    assert result["status"] == "ok_dry_run"
    assert result["ticker_count"] == 5  # capped to first 5
    fake_s3.put_object.assert_not_called()


def test_empty_tickers_list_errors_immediately():
    result = uc.collect(bucket="test-bucket", tickers=[], run_date="2026-06-28")
    assert result["status"] == "error"
    assert "no tickers" in result["error"].lower()
