"""Tests for ``collectors/fred_history.py`` — Stage 2.5b of regime-
conditioning rebuild.

Validates:
- ``fetch_fred_history`` retry + parse contract (mocked HTTP)
- ``fred_history_to_ohlcv`` schema invariant matches yfinance parquet shape
- ``backfill_to_s3`` orchestration (mocked S3 + FRED)
- ``FRED_HISTORY_MAP`` covers TWO + HYOAS

Plan doc: ~/Development/alpha-engine-docs/private/regime-conditioning-260510.md
"""
from __future__ import annotations

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.fred_history import (
    FRED_HISTORY_MAP,
    backfill_to_s3,
    fetch_fred_history,
    fred_history_to_ohlcv,
)


# ── FRED_HISTORY_MAP contract ──────────────────────────────────────────


class TestFredHistoryMap:

    def test_two_maps_to_dgs2(self):
        assert FRED_HISTORY_MAP["TWO"] == "DGS2"

    def test_hyoas_maps_to_bamlh0a0hym2(self):
        assert FRED_HISTORY_MAP["HYOAS"] == "BAMLH0A0HYM2"

    def test_baa10y_maps_to_baa10y(self):
        # Stage 2.5c: Moody's BAA Corporate Bond Yield Relative to 10Y
        # Treasury. Full 40y FRED history (1986+) — full-corpus credit
        # regime signal that HYOAS cannot provide (license-gated to 2023+).
        assert FRED_HISTORY_MAP["BAA10Y"] == "BAA10Y"

    def test_no_unintended_entries(self):
        # Stage 2.5b shipped TWO + HYOAS; Stage 2.5c added BAA10Y.
        # Lock so a future drive-by addition doesn't slip through
        # unreviewed.
        assert set(FRED_HISTORY_MAP.keys()) == {"TWO", "HYOAS", "BAA10Y"}


# ── fetch_fred_history ──────────────────────────────────────────────────


def _mock_fred_response(observations: list[dict]) -> MagicMock:
    """Build a mocked requests.Response carrying a FRED API payload."""
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    resp.json = MagicMock(return_value={"observations": observations})
    return resp


class TestFetchFredHistory:

    def test_basic_parse(self):
        observations = [
            {"date": "2018-01-02", "value": "1.95"},
            {"date": "2018-01-03", "value": "2.01"},
            {"date": "2018-01-04", "value": "2.10"},
        ]
        with patch("collectors.fred_history.requests.get",
                   return_value=_mock_fred_response(observations)):
            df = fetch_fred_history("DGS2", api_key="fake")
        assert len(df) == 3
        assert list(df.columns) == ["value"]
        assert df.index[0] == pd.Timestamp("2018-01-02")
        assert df["value"].iloc[0] == 1.95
        assert df["value"].iloc[-1] == 2.10

    def test_drops_missing_observations(self):
        # FRED uses "." as the missing-value marker.
        observations = [
            {"date": "2018-01-02", "value": "1.95"},
            {"date": "2018-01-03", "value": "."},
            {"date": "2018-01-04", "value": "2.10"},
        ]
        with patch("collectors.fred_history.requests.get",
                   return_value=_mock_fred_response(observations)):
            df = fetch_fred_history("DGS2", api_key="fake")
        assert len(df) == 2
        assert df.index.tolist() == [pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-04")]

    def test_sorts_ascending(self):
        # FRED returns asc by request param, but lock the post-condition
        # in case server returns out-of-order.
        observations = [
            {"date": "2018-01-04", "value": "2.10"},
            {"date": "2018-01-02", "value": "1.95"},
            {"date": "2018-01-03", "value": "2.01"},
        ]
        with patch("collectors.fred_history.requests.get",
                   return_value=_mock_fred_response(observations)):
            df = fetch_fred_history("DGS2", api_key="fake")
        assert df.index.is_monotonic_increasing

    def test_raises_when_no_api_key(self):
        original = os.environ.pop("FRED_API_KEY", None)
        try:
            with pytest.raises(RuntimeError, match="FRED_API_KEY not set"):
                fetch_fred_history("DGS2")
        finally:
            if original is not None:
                os.environ["FRED_API_KEY"] = original

    def test_raises_when_no_observations(self):
        with patch("collectors.fred_history.requests.get",
                   return_value=_mock_fred_response([])):
            with pytest.raises(RuntimeError, match="no observations"):
                fetch_fred_history("DGS2", api_key="fake")

    def test_raises_when_all_missing(self):
        observations = [
            {"date": "2018-01-02", "value": "."},
            {"date": "2018-01-03", "value": "."},
        ]
        with patch("collectors.fred_history.requests.get",
                   return_value=_mock_fred_response(observations)):
            with pytest.raises(RuntimeError, match="every observation"):
                fetch_fred_history("DGS2", api_key="fake")

    def test_retries_on_request_exception(self):
        import requests as _requests
        call_count = [0]

        def flaky_get(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                raise _requests.exceptions.ConnectionError("transient")
            return _mock_fred_response([{"date": "2018-01-02", "value": "1.95"}])

        with patch("collectors.fred_history.requests.get", side_effect=flaky_get), \
             patch("collectors.fred_history.time.sleep"):
            df = fetch_fred_history("DGS2", api_key="fake")
        assert len(df) == 1
        assert call_count[0] == 3


# ── fred_history_to_ohlcv ──────────────────────────────────────────────


class TestFredHistoryToOhlcv:

    def test_schema_matches_yfinance_parquet_shape(self):
        # Predictor reads parquets with these columns. The OHLCV reshape
        # MUST emit them so cfg.MOMENTUM_FEATURES + cfg.VOLATILITY_FEATURES
        # downstream readers don't trip on missing columns.
        df_fred = pd.DataFrame(
            {"value": [1.95, 2.01, 2.10]},
            index=pd.DatetimeIndex(["2018-01-02", "2018-01-03", "2018-01-04"]),
        )
        out = fred_history_to_ohlcv(df_fred)
        for col in ("Open", "High", "Low", "Close", "Adj_Close", "Volume", "VWAP", "source"):
            assert col in out.columns

    def test_ohlc_replicate_value(self):
        df_fred = pd.DataFrame(
            {"value": [1.95, 2.01]},
            index=pd.DatetimeIndex(["2018-01-02", "2018-01-03"]),
        )
        out = fred_history_to_ohlcv(df_fred)
        # FRED single-value → all OHLC the same, no intra-day range.
        assert (out["Open"] == out["Close"]).all()
        assert (out["High"] == out["Close"]).all()
        assert (out["Low"] == out["Close"]).all()
        assert out["Close"].iloc[0] == 1.95

    def test_volume_is_zero_vwap_is_none(self):
        df_fred = pd.DataFrame(
            {"value": [1.95]},
            index=pd.DatetimeIndex(["2018-01-02"]),
        )
        out = fred_history_to_ohlcv(df_fred)
        assert out["Volume"].iloc[0] == 0
        assert out["VWAP"].iloc[0] is None

    def test_source_is_fred(self):
        df_fred = pd.DataFrame(
            {"value": [1.95]},
            index=pd.DatetimeIndex(["2018-01-02"]),
        )
        out = fred_history_to_ohlcv(df_fred)
        assert out["source"].iloc[0] == "fred"

    def test_raises_when_value_column_missing(self):
        df = pd.DataFrame({"foo": [1.0]}, index=pd.DatetimeIndex(["2018-01-02"]))
        with pytest.raises(ValueError, match="Expected 'value' column"):
            fred_history_to_ohlcv(df)


# ── backfill_to_s3 ─────────────────────────────────────────────────────


class TestBackfillToS3:

    def _patch_fetch_returning(self, value: float = 1.95, n_rows: int = 100):
        df = pd.DataFrame(
            {"value": [value] * n_rows},
            index=pd.date_range("2018-01-02", periods=n_rows, freq="B"),
        )
        return patch(
            "collectors.fred_history.fetch_fred_history",
            return_value=df,
        )

    def test_dry_run_skips_s3_upload(self):
        with self._patch_fetch_returning():
            result = backfill_to_s3(
                bucket="test-bucket",
                tickers=["TWO"],
                dry_run=True,
            )
        assert result["status"] == "ok"
        assert result["dry_run"] is True
        assert result["refreshed"] == 1
        assert result["per_ticker"]["TWO"]["status"] == "ok"

    def test_unknown_ticker_raises(self):
        with pytest.raises(ValueError, match="Unknown FRED-history tickers"):
            backfill_to_s3(
                bucket="test-bucket",
                tickers=["UNKNOWN"],
                dry_run=True,
            )

    def test_default_tickers_is_all_known(self):
        with self._patch_fetch_returning():
            result = backfill_to_s3(
                bucket="test-bucket",
                tickers=None,  # → all known
                dry_run=True,
            )
        assert result["total"] == len(FRED_HISTORY_MAP)
        for ticker in FRED_HISTORY_MAP:
            assert ticker in result["per_ticker"]

    def test_per_ticker_error_does_not_abort_others(self):
        # First ticker fails; second succeeds. backfill should report
        # partial status, not raise.
        df_ok = pd.DataFrame(
            {"value": [1.95] * 10},
            index=pd.date_range("2018-01-02", periods=10, freq="B"),
        )

        def maybe_fail(series_id, period_years=10, api_key=None):
            if series_id == "DGS2":
                raise RuntimeError("synthetic failure for TWO")
            return df_ok

        with patch("collectors.fred_history.fetch_fred_history", side_effect=maybe_fail):
            result = backfill_to_s3(
                bucket="test-bucket",
                tickers=["TWO", "HYOAS"],
                dry_run=True,
            )
        assert result["status"] == "partial"
        assert result["refreshed"] == 1
        assert result["per_ticker"]["TWO"]["status"] == "error"
        assert result["per_ticker"]["HYOAS"]["status"] == "ok"

    def test_uploads_to_s3_when_not_dry_run(self):
        """Wave 3 PR4 (cutover): each FRED-sourced ticker writes to ONLY the
        new ``reference/price_cache/`` prefix — write-both is retired and the
        legacy ``predictor/price_cache/`` write is GONE."""
        with self._patch_fetch_returning(), \
             patch("collectors.fred_history.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3
            result = backfill_to_s3(
                bucket="test-bucket",
                tickers=["TWO"],
                dry_run=False,
            )
        assert result["dry_run"] is False
        # upload_file called once — single (reference-only) write post-cutover.
        assert mock_s3.upload_file.call_count == 1
        keys = sorted(
            call.args[2] for call in mock_s3.upload_file.call_args_list
        )
        assert keys == ["reference/price_cache/TWO.parquet"]
        assert not any(
            k.startswith("predictor/price_cache/") for k in keys
        )
        buckets = {call.args[1] for call in mock_s3.upload_file.call_args_list}
        assert buckets == {"test-bucket"}
