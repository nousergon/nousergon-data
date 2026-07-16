"""Tests for the weekday EOD incremental price_cache refresh (config#2756).

``collect_daily_incremental`` / ``_refresh_stale(merge=True)`` fetch a short
recent yfinance window and MERGE it onto each ticker's existing parquet,
instead of the weekly ``collect()``'s full 10y auto_adjust=True rewrite —
so the Tue-Fri EOD pass can advance price_cache without a full-history
refetch for the whole universe every day.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd
from botocore.exceptions import ClientError

from collectors import prices


def _existing_parquet_bytes(dates: list[str], closes: list[float]) -> bytes:
    df = pd.DataFrame(
        {"Open": closes, "High": closes, "Low": closes, "Close": closes, "Volume": [1000] * len(closes)},
        index=pd.to_datetime(dates),
    )
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy")
    return buf.getvalue()


def _s3_with_existing(existing: dict[str, bytes]) -> MagicMock:
    s3 = MagicMock()

    def _get_object(Bucket, Key):
        ticker = Key.rsplit("/", 1)[-1].replace(".parquet", "")
        if ticker in existing:
            return {"Body": io.BytesIO(existing[ticker])}
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    s3.get_object.side_effect = _get_object

    # Capture uploaded parquet bytes immediately (the local file lives inside
    # a TemporaryDirectory that's gone by the time _refresh_stale returns).
    uploaded: dict[str, bytes] = {}

    def _upload_file(local_path, Bucket, Key):
        with open(local_path, "rb") as f:
            uploaded[Key] = f.read()

    s3.upload_file.side_effect = _upload_file
    s3.uploaded = uploaded
    return s3


def _yf_download_frame(dates: list[str], close: float) -> pd.DataFrame:
    return pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close, "Volume": 1000},
        index=pd.to_datetime(dates),
    )


class TestReadExistingCache:
    def test_returns_none_when_absent(self):
        s3 = _s3_with_existing({})
        assert prices._read_existing_cache(s3, "bucket", "reference/price_cache/", "AAPL") is None

    def test_reads_existing_parquet(self):
        payload = _existing_parquet_bytes(["2026-07-01"], [100.0])
        s3 = _s3_with_existing({"AAPL": payload})
        df = prices._read_existing_cache(s3, "bucket", "reference/price_cache/", "AAPL")
        assert df is not None
        assert df.loc["2026-07-01", "Close"] == 100.0

    def test_other_client_error_logs_and_returns_none(self):
        s3 = MagicMock()
        s3.get_object.side_effect = ClientError({"Error": {"Code": "403"}}, "GetObject")
        assert prices._read_existing_cache(s3, "bucket", "reference/price_cache/", "AAPL") is None


class TestRefreshStaleMerge:
    @patch("collectors.prices.yf.download")
    def test_merge_true_preserves_prior_history_and_appends_new_row(self, mock_download):
        existing_payload = _existing_parquet_bytes(
            ["2026-07-01", "2026-07-02"], [100.0, 101.0],
        )
        s3 = _s3_with_existing({"AAPL": existing_payload})
        mock_download.return_value = _yf_download_frame(["2026-07-03"], 102.0)

        refreshed, failed = prices._refresh_stale(
            s3, "bucket", "reference/price_cache/", ["AAPL"], "5d", 50, merge=True,
        )

        assert refreshed == 1
        assert failed == []
        [key] = [k for k in s3.uploaded if k.endswith("AAPL.parquet")]
        written = pd.read_parquet(io.BytesIO(s3.uploaded[key]))
        assert list(written.index.strftime("%Y-%m-%d")) == [
            "2026-07-01", "2026-07-02", "2026-07-03",
        ]
        assert written.loc["2026-07-03", "Close"] == 102.0
        # Prior history untouched.
        assert written.loc["2026-07-01", "Close"] == 100.0

    @patch("collectors.prices.yf.download")
    def test_merge_true_new_window_overrides_overlapping_date(self, mock_download):
        existing_payload = _existing_parquet_bytes(["2026-07-01"], [100.0])
        s3 = _s3_with_existing({"AAPL": existing_payload})
        # Same date refetched with a revised close (e.g. corporate-action re-basing).
        mock_download.return_value = _yf_download_frame(["2026-07-01"], 99.0)

        refreshed, failed = prices._refresh_stale(
            s3, "bucket", "reference/price_cache/", ["AAPL"], "5d", 50, merge=True,
        )

        assert refreshed == 1
        [key] = [k for k in s3.uploaded if k.endswith("AAPL.parquet")]
        written = pd.read_parquet(io.BytesIO(s3.uploaded[key]))
        assert len(written) == 1
        assert written.loc["2026-07-01", "Close"] == 99.0

    @patch("collectors.prices.yf.download")
    def test_merge_true_first_ever_write_when_no_existing_parquet(self, mock_download):
        s3 = _s3_with_existing({})
        mock_download.return_value = _yf_download_frame(["2026-07-03"], 102.0)

        refreshed, failed = prices._refresh_stale(
            s3, "bucket", "reference/price_cache/", ["AAPL"], "5d", 50, merge=True,
        )

        assert refreshed == 1
        assert failed == []

    @patch("collectors.prices.yf.download")
    def test_merge_false_ignores_existing_parquet_get_object(self, mock_download):
        # Weekly (merge=False) full-rewrite path must not touch existing state.
        s3 = _s3_with_existing({"AAPL": _existing_parquet_bytes(["2026-07-01"], [100.0])})
        mock_download.return_value = _yf_download_frame(
            ["2026-06-01", "2026-07-01"], 55.0,
        )

        refreshed, failed = prices._refresh_stale(
            s3, "bucket", "reference/price_cache/", ["AAPL"], "10y", 50, merge=False,
        )

        assert refreshed == 1
        s3.get_object.assert_not_called()


class TestCollectDailyIncremental:
    @patch("collectors.prices.boto3.client")
    @patch("collectors.prices._refresh_stale")
    def test_dry_run_short_circuits_before_any_fetch(self, mock_refresh, mock_boto):
        result = prices.collect_daily_incremental(
            bucket="bucket", tickers=["AAPL", "MSFT"], dry_run=True,
        )
        assert result["status"] == "ok_dry_run"
        mock_refresh.assert_not_called()

    @patch("collectors.prices.boto3.client")
    @patch("collectors.prices._refresh_stale")
    def test_calls_refresh_stale_with_merge_true_unconditionally(self, mock_refresh, mock_boto):
        mock_refresh.return_value = (2, [])
        result = prices.collect_daily_incremental(bucket="bucket", tickers=["AAPL", "MSFT"])

        assert result["status"] == "ok"
        assert result["refreshed"] == 2
        args, kwargs = mock_refresh.call_args
        assert kwargs.get("merge") is True or args[-1] is True

    @patch("collectors.prices.boto3.client")
    @patch("collectors.prices._refresh_stale")
    def test_partial_status_when_some_tickers_fail(self, mock_refresh, mock_boto):
        mock_refresh.return_value = (1, ["MSFT"])
        result = prices.collect_daily_incremental(bucket="bucket", tickers=["AAPL", "MSFT"])
        assert result["status"] == "partial"
        assert result["failed"] == 1
