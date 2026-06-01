"""Tests for the ``source`` parameter on collectors.daily_closes.collect.

Three modes:

  * ``yfinance_only`` (EOD pass) — polygon skipped entirely. yfinance + FRED only.
    VWAP=None for everything. Used by the EOD SF post-close.
  * ``polygon_only`` (morning pass) — polygon required (raises on failure).
    No yfinance fallback for stocks. FRED still serves the 4 indices polygon
    never provides. Used by the new MorningEnrich step in the weekday SF.
  * ``auto`` (legacy) — historical chain: polygon → FRED → yfinance fallback.
    Kept for backfill scripts.

These guard against the 2026-04-17 → 2026-04-23 silent-fail incident where
polygon's 403 caused yfinance to silently substitute, writing VWAP=None
across the entire universe.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from collectors import daily_closes
from polygon_client import PolygonForbiddenError


def _no_existing_parquet_s3():
    """S3 mock where head_object always returns 404 — fresh-day path."""
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadObject",
    )
    return s3


def _existing_parquet_s3(closes_by_ticker: dict[str, float], last_modified: datetime | None = None):
    """S3 mock with a pre-existing parquet that head_object/get_object surface."""
    s3 = MagicMock()
    s3.head_object.return_value = {
        "LastModified": last_modified or datetime(2026, 4, 22, 21, 0, 0, tzinfo=timezone.utc),
        "ContentLength": 12345,
        "ContentType": "application/octet-stream",
    }
    df = pd.DataFrame(
        [{"Open": v, "High": v, "Low": v, "Close": v, "Adj_Close": v, "Volume": 0, "VWAP": None}
         for v in closes_by_ticker.values()],
        index=pd.Index(list(closes_by_ticker.keys()), name="ticker"),
    )
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    buf.seek(0)
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: buf.getvalue())}
    return s3


# ── source validation ───────────────────────────────────────────────────────


def test_invalid_source_raises_value_error():
    with pytest.raises(ValueError, match="Invalid source"):
        daily_closes.collect(
            bucket="b", tickers=["AAPL"], run_date="2026-04-23", source="bogus"
        )


# ── yfinance_only mode ──────────────────────────────────────────────────────


def test_yfinance_only_skips_polygon_entirely():
    """yfinance_only must not call polygon (avoids the 403 + silent-fall-through path)."""
    s3 = _no_existing_parquet_s3()

    fake_yf_records = [
        {"ticker": "AAPL", "date": "2026-04-23",
         "Open": 100.0, "High": 105.0, "Low": 99.0, "Close": 103.0,
         "Adj_Close": 103.0, "Volume": 1_000_000, "VWAP": None}
    ]

    def fake_yf(_tickers, _date, records_out):
        records_out.extend(fake_yf_records)
        return len(fake_yf_records)

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", side_effect=fake_yf), \
         patch("collectors.daily_closes._fetch_polygon_closes") as polygon_spy:
        result = daily_closes.collect(
            bucket="b", tickers=["AAPL"], run_date="2026-04-23",
            source="yfinance_only", dry_run=True,
        )

    polygon_spy.assert_not_called()
    assert result["status"] == "ok_dry_run"
    assert result["source"] == "yfinance_only"
    assert result["polygon"] == 0


def test_yfinance_only_hard_fails_below_coverage_threshold():
    """yfinance_only must hard-fail if yfinance returns fewer stocks than threshold."""
    s3 = _no_existing_parquet_s3()
    # 100 tickers, yfinance returns only 50 → 50% coverage, well below 95% threshold
    tickers = [f"T{i:03d}" for i in range(100)]

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0), \
         patch("collectors.daily_closes._fetch_fred_closes", return_value=0):
        with pytest.raises(RuntimeError, match="below 95% threshold"):
            daily_closes.collect(
                bucket="b", tickers=tickers, run_date="2026-04-23",
                source="yfinance_only", dry_run=True,
            )


# ── polygon_only mode ───────────────────────────────────────────────────────


def test_polygon_only_propagates_polygon_forbidden():
    """polygon_only must propagate PolygonForbiddenError — no silent yfinance fallback."""
    s3 = _no_existing_parquet_s3()

    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.side_effect = PolygonForbiddenError("403 simulation")

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client), \
         patch("collectors.daily_closes._fetch_yfinance_closes") as yf_spy:
        with pytest.raises(PolygonForbiddenError):
            daily_closes.collect(
                bucket="b", tickers=["AAPL"], run_date="2026-04-23",
                source="polygon_only", dry_run=True,
            )
    yf_spy.assert_not_called()


def test_polygon_only_does_not_call_yfinance_for_equities():
    """polygon_only must never call yfinance for the EQUITY universe — even when
    polygon coverage is partial. (The FRED-index macro tickers have their own
    loud yfinance backstop, exercised by
    ``test_polygon_only_calls_yfinance_backstop_for_missing_macro``.)"""
    s3 = _no_existing_parquet_s3()

    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 100, "high": 105, "low": 99, "close": 103, "volume": 1_000_000, "vwap": 102.5},
        "MSFT": {"open": 200, "high": 210, "low": 199, "close": 206, "volume": 2_000_000, "vwap": 205.0},
    }

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client), \
         patch("collectors.daily_closes._fetch_yfinance_closes") as yf_spy:
        result = daily_closes.collect(
            bucket="b", tickers=["AAPL", "MSFT"], run_date="2026-04-23",
            source="polygon_only", dry_run=True,
        )

    yf_spy.assert_not_called()
    assert result["polygon"] == 2
    assert result["yfinance"] == 0


def test_polygon_only_hard_fails_on_empty_polygon_response():
    """polygon_only must hard-fail (not skip) when polygon returns empty for stocks."""
    s3 = _no_existing_parquet_s3()
    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.return_value = {}

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client):
        with pytest.raises(RuntimeError, match="0 tickers"):
            daily_closes.collect(
                bucket="b", tickers=["AAPL", "MSFT"], run_date="2026-04-23",
                source="polygon_only", dry_run=True,
            )


def test_polygon_only_writes_vwap_from_polygon():
    """When polygon returns a VWAP, it must land in the parquet."""
    s3 = _no_existing_parquet_s3()
    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 100, "high": 105, "low": 99, "close": 103, "volume": 1_000_000, "vwap": 102.5},
    }

    captured = {}
    def capture_put(**kwargs):
        captured.update(kwargs)
        return {}
    s3.put_object.side_effect = capture_put

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client):
        result = daily_closes.collect(
            bucket="b", tickers=["AAPL"], run_date="2026-04-23",
            source="polygon_only",
        )

    assert result["status"] == "ok"
    assert result["polygon"] == 1
    # Decode and verify VWAP populated
    import io
    df = pd.read_parquet(io.BytesIO(captured["Body"]), engine="pyarrow")
    assert df.loc["AAPL", "VWAP"] == 102.5


# ── overwrite + discrepancy logging ─────────────────────────────────────────


def test_polygon_only_calls_yfinance_backstop_for_missing_macro():
    """When FRED fails to supply a macro index ticker (the 2026-06-01 TNX 429),
    polygon_only must fall through to yfinance for that ticker ONLY — never for
    equities."""
    s3 = _no_existing_parquet_s3()
    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 100, "high": 105, "low": 99, "close": 103, "volume": 1_000_000, "vwap": 102.5},
    }

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client), \
         patch("collectors.daily_closes._fetch_fred_closes", return_value=0), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0) as yf_spy:
        daily_closes.collect(
            bucket="b", tickers=["AAPL", "^TNX"], run_date="2026-04-23",
            source="polygon_only", dry_run=True,
        )

    yf_spy.assert_called_once()
    backstop_tickers = yf_spy.call_args.args[0]
    assert backstop_tickers == ["^TNX"], "only the macro ticker may hit the yfinance backstop"
    assert "AAPL" not in backstop_tickers, "equities must never reach the yfinance backstop"


def test_polygon_only_retains_prior_macro_cell_on_live_gap():
    """2026-06-01 regression: a macro ticker the live pass cannot refresh (polygon
    never serves it, FRED 429, yfinance down) must RETAIN its prior parquet value
    rather than being blanked by the overwrite."""
    s3 = _existing_parquet_s3(
        {"AAPL": 103.0, "TNX": 4.5},
        last_modified=datetime(2026, 4, 22, 21, 0, 0, tzinfo=timezone.utc),  # post-close
    )
    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.return_value = {  # polygon serves AAPL only
        "AAPL": {"open": 100, "high": 105, "low": 99, "close": 103.5, "volume": 1_000_000, "vwap": 102.5},
    }

    captured = {}
    s3.put_object.side_effect = lambda **kw: captured.update(kw) or {}

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client), \
         patch("collectors.daily_closes._fetch_fred_closes", return_value=0), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0):
        result = daily_closes.collect(
            bucket="b", tickers=["AAPL", "^TNX"], run_date="2026-04-22",
            source="polygon_only",
        )

    assert result["status"] == "ok"
    import io
    df = pd.read_parquet(io.BytesIO(captured["Body"]), engine="pyarrow")
    assert "TNX" in df.index, "TNX must be retained, not blanked by the overwrite"
    assert df.loc["TNX", "Close"] == 4.5
    assert df.loc["AAPL", "Close"] == 103.5, "AAPL still overwritten with fresh polygon close"


def test_polygon_only_always_overwrites_existing_parquet():
    """polygon_only must NEVER skip on existing parquet — it re-fetches and
    overwrites every ticker the live pass *can* refresh (no skip-on-exists).
    Tickers the live pass cannot refresh are retained, not dropped — see
    ``test_polygon_only_retains_prior_macro_cell_on_live_gap``."""
    s3 = _existing_parquet_s3(
        {"AAPL": 103.0, "MSFT": 206.0},
        last_modified=datetime(2026, 4, 22, 21, 0, 0, tzinfo=timezone.utc),  # post-close
    )
    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 100, "high": 105, "low": 99, "close": 103.5, "volume": 1_000_000, "vwap": 102.5},
        "MSFT": {"open": 200, "high": 210, "low": 199, "close": 206.5, "volume": 2_000_000, "vwap": 205.0},
    }

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client):
        result = daily_closes.collect(
            bucket="b", tickers=["AAPL", "MSFT"], run_date="2026-04-22",
            source="polygon_only", dry_run=True,
        )

    assert result["status"] == "ok_dry_run"
    assert result["polygon"] == 2
    assert result.get("skipped") is not True


def test_polygon_only_logs_close_discrepancy(caplog):
    """When polygon overwrites yfinance, large Close discrepancies must be logged."""
    s3 = _existing_parquet_s3(
        {"AAPL": 100.0, "MSFT": 200.0},  # yfinance prior values
    )
    mock_pg_client = MagicMock()
    # AAPL ~5% drift — should log ERROR. MSFT ~0.5% drift — below threshold.
    mock_pg_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 105, "high": 110, "low": 104, "close": 105.5, "volume": 1_000_000, "vwap": 105.0},
        "MSFT": {"open": 200, "high": 210, "low": 199, "close": 201.0, "volume": 2_000_000, "vwap": 200.5},
    }

    import logging
    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client), \
         caplog.at_level(logging.INFO, logger="collectors.daily_closes"):
        daily_closes.collect(
            bucket="b", tickers=["AAPL", "MSFT"], run_date="2026-04-22",
            source="polygon_only", dry_run=True,
        )

    aapl_records = [r for r in caplog.records if "AAPL" in r.message and "OVERWRITE" in r.message]
    assert aapl_records, "expected AAPL discrepancy log (5% drift > 5% error threshold)"
    assert aapl_records[0].levelno >= logging.ERROR
    summary = [r for r in caplog.records if "discrepancy summary" in r.message]
    assert summary, "expected discrepancy summary log"


# ── auto mode preserves legacy behavior ─────────────────────────────────────


def test_auto_mode_falls_back_through_chain_on_polygon_403():
    """auto mode (legacy) must keep the silent fallback for backwards compat."""
    s3 = _no_existing_parquet_s3()
    mock_pg_client = MagicMock()
    mock_pg_client.get_grouped_daily.side_effect = PolygonForbiddenError("403 simulation")

    fake_yf_records = [
        {"ticker": "AAPL", "date": "2026-04-23",
         "Open": 100.0, "High": 105.0, "Low": 99.0, "Close": 103.0,
         "Adj_Close": 103.0, "Volume": 1_000_000, "VWAP": None}
    ]

    def fake_yf(_tickers, _date, records_out):
        records_out.extend(fake_yf_records)
        return len(fake_yf_records)

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
         patch("polygon_client.polygon_client", return_value=mock_pg_client), \
         patch("collectors.daily_closes._fetch_yfinance_closes", side_effect=fake_yf):
        result = daily_closes.collect(
            bucket="b", tickers=["AAPL"], run_date="2026-04-23",
            source="auto", dry_run=True,
        )

    # auto mode masks the failure — historical behavior preserved
    assert result["status"] == "ok_dry_run"
    assert result["polygon"] == 0
    assert result["yfinance"] == 1
