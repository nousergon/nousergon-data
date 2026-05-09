"""Regression tests for `weekly_collector._detect_chronic_gap_polygon_recovery`.

The chronic_polygon_gaps allowlist (alpha-engine-config #88, predictor.yaml
``chronic_polygon_gaps.tickers``) was added because polygon doesn't reliably
serve BF-B / BRK-B / MOG-A / PSTG. If polygon coverage RECOVERS for any
of these — e.g. polygon adds a Berkshire B share class CIK or fixes a
flaky data feed — the allowlist entry becomes operational debt.

This module's drift alarm reads the polygon_only daily_closes parquet
written by ``daily_closes.collect`` and counts how many chronic tickers
polygon DID cover today. CW metric
``AlphaEngine/Data/chronic_gap_polygon_recovery_count`` carries the
count so an alarm can fire if it's > 0 across multiple cycles
(operator action: prune the allowlist).
"""
from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd


def _stub_s3_with_parquet(parquet_df: pd.DataFrame, expected_key: str):
    """Build an S3 stub that serves a parquet body for ``expected_key`` and
    raises NoSuchKey for anything else."""
    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey

    def _get_object(Bucket, Key):
        if Key != expected_key:
            raise _NoSuchKey(f"key={Key} not stubbed")
        buf = io.BytesIO()
        parquet_df.to_parquet(buf, engine="pyarrow")
        buf.seek(0)
        return {"Body": MagicMock(read=lambda buf=buf: buf.read())}

    s3.get_object.side_effect = _get_object
    return s3


def _daily_closes_frame(tickers: list[str]) -> pd.DataFrame:
    """Build a daily_closes parquet shape: index=ticker, OHLCV cols."""
    df = pd.DataFrame(
        {
            "Open": [100.0] * len(tickers),
            "High": [101.0] * len(tickers),
            "Low":  [99.0] * len(tickers),
            "Close": [100.5] * len(tickers),
            "Volume": [1_000_000] * len(tickers),
        },
        index=tickers,
    )
    df.index.name = "ticker"
    return df


def test_drift_detection_no_recovery_returns_zero():
    """Steady-state happy path: polygon doesn't cover any chronic ticker
    (the expected case); recovery count = 0; metric emits 0."""
    from weekly_collector import _detect_chronic_gap_polygon_recovery

    # 3 normal tickers, no chronic coverage
    parquet = _daily_closes_frame(["AAPL", "MSFT", "GOOG"])
    s3 = _stub_s3_with_parquet(parquet, "staging/daily_closes/2026-05-09.parquet")

    cw = MagicMock()
    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        result = _detect_chronic_gap_polygon_recovery(
            bucket="test-bucket",
            target_date="2026-05-09",
            chronic_tickers=["BF-B", "BRK-B", "MOG-A", "PSTG"],
        )

    assert result["status"] == "ok"
    assert result["chronic_tickers_checked"] == 4
    assert result["polygon_recovered"] == []
    assert sorted(result["absent_as_expected"]) == ["BF-B", "BRK-B", "MOG-A", "PSTG"]
    cw.put_metric_data.assert_called_once()
    call = cw.put_metric_data.call_args
    metric_name = call.kwargs["MetricData"][0]["MetricName"]
    metric_value = call.kwargs["MetricData"][0]["Value"]
    assert metric_name == "chronic_gap_polygon_recovery_count"
    assert metric_value == 0.0


def test_drift_detection_partial_recovery_logs_and_emits():
    """Drift signal: polygon covered 2 of 4 chronic tickers. Recovery list
    surfaces in the result; CW metric emits the count."""
    from weekly_collector import _detect_chronic_gap_polygon_recovery

    # 3 normal + 2 chronic that recovered (BRK-B, PSTG appear)
    parquet = _daily_closes_frame(
        ["AAPL", "MSFT", "GOOG", "BRK-B", "PSTG"]
    )
    s3 = _stub_s3_with_parquet(parquet, "staging/daily_closes/2026-05-09.parquet")
    cw = MagicMock()

    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        result = _detect_chronic_gap_polygon_recovery(
            bucket="test-bucket",
            target_date="2026-05-09",
            chronic_tickers=["BF-B", "BRK-B", "MOG-A", "PSTG"],
        )

    assert result["status"] == "ok"
    assert sorted(result["polygon_recovered"]) == ["BRK-B", "PSTG"]
    assert sorted(result["absent_as_expected"]) == ["BF-B", "MOG-A"]
    metric_value = cw.put_metric_data.call_args.kwargs["MetricData"][0]["Value"]
    assert metric_value == 2.0


def test_drift_detection_handles_missing_parquet():
    """Best-effort: parquet read failure returns status=skipped without
    raising. MorningEnrich must not be blocked by a transient S3 read
    error in observability code."""
    from weekly_collector import _detect_chronic_gap_polygon_recovery

    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey
    s3.get_object.side_effect = _NoSuchKey("simulated read failure")

    with patch("weekly_collector.boto3.client", return_value=s3):
        result = _detect_chronic_gap_polygon_recovery(
            bucket="test-bucket",
            target_date="2026-05-09",
            chronic_tickers=["BF-B", "BRK-B"],
        )

    assert result["status"] == "skipped"
    assert len(result["errors"]) == 1


def test_drift_detection_empty_chronic_list_is_noop():
    """No chronic tickers configured → no metric emit, no S3 read."""
    from weekly_collector import _detect_chronic_gap_polygon_recovery

    s3 = MagicMock()
    cw = MagicMock()
    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        result = _detect_chronic_gap_polygon_recovery(
            bucket="test-bucket",
            target_date="2026-05-09",
            chronic_tickers=[],
        )

    assert result["status"] == "ok"
    assert result["chronic_tickers_checked"] == 0
    assert result["polygon_recovered"] == []
    s3.get_object.assert_not_called()
    cw.put_metric_data.assert_not_called()


def test_drift_detection_metric_emit_failure_is_swallowed():
    """CW metric emission failure must not raise — drift alarm is purely
    observability, never load-bearing. Result still records the recovery
    count for the manifest."""
    from weekly_collector import _detect_chronic_gap_polygon_recovery

    parquet = _daily_closes_frame(["AAPL", "BRK-B"])
    s3 = _stub_s3_with_parquet(parquet, "staging/daily_closes/2026-05-09.parquet")
    cw = MagicMock()
    cw.put_metric_data.side_effect = RuntimeError("CW throttled")

    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        result = _detect_chronic_gap_polygon_recovery(
            bucket="test-bucket",
            target_date="2026-05-09",
            chronic_tickers=["BRK-B"],
        )

    assert result["status"] == "ok"
    assert result["polygon_recovered"] == ["BRK-B"]
