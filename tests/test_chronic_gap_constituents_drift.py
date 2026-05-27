"""Regression tests for `weekly_collector._detect_chronic_gap_constituents_drift`.

Mirrors `_detect_chronic_gap_polygon_recovery` on the inverse axis. The
polygon-recovery detector catches "polygon now serves this — remove from
allowlist"; this detector catches "ticker no longer a constituent —
remove from allowlist".

Origin: 2026-05-27 flow-doctor ERROR "Ticker PSTG not found in universe".
PSTG dropped from S&P 500/400 constituents between the 5/16 and 5/23
weekly partitions but stayed in the chronic_polygon_gaps allowlist;
MorningEnrich's chronic-gap self-heal yfinance-backfilled PSTG.parquet
then called ``backfill(ticker_filter='PSTG')``, which hard-erred
against the constituents filter at ``builders/backfill.py``. This
detector is the GATE that closes the loop so a config that lags a
constituents change becomes a WARN + skip instead of a hard ERROR.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _patch_constituents_loader(constituents_set, weekly_date="2026-05-23"):
    """Patch the shared constituents loader to return the given set + date."""
    return patch(
        "builders._constituents_loader.load_constituents_for_run_date",
        return_value=(set(constituents_set), weekly_date),
    )


def test_constituents_drift_no_drop_returns_full_list():
    """Steady-state happy path: every chronic ticker is still a
    constituent; nothing dropped, CW metric emits 0."""
    from weekly_collector import _detect_chronic_gap_constituents_drift

    cw = MagicMock()
    s3 = MagicMock()
    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        with _patch_constituents_loader(
            constituents_set={"AAPL", "MSFT", "BF-B", "PSTG"},
            weekly_date="2026-05-16",
        ):
            result = _detect_chronic_gap_constituents_drift(
                bucket="test-bucket",
                chronic_tickers=["BF-B", "PSTG"],
            )

    assert result["status"] == "ok"
    assert result["chronic_tickers_checked"] == 2
    assert sorted(result["still_constituents"]) == ["BF-B", "PSTG"]
    assert result["dropped_non_constituent"] == []
    assert result["weekly_date"] == "2026-05-16"
    cw.put_metric_data.assert_called_once()
    metric = cw.put_metric_data.call_args.kwargs["MetricData"][0]
    assert metric["MetricName"] == "chronic_gap_non_constituent_count"
    assert metric["Value"] == 0.0


def test_constituents_drift_drops_non_constituent_pstg():
    """The 2026-05-27 PSTG case verbatim: PSTG no longer in constituents
    set (5/23 partition); detector drops it; remaining chronic ticker
    proceeds; CW metric emits 1."""
    from weekly_collector import _detect_chronic_gap_constituents_drift

    cw = MagicMock()
    s3 = MagicMock()
    # 5/23 constituents: AAPL/MSFT/BF-B in, PSTG OUT
    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        with _patch_constituents_loader(
            constituents_set={"AAPL", "MSFT", "BF-B"},
            weekly_date="2026-05-23",
        ):
            result = _detect_chronic_gap_constituents_drift(
                bucket="test-bucket",
                chronic_tickers=["BF-B", "PSTG"],
            )

    assert result["status"] == "ok"
    assert result["still_constituents"] == ["BF-B"]
    assert result["dropped_non_constituent"] == ["PSTG"]
    assert result["weekly_date"] == "2026-05-23"
    metric = cw.put_metric_data.call_args.kwargs["MetricData"][0]
    assert metric["MetricName"] == "chronic_gap_non_constituent_count"
    assert metric["Value"] == 1.0


def test_constituents_drift_drops_all_chronic_tickers():
    """Pathological case: every chronic ticker has dropped. Returned
    still_constituents list is empty; caller's self-heal then becomes
    a no-op (no yfinance fetch, no backfill call)."""
    from weekly_collector import _detect_chronic_gap_constituents_drift

    cw = MagicMock()
    s3 = MagicMock()
    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        with _patch_constituents_loader(
            constituents_set={"AAPL", "MSFT"},
            weekly_date="2026-05-30",
        ):
            result = _detect_chronic_gap_constituents_drift(
                bucket="test-bucket",
                chronic_tickers=["BF-B", "PSTG"],
            )

    assert result["status"] == "ok"
    assert result["still_constituents"] == []
    assert sorted(result["dropped_non_constituent"]) == ["BF-B", "PSTG"]
    metric_value = cw.put_metric_data.call_args.kwargs["MetricData"][0]["Value"]
    assert metric_value == 2.0


def test_constituents_drift_empty_chronic_list_is_noop():
    """No chronic tickers configured → no S3 read, no metric emit."""
    from weekly_collector import _detect_chronic_gap_constituents_drift

    cw = MagicMock()
    s3 = MagicMock()
    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        result = _detect_chronic_gap_constituents_drift(
            bucket="test-bucket",
            chronic_tickers=[],
        )

    assert result["status"] == "ok"
    assert result["chronic_tickers_checked"] == 0
    assert result["still_constituents"] == []
    assert result["dropped_non_constituent"] == []
    cw.put_metric_data.assert_not_called()


def test_constituents_drift_falls_through_on_read_failure():
    """Best-effort: a constituents read failure logs a WARN and returns
    status='skipped' with the FULL chronic list as still_constituents.
    The caller's self-heal proceeds with the original list and the
    existing backfill-side hard-err remains the load-bearing surface."""
    from weekly_collector import _detect_chronic_gap_constituents_drift

    cw = MagicMock()
    s3 = MagicMock()

    def _raise_on_load(*_args, **_kwargs):
        raise RuntimeError("simulated S3 read failure")

    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        with patch(
            "builders._constituents_loader.load_constituents_for_run_date",
            side_effect=_raise_on_load,
        ):
            result = _detect_chronic_gap_constituents_drift(
                bucket="test-bucket",
                chronic_tickers=["BF-B", "PSTG"],
            )

    assert result["status"] == "skipped"
    # Crucial: still_constituents preserves the FULL list on failure so
    # the caller falls back to the pre-drift-detection behavior.
    assert sorted(result["still_constituents"]) == ["BF-B", "PSTG"]
    assert result["dropped_non_constituent"] == []
    assert len(result["errors"]) == 1
    # No CW emit on the skipped path — it's an observability metric;
    # missing-data is the right CW shape for an upstream substrate fault.
    cw.put_metric_data.assert_not_called()


def test_constituents_drift_metric_emit_failure_does_not_raise():
    """Best-effort metric emit: if CW put_metric_data fails, the
    detector still returns the filter result without raising. The
    drift filter result is the primary deliverable; CW emit is
    observability hung off it."""
    from weekly_collector import _detect_chronic_gap_constituents_drift

    cw = MagicMock()
    cw.put_metric_data.side_effect = RuntimeError("CW emit failed")
    s3 = MagicMock()
    with patch("weekly_collector.boto3.client",
               side_effect=lambda svc: s3 if svc == "s3" else cw):
        with _patch_constituents_loader(
            constituents_set={"AAPL", "BF-B"},
            weekly_date="2026-05-23",
        ):
            result = _detect_chronic_gap_constituents_drift(
                bucket="test-bucket",
                chronic_tickers=["BF-B", "PSTG"],
            )

    # Filter logic still landed despite CW failure.
    assert result["status"] == "ok"
    assert result["still_constituents"] == ["BF-B"]
    assert result["dropped_non_constituent"] == ["PSTG"]
