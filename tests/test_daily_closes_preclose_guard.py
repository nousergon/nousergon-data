"""Regression tests for the daily_closes pre-close skip guard.

Contract: ``collect()`` must only short-circuit with ``skipped=True`` when the
existing ``{run_date}.parquet`` was written at or after ``dates.SETTLED_AFTER_ET``
on ``run_date`` — the hour the session's official closing print is final. A write
before that is either a morning-side stale fetch (polygon returns T-1's aggregate
stamped under today's key) or an unsettled post-close fetch, and must be
overwritten by an authoritative collection.

alpha-engine-config-I11354 moved that threshold from the bare 16:00 ET close to
the settlement hour. The close is when the session ends; it is not when the bar
is final.

Incident that forced this: 2026-04-20. The predictor's morning DailyData
Step Function wrote the parquet at 06:07 PT with Friday's closes stamped
as Monday. The 16:14 PT post-close rerun hit the old ``head_object →
skip`` short-circuit and propagated the stale data through daily_append
into ArcticDB for every ticker, producing a false α = −1.33% on the EOD
email vs the real +0.08%.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from collectors import daily_closes


def _head_object_response(last_modified: datetime) -> dict:
    return {
        "LastModified": last_modified,
        "ContentLength": 12345,
        "ContentType": "application/octet-stream",
    }


def test_post_close_write_is_skipped():
    """If the parquet exists and was written after NYSE close, skip."""
    # 2026-04-20 is EDT; NYSE close = 20:00 UTC. Post-close write = 23:14 UTC.
    post_close = datetime(2026, 4, 20, 23, 14, 0, tzinfo=timezone.utc)

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = _head_object_response(post_close)

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3):
        result = daily_closes.collect(
            bucket="test-bucket",
            tickers=["AAPL"],
            run_date="2026-04-20",
        )

    assert result["skipped"] is True
    assert result["status"] == "ok"
    # No write attempted — we trusted the post-close file.
    mock_s3.put_object.assert_not_called()


def test_pre_close_write_forces_refetch():
    """If the parquet exists but was written pre-close, refuse to skip."""
    # 2026-04-20 morning write at 13:07 UTC (06:07 PT) — well before 20:00 UTC close.
    pre_close = datetime(2026, 4, 20, 13, 7, 0, tzinfo=timezone.utc)

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = _head_object_response(pre_close)

    # Force polygon+yfinance paths to return nothing so collect() bails with
    # "no data fetched" — we only need to prove the early-skip was NOT taken.
    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0):
        result = daily_closes.collect(
            bucket="test-bucket",
            tickers=["AAPL"],
            run_date="2026-04-20",
        )

    # Early-skip path would have returned {"skipped": True, "status": "ok"}.
    # We should have fallen through to the fetch path instead.
    assert result.get("skipped") is not True
    assert "skipped" not in result or result["skipped"] is not True


def test_is_post_close_write_edt():
    """2026-04-20 is in EDT, so 18:15 ET (``dates.SETTLED_AFTER_ET``) = 22:15 UTC.

    Rewritten for alpha-engine-config-I11354: this predicate used to fire at the
    bare 16:00 ET close, which declared a 16:06 ET parquet authoritative and made
    every later same-source pass skip it. Measured that day: Volume short on
    920/920 files (median 20.5 %), Close off on 442. The skip threshold is now
    the SETTLEMENT hour, not the close.
    """
    run_date = "2026-04-20"
    # 16:00 ET — the old threshold. The whole point of the change: NOT skippable.
    assert not daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 20, 0, 0, tzinfo=timezone.utc), run_date
    )
    # 16:06 ET — the measured v1 postclose fetch. NOT skippable.
    assert not daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 20, 6, 0, tzinfo=timezone.utc), run_date
    )
    # 16:45 ET — the standalone `data-collection-eod` time. NOT skippable.
    assert not daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 20, 45, 0, tzinfo=timezone.utc), run_date
    )
    # 1s before settlement → not skippable
    assert not daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 22, 14, 59, tzinfo=timezone.utc), run_date
    )
    # Exactly 18:15 ET → skippable (boundary is inclusive, as it was at 16:00)
    assert daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 22, 15, 0, tzinfo=timezone.utc), run_date
    )
    # 18:41 ET — the measured-settled fetch → skippable
    assert daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 22, 41, 0, tzinfo=timezone.utc), run_date
    )


def test_is_post_close_write_est():
    """2026-01-15 is in EST, so 18:15 ET = 23:15 UTC.

    The threshold is an EXCHANGE clock. A UTC literal would drift by an hour
    twice a year and would declare a full hour of unsettled writes authoritative
    every winter.
    """
    run_date = "2026-01-15"
    # 21:00 UTC = 16:00 ET — the old threshold, and 2h15m short of settlement.
    assert not daily_closes._is_post_close_write(
        datetime(2026, 1, 15, 21, 0, 0, tzinfo=timezone.utc), run_date
    )
    # 22:15 UTC = 17:15 EST — the SUMMER settlement instant, still short in winter.
    assert not daily_closes._is_post_close_write(
        datetime(2026, 1, 15, 22, 15, 0, tzinfo=timezone.utc), run_date
    )
    # 23:15 UTC = 18:15 EST → skippable
    assert daily_closes._is_post_close_write(
        datetime(2026, 1, 15, 23, 15, 0, tzinfo=timezone.utc), run_date
    )


def test_the_skip_threshold_is_the_declared_settlement_constant():
    """One declared hour, not two literals drifting apart.

    `dates.SETTLED_AFTER_ET` also drives `dates.bar_settlement`, the observe-mode
    verdict on D03/D19 manifests. If this predicate kept its own copy, a future
    move of the constant (alpha-engine-config-I11356 measures it properly) would
    silently leave the skip threshold behind — which is the defect this PR fixes,
    recurring one level down.
    """
    import dates

    run_date = "2026-04-20"
    hh, mm = (int(p) for p in dates.SETTLED_AFTER_ET.split(":"))
    at_threshold = datetime(2026, 4, 20, hh, mm, tzinfo=ZoneInfo("America/New_York"))
    assert daily_closes._is_post_close_write(at_threshold, run_date)
    assert not daily_closes._is_post_close_write(
        at_threshold - timedelta(seconds=1), run_date
    )
    # And the two graders agree on the same instant, by construction.
    assert dates.bar_settlement(at_threshold, run_date) == "settled"
    assert dates.bar_settlement(at_threshold - timedelta(seconds=1), run_date) == "provisional"


def test_a_provisional_parquet_is_refetched_not_skipped():
    """End to end: the 16:06 ET artifact no longer short-circuits a re-run.

    This is the behaviour change. Before, `collect()` returned
    ``{"skipped": True}`` and the unsettled bar stayed published; now it falls
    through to the fetch path and overwrites it.
    """
    provisional = datetime(2026, 4, 20, 20, 6, tzinfo=timezone.utc)  # 16:06 ET
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = _head_object_response(provisional)

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0):
        result = daily_closes.collect(
            bucket="test-bucket", tickers=["AAPL"], run_date="2026-04-20",
        )

    assert result.get("skipped") is not True


def test_a_settled_parquet_is_still_skipped_and_grades_settled():
    """The skip survives for an artifact written after settlement — and the
    manifest verdict it carries can only ever be `settled` now, because the skip
    predicate and the grader read the same constant."""
    settled = datetime(2026, 4, 20, 22, 41, tzinfo=timezone.utc)  # 18:41 ET
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = _head_object_response(settled)

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0):
        result = daily_closes.collect(
            bucket="test-bucket", tickers=["AAPL"], run_date="2026-04-20",
        )

    assert result["skipped"] is True
    assert result["status"] == "ok"
    assert [g["verdict"] for g in result["guards"]] == ["settled"]


def test_missing_object_proceeds_to_fetch():
    """404 on head_object is the expected fresh-day case — proceed to write."""
    from botocore.exceptions import ClientError

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadObject",
    )

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0):
        result = daily_closes.collect(
            bucket="test-bucket",
            tickers=["AAPL"],
            run_date="2026-04-20",
        )

    # Should not have short-circuited as skipped; fell through to fetch path.
    assert result.get("skipped") is not True


def test_head_object_auth_failure_propagates():
    """Non-404 errors on head_object must not silently fall through."""
    from botocore.exceptions import ClientError

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}},
        "HeadObject",
    )

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3):
        with pytest.raises(ClientError):
            daily_closes.collect(
                bucket="test-bucket",
                tickers=["AAPL"],
                run_date="2026-04-20",
            )
