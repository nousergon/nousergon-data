"""Unit tests for execution_digest.py (config#1672)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from execution_digest import (
    STATE_DURATION_FLOORS_SEC,
    build_execution_digest,
    build_state_durations,
    format_digest_lines,
    parse_task_state_durations,
    StateDuration,
)


def _ts(base: datetime, offset_sec: int) -> datetime:
    return base.replace(tzinfo=timezone.utc) + __import__("datetime").timedelta(seconds=offset_sec)


def test_parse_task_state_durations_computes_wall_clock():
    base = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "type": "TaskStateEntered",
            "timestamp": base,
            "taskStateEnteredEventDetails": {"name": "PredictorTraining"},
        },
        {
            "type": "TaskStateExited",
            "timestamp": _ts(base, 120),
            "taskStateExitedEventDetails": {"name": "PredictorTraining"},
        },
    ]
    assert parse_task_state_durations(events)["PredictorTraining"] == 120


def test_floor_breach_detected_when_under_minimum():
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PredictorTraining": 120},
        is_preflight=False,
        execution_start=start,
        run_date="2026-07-04",
        s3_client=None,
    )
    assert len(rows) == 1
    assert rows[0].floor_breach is True
    assert rows[0].anomaly is True


def test_preflight_suppresses_floor_breach():
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PredictorTraining": 30},
        is_preflight=True,
        execution_start=start,
        run_date=None,
        s3_client=None,
    )
    assert rows[0].floor_breach is False


def test_format_digest_sorts_anomalies_visually():
    rows = [
        StateDuration("Backtester", 600, 600, False, False),
        StateDuration("PredictorTraining", 120, 1200, True, False),
    ]
    lines = format_digest_lines(rows)
    assert any("PredictorTraining" in line and "⚠️" in line for line in lines)
    assert any("Backtester" in line and "✓" in line for line in lines)


def test_build_execution_digest_hollow_on_fast_predictor():
    start_ms = 1_700_000_000_000
    sf = MagicMock()
    base = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    sf.get_execution_history.return_value = {
        "events": [
            {
                "type": "TaskStateEntered",
                "timestamp": base,
                "taskStateEnteredEventDetails": {"name": "PredictorTraining"},
            },
            {
                "type": "TaskStateExited",
                "timestamp": _ts(base, 90),
                "taskStateExitedEventDetails": {"name": "PredictorTraining"},
            },
        ],
    }
    lines, hollow = build_execution_digest(
        execution_arn="arn:aws:states:us-east-1:123:execution:sm:exec",
        is_preflight=False,
        execution_start_ms=start_ms,
        run_date="2026-07-04",
        sf_client=sf,
        s3_client=None,
    )
    assert hollow is True
    assert any("PredictorTraining" in line for line in lines)
    assert STATE_DURATION_FLOORS_SEC["PredictorTraining"] == 20 * 60


def test_history_fetch_failure_surfaces_marker():
    sf = MagicMock()
    sf.get_execution_history.side_effect = RuntimeError("throttled")
    lines, hollow = build_execution_digest(
        execution_arn="arn:exec",
        is_preflight=False,
        execution_start_ms=1_700_000_000_000,
        run_date=None,
        sf_client=sf,
        s3_client=None,
    )
    assert hollow is False
    assert any("digest unavailable" in line for line in lines)
