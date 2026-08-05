"""SF-envelope completion marker wiring — preopen/weekday SF (config#2857).

Companion to tests/test_sf_completion_marker_wiring.py (the Saturday SF).
Pins that the real completion path (RunDaemon success, its non-fatal Catch,
and the skip-gate edge) all converge on WriteCompletionMarker before
PipelineComplete, while a holiday skip or a real failure never does.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"


@pytest.fixture
def daily_states():
    doc = json.loads((_INFRA / "step_function_daily.json").read_text())
    return doc["States"]


def test_marker_state_shape(daily_states):
    st = daily_states["WriteCompletionMarker"]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    assert "ne-preopen-trading-pipeline" in st["Parameters"]["Key.$"]
    assert "$$.Execution.StartTime" in st["Parameters"]["Key.$"]
    body = st["Parameters"]["Body.$"]
    assert "ne-preopen-trading-pipeline" in body
    assert "$$.Execution.Id" in body
    assert st["Next"] == "PipelineComplete"
    assert "Catch" not in st
    (retry,) = st["Retry"]
    assert retry["ErrorEquals"] == ["States.ALL"]
    assert retry["MaxAttempts"] >= 2


def test_run_daemon_success_and_catch_both_converge_on_marker(daily_states):
    run_daemon = daily_states["RunDaemon"]
    assert run_daemon["Next"] == "WriteCompletionMarker"
    (catch,) = run_daemon["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "WriteCompletionMarker"


def test_skip_run_daemon_edge_converges_on_marker(daily_states):
    (choice,) = daily_states["CheckSkipRunDaemon"]["Choices"]
    assert choice["Next"] == "WriteCompletionMarker"


def test_holiday_skip_is_excluded_from_marker(daily_states):
    """A market-holiday skip must never satisfy the completion-marker SLA —
    the box is never even booted on that path."""
    holiday = daily_states["NotifyHolidaySkip"]
    assert holiday.get("Next") != "WriteCompletionMarker"
    assert "WriteCompletionMarker" not in json.dumps(holiday)
