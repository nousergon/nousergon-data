"""SF-envelope completion marker wiring — postclose/EOD SF (config#2857).

Companion to test_sf_completion_marker_wiring.py (Saturday) and
test_sf_completion_marker_wiring_daily.py (preopen). config-I2702 deliverable
#4 + Brian's 2026-07-28 Option-A ruling (alpha-engine-config#2699) keeps the
degraded path as a distinct Fail terminal (DegradedRun), visible to all
status-keyed watchers — so unlike the other two SFs, this one writes the
marker TWICE (once per outcome) rather than converging both paths onto one
shared marker state.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"


@pytest.fixture
def eod_states():
    doc = json.loads((_INFRA / "step_function_eod.json").read_text())
    return doc["States"]


@pytest.mark.parametrize(
    "marker_name,terminal_target",
    [
        ("WriteCompletionMarkerNormal", "NormalSucceeded"),
        ("WriteCompletionMarkerDegraded", "DegradedRun"),
    ],
)
def test_marker_state_shape(eod_states, marker_name, terminal_target):
    st = eod_states[marker_name]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    assert "ne-postclose-trading-pipeline" in st["Parameters"]["Key.$"]
    assert "$.run_date" in st["Parameters"]["Key.$"]
    body = st["Parameters"]["Body.$"]
    assert "ne-postclose-trading-pipeline" in body
    assert "$$.Execution.Id" in body
    assert st["Next"] == terminal_target
    assert "Catch" not in st
    (retry,) = st["Retry"]
    assert retry["ErrorEquals"] == ["States.ALL"]
    assert retry["MaxAttempts"] >= 2


def test_check_degraded_outcome_routes_through_markers(eod_states):
    choice = eod_states["CheckDegradedOutcome"]
    assert choice["Default"] == "WriteCompletionMarkerNormal"
    (degraded_choice,) = choice["Choices"]
    assert degraded_choice["Next"] == "WriteCompletionMarkerDegraded"
