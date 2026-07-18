"""SF-envelope completion marker wiring — postclose/EOD SF (config#2857).

Companion to test_sf_completion_marker_wiring.py (Saturday) and
test_sf_completion_marker_wiring_daily.py (preopen). config-I2702 deliverable
#4 deliberately keeps NormalSucceeded/DegradedSucceeded as two distinct named
terminals (visible in the SF console/notifications) — so unlike the other two
SFs, this one writes the marker TWICE (once per outcome) rather than
converging both paths onto one shared marker state.
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
    "marker_name,succeed_target",
    [
        ("WriteCompletionMarkerNormal", "NormalSucceeded"),
        ("WriteCompletionMarkerDegraded", "DegradedSucceeded"),
    ],
)
def test_marker_state_shape(eod_states, marker_name, succeed_target):
    st = eod_states[marker_name]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    assert "ne-postclose-trading-pipeline" in st["Parameters"]["Key.$"]
    assert "$.run_date" in st["Parameters"]["Key.$"]
    body = st["Parameters"]["Body.$"]
    assert "ne-postclose-trading-pipeline" in body
    assert "$$.Execution.Id" in body
    assert st["Next"] == succeed_target
    assert "Catch" not in st
    (retry,) = st["Retry"]
    assert retry["ErrorEquals"] == ["States.ALL"]
    assert retry["MaxAttempts"] >= 2


def test_check_degraded_outcome_routes_through_markers(eod_states):
    choice = eod_states["CheckDegradedOutcome"]
    assert choice["Default"] == "WriteCompletionMarkerNormal"
    (degraded_choice,) = choice["Choices"]
    assert degraded_choice["Next"] == "WriteCompletionMarkerDegraded"
