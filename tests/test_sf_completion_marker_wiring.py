"""SF-envelope completion marker wiring (config#2857).

The completion marker is an end-of-SF terminal artifact, independent of
downstream pipeline deliverables, proving the Step Functions execution
itself reached its real success terminal (config#1724 independent-signal
doctrine). These tests pin the weekly (Saturday) SF's wiring: every real
completion path converges into ``WriteCompletionMarker`` before ending,
while the Friday-PM preflight (shell_run) dry-pass is excluded.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"


@pytest.fixture
def weekly_states():
    doc = json.loads((_INFRA / "step_function.json").read_text())
    return doc["States"]


REAL_COMPLETION_NOTIFIERS = [
    "NotifyComplete",
    "NotifyCompleteDegraded",
    "NotifyCompleteGatesDegraded",
    "NotifyCompleteHealthDegraded",
    "NotifyCompleteGatesAndHealthDegraded",
]

PREFLIGHT_NOTIFIERS = [
    "NotifyShellRunComplete",
    "NotifyShellRunCompleteDegraded",
]


def test_marker_state_shape(weekly_states):
    st = weekly_states["WriteCompletionMarker"]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    assert st["Parameters"]["Key.$"] == (
        "States.Format('_sf_completion/ne-weekly-freshness-pipeline/{}.json', $.run_date)"
    )
    body = st["Parameters"]["Body.$"]
    assert "ne-weekly-freshness-pipeline" in body
    assert "$$.Execution.Id" in body
    assert "$.run_date" in body
    assert st["End"] is True
    # Deliberate: no swallow-all Catch (unlike the SNS notifiers) — a marker
    # that genuinely cannot be written should surface as a real failure,
    # not be silently swallowed the way a non-fatal notify is.
    assert "Catch" not in st
    (retry,) = st["Retry"]
    assert retry["ErrorEquals"] == ["States.ALL"]
    assert retry["MaxAttempts"] >= 2


@pytest.mark.parametrize("name", REAL_COMPLETION_NOTIFIERS)
def test_real_completion_paths_converge_on_marker(weekly_states, name):
    st = weekly_states[name]
    assert "End" not in st, f"{name} must route through WriteCompletionMarker, not End directly"
    assert st["Next"] == "WriteCompletionMarker"


@pytest.mark.parametrize("name", PREFLIGHT_NOTIFIERS)
def test_preflight_paths_are_excluded_from_marker(weekly_states, name):
    """A Friday-PM dry pass must never satisfy the completion-marker SLA."""
    st = weekly_states[name]
    assert st.get("End") is True
    assert st.get("Next") != "WriteCompletionMarker"
