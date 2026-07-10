"""Pins config#1645 groom-dispatch SF relaunch recovery wiring.

Origin: 2026-07-06 — Opus groom died ~7 min in (spot/OOM); the dispatch SF
detected marker-absent and entered PrepRelaunch, then ExecutionFailed at
NotifyRelaunch with States.Runtime because PrepRelaunch dropped $.groomPoll
(and CheckCompletionMarker headObject returned 403 — IAM had GetObject but not
HeadObject). Recovery never launched a second box.

This test catches regressions like:
- PrepRelaunch / SetForceOnDemand stripping groomPoll (relaunch notify/runtime)
- NotifyRelaunch blocking LaunchGroomSpot (notify must follow launch)
- SF execution role missing s3:HeadObject for the completion-marker check
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_groom.json"
_IAM_PATH = (
    _REPO_ROOT
    / "infrastructure"
    / "lambdas"
    / "scheduled-groom-dispatcher"
    / "sf-execution-iam-policy.json"
)

_PRESERVE_PATHS = ("groomPoll.$", "groomLaunch.$")


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def iam_policy() -> dict:
    return json.loads(_IAM_PATH.read_text())


def test_completion_marker_check_uses_head_object(states):
    st = states["CheckCompletionMarker"]
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:headObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    assert "groom/_control/completed/" in st["Parameters"]["Key.$"]


def test_sf_role_grants_head_object_on_completion_marker(iam_policy):
    marker_stmts = [
        s
        for s in iam_policy["Statement"]
        if s.get("Sid") == "CheckGroomRunCompletionMarker"
    ]
    assert len(marker_stmts) == 1
    actions = marker_stmts[0]["Action"]
    if isinstance(actions, str):
        actions = [actions]
    assert "s3:HeadObject" in actions
    assert "s3:GetObject" in actions


def test_prep_relaunch_preserves_poll_context_and_routes_to_force_on_demand_gate(states):
    st = states["PrepRelaunch"]
    params = st["Parameters"]
    for key in _PRESERVE_PATHS:
        assert key in params, f"PrepRelaunch must preserve {key} through relaunch"
    assert st["Next"] == "CheckForceOnDemand"


def test_set_force_on_demand_preserves_poll_context(states):
    st = states["SetForceOnDemand"]
    params = st["Parameters"]
    for key in _PRESERVE_PATHS:
        assert key in params
    assert st["Next"] == "LaunchGroomSpot"
    assert params["fod"] == {"force_on_demand": True}


def test_relaunch_critical_path_launch_before_notify(states):
    """LaunchGroomSpot must precede NotifyRelaunch; notify must not gate relaunch."""
    assert states["CheckForceOnDemand"]["Default"] == "LaunchGroomSpot"
    assert states["SetForceOnDemand"]["Next"] == "LaunchGroomSpot"
    assert states["LaunchGroomSpot"]["Next"] == "RelaunchNotifyGate"
    assert states["RelaunchNotifyGate"]["Default"] == "CheckLaunched"
    relaunch_choices = states["RelaunchNotifyGate"]["Choices"]
    assert relaunch_choices[0]["Next"] == "NotifyRelaunch"
    assert states["NotifyRelaunch"]["Next"] == "CheckLaunched"
    assert states["NotifyRelaunch"]["Catch"][0]["Next"] == "CheckLaunched"


def test_notify_relaunch_message_uses_preserved_groom_poll_status(states):
    msg = states["NotifyRelaunch"]["Parameters"]["Message.$"]
    assert "$.groomPoll.Status" in msg
    assert "States.JsonToString($.fod.force_on_demand)" in msg
