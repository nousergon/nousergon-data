"""The weekly SF's terminal Fail must name the underlying error.

alpha-engine-config#5601. Every failure of this pipeline used to terminate
with an identical static ``Cause``, so ``states:DescribeExecution`` reported
41 of the last 60 runs as the same opaque ``PipelineFailure``. No
control-plane consumer could tell one failure mode from another, and "is
this one bug recurring or forty different bugs" was answerable only by
opening CloudWatch per run, by hand.

These tests pin the two properties that make a failure histogram possible,
and the structural precondition that makes reading ``$.error`` at the Fail
state safe.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SF_JSON = REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(SF_JSON.read_text())["States"]


def _inbound_edges(states: dict, target: str) -> list[str]:
    """Every state with a Next/Default edge into ``target`` (including the
    edges nested inside Choice/Catch/Retry blocks)."""
    found: list[str] = []

    def walk(owner: str, node) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("Next", "Default") and value == target:
                    found.append(owner)
                else:
                    walk(owner, value)
        elif isinstance(node, list):
            for item in node:
                walk(owner, item)

    for name, body in states.items():
        walk(name, body)
    return found


class TestTerminalFailNamesTheError:
    def test_cause_is_derived_not_constant(self, states):
        fail = states["FailExecution"]
        assert "CausePath" in fail, (
            "FailExecution must derive its Cause from the run's own $.error. A "
            "static Cause makes every failed execution look identical to "
            "DescribeExecution and to every control-plane consumer."
        )
        assert "Cause" not in fail, "Cause and CausePath are mutually exclusive"

    def test_cause_carries_the_underlying_error(self, states):
        assert "States.JsonToString($.error)" in states["FailExecution"]["CausePath"]

    def test_error_label_stays_stable(self, states):
        # Consumers key on the Error label; the per-run detail belongs in
        # Cause. Making Error dynamic would break them for no gain.
        fail = states["FailExecution"]
        assert fail.get("Error") == "PipelineFailure"
        assert "ErrorPath" not in fail

    def test_retry_guidance_survives_in_the_cause(self, states):
        # The old static Cause carried the do-not-redrive warning and the
        # weekly_sf_rerun.py escape hatch. Both must survive the rewrite —
        # this string is what an operator reads at 2am.
        cause_path = states["FailExecution"]["CausePath"]
        assert "Do NOT redrive-execution" in cause_path
        assert "weekly_sf_rerun.py" in cause_path


class TestReadingErrorAtTheFailStateIsSafe:
    """``CausePath`` throws States.Runtime on a missing path, which would
    REPLACE the real failure with a runtime error. These are the structural
    preconditions that guarantee ``$.error`` is present."""

    def test_fail_execution_has_exactly_one_inbound_edge(self, states):
        assert _inbound_edges(states, "FailExecution") == ["HandleFailure"]

    def test_normalization_dominates_every_path_to_handle_failure(self, states):
        """NormalizeFailureContext (config#1819) is what guarantees $.error, so
        it must DOMINATE HandleFailure — every path in reaches it first.

        Walked as an explicit chain rather than "does it appear somewhere
        upstream": a new edge that jumps straight to HandleFailure, or to a
        label state, would still leave the name reachable while bypassing
        normalization — and CausePath would then throw on that path only.
        """
        assert sorted(_inbound_edges(states, "HandleFailure")) == [
            "NormalizeFailureContextPreflightLabel",
            "NormalizeFailureContextRealLabel",
        ]
        for label_state in (
            "NormalizeFailureContextPreflightLabel",
            "NormalizeFailureContextRealLabel",
        ):
            assert _inbound_edges(states, label_state) == [
                "NormalizeFailureContextRepin"
            ]
        assert _inbound_edges(states, "NormalizeFailureContextRepin") == [
            "NormalizeFailureContext"
        ]

    def test_every_task_catch_routes_into_normalization(self, states):
        # The 30 failure paths all converge on NormalizeFailureContext. If a
        # Catch is ever pointed elsewhere on the way to HandleFailure, the
        # dominator test above is what breaks — this one records the width of
        # the funnel so a shrink is visible too.
        assert len(_inbound_edges(states, "NormalizeFailureContext")) >= 25

    def test_handle_failure_already_renders_the_same_expression(self, states):
        # Proof the expression is production-proven, not newly invented.
        message = states["HandleFailure"]["Parameters"]["Message.$"]
        assert "States.JsonToString($.error)" in message
