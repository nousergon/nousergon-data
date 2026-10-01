"""The self-heal's replay must sync the executor checkout (alpha-engine-config-I7586).

`HealDispatchReplay` carried `skip_refresh_executor_deploy: true`, justified in
its own comment as:

    RefreshExecutorDeploy already ran at the top of THIS execution and re-froze
    .frozen_executor_sha — the box is already known-fresh.

That premise holds for the PARENT and not for the CHILD. This state's own
successors are `HealConvergedNotify` -> `StopTradingInstance`: the parent stops
the trading box out from under the fire-and-forget replay it just started, the
replay's `StartTradingInstance` boots it again, and a rebooted box is not on
the SHA the parent froze.

Reproduced by hand on 2026-08-17 with this exact input shape — the replay
reached `EODReconcile` and died in `executor/preflight.py::check_deploy_drift`:

    RuntimeError: Deploy drift: executor checkout at /home/ec2-user/alpha-engine
    is on dfbab49b7b06 but this run pinned EXPECTED_EXECUTOR_SHA=1ea15df6ce83 at
    its freshness gate.

Terminal `EODPipelineFailure`. Re-running the identical input WITHOUT the flag
succeeded first try.

Consequence, and the reason this is a P1 rather than a note: the self-heal
loop's entire convergence path ends in `HealDispatchReplay`. If the loop ever
converges, the replay it dispatches fails this way — so **the convergence path
has never worked**. On 2026-08-17 the loop reached `HealNonConvergent` instead,
so the flag was never exercised in production. That is luck, not coverage.

The flag reads as a harmless optimisation and will be re-added by anyone
reasoning from the parent's state alone. Hence a test rather than a comment.

alpha-engine-config-I11269 (post-close split): the heal loop and its replay now
live in ne-postclose-reconcile-pipeline (step_function_eod_reconcile.json).
The reasoning above is unchanged — the parent still stops the box right after
dispatching — so every pin is kept and re-pointed at that definition.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

SF = json.loads(
    (
        Path(__file__).resolve().parents[1] / "infrastructure" / "step_function_eod_reconcile.json"
    ).read_text(encoding="utf-8")
)
STATES = SF["States"]
REPLAY_INPUT = STATES["HealDispatchReplay"]["Parameters"]["Input"]


def test_the_replay_does_not_skip_the_deploy_refresh():
    assert "skip_refresh_executor_deploy" not in REPLAY_INPUT, (
        "HealDispatchReplay must not skip RefreshExecutorDeploy: the parent stops "
        "the box (StopTradingInstance) right after dispatching this replay, so the "
        "child boots a box that is no longer on the frozen SHA and dies in "
        "check_deploy_drift. See alpha-engine-config-I7586."
    )


@pytest.mark.parametrize("flag", ["skip_post_market_data"])
def test_the_other_skip_flags_are_retained(flag):
    """Guard against an over-broad fix. Only the deploy-refresh flag has a
    premise a box restart invalidates.

    `skip_post_market_data` is correct — the heal loop just ran it.
    """
    assert REPLAY_INPUT.get(flag) is True


def test_a_second_live_ib_capture_is_structurally_impossible():
    """`skip_capture_snapshot` used to be retained here because a second
    live-IB capture is precisely what it exists to avoid. Since the I11269
    split the replay targets THIS machine (``$$.StateMachine.Id``), which has
    no CaptureSnapshot at all — the 16:00 ne-postclose-trading-pipeline owns
    it — so the protection is structural and the flag would be a dead input.
    Pinned both ways so neither half can drift back alone."""
    assert STATES["HealDispatchReplay"]["Parameters"]["StateMachineArn.$"] == "$$.StateMachine.Id"
    assert "CaptureSnapshot" not in STATES
    assert "skip_capture_snapshot" not in REPLAY_INPUT


def test_the_parent_still_stops_the_box_after_dispatching():
    """The pin on WHY the flag is wrong. If this ordering ever changes, the
    premise behind the original flag becomes true again and this whole test
    module should be revisited rather than silently kept.
    """
    assert STATES["HealDispatchReplay"]["Next"] == "HealConvergedNotify"
    assert STATES["HealConvergedNotify"]["Next"] == "StopTradingInstance"


def test_the_dispatch_is_still_fire_and_forget():
    """Non-`.sync` startExecution. If it became `.sync` the parent would wait,
    the box would not be stopped underneath the child, and the reasoning above
    would change."""
    assert STATES["HealDispatchReplay"]["Resource"] == "arn:aws:states:::states:startExecution"


class TestConvergenceNotificationDoesNotOverclaim:
    """`HealConvergedNotify` fires on a DISPATCH, not on an outcome."""

    MSG = STATES["HealConvergedNotify"]["Parameters"]["Message.$"]
    SUBJECT = STATES["HealConvergedNotify"]["Parameters"]["Subject"]

    def test_it_does_not_assert_no_operator_action_required(self):
        """Under the I7586 defect that claim was false in every case this state
        could be reached — and it is a claim this state structurally cannot make,
        because the dispatch is fire-and-forget."""
        combined = (self.MSG + self.SUBJECT).lower()
        assert "no operator action required" not in combined

    def test_it_names_the_replay_execution(self):
        """The convergence signal belongs to the replay's own terminal, so the
        message has to point at it."""
        assert "heal_replay_dispatch" in self.MSG

    def test_it_says_what_it_is_reporting(self):
        assert "dispatch" in (self.MSG + self.SUBJECT).lower()


def test_replay_input_carries_the_identifiers_the_child_needs():
    """Guard-the-guard: the assertions above are about what is ABSENT, so also
    assert the input is still a usable execution input."""
    for key in ("trading_instance_id.$", "ec2_instance_id.$", "run_date.$"):
        assert key in REPLAY_INPUT
    assert REPLAY_INPUT["pipeline_role"] == "operator-replay"
    assert REPLAY_INPUT["triggered_by"] == "eod-self-heal"
