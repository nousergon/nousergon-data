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

2026-10-08 heal-replay stop race (eod-reconcile-backstop-2026-10-08-1791512138):
the parent no longer stops the box after a successful dispatch — the stop
raced the replay, which inherited a STOPPING box and failed at
RefreshExecutorDeploy (DeliveryTimedOut). The replay now owns the box
(tests/test_heal_replay_owns_the_trading_box.py). The refresh is kept anyway:
the replay still cannot prove the box it lands on is on the parent's frozen
SHA (an operator stop, a ForceStopInstance or any reboot in between breaks
it), and a refresh on a box already on that SHA costs one SSM round-trip.
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
        "HealDispatchReplay must not skip RefreshExecutorDeploy: the child cannot "
        "prove the box it lands on is still on the parent's frozen SHA, and when it "
        "is not, EODReconcile dies in check_deploy_drift. See alpha-engine-config-I7586."
    )


@pytest.mark.parametrize("flag", ["skip_post_market_data"])
def test_the_other_skip_flags_are_retained(flag):
    """Guard against an over-broad fix. Only the deploy-refresh flag has a
    premise a box restart invalidates.

    `skip_post_market_data` is correct — the heal loop just ran it.
    """
    assert REPLAY_INPUT.get(flag) is True


def test_a_second_live_ib_capture_cannot_replace_the_days_record():
    """`skip_capture_snapshot` used to be retained here because a second
    live-IB capture is precisely what it exists to avoid. Since
    alpha-engine-config-I12220 this machine has a CaptureSnapshot again, but
    only as the missing-snapshot self-heal: its command exits before the
    capturer whenever trades/snapshots/{run_date}.json exists, and the
    capturer it runs carries no --force, so its create-only put can never
    replace a snapshot that raced in first. The replay (which targets THIS
    machine) therefore finds the snapshot present and opens no IB session;
    the flag would still be a dead input."""
    assert STATES["HealDispatchReplay"]["Parameters"]["StateMachineArn.$"] == "$$.StateMachine.Id"
    assert "skip_capture_snapshot" not in REPLAY_INPUT
    commands = STATES["CaptureSnapshot"]["Parameters"]["Parameters"]["commands.$"]
    assert commands.index("s3api head-object") < commands.index("snapshot_capturer.py")
    assert "--force" not in commands


def test_the_parent_hands_the_box_to_the_child_after_dispatching():
    """Revisited on the 2026-10-08 heal-replay stop race, as this pin asked.
    The parent used to stop the box right after dispatching; it now routes
    around the drain and the stop, so the child inherits a RUNNING box. That
    does not make the original flag right (see the module docstring): the
    refresh stays, and the property pinned here is the hand-off itself.
    """
    assert STATES["HealDispatchReplay"]["Next"] == "HealConvergedNotify"
    assert STATES["HealConvergedNotify"]["Next"] == "ReadExerciseCadence"


def test_the_dispatch_is_still_fire_and_forget():
    """Non-`.sync` startExecution. If it became `.sync` the parent would wait
    for the child and the hand-off reasoning above would change."""
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
