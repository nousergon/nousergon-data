"""Pins the spot retry ladder's capacity diversification.

Origin — Brian ruling 2026-07-31 (alpha-engine-config-I5923): "we should be
attempting different instance types before using on demand. at least two
different types. otherwise we are practically defaulting to on demand during
prime time."

Two independent defects produced that behaviour:

  1. ``max_retries`` was 1, so the SINGLE relaunch was also the LAST one and
     ``CheckForceOnDemand`` forced it to on-demand. The lane went spot ->
     on-demand having tried exactly ONE spot capacity pool.

  2. ``krepis.ec2_spot.launch`` walks ``for instance_type in instance_types:
     for subnet_id in subnets`` in FIXED order and rotates only on a launch-time
     capacity ERROR. A mid-run reclamation is not a launch error, so every
     relaunch restarted at the head of the pool — the type that had just proved
     exhausted. Raising max_retries alone would have bought three attempts
     against ONE pool.

Fixing either alone leaves the ruling unmet, so both are pinned here.

This module pins the STATE MACHINE half — that the ladder has room for spot
attempts and that both relaunch paths carry the counter the rotation reads. The
rotation function itself is tested in the dispatcher's own co-located
``test_handler.py``, which already carries the boto3/ec2_spot stubs its import
needs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_groom.json"

@pytest.fixture(scope="module")
def item_selector() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]["MapLaunches"]["ItemSelector"]


@pytest.fixture(scope="module")
def lane_states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]["MapLaunches"]["ItemProcessor"]["States"]


# ── The ladder has room for spot attempts before the on-demand rung ───────────

def test_there_is_no_in_cycle_ladder(item_selector):
    """SUPERSEDED RULING, recorded rather than deleted.

    Was `test_ladder_leaves_at_least_two_spot_retries_before_on_demand`,
    asserting `max_retries >= 3` — Brian's ruling of 2026-07-31 (I5923), which
    existed so a reclaim would try two alternate spot pools before falling back
    to on-demand during prime time.

    **Superseded 2026-08-03**, by the same person: *"lets set max retry to 0...
    so if a spot dies, it dies."* The two could not both hold. Each relaunch is
    a NEW BOX with its own 3.5h wall (alpha-engine-config
    groom_spot_bootstrap.sh MAX_RUNTIME_SECONDS=12600), so two attempts is ~7h
    of wall-clock whatever the SF ceiling allows — a bounded cycle and a
    multi-rung in-cycle ladder are arithmetically incompatible, and the 7h
    cycles were the thing being fixed.

    **What was given up:** the two alternate-spot-pool rungs and the on-demand
    rung. A reclaimed lane now waits for the next cycle instead of relaunching
    inside this one. Safe because the reconciler is level-triggered
    (groom-sweep-policy.md §5.1) — the lane loses latency, never work.

    **What replaced it:** the outcome rollup
    (`alpha-engine-config scripts/groom_run_outcomes.py`), which reports the
    got-through / died / hit-the-wall rates. That measurement was Brian's
    stated condition for accepting max_retries=0, so a bad spot afternoon is
    now visible as reduced throughput rather than hidden inside a cycle that
    never ends.

    The pool-rotation machinery below is deliberately LEFT INTACT: it costs
    nothing while unused, and restoring a ladder should be a one-value change
    rather than a rebuild.
    """
    max_retries = item_selector["max_retries"]
    assert max_retries == 0, (
        f"max_retries={max_retries}, expected 0 (Brian's ruling 2026-08-03). "
        "Raising it re-opens the conflict with the 3.5h box wall: each retry is "
        "a new box with its own full budget, so N attempts is N x 3.5h of "
        "wall-clock. If a ladder is wanted again, the box wall and the SF "
        "ceiling have to move with it — see "
        "tests/test_sf_groom_relaunch_wiring.py::test_state_machine_declares_a_global_ceiling."
    )


def test_initial_attempt_is_seeded(item_selector):
    """The pool rotation is driven by $.fod.attempt; it must start defined."""
    assert item_selector["fod"]["attempt"] == 0
    assert item_selector["retry_count"] == 0


def test_relaunch_states_carry_the_attempt_counter(lane_states):
    """Both relaunch paths must emit `attempt`, or the rotation is inert.

    PrepRelaunch reaches LaunchGroomSpot directly (non-final retries) and
    SetForceOnDemand reaches it on the final one. A path that drops `attempt`
    silently reverts that attempt to the head of the pool.
    """
    for name in ("PrepRelaunch", "SetForceOnDemand"):
        fod = lane_states[name]["Parameters"]["fod"]
        assert "attempt" in fod or "attempt.$" in fod, (
            f"{name} drops fod.attempt — that attempt would restart at the head "
            "of the instance-type pool, i.e. the pool that just failed"
        )


def test_prep_relaunch_attempt_matches_its_own_incremented_retry_count(lane_states):
    """`attempt` and `retry_count` must advance together.

    PrepRelaunch cannot read its own output, so both fields independently
    compute States.MathAdd($.retry_count, 1). If they drift, the rotation
    offset stops tracking the ladder position.
    """
    params = lane_states["PrepRelaunch"]["Parameters"]
    assert params["retry_count.$"] == params["fod"]["attempt.$"], (
        "PrepRelaunch's retry_count and fod.attempt must be the same expression"
    )


def test_set_force_on_demand_preserves_the_attempt(lane_states):
    """The on-demand rung still reports its ladder position for observability."""
    assert lane_states["SetForceOnDemand"]["Parameters"]["fod"]["attempt.$"] == "$.retry_count"
