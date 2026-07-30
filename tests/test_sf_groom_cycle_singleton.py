"""Structural guards for the groom dispatch SF's cycle singleton
(alpha-engine-config-I5371, groom-sweep-policy §2.1).

**What went wrong.** The dispatch state machine declared `TimeoutSeconds: 72000`
(20 hours) while its EventBridge triggers fire every 8 hours (04:00/12:00/20:00
UTC), and it carried no singleton/mutex state. Those two facts together mean up
to three cycles can be alive simultaneously *by construction* — no failure of any
kind is required to produce the overlap.

Measured live 2026-07-29: executions `e46a697a` (started 7/28 21:00 PT) and
`476a69eb` (started 7/29 05:00 PT) were both RUNNING at the same moment, each
hung on its task-token callback, alongside `476a690a` which had just failed after
18 hours. Each cycle independently enumerated the backlog and dispatched an agent
at the same issues, producing 19 duplicate PRs across 16 clusters — most starkly
crucible-dashboard#588/#589/#590, three PRs against alpha-engine-config#4790
opened three minutes apart under three different branch-naming conventions.

Two independent properties are asserted here because either one alone is
insufficient:

  1. The execution ARN reaches the decide phase, so the Lambda *can* enforce the
     singleton at all. Without it the guard is inert and silently passes.
  2. The execution ceiling is shorter than the trigger interval, so overlap
     cannot be reintroduced by a slow-but-healthy cycle even if the singleton
     probe is ever weakened or bypassed.

§2.1's rule — "where two mechanisms bound the same thing, the tighter one is the
real budget" — is why the ceiling is asserted as a number here rather than left
to documentation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_DEF = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function_groom.json"

# The dispatch schedule (groom-sweep-policy §5): 04:00 / 12:00 / 20:00 UTC.
TRIGGER_INTERVAL_SECONDS = 8 * 3600


@pytest.fixture(scope="module")
def sf() -> dict:
    with _DEF.open() as fh:
        return json.load(fh)


def test_ceiling_is_strictly_less_than_the_trigger_interval(sf):
    """alpha-engine-config-I5372 (Brian's Option-1 ruling, 2026-07-29): the
    global ceiling must be strictly less than the 8h trigger interval so the
    schedule itself is the outermost bound and overlap is impossible without a
    mutex. The cycle singleton (alpha-engine-config-I5371) remains as defence
    in depth — but the ceiling is now the PRIMARY mechanism, and this assertion
    must never silently regress.

    Before this ruling the ceiling was 72000 (20h) against an 8h interval, so
    overlap was possible by construction — the disjunction in the old
    test_overlapping_cycles_are_prevented_by_at_least_one_mechanism was only
    correct while the ceiling could not bound overlap.
    """
    ceiling = sf.get("TimeoutSeconds")
    assert ceiling is not None, (
        "the dispatch SF must declare a top-level TimeoutSeconds")
    assert ceiling < TRIGGER_INTERVAL_SECONDS, (
        f"ceiling {ceiling}s must be strictly less than the {TRIGGER_INTERVAL_SECONDS}s "
        f"trigger interval — a ceiling >= interval reintroduces the I5372 defect: "
        f"overlap is possible by construction regardless of the singleton guard")


def test_overlapping_cycles_are_prevented_by_at_least_one_mechanism(sf):
    """The safety property, tightened per alpha-engine-config-I5372 (Brian's
    Option-1 ruling, 2026-07-29).

    The ceiling (25200s / 7h) is now strictly less than the trigger interval
    (28800s / 8h), so the schedule itself is the outermost bound and overlap is
    impossible without a mutex. The cycle singleton (I5371) is defence in depth
    — not the only guard, and the disjunction is no longer sufficient: a future
    ceiling regression must fail THIS test even if the singleton still holds.

    Lane budget is now 3h × 2 attempts = 6h worst-case, composes to strictly
    less than the 7h ceiling, which in turn is strictly less than the 8h
    interval. `test_ceiling_is_strictly_less_than_the_trigger_interval` above
    asserts the outer bound independently.
    """
    ceiling = sf.get("TimeoutSeconds")
    assert ceiling is not None, (
        "the dispatch SF must declare a top-level TimeoutSeconds — without one "
        "a hung cycle runs until the account's 1-year service ceiling")

    # The ceiling MUST bound overlap (the primary mechanism post-I5372).
    assert ceiling < TRIGGER_INTERVAL_SECONDS, (
        f"ceiling={ceiling}s >= trigger interval {TRIGGER_INTERVAL_SECONDS}s — "
        f"overlap is possible by construction regardless of the singleton guard")

    # Singleton is defence in depth — still required.
    singleton_wired = (
        sf["States"]["InitRunState"]["Parameters"]["decideMarker"].get("executionArn.$")
        == "$$.Execution.Id"
    )
    assert singleton_wired, (
        "the cycle-singleton guard must remain wired as defence in depth even "
        "when the ceiling bounds overlap — a ceiling-only defence has no second "
        "layer against a misconfigured redeploy")


def test_decide_phase_receives_the_execution_arn(sf):
    """The singleton is enforced in the Lambda's decide phase, which can only
    identify its own execution if the SF hands it the context object's
    Execution.Id. `States.JsonMerge` cannot reach `$$`, so the value must be
    seeded in InitRunState alongside the decide_only marker."""
    marker = sf["States"]["InitRunState"]["Parameters"]["decideMarker"]
    assert marker.get("executionArn.$") == "$$.Execution.Id", (
        "InitRunState must seed executionArn from the SF context object; "
        "without it _concurrent_cycle_blockers() short-circuits to 'no cycle "
        "context' and the singleton guard is silently inert")


def test_execution_arn_survives_the_merge_into_the_decide_payload(sf):
    """Seeding the ARN is only half the contract — DecideLaunches must actually
    merge `decideMarker` into the Lambda payload, or the field is seeded and
    then dropped."""
    payload = sf["States"]["DecideLaunches"]["Parameters"]["Payload.$"]
    assert "decideMarker" in payload, (
        f"DecideLaunches payload {payload!r} does not merge decideMarker — the "
        f"seeded executionArn would never reach the Lambda")


def test_every_task_state_declares_a_timeout(sf):
    """§2.1 no-unbounded-state: a Task with no TimeoutSeconds inherits the
    global ceiling, so its hang is indistinguishable from slow work until the
    whole execution times out."""
    missing = [name for name, body in sf["States"].items()
               if body.get("Type") == "Task" and "TimeoutSeconds" not in body]
    assert not missing, f"Task states without an explicit TimeoutSeconds: {missing}"
