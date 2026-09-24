"""In-execution substrate-loss recovery for the weekly SF (config-I7119).

A reclaimed launcher spot used to be TERMINAL: every ``Extract*SubstrateLost
Error`` state routed straight to ``NormalizeFailureContext`` -> ``FailExecution``,
and recovery meant a human running ``scripts/weekly_sf_rerun.py``. Measured
2026-08-12: 3 of 11 recent spot requests in the account died
``instance-terminated-no-capacity``, one of them mid-``DataPhase1`` on that
day's scheduled run (resource ids in alpha-engine-config-I7119).

These tests pin the replacement path: ONE bounded, forced-on-demand relaunch
of the launcher box, then resume at the interrupted stage.

Every list below is DERIVED from the definition, never hand-enumerated — the
failure mode this file guards against is a new stage gaining a substrate-lost
branch and silently not gaining a resume target.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"

GATE = "SubstrateRelaunchGate"
RESUME = "ResumeAfterSubstrateRelaunch"


@pytest.fixture(scope="module")
def doc():
    return json.loads((_INFRA / "step_function.json").read_text())


@pytest.fixture(scope="module")
def states(doc):
    return doc["States"]


def _scopes(container, label="<root>"):
    """(label, states) for the top level and every Parallel/Map branch.

    ASL state names are scoped: a Parallel branch cannot transition to a
    top-level state, which is exactly why the branch sites are excluded below.
    """
    yield label, container["States"]
    for name, st in container["States"].items():
        for i, br in enumerate(st.get("Branches") or []):
            yield from _scopes(br, f"{label}/{name}[{i}]")
        inner = st.get("Iterator") or st.get("ItemProcessor")
        if inner:
            yield from _scopes(inner, f"{label}/{name}<iter>")


def _substrate_lost(states):
    return {k: v for k, v in states.items() if k.endswith("SubstrateLostError")}


def test_every_top_level_substrate_lost_site_routes_to_the_gate(states):
    """The CLASS, not the DataPhase1 instance that surfaced it."""
    sites = _substrate_lost(states)
    assert len(sites) >= 8, f"expected the full sequential set, got {sorted(sites)}"
    wrong = {k: v.get("Next") for k, v in sites.items() if v.get("Next") != GATE}
    assert not wrong, f"substrate-lost sites bypassing {GATE}: {wrong}"


def test_every_gated_phase_has_a_resume_target(states):
    """A phase label with no resume branch would fall to Default and fail loud
    — correct, but silent about the wiring gap. Pin it at build time instead."""
    phases = {
        v["Parameters"]["phase"] for v in _substrate_lost(states).values()
    }
    resume = states[RESUME]
    covered = set()
    for rule in resume["Choices"]:
        leaves = rule.get("And", [rule])
        # config#2275: the path is IsPresent-guarded before it is dereferenced.
        assert any(leaf.get("IsPresent") is True for leaf in leaves), (
            f"resume rule for {rule} dereferences $.error.phase unguarded"
        )
        covered |= {
            leaf["StringEquals"] for leaf in leaves if "StringEquals" in leaf
        }
    assert phases <= covered, f"phases with no resume target: {sorted(phases - covered)}"
    # alpha-engine-config#5950: the Default is taken precisely when $.error.phase
    # is ABSENT, and the terminal it leads to dereferences $.error — so the one
    # path that exists to report an unrecognised phase could not run. A
    # normalizer Pass now floors $.error on that edge. Walk through it rather
    # than accepting either shape: the terminal must still be the loud one.
    terminal = resume["Default"]
    while states[terminal]["Type"] == "Pass" and "phase" not in (
        states[terminal].get("Parameters") or {}
    ):
        terminal = states[terminal]["Next"]
    assert states[terminal]["Parameters"]["phase"] == (
        "SubstrateRelaunch/UnknownResumePhase"
    ), "an unrecognised phase must fail loud, and say so"


def test_resume_targets_exist_and_reset_their_poll_counter(states):
    """Resuming at the stage TASK (not its wait loop) is what resets the
    stage's poll counter — resuming mid-loop would re-enter with the counter
    already exhausted and give up immediately."""
    for choice in states[RESUME]["Choices"]:
        target = choice["Next"]
        assert target in states, f"resume target {target} does not exist"
        st = states[target]
        if st["Type"] != "Task":
            continue  # bootstrap-phase resume lands on a Choice by design
        if target == "SubstrateHealthGate":
            # alpha-engine-config-I11268: the bootstrap-phase resume re-enters
            # through the substrate gate (a one-shot Lambda probe, not a polled
            # stage) so the replacement box is gated before CheckShellRun.
            assert st["Next"] == "CheckSubstrateHealthGate"
            continue
        assert st["Next"].startswith("Init") and st["Next"].endswith("PollCount"), (
            f"{target} no longer leads with its Init*PollCount state; resuming "
            f"there would inherit an exhausted poll counter"
        )


def test_the_relaunch_is_bounded_and_spent_before_launching(states):
    gate = states[GATE]
    (choice,) = gate["Choices"]
    assert choice["Variable"] == "$.substrate_relaunch_attempts"
    assert choice["NumericLessThan"] == 1, "exactly one relaunch"
    exhausted = states[gate["Default"]]
    assert exhausted["ResultPath"] == "$.error", (
        "the exhausted path must write $.error — HandleFailure's "
        "States.JsonToString($.error) throws States.Runtime otherwise"
    )
    assert exhausted["Parameters"]["phase"] == "SubstrateRelaunch/Exhausted", (
        "losing two launcher boxes is a capacity event; reporting it as the "
        "original stage's failure sends operators to the wrong system"
    )
    assert exhausted["Next"] == "NormalizeFailureContext", "exhausted must fail loud"

    # Spending the attempt BEFORE the launch is what makes a second loss
    # terminal rather than an infinite relaunch loop.
    record = states[choice["Next"]]
    assert record["Type"] == "Pass"
    assert record["Result"] == 1
    assert record["ResultPath"] == "$.substrate_relaunch_attempts"
    assert record["Next"] == "RelaunchWeeklyFreshnessSpot"


def test_the_counter_always_exists(states):
    """The gate and the completion marker both read the counter by path; an
    absent field makes the Choice fall to Default and States.Format fail."""
    blob = states["InitializeInput"]["Parameters"]["merged.$"]
    assert '"substrate_relaunch_attempts":0' in blob


def test_the_launcher_box_is_on_demand_from_the_first_launch(states):
    """config-I7120: the FIRST launch is on-demand too, not only the relaunch.

    The launcher box is the single shared substrate every stage addresses via
    ``$.ec2_instance_id``. ``SubstrateRelaunchGate`` (config-I7119) recovers the
    8 top-level substrate-lost sites; the 5 inside ``ResearchPredictorParallel``
    are structurally unreachable from it (a Parallel branch cannot transition to
    a top-level state) and sit in the LARGEST exposure window of the run. The
    only measure covering all 13 is removing the reclaim rather than recovering
    from it.

    This is a deliberate, written deviation from cost-management-policy's
    interruptible-by-default: the box is a pure orchestrator (the expensive
    compute runs on the NESTED spots it launches, which stay spot), so the delta
    is roughly $0.2-0.7 per run against a lost weekly belief-refresh cycle.
    Flipping it back to spot is a one-key diff and must be a deliberate ruling,
    which is why it is pinned here rather than left to the definition.
    """
    payload = states["DispatchWeeklyFreshnessSpot"]["Parameters"]["Payload"]
    assert payload.get("force_on_demand") is True, (
        "the weekly launcher box must launch ON-DEMAND from the start — a "
        "reclaim of this one box kills the whole execution, and the 5 "
        "in-Parallel substrate-lost sites have no recovery path (config-I7120)"
    )


def test_relaunch_forces_on_demand(states):
    """Spot-first would re-enter the pool that just reclaimed the box."""
    payload = states["RelaunchWeeklyFreshnessSpot"]["Parameters"]["Payload"]
    assert payload["force_on_demand"] is True
    orig = states["DispatchWeeklyFreshnessSpot"]["Parameters"]
    assert (
        states["RelaunchWeeklyFreshnessSpot"]["Parameters"]["FunctionName"]
        == orig["FunctionName"]
    ), "the relaunch must go through the SAME dispatcher, not a parallel one"


def test_relaunch_replaces_the_instance_id_and_reuses_the_bootstrap_loop(states):
    merge = states["MergeRelaunchedSpotInstanceId"]
    assert '"ec2_instance_id"' in merge["Parameters"]["merged.$"]
    assert merge["Parameters"]["merged.$"].rstrip().endswith("false)"), (
        "shallow JsonMerge with the new object second is what REPLACES "
        "ec2_instance_id instead of appending a second box"
    )
    # Reusing the shared loop is what gives a second loss during re-bootstrap
    # the same liveness handling as the first.
    assert merge["Next"] == "InitWeeklyFreshnessSpotBootstrapPollCount"


def test_bootstrap_success_after_a_relaunch_resumes_instead_of_restarting(states):
    """Without this branch the replacement box falls through to CheckShellRun
    and re-runs every stage the lost box already completed.

    The branch lives on a DEDICATED router rather than as a second Success rule
    on the status check: ASL evaluates Choices in order, so a relaunch-aware
    Success rule would have to sit ahead of the plain one, and the happy-path
    walkers in tests/ identify the forward edge as the first rule carrying a
    Success leaf — they would follow the recovery edge on a first boot.
    """
    success = [
        c
        for c in states["CheckWeeklyFreshnessSpotBootstrapStatus"]["Choices"]
        if c.get("Variable") == "$.weekly_freshness_spot_poll.Status"
        and c.get("StringEquals") == "Success"
    ]
    assert len(success) == 1, "exactly one Success edge, or the walkers ambiguate"
    router = states[success[0]["Next"]]
    assert router["Type"] == "Choice"
    # alpha-engine-config-I11268: via the substrate gate, whose HEALTHY edge
    # lands on CheckShellRun.
    assert router["Default"] == "SubstrateHealthGate", "first boot still runs stage 1"
    (relaunched,) = router["Choices"]
    assert relaunched["Variable"] == "$.substrate_relaunch_attempts"
    assert relaunched["NumericGreaterThan"] == 0
    assert relaunched["Next"] == RESUME


def test_the_recovery_is_measurable_on_the_completion_artifact(states):
    """principles.md #7: an auto-recovery no number reports is unobserved.
    The count rides the durable completion marker the console already reads.

    alpha-engine-config-I11106: the second assertion used to be
    ``body.rstrip().endswith("$.run_date, $.substrate_relaunch_attempts)")``,
    which pinned the count to being the LAST States.Format argument. That is
    positional coupling, not the invariant this test is named for — it breaks
    the next time any field joins the marker, which is exactly what happened
    when ``model_zoo_unservable`` was threaded onto it. The invariant is that
    the count is ON the marker: the placeholder is in the body template AND
    the path is among the Format arguments. Both are asserted below, so the
    guard is not weakened — a marker that drops the count still fails here.
    """
    body = states["WriteCompletionMarker"]["Parameters"]["Body.$"]
    assert '"substrate_relaunches":{}' in body
    args = body.rstrip().rstrip(")").split(",")
    assert any(a.strip() == "$.substrate_relaunch_attempts" for a in args), (
        "substrate_relaunch_attempts is not among the completion marker's "
        f"States.Format arguments: {body}"
    )


def test_parallel_branch_sites_are_knowingly_out_of_scope(doc):
    """ASL scoping forbids a Parallel branch transitioning to a top-level
    state, and both branches share ONE launcher box, so a branch-local
    relaunch would swap the instance id under its sibling mid-flight.

    This asserts the boundary rather than the absence: if a branch site is
    ever wired to the top-level gate the definition will not deploy, and if
    the count changes someone must decide deliberately. Tracked as
    alpha-engine-config-I7120.
    """
    branch_sites = {
        f"{label}:{name}"
        for label, states in _scopes(doc)
        if label != "<root>"
        for name in _substrate_lost(states)
    }
    assert len(branch_sites) == 5, (
        f"the in-Parallel substrate-lost set changed: {sorted(branch_sites)} — "
        f"re-examine I7120 before adjusting this number"
    )
    for label, states in _scopes(doc):
        if label == "<root>":
            continue
        for name, st in _substrate_lost(states).items():
            assert st["Next"] in states, (
                f"{label}:{name} targets {st['Next']}, which is not in its own "
                f"scope — ASL will reject this definition"
            )
