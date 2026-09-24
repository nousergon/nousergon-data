"""config#5688 — a bounded re-issue must never target a dead instance.

The weekly pipeline's three `*RetryGate` Choice states re-issue
`ssm:sendCommand` to `$.ec2_instance_id`. Their original rationale assumed an
always-on launcher box. config#2248 replaced that box with a per-execution
ephemeral spot (`DispatchWeeklyFreshnessSpot`), which makes *the instance is
gone* the dominant reason a poll goes non-`Success` — precisely the case where
re-issuing to the same instance id can only raise
`Ssm.InvalidInstanceIdException`.

Measured on execution ``158e7fe8-a6ca-4278-b097-c02ca4e34752`` (2026-07-30):
after `WaitForRAGIngestion` returned ``Undeliverable``, the gate re-issued and
burned **5 consecutive TaskFailed over 4m20s** on an outcome that was
deterministic from the first attempt — the pipeline's one bounded retry spent
as a guaranteed no-op.

The fix is structural: branch on *why* the poll failed before deciding to
re-issue. These tests keep it that way, and — because the stale
``always-on instance`` comment is what let the premise survive config#2248 —
also assert that text cannot come back.
"""

from __future__ import annotations

import json
import pathlib

import pytest

_DEFINITION = (
    pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"
)
_RAW = _DEFINITION.read_text(encoding="utf-8")
_DEF = json.loads(_RAW)

# StatusDetails values SSM reports when the target instance is gone. A
# re-issue in either state is a guaranteed no-op.
_SUBSTRATE_LOST_DETAILS = {"Undeliverable", "Terminated"}


def _iter_scopes(node):
    """Yield every `States` map in the definition (top level + every branch)."""
    if isinstance(node, dict):
        states = node.get("States")
        if isinstance(states, dict):
            yield states
        for value in node.values():
            yield from _iter_scopes(value)
    elif isinstance(node, list):
        for value in node:
            yield from _iter_scopes(value)


_SCOPES = list(_iter_scopes(_DEF))


def _gate_scopes():
    """(gate_name, scope) for every Choice state named `*RetryGate`."""
    for scope in _SCOPES:
        for name, state in scope.items():
            if name.endswith("RetryGate") and state.get("Type") == "Choice":
                # *LivenessGate states are deliberately excluded here: they add
                # a substrate-loss branch WITHOUT a bounded re-issue (config#6938
                # — the box those stages address is the shared launcher, so a
                # re-issue could not help). alpha-engine-config-I9329 added a
                # THIRD member of that family with a stronger answer: the
                # eval-judge gates address a DEDICATED ephemeral box, so on
                # substrate loss they launch a NEW one rather than giving up —
                # still never a re-issue to a dead id, which is the only thing
                # the tests below assert. The gate tests here are about the
                # re-issue contract; the no-gate test further down covers every
                # kind.
                yield name, state, scope


def _leaf_conditions(rule):
    """Flatten a Choice rule into its leaf comparison dicts."""
    for key in ("And", "Or", "Not"):
        if key in rule:
            operands = rule[key]
            if isinstance(operands, dict):
                operands = [operands]
            for operand in operands:
                yield from _leaf_conditions(operand)
            return
    yield rule


def _reaches(scope: dict, start: str | None, target: str) -> bool:
    """Is ``target`` reachable from ``start`` within a single ASL scope?

    Scope-local by design: a Parallel branch cannot transition out to a
    top-level state, so a cross-scope "reachability" claim would be false.
    """
    seen, stack = set(), [start] if start else []
    while stack:
        cur = stack.pop()
        if cur == target:
            return True
        if cur in seen or cur not in scope:
            continue
        seen.add(cur)
        st = scope[cur]
        nxt = [st.get("Next"), st.get("Default")]
        nxt += [c.get("Next") for c in st.get("Choices", []) or []]
        nxt += [c.get("Next") for c in st.get("Catch", []) or []]
        stack += [n for n in nxt if isinstance(n, str)]
    return False


def _gate_ids():
    return [name for name, _, _ in _gate_scopes()]


def test_there_are_retry_gates_to_check():
    """Guard against the tests silently passing on an empty set."""
    assert _gate_ids(), (
        "no *RetryGate Choice states found in the weekly definition — if the "
        "bounded-re-issue pattern was retired, delete this test with the reason "
        "rather than leaving it vacuously green"
    )


@pytest.mark.parametrize("gate_name", _gate_ids())
def test_reissue_is_guarded_by_a_liveness_branch(gate_name):
    """No `*Reissue` route may be reachable without a substrate-loss branch first.

    The dead-instance branch must be evaluated BEFORE any rule that routes to
    the re-issue, since ASL evaluates Choices in order.
    """
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)

    rules = gate.get("Choices", [])
    routes = [(i, r.get("Next", "")) for i, r in enumerate(rules)]
    routes.append((len(rules), gate.get("Default", "")))
    reissue_positions = [i for i, target in routes if target.endswith("Reissue")]
    if not reissue_positions:
        pytest.skip(f"{gate_name} no longer re-issues; nothing to guard")

    liveness_positions = []
    for i, rule in enumerate(rules):
        values = {
            c.get("StringEquals")
            for c in _leaf_conditions(rule)
            if str(c.get("Variable", "")).endswith("_poll.StatusDetails")
        }
        if values & _SUBSTRATE_LOST_DETAILS:
            liveness_positions.append(i)

    assert liveness_positions, (
        f"{gate_name} routes to a re-issue without ever branching on "
        f"$.<stage>_poll.StatusDetails. After config#2248 the launcher is an "
        f"ephemeral spot, so re-issuing ssm:sendCommand to a terminated "
        f"instance id can only raise Ssm.InvalidInstanceIdException — the "
        f"pipeline's one bounded retry spent on a deterministic no-op "
        f"(config#5688)."
    )
    assert min(liveness_positions) < min(reissue_positions), (
        f"{gate_name} evaluates its re-issue rule before the substrate-loss "
        f"branch; ASL takes the first matching Choice, so the dead-instance "
        f"case would still re-issue"
    )


@pytest.mark.parametrize("gate_name", _gate_ids())
def test_substrate_lost_branch_is_distinguishable_and_reaches_the_notifier(gate_name):
    """The dead-instance path emits its own phase and joins the failure chain.

    Substrate loss and workload failure arriving as the same `phase` is what
    makes the two indistinguishable in failure telemetry. The target must also
    hand off to the same downstream state its non-substrate sibling does, so
    the alert body is produced by the identical chain.
    """
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)

    def _is_substrate_rule(rule):
        return bool(
            {
                c.get("StringEquals")
                for c in _leaf_conditions(rule)
                if str(c.get("Variable", "")).endswith("_poll.StatusDetails")
            }
            & _SUBSTRATE_LOST_DETAILS
        )

    targets = {r.get("Next") for r in gate.get("Choices", []) if _is_substrate_rule(r)}
    if not targets:
        pytest.skip(f"{gate_name} has no substrate-loss branch (covered by the guard test)")

    # The sibling is the gate's OTHER terminal route (retry-budget exhaustion),
    # found structurally rather than by name — the four gates do not share a
    # naming convention (Extract*Error vs SetDataPhase2ExhaustedError).
    siblings = [
        scope[r["Next"]]
        for r in gate.get("Choices", [])
        if not _is_substrate_rule(r) and r.get("Next") in scope
    ]
    assert siblings, (
        f"{gate_name} has no non-substrate error route to mirror; the substrate "
        f"branch cannot be checked for chain parity"
    )
    sibling = siblings[0]

    for target in targets:
        state = scope.get(target)
        assert state is not None, f"{gate_name} routes to {target}, absent from its scope"
        phase = state.get("Parameters", {}).get("phase", "")
        assert phase.endswith("/SubstrateLost"), (
            f"{target} emits phase {phase!r}; substrate loss must be separable "
            f"from workload failure in the SNS body (config#5688)"
        )
        assert state.get("ResultPath") == "$.error", (
            f"{target} must write $.error — HandleFailure's "
            f"States.JsonToString($.error) throws States.Runtime otherwise"
        )
        # Both routes must still converge on the same failure-notification
        # chain, but config-I7119 interposes a RECOVERY path on the substrate
        # side: one bounded forced-on-demand relaunch of the launcher box, and
        # only if that budget is already spent does the run fail. So the
        # handoff is now reachability, not identity — a substrate-lost site
        # that could never reach the notifier would still be the bug this
        # assertion was written to catch.
        chain_entry = sibling.get("Next")
        assert _reaches(scope, state.get("Next"), chain_entry), (
            f"{target} hands off to {state.get('Next')!r}, from which "
            f"{gate_name}'s non-substrate chain entry {chain_entry!r} is "
            f"UNREACHABLE; both must join the same failure-notification chain"
        )


@pytest.mark.parametrize("gate_name", _gate_ids())
def test_substrate_lost_branch_does_not_consume_the_retry_counter(gate_name):
    """A substrate event must not spend the retry a workload failure needs."""
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)

    for rule in gate.get("Choices", []):
        leaves = list(_leaf_conditions(rule))
        is_liveness = {
            c.get("StringEquals")
            for c in leaves
            if str(c.get("Variable", "")).endswith("_poll.StatusDetails")
        } & _SUBSTRATE_LOST_DETAILS
        if not is_liveness:
            continue
        target = scope.get(rule.get("Next"), {})
        assert not str(target.get("ResultPath", "")).endswith("_attempts"), (
            f"{gate_name}'s substrate-loss branch writes an attempts counter; "
            f"a later genuine transient workload failure then has no retry left"
        )


# ---------------------------------------------------------------------------
# Drill — execute the gate logic, don't just inspect its shape
#
# weekly-sf-policy.md §7.2 route 1: the rehearsal for this change is a
# CI-validatable path, not a live Saturday. Shape assertions alone would pass a
# branch wired to the wrong target, so these feed real poll payloads through the
# same ASL mini-evaluator test_sf_choice_guards uses (shared rather than
# re-implemented) and assert where each one actually lands.
# ---------------------------------------------------------------------------

from tests.test_sf_choice_guards import _eval_rule  # noqa: E402  (shared drill evaluator)


def _route(gate: dict, payload: dict) -> str:
    for rule in gate.get("Choices", []):
        if _eval_rule(rule, payload):
            return rule["Next"]
    return gate["Default"]


def _poll_key(gate_name: str, scope: dict) -> str:
    """The `$.<stage>_poll` key this gate's branch reads."""
    gate = scope[gate_name]
    for rule in gate.get("Choices", []):
        for cond in _leaf_conditions(rule):
            var = str(cond.get("Variable", ""))
            if var.endswith("_poll.StatusDetails"):
                return var.split(".")[1]
    raise AssertionError(f"{gate_name} has no _poll.StatusDetails branch")


@pytest.mark.parametrize("gate_name", _gate_ids())
@pytest.mark.parametrize("status_details", sorted(_SUBSTRATE_LOST_DETAILS))
def test_drill_dead_instance_never_reaches_a_reissue(gate_name, status_details):
    """A poll reporting a gone instance must not route to the re-issue."""
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)
    key = _poll_key(gate_name, scope)

    target = _route(gate, {key: {"Status": "Failed", "StatusDetails": status_details}})
    assert target.endswith("SubstrateLostError"), (
        f"{gate_name} routes a {status_details!r} poll to {target!r}; "
        f"ssm:sendCommand to that instance id can only raise "
        f"Ssm.InvalidInstanceIdException"
    )


@pytest.mark.parametrize("gate_name", _gate_ids())
def test_drill_live_instance_workload_failure_still_gets_its_one_reissue(gate_name):
    """The unchanged half: a real workload failure on a live box retries once."""
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)
    key = _poll_key(gate_name, scope)
    poll = {"Status": "Failed", "StatusDetails": "Failed", "ResponseCode": 1}

    first = _route(gate, {key: poll})
    assert first.endswith("Reissue"), (
        f"{gate_name} sends a first live-instance failure to {first!r} instead "
        f"of re-issuing — the bounded retry this gate exists for is gone"
    )

    attempts_key = scope[first]["ResultPath"].lstrip("$.")
    second = _route(gate, {key: poll, attempts_key: 1})
    assert not second.endswith("Reissue"), (
        f"{gate_name} re-issues a second time ({second!r}); the bound is one"
    )
    assert not second.endswith("SubstrateLostError"), (
        f"{gate_name} attributes an exhausted live-instance failure to "
        f"substrate loss ({second!r}) — that misreports a real workload failure "
        f"as an infrastructure event"
    )


# ---------------------------------------------------------------------------
# A stage with NO gate is the defect this file could not see
# ---------------------------------------------------------------------------

#: Poll stages whose non-Success arm is a NON-terminal, fail-open record —
#: the same class the ``*Degraded`` skip below covers by name, listed here
#: because the arm is deliberately NOT named Degraded (it sets no degraded
#: flag and does not change how the run ends). Each entry's default target
#: must proceed, which ``_fail_open_default_proceeds`` verifies rather than
#: trusting the entry. Add-by-PR-only.
_FAIL_OPEN_OBSERVE_STAGES = {
    # alpha-engine-config-I11312: the observe-mode on-spot preflight. Its
    # non-Success arm records observed:false and proceeds to CheckShellRun,
    # so a lost box costs this probe's verdict, never the run — the loss is
    # then met by MorningEnrich's own liveness branch, the first stage that
    # addresses the box after it (tests/test_sf_preflight_on_spot_wiring.py).
    "CheckWeeklyPreflightOnSpotStatus": "WeeklyPreflightOnSpotUnobserved",
}


def _fail_open_default_proceeds(scope: dict, target: str) -> bool:
    """The target is a Pass chain that never sets $.error and reaches
    CheckShellRun without passing a Fail state or the failure normalizer."""
    seen = set()
    while target not in seen and target in scope:
        seen.add(target)
        st = scope[target]
        if st.get("Type") == "Fail" or target in ("NormalizeFailureContext", "HandleFailure"):
            return False
        if st.get("ResultPath") == "$.error":
            return False
        if target == "CheckShellRun":
            return True
        target = st.get("Next")
        if target is None:
            return False
    return False


def _branch_terminal_ssm_stages():
    """(stage, check_state, scope) for every SSM poll whose non-Success arm is
    terminal for its branch.

    The gate discovery above enumerates gates that EXIST, so a stage carrying
    none is invisible to it — which is exactly how PredictorTraining went
    ungated through config#5688's sweep and then lost Branch B to a routine
    spot reclaim on 2026-08-11 (config#6938). This walks the poll Choices
    instead, so the absence is what gets asserted.
    """
    for scope in _SCOPES:
        for name, state in scope.items():
            if not (name.startswith("Check") and name.endswith("Status")):
                continue
            if state.get("Type") != "Choice":
                continue
            poll_vars = {
                str(c.get("Variable", ""))
                for rule in state.get("Choices", [])
                for c in _leaf_conditions(rule)
            }
            if not any(v.endswith("_poll.Status") for v in poll_vars):
                continue  # not an SSM poll gate

            # Only stages whose non-Success arm is TERMINAL. A stage that
            # routes to a *Degraded pass already survives substrate loss —
            # the run continues with the degrade recorded — so a liveness
            # branch would add a distinction without a consequence. The
            # asymmetry is the point: the gates matter exactly where losing
            # the box loses the run.
            default = scope[name].get("Default", "")
            target = scope.get(default, {})
            if default.endswith(("Degraded", "Failed")) and not target.get("Parameters", {}).get("phase"):
                continue
            # alpha-engine-config-I7267: PitParityLookahead/Walkforward's
            # Default is now an indirection — a RESOURCE_KILL marker check
            # (own Task/Wait/Choice trio, named `{stage}ResourceKillCheck` /
            # `WaitFor{stage}ResourceKillCheck` /
            # `Check{stage}ResourceKillCheckOutcome`) — before falling to the
            # SAME *Degraded terminal as before. EVERY non-Success path
            # still survives substrate loss identically to the pre-I7267
            # shape: the check Task's own Catch, the check Wait's own Catch,
            # AND the check Choice's Default all land on the stage's
            # existing *Degraded terminal — the indirection only ever ADDS a
            # RESOURCE_KILL classification on top of an already-degraded
            # outcome, never changes what happens to a reclaimed instance.
            # Verified structurally (not just this exemption's say-so) by
            # test_sf_parity_resource_kill_halt_i7267.py, which pins all
            # three of those routes to *Degraded by name.
            if default.endswith("ResourceKillCheck"):
                continue
            if _FAIL_OPEN_OBSERVE_STAGES.get(name) == default:
                assert _fail_open_default_proceeds(scope, default), (
                    f"{name} is listed as fail-open but its Default {default!r} "
                    "no longer proceeds to CheckShellRun"
                )
                continue
            yield name[len("Check"):-len("Status")], name, scope


def test_there_are_ssm_poll_stages_to_check():
    assert list(_branch_terminal_ssm_stages()), (
        "no Check*Status SSM poll Choice states found — if the polling shape "
        "changed, retarget this test rather than leaving it vacuously green"
    )


@pytest.mark.parametrize(
    "stage,check_state,scope",
    list(_branch_terminal_ssm_stages()),
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_every_ssm_poll_stage_has_a_liveness_branch(stage, check_state, scope):
    """The non-Success route must reach a substrate-loss branch somewhere.

    Either the Default is a *RetryGate carrying one, or the stage routes
    directly to a state whose phase names substrate loss. A stage that treats
    every non-Success identically cannot tell a reclaimed instance from a
    failed workload, and pays a full pipeline failure for the former.
    """
    default = scope[check_state].get("Default", "")
    target = scope.get(default, {})

    if default.endswith(("RetryGate", "LivenessGate")):
        values = {
            c.get("StringEquals")
            for rule in target.get("Choices", [])
            for c in _leaf_conditions(rule)
            if str(c.get("Variable", "")).endswith("_poll.StatusDetails")
        }
        assert values & _SUBSTRATE_LOST_DETAILS, (
            f"{check_state}.Default routes to {default}, which never branches "
            f"on StatusDetails — a reclaimed instance is handled as a workload "
            f"failure (config#5688)"
        )
        return

    phase = target.get("Parameters", {}).get("phase", "")
    assert phase.endswith("/SubstrateLost"), (
        f"{check_state}.Default routes straight to {default!r} with phase "
        f"{phase!r}: this stage has NO liveness branch at all. A spot reclaim "
        f"— the failure mode most expected on this substrate — is recorded as "
        f"a {stage} failure and takes the branch down on first occurrence. "
        f"That is config#6938, measured on watch-rerun-2026-08-10-8."
    )


def test_stale_always_on_instance_premise_is_gone():
    """The comment that let the invalidated premise survive config#2248."""
    assert "always-on instance" not in _RAW, (
        "a *RetryGate Comment still claims an always-on instance. There is no "
        "always-on box in this pipeline since config#2248; that text is what "
        "let the stale premise survive the change (doc-maintenance-policy: a "
        "load-bearing comment contradicted by the live system is a same-turn "
        "correction)."
    )
