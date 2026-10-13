"""alpha-engine-config-I7267 — pit_parity's per-pass timeout must HALT the
weekly SF (Brian's 2026-08-13 ruling, sf-pipeline-policy §3
SFP-3-resource-kill-halts-and-is-named, option (a)), distinct from the
existing "could not conclude" fail-open DEGRADED path.

Pre-fix: obligation 3 (name it) was met — the per-pass artifact
(parity/{run_date}/pit_stats_{pass}.json) already carries
``"timed_out": true`` when crucible-backtester's internal
``_PIT_PARITY_PASS_TIMEOUT`` (analysis/pit_parity.py) kills a pass mid-work
— but obligation 1 (halt) was not: the SF's routing never read that flag,
so a timed-out pass still folded into the generic DEGRADED fail-open and
the pipeline continued through PitParityCompare -> Evaluator -> ReportCard
-> Director as if the check had merely "couldn't conclude".

This closes obligation 1 for the three parity states named in the issue
(PitParityLookahead, PitParityWalkforward, ParityReplay) plus PitParityCompare:

- PitParityLookahead / PitParityWalkforward: neither carries a
  resource_kill classification at the ssm_log_capture/StandardErrorContent
  layer (the per-pass timeout is caught INSIDE Python via
  ``subprocess.TimeoutExpired``, converted to a plain non-zero exit — never
  a kernel/shell "Killed" line, never an OOM/TIMEOUT returncode). So each
  pass's own commands now write a well-known S3 marker key
  (``.pit_stats_{pass}.resource_kill``) when the just-published artifact
  reads back ``timed_out: true``, and a follow-up `aws s3api head-object`
  check (same instance, no new IAM) classifies it.
- ParityReplay / PitParityCompare: krepis.ssm_log_capture's own
  ``RESOURCE KILL (OOM):``/``RESOURCE KILL (TIMEOUT):`` classification
  (alpha-engine-config-I7258) already lands in the SF's captured
  StandardErrorContent — no new mechanism needed, just a routing check.

A RESOURCE_KILL branch status (or PitParityCompare's own StandardErrorContent
match) routes into the SAME shared hard-fail chokepoint every other genuine
SF failure uses (config#1819's NormalizeFailureContext -> HandleFailure ->
FailExecution), so PitParityCompare (when the killed branch is
lookahead/walkforward) and every stage after it do not run this week — the
cause is a distinct, named terminal state, verifiable from
``get-execution-history`` alone. The pre-existing "could not conclude"
DEGRADED path (spot dispatch failure, poll-budget exhaustion, an
unclassified script error) is UNCHANGED and still fail-opens through the
compare join (sf-pipeline-policy §2.3a).

No live weekly SF execution was triggered to produce this coverage (this is
a definition-only PR; the fixture-based substitute below walks the ACTUAL
``Next``/``Default``/``Choices`` edges in ``step_function.json`` for both
scenarios — the same state-name transitions a real ``get-execution-history``
call would show).
"""
from __future__ import annotations

import json
import pathlib
import re

import pytest

from tests.sf_run_slot_release_helpers import through_release

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_WEEKLY.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


@pytest.fixture(scope="module")
def branches(states) -> dict:
    """base-state-name -> that branch's States dict, keyed by CheckSkip*'s
    Default (the branch's own launcher Task name)."""
    out = {}
    for branch in states["ParityParallel"]["Branches"]:
        gate = branch["States"][branch["StartAt"]]
        out[gate["Default"]] = branch["States"]
    return out


# ---------------------------------------------------------------------------
# PitParityLookahead / PitParityWalkforward: the marker-check chain
# ---------------------------------------------------------------------------

_PASS_BASES = ("PitParityLookahead", "PitParityWalkforward")


@pytest.mark.parametrize("base", _PASS_BASES)
def test_pass_exit_nonzero_routes_through_resource_kill_check_first(branches, base):
    """A pass's own terminal non-Success no longer lands DIRECTLY on
    *Degraded — it routes through the marker check first."""
    check = branches[base][f"Check{base}Status"]
    assert check["Default"] == f"{base}ResourceKillCheck"


@pytest.mark.parametrize("base", _PASS_BASES)
def test_pass_commands_write_the_marker_on_timed_out_true_only(states, base):
    """The pass's own commands.$ must (a) preserve the original exit code
    ($_pit_pass_rc, restored via the trailing `exit $_pit_pass_rc`) and (b)
    gate the marker PUT on BOTH a non-zero exit AND the artifact's own
    timed_out-true pattern — never write the marker for a clean run
    or for a non-timeout failure whose artifact says timed_out:false (or
    has no artifact at all, e.g. a true infra failure)."""
    for br in states["ParityParallel"]["Branches"]:
        if base in br["States"]:
            cmds = br["States"][base]["Parameters"]["Parameters"]["commands.$"]
            break
    else:
        pytest.fail(f"{base} not found in any ParityParallel branch")

    assert "'set +e'" in cmds, f"{base}: must disable set -e before capturing $?"
    assert "'_pit_pass_rc=$?'" in cmds, f"{base}: must capture the pass's own exit code"
    assert "$_pit_pass_rc -ne 0" in cmds, f"{base}: marker write must be gated on a non-zero exit"
    assert "timed_out[^a-z]+true" in cmds, f"{base}: marker write must be gated on the artifact's timed_out:true"
    # The escape-grammar rule this state broke on 2026-08-19 lives in
    # test_sf_asl_intrinsic_literals.py (a backslash-escaped double quote is
    # rejected by AWS's intrinsic parser); the marker gate is therefore
    # written with a bracket expression instead of an escaped quote.
    assert cmds.rstrip(")").endswith("'exit $_pit_pass_rc'"), (
        f"{base}: the ORIGINAL pass exit code must be the command's final "
        f"exit — the existing Check{base}Status Success/Failed routing must "
        f"be byte-identical to before this change"
    )


@pytest.mark.parametrize("base", _PASS_BASES)
def test_resource_kill_check_task_uses_the_instance_role_no_new_sf_iam(branches, base):
    """The marker check dispatches via the SAME ssm:sendCommand document to
    the SAME instance as every sibling spot stage — it runs under the
    EC2 instance's own IAM role (which already read/wrote this S3 prefix
    for the pass itself), not the Step-Functions execution role, so this
    needs no new SF-role IAM grant."""
    check_name = f"{base}ResourceKillCheck"
    task = branches[base][check_name]
    assert task["Resource"] == "arn:aws:states:::aws-sdk:ssm:sendCommand"
    assert task["Parameters"]["DocumentName"] == "AWS-RunShellScript"
    assert task["Parameters"]["InstanceIds.$"] == "$.ec2_instance_id"
    assert "head-object" in task["Parameters"]["Parameters"]["commands.$"]


@pytest.mark.parametrize("base", _PASS_BASES)
def test_resource_kill_check_send_and_wait_catch_fall_back_to_degraded(branches, base):
    """The check's own send/poll Catch (instance gone, SSM unreachable)
    must fall back to the EXISTING *Degraded terminal — this check can only
    ADD a RESOURCE_KILL classification, never introduce a new way to break
    the pipeline or block on its own failure."""
    b = branches[base]
    for name in (f"{base}ResourceKillCheck", f"WaitFor{base}ResourceKillCheck"):
        catches = b[name].get("Catch", [])
        assert catches, f"{name} must keep a fail-soft Catch"
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == f"{base}Degraded"


@pytest.mark.parametrize("base", _PASS_BASES)
def test_resource_kill_check_choice_success_means_marker_found(branches, base):
    """Success (the head-object exited 0, i.e. the marker key exists) is
    the ONLY path to the RESOURCE_KILL terminal; anything else (404/Failed,
    InProgress on a sub-5s command) falls to the existing *Degraded — this
    is a pure ADD, never a narrowing of existing coverage."""
    choice = branches[base][f"Check{base}ResourceKillCheckOutcome"]
    assert choice["Type"] == "Choice"
    success = [
        c["Next"] for c in choice["Choices"]
        if c.get("StringEquals") == "Success"
    ]
    assert success == [f"{base}ResourceKill"]
    assert choice["Default"] == f"{base}Degraded"


@pytest.mark.parametrize("base", _PASS_BASES)
def test_resource_kill_terminal_ends_branch_success_with_resource_kill_status(branches, base):
    """Mirrors the *Degraded shape EXACTLY (Pass, End:true, same ResultPath)
    except for the status literal — sibling-isolation (§4 blast radius) is
    unchanged: a resource-killed branch still never aborts its siblings."""
    degraded = branches[base][f"{base}Degraded"]
    resource_kill = branches[base][f"{base}ResourceKill"]
    assert resource_kill["Type"] == "Pass"
    assert resource_kill.get("End") is True
    # alpha-engine-config-I8194: both terminals now nest their envelope
    # under the same branch key and carry no ResultPath; the mirror
    # assertion is on that key rather than on ResultPath.
    assert resource_kill["Parameters"] == {
        next(iter(degraded["Parameters"])): {"status": "RESOURCE_KILL"}
    }
    assert "ResultPath" not in resource_kill
    assert set(resource_kill["Parameters"]) == set(degraded["Parameters"])


# ---------------------------------------------------------------------------
# SCENARIO 1 (the issue's live-measured case): timed_out:true routes to halt
# ---------------------------------------------------------------------------


def _walk(states: dict, start: str, max_depth: int = 25) -> list[str]:
    """Deterministic forward walk (Pass/Task Next only — stop at a Choice,
    Parallel, or a state with no Next) from `start`, for asserting what a
    real get-execution-history sequence would show."""
    order = [start]
    cur = start
    depth = 0
    while depth < max_depth:
        st = states.get(cur)
        if st is None or st.get("Type") not in ("Pass", "Task") or st.get("End"):
            break
        nxt = st.get("Next")
        if not nxt:
            break
        order.append(nxt)
        cur = nxt
        depth += 1
    return order


@pytest.mark.parametrize("base", _PASS_BASES)
def test_timed_out_true_execution_history_reaches_the_hard_fail_terminal(states, branches, base):
    """SCENARIO: a pass's per-pass timeout fires (crucible-backtester's
    _PIT_PARITY_PASS_TIMEOUT), the artifact is published with
    timed_out:true, the marker is written, and the check's head-object
    confirms it (Status: Success).

    Simulated get-execution-history sequence:
      Check{base}Status (Default) -> {base}ResourceKillCheck ->
      WaitFor{base}ResourceKillCheck -> Check{base}ResourceKillCheckOutcome
      (Status==Success) -> {base}ResourceKill -> [branch ends] ->
      AggregateParityBranchOutcomes -> CheckParityBranchOutcomes
      ($.parity_branch_outcomes.<x>_status == RESOURCE_KILL) ->
      PitParityResourceKillDetected -> NormalizeFailureContext -> ... ->
      FailExecution.

    PitParityCompare must NEVER appear in this history — option (a) of the
    ruling: downstream stages this run simply do not run.
    """
    b = branches[base]
    check = b[f"Check{base}Status"]
    assert check["Default"] == f"{base}ResourceKillCheck"

    outcome = b[f"Check{base}ResourceKillCheckOutcome"]
    success_next = next(
        c["Next"] for c in outcome["Choices"] if c.get("StringEquals") == "Success"
    )
    assert success_next == f"{base}ResourceKill"

    terminal = b[f"{base}ResourceKill"]
    # alpha-engine-config-I8194: envelope nested under the branch key.
    assert list(terminal["Parameters"].values()) == [
        {"status": "RESOURCE_KILL"}
    ]
    assert terminal.get("End") is True  # branch ends SUCCESS — siblings unaffected

    # Post-join: simulate the aggregate/fold reading this branch's status.
    cbo = states["CheckParityBranchOutcomes"]
    resource_kill_choice = cbo["Choices"][0]
    assert resource_kill_choice["Next"] == "PitParityResourceKillDetected"
    var_suffix = {
        "PitParityLookahead": "pit_lookahead_status",
        "PitParityWalkforward": "pit_walkforward_status",
    }[base]
    assert any(
        cond["Variable"] == f"$.parity_branch_outcomes.{var_suffix}"
        and cond["StringEquals"] == "RESOURCE_KILL"
        for cond in resource_kill_choice["Or"]
    )

    history = _walk(states, "PitParityResourceKillDetected")
    assert "NormalizeFailureContext" in history
    assert "PitParityCompare" not in history
    assert "CheckSkipPitParityCompare" not in history
    # NormalizeFailureContext -> NormalizeFailureContextRepin (a Choice, one
    # hop past this deterministic Pass/Task-only walker) -> HandleFailure ->
    # FailExecution is the SAME shared chokepoint every other genuine SF
    # failure already uses — asserted directly by name (config#1819).
    assert states["NormalizeFailureContext"]["Next"] == "NormalizeFailureContextRepin"
    assert through_release(states, states["HandleFailure"]["Next"]) == "FailExecution"
    assert states["FailExecution"]["Type"] == "Fail"


# ---------------------------------------------------------------------------
# SCENARIO 2: a normal "could not conclude" (non-timeout) still degrades
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("base", _PASS_BASES)
def test_non_timeout_failure_execution_history_still_reaches_compare(states, branches, base):
    """SCENARIO: the pass fails for a reason OTHER than its internal
    timeout (spot dispatch failure, poll-budget exhaustion, a plain script
    crash) — no marker was ever written, so the head-object 404s (Status:
    Failed, the Choice's Default arm).

    Simulated get-execution-history sequence:
      Check{base}Status (Default) -> {base}ResourceKillCheck ->
      WaitFor{base}ResourceKillCheck -> Check{base}ResourceKillCheckOutcome
      (Default, marker absent) -> {base}Degraded -> [branch ends] ->
      AggregateParityBranchOutcomes -> CheckParityBranchOutcomes
      (DEGRADED fold) -> ParityDegraded -> PublishParityDegraded ->
      CheckSkipPitParityCompare -> PitParityCompare (compare STILL runs
      and emits verdict UNKNOWN, sf-pipeline-policy §2.3a) -> ... ->
      CheckSkipEvaluator. NormalizeFailureContext/HandleFailure/
      FailExecution must NEVER appear.
    """
    b = branches[base]
    outcome = b[f"Check{base}ResourceKillCheckOutcome"]
    assert outcome["Default"] == f"{base}Degraded"

    degraded = b[f"{base}Degraded"]
    # alpha-engine-config-I8194: envelope nested under the branch key.
    assert list(degraded["Parameters"].values()) == [{"status": "DEGRADED"}]
    assert "ResultPath" not in degraded
    assert degraded.get("End") is True

    cbo = states["CheckParityBranchOutcomes"]
    degraded_choice = cbo["Choices"][1]
    assert degraded_choice["Next"] == "ParityDegraded"
    var_suffix = {
        "PitParityLookahead": "pit_lookahead_status",
        "PitParityWalkforward": "pit_walkforward_status",
    }[base]
    assert any(
        cond["Variable"] == f"$.parity_branch_outcomes.{var_suffix}"
        and cond["StringEquals"] == "DEGRADED"
        for cond in degraded_choice["Or"]
    )

    history = _walk(states, "ParityDegraded")
    assert "PublishParityDegraded" in history
    assert "CheckSkipPitParityCompare" in history
    assert "NormalizeFailureContext" not in history
    assert "HandleFailure" not in history
    assert "FailExecution" not in history


# ---------------------------------------------------------------------------
# ParityReplay + PitParityCompare: the StandardErrorContent mechanism
# ---------------------------------------------------------------------------


def test_parity_replay_resource_kill_classified_from_standard_error_content(branches):
    b = branches["ParityReplay"]
    check = b["CheckParityReplayStatus"]
    rk_choice = next(
        c for c in check["Choices"] if c.get("StringMatches") == "*RESOURCE KILL*"
    )
    assert rk_choice["Variable"] == "$.parity_replay_poll.StandardErrorContent"
    assert rk_choice["Next"] == "ParityReplayResourceKill"
    assert check["Default"] == "ParityReplayDegraded"

    terminal = b["ParityReplayResourceKill"]
    assert terminal["Type"] == "Pass"
    assert terminal.get("End") is True
    # alpha-engine-config-I8194: envelope nested under the branch key,
    # ResultPath gone; the mirror assertion moves to the key.
    assert terminal["Parameters"] == {
        "branch_parity_replay": {"status": "RESOURCE_KILL"}
    }
    assert "ResultPath" not in terminal
    assert set(terminal["Parameters"]) == set(
        b["ParityReplayDegraded"]["Parameters"]
    )

    cbo_choice_vars = {
        c["Variable"] for c in
        __import__("json").loads(_WEEKLY.read_text())["States"]["CheckParityBranchOutcomes"]["Choices"][0]["Or"]
    }
    assert "$.parity_branch_outcomes.parity_replay_status" in cbo_choice_vars


def test_parity_replay_success_is_checked_before_resource_kill_pattern(branches):
    """Ordering: Success must be checked BEFORE the StandardErrorContent
    pattern match, so a happy-path poll can never be misrouted even if
    StandardErrorContent happened to contain the substring incidentally."""
    check = branches["ParityReplay"]["CheckParityReplayStatus"]
    kinds = [
        "Success" if c.get("StringEquals") == "Success"
        else "RESOURCE_KILL" if c.get("StringMatches") == "*RESOURCE KILL*"
        else "other"
        for c in check["Choices"]
    ]
    assert kinds.index("Success") < kinds.index("RESOURCE_KILL")


def test_pit_parity_compare_resource_kill_routes_directly_to_hard_fail(states):
    """PitParityCompare is SEQUENTIAL (not a Parallel branch) — no
    sibling-isolation reason to fail-open a resource kill there, so it
    routes STRAIGHT into the shared hard-fail chokepoint rather than
    through a branch-status fold."""
    check = states["CheckPitParityCompareStatus"]
    rk_choice = next(
        c for c in check["Choices"] if c.get("StringMatches") == "*RESOURCE KILL*"
    )
    assert rk_choice["Variable"] == "$.pit_parity_compare_poll.StandardErrorContent"
    assert rk_choice["Next"] == "PitParityCompareResourceKill"
    assert check["Default"] == "ParityCompareDegraded"

    kinds = [
        "Success" if c.get("StringEquals") == "Success"
        else "RESOURCE_KILL" if c.get("StringMatches") == "*RESOURCE KILL*"
        else "other"
        for c in check["Choices"]
    ]
    assert kinds.index("Success") < kinds.index("RESOURCE_KILL")

    terminal = states["PitParityCompareResourceKill"]
    assert terminal["Type"] == "Pass"
    assert terminal["Parameters"]["Error"] == "PitParityResourceKillHalt"
    assert terminal["ResultPath"] == "$.error"
    assert terminal["Next"] == "NormalizeFailureContext"

    history = _walk(states, "PitParityCompareResourceKill")
    assert "NormalizeFailureContext" in history
    # NormalizeFailureContext -> NormalizeFailureContextRepin (a Choice, one
    # hop past this deterministic Pass/Task-only walker) -> HandleFailure ->
    # FailExecution is the SAME shared chokepoint every other genuine SF
    # failure already uses — asserted directly by name (config#1819).
    assert states["NormalizeFailureContext"]["Next"] == "NormalizeFailureContextRepin"
    assert through_release(states, states["HandleFailure"]["Next"]) == "FailExecution"


# ---------------------------------------------------------------------------
# Every route the fold can take is exhaustive and correctly ordered
# ---------------------------------------------------------------------------


def test_check_parity_branch_outcomes_checks_resource_kill_before_degraded(states):
    cbo = states["CheckParityBranchOutcomes"]
    assert len(cbo["Choices"]) == 2
    assert cbo["Choices"][0]["Next"] == "PitParityResourceKillDetected"
    assert cbo["Choices"][1]["Next"] == "ParityDegraded"
    assert cbo["Default"] == "CheckSkipPitParityCompare"


def test_pit_parity_resource_kill_detected_carries_forensics_and_error_name(states):
    st = states["PitParityResourceKillDetected"]
    assert st["Type"] == "Pass"
    assert st["Parameters"]["Error"] == "PitParityResourceKillHalt"
    assert "Cause.$" in st["Parameters"]
    assert st["ResultPath"] == "$.error"
    assert st["Next"] == "NormalizeFailureContext"


# --- the marker gate's grep pattern, exercised as a predicate --------------
#
# The gate was `grep -qF '"timed_out": true'` until 2026-08-20, when the
# backslash-escaped quotes in that literal were found to be illegal inside an
# ASL intrinsic (they blocked every deploy from 2026-08-19 and halted the
# 2026-08-20 preopen). The replacement, `grep -qE 'timed_out[^a-z]+true'`, is
# deliberately LOOSER than an exact-bytes match: it asks whether the pass
# reported a timeout rather than how the artifact writer spaced its JSON. These
# cases pin that the widening does not reach anything it should not.

_MARKER_GATE_PATTERN = "timed_out[^a-z]+true"


@pytest.mark.parametrize(
    "line, expected",
    [
        ('  "timed_out": true,', True),
        ('{"timed_out":true}', True),
        ('  "timed_out" : true', True),
        ('  "timed_out": false,', False),
        # a later key's `true` must not satisfy the gate — the intervening
        # lowercase key name breaks [^a-z]+
        ('{"timed_out": false, "verdict_clean": true}', False),
        ('{"verdict": "FAIL"}', False),
    ],
)
def test_marker_gate_pattern_matches_only_a_timed_out_pass(line, expected):
    assert bool(re.search(_MARKER_GATE_PATTERN, line)) is expected


def test_marker_gate_pattern_is_the_one_the_definition_ships(states):
    for base in ("PitParityLookahead", "PitParityWalkforward"):
        for br in states["ParityParallel"]["Branches"]:
            if base in br["States"]:
                cmds = br["States"][base]["Parameters"]["Parameters"]["commands.$"]
                assert _MARKER_GATE_PATTERN in cmds, (
                    f"{base}: this module's pattern cases are asserted against a "
                    f"pattern the definition no longer uses"
                )
                break
        else:
            pytest.fail(f"{base} not found in any ParityParallel branch")
