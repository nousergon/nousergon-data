"""config#2276 — the weekly-SF tail health checks must be honest: they may
fail SOFT, but never SILENTLY.

Pre-fix, the two post-pipeline health stages could no-op while the run
emailed the plain real-run SUCCESS:

  1. ``SaturdayHealthCheck`` / ``WeeklySubstrateHealthCheck`` Catches routed
     DIRECTLY to ``NotifyComplete`` — bypassing BOTH ``CheckShellRunNotify``
     and ``CheckGateDegradedNotify`` (config#2278), so a health-check crash
     on a gates-degraded or preflight run sent the plain real-run SUCCESS
     email, and silently skipped the ReportCard/Director advisory tail.
  2. Each ``WaitFor*`` poll was CHECK-ONCE: the single getCommandInvocation
     usually returned InProgress and the SF moved on — a hung/failing
     health_checker.py was invisible, and the substrate command was
     dispatched while the freshness command's ``git pull`` still held the
     dashboard repo's ref lock (the recurring live 'Cannot fast-forward to
     multiple branches' / 'cannot lock ref' sub-second failures observed
     2026-06-19 → 2026-07-11 — every one of which still emailed SUCCESS).
  3. ``WeeklySubstrateHealthCheck`` ran ``pip install --quiet --upgrade -r
     requirements.txt`` mid-pipeline (live PyPI dependency inside an
     observability stage) and swallowed the constituents-drift sub-step
     with ``|| true``.

Shape pinned here (mirrors test_sf_prespend_gate_alerting.py / config#2278):
  * poll-to-terminal-status loop per check (WaitFor → Check*Status Choice →
    Success edge | in-flight Wait loop | Default → *Degraded Pass);
  * every Catch on the four health states routes through its *Degraded
    Pass (``health_check_degraded: true``) and CONTINUES the tail;
  * ``health_check_degraded`` threads into the completion-email selection:
    CheckShellRunNotify → CheckGateDegradedNotify → constants-only degraded
    notifiers (gates+health / gates / health), Default NotifyComplete;
  * no runtime pip; no ``|| true`` drift swallow; timeout convention
    (inner executionTimeout = budget, delivery 60, outer = inner + 30).
"""
from __future__ import annotations

import json
import pathlib

import pytest

from tests.sf_command_utils import extract_commands
from tests.sf_degraded_summary_helpers import (
    assert_completion_notifier_chain,
    assert_degraded_continuation,
    notify_target,
)

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


CHECKS = [
    # (send state, wait state, status choice, poll wait, degraded pass,
    #  poll result path, degraded proceeds-to, inner executionTimeout)
    ("SaturdayHealthCheck", "WaitForSaturdayHealthCheck",
     "CheckSaturdayHealthCheckStatus", "SaturdayHealthCheckPollWait",
     "SaturdayHealthCheckDegraded", "$.health_check_poll",
     "WeeklySubstrateHealthCheck", 300),
    ("WeeklySubstrateHealthCheck", "WaitForWeeklySubstrateHealthCheck",
     "CheckSubstrateHealthCheckStatus", "SubstrateHealthCheckPollWait",
     "SubstrateHealthCheckDegraded", "$.substrate_check_poll",
     # config#6054: the tail entry is the per-stage skip gate; its Default is
     # ReportCard, so the degrade-then-proceed property holds.
     # alpha-engine-config-I7620: RunScope is now the tail's head and routes
     # straight on to that gate, so the property is unchanged — a degraded
     # substrate check still proceeds, and now does so with a scope block.
     "RunScope", 240),
]
_IDS = [c[0] for c in CHECKS]


# ---------------------------------------------------------------------------
# Catch routing + degraded flag
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("send", "wait", "status", "_pw", "degraded", "_poll", "proceed", "_t"),
    CHECKS, ids=_IDS)
def test_send_and_wait_catches_route_through_degraded_pass(
    states, send, wait, status, _pw, degraded, _poll, proceed, _t
):
    for name in (send, wait):
        catches = states[name]["Catch"]
        assert catches, f"{name} must keep its fail-soft Catch"
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == degraded, (
                f"{name} Catch must set the degraded flag via {degraded}, "
                f"not {c['Next']!r} — a direct jump to a notifier is the "
                "silent-skip masking config#2276 closed"
            )

    degraded_state = states[degraded]
    assert degraded_state["Type"] == "Pass"
    assert degraded_state["Result"] is True
    assert degraded_state["ResultPath"] == "$.health_check_degraded"
    # Fail-soft: degrade then PROCEED with the rest of the tail — the two
    # checks are independent, and ReportCard/Director are Lambdas that must
    # not be skipped because an EC2 health command failed.
    assert_degraded_continuation(states, degraded, proceed)


def test_only_health_degraded_passes_set_health_check_degraded(states):
    """The completion-email marker must be SF-controlled: exactly the two
    health-degraded Pass states may write $.health_check_degraded (mirror of
    the $.gate_degraded writers pin in test_sf_prespend_gate_alerting.py)."""
    writers = [
        name for name, st in states.items()
        if st.get("ResultPath") == "$.health_check_degraded"
    ]
    assert sorted(writers) == [
        "SaturdayHealthCheckDegraded", "SubstrateHealthCheckDegraded",
    ]


def test_no_health_state_catch_targets_notify_complete_directly(states):
    """The exact pre-fix defect: any of the four health states' Catch
    jumping straight to a success notifier."""
    for send, wait, *_ in CHECKS:
        for name in (send, wait):
            targets = [c["Next"] for c in states[name].get("Catch", [])]
            assert "NotifyComplete" not in targets
            # even the selection-chain entry is wrong as a Catch target —
            # the degraded Pass must come first so the flag is set
            assert "CheckShellRunNotify" not in targets


# ---------------------------------------------------------------------------
# poll-to-terminal-status loop (the check-once fix)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("send", "wait", "status", "poll_wait", "degraded", "poll", "proceed", "_t"),
    CHECKS, ids=_IDS)
def test_poll_resolves_to_terminal_status(
    states, send, wait, status, poll_wait, degraded, poll, proceed, _t
):
    # alpha-engine-config-I5687: send now dispatches through the
    # poll-budget seed (Init<Label>PollCount) before the first poll, and the
    # loop-back branch is a bounded And[] (IsPresent + Or[...] +
    # NumericLessThan cap) rather than a bare Or[] — mirrors the
    # DataPhase2/ThinkTank precedent.
    label = {
        "SaturdayHealthCheck": "SaturdayHealthCheck",
        "WeeklySubstrateHealthCheck": "SubstrateHealthCheck",
    }[send]
    init_name = f"Init{label}PollCount"
    merge_name = f"Merge{label}PollCount"

    assert states[send]["Next"] == init_name
    assert states[init_name]["Next"] == wait
    assert states[wait]["Next"] == status, (
        f"{wait} must feed the terminal-status Choice — check-once polling "
        "is how a failing health checker stayed invisible (config#2276)"
    )

    choice = states[status]
    rules = choice["Choices"]
    success = next(r for r in rules if r.get("StringEquals") == "Success")
    assert success["Variable"] == f"{poll}.Status"
    assert success["Next"] == proceed

    bounded = next(r for r in rules if "And" in r)
    poll_var = poll.replace("_poll", "_polls")
    variables = {cond.get("Variable") for cond in bounded["And"]}
    assert poll_var in variables
    or_block = next(cond["Or"] for cond in bounded["And"] if "Or" in cond)
    looped = {op["StringEquals"] for op in or_block}
    assert looped == {"InProgress", "Pending", "Delayed"}, (
        f"{status} must loop on exactly the non-terminal statuses; got {looped}"
    )
    increment_name = f"{label}Wait"
    assert bounded["Next"] == increment_name
    assert states[increment_name]["Next"] == poll_wait
    assert states[poll_wait]["Type"] == "Wait"
    assert states[poll_wait]["Next"] == merge_name
    assert states[merge_name]["Next"] == wait

    # THE drill edge: a terminal non-Success (Failed / TimedOut / Cancelled
    # — incl. executionTimeout expiry killing a hung checker) must land on
    # the degraded Pass, not fall through to a plain notifier.
    assert choice["Default"] == degraded


# ---------------------------------------------------------------------------
# degraded flag threads into the completion-email selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("payload", "expected"), [
    # both flag families set → subject reflects both
    ({"gate_degraded": True, "health_check_degraded": True},
     "NotifyCompleteGatesAndHealthDegraded"),
    ({"gate_degraded": True}, "NotifyCompleteGatesDegraded"),
    ({"health_check_degraded": True}, "NotifyCompleteHealthDegraded"),
    ({}, "NotifyComplete"),  # clean run byte-identical
    # a preflight run still gets the shell-run notifier regardless of flags
    ({"shell_run": True, "health_check_degraded": True},
     "NotifyShellRunComplete"),
])
def test_degraded_flags_select_the_right_completion_notifier(
    states, payload, expected
):
    assert notify_target(states, payload) == expected


@pytest.mark.parametrize("notifier", [
    "NotifyCompleteHealthDegraded", "NotifyCompleteGatesAndHealthDegraded",
])
def test_degraded_notifiers_mirror_config_1819_shape(states, notifier):
    st = states[notifier]
    assert st["Resource"] == "arn:aws:states:::sns:publish"
    assert st["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
    # config#1819: constants only — no States.Format against state fields.
    assert "Subject.$" not in st["Parameters"]
    assert "Message.$" not in st["Parameters"]
    subject = st["Parameters"]["Subject"]
    # alpha-engine-config-I7418: this used to assert "SUCCESS" in subject.
    # Since config-I6891 a degraded run routes through CheckDegradedOutcome ->
    # WriteCompletionMarkerDegraded -> DegradedRun, a **Fail** state — so a
    # notifier whose subject leads with SUCCESS states the opposite of the
    # run's own terminal, and the guard was pinning the false claim.
    assert "DEGRADED" in subject
    assert "SUCCESS" not in subject, (
        "a degraded run terminates FAILED (config-I6891); a subject leading "
        "with SUCCESS contradicts the execution's own status"
    )
    assert 0 < len(subject) <= 100
    assert "\n" not in subject
    assert "health checks" in subject
    # config#2857: the real-completion path no longer Ends here directly —
    # it converges into the SF-envelope completion marker before ending.
    assert "End" not in st
    assert_completion_notifier_chain(states, notifier)
    (catch,) = st["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "NotifyCompleteDegraded"  # config#1819 idiom


def test_both_flags_subject_names_both_families(states):
    subject = states["NotifyCompleteGatesAndHealthDegraded"]["Parameters"]["Subject"]
    assert "gates" in subject and "health checks" in subject


# ---------------------------------------------------------------------------
# command hygiene: no runtime pip, no || true drift swallow
# ---------------------------------------------------------------------------

def _all_command_arrays(states):
    for name, st in states.items():
        cmds = (st.get("Parameters", {}) or {}).get("Parameters", {}).get("commands")
        if cmds:
            yield name, cmds


def test_no_runtime_pip_install_anywhere_in_definition(states):
    """config#2276: deps come from the dashboard box's deploy-time venv sync
    (crucible-dashboard infrastructure/deploy-on-merge.sh pip-installs on
    requirements.txt diff; nousergon-lib is tag-pinned so a lib bump always
    diffs requirements.txt). A live PyPI/network dependency mid-pipeline is
    forbidden — --upgrade could also float unpinned transitive deps past
    tested versions."""
    offenders = [
        name for name, cmds in _all_command_arrays(states)
        if "pip install" in " ".join(cmds)
    ]
    assert not offenders, f"runtime pip install in: {offenders}"


def test_constituents_drift_step_is_fail_visible(states):
    # alpha-engine-config-I7047 (2026-08-12): the three inline commands
    # (transparency sweep, constituents_drift_check, phase_marker_sweep)
    # moved out of this SF definition into crucible-dashboard
    # infrastructure/substrate_health_check.sh, invoked here through
    # krepis.ssm_log_capture (mirrors the 17 other Saturday SF stages) —
    # the prior inline `trap 'aws s3 cp ... EXIT'` wrapper collapsed under
    # ASL's States.Array escape semantics (`trap: s3: invalid signal
    # specification`, rc=127) on every run using it, which is why the
    # 2026-08-08 scheduled run finished DEGRADED despite every real work
    # stage succeeding. The "drift check must not swallow its own exit
    # code with `|| true`" invariant this test used to pin now lives in
    # crucible-dashboard's own test suite
    # (tests/test_substrate_health_check_weekly_wiring.py), since the
    # command itself is no longer visible in this repo's SF JSON. What
    # THIS repo can still pin: no runtime `pip install` (unchanged
    # invariant, config#2276) and that the SF invokes the extracted
    # script through the krepis wrapper rather than any inline command.
    cmds = extract_commands(states["WeeklySubstrateHealthCheck"])
    assert cmds[0] == "set -eo pipefail"
    assert not any("pip install" in c for c in cmds)
    assert not any("trap 'aws s3 cp" in c for c in cmds), (
        "I7047: the inline trap/log-ship anti-pattern must not return to "
        "this state — krepis.ssm_log_capture is the sole log-capture path"
    )
    wrapper_line = next(
        c for c in cmds if "krepis.ssm_log_capture" in c
    )
    assert "--slug substrate-health-check" in wrapper_line
    assert "bash infrastructure/substrate_health_check.sh --run-date" in wrapper_line
    assert "$$.Execution.Name" in wrapper_line or "correlation-id" in wrapper_line


# ---------------------------------------------------------------------------
# timeout convention
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("send", "_w", "_s", "_pw", "_d", "_p", "_pr", "inner"),
    CHECKS, ids=_IDS)
def test_timeout_convention(states, send, _w, _s, _pw, _d, _p, _pr, inner):
    """config#2276 convention: inner executionTimeout = script budget
    (agent-enforced; expiry surfaces as terminal non-Success → degraded);
    SSM Parameters.TimeoutSeconds = 60 uniform (DELIVERY timeout);
    outer Task TimeoutSeconds = inner + 30."""
    st = states[send]
    ssm_params = st["Parameters"]["Parameters"]
    assert ssm_params["executionTimeout"] == [str(inner)]
    assert st["Parameters"]["TimeoutSeconds"] == 60
    assert st["TimeoutSeconds"] == inner + 30
