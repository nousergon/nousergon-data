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
     "ReportCard", 240),
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
    assert degraded_state["Next"] == proceed


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
    assert states[send]["Next"] == wait
    assert states[wait]["Next"] == status, (
        f"{wait} must feed the terminal-status Choice — check-once polling "
        "is how a failing health checker stayed invisible (config#2276)"
    )

    choice = states[status]
    rules = choice["Choices"]
    success = next(r for r in rules if r.get("StringEquals") == "Success")
    assert success["Variable"] == f"{poll}.Status"
    assert success["Next"] == proceed

    in_flight = next(r for r in rules if "Or" in r)
    looped = {op["StringEquals"] for op in in_flight["Or"]}
    assert looped == {"InProgress", "Pending", "Delayed"}, (
        f"{status} must loop on exactly the non-terminal statuses; got {looped}"
    )
    assert in_flight["Next"] == poll_wait
    assert states[poll_wait]["Type"] == "Wait"
    assert states[poll_wait]["Next"] == wait

    # THE drill edge: a terminal non-Success (Failed / TimedOut / Cancelled
    # — incl. executionTimeout expiry killing a hung checker) must land on
    # the degraded Pass, not fall through to a plain notifier.
    assert choice["Default"] == degraded


# ---------------------------------------------------------------------------
# degraded flag threads into the completion-email selection
# ---------------------------------------------------------------------------

def _notify_target(states, data: dict) -> str:
    """Evaluate the CheckShellRunNotify → CheckGateDegradedNotify selection
    with ASL short-circuit semantics against a partial payload."""
    def eval_rule(rule):
        if "And" in rule:
            return all(eval_rule(op) for op in rule["And"])
        var, present = rule["Variable"].lstrip("$."), rule["Variable"].lstrip("$.") in data
        if "IsPresent" in rule:
            return present == rule["IsPresent"]
        assert present, f"unguarded dereference of {var} in drill payload {data}"
        return data[var] == rule["BooleanEquals"]

    cur = "CheckShellRunNotify"
    while states[cur]["Type"] == "Choice":
        for rule in states[cur]["Choices"]:
            if eval_rule(rule):
                cur = rule["Next"]
                break
        else:
            cur = states[cur]["Default"]
    return cur


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
    assert _notify_target(states, payload) == expected


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
    assert "SUCCESS" in subject and "DEGRADED" in subject
    assert 0 < len(subject) <= 100
    assert "\n" not in subject
    assert "health checks" in subject
    assert st["End"] is True
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
    cmds = states["WeeklySubstrateHealthCheck"]["Parameters"]["Parameters"]["commands"]
    (drift_line,) = [c for c in cmds if "constituents_drift_check" in c]
    assert "|| true" not in drift_line, (
        "config#2276: the drift check exits 1 on alert-worthy drift (the "
        "2026-05-23 BNY/P/SN incident surface) — '|| true' swallowed exactly "
        "that signal; under set -eo pipefail its failure must fail the "
        "command so the poll Choice degrades the completion email"
    )
    # the log-upload trap's own '|| true' is fine (best-effort log shipping,
    # inline-commented failure mode) — pin that the drift WORK line is the
    # only place we assert on.
    assert cmds[0].startswith("set -eo pipefail")


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
