"""alpha-engine-config-I6025 — the Parity stage must DEGRADE, not FAIL, the
weekly SF.

Pre-fix (the 2026-08-01 watch-rerun-2026-08-01-3 failure): the Parity state's
SSM command hit its 2h executionTimeout (ResponseCode 137, ExecutionTimedOut)
and CheckParityStatus.Default routed through ExtractParityError →
NormalizeFailureContext → HandleFailure, FAILING a run that had already
cleared Backtester, PredictorBacktest and PortfolioOptimizerBacktest. But
parity's VERDICT is observational today (the parity_alarms gate sits in
OBSERVE) and NO downstream SF stage consumes its artifacts (verified
dependency audit in the issue: evaluate.py::_load_pit_parity_report is
best-effort and explicitly excluded from critical-artifact accounting) —
an observational stage must not be able to fail the pipeline by taking too
long. Brian's 2026-08-01 ruling kept parity on the SF; the execution failure
degrades, pages distinctly, and the run continues.

Shape pinned here (mirrors test_sf_health_check_honesty_wiring.py /
config#2276 + the config#2302 ReportCard/Director degrade pattern):

  * every Parity non-success path converges on the ParityDegraded Pass
    (CheckParityStatus.Default = terminal non-Success command status; the
    Parity + WaitForParity Task Catches = send/poll infra failure);
  * ParityDegraded sets the SF-controlled $.parity_degraded flag (specifics
    live on the execution record: $.parity_poll / $.parity_error);
  * PublishParityDegraded pages distinctly (constants-only Subject/Message
    per config#1819) with a best-effort Catch, and the run CONTINUES to
    CheckSkipEvaluator → Evaluator — never HandleFailure;
  * absence of a verdict must never render identically to a clean pass: the
    page fires immediately + the ARTIFACT_REGISTRY freshness-monitor SLA
    alarms on missing parity artifacts.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


def _catches(states, name) -> list:
    return states[name].get("Catch", [])


# ---------------------------------------------------------------------------
# Catch routing + degraded flag
# ---------------------------------------------------------------------------


def test_send_and_wait_catches_route_through_parity_degraded(states):
    """Both Task states (send + poll) must Catch States.ALL → ParityDegraded
    with the specifics on $.parity_error — never a direct jump to a
    notifier, and never NormalizeFailureContext (that is the SF-failing
    path this change retires for parity)."""
    for name in ("Parity", "WaitForParity"):
        catches = _catches(states, name)
        assert catches, f"{name} must keep its fail-soft Catch"
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == "ParityDegraded", (
                f"{name} Catch must set the degraded flag via "
                f"ParityDegraded, not {c['Next']!r}"
            )
            assert c["ResultPath"] == "$.parity_error"

    degraded = states["ParityDegraded"]
    assert degraded["Type"] == "Pass"
    assert degraded["Result"] is True
    assert degraded["ResultPath"] == "$.parity_degraded"
    # Degrade then PROCEED — Evaluator/ReportCard/Director do not consume
    # parity artifacts and must not be skipped because parity failed.
    assert degraded["Next"] == "PublishParityDegraded"


def test_only_parity_degraded_sets_parity_degraded(states):
    """The degraded flag must be SF-controlled: exactly the ParityDegraded
    Pass may write $.parity_degraded (mirror of the health-check writers
    pin)."""
    writers = [
        name for name, st in states.items()
        if st.get("ResultPath") == "$.parity_degraded"
    ]
    assert writers == ["ParityDegraded"]


def test_extract_parity_error_retired(states):
    """The old SF-failing normalizer is gone — no parity path may reach
    HandleFailure."""
    assert "ExtractParityError" not in states
    for name in ("Parity", "WaitForParity"):
        targets = [c["Next"] for c in _catches(states, name)]
        assert "NormalizeFailureContext" not in targets, (
            f"{name} still routes a Catch to NormalizeFailureContext — "
            "parity must degrade, not fail the SF (alpha-engine-config-I6025)"
        )


# ---------------------------------------------------------------------------
# publish + continue
# ---------------------------------------------------------------------------


def test_publish_parity_degraded_pages_constants_and_continues(states):
    pub = states["PublishParityDegraded"]
    assert pub["Type"] == "Task"
    assert pub["Resource"] == "arn:aws:states:::sns:publish"
    # config#1819: constants-only Subject/Message — the only .$ reference is
    # the topic ARN floor from InitializeInput (a parameterized Subject/
    # Message would reintroduce the SNS-contract States.Runtime class).
    assert pub["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
    assert "Subject" in pub["Parameters"]
    assert "Subject.$" not in pub["Parameters"]
    assert "Message.$" not in pub["Parameters"]
    assert "DEGRADED" in pub["Parameters"]["Subject"]
    assert len(pub["Parameters"]["Subject"]) <= 100
    # best-effort: a publish failure must not block the non-fatal degrade
    # path this alert decorates
    catches = _catches(states, "PublishParityDegraded")
    assert catches and catches[0]["ErrorEquals"] == ["States.ALL"]
    assert catches[0]["Next"] == "CheckSkipEvaluator"
    # and the happy publish continues to Evaluator's skip-gate too
    assert pub["Next"] == "CheckSkipEvaluator"


def test_no_parity_state_catch_targets_notify_or_handle_directly(states):
    """The pre-fix defect class: a Catch jumping straight to a notifier or
    to the failure handler, skipping the degraded flag."""
    for name in ("Parity", "WaitForParity"):
        targets = [c["Next"] for c in _catches(states, name)]
        assert "NotifyComplete" not in targets
        assert "CheckShellRunNotify" not in targets
        assert "HandleFailure" not in targets


# ---------------------------------------------------------------------------
# poll-to-terminal-status loop
# ---------------------------------------------------------------------------


def test_poll_resolves_to_terminal_status(states):
    assert states["Parity"]["Next"] == "WaitForParity"
    assert states["WaitForParity"]["Next"] == "CheckParityStatus"

    choice = states["CheckParityStatus"]
    rules = choice["Choices"]
    success = next(r for r in rules if r.get("StringEquals") == "Success")
    assert success["Variable"] == "$.parity_poll.Status"
    assert success["Next"] == "CheckSkipEvaluator"

    in_flight = {r["StringEquals"]: r["Next"] for r in rules
                 if r.get("StringEquals") in ("InProgress", "Pending")}
    assert in_flight == {"InProgress": "ParityWait", "Pending": "ParityWait"}
    assert states["ParityWait"]["Next"] == "WaitForParity"

    # THE pin: terminal non-Success (TimedOut / Failed / Cancelled) degrades
    # instead of failing the SF.
    assert choice["Default"] == "ParityDegraded"


# ---------------------------------------------------------------------------
# completion-email honesty: a degraded parity run must NOT email the plain
# SUCCESS — the immediate page + execution record carry it (the degraded
# notifier enumeration is bounded at the gates/health families per the
# config#2276 composition note; parity pages distinctly at PublishParityDegraded).
# ---------------------------------------------------------------------------


def test_degraded_parity_never_reaches_notify_complete_without_flag(states):
    """The degrade chain must set $.parity_degraded BEFORE any completion
    path — walking from CheckParityStatus.Default, the flag is present and
    the run continues through Evaluator/ReportCard/Director."""
    seen = set()
    stack = ["ParityDegraded"]
    while stack:
        name = stack.pop()
        if name in seen or name not in states:
            continue
        seen.add(name)
        nxt = states[name].get("Next")
        if nxt:
            stack.append(nxt)
        for c in states[name].get("Catch", []):
            stack.append(c["Next"])
    assert "CheckSkipEvaluator" in seen
    assert "NormalizeFailureContext" not in seen
    assert "HandleFailure" not in seen
    assert "PublishParityDegraded" in seen
