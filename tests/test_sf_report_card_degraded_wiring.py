"""alpha-engine-config#6685 — a ReportCard failure must never terminate the
weekly SF at plain ``NotifyComplete`` ("All steps completed successfully").

Pre-fix: ``ReportCard``'s ``Catch(States.ALL)`` routed straight to
``PublishReportCardDegraded`` (a standalone, best-effort SNS PAGE alert) and
then continued to ``CheckShellRunNotify`` → ``CheckGateDegradedNotify``,
which branched ONLY on ``$.gate_degraded`` / ``$.health_check_degraded``. A
ReportCard failure on an otherwise-clean run (both of those absent) fell
through the ``Default`` edge straight to ``NotifyComplete`` — the terminal
notification never named ReportCard as degraded, violating
weekly-sf-policy.md §2.3 ("a pipeline reaching NotifyComplete having
degraded any stage must say so, by name, in the notification"). The
Director had the identical shape; Brian ruled 2026-08-04
(alpha-engine-config#6408) that a Director failure is terminal instead
(nousergon-data#1233). ReportCard is advisory-only (non-fatal by design —
see ReportCard's own Comment), so the fix here is the OTHER shape: make the
degradation VISIBLE in the terminal notify, not fail the execution.

Shape pinned here (mirrors test_sf_prespend_gate_alerting.py /
test_sf_health_check_honesty_wiring.py):
  1. ``ReportCard``'s Catch now routes to a new ``ReportCardDegraded`` Pass
     state (mirrors ``LibPinGateDegraded`` / ``SaturdayHealthCheckDegraded``
     shape) which sets ``$.report_card_degraded: true`` before continuing,
     unchanged, to ``PublishReportCardDegraded`` (the immediate PAGE alert,
     config#2302).
  2. ``report_card_degraded`` threads into ``CheckGateDegradedNotify``
     alongside the pre-existing ``gate_degraded`` / ``health_check_degraded``
     flags. Per that Choice state's own config#2276 comment predicting this
     exact moment ("a THIRD flag family should trigger a refactor to a
     data-driven degraded notifier, not a 7-way enumeration"), the fix FOLDS
     rather than enumerates: any combination of report_card_degraded with
     EITHER other family routes to one generic ``NotifyCompleteMultipleDegraded``
     (names report card explicitly, points at the execution record for
     which other family also fired) instead of 4 additional per-combination
     hardcoded Task states; report_card_degraded alone gets its own
     single-flag notifier ``NotifyCompleteReportCardDegraded`` mirroring
     ``NotifyCompleteGatesDegraded`` / ``NotifyCompleteHealthDegraded``
     exactly. The pre-existing gates/health-only rules are untouched.
  3. The hard invariant this module exists to assert: for EVERY one of the
     8 boolean combinations of (gate_degraded, health_check_degraded,
     report_card_degraded), a payload with report_card_degraded=true NEVER
     resolves to plain ``NotifyComplete``, and always resolves to a notifier
     whose Subject/Message names "report card".
"""
from __future__ import annotations

import itertools
import json
import pathlib

import pytest
from tests.sf_degraded_summary_helpers import (
    assert_completion_notifier_chain,
    assert_degraded_continuation,
    notify_target,
)

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


# ---------------------------------------------------------------------------
# ReportCard's Catch sets the flag before alerting (config#6685)
# ---------------------------------------------------------------------------


def test_report_card_catch_routes_through_degraded_pass(states):
    (catch,) = states["ReportCard"]["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "ReportCardDegraded", (
        "ReportCard's Catch must set the degraded flag via ReportCardDegraded, "
        f"not {catch['Next']!r} — a direct jump to the alert Task is the exact "
        "silent-terminal-notify gap config#6685 closed"
    )
    assert catch["ResultPath"] == "$.report_card_error"


def test_report_card_degraded_pass_shape_and_proceeds_to_publish(states):
    st = states["ReportCardDegraded"]
    assert st["Type"] == "Pass"
    assert st["Result"] is True
    assert st["ResultPath"] == "$.report_card_degraded"
    # Fail-soft: set the flag, then proceed to the existing PAGE alert
    # (config#2302) unchanged.
    assert_degraded_continuation(states, "ReportCardDegraded", "PublishReportCardDegraded")


def test_only_report_card_degraded_pass_sets_the_flag(states):
    """SF-controlled: exactly one Pass state may write
    $.report_card_degraded (mirror of test_only_health_degraded_passes_set_
    health_check_degraded in test_sf_health_check_honesty_wiring.py)."""
    writers = [
        name for name, st in states.items()
        if st.get("ResultPath") == "$.report_card_degraded"
    ]
    assert writers == ["ReportCardDegraded"]


def test_publish_report_card_degraded_unchanged_downstream(states):
    """The existing immediate PAGE alert (config#2302) still fires and still
    proceeds to the notify gate — this fix adds a flag upstream of it, it does
    not change its own wiring.

    alpha-engine-config-I7813 moved the gate one hop: the degraded route now
    lands on CheckSkipScannerLeaderboard, which routes to CheckShellRunNotify
    on the skip arm and after the leaf on the run arm. Load-bearing rather than
    cosmetic — the observe-only board must still be built on a run whose report
    card failed, because it does not consume the report card."""
    st = states["PublishReportCardDegraded"]
    assert st["Next"] == "CheckSkipScannerLeaderboard"
    (catch,) = st["Catch"]
    assert catch["Next"] == "CheckSkipScannerLeaderboard"


# ---------------------------------------------------------------------------
# The hard invariant: no path from ReportCard's Catch reaches plain
# NotifyComplete, for every combination of the three degraded flags.
# ---------------------------------------------------------------------------


_FLAG_COMBOS = [
    dict(zip(("gate_degraded", "health_check_degraded", "report_card_degraded"), bits))
    for bits in itertools.product([True, False], repeat=3)
]


@pytest.mark.parametrize("combo", _FLAG_COMBOS, ids=lambda c: (
    "g" if c["gate_degraded"] else "-")
    + ("h" if c["health_check_degraded"] else "-")
    + ("r" if c["report_card_degraded"] else "-")
)
def test_report_card_degraded_never_reaches_plain_notify_complete(states, combo):
    # A Choice payload only carries the flags that are actually set — an
    # absent flag is IsPresent-guarded, not present-as-false.
    payload = {k: True for k, v in combo.items() if v}
    target = notify_target(states, payload)
    if combo["report_card_degraded"]:
        assert target != "NotifyComplete", (
            f"payload {payload} reached plain NotifyComplete — a ReportCard "
            "degradation must always surface in the terminal notification "
            "(weekly-sf-policy.md §2.3)"
        )
        assert target in (
            "NotifyCompleteReportCardDegraded", "NotifyCompleteMultipleDegraded",
        )
        subject = states[target]["Parameters"]["Subject"]
        message = states[target]["Parameters"]["Message"]
        assert "report card" in subject.lower() or "report card" in message.lower(), (
            f"{target}'s Subject/Message must name ReportCard as degraded, by name"
        )
    else:
        assert target not in (
            "NotifyCompleteReportCardDegraded", "NotifyCompleteMultipleDegraded",
        )


# ---------------------------------------------------------------------------
# The two new notify states mirror the established config#1819 shape.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("notifier", [
    "NotifyCompleteReportCardDegraded", "NotifyCompleteMultipleDegraded",
])
def test_new_notifiers_mirror_config_1819_shape(states, notifier):
    st = states[notifier]
    assert st["Type"] == "Task"
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
    # config#2857: converges into the SF-envelope completion marker.
    assert "End" not in st
    assert_completion_notifier_chain(states, notifier)
    (catch,) = st["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "NotifyCompleteDegraded"


def test_report_card_only_subject_names_report_card(states):
    subject = states["NotifyCompleteReportCardDegraded"]["Parameters"]["Subject"]
    assert "report card" in subject.lower()


def test_multiple_degraded_names_report_card(states):
    """alpha-engine-config-I6025: NotifyCompleteMultipleDegraded is now also
    reachable via parity+gate / parity+health combos that do NOT involve
    report_card_degraded at all, so the Subject can no longer hardcode
    "report card" as though it were always the trigger (that claim would be
    false on those combos). The generalized notifier still names report
    card, generically, in its Message body — subject-or-message is the
    invariant asserted here and throughout this module and
    test_sf_parity_gate_notify_wiring.py."""
    st = states["NotifyCompleteMultipleDegraded"]
    subject = st["Parameters"]["Subject"]
    message = st["Parameters"]["Message"]
    assert "report card" in subject.lower() or "report card" in message.lower()
