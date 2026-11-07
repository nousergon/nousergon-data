"""alpha-engine-config#6025 / alpha-engine-config#6044 — a Parity degradation
must never resolve to plain ``NotifyComplete``.

Context: nousergon-data#1206 made a Parity SSM timeout/crash DEGRADE the
weekly run instead of failing it (``ParityDegraded`` → ``PublishParityDegraded``
pages immediately, SF continues to Evaluator/ReportCard/Director). That
satisfied weekly-sf-policy.md §2.3 (visible to the operator via the
immediate page) but NOT §2.3a (propagated to the machine / the terminal
completion surface): the original PR set ``$.parity_degraded`` on the
execution record but never threaded it into ``CheckGateDegradedNotify``, so
a degraded-parity run's terminal email still said plain "SUCCESS" — the
exact "absence of a verdict rendered as a clean pass" shape
alpha-engine-config#6044 (RULED) forbids.

This module pins the fix: ``parity_degraded`` is a fourth flag family in
``CheckGateDegradedNotify``, folded into the existing generic
``NotifyCompleteMultipleDegraded`` when combined with any other family
(mirrors how #1258/config#6685 added report_card_degraded as the third
family), and routed to its own single-flag notifier
(``NotifyCompleteParityDegraded``) when parity is the only thing degraded —
same shape as ``NotifyCompleteReportCardDegraded`` /
``NotifyCompleteGatesDegraded`` / ``NotifyCompleteHealthDegraded``.

The hard invariant this module exists to assert: for every one of the 16
boolean combinations of (gate_degraded, health_check_degraded,
report_card_degraded, parity_degraded), a payload with parity_degraded=true
NEVER resolves to plain ``NotifyComplete``, and always resolves to a
notifier whose Subject/Message names "parity".
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
# ParityDegraded sets the flag before alerting (alpha-engine-config-I6025)
# ---------------------------------------------------------------------------


def test_parity_degraded_pass_shape_and_proceeds_to_publish(states):
    st = states["ParityDegraded"]
    assert st["Type"] == "Pass"
    assert st["Result"] is True
    assert st["ResultPath"] == "$.parity_degraded"
    assert_degraded_continuation(states, "ParityDegraded", "PublishParityDegraded")


def test_only_parity_degraded_passes_set_the_flag(states):
    """SF-controlled: exactly TWO Pass states may write $.parity_degraded —
    ParityDegraded (the post-join branch-degraded fold) and
    ParityCompareDegraded (the compare stage's own failure), both added/
    restructured by alpha-engine-config#6030. Anything else writing the flag
    fails here."""
    writers = sorted(
        name for name, st in states.items()
        if st.get("ResultPath") == "$.parity_degraded"
    )
    assert writers == ["ParityCompareDegraded", "ParityDegraded"]


def test_publish_parity_degraded_has_no_stray_parameters_timeout(states):
    """Regression guard: the original PR nested a bogus 'TimeoutSeconds' key
    inside sns:publish's Parameters dict — not a valid SNS Publish API
    parameter, and NOT the SF-level TimeoutSeconds the structural contract
    test (test_sf_structural_contract.py) checks for. Left in place, the SNS
    call would fail at runtime (InvalidParameter) and be silently swallowed
    by the best-effort Catch, defeating the immediate page this state exists
    to send."""
    assert "TimeoutSeconds" not in states["PublishParityDegraded"]["Parameters"]


def test_publish_parity_degraded_proceeds_to_compare_gate(states):
    """alpha-engine-config#6030: the branch-degraded page proceeds to the
    COMPARE gate (§2.3a — the join still runs and emits verdict UNKNOWN),
    while the compare-degraded page proceeds to the Evaluator gate."""
    st = states["PublishParityDegraded"]
    assert st["Next"] == "CheckSkipPitParityCompare"
    (catch,) = st["Catch"]
    assert catch["Next"] == "CheckSkipPitParityCompare"

    st = states["PublishParityCompareDegraded"]
    assert st["Next"] == "CheckSkipEvaluator"
    (catch,) = st["Catch"]
    assert catch["Next"] == "CheckSkipEvaluator"


# ---------------------------------------------------------------------------
# The hard invariant: no path with parity_degraded=true reaches plain
# NotifyComplete, for every combination of the four degraded flags.
# ---------------------------------------------------------------------------


_FLAGS = ("gate_degraded", "health_check_degraded", "report_card_degraded", "parity_degraded")
_FLAG_COMBOS = [dict(zip(_FLAGS, bits)) for bits in itertools.product([True, False], repeat=4)]


@pytest.mark.parametrize("combo", _FLAG_COMBOS, ids=lambda c: (
    ("g" if c["gate_degraded"] else "-")
    + ("h" if c["health_check_degraded"] else "-")
    + ("r" if c["report_card_degraded"] else "-")
    + ("p" if c["parity_degraded"] else "-")
))
def test_parity_degraded_never_reaches_plain_notify_complete(states, combo):
    # A Choice payload only carries the flags that are actually set — an
    # absent flag is IsPresent-guarded, not present-as-false.
    payload = {k: True for k, v in combo.items() if v}
    target = notify_target(states, payload)
    if combo["parity_degraded"]:
        assert target != "NotifyComplete", (
            f"payload {payload} reached plain NotifyComplete — a Parity "
            "degradation must always surface in the terminal notification "
            "(weekly-sf-policy.md §2.3 / §2.3a: absence of a verdict must "
            "never be read as a clean pass)"
        )
        assert target in ("NotifyCompleteParityDegraded", "NotifyCompleteMultipleDegraded")
        subject = states[target]["Parameters"]["Subject"]
        message = states[target]["Parameters"]["Message"]
        assert "parity" in subject.lower() or "parity" in message.lower(), (
            f"{target}'s Subject/Message must name parity as degraded, by name"
        )
    else:
        assert target not in ("NotifyCompleteParityDegraded",)
        # A non-parity-only combo must not accidentally claim parity in the
        # Multiple bucket's routing — the OTHER family's own test module
        # (test_sf_report_card_degraded_wiring.py etc.) covers non-parity
        # correctness; this just confirms parity's absence doesn't force
        # the Multiple bucket when nothing else is set either.
        if not any(combo.values()):
            assert target == "NotifyComplete"


# ---------------------------------------------------------------------------
# NotifyCompleteParityDegraded mirrors the established config#1819 shape.
# ---------------------------------------------------------------------------


def test_notify_complete_parity_degraded_mirrors_config_1819_shape(states):
    st = states["NotifyCompleteParityDegraded"]
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
    assert_completion_notifier_chain(states, "NotifyCompleteParityDegraded")
    (catch,) = st["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "NotifyCompleteDegraded"


def test_parity_only_subject_names_parity(states):
    subject = states["NotifyCompleteParityDegraded"]["Parameters"]["Subject"]
    assert "parity" in subject.lower()
