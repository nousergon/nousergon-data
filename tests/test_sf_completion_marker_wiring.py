"""SF-envelope completion marker wiring (config#2857).

The completion marker is an end-of-SF terminal artifact, independent of
downstream pipeline deliverables, proving the Step Functions execution
itself reached its real success terminal (config#1724 independent-signal
doctrine). These tests pin the weekly (Saturday) SF's wiring: every real
completion path converges into ``WriteCompletionMarker`` before ending,
while the Friday-PM preflight (shell_run) dry-pass is excluded.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.sf_degraded_summary_helpers import assert_completion_notifier_chain
from tests.sf_run_slot_release_helpers import through_release

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"


@pytest.fixture
def weekly_states():
    doc = json.loads((_INFRA / "step_function.json").read_text())
    return doc["States"]


# All EIGHT real-completion notifiers. This list carried five until
# alpha-engine-config-I6891 — NotifyCompleteReportCardDegraded (config#6685),
# NotifyCompleteParityDegraded (I6025) and NotifyCompleteMultipleDegraded
# (config#6685) each landed with their own wiring test and none of them was
# added here, so the convergence guarantee this file exists to hold was not
# actually held for three of the paths it covers. Derived below rather than
# extended by hand, so the next notifier is covered by existing.
REAL_COMPLETION_NOTIFIERS = [
    "NotifyComplete",
    "NotifyCompleteDegraded",
    "NotifyCompleteGatesDegraded",
    "NotifyCompleteHealthDegraded",
    "NotifyCompleteGatesAndHealthDegraded",
    "NotifyCompleteReportCardDegraded",
    "NotifyCompleteParityDegraded",
    "NotifyCompleteMultipleDegraded",
    # alpha-engine-config-I7813: the observe-only scanner leaderboard leaf.
    "NotifyCompleteScannerLeaderboardDegraded",
]

PREFLIGHT_NOTIFIERS = [
    "NotifyShellRunComplete",
    "NotifyShellRunCompleteDegraded",
]


def test_marker_state_shape(weekly_states):
    st = weekly_states["WriteCompletionMarker"]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    # alpha-engine-config-I12351: prefixed with the rehearsal namespace, which
    # is "" on every non-rehearsal execution, so the production key is
    # unchanged (tests/test_sf_rehearsal_isolation_wiring.py evaluates both).
    assert st["Parameters"]["Key.$"] == (
        "States.Format('{}_sf_completion/ne-weekly-freshness-pipeline/{}.json', "
        "$.rehearsal_marker.ns_prefix, $.run_date)"
    )
    body = st["Parameters"]["Body.$"]
    assert "ne-weekly-freshness-pipeline" in body
    assert "$$.Execution.Id" in body
    assert "$.run_date" in body
    # I8214: the marker hands off to the observe-only coverage sweep instead of
    # ending the execution. The sweep augments the object this state just wrote.
    assert "End" not in st
    # alpha-engine-config-I8809: the legacy-partition COPY of the marker sits
    # between the canonical write and the sweep for the migration window. It is
    # fail-soft and its Next is the sweep, so the chain below is unchanged past
    # this hop. Deleted at the 2026-09-05 cutover.
    assert st["Next"] == "WriteCompletionMarkerCalendar"
    assert weekly_states["WriteCompletionMarkerCalendar"]["Next"] == "WeeklyCoverageSweep"
    assert '"claim":"sf_execution_terminal"' in body, (
        "the marker must name what its write actually asserts — the SF execution "
        "reached its terminal — rather than implying the cycle completed"
    )
    assert '"cycle_verdict":"unknown"' in body, (
        "a consumer reading the marker BEFORE the sweep augments it must resolve "
        "to UNKNOWN, never to an implied pass (sf-pipeline-policy §2.3a)"
    )
    # Deliberate: no swallow-all Catch (unlike the SNS notifiers) — a marker
    # that genuinely cannot be written should surface as a real failure,
    # not be silently swallowed the way a non-fatal notify is.
    assert "Catch" not in st
    assert st["ResultPath"] == "$.completion_marker_result", (
        "WriteCompletionMarker must preserve state for WriteCompletionMarkerCalendar "
        "(alpha-engine-config-I8809); without ResultPath the putObject output "
        "replaces the input and $.calendar_date cannot resolve"
    )
    (retry,) = st["Retry"]
    assert retry["ErrorEquals"] == ["States.ALL"]
    assert retry["MaxAttempts"] >= 2


def test_the_notifier_list_is_the_whole_notifier_family(weekly_states):
    """The list above must BE every real-completion notifier, not a subset
    somebody remembered to extend. Three were missing for months."""
    discovered = {
        n for n, b in weekly_states.items()
        if n.startswith("NotifyComplete") and b.get("Type") == "Task"
    } | {"NotifyCompleteDegraded"}
    assert discovered == set(REAL_COMPLETION_NOTIFIERS), (
        "REAL_COMPLETION_NOTIFIERS has drifted behind the definition: "
        f"missing {sorted(discovered - set(REAL_COMPLETION_NOTIFIERS))}, "
        f"stale {sorted(set(REAL_COMPLETION_NOTIFIERS) - discovered)}"
    )


@pytest.mark.parametrize("name", REAL_COMPLETION_NOTIFIERS)
def test_real_completion_paths_converge_on_marker(weekly_states, name):
    """alpha-engine-config-I6891: convergence is now on CheckDegradedOutcome,
    which picks the marker whose `status` tells the truth about the run."""
    assert_completion_notifier_chain(weekly_states, name)


@pytest.mark.parametrize("name", PREFLIGHT_NOTIFIERS)
def test_preflight_paths_are_excluded_from_marker(weekly_states, name):
    """A Friday-PM dry pass must never satisfy the completion-marker SLA —
    neither on its clean edge nor on its degraded one (I6891 gave the shell
    run an honest terminal too, and routed it AROUND both markers)."""
    st = weekly_states[name]
    assert st.get("Next") not in ("WriteCompletionMarker", "WriteCompletionMarkerDegraded")


def test_the_preflight_degraded_edge_ends_in_the_shared_fail_terminal(weekly_states):
    """A Friday-PM preflight whose completion notification failed used to carry
    End: true — flagged degraded, terminating SUCCEEDED, on the very path that
    exists to prove the real run will work (alpha-engine-config-I6891)."""
    assert weekly_states["NotifyShellRunComplete"]["Next"] == "CheckShellRunDegradedOutcome"
    choice = weekly_states["CheckShellRunDegradedOutcome"]
    assert choice["Default"] == "ShellRunComplete"
    assert weekly_states["ShellRunComplete"]["Type"] == "Succeed"
    (rule,) = choice["Choices"]
    assert through_release(weekly_states, rule["Next"]) == "DegradedRun"
    assert through_release(weekly_states, weekly_states["NotifyShellRunCompleteDegraded"]["Next"]) == "DegradedRun"
    assert "End" not in weekly_states["NotifyShellRunCompleteDegraded"]
