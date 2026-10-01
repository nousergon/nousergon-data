"""alpha-engine-config#5569: CaptureSnapshot is the EOD pipeline's only stage
with an irreversible per-day deadline — every other stage is re-runnable, but
``trades/snapshots/{run_date}.json`` must exist before midnight or
``eod_reconcile``'s correction path (which sources state from that S3
snapshot for a past ``--date``) is permanently gone. Verified live
2026-07-29: 2026-07-27's snapshot failed with no same-day retry and no
same-day page, and is now permanently unrecoverable
(alpha-engine-config-I5325, ruled an accepted gap).

This module pins the same-day fix in ``infrastructure/step_function_eod.json``
(deliverables #1 + #2 of the issue; deliverables #3 — a pre-midnight positive
existence checkpoint needing a new scheduled Lambda/SF trigger — and #4 — the
crucible-executor ``snapshot_capturer.py``/runbook comment — are cross-repo
and OUT OF SCOPE for this file/PR, see the PR body):

  1. A bounded 1-retry same-day budget around CaptureSnapshot, mirroring the
     CheckDataSpotRetryBudget/IncrementDataSpotRetry idiom used elsewhere in
     this same file for the post-market data-spot phases.
  2. An IMMEDIATE same-day page on the FIRST CaptureSnapshot failure — before
     the retry even runs — so an operator has the whole retry window to react
     (mirrors PublishDataSpotFailureImmediate's SNS-with-own-Catch shape).
  3. A SECOND, distinctly-worded page when the retry budget is exhausted,
     marked as an irreversible-deadline failure (distinct from the ordinary
     EOD-stage HandleFailure notify — the required operator response is
     different: act now or lose the day).
  4. UNLIKE the neighboring data-spot retry idiom (which is deliberately
     fail-OPEN — a data-spot miss must not block reconcile/instance-stop),
     this retry stays fail-CLOSED: exhausting the budget still reaches
     HandleFailure -> FailExecution. No silent fallback to live IB — that
     constraint is deliberate (Phase 2 cutover) and unchanged by this fix.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_EOD = _REPO_ROOT / "infrastructure" / "step_function_eod.json"

_SNS_PUBLISH = "arn:aws:states:::sns:publish"
_HALT = {"FailExecution"}  # the true terminal — HandleFailure IS the expected destination here.


@pytest.fixture(scope="module")
def eod() -> dict:
    return json.loads(_EOD.read_text())["States"]


def _all_targets(state: dict) -> list[str]:
    t: list[str] = []
    for k in ("Next", "Default"):
        if k in state:
            t.append(state[k])
    for c in state.get("Choices", []):
        if "Next" in c:
            t.append(c["Next"])
    for c in state.get("Catch", []):
        if "Next" in c:
            t.append(c["Next"])
    return t


class TestRetryCounterInitialization:
    def test_counter_initialized_before_first_capture_attempt(self, eod):
        assert eod["CheckSkipCaptureSnapshot"]["Default"] == "InitCaptureSnapshotRetryCounter"
        counter = eod["InitCaptureSnapshotRetryCounter"]
        assert counter["Type"] == "Pass"
        assert counter["ResultPath"] == "$.capture_snapshot_retry"
        assert counter["Result"] == {"attempts": 0}
        assert counter["Next"] == "CaptureSnapshot"

    def test_operator_replay_skip_path_untouched(self, eod):
        # The pre-existing skip_capture_snapshot=true operator-replay gate
        # still bypasses CaptureSnapshot (and therefore the retry machinery)
        # entirely — this fix must not disturb that rerun path.
        # alpha-engine-config-I11269: CaptureSnapshot is the 16:00 machine's
        # last work task since the post-close split, so the bypass lands on
        # its box stop and then the degraded-outcome tail (the precondition
        # probe it used to reach moved to the reconcile machine). Still a
        # bypass of the whole retry machinery, and still gated on
        # operator-replay.
        st = eod["CheckSkipCaptureSnapshot"]
        assert st["Choices"][0]["Next"] == "StopTradingInstance"
        assert st["Default"] == "InitCaptureSnapshotRetryCounter"
        retry_states = {
            "InitCaptureSnapshotRetryCounter", "CaptureSnapshot", "WaitForCaptureSnapshot",
            "CheckSnapshotStatus", "CheckCaptureSnapshotRetryBudget",
            "IncrementCaptureSnapshotRetry", "PageCaptureSnapshotFailureImmediate",
        }
        assert st["Choices"][0]["Next"] not in retry_states


class TestThreeFailureModesFunnelIntoRetryBudget:
    """CaptureSnapshot can fail three ways: the sendCommand launch itself
    (Catch), the SSM poll (WaitForCaptureSnapshot's Catch), or a non-Success
    terminal SSM status (SnapshotStatusError). All three must route into the
    same bounded retry budget, not straight to HandleFailure."""

    def test_capture_snapshot_catch_routes_to_retry_budget(self, eod):
        catch = eod["CaptureSnapshot"]["Catch"]
        assert len(catch) == 1
        assert catch[0]["ErrorEquals"] == ["States.ALL"]
        assert catch[0]["Next"] == "CheckCaptureSnapshotRetryBudget"
        assert catch[0]["ResultPath"] == "$.error"

    def test_wait_for_capture_snapshot_catch_routes_to_retry_budget(self, eod):
        catch = eod["WaitForCaptureSnapshot"]["Catch"]
        assert len(catch) == 1
        assert catch[0]["ErrorEquals"] == ["States.ALL"]
        assert catch[0]["Next"] == "CheckCaptureSnapshotRetryBudget"
        assert catch[0]["ResultPath"] == "$.error"

    def test_snapshot_status_error_routes_to_retry_budget(self, eod):
        st = eod["SnapshotStatusError"]
        assert st["Type"] == "Pass"
        assert st["ResultPath"] == "$.error"
        assert st["Next"] == "CheckCaptureSnapshotRetryBudget"

    def test_check_snapshot_status_default_unchanged(self, eod):
        # CheckSnapshotStatus's own Default still funnels into
        # SnapshotStatusError first (unchanged) — SnapshotStatusError is the
        # one that now redirects onward into the retry budget.
        assert eod["CheckSnapshotStatus"]["Default"] == "SnapshotStatusError"


class TestBoundedOneRetry:
    def test_retry_budget_is_one_retry_then_exhausted(self, eod):
        st = eod["CheckCaptureSnapshotRetryBudget"]
        assert st["Type"] == "Choice"
        assert len(st["Choices"]) == 1
        cond = st["Choices"][0]
        assert cond["Variable"] == "$.capture_snapshot_retry.attempts"
        assert cond["NumericLessThan"] == 1
        assert cond["Next"] == "PageCaptureSnapshotFailureImmediate"
        assert st["Default"] == "CaptureSnapshotRetryExhausted"

    def test_increment_relaunches_capture_snapshot(self, eod):
        inc = eod["IncrementCaptureSnapshotRetry"]
        assert inc["Type"] == "Pass"
        assert inc["ResultPath"] == "$.capture_snapshot_retry"
        assert inc["Parameters"]["attempts.$"] == "States.MathAdd($.capture_snapshot_retry.attempts, 1)"
        assert inc["Next"] == "CaptureSnapshot"

    def test_immediate_page_leads_into_increment(self, eod):
        pub = eod["PageCaptureSnapshotFailureImmediate"]
        assert pub["Next"] == "IncrementCaptureSnapshotRetry"
        for c in pub.get("Catch", []):
            assert c["Next"] == "IncrementCaptureSnapshotRetry"


class TestImmediatePageOnFirstFailure:
    """Deliverable #2 (immediate half): page same-day BEFORE the retry runs,
    mirroring PublishDataSpotFailureImmediate's SNS-with-own-Catch shape."""

    def test_is_sns_publish_with_timeout_and_own_catch(self, eod):
        st = eod["PageCaptureSnapshotFailureImmediate"]
        assert st["Type"] == "Task"
        assert st["Resource"] == _SNS_PUBLISH
        assert "TimeoutSeconds" in st
        assert st.get("Catch"), "must carry its own Catch — an SNS-side failure cannot block the retry"
        for c in st["Catch"]:
            assert c["ErrorEquals"] == ["States.ALL"]

    def test_message_names_the_irreversibility_and_precedent(self, eod):
        msg = eod["PageCaptureSnapshotFailureImmediate"]["Parameters"]["Message.$"]
        assert "alpha-engine-config-I5325" in msg
        assert "run_date" not in msg or "$.run_date" in msg  # threaded, not hardcoded
        assert "$.run_date" in msg
        assert "$.error" in msg

    def test_subject_distinct_from_ordinary_and_irreversible_pages(self, eod):
        immediate_subject = eod["PageCaptureSnapshotFailureImmediate"]["Parameters"]["Subject"]
        handle_failure_subject = eod["HandleFailure"]["Parameters"]["Subject"]
        irreversible_subject = eod["PageCaptureSnapshotIrreversibleFailure"]["Parameters"]["Subject"]
        assert len({immediate_subject, handle_failure_subject, irreversible_subject}) == 3
        assert 0 < len(immediate_subject) <= 100
        assert "\n" not in immediate_subject


def _through_normalizers(states: dict, name: str) -> str:
    """Resolve a transition target past any pure-Pass normalizer in front of it.

    alpha-engine-config#5950 inserted floors that sit between an edge and its
    real destination, flooring the optional fields the destination dereferences.
    A Pass has no Choices, so it cannot change WHICH destination is reached —
    walking through it keeps these guards asserting the destination rather than
    widening them to accept any Pass, which is how a wrong destination would get
    in behind one.
    """
    seen = set()
    while (
        name in states
        and states[name].get("Type") == "Pass"
        and "Next" in states[name]
        and name not in seen
    ):
        seen.add(name)
        name = states[name]["Next"]
    return name


class TestExhaustedRetryIsIrreversibleDeadline:
    """Deliverable #2 (terminal half): a SECOND consecutive CaptureSnapshot
    failure is the true irreversible-deadline moment and pages distinctly."""

    def test_exhausted_normalizes_distinct_error_payload(self, eod):
        st = eod["CaptureSnapshotRetryExhausted"]
        assert st["Type"] == "Pass"
        assert st["Parameters"]["source"] == "capture_snapshot_retry_exhausted"
        assert st["Parameters"]["run_date.$"] == "$.run_date"
        assert st["Parameters"]["attempts.$"] == "$.capture_snapshot_retry.attempts"
        assert st["Parameters"]["last_error.$"] == "$.error"
        assert st["ResultPath"] == "$.error"
        assert st["Next"] == "PageCaptureSnapshotIrreversibleFailure"

    def test_irreversible_page_is_sns_publish_with_timeout_and_own_catch(self, eod):
        st = eod["PageCaptureSnapshotIrreversibleFailure"]
        assert st["Type"] == "Task"
        assert st["Resource"] == _SNS_PUBLISH
        assert "TimeoutSeconds" in st
        assert st.get("Catch"), "must carry its own Catch — an SNS-side failure cannot block the hard-fail path"
        for c in st["Catch"]:
            assert c["ErrorEquals"] == ["States.ALL"]
            # config#5950: this Catch sets no ResultPath, so it reached
            # HandleFailure without the $.error that HandleFailure formats — the
            # SNS-side failure path died in States.Runtime instead of reporting.
            # It now passes through the floor; the destination is unchanged.
            assert _through_normalizers(eod, c["Next"]) == "HandleFailure"

    def test_irreversible_message_marks_the_deadline(self, eod):
        subject = eod["PageCaptureSnapshotIrreversibleFailure"]["Parameters"]["Subject"]
        msg = eod["PageCaptureSnapshotIrreversibleFailure"]["Parameters"]["Message.$"]
        assert "IRREVERSIBLE" in subject.upper()
        assert "midnight" in subject.lower() or "midnight" in msg.lower()
        assert "alpha-engine-config-I5325" in msg
        assert "$.error" in msg

    def test_irreversible_page_reaches_handle_failure_not_fail_open(self, eod):
        # config#5950: NormalizeEODFailureContext now floors $.error on this
        # edge, because HandleFailure dereferences it and this edge's ResultPath
        # writes $.capture_snapshot_irreversible_notify instead. The destination
        # is unchanged; resolve through the Pass rather than accepting one.
        # UNLIKE the neighboring data-spot retry idiom (deliberately fail-open
        # to ExtractDataSpotError/CheckSkipCaptureSnapshot), CaptureSnapshot's
        # exhausted retry must still hard-fail into HandleFailure -> FailExecution.
        assert _through_normalizers(
            eod, eod["PageCaptureSnapshotIrreversibleFailure"]["Next"]
        ) == "HandleFailure"


class TestNoSilentFallbackPreserved:
    """The pre-existing hard-fail constraint (no fallback to live IB) survives
    the retry insertion — the retry gives one more chance at the SAME durable
    S3-snapshot path, it never routes around CaptureSnapshot to a different
    data source."""

    def test_retry_relaunches_the_same_capture_snapshot_state(self, eod):
        assert eod["IncrementCaptureSnapshotRetry"]["Next"] == "CaptureSnapshot"

    def test_all_capture_snapshot_retry_states_eventually_reach_handle_failure_or_success(self, eod):
        # Exhaustive: every terminal branch of the new retry/page machinery
        # either loops back to CaptureSnapshot (retry) or lands on
        # HandleFailure (hard fail) — never a bare Fail/silent skip, and
        # never the reconcile-precondition success path directly (that only
        # happens via CheckSnapshotStatus's own Success branch, untouched).
        new_states = [
            "InitCaptureSnapshotRetryCounter",
            "CheckCaptureSnapshotRetryBudget",
            "PageCaptureSnapshotFailureImmediate",
            "IncrementCaptureSnapshotRetry",
            "CaptureSnapshotRetryExhausted",
            "PageCaptureSnapshotIrreversibleFailure",
        ]
        allowed = {"CaptureSnapshot", "IncrementCaptureSnapshotRetry",
                   "PageCaptureSnapshotFailureImmediate", "CaptureSnapshotRetryExhausted",
                   "PageCaptureSnapshotIrreversibleFailure", "HandleFailure"}
        for name in new_states:
            for raw in _all_targets(eod[name]):
                tgt = _through_normalizers(eod, raw)
                assert tgt in allowed, (
                    f"{name} -> {raw}"
                    + (f" (-> {tgt})" if tgt != raw else "")
                    + " escapes the retry/page machinery unexpectedly"
                )


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-q"]))
