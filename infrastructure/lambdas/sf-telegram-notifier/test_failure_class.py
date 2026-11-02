"""failure_class: every failure alert says whether anything retries it (Brian, 2026-11-02)."""

from __future__ import annotations

import pytest

from failure_class import CREDENTIAL, GUARD, TRANSIENT, UNKNOWN, classify_failure

# The real 2026-11-02 preopen refusal (execution 2dc0bf2a), trimmed.
STALE_CHAMPION_2026_11_02 = (
    "[morning-planner] SSM command f1480f35 terminal status Failed (rc=1): Failed | "
    "stderr: ssm_log_capture: ERROR: [executor] failed (rc=1) — "
    "executor.champion.StaleChampionFeedError: scanner_predictor_direct champion cohort "
    "is stale: prediction_date=2026-10-23 is 10 calendar day(s) before run_date=2026-11-02 "
    "(max allowed 8). A stale champion feed must not trade silently."
)


@pytest.mark.parametrize(
    "cause, kind",
    [
        (STALE_CHAMPION_2026_11_02, GUARD),
        ("States.TaskFailed: MarketHoursBlocked", GUARD),
        ("DeployDriftGate: executor sha drifted", GUARD),
        ("remote: Write access to repository not granted. fatal: ... 403", CREDENTIAL),
        ("EODReconcile state failed: NoCredentialsError", CREDENTIAL),
        ("An error occurred (ThrottlingException): Rate exceeded", TRANSIENT),
        ("InvalidInstanceId: Instances not in a valid state for account", TRANSIENT),
        ("ssm status DeliveryTimedOut", TRANSIENT),
        ("connect: Connection refused", UNKNOWN),
        ("", UNKNOWN),
        (None, UNKNOWN),
    ],
)
def test_classifies(cause, kind):
    assert classify_failure(cause).kind == kind


def test_guard_wins_over_a_transient_word_in_the_same_text():
    cause = "PollTimeout while waiting; StaleChampionFeedError: cohort is stale"
    assert classify_failure(cause).kind == GUARD


def test_every_line_starts_with_auto_fix():
    for cause in (STALE_CHAMPION_2026_11_02, "403", "Throttling", "???"):
        assert classify_failure(cause).auto_fix_line.startswith("Auto-fix: ")


def test_guard_line_says_a_retry_cannot_help():
    line = classify_failure(STALE_CHAMPION_2026_11_02).auto_fix_line
    assert "retry gives the same answer" in line
