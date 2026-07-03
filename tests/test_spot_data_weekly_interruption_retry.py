"""Pins the mid-run spot-interruption retry in
infrastructure/spot_data_weekly.sh.

Origin: 2026-05-30 Saturday SF DataPhase1 failure. The nested data spot
(i-02e498e018441751f, c5.large/us-east-1a) was reclaimed by AWS *mid-
workload* with spot-request status `instance-terminated-no-capacity`.
The lib launcher (nousergon_lib.ec2_spot) rotates instance_type ×
subnet on *acquisition* capacity errors, but nothing relaunched after a
*mid-run* reclamation — the workload SSM command returned ResponseCode
-1, the orchestrator exited 1, and the entire weekly pipeline failed.

The fix adds an EXIT trap (`on_exit`) that classifies the failure: a
CONFIRMED spot interruption (no-capacity / price / capacity-
oversubscribed reclamation, or all-combinations-exhausted launch)
relaunches a fresh spot up to MAX_SPOT_ATTEMPTS; a GENUINE workload
error is NOT retried and fails loud (blind retry would mask a real bug).

These are static greps (the script only runs end-to-end on a real spot)
mirroring tests/test_spot_data_weekly_run_modes.py — they catch the
regression class where the retry is removed, un-gated from the
interruption-classification, or made to retry genuine workload errors.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"


@pytest.fixture(scope="module")
def text() -> str:
    return _SCRIPT.read_text()


class TestRetryConfig:
    def test_max_attempts_env_overridable(self, text):
        assert 'MAX_SPOT_ATTEMPTS="${MAX_SPOT_ATTEMPTS:-2}"' in text, (
            "MAX_SPOT_ATTEMPTS must default to 2 (one relaunch) and be env-"
            "overridable; raising it requires bumping the SF executionTimeout."
        )

    def test_attempt_counter_threaded_via_env(self, text):
        assert 'SPOT_ATTEMPT="${SPOT_ATTEMPT:-1}"' in text, (
            "SPOT_ATTEMPT must be env-threaded so a re-exec knows its attempt #."
        )

    def test_orig_args_captured_before_parse(self, text):
        """ORIG_ARGS must be captured BEFORE the arg-parse while-loop's
        shifts consume the positional params, so the re-exec can replay
        the identical mode/flags."""
        cap = text.index('ORIG_ARGS=("$@")')
        parse = text.index("while [[ $# -gt 0 ]]; do")
        assert cap < parse, "ORIG_ARGS must be captured before the parse loop."


class TestTrapInstalledBeforeLaunch:
    def test_on_exit_trap_installed(self, text):
        assert "trap on_exit EXIT" in text, "on_exit must be the EXIT trap."

    def test_trap_precedes_launch(self, text):
        """The trap must be armed BEFORE the ec2_spot launch so it also
        covers an all-combinations-exhausted launch (rc 64), not only a
        mid-run reclamation."""
        trap_at = text.index("trap on_exit EXIT")
        launch_at = text.index("krepis.ec2_spot launch")
        assert trap_at < launch_at, (
            "trap on_exit EXIT must be installed before the spot launch."
        )


class TestInterruptionClassification:
    @pytest.mark.parametrize(
        "code",
        [
            "instance-terminated-no-capacity",
            "instance-terminated-by-price",
            "instance-terminated-capacity-oversubscribed",
            "instance-stopped-no-capacity",
            "marked-for-termination",
        ],
    )
    def test_spot_request_status_codes_classified(self, text, code):
        assert code in text, (
            f"spot-request status {code!r} must be classified as a retryable "
            "interruption in _spot_failure_reason."
        )

    def test_launch_exhaustion_rc64_retryable(self, text):
        assert re.search(r'\[ "\$rc" -eq 64 \]', text), (
            "ec2_spot rc 64 (all instance_type × subnet exhausted) must be "
            "treated as a retryable capacity interruption."
        )

    def test_instance_statereason_fallback(self, text):
        assert "Server.SpotInstanceTermination" in text, (
            "Instance StateReason fallback must recognize spot reclamation."
        )

    def test_classifier_queries_spot_request_before_terminate(self, text):
        """Classification must read the spot-request status; the comment +
        call order ensure it happens before cleanup() terminates."""
        assert "describe-spot-instance-requests" in text
        # on_exit computes `reason` before calling cleanup.
        on_exit = text[text.index("on_exit() {"):]
        reason_at = on_exit.index("_spot_failure_reason")
        cleanup_at = on_exit.index("\n    cleanup")
        assert reason_at < cleanup_at, (
            "Failure must be classified BEFORE cleanup() terminates the "
            "instance (the spot-request status is only queryable while it lives)."
        )


class TestFailLoudOnGenuineError:
    def test_retry_gated_on_nonempty_reason(self, text):
        """The relaunch must be gated on a non-empty interruption reason —
        a genuine workload failure (empty reason) must NOT retry."""
        assert re.search(
            r'\[ "\$rc" -ne 0 \] && \[ -n "\$reason" \] && '
            r'\[ "\$SPOT_ATTEMPT" -lt "\$MAX_SPOT_ATTEMPTS" \]',
            text,
        ), (
            "Relaunch must require rc!=0 AND a confirmed interruption reason "
            "AND attempts remaining — genuine workload errors fail loud."
        )

    def test_exhausted_attempts_fail_loud(self, text):
        assert "persisted across all $MAX_SPOT_ATTEMPTS attempt(s)" in text, (
            "When interruption persists across all attempts the script must "
            "surface a loud ERROR and propagate the non-zero exit."
        )

    def test_original_exit_code_propagated(self, text):
        on_exit = text[text.index("on_exit() {"):]
        assert 'exit "$rc"' in on_exit, (
            "on_exit must propagate the original failure code, not mask it."
        )


class TestReexecAndObservability:
    def test_reexec_preserves_args_and_increments_attempt(self, text):
        assert (
            'SPOT_ATTEMPT=$((SPOT_ATTEMPT + 1)) exec bash "$0" '
            '${ORIG_ARGS[@]+"${ORIG_ARGS[@]}"}'
            in text
        ), (
            "Relaunch must self-re-exec via `exec bash \"$0\"` with the "
            "preserved ORIG_ARGS and an incremented SPOT_ATTEMPT."
        )

    def test_trap_disarmed_before_reexec(self, text):
        """`trap - EXIT` must precede the exec so the replaced process does
        not double-run cleanup for the already-terminated instance."""
        on_exit = text[text.index("on_exit() {"):]
        disarm_at = on_exit.index("trap - EXIT")
        exec_at = on_exit.index("exec bash")
        assert disarm_at < exec_at

    def test_retry_emits_named_cloudwatch_metric(self, text):
        assert 'metric-name "SpotInterruptionRetry"' in text, (
            "Each absorbed interruption must emit the SpotInterruptionRetry "
            "CloudWatch metric — the retry is observable, never silent."
        )
