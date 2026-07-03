"""Pins the mid-run spot-interruption retry in
infrastructure/spot_data_weekly.sh.

Origin: 2026-05-30 Saturday SF DataPhase1 failure. The nested data spot
(i-02e498e018441751f, c5.large/us-east-1a) was reclaimed by AWS *mid-
workload* with spot-request status `instance-terminated-no-capacity`.
The lib launcher (krepis.ec2_spot) rotates instance_type × subnet on
*acquisition* capacity errors, but nothing relaunched after a *mid-run*
reclamation — the workload SSM command returned ResponseCode -1, the
orchestrator exited 1, and the entire weekly pipeline failed.

The original fix (PR #349) added an EXIT trap (`on_exit`) with an INLINE
classifier (`_spot_failure_reason`) reading `describe-spot-instance-
requests` + the instance StateReason directly. alpha-engine-config#883
observed that this file was the reference implementation the backtester's
and predictor's launchers copied — and each grew a subtly divergent
counter/threshold convention. nousergon-lib PR #133 (v0.65.0+, module now
lives in ``krepis.ec2_spot``) lifted the classify+decide DECISION into a
shared chokepoint (`python -m krepis.ec2_spot relaunch-decision`); this
repo's alpha-engine-predictor sibling adopted it in PR #308. This is
launcher 3/3 (data): `_spot_failure_reason`'s mid-run branch now calls the
lib chokepoint instead of re-inlining the AWS describe-calls, while the
launch-time-capacity-exhaustion bypass (ec2_spot rc 64 — a feature this
launcher has that the siblings don't) is unchanged.

These are static greps (the script only runs end-to-end on a real spot)
mirroring tests/test_spot_data_weekly_run_modes.py — they catch the
regression class where the retry is removed, un-gated from the
interruption-classification, made to retry genuine workload errors, or
where the lib chokepoint is silently swapped back out for a re-inlined
AWS describe-calls classifier.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"


@pytest.fixture(scope="module")
def text() -> str:
    return _SCRIPT.read_text()


def test_script_syntactically_valid():
    """``bash -n`` must accept the script — the lib-chokepoint call is a
    command substitution + parameter-expansion flag list that must parse
    cleanly under bash."""
    r = subprocess.run(["bash", "-n", str(_SCRIPT)], capture_output=True, text=True)
    assert r.returncode == 0, f"bash -n rejected spot_data_weekly.sh:\n{r.stderr}"


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

    def test_sf_execution_timeout_threaded_via_env(self, text):
        """#883 — the MAX_SPOT_ATTEMPTS <-> SF-executionTimeout coupling
        guard needs SF_EXECUTION_TIMEOUT threaded (empty by default; the
        lib guard is inert until an operator sets it to the actual outer
        SF state's executionTimeout)."""
        assert 'SF_EXECUTION_TIMEOUT="${SF_EXECUTION_TIMEOUT:-}"' in text, (
            "SF_EXECUTION_TIMEOUT must be env-overridable (default empty) "
            "so the lib's --sf-execution-timeout/--per-attempt-seconds "
            "coupling guard can be armed without editing this script."
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


class TestLibChokepointAdoption:
    """#883 — the mid-run reclaim DECISION must come from the shared lib
    chokepoint, not a re-inlined per-launcher classifier."""

    def _classifier_body(self, text: str) -> str:
        m = re.search(
            r"^_spot_failure_reason\(\)\s*\{.*?^\}", text, re.MULTILINE | re.DOTALL
        )
        assert m, "no _spot_failure_reason() function found in spot_data_weekly.sh"
        return m.group(0)

    def test_uses_lib_relaunch_decision_chokepoint(self, text):
        body = self._classifier_body(text)
        assert "krepis.ec2_spot relaunch-decision" in body, (
            "_spot_failure_reason() does not call the lib chokepoint "
            "`python -m krepis.ec2_spot relaunch-decision` — #883 lifts the "
            "mid-run reclaim classify+decide logic into the lib; the "
            "launcher must consume it, not re-inline the AWS describe-calls."
        )
        for flag in ("--instance-id", "--attempt", "--max-attempts"):
            assert flag in body, (
                f"_spot_failure_reason()'s relaunch-decision call is missing "
                f"{flag} — the lib needs the 1-based attempt + total-"
                "attempts budget to decide."
            )

    def test_uses_same_lib_python_convention_as_existing_launch_call(self, text):
        """The new relaunch-decision call must reuse the exact
        $LIB_PYTHON venv-path convention this script already uses for
        `krepis.ec2_spot launch` and `krepis.ssm_dispatcher run` — not a
        bare `python`/`python3` that resolves to system python (which
        does not carry the lib)."""
        assert (
            '"$LIB_PYTHON" -m krepis.ec2_spot relaunch-decision' in text
        ), (
            "relaunch-decision must be invoked as "
            '`"$LIB_PYTHON" -m krepis.ec2_spot relaunch-decision`, matching '
            "the launch call's convention."
        )

    def test_no_inline_reclaim_classifier(self, text):
        """The launcher must NOT re-inline the spot-reclaim classification
        (that's exactly what #883 lifted to the lib). A non-comment line
        that queries the spot-request status or instance StateReason
        directly is the divergent-copy regression the chokepoint exists
        to prevent."""
        forbidden = (
            "describe-spot-instance-requests",
            "Server.SpotInstanceTermination",
            "Server.InsufficientInstanceCapacity",
            "instance-terminated-no-capacity",
            "instance-terminated-by-price",
            "instance-terminated-capacity-oversubscribed",
        )
        offenders = []
        for i, raw in enumerate(_SCRIPT.read_text().splitlines(), start=1):
            if raw.strip().startswith("#"):
                continue
            for tok in forbidden:
                if tok in raw:
                    offenders.append((i, tok, raw.strip()))
        assert not offenders, (
            "spot_data_weekly.sh re-inlines reclaim classification instead "
            "of using the lib chokepoint:\n"
            + "\n".join(f"  line {n}: {tok} :: {ln}" for n, tok, ln in offenders)
            + "\n\nRoute through `python -m krepis.ec2_spot relaunch-decision`."
        )

    def test_launch_capacity_exhaustion_bypass_retained(self, text):
        """rc 64 (ec2_spot launch-time capacity exhaustion — no instance
        ever existed) must still bypass the lib call directly; there is
        nothing for the lib's describe-instances to classify. This is a
        launcher-specific feature (not present in the backtester/predictor
        siblings), unrelated to the mid-run reclaim classifier #883 lifts,
        and must survive the migration unchanged."""
        body = self._classifier_body(text)
        assert re.search(r'\[ "\$rc" -eq 64 \]', body), (
            "ec2_spot rc 64 (all instance_type × subnet exhausted) must "
            "still be treated as a retryable capacity interruption "
            "WITHOUT calling the lib (no instance exists to describe)."
        )

    def test_classifier_calls_lib_before_terminate(self, text):
        """Classification must happen while the instance still exists —
        BEFORE cleanup() terminates it. on_exit computes `reason` (which
        calls the lib chokepoint) before calling cleanup."""
        on_exit = text[text.index("on_exit() {"):]
        reason_at = on_exit.index("_spot_failure_reason")
        cleanup_at = on_exit.index("\n    cleanup")
        assert reason_at < cleanup_at, (
            "Failure must be classified (via the lib chokepoint) BEFORE "
            "cleanup() terminates the instance — the lib's describe-"
            "instances call requires a live instance."
        )
        decide_idx = text.index("krepis.ec2_spot relaunch-decision")
        term_idx = text.index("aws ec2 terminate-instances")
        assert decide_idx < term_idx, (
            "the lib relaunch-decision call must appear before "
            "terminate-instances in the script."
        )


class TestFailLoudOnGenuineError:
    def test_retry_gated_on_nonempty_reason(self, text):
        """The relaunch must be gated on a non-empty interruption reason —
        a genuine workload failure (empty reason, i.e. the lib classified
        it as not a confirmed reclaim) must NOT retry."""
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

    def test_retry_metric_carries_process_dimension(self, text):
        """The metric must carry Process=data-weekly to discriminate this
        launcher from the backtester/predictor siblings on the shared
        AlphaEngine namespace (unchanged by the #883 migration)."""
        assert 'dimensions "Process=data-weekly"' in text, (
            "SpotInterruptionRetry must carry Process=data-weekly."
        )
