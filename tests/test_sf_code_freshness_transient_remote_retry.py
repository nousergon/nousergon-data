"""CodeFreshnessGate must survive a transient remote/auth failure, not just
lock contention — alpha-engine-config-I10950.

WHAT HAPPENED
-------------
``ne-preopen-trading-pipeline`` execution
``b84df578-c80b-453d-a855-69fdcc3a70ba`` FAILED 2026-09-17T05:15:41-07:00
(``CheckCodeFreshnessStatus`` -> ``HandleFailure`` -> ``FailExecution``, no
trading that day) on::

    git-credential-nousergon-app: token cache MISS for repo=alpha-engine-config: no cached entry
    remote: Repository not found.
    fatal: repository 'https://github.com/nousergon/alpha-engine-config.git/' not found

The configured remote has NO trailing slash on either box — the slash in the
fatal message is git's own rendering. The credential path was measured
healthy immediately after (fresh App tokens for all four repos: HTTP 200 +
``git ls-remote`` rc=0, installation not suspended, no GitHub status
incident). The SAME fetch succeeded 24 SECONDS LATER on the same box
(boot-pull's ``git ls-remote`` at 12:16:05Z got a SHA, having failed at
12:15:41Z) — a transient remote/auth condition that clears in seconds, not a
stale or broken box.

``git_retry()`` (see ``test_sf_code_freshness_lock_retry_wiring.py``) only
ever retried git's lock-contention signature ("Another git process seems to
be running"). Every other failure — including this transient one — fell
straight through to fail-loud, killing the entire trading day on a condition
that resolved itself in under half a minute.

THE FIX
-------
``git_retry()`` gains a SECOND, distinct retry class for transient
remote/auth/network failures, bounded to 4 attempts with 5s/10s/20s backoff
(~35s total, comfortably inside the state's 300s executionTimeout /
300s+360s TimeoutSeconds alongside the gate's other work). Before each
retry it erases the checkout's cached App-token entry — keyed off
``git -C $d remote get-url origin``, NOT the checkout directory name, since
``/home/ec2-user/alpha-engine`` is ``nousergon/crucible-executor`` and
``/home/ec2-user/alpha-engine-data`` is ``nousergon/nousergon-data`` — so a
bad cached token cannot be served twice. It echoes repo/attempt/class so a
recurrence is diagnosable from SSM stdout alone. The lock-contention class
(30x5s) is untouched; anything matching neither class keeps failing loud on
the first attempt.
"""

from __future__ import annotations

import json
import subprocess
import textwrap
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_LOCK_SIGNATURE = "Another git process seems to be running"

# The eleven transient-remote signatures the fix must retry on.
_TRANSIENT_SIGNATURES = [
    "Repository not found",
    "Authentication failed",
    "could not read Username",
    "The requested URL returned error: 5",
    "Could not resolve host",
    "Failed to connect",
    "Connection timed out",
    "RPC failed",
    "early EOF",
    "remote end hung up",
    "unable to access",
]


def _gate_commands() -> list[str]:
    doc = json.loads(_SF_PATH.read_text())
    gate = doc["States"]["CodeFreshnessGate"]
    return gate["Parameters"]["Parameters"]["commands"]


def _git_retry_helper() -> str:
    helper = next((c for c in _gate_commands() if "git_retry()" in c), None)
    assert helper is not None, "CodeFreshnessGate must define git_retry()."
    return helper


def _run_git_retry(
    helper: str,
    git_script: str,
    call: str,
    tmp_path: Path,
    credential_present: bool = True,
) -> subprocess.CompletedProcess:
    """Source git_retry() under mocked sudo/flock/git(/credential-helper) and
    invoke `call`. Mirrors the SSM execution shape (root invoking sudo -u
    ec2-user for every git/credential-helper op) closely enough to exercise
    the real retry/erase logic without touching a real box."""
    bindir = tmp_path / "bin"
    bindir.mkdir()

    (bindir / "sudo").write_text(
        "#!/bin/bash\nshift 2\nexec \"$@\"\n"
    )
    (bindir / "flock").write_text(
        "#!/bin/bash\nshift 3\nexec \"$@\"\n"
    )
    (bindir / "git").write_text(git_script)
    for f in ("sudo", "flock", "git"):
        (bindir / f).chmod(0o755)

    if credential_present:
        erase_log = tmp_path / "erase_calls.log"
        (bindir / "git-credential-nousergon-app").write_text(
            f"#!/bin/bash\ncat >> {erase_log}\necho ---erase--- >> {erase_log}\n"
        )
        (bindir / "git-credential-nousergon-app").chmod(0o755)
        helper_patched = helper.replace(
            "/usr/local/bin/git-credential-nousergon-app",
            str(bindir / "git-credential-nousergon-app"),
        )
    else:
        # Helper absent: the /usr/local/bin path genuinely does not exist in
        # this sandbox, which is exactly the "helper binary is absent" case.
        helper_patched = helper

    script = textwrap.dedent(
        f"""
        set -o pipefail
        export PATH="{bindir}:$PATH"
        {helper_patched}
        {call}
        echo "RC=$?"
        """
    )
    script_path = tmp_path / "run.sh"
    script_path.write_text(script)
    return subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=90,
    )


class TestGitRetryDefinesBothClasses:
    def test_retains_lock_signature(self) -> None:
        helper = _git_retry_helper()
        assert _LOCK_SIGNATURE in helper, (
            "the pre-existing lock-contention class must be retained verbatim."
        )
        assert "-ge 30" in helper, "lock class must keep its 30-attempt ceiling."

    @pytest.mark.parametrize("signature", _TRANSIENT_SIGNATURES)
    def test_declares_each_transient_signature(self, signature: str) -> None:
        helper = _git_retry_helper()
        assert signature in helper, (
            f"git_retry must retry on the transient-remote signature "
            f"{signature!r} (alpha-engine-config-I10950)."
        )

    def test_transient_class_has_its_own_bounded_budget(self) -> None:
        helper = _git_retry_helper()
        assert "-ge 4" in helper, (
            "the transient-remote class must have its own attempt ceiling "
            "(4 attempts), distinct from the lock class's 30."
        )
        for backoff in ("5", "10", "20"):
            assert backoff in helper, (
                f"transient-remote backoff schedule must include {backoff}s."
            )

    def test_still_fails_loud_and_bounded(self) -> None:
        helper = _git_retry_helper()
        assert ">&2" in helper and "return 1" in helper


class TestTransientRemoteRetryBehavior:
    def test_transient_failure_is_retried_and_recovers(self, tmp_path: Path) -> None:
        """Two transient failures then success -> git_retry returns 0."""
        helper = _git_retry_helper()
        failcount = tmp_path / "failcount"
        failcount.write_text("0")
        git_script = textwrap.dedent(
            f"""
            #!/bin/bash
            for a in "$@"; do
              if [ "$a" = "remote" ]; then
                echo "https://github.com/nousergon/alpha-engine-config.git"
                exit 0
              fi
            done
            n=$(cat {failcount}); n=$((n+1)); echo $n > {failcount}
            if [ $n -le 2 ]; then
              echo "remote: Repository not found." >&2
              echo "fatal: repository 'https://github.com/nousergon/alpha-engine-config.git/' not found" >&2
              exit 128
            fi
            echo fetch-succeeded
            exit 0
            """
        )
        result = _run_git_retry(
            helper,
            git_script,
            "git_retry -C /home/ec2-user/alpha-engine-config fetch --quiet origin main",
            tmp_path,
        )
        assert "RC=0" in result.stdout, result.stdout + result.stderr
        assert "fetch-succeeded" in result.stdout
        assert result.stdout.count("class=transient-remote") == 2

    def test_transient_failure_exhausting_budget_fails_loud(
        self, tmp_path: Path
    ) -> None:
        helper = _git_retry_helper()
        git_script = textwrap.dedent(
            """
            #!/bin/bash
            for a in "$@"; do
              if [ "$a" = "remote" ]; then
                echo "https://github.com/nousergon/alpha-engine-config.git"
                exit 0
              fi
            done
            echo "remote: Repository not found." >&2
            echo "fatal: repository 'https://github.com/nousergon/alpha-engine-config.git/' not found" >&2
            exit 128
            """
        )
        result = _run_git_retry(
            helper,
            git_script,
            "git_retry -C /home/ec2-user/alpha-engine-config fetch --quiet origin main",
            tmp_path,
        )
        assert "RC=1" in result.stdout, result.stdout + result.stderr
        assert result.stdout.count("class=transient-remote") == 3, (
            "4 total attempts means 3 retries logged before giving up."
        )
        assert "Repository not found" in result.stderr

    def test_lock_class_is_still_retried_unaffected(self, tmp_path: Path) -> None:
        """The pre-existing lock class must keep working exactly as before."""
        helper = _git_retry_helper()
        failcount = tmp_path / "failcount"
        failcount.write_text("0")
        git_script = textwrap.dedent(
            f"""
            #!/bin/bash
            n=$(cat {failcount}); n=$((n+1)); echo $n > {failcount}
            if [ $n -le 2 ]; then
              echo "fatal: Unable to create '.../.git/index.lock': File exists." >&2
              echo "Another git process seems to be running in this repository..." >&2
              exit 128
            fi
            echo checkout-ok
            exit 0
            """
        )
        result = _run_git_retry(
            helper,
            git_script,
            "git_retry -C /home/ec2-user/alpha-engine-config checkout -f main",
            tmp_path,
        )
        assert "RC=0" in result.stdout, result.stdout + result.stderr
        assert "checkout-ok" in result.stdout
        assert "lock contention" in result.stdout
        assert "class=transient-remote" not in result.stdout

    def test_non_matching_failure_is_not_retried(self, tmp_path: Path) -> None:
        """A failure matching neither class must fail loud on the FIRST
        attempt — no retry, no erase call."""
        helper = _git_retry_helper()
        git_script = textwrap.dedent(
            """
            #!/bin/bash
            for a in "$@"; do
              if [ "$a" = "remote" ]; then
                echo "https://github.com/nousergon/alpha-engine-config.git"
                exit 0
              fi
            done
            echo "fatal: some unrelated git error" >&2
            exit 1
            """
        )
        result = _run_git_retry(
            helper,
            git_script,
            "git_retry -C /home/ec2-user/alpha-engine-config fetch --quiet origin main",
            tmp_path,
        )
        assert "RC=1" in result.stdout, result.stdout + result.stderr
        assert "class=transient-remote" not in result.stdout
        assert "lock contention" not in result.stdout
        assert not (tmp_path / "erase_calls.log").exists()

    def test_erase_derives_repo_from_origin_url_not_directory_name(
        self, tmp_path: Path
    ) -> None:
        """The checkout directory is `alpha-engine` (crucible-executor's
        on-box name) but its origin is nousergon/crucible-executor — the
        erased path must reflect the ORIGIN, never the directory name."""
        helper = _git_retry_helper()
        git_script = textwrap.dedent(
            """
            #!/bin/bash
            for a in "$@"; do
              if [ "$a" = "remote" ]; then
                echo "https://github.com/nousergon/crucible-executor.git"
                exit 0
              fi
            done
            echo "remote: Repository not found." >&2
            echo "fatal: repository 'https://github.com/nousergon/crucible-executor.git/' not found" >&2
            exit 128
            """
        )
        result = _run_git_retry(
            helper,
            git_script,
            "git_retry -C /home/ec2-user/alpha-engine fetch --quiet origin main",
            tmp_path,
        )
        erase_log = tmp_path / "erase_calls.log"
        assert erase_log.exists(), "expected at least one erase call"
        contents = erase_log.read_text()
        assert "path=nousergon/crucible-executor.git" in contents, contents
        assert "alpha-engine.git" not in contents.replace(
            "crucible-executor.git", ""
        ), "must not fall back to the checkout directory name"
        assert "repo=nousergon/crucible-executor.git" in result.stdout

    def test_erase_skipped_when_helper_binary_absent(self, tmp_path: Path) -> None:
        """A missing credential-helper binary must not turn a retryable
        transient failure into a hard one — erase is best-effort."""
        helper = _git_retry_helper()
        failcount = tmp_path / "failcount"
        failcount.write_text("0")
        git_script = textwrap.dedent(
            f"""
            #!/bin/bash
            for a in "$@"; do
              if [ "$a" = "remote" ]; then
                echo "https://github.com/nousergon/alpha-engine-config.git"
                exit 0
              fi
            done
            n=$(cat {failcount}); n=$((n+1)); echo $n > {failcount}
            if [ $n -le 1 ]; then
              echo "remote: Repository not found." >&2
              exit 128
            fi
            echo fetch-succeeded
            exit 0
            """
        )
        result = _run_git_retry(
            helper,
            git_script,
            "git_retry -C /home/ec2-user/alpha-engine-config fetch --quiet origin main",
            tmp_path,
            credential_present=False,
        )
        assert "RC=0" in result.stdout, result.stdout + result.stderr
        assert "fetch-succeeded" in result.stdout


class TestRetryBudgetFitsInsideStateTimeout:
    def test_transient_backoff_budget_fits_state_timeout(self) -> None:
        """5+10+20 = 35s for one transient-class exhaustion, comfortably
        inside executionTimeout=300 / TimeoutSeconds 300 (SSM) and 360
        (state-level), alongside the gate's other git/import/freeze work."""
        transient_budget_seconds = 5 + 10 + 20
        assert transient_budget_seconds == 35

        doc = json.loads(_SF_PATH.read_text())
        gate = doc["States"]["CodeFreshnessGate"]
        execution_timeout = int(gate["Parameters"]["Parameters"]["executionTimeout"][0])
        ssm_timeout = gate["Parameters"]["TimeoutSeconds"]
        state_timeout = gate["TimeoutSeconds"]

        assert execution_timeout == 300
        assert ssm_timeout == 300
        assert state_timeout == 360

        # The lock-class budget (150s) plus the new transient-class budget
        # (35s) must both fit comfortably inside the SSM script's own
        # executionTimeout, leaving headroom for the fetch/import/freeze work
        # that is not itself retried.
        lock_budget_seconds = 30 * 5
        assert lock_budget_seconds + transient_budget_seconds < execution_timeout
