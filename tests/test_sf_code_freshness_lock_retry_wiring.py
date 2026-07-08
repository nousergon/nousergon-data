"""CodeFreshnessGate must serialize its git ops behind boot-pull.service.

2026-07-08 preopen-trading failure: the weekday SF's ``CodeFreshnessGate``
(config#1811) and the trading box's ``boot-pull.service`` (a systemd oneshot
that runs the SAME ``git fetch / checkout -f main / reset --hard origin/main``
on the same three repos at every daily boot) are two concurrent git writers.
The gate fires the instant SSM reports the instance ``Online`` — which can be
WHILE boot-pull's ``git reset --hard`` still holds
``alpha-engine-data/.git/index.lock``. The gate's ``checkout -f main`` /
``reset --hard`` then died with::

    fatal: Unable to create '.../.git/index.lock': File exists.
    Another git process seems to be running in this repository...
    exit status 128

which routed COMMAND_FAILED -> HandleFailure -> FailExecution: no orders placed.

The fix wraps the gate's mutating git ops (fetch + self-heal checkout/reset) in
a ``git_retry`` helper that retries ONLY on git's lock-contention signature,
bounded to ~150s (> boot-pull's 120s ``TimeoutStartSec``), and FAILS LOUD on any
other error or if the lock persists past the budget. This pins that contract so
a future edit cannot silently drop back to bare, race-prone git calls.
"""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

# git's stable, version-independent message for ANY *.lock contention
# (index.lock, packed-refs.lock, shallow.lock, HEAD.lock). Retrying on this
# exact phrase is what serializes the gate behind boot-pull.
_LOCK_SIGNATURE = "Another git process seems to be running"


def _gate_commands() -> list[str]:
    doc = json.loads(_SF_PATH.read_text())
    gate = doc["States"]["CodeFreshnessGate"]
    return gate["Parameters"]["Parameters"]["commands"]


def test_gate_defines_git_retry_helper() -> None:
    cmds = _gate_commands()
    joined = "\n".join(cmds)
    assert "git_retry()" in joined, (
        "CodeFreshnessGate must define a git_retry() helper that serializes its "
        "git ops behind boot-pull.service (2026-07-08 index.lock race)."
    )


def test_git_retry_matches_only_lock_contention() -> None:
    """The retry must key on git's lock signature, not blanket-retry."""
    helper = next((c for c in _gate_commands() if "git_retry()" in c), None)
    assert helper is not None
    assert _LOCK_SIGNATURE in helper, (
        "git_retry must retry ONLY on git's lock-contention signature "
        f"({_LOCK_SIGNATURE!r}) — a blanket retry would mask real git errors."
    )


def test_git_retry_fails_loud_and_is_bounded() -> None:
    """A bounded retry that returns non-zero past budget = fail-loud.

    Under the block's `set -eo pipefail`, a non-zero git_retry return aborts the
    command -> COMMAND_FAILED -> HandleFailure. The helper must (a) have a finite
    attempt ceiling and (b) return non-zero on both the non-lock and
    budget-exhausted paths. We pin the fail-loud markers rather than swallow.
    """
    helper = next((c for c in _gate_commands() if "git_retry()" in c), None)
    assert helper is not None
    # bounded: an explicit attempt ceiling is present.
    assert "-ge 30" in helper, "git_retry must be bounded (attempt ceiling)."
    # fail-loud: emits the real error to stderr and returns non-zero.
    assert ">&2" in helper and "return 1" in helper, (
        "git_retry must surface the real error (>&2) and return non-zero "
        "(fail-loud) when it gives up — never swallow."
    )


def test_self_heal_git_mutations_go_through_git_retry() -> None:
    """The checkout/reset that took the lock must not be bare git calls."""
    cmds = _gate_commands()
    self_heal = next((c for c in cmds if "SELF-HEAL" in c), None)
    assert self_heal is not None, "CodeFreshnessGate self-heal line missing."
    assert "git_retry -C $d checkout -f main" in self_heal, (
        "self-heal `checkout -f main` must go through git_retry (race-safe)."
    )
    assert "git_retry -C $d reset --hard origin/main" in self_heal, (
        "self-heal `reset --hard` must go through git_retry (race-safe)."
    )
    # The race-prone bare forms must be gone from the self-heal.
    assert "sudo -u ec2-user git -C $d checkout" not in self_heal
    assert "sudo -u ec2-user git -C $d reset" not in self_heal


def test_fetch_loop_goes_through_git_retry() -> None:
    cmds = _gate_commands()
    fetch = next((c for c in cmds if "fetch --quiet origin main" in c), None)
    assert fetch is not None
    assert "git_retry -C /home/ec2-user/$r fetch --quiet origin main" in fetch, (
        "the fetch loop must go through git_retry so a concurrent boot-pull "
        "fetch can't fail the gate on a ref/pack lock."
    )
