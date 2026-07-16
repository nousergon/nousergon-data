"""Pins the EOD-SF executor-deploy-refresh invariant — HOISTED to a single
top-of-pipeline chokepoint (config#1549), NARROWED to a conditional refresh
(alpha-engine-config-I2722, 2026-07-16 — see TestConditionalRefresh below).

Incident (2026-06-30 EOD, run for 2026-06-30 executed 2026-07-01): the
``ne-postclose-trading-pipeline`` EOD reconcile hard-failed at
``executor/preflight.py::check_deploy_drift`` — the trading instance's
crucible-executor checkout was on ``b7e52b1`` (what the ~6:10 AM PT
``boot-pull.service`` correctly pulled) while ``origin/main`` had advanced
to ``8c3014e``. Three executor PRs (#319/#317/#318) merged 08:07–08:54 PT,
*hours after* the morning boot-pull. The trading instance is long-lived
(boots in the morning, trades all day, EOD-reconciles on the same box),
so the checkout only refreshes once per day at boot; any executor PR that
merges during the trading day leaves EOD reconcile on stale code and the
(correct, fail-loud) drift guard refuses to reconcile NAV.

The 2026-06-30 same-day recovery (nousergon-data#574) patched ONLY the
``EODReconcile`` step (the one work step that happens to carry a drift
guard). But the broader class is that the EOD pipeline refreshes the
executor/data code only once per day at morning boot, so EVERY EOD step —
``PostMarketData`` / ``PostMarketArcticAppend`` / ``CaptureSnapshot``,
which run BEFORE the reconcile and have NO drift guard — executed stale
intraday code *silently* (no fail-loud surface), and a stale-code snapshot
that a fresh-code reconcile then reads is a latent cross-version
consistency risk.

Root-cause fix at the correct layer (config#1549): hoist the refresh out
of the reconcile step into a single ``RefreshExecutorDeploy`` SSM state at
the TOP of the pipeline — right after the SSM-readiness gate, before the
first work gate ``CheckSkipPostMarketData`` — invoking the canonical,
tested ``infrastructure/boot-pull.sh`` ONCE (as ``ec2-user``) so the
*entire* EOD run (postmarket → arctic → snapshot → reconcile) executes on
latest ``origin/main`` by construction. #574's per-step refresh is removed
from ``EODReconcile`` (the top-level step subsumes it). This lifts the
invariant "EOD runs latest main" to one chokepoint instead of N per-step
patches, and closes the silent-stale-code exposure on the non-guarded
steps.

Load-bearing details this test pins so the fix cannot silently rot:

1. The refresh lives in its OWN ``RefreshExecutorDeploy`` state (not
   inline in ``EODReconcile``), wired via the async
   send → ``WaitFor…`` → ``Check…Status`` triplet like every other EOD
   SSM work step, and gated by ``CheckSkipRefreshExecutorDeploy`` for
   per-task rerunnability.

2. The refresh runs **as ``ec2-user``** (``sudo -u ec2-user``). SSM
   ``AWS-RunShellScript`` runs as root, but ``boot-pull.service`` is
   ``User=ec2-user`` and the checkout + ``~/.netrc`` are ec2-user-owned;
   boot-pull.sh has no ``safe.directory``/``sudo`` shim, so a root
   invocation would trip git's dubious-ownership guard (CVE-2022-24765).

3. It runs **before** any work step (topologically upstream of
   ``PostMarketData``), and ``EODReconcile`` no longer carries an inline
   boot-pull.

4. **FAIL-LOUD is strengthened**: because the non-guarded work steps now
   depend on this refresh having succeeded, ``RefreshExecutorDeploy`` must
   NOT swallow a boot-pull failure with ``|| echo`` — a failed refresh
   surfaces as a non-zero SSM status → HandleFailure → ForceStopInstance
   BEFORE any stale-code work step runs. (The drift guard in
   ``eod_reconcile.py`` remains the authoritative gate downstream.)

alpha-engine-config-I2722 (2026-07-16): NARROWED to a conditional refresh.
A hard-fail/always-refresh reading of "refresh at the top of every EOD run"
was too literal — running the full multi-repo boot-pull.sh (pull + pip
install + systemd-unit sync + trades.db restore, up to the 600s budget)
EVERY EOD run, even on a day with zero executor merges, wastes the bulk of
its own budget on a no-op refresh. ``RefreshExecutorDeploy`` now fetches
origin and compares local HEAD to origin/main FIRST; boot-pull.sh (and the
rest of the drifted-branch behavior — unchanged byte-for-byte, including
the pin re-freeze and the fail-loud property) only runs when the checkout
has actually drifted. ``TestConditionalRefresh`` below pins the new shape;
every pre-existing test above is UNCHANGED and still passes, since they all
match on `"boot-pull.sh" in line` / `"sudo -u ec2-user"` / etc. without
caring which branch of the if/else contains the matched line.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def refresh_commands(states) -> list[str]:
    # RefreshExecutorDeploy uses a plain `commands` JSON array (no $.run_date
    # splice needed, unlike EODReconcile's States.Array/States.Format form).
    return states["RefreshExecutorDeploy"]["Parameters"]["Parameters"]["commands"]


def _index_of(cmds: list[str], needle: str) -> int:
    return next((i for i, c in enumerate(cmds) if needle in c), -1)


class TestRefreshHoistedToChokepoint:
    def test_refresh_state_exists(self, states):
        assert "RefreshExecutorDeploy" in states, (
            "config#1549: the executor-deploy refresh must be a dedicated "
            "top-of-pipeline RefreshExecutorDeploy state, not inline in "
            "EODReconcile."
        )
        st = states["RefreshExecutorDeploy"]
        assert "ssm:sendCommand" in st["Resource"]

    def test_boot_pull_refresh_present(self, refresh_commands):
        idx = _index_of(refresh_commands, "infrastructure/boot-pull.sh")
        assert idx != -1, (
            "RefreshExecutorDeploy must refresh the executor checkout via "
            "infrastructure/boot-pull.sh so the whole EOD run executes latest "
            "origin/main (2026-06-30 intraday-merge incident)."
        )

    def test_refresh_runs_as_ec2_user(self, refresh_commands):
        idx = _index_of(refresh_commands, "infrastructure/boot-pull.sh")
        assert idx != -1
        line = refresh_commands[idx]
        assert "sudo -u ec2-user" in line, (
            "boot-pull.sh must run as ec2-user (matching boot-pull.service "
            "User=ec2-user and the ec2-user-owned checkout/~/.netrc). SSM "
            "runs as root; a root git fetch/reset on the ec2-user-owned "
            "checkout trips git's dubious-ownership guard (CVE-2022-24765)."
        )

    def test_pipefail_first(self, refresh_commands):
        assert refresh_commands[0].startswith("set ") and "pipefail" in refresh_commands[0]

    def test_refresh_is_fail_loud_not_swallowed(self, refresh_commands):
        # The whole point of the hoist is that non-drift-guarded work steps
        # depend on the refresh having succeeded — so a failed boot-pull must
        # NOT be swallowed with `|| echo` (as #574's inline form did, relying
        # on the downstream reconcile guard). It must propagate a non-zero exit.
        idx = _index_of(refresh_commands, "infrastructure/boot-pull.sh")
        assert "|| echo" not in refresh_commands[idx] and "|| true" not in refresh_commands[idx], (
            "RefreshExecutorDeploy must fail loud on a boot-pull failure — "
            "swallowing it would let PostMarketData/ArcticAppend/Snapshot run "
            "stale code silently, the exact exposure config#1549 closes."
        )

    def test_refresh_does_not_swallow_via_tee(self, refresh_commands):
        idx = _index_of(refresh_commands, "infrastructure/boot-pull.sh")
        assert "| tee " not in refresh_commands[idx]

    def test_refresh_runs_before_any_work_step(self, states):
        # RefreshExecutorDeploy (via its Check…Status Success edge) enters the
        # first work gate CheckSkipPostMarketData; and the SSM-readiness gate
        # enters the refresh gate first. So the refresh is topologically
        # upstream of every work step.
        succ = [c["Next"] for c in states["CheckRefreshExecutorDeployStatus"]["Choices"]
                if c.get("StringEquals") == "Success"]
        assert succ == ["CheckSkipPostMarketData"]
        online = [c["Next"] for c in states["SSMReadyChoice"]["Choices"]
                  if any(x.get("StringEquals") == "Online" for x in c.get("And", []))]
        assert online == ["CheckSkipRefreshExecutorDeploy"]


class TestReconcileNoLongerCarriesInlineRefresh:
    """#574's per-step boot-pull is removed from EODReconcile — the top-level
    RefreshExecutorDeploy chokepoint subsumes it (config#1549)."""

    def test_eod_reconcile_has_no_inline_boot_pull(self, states):
        params = states["EODReconcile"]["Parameters"]["Parameters"]
        # EODReconcile uses commands.$ (States.Array/States.Format for $.run_date).
        blob = params.get("commands.$") or json.dumps(params.get("commands", []))
        assert "boot-pull.sh" not in blob, (
            "EODReconcile must NOT carry an inline boot-pull refresh — it is "
            "hoisted to RefreshExecutorDeploy at the top of the pipeline "
            "(config#1549). Leaving both would double-refresh."
        )

    def test_eod_reconcile_still_runs_reconcile(self, states):
        params = states["EODReconcile"]["Parameters"]["Parameters"]
        blob = params.get("commands.$") or json.dumps(params.get("commands", []))
        assert "executor/eod_reconcile.py" in blob


class TestConditionalRefresh:
    """alpha-engine-config-I2722 (2026-07-16): RefreshExecutorDeploy narrowed
    to a conditional refresh — fast drift-check (fetch + rev-parse compare)
    runs unconditionally; the expensive full boot-pull.sh refresh only runs
    when the checkout has actually drifted from origin/main. A hard-fail (or
    always-refresh) reading would waste most of the 600s budget on a no-op
    refresh on every EOD run that happens to land on a day with zero executor
    merges — the common case."""

    def test_fetches_origin_before_comparing(self, refresh_commands):
        idx = _index_of(refresh_commands, "git fetch origin")
        assert idx != -1, (
            "RefreshExecutorDeploy must fetch origin before comparing local "
            "HEAD to origin/main — otherwise the drift check compares against "
            "a stale local remote-tracking ref."
        )
        assert "sudo -u ec2-user" in refresh_commands[idx], (
            "the fetch must run as ec2-user, same as every other git op in "
            "this state (CVE-2022-24765 dubious-ownership guard)."
        )

    def test_compares_local_head_to_origin_main(self, refresh_commands):
        joined = "\n".join(refresh_commands)
        assert "rev-parse HEAD" in joined and "rev-parse origin/main" in joined, (
            "the drift check must compare local HEAD against origin/main."
        )

    def test_boot_pull_only_runs_on_the_drifted_branch(self, refresh_commands):
        # boot-pull.sh must be textually inside an else (or equivalent
        # not-equal) branch, not run unconditionally at top level.
        fetch_idx = _index_of(refresh_commands, "git fetch origin")
        bootpull_idx = _index_of(refresh_commands, "infrastructure/boot-pull.sh")
        assert fetch_idx != -1 and bootpull_idx != -1
        assert fetch_idx < bootpull_idx, (
            "the drift check (fetch + compare) must run BEFORE boot-pull.sh — "
            "that's the whole point of the fast path."
        )
        # There must be a conditional (if/else) somewhere between the compare
        # and boot-pull.sh — a purely sequential script would run boot-pull.sh
        # unconditionally every time, defeating the narrowing.
        between = refresh_commands[fetch_idx:bootpull_idx]
        assert any("if " in c or c.strip() == "else" for c in between), (
            "expected an if/else between the drift check and boot-pull.sh — "
            f"got: {between!r}"
        )

    def test_fast_path_does_not_invoke_boot_pull(self, refresh_commands):
        # The "current, skip" branch (the `if` body, before `else`) must not
        # itself contain a boot-pull.sh invocation — only the drifted (else)
        # branch may run it.
        joined = "\n".join(refresh_commands)
        if_start = joined.index("if [")
        else_start = joined.index("\nelse")
        fast_path_body = joined[if_start:else_start]
        assert "boot-pull.sh" not in fast_path_body, (
            "the fast (not-drifted) path must not run boot-pull.sh — that "
            "defeats the whole point of the conditional narrowing."
        )

    def test_timeouts_unchanged(self, states):
        # The drifted path still needs the full budget boot-pull.sh always
        # required; only the fast path benefits from finishing early. No
        # reason to shrink the ceiling.
        st = states["RefreshExecutorDeploy"]
        assert st["Parameters"]["Parameters"]["executionTimeout"] == ["600"]
        assert st["Parameters"]["TimeoutSeconds"] == 600
        assert st["TimeoutSeconds"] == 660
