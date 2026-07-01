"""Pins the EOD-SF deploy-refresh-before-reconcile invariant.

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

Root-cause fix at the correct layer: the EOD pipeline must bring the
executor checkout to ``origin/main`` **before** the drift-guarded
``eod_reconcile.py``, using the canonical, tested refresh path
(``infrastructure/boot-pull.sh`` — the same script ``boot-pull.service``
runs, and the exact recovery the drift-guard message itself prescribes).
This makes EOD reconcile run latest ``main`` by construction and closes
the intraday-merge window structurally.

Two load-bearing details this test pins so the fix cannot silently rot:

1. The refresh must run **as ``ec2-user``** (``sudo -u ec2-user``). SSM
   ``AWS-RunShellScript`` runs as root, but ``boot-pull.service`` is
   ``User=ec2-user`` and the checkout + ``~/.netrc`` are ec2-user-owned;
   boot-pull.sh has no ``safe.directory``/``sudo`` shim, so a root
   invocation would trip git's dubious-ownership guard (CVE-2022-24765).

2. The refresh must appear **before** ``eod_reconcile.py`` in the command
   array — refreshing after the reconcile would be pointless.

FAIL-LOUD is preserved and asserted elsewhere: the drift guard remains the
authoritative gate; this step only removes the *false-positive* staleness
that an intraday merge injects. If boot-pull itself fails, the checkout
stays stale and ``eod_reconcile.py`` hard-fails LOUD — this test does not
weaken that.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"


@pytest.fixture(scope="module")
def eod_commands() -> list[str]:
    doc = json.loads(_SF_PATH.read_text())
    return doc["States"]["EODReconcile"]["Parameters"]["Parameters"]["commands"]


def _index_of(cmds: list[str], needle: str) -> int:
    return next((i for i, c in enumerate(cmds) if needle in c), -1)


class TestDeployRefreshBeforeReconcile:
    def test_boot_pull_refresh_present(self, eod_commands):
        idx = _index_of(eod_commands, "infrastructure/boot-pull.sh")
        assert idx != -1, (
            "EODReconcile must refresh the executor checkout via "
            "infrastructure/boot-pull.sh before running eod_reconcile.py — "
            "otherwise an executor PR merged during the trading day leaves "
            "the checkout stale and the deploy-drift guard hard-fails "
            "(2026-06-30 incident)."
        )

    def test_refresh_runs_as_ec2_user(self, eod_commands):
        idx = _index_of(eod_commands, "infrastructure/boot-pull.sh")
        assert idx != -1
        line = eod_commands[idx]
        assert "sudo -u ec2-user" in line, (
            "boot-pull.sh must run as ec2-user (matching boot-pull.service "
            "User=ec2-user and the ec2-user-owned checkout/~/.netrc). SSM "
            "runs as root; a root git fetch/reset on the ec2-user-owned "
            "checkout trips git's dubious-ownership guard (CVE-2022-24765)."
        )

    def test_refresh_precedes_reconcile(self, eod_commands):
        refresh_idx = _index_of(eod_commands, "infrastructure/boot-pull.sh")
        reconcile_idx = _index_of(eod_commands, "executor/eod_reconcile.py")
        assert refresh_idx != -1 and reconcile_idx != -1
        assert refresh_idx < reconcile_idx, (
            "The deploy refresh must run BEFORE eod_reconcile.py — "
            "refreshing after the reconcile is a no-op."
        )

    def test_pipefail_still_first(self, eod_commands):
        # The refresh insertion must not displace `set -o pipefail` from
        # the first slot (test_sf_ssm_pipefail_wiring pins this too; belt
        # and suspenders so the two invariants can't drift apart).
        assert eod_commands[0].startswith("set ") and "pipefail" in eod_commands[0]

    def test_refresh_does_not_swallow_via_tee(self, eod_commands):
        # The refresh line must not introduce a competing `| tee` work
        # line (that would confuse the S3-log-ship guard, which keys off
        # the first tee'd logfile). It writes to its own file via `>`.
        idx = _index_of(eod_commands, "infrastructure/boot-pull.sh")
        assert "| tee " not in eod_commands[idx]
