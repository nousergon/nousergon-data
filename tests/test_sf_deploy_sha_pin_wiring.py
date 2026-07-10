"""config#1955 / config#2042: pin the deploy-drift freshness target at pipeline start.

The executor's ``check_deploy_drift`` historically compared the box's HEAD
against a LIVE-fetched ``origin/main`` — a moving target during the ~48-min
weekday pipeline. ``ne-groomer[bot]`` merges benign docs/config commits
through the trading day, so any commit landing between the freshness gate (T0)
and ``RunMorningPlanner`` (~T0+48min) retroactively failed an already-validated
run (2026-07-08 preopen FailExecution: a docs-only CONTRIBUTING.md merge tripped
it — no orders placed).

Fix: the ``CodeFreshnessGate`` freezes the resolved crucible-executor HEAD (the
SHA it synced the box to) into ``/home/ec2-user/.frozen_executor_sha`` at T0.
``RunMorningPlanner`` exports it as ``EXPECTED_EXECUTOR_SHA``; the
systemd-restarted daemon reads the file directly (its process env cannot inherit
the RunDaemon SSM shell's exports). These guards pin that wiring.

config#2042 (2026-07-09): the EOD pipeline's ``RefreshExecutorDeploy`` (config#1549)
re-pulls ``origin/main`` at the top of the EOD run by design, but never re-froze the
pin file — so a same-day executor merge landing after the morning ``CodeFreshnessGate``
moved the box past its own stale pin and ``EODReconcile`` hard-failed (zero
``eod_report.json`` for the day). ``RefreshExecutorDeploy`` must re-freeze the pin
after ``boot-pull.sh`` succeeds, mirroring ``CodeFreshnessGate``'s own pattern.
"""

from __future__ import annotations

import json
from pathlib import Path

_INFRA_DIR = Path(__file__).resolve().parent.parent / "infrastructure"
_SF_PATH = _INFRA_DIR / "step_function_daily.json"
_EOD_SF_PATH = _INFRA_DIR / "step_function_eod.json"
_PIN_FILE = "/home/ec2-user/.frozen_executor_sha"


def _commands(state: str, sf_path: Path = _SF_PATH) -> list[str]:
    doc = json.loads(sf_path.read_text())
    return doc["States"][state]["Parameters"]["Parameters"]["commands"]


def test_freshness_gate_freezes_executor_sha_after_verify() -> None:
    """The gate must write the executor HEAD to the pin file, and only AFTER
    the post-heal freshness verify (so the pin is the confirmed-fresh SHA)."""
    cmds = _commands("CodeFreshnessGate")
    freeze = next(
        (c for c in cmds if _PIN_FILE in c and "rev-parse HEAD" in c), None
    )
    assert freeze is not None, (
        "CodeFreshnessGate must freeze `git -C .../alpha-engine rev-parse HEAD` "
        f"into {_PIN_FILE} (the T0 deploy-drift pin)."
    )
    assert "/home/ec2-user/alpha-engine" in freeze, (
        "the frozen SHA must be the crucible-executor (alpha-engine) checkout HEAD."
    )
    joined = "\n".join(cmds)
    # Ordering: the freeze must come AFTER the CODE-STALE-AFTER-HEAL verify.
    assert joined.index("CODE-STALE-AFTER-HEAL") < joined.index(_PIN_FILE), (
        "freeze the SHA only after the box is verified fresh (post-heal verify)."
    )


def test_morning_planner_exports_pinned_sha() -> None:
    """RunMorningPlanner (direct-python) must export EXPECTED_EXECUTOR_SHA from
    the T0 pin file so check_deploy_drift validates against the frozen SHA,
    not a live origin/main."""
    cmds = _commands("RunMorningPlanner")
    exp = next((c for c in cmds if "EXPECTED_EXECUTOR_SHA" in c), None)
    assert exp is not None, "RunMorningPlanner must export EXPECTED_EXECUTOR_SHA."
    assert _PIN_FILE in exp, (
        f"EXPECTED_EXECUTOR_SHA must be sourced from the T0 pin file {_PIN_FILE}."
    )
    # Must be exported BEFORE the executor runs.
    joined = "\n".join(cmds)
    assert joined.index("EXPECTED_EXECUTOR_SHA") < joined.index("python executor/main.py"), (
        "EXPECTED_EXECUTOR_SHA must be exported before `python executor/main.py`."
    )
    # Fail-soft on a missing pin file (manual/off-pipeline): must not abort the
    # step under `set -eo pipefail` — the executor then live-fetches.
    assert "|| true" in exp or "2>/dev/null" in exp, (
        "a missing pin file must not abort RunMorningPlanner (executor falls "
        "back to a live origin/main fetch)."
    )


def test_eod_refresh_executor_deploy_repins_sha_after_bootpull() -> None:
    """config#2042: RefreshExecutorDeploy re-pulls origin/main via boot-pull.sh
    at the top of the EOD pipeline (config#1549) by design, so it must re-freeze
    the pin file to match — otherwise EODReconcile's check_deploy_drift compares
    the freshly-pulled box against the stale morning CodeFreshnessGate pin and
    hard-fails on any executor PR merged between the morning gate and EOD."""
    cmds = _commands("RefreshExecutorDeploy", sf_path=_EOD_SF_PATH)
    bootpull = next((c for c in cmds if "boot-pull.sh" in c), None)
    assert bootpull is not None, "RefreshExecutorDeploy must run boot-pull.sh."
    freeze = next(
        (c for c in cmds if _PIN_FILE in c and "rev-parse HEAD" in c), None
    )
    assert freeze is not None, (
        "RefreshExecutorDeploy must re-freeze `git -C .../alpha-engine rev-parse "
        f"HEAD` into {_PIN_FILE} after boot-pull.sh, mirroring CodeFreshnessGate's "
        "own pin pattern — otherwise a same-day executor merge desyncs the box "
        "from the stale morning pin and EODReconcile hard-fails."
    )
    assert "/home/ec2-user/alpha-engine" in freeze, (
        "the re-frozen SHA must be the crucible-executor (alpha-engine) checkout HEAD."
    )
    # Ordering: re-freeze must come AFTER boot-pull.sh actually refreshes the checkout.
    joined = "\n".join(cmds)
    assert joined.index("boot-pull.sh") < joined.index(_PIN_FILE), (
        "re-freeze the SHA only after boot-pull.sh has refreshed the checkout."
    )
