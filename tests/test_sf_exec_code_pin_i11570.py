"""One code SHA per repo per weekly-SF execution — alpha-engine-config-I11570.

Finding (rehearsal-2026-09-24-1): the DataPhase1 re-issue re-ran its stage
command, whose first line was ``git -C /home/ec2-user/alpha-engine-data pull
--ff-only origin main``. main had moved, so the box's checkout advanced
``dafacc5 -> 6d2460d`` (PR1927) in the middle of the execution, and one
execution ran two code SHAs. Its outputs could not be tied to either.

The fix has two halves, and this module pins both:

1. **Launcher box.** Every weekly-SF stage syncs its checkouts through
   ``infrastructure/exec_code_pin.sh <execution name> <checkout>``. The first
   call per (execution, checkout) pulls main and records the SHA; every later
   call in the same execution — the next stage, a re-issue, a parallel
   sibling — checks that SHA out and never pulls.
2. **Spot worker.** MorningEnrich / DataPhase1 / RAGIngestion launch a worker
   that clones ``--branch main`` on every launch, so a re-issue's worker would
   still land on newer code. ``_spot_common.sh::bootstrap_spot`` now checks the
   worker out at the launcher's pin, read via ``$RUN_TOKEN`` (the execution
   name ``krepis.ssm_log_capture`` hands its child).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from infrastructure.sf_commands import extract_commands, render_commands

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_SF_PATH = _INFRA / "step_function.json"
_HELPER = _INFRA / "exec_code_pin.sh"
_SPOT_COMMON = _INFRA / "_spot_common.sh"

_BOX_HELPER = "/home/ec2-user/alpha-engine-data/infrastructure/exec_code_pin.sh"
_LOCK = "/home/ec2-user/.ae-git-sync.lock"

# Any command that moves a box checkout to something other than the pin.
_UNPINNED_SYNC = re.compile(
    r"\bgit\s+(?:-C\s+\S+\s+)?(?:-c\s+\S+\s+)*"
    r"(?:pull|fetch|reset\s+--hard|checkout\s+(?:-f\s+)?main)\b"
)
_HELPER_CALL = re.compile(
    r"flock -w \d+ " + re.escape(_LOCK) + r" bash " + re.escape(_BOX_HELPER)
    + r" (\S+) (/home/ec2-user/[\w.-]+)"
)


def _definition() -> dict:
    return json.loads(_SF_PATH.read_text())


def _ssm_stages(states: dict, prefix: str = ""):
    for name, state in states.items():
        if state.get("Resource") == "arn:aws:states:::aws-sdk:ssm:sendCommand":
            yield prefix + name, state
        for branch in state.get("Branches", []) or []:
            yield from _ssm_stages(branch["States"], f"{prefix}{name}/")


def _all_states(states: dict):
    for name, state in states.items():
        yield name, state
        for branch in state.get("Branches", []) or []:
            yield from _all_states(branch["States"])


_CONTEXT = {"Execution": {"Name": "exec-under-test", "Id": "arn:x", "StartTime": "2026-09-26T00:00:00Z"}}
_BINDINGS = {"run_date": "2026-09-26", "preflight_args": ""}


def _rendered(state: dict) -> list[str]:
    try:
        return render_commands(state, _BINDINGS, _CONTEXT)
    except Exception:  # a stage bound to a per-stage Payload path
        return extract_commands(state)


# ── 1. launcher box: the definition ─────────────────────────────────────────


def test_no_weekly_stage_syncs_a_checkout_to_main_directly():
    """Every checkout sync goes through the pin helper. A bare pull (or
    fetch/reset/checkout of main) anywhere in a stage command is exactly the
    line that advanced DataPhase1's code on its re-issue."""
    offenders = []
    for name, state in _ssm_stages(_definition()["States"]):
        for cmd in extract_commands(state):
            if _UNPINNED_SYNC.search(cmd):
                offenders.append(f"{name}: {cmd[:200]}")
    assert not offenders, (
        "weekly-SF stage syncs a checkout without the execution pin — a "
        "re-issue of this stage would run newer code than its first attempt "
        "(alpha-engine-config-I11570). Use "
        f"`flock -w 150 {_LOCK} bash {_BOX_HELPER} <$$.Execution.Name> <checkout>`:\n"
        + "\n".join(offenders)
    )


def test_every_checkout_sync_is_keyed_on_the_execution_name():
    """The pin key must be ``$$.Execution.Name``: the same across every stage,
    re-issue and parallel branch of one execution, and different for a new
    execution (scripts/weekly_sf_rerun.py), which must resolve main afresh."""
    seen = 0
    for name, state in _ssm_stages(_definition()["States"]):
        for cmd in _rendered(state):
            for m in _HELPER_CALL.finditer(cmd):
                seen += 1
                assert m.group(1) in ("exec-under-test", "{}"), (
                    f"{name}: pin keyed on {m.group(1)!r}, not the execution name"
                )
    # 25 checkout syncs across the weekly definition on 2026-09-25.
    assert seen >= 20, f"expected the pin helper on every stage, found {seen}"


def test_first_box_stages_resolve_the_data_pin_before_any_workload():
    """MorningEnrich and DataPhase1 must sync alpha-engine-data (and config)
    through the helper BEFORE they run their launcher, so the worker pin
    (_spot_common.sh) finds the execution's pin file on the box."""
    states = _definition()["States"]
    for stage in ("MorningEnrich", "DataPhase1"):
        cmds = _rendered(states[stage])
        pin_idx = [i for i, c in enumerate(cmds) if _HELPER_CALL.search(c)]
        run_idx = [i for i, c in enumerate(cmds) if "ssm_log_capture" in c]
        assert pin_idx and run_idx and max(pin_idx) < min(run_idx), stage
        pinned = {m.group(2) for c in cmds for m in _HELPER_CALL.finditer(c)}
        assert pinned == {"/home/ec2-user/alpha-engine-data", "/home/ec2-user/alpha-engine-config"}


def test_every_reissue_reenters_a_pinned_stage():
    """The bug was on the re-issue path. Every ``*Reissue`` Pass state loops
    back into an SSM stage whose checkout sync is the pin helper, so the
    re-issue checks out the SHA the failed attempt ran."""
    all_states = dict(_all_states(_definition()["States"]))
    reissues = {n: s for n, s in all_states.items() if n.endswith("Reissue")}
    assert "DataPhase1Reissue" in reissues
    for name, state in reissues.items():
        target = all_states[state["Next"]]
        cmds = _rendered(target)
        assert any(_HELPER_CALL.search(c) for c in cmds), (
            f"{name} -> {state['Next']} re-enters a stage that does not sync "
            "through the execution pin"
        )


# ── 1b. launcher box: the helper's behaviour ────────────────────────────────


def _git(*args: str, cwd: Path) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
    }
    return subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(work: Path, msg: str) -> str:
    (work / "f.txt").write_text(msg)
    _git("add", "f.txt", cwd=work)
    _git("commit", "-q", "-m", msg, cwd=work)
    _git("push", "-q", "origin", "main", cwd=work)
    return _git("rev-parse", "HEAD", cwd=work)


@pytest.fixture()
def box(tmp_path: Path):
    origin = tmp_path / "origin.git"
    _git("init", "-q", "--bare", "-b", "main", str(origin), cwd=tmp_path)
    author = tmp_path / "author"
    _git("clone", "-q", str(origin), str(author), cwd=tmp_path)
    _git("checkout", "-q", "-b", "main", cwd=author)
    first = _commit(author, "A")
    checkout = tmp_path / "alpha-engine-data"
    _git("clone", "-q", "--depth", "1", "--branch", "main", f"file://{origin}", str(checkout), cwd=tmp_path)
    return {"tmp": tmp_path, "author": author, "checkout": checkout, "first": first}


def _pin(box: dict, execution: str) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "AE_EXEC_PIN_ROOT": str(box["tmp"] / "pins"),
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
    }
    return subprocess.run(
        ["bash", str(_HELPER), execution, str(box["checkout"])],
        env=env, capture_output=True, text=True,
    )


def _head(box: dict) -> str:
    return _git("rev-parse", "HEAD", cwd=box["checkout"])


def test_reissue_in_the_same_execution_keeps_the_first_sha(box):
    """The rehearsal-2026-09-24-1 sequence: first attempt resolves main, main
    moves, the re-issue runs again. The re-issue must run the SAME SHA."""
    r = _pin(box, "rehearsal-2026-09-24-1")
    assert r.returncode == 0, r.stderr
    assert _head(box) == box["first"]
    assert r.stdout == "", "the helper must keep stdout clean (ResolveZooSpecs parses it)"
    pin_file = box["tmp"] / "pins" / "rehearsal-2026-09-24-1" / "alpha-engine-data"
    assert pin_file.read_text().strip() == box["first"]

    newer = _commit(box["author"], "B")  # PR1927 merges mid-execution
    r = _pin(box, "rehearsal-2026-09-24-1")  # DataPhase1Reissue
    assert r.returncode == 0, r.stderr
    assert _head(box) == box["first"] != newer


def test_a_moved_checkout_is_put_back_on_the_pin(box):
    """Anything else that advanced the checkout mid-execution is undone at the
    next stage boundary, not inherited by it."""
    assert _pin(box, "exec-1").returncode == 0
    newer = _commit(box["author"], "B")
    _git("pull", "-q", "--ff-only", "origin", "main", cwd=box["checkout"])
    assert _head(box) == newer
    r = _pin(box, "exec-1")
    assert r.returncode == 0, r.stderr
    assert _head(box) == box["first"]


def test_a_new_execution_resolves_latest_main(box):
    """A fresh execution (or scripts/weekly_sf_rerun.py reusing the box under a
    new execution name) still starts from latest main — including after a
    previous execution left the checkout detached on its pin."""
    assert _pin(box, "exec-1").returncode == 0
    newer = _commit(box["author"], "B")
    _git("pull", "-q", "--ff-only", "origin", "main", cwd=box["checkout"])
    assert _pin(box, "exec-1").returncode == 0  # detaches back on A
    r = _pin(box, "exec-2")
    assert r.returncode == 0, r.stderr
    assert _head(box) == newer


@pytest.mark.parametrize("bad", ["", "../escape", "a/b", ".."])
def test_refuses_an_execution_name_that_is_not_a_path_component(box, bad):
    assert _pin(box, bad).returncode != 0


def test_a_corrupt_pin_fails_loud(box):
    pin_dir = box["tmp"] / "pins" / "exec-1"
    pin_dir.mkdir(parents=True)
    (pin_dir / "alpha-engine-data").write_text("not-a-sha\n")
    r = _pin(box, "exec-1")
    assert r.returncode != 0
    assert "not a 40-hex SHA" in r.stderr


# ── 2. spot worker: _spot_common.sh ─────────────────────────────────────────


def _function_source(name: str) -> str:
    text = _SPOT_COMMON.read_text()
    m = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.M | re.S)
    assert m, f"{name}() not found in _spot_common.sh"
    return m.group(0)


def _worker_pin(tmp_path: Path, *, run_token: str | None, branch: str = "main") -> subprocess.CompletedProcess:
    script = (
        "set -euo pipefail\n"
        + _function_source("_exec_code_pin_sha")
        + '_exec_code_pin_sha\n'
    )
    env = {"PATH": os.environ["PATH"], "AE_EXEC_PIN_ROOT": str(tmp_path), "BRANCH": branch}
    if run_token is not None:
        env["RUN_TOKEN"] = run_token
    return subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)


def test_worker_reads_the_launchers_pin_via_run_token(tmp_path):
    sha = "a" * 40
    (tmp_path / "exec-1").mkdir()
    (tmp_path / "exec-1" / "alpha-engine-data").write_text(sha + "\n")
    r = _worker_pin(tmp_path, run_token="exec-1")
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == sha
    # No pin for this execution (laptop run, older launcher) -> no pin.
    assert _worker_pin(tmp_path, run_token="exec-2").stdout.strip() == ""
    assert _worker_pin(tmp_path, run_token=None).stdout.strip() == ""
    # An explicit --branch is an operator asking for that branch: honour it.
    assert _worker_pin(tmp_path, run_token="exec-1", branch="feature").stdout.strip() == ""


def test_worker_is_pinned_inside_bootstrap_before_deps():
    """bootstrap_spot() must end by pinning the worker checkout, so
    install_deps (always called right after it) installs requirements.txt from
    the pinned tree, and an in-stage relaunch (_spot_relaunch.sh re-execs the
    launcher) re-pins the replacement worker."""
    body = _function_source("bootstrap_spot")
    assert body.rstrip().splitlines()[-2].strip() == "pin_worker_checkout"
    pin = _function_source("pin_worker_checkout")
    assert "checkout -q --detach" in pin
    assert "exit 1" in pin, "a worker that cannot reach the pin must fail, not run main"
    for launcher in ("spot_morning_enrich.sh", "spot_data_phase1.sh", "spot_rag_ingestion.sh"):
        text = (_INFRA / launcher).read_text()
        assert text.index("\nbootstrap_spot\n") < text.index("\ninstall_deps\n"), launcher
