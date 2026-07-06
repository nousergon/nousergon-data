"""Pin: every daily-SF SSM entrypoint that `git pull`s alpha-engine-data and
then runs python from its venv MUST self-heal the venv<->requirements
`alpha-engine-lib` pin (scripts/ensure_lib_pin.sh) BETWEEN the pull and the
work line.

Closes the 2026-06-10 weekday-SF failure: data #385 bumped the alpha-engine-lib
pin AND imported a new symbol (`guard_entrypoint`) in one commit; MorningEnrich
`git pull`ed the new code but the box venv was never reinstalled, so the import
resolved against the stale installed lib -> ImportError -> pipeline FAILED.

In-scope states are detected structurally (pull-data-repo + run-from-data-dir),
not hard-coded, so a future data-repo entrypoint added without the heal is
caught here. `test_known_data_entrypoints_in_scope` pins the currently-known
set so a refactor that accidentally drops detection is also caught.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from tests.sf_command_utils import extract_commands

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DAILY_SF = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_DATA_PULL = "git -C /home/ec2-user/alpha-engine-data pull"
_DATA_CD = "cd /home/ec2-user/alpha-engine-data"
_VENV_ACTIVATE = "source .venv/bin/activate"
_HEAL = "scripts/ensure_lib_pin.sh"


def _data_repo_entrypoints() -> dict[str, list[str]]:
    sf = json.loads(_DAILY_SF.read_text())
    out: dict[str, list[str]] = {}
    for name, state in sf["States"].items():
        if not state.get("Resource", "").endswith("ssm:sendCommand"):
            continue
        cmds = extract_commands(state)
        joined = "\n".join(cmds)
        # In scope: pulls the data repo AND runs from the data-repo VENV.
        # config#1807 refinement: the venv-activate marker is load-bearing —
        # the data states moved to the daily data spot, where deps are
        # installed FRESH from requirements.txt at every bootstrap (the pin
        # is satisfied by construction; there is no pull->stale-venv window),
        # and LaunchDailyDataSpot pulls the data repo on ae-dashboard but
        # only runs bash + the DASHBOARD venv (maintained by the dashboard
        # deploy), never data-repo python.
        if _DATA_PULL in joined and _DATA_CD in joined and _VENV_ACTIVATE in joined:
            out[name] = cmds
    return out


def test_known_data_entrypoints_in_scope():
    # RunDailyNews removed (alpha-engine-config#1089) — the standalone 04:00
    # daily-news chain now produces the artifact, so it is no longer a weekday-SF
    # data-repo entrypoint. MorningEnrich + MorningArcticAppend + ChronicGapSelfHeal
    # remain (they pull alpha-engine-data and run from its venv).
    eps = set(_data_repo_entrypoints())
    # config#1807: MorningEnrich / MorningArcticAppend / ChronicGapSelfHeal
    # moved to the daily data spot (fresh clone + fresh deps per bootstrap
    # — no pull->stale-venv window remains), so NO daily-SF state is a
    # data-venv entrypoint today. The detector + parametrized heal test
    # stay armed for any future re-addition.
    assert eps == set(), sorted(eps)


@pytest.mark.parametrize("name", sorted(_data_repo_entrypoints()))
def test_entrypoint_self_heals_lib_pin_after_pull_before_run(name):
    cmds = _data_repo_entrypoints()[name]

    heal_idx = next((i for i, c in enumerate(cmds) if _HEAL in c), None)
    assert heal_idx is not None, f"{name}: missing {_HEAL} self-heal step"

    pull_idx = next(i for i, c in enumerate(cmds) if _DATA_PULL in c)
    activate_idx = next(i for i, c in enumerate(cmds) if _VENV_ACTIVATE in c)
    work_idx = next(
        i for i, c in enumerate(cmds) if c.strip().startswith("python ")
    )

    assert pull_idx < heal_idx, f"{name}: heal must run AFTER the git pull"
    assert activate_idx < heal_idx, f"{name}: heal must run AFTER venv activate"
    assert heal_idx < work_idx, f"{name}: heal must run BEFORE the python work line"


def test_heal_script_exists_and_executable():
    p = _REPO_ROOT / "scripts" / "ensure_lib_pin.sh"
    assert p.exists(), "scripts/ensure_lib_pin.sh missing"
    assert os.stat(p).st_mode & stat.S_IXUSR, "ensure_lib_pin.sh must be executable"
