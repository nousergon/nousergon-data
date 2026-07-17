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
    # data-repo entrypoint.
    #
    # config#1767 (Phase 2): MorningEnrich + MorningArcticAppend were relocated OFF
    # the on-trading SSM path onto the ephemeral data spot (the spot bootstrap in
    # infrastructure/lambdas/data-spot-dispatcher/index.py clones + venvs the data
    # repo there). They are therefore no longer on-trading ssm:sendCommand
    # entrypoints in this SF.
    #
    # alpha-engine-config-I2717 (2026-07-16): ChronicGapSelfHeal — the last
    # remaining on-trading data-repo entrypoint this test used to pin — was
    # ALSO removed (moved to the standalone --daily-heal job, on its own
    # ephemeral spot box that clones fresh rather than pulling a persistent
    # checkout, so it is out of scope for this detector by construction, same
    # as the data-spot-dispatcher's other workloads). The weekday SF now has
    # ZERO on-trading data-repo entrypoints in this detector's scope — this is
    # the expected end state of config#1767 + I2717's cumulative decoupling,
    # not a detection regression.
    eps = set(_data_repo_entrypoints())
    assert eps == set(), (
        f"expected NO on-trading data-repo entrypoints left in the weekday SF "
        f"(all moved to spot boxes), found: {sorted(eps)}"
    )
    # Guard the relocation: the moved states must NOT be on-trading entrypoints.
    assert "MorningEnrich" not in eps
    assert "MorningArcticAppend" not in eps
    assert "ChronicGapSelfHeal" not in eps


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
