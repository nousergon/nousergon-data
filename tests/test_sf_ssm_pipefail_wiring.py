"""Every SSM RunShellScript command array must start with `set -eo pipefail`.

Regression target: 2026-05-11 weekday SF silent MorningEnrich failure.
`weekly_collector.py --morning-enrich 2>&1 | tee -a /var/log/morning-enrich.log`
exited 1 from constituents-preflight, but `tee` returned 0 and the
pipeline ran on its exit code. SSM reported `ResponseCode: 0,
Status: Success`, the SF moved past MorningEnrich without an order-book
write, and the morning planner aborted minutes later with
"daily_data: 46h stale".

Without `set -o pipefail`, any `cmd | tee` (or other pipe to a benign
sink) silently swallows non-zero exits from `cmd`. `set -e` then has
nothing to react to. Both flags must be present, and they must be the
first command in the array so they cover everything that follows.

This test walks all three SF defns (saturday + weekday + eod) and
asserts every `commands` array begins with `set -eo pipefail`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_SF_PATHS = [
    _INFRA / "step_function.json",
    _INFRA / "step_function_daily.json",
    _INFRA / "step_function_eod.json",
]


def _iter_ssm_command_blocks(sf_doc: dict):
    """Yield (state_name, commands_list) for every SSM RunShellScript task."""
    for state_name, state in sf_doc.get("States", {}).items():
        if state.get("Type") != "Task":
            continue
        resource = state.get("Resource", "")
        if "ssm:sendCommand" not in resource:
            continue
        params = state.get("Parameters", {})
        if params.get("DocumentName") != "AWS-RunShellScript":
            continue
        inner = params.get("Parameters", {})
        cmds = inner.get("commands")
        if isinstance(cmds, list):
            yield state_name, cmds


@pytest.mark.parametrize("sf_path", _SF_PATHS, ids=lambda p: p.name)
def test_every_ssm_command_block_starts_with_pipefail(sf_path: Path) -> None:
    """Pipe-to-tee + no pipefail silently masks non-zero exits.

    Every SSM RunShellScript invocation in the SF must set `pipefail`
    as its first command. `pipefail` is the load-bearing fix: it
    propagates non-zero exits through pipes (notably
    `python ... 2>&1 | tee -a /var/log/foo.log`). Without it, SSM
    reports `ResponseCode: 0` for failed scripts and the SF treats
    the state as Success.

    Both `set -eo pipefail` (Saturday + weekday convention; preferred —
    `set -e` also aborts on the first non-zero exit) and `set -o
    pipefail` (EOD convention) are accepted. The bug being prevented
    is the absence of `pipefail` entirely.
    """
    sf_doc = json.loads(sf_path.read_text())
    offenders: list[str] = []
    for state_name, cmds in _iter_ssm_command_blocks(sf_doc):
        first = cmds[0] if cmds else None
        # Accept any `set -...o... pipefail` first line. The simple
        # substring check is sufficient given the controlled shape of
        # these command arrays.
        if not first or "pipefail" not in first or not first.startswith("set "):
            offenders.append(f"{state_name}: first cmd = {first!r}")
    assert not offenders, (
        f"{sf_path.name}: SSM command blocks missing `pipefail` "
        f"as first command:\n  - " + "\n  - ".join(offenders) + "\n\n"
        "Add 'set -eo pipefail' as the first entry of each `commands` "
        "array. See 2026-05-11 silent-MorningEnrich incident."
    )
