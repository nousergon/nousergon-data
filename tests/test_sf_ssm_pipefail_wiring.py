"""SSM RunShellScript command-array hygiene checks.

Two rules pinned here:

1. Every command array must START with `set ... pipefail`. Without it,
   `cmd | tee` silently masks non-zero exits from `cmd` (2026-05-11
   silent-MorningEnrich incident).

2. Every weekday-SF command array must also `export
   FLOW_DOCTOR_ENABLED=1`. The 2026-05-05 migration from boot-triggered
   systemd to SF-triggered SSM dropped this env var on the
   trading-instance SSM path (systemd units had it baked in via
   `Environment=`). Without it, `alpha_engine_lib.logging.setup_logging`
   skips attaching `FlowDoctorHandler` and ERROR-level logs go only to
   stdout — exactly the failure mode on 2026-05-11, where two ERROR
   logs from `weekly_collector` fired but flow-doctor never escalated.
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


_DAILY_SF_PATH = _INFRA / "step_function_daily.json"


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


def test_weekday_sf_ssm_blocks_export_flow_doctor_enabled() -> None:
    """Every weekday-SF SSM command array must `export FLOW_DOCTOR_ENABLED=1`.

    `alpha_engine_lib.logging.setup_logging` only attaches
    `FlowDoctorHandler` when this env var is set; otherwise ERROR-level
    logs go only to stdout and never enter the dispatch pipeline
    (email + GitHub issue + S3 changelog).

    Regression target: the 2026-05-05 systemd → SSM migration silently
    dropped flow-doctor coverage on the trading-instance SSM path. The
    disabled-but-retained systemd units had `Environment=FLOW_DOCTOR_ENABLED=1`
    baked in; SSM `RunShellScript` only sources `.alpha-engine.env` and
    the env file never gained the flag. On 2026-05-11 MorningEnrich
    emitted two ERROR logs from `weekly_collector` and flow-doctor
    never escalated.

    Pinning as an `export` in the command array — not as a value in
    `.alpha-engine.env` — keeps the contract version-controlled and
    survives env-file drift or instance rebuilds.
    """
    sf_doc = json.loads(_DAILY_SF_PATH.read_text())
    offenders: list[str] = []
    for state_name, cmds in _iter_ssm_command_blocks(sf_doc):
        # Match either `export FLOW_DOCTOR_ENABLED=1` or
        # `FLOW_DOCTOR_ENABLED=1 ...` syntax; both achieve the same
        # effect in a RunShellScript invocation.
        has_flag = any(
            "FLOW_DOCTOR_ENABLED=1" in c for c in cmds
        )
        if not has_flag:
            offenders.append(state_name)
    assert not offenders, (
        f"{_DAILY_SF_PATH.name}: SSM command blocks missing "
        f"`FLOW_DOCTOR_ENABLED=1`:\n  - " + "\n  - ".join(offenders) + "\n\n"
        "Add `\"export FLOW_DOCTOR_ENABLED=1\"` to each `commands` array. "
        "See 2026-05-11 incident — flow-doctor silently skipped because "
        "setup_logging's env-var gate falls through to 'disabled'."
    )
