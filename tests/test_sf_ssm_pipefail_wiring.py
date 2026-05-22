"""SSM RunShellScript command-array hygiene checks.

Three rules pinned here:

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

3. Every long-running command block must ship its log to S3 before the
   instance terminates. SSM's `StandardOutputContent` is capped at 24KB
   and `StandardOutputUrl` is empty (no CloudWatchOutputConfig), so when
   a long step exits 1 on a stopped/terminated instance the actual error
   is otherwise unrecoverable. Recurring diagnostic gap: MorningEnrich
   2026-05-15, DataPhase1 2026-05-03, backtester 2026-04-22.

   Two equivalent forms satisfy this invariant:
   (a) **Inline bash trap** — `trap 'aws s3 cp /var/log/X.log
       "s3://..." --only-show-errors || true' EXIT` BEFORE a
       `| tee /var/log/X.log` work line. Used by states whose
       `commands` is a plain JSON array.
   (b) **`alpha_engine_lib.ssm_log_capture` CLI** — a single
       `python -m alpha_engine_lib.ssm_log_capture run --slug X
       --log /var/log/X.log -- bash <launcher> ...` invocation that
       internalizes both the trap and the tee. Used by states whose
       `commands.$` is a `States.Array(...)` (the ASL escape surface
       for inner single quotes is broken, so the inline-trap form
       cannot live there — see alpha-engine-lib PR #57 / 2026-05-22
       Friday-PM dry-pass break).

   The lib CLI is the institutional chokepoint per the CLAUDE.md SOTA
   sub-sub-rule ("Pure-Bash primitives can stay mirrored unless
   re-expressible as a Python CLI entry callable from Bash"). The
   inline-trap form is retained for plain `commands` arrays that have
   never been broken by ASL escape behavior.

   In either form: the log-capture step must not be able to override
   the script's real exit status. Inline traps use `|| true`; the lib
   CLI propagates the inner subprocess exit code verbatim and swallows
   S3 errors at WARNING.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tests.sf_command_utils import extract_commands


_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_SF_PATHS = [
    _INFRA / "step_function.json",
    _INFRA / "step_function_daily.json",
    _INFRA / "step_function_eod.json",
]


def _iter_ssm_command_blocks(sf_doc: dict):
    """Yield (state_name, commands_list) for every SSM RunShellScript task.

    Resolves BOTH ``commands`` (plain JSON array) and ``commands.$``
    (``States.Array(...)`` intrinsic) forms via the shared
    :func:`tests.sf_command_utils.extract_commands` helper. This was a
    silent test gap that masked the 2026-05-22 Friday-PM dry-pass break
    — the original implementation only iterated ``commands``, so the
    Saturday-SF states that PR #253 (2026-05-17) flipped to
    ``commands.$`` form silently stopped being covered. The
    inline-bash-trap regression that followed went undetected by this
    file until the dry-pass actually fired in production. The
    extraction now covers both forms; the broken-trap class cannot
    silently slip past again.
    """
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
        if "commands" in inner and isinstance(inner["commands"], list):
            yield state_name, list(inner["commands"])
        elif "commands.$" in inner:
            yield state_name, extract_commands(state)


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


_TEE_WORK_RE = re.compile(r"\| tee (?:-a )?(/var/log/[\w.-]+\.log)")
_LIB_CLI_RE = re.compile(
    r"alpha_engine_lib\.ssm_log_capture\s+run\s+.*?--slug\s+(\S+)\s+--log\s+(/var/log/[\w.-]+\.log)"
)


@pytest.mark.parametrize("sf_path", _SF_PATHS, ids=lambda p: p.name)
def test_long_ssm_steps_ship_log_to_s3(sf_path: Path) -> None:
    """Every long SSM step must ship its log to S3 — via inline trap OR lib CLI.

    Regression target: the recurring "step exits 1 but the cause is past
    SSM's 24KB StandardOutputContent cap and the instance is gone" gap
    (MorningEnrich 2026-05-15, DataPhase1 2026-05-03, backtester
    2026-04-22). Two satisfying shapes (see module docstring rule #3):

    (a) **Inline bash trap** before `| tee /var/log/X.log` work line —
        original form, plain `commands` JSON arrays only.
    (b) **`alpha_engine_lib.ssm_log_capture` CLI** — single
        `python -m alpha_engine_lib.ssm_log_capture run --slug X
        --log /var/log/X.log -- bash <launcher>` line; internalizes the
        trap. Required form for `commands.$ States.Array(...)` states
        (ASL doesn't unescape `\\'` in arg strings; the inline-trap
        form breaks there — caught by the 2026-05-22 Friday-PM
        dry-pass).
    """
    sf_doc = json.loads(sf_path.read_text())
    offenders: list[str] = []
    for state_name, cmds in _iter_ssm_command_blocks(sf_doc):
        # Form (b): lib CLI satisfies the invariant on its own — the
        # tee + S3 ship + exit-code propagation are all internalized.
        lib_idx = next(
            (i for i, c in enumerate(cmds) if _LIB_CLI_RE.search(c)),
            None,
        )
        if lib_idx is not None:
            # Optional consistency check: slug-vs-logpath should match
            # the canonical layout (slug == basename of logfile minus
            # .log) — caller convention, not strictly required by the
            # lib but it's what every state in the fleet uses today.
            m = _LIB_CLI_RE.search(cmds[lib_idx])
            slug, logfile = m.group(1), m.group(2)
            log_basename = Path(logfile).stem
            if slug != log_basename:
                offenders.append(
                    f"{state_name}: lib CLI slug={slug!r} doesn't match "
                    f"log basename={log_basename!r}; S3 _ssm_logs/ tree "
                    f"will land logs under unexpected key prefix"
                )
            continue

        # Form (a): inline trap before `| tee` work line.
        work_idx = next(
            (i for i, c in enumerate(cmds) if _TEE_WORK_RE.search(c)),
            None,
        )
        if work_idx is None:
            continue  # short step, output fits in the 24KB cap
        logfile = _TEE_WORK_RE.search(cmds[work_idx]).group(1)
        trap_idx = next(
            (
                i
                for i, c in enumerate(cmds)
                if c.startswith("trap ")
                and "_ssm_logs" in c
                and logfile in c
                and c.rstrip().endswith("EXIT")
            ),
            None,
        )
        if trap_idx is None:
            offenders.append(f"{state_name}: no S3 EXIT trap for {logfile}")
            continue
        if trap_idx >= work_idx:
            offenders.append(
                f"{state_name}: trap at idx {trap_idx} not before "
                f"work line at idx {work_idx}"
            )
        if "|| true" not in cmds[trap_idx]:
            offenders.append(
                f"{state_name}: trap missing `|| true` (would override "
                f"the real exit status the SF Catch needs to see)"
            )
    assert not offenders, (
        f"{sf_path.name}: long SSM steps missing the S3 log-capture "
        f"surface:\n  - " + "\n  - ".join(offenders) + "\n\n"
        "Either add `\"trap 'aws s3 cp /var/log/X.log "
        "\\\"s3://alpha-engine-research/_ssm_logs/<slug>/...\\\" "
        "--only-show-errors || true' EXIT\"` immediately before the "
        "`| tee` work line (only works in plain `commands` arrays), "
        "OR switch the work line to `python -m alpha_engine_lib.ssm_log_capture "
        "run --slug X --log /var/log/X.log -- bash <launcher>` (required "
        "for `commands.$ States.Array(...)` states). See 2026-05-22 "
        "Friday-PM dry-pass + alpha-engine-lib PR #57."
    )
