"""Fatal SSM-stage diagnostics must reach whoever reads the failure alert.

alpha-engine-config-I8685. `HandleFailure` publishes `States.JsonToString($)`
to SNS, so a stage's failure is only as legible as the fields its poll result
carries. On 2026-08-26 the preopen lost a trading session and the notification
carried `detail` ("SSM command <id> terminal status Failed (rc=1)") and
`StandardErrorContent` ("failed to run commands: exit status 1") — the SSM
agent's own generic line. The sentence that explained it,
``CODE-STALE-AFTER-HEAL alpha-engine branch=main head=20ca44aa
upstream=c5edc712``, was on stdout, which nothing in the notification path
carried. Diagnosis began with `get-execution-history` and then
`ssm list-command-invocations --details`.

Two independent rules, because either alone leaves a hole:

1. **Stderr discipline** (`test_fatal_diagnostics_are_written_to_stderr`) —
   a diagnostic on a path that ends in a non-zero exit goes to stderr, which
   the poll result carries. This is the contract.
2. **Diagnostic coverage** (`test_every_ssm_poller_carries_a_diagnostic_field`)
   — every poller surfaces at least one field containing what the script
   said, so a stage that gets rule 1 wrong still surfaces something. This is
   the backstop for when the contract is not kept.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFS = {
    "daily": _REPO_ROOT / "infrastructure" / "step_function_daily.json",
    "eod": _REPO_ROOT / "infrastructure" / "step_function_eod.json",
    # alpha-engine-config-I11269: the collector-gated half of the post-close run.
    "eod_reconcile": _REPO_ROOT / "infrastructure" / "step_function_eod_reconcile.json",
}

# States whose poll result carries no field containing script output.
#
# This list may only ever SHRINK. It is not a carve-out for "these are fine" —
# every entry is a stage whose failure reaches the operator as an exit code and
# nothing else, which is strictly worse than the 2026-08-26 preopen failure that
# at least carried the SSM agent's generic stderr line.
#
# All three use the DIRECT `aws-sdk:ssm:getCommandInvocation` integration rather
# than the ssm-liveness-poller Lambda, and their ResultSelectors lift only
# Status / ResponseCode / StatusDetails / CommandId / InstanceId. The one-line
# fix — adding `StandardErrorContent.$` and `StandardOutputContent.$` — is NOT
# obviously safe: a ResultSelector `.$` path that is absent in the response
# raises States.Runtime, killing the stage it was meant to explain, and the EOD
# pipeline cannot be rehearsed on demand (sf-pipeline-policy.md §7a). Tracked
# with the two options in alpha-engine-config-I8703; consolidating them onto the
# Lambda poller, which normalises absent fields, is the recommended route.
#
# alpha-engine-config-I11269 (post-close split, 2026-09-30): no new poller was
# written. WaitForEOD MOVED, byte-identical, into the reconcile machine, and
# WaitForRefreshExecutorDeploy is the same state carried by BOTH machines (each
# boots the box, so each refreshes the checkout) — the uncovered code is the
# same three pollers, now counted per machine they live in.
_UNCOVERED_POLLERS = {
    ("eod", "WaitForCaptureSnapshot"),
    ("eod", "WaitForRefreshExecutorDeploy"),
    ("eod_reconcile", "WaitForEOD"),
    ("eod_reconcile", "WaitForRefreshExecutorDeploy"),
}

# A field whose value plausibly carries what the script printed.
_DIAGNOSTIC_FIELDS = ("StandardErrorContent", "StandardOutputContent", "detail")


def _load(which: str) -> dict:
    return json.loads(_DEFS[which].read_text())


def _shell_scripts(state: dict) -> list[str]:
    """Both spellings: an inline `commands` array and a `States.Array(...)` ref."""
    inner = (state.get("Parameters") or {}).get("Parameters") or {}
    commands = inner.get("commands")
    if isinstance(commands, list):
        return [c for c in commands if isinstance(c, str)]
    ref = inner.get("commands.$")
    if isinstance(ref, str):
        return re.findall(r"'((?:[^'\\]|\\.)*)'", ref)
    return []


def test_fatal_diagnostics_are_written_to_stderr() -> None:
    offenders = []
    for which in _DEFS:
        for name, state in _load(which)["States"].items():
            for line in _shell_scripts(state):
                if not re.search(r"\b(echo|printf)\b", line):
                    continue
                if not re.search(r"exit\s+[1-9]", line):
                    continue
                if ">&2" not in line:
                    offenders.append(f"{which}:{name}: {line[:160]}")

    assert not offenders, (
        "these lines print a diagnostic on a path that exits non-zero, but "
        "write it to stdout — which the SNS failure notification does not "
        "carry, so the operator gets an exit code and nothing else:\n  "
        + "\n  ".join(offenders)
    )


def test_every_ssm_poller_carries_a_diagnostic_field() -> None:
    uncovered = set()
    for which in _DEFS:
        for name, state in _load(which)["States"].items():
            if "getCommandInvocation" not in str(state.get("Resource", "")):
                continue
            selector = json.dumps(state.get("ResultSelector") or {})
            if not any(f in selector for f in _DIAGNOSTIC_FIELDS):
                uncovered.add((which, name))

    new = uncovered - _UNCOVERED_POLLERS
    assert not new, (
        "these SSM pollers surface no field carrying what the script said, so "
        "their failures reach the operator as an exit code and nothing else: "
        f"{sorted(new)}. Add StandardErrorContent/StandardOutputContent to the "
        "ResultSelector, or route the stage through the ssm-liveness-poller "
        "Lambda (which normalises absent fields and inlines both tails into "
        "`detail`)."
    )

    fixed = _UNCOVERED_POLLERS - uncovered
    assert not fixed, (
        f"{sorted(fixed)} now carries a diagnostic field — remove it from "
        "_UNCOVERED_POLLERS. That list may only shrink, and leaving a fixed "
        "entry in it overstates the remaining gap."
    )


def test_output_tails_never_enter_sf_state_via_a_resultselector() -> None:
    """The tails ride in `detail`, and must NOT be mapped as their own fields.

    This is the constraint that makes the fix a two-line inline rather than the
    obvious `StandardOutputContent.$` ResultSelector entry. SSM stdout is ~24 KB
    per invocation and `HandleFailure` serialises the whole of `$` via
    `States.JsonToString`, so mapping it re-opens the `States.DataLimitExceeded`
    class that killed the Saturday SF twice (2026-06-06
    `ResearchPredictorParallel`, 2026-06-19 `WaitForEvaluator`) and was closed on
    the weekday and EOD pipelines in config#1163. See
    `test_sf_poll_resultselector.py`, which guards that invariant directly.

    `detail` is bounded by construction and is written only on the terminal
    COMMAND_FAILED branch, so it appears once per failed stage rather than on
    every poll iteration.
    """
    for which in _DEFS:
        for name, state in _load(which)["States"].items():
            selector = json.dumps(state.get("ResultSelector") or {})
            assert "stdout_tail" not in selector, (
                f"{which}:{name} maps the poller's stdout_tail into SF state. "
                "It must reach the operator through `detail` instead — mapping "
                "it re-opens the 256 KB DataLimitExceeded class."
            )
            assert "StandardOutputContent" not in selector, (
                f"{which}:{name} maps StandardOutputContent — same class."
            )
