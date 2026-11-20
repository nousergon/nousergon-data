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
# This set may only ever shrink; alpha-engine-config-I8703 closes the final EOD
# entries by routing them through the existing liveness poller contract.
_UNCOVERED_POLLERS: set[tuple[str, str]] = set()

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


_AFFECTED_EOD_POLLERS = {
    ("eod", "WaitForCaptureSnapshot", "snapshot_result.poll", "snapshot_result"),
    (
        "eod",
        "WaitForRefreshExecutorDeploy",
        "refresh_executor_deploy_result.poll",
        "refresh_executor_deploy_result",
    ),
    ("eod_reconcile", "WaitForEOD", "eod_result.poll", "eod_result"),
    (
        "eod_reconcile",
        "WaitForRefreshExecutorDeploy",
        "refresh_executor_deploy_result.poll",
        "refresh_executor_deploy_result",
    ),
}


def test_affected_eod_pollers_use_the_bounded_liveness_contract() -> None:
    """The shared poller normalises optional SSM output fields before ASL sees them.

    Its handler tests exercise missing stdout and empty tails.  This wiring guard
    proves every affected EOD state consumes that safe contract, retains both
    bounded diagnostics, and never maps the raw stdout field into state.
    """
    for which, name, slot, result in _AFFECTED_EOD_POLLERS:
        state = _load(which)["States"][name]
        assert state["Resource"] == "arn:aws:states:::lambda:invoke"
        payload = state["Parameters"]["Payload"]
        assert payload["command_id.$"] == f"$.{result}.Command.CommandId"
        assert payload["instance_id.$"] == "$.trading_instance_id[0]"
        assert payload["attempts.$"] == f"$.{slot}.attempts"
        assert payload["ping_misses.$"] == f"$.{slot}.ping_misses"
        assert "alpha-engine-ssm-liveness-poller" in state["Parameters"]["FunctionName"]
        assert state["ResultSelector"]["detail.$"] == "$.Payload.detail"
        assert (
            state["ResultSelector"]["StandardErrorContent.$"] == "$.Payload.stderr_tail"
        )
        assert "StandardOutputContent.$" not in state["ResultSelector"]
        assert "stdout_tail.$" not in state["ResultSelector"]


def test_affected_eod_failures_copy_diagnostic_detail_into_error() -> None:
    """Terminal pages serialize an error that names what the script reported."""
    expected = {
        ("eod", "SnapshotStatusError", "snapshot_result.poll"),
        (
            "eod",
            "RefreshExecutorDeployStatusError",
            "refresh_executor_deploy_result.poll",
        ),
        ("eod_reconcile", "EODStatusError", "eod_result.poll"),
        (
            "eod_reconcile",
            "RefreshExecutorDeployStatusError",
            "refresh_executor_deploy_result.poll",
        ),
    }
    for which, name, slot in expected:
        params = _load(which)["States"][name]["Parameters"]
        assert params["detail.$"] == f"$.{slot}.detail"


def test_eod_liveness_classification_is_observe_only_for_five_cycles() -> None:
    """Status keeps routing outcomes while verdict is visible but unread."""
    for which, name, _, _ in _AFFECTED_EOD_POLLERS:
        definition = _load(which)
        state = definition["States"][name]
        assert "OBSERVE mode" in state["Comment"]
        assert "5 consecutive EOD cycles" in state["Comment"]
        result_path = state["ResultPath"]
        choice = definition["States"][state["Next"]]
        assert all(
            branch["Variable"] == f"{result_path}.Status"
            for branch in choice["Choices"]
        )
        assert "verdict.$" in state["ResultSelector"]


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
