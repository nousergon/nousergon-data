"""Regression test for 5/23-SF P0 sweep (e) — every `getCommandInvocation`
poll Task must retry on BOTH error-name forms:

  - `Ssm.InvocationDoesNotExistException` (AWS SDK v1 / lambda-layer form)
  - `Ssm.InvocationDoesNotExist` (AWS SDK v2 / aws-sdk:ssm direct form)

The 2026-05-23 SF scheduled execution's event 16 surfaced an
`InvocationDoesNotExist` race at the start of `WaitForMorningEnrich`. The
existing Retry policy had only the `Exception`-suffixed form, so the
error fell through to the Catch block instead of retrying. SF Catch
absorbed it (subsequent state continued via the WaitFor polling loop),
but the race surface remains a substrate weakness for any future
SF cycle.

Pins:
  1. Every Retry block that lists `Ssm.InvocationDoesNotExistException`
     ALSO lists `Ssm.InvocationDoesNotExist`.
  2. Every `getCommandInvocation` Task has a Retry policy covering
     both error forms.
  3. Coverage extends across all 3 SF JSONs (Saturday + weekday + EOD).
"""
from __future__ import annotations

import json
import pathlib

import pytest


SF_JSONS = [
    "infrastructure/step_function.json",          # Saturday
    "infrastructure/step_function_daily.json",    # weekday
    "infrastructure/step_function_eod.json",      # EOD
]


def _iter_states(definition: dict):
    """Yield (state_name, state_def, parent_path) for every Task state
    in the SF definition, recursing into Parallel/Branches + Map/Iterator."""
    def _walk(states: dict, path: str = ""):
        for name, state in states.items():
            full_path = f"{path}.{name}" if path else name
            yield full_path, state
            if state.get("Type") == "Parallel":
                for i, branch in enumerate(state.get("Branches", [])):
                    yield from _walk(branch.get("States", {}), f"{full_path}.branch{i}")
            elif state.get("Type") == "Map":
                iterator = state.get("Iterator") or state.get("ItemProcessor", {})
                yield from _walk(iterator.get("States", {}), f"{full_path}.iterator")
    yield from _walk(definition.get("States", {}))


def _find_get_command_invocation_states(definition: dict) -> list[tuple[str, dict]]:
    """Return [(state_path, state_def), ...] for every Task with the
    getCommandInvocation Resource (with or without arn-prefix variants)."""
    found = []
    for path, state in _iter_states(definition):
        if state.get("Type") != "Task":
            continue
        resource = state.get("Resource", "")
        if "ssm:getCommandInvocation" in resource:
            found.append((path, state))
    return found


@pytest.mark.parametrize("sf_path", SF_JSONS)
def test_every_get_command_invocation_retries_both_error_forms(sf_path):
    """Every `getCommandInvocation` Task MUST retry on both
    InvocationDoesNotExistException AND InvocationDoesNotExist."""
    repo_root = pathlib.Path(__file__).parent.parent
    with open(repo_root / sf_path) as fh:
        definition = json.load(fh)
    targets = _find_get_command_invocation_states(definition)
    assert targets, f"No getCommandInvocation Tasks found in {sf_path}"

    missing_both: list[str] = []
    missing_bare: list[str] = []
    for path, state in targets:
        retries = state.get("Retry", [])
        # Find any Retry block whose ErrorEquals lists either form.
        invocation_errors = set()
        for retry in retries:
            errors = retry.get("ErrorEquals", [])
            invocation_errors |= {
                e for e in errors if "InvocationDoesNotExist" in e
            }
        if not invocation_errors:
            missing_both.append(path)
            continue
        if "Ssm.InvocationDoesNotExist" not in invocation_errors:
            missing_bare.append(path)

    assert not missing_both, (
        f"{sf_path}: {len(missing_both)} getCommandInvocation Task(s) lack any "
        f"InvocationDoesNotExist retry:\n  " + "\n  ".join(missing_both)
    )
    assert not missing_bare, (
        f"{sf_path}: {len(missing_bare)} getCommandInvocation Task(s) only retry "
        f"the Exception-suffixed form (`Ssm.InvocationDoesNotExistException`) and "
        f"miss the bare form (`Ssm.InvocationDoesNotExist`) — the 5/23 SF event-16 "
        f"race surface:\n  " + "\n  ".join(missing_bare)
    )


@pytest.mark.parametrize("sf_path", SF_JSONS)
def test_retry_blocks_with_either_form_list_both(sf_path):
    """Any Retry block that already lists `Ssm.InvocationDoesNotExistException`
    MUST also list `Ssm.InvocationDoesNotExist` — regression-pin against a
    future SF JSON edit that drops the bare form by mistake."""
    repo_root = pathlib.Path(__file__).parent.parent
    with open(repo_root / sf_path) as fh:
        content = fh.read()
    # String-level pin so the assertion catches even Retry blocks that
    # appear outside the getCommandInvocation Task contexts (e.g. nested
    # parallel branches).
    n_exception = content.count('"Ssm.InvocationDoesNotExistException"')
    # The bare form's distinct string `"Ssm.InvocationDoesNotExist"` (with
    # CLOSING quote — Exception form has Exception" instead).
    n_bare = content.count('"Ssm.InvocationDoesNotExist"')
    assert n_bare == n_exception, (
        f"{sf_path}: {n_exception} Exception-suffixed occurrences but "
        f"{n_bare} bare-form occurrences. Every Retry block with the "
        f"Exception form must also list the bare form per 5/23 SF event-16 fix."
    )
