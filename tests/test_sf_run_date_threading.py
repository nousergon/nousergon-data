"""Locks the run_date-at-SF-start fix (fix/sf-stamp-run-date).

2026-05-17 Saturday SF failed at the Evaluator: backtest/{date}/ was
keyed off a per-stage wall-clock date, so a ~2.5h run that straddled
UTC midnight split (Backtester wrote backtest/2026-05-17/, Evaluator's
post-midnight spot looked in backtest/2026-05-18/). Fix: stamp run_date
ONCE at InitializeInput from $$.Execution.StartTime and thread
`export RUN_DATE='<$.run_date>'` into every spot stage's SSM command.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.sf_command_utils import extract_commands

_SF_PATH = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


def test_initialize_input_stamps_run_date_from_execution_start(states):
    """InitializeInput stamps run_date = date($$.Execution.StartTime).
    Constant for the whole execution → every stage gets ONE date."""
    expr = states["InitializeInput"]["Parameters"]["merged.$"]
    assert "$$.Execution.StartTime" in expr
    assert "run_date" in expr
    # date portion via StringSplit on 'T' then ArrayGetItem(...,0)
    assert "States.StringSplit($$.Execution.StartTime,'T')" in expr
    assert "States.ArrayGetItem(" in expr


def test_initialize_input_user_run_date_still_wins(states):
    """$$.Execution.Input is merged LAST so a manually passed run_date
    overrides the stamp (same user-input-wins semantics as sns default)."""
    expr = states["InitializeInput"]["Parameters"]["merged.$"]
    assert expr.rstrip().endswith("$$.Execution.Input,false)")


@pytest.mark.parametrize("stage", ["Backtester", "Parity", "Evaluator"])
def test_spot_stage_exports_run_date_before_launch(states, stage):
    """Each spot stage injects `export RUN_DATE=<run_date>` immediately
    before the spot_backtest.sh launch so spot_backtest.sh resolves the
    SF-declared date from env instead of recomputing wall-clock."""
    cmds = extract_commands(states[stage])
    rd_idx = next(
        i for i, c in enumerate(cmds)
        if c.startswith("export RUN_DATE=")
    )
    spot_idx = next(
        i for i, c in enumerate(cmds) if "spot_backtest.sh" in c
    )
    assert rd_idx < spot_idx, (
        f"{stage}: export RUN_DATE must precede the spot_backtest.sh launch"
    )
    # value is threaded from the SF-stamped $.run_date (States.Format)
    raw_expr = states[stage]["Parameters"]["Parameters"]["commands.$"]
    assert "States.Format('export RUN_DATE=" in raw_expr
    assert "$.run_date" in raw_expr
