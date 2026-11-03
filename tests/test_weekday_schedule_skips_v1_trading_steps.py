"""The weekday preopen run skips v1's trading steps but still boots the box.

Brian's ruling of 2026-11-01 (Crucible v2 phase 4): paper orders move from the
v1 executor to crucible-trader. The v1 steps that build and execute a book
(PredictorInference, RunMorningPlanner, RunDaemon) are skipped through the
state machine's own per-task gates. The run itself must keep starting: it is
what boots the trading box that the v2 trader's session, reconcile and
shadow-book timers run on.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ORCH = _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
_SF = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

# flag -> (gate Choice, the stage it skips)
_SKIPS = {
    "skip_predictor_inference": ("CheckSkipPredictorInference", "PredictorInference"),
    "skip_morning_planner": ("CheckSkipMorningPlanner", "RunMorningPlanner"),
    "skip_run_daemon": ("CheckSkipRunDaemon", "RunDaemon"),
}


def _schedule_block() -> str:
    text = _ORCH.read_text()
    start = text.index("  WeekdayPipelineSchedule:\n")
    end = text.index("\n  # ----", start)
    return text[start:end]


def _schedule_input() -> dict[str, object]:
    m = re.search(r'"Input": "(.*)"\n', _schedule_block())
    assert m, "WeekdayPipelineSchedule Input not found: CFN shape changed"
    return json.loads(m.group(1).replace('\\"', '"'))


def test_the_schedule_passes_every_v1_trading_skip_flag() -> None:
    payload = _schedule_input()
    for flag in _SKIPS:
        assert payload.get(flag) is True, f"{flag} missing from the weekday schedule input"


def test_the_schedule_stays_enabled_because_it_boots_the_v2_trader_box() -> None:
    assert re.search(r"\n      State: ENABLED\n", _schedule_block())


def test_each_flag_routes_past_exactly_its_stage() -> None:
    states = json.loads(_SF.read_text())["States"]
    for flag, (gate, stage) in _SKIPS.items():
        choice = states[gate]
        conds = choice["Choices"][0]["And"]
        assert {c["Variable"] for c in conds} == {f"$.{flag}"}
        assert choice["Choices"][0]["Next"] != stage
        assert choice["Default"] == stage


def test_the_box_still_starts_before_the_skipped_stages() -> None:
    states = json.loads(_SF.read_text())["States"]
    assert states["StartExecutorEC2"]["Type"] == "Task"
    assert "StartExecutorEC2" not in {gate for gate, _ in _SKIPS.values()}
