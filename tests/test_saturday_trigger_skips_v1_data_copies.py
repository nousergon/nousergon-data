"""The Saturday cadence run leaves D15/D16/D46 to their v2 writer (alpha-engine-config-I11832).

The v2 weekly schedule (`nousergon-data-collection.yaml :: WeeklySchedule`)
runs `alternative-phase-two` (D15) and `rag-weekly-ingestion` (D16 + D46): the
same commands the v1 weekly SF's `DataPhase2` and `RAGIngestion` states run,
against the same S3 keys and RAG tables. With both left on, the two writers
overlap on a Saturday: phase 2's same-date auto-skip only fires on a COMPLETED
prior run's ok marker, so a v1 DataPhase2 that starts while v2's
alternative-phase-two is mid-crawl runs in full, both crawl Finnhub at once
against one rate limit, and whichever finishes later owns the per-ticker JSON.

The cadence trigger therefore passes `skip_data_phase2` and `skip_rag_ingestion`.
Four properties keep that honest:

1. **The flags are present** in the one source of truth for the cadence input.
2. **They are sufficient.** Each flag's gate routes past its stage, and the
   stage is reachable from nothing else but its own retry loop, so a true flag
   means the stage cannot run.
3. **Nothing later in the v1 run reads the stage's result**, so skipping it
   cannot change what this execution computes.
4. **The skip is not a silent removal.** The v2 schedule still names both
   workloads, and both units in its verify_units. If either leaves the v2
   schedule, this test fails, because the v1 skip would then leave the unit
   with no writer at all.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ORCH = _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
_COLLECTION = _REPO_ROOT / "infrastructure" / "cloudformation" / "nousergon-data-collection.yaml"
_SF = _REPO_ROOT / "infrastructure" / "step_function.json"

# flag -> (gate Choice, the stage it skips, the gate's skip target,
#          the v2 workload that writes the same unit, the units it covers)
_SKIPS = {
    "skip_data_phase2": (
        "CheckSkipDataPhase2", "DataPhase2", "CheckSkipEvalJudge",
        "alternative-phase-two", ("D15",),
    ),
    "skip_rag_ingestion": (
        "CheckSkipRAGIngestion", "RAGIngestion", "CheckSkipRegimeRetrospectiveEval",
        "rag-weekly-ingestion", ("D16", "D46"),
    ),
}


def _saturday_target_input() -> dict:
    text = _ORCH.read_text()
    body = text[text.index("SaturdayTrigger:"):]
    m = re.search(r"\n          Input: !Sub \|\n(.*?)\n\n", body, re.S)
    assert m, "SaturdayTrigger target Input block not found — CFN shape changed"
    return json.loads(m.group(1))


def _weekly_schedule_input() -> dict:
    text = _COLLECTION.read_text()
    body = text[text.index("  WeeklySchedule:"):]
    m = re.search(r"\n        Input: '(.*?)'\n", body)
    assert m, "WeeklySchedule Input not found — collection stack shape changed"
    return json.loads(m.group(1))


def _all_states() -> dict:
    """Every state in the weekly definition, Parallel branches flattened."""
    out: dict = {}

    def walk(states: dict) -> None:
        for name, st in states.items():
            out[name] = st
            for branch in st.get("Branches", []):
                walk(branch["States"])

    walk(json.loads(_SF.read_text())["States"])
    return out


def _successors(st: dict) -> set[str]:
    nxt = {st[k] for k in ("Next", "Default") if k in st}
    nxt |= {c["Next"] for c in st.get("Choices", [])}
    nxt |= {c["Next"] for c in st.get("Catch", [])}
    return nxt


def _reachable(states: dict, start: str) -> set[str]:
    seen, todo = set(), [start]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        todo.extend(_successors(states[name]))
    return seen


@pytest.mark.parametrize("flag", sorted(_SKIPS))
def test_cadence_input_skips_the_v1_copy(flag: str) -> None:
    payload = _saturday_target_input()
    assert payload.get(flag) is True, (
        f"the Saturday cadence trigger no longer passes {flag}: true, so the v1 "
        "weekly SF writes the same unit as the v2 weekly schedule again "
        "(alpha-engine-config-I11832)."
    )


def test_cadence_input_keeps_its_other_fields() -> None:
    payload = _saturday_target_input()
    assert payload["pipeline_role"] == "weekly"
    assert payload["skip_parity"] is True
    assert "sns_topic_arn" in payload
    assert "ec2_instance_id" not in payload


@pytest.mark.parametrize("flag", sorted(_SKIPS))
def test_the_flag_alone_keeps_the_stage_from_running(flag: str) -> None:
    gate, stage, skip_target, _, _ = _SKIPS[flag]
    states = _all_states()
    gate_state = states[gate]
    taken = [
        c["Next"] for c in gate_state["Choices"]
        if any(
            sub.get("Variable") == f"$.{flag}" and sub.get("BooleanEquals") is True
            for sub in c.get("And", [c])
        )
    ]
    assert taken == [skip_target], f"{gate} must route {flag}=true to {skip_target}"
    assert gate_state["Default"] == stage

    # The only edges INTO the stage are the gate's Default and its own
    # retry loop, so a true flag leaves no path that runs it.
    own_loop = _reachable(states, stage) - _reachable(states, skip_target)
    entries = {
        name for name, st in states.items()
        if stage in _successors(st) and name not in own_loop
    }
    assert entries == {gate}, f"{stage} is entered from {sorted(entries)}, not only {gate}"


@pytest.mark.parametrize("flag", sorted(_SKIPS))
def test_nothing_after_the_stage_reads_its_result(flag: str) -> None:
    _, stage, skip_target, _, _ = _SKIPS[flag]
    states = _all_states()
    result_path = states[stage]["ResultPath"]  # e.g. $.data_phase2_result
    prefix = result_path.removeprefix("$.").removesuffix("_result")
    own_loop = _reachable(states, stage) - _reachable(states, skip_target)
    readers = sorted(
        name for name, st in states.items()
        if name not in own_loop
        and re.search(
            rf"\$\.{re.escape(prefix)}_",
            json.dumps({k: v for k, v in st.items() if k not in ("Comment", "Branches")}),
        )
    )
    assert readers == [], (
        f"{readers} read {prefix}_* outside {stage}'s own loop; skipping {stage} "
        "on the cadence run would change what those states see"
    )


@pytest.mark.parametrize("flag", sorted(_SKIPS))
def test_the_v2_schedule_still_writes_the_unit(flag: str) -> None:
    _, _, _, workload, units = _SKIPS[flag]
    weekly = _weekly_schedule_input()
    assert workload in weekly["workloads"], (
        f"the v2 weekly schedule no longer runs {workload}; with {flag} on the "
        "cadence trigger the unit would have NO writer. Drop the flag first."
    )
    for unit in units:
        assert unit in weekly["verify_units"], f"{unit} no longer verified by the v2 weekly"
