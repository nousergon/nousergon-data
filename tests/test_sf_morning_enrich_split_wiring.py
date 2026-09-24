"""Pins the REMOVAL of MorningEnrich + DataPhase1 from the Saturday SF, and
the bounded collection-readiness wait that replaces them.

History: the preflight-task-split (2026-05-16) split the old monolithic
``DataPhase1`` (``spot_data_weekly.sh --data-only`` = morning-enrich THEN
phase1 on one spot) into a MorningEnrich quartet followed by a DataPhase1
quartet, so a phase1 failure never re-ran the 28-min morning-enrich. This file
pinned that split (see git history for that version, superseded here).

alpha-engine-config-I11269 (Brian's ruling (b), 2026-09-21): the standalone
``ne-data-collection-weekly`` state machine now runs morning-enrich and
weekly-phase-one (in that order, MaxConcurrency 1 — the split's ordering
property now lives in that machine's workload list, pinned by
tests/test_data_collection_stack.py). The v1 weekly SF no longer runs either
stage: ``CheckSkipMorningEnrich.Default`` enters ``WaitForCollectionManifests``
(I11264), a bounded poll of the collection-readiness probe over the run
manifests of the units the weekly legs read.

This test catches regressions like:
- Someone re-adds MorningEnrich / DataPhase1 (or any state of their
  quartets) — a v1 run would then double-write against the standalone
  schedule.
- A dangling reference to a removed state.
- The wait being bypassed on the no-skip path (ResearchPredictorParallel
  reachable from CheckSkipMorningEnrich.Default without passing the wait).
- The weekly wait failing OPEN: unlike the weekday/EOD waits, a not-ready
  weekly collection halts through NormalizeFailureContext exactly like the
  removed stages' error paths did, because research/training/backtest must
  not run on a universe the collector did not finish.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tests.sf_command_utils import extract_commands

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"

_REMOVED_STATES = [
    "MorningEnrich", "InitMorningEnrichPollCount", "WaitForMorningEnrich",
    "CheckMorningEnrichStatus", "MorningEnrichWait", "MorningEnrichPollWait",
    "MergeMorningEnrichPollCount", "MorningEnrichRetryGate", "MorningEnrichReissue",
    "ExtractMorningEnrichError", "ExtractMorningEnrichSubstrateLostError",
    "CheckSkipDataPhase1", "DataPhase1", "InitDataPhase1PollCount",
    "WaitForDataPhase1", "CheckDataPhase1Status", "DataPhase1Wait",
    "DataPhase1PollWait", "MergeDataPhase1PollCount", "DataPhase1RetryGate",
    "DataPhase1Reissue", "ExtractDataPhase1Error",
    "ExtractDataPhase1SubstrateLostError",
]

_WAIT_BLOCK = {
    "InitCollectionReadinessPoll", "SeedCollectionReadiness",
    "WaitForCollectionManifests", "CheckCollectionReadiness",
    "CheckCollectionReadinessBudget", "CollectionReadinessPollWait",
    "IncrementCollectionReadinessPoll", "ExtractCollectionNotReadyError",
}


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


def _targets(state: dict) -> list[str]:
    out: list[str] = []
    for k in ("Next", "Default"):
        if k in state:
            out.append(state[k])
    for c in state.get("Choices", []):
        if "Next" in c:
            out.append(c["Next"])
    for c in state.get("Catch", []):
        if "Next" in c:
            out.append(c["Next"])
    return out


class TestRemoval:
    @pytest.mark.parametrize("name", _REMOVED_STATES)
    def test_state_absent(self, states, name):
        assert name not in states, (
            f"{name} must not run from the v1 weekly SF — ne-data-collection-weekly owns it"
        )

    def test_the_removed_list_is_exactly_what_main_had(self):
        """Non-vacuity: every name above existed on the pre-cutover
        definition, so the absence pins test a real removal, not a typo."""
        try:
            old = subprocess.run(
                ["git", "show", "origin/main:infrastructure/step_function.json"],
                cwd=_REPO_ROOT, capture_output=True, text=True, check=True,
            ).stdout
        except (OSError, subprocess.CalledProcessError) as exc:
            pytest.skip(f"no origin/main to compare against: {exc}")
        old_states = json.loads(old)["States"]
        if "MorningEnrich" not in old_states:
            pytest.skip("origin/main already carries the removal")
        assert set(_REMOVED_STATES) <= set(old_states)

    def test_no_dangling_reference_to_removed_states(self, states):
        for name, st in states.items():
            for t in _targets(st):
                assert t not in _REMOVED_STATES, f"{name} references removed state {t!r}"

    @pytest.mark.parametrize("script", ["spot_morning_enrich.sh", "spot_data_phase1.sh"])
    def test_no_state_runs_the_removed_stage_scripts(self, states, script):
        for name, st in states.items():
            if not st.get("Resource", "").endswith("ssm:sendCommand"):
                continue
            assert script not in " ".join(extract_commands(st)), (
                f"{name} still runs {script} from the v1 weekly SF"
            )


class TestChainOrdering:
    def test_shell_run_default_still_enters_the_skip_gate(self, states):
        assert states["CheckShellRun"]["Default"] == "CheckSkipMorningEnrich"

    def test_substrate_gate_still_precedes_check_shell_run(self, states):
        # alpha-engine-config-I11268: SubstrateHealthGate sits on every
        # box-acquiring path ahead of CheckShellRun, so the skip edge below
        # bypasses no gate. HEALTHY -> on-spot preflight -> CheckShellRun.
        assert states["SubstrateHealthGate"]["Next"] == "CheckSubstrateHealthGate"
        assert states["RecordWeeklyPreflightOnSpot"]["Next"] == "CheckShellRun"
        assert states["CheckSubstrateHealthGate"]["Default"] == "ExtractSubstrateHealthGateError"

    def test_skip_morning_enrich_default_enters_the_wait(self, states):
        assert states["CheckSkipMorningEnrich"]["Default"] == "InitCollectionReadinessPoll"

    def test_skip_flag_routes_past_the_wait(self, states):
        choices = states["CheckSkipMorningEnrich"]["Choices"]
        assert len(choices) == 1
        assert {c["Variable"] for c in choices[0]["And"]} == {"$.skip_morning_enrich"}
        assert choices[0]["Next"] == "ResearchPredictorParallel"

    def test_the_no_skip_path_cannot_reach_research_without_the_wait(self, states):
        """Walk everything reachable from the skip gate's Default, stopping at
        ResearchPredictorParallel and at the failure chokepoint: the region is
        exactly the wait block, so no edge reaches the research legs except
        CheckCollectionReadiness's ready edge."""
        seen: set[str] = set()
        todo = [states["CheckSkipMorningEnrich"]["Default"]]
        while todo:
            n = todo.pop()
            if n in seen or n in ("ResearchPredictorParallel", "NormalizeFailureContext"):
                continue
            seen.add(n)
            todo.extend(_targets(states[n]))
        assert seen == _WAIT_BLOCK, sorted(seen ^ _WAIT_BLOCK)
        into_research = sorted(
            n for n in seen if "ResearchPredictorParallel" in _targets(states[n])
        )
        assert into_research == ["CheckCollectionReadiness"]


class TestWeeklyWaitFailsClosed:
    def test_not_ready_halts_through_the_failure_chokepoint(self, states):
        st = states["ExtractCollectionNotReadyError"]
        assert st["Type"] == "Pass"
        assert st["ResultPath"] == "$.error"
        assert st["Next"] == "NormalizeFailureContext"
        assert st["Parameters"]["phase"] == "CollectionReadiness"

    def test_both_not_ready_edges_reach_the_error_state(self, states):
        settled = [
            c for c in states["CheckCollectionReadiness"]["Choices"]
            if c["And"][0]["Variable"] == "$.collection_readiness.settled"
        ]
        assert [c["Next"] for c in settled] == ["ExtractCollectionNotReadyError"]
        budget = [
            c for c in states["CheckCollectionReadinessBudget"]["Choices"]
            if {leaf["Variable"] for leaf in c.get("And", [c])}
            == {"$.collection_readiness_poll.attempts"}
        ]
        assert [c["Next"] for c in budget] == ["ExtractCollectionNotReadyError"]

    def test_a_raising_probe_spends_a_poll_it_does_not_halt(self, states):
        catch = states["WaitForCollectionManifests"]["Catch"]
        assert [c["Next"] for c in catch] == ["CheckCollectionReadinessBudget"]


class TestShellRunRunsTheWaitDry:
    """The Friday shell run (shell_run=true) ran MorningEnrich/DataPhase1 with
    --preflight-only. Their replacement runs dry too: one probe, then on. A
    Friday has no Saturday collection to wait for, so a real wait would burn
    its whole budget and then fail the preflight on an expected condition."""

    @staticmethod
    def _is_shell_run(rule: dict) -> bool:
        return {c["Variable"] for c in rule.get("And", [])} == {"$.shell_run"}

    def test_one_dry_probe_then_continue(self, states):
        choices = states["CheckCollectionReadiness"]["Choices"]
        order = [
            "ready" if c["And"][0]["Variable"] == "$.collection_readiness.ready"
            else "shell_run" if self._is_shell_run(c)
            else "settled"
            for c in choices
        ]
        # ready first (a green probe is a green probe), then the dry exit
        # BEFORE the settled-not-ok halt: a Friday verdict never halts.
        assert order == ["ready", "shell_run", "settled"]
        assert choices[1]["Next"] == "ResearchPredictorParallel"

    def test_a_raising_dry_probe_fails_the_preflight_now(self, states):
        choices = states["CheckCollectionReadinessBudget"]["Choices"]
        dry = [c for c in choices if self._is_shell_run(c)]
        assert [c["Next"] for c in dry] == ["ExtractCollectionNotReadyError"]
        assert choices.index(dry[0]) == 0  # before the attempts bound

    def test_the_real_run_is_unaffected(self, states):
        """shell_run absent: neither dry rule can fire, so the pinned
        budget/verdict edges above are the whole behaviour."""
        for name in ("CheckCollectionReadiness", "CheckCollectionReadinessBudget"):
            for c in states[name]["Choices"]:
                if self._is_shell_run(c):
                    assert c["And"][0] == {"Variable": "$.shell_run", "IsPresent": True}
                    assert c["And"][1]["BooleanEquals"] is True
