"""Pins the ChronicGapSelfHeal fail-soft split in the WEEKDAY SF.

Origin: 2026-06-11 — the weekday pipeline FAILED. The chronic-polygon-gap
self-heal ran INLINE at the tail of MorningEnrich (``weekly_collector.py
--morning-enrich``). It is best-effort by design, but inline it could not
honour that: an unbounded ``yf.download`` hang ran out MorningEnrich's SSM
``executionTimeout`` and SIGKILLed (137) the whole command — AFTER the
load-bearing ``daily_append`` had already completed (~20 min of work). The
SF saw MorningEnrich ``Failed`` → ``HandleFailure`` → the weekday pipeline
failed with no predictions / planner / daemon that day.

The fix (per the standing rule — a best-effort downstream step must never
force re-running a completed upstream task; the same rule that split
MorningEnrich out of DataPhase1) moves the heal into its OWN fail-soft SF
state, ``ChronicGapSelfHeal``, that runs AFTER MorningEnrich and routes to
PredictorInference on EVERY terminal outcome (success, failure, timeout).

This test catches regressions like:
- Someone reroutes CheckMorningEnrichStatus(Success) straight back to
  PredictorInference, dropping the ChronicGapSelfHeal state.
- Someone makes ChronicGapSelfHeal (or its poll) fatal by routing a
  failure to HandleFailure instead of PredictorInference.
- Someone drops ``--skip-chronic-heal`` from the weekday MorningEnrich
  command, re-introducing the inline (un-isolated) heal.
- Someone points the heal state at the wrong entrypoint.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.sf_command_utils import extract_commands

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_HEAL = "ChronicGapSelfHeal"
_POLL = "WaitForChronicGap"
_CHECK = "CheckChronicGapStatus"
_WAIT = "ChronicGapWait"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


class TestStatePresence:
    @pytest.mark.parametrize("name", [_HEAL, _POLL, _CHECK, _WAIT])
    def test_state_exists(self, states, name):
        assert name in states, f"{name} missing from weekday SF States"


class TestChainOrdering:
    """CheckMorningEnrichStatus(Success) → ChronicGapSelfHeal →
    WaitForChronicGap → CheckChronicGapStatus(terminal) → PredictorInference."""

    def test_morning_enrich_success_routes_to_heal(self, states):
        success = [
            c["Next"]
            for c in states["CheckMorningEnrichStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == [_HEAL], (
            "MorningEnrich success must hand off to ChronicGapSelfHeal — the "
            "heal runs AFTER a completed (load-bearing) MorningEnrich."
        )

    def test_heal_routes_to_poll(self, states):
        assert states[_HEAL]["Next"] == _POLL

    def test_poll_routes_to_status_check(self, states):
        assert states[_POLL]["Next"] == _CHECK

    def test_status_inprogress_and_pending_loop_via_wait(self, states):
        nexts = {
            c["StringEquals"]: c["Next"]
            for c in states[_CHECK]["Choices"]
        }
        assert nexts["InProgress"] == _WAIT
        assert nexts["Pending"] == _WAIT
        assert states[_WAIT]["Next"] == _POLL

    def test_heal_precedes_predictor_inference(self, sf, states):
        """Walk the happy path from MorningEnrich success and assert
        ChronicGapSelfHeal is visited strictly before PredictorInference."""
        order: list[str] = []
        seen: set[str] = set()
        cur = _HEAL
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            order.append(cur)
            st = states[cur]
            if st.get("Type") == "Choice":
                # Status check: only InProgress/Pending loop; the terminal
                # path is the Default (= proceed to PredictorInference).
                cur = st.get("Default")
            else:
                cur = st.get("Next")
            if cur == "PredictorInference":
                order.append(cur)
                break
        assert _HEAL in order, order
        assert "PredictorInference" in order, order
        assert order.index(_HEAL) < order.index("PredictorInference"), order


class TestFailSoft:
    """A heal failure / hang / timeout must NEVER fail the pipeline — every
    terminal edge routes to PredictorInference, never HandleFailure."""

    def test_check_default_is_predictor_inference_not_handlefailure(self, states):
        # Inverse of CheckMorningEnrichStatus, whose Default is HandleFailure.
        assert states[_CHECK]["Default"] == "PredictorInference", (
            "Chronic-gap heal is best-effort: any terminal status (incl. "
            "failure) must proceed to PredictorInference, not HandleFailure."
        )

    def test_heal_catch_routes_to_predictor_inference(self, states):
        catches = states[_HEAL].get("Catch", [])
        assert catches, f"{_HEAL} must have a fail-soft Catch"
        for c in catches:
            assert c["Next"] == "PredictorInference", (
                f"{_HEAL} Catch must route to PredictorInference (fail-soft), "
                f"got {c['Next']}"
            )

    def test_poll_catch_routes_to_predictor_inference(self, states):
        catches = states[_POLL].get("Catch", [])
        assert catches, f"{_POLL} must have a fail-soft Catch"
        for c in catches:
            assert c["Next"] == "PredictorInference"

    def test_no_heal_state_routes_to_handlefailure(self, states):
        """No Next/Default/Catch target across the heal quartet may be
        HandleFailure (checks routing edges, not comment text)."""
        def _targets(o):
            out = []
            if isinstance(o, dict):
                for k, v in o.items():
                    if k in ("Next", "Default") and isinstance(v, str):
                        out.append(v)
                    else:
                        out.extend(_targets(v))
            elif isinstance(o, list):
                for x in o:
                    out.extend(_targets(x))
            return out

        for name in (_HEAL, _POLL, _CHECK, _WAIT):
            targets = _targets(states[name])
            assert "HandleFailure" not in targets, (
                f"{name} routes to HandleFailure {targets} — the heal is fail-soft."
            )


class TestSsmCommandShape:
    def test_heal_invokes_chronic_gap_heal_entrypoint(self, states):
        cmds = extract_commands(states[_HEAL])
        joined = "\n".join(cmds)
        assert "weekly_collector.py --chronic-gap-heal" in joined, (
            "ChronicGapSelfHeal must invoke the standalone --chronic-gap-heal "
            f"entrypoint. Commands:\n{joined}"
        )

    def test_heal_has_pipefail_and_deployed_exports(self, states):
        cmds = extract_commands(states[_HEAL])
        assert cmds[0] == "set -eo pipefail"
        joined = "\n".join(cmds)
        assert "export FLOW_DOCTOR_ENABLED=1" in joined
        assert "export ALPHA_ENGINE_DEPLOYED=1" in joined

    def test_heal_has_bounded_execution_timeout(self, states):
        et = states[_HEAL]["Parameters"]["Parameters"]["executionTimeout"]
        # SSM executionTimeout is a single-element string list.
        secs = int(et[0]) if isinstance(et, list) else int(et)
        assert 0 < secs <= 600, (
            "The heal state must carry a short, bounded SSM executionTimeout so "
            "a hang is capped in its OWN state (not MorningEnrich's). "
            f"got {secs}s"
        )

    def test_weekday_morning_enrich_skips_inline_heal(self, states):
        cmds = extract_commands(states["MorningEnrich"])
        joined = "\n".join(cmds)
        assert "--morning-enrich --skip-chronic-heal" in joined, (
            "The weekday MorningEnrich must pass --skip-chronic-heal so the "
            "inline heal is not double-run alongside the ChronicGapSelfHeal "
            "state (and MorningEnrich stays fully isolated from the heal)."
        )
