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
state, ``ChronicGapSelfHeal``, that runs AFTER the data phase and routes to
PredictorInference on EVERY terminal outcome (success, failure, timeout).

config#1767 (Phase 2): MorningEnrich + MorningArcticAppend were relocated OFF
the always-on trading box onto TWO independent ephemeral spot boxes via the
data-spot-dispatcher Lambda. ChronicGapSelfHeal stays on the trading box (it
is a small, fail-soft, best-effort heal — 300s timeout — not the load-bearing
data fetch config#1767 exists to move off-box) but was upgraded (config#1811)
to the liveness-aware SSM poll pattern (an independent SSM PingStatus check
alongside command status) so a wedged trading box is detected in ~1 minute
instead of blindly polling a frozen ``InProgress`` status for up to an hour
(the exact 2026-07-06 config#1807 incident shape).

This test catches regressions like:
- Someone reroutes the data-phase success straight back to PredictorInference,
  dropping the ChronicGapSelfHeal state.
- Someone makes ChronicGapSelfHeal (or its poll) fatal by routing a
  COMMAND_FAILED/POLL_BUDGET_EXHAUSTED verdict to HandleFailure instead of
  PredictorInference.
- Someone drops ``--skip-chronic-heal`` from the data-spot-dispatcher's
  morning-enrich workload, re-introducing the inline (un-isolated) heal.
- Someone points the heal state at the wrong entrypoint.
- Someone routes INSTANCE_UNRESPONSIVE fail-soft instead of stamp+force-stop
  (config#1811 carve-out — a wedged HOST is not a heal failure).
- Someone terminates the (persistent, reserved) trading box instead of
  stopping it on an unresponsive verdict.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.sf_command_utils import extract_commands

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_HEAL = "ChronicGapSelfHeal"
_INIT = "InitChronicGapPoll"
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
    @pytest.mark.parametrize("name", [_HEAL, _INIT, _POLL, _CHECK, _WAIT])
    def test_state_exists(self, states, name):
        assert name in states, f"{name} missing from weekday SF States"


class TestChainOrdering:
    """CheckMorningArcticAppendSpotStatus(Success) → CheckSkipChronicGapHeal →
    ChronicGapSelfHeal → InitChronicGapPoll → WaitForChronicGap →
    CheckChronicGapStatus(terminal) → PredictorInference (via
    CheckSkipPredictorInference)."""

    def test_heal_runs_after_data_spot_phase(self, states):
        # config#1767 (Phase 2): the enrich + daily_append were relocated OFF the
        # trading box onto two independent ephemeral spot boxes. The heal still
        # runs (behind its skip-gate) AFTER the spot data phase, before
        # predictions. The Arctic append spot's Success rejoins the trading path
        # at CheckSkipChronicGapHeal.
        append_success = [
            c["Next"]
            for c in states["CheckMorningArcticAppendSpotStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert append_success == ["CheckSkipChronicGapHeal"]
        assert states["CheckSkipChronicGapHeal"]["Default"] == _HEAL
        # The old on-trading enrich/append states are gone.
        assert "MorningEnrich" not in states
        assert "MorningArcticAppend" not in states

    def test_heal_routes_to_poll(self, states):
        # config#1811: an Init pass seeds the liveness-poll counters first.
        assert states[_HEAL]["Next"] == _INIT
        assert states[_INIT]["Next"] == _POLL

    def test_poll_routes_to_status_check(self, states):
        assert states[_POLL]["Next"] == _CHECK

    def test_status_inprogress_loops_via_wait(self, states):
        # config#1811: the ssm-liveness-poller folds Pending/Delayed/registering
        # into a single IN_PROGRESS verdict; only that verdict keeps polling.
        nexts = {c["StringEquals"]: c["Next"] for c in states[_CHECK]["Choices"]}
        assert nexts["IN_PROGRESS"] == _WAIT
        assert states[_WAIT]["Next"] == _POLL

    def test_unresponsive_host_is_not_fail_soft(self, states):
        """config#1811 carve-out: INSTANCE_UNRESPONSIVE is a HOST failure,
        not a heal failure — the same trading box every later step (planner,
        daemon) needs. Proceeding fail-soft would just fail at
        RunMorningPlanner after burning PredictorInference. It must route
        to the stamp → force-stop → HandleFailure chain instead."""
        nexts = {c["StringEquals"]: c["Next"] for c in states[_CHECK]["Choices"]}
        assert nexts["INSTANCE_UNRESPONSIVE"] == "StampChronicGapUnresponsive"
        assert (
            # ChronicGapSelfHeal runs on the persistent, reserved trading box
            # (not an ephemeral spot) — an unresponsive host is STOPPED, never
            # terminated.
            states["StampChronicGapUnresponsive"]["Next"]
            == "ForceStopUnresponsiveInstance"
        )

    def test_heal_precedes_predictor_inference(self, sf, states):
        """Walk the happy path from ChronicGapSelfHeal and assert
        PredictorInference is visited strictly after it."""
        order: list[str] = []
        seen: set[str] = set()
        cur = _HEAL
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            order.append(cur)
            st = states[cur]
            if st.get("Type") == "Choice":
                # Status check: only IN_PROGRESS loops; the terminal path is
                # the Default (= proceed toward PredictorInference).
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
    terminal edge routes toward PredictorInference, never HandleFailure."""

    # Since L4606 the forward target is the CheckSkipPredictorInference rerun
    # gate (whose Default runs PredictorInference) rather than PredictorInference
    # directly — still fail-soft: it proceeds toward predictions, never to
    # HandleFailure.
    _FWD = "CheckSkipPredictorInference"

    def test_check_default_proceeds_toward_predictions_not_handlefailure(self, states):
        assert states[_CHECK]["Default"] == self._FWD, (
            "Chronic-gap heal is best-effort: any terminal status (incl. "
            "failure) must proceed toward PredictorInference, not HandleFailure."
        )
        assert states[self._FWD]["Default"] == "PredictorInference"

    def test_heal_catch_proceeds_toward_predictions(self, states):
        catches = states[_HEAL].get("Catch", [])
        assert catches, f"{_HEAL} must have a fail-soft Catch"
        for c in catches:
            assert c["Next"] == self._FWD, (
                f"{_HEAL} Catch must proceed toward PredictorInference via "
                f"{self._FWD} (fail-soft), got {c['Next']}"
            )

    def test_poll_catch_proceeds_toward_predictions(self, states):
        catches = states[_POLL].get("Catch", [])
        assert catches, f"{_POLL} must have a fail-soft Catch"
        for c in catches:
            assert c["Next"] == self._FWD

    def test_no_heal_state_routes_to_handlefailure(self, states):
        """No Next/Default/Catch target across the heal quintet may be
        HandleFailure (checks routing edges, not comment text). The
        INSTANCE_UNRESPONSIVE host-failure path is exempt — it legitimately
        alerts via ForceStopUnresponsiveInstance -> HandleFailure, since a
        wedged trading box is a real incident, not a heal failure."""
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

        for name in (_HEAL, _INIT, _POLL, _CHECK, _WAIT):
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

    def test_heal_runs_on_trading_box(self, states):
        # ChronicGapSelfHeal stays on the persistent trading box (unlike
        # MorningEnrich/MorningArcticAppend, which moved to ephemeral spots).
        assert states[_HEAL]["Parameters"]["InstanceIds.$"] == "$.trading_instance_id"

    def test_data_spot_morning_enrich_workload_skips_inline_heal(self, states):
        # config#1767: the enrich command moved from an on-trading SSM state into
        # the data-spot-dispatcher Lambda's workload map. It must STILL pass
        # --skip-chronic-heal so the inline heal is not double-run alongside the
        # on-trading ChronicGapSelfHeal state (which stays on the trading box).
        disp = (
            _REPO_ROOT / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
        ).read_text()
        assert "--morning-enrich " in disp
        assert "--skip-chronic-heal" in disp, (
            "The data-spot morning-enrich workload must pass --skip-chronic-heal "
            "so the inline heal is not double-run with the on-trading heal state."
        )
        assert "--skip-arctic-append" in disp, (
            "The morning-enrich workload must also pass --skip-arctic-append — "
            "the Arctic append is its own separate spot workload."
        )
