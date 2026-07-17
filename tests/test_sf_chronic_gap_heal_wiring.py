"""Pins the REMOVAL of ChronicGapSelfHeal (+ its liveness-poll quintet and
skip-gate) from the WEEKDAY SF, and the rewiring left behind.

Origin: alpha-engine-config-I2717 (Brian ruling 2026-07-16, option 1 —
standalone daily heal) + the preopen half of alpha-engine-config-I2722. The
2026-07-16 incident (a heal firing inline ate the preopen's poll budget) led
to the decision to move the universe-gap self-heal AND the chronic-polygon-gap
heal (this state's logic) entirely OFF ``ne-preopen-trading-pipeline`` into a
single standalone EventBridge-triggered daily-heal spot job (~09:00 UTC,
weekly_collector.py ``--daily-heal``, see ``weekly_collector._run_daily_heal``
and the ``daily-heal`` workload in
``infrastructure/lambdas/data-spot-dispatcher/index.py``).

This file previously pinned ChronicGapSelfHeal's presence + fail-soft wiring
(see git history for that version, superseded here) — that state, its
liveness-poll quintet (``InitChronicGapPoll``/``WaitForChronicGap``/
``CheckChronicGapStatus``/``ChronicGapWait``/``StampChronicGapUnresponsive``),
and its skip-gate (``CheckSkipChronicGapHeal``) are now DELETED from
``step_function_daily.json`` — 7 states removed in total. The 5 states that
used to route into ``CheckSkipChronicGapHeal`` now route directly to
``CheckSkipPredictorInference`` instead: ``CheckSkipMorningEnrich``,
``CheckMorningEnrichSpotLaunched``, ``CheckMorningArcticAppendSpotLaunched``,
``CheckMorningArcticAppendSpotStatus`` (its "Success" choice), and
``PublishDataSpotFailureImmediate`` (both its plain ``Next`` and its Catch's
``Next``).

This test catches regressions like:
- Someone re-adds ChronicGapSelfHeal (or any state in its quintet/skip-gate)
  to this SF instead of the standalone daily-heal job — reopening the exact
  preopen poll-budget risk I2717 exists to close.
- Someone leaves a dangling reference to one of the removed state names.
- The 5 rewired predecessors drifting off ``CheckSkipPredictorInference``.
- ``ForceStopUnresponsiveInstance`` (SHARED with the code-freshness-gate and
  morning-planner liveness loops) getting accidentally deleted along with the
  chronic-gap quintet it used to also serve.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_REMOVED_STATES = [
    "CheckSkipChronicGapHeal",
    "ChronicGapSelfHeal",
    "InitChronicGapPoll",
    "WaitForChronicGap",
    "CheckChronicGapStatus",
    "ChronicGapWait",
    "StampChronicGapUnresponsive",
]


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


class TestChronicGapHealQuintetRemoved:
    @pytest.mark.parametrize("name", _REMOVED_STATES)
    def test_state_absent(self, states, name):
        assert name not in states, (
            f"{name} must NOT be in the weekday SF — the chronic-gap heal "
            "moved to the standalone --daily-heal job (alpha-engine-"
            "config-I2717/I2722)."
        )

    def test_shared_force_stop_state_survives(self, states):
        # ForceStopUnresponsiveInstance is SHARED with the code-freshness-gate
        # and morning-planner liveness loops — it must NOT be deleted along
        # with the chronic-gap quintet.
        assert "ForceStopUnresponsiveInstance" in states
        assert states["ForceStopUnresponsiveInstance"]["Resource"] == (
            "arn:aws:states:::aws-sdk:ec2:stopInstances"
        )

    def test_no_dangling_reference_to_removed_states(self, states):
        """No Next/Default/Catch edge anywhere in the SF may point at a
        removed state name — a dangling reference would be a hard ASL
        validation failure at deploy time."""
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

        for name, st in states.items():
            for t in _targets(st):
                assert t not in _REMOVED_STATES, (
                    f"{name} references removed state {t!r}"
                )


class TestRewiredPredecessorsRouteToPredictorInferenceGate:
    """The 5 states that used to enter CheckSkipChronicGapHeal now enter
    CheckSkipPredictorInference directly."""

    def test_check_skip_morning_enrich_skip_edge(self, states):
        choices = states["CheckSkipMorningEnrich"]["Choices"]
        assert len(choices) == 1
        assert choices[0]["Next"] == "CheckSkipPredictorInference"

    def test_morning_enrich_spot_launched_default(self, states):
        assert states["CheckMorningEnrichSpotLaunched"]["Default"] == (
            "CheckSkipPredictorInference"
        )

    def test_arctic_append_spot_launched_default(self, states):
        assert states["CheckMorningArcticAppendSpotLaunched"]["Default"] == (
            "CheckSkipPredictorInference"
        )

    def test_arctic_append_spot_status_success_edge(self, states):
        success = [
            c["Next"]
            for c in states["CheckMorningArcticAppendSpotStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["CheckSkipPredictorInference"]

    def test_publish_data_spot_failure_immediate_routes_forward(self, states):
        st = states["PublishDataSpotFailureImmediate"]
        assert st["Next"] == "CheckSkipPredictorInference"
        assert st["Catch"][0]["Next"] == "CheckSkipPredictorInference"


class TestPredictorInferenceGateUnchanged:
    """CheckSkipPredictorInference itself is untouched by this refactor —
    still the same skip-gate shape, still defaulting to PredictorInference."""

    def test_gate_shape(self, states):
        gate = states["CheckSkipPredictorInference"]
        assert gate["Type"] == "Choice"
        assert gate["Default"] == "PredictorInference"
        choices = gate["Choices"]
        assert len(choices) == 1
        variables = {c["Variable"] for c in choices[0]["And"]}
        assert variables == {"$.skip_predictor_inference"}
        assert choices[0]["Next"] == "CheckSkipMorningPlanner"
