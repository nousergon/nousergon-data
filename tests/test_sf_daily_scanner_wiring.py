"""The Scanner is NOT in the weekday pipeline — it forms its cuts WEEKLY.

**Brian ruling 2026-08-20 (`alpha-engine-config-I7811`):** *"the scanner should
be running weekly, generating the two groups, and then this should be what gets
passed into research and/or predictor - on a weekly basis."*

This file used to pin the opposite. `alpha-engine-config-I6494` Ruling A put a
Scanner state on the preopen pipeline on 2026-08-04, and its stated reason was:

    "Predictor daily inference still depends on `universe_membership` freshness
    (`MEMBERSHIP_MAX_AGE_DAYS`); a weekday-stale cut is the failure mode I4818
    was opened for."

Measured 2026-08-20 against the tree: `crucible-predictor/inference/stages/
load_universe.py:113` sets `MEMBERSHIP_MAX_AGE_DAYS = 10` and walks back that
far. **A weekly cut is at most 7 days old and never trips the gate that
justified daily formation.** The cadence was set by a threshold, and the
threshold permits weekly.

The second reason is what it cost. Daily formation is what put the Scanner on
the 06:30-market-open critical path at all, and on 2026-08-20 it hit its 450s
Lambda ceiling there and degraded the preopen run — after the 2026-08-17 scoring
merges took its dominant read from ~(tens of symbols x 107 days) to ~(904
symbols x 569 days). Second ceiling collision in nine days
(`alpha-engine-config-I7812`).

So this file is now a RETIREMENT GUARD, in the shape `I7817` asks for: it fails
if the Scanner comes back to the weekday pipeline without a ruling, and it
checks that the weekly one is still there — a removal that also lost the weekly
formation would satisfy "not in the daily pipeline" while silently ending
scanning altogether.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"
_WEEKLY_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"

# The states I7811 removed from the weekday pipeline.
_RETIRED_WEEKDAY_STATES = (
    "Scanner",
    "CheckSkipScanner",
    "SetScannerDegradedFlag",
    "PublishScannerFailureImmediate",
)


@pytest.fixture(scope="module")
def daily() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def weekly() -> dict:
    return json.loads(_WEEKLY_SF_PATH.read_text())


def _walk(states: dict):
    """Every state in a definition, including inside Parallel branches and Map
    processors — a state re-added inside a branch is still re-added."""
    for name, body in states.items():
        yield name, body
        for branch in body.get("Branches", []) or []:
            yield from _walk(branch.get("States", {}))
        for key in ("ItemProcessor", "Iterator"):
            proc = body.get(key)
            if isinstance(proc, dict) and "States" in proc:
                yield from _walk(proc["States"])


class TestScannerIsGoneFromTheWeekdayPipeline:
    @pytest.mark.parametrize("name", _RETIRED_WEEKDAY_STATES)
    def test_the_state_does_not_exist(self, daily, name):
        present = {n for n, _ in _walk(daily["States"])}
        assert name not in present, (
            f"{name} is back in step_function_daily.json. The scanner forms its "
            f"cuts WEEKLY (Brian ruling 2026-08-20, alpha-engine-config-I7811); "
            f"re-adding it to the market-open critical path is a ruling, not a "
            f"wiring change."
        )

    def test_nothing_routes_to_a_removed_state(self, daily):
        """A dangling `Next` is a `States.Runtime` at execution time, not a
        parse error — AWS accepts the definition and the run dies mid-flight."""
        dangling = []
        for name, body in _walk(daily["States"]):
            targets = [body.get("Next"), body.get("Default")]
            targets += [c.get("Next") for c in body.get("Choices", []) or []]
            targets += [c.get("Next") for c in body.get("Catch", []) or []]
            for t in targets:
                if t in _RETIRED_WEEKDAY_STATES:
                    dangling.append(f"{name} -> {t}")
        assert not dangling, f"routes into removed scanner states: {dangling}"

    def test_the_data_phase_now_reaches_the_predictor_gate_directly(self, daily):
        """The edges that used to converge on `CheckSkipScanner` must now
        converge on `CheckSkipPredictorInference` — not on nothing, and not on
        some other stage that happens to parse."""
        states = dict(_walk(daily["States"]))
        # alpha-engine-config-I11269: the two spot *Launched gates left with the
        # spot data phase; the readiness wait's ready edge and the skip edge are
        # the data phase's forward exits now.
        assert states["CheckCollectionReadiness"]["Choices"][0]["Next"] == (
            "CheckSkipPredictorInference"
        ), "the ready edge must fall through to the predictor gate"
        assert states["CheckSkipMorningEnrich"]["Choices"][0]["Next"] == (
            "CheckSkipPredictorInference"
        )
        assert (
            states["PublishDataSpotFailureImmediate"]["Next"]
            == "CheckSkipPredictorInference"
        )


class TestTheWeeklyScannerSurvives:
    """The other half of the ruling. A change that removed the weekday Scanner
    AND the weekly one would pass every assertion above while ending scanning
    altogether — the cut would simply stop being formed, and the predictor's
    10-day age walk would hide it for a week and a half."""

    def test_the_weekly_pipeline_still_has_a_scanner(self, weekly):
        present = {n for n, _ in _walk(weekly["States"])}
        assert "Scanner" in present, (
            "step_function.json lost its Scanner state — the weekly pipeline is "
            "now the ONLY place the two cuts are formed (I7811)"
        )

    def test_the_weekly_scanner_invokes_the_scanner_lambda(self, weekly):
        scanner = dict(_walk(weekly["States"]))["Scanner"]
        payload = json.dumps(scanner.get("Parameters", {}))
        assert "alpha-engine-research-scanner" in payload, (
            "the weekly Scanner must still invoke the scanner Lambda"
        )
