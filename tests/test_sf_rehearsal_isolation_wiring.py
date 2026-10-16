"""alpha-engine-config-I12351: the weekly SF carries the rehearsal marker and
never writes a live key from a rehearsal; production keys are unchanged.

The marker is the EXECUTION NAME prefix ``rehearsal-`` (the rehearsal door
names every run ``rehearsal-<date>-<n>``; crucible-backtester#790 keys on the
same prefix). It is derived after InitializeInput and overwrites any
``rehearsal_marker`` an input might carry.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SF = json.loads((ROOT / "infrastructure" / "step_function.json").read_text())
TOP = SF["States"]


def _all_states() -> dict[str, dict]:
    out: dict[str, dict] = {}

    def walk(states: dict) -> None:
        for name, s in states.items():
            out[name] = s
            for b in s.get("Branches", []):
                walk(b["States"])
            it = s.get("ItemProcessor") or s.get("Iterator")
            if it:
                walk(it["States"])

    walk(TOP)
    return out


ALL = _all_states()
BRANCH_A = TOP["ResearchPredictorParallel"]["Branches"][0]["States"]
IS_REHEARSAL = [
    {"Variable": "$.rehearsal_marker.is_rehearsal", "IsPresent": True},
    {"Variable": "$.rehearsal_marker.is_rehearsal", "BooleanEquals": True},
]


def _format(template: str, *args: str) -> str:
    """Evaluate a States.Format template the way Step Functions does."""
    parts = template.split("{}")
    assert len(parts) == len(args) + 1
    return "".join(p + a for p, a in zip(parts, [*args, ""]))


def test_marker_is_derived_from_the_execution_name_right_after_initialize_input():
    assert TOP["InitializeInput"]["Next"] == "ReadRehearsalMarker"
    read = TOP["ReadRehearsalMarker"]
    assert read["Parameters"] == {
        "head.$": "States.ArrayGetItem(States.StringSplit($$.Execution.Name, '-'), 0)"
    }
    assert read["ResultPath"] == "$.rehearsal_marker"
    check = TOP["CheckRehearsalMarker"]
    (rule,) = check["Choices"]
    assert {"Variable": "$.rehearsal_marker.head", "StringEquals": "rehearsal"} in rule["And"]
    assert rule["Next"] == "ApplyRehearsalNamespace"
    assert check["Default"] == "ApplyProductionNamespace"
    for n in ("ApplyRehearsalNamespace", "ApplyProductionNamespace"):
        assert TOP[n]["Next"] == "CheckWeeklyRunDayGate"
        assert TOP[n]["ResultPath"] == "$.rehearsal_marker"


def test_production_namespace_is_empty_and_rehearsal_namespace_matches_the_lambdas():
    prod = TOP["ApplyProductionNamespace"]["Parameters"]
    assert prod["ns_prefix"] == "" and prod["is_rehearsal"] is False
    reh = TOP["ApplyRehearsalNamespace"]["Parameters"]
    assert reh["is_rehearsal"] is True
    # crucible-research data/rehearsal.py / crucible-predictor regime/rehearsal.py
    # namespace_prefix(): f"rehearsal/{name}/"
    assert reh["ns_prefix.$"] == "States.Format('rehearsal/{}/', $$.Execution.Name)"


def test_isolated_lambda_stages_receive_the_execution_name():
    for n in ("Scanner", "ScannerLeaderboard", "SignalsEnvelope"):
        assert ALL[n]["Parameters"]["Payload"]["execution_name.$"] == "$$.Execution.Name", n
    for n in ("RegimeSubstrate", "RegimeRetrospectiveEval"):
        assert ALL[n]["Parameters"]["Payload"]["execution_arn.$"] == "$$.Execution.Id", n


def test_every_completion_marker_is_namespaced_and_unchanged_in_production():
    markers = {
        n: s["Parameters"]["Key.$"]
        for n, s in ALL.items()
        if "_sf_completion/" in str((s.get("Parameters") or {}).get("Key.$", ""))
    }
    assert set(markers) == {
        "WriteCompletionMarker", "WriteCompletionMarkerDegraded",
        "WriteCompletionMarkerCalendar", "WriteCompletionMarkerDegradedCalendar",
    }
    for n, key in markers.items():
        m = re.fullmatch(r"States\.Format\('(.*)', \$\.rehearsal_marker\.ns_prefix, \$\.(run_date|calendar_date)\)", key)
        assert m, (n, key)
        tmpl = m.group(1)
        assert _format(tmpl, "", "2026-10-16") == "_sf_completion/ne-weekly-freshness-pipeline/2026-10-16.json"
        assert _format(tmpl, "rehearsal/rehearsal-2026-10-15-1/", "2026-10-15") == (
            "rehearsal/rehearsal-2026-10-15-1/_sf_completion/ne-weekly-freshness-pipeline/2026-10-15.json"
        )


def test_no_state_writes_a_literal_universe_membership_or_regime_pointer():
    for n, s in ALL.items():
        params = s.get("Parameters") or {}
        if s.get("Resource", "").endswith(":s3:putObject"):
            assert "ns_prefix" in params.get("Key.$", ""), n


def test_scanner_kill_probe_reads_the_runs_own_pointer():
    p = ALL["ProbeUniverseMembershipPointer"]["Parameters"]
    assert "Key" not in p
    tmpl = re.fullmatch(r"States\.Format\('(.*)', \$\.rehearsal_marker\.ns_prefix\)", p["Key.$"]).group(1)
    assert _format(tmpl, "") == "universe_membership/latest.json"


def test_collection_readiness_degrades_only_for_a_rehearsal():
    cr = TOP["CheckCollectionReadiness"]["Choices"]
    idx = [i for i, c in enumerate(cr) if c.get("Next") == "RehearsalCollectionNotReadyDegraded"]
    assert len(idx) == 1
    assert cr[idx[0]]["And"] == IS_REHEARSAL
    settled = [i for i, c in enumerate(cr) if c.get("Next") == "ExtractCollectionNotReadyError"]
    assert idx[0] < min(settled), "the rehearsal rule must precede the not-ok refusal"
    budget = TOP["CheckCollectionReadinessBudget"]["Choices"]
    reh = [c for c in budget if c.get("Next") == "RehearsalCollectionNotReadyDegraded"]
    assert len(reh) == 1 and reh[0]["And"] == IS_REHEARSAL
    # Production keeps both refusals.
    assert sum(c.get("Next") == "ExtractCollectionNotReadyError" for c in budget) == 2
    deg = TOP["RehearsalCollectionNotReadyDegraded"]
    assert deg["Next"] == "ResearchPredictorParallel"
    assert deg["ResultPath"] == "$.rehearsal_degradations"


def test_eval_judge_date_keys_off_the_pinned_cycle_for_a_rehearsal():
    check = BRANCH_A["CheckEvalCadenceRehearsal"]
    assert check["Choices"][0]["And"] == IS_REHEARSAL
    assert check["Choices"][0]["Next"] == "ComputeEvalCadenceRehearsal"
    assert check["Default"] == "ComputeEvalCadence"
    reh = BRANCH_A["ComputeEvalCadenceRehearsal"]["Parameters"]
    assert reh["eval_date.$"] == "$.calendar_date"
    assert "Execution.StartTime" not in reh["day_of_month.$"]
    # Production is untouched.
    assert BRANCH_A["ComputeEvalCadence"]["Parameters"]["eval_date.$"] == (
        "States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)"
    )
