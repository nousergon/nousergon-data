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
        assert TOP[n]["ResultPath"] == "$.rehearsal_marker"
    assert TOP["ApplyProductionNamespace"]["Next"] == "CheckWeeklyRunDayGate"
    # The rehearsal branch forces the champion-risk skips first (see below).
    assert TOP["ApplyRehearsalNamespace"]["Next"] == "ApplyRehearsalChampionSkips"
    assert TOP["ApplyRehearsalChampionSkips"]["Next"] == "CheckWeeklyRunDayGate"


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


def test_report_card_receives_the_execution_name_additively():
    """rehearsal-2026-10-16-1: ReportCard read live signals/ and raised
    StaleInputArtifactError, because its Payload named nothing that said
    "rehearsal". It now carries the same marker the isolated Lambda stages do
    (crucible-evaluator grading/rehearsal.py keys on it).

    ADDITIVE, and asserted as such: every key the Payload carried before is
    still there with the same value, so production's invocation differs only by
    one field that a pre-existing handler ignores and the new one treats as
    production for any name that is not ``rehearsal-*``."""
    payload = ALL["ReportCard"]["Parameters"]["Payload"]
    assert payload["execution_name.$"] == "$$.Execution.Name"
    assert ALL["ReportCard"]["Parameters"]["FunctionName"] == "alpha-engine-evaluator:live"
    assert {k: v for k, v in payload.items() if k != "gate_state"} == {
        "date.$": "$.run_date",
        "dry_run.$": "$.research_dry",
        "snapshot": True,
        "execution_name.$": "$$.Execution.Name",
        "run_scope.$": "$.run_scope_result.Payload",
    }
    # Not $.rehearsal_marker.*: the context object is present on every path
    # that reaches ReportCard, so the reference cannot throw States.Runtime on
    # a valid path the way an unseeded state-input path can.
    assert "rehearsal_marker" not in json.dumps(payload)


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


# ── (A) champion-risk isolation ─────────────────────────────────────────────
#
# A rehearsal must never change the live champion or add an arena week to a
# champion-challenger compare. These are the Task states whose live writes do
# (see ApplyRehearsalChampionSkips' and CheckSkipPredictorTraining's comments
# for the key and the reader of each).
CHAMPION_RISK_TASKS = {
    "ChallengerShadow",
    "ThinkTankCoverage",
    "EvalJudgeSubmitFirstSaturday",
    "EvalJudgeSubmitWeekly",
    "DispatchEvalJudgeSpot",
    "EvalJudgeProcess",
    "EvalRollingMean",
    "PredictorTraining",
    "ResolveZooSpecs",
    "ModelZooTrainMap",
    "ModelZooSelect",
    "ReadModelZooArenaCycle",
    "Director",
}
FORCED_SKIPS = {
    "skip_challenger_shadow": True,
    "skip_thinktank_coverage": True,
    "skip_eval_judge": True,
    "skip_director": True,
    # C30: DataPhase2 writes D15's production keys and the data registry's
    # shared completion marker; production's Saturday run skips it too.
    "skip_data_phase2": True,
}
#: Non-skip fields the rehearsal literal also forces (alpha-engine-config-I12351).
#: pipeline_role: CheckMutexRole acquires production's DynamoDB run-slot only for
#: the unattended cadence roles, so a rehearsal is pinned to a role outside them.
FORCED_FIELDS = {"pipeline_role": "watch-rerun"}
#: Task states that write PRODUCTION data keys with no rehearsal namespace
#: (alpha-engine-config-I12351 C30). Not champion risk, but a live write all
#: the same: rehearsal-2026-10-16-2's DataPhase2 left
#: data/2026-10-16/.phases/alternative.json status=ok, which v2's
#: alternative-phase-two would have auto-skipped on.
PRODUCTION_DATA_TASKS = {"DataPhase2"}


def _leaf(cond: dict, known: dict):
    """True/False when the rule's variable is known, else None (unknown)."""
    if "And" in cond:
        vals = [_leaf(c, known) for c in cond["And"]]
        if any(v is False for v in vals):
            return False
        return True if all(v is True for v in vals) else None
    if "Or" in cond:
        vals = [_leaf(c, known) for c in cond["Or"]]
        if any(v is True for v in vals):
            return True
        return False if all(v is False for v in vals) else None
    if "Not" in cond:
        v = _leaf(cond["Not"], known)
        return None if v is None else not v
    var = cond.get("Variable")
    if var not in known:
        return None
    val = known[var]
    if "IsPresent" in cond:
        return (val is not _ABSENT) == cond["IsPresent"]
    if val is _ABSENT:
        return False
    for op in ("BooleanEquals", "StringEquals", "NumericEquals"):
        if op in cond:
            return val == cond[op]
    return None


_ABSENT = object()


def _reachable(start: str, states: dict, known: dict) -> set[str]:
    """Every state reachable from ``start``, pruning Choice rules whose
    variables are all known. Unknown rules are explored both ways."""
    seen: set[str] = set()
    todo = [(start, states)]
    while todo:
        name, scope = todo.pop()
        if name in seen or name not in scope:
            continue
        seen.add(name)
        s = scope[name]
        nxt: list[str] = []
        if s["Type"] == "Choice":
            decided = False
            for rule in s["Choices"]:
                v = _leaf(rule, known)
                if v is True:
                    nxt.append(rule["Next"])
                    decided = True
                    break
                if v is None:
                    nxt.append(rule["Next"])
            if not decided and s.get("Default"):
                nxt.append(s["Default"])
        else:
            if s.get("Next"):
                nxt.append(s["Next"])
            nxt += [c["Next"] for c in s.get("Catch", [])]
        for b in s.get("Branches", []):
            todo.append((b["StartAt"], b["States"]))
        it = s.get("ItemProcessor") or s.get("Iterator")
        if it:
            todo.append((it["StartAt"], it["States"]))
        todo += [(n, scope) for n in nxt]
        if scope is not TOP:
            # a branch's states can also be reached by name from the top scope
            todo += [(n, TOP) for n in nxt if n in TOP and n not in scope]
    return seen


def test_champion_skips_are_forced_only_on_the_rehearsal_branch():
    reh = TOP["ApplyRehearsalNamespace"]
    assert reh["Next"] == "ApplyRehearsalChampionSkips"
    assert TOP["ApplyProductionNamespace"]["Next"] == "CheckWeeklyRunDayGate"
    skips = TOP["ApplyRehearsalChampionSkips"]
    assert skips["Next"] == "CheckWeeklyRunDayGate"
    assert skips["OutputPath"] == "$.merged"
    expr = skips["Parameters"]["merged.$"]
    m = re.fullmatch(r"States\.JsonMerge\(\$, States\.StringToJson\('(.*)'\), false\)", expr)
    assert m, expr
    # The literal is the SECOND argument, so it wins over anything the input says.
    assert json.loads(m.group(1)) == {**FORCED_SKIPS, **FORCED_FIELDS}
    into = [n for n, s in ALL.items() if s.get("Next") == "ApplyRehearsalChampionSkips"
            or s.get("Default") == "ApplyRehearsalChampionSkips"
            or any(c.get("Next") == "ApplyRehearsalChampionSkips" for c in s.get("Choices", []))]
    assert into == ["ApplyRehearsalNamespace"]
    assert set(reh["Parameters"]["champion_skips"]) >= {
        "ChallengerShadow", "ThinkTankCoverage", "EvalRollingMean", "PredictorTraining", "ModelZoo", "Director",
    }


def test_each_forced_skip_is_one_the_graph_honours():
    branch_a = BRANCH_A
    for choice, flag, skipped in (
        (branch_a["CheckSkipChallengerShadow"], "skip_challenger_shadow", "ChallengerShadow"),
        (branch_a["CheckSkipThinkTankCoverage"], "skip_thinktank_coverage", "ThinkTankCoverage"),
        (branch_a["CheckSkipEvalJudge"], "skip_eval_judge", "CheckEvalCadenceRehearsal"),
        (TOP["CheckSkipDirector"], "skip_director", "PrepareDirectorOnSpot"),
        (branch_a["CheckSkipDataPhase2"], "skip_data_phase2", "RehearsalClockBeforeDataPhase2"),
    ):
        (rule,) = [c for c in choice["Choices"] if f"$.{flag}" in json.dumps(c)]
        assert rule["Next"] != skipped
        assert choice["Default"] == skipped


def test_predictor_branch_skips_training_and_the_zoo_for_a_rehearsal_only():
    b1 = TOP["ResearchPredictorParallel"]["Branches"][1]["States"]
    first = b1["CheckSkipPredictorTraining"]["Choices"][0]
    assert first["And"] == IS_REHEARSAL
    assert first["Next"] == "PredictorTrainingSkipped"
    assert b1["PredictorTrainingSkipped"]["End"] is True
    # Production: the operator rules after it are unchanged and still need
    # skip_predictor_training.
    for rule in b1["CheckSkipPredictorTraining"]["Choices"][1:]:
        assert "$.skip_predictor_training" in json.dumps(rule)
    assert b1["CheckSkipPredictorTraining"]["Default"] == "PredictorTraining"


def _rehearsal_known() -> dict:
    known = {f"$.{k}": v for k, v in FORCED_SKIPS.items()}
    known["$.rehearsal_marker.is_rehearsal"] = True
    known["$.rehearsal_marker.head"] = "rehearsal"
    return known


def test_a_rehearsal_reaches_no_champion_risk_writer():
    """Walk the graph from the marker the way a rehearsal resolves it: no
    state that writes the live champion, weights, a model-zoo or research
    arena record, or the Director's carryover ledger is reachable."""
    reached = _reachable("ApplyRehearsalNamespace", TOP, _rehearsal_known())
    assert "CheckWeeklyRunDayGate" in reached and "ResearchPredictorParallel" in reached
    assert reached & CHAMPION_RISK_TASKS == set(), sorted(reached & CHAMPION_RISK_TASKS)


def test_a_rehearsal_reaches_no_production_data_writer():
    """C30: a rehearsal reaches no stage that writes a live data key."""
    reached = _reachable("ApplyRehearsalNamespace", TOP, _rehearsal_known())
    assert "CheckSkipDataPhase2" in reached
    assert reached & PRODUCTION_DATA_TASKS == set(), sorted(reached & PRODUCTION_DATA_TASKS)


def test_production_still_reaches_every_champion_writer():
    """The same walk for a production execution: every one of those writers
    stays reachable, so production behaviour is unchanged."""
    known = {
        "$.rehearsal_marker.is_rehearsal": False,
        "$.rehearsal_marker.head": "a1b2c3d4",
        **{f"$.{k}": _ABSENT for k in FORCED_SKIPS},
        "$.skip_predictor_training": _ABSENT,
    }
    reached = _reachable("ApplyProductionNamespace", TOP, known)
    assert "ApplyRehearsalChampionSkips" not in reached
    assert CHAMPION_RISK_TASKS <= reached, sorted(CHAMPION_RISK_TASKS - reached)
    assert PRODUCTION_DATA_TASKS <= reached, sorted(PRODUCTION_DATA_TASKS - reached)


# ── (B) deadline stop ───────────────────────────────────────────────────────
#
# A rehearsal must not still be running when production starts. The guard
# stamps $.rehearsal_deadline (the next production slot - 2 h); before every
# long stage a REHEARSAL reads the clock and, past the deadline, does not start
# it: on the top level the execution ends at RehearsalDeadlineStop, inside
# branch A the stage takes its operator-skip edge.
TOP_CHECKPOINTS = {
    "CheckSkipBacktesterStageOnly": "Backtester",
    "CheckSkipPredictorBacktest": "PredictorBacktest",
    "CheckSkipPortfolioOptimizerBacktest": "PortfolioOptimizerBacktest",
    "CheckSkipEvaluator": "EvaluatorDiagnostics",
    "CheckSkipReportCard": "ReportCard",
}
BRANCH_CHECKPOINTS = {
    "CheckSkipDataPhase2": "DataPhase2",
    "CheckSkipRAGIngestion": "RAGIngestion",
}
PAST_DEADLINE = [
    *IS_REHEARSAL,
    {"Variable": "$.rehearsal_deadline", "IsPresent": True},
    {"Variable": "$.rehearsal_deadline", "IsTimestamp": True},
    {"Variable": "$.rehearsal_clock.now", "TimestampGreaterThanEqualsPath": "$.rehearsal_deadline"},
]


def _checkpoint(scope: dict, gate: str, stage: str) -> dict:
    clock_name = scope[gate]["Default"]
    assert clock_name == f"RehearsalClockBefore{stage}"
    clock = scope[clock_name]
    assert clock["Type"] == "Pass"
    assert clock["Parameters"] == {"now.$": "$$.State.EnteredTime"}
    assert clock["ResultPath"] == "$.rehearsal_clock"
    check = scope[clock["Next"]]
    assert clock["Next"] == f"CheckRehearsalDeadlineBefore{stage}"
    assert check["Type"] == "Choice"
    (rule,) = check["Choices"]
    assert rule["And"] == PAST_DEADLINE
    # Not past the deadline (and always, in production): the stage runs.
    assert check["Default"] == stage
    return rule


def test_top_level_long_stages_stop_a_rehearsal_past_its_deadline():
    for gate, stage in TOP_CHECKPOINTS.items():
        rule = _checkpoint(TOP, gate, stage)
        assert rule["Next"] == "RecordRehearsalDeadlineStop", gate
    rec = TOP["RecordRehearsalDeadlineStop"]
    assert rec["ResultPath"] == "$.rehearsal_deadline_stop"
    assert rec["Next"] == "RehearsalDeadlineStop"
    # A clean end: a Fail would page through the weekly pipeline's alarms.
    assert TOP["RehearsalDeadlineStop"]["Type"] == "Succeed"


def test_branch_a_long_stages_take_their_skip_edge_past_the_deadline():
    for gate, stage in BRANCH_CHECKPOINTS.items():
        rule = _checkpoint(BRANCH_A, gate, stage)
        (operator_skip,) = [c for c in BRANCH_A[gate]["Choices"]]
        assert rule["Next"] == operator_skip["Next"], gate


def test_only_a_deadline_checkpoint_reaches_the_deadline_stop():
    into = sorted(
        n for n, s in ALL.items()
        if any(c.get("Next") == "RecordRehearsalDeadlineStop" for c in s.get("Choices", []))
        or s.get("Next") == "RecordRehearsalDeadlineStop" or s.get("Default") == "RecordRehearsalDeadlineStop"
    )
    assert into == sorted(f"CheckRehearsalDeadlineBefore{s}" for s in TOP_CHECKPOINTS.values())


def test_production_never_matches_a_deadline_rule():
    """Every rule that reads the deadline is conjoined with is_rehearsal ==
    true, which ApplyProductionNamespace sets false."""
    for n, s in ALL.items():
        for c in s.get("Choices", []):
            if "$.rehearsal_deadline" in json.dumps(c):
                assert c["And"][:2] == IS_REHEARSAL, n
    known = {"$.rehearsal_marker.is_rehearsal": False}
    for gate, stage in {**TOP_CHECKPOINTS}.items():
        check = TOP[f"CheckRehearsalDeadlineBefore{stage}"]
        assert _leaf(check["Choices"][0], known) is False


def _mutex_roles() -> set[str]:
    """The pipeline_role values CheckMutexRole sends to the run-slot mutex."""
    roles: set[str] = set()

    def walk(rule):
        if isinstance(rule, dict):
            if rule.get("Variable") == "$.pipeline_role" and "StringEquals" in rule:
                roles.add(rule["StringEquals"])
            for v in rule.values():
                walk(v)
        elif isinstance(rule, list):
            for v in rule:
                walk(v)

    for choice in TOP["CheckMutexRole"]["Choices"]:
        if choice.get("Next") == "ComputeMutexTtl":
            walk(choice)
    return roles


def test_a_rehearsal_can_never_take_the_run_slot_mutex():
    """alpha-engine-config-I12351 (run-slot). rehearsal-2026-10-16-2 bypassed the
    mutex only because the rehearsal door passed pipeline_role=watch-rerun. The
    rehearsal literal now forces it, and it must stay outside the roles that
    acquire production's run-slot, or a hand-started rehearsal-* with
    pipeline_role=weekly could hold Saturday's slot."""
    roles = _mutex_roles()
    assert {"daily", "weekly", "eod"} <= roles, roles
    assert FORCED_FIELDS["pipeline_role"] not in roles
    assert TOP["CheckMutexRole"]["Default"] != "ComputeMutexTtl"
    # The forced role must reach CheckMutexRole: nothing may overwrite it.
    for name, st in ALL.items():
        assert st.get("ResultPath") != "$.pipeline_role", name


def test_the_lambda_stages_that_isolate_themselves_get_the_execution_name():
    """ResearchSelfTest, AggregateCosts and RationaleClustering wrote live keys in
    rehearsal-2026-10-16-2 (research/<date>/self_test.json, nine
    decision_artifacts/_cost/<date>/cost.parquet, the console row). Their
    handlers isolate on execution_name, like Scanner and SignalsEnvelope."""
    for name in ("ResearchSelfTest", "AggregateCosts", "RationaleClustering",
                 "Scanner", "SignalsEnvelope", "ScannerLeaderboard", "ReportCard"):
        payload = ALL[name]["Parameters"]["Payload"]
        assert payload.get("execution_name.$") == "$$.Execution.Name", name
