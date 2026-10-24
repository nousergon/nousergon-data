"""Tests for weekly_run_report: issue bullets + Director summary on the weekly
terminal Telegram (Brian, 2026-10-24).

The fixtures are trimmed copies of what the 2026-10-24 weekly run
(cycle 2026-10-23) actually left in S3.
"""

from __future__ import annotations

import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import weekly_run_report as wr  # noqa: E402

RUN_DATE = "2026-10-23"
EXEC_ARN = (
    "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:"
    "83096d39-1e13-a1e9-358b-7324edd62588_bd991acb-c502-d1fe-7bec-1af17ef43f04"
)
START = datetime(2026, 10, 24, 9, 0, 49, tzinfo=timezone.utc)

PLAN = {
    "system_summary": "Overall RED. Correctness primitives passed (attestation PASS) ...",
    "top_risks": [
        "Execution is destroying measured alpha: exit_rules_contribution_lift = -0.02611 (CI [-0.03073, -0.02179], N=90, 21d)",
        "The measurement backbone is degrading: sf_success_rate_4w 75.4%",
        "Predictor direction skill is negative at the canonical 21d horizon (48.00% vs 54.23%)",
        "Statistical power: at N=159 the sharpe CI cannot separate target 1 from red-line 0",
    ],
    "attestation": {
        "verdict": "PASS",
        "arithmetic_verdict": "PASS",
        "contamination_verdict": "UNKNOWN",
        "evaluator_verdict": "PASS",
        "backtester_verdict": "PASS",
        "evaluator_stage_verdict": "PASS",
        "promotion_withheld": False,
    },
    "route_degraded": False,
    "advisory_unverified": False,
    "plan_call_telemetry": {
        "DirectorPlanLatencySeconds": 423.977,
        "DirectorPlanLatencyAmber": 0,
        "DirectorPlanAmberSeconds": 536.4,
        "DirectorPlanCarryoverItems": 20,
        "DirectorPlanCarryoverOmitted": 20,
    },
}
RETRO = {"plan_call_telemetry": {
    "RetroJudgeLatencySeconds": 183.346, "RetroJudgeLatencyAmber": 1, "RetroJudgeCeilingSeconds": 120.0,
}}
STAGE_OUTPUTS = {
    "execution_arn": EXEC_ARN,
    "findings": [{"stage": "Director", "verdict": "skipped", "detail": "stage did not enter this execution"}],
    "defects": [{
        "stage": "EvaluatorOptimize", "artifact_id": "config_executor_params", "verdict": "stale",
        "detail": "object predates this execution by 13d 15h — the declaring stage did not write it on this run",
    }],
    "unmeasured": [],
}
WALK_FORWARD = {"status": "insufficient_data", "reason": "only 0 prior week(s) of weight history loaded"}
COVERAGE_OK = {"stage": "Director", "status": "COVERED", "is_finding": False}
COVERAGE_BAD = {"stage": "Backtester", "status": "STALE", "is_finding": True, "stale": ["backtest_report"]}
HEALTH_OK = {"module": "research", "status": "ok", "warnings": [], "error": None, "deliverables": []}
HEALTH_WARN = {"module": "predictor_training", "status": "ok", "warnings": ["ic_30d missing"], "error": None}

EXEC_OUTPUT = {
    "run_date": RUN_DATE,
    "skip_parity": True,
    "skip_rag": True,
    "skip_scanner": False,
    "model_zoo_unservable": True,
    "director_retro_refused": False,
    "cost_coverage_gap": False,
    "weekly_preflight_blind_spot": {"present": False},
    "substrate_relaunch_attempts": 0,
}


class _AccessDenied(Exception):
    def __init__(self):
        self.response = {"Error": {"Code": "AccessDenied"}}


class _NoSuchKey(Exception):
    def __init__(self):
        self.response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    def __init__(self, objects: dict, denied: tuple = ()):
        self.objects = objects  # key -> (doc, LastModified)
        self.denied = denied

    def get_object(self, Bucket, Key):
        if any(Key.startswith(d) for d in self.denied):
            raise _AccessDenied()
        if Key not in self.objects:
            raise _NoSuchKey()
        return {"Body": io.BytesIO(json.dumps(self.objects[Key][0]).encode())}

    def list_objects_v2(self, Bucket, Prefix, Delimiter=None, ContinuationToken=None):
        if any(Prefix.startswith(d) for d in self.denied):
            raise _AccessDenied()
        out = []
        for k, (_doc, mod) in self.objects.items():
            if k.startswith(Prefix) and (Delimiter is None or "/" not in k[len(Prefix):]):
                out.append({"Key": k, "LastModified": mod})
        return {"Contents": out}


def _today_s3(**kw) -> FakeS3:
    later = datetime(2026, 10, 24, 11, 0, tzinfo=timezone.utc)
    earlier = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)
    return FakeS3({
        f"director/{RUN_DATE}/action_plan.json": (PLAN, later),
        f"director/{RUN_DATE}/retro.json": (RETRO, later),
        f"_stage_outputs/ne-weekly-freshness-pipeline/{RUN_DATE}.json": (STAGE_OUTPUTS, later),
        f"backtest/{RUN_DATE}/walk_forward_stability.json": (WALK_FORWARD, later),
        f"_stage_coverage/{RUN_DATE}/Director.json": (COVERAGE_OK, later),
        f"_stage_coverage/{RUN_DATE}/Backtester.json": (COVERAGE_BAD, later),
        "health/research.json": (HEALTH_OK, later),
        "health/predictor_training.json": (HEALTH_WARN, later),
        # Rewritten before this run started: not this run's, not reported.
        "health/executor.json": ({"module": "executor", "status": "error"}, earlier),
    }, **kw)


def _report(s3, **kw):
    return wr.build_weekly_report(
        s3_client=s3, run_date=RUN_DATE, execution_arn=EXEC_ARN, execution_start=START,
        exec_input={}, exec_output=EXEC_OUTPUT, **kw,
    )


def test_todays_run_lists_every_imperfect_step():
    report = _report(_today_s3())
    steps = [i.step for i in report.issues]
    text = "\n".join(f"{i.step}: {i.problem}" for i in report.issues)
    assert "Declared skips" in steps and "`skip_parity`" in text and "`skip_rag`" in text
    assert "skip_scanner" not in text  # false flags are not skips
    assert "Model zoo" in steps
    assert "look-ahead contamination verdict UNKNOWN" in text
    assert "20 of 20 carryover items" in text
    assert "Director retro" in steps
    assert "EvaluatorOptimize" in steps  # the stale executor_params defect
    assert "insufficient" in text  # walk-forward
    assert "Backtester" in steps and "stale: backtest" in text  # coverage record
    assert "predictor" in text and "ic" in text  # health warning
    assert "executor" not in steps  # health file from before the run
    # The stage-output sweep's "skipped" verdicts are timing artefacts.
    assert "did not enter" not in text


def test_every_bullet_carries_an_auto_fix_line():
    lines = wr.format_report_lines(_report(_today_s3()))
    bullets = [i for i, line in enumerate(lines) if line.startswith("• ")]
    assert bullets
    for i in bullets:
        assert lines[i + 1].strip().startswith("Auto-fix: ")


def test_declared_skip_says_none_needed_and_others_name_the_sweep():
    report = _report(_today_s3())
    by_step = {i.step: i for i in report.issues}
    assert by_step["Declared skips"].auto_fix.startswith("none needed")
    assert not by_step["Declared skips"].needs_triage
    assert "nightly sweep" in by_step["Model zoo"].auto_fix
    assert "alert-drain" in by_step["EvaluatorOptimize"].auto_fix


def test_director_summary_status_and_top_three():
    report = _report(_today_s3())
    assert report.director.status == "RED"
    assert len(report.director.concerns) == 3
    lines = wr.format_report_lines(report)
    joined = "\n".join(lines)
    assert "*Director:* 🔴 overall RED" in joined
    assert "1. Execution is destroying" in joined
    assert "4. " not in joined
    # Underscores and brackets are escaped for Telegram Markdown.
    assert r"exit\_rules\_contribution\_lift" in joined
    assert r"\[-0.03073" in joined


def test_unreadable_source_is_a_bullet_not_a_clean_step():
    report = _report(_today_s3(denied=("director/",)))
    text = "\n".join(i.problem for i in report.issues)
    assert "could not read director/2026-10-23/action" in text
    assert "AccessDenied" in text
    assert report.director is None
    # Unreadable sources sit ahead of the cap so they are never elided.
    idx = [n for n, i in enumerate(report.issues) if "could not read" in i.problem]
    assert max(idx) < wr.MAX_ISSUES


def test_missing_director_plan_is_reported():
    s3 = _today_s3()
    del s3.objects[f"director/{RUN_DATE}/action_plan.json"]
    report = _report(s3)
    assert any(i.problem == "no action plan was written for this run" for i in report.issues)


def test_stage_output_doc_for_another_execution_is_ignored():
    s3 = _today_s3()
    doc, mod = s3.objects[f"_stage_outputs/ne-weekly-freshness-pipeline/{RUN_DATE}.json"]
    s3.objects[f"_stage_outputs/ne-weekly-freshness-pipeline/{RUN_DATE}.json"] = (
        dict(doc, execution_arn="arn:other"), mod)
    assert "EvaluatorOptimize" not in [i.step for i in _report(s3).issues]


def test_cap_is_announced():
    many = {f"skip_x{n}": True for n in range(3)}
    out = dict(EXEC_OUTPUT, **many, gate_degraded=True, health_check_degraded=True,
               report_card_degraded=True, parity_degraded=True)
    report = wr.build_weekly_report(
        s3_client=_today_s3(), run_date=RUN_DATE, execution_arn=EXEC_ARN, execution_start=START,
        exec_input={}, exec_output=out,
    )
    assert len(report.issues) > wr.MAX_ISSUES
    lines = wr.format_report_lines(report)
    assert any("more issues not shown" in line for line in lines)


def test_read_budget_spent_is_reported():
    ticks = iter([0.0] + [100.0] * 100)
    report = _report(_today_s3(), clock=lambda: next(ticks))
    assert any("read budget spent" in i.problem for i in report.issues)


def test_clean_run_says_so():
    s3 = FakeS3({f"director/{RUN_DATE}/action_plan.json": (
        {"system_summary": "Overall GREEN.", "top_risks": [], "attestation": {"contamination_verdict": "PASS"}},
        START)})
    report = wr.build_weekly_report(
        s3_client=s3, run_date=RUN_DATE, execution_arn=EXEC_ARN, execution_start=START,
        exec_input={}, exec_output={"run_date": RUN_DATE},
    )
    assert report.issues == []
    assert not wr.report_needs_triage(report)
    assert wr.overall_auto_fix_line(report) == "Auto-fix: none needed (clean run)."
    assert "*Issues:* none found" in "\n".join(wr.format_report_lines(report))


def test_no_run_date_is_reported_not_guessed():
    report = wr.build_weekly_report(
        s3_client=_today_s3(), run_date=None, execution_arn=EXEC_ARN, execution_start=START,
        exec_input={}, exec_output={},
    )
    assert any("run date unknown" in i.problem for i in report.issues)
    assert report.director is None
