"""The sweep reports honestly: a run that could not measure is never a pass,
and it never advances the clean-run pointer (alpha-engine-config-I7249).

The obligation this file exists to discharge: **"could not measure" must be
distinguishable from "measured a pass" on every surface the sweep writes** —
the report, the metrics, the notification, and the console row. Three
detectors in this fleet have shipped unable to fail, and the specific way this
one could fail silently is by folding an unmeasured stage into green.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import subprocess
import sys

import pytest

from infrastructure import preflight_sweep as ps
from infrastructure.preflight_sweep_console import (
    ENVELOPE_ATTENTION,
    ENVELOPE_ERROR,
    ENVELOPE_OK,
    envelopes,
    load_cadence,
    rollup_envelope,
    stage_check_id,
    stage_envelope,
)
from infrastructure.preflight_sweep import update_streaks
from infrastructure.preflight_sweep_stages import Stage

REPO = pathlib.Path(__file__).resolve().parent.parent
SF_PATH = REPO / "infrastructure" / "step_function.json"
MANIFEST_PATH = REPO / "infrastructure" / "preflight_sweep_manifest.json"
CADENCE_PATH = REPO / "infrastructure" / "preflight_sweep_cadence.json"


class FakeAws:
    def __init__(self, listing: dict[str, list[dict]] | None = None):
        self.objects: dict[str, dict] = {}
        self.metrics: list[tuple[str, float]] = []
        self.notifications: list[tuple[str, str]] = []
        self.listing = listing or {}
        self.list_calls: list[str] = []

    def put_json(self, key, payload):
        self.objects[key] = payload

    def get_json(self, key):
        return self.objects.get(key)

    def list_objects(self, prefix, max_keys=1000):
        self.list_calls.append(prefix)
        return self.listing.get(prefix, [])

    def put_text(self, key, body):
        self.objects[key] = body

    def put_metrics(self, metrics):
        self.metrics.extend(metrics)

    def publish(self, topic_arn, subject, message):
        self.notifications.append((subject, message))


def _completed(returncode: int, stderr: str = ""):
    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=["bash"], returncode=returncode, stdout="", stderr=stderr
        )

    return runner


def _stage(name="DataPhase1"):
    return Stage(
        name=name,
        classification="sweepable",
        box_dir="alpha-engine-data",
        repo="nousergon/nousergon-data",
        launcher="infrastructure/spot_data_phase1.sh",
        commands=["echo hi"],
    )


# ── Per-stage verdicts ───────────────────────────────────────────────────────



def _sweep_run_date() -> str:
    """The run_date the sweep itself derives — UTC, never local.

    `preflight_sweep.sweep` binds `run_date` from
    `dt.datetime.now(dt.timezone.utc).date()`, mirroring the SF's own
    `InitializeInput`. A fixture built from `dt.date.today()` (LOCAL) agrees
    with it only outside the hours where the two dates differ — 17:00-24:00
    Pacific, every day. Measured 2026-08-15 23:5x PT: the sweep probed
    `backtest/2026-08-16/` while the fixture declared `backtest/2026-08-15/`,
    and `test_a_genuinely_broken_backtest_stage_still_fails_when_upstream_is_present`
    failed on an ambient clock rather than on anything about the code.

    The upstream-PENDING test is the worse half: a mismatched date produces
    an empty upstream probe, which is exactly the state it asserts — so it
    passed for the wrong reason in that window and would have kept passing
    if the probe had stopped working entirely.

    Second half, measured 2026-08-26: the probe is dated by the NYSE TRADING
    DAY, not the calendar date, because that is what the launcher normalizes
    RUN_DATE to before reading the prefix. A fixture keyed on the raw UTC date
    would miss the probe on every Saturday, Sunday and market holiday — the
    same passed-for-the-wrong-reason failure, one axis over.
    """
    return ps.normalize_probe_date(
        dt.datetime.now(dt.timezone.utc).date().isoformat(),
        ps.NORMALIZE_NYSE_TRADING_DAY,
    )


def test_a_clean_preflight_is_a_pass(tmp_path):
    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=_completed(0))
    assert result.verdict == ps.PASSED
    assert result.returncode == 0


def test_a_non_zero_preflight_is_a_failure_carrying_its_last_stderr_line(tmp_path):
    runner = _completed(1, stderr="warning: noise\nERROR: predictor.yaml not found\n")
    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=runner)
    assert result.verdict == ps.FAILED
    # No naked return codes: the operator must not have to open a log to learn
    # what broke.
    assert result.last_stderr_line == "ERROR: predictor.yaml not found"
    assert "rc=1" in result.reason


def test_an_oom_is_named_as_a_resource_kill_not_a_generic_failure(tmp_path):
    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=_completed(137))
    assert result.verdict == ps.FAILED
    assert "RESOURCE KILL" in result.reason and "OOM" in result.reason


def test_a_timeout_is_a_hard_fail_named_as_a_resource_kill(tmp_path):
    def runner(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="bash", timeout=60)

    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=runner)
    assert result.verdict == ps.FAILED
    assert "RESOURCE KILL" in result.reason
    assert "Not retried" in result.reason


def test_a_harness_fault_is_unmeasured_not_a_stage_failure(tmp_path):
    """The bug class: a detector that cannot tell its own harness fault from a
    finding reports the harness fault AS the defect, always in the alarming
    direction."""

    def runner(*_a, **_k):
        raise OSError("no such executable: bash")

    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=runner)
    assert result.verdict == ps.UNMEASURED
    assert result.verdict != ps.FAILED


# ── Run-level honesty ────────────────────────────────────────────────────────


def test_a_derivation_failure_is_reported_unmeasured_and_never_ok(tmp_path):
    broken = tmp_path / "broken.json"
    broken.write_text("{not json")
    report = ps.sweep(
        definition_path=broken,
        manifest_path=MANIFEST_PATH,
        checkout_root=str(tmp_path),
        run_id="t",
    )
    assert report.measured is False
    assert report.outcome == ps.OUTCOME_FAILED
    assert report.unmeasured_reason and "NOT a clean run" in report.unmeasured_reason


def test_an_unmeasured_run_never_advances_the_clean_pointer(tmp_path):
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        outcome=ps.OUTCOME_FAILED,
        measured=False,
        unmeasured_reason="no box",
    )
    ps.emit(report, aws, "arn:sns")
    assert f"{ps.REPORT_PREFIX}/last_clean.json" not in aws.objects
    assert f"{ps.REPORT_PREFIX}/latest.json" in aws.objects


def test_a_clean_run_does_advance_the_clean_pointer():
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        outcome=ps.OUTCOME_OK,
        measured=True,
        stages_declared=1,
        stages_passed=1,
        results=[{"stage": "A", "verdict": ps.PASSED}],
    )
    ps.emit(report, aws, "arn:sns")
    assert f"{ps.REPORT_PREFIX}/last_clean.json" in aws.objects


def test_the_deadman_subject_is_emitted_even_on_an_unmeasured_run():
    """The alarm fires on the ABSENCE of this metric, so a run that reported
    'I could not measure' must still emit it — otherwise one incident produces
    two notifications, the sweep's own and the deadman's."""
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        outcome=ps.OUTCOME_FAILED,
        measured=False,
        unmeasured_reason="bootstrap failed",
    )
    ps.emit(report, aws, "arn:sns")
    assert ("PreflightSweepRunCompleted", 1) in aws.metrics


def test_one_notification_carries_every_failing_stage_by_name():
    """observability-policy §7.2a: one notification per group failure, never
    one per member — and the group notification carries the full member list."""
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at="now",
        outcome=ps.OUTCOME_FAILED,
        measured=True,
        stages_declared=3,
        stages_failed=2,
        results=[
            {"stage": "DataPhase1", "verdict": ps.FAILED, "reason": "rc=1",
             "last_stderr_line": "ERROR: boom"},
            {"stage": "Backtester", "verdict": ps.FAILED, "reason": "rc=137"},
            {"stage": "ParityReplay", "verdict": ps.PASSED},
        ],
    )
    subject, message = ps.render_notification(report)
    assert "DataPhase1" in message and "Backtester" in message
    assert "ERROR: boom" in message
    assert "2 failed" in subject


def test_the_unmeasured_notification_says_so_in_its_subject():
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID, run_id="t", started_at="now",
        measured=False, unmeasured_reason="the box never booted",
    )
    subject, message = ps.render_notification(report)
    assert "COULD NOT MEASURE" in subject
    assert "the box never booted" in message


def test_a_run_that_swept_nothing_is_not_reported_ok(tmp_path):
    """Zero stages passing and zero failing is not a clean sweep, it is a
    sweep that did not happen."""
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"States": {}}))
    # Derivation will fail on the absent ApplyShellRunDefaults, which is
    # itself the correct loud outcome.
    report = ps.sweep(empty, MANIFEST_PATH, str(tmp_path), "t")
    assert report.outcome != ps.OUTCOME_OK
    assert report.measured is False


def test_the_live_definition_derives_a_non_empty_stage_set(tmp_path):
    """Guards against the sweep silently grading an empty pipeline."""
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    assert report.stages_declared > 0
    # No BLOCKING finding: the acknowledged no-dry-path stages are findings by
    # design now (they must be named), and they must not fail the run.
    assert [f for f in report.coverage_findings if ps._is_blocking(f)] == []


# ── I7323: an unmet same-day upstream is UNSWEEPABLE, never FAILED ───────────

DECL = {
    "stage": "PredictorBacktest",
    "produced_by": "Backtester",
    "prefix": "backtest/{run_date}/",
    "ignore_subprefixes": [".phases/"],
    "reason": "declared",
}
BINDINGS = {"run_date": "2026-08-14"}


def _failed(stage="PredictorBacktest"):
    return ps.StageResult(stage=stage, verdict=ps.FAILED, returncode=1,
                          reason="preflight exited rc=1")


def test_a_failure_on_an_absent_declared_upstream_is_unsweepable_not_failed():
    result = ps.classify_upstream(_failed(), DECL, BINDINGS, lambda _p: [])
    assert result.verdict == ps.UNSWEEPABLE_VERDICT
    assert result.unsweepable_kind == ps.UNSWEEPABLE_UPSTREAM_PENDING
    # The reason must name BOTH the unmet prefix and its producing stage —
    # otherwise the operator gets a verdict with nothing to act on.
    assert "backtest/2026-08-14/" in result.reason
    assert "Backtester" in result.reason


def test_the_prefix_gotcha_a_prefix_holding_only_sweep_markers_is_not_content():
    """NON-INFERABLE: backtest/<date>/ EXISTS on a day nothing produced it,
    because the sweep's own .phases/ markers live under it. A probe testing
    prefix EXISTENCE would read as satisfied and measure nothing at all."""
    listing = [
        {"Key": "backtest/2026-08-14/.phases/preflight.json", "Size": 235},
        {"Key": "backtest/2026-08-14/.phases/runtime_smoke.json", "Size": 239},
    ]
    present, detail = ps.upstream_content(
        "backtest/2026-08-14/", [".phases/"], lambda _p: listing
    )
    assert present is False
    assert detail["content_keys"] == 0 and detail["ignored_keys"] == 2

    result = ps.classify_upstream(_failed(), DECL, BINDINGS, lambda _p: listing)
    assert result.verdict == ps.UNSWEEPABLE_VERDICT


def test_a_zero_byte_key_is_not_upstream_content():
    present, _ = ps.upstream_content(
        "backtest/2026-08-14/",
        [".phases/"],
        lambda _p: [{"Key": "backtest/2026-08-14/_SUCCESS", "Size": 0}],
    )
    assert present is False


def test_a_real_failure_stays_failed_when_its_declared_upstream_is_populated():
    """The regression this whole mechanism must not introduce: a declaration
    that swallows genuine defects. The declaration only ARMS the check; the
    prefix is still probed on the day."""
    listing = [
        {"Key": "backtest/2026-08-14/.phases/preflight.json", "Size": 235},
        {"Key": "backtest/2026-08-14/results/portfolio.parquet", "Size": 91234},
    ]
    result = ps.classify_upstream(_failed(), DECL, BINDINGS, lambda _p: listing)
    assert result.verdict == ps.FAILED
    assert "REAL failure" in result.reason


def test_a_reworded_launcher_error_cannot_reclassify_anything():
    """The classification is DECLARED per stage, never inferred from stderr.
    A stage with no declaration keeps its failure however its error reads."""
    result = _failed(stage="DataPhase1")
    result.last_stderr_line = (
        "ERROR: s3://alpha-engine-research/backtest/2026-08-14/ is empty or unreachable."
    )
    # sweep() only consults upstream_dependencies(manifest); DataPhase1 is not
    # in it, so classify_upstream is never reached for that stage.
    manifest = json.loads(MANIFEST_PATH.read_text())
    from infrastructure.preflight_sweep_stages import upstream_dependencies

    assert "DataPhase1" not in upstream_dependencies(manifest)
    assert result.verdict == ps.FAILED


def test_an_upstream_pending_row_carries_the_manifests_written_acknowledgement():
    """Mirrors test_a_no_dry_path_stage_gets_its_own_verdict_row_with_repo_and_launcher:
    the manifest's WRITTEN reason must reach the row, not just the auto-generated
    per-run "NOT MEASURABLE TODAY" text — alpha-engine-config-I10717. Without
    this, an operator reading the report (or the notification, which already
    prints acknowledged_reason generically for every unsweepable row) cannot
    tell a declared, cyclical gap from a new one without opening
    preflight_sweep_manifest.json."""
    result = ps.classify_upstream(_failed(), DECL, BINDINGS, lambda _p: [])
    assert result.verdict == ps.UNSWEEPABLE_VERDICT
    assert result.acknowledged_reason == DECL["reason"]


def test_find_recent_populated_upstream_returns_the_most_recent_hit():
    """Walks backward day by day and stops at the FIRST (most recent, i.e.
    smallest offset) populated date — not the oldest one in the window, and
    not one past it."""
    def lister(prefix):
        if prefix == "backtest/2026-08-11/":
            return [{"Key": "backtest/2026-08-11/results/backtest.parquet", "Size": 100}]
        if prefix == "backtest/2026-08-08/":
            return [{"Key": "backtest/2026-08-08/results/backtest.parquet", "Size": 100}]
        return []

    found = ps.find_recent_populated_upstream(
        "backtest/{run_date}/", {"run_date": "2026-08-14"}, [".phases/"], lister, 8,
    )
    assert found is not None
    date, detail = found
    assert date == "2026-08-11"
    assert detail["age_days"] == 3
    assert detail["prefix"] == "backtest/2026-08-11/"


def test_find_recent_populated_upstream_none_when_outside_the_window():
    def lister(prefix):
        # Only populated 9 days back — outside an 8-day window.
        return (
            [{"Key": "backtest/2026-08-05/results/backtest.parquet", "Size": 100}]
            if prefix == "backtest/2026-08-05/" else []
        )

    found = ps.find_recent_populated_upstream(
        "backtest/{run_date}/", {"run_date": "2026-08-14"}, [".phases/"], lister, 8,
    )
    assert found is None


def test_find_recent_populated_upstream_ignores_phase_markers_like_the_same_day_probe():
    def lister(prefix):
        return [{"Key": f"{prefix}.phases/preflight.json", "Size": 235}]

    found = ps.find_recent_populated_upstream(
        "backtest/{run_date}/", {"run_date": "2026-08-14"}, [".phases/"], lister, 8,
    )
    assert found is None


def test_find_recent_populated_upstream_a_bad_candidate_probe_does_not_abort_the_walk():
    """A lister raising on ONE candidate day (e.g. a transient S3 hiccup while
    probing a specific date) must not stop the walk from finding a real hit
    on a different day — that is a fact about one probe, not about the
    upstream's whole history."""
    def lister(prefix):
        if prefix == "backtest/2026-08-13/":
            raise RuntimeError("transient")
        if prefix == "backtest/2026-08-12/":
            return [{"Key": "backtest/2026-08-12/results/backtest.parquet", "Size": 100}]
        return []

    found = ps.find_recent_populated_upstream(
        "backtest/{run_date}/", {"run_date": "2026-08-14"}, [".phases/"], lister, 8,
    )
    assert found is not None
    assert found[0] == "2026-08-12"


def test_remeasure_upstream_pending_falls_back_to_classify_when_the_override_stage_is_not_sweepable():
    """If re-deriving the definition against the override run_date does not
    even produce a SWEEPABLE stage (e.g. definition drift breaks the override
    binding), the function must not claim a measurement that never happened —
    it returns classify_upstream's own unsweepable/upstream_pending verdict."""
    listing = {"backtest/2026-08-11/": [
        {"Key": "backtest/2026-08-11/results/backtest.parquet", "Size": 100}
    ]}

    def lister(prefix):
        return listing.get(prefix, [])

    bad_definition = {"States": {}}  # derive_stages finds nothing for this name
    result = ps.remeasure_upstream_pending(
        _failed(), DECL, {"run_date": "2026-08-14"}, lister,
        definition=bad_definition, context={"Execution": {"Name": "t", "Id": "t"}},
        checkout_root="/nonexistent", stage_timeout=30, runner=lambda *a, **k: None,
    )
    assert result.verdict == ps.UNSWEEPABLE_VERDICT
    assert result.unsweepable_kind == ps.UNSWEEPABLE_UPSTREAM_PENDING
    assert result.measured_against_run_date is None


def test_an_unprobeable_upstream_is_unmeasured_not_a_guess_in_either_direction():
    def boom(_prefix):
        raise RuntimeError("AccessDenied")

    result = ps.classify_upstream(_failed(), DECL, BINDINGS, boom)
    assert result.verdict == ps.UNMEASURED
    assert result.verdict not in (ps.FAILED, ps.UNSWEEPABLE_VERDICT)
    assert "AccessDenied" in result.reason


def test_an_upstream_pending_stage_does_not_count_toward_failures_or_page():
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID, run_id="t", started_at="now",
        outcome=ps.OUTCOME_DEGRADED, measured=True, stages_declared=2,
        stages_failed=0, stages_unsweepable=1,
        stages_unsweepable_upstream_pending=1, stages_unsweepable_coverage_defect=0,
        results=[
            {"stage": "Backtester", "verdict": ps.PASSED},
            {"stage": "PredictorBacktest", "verdict": ps.UNSWEEPABLE_VERDICT,
             "unsweepable_kind": ps.UNSWEEPABLE_UPSTREAM_PENDING,
             "reason": "NOT MEASURABLE TODAY — backtest/2026-08-14/"},
        ],
    )
    aws = FakeAws()
    ps.emit(report, aws, "arn:sns")
    assert ("PreflightSweepStagesFailed", 0) in aws.metrics
    assert ("PreflightSweepStagesUnsweepableUpstreamPending", 1) in aws.metrics
    assert ("PreflightSweepStagesUnsweepableCoverageDefect", 0) in aws.metrics
    subject, _ = ps.render_notification(report)
    assert "failed" not in subject.split("—")[0].lower() or "no failures" in subject
    # And it never advances the clean-run pointer: nothing failed, but the
    # sweep did not cover everything it declares.
    assert f"{ps.REPORT_PREFIX}/last_clean.json" not in aws.objects


def test_a_coverage_defect_unsweepable_still_fails_the_run(tmp_path):
    """The loud kind must stay loud: /nonexistent has no launchers at all."""
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    assert report.stages_unsweepable_coverage_defect > 0
    assert report.outcome == ps.OUTCOME_FAILED


# ── I7323 (3): unsweepable forever is its own finding, not coverage ──────────


def _unsweepable(stage="PredictorBacktest"):
    return ps.StageResult(
        stage=stage, verdict=ps.UNSWEEPABLE_VERDICT,
        unsweepable_kind=ps.UNSWEEPABLE_UPSTREAM_PENDING, reason="upstream absent",
    )


def test_a_streak_below_the_threshold_is_not_yet_a_finding():
    state, findings = update_streaks(None, [_unsweepable()], "r1", "now", 8)
    assert state["streaks"]["PredictorBacktest"]["consecutive_runs"] == 1
    assert findings == []


def test_unsweepable_on_every_run_past_the_threshold_becomes_its_own_finding():
    prior = {"streaks": {"PredictorBacktest": {"consecutive_runs": 7, "since": "d0"}}}
    _state, findings = update_streaks(prior, [_unsweepable()], "r8", "now", 8)
    assert len(findings) == 1
    assert findings[0]["stage"] == "PredictorBacktest"
    assert findings[0]["consecutive_runs"] == 8
    assert "measured NOTHING" in findings[0]["finding"]


def test_a_stage_that_became_measurable_has_its_streak_dropped():
    prior = {"streaks": {"PredictorBacktest": {"consecutive_runs": 7, "since": "d0"}}}
    passed = ps.StageResult(stage="PredictorBacktest", verdict=ps.PASSED)
    state, findings = update_streaks(prior, [passed], "r8", "now", 8)
    assert "PredictorBacktest" not in state["streaks"]
    assert findings == []


def test_the_persistent_finding_is_emitted_separately_from_coverage_findings():
    """It must not read as coverage: a stage nothing has measured for a week is
    the opposite of a covered stage."""
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID, run_id="t", started_at="now",
        outcome=ps.OUTCOME_FAILED, measured=True, stages_declared=1,
        persistent_unsweepable_findings=[
            {"stage": "PredictorBacktest", "consecutive_runs": 9, "threshold_runs": 8,
             "since": "d0", "finding": "PredictorBacktest measured NOTHING for 9 runs"}
        ],
        results=[{"stage": "PredictorBacktest", "verdict": ps.UNSWEEPABLE_VERDICT,
                  "unsweepable_kind": ps.UNSWEEPABLE_UPSTREAM_PENDING}],
    )
    assert report.coverage_findings == []
    _subject, message = ps.render_notification(report)
    assert "PERSISTENTLY UNSWEEPABLE" in message
    aws = FakeAws()
    ps.emit(report, aws, "arn:sns")
    assert ("PreflightSweepPersistentUnsweepable", 1) in aws.metrics


def test_the_streak_threshold_is_declared_in_days_and_derived_against_cadence():
    manifest = json.loads(MANIFEST_PATH.read_text())
    cadence = load_cadence(CADENCE_PATH)
    assert manifest["unsweepable_streak_threshold_days"] > 7, (
        "a threshold of 7 or less fires on the ordinary weekday state of the "
        "backtest chain and would page every week"
    )
    assert ps.streak_threshold_runs(manifest, cadence) == (
        manifest["unsweepable_streak_threshold_days"]
    ), "daily cadence means one run per day"


def test_an_unreadable_streak_history_is_named_never_treated_as_no_history(tmp_path):
    """Silently resetting to zero would make the finding unable to fire — the
    exact bug class of a detector that cannot report."""

    class Broken(FakeAws):
        def get_json(self, key):
            raise RuntimeError("AccessDenied")

    aws = Broken()
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t", aws=aws)
    assert "unavailable" in report.unsweepable_streak_state
    assert "AccessDenied" in report.unsweepable_streak_state
    assert f"{ps.REPORT_PREFIX}/{ps.STREAK_STATE_KEY_SUFFIX}" not in aws.objects


# ── I7324: every declared stage carries a verdict, named on every surface ────


def test_every_declared_stage_carries_a_verdict_row():
    """THE missing assertion that is I7324's root cause: 19 declared, 16 rows,
    and which 3 were missing was unrecoverable from the report."""
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    assert len(report.results) == report.stages_declared
    assert {r["stage"] for r in report.results} == {
        s["stage"] for s in report.declared_stages
    }


def test_the_declared_stage_list_is_serialised_so_declared_minus_swept_is_auditable():
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    assert len(report.declared_stages) == report.stages_declared
    assert all({"stage", "classification"} <= set(s) for s in report.declared_stages)


def test_a_no_dry_path_stage_gets_its_own_verdict_row_with_repo_and_launcher():
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    rows = [r for r in report.results if r["verdict"] == ps.NO_DRY_PATH_VERDICT]
    assert len(rows) == report.stages_no_dry_path > 0
    for row in rows:
        assert row["reason"], "a row with no reason is a count with extra steps"
        assert row["acknowledged_reason"], (
            "the manifest's written acknowledgement must reach the report — an "
            "operator must not have to open the repo to learn why"
        )
        assert set(row) >= {"stage", "repo", "launcher", "reason"}


def test_coverage_findings_are_non_empty_whenever_a_stage_has_no_dry_path():
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    assert report.stages_no_dry_path > 0
    named = {f.get("stage") for f in report.coverage_findings
             if f.get("kind") == ps.FINDING_NO_DRY_PATH}
    assert named == {
        r["stage"] for r in report.results if r["verdict"] == ps.NO_DRY_PATH_VERDICT
    }


def test_an_acknowledged_no_dry_path_gap_is_named_but_does_not_fail_the_run():
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    gaps = [f for f in report.coverage_findings
            if f["kind"] == ps.FINDING_NO_DRY_PATH]
    assert gaps and all(f["blocking"] is False for f in gaps)


def test_the_notification_names_every_non_passed_category_not_only_failures():
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID, run_id="t", started_at="now",
        outcome=ps.OUTCOME_DEGRADED, measured=True,
        stages_declared=4, stages_passed=1, stages_no_dry_path=1,
        stages_unsweepable=1, stages_unsweepable_upstream_pending=1,
        stages_unmeasured=1,
        coverage_findings=[
            {"kind": ps.FINDING_NO_DRY_PATH, "stage": "SaturdayHealthCheck",
             "blocking": False, "finding": "SaturdayHealthCheck has NO dry path"},
        ],
        results=[
            {"stage": "DataPhase1", "verdict": ps.PASSED},
            {"stage": "SaturdayHealthCheck", "verdict": ps.NO_DRY_PATH_VERDICT,
             "reason": "threads no $.preflight_args",
             "acknowledged_reason": "consumer, not a precondition"},
            {"stage": "PredictorBacktest", "verdict": ps.UNSWEEPABLE_VERDICT,
             "unsweepable_kind": ps.UNSWEEPABLE_UPSTREAM_PENDING,
             "reason": "NOT MEASURABLE TODAY"},
            {"stage": "EvaluatorOptimize", "verdict": ps.UNMEASURED,
             "reason": "worker died"},
        ],
    )
    subject, message = ps.render_notification(report)
    # Every non-passed stage is named in the BODY.
    for stage in ("SaturdayHealthCheck", "PredictorBacktest", "EvaluatorOptimize"):
        assert stage in message, stage
    assert "NO DRY PATH" in message
    assert "consumer, not a precondition" in message
    # And the subject's denominator is not silently over a hidden category.
    assert "no dry path" in subject or "no-dry-path" in subject
    assert "4" in subject


def test_a_missing_verdict_is_filled_in_as_not_attempted_and_fails_the_run():
    """The invariant is self-healing AND loud: a hole in the sweep's own
    coverage outranks anything the sweep found."""
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    assert len(report.results) == report.stages_declared
    # Nothing should be missing today; the guard is that if it ever is, the
    # run fails rather than under-reporting.
    if report.stages_not_attempted:
        assert report.outcome == ps.OUTCOME_FAILED


# ── End to end, over the LIVE definition ─────────────────────────────────────


def _fake_checkout(root: pathlib.Path) -> pathlib.Path:
    """A checkout where every declared launcher exists and implements the flag,
    so the live definition's stages classify SWEEPABLE and actually run.

    With ONE deliberate exception: a stage acknowledged in the manifest's
    `dedicated_box_stages` runs on a box its own dispatcher creates, and the
    sweep's launcher box does not carry that checkout at all. Fabricating its
    launcher here would model a box that does not exist and would make the
    acknowledgement read as stale on every run — which is exactly the
    stale-entry finding manifest_disagreement is supposed to raise, so the
    fixture must reproduce the real box's contents rather than defeat it.
    """
    from infrastructure.preflight_sweep_stages import (
        derive_shell_run_bindings,
        derive_stages,
        apply_map_bindings,
        load_manifest,
    )

    definition = json.loads(SF_PATH.read_text())
    manifest = load_manifest(MANIFEST_PATH)
    dedicated = {
        entry["stage"] for entry in (manifest.get("dedicated_box_stages") or [])
    }
    bindings = apply_map_bindings(derive_shell_run_bindings(definition), manifest)
    bindings.setdefault("run_date", "2026-08-14")
    for stage in derive_stages(definition, bindings, {"Execution": {"Name": "t", "Id": "t"}}, "/x"):
        if not (stage.box_dir and stage.launcher) or stage.name in dedicated:
            continue
        path = root / stage.box_dir / stage.launcher
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/bash\n# --preflight-only\n")
    return root


def test_the_weekday_case_remeasures_against_the_last_populated_instance(tmp_path):
    """The SOTA half of alpha-engine-config-I10717: the weekly SF has not run
    TODAY, but it wrote real content 3 days ago (last Saturday), which is well
    inside the declared 8-day max_age_days. Both stages accept --run-date and
    implement a real --preflight-only path, so the sweep re-derives and
    re-runs them against that populated instance instead of settling for
    upstream_pending — a stage whose environment is healthy is PASSED for
    real, not merely acknowledged as unmeasured."""
    root = _fake_checkout(tmp_path)
    run_date = _sweep_run_date()
    historical_date = (dt.date.fromisoformat(run_date) - dt.timedelta(days=3)).isoformat()

    def runner(argv, **_kwargs):
        script = argv[-1]
        backtest_chain = (
            "spot_predictor_backtest.sh" in script
            or "spot_portfolio_optimizer_backtest.sh" in script
        )
        # Mirrors the real script's own S3-nonempty gate: it only proceeds
        # past the stage preflight when RUN_DATE points at a populated
        # prefix — which today's date is not, and the historical one is.
        rc = 0 if not backtest_chain or f"RUN_DATE='{historical_date}'" in script else 1
        return subprocess.CompletedProcess(
            args=argv, returncode=rc, stdout="",
            stderr=("" if rc == 0 else
                    f"ERROR: s3://alpha-engine-research/backtest/{run_date}/ is empty "
                    "or unreachable.\n"),
        )

    aws = FakeAws(listing={
        f"backtest/{run_date}/": [
            {"Key": f"backtest/{run_date}/.phases/preflight.json", "Size": 235},
            {"Key": f"backtest/{run_date}/.phases/runtime_smoke.json", "Size": 239},
        ],
        f"backtest/{historical_date}/": [
            {"Key": f"backtest/{historical_date}/predictor_sweep_df.parquet", "Size": 91234},
        ],
    })
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws, runner=runner)

    assert report.stages_failed == 0
    assert report.stages_unsweepable_upstream_pending == 0
    assert report.stages_unsweepable_coverage_defect == 0
    remeasured = {
        r["stage"]: r for r in report.results
        if r["stage"] in ("PredictorBacktest", "PortfolioOptimizerBacktest")
    }
    assert set(remeasured) == {"PredictorBacktest", "PortfolioOptimizerBacktest"}
    for stage, row in remeasured.items():
        assert row["verdict"] == ps.PASSED, (stage, row)
        assert row["measured_against_run_date"] == historical_date, (stage, row)
        assert "MEASURED AGAINST" in row["reason"]
        assert historical_date in row["reason"]
    # Nothing left to keep the run out of `ok` — the pair is MEASURED, not
    # merely acknowledged as unmeasurable.
    assert report.outcome == ps.OUTCOME_OK
    ps.emit(report, aws, "arn:sns")
    assert f"{ps.REPORT_PREFIX}/last_clean.json" in aws.objects


def test_the_weekday_case_stays_upstream_pending_when_nothing_is_populated_within_max_age(tmp_path):
    """THE case that made the sweep's first run email '2 failed', now the
    RARER edge (real staleness, not the ordinary weekday state): the weekly
    SF has not run today, AND nothing populated the upstream prefix at any
    point in the declared 8-day max_age_days window either — the remeasure
    attempt (alpha-engine-config-I10717 (2)) has nothing to re-run against,
    so the stage stays upstream_pending/degraded, now carrying an explicit
    staleness note rather than reading as the routine case."""
    root = _fake_checkout(tmp_path)

    def runner(argv, **_kwargs):
        script = argv[-1]
        rc = 1 if ("spot_predictor_backtest.sh" in script
                   or "spot_portfolio_optimizer_backtest.sh" in script) else 0
        return subprocess.CompletedProcess(
            args=argv, returncode=rc, stdout="",
            stderr=("ERROR: s3://alpha-engine-research/backtest/2026-08-14/ is empty "
                    "or unreachable.\n" if rc else ""),
        )

    run_date = _sweep_run_date()
    aws = FakeAws(listing={f"backtest/{run_date}/": [
        {"Key": f"backtest/{run_date}/.phases/preflight.json", "Size": 235},
        {"Key": f"backtest/{run_date}/.phases/runtime_smoke.json", "Size": 239},
    ]})
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws, runner=runner)

    assert report.stages_failed == 0
    assert report.stages_unsweepable_upstream_pending == 2
    assert report.stages_unsweepable_coverage_defect == 0
    pending = {r["stage"] for r in report.results
               if r.get("unsweepable_kind") == ps.UNSWEEPABLE_UPSTREAM_PENDING}
    assert pending == {"PredictorBacktest", "PortfolioOptimizerBacktest"}
    for r in report.results:
        if r["stage"] in pending:
            assert "STALENESS finding" in r["reason"]
            assert r.get("measured_against_run_date") is None
    # alpha-engine-config-I7267: +2 (PitParityLookaheadResourceKillCheck,
    # PitParityWalkforwardResourceKillCheck), both acknowledged no-dry-path
    # stages in infrastructure/preflight_sweep_manifest.json.
    # alpha-engine-config-I9329: +1 (ResearchPredictorParallel.EvalJudgeProcess)
    # — the eval-judge cutover turned that state into an ssm:sendCommand stage
    # that threads $.preflight_args, so the denominator GREW. This tripwire
    # firing on a stage addition is the tripwire working: a new stage must be
    # picked up automatically or fail, never counted silently.
    assert len(report.results) == report.stages_declared == 22
    # It does not page and it does not claim a clean run.
    assert report.outcome == ps.OUTCOME_DEGRADED
    ps.emit(report, aws, "arn:sns")
    assert ("PreflightSweepStagesFailed", 0) in aws.metrics
    assert f"{ps.REPORT_PREFIX}/last_clean.json" not in aws.objects


def test_a_genuinely_broken_backtest_stage_still_fails_when_upstream_is_present(tmp_path):
    """The other half of the closes-when: the declaration must not become a
    blanket excuse on the day the pipeline HAS run."""
    root = _fake_checkout(tmp_path)

    def runner(argv, **_kwargs):
        rc = 1 if "spot_predictor_backtest.sh" in argv[-1] else 0
        return subprocess.CompletedProcess(args=argv, returncode=rc, stdout="", stderr="")

    run_date = _sweep_run_date()
    aws = FakeAws(listing={f"backtest/{run_date}/": [
        {"Key": f"backtest/{run_date}/.phases/preflight.json", "Size": 235},
        {"Key": f"backtest/{run_date}/results/backtest.parquet", "Size": 8123},
    ]})
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws, runner=runner)
    failed = {r["stage"] for r in report.results if r["verdict"] == ps.FAILED}
    assert "PredictorBacktest" in failed
    assert report.outcome == ps.OUTCOME_FAILED


def test_the_report_written_to_s3_carries_a_verdict_for_every_declared_stage(tmp_path):
    root = _fake_checkout(tmp_path)
    aws = FakeAws()
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws,
                      runner=_completed(0))
    ps.emit(report, aws, "arn:sns")
    written = aws.objects[f"{ps.REPORT_PREFIX}/latest.json"]
    assert len(written["results"]) == written["stages_declared"]
    assert written["declared_stages"]
    # And a console row exists for the stages that have no dry path, so they
    # are visible rather than absent from the surface entirely.
    for stage in ("SaturdayHealthCheck", "WeeklySubstrateHealthCheck",
                  "ResearchPredictorParallel.ResolveZooSpecs"):
        assert f"ops/checks/{stage_check_id(stage)}/latest.json" in aws.objects


# ── Console surface ──────────────────────────────────────────────────────────


def test_the_declared_cadence_is_read_never_defaulted():
    cadence = load_cadence(CADENCE_PATH)
    assert cadence["sweep_cadence"] in cadence["allowed_values"]
    assert cadence["cadence_minutes"] > 0


def test_a_cadence_manifest_that_cannot_be_read_raises(tmp_path):
    missing = tmp_path / "nope.json"
    with pytest.raises(Exception):
        load_cadence(missing)


@pytest.fixture
def cadence():
    return load_cadence(CADENCE_PATH)


def test_a_passed_stage_renders_ok_with_its_own_age(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": ps.PASSED}, "run-1", "2026-08-13T08:00:00+00:00", cadence
    )
    assert body["status"] == ENVELOPE_OK
    # ran_at + cadence_minutes are what let the console re-derive staleness
    # itself rather than trusting the status written here.
    assert body["ran_at"] == "2026-08-13T08:00:00+00:00"
    assert body["cadence_minutes"] == cadence["cadence_minutes"]


def test_an_unmeasured_stage_never_renders_green(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": ps.UNMEASURED}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ATTENTION
    assert body["status"] != ENVELOPE_OK


def test_an_unsweepable_stage_is_an_error_because_it_is_unmonitored(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": "unsweepable"}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ERROR


def test_an_upstream_pending_stage_renders_attention_not_error_and_not_green(cadence):
    body = stage_envelope(
        {"stage": "PredictorBacktest", "verdict": "unsweepable",
         "unsweepable_kind": ps.UNSWEEPABLE_UPSTREAM_PENDING}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ATTENTION
    assert body["status"] != ENVELOPE_OK


def test_an_unrecognised_unsweepable_kind_keeps_the_loud_default(cadence):
    body = stage_envelope(
        {"stage": "X", "verdict": "unsweepable", "unsweepable_kind": "brand_new"},
        "r", "t", cadence,
    )
    assert body["status"] == ENVELOPE_ERROR


def test_a_no_dry_path_stage_renders_attention_never_green(cadence):
    """It is not healthy — the sweep asserts nothing whatever about it. A row
    reading `ok` would claim precondition-health nobody measured."""
    body = stage_envelope(
        {"stage": "SaturdayHealthCheck", "verdict": "no_dry_path",
         "reason": "threads no $.preflight_args"}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ATTENTION
    assert body["status"] != ENVELOPE_OK
    assert "no declared envelope mapping" not in body["verdict"]


def test_the_rollup_names_the_no_dry_path_stages_rather_than_counting_them(cadence):
    report = {
        "run_id": "r", "measured": True, "outcome": "degraded",
        "stages_declared": 3, "stages_no_dry_path": 1, "stages_passed": 2,
        "results": [
            {"stage": "A", "verdict": ps.PASSED},
            {"stage": "B", "verdict": ps.PASSED},
            {"stage": "SaturdayHealthCheck", "verdict": "no_dry_path"},
        ],
    }
    rollup = rollup_envelope(report, cadence, "t")
    assert rollup["no_dry_path_stages"] == ["SaturdayHealthCheck"]
    # The no-dry-path row must not cancel out the stage it represents.
    assert rollup["stages_expected"] == 2 and rollup["stages_reported"] == 2
    assert rollup["status"] == ENVELOPE_ATTENTION


def test_an_acknowledged_coverage_gap_does_not_turn_the_rollup_red(cadence):
    rollup = rollup_envelope(
        {"run_id": "r", "measured": True, "stages_declared": 1, "stages_passed": 1,
         "stages_no_dry_path": 0,
         "coverage_findings": [{"kind": "no_dry_path", "blocking": False,
                                "finding": "acknowledged"}],
         "results": [{"stage": "A", "verdict": ps.PASSED}]},
        cadence, "t",
    )
    assert rollup["status"] == ENVELOPE_OK
    assert rollup["coverage_findings_blocking"] == 0


def test_a_legacy_string_coverage_finding_is_still_treated_as_blocking(cadence):
    rollup = rollup_envelope(
        {"run_id": "r", "measured": True, "stages_declared": 1, "stages_passed": 1,
         "coverage_findings": ["a stage lost its dry path"],
         "results": [{"stage": "A", "verdict": ps.PASSED}]},
        cadence, "t",
    )
    assert rollup["status"] == ENVELOPE_ERROR


def test_a_verdict_with_no_declared_mapping_is_not_silently_green(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": "something_new"}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ATTENTION
    assert "no declared envelope mapping" in body["verdict"]


def test_every_row_says_it_covers_preconditions_not_output_correctness(cadence):
    body = stage_envelope({"stage": "X", "verdict": ps.PASSED}, "r", "t", cadence)
    assert "PRECONDITIONS only" in body["summary"]
    assert "output is correct" in body["summary"]


def test_a_stage_the_sweep_produced_no_verdict_for_is_counted_unobserved(cadence):
    """UNOBSERVED must be visible and inside the denominator. A stage nobody
    checks is the one that breaks."""
    report = {
        "run_id": "r", "measured": True, "outcome": "ok",
        "stages_declared": 19, "stages_no_dry_path": 3,
        "stages_passed": 10, "results": [{"stage": f"S{i}", "verdict": ps.PASSED} for i in range(10)],
    }
    rollup = rollup_envelope(report, cadence, "t")
    assert rollup["stages_expected"] == 16
    assert rollup["stages_reported"] == 10
    assert rollup["stages_unobserved"] == 6
    # And it must not render green while six stages are unaccounted for.
    assert rollup["status"] == ENVELOPE_ERROR


def test_the_rollup_publishes_zero_unobserved_rather_than_omitting_it(cadence):
    report = {
        "run_id": "r", "measured": True, "outcome": "ok",
        "stages_declared": 3, "stages_no_dry_path": 0, "stages_passed": 3,
        "results": [{"stage": f"S{i}", "verdict": ps.PASSED} for i in range(3)],
    }
    rollup = rollup_envelope(report, cadence, "t")
    assert rollup["stages_unobserved"] == 0
    assert "stages_unobserved" in rollup
    assert rollup["status"] == ENVELOPE_OK


def test_an_unmeasured_run_rollup_is_an_error_row(cadence):
    rollup = rollup_envelope(
        {"run_id": "r", "measured": False, "unmeasured_reason": "no box"}, cadence, "t"
    )
    assert rollup["status"] == ENVELOPE_ERROR
    assert rollup["unmeasured_reason"] == "no box"


def test_every_run_publishes_a_row_for_every_reported_stage_plus_the_rollup(cadence):
    report = {
        "run_id": "r", "measured": True, "outcome": "failed",
        "stages_declared": 2, "stages_no_dry_path": 0, "stages_failed": 1,
        "results": [
            {"stage": "A", "verdict": ps.PASSED},
            {"stage": "B", "verdict": ps.FAILED, "reason": "rc=1"},
        ],
    }
    published = envelopes(report, cadence)
    keys = {k for k, _ in published}
    assert f"ops/checks/{stage_check_id('A')}/latest.json" in keys
    assert f"ops/checks/{stage_check_id('B')}/latest.json" in keys
    assert "ops/checks/ae-preflight-sweep/latest.json" in keys


def test_a_parallel_branch_stage_gets_a_stable_unique_console_id():
    a = stage_check_id("ResearchPredictorParallel.PredictorTraining")
    b = stage_check_id("ParityParallel.PredictorTraining")
    assert a != b
    assert "." not in a


def test_console_rows_are_published_even_when_the_run_failed(cadence):
    """A row that stops being republished is what makes the console render
    STALE. Skipping the write on a bad run turns a loud failure into a quiet
    ageing green."""
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID, run_id="r", started_at="now",
        outcome=ps.OUTCOME_FAILED, measured=True, stages_declared=1, stages_failed=1,
        results=[{"stage": "A", "verdict": ps.FAILED, "reason": "rc=1"}],
    )
    ps.emit(report, aws, "arn:sns")
    assert f"ops/checks/{stage_check_id('A')}/latest.json" in aws.objects
    assert "ops/checks/ae-preflight-sweep/latest.json" in aws.objects


def test_the_sweep_derives_run_date_from_utc_not_local_time():
    """Pins what `_sweep_run_date` above mirrors.

    The sweep binds `run_date` from the execution's UTC start time, matching
    the SF's `InitializeInput`. Two tests build S3 fixtures from that date; if
    the sweep ever switched to local time they would silently start probing a
    prefix the fixture does not declare, and the upstream-pending assertions
    would pass vacuously rather than fail. Caught live 2026-08-15 in the
    17:00-24:00 PT window (alpha-engine-config-I7431).
    """
    import inspect

    src = inspect.getsource(ps.sweep)
    assert "started.date().isoformat()" in src, (
        "sweep no longer binds run_date from `started` — re-derive "
        "_sweep_run_date() from whatever it uses now"
    )
    assert "dt.datetime.now(dt.timezone.utc)" in inspect.getsource(ps), (
        "the sweep's `started` is no longer UTC; _sweep_run_date() is wrong"
    )
    assert "dt.date.today()" not in src, (
        "sweep uses LOCAL today() for run_date — the fixtures, the SF's "
        "InitializeInput and the sweep must all agree on one clock"
    )


# ── The probe is dated by the NYSE trading day the LAUNCHER uses ─────────────
# Measured 2026-08-26. `spot_common_normalize_run_date` (crucible-backtester
# infrastructure/_spot_common.sh) snaps RUN_DATE back to the previous trading
# day before the stage preflight reads `backtest/${RUN_DATE}/`, so on every
# non-trading day the sweep's raw calendar probe named a prefix the stage never
# read — and an empty probe DOWNGRADES a real failure to upstream_pending,
# which does not page.

_WEEKEND_BINDINGS = {"run_date": "2026-08-23"}  # a Sunday
_NORMALIZED_DECL = {**DECL, "date_normalization": "nyse_trading_day"}


def test_the_probe_uses_the_trading_day_the_launcher_normalized_to():
    seen: list[str] = []

    def lister(prefix):
        seen.append(prefix)
        return []

    ps.classify_upstream(_failed(), _NORMALIZED_DECL, _WEEKEND_BINDINGS, lister)
    assert seen == ["backtest/2026-08-21/"], (
        "the Sunday probe must name the Friday prefix the stage actually read"
    )


def test_a_real_weekend_failure_is_not_downgraded_when_the_traded_prefix_is_full():
    """The regression this exists for: before the normalization, the probe named
    the always-empty Sunday prefix and reclassified a REAL failure."""
    listing = {"backtest/2026-08-21/": [
        {"Key": "backtest/2026-08-21/results/backtest.parquet", "Size": 8123},
    ]}
    result = ps.classify_upstream(
        _failed(), _NORMALIZED_DECL, _WEEKEND_BINDINGS,
        lambda p: listing.get(p, []),
    )
    assert result.verdict == ps.FAILED
    assert "IS populated" in result.reason


def test_an_uncomputable_probe_date_is_unmeasured_never_the_calendar_date(monkeypatch):
    """Falling back to the calendar date IS the defect, so it must never happen:
    the sweep says it could not tell instead."""
    def boom(_run_date, _mode):
        raise ps.DateNormalizationUnavailable("no trading calendar on this box")

    monkeypatch.setattr(ps, "normalize_probe_date", boom)
    result = ps.classify_upstream(
        _failed(), _NORMALIZED_DECL, _WEEKEND_BINDINGS, lambda _p: [])
    assert result.verdict == ps.UNMEASURED
    assert "no trading calendar on this box" in result.reason


def test_every_declared_upstream_dependency_declares_its_probe_date_rule():
    manifest = ps.load_manifest(MANIFEST_PATH)
    decls = manifest["upstream_artifact_dependencies"]
    assert decls
    for entry in decls:
        assert entry.get("date_normalization") in ("none", "nyse_trading_day"), entry


# ── A ratified no-dry-path stage must not make `ok` unreachable ──────────────
# 13 of 13 runs (2026-08-14..2026-08-26) were non-clean and
# `_preflight_sweep/last_clean.json` was never written once, because `clean`
# was gated on the raw no-dry-path count and five stages are permanently
# ratified as having none. A status with one reachable value grades nothing.


def test_a_run_whose_only_gap_is_ratified_no_dry_path_stages_is_ok(tmp_path):
    root = _fake_checkout(tmp_path)
    run_date = _sweep_run_date()
    aws = FakeAws(listing={f"backtest/{run_date}/": [
        {"Key": f"backtest/{run_date}/results/backtest.parquet", "Size": 8123},
    ]})
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws,
                      runner=_completed(0))
    assert report.stages_failed == 0
    assert report.stages_no_dry_path > 0, "the ratified gaps are still counted"
    assert report.stages_no_dry_path_unacknowledged == 0
    assert report.outcome == ps.OUTCOME_OK
    ps.emit(report, aws, "arn:sns")
    assert f"{ps.REPORT_PREFIX}/last_clean.json" in aws.objects


def test_an_unacknowledged_no_dry_path_stage_still_keeps_the_run_out_of_ok(tmp_path):
    root = _fake_checkout(tmp_path)
    manifest = json.loads(MANIFEST_PATH.read_text())
    manifest["no_dry_path_stages"] = [
        e for e in manifest["no_dry_path_stages"]
        if e["stage"] != "SaturdayHealthCheck"
    ]
    stripped = tmp_path / "manifest.json"
    stripped.write_text(json.dumps(manifest))
    report = ps.sweep(SF_PATH, stripped, str(root), "t", runner=_completed(0))
    assert report.stages_no_dry_path_unacknowledged == 1
    assert report.outcome == ps.OUTCOME_FAILED


def test_the_gaps_stay_named_on_every_surface_even_on_an_ok_run(tmp_path):
    """Reachable OK must not be bought by hiding the coverage gap."""
    root = _fake_checkout(tmp_path)
    run_date = _sweep_run_date()
    aws = FakeAws(listing={f"backtest/{run_date}/": [
        {"Key": f"backtest/{run_date}/results/backtest.parquet", "Size": 8123},
    ]})
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws,
                      runner=_completed(0))
    assert report.outcome == ps.OUTCOME_OK
    named = {f.get("stage") for f in report.coverage_findings
             if f.get("kind") == ps.FINDING_NO_DRY_PATH}
    rows = {r["stage"] for r in report.results
            if r["verdict"] == ps.NO_DRY_PATH_VERDICT}
    assert named == rows and len(rows) == report.stages_no_dry_path > 0


# ── The zero-spend rehearsal must agree with the live path ──────────────────


def test_the_derive_only_rehearsal_reports_no_blocking_finding_on_the_real_pipeline():
    """`--derive-only` is the documented zero-spend rehearsal. It bound run_date
    AFTER the map-binding scan, so it emitted a blocking
    map_binding_disagreement for `$.run_date` that `sweep()` never emits — the
    rehearsal said the pipeline could not be rendered while production rendered
    all 21 stages. It also used LOCAL `date.today()` against the sweep's UTC,
    the timezone-dependence class of alpha-engine-config#7390."""
    proc = subprocess.run(
        [sys.executable, str(REPO / "infrastructure/preflight_sweep.py"),
         "--derive-only", "--checkout-root", "/nonexistent",
         "--definition", str(SF_PATH), "--manifest", str(MANIFEST_PATH)],
        capture_output=True, text=True, cwd=str(REPO),
        env={**os.environ, "PYTHONPATH": str(REPO)},
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    out = json.loads(proc.stdout)
    assert out["coverage_findings"] == []
    assert len(out["stages"]) > 0


# ── Dedicated-box stages: acknowledged, never silently excluded (I9329) ──────
#
# ResearchPredictorParallel.EvalJudgeProcess threads $.preflight_args and its
# launcher DOES implement --preflight-only, but nousergon-data-PR1593 moved it
# onto a spot box that its own dispatcher bootstraps. The sweep's launcher box
# has no crucible-research checkout and no .venv there, so derivation reports
# "launcher not present in the checkout" and the stage classified
# coverage_defect — the SOLE failure input on 7 consecutive nightly runs
# (2026-08-30..2026-09-06), one short of the streak threshold of 8. The fix is
# a DECLARED acknowledgement with its own unsweepable kind, not an exclusion:
# these tests hold the line that acknowledging a gap never hides it.

DEDICATED_STAGE = "ResearchPredictorParallel.EvalJudgeProcess"


def _dedicated_result(report):
    return next(r for r in report.results if r["stage"] == DEDICATED_STAGE)


def test_the_dedicated_box_stage_is_acknowledged_in_the_live_manifest():
    """The declaration is in the manifest, not in this file — a test that
    carries its own manifest proves the parser works and nothing about the
    pipeline."""
    manifest = json.loads(MANIFEST_PATH.read_text())
    declared = {e["stage"]: e for e in manifest["dedicated_box_stages"]}
    assert DEDICATED_STAGE in declared
    entry = declared[DEDICATED_STAGE]
    assert entry["acknowledged"], "an acknowledgement with no date cannot age"
    assert entry["reason"].strip(), "an acknowledgement with no reason grades nothing"


def test_an_acknowledged_dedicated_box_stage_keeps_its_own_kind_and_does_not_fail_the_run(
    tmp_path,
):
    """The whole deliverable in one test: everything else passes, the run is
    OK, and the gap is still carried by its own kind rather than by silence."""
    root = _fake_checkout(tmp_path)
    run_date = _sweep_run_date()
    aws = FakeAws(listing={f"backtest/{run_date}/": [
        {"Key": f"backtest/{run_date}/results/backtest.parquet", "Size": 8123},
    ]})
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws,
                      runner=_completed(0))

    row = _dedicated_result(report)
    assert row["verdict"] == ps.UNSWEEPABLE_VERDICT
    assert row["unsweepable_kind"] == ps.UNSWEEPABLE_DEDICATED_BOX
    assert row["acknowledged_reason"], "the row must carry the written reason"
    # Out of the count that fails the run, and into its own.
    assert report.stages_unsweepable_dedicated_box == 1
    assert report.stages_unsweepable_coverage_defect == 0
    assert report.stages_failed == 0
    assert report.outcome == ps.OUTCOME_OK


def test_the_dedicated_box_gap_is_named_on_every_surface_on_that_ok_run(tmp_path):
    """OK is never bought by hiding the gap: a non-blocking finding, the
    notification's acknowledged-gaps block, the declared-stage row, and an
    ATTENTION console row — none of them green, none of them absent."""
    root = _fake_checkout(tmp_path)
    run_date = _sweep_run_date()
    aws = FakeAws(listing={f"backtest/{run_date}/": [
        {"Key": f"backtest/{run_date}/results/backtest.parquet", "Size": 8123},
    ]})
    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", aws=aws,
                      runner=_completed(0))
    assert report.outcome == ps.OUTCOME_OK

    findings = [f for f in report.coverage_findings
                if f.get("kind") == ps.FINDING_DEDICATED_BOX]
    assert len(findings) == 1
    assert findings[0]["stage"] == DEDICATED_STAGE
    assert findings[0]["blocking"] is False, (
        "a declared dedicated-box gap must not fail the run"
    )

    declared = {d["stage"]: d for d in report.declared_stages}
    assert declared[DEDICATED_STAGE]["dedicated_box_acknowledged"] is True

    ps.emit(report, aws, "arn:sns")
    _subject, body = aws.notifications[-1]
    assert DEDICATED_STAGE in body
    assert "COVERAGE GAPS (acknowledged" in body
    assert "dedicated-box 1" in body, "the counter line names the new kind"

    envelope = stage_envelope(_dedicated_result(report), "t", "now",
                              load_cadence(CADENCE_PATH))
    assert envelope["status"] == ENVELOPE_ATTENTION, (
        "unreachable is never green and never a silent error"
    )
    rollup = rollup_envelope(report.to_dict(), load_cadence(CADENCE_PATH), "now")
    assert rollup["stages_unsweepable_dedicated_box"] == 1
    assert rollup["dedicated_box_stages"] == [DEDICATED_STAGE]
    assert rollup["status"] == ENVELOPE_ATTENTION


def test_an_acknowledged_dedicated_box_stage_never_escalates_the_streak():
    """It is unsweepable on every run BY CONSTRUCTION, so a streak would report
    the declaration back as a discovery on a schedule, forever."""
    dedicated = ps.StageResult(
        stage=DEDICATED_STAGE,
        verdict=ps.UNSWEEPABLE_VERDICT,
        unsweepable_kind=ps.UNSWEEPABLE_DEDICATED_BOX,
    )
    defect = ps.StageResult(
        stage="SomeOtherStage",
        verdict=ps.UNSWEEPABLE_VERDICT,
        unsweepable_kind=ps.UNSWEEPABLE_COVERAGE_DEFECT,
    )
    prior = {"streaks": {
        DEDICATED_STAGE: {"consecutive_runs": 99, "since": "2026-08-30T08:00:00+00:00"},
        "SomeOtherStage": {"consecutive_runs": 7, "since": "2026-08-30T08:00:00+00:00"},
    }}
    state, findings = update_streaks(prior, [dedicated, defect], "run", "now", 8)
    assert DEDICATED_STAGE not in state["streaks"], (
        "the acknowledged stage accrues no streak at all"
    )
    assert {f["stage"] for f in findings} == {"SomeOtherStage"}, (
        "the real coverage defect must still escalate — the exclusion is "
        "declared per stage, not a blanket softening"
    )


def test_an_unacknowledged_unsweepable_stage_is_unchanged_and_still_fails(tmp_path):
    """The direction that must NOT move. Drop the declaration and the same
    stage is a coverage_defect that fails the run, exactly as before."""
    root = _fake_checkout(tmp_path)
    manifest = json.loads(MANIFEST_PATH.read_text())
    manifest["dedicated_box_stages"] = []
    stripped = tmp_path / "manifest-no-ack.json"
    stripped.write_text(json.dumps(manifest))
    report = ps.sweep(SF_PATH, stripped, str(root), "t", runner=_completed(0))

    row = _dedicated_result(report)
    assert row["unsweepable_kind"] == ps.UNSWEEPABLE_COVERAGE_DEFECT
    assert row["acknowledged_reason"] is None
    assert report.stages_unsweepable_coverage_defect == 1
    assert report.stages_unsweepable_dedicated_box == 0
    assert report.outcome == ps.OUTCOME_FAILED
    assert not [f for f in report.coverage_findings
                if f.get("kind") == ps.FINDING_DEDICATED_BOX]


def test_a_stale_dedicated_box_acknowledgement_fails_the_run(tmp_path):
    """The acknowledgement is graded the other way too. A stage acknowledged as
    unreachable that the sweep CAN now reach is a blocking finding — an entry
    that outlives its cause is the silent exclusion this design exists to
    prevent."""
    root = _fake_checkout(tmp_path)
    # Fabricate the launcher the real launcher box does not carry: the stage is
    # now sweepable, so the declaration is stale.
    manifest = json.loads(MANIFEST_PATH.read_text())
    from infrastructure.preflight_sweep_stages import (
        apply_map_bindings,
        derive_shell_run_bindings,
        derive_stages,
    )
    definition = json.loads(SF_PATH.read_text())
    bindings = apply_map_bindings(derive_shell_run_bindings(definition), manifest)
    bindings.setdefault("run_date", "2026-08-14")
    stage = next(
        s for s in derive_stages(definition, bindings,
                                 {"Execution": {"Name": "t", "Id": "t"}}, "/x")
        if s.name == DEDICATED_STAGE
    )
    path = root / stage.box_dir / stage.launcher
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\n# --preflight-only\n")

    report = ps.sweep(SF_PATH, MANIFEST_PATH, str(root), "t", runner=_completed(0))
    stale = [f for f in report.coverage_findings
             if f.get("kind") == ps.FINDING_MANIFEST_DISAGREEMENT
             and DEDICATED_STAGE in f["finding"]]
    assert stale, report.coverage_findings
    assert stale[0]["blocking"] is True
    assert report.outcome == ps.OUTCOME_FAILED
