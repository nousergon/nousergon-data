"""
Tests for validators/stage_output_sweep.py — the stage-level output assertion
(alpha-engine-config-I7167).

Two obligations shape this file, both from measured fleet failures rather than
from a coverage target:

1. **The assertion must be PROVEN able to fail.** This fleet has shipped three
   detectors that could not — an assertion whose failing branch is never
   exercised is indistinguishable from one that always passes, and the
   difference only shows up on the run you needed it for. Every verdict here
   has a test that produces it, and ``test_enforce_exits_1_on_a_defect`` walks
   the whole path from a stale key to a non-zero process exit.

2. **"Could not measure" must never be reported as "found a defect"**, and
   never as health either. The ``unmeasured`` verdict has its own tests in both
   directions, including the one that matters most —
   ``test_s3_permission_error_is_unmeasured_not_missing``, because a 403
   rendered as a missing artifact is a detector reporting its own harness fault
   as a finding about the system, always in the alarming direction.

The five 2026-08-08 instances are replayed in ``TestAugust08Replay`` against
the live evidence recorded on I7167, each asserting the sweep WOULD have caught
what no surface reported.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from validators import stage_output_sweep as sos

RUN_DATE = "2026-08-08"
# The scheduled 2026-08-08 weekly run, execution
# 9b34ac0f-5e2f-f70b-d668-61b4c4751654_6fb6adf9-55fa-4426-6d06-79e4579bcaff:
# 02:00:49 → 06:20:44 PDT, terminated SUCCEEDED.
EXEC_START = datetime(2026, 8, 8, 9, 0, 49, tzinfo=timezone.utc)

PIPELINE = "ne-weekly-freshness-pipeline"


def _row(artifact_id, template, stage, pipeline=PIPELINE, **kw):
    row = {
        "artifact_id": artifact_id,
        "s3_key_template": template,
        "cadence": "eod_sf",
        "sla_minutes_after_cron": 60,
        "severity": "warning",
        "owner_repo": "alpha-engine-data",
        "created_at": "2026-06-01",
        "produced_by": [{"pipeline": pipeline, "stage": stage}],
    }
    row.update(kw)
    return row


def _head_from(mapping):
    """Build a ``head`` callable from ``{key: last_modified | 'error:<why>'}``.

    A key absent from the mapping is an absent object; a value beginning
    ``error:`` is an S3 fault. Injecting the head is what makes every branch —
    including the ones S3 will not produce on demand — actually reachable.
    """

    def head(bucket, key):
        if key not in mapping:
            return "absent", None
        value = mapping[key]
        if isinstance(value, str) and value.startswith("error:"):
            return "error", value[len("error:"):]
        return "found", value

    return head


# ---------------------------------------------------------------------------
# resolve_key
# ---------------------------------------------------------------------------


class TestResolveKey:
    @pytest.mark.parametrize("placeholder", ["date", "trading_day", "run_date"])
    def test_run_date_placeholders_resolve(self, placeholder):
        got = sos.resolve_key("backtest/{%s}/report.md" % placeholder, run_date=RUN_DATE, cycle_date=RUN_DATE)
        assert got == f"backtest/{RUN_DATE}/report.md"

    def test_unplaceholdered_template_passes_through(self):
        assert (
            sos.resolve_key("market_data/macro_history.parquet", run_date=RUN_DATE, cycle_date=RUN_DATE)
            == "market_data/macro_history.parquet"
        )

    def test_unresolvable_placeholder_returns_none(self):
        """A key this sweep cannot build is not a key that is missing.

        head-ing a literal ``{cycle_label}`` would 404 every time and report a
        confident, permanent, entirely false `missing` — the most convincing
        wrong finding this sweep could produce.
        """
        assert sos.resolve_key("x/{cycle_label}/y.json", run_date=RUN_DATE, cycle_date=RUN_DATE) is None

    def test_mixed_resolvable_and_unresolvable_is_none(self):
        assert sos.resolve_key("a/{date}/{cycle_label}.json", run_date=RUN_DATE, cycle_date=RUN_DATE) is None


class TestCycleDate:
    """The regression guard for this module's own worst bug.

    Resolving ``{date}`` / ``{trading_day}`` to ``$.run_date`` produced 28
    confident false ``missing`` verdicts against the 2026-08-08 run — measured,
    not hypothetical: that run's ``$.run_date`` was ``2026-08-08`` while every
    artifact it wrote landed under ``2026-08-07``, and ``backtest/2026-08-08/``
    does not exist at all.

    A detector wrong in that direction, at that volume, is worse than no
    detector: it gets muted within a week and takes the real finding with it.
    """

    def test_saturday_run_resolves_to_the_prior_trading_day(self):
        assert sos.resolve_cycle_date("2026-08-08") == "2026-08-07"

    def test_a_trading_day_resolves_to_itself(self):
        assert sos.resolve_cycle_date("2026-08-07") == "2026-08-07"

    def test_cycle_date_is_not_run_date_by_default_in_main(
        self, monkeypatch, registry_file
    ):
        """``main()`` must NOT fall back to run_date for the cycle date.

        Pinned as its own test because every other test in TestMain passes
        ``--cycle-date`` explicitly, so none of them would notice the default
        regressing.
        """
        captured = {}

        def _fake_sweep(**kw):
            captured.update(kw)
            return {"status": "ok", "enforce": False, "defects": [], "unmeasured": [],
                    "checked": 0}

        monkeypatch.setattr(sos, "sweep", _fake_sweep)
        sos.main(["--run-date", "2026-08-08", "--registry", registry_file, "--no-alert"])
        assert captured["cycle_date"] is None, (
            "main() must pass cycle_date=None so sweep() resolves it via "
            "expected_last_close rather than silently reusing run_date"
        )

    def test_unresolved_cycle_date_is_unmeasured_not_missing(self):
        """When the resolver is unavailable, a dated key is UNMEASURED.

        Falling back to run_date here would reintroduce the 28-false-positive
        failure in the one situation where nobody would be looking for it.
        """
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "S")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({}), cycle_date=None,
        )[0]
        assert f["verdict"] == sos.UNMEASURED
        assert "cycle date unresolved" in f["detail"]

    def test_undated_keys_still_check_without_a_cycle_date(self):
        """A pointer-pattern key carries no placeholder, so losing the cycle
        date must not take it down too."""
        decls = sos.declared_outputs([_row("a", "pointer/latest.json", "S")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({}), cycle_date=None,
        )[0]
        assert f["verdict"] == sos.MISSING


class TestParseExecutionStart:
    def test_z_suffix(self):
        assert sos.parse_execution_start("2026-08-08T09:00:49Z") == EXEC_START

    def test_offset_form(self):
        assert sos.parse_execution_start("2026-08-08T09:00:49+00:00") == EXEC_START

    def test_naive_is_assumed_utc(self):
        assert sos.parse_execution_start("2026-08-08T09:00:49") == EXEC_START

    def test_garbage_raises(self):
        with pytest.raises(ValueError):
            sos.parse_execution_start("not-a-timestamp")


# ---------------------------------------------------------------------------
# Registry loading — the empty-producer-set trap
# ---------------------------------------------------------------------------


class TestLoadRegistry:
    def test_local_path_round_trip(self, tmp_path):
        import yaml

        path = tmp_path / "reg.yaml"
        path.write_text(yaml.safe_dump({"artifacts": [_row("a", "k", "S")]}))
        assert len(sos.load_registry_rows(registry_path=str(path))) == 1

    def test_missing_file_raises_rather_than_returning_empty(self, tmp_path):
        with pytest.raises(sos.RegistryUnreadable):
            sos.load_registry_rows(registry_path=str(tmp_path / "nope.yaml"))

    def test_empty_artifacts_list_raises(self, tmp_path):
        """An assertion over an empty producer set always passes.

        Returning ``[]`` here would make an unreadable/misparsed registry
        indistinguishable from a perfectly clean run — the failure mode this
        whole module exists to end, reproduced inside the detector.
        """
        import yaml

        path = tmp_path / "reg.yaml"
        path.write_text(yaml.safe_dump({"artifacts": []}))
        with pytest.raises(sos.RegistryUnreadable):
            sos.load_registry_rows(registry_path=str(path))

    def test_unparseable_yaml_raises(self, tmp_path):
        path = tmp_path / "reg.yaml"
        path.write_text("artifacts: [oops\n  - : :\n")
        with pytest.raises(sos.RegistryUnreadable):
            sos.load_registry_rows(registry_path=str(path))


class TestDeclaredOutputs:
    def test_filters_to_the_named_pipeline(self):
        rows = [
            _row("weekly", "a/{date}.json", "Backtester"),
            _row("preopen", "b/{date}.json", "Scanner", pipeline="ne-preopen-trading-pipeline"),
        ]
        got = sos.declared_outputs(rows, pipeline=PIPELINE)
        assert [d["artifact_id"] for d in got] == ["weekly"]

    def test_dual_producer_row_contributes_to_both(self):
        """The Scanner artifacts are written by the weekday preopen SF AND the
        weekly SF. A registry that can name only one is why a stopped weekly
        stage hides behind a live daily one — so both must be emitted.
        """
        row = _row("scanner", "scanner/{date}.json", "Scanner")
        row["produced_by"].append(
            {"pipeline": "ne-preopen-trading-pipeline", "stage": "Scanner"}
        )
        assert len(sos.declared_outputs([row], pipeline=PIPELINE)) == 1
        assert len(
            sos.declared_outputs([row], pipeline="ne-preopen-trading-pipeline")
        ) == 1

    def test_rows_without_produced_by_are_ignored(self):
        row = _row("x", "k", "S")
        del row["produced_by"]
        assert sos.declared_outputs([row], pipeline=PIPELINE) == []


# ---------------------------------------------------------------------------
# evaluate — one test per verdict, both polarities
# ---------------------------------------------------------------------------


class TestVerdicts:
    def _one(self, template, head_map, **kw):
        decls = sos.declared_outputs([_row("a", template, "Backtester")], pipeline=PIPELINE)
        kw.setdefault("execution_start", EXEC_START)
        kw.setdefault("cycle_date", RUN_DATE)
        return sos.evaluate(decls, run_date=RUN_DATE, head=_head_from(head_map), **kw)[0]

    def test_wrote_when_written_after_execution_start(self):
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": EXEC_START + timedelta(hours=2)})
        assert f["verdict"] == sos.WROTE

    def test_wrote_on_exact_boundary(self):
        """LastModified == execution start is INSIDE the window.

        An exclusive boundary would report a stage that wrote in the run's
        first second as stale — a false defect at the one moment the pipeline
        is least likely to be watched.
        """
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": EXEC_START})
        assert f["verdict"] == sos.WROTE

    def test_stale_when_the_key_predates_the_run(self):
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": EXEC_START - timedelta(days=87)})
        assert f["verdict"] == sos.STALE
        assert "87d" in f["detail"]

    def test_missing_when_no_object_exists(self):
        f = self._one("b/{date}.json", {})
        assert f["verdict"] == sos.MISSING

    def test_s3_permission_error_is_unmeasured_not_missing(self):
        """A 403 is not an absent artifact.

        Reporting it as ``missing`` is a detector filing its own harness fault
        as a defect in the system it watches — this fleet's most-repeated
        detector bug, and always in the alarming direction.
        """
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": "error:AccessDenied"})
        assert f["verdict"] == sos.UNMEASURED
        assert f["verdict"] != sos.MISSING
        assert "AccessDenied" in f["detail"]

    def test_transient_s3_fault_is_unmeasured_not_wrote(self):
        """The other polarity: an unanswerable check is not health either."""
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": "error:SlowDown"})
        assert f["verdict"] == sos.UNMEASURED
        assert f["verdict"] != sos.WROTE

    def test_present_key_without_a_window_is_unmeasured_not_wrote(self):
        """Absence is measurable without an execution window; staleness is not.

        Degrading a present key to ``wrote`` here is exactly how instances 2, 3
        and 4 stayed invisible for months: the key existed, so everything
        looked fine.
        """
        f = self._one(
            "b/{date}.json",
            {f"b/{RUN_DATE}.json": EXEC_START - timedelta(days=87)},
            execution_start=None,
        )
        assert f["verdict"] == sos.UNMEASURED

    def test_absence_is_still_a_defect_without_a_window(self):
        f = self._one("b/{date}.json", {}, execution_start=None)
        assert f["verdict"] == sos.MISSING

    def test_missing_last_modified_is_unmeasured(self):
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": None})
        assert f["verdict"] == sos.UNMEASURED

    def test_unresolvable_template_is_unmeasured(self):
        f = self._one("b/{cycle_label}.json", {})
        assert f["verdict"] == sos.UNMEASURED

    def test_unresolved_producer_row_is_its_own_verdict(self):
        """config-I7180's ratcheted gap is neither a pass nor a defect.

        Folding UNRESOLVED into ``wrote`` would let 20 unattributed artifacts
        inflate the green number; folding it into ``missing`` would fail every
        run for a registry gap that is already tracked and published.
        """
        decls = sos.declared_outputs([_row("a", "k", "UNRESOLVED")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, head=_head_from({}), cycle_date=RUN_DATE
        )[0]
        assert f["verdict"] == sos.UNRESOLVED
        assert f["verdict"] not in sos.DEFECT_VERDICTS


# ---------------------------------------------------------------------------
# I7392 — run-mode awareness: the `dry` verdict, same-cycle sibling
# attribution, self-exclusion, alert suppression, and run_mode parsing.
# ---------------------------------------------------------------------------


class TestDryVerdict:
    """The 2026-08-21 Friday shell run: ApplyShellRunDefaults guarantees zero
    producer writes, by construction. A missing/stale key on that run is not
    a defect — it is the dry-path contract working as designed.
    """

    def _one(self, template, head_map, run_mode, **kw):
        decls = sos.declared_outputs([_row("a", template, "Backtester")], pipeline=PIPELINE)
        kw.setdefault("execution_start", EXEC_START)
        kw.setdefault("cycle_date", RUN_DATE)
        return sos.evaluate(
            decls, run_date=RUN_DATE, head=_head_from(head_map), run_mode=run_mode, **kw
        )[0]

    def test_missing_key_on_a_dry_run_is_dry_not_missing(self):
        f = self._one("b/{date}.json", {}, sos.RUN_MODE_DRY)
        assert f["verdict"] == sos.DRY
        assert f["verdict"] != sos.MISSING
        assert f["verdict"] not in sos.DEFECT_VERDICTS

    def test_stale_key_on_a_dry_run_is_dry_not_stale(self):
        f = self._one(
            "b/{date}.json",
            {f"b/{RUN_DATE}.json": EXEC_START - timedelta(days=5)},
            sos.RUN_MODE_DRY,
        )
        assert f["verdict"] == sos.DRY
        assert f["verdict"] != sos.STALE
        assert f["verdict"] not in sos.DEFECT_VERDICTS

    def test_missing_key_on_a_real_run_is_still_missing(self):
        """The carve-out is scoped to run_mode=dry only — a real weekly or
        exercise run with a genuinely missing artifact must still fail loud.
        """
        f = self._one("b/{date}.json", {}, "weekly")
        assert f["verdict"] == sos.MISSING

    def test_missing_key_with_unknown_run_mode_is_still_missing(self):
        """Unknown run_mode must never be treated as license to go quiet —
        same "fail toward asserting" rule as entered_stages=None."""
        f = self._one("b/{date}.json", {}, None)
        assert f["verdict"] == sos.MISSING

    def test_a_key_actually_written_under_a_dry_run_still_reports_wrote(self):
        """SignalsEnvelope/ChallengerShadow run for-real even under
        shell_run=true — a key they DID write must still read WROTE, not be
        swept into the dry downgrade."""
        f = self._one(
            "b/{date}.json",
            {f"b/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)},
            sos.RUN_MODE_DRY,
        )
        assert f["verdict"] == sos.WROTE

    def test_s3_error_on_a_dry_run_is_still_unmeasured_not_dry(self):
        """Predicate 3 (analogous to I7392's sibling issue I7412's own
        predicate 3): the dry path must still fail loud on a real defect. A
        denied S3 read on a dry run is a harness fault, not evidence the
        dry-path contract explains an absence."""
        f = self._one(
            "b/{date}.json", {f"b/{RUN_DATE}.json": "error:AccessDenied"}, sos.RUN_MODE_DRY
        )
        assert f["verdict"] == sos.UNMEASURED
        assert f["verdict"] != sos.DRY


class TestSiblingAttribution:
    """I7392 item D: three artifacts on the 2026-08-21 run were flagged STALE
    at 14min-1h old — written by the postclose pipeline for the SAME cycle,
    minutes before this execution started. Anchored on cycle_date instead of
    execution_start, that stops reading as a silent stage.
    """

    def _one(self, last_modified, cycle_date=RUN_DATE, run_mode=None):
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE)
        head = _head_from({f"b/{RUN_DATE}.json": last_modified})
        return sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, head=head,
            cycle_date=cycle_date, run_mode=run_mode,
        )[0]

    def test_same_cycle_write_minutes_before_start_is_wrote_by_sibling(self):
        f = self._one(EXEC_START - timedelta(minutes=14))
        assert f["verdict"] == sos.WROTE_BY_SIBLING
        assert f["verdict"] not in sos.DEFECT_VERDICTS
        assert "cycle_date" in f["detail"]

    def test_same_cycle_write_one_hour_before_start_is_wrote_by_sibling(self):
        f = self._one(EXEC_START - timedelta(hours=1))
        assert f["verdict"] == sos.WROTE_BY_SIBLING

    def test_a_prior_cycle_write_is_still_stale(self):
        """The regression guard: an object genuinely from an OLDER cycle
        (last_modified's date != cycle_date) must not be laundered into
        wrote_by_sibling just because it predates execution_start."""
        f = self._one(EXEC_START - timedelta(days=87))
        assert f["verdict"] == sos.STALE

    def test_sibling_attribution_beats_the_dry_downgrade(self):
        """A same-cycle sibling write explains the object regardless of THIS
        run's own mode — it must not collapse into `dry` on a shell run."""
        f = self._one(EXEC_START - timedelta(minutes=14), run_mode=sos.RUN_MODE_DRY)
        assert f["verdict"] == sos.WROTE_BY_SIBLING


class TestSelfExclusion:
    """I7392 item C: the sweep's own verdict key is checked before it is
    written, so asserting it against itself is a guaranteed false MISSING on
    every run, forever — on real scheduled runs too, not only rehearsals.
    """

    def test_own_verdict_key_is_excluded_not_evaluated(self):
        row = _row(
            "stage_output_sweep_verdict",
            sos.VERDICT_KEY_TEMPLATE.replace("{pipeline}", PIPELINE).replace(
                "{run_date}", "{run_date}"
            ),
            "WeeklySubstrateHealthCheck",
        )
        decls = sos.declared_outputs([row], pipeline=PIPELINE)
        findings = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, head=_head_from({}),
            cycle_date=RUN_DATE, pipeline=PIPELINE, verdict_bucket=sos.DEFAULT_BUCKET,
        )
        assert findings == []

    def test_a_different_pipelines_verdict_key_is_not_excluded(self):
        """The exclusion is scoped to THIS pipeline's own verdict key —
        another pipeline's verdict artifact is a real declared artifact."""
        other_pipeline = "ne-preopen-trading-pipeline"
        row = _row(
            "other_verdict",
            sos.VERDICT_KEY_TEMPLATE.replace("{pipeline}", other_pipeline).replace(
                "{run_date}", "{run_date}"
            ),
            "SomeStage",
        )
        decls = sos.declared_outputs([row], pipeline=PIPELINE)
        findings = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, head=_head_from({}),
            cycle_date=RUN_DATE, pipeline=PIPELINE, verdict_bucket=sos.DEFAULT_BUCKET,
        )
        assert len(findings) == 1
        assert findings[0]["verdict"] == sos.MISSING

    def test_without_a_pipeline_argument_self_exclusion_is_a_no_op(self):
        """Backward-compatible default: callers that do not pass `pipeline`
        (e.g. call sites predating I7392) see the pre-existing behaviour."""
        row = _row(
            "stage_output_sweep_verdict",
            sos.VERDICT_KEY_TEMPLATE.replace("{pipeline}", PIPELINE).replace(
                "{run_date}", "{run_date}"
            ),
            "WeeklySubstrateHealthCheck",
        )
        decls = sos.declared_outputs([row], pipeline=PIPELINE)
        findings = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, head=_head_from({}),
            cycle_date=RUN_DATE,
        )
        assert len(findings) == 1
        assert findings[0]["verdict"] == sos.MISSING


class TestParseRunMode:
    def test_shell_run_true_is_dry_regardless_of_pipeline_role(self):
        notes: list[str] = []
        mode = sos._parse_run_mode(
            json.dumps({"shell_run": True, "pipeline_role": "shell-run", "skip_parity": True}),
            notes=notes,
        )
        assert mode == sos.RUN_MODE_DRY
        assert notes == []

    def test_pipeline_role_used_when_shell_run_absent(self):
        notes: list[str] = []
        mode = sos._parse_run_mode(json.dumps({"pipeline_role": "weekly"}), notes=notes)
        assert mode == "weekly"
        assert notes == []

    def test_pipeline_role_exercise(self):
        notes: list[str] = []
        mode = sos._parse_run_mode(json.dumps({"pipeline_role": "exercise"}), notes=notes)
        assert mode == "exercise"

    def test_shell_run_false_falls_back_to_pipeline_role(self):
        notes: list[str] = []
        mode = sos._parse_run_mode(
            json.dumps({"shell_run": False, "pipeline_role": "weekly"}), notes=notes,
        )
        assert mode == "weekly"

    def test_missing_input_degrades_to_none_with_a_note(self):
        notes: list[str] = []
        mode = sos._parse_run_mode(None, notes=notes)
        assert mode is None
        assert len(notes) == 1

    def test_unparseable_json_degrades_to_none_with_a_note(self):
        notes: list[str] = []
        mode = sos._parse_run_mode("{not json", notes=notes)
        assert mode is None
        assert len(notes) == 1

    def test_non_object_json_degrades_to_none_with_a_note(self):
        notes: list[str] = []
        mode = sos._parse_run_mode("[1, 2, 3]", notes=notes)
        assert mode is None
        assert len(notes) == 1

    def test_neither_field_present_degrades_to_none_with_a_note(self):
        notes: list[str] = []
        mode = sos._parse_run_mode(json.dumps({"skip_parity": True}), notes=notes)
        assert mode is None
        assert len(notes) == 1


class TestExecutionContextRunMode:
    def test_shell_run_input_sets_dry_run_mode(self):
        class _Sfn(_FakeSfn):
            def describe_execution(self, executionArn):  # noqa: N803
                return {
                    "startDate": EXEC_START,
                    "status": "FAILED",
                    "input": json.dumps({"shell_run": True, "pipeline_role": "shell-run"}),
                }

        ctx = sos.read_execution_context("arn:x", sfn_client=_Sfn())
        assert ctx.run_mode == sos.RUN_MODE_DRY

    def test_weekly_input_sets_weekly_run_mode(self):
        class _Sfn(_FakeSfn):
            def describe_execution(self, executionArn):  # noqa: N803
                return {
                    "startDate": EXEC_START,
                    "status": "SUCCEEDED",
                    "input": json.dumps({"pipeline_role": "weekly"}),
                }

        ctx = sos.read_execution_context("arn:x", sfn_client=_Sfn())
        assert ctx.run_mode == "weekly"


class TestShouldAlert:
    def _doc(self, **kw):
        base = {"enforce": False, "run_mode": None}
        base.update(kw)
        return base

    def test_observe_weekly_alerts(self):
        assert sos._should_alert(self._doc(run_mode="weekly")) is True

    def test_observe_dry_run_suppresses(self):
        assert sos._should_alert(self._doc(run_mode=sos.RUN_MODE_DRY)) is False

    def test_observe_exercise_suppresses(self):
        assert sos._should_alert(self._doc(run_mode="exercise")) is False

    def test_observe_unknown_run_mode_alerts(self):
        """Fail toward alerting on an unresolved run_mode — same posture as
        entered_stages=None and cycle_date=None elsewhere in this module."""
        assert sos._should_alert(self._doc(run_mode=None)) is True

    def test_enforce_mode_always_alerts_even_on_dry(self):
        assert sos._should_alert(self._doc(enforce=True, run_mode=sos.RUN_MODE_DRY)) is True


class TestEmitMetrics:
    class _FakeCloudWatch:
        def __init__(self):
            self.calls = []

        def put_metric_data(self, **kw):
            self.calls.append(kw)

    def test_emits_stage_output_defects_dimensioned_by_pipeline_and_run_mode(self):
        cw = self._FakeCloudWatch()
        document = {
            "pipeline": PIPELINE, "run_mode": sos.RUN_MODE_DRY,
            "defects": [{}, {}],
        }
        sos._emit_metrics(cw, document)
        assert len(cw.calls) == 1
        call = cw.calls[0]
        assert call["Namespace"] == "AlphaEngine/Substrate"
        datum = call["MetricData"][0]
        assert datum["MetricName"] == "StageOutputDefects"
        assert datum["Value"] == 2.0
        dims = {d["Name"]: d["Value"] for d in datum["Dimensions"]}
        assert dims == {"Pipeline": PIPELINE, "RunMode": sos.RUN_MODE_DRY}

    def test_unknown_run_mode_dimensions_as_the_literal_string(self):
        cw = self._FakeCloudWatch()
        sos._emit_metrics(cw, {"pipeline": PIPELINE, "run_mode": None, "defects": []})
        dims = {d["Name"]: d["Value"] for d in cw.calls[0]["MetricData"][0]["Dimensions"]}
        assert dims["RunMode"] == "unknown"

    def test_sweep_metric_emit_failure_does_not_lose_the_verdict(self, registry_file):
        """Matches _publish_verdict's contract: a CW-emit fault must not sink
        the sweep's primary deliverable (I7392 item E, freshness-monitor's
        split-trap convention)."""
        class _BoomCloudWatch:
            def put_metric_data(self, **kw):
                raise RuntimeError("cloudwatch down")

        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START,
            registry_path=registry_file, s3_client=s3, cloudwatch_client=_BoomCloudWatch(),
            alert=False, emit_metrics=True,
        )
        assert doc["status"] == "stage_output_missing"

    def test_emit_metrics_default_off_does_not_touch_boto3(self, registry_file, monkeypatch):
        """sweep()'s default must not construct a real boto3 CloudWatch
        client — every other test in this file relies on that, and a
        default-on metric emit would silently start making live AWS calls."""
        def _boom(*a, **kw):
            raise AssertionError("boto3.client must not be called when emit_metrics=False")

        import boto3

        monkeypatch.setattr(boto3, "client", _boom)
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START,
            registry_path=registry_file, s3_client=s3, alert=False,
        )
        assert doc["status"] == "stage_output_missing"


class TestSweepRunModeIntegration:
    """End-to-end: run_mode threaded from sweep()'s explicit argument through
    to the published document and the alert-suppression decision."""

    def test_dry_run_mode_downgrades_defects_and_suppresses_the_alert(
        self, registry_file, monkeypatch
    ):
        alerted = []
        monkeypatch.setattr(sos, "_alert", lambda doc: alerted.append(doc))
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START,
            registry_path=registry_file, s3_client=s3, run_mode=sos.RUN_MODE_DRY,
        )
        assert doc["counts"].get(sos.DRY) == 1
        assert doc["defects"] == []
        assert doc["status"] == "ok"
        assert alerted == []

    def test_weekly_run_mode_still_alerts_on_a_real_defect(self, registry_file, monkeypatch):
        alerted = []
        monkeypatch.setattr(sos, "_alert", lambda doc: alerted.append(doc))
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START,
            registry_path=registry_file, s3_client=s3, run_mode="weekly",
        )
        assert doc["status"] == "stage_output_missing"
        assert len(alerted) == 1

    def test_exercise_run_mode_publishes_but_does_not_alert(self, registry_file, monkeypatch):
        alerted = []
        monkeypatch.setattr(sos, "_alert", lambda doc: alerted.append(doc))
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START,
            registry_path=registry_file, s3_client=s3, run_mode="exercise",
        )
        assert doc["status"] == "stage_output_missing"
        assert alerted == []


class TestEnteredStages:
    def test_stage_not_entered_is_skipped_when_the_set_is_known(self):
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, cycle_date=RUN_DATE,
            head=_head_from({}), entered_stages=frozenset({"MorningEnrich"}),
        )[0]
        assert f["verdict"] == sos.SKIPPED

    def test_unknown_entered_set_never_excuses_a_stage(self):
        """"I don't know which stages ran" must not become "the stage was
        allowed not to run" — that reasoning turns every degraded run into a
        blanket amnesty and is how a silent stage buys itself cover.
        """
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, cycle_date=RUN_DATE,
            head=_head_from({}), entered_stages=None,
        )[0]
        assert f["verdict"] == sos.MISSING

    def test_entered_stage_is_still_asserted(self):
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, cycle_date=RUN_DATE,
            head=_head_from({}), entered_stages=frozenset({"Backtester"}),
        )[0]
        assert f["verdict"] == sos.MISSING


# ---------------------------------------------------------------------------
# summarise / exit_code — the enforcement ladder
# ---------------------------------------------------------------------------


class TestSummarise:
    def test_defect_outranks_unmeasured_in_the_status(self):
        findings = [
            {"verdict": sos.MISSING}, {"verdict": sos.UNMEASURED}, {"verdict": sos.WROTE},
        ]
        s = sos.summarise(findings)
        assert s["status"] == "stage_output_missing"
        assert len(s["defects"]) == 1 and len(s["unmeasured"]) == 1

    def test_unmeasured_alone_is_not_ok(self):
        """A sweep that could not look has not reported a clean run."""
        s = sos.summarise([{"verdict": sos.UNMEASURED}, {"verdict": sos.WROTE}])
        assert s["status"] == "stage_output_unmeasured"

    def test_all_wrote_is_ok(self):
        assert sos.summarise([{"verdict": sos.WROTE}])["status"] == "ok"

    def test_unresolved_and_skipped_do_not_break_ok(self):
        s = sos.summarise(
            [{"verdict": sos.WROTE}, {"verdict": sos.UNRESOLVED}, {"verdict": sos.SKIPPED}]
        )
        assert s["status"] == "ok"


class TestExitCode:
    def _doc(self, **kw):
        base = {
            "status": "ok", "enforce": False, "defects": [], "unmeasured": [],
        }
        base.update(kw)
        return base

    def test_observe_mode_exits_0_on_a_defect(self):
        """The I6891 blast radius, encoded.

        A non-zero exit here fails WeeklySubstrateHealthCheck → degraded →
        DegradedRun (a Fail state) → the whole four-hour run terminates FAILED.
        Three of I7167's five instances are still open, so an enforcing default
        would hard-fail the next scheduled run for defects that predate the
        detector.
        """
        assert sos.exit_code(self._doc(status="stage_output_missing", defects=[{}])) == 0

    def test_enforce_exits_1_on_a_defect(self):
        assert sos.exit_code(
            self._doc(enforce=True, status="stage_output_missing", defects=[{}])
        ) == 1

    def test_enforce_exits_2_on_unmeasured(self):
        """2, not 1 — the operator's next move differs, and collapsing them is
        how a permissions gap gets filed as a data defect."""
        assert sos.exit_code(
            self._doc(enforce=True, status="stage_output_unmeasured", unmeasured=[{}])
        ) == 2

    def test_unreadable_registry_exits_2_even_in_observe_mode(self):
        """Observe mode excuses FINDINGS, never a sweep that could not run."""
        assert sos.exit_code(self._doc(status="registry_unreadable")) == 2

    def test_clean_run_exits_0_in_both_modes(self):
        assert sos.exit_code(self._doc()) == 0
        assert sos.exit_code(self._doc(enforce=True)) == 0


# ---------------------------------------------------------------------------
# The five 2026-08-08 instances, replayed
# ---------------------------------------------------------------------------


class TestAugust08Replay:
    """Each instance from alpha-engine-config-I7167's root-cause table, replayed
    against the live evidence recorded there. Every one asserts the sweep would
    have produced a non-``wrote`` verdict on a run that terminated SUCCEEDED
    with nothing on any surface.

    The point of replaying all five is that they had FIVE DIFFERENT root causes
    — a shell trap, a caller/contract disagreement, a masked sibling, a
    wall-kill and a prefix cutover — and the assertion is indifferent to all of
    them. That indifference is the deliverable: it is what covers the sixth
    cause, which nobody has found yet.
    """

    def _verdict(self, artifact_id, template, stage, head_map):
        decls = sos.declared_outputs(
            [_row(artifact_id, template, stage)], pipeline=PIPELINE
        )
        return sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from(head_map), cycle_date=RUN_DATE,
        )[0]

    def test_instance_1_substrate_health_check_log_absent(self):
        """SSM exit 127 at line 5 — ``trap: --only-show-errors: invalid signal
        specification``. The stage died before its first check ran, so its log
        object never existed. (Root cause since fixed on main: the stage now
        runs through ``krepis.ssm_log_capture``.)"""
        f = self._verdict(
            "substrate_health_check_log",
            "_ssm_logs/substrate-health-check/{date}/run.log",
            "WeeklySubstrateHealthCheck",
            {},
        )
        assert f["verdict"] == sos.MISSING

    def test_instance_2_executor_optimizer_recommendation_87_days_stale(self):
        """``produce_artifact()`` is documented to write unconditionally; its
        only caller fires it when ``status == "ok"``. The call site wins, so
        the key froze at 2026-05-18 — 87 days before this run, and present the
        whole time."""
        key = f"config/executor_params/recommendations/{RUN_DATE}/from_executor_optimizer.json"
        f = self._verdict(
            "executor_optimizer_recommendation",
            "config/executor_params/recommendations/{date}/from_executor_optimizer.json",
            "Backtester",
            {key: datetime(2026, 5, 18, 12, tzinfo=timezone.utc)},
        )
        assert f["verdict"] == sos.STALE

    def test_instance_3_parity_report_masked_by_a_fresh_sibling(self):
        """``parity_report.json`` last wrote 2026-07-17, but ``report.md`` in
        the SAME prefix updated on this run. A prefix scan sees recent
        activity; a per-key, producer-anchored assertion does not.
        """
        head = {
            f"backtest/{RUN_DATE}/parity_report.json":
                datetime(2026, 7, 17, 12, tzinfo=timezone.utc),
            f"backtest/{RUN_DATE}/report.md": EXEC_START + timedelta(hours=4),
        }
        stale = self._verdict(
            "parity_report", "backtest/{date}/parity_report.json", "ParityReplay", head
        )
        fresh = self._verdict(
            "backtest_report", "backtest/{date}/report.md", "Backtester", head
        )
        assert stale["verdict"] == sos.STALE
        assert fresh["verdict"] == sos.WROTE, (
            "the masking sibling must still read healthy — a check that fails "
            "the whole prefix on one dead key is a check that gets muted"
        )

    def test_instance_4_replay_concordance_wall_killed(self):
        """Killed at its 900s ceiling in 22 of 38 observed runs. Killed at the
        wall means no artifact AND no cause — the stage cannot report its own
        death, which is precisely why the assertion has to be external to it.
        """
        f = self._verdict(
            "replay_summary",
            "decision_artifacts/_replay_summary/{date}/summary.json",
            "ReplayConcordance",
            {},
        )
        assert f["verdict"] == sos.MISSING

    def test_instance_5_aggregate_costs_parquet_absent(self):
        """``_cost_raw/2026-08-08/`` held zero objects (a prefix cutover skew,
        per I7179), so the aggregator honestly no-opped and wrote no parquet.
        The stage was the honest one here — and the assertion still fires,
        because the assertion is about the ARTIFACT, not about whether the
        stage had a good reason.
        """
        f = self._verdict(
            "cost_parquet",
            "decision_artifacts/_cost/{date}/cost.parquet",
            "AggregateCosts",
            {},
        )
        assert f["verdict"] == sos.MISSING

    def test_the_whole_08_08_run_would_have_been_flagged(self):
        """End to end: the five instances plus healthy stages, in one sweep.

        The run terminated SUCCEEDED. In enforce mode this sweep exits 1, and
        in observe mode it exits 0 while still reporting five defects — which
        is the difference between the two modes stated as a test rather than as
        a comment.
        """
        rows = [
            _row("substrate_log", "_ssm_logs/substrate-health-check/{date}/run.log",
                 "WeeklySubstrateHealthCheck"),
            _row("executor_rec",
                 "config/executor_params/recommendations/{date}/from_executor_optimizer.json",
                 "Backtester"),
            _row("parity_report", "backtest/{date}/parity_report.json", "ParityReplay"),
            _row("replay_summary", "decision_artifacts/_replay_summary/{date}/summary.json",
                 "ReplayConcordance"),
            _row("cost_parquet", "decision_artifacts/_cost/{date}/cost.parquet",
                 "AggregateCosts"),
            _row("backtest_report", "backtest/{date}/report.md", "Backtester"),
            _row("predictor_manifest", "predictor/weights/meta/manifest.json",
                 "PredictorTraining"),
        ]
        head = _head_from({
            f"config/executor_params/recommendations/{RUN_DATE}/from_executor_optimizer.json":
                datetime(2026, 5, 18, 12, tzinfo=timezone.utc),
            f"backtest/{RUN_DATE}/parity_report.json":
                datetime(2026, 7, 17, 12, tzinfo=timezone.utc),
            f"backtest/{RUN_DATE}/report.md": EXEC_START + timedelta(hours=4),
            "predictor/weights/meta/manifest.json": EXEC_START + timedelta(hours=1),
        })
        findings = sos.evaluate(
            sos.declared_outputs(rows, pipeline=PIPELINE),
            run_date=RUN_DATE, execution_start=EXEC_START, head=head,
            cycle_date=RUN_DATE,
        )
        summary = sos.summarise(findings)

        assert summary["counts"].get(sos.WROTE) == 2
        assert len(summary["defects"]) == 5
        assert {f["stage"] for f in summary["defects"]} == {
            "WeeklySubstrateHealthCheck", "Backtester", "ParityReplay",
            "ReplayConcordance", "AggregateCosts",
        }
        assert sos.exit_code({**summary, "enforce": True}) == 1
        assert sos.exit_code({**summary, "enforce": False}) == 0


# ---------------------------------------------------------------------------
# sweep() / main() — publish, alert and the end-to-end exit path
# ---------------------------------------------------------------------------


class _FakeS3:
    def __init__(self, objects=None, put_raises=None):
        self.objects = objects or {}
        self.puts = []
        self.put_raises = put_raises

    def head_object(self, Bucket, Key):  # noqa: N803 - boto3 kwarg casing
        if Key not in self.objects:
            err = Exception("not found")
            err.response = {"Error": {"Code": "404"}}
            raise err
        return {"LastModified": self.objects[Key]}

    def put_object(self, **kw):
        if self.put_raises:
            raise self.put_raises
        self.puts.append(kw)
        return {}


@pytest.fixture()
def registry_file(tmp_path):
    import yaml

    path = tmp_path / "ARTIFACT_REGISTRY.yaml"
    path.write_text(yaml.safe_dump({"artifacts": [
        _row("good", "good/{date}.json", "PredictorTraining"),
        _row("bad", "bad/{date}.json", "ReplayConcordance"),
    ]}))
    return str(path)


class TestSweep:
    def test_finds_the_defect_and_publishes_the_verdict(self, registry_file):
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START, registry_path=registry_file,
            s3_client=s3, alert=False,
        )
        assert doc["status"] == "stage_output_missing"
        assert [f["stage"] for f in doc["defects"]] == ["ReplayConcordance"]
        assert len(s3.puts) == 1
        published = json.loads(s3.puts[0]["Body"])
        assert published["schema"] == "stage_output_sweep-1.0.0"
        assert s3.puts[0]["Key"] == f"_stage_outputs/{PIPELINE}/{RUN_DATE}.json"

    def test_verdict_publish_failure_does_not_lose_the_verdict(self, registry_file):
        """The reporter must not destroy what it reports.

        An exception on the verdict WRITE would convert an observability fault
        into a pipeline failure — and, in enforce mode, into a hard-failed
        four-hour run caused by the detector rather than by the pipeline.
        """
        s3 = _FakeS3(
            {f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)},
            put_raises=RuntimeError("s3 down"),
        )
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START, registry_path=registry_file,
            s3_client=s3, alert=False,
        )
        assert doc["status"] == "stage_output_missing"
        assert len(doc["defects"]) == 1

    def test_unreadable_registry_returns_a_document_not_an_exception(self, tmp_path):
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=str(tmp_path / "absent.yaml"),
            s3_client=_FakeS3(), alert=False, publish=False,
        )
        assert doc["status"] == "registry_unreadable"
        assert sos.exit_code(doc) == 2

    def test_no_publish_writes_nothing(self, registry_file):
        s3 = _FakeS3()
        sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            alert=False, publish=False,
        )
        assert s3.puts == []


class TestMain:
    def _run(self, monkeypatch, argv, s3):
        monkeypatch.setattr(sos, "_alert", lambda doc: None)
        import boto3

        monkeypatch.setattr(boto3, "client", lambda *a, **k: s3)
        return sos.main(argv)

    def test_observe_mode_exits_0_with_a_real_defect(self, monkeypatch, registry_file):
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--no-alert",
        ], s3)
        assert code == 0

    def test_enforce_mode_exits_1_with_the_same_defect(self, monkeypatch, registry_file):
        """The proof the assertion CAN fail, walked end to end from a real
        absent key to a non-zero process exit."""
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--enforce", "--no-alert",
        ], s3)
        assert code == 1

    def test_enforce_mode_exits_0_on_a_clean_run(self, monkeypatch, registry_file):
        """The other polarity — an absence-alarm is blind in BOTH directions,
        and a detector that fires on a healthy run is the one that gets muted.
        """
        s3 = _FakeS3({
            f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1),
            f"bad/{RUN_DATE}.json": EXEC_START + timedelta(hours=2),
        })
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--enforce", "--no-alert",
        ], s3)
        assert code == 0

    def test_malformed_execution_start_exits_2_rather_than_dropping_the_window(
        self, monkeypatch, registry_file
    ):
        """A mis-typed timestamp must not silently degrade every assertion to
        ``unmeasured`` — that is a detector switched off by a typo.
        """
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "08/08/2026", "--no-alert",
        ], _FakeS3())
        assert code == 2

    def test_entered_stage_flag_excuses_only_the_absent_stage(
        self, monkeypatch, registry_file
    ):
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--enforce", "--no-alert",
            "--entered-stage", "PredictorTraining",
        ], s3)
        assert code == 0, "ReplayConcordance never entered, so its artifact is not owed"


class _FakeSfn:
    """Step Functions double. ``describe_raises`` / ``history_raises`` exist so
    each half of the execution context can be failed INDEPENDENTLY — the two
    degrade separately by design and a shared failure switch would hide that.
    """

    def __init__(self, start=None, entered=(), describe_raises=None,
                 history_raises=None, pages=None):
        self.start = start
        self.entered = list(entered)
        self.describe_raises = describe_raises
        self.history_raises = history_raises
        self.pages = pages

    def describe_execution(self, executionArn):  # noqa: N803
        if self.describe_raises:
            raise self.describe_raises
        return {"startDate": self.start, "status": "SUCCEEDED"}

    def get_execution_history(self, **kw):
        if self.history_raises:
            raise self.history_raises
        if self.pages is not None:
            index = 0 if "nextToken" not in kw else int(kw["nextToken"])
            return self.pages[index]
        return {
            "events": [
                {"type": "TaskStateEntered",
                 "stateEnteredEventDetails": {"name": n}}
                for n in self.entered
            ]
        }


class TestExecutionContext:
    def test_reads_start_and_entered_stages(self):
        ctx = sos.read_execution_context(
            "arn:x", sfn_client=_FakeSfn(start=EXEC_START, entered=["A", "B"]),
        )
        assert ctx.start == EXEC_START
        assert ctx.entered_stages == frozenset({"A", "B"})
        # _FakeSfn's describe_execution carries no 'input' field, so run_mode
        # degrades to UNKNOWN with one context note — never to silence.
        assert ctx.run_mode is None
        assert len(ctx.notes) == 1
        assert "run_mode is UNKNOWN" in ctx.notes[0]

    def test_paginates_the_history(self):
        """A four-hour weekly run's history runs to thousands of events; a
        single un-paginated page would silently truncate the entered set and
        mark late stages skipped — false GREEN, the dangerous polarity."""
        pages = [
            {"events": [{"type": "TaskStateEntered",
                         "stateEnteredEventDetails": {"name": "Early"}}],
             "nextToken": "1"},
            {"events": [{"type": "TaskStateEntered",
                         "stateEnteredEventDetails": {"name": "Late"}}]},
        ]
        ctx = sos.read_execution_context("arn:x", sfn_client=_FakeSfn(pages=pages))
        assert ctx.entered_stages == frozenset({"Early", "Late"})

    def test_only_entered_events_count(self):
        """Filtering on EXIT events would excuse every stage that entered and
        died — precisely the population this sweep exists to catch."""
        class _Sfn(_FakeSfn):
            def get_execution_history(self, **kw):
                return {"events": [
                    {"type": "TaskStateEntered",
                     "stateEnteredEventDetails": {"name": "Ran"}},
                    {"type": "TaskStateExited",
                     "stateExitedEventDetails": {"name": "Exited"}},
                ]}

        ctx = sos.read_execution_context("arn:x", sfn_client=_Sfn())
        assert ctx.entered_stages == frozenset({"Ran"})

    def test_describe_denied_still_yields_entered_stages(self):
        ctx = sos.read_execution_context(
            "arn:x",
            sfn_client=_FakeSfn(entered=["A"], describe_raises=RuntimeError("AccessDenied")),
        )
        assert ctx.start is None
        assert ctx.entered_stages == frozenset({"A"})
        assert any("describe_execution failed" in n for n in ctx.notes)

    def test_history_denied_still_yields_the_start(self):
        ctx = sos.read_execution_context(
            "arn:x",
            sfn_client=_FakeSfn(start=EXEC_START, history_raises=RuntimeError("AccessDenied")),
        )
        assert ctx.start == EXEC_START
        assert ctx.entered_stages is None
        assert any("UNKNOWN" in n for n in ctx.notes)

    def test_both_denied_degrades_to_nothing_known_and_says_so(self):
        ctx = sos.read_execution_context(
            "arn:x",
            sfn_client=_FakeSfn(describe_raises=RuntimeError("x"),
                                history_raises=RuntimeError("y")),
        )
        assert ctx.start is None and ctx.entered_stages is None
        assert len(ctx.notes) == 2

    def test_no_injected_client_resolves_a_region_with_no_env_set(self, monkeypatch):
        """Regression for alpha-engine-config-I7428: SSM AWS-RunShellScript
        runs with neither AWS_REGION nor AWS_DEFAULT_REGION set and no
        per-user boto profile. Before the fix, ``boto3.client("stepfunctions")``
        with no ``sfn_client`` injected raised NoRegionError on every box
        run and the execution context degraded with
        "stepfunctions client unavailable: You must specify a region." on
        every weekly run. Clearing both env vars and forcing krepis's
        session/IMDS fallbacks to miss reproduces the exact box shape; the
        client must still be constructed (via
        krepis.aws_region.resolve_region()'s DEFAULT_REGION floor) rather
        than raising NoRegionError."""
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        from krepis import aws_region

        monkeypatch.setattr(aws_region, "_from_botocore_session", lambda: None)
        monkeypatch.setattr(aws_region, "_from_imds", lambda: None)

        ctx = sos.read_execution_context("arn:x")

        assert not any(
            "NoRegionError" in n or "You must specify a region" in n
            for n in ctx.notes
        )


class TestGatedOutRun:
    """The 2026-08-13 scheduled run terminated SUCCEEDED in 5.4 seconds,
    gated out by ``WeeklyRunDayGate`` having done no work. Roughly two of
    every three firings are such runs.

    Without the entered-stage set this sweep would report every declared
    artifact missing on those — ~30 false defects, twice a week, which is how
    a detector earns its mute. With it, the run is correctly silent.
    """

    def test_gated_out_run_reports_no_defects(self, registry_file):
        s3 = _FakeS3()
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_arn="arn:x",
            sfn_client=_FakeSfn(start=EXEC_START, entered=["WeeklyRunDayGate"]),
            alert=False, publish=False, enforce=True,
        )
        assert doc["defects"] == []
        assert doc["counts"].get(sos.SKIPPED) == 2
        assert sos.exit_code(doc) == 0

    def test_a_deep_run_with_a_silent_stage_still_fails(self, registry_file):
        """The other polarity — the skip excuse must not become a blanket
        amnesty. A stage that DID enter and wrote nothing is still a defect."""
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_arn="arn:x",
            sfn_client=_FakeSfn(
                start=EXEC_START, entered=["PredictorTraining", "ReplayConcordance"]
            ),
            alert=False, publish=False, enforce=True,
        )
        assert [f["stage"] for f in doc["defects"]] == ["ReplayConcordance"]
        assert sos.exit_code(doc) == 1

    def test_history_denied_does_not_excuse_anything(self, registry_file):
        """Losing the history must fail toward asserting, not toward amnesty."""
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_arn="arn:x",
            sfn_client=_FakeSfn(start=EXEC_START, history_raises=RuntimeError("AccessDenied")),
            alert=False, publish=False, enforce=True,
        )
        assert doc["entered_stages_known"] is False
        assert [f["stage"] for f in doc["defects"]] == ["ReplayConcordance"]
        assert doc["context_notes"]

    def test_explicit_arguments_win_over_the_api(self, registry_file):
        """Replaying a historical run whose ARN no longer resolves must still
        honour an explicitly-supplied window."""
        s3 = _FakeS3()
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_start=EXEC_START, entered_stages=frozenset({"PredictorTraining"}),
            execution_arn="arn:x",
            sfn_client=_FakeSfn(start=datetime(2020, 1, 1, tzinfo=timezone.utc),
                                entered=["Something", "Else"]),
            alert=False, publish=False,
        )
        assert doc["execution_start"] == EXEC_START.isoformat()
        assert doc["entered_stage_count"] == 1


class TestAlertBody:
    def test_alert_names_the_mode_and_separates_the_two_finding_classes(
        self, monkeypatch
    ):
        published = {}

        class _Result:
            any_ok = True

        class _Alerts:
            @staticmethod
            def publish(message, **kw):
                published["message"] = message
                published["kw"] = kw
                return _Result()

        import sys as _sys

        monkeypatch.setitem(_sys.modules, "nousergon_lib", type("M", (), {"alerts": _Alerts}))
        monkeypatch.setitem(_sys.modules, "nousergon_lib.alerts", _Alerts)

        sos._alert({
            "pipeline": PIPELINE, "run_date": RUN_DATE, "enforce": False,
            "entered_stages_known": False,
            "defects": [{"stage": "ParityReplay", "artifact_id": "parity_report",
                         "verdict": sos.STALE, "severity": "critical"}],
            "unmeasured": [{"stage": "Backtester", "artifact_id": "x",
                            "verdict": sos.UNMEASURED}],
        })
        message = published["message"]
        assert "OBSERVE" in message
        assert "DEFECT" in message and "UNMEASURED" in message
        assert "NOT evidence of health" in message
        assert "1 artifact(s) across 1 stage(s)" in message
        # severity comes from the MAX finding severity ("critical" here), not
        # from the defect COUNT — see the warning-only test below for the
        # other polarity (I7392 item B).
        assert published["kw"]["severity"] == "error"

    def test_many_warning_defects_alert_at_warn_not_error(self, monkeypatch):
        """The regression this replaces: 82 warning-severity findings alone
        used to force severity='error' purely from the defect COUNT. Now the
        MAX finding severity decides — all-warning stays 'warn' regardless
        of how many there are.
        """
        published = {}

        class _Result:
            any_ok = True

        class _Alerts:
            @staticmethod
            def publish(message, **kw):
                published["kw"] = kw
                published["message"] = message
                return _Result()

        import sys as _sys

        monkeypatch.setitem(_sys.modules, "nousergon_lib", type("M", (), {"alerts": _Alerts}))
        monkeypatch.setitem(_sys.modules, "nousergon_lib.alerts", _Alerts)

        sos._alert({
            "pipeline": PIPELINE, "run_date": RUN_DATE, "enforce": False,
            "entered_stages_known": True,
            "defects": [
                {"stage": f"Stage{i}", "artifact_id": f"a{i}",
                 "verdict": sos.MISSING, "severity": "warning"}
                for i in range(82)
            ],
            "unmeasured": [],
        })
        assert published["kw"]["severity"] == "warn"
        assert "82 artifact(s) across 82 stage(s)" in published["message"]

    def test_unmeasured_only_alerts_at_warn_not_error(self, monkeypatch):
        published = {}

        class _Result:
            any_ok = True

        class _Alerts:
            @staticmethod
            def publish(message, **kw):
                published["kw"] = kw
                return _Result()

        import sys as _sys

        monkeypatch.setitem(_sys.modules, "nousergon_lib", type("M", (), {"alerts": _Alerts}))
        monkeypatch.setitem(_sys.modules, "nousergon_lib.alerts", _Alerts)

        sos._alert({
            "pipeline": PIPELINE, "run_date": RUN_DATE, "enforce": True,
            "entered_stages_known": True, "defects": [],
            "unmeasured": [{"stage": "S", "artifact_id": "a", "verdict": sos.UNMEASURED}],
        })
        assert published["kw"]["severity"] == "warn"


# ---------------------------------------------------------------------------
# gated-forever rows (alpha-engine-config-I9617)
# ---------------------------------------------------------------------------

#: The 2026-08-29 scheduled weekly run, execution
#: ``965d925b-f9d6-ce5e-e059-9405433a0724_c0271e3b-a27f-a834-b303-3e3803fffd16``:
#: ran ~5h and ended at the ``DegradedRun`` terminal. Its
#: ``WeeklySubstrateHealthCheck`` stage logged
#: ``STAGE OUTPUT MISSING: 6 declared artifact(s) absent or stale for
#: run_date=2026-08-28``, and every one of the six was a registry row carrying
#: ``grace_period_cycles: 999`` — a study whose producer auto-skips when its
#: own precondition is unmet.
AUG29_RUN_DATE = "2026-08-28"
AUG29_EXEC_START = datetime(2026, 8, 29, 9, 0, 0, tzinfo=timezone.utc)

#: (artifact_id, s3_key_template, producing stage) — transcribed from
#: ARTIFACT_REGISTRY.yaml as of 2026-08-31. All eight rows in the live
#: registry that carry ``grace_period_cycles: 999``; the six the 2026-08-29
#: run actually reported are the first five plus ``regime_state_dated``.
AUG29_GATED_ROWS = [
    ("backtest_gamma_sweep", "backtest/{trading_day}/gamma_sweep.json", "Backtester"),
    ("backtest_horizon_net_alpha",
     "backtest/{trading_day}/horizon_net_alpha.json", "Backtester"),
    ("backtest_model_version_net_alpha",
     "backtest/{trading_day}/model_version_net_alpha.json", "Backtester"),
    ("backtest_double_sort", "backtest/{trading_day}/double_sort.json", "Backtester"),
    ("backtest_sizing_shootout",
     "backtest/{trading_day}/sizing_shootout.json", "Backtester"),
    ("regime_state_dated", "regime/{trading_day}.json", "RegimeSubstrate"),
]


class TestAbsenceDeclaredCorrect:
    def test_grace_999_is_the_legacy_gated_forever_marker(self):
        reason = sos.absence_declared_correct(
            _row("a", "k/{date}.json", "Backtester", grace_period_cycles=999)
        )
        assert reason and "grace_period_cycles: 999" in reason

    def test_event_driven_names_its_liveness_anchor(self):
        reason = sos.absence_declared_correct(
            _row("a", "k/{date}.json", "Backtester",
                 cadence="event_driven", liveness_via="config_apply_audit")
        )
        assert reason and "event_driven" in reason
        assert "config_apply_audit" in reason

    def test_an_ordinary_grace_period_is_not_a_gate(self):
        """The predicate keys on the gated-forever marker, not on grace at
        all. A newly-onboarded row with ``grace_period_cycles: 2`` is still
        fully assertable — otherwise the fix would silently mute a third of
        the registry."""
        assert sos.absence_declared_correct(
            _row("a", "k/{date}.json", "Backtester", grace_period_cycles=2)
        ) is None
        assert sos.absence_declared_correct(
            _row("a", "k/{date}.json", "Backtester")
        ) is None

    def test_a_non_numeric_grace_is_not_treated_as_a_gate(self):
        """Fail toward asserting: an unparseable declaration is not a licence
        to stop checking."""
        assert sos.absence_declared_correct(
            _row("a", "k/{date}.json", "Backtester", grace_period_cycles="lots")
        ) is None

    def test_declared_outputs_carries_the_reason(self):
        decl = sos.declared_outputs(
            [_row("a", "k/{date}.json", "Backtester", grace_period_cycles=999)],
            pipeline=PIPELINE,
        )[0]
        assert decl["absence_declared_correct"]


class TestAugust29GatedReplay:
    """Replays the 2026-08-29 finding set from the recorded registry rows plus
    the recorded S3 state (none of the six keys existed). Before the fix all
    six were ``missing`` and the sweep was one ``--enforce`` flip from
    terminating a four-hour run on them."""

    def _findings(self, *, gated: bool):
        rows = [
            _row(aid, tmpl, stage,
                 **({"grace_period_cycles": 999} if gated else {}))
            for aid, tmpl, stage in AUG29_GATED_ROWS
        ]
        return sos.evaluate(
            sos.declared_outputs(rows, pipeline=PIPELINE),
            run_date=AUG29_RUN_DATE,
            execution_start=AUG29_EXEC_START,
            head=_head_from({}),
            cycle_date=AUG29_RUN_DATE,
        )

    def test_all_six_are_gated_not_missing(self):
        findings = self._findings(gated=True)
        assert len(findings) == 6
        assert {f["verdict"] for f in findings} == {sos.GATED}
        summary = sos.summarise(findings)
        assert summary["defects"] == []
        assert summary["status"] == "ok"
        assert len(summary["gated"]) == 6
        assert summary["counts"][sos.GATED] == 6

    def test_the_same_rows_without_the_gate_are_the_recorded_failure(self):
        """The pre-fix behaviour, asserted so the guard cannot pass by
        accident: strip the registry's gated-forever marker and the identical
        S3 state reproduces exactly the six MISSING findings the run logged."""
        findings = self._findings(gated=False)
        assert [f["verdict"] for f in findings] == [sos.MISSING] * 6
        assert sos.summarise(findings)["status"] == "stage_output_missing"

    def test_the_reason_is_on_the_finding_not_swallowed(self):
        """A row excused with no reason on the surface is indistinguishable
        from a row nobody checked."""
        f = self._findings(gated=True)[0]
        assert "NOT a defect" in f["detail"]
        assert "missing suppressed" in f["detail"]
        assert "grace_period_cycles: 999" in f["detail"]


class TestGatedRowBothPolarities:
    def test_a_gated_row_that_did_write_still_reports_wrote(self):
        """The downgrade only ever applies to a would-be defect. A gated study
        whose gate DID clear this week is a real, attributable write."""
        key = f"backtest/{RUN_DATE}/gamma_sweep.json"
        decls = sos.declared_outputs(
            [_row("backtest_gamma_sweep", "backtest/{date}/gamma_sweep.json",
                  "Backtester", grace_period_cycles=999)],
            pipeline=PIPELINE,
        )
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({key: EXEC_START + timedelta(hours=2)}),
            cycle_date=RUN_DATE,
        )[0]
        assert f["verdict"] == sos.WROTE

    def test_a_stale_gated_row_is_gated_not_stale(self):
        """Last cycle's product left behind while this cycle's gate did not
        clear is the same statement about the gate as an absent key."""
        key = f"backtest/{RUN_DATE}/gamma_sweep.json"
        decls = sos.declared_outputs(
            [_row("backtest_gamma_sweep", "backtest/{date}/gamma_sweep.json",
                  "Backtester", grace_period_cycles=999)],
            pipeline=PIPELINE,
        )
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({key: EXEC_START - timedelta(days=21)}),
            cycle_date=RUN_DATE,
        )[0]
        assert f["verdict"] == sos.GATED
        assert "stale suppressed" in f["detail"]

    def test_a_mandatory_artifact_absent_still_fails_the_run(self):
        """The polarity that matters: the excuse is per-row and derived from
        the registry, never a blanket amnesty. A non-gated declared artifact
        that nobody wrote is still a defect that exits non-zero under
        --enforce."""
        decls = sos.declared_outputs(
            [_row("backtest_report", "backtest/{date}/report.md", "Backtester"),
             _row("backtest_gamma_sweep", "backtest/{date}/gamma_sweep.json",
                  "Backtester", grace_period_cycles=999)],
            pipeline=PIPELINE,
        )
        findings = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({}), cycle_date=RUN_DATE,
        )
        summary = sos.summarise(findings)
        assert [f["artifact_id"] for f in summary["defects"]] == ["backtest_report"]
        assert [f["artifact_id"] for f in summary["gated"]] == ["backtest_gamma_sweep"]
        assert summary["status"] == "stage_output_missing"

    def test_unmeasured_is_not_excused_by_the_gate(self):
        """A check that could not answer is not excused by a gate it never
        reached — otherwise a permissions regression on a gated row would
        render as a clean pass."""
        key = f"backtest/{RUN_DATE}/gamma_sweep.json"
        decls = sos.declared_outputs(
            [_row("backtest_gamma_sweep", "backtest/{date}/gamma_sweep.json",
                  "Backtester", grace_period_cycles=999)],
            pipeline=PIPELINE,
        )
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({key: "error:403 AccessDenied"}), cycle_date=RUN_DATE,
        )[0]
        assert f["verdict"] == sos.UNMEASURED


# ---------------------------------------------------------------------------
# The producer-chosen ``*`` segment (alpha-engine-config-I10200)
# ---------------------------------------------------------------------------
#
# `crucible-predictor` rescoped `predictor/diagnostics/oos_rows/…` by the model
# FAMILY under alpha-engine-config-I9378 so a parallel zoo spec could not
# overwrite the champion's diagnostic. The registry's flat row then made this
# sweep report `PredictorTraining` MISSING for the 2026-09-04 cycle while
# `oos_rows/v3.0-meta/2026-09-04.parquet` had existed since 2026-09-05 — one of
# the three findings holding `alpha-engine-stage-coverage-findings` in ALARM.
#
# A resolved key carrying `*` is a PATTERN and cannot be head-ed, so it is
# resolved by listing. What these tests pin hardest is what the resolution must
# NOT do: clear the row off the unscoped key (the shape I9378 removed), and
# report a listing it could not complete as an absent artifact.

_OOS_DATED = "predictor/diagnostics/oos_rows/*/{date}.parquet"
_OOS_LATEST = "predictor/diagnostics/oos_rows/*/latest.parquet"


def _find_from(mapping):
    """Build a ``find`` callable from ``{key: last_modified | 'error:<why>'}``.

    Matches the pattern the same way :func:`stage_output_sweep._newest_match`
    does — compiled shape, newest survivor — without an S3 client, so the
    verdict branches stay reachable. A mapping VALUE beginning ``error:``
    makes the whole listing fail, which is the branch that must not render as
    ``absent``.
    """

    def find(bucket, pattern):
        compiled = sos._wildcard_support()["key_pattern"](pattern)
        newest_key, newest_lm = "", None
        for key, value in mapping.items():
            if isinstance(value, str) and value.startswith("error:"):
                return "error", value[len("error:"):], ""
            if compiled.match(key) is None:
                continue
            if newest_lm is None or value > newest_lm:
                newest_lm, newest_key = value, key
        if newest_lm is None:
            return "absent", None, ""
        return "found", newest_lm, newest_key

    return find


class TestWildcardSegment:
    def _one(self, template, objects, *, find=True, **kw):
        decls = sos.declared_outputs(
            [_row("a", template, "PredictorTraining")], pipeline=PIPELINE,
        )
        kw.setdefault("execution_start", EXEC_START)
        kw.setdefault("cycle_date", RUN_DATE)
        return sos.evaluate(
            decls,
            run_date=RUN_DATE,
            head=_head_from({}),
            find=_find_from(objects) if find else None,
            **kw,
        )[0]

    def test_resolve_key_passes_the_wildcard_through(self):
        assert sos.resolve_key(
            _OOS_DATED, run_date=RUN_DATE, cycle_date=RUN_DATE,
        ) == f"predictor/diagnostics/oos_rows/*/{RUN_DATE}.parquet"

    def test_resolve_key_rejects_an_illegal_wildcard_shape(self):
        """A malformed row is `unmeasured` with a reason naming the template —
        not a pattern that silently matches the wrong set."""
        for bad in (
            "predictor/diagnostics/oos_rows/*",
            "predictor/diagnostics/oos_rows/**/x.parquet",
            "a/*/b/*/{date}.parquet",
            "a/v*/{date}.parquet",
        ):
            assert sos.resolve_key(
                bad, run_date=RUN_DATE, cycle_date=RUN_DATE,
            ) is None, bad

    def test_the_2026_09_04_shaped_case_reports_wrote(self):
        """The Closes-when of I10200 on this side of the cascade."""
        key = f"predictor/diagnostics/oos_rows/v3.0-meta/{RUN_DATE}.parquet"
        f = self._one(_OOS_DATED, {key: EXEC_START + timedelta(hours=2)})
        assert f["verdict"] == sos.WROTE
        # The pattern says where it was expected; `key` says which instance
        # the verdict was taken over, so the verdict is verifiable by hand.
        assert f["pattern"] == f"predictor/diagnostics/oos_rows/*/{RUN_DATE}.parquet"
        assert f["key"] == key

    def test_latest_pointer_row_resolves_too(self):
        key = "predictor/diagnostics/oos_rows/v3.0-meta/latest.parquet"
        f = self._one(_OOS_LATEST, {key: EXEC_START + timedelta(hours=2)})
        assert f["verdict"] == sos.WROTE
        assert f["key"] == key

    def test_the_unscoped_key_does_not_clear_the_scoped_row(self):
        """The flat key is the shape I9378 REMOVED, to stop a zoo run
        overwriting the champion's diagnostic. Finding one must not be read as
        the stage having written the scoped artifact."""
        flat = f"predictor/diagnostics/oos_rows/{RUN_DATE}.parquet"
        f = self._one(_OOS_DATED, {flat: EXEC_START + timedelta(hours=2)})
        assert f["verdict"] == sos.MISSING
        assert f["key"] is None

    def test_a_sibling_diagnostic_does_not_clear_the_row(self):
        sibling = f"predictor/diagnostics/xsec_sd/v3.0-meta/{RUN_DATE}.parquet"
        f = self._one(_OOS_DATED, {sibling: EXEC_START + timedelta(hours=2)})
        assert f["verdict"] == sos.MISSING

    def test_another_cycles_instance_does_not_clear_this_run(self):
        """The pattern is anchored to THIS run's cycle date — only the
        producer's segment is free. A recency match would let last week's
        instance clear this week's silent stage."""
        other = "predictor/diagnostics/oos_rows/v3.0-meta/2026-07-04.parquet"
        f = self._one(_OOS_DATED, {other: EXEC_START + timedelta(hours=2)})
        assert f["verdict"] == sos.MISSING

    def test_newest_matching_family_wins(self):
        older = f"predictor/diagnostics/oos_rows/v3.0-meta/{RUN_DATE}.parquet"
        newer = f"predictor/diagnostics/oos_rows/v4.0-zoo/{RUN_DATE}.parquet"
        f = self._one(_OOS_DATED, {
            older: EXEC_START + timedelta(hours=1),
            newer: EXEC_START + timedelta(hours=3),
        })
        assert f["verdict"] == sos.WROTE
        assert f["key"] == newer

    def test_stale_instance_is_still_stale(self):
        key = f"predictor/diagnostics/oos_rows/v3.0-meta/{RUN_DATE}.parquet"
        f = self._one(_OOS_DATED, {key: EXEC_START - timedelta(days=30)})
        assert f["verdict"] == sos.STALE

    def test_listing_failure_is_unmeasured_not_missing(self):
        key = f"predictor/diagnostics/oos_rows/v3.0-meta/{RUN_DATE}.parquet"
        f = self._one(_OOS_DATED, {key: "error:AccessDenied"})
        assert f["verdict"] == sos.UNMEASURED
        assert "AccessDenied" in f["detail"]

    def test_no_lister_supplied_is_unmeasured_not_missing(self):
        """A caller that cannot list cannot answer these rows. Saying so is
        the only honest option — head-ing a literal `*` would 404 and report a
        confident, permanent, entirely false `missing`."""
        f = self._one(_OOS_DATED, {}, find=False)
        assert f["verdict"] == sos.UNMEASURED
        assert "*" in f["detail"]

    def test_fixed_key_rows_never_reach_the_lister(self):
        """The wildcard path must not move a single existing row's verdict."""
        decls = sos.declared_outputs(
            [_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE,
        )

        def _explode(bucket, pattern):  # pragma: no cover - must not be called
            raise AssertionError("a fixed-key row must be head-ed, not listed")

        f = sos.evaluate(
            decls,
            run_date=RUN_DATE,
            head=_head_from({f"b/{RUN_DATE}.json": EXEC_START}),
            find=_explode,
            execution_start=EXEC_START,
            cycle_date=RUN_DATE,
        )[0]
        assert f["verdict"] == sos.WROTE
        assert f["pattern"] is None


class TestNewestMatchAgainstS3:
    """`_newest_match` itself, against a stubbed paginator."""

    def _s3(self, objects, *, pages=None, raises=None):
        class _Paginator:
            def paginate(self, **kwargs):
                if raises is not None:
                    raise raises
                contents = [
                    {"Key": k, "LastModified": v}
                    for k, v in objects.items()
                    if k.startswith(kwargs["Prefix"])
                ]
                return iter(pages if pages is not None else [{"Contents": contents}])

        class _S3:
            def get_paginator(self, name):
                assert name == "list_objects_v2"
                return _Paginator()

        return _S3()

    def test_lists_the_prefix_before_the_star(self):
        key = f"predictor/diagnostics/oos_rows/v3.0-meta/{RUN_DATE}.parquet"
        state, lm, found = sos._newest_match(
            self._s3({key: EXEC_START}),
            "alpha-engine-research",
            f"predictor/diagnostics/oos_rows/*/{RUN_DATE}.parquet",
        )
        assert (state, lm, found) == ("found", EXEC_START, key)

    def test_empty_prefix_is_absent(self):
        state, lm, found = sos._newest_match(
            self._s3({}),
            "alpha-engine-research",
            f"predictor/diagnostics/oos_rows/*/{RUN_DATE}.parquet",
        )
        assert (state, lm, found) == ("absent", None, "")

    def test_list_error_is_an_error_not_absent(self):
        state, detail, found = sos._newest_match(
            self._s3({}, raises=RuntimeError("boom")),
            "alpha-engine-research",
            f"predictor/diagnostics/oos_rows/*/{RUN_DATE}.parquet",
        )
        assert state == "error"
        assert "boom" in detail

    def test_truncated_scan_is_an_error_not_absent(self):
        """S3 lists lexically, so the keys past the cap are not a random
        sample — and 'I could not see the whole prefix' is not evidence the
        artifact is missing (alpha-engine-config-I7617)."""
        state, detail, found = sos._newest_match(
            self._s3({}, pages=[{"Contents": []} for _ in range(5)]),
            "alpha-engine-research",
            f"predictor/diagnostics/oos_rows/*/{RUN_DATE}.parquet",
            cap_pages=2,
        )
        assert state == "error"
        assert "scan cap" in detail
