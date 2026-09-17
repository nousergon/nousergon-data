"""Unit tests for scripts/weekly_sf_rerun.py (config#2277) + lockstep guards
pinning its declarative stage table against infrastructure/step_function.json.

Three recorded-shape execution-history fixtures (tests/fixtures/
weekly_sf_rerun/, synthesized from the REAL 2026-07-11 scheduled-run failure
history's event vocabulary):

- ``parallel_branch_failure``: branch A dies at RAGIngestion, branch B
  completes through the model zoo (the actual 2026-07-11 shape);
- ``tail_stage_failure``: rebuilt for the alpha-engine-config#6030 parity
  split — the PitParityWalkforward branch DEGRADES fail-open (its siblings
  and the compare complete; the compare emits verdict UNKNOWN), and the run
  then fails terminally at Evaluator. Exercises the skip_backtester
  OVERSHOOT drop (its skip route jumps a degraded parity-family stage's
  gate) and per-branch skip emission;
- ``early_failure``: DataPhase1 fails with only MorningEnrich completed;
- ``director_degraded``: the REAL watch-rerun-2026-08-01-4 history (the
  "permanent fixture" for alpha-engine-config-I6055 — see its test) — the
  Director hard-failed (ModuleNotFoundError: openai, event 858),
  PublishDirectorDegraded absorbed it (PRE-FIX behavior — config#6408 now
  routes Director failure to terminal FailExecution), the tail's health
  checks degraded too, and the run only failed terminally at
  WriteCompletionMarker (S3.AccessDeniedException). PublishDirectorDegraded
  is RETAINED in the degraded_witness set for backward compatibility with
  pre-fix execution histories.

Plus the config#2280 mutex-steal decision matrix and the role-gating
verification (config#2277 deliverable 2).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "weekly_sf_rerun.py"
FIXTURES = Path(__file__).parent / "fixtures" / "weekly_sf_rerun"
SF_PATH = REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location("weekly_sf_rerun", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    # register BEFORE exec: dataclass field resolution under
    # `from __future__ import annotations` looks the module up in sys.modules
    sys.modules["weekly_sf_rerun"] = m
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def sf_def() -> dict:
    return json.loads(SF_PATH.read_text())


def _events(name: str) -> list:
    return json.loads((FIXTURES / f"{name}.json").read_text())["events"]


# ---------------------------------------------------------------------------
# Skip-set derivation over the three fixtures
# ---------------------------------------------------------------------------

class TestDerivePlan:
    def test_parallel_branch_failure(self, mod):
        plan = mod.derive_plan(_events("parallel_branch_failure"))
        assert plan.run_date == "2026-07-11"
        assert "InitializeInput" in plan.run_date_provenance
        assert set(plan.skip_flags) == {
            "skip_morning_enrich",
            "skip_data_phase1",
            "skip_scanner",
            "skip_regime_substrate",
            "skip_signals_envelope",
            # alpha-engine-config-I7726 — new stage between
            # SignalsEnvelope and ChallengerShadow.
            "skip_research_self_test",
            "skip_challenger_shadow",
            "skip_predictor_training",
        }
        assert plan.failed == ["rag_ingestion"]
        # lib-pin check completed but is deliberately NOT skipped
        assert "lib_pin_drift_check" in plan.completed
        assert "skip_lib_pin_drift_check" not in plan.skip_flags

    def test_tail_stage_failure_drops_backtester_overshoot(self, mod):
        plan = mod.derive_plan(_events("tail_stage_failure"))
        # alpha-engine-config#6030 shape: the walkforward BRANCH degraded
        # fail-open (the run continued), and the run failed terminally at
        # Evaluator.
        assert plan.failed == ["evaluator"]
        assert "pit_parity_walkforward" in plan.degraded
        assert "parity" in plan.degraded  # the post-join family fold
        # completed branches + the compare emit their fine-grained flags —
        # ONLY the degraded branch reruns (the #6030 closes-when)
        assert plan.skip_flags.get("skip_pit_parity_lookahead") is True
        assert plan.skip_flags.get("skip_parity_replay") is True
        assert plan.skip_flags.get("skip_pit_parity_compare") is True
        assert "skip_pit_parity_walkforward" not in plan.skip_flags
        # the family flag is never auto-emitted (it would bypass the
        # degraded branch)
        assert "skip_parity" not in plan.skip_flags
        # skip_backtester completed but its whole-pair skip route would
        # bypass the degraded parity-family gate — replaced with
        # skip_backtester_stage_only (config#2362 Option A) so Backtester's
        # SSM task isn't re-run while the tail gates still compose.
        assert "skip_backtester" not in plan.skip_flags
        assert plan.skip_flags.get("skip_backtester_stage_only") is True
        assert any("skip_backtester_stage_only" in n for n in plan.notes)
        assert set(plan.skip_flags) == {
            "skip_morning_enrich",
            "skip_data_phase1",
            "skip_scanner",
            "skip_regime_substrate",
            "skip_signals_envelope",
            # alpha-engine-config-I7726 — new stage between
            # SignalsEnvelope and ChallengerShadow.
            "skip_research_self_test",
            "skip_challenger_shadow",
            "skip_rag_ingestion",
            "skip_regime_retrospective_eval",
            # skip_research retired: alpha-engine-config-I2515 Phase B
            # removed the multi-agent Research state entirely.
            "skip_data_phase2",
            "skip_eval_judge",
            "skip_rationale_clustering",
            # skip_replay_concordance retired: alpha-engine-config-I10539
            # (Brian ruling 2026-09-16, option b) removed the
            # ReplayConcordance stage from the weekly SF.
            "skip_counterfactual",
            # skip_aggregate_costs is NOT here (alpha-engine-config-I7194):
            # the aggregator left Branch A for the top-level tail, where it
            # runs AFTER Director — so a run that failed at Evaluator never
            # reached it, and a stage the run never attempted must not be
            # skipped on the rerun. The fixture was trimmed to match: it is a
            # pre-move history, and the two events it carried for those
            # states cannot occur under this topology. A REAL pre-move
            # history replayed after this change derives aggregate_costs as
            # failed rather than complete, which re-runs an idempotent
            # Lambda — the safe direction, same as director's inversion.
            "skip_predictor_training",
            "skip_backtester_stage_only",
            "skip_predictor_backtest",
            "skip_portfolio_optimizer_backtest",
            "skip_pit_parity_lookahead",
            "skip_parity_replay",
            "skip_pit_parity_compare",
        }
        # the failed stage must never carry its own skip flag
        assert "skip_evaluator" not in plan.skip_flags

    def test_early_failure(self, mod):
        plan = mod.derive_plan(_events("early_failure"))
        assert plan.failed == ["data_phase1"]
        assert set(plan.skip_flags) == {"skip_morning_enrich"}

    @pytest.mark.parametrize(
        "fixture",
        ["parallel_branch_failure", "tail_stage_failure", "early_failure"],
    )
    def test_rerun_input_contract(self, mod, fixture):
        """The emitted input must carry the original run_date, the
        watch-rerun role, and the sns passthrough — the exact config#2277
        contract. config#2248: the fixtures' original execution input no
        longer carries ec2_instance_id (the live SaturdayTrigger Input
        dropped it — the weekly SF's own CheckSpotDispatchNeeded/
        DispatchWeeklyFreshnessSpot states populate it from a fresh
        ephemeral spot instead), so a rerun of a post-config#2248 execution
        correctly omits it too and goes through that same dispatch path —
        see test_rerun_passes_through_explicit_ec2_instance_id_when_present
        below for the operator-override case where it IS present."""
        plan = mod.derive_plan(_events(fixture))
        inp = plan.rerun_input()
        assert inp["run_date"] == "2026-07-11"
        assert inp["pipeline_role"] == "watch-rerun"
        assert "ec2_instance_id" not in inp
        assert inp["sns_topic_arn"] == (
            "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts"
        )
        for flag, val in plan.skip_flags.items():
            assert inp[flag] is val is True

    def test_rerun_passes_through_explicit_ec2_instance_id_when_present(self, mod):
        """config#2248 escape hatch: rerun_input() is a generic passthrough
        (`dict(self.original_input)`) — if an operator's original
        StartExecution input DID carry an explicit ec2_instance_id (manual
        override, or a redrive against a still-live launcher box), the
        rerun must carry it through unchanged rather than stripping it, so
        the SF's CheckSpotDispatchNeeded Choice skips a second dispatch."""
        events = _events("early_failure")
        started = next(e for e in events if "executionStartedEventDetails" in e)
        inp = json.loads(started["executionStartedEventDetails"]["input"])
        inp["ec2_instance_id"] = ["i-manualoverride"]
        started["executionStartedEventDetails"]["input"] = json.dumps(inp)
        plan = mod.derive_plan(events)
        assert plan.rerun_input()["ec2_instance_id"] == ["i-manualoverride"]

    def test_explicit_input_run_date_wins(self, mod):
        events = _events("early_failure")
        started = next(e for e in events if "executionStartedEventDetails" in e)
        inp = json.loads(started["executionStartedEventDetails"]["input"])
        inp["run_date"] = "2026-07-04"
        started["executionStartedEventDetails"]["input"] = json.dumps(inp)
        plan = mod.derive_plan(events)
        assert plan.run_date == "2026-07-04"
        assert "explicit" in plan.run_date_provenance

    def test_run_date_falls_back_to_start_time(self, mod):
        events = [
            e for e in _events("early_failure")
            if e.get("stateExitedEventDetails", {}).get("name") != "InitializeInput"
        ]
        start = datetime(2026, 7, 11, 9, 0, tzinfo=timezone.utc)
        plan = mod.derive_plan(events, start_time=start)
        assert plan.run_date == "2026-07-11"
        assert "FALLBACK" in plan.run_date_provenance

    def test_inherited_skip_of_a_failed_stage_is_dropped_not_carried(self, mod):
        """A skip flag on the SOURCE execution's own input never reaches the
        emitted input (alpha-engine-config-I7259).

        This previously raised ``SystemExit(... unreachable ...)``: the helper
        preserved the source input wholesale, so an inherited
        ``skip_backtester`` would have bypassed a stage that must re-run, and
        refusing was the only safe answer. Dropping inherited skips removes the
        condition rather than detecting it — dropping a skip can only cause
        MORE stages to run, never fewer, so it can never strand a must-rerun
        stage. The reachability guard is retained for DERIVED skips, where an
        overshadowing route is still possible (see the
        ``skip_backtester`` -> ``skip_backtester_stage_only`` demotion).

        The motivating instance was quieter than this one and is why detection
        was not enough: ``watch-rerun-2026-08-13-3`` carried
        ``skip_pit_parity_compare``, which the derivation never produced.
        ``pit_parity_compare`` was neither failed nor degraded, so it was not
        in ``must_rerun`` and the guard above could not see it — the flag rode
        into the next rerun's input, disabling the stage that consumes the
        three parity passes that same rerun existed to recompute.
        """
        events = _events("tail_stage_failure")
        started = next(e for e in events if "executionStartedEventDetails" in e)
        inp = json.loads(started["executionStartedEventDetails"]["input"])
        inp["skip_backtester"] = True  # would jump the degraded branch's gate
        started["executionStartedEventDetails"]["input"] = json.dumps(inp)

        plan = mod.derive_plan(events)
        emitted = plan.rerun_input()

        assert emitted.get("skip_backtester") is not True, (
            "an inherited skip_backtester reached the emitted input — it would "
            "bypass a stage that must re-run"
        )
        assert "skip_backtester" in plan.dropped_inherited_skips, (
            "the dropped flag is not reported, so the operator cannot see that "
            "the emitted input differs from the source input"
        )

    def test_dropped_inherited_skips_excludes_derived_ones(self, mod):
        """A flag that is BOTH inherited and derived is emitted, and is not
        reported as dropped — it belongs to the plan on its own merit."""
        events = _events("tail_stage_failure")
        started = next(e for e in events if "executionStartedEventDetails" in e)
        inp = json.loads(started["executionStartedEventDetails"]["input"])
        plan = mod.derive_plan(events)
        derived = sorted(plan.skip_flags)
        assert derived, "fixture derived no skips — test would be vacuous"
        inp[derived[0]] = True
        started["executionStartedEventDetails"]["input"] = json.dumps(inp)

        plan = mod.derive_plan(events)
        emitted = plan.rerun_input()
        assert emitted[derived[0]] is True
        assert derived[0] not in plan.dropped_inherited_skips

    def test_non_skip_keys_still_pass_through(self, mod):
        """Only ``skip_*`` is dropped. ``sns_topic_arn`` / ``ec2_instance_id``
        passthrough is the reason the emitted input starts from the source
        input at all."""
        events = _events("tail_stage_failure")
        started = next(e for e in events if "executionStartedEventDetails" in e)
        inp = json.loads(started["executionStartedEventDetails"]["input"])
        inp["sns_topic_arn"] = "arn:aws:sns:us-east-1:1:topic"
        inp["ec2_instance_id"] = "i-abc123"
        started["executionStartedEventDetails"]["input"] = json.dumps(inp)

        emitted = mod.derive_plan(events).rerun_input()
        assert emitted["sns_topic_arn"] == "arn:aws:sns:us-east-1:1:topic"
        assert emitted["ec2_instance_id"] == "i-abc123"

    def test_degraded_tail_is_rerun_not_skipped(self, mod):
        """alpha-engine-config-I6055: a stage that DEGRADED (ran, failed,
        and was absorbed by a Publish*Degraded route so the pipeline could
        continue) must NEVER be treated as complete — skipping it is what
        made watch-rerun-2026-08-01-5 go green while doing nothing about
        the Director hard-fail its rerun was started for. This fixture IS
        the real watch-rerun-2026-08-01-4 history: Director failed with
        'No module named openai' (event 858), PublishDirectorDegraded
        absorbed it (PRE-FIX behavior — config#6408 now routes Director
        failure to terminal FailExecution), the health checks degraded too,
        and the run only failed terminally at WriteCompletionMarker.
        PublishDirectorDegraded is retained in degraded_witness for
        backward compatibility with pre-fix execution histories — new
        executions never enter it."""
        plan = mod.derive_plan(_events("director_degraded"))
        # the degraded tail must re-run — never skipped, never "completed".
        # alpha-engine-config-I8167: ownership of the health-check degraded
        # routes moved from the deprecated "post_eval" alias row to the new
        # "saturday_health_check" row (emit_skip=True) — post_eval no longer
        # carries a degraded_witness at all.
        assert "saturday_health_check" in plan.degraded
        assert "saturday_health_check" not in plan.completed
        assert "skip_saturday_health_check" not in plan.skip_flags
        assert "post_eval" not in plan.degraded
        assert "skip_post_eval" not in plan.skip_flags
        # the degradation must be surfaced in the derivation notes
        # (PublishDirectorDegraded retained in degraded_witness for backward compat)
        assert any("DEGRADED" in n and "PublishDirectorDegraded" in n for n in plan.notes)
        # stages that genuinely completed cleanly keep their skip flags
        # (evaluator really ran to completion on this execution). The parity
        # FAMILY reads completed (pre-#6030 history, witness
        # CheckSkipEvaluator) but skip_parity is deliberately never
        # auto-emitted post-#6030 — a rerun of an old history re-runs the
        # parity family conservatively (the old bundled artifacts cannot
        # witness the new per-stage set).
        assert plan.skip_flags.get("skip_evaluator") is True
        assert "skip_parity" not in plan.skip_flags
        assert "evaluator" in plan.completed and "parity" in plan.completed
        # and the rerun input must not bypass the tail
        assert "skip_post_eval" not in plan.rerun_input()

    def test_degraded_branch_a_stage_is_rerun_not_skipped(self, mod):
        """The same degraded-overrides-witness rule holds for branch A: a
        Mark*Degraded state must drop that stage's skip flag even when the
        stage's witness was also entered — degraded beats completed.

        This used to be pinned on ThinkTankDegraded, whose chain was removed
        from the weekly SF on 2026-08-10 (Brian ruling: the Think Tank runs
        daily in shadow mode, outside this pipeline). ChallengerShadow is the
        same shape — an observe-only branch-A stage with its own degraded
        state — so the rule is pinned there instead of going unpinned."""
        events = _events("tail_stage_failure")
        events = list(events) + [
            {
                "type": "PassStateEntered",
                "id": 99999,
                "timestamp": "2026-07-11T09:00:00Z",
                "stateEnteredEventDetails": {"name": "MarkChallengerShadowDegraded"},
            }
        ]
        plan = mod.derive_plan(events)
        assert "challenger_shadow" in plan.degraded
        assert "challenger_shadow" not in plan.completed
        assert "skip_challenger_shadow" not in plan.skip_flags


# ---------------------------------------------------------------------------
# Mutex-steal decision matrix (config#2280 contract)
# ---------------------------------------------------------------------------

class TestMutexDecisionMatrix:
    KEY = "ne-weekly-freshness-pipeline#weekly#2026-07-11"
    SRC = "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:x"
    HOLDER = SRC.replace(":x", ":holder")

    def _item(self, arn=HOLDER):
        item = {"mutex_key": {"S": self.KEY}}
        if arn is not None:
            item["execution_arn"] = {"S": arn}
        return item

    def test_no_item_proceeds(self, mod):
        d = mod.decide_mutex_action(None, None, self.KEY, self.SRC)
        assert d.action == "proceed"

    def test_running_holder_aborts_never_steals(self, mod):
        d = mod.decide_mutex_action(self._item(), "RUNNING", self.KEY, self.SRC)
        assert d.action == "abort"
        assert "RUNNING" in d.reason

    def test_succeeded_holder_aborts(self, mod):
        d = mod.decide_mutex_action(self._item(), "SUCCEEDED", self.KEY, self.SRC)
        assert d.action == "abort"
        assert d.manual_cmd  # operator escape hatch is named
        assert "duplicate" in d.reason

    @pytest.mark.parametrize("status", ["FAILED", "TIMED_OUT", "ABORTED"])
    def test_terminal_failed_holder_steals(self, mod, status):
        d = mod.decide_mutex_action(self._item(), status, self.KEY, self.SRC)
        assert d.action == "steal"
        assert d.holder_arn == self.HOLDER
        assert d.holder_status == status
        # loud output names what is deleted and why it is safe
        assert "TERMINAL" in d.reason and "safe" in d.reason

    def test_item_without_holder_arn_aborts_with_manual_cmd(self, mod):
        d = mod.decide_mutex_action(self._item(arn=None), None, self.KEY, self.SRC)
        assert d.action == "abort"
        assert "delete-item" in d.manual_cmd

    def test_undescribable_holder_aborts(self, mod):
        d = mod.decide_mutex_action(self._item(), None, self.KEY, self.SRC)
        assert d.action == "abort"
        assert "terminal proof" in d.reason


# ---------------------------------------------------------------------------
# Role-gating verification (config#2277 deliverable 2)
# ---------------------------------------------------------------------------

class TestRoleGating:
    def test_current_weekly_definition_renders_flags_live(self, mod, sf_def):
        """The weekly SF's skip gates are role-UNCONDITIONAL today (unlike
        the EOD SF's config#1614 operator-replay conjunct) — the emitted
        watch-rerun role must render its own flags live."""
        mod.verify_skip_flags_live(sf_def, mod.EMITTED_ROLE)

    def test_no_weekly_skip_gate_references_pipeline_role_today(self, sf_def, mod):
        """Drift tripwire: the helper's whole role choice rests on the
        weekly skip gates being role-unconditional. If someone ports the
        EOD-style role gating to the weekly SF, this test forces the
        helper's EMITTED_ROLE / derivation to be revisited in the same PR."""
        for name, state in mod._walk_states(sf_def["States"]):
            if name.startswith("CheckSkip") and state.get("Type") == "Choice":
                assert "$.pipeline_role" not in json.dumps(state.get("Choices")), (
                    f"{name} now conjuncts pipeline_role — update "
                    f"scripts/weekly_sf_rerun.py's role handling + this test"
                )

    def test_eod_style_gating_fails_loudly(self, mod):
        gated = {
            "States": {
                "CheckSkipFoo": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "And": [
                                {"Variable": "$.skip_foo", "BooleanEquals": True},
                                {"Variable": "$.pipeline_role", "StringEquals": "operator-replay"},
                            ],
                            "Next": "Bar",
                        }
                    ],
                    "Default": "Foo",
                }
            }
        }
        with pytest.raises(SystemExit, match="role gating"):
            mod.verify_skip_flags_live(gated, "watch-rerun")
        # ...but passes when the emitted role IS in the live set
        gated["States"]["CheckSkipFoo"]["Choices"][0]["And"][1]["StringEquals"] = "watch-rerun"
        mod.verify_skip_flags_live(gated, "watch-rerun")

    def test_emitted_role_bypasses_mutex_and_run_day_gate(self, mod, sf_def):
        """watch-rerun must NOT be in the CheckMutexRole cadence allowlist
        (else every rerun would deadlock on the failed run's slot without a
        steal) and must NOT trigger the weekly run-day gate (else a Sunday
        recovery silently Succeed-skips)."""
        states = sf_def["States"]
        mutex_rule = json.dumps(states["CheckMutexRole"]["Choices"])
        assert f'"{mod.EMITTED_ROLE}"' not in mutex_rule
        gate_rule = json.dumps(states["CheckWeeklyRunDayGate"]["Choices"])
        assert '"weekly"' in gate_rule and f'"{mod.EMITTED_ROLE}"' not in gate_rule
        # and the script's CADENCE_ROLES mirror stays in lockstep
        seen = {
            c["StringEquals"]
            for c in states["CheckMutexRole"]["Choices"][0]["And"][1]["Or"]
        }
        assert seen == set(mod.CADENCE_ROLES)


# ---------------------------------------------------------------------------
# Stage-table lockstep with the SF definition
# ---------------------------------------------------------------------------

class TestStageTableLockstep:
    """The helper is only correct while its declarative STAGES table matches
    the deployed skip-gate topology. These guards fail the build the moment
    the SF definition and the table drift."""

    @pytest.fixture(scope="class")
    def all_states(self, ):
        d = json.loads(SF_PATH.read_text())

        def walk(states):
            for name, state in states.items():
                yield name, state
                if state.get("Type") == "Parallel":
                    for b in state.get("Branches", []):
                        yield from walk(b["States"])
                if state.get("Type") == "Map":
                    it = state.get("Iterator") or state.get("ItemProcessor") or {}
                    yield from walk(it.get("States", {}))

        return dict(walk(d["States"]))

    def test_every_stage_state_exists(self, mod, all_states):
        for stage in mod.STAGES:
            assert stage.gate in all_states, f"{stage.name}: gate {stage.gate} missing"
            assert all_states[stage.gate]["Type"] == "Choice"
            assert stage.work in all_states, f"{stage.name}: work {stage.work} missing"
            for w in stage.witness:
                assert w in all_states, f"{stage.name}: witness {w} missing"

    def test_every_gate_tests_its_flag(self, mod, all_states):
        for stage in mod.STAGES:
            choices = json.dumps(all_states[stage.gate]["Choices"])
            assert f"$.{stage.flag}" in choices, (
                f"{stage.name}: gate {stage.gate} no longer tests {stage.flag}"
            )

    def test_every_checkskip_gate_is_covered_by_a_stage(self, mod, all_states):
        """Completeness: a NEW CheckSkip* gate in the SF without a STAGES row
        means the helper would silently never skip that stage."""
        gates = {s.gate for s in mod.STAGES}
        for name, state in all_states.items():
            if name.startswith("CheckSkip") and state.get("Type") == "Choice":
                if name == "CheckSkipPredictorTraining":
                    assert name in gates
                    continue
                assert name in gates, (
                    f"new skip gate {name} is not covered by "
                    f"scripts/weekly_sf_rerun.py STAGES — add a row"
                )

    def test_skip_route_lands_in_witness_except_backtester(self, mod, all_states):
        """For every stage, the gate's skip route must land inside the
        stage's witness set — that is what makes 'witness entered' mean
        'completed OR skipped'. The single deliberate exception is
        skip_backtester, whose legacy whole-pair jump lands PAST its
        witness (the overshoot the DROP logic in derive_plan handles)."""
        for stage in mod.STAGES:
            gate = all_states[stage.gate]
            skip_targets = {c["Next"] for c in gate["Choices"]}
            if stage.name == "backtester":
                assert skip_targets == {"CheckSkipEvaluator"}, (
                    "CheckSkipBacktester's overshoot target changed — "
                    "revisit BACKTESTER_OVERSHADOWED + the DROP logic"
                )
                continue
            if stage.name == "predictor_training":
                # two skip routes: preset fast-path Pass + freshness-proof path
                assert "PredictorTrainingSkipped" in skip_targets
                assert skip_targets <= {
                    "PredictorTrainingSkipped",
                    "ValidatePredictorSkipWeightsFresh",
                }
                continue
            if stage.name == "director":
                # config#6054: DELIBERATE exception, inverted from the
                # convention. director's witness is the success-only
                # DirectorComplete Pass state, and its skip route lands on
                # CheckSkipScannerLeaderboard (alpha-engine-config-I7813 —
                # was CheckShellRunNotify until the observe-only scanner board
                # became a leaf state between the two) — which every bypass
                # path also enters.
                # Witnessing on the skip target would mark a bypassed
                # Director complete and skip it on the rerun (the I6055
                # trap). Cost of the inversion: an original run that SKIPPED
                # director yields a rerun that re-runs it — the safe
                # direction for an advisory stage.
                assert skip_targets == {"CheckSkipScannerLeaderboard"}
                assert "DirectorComplete" not in skip_targets
                continue
            if stage.name == "scanner_leaderboard":
                # alpha-engine-config-I7813: same inverted convention as
                # director, and for the same I6055 reason — the witness is the
                # success-only ScannerLeaderboardComplete Pass, while the skip
                # route lands on the tail's shared convergence point, which
                # every bypass path also enters. alpha-engine-config-I7194
                # moved that point one state earlier — from CheckShellRunNotify
                # to CheckSkipAggregateCosts — so the aggregator runs after
                # Director. An original run that skipped the leaf yields a
                # rerun that re-runs it: the safe direction for an observe-only
                # board that costs one Lambda invocation.
                assert skip_targets == {"CheckSkipAggregateCosts"}
                assert "ScannerLeaderboardComplete" not in skip_targets
                continue
            if stage.name == "backtester_stage_only":
                # config#2362 Option A additive gate: deliberately empty
                # witness (it shares Backtester's work state with the
                # "backtester" row, which already owns completion/failure
                # detection for that physical task) — checked structurally
                # here instead.
                assert skip_targets == {"CheckSkipPredictorBacktest"}, (
                    "CheckSkipBacktesterStageOnly's skip route changed — "
                    "update the config#2362 Option A additive gate"
                )
                continue
            if stage.name == "pit_parity_compare":
                # the compare's skip route overshoots its witness to the
                # evaluator gate — like backtester_stage_only, checked
                # structurally: a skipped compare emits nothing and the
                # original input's flag carries over.
                assert skip_targets == {"CheckSkipEvaluator"}, (
                    "CheckSkipPitParityCompare's skip route changed — "
                    "update STAGES (alpha-engine-config#6030)"
                )
                continue
            assert skip_targets & stage.witness, (
                f"{stage.name}: skip route {skip_targets} no longer lands in "
                f"witness {set(stage.witness)} — update STAGES"
            )

    def test_backtester_overshadow_list_matches_topology(self, mod, all_states):
        """predictor_backtest/portfolio_optimizer_backtest/parity gates are
        only reachable through CheckSkipBacktester's RUN path."""
        assert mod.BACKTESTER_OVERSHADOWED == (
            "predictor_backtest",
            "portfolio_optimizer_backtest",
            "pit_parity_lookahead",
            "pit_parity_walkforward",
            "parity_replay",
            "pit_parity_compare",
        )
        # config#2362 Option A: CheckSkipBacktester's Default now falls
        # through the additive CheckSkipBacktesterStageOnly gate before
        # Backtester, rather than landing on Backtester directly.
        assert all_states["CheckSkipBacktester"]["Default"] == "CheckSkipBacktesterStageOnly"
        assert all_states["CheckSkipBacktesterStageOnly"]["Default"] == "Backtester"

    def test_every_degraded_state_is_mapped(self, mod, all_states):
        """Completeness (alpha-engine-config-I6055): a NEW *Degraded /
        Publish*Degraded route in the SF without a STAGES degraded_witness
        row means the helper would silently treat a degraded stage as
        completed and skip it on the rerun — the exact 2026-08-01 defect.
        The Notify*Degraded family is the terminal degraded-completion
        EMAIL surface, not a stage degrading, so it is deliberately
        unmapped."""
        mapped: dict = {}
        historical = getattr(mod, "HISTORICAL_DEGRADED_WITNESS", frozenset())
        for stage in mod.STAGES:
            for d in stage.degraded_witness:
                if d in historical:
                    continue  # retained for backward compat (pre-fix histories)
                assert d in all_states, (
                    f"{stage.name}: degraded witness {d} is not a state in "
                    f"infrastructure/step_function.json — update STAGES"
                )
                assert d not in mapped, (
                    f"degraded state {d} is mapped to both {mapped[d]} and "
                    f"{stage.name} — each degraded route must own exactly "
                    f"one stage"
                )
                mapped[d] = stage.name
        # alpha-engine-config#6722: CheckResearchPredictorDegraded (Choice)
        # and SetResearchPredictorDegraded (Pass) are the terminal AGGREGATE
        # fold that decides the completion-EMAIL routing (folded into
        # NotifyCompleteMultipleDegraded) — the same KIND of thing as the
        # Notify*Degraded family the docstring above already excludes, not a
        # per-stage rerun witness. The actual per-stage rerun signal is the
        # GRANULAR Mark*Degraded/ThinkTankDegraded states each branch owner
        # threads (those ARE mapped, above) — SetResearchPredictorDegraded
        # firing is always causally downstream of one of those, so it
        # carries no additional rerun-actionable information of its own.
        _AGGREGATE_FOLD_EXCLUDED = {
            "CheckResearchPredictorDegraded",
            "SetResearchPredictorDegraded",
            # alpha-engine-config-I6891 gave this fold a summary sibling too;
            # it inherits the exclusion for the same reason.
            "SetResearchPredictorDegradedSummary",
            # Branch INITIALISERS, surfaced by the substring predicate below.
            # They set the branch's degraded flag to FALSE at branch start —
            # entering one says the branch began, never that anything degraded,
            # so mapping them would make every run re-run the whole branch.
            "InitResearchDegradedFlag",
            "InitPredictorDegradedFlag",
            # alpha-engine-config-I8809: the fail-open floor for the graph's
            # date normalization. It is a routing Pass, not a stage — it
            # dispatches nothing, writes no artifact, and runs BEFORE any spend,
            # so there is nothing for a rerun to redo on the strength of it.
            # What it does record lives on the state input instead, where every
            # downstream writer carries it: run_date_family stays
            # 'calendar_date', naming the family the run actually used.
            "NormalizeRunDatesDegraded",
            # alpha-engine-config#5950: the floor for the optional degradation
            # fields NotifyCompleteMultipleDegraded dereferences. Same kind as
            # NormalizeRunDatesDegraded above — a Pass on the notify edge that
            # dispatches nothing, writes no artifact and runs after all spend,
            # matched here only by the "Degraded" substring in its name. It sits
            # BETWEEN CheckGateDegradedNotify and the notifier, so entering it
            # says a notification is being composed, never that a stage degraded;
            # the granular Mark*/Set*Degraded states carry that and ARE mapped.
            "NormalizeMultipleDegradedContext",
        }
        # alpha-engine-config-I6891: the predicate was `endswith("Degraded")`,
        # which the Set*DegradedSummary states this issue added would all have
        # slipped past — a detector whose reach is a NAME SUFFIX goes blind the
        # first time a state is named with anything after it. Substring match,
        # with the terminal family excluded by an explicit, reasoned list the
        # helper itself declares.
        terminal_family = getattr(mod, "TERMINAL_DEGRADED_FAMILY", frozenset())
        for name in all_states:
            if (
                "Degraded" in name
                and not name.startswith("Notify")
                and name not in _AGGREGATE_FOLD_EXCLUDED
                and name not in terminal_family
            ):
                assert name in mapped, (
                    f"degraded route {name} is not covered by any STAGES "
                    f"degraded_witness — a degraded {name} stage would be "
                    f"skipped as complete on a rerun; add it to STAGES"
                )

    def test_parity_degraded_route_is_mapped_in_stages(self, mod, all_states):
        """alpha-engine-config-I6025: the SF now routes every parity
        non-success through ParityDegraded → PublishParityDegraded and
        CONTINUES. A degraded parity must re-run on a mechanical rerun —
        never be skipped as completed — so the STAGES row must map both
        degraded-route states. (The full degraded-overrides-witness logic
        ships with alpha-engine-config-I6055; this pins the mapping so the
        guard can never miss it.)"""
        parity = next(s for s in mod.STAGES if s.name == "parity")
        # alpha-engine-config-I6891 added the $.degraded_summary sibling that
        # feeds the DegradedRun terminal; it is on the same fail-open path and
        # witnesses the same fact, so it belongs to the same row.
        assert parity.degraded_witness == frozenset({
            "ParityDegraded", "SetParityDegradedSummary", "PublishParityDegraded",
        })
        for d in parity.degraded_witness:
            assert d in all_states, (
                f"parity degraded witness {d} is not a state in "
                f"infrastructure/step_function.json — update STAGES"
            )


# ---------------------------------------------------------------------------
# config#3134 — mode=backtest-eval preset routes past all four lane-A gates
# ---------------------------------------------------------------------------

def _extract_preset_flags(all_states: dict) -> dict:
    """Mechanically parse the exact skip_* literal ApplyBacktestEvalPreset
    seeds, the same way _initialize_input_floors parses InitializeInput's
    literal in test_sf_choice_guards.py — so this test can never silently
    drift from the live Pass state's Parameters."""
    expr = all_states["ApplyBacktestEvalPreset"]["Parameters"]["merged.$"]
    start = expr.index("States.StringToJson('") + len("States.StringToJson('")
    end = expr.index("')", start)
    literal = expr[start:end].replace('\\"', '"')
    return json.loads(literal)


def _choice_next(state: dict, flags: dict) -> str:
    """Evaluate a single-rule And[IsPresent, BooleanEquals] skip-gate Choice
    (the shape every skip_* gate in this SF uses) against `flags` and
    return the resulting Next state name."""
    assert state["Type"] == "Choice"
    rule = state["Choices"][0]
    var = rule["And"][1]["Variable"].removeprefix("$.")
    if flags.get(var) is True:
        return rule["Next"]
    return state["Default"]


class TestBacktestEvalPresetLaneA:
    """config#3134 acceptance: a mode=backtest-eval execution's derived
    input must route the CheckSkip choices past every lane-A state
    (Scanner, SignalsEnvelope, ChallengerShadow — ThinkTankCoverage was the
    fourth until its chain left the weekly SF on 2026-08-10) —
    verified directly against the SF's Choice logic, mirroring the
    Backtester+Evaluator-only contract config#830 established for the
    non-lane-A stages."""

    @pytest.fixture(scope="class")
    def all_states(self):
        d = json.loads(SF_PATH.read_text())

        def walk(states):
            for name, state in states.items():
                yield name, state
                if state.get("Type") == "Parallel":
                    for b in state.get("Branches", []):
                        yield from walk(b["States"])
                if state.get("Type") == "Map":
                    it = state.get("Iterator") or state.get("ItemProcessor") or {}
                    yield from walk(it.get("States", {}))

        return dict(walk(d["States"]))

    @pytest.fixture(scope="class")
    def preset_flags(self, all_states):
        return _extract_preset_flags(all_states)

    def test_preset_sets_every_lane_a_flag_true(self, preset_flags):
        for flag in (
            "skip_scanner",
            "skip_signals_envelope",
            "skip_challenger_shadow",
        ):
            assert preset_flags.get(flag) is True, (
                f"mode=backtest-eval preset must seed {flag}=true"
            )

    @pytest.mark.parametrize(
        ("gate", "expected_skip_next"),
        [
            ("CheckSkipScanner", "CheckSkipRegimeSubstrate"),
            # alpha-engine-config-I7726 inserted CheckSkipResearchSelfTest
            # between these two. The preset must route past it as well, or a
            # Backtester+Evaluator-only replay spends a Lambda invocation on a
            # verdict about research code it does not touch.
            ("CheckSkipSignalsEnvelope", "CheckSkipResearchSelfTest"),
            ("CheckSkipResearchSelfTest", "CheckSkipChallengerShadow"),
            ("CheckSkipChallengerShadow", "CheckSkipRAGIngestion"),
        ],
    )
    def test_preset_flags_route_past_each_lane_a_gate(
        self, all_states, preset_flags, gate, expected_skip_next
    ):
        assert _choice_next(all_states[gate], preset_flags) == expected_skip_next, (
            f"{gate}: mode=backtest-eval's seeded flags must route past "
            f"this lane-A gate to {expected_skip_next}"
        )

    def test_backtester_and_evaluator_are_not_skipped(self, preset_flags):
        """config#830's original contract must still hold: the preset skips
        lane A too now, but still runs ONLY Backtester + Evaluator."""
        assert preset_flags.get("skip_backtester") is not True
        assert preset_flags.get("skip_evaluator") is not True


# ---------------------------------------------------------------------------
# Rerun naming
# ---------------------------------------------------------------------------

class _FakeSF:
    def __init__(self, names):
        self._names = names

    def list_executions(self, **kwargs):
        return {
            "executions": [
                {"name": n, "executionArn": f"arn:x:{n}", "status": "FAILED"}
                for n in self._names
            ]
        }


class TestRerunNaming:
    def test_first_rerun_is_n1(self, mod):
        sf = _FakeSF(["b90418ee-x", "offcycle-shell-1"])
        assert mod.next_rerun_name(sf, "arn:sm", "2026-07-11") == "watch-rerun-2026-07-11-1"

    def test_n_is_one_plus_max_prior(self, mod):
        sf = _FakeSF(
            ["watch-rerun-2026-07-11-1", "watch-rerun-2026-07-11-3",
             "watch-rerun-2026-07-04-9", "watch-rerun-2026-07-11-2"]
        )
        assert mod.next_rerun_name(sf, "arn:sm", "2026-07-11") == "watch-rerun-2026-07-11-4"

    def test_other_run_dates_do_not_collide(self, mod):
        sf = _FakeSF(["watch-rerun-2026-07-04-2"])
        assert mod.next_rerun_name(sf, "arn:sm", "2026-07-11") == "watch-rerun-2026-07-11-1"


# ── Renames must not blind the planner to older histories (config-I3112) ─────


class TestHistoricalWorkStateNames:
    """A work-state rename must keep failure detection working on histories
    captured BEFORE the rename.

    `derive_plan` attributes a failure by `work in entered`. This script's
    whole job is to read a PAST execution, so the day a work state is renamed
    or split, every history older than that change stops matching. The stage
    silently drops out of `plan.failed`, and the only trace is derive_plan's
    "no failed or degraded WORK stage identified" warning — which says
    *pre-workload failure*, i.e. it misattributes a lost signal as a different,
    benign condition. `tail_stage_failure.json` is a captured real execution
    from before the config-I3112 Evaluator split and is deliberately NOT
    rewritten: it is evidence, not a test double.
    """

    def test_pre_split_history_still_attributes_the_evaluator_failure(self, mod):
        events = _events("tail_stage_failure")
        entered = {e["stateEnteredEventDetails"]["name"]
                   for e in events if "stateEnteredEventDetails" in e}
        assert "Evaluator" in entered and "EvaluatorDiagnostics" not in entered, (
            "fixture no longer exercises the pre-split shape — pick another "
            "captured history rather than rewriting this one"
        )
        assert mod.derive_plan(events).failed == ["evaluator"]

    def test_post_split_history_attributes_it_by_the_current_name(self, mod):
        """The same failure, expressed in the post-split vocabulary."""
        events = [
            e for e in _events("tail_stage_failure")
            if e.get("stateEnteredEventDetails", {}).get("name") != "Evaluator"
        ]
        events.append({
            "type": "TaskStateEntered",
            "stateEnteredEventDetails": {"name": "EvaluatorDiagnostics"},
        })
        assert mod.derive_plan(events).failed == ["evaluator"]

    def test_a_failure_in_the_second_half_is_still_the_evaluator_stage(self, mod):
        """One Stage row covers both halves.

        EvaluatorDiagnostics is entered on every path that reaches
        EvaluatorOptimize, and the witness (CheckSkipPostEval) is reached only
        after both succeed — so a failure in the SECOND half leaves the first
        entered and the witness un-entered, which is exactly the failed shape.
        """
        events = [
            e for e in _events("tail_stage_failure")
            if e.get("stateEnteredEventDetails", {}).get("name") != "Evaluator"
        ]
        for name in ("EvaluatorDiagnostics", "EvaluatorOptimize"):
            events.append({
                "type": "TaskStateEntered",
                "stateEnteredEventDetails": {"name": name},
            })
        plan = mod.derive_plan(events)
        assert plan.failed == ["evaluator"]
        assert "skip_evaluator" not in plan.skip_flags, (
            "a failed evaluator must re-run — the flag gates BOTH halves"
        )

    def test_every_historical_work_name_is_gone_from_the_live_definition(self, mod):
        """A historical alias that still exists live is not historical.

        If both names are reachable, `work in entered or historical in entered`
        double-counts the same stage and the alias silently becomes a second
        live work state nobody declared.
        """
        sf = json.loads(
            (Path(__file__).resolve().parent.parent
             / "infrastructure" / "step_function.json").read_text())
        live = set(sf["States"])
        for stage in mod.STAGES:
            overlap = stage.historical_work & live
            assert not overlap, (
                f"{stage.name}: historical_work {sorted(overlap)} still exists "
                f"in step_function.json — remove the alias or the rename is "
                f"not complete"
            )


# ---------------------------------------------------------------------------
# alpha-engine-config-I7443: a rerun must carry the cycle's run_date, never
# mint one across a UTC-midnight boundary, and a skip_predictor_training
# whose weights are stale for that run_date must be rejected BEFORE a
# dispatch is spent (sf-pipeline-policy §2.5).
# ---------------------------------------------------------------------------

def _history(*, explicit_run_date=None, init_run_date=None, normalized_run_date=None, extra_input=None):
    """Build a minimal synthetic execution-history events list."""
    inp = {"pipeline_role": "watch-rerun"}
    if extra_input:
        inp.update(extra_input)
    if explicit_run_date is not None:
        inp["run_date"] = explicit_run_date
    events = [{
        "type": "ExecutionStarted",
        "executionStartedEventDetails": {"input": json.dumps(inp)},
    }]
    if init_run_date is not None:
        out = dict(inp)
        out["run_date"] = init_run_date
        out["calendar_date"] = init_run_date
        events.append({
            "type": "PassStateExited",
            "stateExitedEventDetails": {
                "name": "InitializeInput",
                "output": json.dumps(out),
            },
        })
    if normalized_run_date is not None:
        base = dict(inp)
        if init_run_date is not None:
            base["calendar_date"] = init_run_date
        base["run_date"] = normalized_run_date
        base["run_date_family"] = "trading_day"
        events.append({
            "type": "PassStateExited",
            "stateExitedEventDetails": {
                "name": "ApplyNormalizedRunDate",
                "output": json.dumps(base),
            },
        })
    return events


class TestRunDateCarriedAcrossUTCMidnight:
    """derive_run_date must carry the ORIGINAL cycle's run_date, never mint
    one from wall-clock/start-time, whenever the source execution's own
    input or InitializeInput output actually resolves one — regardless of
    what UTC calendar date the rerun happens to be launched on.

    Regression for alpha-engine-config-I7443: watch-rerun-2026-08-16-1/-2
    carried run_date=2026-08-16 though the recovered cycle was 2026-08-15;
    a start_time that has already crossed UTC midnight relative to the
    source's own explicit/InitializeInput run_date must NOT leak in.
    """

    def test_explicit_input_run_date_wins_over_a_later_start_time(self, mod):
        events = _history(explicit_run_date="2026-08-15")
        # start_time is AFTER UTC midnight relative to the cycle's own date —
        # exactly the shape of the real 2026-08-15/16 incident.
        late_start = datetime(2026, 8, 16, 1, 51, 10, tzinfo=timezone.utc)
        run_date, provenance = mod.derive_run_date(events, late_start)
        assert run_date == "2026-08-15"
        assert "explicit" in provenance
        assert "2026-08-16" not in provenance

    def test_initialize_input_run_date_wins_over_a_later_start_time(self, mod):
        events = _history(init_run_date="2026-08-15")
        late_start = datetime(2026, 8, 16, 1, 51, 10, tzinfo=timezone.utc)
        run_date, provenance = mod.derive_run_date(events, late_start)
        assert run_date == "2026-08-15"
        assert "InitializeInput" in provenance

    def test_normalized_trading_day_wins_over_initialize_calendar_day(self, mod):
        """alpha-engine-config-I8809: Saturday 2026-08-29 run keyed cycle
        2026-08-28; rerun must carry the trading day or skip-coherence on
        predictor weights rejects a valid recovery plan."""
        events = _history(init_run_date="2026-08-29", normalized_run_date="2026-08-28")
        late_start = datetime(2026, 8, 29, 9, 0, 49, tzinfo=timezone.utc)
        run_date, provenance = mod.derive_run_date(events, late_start)
        assert run_date == "2026-08-28"
        assert "ApplyNormalizedRunDate" in provenance

    def test_calendar_date_carried_from_initialize_input(self, mod):
        events = _history(init_run_date="2026-08-29", normalized_run_date="2026-08-28")
        late_start = datetime(2026, 8, 30, 10, 0, 0, tzinfo=timezone.utc)
        calendar_date, provenance = mod.derive_calendar_date(events, late_start)
        assert calendar_date == "2026-08-29"
        assert "InitializeInput" in provenance or "ApplyNormalizedRunDate" in provenance

    def test_rerun_input_emits_calendar_date(self, mod):
        events = _history(init_run_date="2026-08-29", normalized_run_date="2026-08-28")
        plan = mod.derive_plan(events, start_time=datetime(2026, 8, 30, 10, 0, 0, tzinfo=timezone.utc))
        emitted = plan.rerun_input()
        assert emitted["run_date"] == "2026-08-28"
        assert emitted["calendar_date"] == "2026-08-29"

    def test_only_a_genuine_pre_workload_failure_falls_back_to_start_time(self, mod):
        """No explicit run_date, no InitializeInput output at all (the
        source never reached it) — the ONLY case where start_time is used,
        and it must be labeled FALLBACK so --start refuses it by default."""
        events = _history()
        start = datetime(2026, 8, 16, 1, 51, 10, tzinfo=timezone.utc)
        run_date, provenance = mod.derive_run_date(events, start)
        assert run_date == "2026-08-16"
        assert provenance.startswith("FALLBACK")

    def test_start_refuses_a_fallback_run_date_without_explicit_acceptance(self, mod):
        """--start must not launch on a FALLBACK-provenance run_date unless
        the operator passes --accept-fallback-run-date (alpha-engine-
        config-I7443: the printed FALLBACK note went unnoticed on
        2026-08-16 and the operator's rerun launched anyway)."""
        plan = mod.RerunPlan(
            run_date="2026-08-16",
            run_date_provenance="FALLBACK: UTC date of the failed execution's start time",
            original_input={},
        )
        with pytest.raises(SystemExit) as exc:
            mod.refuse_fallback_run_date_without_acceptance(plan, accept=False)
        assert "FALLBACK" in str(exc.value)
        assert "--accept-fallback-run-date" in str(exc.value)
        # explicit acceptance lets it through
        mod.refuse_fallback_run_date_without_acceptance(plan, accept=True)

    def test_non_fallback_provenance_never_refused(self, mod):
        plan = mod.RerunPlan(
            run_date="2026-08-15",
            run_date_provenance="explicit run_date in the failed execution's input",
            original_input={},
        )
        mod.refuse_fallback_run_date_without_acceptance(plan, accept=False)  # does not raise

    def test_accept_fallback_flag_is_wired_into_the_cli(self, mod, capsys):
        with pytest.raises(SystemExit) as exc:
            mod.main(["--help"])
        assert exc.value.code == 0
        assert "--accept-fallback-run-date" in capsys.readouterr().out


class TestPredictorSkipFreshness:
    """check_predictor_skip_freshness mirrors the SF's own
    ValidatePredictorSkipWeightsFresh / CheckPredictorSkipWeightsFresh
    predicate (infrastructure/step_function.json): HeadObject the live
    weights manifest and require LastModified's DATE >= calendar_date. Checked
    here BEFORE any dispatch — alpha-engine-config-I7443."""

    class _FakeS3:
        def __init__(self, *, last_modified=None, error=None):
            self._last_modified = last_modified
            self._error = error

        def head_object(self, **kwargs):
            if self._error is not None:
                raise self._error
            return {"LastModified": self._last_modified}

    def test_noop_when_skip_predictor_training_not_set(self, mod):
        s3 = self._FakeS3(error=RuntimeError("must not be called"))
        mod.check_predictor_skip_freshness(s3, "2026-08-16", {})
        mod.check_predictor_skip_freshness(
            s3, "2026-08-16", {"skip_predictor_training": False}
        )

    def test_fresh_manifest_passes(self, mod):
        s3 = self._FakeS3(
            last_modified=datetime(2026, 8, 16, 3, 0, 0, tzinfo=timezone.utc)
        )
        mod.check_predictor_skip_freshness(
            s3, "2026-08-16", {"skip_predictor_training": True}
        )  # does not raise

    def test_manifest_older_than_calendar_date_is_rejected(self, mod):
        """The real 2026-08-15/16 incident shape: manifest last written
        2026-08-15, skip claimed for calendar_date 2026-08-16."""
        s3 = self._FakeS3(
            last_modified=datetime(2026, 8, 15, 20, 0, 0, tzinfo=timezone.utc)
        )
        with pytest.raises(mod.SkipCoherenceError) as exc:
            mod.check_predictor_skip_freshness(
                s3, "2026-08-16", {"skip_predictor_training": True}
            )
        assert "2026-08-15" in str(exc.value)
        assert "2026-08-16" in str(exc.value)

    def test_s3_error_is_rejected_not_silently_trusted(self, mod):
        s3 = self._FakeS3(error=RuntimeError("boom"))
        with pytest.raises(mod.SkipCoherenceError):
            mod.check_predictor_skip_freshness(
                s3, "2026-08-16", {"skip_predictor_training": True}
            )

    def test_coerce_clamps_calendar_date_to_manifest(self, mod):
        s3 = self._FakeS3(
            last_modified=datetime(2026, 8, 28, 17, 46, 15, tzinfo=timezone.utc)
        )
        cal, note = mod.coerce_calendar_date_for_predictor_skip(
            s3, "2026-08-29", {"skip_predictor_training": True}
        )
        assert cal == "2026-08-28"
        assert note is not None
        mod.check_predictor_skip_freshness(
            s3, cal, {"skip_predictor_training": True}
        )  # does not raise


class TestCadenceDeclaredSkipsAreCarried:
    """alpha-engine-config-I8153.

    A skip the CADENCE declares for itself is not an operator's stray flag, and
    dropping it made every mechanical rerun of a scheduled run silently
    RE-ENABLE a stage the cadence has deliberately disabled. On 2026-08-22 that
    meant `--start` would have launched the full parity family — a stage
    alpha-engine-config-I7309 records as unable to finish in any budget — and
    the only defence was reading a NOTE and hand-editing the emitted JSON.
    """

    def test_the_cadence_trigger_declares_skip_parity(self, mod):
        """The loader reads the real CFN, not a fixture: if the declaration
        moves or is renamed, this fails here rather than at 02:00 on a
        Saturday."""
        assert mod.cadence_declared_skips() == {"skip_parity": True}

    def test_the_emitted_input_carries_it(self, mod):
        plan = mod.derive_plan(_events("director_degraded"))
        emitted = plan.rerun_input()
        assert emitted.get("skip_parity") is True, (
            "the rerun no longer carries the cadence's own skip_parity — a "
            "recovery would run a stage the scheduled run does not"
        )
        assert "skip_parity" in plan.cadence_skips

    def test_it_is_reported_apart_from_the_derived_set(self, mod):
        """It is NOT derived from what the source execution completed, so it
        must not read as if it were — the derived set is this script's product
        and its provenance is the whole reason to trust it."""
        plan = mod.derive_plan(_events("director_degraded"))
        plan.rerun_input()
        assert "skip_parity" not in plan.skip_flags
        assert "skip_parity" not in plan.dropped_inherited_skips

    def test_a_non_declared_inherited_flag_is_still_dropped(self, mod):
        """I7259 is unchanged for ad-hoc flags — the new rule splits DECLARED
        from AD-HOC, it does not re-open inheritance."""
        plan = mod.derive_plan(_events("director_degraded"))
        plan.original_input = dict(plan.original_input)
        plan.original_input["skip_pit_parity_compare"] = True
        emitted = plan.rerun_input()
        assert "skip_pit_parity_compare" not in emitted
        assert "skip_pit_parity_compare" in plan.dropped_inherited_skips

    def test_an_unreadable_declaration_raises_rather_than_defaulting(self, mod, tmp_path):
        """An empty cadence-skip set and an unreadable one are the same value
        and opposite facts. Defaulting the second to the first re-enables every
        stage the cadence disables, silently."""
        missing = tmp_path / "nope.yaml"
        with pytest.raises(mod.CadenceSkipsUnreadable):
            mod.cadence_declared_skips(missing)

        shapeless = tmp_path / "shapeless.yaml"
        shapeless.write_text("Resources:\n  SomethingElse: {}\n")
        with pytest.raises(mod.CadenceSkipsUnreadable):
            mod.cadence_declared_skips(shapeless)


# ---------------------------------------------------------------------------
# alpha-engine-config-I8161 — chained recovery derives from the CHAIN
# ---------------------------------------------------------------------------

def _chain_history(entered, *, run_date="2026-08-22", extra_input=None):
    """A synthetic history: the ExecutionStarted event plus one stateEntered
    event per name in ``entered`` (order preserved, though nothing reads it)."""
    inp = {"pipeline_role": "weekly", "run_date": run_date}
    if extra_input:
        inp.update(extra_input)
    events = [{
        "type": "ExecutionStarted",
        "executionStartedEventDetails": {"input": json.dumps(inp)},
    }]
    events += [
        {"type": "ChoiceStateEntered", "stateEnteredEventDetails": {"name": n}}
        for n in entered
    ]
    return events


# The backtester family is the measured instance (alpha-engine-config-I8161):
# skip_backtester's skip route jumps straight to CheckSkipEvaluator, so a rerun
# that honours it enters NONE of the family's witnesses — every stage the
# ORIGINAL run completed there reads as not-completed in the rerun's own history.
_RUN1_COMPLETED_THROUGH_THE_BACKTESTER_FAMILY = [
    "InitializeInput", "CheckSkipBacktester", "Backtester",
    "CheckSkipPredictorBacktest",              # witness: backtester
    "PredictorBacktest",
    "CheckSkipPortfolioOptimizerBacktest",     # witness: predictor_backtest
    "PortfolioOptimizerBacktest",
    "CheckSkipParity",                         # witness: portfolio_optimizer_backtest
    "CheckSkipEvaluator", "EvaluatorDiagnostics",   # ... and dies here
]
_RUN2_SKIPPED_THEM_AND_DIED_AT_THE_SAME_PLACE = [
    "InitializeInput", "CheckSkipBacktester",
    "CheckSkipEvaluator", "EvaluatorDiagnostics",
]


class TestChainedRecoveryKeepsTheOriginalRunsProgress:
    """alpha-engine-config-I8161.

    ``weekly_sf_rerun.py`` defaults to the LATEST failed execution, which after
    one recovery is the RERUN — whose history contains only the stages it did
    not skip. Measured 2026-08-22: ``skip_backtester``,
    ``skip_predictor_backtest`` and ``skip_portfolio_optimizer_backtest`` moved
    from *derived skips* (deriving from the scheduled run) to *dropped skips*
    (deriving from ``watch-rerun-2026-08-22-1``), so a second recovery would
    have re-run the backtester family — hours of spot compute — for stages that
    completed cleanly four hours earlier.
    """

    def test_a_second_recovery_still_skips_what_the_first_run_completed(self, mod):
        """The guard the issue specifies: run 1 completes A and B then fails at
        C; run 2 skips A and B and fails at C again; run 3's plan skips A and B.
        Against the pre-fix single-execution derivation this FAILS — run 2's
        history witnesses none of them."""
        plan = mod.derive_plan(
            _chain_history(_RUN2_SKIPPED_THEM_AND_DIED_AT_THE_SAME_PLACE),
            prior_histories=[(
                "scheduled-run",
                _chain_history(_RUN1_COMPLETED_THROUGH_THE_BACKTESTER_FAMILY),
            )],
            source_label="watch-rerun-2026-08-22-1",
        )
        assert plan.failed == ["evaluator"]
        for flag in (
            "skip_backtester",
            "skip_predictor_backtest",
            "skip_portfolio_optimizer_backtest",
        ):
            assert plan.skip_flags.get(flag) is True, (
                f"{flag} was completed by the scheduled run and never attempted "
                "again — a second recovery must not re-burn it"
            )

    def test_the_source_alone_would_lose_them(self, mod):
        """The same source execution WITHOUT the chain — the measured pre-fix
        behaviour, kept as the contrast that makes the fix legible."""
        plan = mod.derive_plan(_chain_history(_RUN2_SKIPPED_THEM_AND_DIED_AT_THE_SAME_PLACE))
        assert "skip_backtester" not in plan.skip_flags
        assert "skip_predictor_backtest" not in plan.skip_flags

    def test_latest_attempt_wins_when_a_completed_stage_later_fails(self, mod):
        """A stage that completed in run 1 and FAILED in run 3 must re-run —
        the chain is a union resolved by recency, never a monotone union of
        completions."""
        plan = mod.derive_plan(
            _chain_history([
                "InitializeInput", "CheckSkipBacktester", "Backtester",
                # no CheckSkipPredictorBacktest: Backtester ran and died
            ]),
            prior_histories=[(
                "scheduled-run",
                _chain_history(_RUN1_COMPLETED_THROUGH_THE_BACKTESTER_FAMILY),
            )],
            source_label="watch-rerun-2026-08-22-1",
        )
        assert "backtester" in plan.failed
        assert "skip_backtester" not in plan.skip_flags
        assert plan.witnessed_by["backtester"] == "watch-rerun-2026-08-22-1"

    def test_an_unattempted_stage_keeps_the_earlier_verdict(self, mod):
        """...and the other direction: completed in run 1, never attempted
        again, stays completed — with the earlier run named as the witness."""
        plan = mod.derive_plan(
            _chain_history(_RUN2_SKIPPED_THEM_AND_DIED_AT_THE_SAME_PLACE),
            prior_histories=[(
                "scheduled-run",
                _chain_history(_RUN1_COMPLETED_THROUGH_THE_BACKTESTER_FAMILY),
            )],
            source_label="watch-rerun-2026-08-22-1",
        )
        assert plan.witnessed_by["backtester"] == "scheduled-run"
        assert plan.witnessed_by["evaluator"] == "watch-rerun-2026-08-22-1"
        assert plan.chain == ["scheduled-run", "watch-rerun-2026-08-22-1"]

    def test_a_degraded_stage_in_a_later_link_beats_an_earlier_completion(self, mod):
        """I6055's rule composes with the chain: degraded is re-run, and a
        LATER degradation overrides an earlier clean completion."""
        plan = mod.derive_plan(
            _chain_history([
                "InitializeInput", "CheckSkipBacktester", "Backtester",
                "CheckSkipPredictorBacktest", "PredictorBacktest",
                "CheckSkipPortfolioOptimizerBacktest",
                "PortfolioOptimizerBacktest", "CheckSkipParity",
                "CheckSkipEvaluator", "EvaluatorDiagnostics",
                "CheckSkipPostEval", "SaturdayHealthCheck",
                "SaturdayHealthCheckDegraded",
            ]),
            prior_histories=[(
                "scheduled-run",
                _chain_history(
                    _RUN1_COMPLETED_THROUGH_THE_BACKTESTER_FAMILY
                    + ["CheckSkipPostEval", "SaturdayHealthCheck", "CheckShellRunNotify"]
                ),
            )],
            source_label="watch-rerun-2026-08-22-1",
        )
        # alpha-engine-config-I8167: degraded-route ownership moved to the
        # new "saturday_health_check" row. "post_eval" (the deprecated,
        # emit_skip=False whole-tail alias) still reads "completed" from the
        # prior link's witness (CheckShellRunNotify) — informational only,
        # since emit_skip=False means it never emits skip_post_eval.
        assert "saturday_health_check" in plan.degraded
        assert "saturday_health_check" not in plan.completed
        assert "post_eval" not in plan.degraded
        assert "skip_post_eval" not in plan.skip_flags

    def test_the_chain_reads_histories_never_a_previous_inputs_flags(self, mod):
        """Immunity to alpha-engine-config-I7259 is structural, not a rule the
        chain has to remember: a hand-added ``skip_*`` on an EARLIER execution's
        input leaves no trace in any history, so it cannot propagate."""
        plan = mod.derive_plan(
            _chain_history(_RUN2_SKIPPED_THEM_AND_DIED_AT_THE_SAME_PLACE),
            prior_histories=[(
                "scheduled-run",
                _chain_history(
                    _RUN1_COMPLETED_THROUGH_THE_BACKTESTER_FAMILY,
                    extra_input={"skip_rationale_clustering": True},
                ),
            )],
            source_label="watch-rerun-2026-08-22-1",
        )
        assert "skip_rationale_clustering" not in plan.skip_flags
        assert "rationale_clustering" not in plan.completed


class TestDirectorReportCardFreshnessCoupling:
    """alpha-engine-config-I8382, measured 2026-08-22 watch-rerun-2026-08-22-3.

    Director's input contract is a report_card.json produced by THIS
    execution. report_card and director are independently-witnessed stages,
    so a rerun can legitimately witness report_card as "completed" (it ran to
    completion in this or an earlier chain link) while director itself never
    completes (its failure is terminal — config#6408 — so it keeps re-running
    across chain links until one succeeds). Left alone, that composes into a
    rerun that SKIPS ReportCard while RE-RUNNING Director against the stale
    card ReportCard last wrote — measured live: a card written by the FAILED
    02:00 execution graded by a Director that ran nearly two hours later.
    """

    # ReportCard runs and completes (witness: CheckSkipDirector + Director
    # entered), then Director itself runs and fails terminally (Director is
    # entered as WORK — detect_failure — but DirectorComplete never is).
    _REPORT_CARD_COMPLETED_DIRECTOR_FAILED = [
        "InitializeInput", "CheckSkipBacktester", "Backtester",
        "CheckSkipPredictorBacktest", "PredictorBacktest",
        "CheckSkipPortfolioOptimizerBacktest", "PortfolioOptimizerBacktest",
        "CheckSkipParity", "CheckSkipEvaluator", "EvaluatorDiagnostics",
        "CheckSkipPostEval", "SaturdayHealthCheck", "CheckShellRunNotify",
        "CheckSkipReportCard", "ReportCard",
        "CheckSkipDirector", "Director",
        # no DirectorComplete: Director hard-failed (terminal, config#6408)
    ]

    def test_director_will_run_forces_report_card_to_rerun(self, mod):
        plan = mod.derive_plan(
            _chain_history(self._REPORT_CARD_COMPLETED_DIRECTOR_FAILED)
        )
        assert "director" in plan.failed
        assert "skip_director" not in plan.skip_flags
        assert "skip_report_card" not in plan.skip_flags, (
            "Director will run this rerun, so ReportCard must run too — "
            "skipping it lets Director grade a card from a different, "
            "possibly-stale attempt"
        )
        assert "report_card" not in plan.completed
        assert any(
            "skip_report_card dropped" in n and "I8382" in n
            for n in plan.notes
        )

    def test_the_dropped_flag_is_actually_reachable(self, mod, sf_def):
        """The emitted input must not just OMIT skip_report_card — ReportCard's
        work state must be reachable under the rest of the derived flags, or
        this is a paper fix."""
        plan = mod.derive_plan(
            _chain_history(self._REPORT_CARD_COMPLETED_DIRECTOR_FAILED)
        )
        reachable = mod._simulate_reachable_works(plan.skip_flags, {})
        assert "report_card" in reachable

    def test_report_card_still_skips_when_director_also_completed(self, mod):
        """Regression: the ordinary case (both witnessed complete) is
        unaffected — nothing here should force an unnecessary ReportCard
        re-run when Director genuinely does not need to run."""
        entered = self._REPORT_CARD_COMPLETED_DIRECTOR_FAILED[:-1] + [
            "DirectorComplete",
        ]
        plan = mod.derive_plan(_chain_history(entered))
        assert plan.skip_flags.get("skip_director") is True
        assert plan.skip_flags.get("skip_report_card") is True
        assert "report_card" in plan.completed


class TestChainMembership:
    """Which executions the chain is built FROM — pure, so the membership rule
    is testable without AWS."""

    @staticmethod
    def _ex(name, day, hour, status="FAILED"):
        return {
            "executionArn": f"arn:aws:states:us-east-1:1:execution:sm:{name}",
            "name": name,
            "status": status,
            "startDate": datetime(2026, 8, day, hour, tzinfo=timezone.utc),
        }

    def test_earlier_terminal_executions_are_candidates_oldest_first(self, mod):
        source = self._ex("watch-rerun-2026-08-22-2", 22, 15)
        execs = [
            source,
            self._ex("watch-rerun-2026-08-22-1", 22, 14),
            self._ex("scheduled", 22, 9, status="FAILED"),
        ]
        cands, non_terminal = mod.chain_candidates(
            execs, "2026-08-22", source["executionArn"], source["startDate"]
        )
        assert [e["name"] for e in cands] == ["scheduled", "watch-rerun-2026-08-22-1"]
        assert non_terminal == []

    def test_the_source_is_the_frontier(self, mod):
        """An execution that started AFTER the source is not part of the chain
        being recovered — with --execution-arn the operator chose that frontier
        deliberately."""
        source = self._ex("watch-rerun-2026-08-22-1", 22, 14)
        later = self._ex("watch-rerun-2026-08-22-2", 22, 15)
        cands, _ = mod.chain_candidates(
            [source, later], "2026-08-22", source["executionArn"], source["startDate"]
        )
        assert cands == []

    def test_a_running_execution_is_excluded_and_reported(self, mod):
        """A partial history is not evidence — and its exclusion is said out
        loud rather than inferred from a shorter skip set."""
        source = self._ex("watch-rerun-2026-08-22-2", 22, 15)
        running = self._ex("watch-rerun-2026-08-22-1", 22, 14, status="RUNNING")
        cands, non_terminal = mod.chain_candidates(
            [source, running], "2026-08-22",
            source["executionArn"], source["startDate"],
        )
        assert cands == []
        assert [e["name"] for e in non_terminal] == ["watch-rerun-2026-08-22-1"]

    def test_a_far_older_cycle_is_out_of_the_window(self, mod):
        source = self._ex("watch-rerun-2026-08-22-1", 22, 14)
        old = self._ex("scheduled-prev-week", 15, 9)
        cands, _ = mod.chain_candidates(
            [source, old], "2026-08-22", source["executionArn"], source["startDate"]
        )
        assert cands == []


# ---------------------------------------------------------------------------
# alpha-engine-config-I8162 — no spot boot when no box stage survives
# ---------------------------------------------------------------------------

def _rule_matches(rule: dict, flags: dict) -> bool:
    """Evaluate one Choice rule (nested And of IsPresent/BooleanEquals pairs,
    or the bare IsPresent passthrough) against an execution input."""
    if "And" in rule:
        return all(_rule_matches(sub, flags) for sub in rule["And"])
    var = rule["Variable"].removeprefix("$.")
    if "IsPresent" in rule:
        return (var in flags) is rule["IsPresent"]
    if "BooleanEquals" in rule:
        return flags.get(var) is rule["BooleanEquals"]
    raise AssertionError(f"unhandled comparison in {rule}")


def _dispatch_gate_next(state: dict, flags: dict) -> str:
    for rule in state["Choices"]:
        if _rule_matches(rule, flags):
            return rule["Next"]
    return state["Default"]


class TestSpotDispatchOnlyWhenABoxStageSurvives:
    """alpha-engine-config-I8162.

    ``watch-rerun-2026-08-22-1`` spent 07:25:26 -> 07:29:29 PT — 4 of the run's
    16 minutes — in ``DispatchWeeklyFreshnessSpot`` ->
    ``WaitForWeeklyFreshnessSpotBootstrap``. Recovery is exactly when the skip
    set is largest, so the most often-unnecessary stage was the one that always
    ran.

    The predicate is DERIVED from ``STAGES`` by reachability over the live
    definition, never hand-listed in the SF: hand-listing drifts in the
    asymmetric direction, where the pipeline denies a boot to a stage that then
    SSM-invokes onto a box it does not have — which FAILS a run rather than
    costing four minutes.
    """

    def test_the_sf_branch_is_exactly_the_derived_predicate(self, mod, sf_def):
        gate = sf_def["States"]["CheckSpotDispatchNeeded"]
        assert gate["Choices"][1] == mod.spot_dispatch_bypass_rule(sf_def), (
            "CheckSpotDispatchNeeded's bypass branch has drifted from "
            "scripts/weekly_sf_rerun.py::spot_dispatch_bypass_rule — regenerate "
            "it there rather than hand-editing the definition"
        )

    def test_the_passthrough_branch_and_default_are_unchanged(self, mod, sf_def):
        gate = sf_def["States"]["CheckSpotDispatchNeeded"]
        assert gate["Choices"][0] == {
            "Variable": "$.ec2_instance_id",
            "IsPresent": True,
            "Next": "NormalizeEc2InstanceId",
        }
        assert gate["Default"] == "DispatchWeeklyFreshnessSpot"
        assert len(gate["Choices"]) == 2

    def test_every_flag_in_the_branch_is_a_real_stage_flag(self, mod, sf_def):
        flags = mod.box_dispatch_flags(sf_def)
        assert set(flags) <= {s.flag for s in mod.STAGES}
        assert flags, "the box-stage set cannot be empty — this SF runs on a box"

    def test_skipping_every_box_stage_routes_past_the_dispatch(self, mod, sf_def):
        gate = sf_def["States"]["CheckSpotDispatchNeeded"]
        flags = {f: True for f in mod.box_dispatch_flags(sf_def)}
        assert _dispatch_gate_next(gate, flags) == mod.SPOT_DISPATCH_CONVERGENCE

    @pytest.mark.parametrize("held_back", range(8))
    def test_leaving_any_one_box_stage_unskipped_still_dispatches(
        self, mod, sf_def, held_back
    ):
        """The asymmetric direction, one stage at a time: a run that still has
        a box stage to execute must still get a box."""
        gate = sf_def["States"]["CheckSpotDispatchNeeded"]
        derived = mod.box_dispatch_flags(sf_def)
        if held_back >= len(derived):
            pytest.skip("fewer box stages than the parametrization covers")
        flags = {f: True for f in derived if f != derived[held_back]}
        assert _dispatch_gate_next(gate, flags) == "DispatchWeeklyFreshnessSpot", (
            f"{derived[held_back]} is unskipped — that stage SSM-invokes onto "
            "$.ec2_instance_id and the run would fail without a box"
        )

    def test_the_predicate_is_sound_against_the_definition_itself(self, mod, sf_def):
        """Not just 'the two files agree' — with the derived flags set, NO state
        that dereferences $.ec2_instance_id is reachable past the gate, and
        dropping any one of them puts at least one back."""
        derived = mod.box_dispatch_flags(sf_def)
        assert mod.box_states_needing_dispatch(
            sf_def, {f: True for f in derived}
        ) == set()
        for flag in derived:
            partial = {f: True for f in derived if f != flag}
            assert mod.box_states_needing_dispatch(sf_def, partial), (
                f"{flag} is redundant — box_dispatch_flags must not emit a "
                "conjunct that carries no coverage"
            )

    def test_a_present_ec2_instance_id_still_wins(self, mod, sf_def):
        """The rerun helper passes a still-live launcher box through verbatim;
        that branch must keep precedence over the new bypass."""
        gate = sf_def["States"]["CheckSpotDispatchNeeded"]
        flags = {f: True for f in mod.box_dispatch_flags(sf_def)}
        flags["ec2_instance_id"] = "i-abc"
        assert _dispatch_gate_next(gate, flags) == "NormalizeEc2InstanceId"

    def test_every_conjunct_is_emittable(self, mod, sf_def):
        """alpha-engine-config-I8167: every flag CheckSpotDispatchNeeded's
        bypass conjunction tests must be a flag derive_plan() can actually
        SET (emit_skip=True) — a conjunct backed only by an emit_skip=False
        stage reads as live in the definition but is dead code for every
        input this script derives. This is a direct assertion pinning
        box_dispatch_flags' own I8167 guard against the live definition, not
        just a unit test of the guard in isolation (see the two tests
        below for that)."""
        for flag in mod.box_dispatch_flags(sf_def):
            stage = mod.STAGES_BY_FLAG[flag]
            assert stage.emit_skip, (
                f"{flag} ({stage.name}) is in the bypass conjunction but "
                "its STAGES row carries emit_skip=False — no input "
                "derive_plan() emits can ever satisfy the bypass. Give the "
                "stage its own emit_skip=True row (skip_saturday_health_check "
                "did this for the health-check span) or prove it redundant."
            )

    def test_skip_saturday_health_check_is_emit_skip_true(self, mod):
        """Pins the specific I8167 fix: the health-check span's flag is
        emittable, replacing the old skip_post_eval conjunct (emit_skip=False,
        a deliberately DEPRECATED alias — see its STAGES row's note)."""
        assert mod.STAGES_BY_NAME["saturday_health_check"].emit_skip is True
        assert mod.STAGES_BY_NAME["post_eval"].emit_skip is False

    def test_a_non_emittable_sole_coverer_fails_the_build(
        self, mod, sf_def, monkeypatch
    ):
        """Regression guard reproducing the exact I8162 defect shape this
        issue fixes: a box-addressing span whose ONLY covering STAGES row
        carries emit_skip=False must fail box_dispatch_flags() loudly,
        never silently ship a dead conjunct. Simulated by removing the
        (emittable) saturday_health_check row from STAGES so skip_post_eval
        — emit_skip=False — becomes, once again, the sole flag the
        reachability minimization can find to cover the health-check span
        against the REAL, unmodified step_function.json."""
        trimmed = tuple(
            s for s in mod.STAGES if s.name != "saturday_health_check"
        )
        monkeypatch.setattr(mod, "STAGES", trimmed)
        monkeypatch.setattr(
            mod, "STAGES_BY_NAME", {s.name: s for s in trimmed}
        )
        monkeypatch.setattr(
            mod, "STAGES_BY_FLAG", {s.flag: s for s in trimmed}
        )
        with pytest.raises(SystemExit, match="emit_skip=False"):
            mod.box_dispatch_flags(sf_def)

    def test_a_helper_generated_director_only_recovery_satisfies_the_bypass(
        self, mod, sf_def
    ):
        """alpha-engine-config-I8167 closes-when: a recovery whose chain
        witnessed every box stage complete (everything but Director) derives
        an input that SATISFIES CheckSpotDispatchNeeded's bypass conjunction.
        Before this fix the bypass was unreachable from any input
        derive_plan() could produce — its sole health-check conjunct
        (skip_post_eval) carried emit_skip=False, so no derived input ever
        set it. Synthesized minimally: one stateEnteredEventDetails per
        witness state for each of the 8 box-conjunct stages, plus Director's
        own work state entered-but-not-completed (the recovery target)."""
        events = [
            {
                "type": "ExecutionStarted",
                "executionStartedEventDetails": {
                    "input": json.dumps({
                        "pipeline_role": "watch-rerun",
                        "run_date": "2026-08-24",
                    }),
                },
            },
            *[
                {"type": "TaskStateEntered", "stateEnteredEventDetails": {"name": n}}
                for n in [
                    "CheckSkipDataPhase1",               # morning_enrich witness
                    "ResearchPredictorParallel",          # data_phase1 witness
                    "CheckSkipRegimeRetrospectiveEval",   # rag_ingestion witness
                    "CheckSkipEvalJudge",                 # data_phase2 witness
                    "ResolveZooSpecs",                    # predictor_training witness
                    "CheckSkipPredictorBacktest",         # backtester witness
                    "CheckSkipPostEval",                  # evaluator witness
                    "RunScope",                           # saturday_health_check witness (I8167)
                    "Director",                           # director's WORK state: entered, unfinished
                ]
            ],
        ]
        plan = mod.derive_plan(events)
        assert "director" in plan.failed

        derived_box_flags = set(mod.box_dispatch_flags(sf_def))
        assert derived_box_flags <= set(plan.skip_flags), (
            f"missing box-conjunct flags: {derived_box_flags - set(plan.skip_flags)}"
        )
        assert all(plan.skip_flags[f] is True for f in derived_box_flags)
        # skip_saturday_health_check specifically — not the old, non-emittable
        # skip_post_eval conjunct — is what got emitted.
        assert plan.skip_flags.get("skip_saturday_health_check") is True

        gate = sf_def["States"]["CheckSpotDispatchNeeded"]
        assert _dispatch_gate_next(gate, plan.skip_flags) == mod.SPOT_DISPATCH_CONVERGENCE, (
            "a helper-generated Director-only recovery input must take the "
            "spot-dispatch bypass, not boot a box to re-run advisory, "
            "idempotent health checks"
        )
