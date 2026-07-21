"""Unit tests for scripts/weekly_sf_rerun.py (config#2277) + lockstep guards
pinning its declarative stage table against infrastructure/step_function.json.

Three recorded-shape execution-history fixtures (tests/fixtures/
weekly_sf_rerun/, synthesized from the REAL 2026-07-11 scheduled-run failure
history's event vocabulary):

- ``parallel_branch_failure``: branch A dies at RAGIngestion, branch B
  completes through the model zoo (the actual 2026-07-11 shape);
- ``tail_stage_failure``: Parity fails with everything through the
  portfolio-optimizer backtest completed — exercises the skip_backtester
  OVERSHOOT drop (its skip route jumps the failed stage's gate);
- ``early_failure``: DataPhase1 fails with only MorningEnrich completed.

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
            "skip_predictor_training",
        }
        assert plan.failed == ["rag_ingestion"]
        # lib-pin check completed but is deliberately NOT skipped
        assert "lib_pin_drift_check" in plan.completed
        assert "skip_lib_pin_drift_check" not in plan.skip_flags

    def test_tail_stage_failure_drops_backtester_overshoot(self, mod):
        plan = mod.derive_plan(_events("tail_stage_failure"))
        assert plan.failed == ["parity"]
        # skip_backtester completed but its skip route would bypass the
        # failed parity gate — must be DROPPED, loudly.
        assert "skip_backtester" not in plan.skip_flags
        assert any("skip_backtester DROPPED" in w for w in plan.warnings)
        assert set(plan.skip_flags) == {
            "skip_morning_enrich",
            "skip_data_phase1",
            "skip_rag_ingestion",
            "skip_regime_substrate",
            "skip_regime_retrospective_eval",
            # skip_research retired: alpha-engine-config-I2515 Phase B
            # removed the multi-agent Research state entirely.
            "skip_data_phase2",
            "skip_eval_judge",
            "skip_rationale_clustering",
            "skip_replay_concordance",
            "skip_counterfactual",
            "skip_aggregate_costs",
            "skip_predictor_training",
            "skip_predictor_backtest",
            "skip_portfolio_optimizer_backtest",
        }
        # the failed stage must never carry its own skip flag
        assert "skip_parity" not in plan.skip_flags

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
        # every warning path still names Scanner's unavoidable re-run
        assert any("Scanner" in w for w in plan.warnings)

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

    def test_refuses_to_skip_a_failed_stage(self, mod):
        """Anti-swallow guard: if the preserved original input carries a
        skip flag whose route would bypass a stage that FAILED, the helper
        must refuse rather than emit an input that silently skips it."""
        events = _events("tail_stage_failure")
        started = next(e for e in events if "executionStartedEventDetails" in e)
        inp = json.loads(started["executionStartedEventDetails"]["input"])
        inp["skip_backtester"] = True  # would jump the failed parity gate
        started["executionStartedEventDetails"]["input"] = json.dumps(inp)
        with pytest.raises(SystemExit, match="unreachable"):
            mod.derive_plan(events)


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
            "parity",
        )
        assert all_states["CheckSkipBacktester"]["Default"] == "Backtester"


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
