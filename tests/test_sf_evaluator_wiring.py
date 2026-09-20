"""Pins the Evaluator state wiring in the Saturday Step Functions JSON.

The Evaluator state was split from the consolidated Backtester state on
2026-05-07 (plan: alpha-engine-docs/private/evaluator-split-260507.md)
for failure isolation, per-stage email, and independent CloudWatch
heartbeats. This test pins the split topology so a future operator
doesn't accidentally reroute Backtester success straight back to
CheckSkipEvalJudge (the pre-split shape) or merge the two states again
without a deliberate ROADMAP item.

**Split again 2026-08-11** — alpha-engine-config-I3112 deliverable 3, Brian
design ruling 2026-07-20. The single ``Evaluator`` state became
``EvaluatorDiagnostics -> EvaluatorOptimize``, each with its own
executionTimeout, poll budget, Catch path and ssm_log_capture slug. What the
bundle cost, from the 2026-07-20 incident arc: one shared 3600s ceiling
SIGKILLed the whole thing mid-work (watch-rerun-2026-07-18-10) and the killed
phase wrote no duration marker, so even post-mortem attribution of the hour was
impossible; an hour of ``CheckEvaluatorStatus`` never revealed which internal
phase was running; and a replay could only replay everything.

The two halves hand off through S3 rather than through process memory
(deliverable 2, ``evaluate_handoff.py``): the diagnostics half writes a
snapshot, the optimize half reads it. Without that, ``--mode optimize``
standalone starts with an empty diagnostics dict and three optimizers silently
skip — which is why the SF split had to wait for the handoff to exist.

Distinct from test_sf_eval_judge_wiring.py: that file pins the
LLM-as-judge Lambda chain (Haiku/Sonnet rubric scoring); this one pins
the spot-based evaluate.py state (per-signal grading + optimizer
auto-apply).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


# ── State presence ────────────────────────────────────────────────────────

# The two halves and the per-half machinery each one owns. Parameterising on
# this rather than asserting the pair by hand means a third half (or a rename)
# cannot land with only one of its eight states wired.
HALVES = ("Diagnostics", "Optimize")


class TestStatesPresent:
    def test_the_merged_state_is_gone(self, states):
        """The old single state must not linger alongside the split.

        A leftover ``Evaluator`` would still be reachable by name from a
        hand-edited execution input or a rerun script, running BOTH halves in
        one process under one ceiling — the exact shape this split removed.
        """
        for gone in ("Evaluator", "WaitForEvaluator", "CheckEvaluatorStatus",
                     "EvaluatorWait", "EvaluatorPollWait",
                     "InitEvaluatorPollCount", "MergeEvaluatorPollCount",
                     "ExtractEvaluatorError"):
            assert gone not in states, (
                f"{gone} survived the config-I3112 split — the merged state "
                f"and the split states must never coexist"
            )

    @pytest.mark.parametrize("half", HALVES)
    def test_all_states_exist_for_each_half(self, states, half):
        for name in (
            f"Evaluator{half}",
            f"InitEvaluator{half}PollCount",
            f"WaitForEvaluator{half}",
            f"CheckEvaluator{half}Status",
            f"Evaluator{half}Wait",
            f"Evaluator{half}PollWait",
            f"MergeEvaluator{half}PollCount",
            f"ExtractEvaluator{half}Error",
        ):
            assert name in states, f"missing SF state: {name}"

    def test_skip_gate_exists(self, states):
        assert "CheckSkipEvaluator" in states


# ── Skip gate ─────────────────────────────────────────────────────────────


class TestSkipEvaluator:
    def test_skip_flag_bypasses_to_health_check(self, states):
        """{"skip_evaluator": true} bypasses BOTH halves, not just the first.

        The flag predates the split and its contract is "no evaluate.py this
        run". A skip that landed on EvaluatorOptimize would run the optimizer
        half against whatever snapshot happened to be in S3 from an earlier
        date — worse than either running or skipping.
        """
        skip = states["CheckSkipEvaluator"]
        choice = skip["Choices"][0]
        conds = choice.get("And") or [choice]
        assert any(c.get("Variable") == "$.skip_evaluator" for c in conds)
        assert choice["Next"] == "CheckSkipPostEval"

    def test_default_runs_the_diagnostics_half_first(self, states):
        assert states["CheckSkipEvaluator"]["Default"] == "EvaluatorDiagnostics"


class TestSkipBacktesterRoutesThroughEvaluatorGate:
    def test_skip_backtester_reaches_evaluator_gate(self, states):
        # alpha-engine-config-I11103: two hops now, via
        # MarkParityVerdictUnknownByCadence. The name of this test is the
        # contract — REACHES the evaluator gate — so it is asserted as a walk.
        cur, hops = states["CheckSkipBacktester"]["Choices"][0]["Next"], 0
        while cur != "CheckSkipEvaluator" and hops < 5:
            cur = states[cur]["Next"]
            hops += 1
        assert cur == "CheckSkipEvaluator"


# ── Task contract, per half ───────────────────────────────────────────────


class TestEvaluatorTasks:
    @pytest.mark.parametrize("half", HALVES)
    def test_invokes_ssm_send_command(self, states, half):
        assert (
            states[f"Evaluator{half}"]["Resource"]
            == "arn:aws:states:::aws-sdk:ssm:sendCommand"
        )

    @pytest.mark.parametrize("half,flag", [("Diagnostics", "diagnostics"),
                                           ("Optimize", "optimize")])
    def test_command_invokes_dedicated_script_with_its_own_half(
        self, states, half, flag
    ):
        """Each state runs spot_evaluator.sh pinned to ITS half.

        Both halves invoking the same launcher with no --eval-half would run
        --mode all twice: the optimize half would redo the 230s diagnostics
        phase and then apply configs off its own fresh copy, so the S3 handoff
        would exist and be ignored. The flag is what makes the split real
        rather than cosmetic.
        """
        from tests.sf_command_utils import extract_commands
        cmds = extract_commands(states[f"Evaluator{half}"])
        spot_cmd = next(c for c in cmds if "spot_evaluator.sh" in c)
        assert f"--eval-half={flag}" in spot_cmd
        # alpha-engine-config-I4442/I4497 SF cutover (2026-08-09,
        # crucible-backtester#631): dedicated launcher, no stage-multiplexing
        # flag; the monolith stays on disk only as the rollback path.
        assert "--skip-stages" not in spot_cmd
        assert "--no-pit-parity" not in spot_cmd
        assert not any("spot_backtest.sh" in c for c in cmds), (
            f"Evaluator{half} must not fall back to the shared monolith launcher."
        )

    @pytest.mark.parametrize("half,slug", [("Diagnostics", "evaluator-diagnostics"),
                                           ("Optimize", "evaluator-optimize")])
    def test_each_half_writes_its_own_log(self, states, half, slug):
        """Separate slug + log path per half.

        Both halves run on their own spot instance and both would otherwise
        write /var/log/evaluator.log under slug `evaluator`, landing in the
        same s3://.../_ssm_logs/evaluator/{date}/ prefix — where the only way
        to tell which half produced a log would be to read it. The split
        exists to make attribution cheap; sharing the slug would give the
        states back their opacity.
        """
        from tests.sf_command_utils import extract_commands
        cmds = extract_commands(states[f"Evaluator{half}"])
        spot_cmd = next(c for c in cmds if "spot_evaluator.sh" in c)
        assert f"--slug {slug} " in spot_cmd
        assert f"/var/log/{slug}.log" in spot_cmd

    def test_slugs_and_logs_are_distinct_between_halves(self, states):
        from tests.sf_command_utils import extract_commands
        cmds = [next(c for c in extract_commands(states[f"Evaluator{h}"])
                     if "spot_evaluator.sh" in c) for h in HALVES]
        assert cmds[0] != cmds[1]

    @pytest.mark.parametrize("half,timeout", [("Diagnostics", 1800),
                                              ("Optimize", 1200)])
    def test_timeout_is_sized_from_measurement(self, states, half, timeout):
        """Per-half ceilings, derived in infrastructure/sf_budgets.py.

        The merged state carried 7200s against a MEASURED 482s stage
        (CheckSkipEvaluator 06:06:03 -> CheckSkipPostEval 06:14:06 on the
        2026-08-08 succeeded execution) — 6.7% utilisation, from an explicitly
        unmeasured 6.9 s/ticker estimate that over-predicted by ~15x. The exact
        values are pinned against the budget table by test_sf_budgets.py; this
        asserts the JSON carries them and keeps the +60s SF wrapper convention.
        """
        st = states[f"Evaluator{half}"]
        assert st["Parameters"]["Parameters"]["executionTimeout"] == [str(timeout)]
        assert st["Parameters"]["TimeoutSeconds"] == timeout
        assert st["TimeoutSeconds"] == timeout + 60

    def test_the_split_did_not_widen_the_stage(self, states):
        """Both halves together must fit inside the old single ceiling."""
        total = sum(states[f"Evaluator{h}"]["Parameters"]["TimeoutSeconds"]
                    for h in HALVES)
        assert total <= 7200, (
            f"the two halves sum to {total}s, more than the 7200s the merged "
            f"state held — a decomposition must not become a budget increase"
        )

    @pytest.mark.parametrize("half", HALVES)
    def test_retry_mirrors_backtester_posture(self, states, half):
        # config#2279: the declared spot-stage gold ladder (4+2 jittered);
        # the exact shape is pinned centrally in
        # test_sf_retry_ladder_convention.py.
        def _sig(state):
            return [
                {k: v for k, v in rule.items() if k != "Comment"}
                for rule in state["Retry"]
            ]
        assert _sig(states[f"Evaluator{half}"]) == _sig(states["Backtester"])
        retry = states[f"Evaluator{half}"]["Retry"][0]
        assert retry["MaxAttempts"] == 4
        assert retry["JitterStrategy"] == "FULL"

    @pytest.mark.parametrize("half", HALVES)
    def test_catch_routes_to_normalize_failure(self, states, half):
        # Evaluator failure halts the pipeline (unlike eval-judge which is
        # observability-only). The optimizer auto-apply contract means a
        # silent evaluator failure could leave stale configs in production —
        # fail loud. config#1819: through NormalizeFailureContext, the single
        # chokepoint in front of HandleFailure.
        catch = states[f"Evaluator{half}"]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "NormalizeFailureContext"

    @pytest.mark.parametrize("half,var", [("Diagnostics", "evaluator_diagnostics"),
                                          ("Optimize", "evaluator_optimize")])
    def test_each_half_owns_a_distinct_result_path(self, states, half, var):
        """Distinct ResultPaths, or the second sendCommand overwrites the
        first's CommandId and the second poll loop polls the wrong command."""
        assert states[f"Evaluator{half}"]["ResultPath"] == f"$.{var}_result"


# ── Ordering: diagnostics strictly before optimize ────────────────────────


class TestHalfOrdering:
    def test_diagnostics_success_runs_optimize(self, states):
        """The producer of the S3 snapshot must complete before its consumer.

        Not a style point: --mode optimize reads the snapshot the diagnostics
        half writes, and on a miss it degrades to an empty dict — three
        optimizers skip silently. Running them concurrently, or optimize
        first, converts a hard ordering requirement into a race whose losing
        outcome is quiet.
        """
        chk = states["CheckEvaluatorDiagnosticsStatus"]
        success = next(c for c in chk["Choices"]
                       if c.get("StringEquals") == "Success")
        assert success["Next"] == "EvaluatorOptimize"

    def test_optimize_success_exits_to_the_post_eval_tail(self, states):
        chk = states["CheckEvaluatorOptimizeStatus"]
        success = next(c for c in chk["Choices"]
                       if c.get("StringEquals") == "Success")
        assert success["Next"] == "CheckSkipPostEval"
        # alpha-engine-config-I8167: the tail gate now defaults one hop
        # downstream, to the new health-check-only skip gate — which itself
        # defaults to SaturdayHealthCheck on a normal run.
        assert states["CheckSkipPostEval"]["Default"] == "CheckSkipSaturdayHealthCheck"
        assert states["CheckSkipSaturdayHealthCheck"]["Default"] == "SaturdayHealthCheck"


# ── Poll loop, per half ───────────────────────────────────────────────────


class TestEvaluatorPollLoops:
    @pytest.mark.parametrize("half,var", [("Diagnostics", "evaluator_diagnostics"),
                                          ("Optimize", "evaluator_optimize")])
    def test_dispatch_seeds_its_own_poll_budget(self, states, half, var):
        # alpha-engine-config-I5687: dispatch goes through the poll-budget
        # seed before the first poll, mirroring DataPhase2/ThinkTank.
        assert states[f"Evaluator{half}"]["Next"] == f"InitEvaluator{half}PollCount"
        seed = states[f"InitEvaluator{half}PollCount"]
        assert seed["Next"] == f"WaitForEvaluator{half}"
        assert seed["ResultPath"] == f"$.{var}_polls"
        assert seed["Result"] == 0

    @pytest.mark.parametrize("half,var", [("Diagnostics", "evaluator_diagnostics"),
                                          ("Optimize", "evaluator_optimize")])
    def test_poll_reads_its_own_command_id(self, states, half, var):
        params = states[f"WaitForEvaluator{half}"]["Parameters"]
        assert params["CommandId.$"] == f"$.{var}_result.Command.CommandId"
        assert states[f"WaitForEvaluator{half}"]["Next"] == f"CheckEvaluator{half}Status"

    @pytest.mark.parametrize("half,cap", [("Diagnostics", 36), ("Optimize", 24)])
    def test_poll_budget_is_derived_from_this_half_timeout(self, states, half, cap):
        """cap = ceil(timeout / 60s poll interval * 1.2 slack).

        Inheriting the merged state's cap=144 would give the optimize half a
        4-hour poll budget behind a 20-minute ceiling — a bound that can never
        bind, which is the defect I5687 exists to prevent.
        """
        chk = states[f"CheckEvaluator{half}Status"]
        bounded = next(c for c in chk["Choices"] if "And" in c)
        numeric = next(cond for cond in bounded["And"]
                       if "NumericLessThan" in cond)
        assert numeric["NumericLessThan"] == cap
        timeout = states[f"Evaluator{half}"]["Parameters"]["TimeoutSeconds"]
        assert cap == -(-timeout // 60) * 12 // 10

    @pytest.mark.parametrize("half", HALVES)
    def test_in_progress_loops_to_this_half_wait(self, states, half):
        chk = states[f"CheckEvaluator{half}Status"]
        bounded = next(c for c in chk["Choices"] if "And" in c)
        or_block = next(cond["Or"] for cond in bounded["And"] if "Or" in cond)
        assert {c["StringEquals"] for c in or_block} == {"InProgress", "Pending"}
        assert bounded["Next"] == f"Evaluator{half}Wait"

    @pytest.mark.parametrize("half", HALVES)
    def test_default_extracts_this_half_error(self, states, half):
        # config#6938: routed through this half's liveness gate, which
        # separates a reclaimed launcher from an Evaluator failure. Assert the
        # route reaches this half's normalizer — never the other half's.
        gate = states[f"CheckEvaluator{half}Status"]["Default"]
        assert gate == f"Evaluator{half}LivenessGate"
        assert states[gate]["Default"] == f"ExtractEvaluator{half}Error"

    @pytest.mark.parametrize("half,var", [("Diagnostics", "evaluator_diagnostics"),
                                          ("Optimize", "evaluator_optimize")])
    def test_wait_increments_and_loops_back_within_its_own_half(
        self, states, half, var
    ):
        # I5687: Wait increments the counter (Pass), sleeps, merges the
        # counter back, then returns to THIS half's poll — never the other's.
        assert states[f"Evaluator{half}Wait"]["Next"] == f"Evaluator{half}PollWait"
        assert (states[f"Evaluator{half}Wait"]["Parameters"]["polls.$"]
                == f"States.MathAdd($.{var}_polls, 1)")
        assert (states[f"Evaluator{half}PollWait"]["Next"]
                == f"MergeEvaluator{half}PollCount")
        merge = states[f"MergeEvaluator{half}PollCount"]
        assert merge["InputPath"] == f"$.{var}_poll_count.polls"
        assert merge["ResultPath"] == f"$.{var}_polls"
        assert merge["Next"] == f"WaitForEvaluator{half}"


# ── Failure normalization, per half ──────────────────────────────────────


class TestExtractEvaluatorErrors:
    @pytest.mark.parametrize("half,var", [("Diagnostics", "evaluator_diagnostics"),
                                          ("Optimize", "evaluator_optimize")])
    def test_phase_label_names_the_half_that_failed(self, states, half, var):
        """The whole point of the split is attribution.

        A shared phase label of "Evaluator" would put both halves' failures in
        the same bucket on the alert and in the flow-doctor record, which is
        the opacity the split removes.
        """
        params = states[f"ExtractEvaluator{half}Error"]["Parameters"]
        assert params["phase"] == f"Evaluator{half}"
        assert params["source"] == f"CheckEvaluator{half}Status.Default"
        assert params["poll.$"] == f"$.{var}_poll"

    def test_phase_labels_are_distinct(self, states):
        labels = {states[f"ExtractEvaluator{h}Error"]["Parameters"]["phase"]
                  for h in HALVES}
        assert len(labels) == len(HALVES)

    @pytest.mark.parametrize("half", HALVES)
    def test_routes_through_the_normalize_chokepoint(self, states, half):
        # config#1819: NormalizeFailureContext, not HandleFailure directly.
        assert (states[f"ExtractEvaluator{half}Error"]["Next"]
                == "NormalizeFailureContext")
