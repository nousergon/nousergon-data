"""Pins the Friday-PM `shell_run` spine in the Saturday SF.

Origin: ROADMAP "Scheduled Friday-PM 'shell run' — automated full-fidelity
preflight of the Saturday SF" (P1, added 2026-05-16). The *prevention* half
of Saturday-SF reliability (the *containment* half — preflight-task-split —
shipped 2026-05-16 in data #249/#250). Motivated by the recent multi-week
Saturday-SF cascade history: a Saturday-fatal break is discovered only when
the unattended 02:00 PT Sat run fails, wasting the week's
research/training/backtest cycle.

The spine is a STRICT SUPERSET of the pre-spine Saturday SF:
- A `CheckShellRun` Choice after `InitializeInput`. `shell_run` absent OR
  false → `CheckSkipMorningEnrich` (the pre-spine `InitializeInput.Next`),
  BYTE-IDENTICAL behaviour to today's real Saturday run.
- `shell_run=true` → `ApplyShellRunDefaults`, a Pass that merges every
  `skip_*` flag = true UNDER the execution input (user per-flag overrides
  still win), then → `CheckSkipMorningEnrich`. Every workload state already
  has a Choice-gated `skip_*`, so the whole workload no-ops via the EXISTING
  skip mechanism (no new dry paths added in the spine).
- A `CheckShellRunNotify` Choice before the success notify. shell_run
  absent/false → the unchanged `NotifyComplete`; shell_run=true →
  `NotifyShellRunComplete` (shell-run-tagged Subject, same SNS substrate).

This test catches regressions like:
- Someone re-points `InitializeInput.Next` straight to
  `CheckSkipMorningEnrich`, silently dropping the shell-run gate.
- Someone makes `CheckShellRun.Default` anything other than
  `CheckSkipMorningEnrich` (breaks the strict-superset / real-Saturday path).
- The `ApplyShellRunDefaults` merge order flips so user overrides lose, or a
  `skip_*` flag is dropped from the defaults blob (a workload state would
  then RUN under shell_run — a side-effecting Saturday workload on a Friday).
- `NotifyComplete` is mutated (the real Saturday SUCCESS email changes).
- The Friday EventBridge rule is shipped ENABLED, or without shell_run=true,
  or pointed at a different SF.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"
_CFN_PATH = (
    _REPO_ROOT / "infrastructure" / "cloudformation"
    / "alpha-engine-orchestration.yaml"
)

# The complete set of Choice-gated skip flags in the Saturday SF. If a new
# workload state is added with a new skip_* flag, it MUST be added to
# ApplyShellRunDefaults too (else that state RUNS under shell_run) — this
# constant + test_shell_defaults_cover_every_skip_gate enforce that.
_EXPECTED_SKIPS = {
    "skip_morning_enrich",
    "skip_data_phase1",
    "skip_rag_ingestion",
    "skip_regime_substrate",
    "skip_regime_retrospective_eval",
    "skip_research",
    "skip_data_phase2",
    "skip_eval_judge",
    "skip_rationale_clustering",
    "skip_replay_concordance",
    "skip_counterfactual",
    "skip_predictor_training",
    "skip_drift_detection",
    "skip_backtester",
    "skip_parity",
    "skip_evaluator",
}


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


def _all_skip_gate_flags(states: dict) -> set[str]:
    """Walk every Choice (incl. Parallel branches) and collect each
    `$.skip_*` Variable it gates on."""
    found: set[str] = set()

    def walk(st: dict) -> None:
        for v in st.values():
            if v.get("Type") == "Choice":
                for c in v.get("Choices", []):
                    conds = c.get("And") or c.get("Or") or [c]
                    for cc in conds:
                        var = cc.get("Variable", "")
                        if var.startswith("$.skip_"):
                            found.add(var[2:])
            if v.get("Type") == "Parallel":
                for b in v["Branches"]:
                    walk(b["States"])

    walk(states)
    return found


class TestStatePresence:
    @pytest.mark.parametrize(
        "name",
        ["CheckShellRun", "ApplyShellRunDefaults",
         "CheckShellRunNotify", "NotifyShellRunComplete"],
    )
    def test_spine_state_exists(self, states, name):
        assert name in states, f"{name} missing — shell-run spine incomplete"


class TestStrictSuperset:
    """shell_run absent/false ⇒ byte-identical to the pre-spine Saturday run."""

    def test_initialize_input_routes_to_shell_run_gate(self, states):
        assert states["InitializeInput"]["Next"] == "CheckShellRun"

    def test_initialize_input_merge_expr_unchanged(self, states):
        # The run_date / sns_topic_arn defaults-under-input merge must be
        # untouched (a regression here would corrupt every real run).
        expr = states["InitializeInput"]["Parameters"]["merged.$"]
        assert expr.startswith("States.JsonMerge(States.JsonMerge(")
        assert "run_date" in expr and "sns_topic_arn" in expr
        assert expr.endswith(",$$.Execution.Input,false)")

    def test_check_shell_run_default_is_pre_spine_target(self, states):
        # Pre-spine InitializeInput.Next was CheckSkipMorningEnrich; the
        # Default of the new gate MUST be exactly that → real Saturday run
        # (no shell_run input) is unchanged.
        assert states["CheckShellRun"]["Default"] == "CheckSkipMorningEnrich"

    def test_check_shell_run_only_fires_on_true_present(self, states):
        choices = states["CheckShellRun"]["Choices"]
        assert len(choices) == 1
        conds = choices[0]["And"]
        kinds = {
            (c["Variable"], "IsPresent" in c, c.get("BooleanEquals"))
            for c in conds
        }
        assert ("$.shell_run", True, None) in kinds  # IsPresent: true
        assert ("$.shell_run", False, True) in kinds  # BooleanEquals: true
        assert choices[0]["Next"] == "ApplyShellRunDefaults"

    def test_notify_complete_is_byte_identical(self, states):
        """The real Saturday SUCCESS email must not change."""
        nc = states["NotifyComplete"]
        assert nc["Resource"] == "arn:aws:states:::sns:publish"
        assert nc["Parameters"]["Subject"] == (
            "Alpha Engine Saturday Pipeline — SUCCESS"
        )
        assert nc["Parameters"]["Message"] == (
            "All steps completed successfully. Check dashboard for results."
        )
        assert nc["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
        assert nc["ResultPath"] == "$.notify_result"
        assert nc["End"] is True

    def test_success_notify_gate_default_is_notify_complete(self, states):
        assert states["CheckShellRunNotify"]["Default"] == "NotifyComplete"


class TestApplyShellRunDefaults:
    """shell_run=true ⇒ every workload state is no-op'd via the EXISTING
    skip mechanism, and user per-flag overrides still win."""

    def _merge_expr(self, states) -> str:
        return states["ApplyShellRunDefaults"]["Parameters"]["merged.$"]

    def test_pass_state_routes_into_existing_skip_chain(self, states):
        st = states["ApplyShellRunDefaults"]
        assert st["Type"] == "Pass"
        assert st["OutputPath"] == "$.merged"
        assert st["Next"] == "CheckSkipMorningEnrich"

    def test_user_input_wins_over_shell_defaults(self, states):
        # States.JsonMerge(defaults, $, false) — $ (current state, carrying
        # the user input) MUST be the 2nd arg so an explicit
        # {"shell_run": true, "skip_research": false} still runs Research.
        expr = self._merge_expr(states)
        assert expr.startswith("States.JsonMerge(States.StringToJson(")
        assert expr.endswith(",$,false)"), (
            "user input ($) must be the 2nd JsonMerge arg so explicit "
            "per-flag overrides win over the shell-run skip defaults"
        )

    def test_shell_defaults_blob_is_valid_json_all_true(self, states):
        expr = self._merge_expr(states)
        m = re.search(r"StringToJson\('(.+?)'\)", expr)
        assert m, "could not extract the embedded skip-defaults JSON blob"
        blob = json.loads(m.group(1))
        assert set(blob) == _EXPECTED_SKIPS, set(blob) ^ _EXPECTED_SKIPS
        assert all(v is True for v in blob.values()), blob

    def test_shell_defaults_cover_every_skip_gate(self, states):
        """Every Choice-gated skip_* in the SF must be force-true'd by
        shell_run — otherwise that workload state RUNS on a Friday dry-pass
        (a side-effecting Saturday workload firing on a Friday)."""
        gated = _all_skip_gate_flags(states)
        assert gated == _EXPECTED_SKIPS, (
            "skip-gate flags drifted from the shell-run defaults blob: "
            f"{gated ^ _EXPECTED_SKIPS}. Add the new flag to "
            "ApplyShellRunDefaults (and _EXPECTED_SKIPS) so the new "
            "workload state no-ops under shell_run."
        )


class TestConsolidatedNotify:
    def test_substrate_check_routes_to_notify_gate(self, states):
        assert (
            states["WaitForWeeklySubstrateHealthCheck"]["Next"]
            == "CheckShellRunNotify"
        )

    def test_shell_run_notify_reuses_sns_substrate(self, states):
        st = states["NotifyShellRunComplete"]
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        assert st["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
        assert "SHELL RUN" in st["Parameters"]["Subject"]
        assert st["ResultPath"] == "$.notify_result"
        assert st["End"] is True

    def test_shell_run_notify_gate_fires_on_true(self, states):
        choices = states["CheckShellRunNotify"]["Choices"]
        assert len(choices) == 1
        assert choices[0]["Next"] == "NotifyShellRunComplete"
        conds = choices[0]["And"]
        assert {c["Variable"] for c in conds} == {"$.shell_run"}


class TestHappyPathTraversal:
    """End-to-end: with shell_run=true the SF must reach
    NotifyShellRunComplete having visited NO workload Task; with shell_run
    absent it must be the pre-spine path (visits MorningEnrich etc.)."""

    def _trace_main(self, sf, states, shell_run: bool) -> list[str]:
        inp = {"shell_run": True} if shell_run else {}
        # ApplyShellRunDefaults force-sets all skips when shell_run=true.
        skips = set(_EXPECTED_SKIPS) if shell_run else set()
        order: list[str] = []
        seen: set[str] = set()
        cur = sf["StartAt"]
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            order.append(cur)
            st = states[cur]
            t = st.get("Type")
            if t == "Choice":
                taken = None
                for c in st.get("Choices", []):
                    conds = c.get("And") or [c]
                    vars_ = [
                        cc.get("Variable", "").replace("$.", "")
                        for cc in conds
                        if "Variable" in cc
                    ]
                    # shell_run gate
                    if vars_ == ["shell_run", "shell_run"] or vars_ == [
                        "shell_run"
                    ]:
                        if inp.get("shell_run") is True:
                            taken = c["Next"]
                            break
                        continue
                    # skip_* gate
                    if vars_ and all(v in _EXPECTED_SKIPS for v in vars_):
                        if all(v in skips for v in vars_):
                            taken = c["Next"]
                            break
                        continue
                    # status checks (Success edge) — not exercised on the
                    # all-skip happy path; fall through to Default
                cur = taken if taken else st.get("Default")
            elif t == "Parallel":
                cur = st.get("Next")
            elif t in ("Succeed", "Fail"):
                break
            else:
                if st.get("End"):
                    order.append("[END]")
                    break
                cur = st.get("Next")
        return order

    def test_shell_run_true_reaches_shell_notify_no_workload(
        self, sf, states
    ):
        order = self._trace_main(sf, states, shell_run=True)
        assert order[-1] == "[END]"
        assert "NotifyShellRunComplete" in order
        assert "NotifyComplete" not in order
        # No side-effecting workload Task visited on the main thread.
        for forbidden in (
            "MorningEnrich",
            "DataPhase1",
            "RAGIngestion",
            "RegimeSubstrate",
            "Backtester",
            "Parity",
            "Evaluator",
        ):
            assert forbidden not in order, (
                f"{forbidden} ran under shell_run — must be skipped"
            )
        # Health/substrate checks DO still run (the bootstrap smoke).
        assert "SaturdayHealthCheck" in order
        assert "WeeklySubstrateHealthCheck" in order

    def test_shell_run_absent_is_pre_spine_path(self, sf, states):
        order = self._trace_main(sf, states, shell_run=False)
        # No shell_run ⇒ Default at CheckShellRun ⇒ run the real workload.
        assert "ApplyShellRunDefaults" not in order
        assert "NotifyShellRunComplete" not in order
        assert order[: order.index("CheckSkipMorningEnrich") + 2] == [
            "InitializeInput",
            "CheckShellRun",
            "CheckSkipMorningEnrich",
            "MorningEnrich",
        ]


class TestFridayEventBridgeRule:
    """The Friday rule must be shipped DISABLED, target the SAME Saturday
    SF, and pass shell_run=true (additive observability, not a backstop)."""

    @pytest.fixture(scope="class")
    def cfn_text(self) -> str:
        return _CFN_PATH.read_text()

    def test_rule_block_present(self, cfn_text):
        assert "FridayShellRunTrigger:" in cfn_text
        assert "Name: alpha-engine-friday-shell-run" in cfn_text

    def test_rule_shipped_disabled(self, cfn_text):
        block = cfn_text.split("FridayShellRunTrigger:", 1)[1].split(
            "WeekdayTrigger:", 1
        )[0]
        assert "State: DISABLED" in block, (
            "Friday shell-run rule MUST ship DISABLED — zero-risk merge; "
            "Brian enables it deliberately (fail-loud / no-backstop design)."
        )
        assert "State: ENABLED" not in block

    def test_rule_targets_same_saturday_sf_with_shell_run(self, cfn_text):
        block = cfn_text.split("FridayShellRunTrigger:", 1)[1].split(
            "WeekdayTrigger:", 1
        )[0]
        assert "!Ref SaturdayPipeline" in block, (
            "must reuse the existing Saturday SF — NOT a parallel SF"
        )
        assert "!Ref EventBridgeSfnRoleArn" in block  # same StartExecution grant
        assert '"shell_run": true' in block

    def test_friday_schedule_after_eod_before_saturday(self, cfn_text):
        block = cfn_text.split("FridayShellRunTrigger:", 1)[1].split(
            "WeekdayTrigger:", 1
        )[0]
        # 21:30 UTC Fri = 14:30 PT (PDT) — after Friday EOD SF (~1:25 PT),
        # ~11.5h before the real Sat 09:00 UTC firing.
        assert "cron(30 21 ? * FRI *)" in block
