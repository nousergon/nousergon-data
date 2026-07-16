"""Pins the Friday-PM `shell_run` spine + KEYSTONE in the Saturday SF.

Origin: ROADMAP "Scheduled Friday-PM 'shell run' — automated full-fidelity
preflight of the Saturday SF" (P1, added 2026-05-16). The *prevention* half
of Saturday-SF reliability (the *containment* half — preflight-task-split —
shipped 2026-05-16 in data #249/#250). Motivated by the recent multi-week
Saturday-SF cascade history: a Saturday-fatal break is discovered only when
the unattended 02:00 PT Sat run fails, wasting the week's
research/training/backtest cycle.

#258 shipped the SPINE (pure-skip every workload via the existing
Choice-gated skip mechanism). The KEYSTONE (feat/sf-shell-run-keystone)
replaced pure-skip with dry EXECUTION for all but 5 documented exceptions.
The SKIP-EXCEPTION REWIRE (feat/sf-rewire-close-skip-exceptions) then
flipped those last 5 skip→dry, so the Friday run boots + exercises the
real bootstrap/import/lib-pin/transport paths for EVERY substantive
workload — ZERO skip-exceptions remain:

The SF is a STRICT SUPERSET of the pre-spine Saturday SF:
- A `CheckShellRun` Choice after `InitializeInput`. `shell_run` absent OR
  false → `CheckSkipMorningEnrich` (the pre-spine `InitializeInput.Next`),
  BYTE-IDENTICAL behaviour to today's real Saturday run. `InitializeInput`
  seeds the dry-path control vars at their NON-DRY identity values
  (`preflight_args=""`, `research_dry=false`, `data_phase2_dry=false`,
  `regime_action="produce"`) so every spot `States.Format` command string is
  char-for-char unchanged and every Lambda Payload behaviourally identical.
- `shell_run=true` → `ApplyShellRunDefaults`, a Pass that merges the
  dry-path control blob UNDER the execution input (user per-flag overrides
  still win), then → `CheckSkipMorningEnrich`. The 8 SPOT states boot + run
  dry via `preflight_args=" --preflight-only"` (the rewire added
  DriftDetection — data #261 exposed `--preflight-only` on
  spot_drift_detection.sh, converting it from a literal `commands` array to
  the same `commands.$`/`States.Format($.preflight_args)` Option-C
  mechanism); the LAMBDA states run dry via their handler's no-write dry
  flag: DataPhase2/RegimeSubstrate/RegimeRetrospectiveEval from
  the keystone (the multi-agent Research state was also dry from the
  keystone but was removed entirely by alpha-engine-config-I2515 Phase B;
  its replacements SignalsEnvelope/ChallengerShadow carry no dry-run
  signal, mirroring ThinkTankCoverage's own convention), PLUS the eval-judge chain
  (EvalJudgeSubmit{FirstSaturday,Weekly}/Poll/Process), RationaleClustering
  (research #202), ReplayConcordance + Counterfactual (backtester #225) —
  all reusing the canonical `$.research_dry` shell-run-dry signal via
  `dry_run_llm.$`. ZERO skip-exceptions are force-set. The #258
  Choice-gated skip_* mechanism is LEFT INTACT (still valid for targeted
  operator skips).
- A `CheckShellRunNotify` Choice before the success notify. shell_run
  absent/false → the unchanged `NotifyComplete`; shell_run=true →
  `NotifyShellRunComplete` (shell-run-tagged Subject, same SNS substrate).

This test catches regressions like:
- Someone re-points `InitializeInput.Next` straight to
  `CheckSkipMorningEnrich`, silently dropping the shell-run gate.
- Someone makes `CheckShellRun.Default` anything other than
  `CheckSkipMorningEnrich` (breaks the strict-superset / real-Saturday path).
- The `ApplyShellRunDefaults` merge order flips so user overrides lose.
- A spot `States.Format` command drifts so `preflight_args=""` no longer
  produces a byte-identical command (the real Saturday run would change).
- A documented-exception Lambda gets routed to an unverified dry path, or a
  verified-clean Lambda silently loses its dry flag.
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
# alpha-engine-config-I2544: the eval-judge chain's dry-path control-var
# wiring lives here now (lifted verbatim — Payload/Retry/Catch semantics
# preserved byte-for-byte).
_ADVISORY_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_advisory.json"
_CFN_PATH = (
    _REPO_ROOT / "infrastructure" / "cloudformation"
    / "alpha-engine-orchestration.yaml"
)

# The complete set of Choice-gated skip flags in the Saturday SF. The #258
# skip_* gates are LEFT INTACT by the keystone (they remain valid for
# targeted operator skips) — this constant + test_skip_gates_still_intact
# enforce that none were deleted.
_EXPECTED_SKIPS = {
    # config#1824: scheduled-weekly run-day gate operator bypass.
    "skip_weekly_run_day_gate",
    # Added 2026-06-08 (L4517 — preventive cross-repo lib-pin drift gate,
    # the first state after InitializeInput).
    "skip_lib_pin_drift_check",
    "skip_morning_enrich",
    "skip_data_phase1",
    "skip_rag_ingestion",
    "skip_regime_substrate",
    "skip_regime_retrospective_eval",
    # skip_research retired: alpha-engine-config-I2515 Phase B removed the
    # multi-agent Research state (and its CheckSkipResearch gate) entirely
    # — there is no longer a Lambda invocation to skip. SignalsEnvelope,
    # its load-bearing replacement, has no skip gate (mirrors Scanner's
    # own unconditional posture — it is now a same-day-freshness producer,
    # not an ad-hoc-rerun-optional step).
    "skip_data_phase2",
    # skip_eval_judge/skip_rationale_clustering/skip_replay_concordance/
    # skip_counterfactual/skip_aggregate_costs retired here:
    # alpha-engine-config-I2544 lifted the whole eval-judge chain (+
    # ReportCard/Director) into the async ne-weekly-advisory-pipeline
    # child SF — these gates no longer exist in THIS SF at all. See
    # test_sf_advisory_pipeline_wiring.py for their coverage there.
    "skip_predictor_training",
    # config#902: skip_drift_detection was removed — the DriftDetection state
    # (and its CheckSkipDriftDetection gate) were collapsed when drift was
    # bundled onto the PredictorTraining spot, so there is no gate to skip.
    "skip_backtester",
    # Added config#830 — give the weekly SF a Backtester→Evaluator-only mid-week
    # path (mode=backtest-eval) without a separate state machine. PredictorBacktest
    # and PortfolioOptimizerBacktest (L4472 split) previously had no skip gate, and
    # the post-eval tail (health checks/report-card/director) could not be stopped.
    "skip_predictor_backtest",
    "skip_portfolio_optimizer_backtest",
    "skip_parity",
    "skip_evaluator",
    "skip_post_eval",
}

# KEYSTONE + skip-exception rewire: the 8 SPOT workload states. Under
# shell_run they BOOT + run dry via preflight_args=" --preflight-only" (a
# States.Format suffix); with preflight_args="" (the real Saturday run) the
# command is byte-identical. DriftDetection joined this set in the
# skip-exception rewire — data #261 added --preflight-only to
# spot_drift_detection.sh, so it was converted from a literal `commands`
# array (hard-skipped under shell_run via skip_drift_detection) to the same
# commands.$/States.Format($.preflight_args) Option-C mechanism the keystone
# used for the other 7 spots.
# Maps state name → (mode token the {} immediately follows, log file).
_SPOT_STATES = {
    "MorningEnrich": (
        "bash infrastructure/spot_data_weekly.sh --morning-enrich-only",
        "/var/log/morning-enrich.log",
    ),
    "DataPhase1": (
        "bash infrastructure/spot_data_weekly.sh --phase1-only",
        "/var/log/data-weekly.log",
    ),
    "RAGIngestion": (
        "bash infrastructure/spot_data_weekly.sh --rag-only",
        "/var/log/rag-ingestion.log",
    ),
    "PredictorTraining": (
        "bash infrastructure/spot_train.sh --full-only",
        "/var/log/predictor-training.log",
    ),
    "Backtester": (
        "bash infrastructure/spot_backtest.sh --mode=param-sweep --no-pit-parity --skip-stages=parity,evaluator",
        "/var/log/backtester.log",
    ),
    "PredictorBacktest": (
        "bash infrastructure/spot_backtest.sh --mode=predictor-backtest --no-pit-parity --skip-stages=parity,evaluator",
        "/var/log/predictor-backtest.log",
    ),
    "PortfolioOptimizerBacktest": (
        "bash infrastructure/spot_backtest.sh --mode=portfolio-optimizer-backtest --no-pit-parity --skip-stages=parity,evaluator",
        "/var/log/portfolio-optimizer.log",
    ),
    "Parity": (
        "bash infrastructure/spot_backtest.sh --pit-parity-enabled=1 --skip-stages=backtest,evaluator",
        "/var/log/parity.log",
    ),
    "Evaluator": (
        "bash infrastructure/spot_backtest.sh --skip-stages=backtest,parity",
        "/var/log/evaluator.log",
    ),
    # config#902: DriftDetection was collapsed — drift is now bundled onto the
    # PredictorTraining spot (crucible-predictor spot_train.sh runs
    # monitoring.drift_detector after training succeeds). Its Friday
    # --preflight-only dry path folds into spot_train.sh --preflight-only, so
    # DriftDetection is no longer a standalone spot state here.
}

# KEYSTONE + skip-exception rewire: the LAMBDA states routed dry (NOT
# skipped) via an input-var ref so the absent path is behaviourally
# identical. DataPhase2/Regime* were dry from the keystone; the
# skip-exception rewire ADDED the eval-judge chain + rationale-clustering
# (research #202 added dry_run_llm) + replay-concordance + counterfactual
# (backtester #225 added dry_run_llm) — all reusing the canonical
# $.research_dry shell-run-dry signal (already true under shell_run / false
# on the real run). DriftDetection is NOT here — it is a SPOT state (see
# _SPOT_STATES) routed dry via --preflight-only, not a Lambda dry flag.
# alpha-engine-config-I2515 Phase B removed the multi-agent Research state
# entirely — its "Research" entry here is retired along with it.
# SignalsEnvelope/ChallengerShadow (its replacements) do NOT thread
# $.research_dry (their Payloads carry no dry-run signal, mirroring
# ThinkTankCoverage's own no-dry-flag convention), so neither is added here.
# state name → (Payload key carrying the dry flag, input var it references).
_DRY_LAMBDA_STATES = {
    "DataPhase2": ("dry_run.$", "$.data_phase2_dry"),
    "RegimeSubstrate": ("action.$", "$.regime_action"),
    "RegimeRetrospectiveEval": ("action.$", "$.regime_action"),
}

# alpha-engine-config-I2544: the eval-judge chain + agent-justification
# triple moved to the async advisory child SF (step_function_advisory.json)
# — same dry-flag wiring, verified against that file by
# TestAdvisoryByteIdenticalAbsentPath below instead of this file's `sf`.
_ADVISORY_DRY_LAMBDA_STATES = {
    "EvalJudgeSubmitFirstSaturday": ("dry_run_llm.$", "$.research_dry"),
    "EvalJudgeSubmitWeekly": ("dry_run_llm.$", "$.research_dry"),
    "EvalJudgePoll": ("dry_run_llm.$", "$.research_dry"),
    "EvalJudgeProcess": ("dry_run_llm.$", "$.research_dry"),
    "RationaleClustering": ("dry_run_llm.$", "$.research_dry"),
    "ReplayConcordance": ("dry_run_llm.$", "$.research_dry"),
    "Counterfactual": ("dry_run_llm.$", "$.research_dry"),
}

# Skip-exception rewire (this PR): ZERO skip-exceptions remain. The
# keystone's 5 documented hard-skips (skip_drift_detection / skip_eval_judge
# / skip_rationale_clustering / skip_replay_concordance / skip_counterfactual)
# were ALL flipped skip→dry — DriftDetection via the spot
# commands.$/States.Format(--preflight-only) mechanism (data #261), the 6
# eval Lambdas via dry_run_llm.$=$.research_dry (research #202 +
# backtester #225). ApplyShellRunDefaults force-sets NO skip_* flag now.
_KEYSTONE_SKIP_EXCEPTIONS: set[str] = set()

# Dry-path control vars + their NON-DRY identity values seeded by
# InitializeInput (so the absent path is byte-identical / behaviourally
# identical) and the DRY values ApplyShellRunDefaults overrides them with.
_CTRL_IDENTITY = {
    "preflight_args": "",
    "research_dry": False,
    "data_phase2_dry": False,
    "regime_action": "produce",
    "pipeline_label": "",
}
_CTRL_DRY = {
    "preflight_args": " --preflight-only",
    "research_dry": True,
    "data_phase2_dry": True,
    "regime_action": "dry_run",
    "pipeline_label": " Preflight",
}


def _eval_intrinsic_args(s: str) -> list[str]:
    """Split a top-level comma-separated ASL-intrinsic arg list, respecting
    single-quoted strings, nested parens, and \\' escapes."""
    args: list[str] = []
    depth = 0
    i = 0
    cur: list[str] = []
    inq = False
    while i < len(s):
        c = s[i]
        if inq:
            if c == "\\" and i + 1 < len(s) and s[i + 1] == "'":
                cur.append("'")
                i += 2
                continue
            if c == "'":
                inq = False
                cur.append(c)
                i += 1
                continue
            cur.append(c)
            i += 1
            continue
        if c == "'":
            inq = True
            cur.append(c)
            i += 1
            continue
        if c == "(":
            depth += 1
            cur.append(c)
            i += 1
            continue
        if c == ")":
            depth -= 1
            cur.append(c)
            i += 1
            continue
        if c == "," and depth == 0:
            args.append("".join(cur).strip())
            cur = []
            i += 1
            continue
        cur.append(c)
        i += 1
    if cur:
        args.append("".join(cur).strip())
    return args


def _eval_expr(e: str, ctx: dict):
    """Resolve the subset of ASL intrinsics the spot commands.$ use:
    string literals, $.var refs, States.Array(...), States.Format(...)."""
    e = e.strip()
    if e.startswith("'") and e.endswith("'"):
        return e[1:-1].replace("\\'", "'")
    if e.startswith("$."):
        return ctx[e[2:]]
    if e.startswith("States.Array("):
        inner = e[len("States.Array(") : -1]
        return [_eval_expr(a, ctx) for a in _eval_intrinsic_args(inner)]
    if e.startswith("States.Format("):
        inner = e[len("States.Format(") : -1]
        parts = _eval_intrinsic_args(inner)
        tmpl = _eval_expr(parts[0], ctx)
        subs = [_eval_expr(p, ctx) for p in parts[1:]]
        out: list[str] = []
        si = 0
        i = 0
        while i < len(tmpl):
            if tmpl[i : i + 2] == "{}":
                out.append(str(subs[si]))
                si += 1
                i += 2
            else:
                out.append(tmpl[i])
                i += 1
        return "".join(out)
    raise AssertionError(f"unhandled intrinsic in spot command: {e[:80]!r}")


def _resolve_spot_commands(state: dict, preflight_args: str) -> list[str]:
    """Resolve a spot state's commands.$ States.Array to the literal list,
    binding $.preflight_args (and $.run_date for the RUN_DATE export)."""
    p = state["Parameters"]["Parameters"]
    assert "commands.$" in p, (
        "spot state must use commands.$ States.Array (so the final entry "
        "can be a States.Format interpolating $.preflight_args)"
    )
    return _eval_expr(
        p["commands.$"],
        {"preflight_args": preflight_args, "run_date": "2026-05-18"},
    )


@pytest.fixture(scope="module")
def orig_spot_cmds() -> dict:
    """Frozen baseline RESOLVED spot command lists for byte-identicality.

    Captured at commit time into a committed fixture so the byte-identical
    proof is HERMETIC. The prior implementation shelled out to
    `git show origin/main:infrastructure/step_function.json` at test time,
    which fails (exit 128) in CI's shallow PR checkout where `origin/main`
    is not a local ref — that was the keystone CI failure.

    History:
    - Originally captured pre-keystone (#258) to prove the Friday-PM
      shell-run mechanism didn't change the real Saturday absent path.
    - **Regenerated 2026-05-22** as part of the inline-trap-to-lib-CLI
      lift (alpha-engine-lib PR #57). The pre-keystone form had a broken
      `'trap \\'aws s3 cp ...\\' EXIT'` inside `commands.$ States.Array`
      (ASL doesn't unescape `\\'` in arg strings — caught by the
      Friday-PM dry-pass). The baseline now reflects the lib-CLI form:
      `python -m krepis.ssm_log_capture run --slug X
      --log Y -- bash <launcher>`. The keystone's byte-identicality
      proof against the new baseline still holds (the absent path runs
      the same lib-CLI invocation with `preflight_args=""`).
    - **Regenerated 2026-07-03** as part of the `nousergon_lib.ssm_log_capture`
      → `krepis.ssm_log_capture` caller migration (config#1646). The
      `nousergon_lib` path became a guard-less re-export shim at lib
      v0.66.0: under `python -m` it exits 0 WITHOUT executing the inner
      command — the 2026-07-03 weekly ran zero EC2 workloads while
      reporting SUCCESS. `krepis.ssm_log_capture` is the canonical
      executable path; `test_ssm_log_capture_wrapper_executes.py` now
      proves executability (not just importability) in CI.

    Regenerate ONLY on a deliberate, reviewed change to a spot state's
    absent-path (`preflight_args=""`) command, by re-extracting the
    resolved spot commands from the new `origin/main` SF.
    """
    p = _REPO_ROOT / "tests" / "fixtures" / "sf_prekeystone_spot_commands.json"
    return json.loads(p.read_text())


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
        # 2026-05-27: L274 SF MutualExclusionGuard inserted a CheckMutexRole
        # Choice between InitializeInput and CheckShellRun. The strict-superset
        # property holds because CheckMutexRole.Default → CheckShellRun (the
        # bypass path that runs for any input without a cadence pipeline_role
        # in {daily, weekly, eod, shell-run}), and AcquireMutex.Next →
        # CheckShellRun (the cadence-role acquire path lands at the same
        # downstream state). See tests/test_sf_mutex_wiring.py for the full
        # mutex-chain contract.
        # 2026-06-08 (L4517): the lib-pin drift gate precedes the mutex; its
        # paths converge on CheckMutexRole (see test_sf_lib_pin_drift_wiring.py),
        # so the mutex→CheckShellRun superset property below is unchanged.
        # config#830: CheckRunMode (cadence preset) precedes the lib-pin gate;
        # its Default → CheckSkipLibPinDriftCheck, so the superset chain holds.
        assert states["InitializeInput"]["Next"] == "CheckWeeklyRunDayGate"
        # config#1824: run-day gate precedes CheckRunMode; bypass Default keeps chain.
        assert states["CheckWeeklyRunDayGate"]["Default"] == "CheckRunMode"
        assert states["CheckRunMode"]["Default"] == "CheckSkipLibPinDriftCheck"
        assert states["CheckMutexRole"]["Default"] == "CheckShellRun", (
            "Mutex bypass path must route to CheckShellRun so the shell-run "
            "chain remains a strict superset for non-cadence inputs"
        )
        assert states["AcquireMutex"]["Next"] == "CheckShellRun", (
            "Mutex acquire path must also land at CheckShellRun so the "
            "shell-run chain runs after a successful cadence acquire"
        )

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
        """The real Saturday SUCCESS email — now deep-links to the console
        pipeline-status page (config#856 push-on-transition revamp)."""
        nc = states["NotifyComplete"]
        assert nc["Resource"] == "arn:aws:states:::sns:publish"
        assert nc["Parameters"]["Subject"] == (
            "Alpha Engine Saturday Pipeline — SUCCESS"
        )
        assert nc["Parameters"]["Message"] == (
            "All steps completed successfully. "
            "View pipeline status: https://console.nousergon.ai/pipeline-status"
        )
        assert nc["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
        assert nc["ResultPath"] == "$.notify_result"
        assert nc["End"] is True

    def test_success_notify_gate_default_is_notify_complete(self, states):
        # config#2278: the real-run success edge now passes through the
        # gate-degraded completion Choice before NotifyComplete.
        assert states["CheckShellRunNotify"]["Default"] == "CheckGateDegradedNotify"
        assert states["CheckGateDegradedNotify"]["Default"] == "NotifyComplete"


class TestApplyShellRunDefaults:
    """shell_run=true ⇒ spots boot dry (--preflight-only), verified-clean
    Lambdas run dry, only the 5 documented-exception states hard-skip, and
    user per-flag overrides still win."""

    def _merge_expr(self, states) -> str:
        return states["ApplyShellRunDefaults"]["Parameters"]["merged.$"]

    def _blob(self, states) -> dict:
        expr = self._merge_expr(states)
        m = re.search(r"StringToJson\('(.+?)'\)", expr)
        assert m, "could not extract the embedded shell-run defaults blob"
        return json.loads(m.group(1))

    def test_pass_state_routes_into_existing_skip_chain(self, states):
        st = states["ApplyShellRunDefaults"]
        assert st["Type"] == "Pass"
        assert st["OutputPath"] == "$.merged"
        assert st["Next"] == "CheckSkipMorningEnrich"

    def test_shell_defaults_win_over_current_state(self, states):
        """States.JsonMerge($, shellDefaults, false) — shellDefaults MUST be
        the 2nd arg so the dry-path control vars actually take effect under
        shell_run=true.

        Why the prior order broke (2026-05-22 evening incident): the
        previous shape was ``States.JsonMerge(shellDefaults, $, false)``
        with the rationale "user input ($) must win for per-flag overrides
        like {shell_run: true, skip_backtester: true}." That rationale
        confused two distinct things: ``$`` at ApplyShellRunDefaults entry
        is NOT raw user input — InitializeInput has already merged its
        defaults blob ``{preflight_args: "", research_dry: false,
        data_phase2_dry: false, regime_action: "produce"}`` into ``$``.
        So ``$ wins`` meant the NON-DRY identity defaults won over the dry
        shellDefaults. Result: every shell_run=true execution silently fell
        back to a full-fat real Saturday run instead of --preflight-only.

        Caught the morning after the 2026-05-22 Friday-PM dry-pass
        post-trap-fix verification: MorningEnrich passed (trap fix worked),
        DataPhase1 spot booted and started collecting short interest +
        universe returns + fundamentals at full power instead of running
        preflight-and-exiting.

        Skip-flag overrides are unaffected by this fix: shellDefaults
        contains NO skip_* keys, so a user-passed
        ``{shell_run: true, skip_backtester: true}`` survives the merge
        unchanged and the downstream Choice gate still skips Backtester.
        Only the 4 keystone control vars (preflight_args / research_dry /
        data_phase2_dry / regime_action) are forced to dry when shell_run.
        That's the actual semantic intent — passing shell_run=true means
        you want dry mode for ALL keystone control vars; partial-override
        within the dry-control-var set was a footgun, not a feature.
        """
        expr = self._merge_expr(states)
        assert expr.startswith("States.JsonMerge($,States.StringToJson("), (
            f"merge expr must start with 'States.JsonMerge($,States.StringToJson(' "
            f"so shellDefaults (the 2nd arg) wins over $. got: {expr[:80]!r}"
        )
        assert expr.endswith(",false)")

    def test_shell_defaults_set_dry_control_vars(self, states):
        """ApplyShellRunDefaults must set every dry-path control var to its
        DRY value (preflight_args=' --preflight-only' with LEADING space;
        research/data_phase2 dry true; regime_action 'dry_run')."""
        blob = self._blob(states)
        for k, v in _CTRL_DRY.items():
            assert blob.get(k) == v, (
                f"shell-run blob {k}={blob.get(k)!r}, expected {v!r}"
            )
        assert blob["preflight_args"].startswith(" "), (
            "preflight_args MUST carry its leading space INSIDE the var so "
            'the absent-path "" yields a byte-identical spot command'
        )

    def test_shell_defaults_force_set_ZERO_skip_exceptions(self, states):
        """Skip-exception rewire: ApplyShellRunDefaults must force-set NO
        skip_* flag at all. The keystone's 5 documented hard-skips were ALL
        flipped skip→dry (DriftDetection → spot --preflight-only; the 6 eval
        Lambdas → dry_run_llm.$=$.research_dry). A regression re-introducing
        any force-set skip_* would silently demote a substantive workload
        back to pure-skip — defeating the whole point of the shell run."""
        blob = self._blob(states)
        skip_keys = {k for k in blob if k.startswith("skip_")}
        assert skip_keys == _KEYSTONE_SKIP_EXCEPTIONS, (
            "ApplyShellRunDefaults must force-set ZERO skip_* (the "
            "skip-exception rewire flipped all 5 keystone exceptions to "
            f"dry); leaked skip_* keys: {skip_keys}"
        )
        assert skip_keys == set(), (
            f"ZERO skip-exceptions must remain; got {skip_keys}"
        )
        # NO workload state may be force-skipped — every substantive task
        # now runs DRY under shell_run (spots via --preflight-only, Lambdas
        # via their no-write dry flag). This explicitly includes the 5
        # previously-excepted states (the rewire's whole purpose).
        for forbidden in (
            "skip_morning_enrich",
            "skip_data_phase1",
            "skip_rag_ingestion",
            "skip_predictor_training",
            "skip_backtester",
            "skip_parity",
            "skip_evaluator",
            "skip_research",
            "skip_data_phase2",
            "skip_regime_substrate",
            "skip_regime_retrospective_eval",
            # The ex-keystone skip-exceptions — now run DRY, not skipped.
            # (config#902 removed skip_drift_detection entirely — the
            # DriftDetection state was collapsed onto the PredictorTraining
            # spot, so there is no drift state to run dry OR skip.)
            "skip_eval_judge",
            "skip_rationale_clustering",
            "skip_replay_concordance",
            "skip_counterfactual",
        ):
            assert forbidden not in blob, (
                f"{forbidden} must NOT be force-skipped — the skip-exception "
                "rewire runs that state DRY under shell_run, not skipped"
            )

    def test_skip_gates_still_intact(self, states):
        """The keystone LEAVES the #258 Choice-gated skip_* mechanism intact
        (still valid for targeted operator skips). None deleted."""
        gated = _all_skip_gate_flags(states)
        assert gated == _EXPECTED_SKIPS, (
            "a Choice-gated skip_* was deleted/added; the keystone keeps "
            f"the #258 skip mechanism intact: {gated ^ _EXPECTED_SKIPS}"
        )

    def test_initialize_input_seeds_nondry_identity_defaults(self, states):
        """InitializeInput must seed every dry-control var at its NON-DRY
        identity value so the absent path is byte/behaviour-identical and
        the spot States.Format / Lambda Payload .$ refs always resolve."""
        expr = states["InitializeInput"]["Parameters"]["merged.$"]
        m = re.search(r"StringToJson\('(\{[^']*?\})'\)", expr)
        assert m, "could not extract InitializeInput defaults blob"
        blob = json.loads(m.group(1))
        for k, v in _CTRL_IDENTITY.items():
            assert blob.get(k) == v, (
                f"InitializeInput {k}={blob.get(k)!r}, expected identity "
                f"{v!r} (absent path must be byte-identical)"
            )
        # The pre-keystone run_date / sns_topic_arn defaults must survive.
        assert "sns_topic_arn" in blob
        assert expr.endswith(",$$.Execution.Input,false)")


class TestByteIdenticalAbsentPath:
    """The CORE invariant (#258 established it, the keystone must preserve
    it): shell_run absent/false ⇒ every spot command string is char-for-
    char identical to the pre-keystone (origin/main) SF, and every Lambda
    Payload is behaviourally identical."""

    def _state(self, sf: dict, name: str) -> dict:
        if name in sf["States"]:
            return sf["States"][name]
        # Parallel-branch states (Research/DataPhase2/PredictorTraining).
        par = sf["States"]["ResearchPredictorParallel"]
        for br in par["Branches"]:
            if name in br["States"]:
                return br["States"][name]
        raise KeyError(name)

    @pytest.mark.parametrize("name", sorted(_SPOT_STATES))
    def test_spot_command_byte_identical_when_preflight_args_empty(
        self, sf, orig_spot_cmds, name
    ):
        new_cmds = _resolve_spot_commands(
            self._state(sf, name), preflight_args=""
        )
        orig_cmds = orig_spot_cmds[name]
        assert new_cmds == orig_cmds, (
            f"{name}: keystone changed the absent-path command — the real "
            f"Saturday run would differ.\norig={orig_cmds[-1]!r}\n"
            f"new ={new_cmds[-1]!r}"
        )

    @pytest.mark.parametrize("name", sorted(_SPOT_STATES))
    def test_spot_command_carries_preflight_only_under_shell_run(
        self, sf, name
    ):
        """Under shell_run (preflight_args=" --preflight-only"), the final
        command line must propagate --preflight-only AFTER the launcher's
        mode token (the {} sits immediately after, leading-space-bearing
        var produces exactly one separating space).

        2026-05-22 reshape: the final line moved from
            ``{token} --preflight-only 2>&1 | tee {log}``
        to
            ``/home/ec2-user/alpha-engine-dashboard/.venv/bin/python -m
              krepis.ssm_log_capture run --slug X --log Y --
              {token} --preflight-only``
        as part of the inline-trap-to-lib-CLI lift (alpha-engine-lib PR #57).
        The lib CLI internalizes tee + S3-ship; --preflight-only still
        sits immediately after the launcher's mode token so the
        orthogonal-modifier shape is preserved.
        """
        token, log = _SPOT_STATES[name]
        slug = Path(log).stem
        cmds = _resolve_spot_commands(
            self._state(sf, name), preflight_args=" --preflight-only"
        )
        final = cmds[-1]
        expected = (
            "/home/ec2-user/alpha-engine-dashboard/.venv/bin/python "
            "-m krepis.ssm_log_capture run "
            f"--slug {slug} --log {log} -- "
            f"{token} --preflight-only"
        )
        assert final == expected, final
        assert "  --preflight-only" not in final, "double space — bad join"

    @pytest.mark.parametrize(
        "name,payload_key,ref", sorted(
            (n, k, r) for n, (k, r) in _DRY_LAMBDA_STATES.items()
        )
    )
    def test_dry_lambda_payload_references_control_var(
        self, sf, name, payload_key, ref
    ):
        """Verified-clean Lambdas route their dry flag via a $.var ref, so
        the absent path (control var at non-dry identity) is behaviourally
        identical and shell_run flips it to the dry value."""
        st = self._state(sf, name)
        payload = st["Parameters"]["Payload"]
        assert payload.get(payload_key) == ref, (
            f"{name}.Payload[{payload_key}] must be {ref} (so the dry flag "
            f"follows the control var); got {payload.get(payload_key)!r}"
        )


class TestAdvisoryByteIdenticalAbsentPath:
    """alpha-engine-config-I2544: same dry-flag-follows-control-var
    invariant as TestByteIdenticalAbsentPath, re-pointed at the eval-judge
    chain's new home (the advisory child SF's wrapper Parallel branch).
    research_dry is threaded into the child's input by the parent SF's
    StartAdvisoryPipeline dispatch (see
    test_sf_research_predictor_parallel_wiring.py), so the same absent-path
    identity argument holds."""

    @pytest.fixture(scope="class")
    def advisory_sf(self) -> dict:
        return json.loads(_ADVISORY_SF_PATH.read_text())

    def _state(self, advisory_sf: dict, name: str) -> dict:
        states = advisory_sf["States"]
        if name in states:
            return states[name]
        wrapper = states["AdvisoryPipelineWrapper"]
        inner = wrapper["Branches"][0]["States"]
        if name in inner:
            return inner[name]
        raise KeyError(name)

    @pytest.mark.parametrize(
        "name,payload_key,ref", sorted(
            (n, k, r) for n, (k, r) in _ADVISORY_DRY_LAMBDA_STATES.items()
        )
    )
    def test_dry_lambda_payload_references_control_var(
        self, advisory_sf, name, payload_key, ref
    ):
        st = self._state(advisory_sf, name)
        payload = st["Parameters"]["Payload"]
        assert payload.get(payload_key) == ref, (
            f"{name}.Payload[{payload_key}] must be {ref} (so the dry flag "
            f"follows the control var); got {payload.get(payload_key)!r}"
        )


class TestConsolidatedNotify:
    def test_substrate_check_routes_to_notify_gate(self, states):
        # alpha-engine-config-I2544: ReportCard + Director were LIFTED out
        # of this SF's tail into the async ne-weekly-advisory-pipeline child
        # SF (dispatched earlier via StartAdvisoryPipeline, right after
        # DataPhase2 — see test_sf_research_predictor_parallel_wiring.py).
        # The substrate check's Success edge (and its Degraded fallback)
        # now converge DIRECTLY on the notify gate — there is nothing left
        # in this SF's tail to route through. Their own ReportCard->Director
        # ->notify wiring (unchanged, byte-for-byte preserved) is covered by
        # test_sf_advisory_pipeline_wiring.py::TestReportCardAndDirectorWiring
        # and its preflight-dry-run coverage by
        # test_sf_advisory_pipeline_wiring.py::
        # TestReportCardAndDirectorWiring::test_report_card_and_director_payload_shape_unchanged.
        # config#2276: the substrate poll resolves to a terminal status first.
        assert (
            states["WaitForWeeklySubstrateHealthCheck"]["Next"]
            == "CheckSubstrateHealthCheckStatus"
        )
        substrate_success = next(
            r
            for r in states["CheckSubstrateHealthCheckStatus"]["Choices"]
            if r.get("StringEquals") == "Success"
        )
        assert substrate_success["Next"] == "CheckShellRunNotify"
        assert states["SubstrateHealthCheckDegraded"]["Next"] == "CheckShellRunNotify"
        assert "ReportCard" not in states
        assert "Director" not in states

    def test_advisory_tail_dry_run_coverage_relocated(self, states):
        """ROADMAP L4504 / alpha-engine-config-I2544: the ReportCard/
        Director dry-execute-on-preflight invariant (both payloads thread
        dry_run.$=$.research_dry so the Friday preflight runs a no-write /
        no-Opus probe rather than polluting the shared carry-over ledger)
        is unchanged, but ReportCard/Director no longer live in THIS SF —
        see test_sf_advisory_pipeline_wiring.py::TestReportCardAndDirectorWiring
        ::test_report_card_and_director_payload_shape_unchanged for the
        re-pointed assertion."""
        for state_name in ("ReportCard", "Director"):
            assert state_name not in states, (
                f"{state_name} still present at top level — alpha-engine-"
                "config-I2544 lifted it into step_function_advisory.json"
            )

    def test_shell_run_notify_reuses_sns_substrate(self, states):
        """NotifyShellRunComplete surfaces the user-facing 'Saturday
        Preflight Pipeline' label so a green Friday dry-pass is
        distinguishable from a real Saturday SUCCESS in the operator's
        inbox. The 'shell_run' / 'shell-run' wording is internal
        mechanism-language (the SF Input flag); 'Preflight' is the
        consumer-facing rename (2026-05-23) — alerts must use the
        consumer-facing label.
        """
        st = states["NotifyShellRunComplete"]
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        assert st["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
        subject = st["Parameters"]["Subject"]
        assert "Saturday Preflight Pipeline" in subject, (
            f"Subject must surface 'Saturday Preflight Pipeline' label "
            f"(not internal 'shell run' wording); got: {subject!r}"
        )
        assert "SUCCESS" in subject
        assert st["ResultPath"] == "$.notify_result"
        assert st["End"] is True

    def test_shell_run_notify_gate_fires_on_true(self, states):
        choices = states["CheckShellRunNotify"]["Choices"]
        assert len(choices) == 1
        assert choices[0]["Next"] == "NotifyShellRunComplete"
        conds = choices[0]["And"]
        assert {c["Variable"] for c in conds} == {"$.shell_run"}

    def test_handle_failure_subject_is_parameterized_by_pipeline_label(self, states):
        """2026-05-23 rename arc: HandleFailure's SNS Subject + Message
        must use ``States.Format`` against ``$.pipeline_label`` so a
        Preflight Pipeline failure surfaces 'Saturday Preflight Pipeline
        — FAILED' (shell_run=true → $.pipeline_label=' Preflight') vs a
        real Saturday SF failure surfacing 'Saturday Pipeline — FAILED'
        ($.pipeline_label='' from InitializeInput defaults). The
        ``pipeline_label`` token is seeded at InitializeInput + overridden
        at ApplyShellRunDefaults — see _CTRL_IDENTITY / _CTRL_DRY.
        """
        st = states["HandleFailure"]
        subject = st["Parameters"].get("Subject.$") or ""
        assert "$.pipeline_label" in subject, (
            f"HandleFailure Subject must reference $.pipeline_label so "
            f"Preflight failures surface a distinct label; got: {subject!r}"
        )
        assert "States.Format" in subject
        assert "Saturday" in subject
        assert "FAILED" in subject
        # The hardcoded literal 'Subject' field must NOT coexist with
        # 'Subject.$' — exactly one of them sets the value, the .$ form
        # wins per ASL semantics.
        assert "Subject" not in st["Parameters"] or (
            st["Parameters"].get("Subject") is None
        )
        # Same for Message.$ — both should be parameterized.
        message = st["Parameters"].get("Message.$") or ""
        assert "$.pipeline_label" in message, (
            "HandleFailure Message must also surface the Preflight label"
        )

    def test_initialize_input_seeds_pipeline_label_at_identity(self, states):
        """The new ``pipeline_label`` control var must default to "" in
        InitializeInput so the real Saturday run's HandleFailure produces
        a byte-identical Subject ('Alpha Engine Saturday Pipeline — FAILED',
        unchanged from pre-rename)."""
        expr = states["InitializeInput"]["Parameters"]["merged.$"]
        m = re.search(r"StringToJson\('(\{[^']*?\})'\)", expr)
        assert m, "could not extract InitializeInput defaults blob"
        blob = json.loads(m.group(1))
        assert blob.get("pipeline_label") == "", (
            f"InitializeInput must seed pipeline_label='' (real Saturday "
            f"identity); got {blob.get('pipeline_label')!r}"
        )

    def test_apply_shell_run_defaults_overrides_pipeline_label(self, states):
        """ApplyShellRunDefaults must override pipeline_label to ' Preflight'
        so HandleFailure renders 'Saturday Preflight Pipeline — FAILED'
        under shell_run=true."""
        expr = states["ApplyShellRunDefaults"]["Parameters"]["merged.$"]
        # Extract the shellDefaults blob from JsonMerge($, defaults, false)
        m = re.search(r"StringToJson\('(\{[^']*?\})'\)", expr)
        assert m, "could not extract ApplyShellRunDefaults blob"
        blob = json.loads(m.group(1))
        assert blob.get("pipeline_label") == " Preflight", (
            f"ApplyShellRunDefaults must override pipeline_label to "
            f"' Preflight' (leading space — same convention as "
            f"preflight_args); got {blob.get('pipeline_label')!r}"
        )


class TestHappyPathTraversal:
    """End-to-end traversal of the deterministic gates (CheckShellRun +
    every CheckSkip<State>). Models a Task/Wait/status-Choice as "the
    workload RUNS, then control proceeds past it" (status checks resolve to
    their success edge on a green run). Asserts the skip-exception-rewire
    semantics: under shell_run EVERY workload gate falls through (its state
    RUNS dry — including DriftDetection, flipped skip→dry), ZERO
    skip-exceptions remain, and the run still reaches
    NotifyShellRunComplete; absent shell_run it is the pre-keystone path."""

    def _trace_main(self, sf, states, shell_run: bool) -> tuple:
        """Returns (visited_state_order, skipped_workload_set). A workload
        that RUNS is VISITED (appears in order); a skipped workload is NOT
        (its CheckSkip gate jumps past it)."""
        # Under shell_run ApplyShellRunDefaults force-sets ONLY the 5
        # documented-exception skips; all other workload gates fall through
        # to run the (dry) state.
        skips = set(_KEYSTONE_SKIP_EXCEPTIONS) if shell_run else set()
        skipped_workloads: set[str] = set()
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
                success_edge = None
                failed_guard = False
                for c in st.get("Choices", []):
                    conds = c.get("And") or c.get("Or") or [c]
                    vars_ = [
                        cc.get("Variable", "").replace("$.", "")
                        for cc in conds
                        if "Variable" in cc
                    ]
                    if vars_ and set(vars_) == {"shell_run"}:
                        if shell_run:
                            taken = c["Next"]
                            break
                        continue
                    if vars_ and all(v.startswith("skip_") for v in vars_):
                        if all(v in skips for v in vars_):
                            # CheckSkip<X>: skip-true → Next (past X);
                            # Default is the workload X itself, now skipped.
                            skipped_workloads.add(st.get("Default"))
                            taken = c["Next"]
                            break
                        continue
                    # Status-check Choice on a GREEN happy-path trace:
                    eqs = {
                        cc.get("StringEquals")
                        for cc in conds
                        if "StringEquals" in cc
                    }
                    if eqs & {"Success", "OK", "SKIPPED"}:
                        # success-continuation edge
                        success_edge = success_edge or c["Next"]
                    if eqs & {"FAILED", "ERROR"}:
                        # a FAILED-guard Choice (CheckBranchOutcomes shape):
                        # on a green run it is NOT taken → Default proceeds.
                        failed_guard = True
                    # InProgress/Pending wait-loop edges: ignored (the poll
                    # resolves to Success on a green run).
                if taken is None:
                    if success_edge is not None:
                        taken = success_edge
                    elif failed_guard:
                        taken = st.get("Default")
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
        return order, skipped_workloads

    def test_shell_run_true_runs_every_workload_dry_zero_skips(
        self, sf, states
    ):
        order, skipped = self._trace_main(sf, states, shell_run=True)
        assert order[-1] == "[END]"
        assert "ApplyShellRunDefaults" in order
        assert "NotifyShellRunComplete" in order
        assert "NotifyComplete" not in order
        # Skip-exception rewire: ZERO main-thread workload states are
        # skipped — every CheckSkip gate falls through so the (dry) state is
        # VISITED. (config#902: DriftDetection is no longer on this trace — it
        # was collapsed onto the PredictorTraining spot, which lives inside the
        # Parallel; its dry path is now spot_train.sh --preflight-only.)
        # (Research/DataPhase2/eval
        # chain/PredictorTraining live inside the Parallel and aren't on
        # this main-thread trace; their dry-routing is asserted by
        # TestByteIdenticalAbsentPath +
        # test_shell_defaults_force_set_ZERO_skip_exceptions.)
        # config#885: Scanner/RAGIngestion/RegimeSubstrate/
        # RegimeRetrospectiveEval were ALSO relocated INTO the Parallel's
        # Branch A (so PredictorTraining forks parallel to them after
        # DataPhase1), so they too are off this main-thread trace now —
        # their dry-routing is covered by the same in-branch dry assertions.
        for ran_dry in (
            "MorningEnrich",
            "DataPhase1",
            "Backtester",
            "PredictorBacktest",
            "PortfolioOptimizerBacktest",
            "Parity",
            "Evaluator",
        ):
            assert ran_dry in order, (
                f"{ran_dry} was NOT visited under shell_run — the rewire "
                "runs it DRY (visited), not skip (jumped past)"
            )
            assert ran_dry not in skipped, (
                f"{ran_dry} was skipped under shell_run — the rewire runs "
                "it DRY"
            )
        # ZERO skip-exceptions remain — nothing is jumped past.
        assert skipped == set(), (
            f"skip-exception rewire leaves nothing skipped; got {skipped}"
        )
        # Health/substrate checks DO still run (the bootstrap smoke).
        assert "SaturdayHealthCheck" in order
        assert "WeeklySubstrateHealthCheck" in order

    def test_shell_run_absent_is_pre_keystone_path(self, sf, states):
        # 2026-05-27: L274 mutex chain inserted CheckMutexRole between
        # InitializeInput and CheckShellRun. With no pipeline_role on the
        # input (the trace's default), CheckMutexRole.Default routes to
        # CheckShellRun — same downstream chain as pre-mutex, with one
        # extra Choice in the visited order.
        order, skipped = self._trace_main(sf, states, shell_run=False)
        assert "ApplyShellRunDefaults" not in order
        assert "NotifyShellRunComplete" not in order
        assert not skipped, "nothing skipped when shell_run absent"
        # 2026-06-08 (L4517): the lib-pin drift gate is now the first hop after
        # InitializeInput; with no skip flag + no drift, its skip/check/gate
        # Defaults converge on CheckMutexRole — same downstream chain, three
        # extra states in the visited order.
        # config#830: CheckRunMode (cadence preset) sits between InitializeInput
        # and the lib-pin gate; with no `mode` on the input it takes its Default
        # to CheckSkipLibPinDriftCheck — one extra Choice in the visited order.
        # config#1824: the run-day gate is the first hop; a role-less input
        # takes its Default straight to CheckRunMode — one extra Choice.
        # config#693 (L4595): the pipeline-contract preflight gate is now
        # composed directly after LibPinDriftGate's pass-through (no drift ->
        # PipelineContractCheck -> PipelineContractGate -> CheckMutexRole on no
        # violation) — two extra states in the visited order.
        assert order[: order.index("CheckSkipMorningEnrich") + 2] == [
            "InitializeInput",
            "CheckWeeklyRunDayGate",
            "CheckRunMode",
            "CheckSkipLibPinDriftCheck",
            "LibPinDriftCheck",
            "LibPinDriftGate",
            "PipelineContractCheck",
            "PipelineContractGate",
            "CheckMutexRole",
            "CheckShellRun",
            "CheckSkipMorningEnrich",
            "MorningEnrich",
        ]


class TestFridayCronRuleRetired:
    """The fixed-time Friday cron rule (alpha-engine-friday-shell-run) was
    RETIRED 2026-05-29 (ROADMAP L4055), superseded by the event-driven
    alpha-engine-eod-success-friday-shell-trigger Lambda (#282). These guard
    against its re-introduction — the cron's three failure modes (no-fire on
    Friday EOD failure, late-rerun blindness, StopTradingInstance race) are
    exactly what the event-driven path eliminates. The shell_run SF gate it
    used to feed is unchanged and still covered by the rest of this file."""

    @pytest.fixture(scope="class")
    def cfn_text(self) -> str:
        return _CFN_PATH.read_text()

    def test_cron_rule_resource_removed(self, cfn_text):
        assert "FridayShellRunTrigger:" not in cfn_text, (
            "The Friday cron rule was retired (L4055) — the event-driven "
            "Lambda is the sole shell-run trigger. Do not re-add the CFN rule."
        )
        assert "Name: alpha-engine-friday-shell-run" not in cfn_text
        assert "cron(45 20 ? * FRI *)" not in cfn_text
