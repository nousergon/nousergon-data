"""Pins the v1 weekday/EOD data phase OFF the ae-trading box — and, since the
decoupled cutover (alpha-engine-config-I11269), off the v1 SFs altogether.

History: config#1767 (Phase 2) moved the weekday pre-open data phase
(MorningEnrich + MorningArcticAppend) and the EOD post-close data phase
(PostMarketData + PostMarketArcticAppend) off the always-on trading box onto an
ephemeral spot box launched by alpha-engine-data-spot-dispatcher. I11269
(Brian's ruling (b), 2026-09-21) removes that spot launch/poll/retry block from
both v1 SFs: the standalone nousergon-data-collection schedules own the
collection, and each v1 SF only WAITS, bounded, for the run manifests of the
units it reads (WaitForCollectionManifests, I11264).

This test pins, structurally (no live infra needed):
  1. The spot launch/poll/retry states are GONE from both SFs, and no state in
     either invokes the data-spot dispatcher — a v1 SF that still launched a
     collection box would double-write against the standalone schedule.
  2. The on-trading data-phase SSM states stay gone (config#1767 deliverable #2).
  3. FAILURE ISOLATION (LOAD-BEARING, config#1767 deliverable #4, unchanged in
     spirit): a not-ready collection — budget exhausted, the producer settled
     not-ok, or every probe raising — routes through the SAME
     ExtractDataSpotError -> SetDataSpotDegradedFlag ->
     PublishDataSpotFailureImmediate normalizer to the CONTINUE path (the
     predictor/daemon path on weekday; the reconcile/snapshot/stop path on
     EOD), NEVER to HandleFailure/FailExecution. The normalizer's names and
     $.data_spot_error / $.degraded_summary contracts are kept deliberately:
     CheckSkipEODReconcile and CheckDegradedOutcome read them.
  4. The dispatcher Lambda's workload map still runs the SAME
     weekly_collector.py entrypoints (it is the standalone collection's
     launcher now; M0 data contract preserved).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DAILY = _REPO_ROOT / "infrastructure" / "step_function_daily.json"
_EOD = _REPO_ROOT / "infrastructure" / "step_function_eod.json"
_DISPATCHER = _REPO_ROOT / "infrastructure" / "lambdas" / "data-spot-dispatcher"

_LAMBDA_INVOKE = "arn:aws:states:::lambda:invoke"
_SSM_SEND = "arn:aws:states:::aws-sdk:ssm:sendCommand"
_DISPATCHER_FN = "alpha-engine-data-spot-dispatcher"
_PROBE_FN = "alpha-engine-collection-readiness-probe"


@pytest.fixture(scope="module")
def daily() -> dict:
    return json.loads(_DAILY.read_text())["States"]


@pytest.fixture(scope="module")
def eod() -> dict:
    return json.loads(_EOD.read_text())["States"]


# ── Terminal HALT states each SF must NEVER reach from a data-phase failure ───
_HALT = {"HandleFailure", "FailExecution", "ForceStopInstance"}

# The bounded readiness-wait block, identical in shape in both SFs.
_WAIT_BLOCK = [
    "InitCollectionReadinessPoll", "SeedCollectionReadiness",
    "WaitForCollectionManifests", "CheckCollectionReadiness",
    "CheckCollectionReadinessBudget", "CollectionReadinessPollWait",
    "IncrementCollectionReadinessPoll", "ExtractCollectionNotReadyError",
    "ExtractDataSpotError", "SetDataSpotDegradedFlag", "PublishDataSpotFailureImmediate",
]


def _all_targets(state: dict) -> list[str]:
    """Every Next/Default/Choice.Next/Catch.Next target of a state."""
    t: list[str] = []
    for k in ("Next", "Default"):
        if k in state:
            t.append(state[k])
    for c in state.get("Choices", []):
        if "Next" in c:
            t.append(c["Next"])
    for c in state.get("Catch", []):
        if "Next" in c:
            t.append(c["Next"])
    return t


def _invokes(state: dict, fn: str) -> bool:
    return (
        state.get("Resource") == _LAMBDA_INVOKE
        and state.get("Parameters", {}).get("FunctionName") == fn
    )


def _reachable_from(states: dict, start: str, stop: str) -> set[str]:
    """Every state reachable from `start` without passing through `stop`."""
    seen: set[str] = set()
    todo = [start]
    while todo:
        name = todo.pop()
        if name in seen or name == stop:
            continue
        seen.add(name)
        todo.extend(_all_targets(states[name]))
    return seen


class _WaitBlockIsolation:
    """Shared fail-open pins for the readiness wait; subclasses bind the SF."""

    SF: str
    CONTINUE: str
    SKIP_GATE: str

    @pytest.fixture
    def sf(self, request):
        return request.getfixturevalue(self.SF)

    def test_the_wait_polls_the_readiness_probe_not_the_dispatcher(self, sf):
        st = sf["WaitForCollectionManifests"]
        assert _invokes(st, _PROBE_FN)
        assert "action" not in st["Parameters"]["Payload"]
        assert "workload" not in st["Parameters"]["Payload"]

    def test_a_raising_probe_spends_a_poll_it_never_halts(self, sf):
        catch = sf["WaitForCollectionManifests"]["Catch"]
        assert [c["Next"] for c in catch] == ["CheckCollectionReadinessBudget"]
        assert catch[0]["ErrorEquals"] == ["States.ALL"]
        # The raise must not overwrite the seeded verdict the normalizer reads.
        assert catch[0]["ResultPath"] != "$.collection_readiness"

    def test_ready_continues_and_settled_not_ok_fails_open(self, sf):
        ch = sf["CheckCollectionReadiness"]["Choices"]
        assert ch[0]["Next"] == self.CONTINUE
        assert ch[1]["Next"] == "ExtractCollectionNotReadyError"
        assert sf["CheckCollectionReadiness"]["Default"] == "CheckCollectionReadinessBudget"

    def test_budget_exhaustion_fails_open(self, sf):
        st = sf["CheckCollectionReadinessBudget"]
        assert st["Choices"][0]["Next"] == "ExtractCollectionNotReadyError"
        assert sf["ExtractCollectionNotReadyError"]["Next"] == "ExtractDataSpotError"

    def test_error_normalizer_continues_not_halts(self, sf):
        # Names and ResultPaths kept on purpose: CheckSkipEODReconcile reads
        # $.data_spot_error; CheckDegradedOutcome reads $.degraded_summary.
        assert sf["ExtractDataSpotError"]["Type"] == "Pass"
        assert sf["ExtractDataSpotError"]["ResultPath"] == "$.data_spot_error"
        assert sf["ExtractDataSpotError"]["Next"] == "SetDataSpotDegradedFlag"
        flag = sf["SetDataSpotDegradedFlag"]
        assert flag["Type"] == "Pass"
        assert flag["Parameters"]["degraded"] is True
        assert flag["ResultPath"] == "$.degraded_summary"
        assert flag["Next"] == "PublishDataSpotFailureImmediate"
        pub = sf["PublishDataSpotFailureImmediate"]
        assert pub["Next"] == self.CONTINUE
        for c in pub.get("Catch", []):
            assert c["Next"] == self.CONTINUE

    def test_no_wait_block_state_reaches_a_halt(self, sf):
        for name in _WAIT_BLOCK:
            for tgt in _all_targets(sf[name]):
                assert tgt not in _HALT, (
                    f"{name} routes to HALT state {tgt} — a not-ready collection "
                    "would block the trading path (config#1767 #4 violation)"
                )

    def test_the_whole_wait_region_is_exactly_the_wait_block(self, sf):
        """Exhaustive, not a list the test chose: walk everything reachable from
        the skip gate's Default up to the continue state. Anything outside the
        declared block (a stray HALT, a leftover spot state) fails here."""
        region = _reachable_from(sf, sf[self.SKIP_GATE]["Default"], self.CONTINUE)
        assert region == set(_WAIT_BLOCK), sorted(region ^ set(_WAIT_BLOCK))

    def test_the_skip_flag_routes_to_continue(self, sf):
        assert sf[self.SKIP_GATE]["Choices"][0]["Next"] == self.CONTINUE
        assert sf[self.SKIP_GATE]["Default"] == "InitCollectionReadinessPoll"

    def test_no_state_invokes_the_data_spot_dispatcher(self, sf):
        offenders = sorted(n for n, st in sf.items() if _invokes(st, _DISPATCHER_FN))
        assert offenders == [], (
            f"{offenders} still launch a collection box from a v1 SF — the "
            "standalone schedule owns collection since I11269"
        )


# ══════════════════════════════════════════════════════════════════════════
# WEEKDAY (step_function_daily.json)
# ══════════════════════════════════════════════════════════════════════════
class TestWeekdayDataPhaseOffTrading:
    """The on-trading data-phase SSM states (config#1767) AND the spot
    launch/poll/retry block that replaced them (removed by I11269) are GONE."""

    @pytest.mark.parametrize(
        "gone",
        [
            # config#1767: on-trading SSM data phase.
            "MorningEnrich", "MorningArcticAppend",
            "WaitForMorningEnrich", "WaitForMorningArcticAppend",
            "CheckMorningEnrichStatus", "CheckMorningArcticAppendStatus",
            "CheckSkipMorningArcticAppend", "MorningEnrichPollTimeout",
            # I11269: the data-spot launch/poll/retry block.
            "InitMorningEnrichRetryCounter", "LaunchMorningEnrichSpot",
            "CheckMorningEnrichSpotLaunched", "PollMorningEnrichSpot",
            "CheckMorningEnrichSpotStatus", "MorningEnrichSpotWait",
            "CheckMorningEnrichRetryBudget", "IncrementMorningEnrichRetry",
            "InitMorningArcticAppendRetryCounter", "LaunchMorningArcticAppendSpot",
            "CheckMorningArcticAppendSpotLaunched", "PollMorningArcticAppendSpot",
            "CheckMorningArcticAppendSpotStatus", "MorningArcticAppendSpotWait",
            "CheckMorningArcticAppendRetryBudget", "IncrementMorningArcticAppendRetry",
        ],
    )
    def test_relocated_state_absent(self, daily, gone):
        assert gone not in daily, f"{gone} must not run from the v1 weekday SF"

    def test_no_ssm_send_targets_trading_instance_for_data(self, daily):
        # No remaining ssm:sendCommand state runs a weekly_collector data workload.
        from tests.sf_command_utils import extract_commands
        for name, st in daily.items():
            if st.get("Resource") != _SSM_SEND:
                continue
            joined = "\n".join(extract_commands(st))
            assert "--morning-enrich" not in joined, f"{name} still runs enrich on-box"
            assert "--morning-arctic-append" not in joined, f"{name} still appends on-box"


class TestWeekdayFailureIsolation(_WaitBlockIsolation):
    """A not-ready morning collection must NOT block daemon start.

    alpha-engine-config-I6494: the continue path rejoins at
    CheckSkipPredictorInference (I7811 removed the weekday Scanner)."""

    SF = "daily"
    CONTINUE = "CheckSkipPredictorInference"
    SKIP_GATE = "CheckSkipMorningEnrich"


# ══════════════════════════════════════════════════════════════════════════
# EOD (step_function_eod.json)
# ══════════════════════════════════════════════════════════════════════════
class TestEODDataPhaseOffTrading:
    @pytest.mark.parametrize(
        "gone",
        [
            # config#1767: on-trading SSM data phase.
            "PostMarketData", "PostMarketArcticAppend",
            "WaitForPostMarketData", "WaitForPostMarketArcticAppend",
            "CheckPostMarketStatus", "CheckPostMarketArcticAppendStatus",
            "CheckSkipPostMarketArcticAppend",
            "PostMarketStatusError", "PostMarketArcticAppendStatusError",
            # I11269: the data-spot launch/poll/retry block.
            "InitDataSpotRetryCounter", "LaunchPostMarketDataSpot",
            "CheckPostMarketDataSpotLaunched", "PollPostMarketDataSpot",
            "CheckPostMarketDataSpotStatus", "PostMarketDataSpotWait",
            "CheckDataSpotRetryBudget", "IncrementDataSpotRetry",
            "InitDataSpotArcticRetryCounter", "LaunchPostMarketArcticAppendSpot",
            "CheckPostMarketArcticAppendSpotLaunched", "PollPostMarketArcticAppendSpot",
            "CheckPostMarketArcticAppendSpotStatus", "PostMarketArcticAppendSpotWait",
            "CheckDataSpotArcticRetryBudget", "IncrementDataSpotArcticRetry",
            "InitDataSpotEdgarRetryCounter", "LaunchEdgarPitFundamentalsDailySpot",
            "CheckEdgarPitFundamentalsDailySpotLaunched", "PollEdgarPitFundamentalsDailySpot",
            "CheckEdgarPitFundamentalsDailySpotStatus", "EdgarPitFundamentalsDailySpotWait",
            "CheckDataSpotEdgarRetryBudget", "IncrementDataSpotEdgarRetry",
            # I11266: the heal loop's own spot relaunch (HealStartCollection
            # starts ne-data-collection-eod instead).
            "HealLaunchPostMarketDataSpot", "HealCheckPostMarketDataSpotLaunched",
            "HealPollPostMarketDataSpot", "HealCheckPostMarketDataSpotStatus",
            "HealPostMarketDataSpotWait", "HealLaunchArcticAppendSpot",
            "HealCheckArcticAppendSpotLaunched", "HealPollArcticAppendSpot",
            "HealCheckArcticAppendSpotStatus", "HealArcticAppendSpotWait",
        ],
    )
    def test_relocated_state_absent(self, eod, gone):
        assert gone not in eod, f"{gone} must not run from the v1 EOD SF"

    def test_reconcile_snapshot_stop_path_intact(self, eod):
        # Deliverable #2: the reconcile/snapshot/instance-stop path stays on the box.
        for kept in ("CaptureSnapshot", "EODReconcile", "StopTradingInstance"):
            assert kept in eod, f"{kept} must remain in the EOD trading path"

    def test_no_ssm_send_targets_trading_instance_for_data(self, eod):
        from tests.sf_command_utils import extract_commands
        for name, st in eod.items():
            if st.get("Resource") != _SSM_SEND:
                continue
            joined = "\n".join(extract_commands(st))
            assert "--post-market-data" not in joined, f"{name} still fetches on-box"
            assert "--post-market-arctic-append" not in joined, f"{name} still appends on-box"


class TestEODFailureIsolation(_WaitBlockIsolation):
    """A not-ready EOD collection must NOT block reconcile + instance-stop — it
    routes to CheckSkipCaptureSnapshot, never HandleFailure."""

    SF = "eod"
    CONTINUE = "CheckSkipCaptureSnapshot"
    SKIP_GATE = "CheckSkipPostMarketData"


class TestEODReconcileSkippedOnDataGap:
    """2026-07-14 incident fix, part 2: even with the retry budget above, the
    data-spot phase can still end in $.data_spot_error (retry exhausted). That
    condition GUARANTEES eod_reconcile.py's _spy_close hard-fail (no fallback
    by design — today's SPY close was never written to ArcticDB), so
    CheckSkipEODReconcile must route around EODReconcile entirely instead of
    letting a guaranteed crash fall through to the generic HandleFailure ->
    FailExecution path (which mislabels a known, self-healing data gap as a
    pipeline defect — the false 'EOD Pipeline — FAILED' page from 2026-07-14)."""

    def test_data_gap_branch_precedes_default(self, eod):
        # config-I2702 (2026-07-15): the $.data_spot_error launch-phase flag
        # test was REPLACED by a fresh verify-by-artifact probe result — see
        # test_sf_eod_precondition_probe_wiring.py for the full pinning of
        # ProbeEODReconcilePrecondition + the closed self-heal loop this
        # branch now feeds into. This test only re-confirms the Choice shape
        # at CheckSkipEODReconcile itself.
        st = eod["CheckSkipEODReconcile"]
        assert st["Type"] == "Choice"
        gap_choices = [
            c for c in st["Choices"]
            if any(cond.get("Variable") == "$.precondition_probe.Payload.precondition_met"
                   for cond in c.get("And", []))
        ]
        assert len(gap_choices) == 1
        conds = gap_choices[0]["And"]
        assert any(c.get("IsPresent") is True for c in conds)
        assert any(c.get("BooleanEquals") is False for c in conds)
        assert gap_choices[0]["Next"] == "SkipEODReconcileDataGap"
        # No leftover reference to the old flag anywhere in this Choice.
        assert not any(c.get("Variable") == "$.data_spot_error" for c in st["Choices"])
        # The pre-existing operator-replay skip_eod_reconcile branch is untouched.
        assert st["Default"] == "EODReconcile"

    def test_skip_state_is_sns_publish_not_a_swallow(self, eod):
        # feedback_no_silent_fails: a skip must still be LOUD — a distinct,
        # accurately-worded SNS publish, not a bare Pass-through.
        st = eod["SkipEODReconcileDataGap"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        subject = st["Parameters"]["Subject"]
        assert 0 < len(subject) <= 100
        assert "\n" not in subject
        assert "SKIPPED" in subject
        assert "FAILED" not in subject, (
            "must read as a known/self-healing skip, not the generic pipeline-"
            "failed alert it replaces"
        )
        message_fmt = st["Parameters"]["Message.$"]
        # I2702: the skip is now decided by the precondition PROBE (verify-by-
        # artifact), not the launch-phase $.data_spot_error flag — the message
        # must reference the probe result and the closed-loop self-heal, and
        # must NOT resurrect the retired manual-operator-replay instruction.
        assert "States.JsonToString($.precondition_probe)" in message_fmt
        assert "self-heal" in message_fmt
        assert "operator-replay" not in message_fmt

    def test_skip_state_never_reaches_a_halt(self, eod):
        # config-I2702: SkipEODReconcileDataGap now enters the closed
        # self-heal loop (SetDegradedFlag) instead of jumping straight to the
        # substrate-check gate — the loop's own reachability (never hitting
        # _HALT, always eventually reaching StopTradingInstance) is pinned in
        # test_sf_eod_precondition_probe_wiring.py.
        for tgt in _all_targets(eod["SkipEODReconcileDataGap"]):
            assert tgt not in _HALT
        assert eod["SkipEODReconcileDataGap"]["Next"] == "SetDegradedFlag"

    def test_skip_states_own_sns_failure_still_continues(self, eod):
        # Mirrors HandleFailure's defense-in-depth: an SNS-side failure here
        # must not block entry into the self-heal loop (config-I2702).
        catches = eod["SkipEODReconcileDataGap"].get("Catch", [])
        assert any(
            c["ErrorEquals"] == ["States.ALL"] and c["Next"] == "SetDegradedFlag"
            for c in catches
        )


# ══════════════════════════════════════════════════════════════════════════
# Dispatcher Lambda + IAM (deliverables #1, #3)
# ══════════════════════════════════════════════════════════════════════════
class TestDispatcherLambdaAndIam:
    def test_dispatcher_package_present(self):
        # deploy.sh is load-bearing, NOT optional: like every sibling dispatcher
        # (scheduled-groom-dispatcher, spot-orphan-reaper), the function is
        # operator-deployed OUTSIDE CloudFormation, so a runnable deploy script
        # IS the deployment mechanism. #643 (config#1767 Phase 2) shipped this
        # dispatcher's source + IAM + SF wiring but NO deploy.sh, so step 1 of
        # the README rollout ("create the Lambda + role") had no tooling and was
        # skipped — the live function was never created and the 2026-07-08 EOD
        # LaunchPostMarketDataSpot got a 404 ResourceNotFoundException. This guard
        # fails loud so a data-spot dispatcher can never again merge un-deployable.
        for f in ("index.py", "iam-policy.json", "sf-execution-iam-policy.json",
                  "requirements.txt", "deploy.sh"):
            assert (_DISPATCHER / f).exists(), f"data-spot-dispatcher/{f} missing"

    def test_dispatcher_deploy_sh_creates_the_function(self):
        # A deploy.sh that exists but doesn't actually create the Lambda would
        # re-open the same gap. Pin the two commands that make it a real,
        # first-time-capable deployer for THIS function.
        deploy = (_DISPATCHER / "deploy.sh").read_text()
        assert "alpha-engine-data-spot-dispatcher" in deploy, \
            "deploy.sh must target the alpha-engine-data-spot-dispatcher function"
        assert "aws lambda create-function" in deploy, \
            "deploy.sh must be able to CREATE the function (first-time bootstrap), not only update it"

    def test_workload_map_preserves_collector_contract(self):
        # M0 contract: the spot workloads run the SAME weekly_collector.py entry
        # points the on-trading states ran — unchanged args = unchanged data paths.
        # The workload KEYS are post-market-* (SF-facing); the VALUES must mirror
        # the old on-trading SSM commands (--daily*, NOT invented --post-market-*).
        src = (_DISPATCHER / "index.py").read_text()
        for token in (
            "--morning-enrich",
            "--morning-arctic-append",
            '"post-market-data"',
            '"post-market-arctic-append"',
        ):
            assert token in src, f"dispatcher workload map missing {token}"
        assert '"post-market-data":' in src
        assert "python weekly_collector.py --daily --skip-arctic-append" in src
        assert '"post-market-arctic-append":' in src
        assert "python weekly_collector.py --daily-arctic-append" in src
        # #643 shipped bogus --post-market-* CLI flags that weekly_collector.py
        # never defined — broke the 2026-07-08 EOD run on first live spot path.
        assert "--post-market-data" not in src.replace(
            '"post-market-data"', ""
        ).replace('"post-market-arctic-append"', "")
        assert "--post-market-arctic-append" not in src.replace(
            '"post-market-arctic-append"', ""
        )
        # The enrich workload must still skip the inline heal + inline append.
        assert "--skip-chronic-heal" in src
        assert "--skip-arctic-append" in src

    def test_daily_heal_workload_present(self):
        # alpha-engine-config-I2717 (2026-07-16): the standalone daily-heal
        # workload, invoked directly by its own EventBridge rule (NOT by
        # either SF — see infrastructure/cloudformation/alpha-engine-
        # orchestration.yaml DailyHealTrigger). Bundles the universe-gap
        # self-heal (formerly the head of --morning-arctic-append) and the
        # chronic-polygon-gap heal (formerly the weekday SF's own
        # ChronicGapSelfHeal state) into one weekly_collector.py invocation.
        src = (_DISPATCHER / "index.py").read_text()
        assert '"daily-heal":' in src
        assert "python weekly_collector.py --daily-heal" in src

    def test_daily_heal_workload_key_satisfies_strict_allowlist_regex(self):
        # _resolve_workload's defense-in-depth allowlist regex
        # (^[a-z][a-z-]{0,63}$) gates every workload key against
        # shell-metacharacter injection — "daily-heal" must satisfy it (mirrors
        # the same check the module already applies to every other key; kept
        # as a literal regex here rather than importing index.py directly, to
        # match this file's existing text-only-assertion convention and avoid
        # a real boto3/nousergon_lib import at collection time).
        import re
        assert re.match(r"^[a-z][a-z-]{0,63}$", "daily-heal")

    def test_bootstrap_clones_private_config_package(self):
        # weekly_collector.load_config resolves experiments/reference/data/config.yaml
        # from a shallow alpha-engine-config clone (2026-07-08 EOD: missing clone →
        # FileNotFoundError on the first live spot path after the CLI-flag fix).
        # The PAT read and the private-repo clone stay in this file (the
        # renderer bakes URLs in as launcher-side literals and so cannot
        # express a ${PAT} URL — see _bootstrap_spec()'s docstring). The
        # exports moved into the SPEC when the bootstrap was cut over to
        # krepis.spot_bootstrap (alpha-engine-config-I7372), so that one is
        # asserted against the RENDERED script rather than the source text —
        # reading the source for it is how a cut-over dispatcher reads as
        # having dropped an export it still emits.
        src = (_DISPATCHER / "index.py").read_text()
        assert "alpha-engine-config" in src
        assert "ssm get-parameter" in src
        assert "/alpha-engine/saturday_sf_watch/github_pat" in src

        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "_data_spot_index_for_test", _DISPATCHER / "index.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        rendered = module._bootstrap_command(
            "morning-enrich", "python weekly_collector.py --morning-enrich", "tok"
        )
        assert "ALPHA_ENGINE_EXPERIMENT_ID=reference" in rendered

    def test_dispatcher_uses_executor_profile_no_ib_exposure(self):
        # Deliverable #3: the spot reuses the Saturday spot's Arctic-write/S3
        # profile (alpha-engine-executor-profile) and the standard fleet SG (no
        # IB port). This mirrors spot_data_weekly.sh rather than minting a role.
        src = (_DISPATCHER / "index.py").read_text()
        assert "alpha-engine-executor-profile" in src
        # No IB gateway port opened anywhere in the launcher.
        assert "4001" not in src and "4002" not in src

    # NOTE: the SF role's lambda:InvokeFunction assertion moved to
    # nous-ergon-ops/tests/test_cross_repo_sf_iam_contract.py when the IAM
    # tree consolidated. It reads an IAM policy file, no longer in this repo;
    # ops is private and can clone this public repo, not the reverse. The
    # invariant is unchanged, enforced from the side that sees both halves.
if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-q"]))
