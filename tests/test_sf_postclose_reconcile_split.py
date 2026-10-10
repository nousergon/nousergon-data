"""Pins the post-close split (alpha-engine-config-I11269 follow-up, 2026-09-30).

Brian: *"can we run all steps in the post close sf that can run immediately
after close and just set up the part that relies on the collector as a
separate sf?"*

Before: ``ne-postclose-trading-pipeline`` started at ~16:00 ET and then sat in
``WaitForCollectionManifests`` for up to 59 x 300 s while the standalone
``ne-data-collection-eod`` (18:15 ET cron) ran, holding the trading box and the
whole evening in one execution.

After, two machines:

* ``ne-postclose-trading-pipeline`` (``step_function_eod.json``) — same name,
  same ~16:00 ET daemon trigger. Market-hours gate, mutex, deploy-drift
  check, box start, SSM readiness, RefreshExecutorDeploy, CaptureSnapshot (with
  its bounded same-day retry and pages), box stop, completion marker. It waits
  for nothing. Since 2026-10-01 (Brian: "lets just stop it when postclose
  part 1 completes") it stops the box on success too; crucible-executor's
  Persistent=true post-close timers catch up at the reconcile machine's boot.
  Its failure path still force-stops it.
* ``ne-postclose-reconcile-pipeline`` (``step_function_eod_reconcile.json``) —
  started by the eod-backstop Lambda when ``ne-data-collection-eod`` reaches a
  terminal state (``alpha-engine-eod-reconcile-trigger``), or by the 02:15 UTC
  backstop (``alpha-engine-eod-reconcile-backstop-daily``). Short readiness
  guard, precondition probe, EODReconcile or the heal loop, box stop, the
  weekly exercise launch, completion marker.

This file pins the shape of the split; the per-stage wiring stays pinned in
the files that already owned it (skip-gates, readiness wait, heal loop,
capture-snapshot retry), each now pointed at the machine its states live in.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
INFRA = REPO / "infrastructure"
POSTCLOSE = INFRA / "step_function_eod.json"
RECONCILE = INFRA / "step_function_eod_reconcile.json"
BACKSTOP_DIR = INFRA / "lambdas" / "eod-backstop"

POSTCLOSE_NAME = "ne-postclose-trading-pipeline"
RECONCILE_NAME = "ne-postclose-reconcile-pipeline"
COLLECTION_NAME = "ne-data-collection-eod"

#: The shared prologue both machines carry: gate, mutex, box start, SSM
#: readiness, deploy refresh, the Option-A terminals and the failure path.
_SHARED = {
    "MarketHoursGate", "MarketHoursGateChoice", "StampMarketHoursVerdictMissing",
    "RecordMarketHoursOverride", "NotifyMarketHoursBlocked", "MarketHoursBlocked",
    "NotifyMarketHoursOverrideMalformed", "MarketHoursOverrideMalformed",
    "NotifyMarketHoursUnverified", "SetMarketHoursUnverifiedDegraded",
    "CheckMutexRole", "AcquireMutex", "SetMutexAcquireDegradedFlag", "MutexConflict",
    "StartTradingInstance", "WaitForInstanceReady", "InitSSMPollCounter",
    "DescribeInstanceInfo", "SSMReadyChoice", "SetSSMReadyExhaustedError",
    "WaitSSMPoll", "IncrementSSMPoll",
    "CheckSkipRefreshExecutorDeploy", "RefreshExecutorDeploy",
    "WaitForRefreshExecutorDeploy", "CheckRefreshExecutorDeployStatus",
    "RefreshExecutorDeployWait", "RefreshExecutorDeployStatusError",
    "StopTradingInstance",
    "CheckDegradedOutcome", "WriteCompletionMarkerNormal", "NormalSucceeded",
    "WriteCompletionMarkerDegraded", "DegradedRun",
    "HandleFailure", "ForceStopInstance", "FailExecution", "NormalizeEODFailureContext",
    # 2026-10-09 in-session box stop: both machines stop the box, so both
    # refuse the pre-session window [08:00, 09:30) ET after the gate.
    "StampStartClockUtc", "PreSessionWindowChoice", "NotifyPreSessionBlocked",
    # alpha-engine-config-I12220: the reconcile machine captures a MISSING
    # snapshot itself (once, no retry budget, presence-gated) instead of
    # failing on it; the capture/poll/page states carry the same names, and
    # so the same pipeline-status registry rows, in both machines.
    "CaptureSnapshot", "WaitForCaptureSnapshot", "CheckSnapshotStatus",
    "SnapshotStatusError", "SnapshotWait", "PageCaptureSnapshotIrreversibleFailure",
}

_POSTCLOSE_ONLY = {
    # alpha-engine-config-I8102: the deploy-drift gate.
    "DeployDriftCheck", "DeployDriftGate", "SetDeployDriftObserveWouldHaltFlag",
    "SetDeployDriftProbeUnreadableFlag", "SetDeployDriftDegradedFlag",
    "PublishDeployDriftDegraded",
    # CaptureSnapshot's skip gate and its bounded same-day retry
    # (alpha-engine-config#5569).
    "CheckSkipCaptureSnapshot", "InitCaptureSnapshotRetryCounter",
    "CheckCaptureSnapshotRetryBudget", "PageCaptureSnapshotFailureImmediate",
    "IncrementCaptureSnapshotRetry", "CaptureSnapshotRetryExhausted",
}

#: Every state that depends on ne-data-collection-eod having run.
_COLLECTOR_DEPENDENT = {
    "CheckSkipPostMarketData", "InitCollectionReadinessPoll", "SeedCollectionReadiness",
    "WaitForCollectionManifests", "CheckCollectionReadiness",
    "CheckCollectionReadinessBudget", "CollectionReadinessPollWait",
    "IncrementCollectionReadinessPoll", "ExtractCollectionNotReadyError",
    "ExtractDataSpotError", "SetDataSpotDegradedFlag", "PublishDataSpotFailureImmediate",
    "ProbeEODReconcilePrecondition", "CheckSkipEODReconcile", "EODReconcile", "WaitForEOD",
    "CheckEODStatus", "EODStatusError", "EODWait", "SkipEODReconcileDataGap",
    "SetDegradedFlag", "CheckHealLoopEligible", "InitHealLoop", "HealLoopGate",
    "HealStartCollection", "HealReProbe", "HealCheckConverged", "HealLoopIncrement",
    "HealDispatchReplay", "HealReplayDispatchFailed", "HealConvergedNotify",
    "HealNonConvergent",
}

_RECONCILE_ONLY = _COLLECTOR_DEPENDENT | {
    # The weekly exercise tail after the box stop.
    "ReadExerciseCadence", "SetCadenceReadDegraded",
    "PublishCadenceReadDegraded", "CheckExerciseCadence", "SetCadenceUnknownValueDegraded",
    "PublishCadenceUnknownValueDegraded", "LaunchWeeklyExerciseRun",
    "SetWeeklyExerciseDegradedFlag", "WeeklyExerciseLaunchFailed",
    # alpha-engine-config-I12020: the bounded drain in front of the box stop
    # that keeps the box up for the v2 trader's boot-time reconcile.
    "DrainTraderReconcile", "WaitForTraderReconcileDrain", "CheckTraderReconcileDrainStatus",
    "TraderReconcileDrainWait", "ExtractTraderReconcileDrainStatusError",
    "SetTraderReconcileDrainDegraded", "PublishTraderReconcileDrainUnsettled",
    # 2026-10-09 in-session box stop: box ownership. This machine stops only a
    # box it started; the post-close machine's stop is the ruled 16:00 stop of
    # the box the preopen pipeline started, so it carries no ownership check.
    "ResolveBoxOwnership", "StampBoxOwned", "StampBoxNotOwned",
    "CheckBoxOwnedBeforeStop", "NotifyStopSkippedNotBoxOwner",
    "CheckBoxOwnedBeforeForceStop",
}


def _doc(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def postclose() -> dict:
    return _doc(POSTCLOSE)


@pytest.fixture(scope="module")
def reconcile() -> dict:
    return _doc(RECONCILE)


def _without_comments(obj):
    """The definition minus every ``Comment`` (which may NAME the collector)."""
    if isinstance(obj, dict):
        return {k: _without_comments(v) for k, v in obj.items() if k != "Comment"}
    if isinstance(obj, list):
        return [_without_comments(v) for v in obj]
    return obj


def _targets(state: dict) -> list[str]:
    out = [state[k] for k in ("Next", "Default") if k in state]
    out += [c["Next"] for c in state.get("Choices", []) if "Next" in c]
    out += [c["Next"] for c in state.get("Catch", []) if "Next" in c]
    return out


def _reachable(states: dict, start: str, blocked: set[str] = frozenset()) -> set[str]:
    seen: set[str] = set()
    todo = [start]
    while todo:
        name = todo.pop()
        if name in seen or name in blocked:
            continue
        seen.add(name)
        todo.extend(_targets(states[name]))
    return seen


# ── the two state lists ──────────────────────────────────────────────────────


def test_the_postclose_machine_is_exactly_the_prologue_plus_the_snapshot(postclose):
    assert set(postclose["States"]) == _SHARED | _POSTCLOSE_ONLY


def test_the_reconcile_machine_is_exactly_the_prologue_plus_the_collector_half(reconcile):
    assert set(reconcile["States"]) == _SHARED | _RECONCILE_ONLY


def test_no_state_is_lost_or_duplicated_by_the_split():
    assert not _POSTCLOSE_ONLY & _RECONCILE_ONLY
    assert not _SHARED & (_POSTCLOSE_ONLY | _RECONCILE_ONLY)


@pytest.mark.parametrize("path", [POSTCLOSE, RECONCILE], ids=lambda p: p.name)
def test_every_state_is_reachable_and_every_edge_resolves(path):
    doc = _doc(path)
    states = doc["States"]
    for name, st in states.items():
        for t in _targets(st):
            assert t in states, f"{path.name}: {name} -> {t} does not exist"
    assert _reachable(states, doc["StartAt"]) == set(states), (
        f"{path.name}: unreachable states "
        f"{sorted(set(states) - _reachable(states, doc['StartAt']))}"
    )


# ── what each machine must NOT contain ───────────────────────────────────────


def test_the_postclose_machine_depends_on_nothing_the_collector_writes(postclose):
    present = sorted(_COLLECTOR_DEPENDENT & set(postclose["States"]))
    assert present == [], f"collector-dependent states left in {POSTCLOSE.name}: {present}"
    blob = json.dumps(_without_comments(postclose))
    for needle in ("alpha-engine-collection-readiness-probe", "alpha-engine-eod-precondition-probe",
                   COLLECTION_NAME, "executor/eod_reconcile.py"):
        assert needle not in blob, needle


def test_the_postclose_machine_stops_the_box_as_soon_as_the_snapshot_is_done(postclose):
    """Brian, 2026-10-01: "lets just stop it when postclose part 1 completes".
    Every route into the Option-A terminals passes StopTradingInstance, and
    nothing on the success path runs after it except the terminal router and
    the completion marker. crucible-executor's post-close timers are
    Persistent=true and catch up at the reconcile machine's boot. The
    FAILURE path still force-stops."""
    states = postclose["States"]
    before_stop = _reachable(states, postclose["StartAt"],
                             blocked={"StopTradingInstance", "HandleFailure"})
    assert not {"CheckDegradedOutcome", "NormalSucceeded", "DegradedRun"} & before_stop
    after_stop = _reachable(states, "StopTradingInstance", blocked={"HandleFailure"})
    assert after_stop == {
        "StopTradingInstance", "CheckDegradedOutcome",
        "WriteCompletionMarkerNormal", "NormalSucceeded",
        "WriteCompletionMarkerDegraded", "DegradedRun",
    }, after_stop
    assert states["StopTradingInstance"]["Resource"].endswith(":ec2:stopInstances")
    assert states["HandleFailure"]["Next"] == "ForceStopInstance"


def test_the_box_timers_that_now_run_at_the_evening_boot_are_persistent():
    """The stop above is safe only because these catch up on the next boot.
    Pinned against crucible-executor's units when that checkout is beside
    this one (as on the fleet laptop); skipped otherwise."""
    systemd = REPO.parent / "crucible-executor" / "infrastructure" / "systemd"
    if not systemd.is_dir():
        pytest.skip("crucible-executor checkout not present")
    for stem in ("alpha-engine-trader-reconcile", "alpha-engine-eod-reconcile-standalone",
                 "alpha-engine-reference-rate-publish"):
        text = (systemd / f"{stem}.timer").read_text(encoding="utf-8")
        assert re.search(r"^Persistent=true$", text, re.MULTILINE), stem


def test_the_reconcile_machine_runs_no_drift_gate_and_no_capture_retry_loop(reconcile):
    states = reconcile["States"]
    present = sorted(_POSTCLOSE_ONLY & set(states))
    assert present == [], present
    # The heal replay targets THIS machine; its CaptureSnapshot is presence-
    # gated, so a replay never recaptures (see test_heal_replay_deploy_refresh_i7586).
    assert states["HealDispatchReplay"]["Parameters"]["StateMachineArn.$"] == "$$.StateMachine.Id"


def test_the_reconcile_machine_still_stops_the_box_on_every_non_failure_path(reconcile):
    """The cost guard the old single machine carried moves with the tail:
    every route into the Option-A terminals passes StopTradingInstance --
    except the one through HealConvergedNotify, where a replay execution
    already owns the box and stops it at its own end (2026-10-08 heal-replay
    stop race, tests/test_heal_replay_owns_the_trading_box.py) and the one
    through NotifyStopSkippedNotBoxOwner, where the box was already running
    when this execution started it, so another actor owns it (2026-10-09
    in-session stop, tests/test_sf_pre_session_window_and_box_ownership.py)."""
    states = reconcile["States"]
    before_stop = _reachable(states, reconcile["StartAt"],
                             blocked={"StopTradingInstance", "HandleFailure",
                                      "HealConvergedNotify",
                                      "NotifyStopSkippedNotBoxOwner"})
    assert not {"CheckDegradedOutcome", "NormalSucceeded", "DegradedRun"} & before_stop


# ── ceilings and markers ─────────────────────────────────────────────────────


def test_the_ceilings(postclose, reconcile):
    # Derivations: tests/test_sf_structural_contract.py and
    # tests/test_v1_collection_readiness_wait.py.
    assert postclose["TimeoutSeconds"] == 3600
    assert reconcile["TimeoutSeconds"] == 14400


@pytest.mark.parametrize("path,name", [(POSTCLOSE, POSTCLOSE_NAME), (RECONCILE, RECONCILE_NAME)],
                         ids=["postclose", "reconcile"])
def test_each_machine_writes_its_own_completion_marker(path, name):
    states = _doc(path)["States"]
    for marker in ("WriteCompletionMarkerNormal", "WriteCompletionMarkerDegraded"):
        blob = json.dumps(states[marker]["Parameters"])
        assert f"_sf_completion/{name}/" in blob, (marker, blob)
        other = RECONCILE_NAME if name == POSTCLOSE_NAME else POSTCLOSE_NAME
        assert f"_sf_completion/{other}/" not in blob


def test_the_eod_artifact_check_follows_the_reconcile_marker():
    """eod_pnl.csv is produced by EODReconcile, so the notifier / watchdog
    artifact verification keys on the machine that runs it."""
    src = (INFRA / "lambdas" / "eod_artifact_verification.py").read_text(encoding="utf-8")
    assert f'EOD_PIPELINE_NAME = "{RECONCILE_NAME}"' in src


# ── the trigger and the backstop ─────────────────────────────────────────────


@pytest.fixture(scope="module")
def backstop_deploy() -> str:
    return (BACKSTOP_DIR / "deploy.sh").read_text(encoding="utf-8")


def _heredoc(text: str, var: str) -> dict:
    m = re.search(rf"{var}=\$\(cat <<EOF\n(.*?)\nEOF\n\)", text, re.S)
    assert m, f"{var} heredoc not found"
    body = m.group(1).replace("${REGION}", "us-east-1").replace("${ACCOUNT_ID}", "711398986525")
    return json.loads(body)


def test_the_trigger_rule_matches_the_collections_terminal_states(backstop_deploy):
    pattern = _heredoc(backstop_deploy, "COLLECTION_TERMINAL_PATTERN")
    assert pattern["source"] == ["aws.states"]
    assert pattern["detail-type"] == ["Step Functions Execution Status Change"]
    detail = pattern["detail"]
    assert detail["stateMachineArn"] == [
        f"arn:aws:states:us-east-1:711398986525:stateMachine:{COLLECTION_NAME}"
    ]
    # FAILED / TIMED_OUT start it too: the reconcile's own probe + heal loop
    # decide what a failed collection means. ABORTED is an operator's stop.
    assert detail["status"] == ["SUCCEEDED", "FAILED", "TIMED_OUT"]
    # The heal loop's own collection must never start a second reconcile.
    assert detail["name"] == [{"anything-but": {"prefix": "v1-eod-heal-"}}]


def test_the_handler_agrees_with_the_rule(backstop_deploy):
    src = (BACKSTOP_DIR / "index.py").read_text(encoding="utf-8")
    pattern = _heredoc(backstop_deploy, "COLLECTION_TERMINAL_PATTERN")
    m = re.search(r"COLLECTION_TRIGGERED_BY\s*=\s*\{(.*?)\}", src, re.S)
    assert m
    statuses = re.findall(r'"([A-Z_]+)"\s*:', m.group(1))
    assert statuses == pattern["detail"]["status"]
    assert 'HEAL_COLLECTION_PREFIX = "v1-eod-heal-"' in src
    heal = _doc(RECONCILE)["States"]["HealStartCollection"]["Parameters"]["Name.$"]
    assert heal.startswith("States.Format('v1-eod-heal-")


def test_both_rules_target_the_backstop_and_are_reconciled_on_every_deploy(backstop_deploy):
    assert 'RECONCILE_TRIGGER_RULE="alpha-engine-eod-reconcile-trigger"' in backstop_deploy
    assert 'RECONCILE_BACKSTOP_RULE="alpha-engine-eod-reconcile-backstop-daily"' in backstop_deploy
    assert "--schedule-expression 'cron(15 2 ? * TUE-SAT *)'" in backstop_deploy
    # Each rule: put-rule with a manifest-resolved state, put-targets, and a
    # tolerated add-permission scoped to that rule's ARN.
    for rule in ("${RECONCILE_TRIGGER_RULE}", "${RECONCILE_BACKSTOP_RULE}"):
        assert f'--state "$(pause_state "{rule}")"' in backstop_deploy
        assert f'--rule "{rule}"' in backstop_deploy
        assert f"rule/{rule}" in backstop_deploy
    # Step 2b is outside the --bootstrap / --apply-iam branches: it runs on the
    # merge deploy, so the rules ship with the merge.
    step2b = backstop_deploy.index('RECONCILE_TRIGGER_RULE="')
    step3 = backstop_deploy.index("# ----- 3. Update function code")
    assert step2b < step3
    block = backstop_deploy[step2b:step3]
    assert "if $BOOTSTRAP" not in block and "APPLY_IAM" not in block


def test_the_backstop_target_input_is_valid_json(backstop_deploy):
    m = re.search(r'--rule "\$\{RECONCILE_BACKSTOP_RULE\}" \\\n\s*--targets "(.*?)" \\', backstop_deploy)
    assert m
    shell_unescaped = m.group(1).replace('\\"', '"').replace("\\\\", "\\")
    targets = json.loads(shell_unescaped.replace("${RECONCILE_FN_ARN}", "arn"))
    assert json.loads(targets[0]["Input"]) == {"mode": "reconcile-backstop"}


def test_both_rules_are_classified_in_the_pause_manifest():
    manifest = json.loads((INFRA / "automation_pause.json").read_text(encoding="utf-8"))
    for rule in ("alpha-engine-eod-reconcile-trigger", "alpha-engine-eod-reconcile-backstop-daily"):
        assert rule in manifest["not_paused"], rule
        assert manifest["not_paused"][rule].strip()
        for block in ("paused", "pending"):
            assert rule not in json.dumps(manifest.get(block, {})), (rule, block)


# ── deploy and registries ────────────────────────────────────────────────────


def test_the_reconcile_machine_is_deployed_validated_and_registered():
    deploy = (INFRA / "deploy-infrastructure.sh").read_text(encoding="utf-8")
    assert 'validate_sf_definition "$EOD_RECONCILE_STAMPED"' in deploy
    assert f":stateMachine:{RECONCILE_NAME}" in deploy
    assert f'"{RECONCILE_NAME}" "Post-close reconcile pipeline"' in deploy
    assert f"/aws/stepfunctions/{RECONCILE_NAME}" in deploy
    # validated BEFORE any update_or_create (all-or-nothing)
    assert deploy.index('validate_sf_definition "$EOD_RECONCILE_STAMPED"') < deploy.index(
        'update_or_create "$EOD_RECONCILE_ARN"')

    from infrastructure.sf_definitions import SF_DEFINITIONS
    assert {"sf_name": RECONCILE_NAME, "definition_file": RECONCILE.name} in SF_DEFINITIONS

    contract = json.loads((INFRA / "sf_entry_contract.json").read_text(encoding="utf-8"))
    assert RECONCILE.name in json.dumps(contract)


def test_the_reconcile_starter_can_start_it_and_nothing_else_new():
    policy = json.loads((BACKSTOP_DIR / "iam-policy.json").read_text(encoding="utf-8"))
    blob = json.dumps(policy)
    assert f"stateMachine:{RECONCILE_NAME}" in blob
    assert f"stateMachine:{COLLECTION_NAME}" in blob  # ListExecutions only
    for st in policy["Statement"]:
        if any(COLLECTION_NAME in r for r in (st["Resource"] if isinstance(st["Resource"], list)
                                              else [st["Resource"]])):
            actions = st["Action"] if isinstance(st["Action"], list) else [st["Action"]]
            assert actions == ["states:ListExecutions"], st
