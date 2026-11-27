"""Pins the alpha-engine-config-I2702 closed-loop EOD self-heal: verify-by-
artifact precondition probe (deliverable #1), the closed heal loop
(deliverable #3), and the degraded terminal state (deliverable #4).

Background (2026-07-15 incident, config-I2699 + config-I2702): the EOD SF's
old ``CheckSkipEODReconcile`` skip decision keyed on a LAUNCH-PHASE flag
(``$.data_spot_error``), which a poll-side transient AWS SDK hiccup could set
even while the underlying collector was still running and later finished
rc=0 — a decision-on-stale-signal bug (same class as the groom auto-clear
trusting a text match, 2026-07-11). Recovery bottomed out in a MANUAL
operator-replay step. This test pins the replacement: a fresh S3 read of a
readback-verified artifact (deliverable #1), an automatic dispatch-reprobe-
replay loop bounded by attempts + a deadline (deliverable #3), and a
terminal state that can never look plain-green when EODReconcile was skipped
(deliverable #4).

Companion tests:
  * test_sf_eod_skipgate_wiring.py — the CheckSkipEODReconcile Choice shape
    + the per-task rerun gate chain (unaffected Choices[0] branch).
  * test_sf_data_spot_relocation_wiring.py — the CheckSkipEODReconcile
    data-gap branch's Choice condition + SkipEODReconcileDataGap's
    fail-open Catch (both updated in the same PR to route into this file's
    heal loop instead of straight to the cost-guard tail).
  * test_sf_payload_uniqueness.py — the new top-level ``$.<X>`` fields this
    file introduces, in the closed namespace registry.
  * test_sf_iam_lambda_grants.py — the new
    ``alpha-engine-eod-precondition-probe`` Lambda ARN is IAM-grantable.
  * infrastructure/lambdas/eod-precondition-probe/test_handler.py — the
    probe Lambda's own unit tests (S3 sentinel read + evaluate + deadline math).

alpha-engine-config-I2722 (2026-07-16): the 3 heal-outcome notifiers below
(``HealConvergedNotify`` / ``HealReplayDispatchFailed`` / ``HealNonConvergent``)
now route to ``StopTradingInstance`` directly — they used to feed
``CheckSkipDailySubstrateHealthCheck``, which (with the rest of the
DailySubstrateHealthCheck chain) was removed and re-homed as a standalone
dashboard-box systemd timer (genuinely consumer-free within this SF,
verified; per-row CloudWatch alarms carry the alerting independently of the
SF). ``TestHandleFailureCostGuardHardening`` below was MOVED here verbatim
from the now-deleted ``test_sf_eod_substrate_check_wiring.py`` — its
HandleFailure/ForceStopInstance/TopicArn-hardening coverage was never
substrate-specific, it just happened to live in that file; ``TestDegradedTerminalState``
above already independently pins the StopTradingInstance -> CheckDegradedOutcome
routing that file also covered.
"""

# ruff: noqa: ANN001, ANN002, ANN003, ANN101, ANN102, ANN201, ANN202, ANN204, ANN205, ANN206

from __future__ import annotations

import json
from collections import deque
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
# alpha-engine-config-I11269: every state this file pins moved to the
# collector-gated reconcile machine when the post-close SF was split.
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod_reconcile.json"
_PROBE_FN = "alpha-engine-eod-precondition-probe"
_DISPATCHER_FN = "alpha-engine-data-spot-dispatcher"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def doc() -> dict:
    return json.loads(_SF_PATH.read_text())


def _targets(state: dict) -> list[str]:
    out: list[str] = []
    if "Next" in state:
        out.append(state["Next"])
    if "Default" in state:
        out.append(state["Default"])
    for c in state.get("Choices", []):
        if "Next" in c:
            out.append(c["Next"])
    for c in state.get("Catch", []):
        if "Next" in c:
            out.append(c["Next"])
    return out


# ── Whole-doc structural sanity (belt-and-suspenders on top of the manual
#    per-state pins below — catches typos in any Next/Default the targeted
#    tests don't happen to enumerate) ────────────────────────────────────────


def test_every_state_reference_resolves(states):
    missing = {
        (name, tgt)
        for name, st in states.items()
        for tgt in _targets(st)
        if tgt not in states
    }
    assert not missing


def test_every_state_reachable_from_start(doc, states):
    seen = set()
    q = deque([doc["StartAt"]])
    while q:
        n = q.popleft()
        if n in seen:
            continue
        seen.add(n)
        q.extend(t for t in _targets(states[n]) if t not in seen)
    assert set(states) - seen == set()


def test_succeed_and_fail_states_have_no_next(states):
    for name, st in states.items():
        if st["Type"] in ("Succeed", "Fail"):
            assert "Next" not in st, f"{name} is Type={st['Type']} but has a Next"


# ── Deliverable #1: ProbeEODReconcilePrecondition ────────────────────────────


class TestPreconditionProbe:
    def test_probe_invokes_the_new_lambda(self, states):
        st = states["ProbeEODReconcilePrecondition"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::lambda:invoke"
        assert st["Parameters"]["FunctionName"] == _PROBE_FN
        assert st["Parameters"]["Payload"] == {"run_date.$": "$.run_date"}
        assert st["ResultPath"] == "$.precondition_probe"
        assert st["Next"] == "CheckSkipEODReconcile"

    def test_probe_infra_failure_falls_through_to_reconcile_not_skip(self, states):
        # Deliberately fail-SAFE-toward-reconcile: a probe-infra failure must
        # NOT set $.precondition_probe at all, so CheckSkipEODReconcile's
        # IsPresent+BooleanEquals(false) test cannot match and falls through
        # to Default (EODReconcile) — the proven _spy_close hard-fail remains
        # the backstop if data is genuinely absent.
        st = states["ProbeEODReconcilePrecondition"]
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == "CheckSkipEODReconcile"
        assert catches[0]["ResultPath"] is None

    def test_reconcile_gate_reads_the_probe_not_the_old_flag(self, states):
        cser = states["CheckSkipEODReconcile"]
        gap_choice = next(
            c for c in cser["Choices"]
            if any(cond.get("Variable") == "$.precondition_probe.Payload.precondition_met"
                   for cond in c.get("And", []))
        )
        assert gap_choice["Next"] == "SkipEODReconcileDataGap"
        assert not any(
            cond.get("Variable") == "$.data_spot_error"
            for c in cser["Choices"] for cond in c.get("And", [])
        )

    def test_probe_is_called_again_inside_the_heal_loop(self, states):
        # HealReProbe is a second call site of the SAME Lambda, re-verifying
        # fresh (never trusting the pre-dispatch probe result) after the
        # heal loop dispatches the missing workload(s).
        st = states["HealReProbe"]
        assert st["Type"] == "Task"
        assert st["Parameters"]["FunctionName"] == _PROBE_FN
        assert st["ResultPath"] == "$.precondition_probe"

    def test_probe_lambda_package_present(self):
        d = _REPO_ROOT / "infrastructure" / "lambdas" / "eod-precondition-probe"
        for f in ("index.py", "test_handler.py", "deploy.sh", "iam-policy.json", "requirements.txt"):
            assert (d / f).exists(), f"missing {d / f}"


# ── Deliverable #4: degraded terminal state ──────────────────────────────────


class TestDegradedTerminalState:
    def test_gap_detection_sets_the_degraded_flag(self, states):
        assert states["SkipEODReconcileDataGap"]["Next"] == "SetDegradedFlag"
        sdf = states["SetDegradedFlag"]
        assert sdf["Type"] == "Pass"
        assert sdf["Parameters"]["degraded"] is True
        assert sdf["ResultPath"] == "$.degraded_summary"

    def test_stop_trading_instance_leads_to_the_degraded_check(self, states):
        """config-I5489 inserted the weekly-exercise launch between the cost
        guard and the degraded check; alpha-engine-config-I6689 further
        inserted the declared-cadence read + gate ahead of the launch itself
        (StopTradingInstance -> ReadExerciseCadence -> CheckExerciseCadence ->
        LaunchWeeklyExerciseRun on cadence=daily). What the test actually
        protects is unchanged: StopTradingInstance is not a terminal, and the
        degraded check is still what decides the terminal — none of the
        inserted states may swallow or bypass it.
        """
        assert "End" not in states["StopTradingInstance"]
        assert states["StopTradingInstance"]["Next"] == "ReadExerciseCadence"
        assert states["ReadExerciseCadence"]["Next"] == "CheckExerciseCadence"
        # cadence=daily is the ONLY branch that launches; weekly-only/off skip
        # straight to the degraded check, same as LaunchWeeklyExerciseRun's
        # own success Next.
        daily_choice = next(
            c for c in states["CheckExerciseCadence"]["Choices"] if c.get("StringEquals") == "daily"
        )
        assert daily_choice["Next"] == "LaunchWeeklyExerciseRun"
        for value in ("weekly-only", "off"):
            choice = next(
                c for c in states["CheckExerciseCadence"]["Choices"] if c.get("StringEquals") == value
            )
            assert choice["Next"] == "CheckDegradedOutcome"
        # both the success and launch-failure routes converge on the check
        assert states["LaunchWeeklyExerciseRun"]["Next"] == "CheckDegradedOutcome"
        assert states["WeeklyExerciseLaunchFailed"]["Next"] == "CheckDegradedOutcome"

    def test_degraded_outcome_routes_on_the_flag(self, states):
        # config-I2767 (2026-07-16 incident): the flag is only assigned on
        # the degraded path, so the comparison MUST be IsPresent-guarded —
        # an unguarded dereference threw States.Runtime on every fully-green
        # day. Absent flag falls to Default NormalSucceeded.
        cdo = states["CheckDegradedOutcome"]
        assert cdo["Type"] == "Choice"
        c = cdo["Choices"][0]
        guard, comparison = c["And"]
        assert guard == {"Variable": "$.degraded_summary.degraded", "IsPresent": True}
        assert comparison["Variable"] == "$.degraded_summary.degraded"
        assert comparison["BooleanEquals"] is True
        # config#2857: both outcomes now route through their own
        # SF-envelope completion marker before their terminal state.
        # The degraded terminal is DegradedRun (Type: Fail, per Brian's
        # 2026-07-28 Option-A ruling, alpha-engine-config#2699) — the
        # marker still fires so the completion artifact exists.
        assert c["Next"] == "WriteCompletionMarkerDegraded"
        assert cdo["Default"] == "WriteCompletionMarkerNormal"
        assert states["WriteCompletionMarkerNormal"]["Next"] == "NormalSucceeded"
        assert states["WriteCompletionMarkerDegraded"]["Next"] == "DegradedRun"

    def test_degraded_run_is_a_fail_state(self, states):
        """Brian's 2026-07-28 Option-A ruling (alpha-engine-config#2699):
        the original DegradedSucceeded (Type: Succeed) left every status-keyed
        watcher seeing green — repeating the 2026-07-15 incident's failure mode.
        Replaced with Type: Fail so FAILED execution status engages sf-watch,
        EventBridge, and sf-telegram-notifier with zero new plumbing."""
        assert states["NormalSucceeded"]["Type"] == "Succeed"
        assert states["DegradedRun"]["Type"] == "Fail"
        assert states["DegradedRun"]["Error"] == "DegradedRun"
        # config-I6856: the terminal now DERIVES its Cause from
        # $.degraded_summary instead of carrying a static string. A static
        # Cause enumerated the paths that could reach it and drifted behind
        # them — 4 setters, 1 named on this definition. Depth of that guard
        # lives in tests/test_degraded_terminal_derives_its_cause.py; this
        # assertion holds only that the terminal still SAYS something.
        assert "CausePath" in states["DegradedRun"]

    def test_a_run_that_never_hits_the_gap_cannot_reach_degraded_run(self, states):
        # Structural sanity: DegradedRun is reachable ONLY via
        # WriteCompletionMarkerDegraded (the config#2857 marker), which
        # is reachable ONLY via CheckDegradedOutcome — there is no direct
        # edge from anywhere else in the file.
        producers = [
            name for name, st in states.items()
            if "DegradedRun" in _targets(st)
        ]
        assert producers == ["WriteCompletionMarkerDegraded"]


# ── Deliverable #3: closed self-heal loop ────────────────────────────────────


class TestHealLoopEligibility:
    def test_operator_replay_does_not_recurse(self, states):
        # A replay execution that still finds the precondition unmet must
        # page immediately, not spawn a further heal loop (replays run with
        # skip_post_market_data=true — no data-spot phase to retry).
        chle = states["CheckHealLoopEligible"]
        assert chle["Type"] == "Choice"
        c = chle["Choices"][0]
        # config-I2767: IsPresent-guarded — $.pipeline_role is an execution-
        # input key this SF never floors; absent falls to Default (live
        # treatment).
        guard, comparison = c["And"]
        assert guard == {"Variable": "$.pipeline_role", "IsPresent": True}
        assert comparison["Variable"] == "$.pipeline_role"
        assert comparison["StringEquals"] == "operator-replay"
        assert c["Next"] == "HealNonConvergent"
        assert chle["Default"] == "InitHealLoop"

    def test_set_degraded_flag_enters_eligibility_check(self, states):
        assert states["SetDegradedFlag"]["Next"] == "CheckHealLoopEligible"


class TestHealLoopBound:
    def test_init_starts_at_zero_attempts(self, states):
        st = states["InitHealLoop"]
        assert st["Type"] == "Pass"
        assert st["Result"] == {"attempts": 0}
        assert st["ResultPath"] == "$.heal_loop"
        assert st["Next"] == "HealInitialSignatureAvailable"

    def test_gate_trips_on_attempts_or_deadline(self, states):
        gate = states["HealLoopGate"]
        assert gate["Type"] == "Choice"
        c = gate["Choices"][0]
        assert "Or" in c
        # config-I2767: the deadline operand is IsPresent-guarded (absent →
        # operand false, loop stays bounded by attempts); the attempts
        # operand is provably floored by InitHealLoop/HealLoopIncrement.
        variables = set()
        for cond in c["Or"]:
            if "And" in cond:
                assert cond["And"][0]["IsPresent"] is True
                assert cond["And"][0]["Variable"] == cond["And"][1]["Variable"]
                variables.add(cond["And"][1]["Variable"])
            else:
                variables.add(cond["Variable"])
        assert variables == {"$.heal_loop.attempts", "$.precondition_probe.Payload.past_deadline"}
        assert c["Next"] == "HealNonConvergent"
        # alpha-engine-config-I11266: the actuator is HealStartCollection now.
        assert gate["Default"] == "HealMarkAttemptStarted"

    def test_attempts_bound_is_two(self, states):
        gate = states["HealLoopGate"]
        attempts_cond = next(
            c for c in gate["Choices"][0]["Or"] if c["Variable"] == "$.heal_loop.attempts"
        )
        assert attempts_cond["NumericGreaterThanEquals"] == 2

    def test_increment_advances_the_counter_and_loops_back(self, states):
        inc = states["HealLoopIncrement"]
        assert inc["Type"] == "Pass"
        assert inc["Parameters"]["attempts.$"] == "States.MathAdd($.heal_loop.attempts, 1)"
        assert inc["ResultPath"] == "$.heal_loop"
        assert inc["Next"] == "HealLoopGate"

    @pytest.mark.parametrize("failure_state", ["HealReProbe", "HealCheckConverged"])
    def test_every_failure_mode_in_the_loop_reaches_the_increment(self, states, failure_state):
        # No dead end anywhere in the act-reprobe chain — every non-success
        # branch must funnel back to HealLoopIncrement so the attempts/deadline
        # bound (not an unbounded retry) is what stops it.
        frontier = deque(_targets(states[failure_state]))
        visited = set()
        while frontier:
            name = frontier.popleft()
            if name in visited or name in {"HealNonConvergent", "HealDispatchReplay"}:
                continue
            visited.add(name)
            frontier.extend(_targets(states[name]))
        assert "HealLoopIncrement" in visited

    def test_every_actuator_outcome_is_reprobed(self, states):
        # alpha-engine-config-I11266: success, FAILED, TIMED_OUT and ABORTED
        # of the started collection all land on HealReProbe — the verdict is
        # the precondition artifact, never the collection's own status (it
        # fails loud on ANY unit; the precondition is the SPY close + macro
        # sentinel). HealReProbe's own Catch then reaches HealLoopIncrement.
        st = states["HealStartCollection"]
        assert set(_targets(st)) == {"HealReadFailureSignature"}
        assert [c["ErrorEquals"] for c in st["Catch"]] == [["States.ALL"]]

    def test_signature_read_is_not_a_substantive_registry_state(self, states):
        # The read feeds a comparison; it is not a pipeline stage. It uses the
        # SDK Lambda integration so no console registry row is owed
        # (aec#7764). A lambda:invoke Task here would need a nousergon-lib
        # STATE_TO_ARCHIVE_PAGE entry first.
        probe = states["HealReadFailureSignature"]
        assert probe["Resource"] == "arn:aws:states:::aws-sdk:lambda:invoke"
        assert probe["Parameters"]["FunctionName"] == "alpha-engine-collection-readiness-probe"
        assert probe["Catch"][0]["Next"] == "HealSetSignatureDegradedFlag"
        flag = states["HealSetSignatureDegradedFlag"]
        assert flag["ResultPath"] == "$.degraded_summary"
        assert flag["Parameters"]["degraded"] is True
        assert flag["Next"] == "HealReProbe"

    def test_fresh_signature_probe_excludes_pre_attempt_manifests(self, states):
        mark = states["HealMarkAttemptStarted"]
        assert mark["Next"] == "HealStartCollection"
        assert mark["Parameters"]["not_before.$"] == "$$.State.EnteredTime"
        payload = mark["Parameters"]["payload"]
        assert payload["not_before.$"] == "$$.State.EnteredTime"
        assert payload["lookback_seconds"] == 0
        # The attempt record is rebuilt on every attempt, so a previous
        # attempt's fresh read can never be compared against.
        assert mark["ResultPath"] == "$.heal_attempt"


# ── aec#7764: synthetic transition simulation ────────────────────────────────
#
# A small, honest walker over the real state machine definition. Task states
# are stubbed (they are AWS calls); Pass and Choice states execute from the
# JSON itself, so a rewiring of the gate/comparison changes what this observes.

_SIG = '[{"reason":"zero variance","unit":"D20"}]'


def _get(doc, path):
    cur = doc
    for part in path[2:].split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(path)
        cur = cur[part]
    return cur


def _put(doc, path, value):
    parts = path[2:].split(".")
    cur = doc
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value


def _eval_rule(rule, doc):
    if "And" in rule:
        return all(_eval_rule(r, doc) for r in rule["And"])
    if "Or" in rule:
        return any(_eval_rule(r, doc) for r in rule["Or"])
    var = rule["Variable"]
    try:
        value = _get(doc, var)
    except KeyError:
        if "IsPresent" in rule:
            return rule["IsPresent"] is False
        raise RuntimeError(f"States.Runtime: {var} absent without IsPresent guard")
    if "IsPresent" in rule:
        return rule["IsPresent"] is True
    if "StringGreaterThan" in rule:
        return isinstance(value, str) and value > rule["StringGreaterThan"]
    if "StringEqualsPath" in rule:
        return value == _get(doc, rule["StringEqualsPath"])
    if "BooleanEquals" in rule:
        return value is rule["BooleanEquals"]
    if "NumericGreaterThanEquals" in rule:
        return value >= rule["NumericGreaterThanEquals"]
    raise AssertionError(f"simulator does not model {rule}")


def _run_heal(states, *, initial_sig, attempts, probe_met=None, start_at="InitHealLoop",
              past_deadline=False, pipeline_role=None, read_raises=None, probe_raises=False):
    """Walk from ``start_at`` and return (final_state, launches, visited).

    ``attempts`` is a list of the fresh failure-signature value each heal
    collection leaves behind (``None`` = the probe response has no signature,
    e.g. a Lambda function error). ``probe_met`` is a per-attempt list of
    precondition_met verdicts for the re-probe.
    """
    doc = {"run_date": "2026-01-02", "degraded_summary": {"degraded": True},
           "collection_readiness": {"failure_signature": initial_sig},
           "precondition_probe": {"Payload": {"past_deadline": past_deadline,
                                              "precondition_met": False}}}
    if pipeline_role:
        doc["pipeline_role"] = pipeline_role
    launches = 0
    visited = []
    name = start_at
    clock = 0
    for _ in range(200):
        visited.append(name)
        st = states[name]
        if name == "HealNonConvergent" or name == "HealDispatchReplay":
            return name, launches, visited
        if name == "HealStartCollection":
            launches += 1
            name = st["Next"]
        elif name == "HealReadFailureSignature":
            idx = launches - 1
            if read_raises and idx in read_raises:
                name = st["Catch"][0]["Next"]
                continue
            sig = attempts[min(idx, len(attempts) - 1)]
            readiness = {} if sig is None else {"failure_signature": sig}
            _put(doc, st["ResultPath"], {"parsed": {"readiness": readiness}})
            name = st["Next"]
        elif name == "HealReProbe":
            idx = launches - 1
            if probe_raises:
                name = st["Catch"][0]["Next"]
                continue
            met = probe_met[min(idx, len(probe_met) - 1)] if probe_met else False
            doc["precondition_probe"] = {"Payload": {"precondition_met": met,
                                                      "past_deadline": past_deadline}}
            name = st["Next"]
        elif st["Type"] == "Pass":
            if "Result" in st:
                value = json.loads(json.dumps(st["Result"]))
            else:
                value = {}
                for k, v in st["Parameters"].items():
                    if k == "attempts.$":
                        value["attempts"] = _get(doc, "$.heal_loop.attempts") + 1
                    elif k.endswith(".$") and v == "$$.State.EnteredTime":
                        clock += 1
                        value[k[:-2]] = f"2026-01-02T20:00:0{clock}Z"
                    elif k.endswith(".$"):
                        value[k[:-2]] = _get(doc, v)
                    elif isinstance(v, dict):
                        value[k] = {kk[:-2] if kk.endswith(".$") else kk:
                                    (f"2026-01-02T20:00:0{clock}Z" if kk.endswith(".$") else vv)
                                    for kk, vv in v.items()}
                    else:
                        value[k] = v
            if "ResultPath" in st:
                _put(doc, st["ResultPath"], value)
            name = st["Next"]
        elif st["Type"] == "Choice":
            for choice in st["Choices"]:
                if _eval_rule(choice, doc):
                    name = choice["Next"]
                    break
            else:
                name = st["Default"]
        else:
            raise AssertionError(f"simulator reached unmodelled {name} ({st['Type']})")
    raise AssertionError(f"heal loop did not terminate: {visited[-12:]}")


class TestRepeatedWorkloadFailureShortCircuit:
    """aec#7764: a deterministic workload failure pages after ONE wasted
    relaunch instead of two, through the same HealNonConvergent state."""

    def test_identical_failure_pages_after_exactly_one_relaunch(self, states):
        final, launches, visited = _run_heal(states, initial_sig=_SIG, attempts=[_SIG])
        assert (final, launches) == ("HealNonConvergent", 1)
        assert visited.count("HealStartCollection") == 1
        assert "HealLoopIncrement" not in visited

    def test_previous_definition_would_have_relaunched_twice(self, states):
        # Red-before: take the shipped definition and remove the comparison
        # (HealCheckConverged's Default back to HealLoopIncrement, the old
        # wiring). The same scenario then burns the second relaunch.
        old = json.loads(json.dumps(states))
        old["HealCheckConverged"]["Default"] = "HealLoopIncrement"
        final, launches, _ = _run_heal(old, initial_sig=_SIG, attempts=[_SIG])
        assert (final, launches) == ("HealNonConvergent", 2)

    def test_differing_signature_keeps_both_attempts(self, states):
        other = '[{"reason":"vendor 429","unit":"D20"}]'
        final, launches, _ = _run_heal(states, initial_sig=_SIG, attempts=[other])
        assert (final, launches) == ("HealNonConvergent", 2)

    def test_signature_differing_on_attempt_two_after_a_match_cannot_happen_stale(self, states):
        # Attempt 1 differs, attempt 2 matches the initial: short-circuits on
        # attempt 2 (still the final attempt, same page).
        other = '[{"reason":"vendor 429","unit":"D20"}]'
        final, launches, _ = _run_heal(states, initial_sig=_SIG, attempts=[other, _SIG])
        assert (final, launches) == ("HealNonConvergent", 2)

    @pytest.mark.parametrize("initial", ["", None])
    def test_missing_initial_signature_never_short_circuits(self, states, initial):
        final, launches, visited = _run_heal(states, initial_sig=initial, attempts=[_SIG])
        assert (final, launches) == ("HealNonConvergent", 2)
        assert "HealCheckRepeatedFailure" in visited

    @pytest.mark.parametrize("fresh", ["", None])
    def test_missing_or_empty_fresh_signature_never_short_circuits(self, states, fresh):
        final, launches, _ = _run_heal(states, initial_sig=_SIG, attempts=[fresh])
        assert (final, launches) == ("HealNonConvergent", 2)

    def test_unreadable_signature_degrades_and_keeps_bounded_retry(self, states):
        final, launches, visited = _run_heal(
            states, initial_sig=_SIG, attempts=[_SIG], read_raises={0, 1})
        assert (final, launches) == ("HealNonConvergent", 2)
        assert "HealSetSignatureDegradedFlag" in visited

    def test_reprobe_error_keeps_bounded_retry(self, states):
        final, launches, visited = _run_heal(
            states, initial_sig=_SIG, attempts=[_SIG], probe_raises=True)
        assert (final, launches) == ("HealNonConvergent", 2)
        assert "HealCheckRepeatedFailure" not in visited

    def test_converged_precondition_still_dispatches_replay_despite_matching_signature(self, states):
        final, launches, _ = _run_heal(
            states, initial_sig=_SIG, attempts=[_SIG], probe_met=[True])
        assert (final, launches) == ("HealDispatchReplay", 1)

    def test_attempt_bound_still_enforced(self, states):
        # Non-matching failures still get exactly the two attempts (see the
        # differing-signature tests) and the gate constant is unchanged.
        gate = states["HealLoopGate"]
        assert gate["Choices"][0]["Or"][0]["NumericGreaterThanEquals"] == 2
        final, launches, _ = _run_heal(states, initial_sig="", attempts=[_SIG])
        assert (final, launches) == ("HealNonConvergent", 2)

    def test_deadline_still_enforced(self, states):
        final, launches, _ = _run_heal(
            states, initial_sig=_SIG, attempts=[_SIG], past_deadline=True)
        assert (final, launches) == ("HealNonConvergent", 0)

    def test_short_circuit_reaches_the_same_page_and_cleanup_as_exhaustion(self, states):
        # Both routes end in the one HealNonConvergent state (one SNS page,
        # one cleanup continuation); nothing new routes around it.
        into = {n for n, st in states.items() if "HealNonConvergent" in _targets(st)}
        assert {"HealLoopGate", "HealCheckRepeatedFailure", "CheckHealLoopEligible"} <= into
        assert states["HealNonConvergent"]["Resource"] == "arn:aws:states:::sns:publish"


class TestHealLoopDispatchChain:
    """alpha-engine-config-I11266: the heal actuator starts the standalone
    ne-data-collection-eod machine (startExecution.sync:2) instead of
    relaunching the two data spots itself — one writer implementation for the
    normal and the heal path. HealReProbe / HealCheckConverged /
    HealDispatchReplay and the HealLoopGate bound are unchanged."""

    _COLLECTION_ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-data-collection-eod"

    def test_actuator_starts_the_standalone_eod_collection_and_waits(self, states):
        st = states["HealStartCollection"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::states:startExecution.sync:2"
        assert st["Parameters"]["StateMachineArn"] == self._COLLECTION_ARN
        inp = st["Parameters"]["Input"]
        assert inp["collection"] == "eod"
        assert inp["workloads"] == ["post-market-data", "post-market-arctic-append"]
        assert inp["require_trading_day"] is False
        # Bounded: .sync never waits past the task timeout.
        assert 0 < st["TimeoutSeconds"] <= 3 * 3600
        # Its result must not clobber the probe verdict the gate reads.
        assert st["ResultPath"] == "$.heal_collection"

    def test_the_old_spot_relaunch_states_are_gone(self, states):
        left = sorted(n for n in states if n.startswith("Heal") and "Spot" in n)
        assert left == [], left

    def test_no_eod_state_invokes_the_data_spot_dispatcher(self, states):
        # alpha-engine-config-I11266: neither the heal loop nor the normal
        # path may launch a collection box from this SF any more.
        def invoking(fn):
            return sorted(
                n for n, st in states.items()
                if st.get("Resource") == "arn:aws:states:::lambda:invoke"
                and st.get("Parameters", {}).get("FunctionName") == fn
            )

        assert invoking(_DISPATCHER_FN) == []
        # Non-vacuity: the same scan does see this SF's real Lambda invokes.
        assert {"ProbeEODReconcilePrecondition", "HealReProbe"} <= set(invoking(_PROBE_FN))

    def test_reprobe_success_chains_to_convergence_check(self, states):
        assert states["HealReProbe"]["Next"] == "HealCheckConverged"

    def test_converged_choice_dispatches_the_replay(self, states):
        hcc = states["HealCheckConverged"]
        c = hcc["Choices"][0]
        # config-I2767: IsPresent-guarded — a partial probe payload falls to
        # Default checks a measured workload signature before the bounded loop.
        guard, comparison = c["And"]
        assert guard == {"Variable": "$.precondition_probe.Payload.precondition_met", "IsPresent": True}
        assert comparison["Variable"] == "$.precondition_probe.Payload.precondition_met"
        assert comparison["BooleanEquals"] is True
        assert c["Next"] == "HealDispatchReplay"
        assert hcc["Default"] == "HealCheckRepeatedFailure"


class TestHealDispatchReplay:
    """Deliverable #3(c): the auto-replay reuses I2700's PROVEN input shape
    (skip_post_market_data), as a SEPARATE self-referential execution.

    Since the I11269 split the replay no longer carries
    ``skip_capture_snapshot``: CaptureSnapshot lives in the 16:00
    ne-postclose-trading-pipeline and is not a state of this machine, so the
    flag would be an undeclared input no state reads."""

    def test_self_referential_start_execution(self, states):
        st = states["HealDispatchReplay"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::states:startExecution"
        assert st["Parameters"]["StateMachineArn.$"] == "$$.StateMachine.Id"

    def test_replay_input_matches_i2700_proven_shape(self, states):
        inp = states["HealDispatchReplay"]["Parameters"]["Input"]
        assert inp["pipeline_role"] == "operator-replay"
        assert inp["skip_post_market_data"] is True
        # No skip_capture_snapshot: since alpha-engine-config-I12220 this
        # machine's CaptureSnapshot is the missing-snapshot self-heal, which has
        # no skip gate (it opens no IB session when the snapshot exists), so
        # the key would still be a dead input.
        assert "skip_capture_snapshot" not in inp
        assert "CheckSkipCaptureSnapshot" not in states
        assert inp["run_date.$"] == "$.run_date"
        assert inp["trading_instance_id.$"] == "$.trading_instance_id"
        assert inp["ec2_instance_id.$"] == "$.ec2_instance_id"

    def test_replay_dispatch_failure_pages_not_silently_succeeds(self, states):
        st = states["HealDispatchReplay"]
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == "HealReplayDispatchFailed"

    def test_success_reaches_converged_notify(self, states):
        assert states["HealDispatchReplay"]["Next"] == "HealConvergedNotify"


class TestHealOutcomeNotifications:
    """The three SNS publishes downstream of the loop — each must be an SNS
    Task (loud, not a swallow) and each must eventually reach the
    cost-guard tail (StopTradingInstance) regardless of its own
    success/failure, mirroring every other best-effort-notify Catch already
    established in this file (PublishDataSpotFailureImmediate).

    alpha-engine-config-I2722 (2026-07-16): these used to route through
    CheckSkipDailySubstrateHealthCheck before reaching the cost-guard tail;
    that gate + the whole DailySubstrateHealthCheck chain (including
    PublishSubstrateHealthCheckDegradedAlert, the mirrored best-effort-notify
    precedent this docstring used to cite) were removed and re-homed as a
    standalone dashboard-box systemd timer, so these now route to
    StopTradingInstance directly."""

    # alpha-engine-config-I7586: HealConvergedNotify's expected substring moved
    # from "CONVERGED" to "replay dispatched". Not a test bent to fit the code —
    # the state fires on a fire-and-forget DISPATCH and cannot know whether the
    # replay converged, and under the deploy-refresh defect it never had. The
    # property this parametrization actually protects (three heal-outcome
    # notifications, three distinct subjects) is unchanged and still asserted.
    @pytest.mark.parametrize("state_name,subject_substr", [
        ("HealConvergedNotify", "replay dispatched"),
        ("HealReplayDispatchFailed", "REPLAY DISPATCH FAILED"),
        ("HealNonConvergent", "DID NOT CONVERGE"),
    ])
    def test_is_sns_publish_with_distinct_subject(self, states, state_name, subject_substr):
        st = states[state_name]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        assert subject_substr in st["Parameters"]["Subject"]
        assert 0 < len(st["Parameters"]["Subject"]) <= 100
        assert "\n" not in st["Parameters"]["Subject"]

    # HealConvergedNotify is not here: after a successful dispatch the replay
    # owns the box and stops it at its own end, so the parent routes around
    # the drain and the stop (2026-10-08 heal-replay stop race,
    # tests/test_heal_replay_owns_the_trading_box.py).
    @pytest.mark.parametrize("state_name", [
        "HealReplayDispatchFailed", "HealNonConvergent",
    ])
    def test_reaches_cost_guard_tail_on_success_and_on_sns_failure(self, states, state_name):
        # alpha-engine-config-I12020: the cost-guard tail is entered through
        # the bounded trader-reconcile drain (DrainTraderReconcile), every
        # edge of which ends at StopTradingInstance
        # (tests/test_sf_trader_reconcile_drain.py).
        # 2026-10-09: entered through the box-ownership check, whose owned
        # edge is the drain (tests/test_sf_pre_session_window_and_box_ownership.py).
        st = states[state_name]
        assert st["Next"] == "CheckBoxOwnedBeforeStop"
        catches = [c for c in st["Catch"] if c["ErrorEquals"] == ["States.ALL"]]
        assert len(catches) == 1
        assert catches[0]["Next"] == "CheckBoxOwnedBeforeStop"
        assert [c["Next"] for c in states["CheckBoxOwnedBeforeStop"]["Choices"]] == [
            "DrainTraderReconcile"
        ]

    def test_nonconvergent_never_reaches_a_halt_state(self, states):
        _HALT = {"HandleFailure", "FailExecution", "ForceStopInstance"}
        for tgt in _targets(states["HealNonConvergent"]):
            assert tgt not in _HALT


# TestIamGrants (probe Lambda grantable + self-start-execution grant) was
# ported to nous-ergon-ops — the SF role policy
# (infrastructure/iam/alpha-engine-step-functions-role.json) now lives there.
# The invariants are enforced in nous-ergon-ops/tests/ per the
# infra/drop-iam-moved-to-ops cleanup.

# ── Moved verbatim from test_sf_eod_substrate_check_wiring.py (deleted
# alpha-engine-config-I2722, 2026-07-16) — this coverage was never
# substrate-specific, it just happened to live in that file. ────────────────


class TestHandleFailureCostGuardHardening:
    """Pin the 2026-05-14 cost-guard hardening on ``HandleFailure``.

    Background: 2026-05-14 EOD recovery v2 SF execution failed at
    ``HandleFailure`` with `Invalid parameter: TopicArn Reason: An
    ARN must have at least 6 elements, not 5`. Root cause: the
    recovery input payload had a malformed ``sns_topic_arn`` (colon
    replaced with a space between ``us-east-1`` and the account ID).
    Because ``HandleFailure`` had no ``Catch``, the SNS publish
    failure aborted the whole SF before reaching ``ForceStopInstance``
    — leaving the trading EC2 running until manual stop. The state's
    own comment (`"Failure alert via SNS — instance still stops to
    avoid cost"`) was unenforced.

    Two-part fix:
    1. Hardcode the SNS topic ARN (no ``$.sns_topic_arn`` indirection)
       so a malformed input field can never block the cost-guard.
    2. Catch ``States.ALL`` on ``HandleFailure`` and route to
       ``ForceStopInstance`` so the cost-guard fires regardless of
       SNS-side failure (throttling, IAM drift, transient outage,
       future failure modes).
    """

    def test_topic_arn_is_literal_not_jsonpath(self, states):
        """Hardcoded ARN — no ``TopicArn.$`` indirection.

        A future PR that re-introduces the JSONPath form
        (``TopicArn.$``) would re-open the malformed-input attack
        surface that broke 2026-05-14 EOD recovery.
        """
        params = states["HandleFailure"]["Parameters"]
        assert "TopicArn" in params, (
            "HandleFailure.Parameters must include a literal 'TopicArn' field."
        )
        assert "TopicArn.$" not in params, (
            "HandleFailure must NOT use 'TopicArn.$' (JSONPath indirection) — "
            "the ARN is fixed and per-execution variability creates a "
            "corruption surface (2026-05-14 incident: malformed sns_topic_arn "
            "in recovery input → 'ARN must have at least 6 elements' → "
            "ForceStopInstance never fired → trading EC2 left running)."
        )
        # Spot-check the ARN shape — exactly 6 colon-separated parts,
        # SNS service, alpha-engine-alerts topic.
        arn = params["TopicArn"]
        parts = arn.split(":")
        assert len(parts) == 6, f"SNS ARN must have 6 parts; got {len(parts)}: {arn!r}"
        assert parts[:3] == ["arn", "aws", "sns"], f"Unexpected ARN prefix: {arn!r}"
        assert parts[5] == "alpha-engine-alerts", (
            f"ARN must point to alpha-engine-alerts topic; got {parts[5]!r}"
        )

    def test_handle_failure_has_catch_to_force_stop_instance(self, states):
        """HandleFailure must Catch States.ALL → ForceStopInstance.

        Defense-in-depth: even with the hardcoded ARN, any SNS-side
        failure (throttling, IAM drift, outage) must NOT block the
        cost-guard. The trading EC2 must always stop.
        """
        catches = states["HandleFailure"].get("Catch")
        assert catches, (
            "HandleFailure must define a 'Catch' block so SNS-side failures "
            "(throttling, IAM drift, outage) do NOT block ForceStopInstance. "
            "Without this, any publish failure leaves the trading EC2 running "
            "(2026-05-14 incident)."
        )
        all_catch = next(
            (c for c in catches if "States.ALL" in c.get("ErrorEquals", [])),
            None,
        )
        assert all_catch is not None, (
            "HandleFailure.Catch must include a 'States.ALL' branch — partial "
            "catches leave failure surfaces uncovered."
        )
        # 2026-10-09: the force-stop is ownership-gated — it stops a box this
        # execution started, never one another actor had up. Both HandleFailure
        # edges go through that check, whose owned edge is ForceStopInstance.
        assert all_catch["Next"] == "CheckBoxOwnedBeforeForceStop", (
            f"HandleFailure Catch must route to the ownership-gated "
            f"ForceStopInstance, not {all_catch['Next']!r}. The cost-guard is "
            "the load-bearing step; alert delivery is best-effort."
        )
        assert states["HandleFailure"]["Next"] == "CheckBoxOwnedBeforeForceStop"
        guard = states["CheckBoxOwnedBeforeForceStop"]
        assert [c["Next"] for c in guard["Choices"]] == ["ForceStopInstance"]

    def test_input_schema_no_longer_requires_sns_topic_arn(self, states):
        """Once the ARN is hardcoded, no state's Parameters or input/output
        path should reference ``$.sns_topic_arn`` — confirms the SF input
        schema can drop the field on the next manual recovery payload.

        Walks the JSON tree (rather than grepping the serialized text) so
        Comment fields that explain *why* the indirection was removed
        don't trip the test.
        """

        def _walk_for_jsonpath_use(node, path="$"):
            hits: list[str] = []
            if isinstance(node, dict):
                for k, v in node.items():
                    sub = f"{path}.{k}"
                    if k == "Comment":
                        # Comments document intent — they're allowed to
                        # mention ``$.sns_topic_arn`` historically.
                        continue
                    if isinstance(v, str) and v == "$.sns_topic_arn":
                        hits.append(sub)
                    elif k.endswith(".$") and isinstance(v, str) and "sns_topic_arn" in v:
                        hits.append(sub)
                    else:
                        hits.extend(_walk_for_jsonpath_use(v, sub))
            elif isinstance(node, list):
                for i, v in enumerate(node):
                    hits.extend(_walk_for_jsonpath_use(v, f"{path}[{i}]"))
            return hits

        live_uses = _walk_for_jsonpath_use(states)
        assert not live_uses, (
            "No state should bind to '$.sns_topic_arn' after the 2026-05-14 "
            "hardening — the ARN is hardcoded in HandleFailure and the input "
            "field is no longer needed. Found live JSONPath references at: "
            f"{live_uses}. A reintroduction means someone re-added the "
            "indirection and re-opened the corruption surface."
        )
