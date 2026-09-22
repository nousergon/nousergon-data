"""alpha-engine-config-I7111 — the market-hours boundary on both trading pipelines.

Brian ruled option (c) on 2026-08-13: the "no trading-pipeline start during
NYSE market hours" boundary belongs to the pipeline, not to one caller.

What it replaces. alpha-engine-config#2932 (2026-07-20) ruled the boundary onto
``alpha-engine-sf-watch-executor-role``'s ``states:StartExecution`` grant,
enforced by ``alpha-engine-sf-watch-market-hours-toggler`` — a Lambda flipping
that role's inline policy between two codified variants on a 5-minute schedule.
Measured 2026-08-12 in account 711398986525: no such function, no such
EventBridge rule. The Lambda was written, committed, and never bootstrapped, so
the boundary was **never once in force** in the three weeks it was believed to
be. It was also caller-scoped, so even fully deployed it could not have covered
the in-session operator starts at 09:32 PT on 2026-08-07 or 08:32 PT on
2026-07-27. IAM has no time-of-day condition key; that absence is why the ruled
mechanism needed a Lambda to simulate one, and why the simulation could
silently not exist.

What this pins:

  * the gate is the FIRST thing both pipelines do, ahead of the mutex;
  * the four verdicts route where the ruling says, and anything else fails
    closed — ``TestChoiceRouting`` evaluates the real ASL Choice rather than
    reading them;
  * the override is refusable-deliberately, not bypassable-accidentally;
  * the two pipelines' unverified-gate postures differ, on purpose, and the
    difference is asserted in both directions rather than left to a comment.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_PREOPEN = "step_function_daily.json"
_POSTCLOSE = "step_function_eod.json"
_BOTH = [_PREOPEN, _POSTCLOSE]

_GATE_STATES = {
    "MarketHoursGate",
    "MarketHoursGateChoice",
    "RecordMarketHoursOverride",
    "NotifyMarketHoursBlocked",
    "MarketHoursBlocked",
    "NotifyMarketHoursOverrideMalformed",
    "MarketHoursOverrideMalformed",
    "NotifyMarketHoursUnverified",
}


def _sf(name: str) -> dict:
    return json.loads((_INFRA / name).read_text())


@pytest.fixture(scope="module")
def defs() -> dict:
    return {name: _sf(name) for name in _BOTH}


# ---------------------------------------------------------------------------
# A minimal ASL Choice evaluator.
#
# Structural assertions can only say the rules are SHAPED right. The ruling is
# about what the pipeline DOES with a verdict, so the rules are executed here
# against the payloads alpha-engine-predictor-inference actually returns
# (crucible-predictor inference/trading_day_gate.py::check_market_hours,
# pinned there by tests/test_market_hours_gate.py).
#
# Only the operators this Choice uses are implemented — StringEquals and
# IsPresent. An unimplemented operator raises rather than silently evaluating
# false, so a future rule using one cannot quietly slip past this harness.
# ---------------------------------------------------------------------------

_UNSET = object()


def _resolve(path: str, doc: dict):
    assert path.startswith("$."), path
    cur = doc
    for part in path[2:].split("."):
        if not isinstance(cur, dict) or part not in cur:
            return _UNSET
        cur = cur[part]
    return cur


class StatesRuntime(Exception):
    """What real ASL raises when a comparator's path does not resolve.

    This harness previously modelled that case as "the rule does not match",
    so an absent verdict fell through to Default and
    test_an_absent_verdict_fails_closed passed — asserting a fail-closed
    behaviour the deployed pipeline did not have. On 2026-08-13 the preopen
    execution took that path for real: verdict absent, States.Runtime at
    MarketHoursGateChoice, execution dead 48s in with no SNS alert and no
    orders placed. `Default` catches an unrecognised verdict, never a missing
    one; only an IsPresent-guarded rule ordered ahead of the comparators does.
    """


def _matches(rule: dict, doc: dict) -> bool:
    if "Not" in rule:
        return not _matches(rule["Not"], doc)
    if "And" in rule:
        return all(_matches(r, doc) for r in rule["And"])
    if "Or" in rule:
        return any(_matches(r, doc) for r in rule["Or"])

    value = _resolve(rule["Variable"], doc)
    ops = [k for k in rule if k not in ("Variable", "Next", "Comment")]
    assert len(ops) == 1, f"expected exactly one comparator, got {ops}"
    op = ops[0]
    if op == "IsPresent":
        # The one operator defined on an absent path — that is its whole job.
        return (value is not _UNSET) is rule[op]
    if value is _UNSET:
        raise StatesRuntime(
            f"Invalid path '{rule['Variable']}': The choice state's condition "
            f"path references an invalid value."
        )
    if op == "StringEquals":
        return value == rule[op]
    raise NotImplementedError(
        f"{op} is not implemented in this harness — implement it rather than "
        "letting the rule evaluate as a silent false"
    )


def evaluate(choice: dict, doc: dict) -> str:
    for rule in choice["Choices"]:
        if _matches(rule, doc):
            return rule["Next"]
    return choice["Default"]


def _payload(verdict: str, **extra) -> dict:
    """The shape the gate Task writes to $.market_hours_gate."""
    return {"market_hours_gate": {"Payload": {"verdict": verdict, **extra}}}


# ---------------------------------------------------------------------------
# Composition — the gate is the first thing either pipeline does
# ---------------------------------------------------------------------------


class TestGateIsAtTheHead:
    def test_preopen_entry_state_hands_straight_to_the_gate(self, defs):
        d = defs[_PREOPEN]
        assert d["StartAt"] == "InitializeInput"
        # InitializeInput only layers defaults (it is where $.sns_topic_arn
        # comes from, which the gate's notify states publish to), so the gate
        # is the first state that can refuse.
        assert d["States"]["InitializeInput"]["Type"] == "Pass"
        assert d["States"]["InitializeInput"]["Next"] == "MarketHoursGate"

    def test_postclose_starts_at_the_gate(self, defs):
        assert defs[_POSTCLOSE]["StartAt"] == "MarketHoursGate"

    @pytest.mark.parametrize("name", _BOTH)
    def test_gate_precedes_the_mutex(self, defs, name):
        # Ordering is deliberate: a run refused for starting in-session must
        # not first take a minute-bucket mutex key it will never use.
        choice = defs[name]["States"]["MarketHoursGateChoice"]
        assert evaluate(choice, _payload("PROCEED")) == "CheckMutexRole"

    @pytest.mark.parametrize("name", _BOTH)
    def test_gate_precedes_every_state_that_spends(self, defs, name):
        # Nothing between the entry point and the gate may boot a box, send an
        # SSM command, launch a spot instance or invoke a worker Lambda.
        states = defs[name]["States"]
        entry = defs[name]["StartAt"]
        seen, cur = [], entry
        while cur != "MarketHoursGate":
            seen.append(cur)
            state = states[cur]
            assert state["Type"] == "Pass", (
                f"{name}: {cur} runs before MarketHoursGate and is a "
                f"{state['Type']}, not a Pass — the boundary must be ahead of "
                "anything that spends"
            )
            cur = state["Next"]
        assert len(seen) <= 1, seen


class TestGateTask:
    @pytest.mark.parametrize("name", _BOTH)
    def test_gate_invokes_the_calendar_action_on_the_predictor_lambda(self, defs, name):
        gate = defs[name]["States"]["MarketHoursGate"]
        assert gate["Type"] == "Task"
        assert gate["Resource"] == "arn:aws:states:::lambda:invoke"
        params = gate["Parameters"]
        assert params["FunctionName"] == "alpha-engine-predictor-inference:live"
        assert params["Payload"]["action"] == "check_market_hours"

    @pytest.mark.parametrize("name", _BOTH)
    def test_verdict_is_judged_on_the_execution_start_not_the_lambda_clock(
        self, defs, name
    ):
        # A property of the execution: deterministic, replayable, and immune to
        # a slow cold start pushing a refused run past the close.
        payload = defs[name]["States"]["MarketHoursGate"]["Parameters"]["Payload"]
        assert payload["now.$"] == "$$.Execution.StartTime"

    @pytest.mark.parametrize("name", _BOTH)
    def test_override_is_read_from_the_raw_execution_input(self, defs, name):
        # Passed WHOLE. An ASL "override.$": "$.market_hours_override"
        # parameter throws States.Runtime on every run that does not carry one
        # — i.e. on every normal day.
        payload = defs[name]["States"]["MarketHoursGate"]["Parameters"]["Payload"]
        assert payload["execution_input.$"] == "$$.Execution.Input"

    @pytest.mark.parametrize("name", _BOTH)
    def test_gate_declares_a_timeout_retry_and_catch(self, defs, name):
        gate = defs[name]["States"]["MarketHoursGate"]
        assert gate["TimeoutSeconds"] == 60
        assert gate["Retry"][0]["MaxAttempts"] == 3
        assert gate["Catch"][0]["ErrorEquals"] == ["States.ALL"]
        assert gate["ResultPath"] == "$.market_hours_gate"
        assert gate["Next"] == "MarketHoursGateChoice"


# ---------------------------------------------------------------------------
# The boundary itself
# ---------------------------------------------------------------------------


def _through_normalizers(states: dict, name: str) -> str:
    """Resolve a transition target past any pure-Pass normalizer in front of it.

    alpha-engine-config#5950 inserted floors that sit between an edge and its
    real destination, flooring the optional fields the destination dereferences.
    A Pass has no Choices, so it cannot change WHICH destination is reached —
    walking through it keeps these guards asserting the destination rather than
    widening them to accept any Pass, which is how a wrong destination would get
    in behind one.
    """
    seen = set()
    while (
        name in states
        and states[name].get("Type") == "Pass"
        and "Next" in states[name]
        and name not in seen
    ):
        seen.add(name)
        name = states[name]["Next"]
    return name


class TestChoiceRouting:
    @pytest.mark.parametrize("name", _BOTH)
    def test_a_closed_market_proceeds(self, defs, name):
        choice = defs[name]["States"]["MarketHoursGateChoice"]
        assert evaluate(choice, _payload("PROCEED")) == "CheckMutexRole"

    @pytest.mark.parametrize("name", _BOTH)
    def test_an_in_session_start_is_refused(self, defs, name):
        states = defs[name]["States"]
        assert (
            evaluate(states["MarketHoursGateChoice"], _payload("BLOCKED"))
            == "NotifyMarketHoursBlocked"
        )
        # …and the refusal is announced before it is terminal.
        notify = states["NotifyMarketHoursBlocked"]
        assert notify["Resource"] == "arn:aws:states:::sns:publish"
        assert notify["Next"] == "MarketHoursBlocked"
        assert states["MarketHoursBlocked"]["Type"] == "Fail"

    @pytest.mark.parametrize("name", _BOTH)
    def test_a_refused_run_never_reads_as_a_clean_success(self, defs, name):
        # sf-pipeline-policy §2.3. A Succeed-shaped skip (the shape
        # NotifyHolidaySkip legitimately uses for a holiday) would tell every
        # status-keyed watcher the pipeline ran.
        states = defs[name]["States"]
        for terminal in ("MarketHoursBlocked", "MarketHoursOverrideMalformed"):
            assert states[terminal]["Type"] == "Fail"
            assert states[terminal].get("End") is not True
        for notify in ("NotifyMarketHoursBlocked", "NotifyMarketHoursOverrideMalformed"):
            assert states[notify].get("End") is not True

    @pytest.mark.parametrize("name", _BOTH)
    def test_the_two_refusals_carry_distinct_errors(self, defs, name):
        # They call for different operator actions — wait for the close vs fix
        # the override — so a watcher must be able to tell them apart from the
        # Error alone.
        states = defs[name]["States"]
        assert states["MarketHoursBlocked"]["Error"] == "MarketHoursBoundary"
        assert (
            states["MarketHoursOverrideMalformed"]["Error"]
            == "MarketHoursOverrideMalformed"
        )

    @pytest.mark.parametrize("name", _BOTH)
    def test_an_absent_verdict_routes_to_the_unverified_path(self, defs, name):
        # config-I2767's lesson, which TradingDayGateChoice already carries: a
        # gate payload without its verdict is a contract violation by our own
        # Lambda. Proceeding to trade on an unverifiable answer is the one
        # outcome that must not happen.
        #
        # It must route through StampMarketHoursVerdictMissing rather than
        # Default: Default is unreachable on an absent path (real ASL raises
        # States.Runtime before it), and the states that report an unevaluable
        # gate all format $.market_hours_gate_error, which nothing sets on this
        # path because the Task's Catch never fired — the invoke SUCCEEDED.
        choice = defs[name]["States"]["MarketHoursGateChoice"]
        assert evaluate(choice, {}) == "StampMarketHoursVerdictMissing"
        assert (
            evaluate(choice, {"market_hours_gate": {"Payload": {}}})
            == "StampMarketHoursVerdictMissing"
        )

    @pytest.mark.parametrize("name", _BOTH)
    def test_the_missing_verdict_guard_is_ordered_first(self, defs, name):
        # Order is the whole mechanism: ASL evaluates rules in sequence and a
        # StringEquals against an unresolvable path raises immediately, so a
        # guard placed after the comparators never runs.
        first = defs[name]["States"]["MarketHoursGateChoice"]["Choices"][0]
        assert first["Not"]["IsPresent"] is True, first
        assert first["Not"]["Variable"] == "$.market_hours_gate.Payload.verdict"
        assert first["Next"] == "StampMarketHoursVerdictMissing"

    @pytest.mark.parametrize("name", _BOTH)
    def test_the_stamp_state_supplies_what_the_unverified_states_format(
        self, defs, name
    ):
        # The reason the guard cannot simply point at NotifyMarketHoursUnverified:
        # that state formats States.JsonToString($.market_hours_gate_error), and
        # on a missing path that call raises States.Runtime itself — swapping one
        # silent runtime death for another.
        states = defs[name]["States"]
        stamp = states["StampMarketHoursVerdictMissing"]
        assert stamp["Type"] == "Pass"
        assert stamp["ResultPath"] == "$.market_hours_gate_error"
        assert stamp["Result"]["Error"] == "MarketHoursGateContractViolation"
        assert stamp["Result"]["Cause"]
        # And it must hand off to a state that already exists in this pipeline.
        assert stamp["Next"] in states

    @pytest.mark.parametrize("name", _BOTH)
    def test_an_absent_verdict_reaches_this_pipelines_declared_posture(
        self, defs, name
    ):
        # The two pipelines answer an unevaluable gate differently and both
        # answers are deliberate: preopen REFUSES (a stale plan against a live
        # market), postclose proceeds DEGRADED (settlement must still happen).
        # An absent verdict must land on the same posture as a failed invoke,
        # not on a generic failure that ignores the distinction.
        states = defs[name]["States"]
        nxt = states["StampMarketHoursVerdictMissing"]["Next"]
        if name.endswith("daily.json"):
            assert nxt == "NotifyMarketHoursUnverified"
            assert states[nxt]["Next"] == "MarketHoursUnverified"
            assert states["MarketHoursUnverified"]["Type"] == "Fail"
        else:
            assert nxt == "SetMarketHoursUnverifiedDegraded"
            assert states[nxt]["Next"] == "NotifyMarketHoursUnverified"

    @pytest.mark.parametrize("name", _BOTH)
    def test_harness_raises_on_an_unguarded_comparator_like_real_asl(
        self, defs, name
    ):
        # Pins the harness fidelity this suite lacked. Without it, the absent
        # -verdict test above passes against a definition that dies with
        # States.Runtime in production — which is precisely what shipped.
        unguarded = {
            "Choices": [
                {
                    "Variable": "$.market_hours_gate.Payload.verdict",
                    "StringEquals": "PROCEED",
                    "Next": "CheckMutexRole",
                }
            ],
            "Default": "HandleFailure",
        }
        with pytest.raises(StatesRuntime, match="references an invalid value"):
            evaluate(unguarded, {"market_hours_gate": {"Payload": {}}})

    @pytest.mark.parametrize("name", _BOTH)
    def test_an_unrecognised_verdict_fails_closed(self, defs, name):
        # config#5950: on the EOD pipeline the Default now passes through
        # NormalizeEODFailureContext, which floors $.error for HandleFailure —
        # that Default was one of three edges reaching the failure reporter
        # without setting the field it formats. Fails closed exactly as before.
        states = defs[name]["States"]
        choice = states["MarketHoursGateChoice"]
        for junk in ("proceed", "PROCEED ", "OK", "", "MAYBE"):
            assert _through_normalizers(
                states, evaluate(choice, _payload(junk))
            ) == "HandleFailure", junk

    # The producer's verdicts, enumerated in crucible-predictor
    # inference/trading_day_gate.py::check_market_hours. One added there
    # without a rule here would land on Default (fail closed) — safe, but
    # this asserts the known ones are each explicitly handled rather than
    # falling through.
    _UNIVERSAL_VERDICTS = {
        "PROCEED",
        "PROCEED_OVERRIDE",
        "BLOCKED",
        "OVERRIDE_MALFORMED",
    }

    @pytest.mark.parametrize("name", _BOTH)
    def test_every_verdict_the_lambda_can_emit_is_routed(self, defs, name):
        choice = defs[name]["States"]["MarketHoursGateChoice"]
        handled = {
            c["StringEquals"] for c in choice["Choices"] if "StringEquals" in c
        }
        expected = set(self._UNIVERSAL_VERDICTS)
        if name == _PREOPEN:
            # alpha-engine-config-I11384. PROCEED_REMEDIATION is preopen-ONLY,
            # and the asymmetry is the contract rather than an omission: the
            # postclose chain stops the box and reconciles the book, and what
            # an unfilled book should mean there is a different question that
            # has not been answered. The Lambda enforces the same split from
            # its own side — it is handed `pipeline` as a LITERAL in each
            # state machine's gate payload and only ever emits this verdict
            # for "preopen" — so a postclose rule here would be dead code
            # AND a claim the producer does not make.
            expected.add("PROCEED_REMEDIATION")
        assert handled == expected

    def test_the_postclose_chain_does_not_route_a_remediation(self, defs):
        # The other half of the asymmetry, asserted rather than left implicit:
        # if PROCEED_REMEDIATION ever reached the postclose Choice it must
        # fail CLOSED on Default, not find a rule nobody reasoned about.
        states = defs[_POSTCLOSE]["States"]
        assert _through_normalizers(
            states,
            evaluate(states["MarketHoursGateChoice"], _payload("PROCEED_REMEDIATION")),
        ) == "HandleFailure"


class TestOverrideIsRecorded:
    @pytest.mark.parametrize("name", _BOTH)
    def test_an_authorised_in_session_start_proceeds_through_a_notify(self, defs, name):
        states = defs[name]["States"]
        assert (
            evaluate(states["MarketHoursGateChoice"], _payload("PROCEED_OVERRIDE"))
            == "RecordMarketHoursOverride"
        )
        record = states["RecordMarketHoursOverride"]
        assert record["Resource"] == "arn:aws:states:::sns:publish"
        assert record["Next"] == "CheckMutexRole"

    @pytest.mark.parametrize("name", _BOTH)
    def test_the_record_carries_the_whole_gate_payload(self, defs, name):
        # Who authorised it, why, and until when — not just "overridden".
        message = defs[name]["States"]["RecordMarketHoursOverride"]["Parameters"][
            "Message.$"
        ]
        assert "States.JsonToString($.market_hours_gate.Payload)" in message
        assert "$$.Execution.Id" in message

    @pytest.mark.parametrize("name", _BOTH)
    def test_a_notify_failure_does_not_convert_an_approval_into_a_refusal(
        self, defs, name
    ):
        # The authorisation was validated and is already in execution history;
        # SNS is the announcement, not the record.
        catch = defs[name]["States"]["RecordMarketHoursOverride"]["Catch"][0]
        assert catch["Next"] == "CheckMutexRole"

    @pytest.mark.parametrize("name", _BOTH)
    def test_a_rejected_override_is_announced_before_it_is_terminal(self, defs, name):
        states = defs[name]["States"]
        assert (
            evaluate(states["MarketHoursGateChoice"], _payload("OVERRIDE_MALFORMED"))
            == "NotifyMarketHoursOverrideMalformed"
        )
        notify = states["NotifyMarketHoursOverrideMalformed"]
        assert notify["Resource"] == "arn:aws:states:::sns:publish"
        assert notify["Next"] == "MarketHoursOverrideMalformed"
        assert notify["Catch"][0]["Next"] == "MarketHoursOverrideMalformed"

    @pytest.mark.parametrize("name", _BOTH)
    def test_both_refusal_messages_state_how_to_authorise_one_start(self, defs, name):
        # An operator who hits the boundary at 09:32 on a broken morning
        # should not have to find the runbook.
        states = defs[name]["States"]
        for notify in ("NotifyMarketHoursBlocked", "NotifyMarketHoursOverrideMalformed"):
            msg = states[notify]["Parameters"]["Message.$"]
            assert "market_hours_override" in msg
            assert "reason" in msg and "authorized_by" in msg and "expires_at" in msg
            assert "24h" in msg


class TestUnverifiedPostureDiffersOnPurpose:
    """The two pipelines take OPPOSITE routes when the gate cannot be
    evaluated. Asserted in both directions so neither can be "made
    consistent" with the other by a later sweep without reading why."""

    def test_preopen_fails_closed(self, defs):
        states = defs[_PREOPEN]["States"]
        assert (
            states["MarketHoursGate"]["Catch"][0]["Next"]
            == "NotifyMarketHoursUnverified"
        )
        assert states["NotifyMarketHoursUnverified"]["Next"] == "MarketHoursUnverified"
        assert states["MarketHoursUnverified"]["Type"] == "Fail"
        assert states["MarketHoursUnverified"]["Error"] == "MarketHoursGateUnverified"

    def test_preopen_fail_closed_costs_no_day_that_was_not_already_lost(self, defs):
        # The load-bearing fact, asserted rather than asserted-in-prose: the
        # gate and PredictorInference are the SAME Lambda. If it cannot answer
        # the gate it cannot produce predictions either, so refusing here
        # forfeits nothing — and it keeps the boundary from evaporating
        # whenever its evaluator is down, which is precisely the failure class
        # I7111 was filed for.
        states = defs[_PREOPEN]["States"]
        gate_fn = states["MarketHoursGate"]["Parameters"]["FunctionName"]
        inference_fn = states["PredictorInference"]["Parameters"]["FunctionName"]
        assert gate_fn.split(":")[0] == inference_fn.split(":")[0]

    def test_postclose_fails_open_degraded(self, defs):
        # sf-pipeline-policy §1.3: NAV continuity is non-negotiable — every
        # trading day terminates with a postclose completion marker and an
        # eod_pnl.csv row. Nothing else in this pipeline touches the predictor
        # Lambda, so refusing here would newly break settlement on an outage
        # otherwise irrelevant to it.
        states = defs[_POSTCLOSE]["States"]
        assert (
            states["MarketHoursGate"]["Catch"][0]["Next"]
            == "SetMarketHoursUnverifiedDegraded"
        )
        degraded = states["SetMarketHoursUnverifiedDegraded"]
        assert degraded["Type"] == "Pass"
        assert degraded["ResultPath"] == "$.degraded_summary"
        assert degraded["Parameters"]["degraded"] is True
        assert degraded["Next"] == "NotifyMarketHoursUnverified"
        assert states["NotifyMarketHoursUnverified"]["Next"] == "CheckMutexRole"

    def test_postclose_degradation_reaches_the_terminal_selector(self, defs):
        # A settlement run that could not verify its own start boundary is not
        # a clean success. $.degraded_summary is the exact path
        # CheckDegradedOutcome dereferences.
        states = defs[_POSTCLOSE]["States"]
        variables = {
            c["Variable"]
            for rule in states["CheckDegradedOutcome"]["Choices"]
            for c in rule.get("And", [rule])
            if "Variable" in c
        }
        assert any(v.startswith("$.degraded_summary") for v in variables)

    def test_postclose_has_no_unverified_fail_state(self, defs):
        assert "MarketHoursUnverified" not in defs[_POSTCLOSE]["States"]

    def test_preopen_has_no_unverified_degraded_state(self, defs):
        assert "SetMarketHoursUnverifiedDegraded" not in defs[_PREOPEN]["States"]


class TestNotifyStatesAreSafe:
    @pytest.mark.parametrize("name", _BOTH)
    def test_every_gate_notify_declares_a_timeout_and_a_catch(self, defs, name):
        states = defs[name]["States"]
        for state_name, state in states.items():
            if state_name not in _GATE_STATES or state.get("Type") != "Task":
                continue
            assert state["TimeoutSeconds"] == 60, state_name
            assert state["Catch"][0]["ErrorEquals"] == ["States.ALL"], state_name

    def test_postclose_notifies_a_hardcoded_topic(self, defs):
        # Mirrors HandleFailure in the same file: the EOD SF is started by the
        # daemon and by operators with hand-written input, so a malformed
        # sns_topic_arn field must never be able to silence a refusal.
        for name in _GATE_STATES:
            state = defs[_POSTCLOSE]["States"].get(name, {})
            if state.get("Resource") != "arn:aws:states:::sns:publish":
                continue
            assert state["Parameters"]["TopicArn"].endswith(":alpha-engine-alerts")
            assert "TopicArn.$" not in state["Parameters"]

    def test_preopen_notifies_the_layered_topic(self, defs):
        # InitializeInput guarantees $.sns_topic_arn before the gate runs, so
        # the preopen gate uses it the way TradingDayGateFailed does.
        for name in _GATE_STATES:
            state = defs[_PREOPEN]["States"].get(name, {})
            if state.get("Resource") != "arn:aws:states:::sns:publish":
                continue
            assert state["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"


class TestNoOtherGuardAlreadyCoveredThis:
    """Recorded as assertions because both guards were, at different points in
    the I7111 analysis, assumed to cover the time-of-day case. Neither does."""

    @pytest.mark.parametrize("name", _BOTH)
    def test_the_mutex_is_a_same_minute_guard_not_a_session_guard(self, defs, name):
        key = defs[name]["States"]["AcquireMutex"]["Parameters"]["Item"]["mutex_key"][
            "S.$"
        ]
        # Keyed on the execution's UTC minute bucket — two runs a minute apart
        # both acquire, whatever the ET clock says.
        assert "$$.Execution.StartTime" in key
        assert "market" not in key.lower()

    def test_the_trading_day_gate_is_a_calendar_day_guard(self, defs):
        params = defs[_PREOPEN]["States"]["TradingDayGate"]["Parameters"]
        assert params["Payload"]["action"] == "check_trading_day"
        assert params["Payload"].get("now") is None


class TestRemediationIsRecorded:
    """alpha-engine-config-I11384 — the same-day repair path.

    Brian, 2026-09-22: a fix affecting intraday trading is deployed and rerun
    the SAME session, never held to the close. The gate refused exactly that
    on 2026-09-22 at 11:45 ET, on a fix already merged and verified, and
    needed a hand-typed override to proceed.
    """

    def test_a_remediation_proceeds_through_a_notify_not_straight_through(
        self, defs
    ):
        states = defs[_PREOPEN]["States"]
        nxt = evaluate(
            states["MarketHoursGateChoice"], _payload("PROCEED_REMEDIATION")
        )
        assert nxt == "RecordMarketHoursRemediation", (
            "a crossed boundary is announced, never silent — the same rule "
            "PROCEED_OVERRIDE has carried since I7111"
        )
        record = states[nxt]
        assert record["Resource"] == "arn:aws:states:::sns:publish"
        assert record["Next"] == "CheckMutexRole"

    def test_a_notify_failure_does_not_convert_a_repair_into_a_refusal(self, defs):
        # The verdict was already decided and is in execution history; SNS is
        # the announcement, not the record.
        record = defs[_PREOPEN]["States"]["RecordMarketHoursRemediation"]
        catch = record["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "CheckMutexRole"

    def test_the_evidence_travels_into_the_announcement(self, defs):
        # A page saying "a boundary was crossed" without saying what granted
        # it is an alert nobody can act on.
        record = defs[_PREOPEN]["States"]["RecordMarketHoursRemediation"]
        msg = record["Parameters"]["Message.$"]
        assert "$.market_hours_gate.Payload" in msg
        assert "$$.Execution.Id" in msg

    def test_it_is_not_terminal_and_not_a_succeed(self, defs):
        # sf-pipeline-policy §2.3 — the same shape the refusal states carry.
        record = defs[_PREOPEN]["States"]["RecordMarketHoursRemediation"]
        assert record["Type"] == "Task"
        assert record.get("End") is not True

    def test_the_postclose_chain_has_no_such_state(self, defs):
        assert "RecordMarketHoursRemediation" not in defs[_POSTCLOSE]["States"]


class TestPipelineLiteralIsUnspoofable:
    """`pipeline` decides eligibility, so it may never come from the caller."""

    def test_each_definition_declares_its_own_pipeline(self, defs):
        assert (
            defs[_PREOPEN]["States"]["MarketHoursGate"]["Parameters"]["Payload"][
                "pipeline"
            ]
            == "preopen"
        )
        assert (
            defs[_POSTCLOSE]["States"]["MarketHoursGate"]["Parameters"]["Payload"][
                "pipeline"
            ]
            == "postclose"
        )

    @pytest.mark.parametrize("name", _BOTH)
    def test_it_is_a_literal_never_a_path_from_execution_input(self, defs, name):
        payload = defs[name]["States"]["MarketHoursGate"]["Parameters"]["Payload"]
        assert "pipeline.$" not in payload, (
            "a `.$` form would read it from the execution, letting any caller "
            "claim to be the preopen chain and unlock PROCEED_REMEDIATION"
        )
        assert isinstance(payload["pipeline"], str)
