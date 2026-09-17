"""A stage timeout that can never bind is not a budget (config-I6855).

`sf-pipeline-policy.md` §4: every stage carries a declared timeout sized to
observed p95 x 1.5, and a stage with no declared timeout is a defect.

There is a second way to have no budget, and it is invisible: declare one the
service can never reach. A `lambda:invoke` task has TWO ceilings — the state's
`TimeoutSeconds` and the function's own `Timeout` — and only the SMALLER one
ever fires. Where the state's value is the larger, it describes nothing.

Measured 2026-08-11 on `ne-preopen-trading-pipeline`. The `Scanner` state
declared `TimeoutSeconds: 600`; `alpha-engine-research-scanner` carried a
300s function timeout. Both invocations died at exactly 300.00s with
`Sandbox.Timedout`, the preopen pipeline terminated DEGRADED, and that day's
universe board, membership, leaderboard and trajectory were never written.
The 600 had never been reachable, so nobody reviewing the definition could
see the real budget — it lived in another repo's deploy script.

Which direction is correct is not symmetric:

  SF <  function   LEGITIMATE. The state's value binds, and a shared
                   multi-purpose Lambda (alpha-engine-predictor-inference is
                   invoked by nine states at budgets from 60s to 900s)
                   deliberately carries per-call budgets below its ceiling.
                   An overrun surfaces as States.Timeout naming the state.
  SF == function   RACE. Whichever fires first is undefined, so the error
                   type an operator sees is not reproducible.
  SF >  function   DEFECT *unless* the function sits at Lambda's 900s
                   SERVICE MAXIMUM. Normally the declaration is decorative
                   and the function's ceiling silently governs. But when the
                   function is already at 900 there is no larger value to
                   give it, and the choice inverts: SF slightly ABOVE 900
                   makes the LAMBDA's timeout fire first, which yields a
                   REPORT line with a duration and the function's logs.
                   SF below 900 makes States.Timeout abort the state while
                   the function keeps running — billed, orphaned, and
                   silent. A guard band above a service-max function is a
                   deliberate choice, not a decorative declaration.

So this file asserts `TimeoutSeconds < function timeout`, and that one is
declared at all — with `_SERVICE_MAX_GUARD_BAND` carved out below.

WHY THE FUNCTION TIMEOUTS ARE CODIFIED HERE rather than read from AWS: this
runs in CI without credentials, and the functions are deployed from four
other repositories. `_FUNCTION_TIMEOUTS_SEC` is therefore a claim about the
live account that can go stale — `test_the_codified_function_timeouts_match_live`
is skipped without credentials and is what proves it has not.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
from pathlib import Path
from typing import Iterator

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
sys.path.insert(0, str(_REPO_ROOT))

from infrastructure.sf_definitions import (  # noqa: E402
    CODIFIED_FUNCTION_TIMEOUTS_SEC,
    DEFINITION_FILES as _DEFS,
    LAMBDA_SERVICE_MAX_SEC as _LAMBDA_SERVICE_MAX_SEC,
    lambda_invoke_states as _shared_lambda_invoke_states,
)

# The codified live-timeout table moved to `infrastructure/sf_definitions.py`
# (alpha-engine-config-I9702 arc, 2026-09-01). It is a claim about the live AWS
# account, and the test that proves it — `test_the_codified_function_timeouts_
# match_live` — is skipped without credentials, which is every CI run of this
# suite. Set NOWHERE in any workflow, it had therefore never executed once.
# Living in a module rather than a test file lets the daily drift workflow, which
# does hold credentials, run the same comparison against the same declaration:
# `infrastructure/step-functions/check-lambda-timeout-drift.py`.
_FUNCTION_TIMEOUTS_SEC = CODIFIED_FUNCTION_TIMEOUTS_SEC

# States whose SF budget is >= the function timeout, measured 2026-08-11.
#
# NOT fixed here. Each needs its stage runtime looked at before a number is
# chosen — sf-pipeline-policy.md §4: "Timeouts are budgets, not
# accommodations. Raise only with a stated reason for the new baseline."
# Picking values to make a test pass is the move that policy forbids, and
# most of these sit on the weekly pipeline, whose behaviour this arc did not
# measure. Tracked as alpha-engine-config-I6897 with the full table.
#
# Pinned as an exact set: the gap cannot widen while that issue is open, and
# an entry cannot outlive its fix without failing.
_KNOWN_UNBOUND_EXPIRY = "2026-10-01"

_KNOWN_UNBOUND: frozenset[tuple[str, str]] = frozenset(
    {
        ("step_function.json", "SignalsEnvelope"),
        ("step_function.json", "ChallengerShadow"),
        ("step_function.json", "EvalJudgeSubmitFirstSaturday"),
        ("step_function.json", "EvalJudgeSubmitWeekly"),
        # EvalRollingMean REMOVED 2026-08-28 (alpha-engine-config#9102). It is
        # now a declared _SERVICE_MAX_GUARD_BAND entry below, not an unbound
        # one: the function moves to Lambda's 900s maximum and its handler
        # self-deadlines, which is the rule's second branch.
        ("step_function.json", "RationaleClustering"),
        ("step_function.json", "Counterfactual"),
        ("step_function.json", "DispatchWeeklyFreshnessSpot"),
        ("step_function_daily.json", "PredictorInference"),
        ("step_function_daily.json", "ReinvokePredictor"),
        ("step_function_eod.json", "ProbeEODReconcilePrecondition"),
        ("step_function_eod.json", "HealReProbe"),
    }
)


# States deliberately declaring a guard band ABOVE a service-max function.
# Not exceptions to the rule; instances of the rule's second branch.
_SERVICE_MAX_GUARD_BAND: dict[tuple[str, str], int] = {
    # alpha-engine-evaluator-director is pinned at the 900s service maximum
    # (crucible-evaluator-PR196, 2026-08-13: the measured requirement is ~195s
    # and the real defect was multiplicative retry loops, not call size —
    # 2 transport x 2 krepis body-level x 3 evaluator = up to 12 model calls
    # against a budget funding 2). 930 gives the function 30s to time out and
    # emit its REPORT line before the state would abort it.
    ("step_function.json", "Director"): 930,
    # ReplayConcordance LEFT this table under alpha-engine-config-I10539
    # (Brian ruling 2026-09-16, option b), which retired the stage from
    # ne-weekly-freshness-pipeline. It is removed rather than left behind for
    # the same reason EvalJudgeProcess is, stated below: a guard-band entry
    # naming a state this walk cannot reach exempts nothing while reading as
    # coverage. The rule it carried (a self-deadlining function needs an SF
    # ceiling ABOVE its own, alpha-engine-config-I7181) is unchanged and is
    # still stated on EvalRollingMean.
    # EvalJudgeProcess LEFT this table on 2026-08-29 (alpha-engine-config-I9329)
    # and did not move to _KNOWN_UNBOUND: it is no longer a lambda:invoke at
    # all. As an ssm:sendCommand it is governed by the INVERSE rule — its
    # executionTimeout must sit strictly BELOW its TimeoutSeconds — which the
    # sendCommand section at the bottom of this file enforces. A guard band
    # entry surviving here would have exempted a state that no longer exists
    # in this walk, i.e. exempted nothing while reading as coverage.
    # alpha-engine-research-eval-rolling-mean joins that class in the
    # alpha-engine-config-I9102 arc. MEASURED, from log stream
    # 2026/08/28/[379]2195e7f6733c410eae3c42e205dc3e59: the stage emitted its
    # rolling mean (the primary deliverable) 1.6s into the invocation, logged
    # its control bands at 22:25:53.652, and then produced nothing for 298s
    # until the 300s function wall — inside
    # scripts.build_agent_quality -> evals.judge_outcome_ic.open_research_db,
    # which downloads a 356 MB SQLite snapshot into a 512 MB function. The SF
    # state raised States.Timeout, the research/predictor branch fail-opened,
    # and the weekly run terminated FAILED for a stage that had succeeded.
    #
    # The fix is in the handler, not here: the four secondary aggregations
    # bolted onto this stage now run under `invocation_budget.run_bounded`
    # (crucible-research), so the stage returns its primary deliverable on its
    # own budget and records an overrunning block as TIMEOUT. That makes it
    # self-deadlining, and a self-deadlining function must not be pre-empted by
    # the state that invoked it — same reasoning as the two entries above, and
    # the same 60s band over a measured ~3.9s Init Duration.
    ("step_function.json", "EvalRollingMean"): 960,
}


def _lambda_invoke_states(definition: str) -> Iterator[tuple[str, str, int | None]]:
    """``(state_name, function_base_name, TimeoutSeconds)`` per ORDERING-GOVERNED state.

    Delegates to ``infrastructure.sf_definitions``, which is the single walk
    ``check-lambda-existence.py`` also uses. This test previously carried its
    own copy, and that copy was wrong in three ways at once — see that module's
    docstring for the full account. The short version: a full-ARN
    ``FunctionName`` parsed to the literal string ``"function"``, the
    completeness guard below whitelisted exactly that mis-parse, and the
    ``.waitForTaskToken``/``.sync`` resource variants were not matched at all.
    Four states were therefore exempt from this file for its whole life, three
    of them on the preopen trading pipeline, and a fourth definition
    (``step_function_groom.json``) was never walked here at all.

    ``.waitForTaskToken`` states are filtered out because for them the state
    timeout and the function timeout bound DIFFERENT waits — the function
    returns after handing off a token and the state waits for someone else to
    call ``SendTaskSuccess`` — so the ordering rule does not apply. That is a
    semantic exclusion, not an exemption, and it lives in
    ``LambdaInvokeState.is_ordering_governed`` so no caller re-derives it.
    """
    for state in _shared_lambda_invoke_states(definition):
        if not state.is_ordering_governed:
            continue
        yield state.state_name, state.normalized_name, state.timeout_seconds


def _states(definition: str) -> dict:
    return json.loads((_INFRA / definition).read_text(encoding="utf-8"))["States"]


@pytest.mark.parametrize("definition", _DEFS)
def test_every_lambda_invoke_declares_a_timeout(definition: str) -> None:
    """§4: a stage with no declared timeout inherits the definition ceiling.

    Its hang is then indistinguishable from a slow run until everything
    times out together.
    """
    missing = sorted(
        name
        for name, _, timeout in _lambda_invoke_states(definition)
        if timeout is None and (definition, name) not in _KNOWN_UNBOUND
    )
    assert not missing, f"{definition}: lambda:invoke states with no TimeoutSeconds: {missing}"


@pytest.mark.parametrize("definition", _DEFS)
def test_the_declared_stage_timeout_is_the_one_that_binds(definition: str) -> None:
    """`TimeoutSeconds` strictly below the function's own timeout.

    Equal is a race — whichever ceiling fires first is undefined, so the
    error an operator sees is not reproducible. Greater is decorative.
    """
    violations: list[str] = []
    for name, fn, timeout in _lambda_invoke_states(definition):
        if (definition, name) in _KNOWN_UNBOUND:
            continue
        fn_timeout = _FUNCTION_TIMEOUTS_SEC.get(fn)
        if fn_timeout is None:
            continue  # covered by test_every_invoked_function_has_a_codified_timeout
        if timeout is None:
            continue  # covered by test_every_lambda_invoke_declares_a_timeout
        band = _SERVICE_MAX_GUARD_BAND.get((definition, name))
        if band is not None:
            # The rule's second branch: the function is at Lambda's service
            # maximum and cannot be raised, so the guard band is deliberate.
            # Still pinned to an exact value — a band is a declared number, and
            # a drifting one is back to being decorative.
            assert fn_timeout == _LAMBDA_SERVICE_MAX_SEC, (
                f"{name} is carved out as a service-max guard band, but {fn} "
                f"is {fn_timeout}s, not the {_LAMBDA_SERVICE_MAX_SEC}s maximum "
                f"— raise the function instead, and drop the carve-out"
            )
            assert timeout == band, (
                f"{name}: TimeoutSeconds={timeout}, expected the declared "
                f"guard band {band}"
            )
            continue
        if timeout >= fn_timeout:
            violations.append(
                f"{name}: TimeoutSeconds={timeout} >= {fn}'s own {fn_timeout}s "
                f"({'race' if timeout == fn_timeout else 'never binds'})"
            )
    assert not violations, f"{definition}:\n  " + "\n  ".join(violations)


@pytest.mark.parametrize("definition", _DEFS)
def test_every_invoked_function_has_a_codified_timeout(definition: str) -> None:
    """A function missing from the map is an unchecked stage, not a passing one.

    Without this, adding a Lambda no one codified would make the ordering
    test silently skip it — the same shape as the whitelist that made the
    weekday notifier render nothing (config-I6857).
    """
    unknown = sorted(
        {
            fn
            for _, fn, _ in _lambda_invoke_states(definition)
            # No name-shaped carve-out here. This filter used to read
            # `and not fn.startswith("function")`, which excluded precisely the
            # states whose full-ARN FunctionName the old parser reduced to the
            # literal string "function" — the guard against silent exemption
            # was the thing granting it.
            if fn not in _FUNCTION_TIMEOUTS_SEC
        }
    )
    assert not unknown, (
        f"{definition} invokes functions absent from _FUNCTION_TIMEOUTS_SEC: {unknown}. "
        "Add each with its measured `aws lambda get-function-configuration --query Timeout`."
    )


def test_the_known_unbound_set_contains_no_stale_entries() -> None:
    """An exception that outlives its fix silently exempts a healthy stage."""
    live = {(d, name) for d in _DEFS for name, _, _ in _lambda_invoke_states(d)}
    stale = sorted(entry for entry in _KNOWN_UNBOUND if entry not in live)
    assert not stale, f"_KNOWN_UNBOUND names states that no longer exist: {stale}"

    still_broken = set()
    for definition, name in _KNOWN_UNBOUND:
        for state, fn, timeout in _lambda_invoke_states(definition):
            if state != name:
                continue
            fn_timeout = _FUNCTION_TIMEOUTS_SEC.get(fn)
            if timeout is None or (fn_timeout is not None and timeout >= fn_timeout):
                still_broken.add((definition, name))
    fixed = sorted(set(_KNOWN_UNBOUND) - still_broken)
    assert not fixed, (
        f"these now bind correctly — remove them from _KNOWN_UNBOUND: {fixed}. "
        "Close alpha-engine-config-I6897 when the set is empty."
    )


@pytest.mark.skipif(
    not os.environ.get("SF_TIMEOUT_LIVE_CHECK"),
    reason="needs AWS credentials; set SF_TIMEOUT_LIVE_CHECK=1 to run",
)
def test_the_codified_function_timeouts_match_live() -> None:
    """The map is a claim about the live account; this is what proves it.

    `sf-pipeline-policy.md` §2.4 — verification reads the deployed artifact,
    not the source claiming to produce it. Without this the ordering guard
    would keep passing against timeouts that moved in another repo, which is
    precisely how the Scanner's 300s ceiling stayed invisible from here.
    """
    import boto3

    client = boto3.client("lambda")
    drift: list[str] = []
    for fn, codified in sorted(_FUNCTION_TIMEOUTS_SEC.items()):
        live = client.get_function_configuration(FunctionName=fn)["Timeout"]
        if live != codified:
            drift.append(f"{fn}: codified {codified}s, live {live}s")
    assert not drift, "\n  ".join(drift)


def test_the_known_unbound_set_has_not_outlived_its_deadline() -> None:
    """An exemption with no deadline is not a deferral, it is a decision.

    Measured across the fleet 2026-09-01: 47 non-empty suppression collections
    hold 277 live entries between them, and NOT ONE of them fails when an entry
    ages. Fifteen of twenty-six sampled had not changed by a single entry since
    the day they were created, and thirteen of the twenty that cite an issue
    outlive an issue that is already CLOSED — the ticket gets verified and
    closed, the exemption stays, and from then on nothing in the repo or the
    backlog knows the defect is still live.

    This set is the best-behaved of them (21 -> 11 over 19 days, four shrink
    events) and it still has no deadline, which is why the deadline goes here
    first. When this fails, the fix is to drain entries — never to move the
    date without a written reason in the same diff.
    """
    if not _KNOWN_UNBOUND:
        return
    deadline = _dt.date.fromisoformat(_KNOWN_UNBOUND_EXPIRY)
    today = _dt.date.today()
    assert today <= deadline, (
        f"_KNOWN_UNBOUND still holds {len(_KNOWN_UNBOUND)} entries past its "
        f"{_KNOWN_UNBOUND_EXPIRY} deadline: {sorted(_KNOWN_UNBOUND)}. "
        "Each is a stage whose declared timeout cannot bind. Fix them, or move "
        "the deadline WITH a written reason in the same diff — "
        "alpha-engine-config-I6897."
    )


def test_every_codified_definition_is_walked() -> None:
    """The guard covered three definitions; four are codified.

    `step_function_groom.json` was never walked here, so its `lambda:invoke`
    states had no ordering coverage at all — and three of its four synchronous
    states were inverted (360s declared against a 300s function). Pinning the
    count against the shared module's list is what stops a fifth definition
    being added to the fleet and silently escaping this file.
    """
    from infrastructure.sf_definitions import SF_DEFINITIONS

    assert set(_DEFS) == {d["definition_file"] for d in SF_DEFINITIONS}
    assert len(_DEFS) == 4


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("alpha-engine-weekly-preflight", "alpha-engine-weekly-preflight"),
        ("alpha-engine-predictor-inference:live", "alpha-engine-predictor-inference"),
        (
            "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-ssm-liveness-poller",
            "alpha-engine-ssm-liveness-poller",
        ),
        (
            "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-evaluator:live",
            "alpha-engine-evaluator",
        ),
    ],
)
def test_a_full_arn_function_name_resolves_to_the_function(raw: str, expected: str) -> None:
    """The regression test for the defect this file was blind to.

    The old parser took ``raw.split(":")[-2]`` and returned the literal
    ``"function"`` for every full ARN, and the completeness guard then
    whitelisted names starting with ``"function"``. Four states were exempt
    from every assertion in this file for its whole life. All three
    `FunctionName` shapes the fleet's definitions actually use are pinned here.
    """
    from infrastructure.sf_definitions import normalize_function_name

    assert normalize_function_name(raw) == expected


def test_no_state_resolves_to_a_placeholder_function_name() -> None:
    """The shape of the old bug, asserted directly against the live definitions.

    A `normalized_name` of "function" (or empty) means the parser did not
    understand the `FunctionName` it was given. That must be a failure, never a
    quiet skip — the previous version of this guard turned exactly that
    condition into an exemption.
    """
    from infrastructure.sf_definitions import all_lambda_invoke_states

    bad = [
        (s.definition_file, s.state_name, s.function_name)
        for s in all_lambda_invoke_states()
        if s.normalized_name in {None, "", "function"}
    ]
    assert not bad, f"FunctionName values the walk could not resolve: {bad}"


def test_wait_for_task_token_states_are_excluded_from_the_ordering_rule() -> None:
    """The carve-out is semantic and must stay narrow.

    ``LaunchGroomSpot`` declares 13800s against a 300s function and is CORRECT:
    the function returns after handing off a task token, so the state's timeout
    bounds a callback wait the function is not part of. Every OTHER groom state
    invokes the same Lambda synchronously and is governed. If this ever selects
    more than the task-token states, the ordering rule has been quietly widened
    into an exemption.
    """
    from infrastructure.sf_definitions import all_lambda_invoke_states

    exempt = {
        (s.definition_file, s.state_name)
        for s in all_lambda_invoke_states()
        if not s.is_ordering_governed
    }
    assert exempt == {("step_function_groom.json", "LaunchGroomSpot")}


# ── SSM sendCommand stages (alpha-engine-config-I6948) ───────────────────────
#
# The same "declare a ceiling the service can never reach" defect, in the other
# direction and with a worse failure mode. An `aws-sdk:ssm:sendCommand` task has
# two ceilings: the state's `TimeoutSeconds` and SSM's own `executionTimeout`
# parameter. Which one should bind is the OPPOSITE of the lambda:invoke case:
#
#   SSM <  SF     CORRECT. SSM kills the command, the agent returns
#                 `Status=TimedOut ResponseCode=137 ExecutionTimedOut`, the poll
#                 state reads it, and the operator gets a named cause.
#   SSM == SF     RACE. Undefined which fires; the error is not reproducible.
#   SSM >  SF     DEFECT, and expensive. The state gives up while the command
#                 KEEPS RUNNING on the box — Step Functions does not cancel an
#                 SSM invocation. The stage fails with a bare `States.Timeout`
#                 naming nothing, a spot instance carries on billing, and the
#                 declared SSM budget is decorative.
#
# Found by this guard on its first run: `RAGIngestion` declared
# `executionTimeout=21600` against `TimeoutSeconds=3660`. config#2938
# (2026-07-18) had deliberately widened that budget 3600 -> 14400 -> 21600 after
# live measurement — "the SEC-filings phase alone needs >=1h before the news
# sweep starts", with the Polygon leg covering ~944 tickers at 5 req/min
# (~3.15h). `TimeoutSeconds` was never moved with it, so the state has been
# killing at 61 minutes ever since and the measured widening has been inert. The
# commit message records the intent; nothing enforced it.
#
# I6948 asked for the ordering to be added to this file specifically, so it
# lives here rather than in a sibling — the defect class is identical ("a stage
# timeout that can never bind"), only the correct direction differs.


def _ssm_send_command_states(states: dict) -> Iterator[tuple[str, int | None, int | None]]:
    """``(state_name, TimeoutSeconds, executionTimeout)`` for every sendCommand.

    Recurses the same way as :func:`_lambda_invoke_states` — every spot-bearing
    stage of the weekly pipeline lives inside `ResearchPredictorParallel` or
    `ParityParallel`, so a top-level-only scan would exempt all of them.

    ``executionTimeout`` is an SSM *document parameter*, so it arrives as a list
    of strings: ``{"Parameters": {"executionTimeout": ["5400"]}}``.
    """
    for name, body in states.items():
        resource = body.get("Resource")
        if isinstance(resource, str) and resource.endswith(":ssm:sendCommand"):
            params = ((body.get("Parameters") or {}).get("Parameters") or {})
            raw = params.get("executionTimeout")
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            try:
                execution_timeout = int(raw) if raw is not None else None
            except (TypeError, ValueError):
                execution_timeout = None
            yield name, body.get("TimeoutSeconds"), execution_timeout
        for branch in body.get("Branches") or []:
            yield from _ssm_send_command_states(branch.get("States") or {})
        iterator = body.get("Iterator") or body.get("ItemProcessor")
        if iterator:
            yield from _ssm_send_command_states(iterator.get("States") or {})


@pytest.mark.parametrize("definition", _DEFS)
def test_every_ssm_send_command_declares_both_ceilings(definition: str) -> None:
    """Neither budget may be left implicit.

    A missing `executionTimeout` takes the SSM document default; a missing
    `TimeoutSeconds` takes the definition ceiling. Either way the stage's real
    budget is somewhere other than the stage.
    """
    missing = [
        f"{name}: TimeoutSeconds={sf} executionTimeout={ssm}"
        for name, sf, ssm in _ssm_send_command_states(_states(definition))
        if sf is None or ssm is None
    ]
    assert not missing, f"{definition}:\n  " + "\n  ".join(missing)


@pytest.mark.parametrize("definition", _DEFS)
def test_ssm_kills_the_command_before_the_state_gives_up(definition: str) -> None:
    """`executionTimeout` strictly below `TimeoutSeconds`.

    Ordered this way the operator sees `TimedOut/137/ExecutionTimedOut` naming
    the SSM command. Inverted, the state abandons a command that is still
    running: bare `States.Timeout`, no cause, and a spot instance still billing.
    """
    violations: list[str] = []
    for name, sf, ssm in _ssm_send_command_states(_states(definition)):
        if sf is None or ssm is None:
            continue  # covered by the declaration test above
        if ssm >= sf:
            violations.append(
                f"{name}: executionTimeout={ssm} >= TimeoutSeconds={sf} "
                f"({'race' if ssm == sf else 'the state abandons a live command'})"
            )
    assert not violations, f"{definition}:\n  " + "\n  ".join(violations)


# The groom dispatch SF drives spot boxes through its dispatcher Lambda and a
# task token, never through ssm:sendCommand — so an empty scan there is the
# correct answer, not a broken walk. Naming it explicitly rather than dropping
# the assertion keeps the non-emptiness guarantee for every definition that
# does carry sendCommand stages.
_DEFS_WITH_SSM_SEND_COMMAND = tuple(d for d in _DEFS if d != "step_function_groom.json")


@pytest.mark.parametrize("definition", _DEFS_WITH_SSM_SEND_COMMAND)
def test_the_ssm_scan_is_not_empty(definition: str) -> None:
    """A recursion bug would make every assertion above vacuously true.

    `step_function.json` alone carries 19 spot-bearing stages, all of them
    nested inside Parallel branches. A scan returning nothing would report
    clean — the empty-denominator shape this fleet keeps re-shipping.
    """
    found = list(_ssm_send_command_states(_states(definition)))
    assert found, f"{definition}: no ssm:sendCommand states found — the walk is broken"
