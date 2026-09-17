"""One walk of the codified Step Functions definitions, shared by every checker.

WHY THIS MODULE EXISTS (alpha-engine-config-I9702 arc, 2026-09-01).

Two checkers in this repo each carried their own private copy of "find every
`lambda:invoke` state and reduce its `FunctionName` to a bare function name":

  * ``infrastructure/step-functions/check-lambda-existence.py`` — correct. It
    matches the `.waitForTaskToken`/`.sync` resource variants and normalizes
    all three `FunctionName` shapes through ``_FULL_ARN_RE``.
  * ``tests/test_sf_lambda_timeout_ordering.py`` — wrong, in three ways at
    once, and silently:

      1. ``raw.split(":")[-2]`` on a full ARN yields the literal string
         ``"function"``, not the function name. Four states carry a full ARN:
         `SubstrateHealthGate` (weekly) and `WaitForCodeFreshness`,
         `WaitForCorrectnessVerdict`, `WaitForMorningPlanner` (preopen).
      2. The completeness check that exists PRECISELY to stop a state being
         skipped silently — "a function missing from the map is an unchecked
         stage, not a passing one" — carried ``not fn.startswith("function")``,
         so it whitelisted exactly the mis-parse in (1). The guard against
         silent exemption exempted them silently.
      3. ``resource.endswith(":lambda:invoke")`` misses the `.waitForTaskToken`
         and `.sync` variants entirely.

    Net effect measured 2026-09-01: three preopen states declared
    ``TimeoutSeconds: 65`` against a 30s function, and one weekly state
    declared 90 against a 90s function, and the guard whose whole job is to
    catch that reported green for both — for as long as it has existed.

    It also walked only three of the four codified definitions, so every
    `lambda:invoke` state in ``step_function_groom.json`` had never been
    checked at all. Three of its four synchronous states were inverted.

That is the "a contract restated twice has already drifted" class. The fix is
not to repair the second copy; it is to delete it. Both checkers now import
from here, so a definition shape this module cannot parse fails BOTH of them
rather than passing one silently.

THE `.waitForTaskToken` CARVE-OUT IS SEMANTIC, NOT AN EXEMPTION. For a
synchronous ``lambda:invoke`` the state's ``TimeoutSeconds`` and the function's
own ``Timeout`` are two ceilings on the SAME wait, and only the smaller fires —
so the state's must be the smaller or it describes nothing. For
``lambda:invoke.waitForTaskToken`` they measure DIFFERENT things: the function
returns immediately after handing off a task token, and the state's timeout
bounds how long the pipeline waits for something else to call
``SendTaskSuccess``. A state timeout far above the function timeout is the
CORRECT shape there (``LaunchGroomSpot`` declares 13800s against a 300s
function and is right to). ``is_ordering_governed`` is where that distinction
lives, so no caller has to re-derive it.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

_INFRA = Path(__file__).resolve().parent

# repo definition file -> live state machine name. The SINGLE codified list;
# check-lambda-existence.py and the timeout-ordering guard both read it, so a
# definition added to the fleet is covered by both or by neither — never by one.
SF_DEFINITIONS: tuple[dict[str, str], ...] = (
    {"sf_name": "ne-weekly-freshness-pipeline", "definition_file": "step_function.json"},
    {"sf_name": "ne-preopen-trading-pipeline", "definition_file": "step_function_daily.json"},
    {"sf_name": "ne-postclose-trading-pipeline", "definition_file": "step_function_eod.json"},
    {"sf_name": "alpha-engine-groom-dispatch", "definition_file": "step_function_groom.json"},
)

DEFINITION_FILES: tuple[str, ...] = tuple(d["definition_file"] for d in SF_DEFINITIONS)

LAMBDA_INVOKE_RESOURCE_RE = re.compile(
    r"^arn:aws:states:::lambda:invoke(\.(waitForTaskToken|sync|sync:2))?$"
)

# A full Lambda ARN, with an optional :version-or-alias qualifier.
_FULL_ARN_RE = re.compile(r"^arn:aws:lambda:[\w-]+:\d+:function:([^:]+)(:.+)?$")

# Lambda's hard service maximum. A function AT this value cannot be raised, so
# the SF-vs-function ordering rule inverts for it — see the timeout-ordering
# guard's `_SERVICE_MAX_GUARD_BAND`.
LAMBDA_SERVICE_MAX_SEC = 900


@dataclass(frozen=True)
class LambdaInvokeState:
    """One `lambda:invoke` task state, with everything a checker needs.

    ``function_name`` is the raw value as written in the definition, or None
    when the state declares no ``Parameters.FunctionName`` at all — absent and
    empty are different defects and a checker should be able to say which;
    ``normalized_name`` is the bare name the Lambda API accepts. Both are kept
    because an error message naming only the normalized form sends a reader
    looking for a string that does not appear in the file.
    """

    definition_file: str
    state_name: str
    function_name: str | None
    normalized_name: str | None
    timeout_seconds: int | None
    resource: str

    @property
    def is_ordering_governed(self) -> bool:
        """True when the state timeout and the function timeout bound the same wait.

        False for ``.waitForTaskToken``, where the state's timeout bounds a
        callback the function does not participate in — see the module
        docstring. ``.sync`` is governed: the service polls the invocation to
        completion, so the function's own ceiling still fires first.
        """
        return not self.resource.endswith(".waitForTaskToken")


def normalize_function_name(raw: str) -> str:
    """Reduce a `FunctionName` value to the bare name the Lambda API accepts.

    Handles all three shapes this fleet's definitions use: a bare name, a bare
    name with a ``:version-or-alias`` qualifier, and a full ARN with or without
    that qualifier.
    """
    match = _FULL_ARN_RE.match(raw)
    if match:
        return match.group(1)
    return raw.split(":", 1)[0]


def load_definition(definition_file: str) -> dict:
    """Parse one codified definition. Raises rather than returning a sentinel.

    A checker that treats an unparseable definition as "no states to check"
    reports green on the one condition it most needs to report.
    """
    return json.loads((_INFRA / definition_file).read_text(encoding="utf-8"))


def lambda_invoke_states(
    definition_file: str, states: dict | None = None
) -> Iterator[LambdaInvokeState]:
    """Every `lambda:invoke` state in one definition, recursing every nesting.

    Descends `Parallel` branches and `Map` iterators (both the older
    ``Iterator`` key and the newer ``ItemProcessor``): the weekly pipeline's
    Scanner, every spot-bearing stage and the whole parity chain live inside
    `ResearchPredictorParallel` or `ParityParallel`, so a top-level-only walk
    would exempt the majority of the pipeline while looking complete.
    """
    if states is None:
        states = load_definition(definition_file)["States"]
    for state_name, body in states.items():
        resource = body.get("Resource")
        if isinstance(resource, str) and LAMBDA_INVOKE_RESOURCE_RE.match(resource):
            raw = (body.get("Parameters") or {}).get("FunctionName")
            raw = raw if isinstance(raw, str) and raw else None
            yield LambdaInvokeState(
                definition_file=definition_file,
                state_name=state_name,
                function_name=raw,
                normalized_name=normalize_function_name(raw) if raw else None,
                timeout_seconds=body.get("TimeoutSeconds"),
                resource=resource,
            )
        for branch in body.get("Branches") or []:
            yield from lambda_invoke_states(definition_file, branch.get("States") or {})
        nested = body.get("Iterator") or body.get("ItemProcessor")
        if isinstance(nested, dict) and "States" in nested:
            yield from lambda_invoke_states(definition_file, nested["States"])


def all_lambda_invoke_states() -> Iterator[LambdaInvokeState]:
    """Every `lambda:invoke` state across every codified definition."""
    for definition_file in DEFINITION_FILES:
        yield from lambda_invoke_states(definition_file)


# ── The codified live-timeout table ──────────────────────────────────────────
#
# A CLAIM about the live AWS account, not a fact derived from this repo. The
# functions are deployed from four other repositories, so nothing in a merge
# here moves them and nothing here notices when they move. Two readers keep it
# honest, and they must read the SAME declaration or the honesty is theatre:
#
#   * tests/test_sf_lambda_timeout_ordering.py — grades each definition's
#     declared stage timeout against it, in CI, with no credentials.
#   * infrastructure/step-functions/check-lambda-timeout-drift.py — grades the
#     table itself against live AWS, on the daily credentialed drift workflow.
#
# The second reader is new (alpha-engine-config-I9702 arc, 2026-09-01). The
# equivalent assertion existed before only as a test guarded by
# `SF_TIMEOUT_LIVE_CHECK`, an environment variable set in no workflow in this
# repo — so the check that proves this table is not stale had never run.
# Live function timeouts, measured 2026-08-11 via
# `aws lambda get-function-configuration --query Timeout`.
CODIFIED_FUNCTION_TIMEOUTS_SEC: dict[str, int] = {
    "alpha-engine-data-spot-dispatcher": 600,
    "alpha-engine-eod-precondition-probe": 30,
    "alpha-engine-evaluator": 660,
    "alpha-engine-evaluator-director": 900,
    "alpha-engine-predictor-inference": 900,
    # alpha-engine-config-I7620. Two read-only Step Functions API calls plus a
    # pure in-memory derivation — no S3 reads, no spot, no model call. Matches
    # the --bootstrap timeout in
    # infrastructure/lambdas/weekly-run-scope/deploy.sh; a generous ceiling here
    # would let an advisory state hold the tail of the weekly run.
    "alpha-engine-weekly-run-scope": 60,
    # alpha-engine-config-I8214. ListExecutions + one DescribeExecution and
    # GetExecutionHistory per contributing execution, a prefix listing, one
    # PUT and one marker read-modify-write. Bounded by the cycle's execution
    # count (single digits), not by the ticker universe. Matches the
    # --bootstrap timeout in
    # infrastructure/lambdas/weekly-coverage-sweep/deploy.sh; the SF state's
    # 240s ceiling is deliberately below it so the SF is the one that binds.
    # MEASURED 2026-08-22 on the first live invocation: 29.4s at 1024 MB,
    # 675 MB peak, over a 40-execution walk. The original 120s/256 MB sizing
    # timed out — at 256 MB the function used 256 of 256 and Lambda scales CPU
    # with memory, so it was CPU-throttled as well as memory-starved.
    "alpha-engine-weekly-coverage-sweep": 300,
    "alpha-engine-predictor-regime-retrospective-eval": 600,
    "alpha-engine-predictor-regime-substrate": 300,
    # alpha-engine-replay-concordance was RETIRED from this pipeline by
    # alpha-engine-config-I10539 (Brian ruling 2026-09-16, option b): its
    # replay corpus — the six-team research agents — was retired under
    # -I1580 on 2026-07-20, and the resulting zero-call run failed the
    # 2026-09-12 canonical weekly SF at AggregateCosts. Its row is removed
    # rather than left behind, for the same reason the eval-judge poll rows
    # below were: an entry naming a function no SF invokes is an unchecked
    # claim.
    "alpha-engine-replay-counterfactual": 600,
    "alpha-engine-research-aggregate-costs": 300,
    # alpha-engine-research-eval-judge-poll and -process were RETIRED by
    # alpha-engine-config-I9329: the poll states existed to drive a provider
    # batch API that no longer exists (-I9263), and Process moved onto a
    # dedicated EC2 spot box because the judge covered 8-15 of ~83 artifacts
    # inside a 900s function. Their rows are removed rather than left behind —
    # an entry naming a function no SF invokes is an unchecked claim, and
    # test_the_codified_function_timeouts_match_live would then assert against
    # a function whose only remaining property is that it is dead.
    "alpha-engine-research-eval-judge-submit": 300,
    # The dispatcher that replaces them. 600s covers the handler's worst case:
    # a spot launch with capacity rotation and an on-demand fallback, plus the
    # full 300s SSM-online wait, before the async detached send-command. It
    # does NOT wait for the bootstrap — the SF's own poll loop does that.
    # Matches --bootstrap's FN_TIMEOUT in
    # infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh.
    "alpha-engine-research-eval-judge-spot-dispatcher": 600,
    # 300 -> 900 by crucible-research infrastructure/deploy.sh in the
    # alpha-engine-config-I9102 arc (both create AND update paths — the sizing
    # previously lived only on create, so no merge could ever re-size the live
    # function). Pinned at the service maximum because the handler is now
    # self-deadlining; see the guard-band entry below.
    "alpha-engine-research-eval-rolling-mean": 900,
    "alpha-engine-research-rationale-clustering": 900,
    "alpha-engine-research-runner": 900,
    # 300 -> 450 by crucible-research-PR601 (p95 x 1.5). The Scanner states
    # moved 600 -> 440 in the same arc so the SF budget binds again.
    "alpha-engine-research-scanner": 450,
    "alpha-engine-research-signals-envelope": 300,
    "alpha-engine-weekly-freshness-spot-dispatcher": 600,
    "alpha-engine-weekly-preflight": 120,
    # ── Newly VISIBLE, not newly added (alpha-engine-config-I9702 arc,
    # 2026-09-01). These three were invoked all along; the old parser reduced
    # their full-ARN `FunctionName` to the literal "function" and the
    # completeness guard whitelisted that string, so no row was ever demanded
    # for them and no ordering assertion ever ran against them.
    #
    # MEASURED 2026-09-01, CloudWatch AWS/Lambda Duration, trailing 45 days:
    #   alpha-engine-ssm-liveness-poller       291 invocations, p95 357ms, max 410ms
    #   alpha-engine-substrate-health-gate      37 invocations, p95 3.55s,  max 3.57s
    #   alpha-engine-scheduled-groom-dispatcher 2856 invocations, p95 22.2s,
    #                                           p99 87.6s, max 195.8s
    "alpha-engine-ssm-liveness-poller": 30,
    "alpha-engine-substrate-health-gate": 90,
    "alpha-engine-scheduled-groom-dispatcher": 300,
}
