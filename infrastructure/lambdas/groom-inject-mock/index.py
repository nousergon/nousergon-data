"""groom-inject-mock — the scripted stand-in for the groom dispatcher, used
ONLY by the fault-injection staging dispatch (alpha-engine-config-I5718).

Why a separate function rather than a flag on the real dispatcher: a
`GROOM_INJECT_MODE` branch inside the production Lambda is one bad conditional
away from launching real spot boxes against the real backlog. Nothing here has
EC2, SSM or GitHub permissions — see iam-policy.json, which grants only
`states:SendTask*` and CloudWatch Logs.

**What it is NOT.** This does not mock the groom's *judgment* — no issue
selection, no model routing, no PR logic. It answers exactly the Task states
the dispatch state machine invokes, with responses shaped like the real
Lambda's, so the STATE MACHINE's recovery topology is what gets exercised:
timeout -> CheckCompletionMarker -> relaunch -> retry budget -> HandleFailure.
That topology is where every failure recorded in this system has actually
lived (config-I4333, I4987, I5371, I5372), and it is the part unit tests
cannot reach.

**How a scenario is driven.** The staging execution's input carries
`inject: {scenario}`. Scenarios are declared in SCENARIOS below; each names the
failure mode from groom-sweep-policy §10.2's minimum standing set that it
reproduces. The driver reads this map out of the DEPLOYED function rather than
keeping its own copy, so driver and mock cannot drift (§2.7 derive-once,
applied to the harness itself).
"""

from __future__ import annotations

import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")

SCENARIOS: dict[str, str] = {
    "callback_never_arrives": "box killed after work, before callback",
    "callback_rejected": "callback rejected",
    "box_dies_mid_run": "box killed mid-run",
    "box_dies_mid_bootstrap": "box killed mid-bootstrap",
    "relaunch_guard_refuses": "a relaunch whose guard refuses it",
    "empty_enumeration": "empty enumeration",
    "malformed_decide_payload": "malformed classify payload",
    "decide_raises": "GitHub search 500",
    "ledger_write_fails": "expectation-record write failure",
    "happy_path": "(control — not a failure mode; proves the harness can pass)",
}


def _scenario(event: dict) -> str:
    """Read the scenario from wherever the SF hands it to us.

    An unknown or absent scenario is NOT defaulted to happy_path — that would
    let a driver bug read as a passing injection, which is the §2.4 failure
    this whole programme exists to catch. It raises instead.
    """
    for candidate in (
        event.get("inject"),
        (event.get("schedInput") or {}).get("inject"),
        (event.get("cycle") or {}).get("inject"),
    ):
        if isinstance(candidate, dict) and candidate.get("scenario"):
            return str(candidate["scenario"])
    raise ValueError(
        f"no inject.scenario in event keys={sorted(event)!r} — the injection "
        "driver did not thread the scenario through, and defaulting it would "
        "make a harness bug look like a passing test"
    )


def _decide_response(scenario: str) -> dict:
    """Answer for DecideLaunches."""
    if scenario == "empty_enumeration":
        # Zero actionable issues -> AllSkipped -> sweep. Post-I5689 this must
        # report as a HEALTHY skip, not a degraded one.
        return {"decide": {"launches": [], "reason": "demand_gate_skip"}}
    if scenario == "malformed_decide_payload":
        return {"decide": {"launches": "not-a-list", "reason": "malformed"}}
    if scenario == "decide_raises":
        raise RuntimeError("injected: GitHub search returned 500")
    return {
        "decide": {
            "trigger": "demand-all",
            "counts": {"low": 1, "mid": 1, "high": 0},
            "launches": [{
                "issue_filter": "mid-only",
                "model": "inject-mock",
                "tier_tag": "mid-only",
                "launch_decided": True,
            }],
        }
    }


def _launch_response(scenario: str, task_token: str) -> dict:
    """Answer for LaunchGroomSpot (invoke.waitForTaskToken).

    Returning normally does NOT complete the state — the SF waits for the task
    token. That is precisely what lets us reproduce a lost callback: answer,
    and never send the token.
    """
    sfn = boto3.client("stepfunctions", region_name=REGION)

    if scenario == "ledger_write_fails":
        # The post-launch expectation write is fail-loud (§2.7): the real
        # Lambda terminates the box and raises.
        raise RuntimeError("injected: expectation-record write failed")

    if scenario == "relaunch_guard_refuses":
        # The I4987 defect: the relaunch fires, the concurrent-tier guard
        # refuses because the PREVIOUS box is still alive, and the SF waits on
        # a no-op until it times out. launched:false and NO token.
        return {"groom": {
            "launched": False, "reason": "concurrent_tier_skip",
            "issue_filter": "mid-only",
            "existing_instance_ids": ["i-injected-previous-attempt"],
        }}

    if scenario == "box_dies_mid_bootstrap":
        return {"groom": {"launched": True, "instance_id": "i-injected-dead-boot",
                          "market": "spot", "run_token": "inject-boot"}}

    if scenario in ("callback_never_arrives", "box_dies_mid_run"):
        return {"groom": {"launched": True, "instance_id": "i-injected-silent",
                          "market": "spot", "run_token": "inject-silent"}}

    if scenario == "callback_rejected":
        if task_token:
            sfn.send_task_failure(
                taskToken=task_token,
                error="InjectedCallbackRejected",
                cause="injected: box reported failure via send-task-failure",
            )
        return {"groom": {"launched": True, "instance_id": "i-injected-rejected"}}

    if task_token:
        _r = sfn.send_task_success(
            taskToken=task_token,
            output=json.dumps({"groomLaunch": {"Payload": {"groom": {
                "launched": True, "completion": "success",
                "instance_id": "i-injected-ok", "run_token": "inject-ok",
            }}}}),
        )
        logger.info('lane completed via task token (http=%s)',
                    _r.get('ResponseMetadata', {}).get('HTTPStatusCode'))
    return {"groom": {"launched": True, "instance_id": "i-injected-ok"}}


def handler(event, _context):  # noqa: ANN001 — Lambda signature
    """Total by construction: every shape either answers or raises loudly."""
    if event.get("mode") == "list_scenarios":
        return {"scenarios": SCENARIOS}

    if event.get("mode") == "cycle_complete":
        return {"cycleNotify": {"notified": True, "degraded": False,
                                "lanes": 0, "injected": True}}

    if event.get("run_mode") == "sweep":
        return {"groom": {"launched": True, "instance_id": "i-injected-sweep",
                          "run_mode": "sweep", "injected": True}}

    scenario = _scenario(event)
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown injection scenario {scenario!r}; "
                         f"known: {sorted(SCENARIOS)}")

    logger.info("inject scenario=%s keys=%s", scenario, sorted(event))

    # Route by the field that UNIQUELY identifies each caller. Ordering matters
    # and the discriminators are not interchangeable: LaunchGroomSpot's payload
    # is JsonMerge(schedInput, launchDecision, fod, $$.Task), so it also carries
    # `trigger`, `run_mode` and `schedule` from schedInput. Routing on `trigger`
    # therefore sends the LANE to the decide branch, the token is never used,
    # and every lane times out — which is exactly what happened on the first
    # live run of this harness (2026-07-30) and cost three relaunch cycles
    # before the log showed `Token` sitting unread in the payload.
    #
    # `Token` is present ONLY on the waitForTaskToken state, so it is checked
    # FIRST and is the only safe discriminator for the lane.
    if event.get("Token"):
        return _launch_response(scenario, event["Token"])

    # DecideLaunches is the only caller with decide_only.
    if "decide_only" in event:
        return _decide_response(scenario)

    # CheckCompletionMarkerTaskToken — reached after a lane timeout, identified
    # by the markers the SF threads through it. The real Lambda relaunches
    # here; answering the same shape exercises the retry budget and
    # HandleFailure path for real.
    if "retryMarker" in event or "decideMarker" in event:
        return {"groom": {"launched": True, "instance_id": "i-injected-relaunch",
                          "run_token": "inject-relaunch"}}

    # No silent fallthrough: an unrecognized caller means the production
    # definition grew a state this mock does not answer, and guessing would
    # make the harness report on a topology it is not actually driving.
    raise ValueError(
        f"unroutable invocation for scenario {scenario!r}; keys={sorted(event)!r} "
        "— the dispatch state machine invoked a state this mock does not model"
    )
