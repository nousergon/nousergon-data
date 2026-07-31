#!/usr/bin/env python3
"""fault_injection_run.py — exercise the groom dispatch's recovery paths on a
schedule, against a staging state machine (alpha-engine-config-I5718).

groom-sweep-policy §2.2 holds that *"a recovery path that has never executed is
presumed broken"*, and §10.2 makes fault injection the standing gate that makes
that enforceable: *"'presumed broken until executed' is only enforceable if
something MAKES them execute."* Until this script, nothing did. Every recovery
path in this system has been found broken at the moment it was first needed.

**The verdict surface is the deliverable, not the runs.** Each failure mode
resolves to exactly one of:

    passed      — injected, and the recovery path reached its expected state
    FAILED      — injected, and it did not
    unverified  — not exercised in this programme

`unverified` is never rendered as healthy and never omitted. A mode that was
skipped, timed out, or has no scenario yet is *unverified*, and the run's exit
code reflects it. That is §2.4 applied to the exercise programme itself: the
absence of an exercise must not read as a passing one.

Usage:
    python3 scripts/fault_injection_run.py --deploy   # (re)build staging SF
    python3 scripts/fault_injection_run.py            # run the programme
    python3 scripts/fault_injection_run.py --dry-run  # show plan, touch nothing
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0, str(REPO_ROOT / "infrastructure" / "lambdas" / "groom-inject-mock")
)

from staging_definition import (  # noqa: E402
    MOCK_LAMBDA,
    build_staging_definition,
    load_prod_definition,
)

REGION = "us-east-1"
ACCOUNT_ID = "711398986525"
STAGING_SF_NAME = "alpha-engine-groom-dispatch-inject"
RESULT_BUCKET = "alpha-engine-research"
RESULT_PREFIX = "groom/_control/fault-injection"

#: What each scenario must PROVE. `terminal` is the execution status the
#: staging run must reach; `must_visit` are states whose presence in the
#: execution history is the actual evidence the recovery path ran.
#:
#: This is the part that makes the harness meaningful. Asserting only "the
#: execution finished" would pass for a state machine that skipped every
#: recovery state — the exact laundering §1 forbids.
EXPECTATIONS: dict[str, dict] = {
    "happy_path": {
        "terminal": "SUCCEEDED",
        "must_visit": ["LaunchGroomSpot", "DispatchEndOfSfSweep",
                       "NotifyCycleComplete"],
        "why": "control — if this fails the harness itself is broken",
    },
    "empty_enumeration": {
        "terminal": "SUCCEEDED",
        "must_visit": ["AllSkipped", "DispatchEndOfSfSweep"],
        "why": "a zero-launch cycle must still dispatch the sweep (§2.5)",
    },
    "callback_never_arrives": {
        "terminal": "FAILED",
        "must_visit": ["LaunchGroomSpot", "CheckCompletionMarkerTaskToken",
                       "HandleFailure"],
        "why": "lane timeout must route to the marker check, then escalate",
    },
    "box_dies_mid_run": {
        "terminal": "FAILED",
        "must_visit": ["CheckCompletionMarkerTaskToken", "HandleFailure"],
        "why": "same path as a lost callback; the SF cannot tell them apart",
    },
    "box_dies_mid_bootstrap": {
        "terminal": "FAILED",
        "must_visit": ["CheckCompletionMarkerTaskToken"],
        "why": "a box that never became healthy must still be reclaimed",
    },
    "relaunch_guard_refuses": {
        "terminal": "FAILED",
        "must_visit": ["CheckCompletionMarkerTaskToken"],
        "why": ("alpha-engine-config-I4987 — the relaunch fires because the box "
                "is alive and its guard refuses because the box is alive. This "
                "scenario is the regression guard for that fix."),
    },
    "callback_rejected": {
        "terminal": "FAILED",
        "must_visit": ["LaunchGroomSpot"],
        "why": "an explicit failure callback must fail the lane, not hang it",
    },
    "decide_raises": {
        "terminal": "FAILED",
        "must_visit": ["HandleDecideFailure"],
        "why": "enumeration failure must be caught, named, and never launch",
    },
    "malformed_decide_payload": {
        "terminal": "SUCCEEDED",
        "must_visit": ["AllSkipped"],
        "why": ("a shape the Choice cannot match must fall to the zero-launch "
                "path, not crash the execution"),
    },
    "ledger_write_fails": {
        "terminal": "FAILED",
        "must_visit": ["HandleFailure", "RecordLaneFailure", "LaneCompleted",
                       "CheckMapLaneOutcomes", "SetMapFailureFromLanes",
                       "DispatchEndOfSfSweep"],
        "why": ("§2.7 — a registration failure is itself a paging condition. "
                "This expectation originally named CheckCompletionMarkerTaskToken "
                "and FAILED on the first full run: that state is reached on a "
                "lane TIMEOUT, not on a lane that RAISES. A raise is caught into "
                "HandleFailure -> RecordLaneFailure -> LaneCompleted so siblings "
                "are not aborted (§2.5), then aggregated. The harness was right "
                "and the expectation was wrong — corrected rather than relaxed. "
                "It is also the only scenario that exercises SetMapFailureFromLanes, "
                "so it is the regression guard for the I5718 Choice fix."),
    },
}


def _clients():
    import boto3
    return (boto3.client("stepfunctions", region_name=REGION),
            boto3.client("s3", region_name=REGION),
            boto3.client("lambda", region_name=REGION))


def _staging_arn() -> str:
    return f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{STAGING_SF_NAME}"


def deploy_staging(sfn, *, dry_run: bool) -> str:
    """Create or update the staging machine from the PRODUCTION definition."""
    definition = build_staging_definition(
        load_prod_definition(REPO_ROOT), account_id=ACCOUNT_ID, region=REGION)
    body = json.dumps(definition)
    arn = _staging_arn()
    # A DEDICATED role, never the production SF role. Reusing production's
    # would (a) fail — it is scoped to the production Lambda and the real
    # alerts topic, which is how the first injection run failed — and (b) be
    # the wrong shape: a staging machine holding production's grants can reach
    # production resources if the definition swap is ever incomplete. This
    # role can invoke ONLY the mock and publish ONLY to the staging topic, so
    # the isolation is enforced by IAM rather than by the swap being correct.
    role = f"arn:aws:iam::{ACCOUNT_ID}:role/alpha-engine-groom-inject-sf-role"
    if dry_run:
        print(f"[dry-run] would deploy {arn} ({len(body)} bytes)")
        return arn
    try:
        sfn.update_state_machine(stateMachineArn=arn, definition=body,
                                 roleArn=role)
        print(f"updated {STAGING_SF_NAME}")
    except sfn.exceptions.StateMachineDoesNotExist:
        sfn.create_state_machine(name=STAGING_SF_NAME, definition=body,
                                 roleArn=role, type="STANDARD")
        print(f"created {STAGING_SF_NAME}")
    return arn


def scenarios_from_deployed_mock(lam) -> dict[str, str]:
    """Read the scenario list out of the DEPLOYED mock, never a local copy.

    §2.7 derive-once. A local list would drift from the function that actually
    answers, and the harness would silently stop covering a mode while still
    reporting on it.
    """
    resp = lam.invoke(FunctionName=MOCK_LAMBDA,
                      Payload=json.dumps({"mode": "list_scenarios"}).encode())
    payload = json.loads(resp["Payload"].read().decode())
    return payload["scenarios"]


def run_scenario(sfn, arn: str, scenario: str, *, timeout_s: int = 240) -> dict:
    """Start one staging execution and resolve it to a verdict."""
    started = sfn.start_execution(
        stateMachineArn=arn,
        input=json.dumps({
            "run_mode": "full", "trigger": "demand-all", "pr_budget": 1,
            "schedule": f"inject-{scenario}",
            "inject": {"scenario": scenario},
        }),
    )
    exec_arn = started["executionArn"]

    deadline = time.monotonic() + timeout_s
    status = "RUNNING"
    while time.monotonic() < deadline:
        status = sfn.describe_execution(executionArn=exec_arn)["status"]
        if status != "RUNNING":
            break
        time.sleep(3)
    else:
        # Never silently treat a hung injection as anything but unverified.
        sfn.stop_execution(executionArn=exec_arn, error="InjectionTimeout")
        return {"scenario": scenario, "verdict": "unverified",
                "detail": f"execution still RUNNING after {timeout_s}s",
                "execution": exec_arn}

    visited = set()
    paginator = sfn.get_paginator("get_execution_history")
    for page in paginator.paginate(executionArn=exec_arn, maxResults=1000):
        for event in page["events"]:
            for key in ("stateEnteredEventDetails", "stateExitedEventDetails"):
                name = (event.get(key) or {}).get("name")
                if name:
                    visited.add(name)

    expected = EXPECTATIONS[scenario]
    missing = [s for s in expected["must_visit"] if s not in visited]
    terminal_ok = status == expected["terminal"]

    if terminal_ok and not missing:
        verdict, detail = "passed", f"reached {status} via {expected['must_visit']}"
    else:
        problems = []
        if not terminal_ok:
            problems.append(f"terminal {status} != expected {expected['terminal']}")
        if missing:
            problems.append(f"never visited {missing}")
        verdict, detail = "FAILED", "; ".join(problems)

    return {"scenario": scenario, "verdict": verdict, "detail": detail,
            "execution": exec_arn, "terminal": status,
            "why": expected["why"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--deploy", action="store_true",
                    help="(re)build the staging state machine, then exit")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="run a single scenario by name")
    args = ap.parse_args()

    sfn, s3, lam = _clients()

    if args.deploy:
        deploy_staging(sfn, dry_run=args.dry_run)
        return 0

    arn = deploy_staging(sfn, dry_run=args.dry_run)
    known = scenarios_from_deployed_mock(lam)

    # Every mode starts UNVERIFIED and must earn a different verdict.
    results = {
        name: {"scenario": name, "verdict": "unverified",
               "detail": "not exercised in this run",
               "failure_mode": known.get(name, "(no scenario declared)")}
        for name in EXPECTATIONS
    }

    # A mode the mock declares but this driver has no expectation for is a
    # COVERAGE HOLE, reported rather than skipped (§2.4 no-silent-caps).
    for name, mode in known.items():
        if name not in EXPECTATIONS:
            results[name] = {"scenario": name, "verdict": "unverified",
                             "detail": "mock declares it; driver has no expectation",
                             "failure_mode": mode}

    to_run = [args.only] if args.only else list(EXPECTATIONS)
    for name in to_run:
        if args.dry_run:
            print(f"[dry-run] would inject {name}")
            continue
        print(f"injecting {name} ...", flush=True)
        try:
            outcome = run_scenario(sfn, arn, name)
        except Exception as exc:  # noqa: BLE001 — a driver error is unverified,
            # never a pass. Recorded with the cause so it cannot be mistaken
            # for a clean run.
            outcome = {"scenario": name, "verdict": "unverified",
                       "detail": f"driver error: {exc}"}
        outcome["failure_mode"] = known.get(name, "(not declared by the mock)")
        results[name] = outcome
        print(f"  {name}: {outcome['verdict']} — {outcome['detail']}")

    passed = sum(1 for r in results.values() if r["verdict"] == "passed")
    failed = sum(1 for r in results.values() if r["verdict"] == "FAILED")
    unverified = sum(1 for r in results.values() if r["verdict"] == "unverified")

    report = {
        "schema_version": 1,
        "passed": passed, "failed": failed, "unverified": unverified,
        "results": results,
    }
    print(f"\nFAULT_INJECTION_DONE passed={passed} failed={failed} "
          f"unverified={unverified}")

    if not args.dry_run:
        # Stamped by the caller, not by the script — Date.now() inside the run
        # would make the artifact key unpredictable for the reader.
        from datetime import datetime, timezone
        stamp = datetime.now(timezone.utc)
        report["run_at"] = stamp.isoformat()
        key = f"{RESULT_PREFIX}/{stamp.strftime('%Y-%m-%d')}.json"
        s3.put_object(Bucket=RESULT_BUCKET, Key=key,
                      Body=json.dumps(report, indent=2).encode())
        print(f"verdict surface -> s3://{RESULT_BUCKET}/{key}")

    # Non-zero on FAILED or on any unverified mode: an exercise programme with
    # holes in it is not a passing programme (§10.2).
    return 1 if (failed or unverified) else 0


if __name__ == "__main__":
    sys.exit(main())
