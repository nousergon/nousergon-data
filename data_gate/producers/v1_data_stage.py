"""Producer for ``data.phase1.v1_data_stage_quiet`` (`alpha-engine-config-I11035`).

Writes ``metrics/v1_data_stage/executions_since_cutover.json`` under
``s3://alpha-engine-research/data_collection``, the key
``data_gate.exit_criteria.read_v1_data_stage_quiet`` already reads and has
read as UNMEASURABLE since `alpha-engine-config-I10954` shipped the clause
with no emitter behind it.

**A gate reads a published artifact; it never surveys live state itself**
(the issue's own rationale for why this cannot be a clause). This module is
the survey: it walks ``states:ListExecutions`` + ``states:GetExecutionHistory``
over ``ne-weekly-freshness-pipeline`` — the ONE v1 state machine the data
stages (``MorningEnrich``, ``DataPhase1``, ``DataPhase2``) live inside as
states, not as separate machines (verified against
``infrastructure/step_function.json``: all three are top-level or
``ResearchPredictorParallel``-nested states of ``ne-weekly-freshness-pipeline``,
never distinct ``stateMachineArn``s) — and publishes the count.

**CUTOVER_UTC is a placeholder, not a measurement (stated assumption).**
`alpha-engine-config-I10655` (the cutover that disables the v1 SFs) is
PRECONDITION-gated and has not run as of 2026-09-18 — the decommission
manifest `nous-ergon-ops/governance/decommission.d/crucible-v1-cutover.yaml`
reads ``status: pending``. I10035 says the real instant "belongs in a
committed artifact" that "the cutover PR is what sets it" — that PR has not
landed. Until it does, this module's own ``CUTOVER_UTC`` constant is the
committed placeholder: the instant this producer started publishing. The
issue's own "Note on ordering" says a non-zero count before cutover is
CORRECT and the producer is "useful before then precisely because it
establishes the baseline" — this constant is that baseline. **The cutover PR
MUST overwrite ``CUTOVER_UTC`` with the true disable instant before phase 1's
exit is evaluated for real**, or this producer is measuring the wrong window;
that overwrite is the cutover PR's own deliverable, not a re-derivation here.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from dataclasses import dataclass
from typing import Any

__all__ = [
    "CUTOVER_UTC",
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "V1_DATA_STAGE_STATE_MACHINE_ARN",
    "V1_DATA_STAGE_STATES",
    "ExecutionCount",
    "build_metric",
    "count_executions_since_cutover",
    "main",
]

logger = logging.getLogger(__name__)

_REGION = "us-east-1"
_ACCOUNT = "711398986525"

#: The ONE v1 state machine the data stages run inside — `infrastructure/
#: step_function.json`, `sf_preflight.py::_REGION`/`_ACCOUNT`.
V1_DATA_STAGE_STATE_MACHINE_ARN = (
    f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:ne-weekly-freshness-pipeline"
)

#: The Task states whose entry means "a v1 data stage actually ran" —
#: distinct from the state machine merely executing (post-cutover the SF
#: keeps running research/predictor/backtester stages; `skip_data_phase1`/
#: `skip_morning_enrich`-style Choice routing means a "quiet" execution
#: enters none of these). Any future data-stage state the cutover disables
#: joins this set in the same PR that adds it to `step_function.json`.
V1_DATA_STAGE_STATES = frozenset(
    {"MorningEnrich", "DataPhase1", "DataPhase2"}
)

#: Placeholder cutover instant — see the module docstring. Replaced by the
#: cutover PR (`alpha-engine-config-I10655`) with the real disable instant.
CUTOVER_UTC = "2026-09-18T00:00:00Z"

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_KEY = "data_collection/metrics/v1_data_stage/executions_since_cutover.json"


@dataclass(frozen=True)
class ExecutionCount:
    executions_since_cutover: int
    executions_scanned: int
    data_stage_executions: tuple[str, ...]


def _parse_cutover(cutover_utc: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(cutover_utc.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _entered_data_stage(sfn: Any, execution_arn: str) -> bool:
    """Did this execution enter any of `V1_DATA_STAGE_STATES`?

    Walks `GetExecutionHistory` (forward, default order) rather than reading
    the execution's output — a Choice-routed skip never enters the Task
    state at all, which is exactly the "quiet" signal this producer exists
    to tell apart from "the state machine fired but routed around the data
    stages".
    """
    paginator = sfn.get_paginator("get_execution_history")
    for page in paginator.paginate(executionArn=execution_arn, maxResults=1000):
        for event in page.get("events", []):
            detail = event.get("stateEnteredEventDetails")
            if detail and detail.get("name") in V1_DATA_STAGE_STATES:
                return True
    return False


def count_executions_since_cutover(
    sfn: Any,
    *,
    state_machine_arn: str = V1_DATA_STAGE_STATE_MACHINE_ARN,
    cutover: dt.datetime,
    scan_cap: int = 500,
) -> ExecutionCount:
    """Executions of `state_machine_arn` started at/after `cutover` that
    entered a v1 data-stage state.

    Pages newest-first and stops once an execution's `startDate` falls
    before `cutover` — `list_executions` is already sorted that way, so this
    never scans the whole history for a machine that runs weekly.
    """
    scanned = 0
    data_stage: list[str] = []
    paginator = sfn.get_paginator("list_executions")
    for page in paginator.paginate(stateMachineArn=state_machine_arn, maxResults=100):
        for execution in page.get("executions", []):
            started = execution["startDate"]
            if started.tzinfo is None:
                started = started.replace(tzinfo=dt.timezone.utc)
            started = started.astimezone(dt.timezone.utc)
            if started < cutover:
                return ExecutionCount(
                    executions_since_cutover=len(data_stage),
                    executions_scanned=scanned,
                    data_stage_executions=tuple(data_stage),
                )
            scanned += 1
            if scanned > scan_cap:
                raise RuntimeError(
                    f"scanned {scanned} executions of {state_machine_arn} without reaching "
                    f"cutover {cutover.isoformat()} — scan_cap={scan_cap} exceeded; the "
                    "cutover instant or the machine ARN is almost certainly wrong"
                )
            if _entered_data_stage(sfn, execution["executionArn"]):
                data_stage.append(execution["executionArn"])
    return ExecutionCount(
        executions_since_cutover=len(data_stage),
        executions_scanned=scanned,
        data_stage_executions=tuple(data_stage),
    )


def build_metric(
    *, cutover_utc: str, count: ExecutionCount, as_of: dt.datetime | None = None
) -> dict:
    as_of = as_of or dt.datetime.now(dt.timezone.utc)
    return {
        "cutover_utc": cutover_utc,
        "executions_since_cutover": count.executions_since_cutover,
        "as_of": as_of.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        # Non-schema diagnostic fields — the clause reader only parses the
        # three above (`data_gate/exit_criteria.py::read_v1_data_stage_quiet`),
        # but a producer that discards its own scan population makes a wrong
        # count unauditable.
        "executions_scanned": count.executions_scanned,
        "data_stage_execution_arns": list(count.data_stage_executions),
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--state-machine-arn", default=V1_DATA_STAGE_STATE_MACHINE_ARN)
    ap.add_argument("--cutover-utc", default=CUTOVER_UTC)
    ap.add_argument("--region", default=_REGION)
    ap.add_argument("--scan-cap", type=int, default=500)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)

    import boto3  # noqa: PLC0415 - deferred so import stays light for tests

    from data_gate.producers._run_record import write_run_record  # noqa: PLC0415

    sfn = boto3.client("stepfunctions", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        cutover = _parse_cutover(args.cutover_utc)
        count = count_executions_since_cutover(
            sfn, state_machine_arn=args.state_machine_arn, cutover=cutover, scan_cap=args.scan_cap
        )
        metric = build_metric(cutover_utc=args.cutover_utc, count=count)
        print(json.dumps(metric, indent=2, sort_keys=True))
    except Exception as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                s3,
                bucket=args.bucket,
                producer="v1_data_stage",
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=str(exc),
            )
        raise

    if not args.no_write:
        s3.put_object(
            Bucket=args.bucket,
            Key=args.key,
            Body=json.dumps(metric, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"WROTE s3://{args.bucket}/{args.key}")

        write_run_record(
            s3,
            bucket=args.bucket,
            producer="v1_data_stage",
            status="ok",
            started_at=started_at,
            finished_at=dt.datetime.now(dt.timezone.utc),
            detail={
                "metric_key": args.key,
                "executions_since_cutover": count.executions_since_cutover,
                "executions_scanned": count.executions_scanned,
            },
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
