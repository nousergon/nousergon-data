"""Producer for ``data.phase1.v1_data_stage_quiet`` (`alpha-engine-config-I11035`).

Writes ``metrics/v1_data_stage/executions_since_cutover.json`` under
``s3://alpha-engine-research/data_collection``, the key
``data_gate.exit_criteria.read_v1_data_stage_quiet`` reads.

**A gate reads a published artifact; it never surveys live state itself**
(I11035's own rationale for why this cannot be a clause). This module is the
survey: it walks ``states:ListExecutions`` + ``states:GetExecutionHistory`` over
EVERY v1 state machine that ran a data stage and publishes the count, in total
and per state machine.

**Three machines, not one** (`alpha-engine-config-I11265`). Under the original
weld all three v1 SFs were disabled together, so surveying only
``ne-weekly-freshness-pipeline`` was harmless. Under the ruled decoupling
(Brian, 2026-09-21, option (b)) ``ne-preopen-trading-pipeline`` and
``ne-postclose-trading-pipeline`` KEEP RUNNING with their data stages removed,
and a survey blind to them would read 0 whether or not those stages were
removed — including if a botched cutover left them live. The per-machine
breakdown makes a non-zero reading name its pipeline.

It counts STATES ENTERED, not executions: ``_entered_data_stage`` walks the
execution history for ``stateEnteredEventDetails.name``, so a surviving v1
execution that never enters a data stage reads 0, which is exactly the
post-cutover "quiet" this clause grades.

**The cutover instant is ``data_gate.cutover.CUTOVER_UTC``** — one committed
fact shared with the parity freeze, PLANNED as the start of the merge window;
that module's docstring says why that is the right value and what to do if the
merge slips. This producer REFUSES to survey from an instant still in the
future: every execution list is empty after a future instant, and publishing
that zero would read quiet on a cutover that has not happened.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from dataclasses import dataclass
from typing import Any

from data_gate.cutover import CUTOVER_UTC, V1_STATE_MACHINE_NAMES

__all__ = [
    "CUTOVER_UTC",
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "V1_DATA_STAGE_STATE_MACHINE_ARNS",
    "V1_DATA_STAGE_STATES",
    "ExecutionCount",
    "MachineCount",
    "build_metric",
    "count_executions_since_cutover",
    "main",
    "public_summary",
]

logger = logging.getLogger(__name__)

_REGION = "us-east-1"
_ACCOUNT = "711398986525"

#: Every v1 state machine a data stage ran inside — `infrastructure/
#: step_function.json` (weekly), `step_function_daily.json` (preopen),
#: `step_function_eod.json` (postclose). The names are
#: `data_gate.cutover.V1_STATE_MACHINE_NAMES`, which the reader also checks a
#: document against, so the survey and its grader cannot disagree about scope.
V1_DATA_STAGE_STATE_MACHINE_ARNS: tuple[str, ...] = tuple(
    f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:{name}" for name in V1_STATE_MACHINE_NAMES
)

#: The Task states whose entry means "a v1 data stage actually ran". Verified
#: against the committed ASLs as of the cutover PR's base (2026-09-24):
#:
#: * weekly (`step_function.json`): `MorningEnrich`, `DataPhase1` (removed by
#:   the cutover), `DataPhase2` (NOT removed — its units are
#:   alpha-engine-config-I10753's own cutover, so the weekly machine still
#:   enters it and this clause reads non-zero until that lands);
#: * preopen (`step_function_daily.json`): `LaunchMorningEnrichSpot`,
#:   `LaunchMorningArcticAppendSpot`;
#: * postclose (`step_function_eod.json`): `LaunchPostMarketDataSpot`,
#:   `LaunchPostMarketArcticAppendSpot`, `LaunchEdgarPitFundamentalsDailySpot`,
#:   and the heal loop's `HealLaunchPostMarketDataSpot`,
#:   `HealLaunchArcticAppendSpot`.
#:
#: `tests/test_v1_data_stage_producer.py` walks all three definitions and fails
#: if any Task state that reaches `alpha-engine-data-spot-dispatcher` is not in
#: this set, so a data stage added or renamed in an ASL cannot leave the survey
#: silently.
#:
#: **`HealStartCollection` is deliberately ABSENT** (alpha-engine-config-I11266
#: deliverable 4). The postclose heal loop now starts the standalone
#: `ne-data-collection-eod` machine rather than running a collector itself —
#: the same single writer the schedule runs — so entering it is not a v1 data
#: stage. Adding it here would red `v1_data_stage_quiet` on every healed day for
#: the one writer the cutover exists to establish. `step_function_eod.json`'s
#: own comment on that state says the same.
V1_DATA_STAGE_STATES = frozenset(
    {
        # weekly
        "MorningEnrich",
        "DataPhase1",
        "DataPhase2",
        # preopen
        "LaunchMorningEnrichSpot",
        "LaunchMorningArcticAppendSpot",
        # postclose
        "LaunchPostMarketDataSpot",
        "LaunchPostMarketArcticAppendSpot",
        "LaunchEdgarPitFundamentalsDailySpot",
        "HealLaunchPostMarketDataSpot",
        "HealLaunchArcticAppendSpot",
    }
)

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_KEY = "data_collection/metrics/v1_data_stage/executions_since_cutover.json"

#: Per machine. The two daily machines run ~250 executions a year each, so a
#: cutover instant set months back exceeds this and RAISES rather than
#: silently truncating the survey (I11265 deliverable 5).
DEFAULT_SCAN_CAP = 500


@dataclass(frozen=True)
class MachineCount:
    state_machine: str
    executions_since_cutover: int
    executions_scanned: int
    data_stage_executions: tuple[str, ...]


@dataclass(frozen=True)
class ExecutionCount:
    machines: tuple[MachineCount, ...]

    @property
    def executions_since_cutover(self) -> int:
        return sum(m.executions_since_cutover for m in self.machines)

    @property
    def executions_scanned(self) -> int:
        return sum(m.executions_scanned for m in self.machines)

    @property
    def data_stage_executions(self) -> tuple[str, ...]:
        return tuple(arn for m in self.machines for arn in m.data_stage_executions)


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_cutover(cutover_utc: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(cutover_utc.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _machine_name(state_machine_arn: str) -> str:
    return state_machine_arn.rsplit(":", 1)[-1]


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


def _count_one(sfn: Any, state_machine_arn: str, cutover: dt.datetime, scan_cap: int) -> MachineCount:
    """Executions of ONE machine started at/after `cutover` that entered a
    v1 data-stage state.

    Pages newest-first and stops once an execution's `startDate` falls
    before `cutover` — `list_executions` is already sorted that way.
    """
    scanned = 0
    data_stage: list[str] = []
    name = _machine_name(state_machine_arn)
    paginator = sfn.get_paginator("list_executions")
    for page in paginator.paginate(stateMachineArn=state_machine_arn, maxResults=100):
        for execution in page.get("executions", []):
            started = execution["startDate"]
            if started.tzinfo is None:
                started = started.replace(tzinfo=dt.timezone.utc)
            started = started.astimezone(dt.timezone.utc)
            if started < cutover:
                return MachineCount(name, len(data_stage), scanned, tuple(data_stage))
            scanned += 1
            if scanned > scan_cap:
                raise RuntimeError(
                    f"scanned {scanned} executions of {name} without reaching cutover "
                    f"{cutover.isoformat()} — scan_cap={scan_cap} (per machine) exceeded; "
                    "the cutover instant or the machine ARN is almost certainly wrong"
                )
            if _entered_data_stage(sfn, execution["executionArn"]):
                data_stage.append(execution["executionArn"])
    return MachineCount(name, len(data_stage), scanned, tuple(data_stage))


def count_executions_since_cutover(
    sfn: Any,
    *,
    state_machine_arns: tuple[str, ...] = V1_DATA_STAGE_STATE_MACHINE_ARNS,
    cutover: dt.datetime,
    scan_cap: int = DEFAULT_SCAN_CAP,
    now: dt.datetime | None = None,
) -> ExecutionCount:
    """Every machine in `state_machine_arns`, each under its own `scan_cap`.

    RAISES when `cutover` is still in the future: the survey of a window that
    has not begun is an empty list, and publishing its zero would read quiet.
    """
    now = now or _now()
    if cutover > now:
        raise RuntimeError(
            f"cutover {cutover.isoformat()} is in the future (now {now.isoformat()}); a "
            "survey from it counts nothing and would publish a vacuous quiet. "
            "data_gate/cutover.py::CUTOVER_UTC is PLANNED until the cutover PR merges."
        )
    if not state_machine_arns:
        raise ValueError("no state machines to survey; an empty survey is not a zero")
    return ExecutionCount(
        tuple(_count_one(sfn, arn, cutover, scan_cap) for arn in state_machine_arns)
    )


def build_metric(
    *, cutover_utc: str, count: ExecutionCount, as_of: dt.datetime | None = None
) -> dict:
    as_of = as_of or _now()
    return {
        "cutover_utc": cutover_utc,
        "executions_since_cutover": count.executions_since_cutover,
        "as_of": as_of.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        # Per machine (alpha-engine-config-I11265 deliverable 1): a non-zero
        # total names its pipeline, and the reader refuses a document that
        # does not cover every v1 machine.
        "per_state_machine": {
            m.state_machine: {
                "executions_since_cutover": m.executions_since_cutover,
                "executions_scanned": m.executions_scanned,
            }
            for m in count.machines
        },
        # Non-schema diagnostic fields — a producer that discards its own scan
        # population makes a wrong count unauditable.
        "executions_scanned": count.executions_scanned,
        "data_stage_execution_arns": list(count.data_stage_executions),
    }


def public_summary(metric: dict) -> dict:
    """The metric minus the execution ARNs (they embed the account ID).

    What `main` prints: this repo and its Actions logs are PUBLIC
    (alpha-engine-config-I11274), so a log gets machine NAMES and counts,
    never ARNs. The full document stays in S3.
    """
    return {k: v for k, v in metric.items() if k != "data_stage_execution_arns"}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument(
        "--state-machine-arn",
        action="append",
        dest="state_machine_arns",
        help="repeatable; default: all three v1 machines",
    )
    ap.add_argument("--cutover-utc", default=CUTOVER_UTC)
    ap.add_argument("--region", default=_REGION)
    ap.add_argument("--scan-cap", type=int, default=DEFAULT_SCAN_CAP)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)

    import boto3  # noqa: PLC0415 - deferred so import stays light for tests

    from data_gate.producers._run_record import write_run_record  # noqa: PLC0415

    sfn = boto3.client("stepfunctions", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    started_at = _now()
    try:
        cutover = _parse_cutover(args.cutover_utc)
        count = count_executions_since_cutover(
            sfn,
            state_machine_arns=tuple(args.state_machine_arns or V1_DATA_STAGE_STATE_MACHINE_ARNS),
            cutover=cutover,
            scan_cap=args.scan_cap,
            now=started_at,
        )
        metric = build_metric(cutover_utc=args.cutover_utc, count=count)
        # NEVER the full document (same class as alpha-engine-config-I11274's
        # CodeQL finding on cost_monthly.py): this repo is PUBLIC and its
        # GitHub Actions logs are public, and `metric` carries
        # `data_stage_execution_arns` — full Step Functions execution ARNs,
        # embedding the account ID. Those stay in the S3 document; a public
        # log gets `public_summary` — machine names and counts only.
        per_machine = ", ".join(
            f"{m.state_machine}={m.executions_since_cutover}" for m in count.machines
        )
        print(
            f"{args.key}: executions_since_cutover={count.executions_since_cutover} "
            f"({per_machine}), status=ok"
        )
        print(json.dumps(public_summary(metric), sort_keys=True))
    except Exception as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                s3,
                bucket=args.bucket,
                producer="v1_data_stage",
                status="error",
                started_at=started_at,
                finished_at=_now(),
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
            finished_at=_now(),
            detail={
                "metric_key": args.key,
                "executions_since_cutover": count.executions_since_cutover,
                "executions_scanned": count.executions_scanned,
                "per_state_machine": {
                    m.state_machine: m.executions_since_cutover for m in count.machines
                },
            },
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
