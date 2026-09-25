"""Producer for ``data.phase1.triggers_reconciled`` (`alpha-engine-config-I11189`
deliverable 2, with `-I11194`'s DISABLED-annotation check).

Writes ``metrics/trigger_observation/latest.json`` under
``s3://alpha-engine-research/data_collection``: what each live trigger a unit
descriptor names actually IS, read from AWS. It publishes observations only —
no verdict. `data_gate/trigger_reconcile.py` does the reconciling at gate-read
time against the descriptors on the gate's own checkout, so a descriptor edited
away from its live trigger reads red on the next gate read without waiting for
this producer (`alpha-engine-config-I11035`: a gate reads a published artifact;
it never surveys live state itself).

**Read-only against AWS.** Three read calls, one per trigger kind, and no other:

* ``states:ListExecutions`` on each owning state machine — its execution START
  instants over the lookback window. Execution history is a first-class source
  because a machine is not always started by a schedule object this producer
  could read instead (no enabled rule or Scheduler entry targets
  ``ne-postclose-trading-pipeline``; measured 2026-09-20).
* ``events:DescribeRule`` on each EventBridge rule — ``State`` and
  ``ScheduleExpression``.
* ``scheduler:GetSchedule`` on each Scheduler entry — ``State``,
  ``ScheduleExpression`` and ``ScheduleExpressionTimezone``.

The only write is this producer's own metric document and run record.

**The owner set is DERIVED from the descriptors**, never hand-listed
(`observability-policy` §2.2): every non-retired unit declaring a
``trigger.schedule`` on an AWS trigger kind contributes its owner. A read that
fails (not found, access denied) is recorded against that owner as
``status: not_found`` / ``error`` — the clause renders that unit UNRECONCILABLE,
never reconciled. Nothing here swallows a failure into a green.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from typing import Any

from data_gate.descriptors import Unit, load_units
from data_gate.trigger_reconcile import OBSERVATION_SCHEMA, owner_key, units_in_scope

__all__ = [
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "LOOKBACK_DAYS",
    "build_document",
    "main",
    "observe_owner",
    "owner_keys",
]

logger = logging.getLogger(__name__)

_REGION = "us-east-1"
_ACCOUNT = "711398986525"

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_KEY = "data_collection/metrics/trigger_observation/latest.json"

#: Three Saturdays for the one weekly schedule, fifteen trading days for the
#: daily ones — enough fires that one missed trigger cannot read as a moved one
#: (`trigger_reconcile._reconcile_executions`'s majority rule). Also the bound
#: `cadence.latest_due_fire` walks back.
LOOKBACK_DAYS = 21

#: A machine that has run far more often than daily over the window is being
#: driven by something other than its schedule (a rerun storm); stop paging
#: rather than walk its whole history, and say so.
_EXECUTION_SCAN_CAP = 1000


def owner_keys(units: list[Unit]) -> list[str]:
    """Every live-surface key the in-scope descriptors name, sorted, deduplicated."""
    in_scope, _ = units_in_scope(units)
    return sorted({k for k in (owner_key(u) for u in in_scope) if k})


def _iso(instant: dt.datetime) -> str:
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=dt.timezone.utc)
    return (
        instant.astimezone(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None) or {}
    return str((response.get("Error") or {}).get("Code") or type(exc).__name__)


def _execution_starts(sfn: Any, machine: str, *, since: dt.datetime) -> list[str]:
    arn = f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:{machine}"
    starts: list[str] = []
    paginator = sfn.get_paginator("list_executions")
    for page in paginator.paginate(
        stateMachineArn=arn, PaginationConfig={"PageSize": 100}
    ):
        for execution in page.get("executions", []):
            started = execution["startDate"]
            if started.tzinfo is None:
                started = started.replace(tzinfo=dt.timezone.utc)
            if started < since:
                return starts  # newest-first: everything after this is older
            starts.append(_iso(started))
            if len(starts) >= _EXECUTION_SCAN_CAP:
                raise RuntimeError(
                    f"{machine}: {len(starts)} executions inside the window without reaching its start; "
                    "refusing to publish a truncated start history as a complete one"
                )
    return starts


def observe_owner(
    key: str, *, sfn: Any, events: Any, scheduler: Any, since: dt.datetime
) -> dict:
    """One live trigger, as observed. Never raises for a per-owner read failure:
    the failure IS the observation, recorded so the clause can name it."""
    kind, _, name = key.partition(":")
    try:
        if kind == "step-functions":
            return {
                "kind": kind,
                "status": "observed",
                "execution_starts": _execution_starts(sfn, name, since=since),
            }
        if kind == "eventbridge-rule":
            rule = events.describe_rule(Name=name)
            return {
                "kind": kind,
                "status": "observed",
                "state": rule.get("State"),
                "schedule_expression": rule.get("ScheduleExpression"),
            }
        if kind == "eventbridge-scheduler":
            group, _, schedule_name = name.partition("/")
            schedule = scheduler.get_schedule(GroupName=group, Name=schedule_name)
            return {
                "kind": kind,
                "status": "observed",
                "state": schedule.get("State"),
                "schedule_expression": schedule.get("ScheduleExpression"),
                "timezone": schedule.get("ScheduleExpressionTimezone") or "UTC",
            }
    except Exception as exc:  # noqa: BLE001 - recorded per owner, see docstring
        code = _error_code(exc)
        status = "not_found" if "NotFound" in code else "error"
        return {"kind": kind, "status": status, "error": code}
    return {"kind": kind, "status": "error", "error": f"unknown trigger kind {kind!r}"}


def build_document(
    keys: list[str],
    *,
    sfn: Any,
    events: Any,
    scheduler: Any,
    now: dt.datetime,
    lookback_days: int = LOOKBACK_DAYS,
) -> dict:
    since = now - dt.timedelta(days=lookback_days)
    return {
        "schema_version": OBSERVATION_SCHEMA,
        "as_of": _iso(now),
        "window_start": _iso(since),
        "lookback_days": lookback_days,
        "owners": {
            key: observe_owner(
                key, sfn=sfn, events=events, scheduler=scheduler, since=since
            )
            for key in keys
        },
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--region", default=_REGION)
    ap.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)

    import boto3  # noqa: PLC0415 - deferred so import stays light for tests

    from data_gate.producers._run_record import write_run_record  # noqa: PLC0415

    s3 = boto3.client("s3", region_name=args.region)
    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        keys = owner_keys(load_units())
        document = build_document(
            keys,
            sfn=boto3.client("stepfunctions", region_name=args.region),
            events=boto3.client("events", region_name=args.region),
            scheduler=boto3.client("scheduler", region_name=args.region),
            now=started_at,
            lookback_days=args.lookback_days,
        )
        statuses = [o["status"] for o in document["owners"].values()]
        # Counts only: this repo's Actions logs are PUBLIC (alpha-engine-
        # config-I11274's posture), and the document names every trigger.
        print(
            f"{args.key}: owners={len(keys)} observed={statuses.count('observed')} "
            f"not_found={statuses.count('not_found')} error={statuses.count('error')}"
        )
    except (
        Exception
    ) as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                s3,
                bucket=args.bucket,
                producer="trigger_observation",
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=str(exc),
            )
        raise

    if args.no_write:
        print(json.dumps(document, indent=2, sort_keys=True))
        return 0

    s3.put_object(
        Bucket=args.bucket,
        Key=args.key,
        Body=json.dumps(document, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"WROTE s3://{args.bucket}/{args.key}")
    write_run_record(
        s3,
        bucket=args.bucket,
        producer="trigger_observation",
        status="ok",
        started_at=started_at,
        finished_at=dt.datetime.now(dt.timezone.utc),
        detail={
            "metric_key": args.key,
            "owners": len(keys),
            "observed": statuses.count("observed"),
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
