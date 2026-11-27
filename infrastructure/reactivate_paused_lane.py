#!/usr/bin/env python3
"""reactivate_paused_lane.py — the reactivation path deliverable 1 of
alpha-engine-config-I8111 asks for: "when a paused lane's triggers are
re-enabled, its DLQ is redriven before or with the first run."

WHY THIS EXISTS. `infrastructure/automation_pause.json`'s own `ruling`
block documents the un-pause procedure: "un-pausing is per-entry: delete
the entry, run automation_pause.py --enforce is NOT needed to re-enable -
re-enable explicitly with the AWS CLI or a deploy.sh reconcile."
`automation_pause.py` deliberately has NO enable path of its own — its
`enforce()` only ever DISABLES drift back to the declared pause, by
design: "re-enabling scheduled work unattended is what the pause
forbids." So today, reactivation IS a raw, hand-typed `aws scheduler
update-schedule` / `aws events enable-rule` command with no code path at
all — which is exactly how `alpha-engine-config-I8110` happened: the four
`alpha-engine-alert-drain-*utc` schedules were re-enabled live on
2026-08-21T21:03Z and NOTHING replayed the 93-message DLQ backlog that had
accumulated while they were off. It drained only because the very next
scheduled run, at 22:00Z, happened to clear it — a recovery by
coincidence, not a mechanism.

This script is the smallest thing that gives reactivation a code path,
without inventing a daemon (alpha-engine-config-I8120 ruling) and without
touching `automation_pause.py`/`automation_pause.json` (both had a
concurrently in-flight PR — `nousergon-data-PR1500` — at the time this was
written). It does NOT read or write the pause manifest; the manifest edit
("delete the entry") documented in `ruling.duration` stays a manual,
deliberate step for whoever runs this — printed as a reminder at the end,
never performed automatically, matching automation_pause.py's own posture
that a trigger's live ON/OFF state is never flipped by unattended code.

WHAT IT DOES, in order:

  1. Redrives the named DLQ back to its live queue via
     `infrastructure.dlq_redrive.redrive` — native `StartMessageMoveTask`,
     started BEFORE any trigger is touched, so "before or with the first
     run" is satisfied by construction: the move task is already moving
     messages by the time the first re-enabled schedule can possibly fire.
  2. Re-enables every named EventBridge Scheduler schedule / EventBridge
     rule. `scheduler:UpdateSchedule` is a full-object replace (no partial
     PATCH, no dedicated enable verb) — mirrors the round-trip
     `automation_pause.py::_disable` already uses for the disable
     direction: `GetSchedule`, flip `State`, `UpdateSchedule` with every
     other field preserved.

FAIL LOUD. Every step that fails raises `ReactivationError` and the script
exits non-zero — no bare except, no "reactivated 3 of 4 and called it
done" silently. Every schedule is still attempted (a failure on one name
does not skip the rest); if any failed, the summary names which, and the
process exits 1 after printing it.

DEFAULT TARGET. The four `alpha-engine-alert-drain-*utc` schedules are the
only lane wired to `nousergon-overseer-intake-dlq` today, so they are the
default. `--schedules` / `--events-rules` override this for a future
paused lane with its own DLQ.

Usage:

    python3 infrastructure/reactivate_paused_lane.py
    python3 infrastructure/reactivate_paused_lane.py \\
        --schedules alpha-engine-alert-drain-0400utc alpha-engine-alert-drain-1000utc \\
        --dlq-name nousergon-overseer-intake-dlq --dest-name nousergon-overseer-intake
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Protocol, TypedDict, cast

import boto3  # pyright: ignore[reportMissingTypeStubs] -- boto3 does not ship type information.
from botocore.exceptions import ClientError  # pyright: ignore[reportMissingTypeStubs] -- botocore does not ship type information.

import dlq_redrive
from dlq_redrive import DlqRedriveError

logger = logging.getLogger("reactivate_paused_lane")

DEFAULT_SCHEDULES = (
    "alpha-engine-alert-drain-0400utc",
    "alpha-engine-alert-drain-1000utc",
    "alpha-engine-alert-drain-1600utc",
    "alpha-engine-alert-drain-2200utc",
)
DEFAULT_DLQ_NAME = "nousergon-overseer-intake-dlq"
DEFAULT_DEST_NAME = "nousergon-overseer-intake"


class _SchedulerClient(Protocol):
    def get_schedule(self, *, Name: str) -> dict[str, object]: ...  # noqa: N803

    def update_schedule(self, **kwargs: object) -> object: ...


class _EventsClient(Protocol):
    def enable_rule(self, *, Name: str) -> object: ...  # noqa: N803


class _Arguments(Protocol):
    schedules: list[str]
    events_rules: list[str]
    dlq_name: str
    dest_name: str
    region: str


class ReactivationResult(TypedDict):
    redrive: dict[str, object]
    enabled: list[str]


class _Redrive(Protocol):
    def __call__(
        self, sqs: object, *, dlq_name: str, dest_name: str
    ) -> dict[str, object]: ...


redrive = cast("_Redrive", getattr(dlq_redrive, "redrive"))


class ReactivationError(RuntimeError):
    """Raised when the DLQ redrive or a trigger re-enable fails
    unexpectedly. Fail-loud by default — a swallowed error here means a
    lane looks reactivated when it is not."""


def _enable_scheduler_schedule(
    scheduler_client: _SchedulerClient, name: str, region: str
) -> None:
    """Round-trips the schedule: GetSchedule -> flip State=ENABLED ->
    UpdateSchedule with every other field carried forward. Mirrors
    automation_pause.py::_disable's documented reasoning: Scheduler has no
    partial-update/enable verb, so the live spec must be round-tripped or
    every unspecified attribute is lost."""
    try:
        spec = scheduler_client.get_schedule(Name=name)
    except ClientError as exc:
        raise ReactivationError(f"scheduler get_schedule({name}) failed: {exc}") from exc
    for derived in ("Arn", "CreationDate", "LastModificationDate", "ResponseMetadata"):
        spec.pop(derived, None)
    spec["Name"] = name
    spec["State"] = "ENABLED"
    try:
        scheduler_client.update_schedule(**spec)
    except ClientError as exc:
        raise ReactivationError(f"scheduler update_schedule({name}) failed: {exc}") from exc


def _enable_events_rule(events_client: _EventsClient, name: str) -> None:
    try:
        events_client.enable_rule(Name=name)
    except ClientError as exc:
        raise ReactivationError(f"events enable_rule({name}) failed: {exc}") from exc


def reactivate(
    sqs_client: object,
    scheduler_client: _SchedulerClient,
    events_client: _EventsClient,
    *,
    schedules: tuple[str, ...] = (),
    events_rules: tuple[str, ...] = (),
    dlq_name: str = DEFAULT_DLQ_NAME,
    dest_name: str = DEFAULT_DEST_NAME,
    region: str = "us-east-1",
) -> ReactivationResult:
    """Redrive first, then re-enable every named trigger. Returns a summary
    dict. Raises ReactivationError only for the redrive step's own
    DlqRedriveError (re-wrapped) or if the caller passed no triggers at
    all — every per-trigger enable failure is collected instead, so one
    bad name does not abort the rest, and reported at the end."""
    if not schedules and not events_rules:
        raise ReactivationError("no --schedules or --events-rules given — nothing to reactivate")

    try:
        redrive_result = redrive(sqs_client, dlq_name=dlq_name, dest_name=dest_name)
    except DlqRedriveError as exc:
        raise ReactivationError(f"DLQ redrive failed — triggers NOT touched: {exc}") from exc
    logger.info("redrive: %s", json.dumps(redrive_result))

    enabled: list[str] = []
    failed: dict[str, str] = {}
    for name in schedules:
        try:
            _enable_scheduler_schedule(scheduler_client, name, region)
            enabled.append(name)
            logger.info("enabled schedule: %s", name)
        except ReactivationError as exc:
            failed[name] = str(exc)
            logger.error("FAILED to enable schedule %s: %s", name, exc)
    for name in events_rules:
        try:
            _enable_events_rule(events_client, name)
            enabled.append(name)
            logger.info("enabled events rule: %s", name)
        except ReactivationError as exc:
            failed[name] = str(exc)
            logger.error("FAILED to enable events rule %s: %s", name, exc)

    if failed:
        raise ReactivationError(
            f"redrive succeeded but {len(failed)} trigger(s) failed to re-enable: {failed}. "
            f"Successfully enabled: {enabled}."
        )

    return {"redrive": redrive_result, "enabled": enabled}


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--schedules", nargs="*", default=list(DEFAULT_SCHEDULES))
    p.add_argument("--events-rules", nargs="*", default=[])
    p.add_argument("--dlq-name", default=DEFAULT_DLQ_NAME)
    p.add_argument("--dest-name", default=DEFAULT_DEST_NAME)
    p.add_argument("--region", default="us-east-1")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = cast("_Arguments", _build_parser().parse_args(argv))
    # boto3's dynamically generated clients have no library-provided types.
    sqs = cast(
        "object",
        boto3.client("sqs", region_name=args.region),  # pyright: ignore[reportUnknownMemberType] -- dynamically generated client
    )
    scheduler = cast(
        "_SchedulerClient",
        boto3.client("scheduler", region_name=args.region),  # pyright: ignore[reportUnknownMemberType] -- dynamically generated client
    )
    events = cast(
        "_EventsClient",
        boto3.client("events", region_name=args.region),  # pyright: ignore[reportUnknownMemberType] -- dynamically generated client
    )
    try:
        result = reactivate(
            sqs, scheduler, events,
            schedules=tuple(args.schedules), events_rules=tuple(args.events_rules),
            dlq_name=args.dlq_name, dest_name=args.dest_name, region=args.region,
        )
    except ReactivationError as exc:
        print(f"REACTIVATION FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    print(
        "\nREMINDER (automation_pause.json ruling.duration): un-pausing is per-entry — "
        "delete each reactivated trigger's entry from `paused`/`pending` (or move it to "
        "`not_paused`) in infrastructure/automation_pause.json in the SAME change. This "
        "script does not edit that file.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
