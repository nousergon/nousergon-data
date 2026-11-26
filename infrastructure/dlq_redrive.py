#!/usr/bin/env python3
"""dlq_redrive.py — replay the Overseer intake DLQ back to its live queue
(alpha-engine-config-I8111).

WHY THIS EXISTS. On 2026-08-21 `nousergon-overseer-intake-dlq` held 93
visible messages, oldest 218h24m (9.1 days) against a 14-day retention —
~4.9 days from silent expiry. Nothing replayed them: `grep -rn
"StartMessageMoveTask"` across the fleet returned zero hits (SQS's native
DLQ redrive was never wired up), and
`alpha-engine-config/scripts/dlq_archive_purge.py` is the only existing DLQ
tool and is archive-then-DELETE, never replay.

RULING (alpha-engine-config-I8120, Brian, 2026-08-21): no standing
autonomous schedule redrives this queue. A pause suspends ACTION, never
OBSERVATION — the DLQ's age-vs-retention threshold is a plain CloudWatch
alarm (`nous-ergon-ops/infrastructure/cloudwatch/alarms/
alpha-engine-watch-plane-overseer-intake-dlq-age.json`), which stays armed
through a pause by design. This module is the ACTOR half: it does not run
on a schedule. It is called by `infrastructure/reactivate_paused_lane.py`
as part of reactivating a paused lane's triggers — "before or with the
first run" (I8111 deliverable 1) — and is also directly invocable as a CLI
for an ad hoc redrive.

MECHANISM. Native SQS `StartMessageMoveTask` (preferred, per I8111's
binding constraint, over a hand-rolled receive/send/delete loop — it is
the AWS-native primitive for exactly this, async, throttleable). Bounded
and idempotent: `ListMessageMoveTasks` is checked before every start, since
SQS permits only one active move task per source queue — a second
invocation while one is already running polls and reports progress rather
than starting a duplicate.

POISON VS BACKLOG (deliverable 3) needed no separate code here — it falls
out of this function plus the queue's own `RedrivePolicy
maxReceiveCount=5` (`infrastructure/setup_overseer_intake.sh`). A message
already in the DLQ when this runs failed 5 times under UNKNOWN conditions
(possibly just "the consumer was paused" — backlog, not poison). Redriving
it back gives a live consumer first refusal with no prejudgment. Only a
message that fails 5 MORE times against that now-live consumer is
unambiguous poison, and SQS's own redrive policy returns exactly that
message to the DLQ a second time — where
`alpha-engine-config/scripts/dlq_archive_purge.py`'s existing
archive-then-purge path already applies.

FAIL LOUD (fleet no-silent-swallows rule). Any unexpected
`StartMessageMoveTask` / `ListMessageMoveTasks` / `GetQueueAttributes`
failure raises `DlqRedriveError` — no bare except, no silent no-op. The
one expected AWS condition (a move task started by a racing invocation) is
matched by message text, not swallowed generically.

CLI usage:

    python3 infrastructure/dlq_redrive.py \\
        --dlq-name nousergon-overseer-intake-dlq \\
        --dest-name nousergon-overseer-intake
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Protocol, TypedDict, cast

import boto3  # pyright: ignore[reportMissingTypeStubs] -- boto3 ships no type information
from botocore.exceptions import (  # pyright: ignore[reportMissingTypeStubs] -- botocore ships no type information
    ClientError,
)

logger = logging.getLogger("dlq_redrive")

DEFAULT_DLQ_NAME = "nousergon-overseer-intake-dlq"
DEFAULT_DEST_NAME = "nousergon-overseer-intake"

_RUNNING_STATUSES = {"RUNNING"}


class _QueueUrlResponse(TypedDict):
    QueueUrl: str


class _QueueAttributesResponse(TypedDict):
    Attributes: dict[str, str]


class _MoveTask(TypedDict, total=False):
    Status: str
    ApproximateNumberOfMessagesMoved: int
    ApproximateNumberOfMessagesToMove: int


class _MoveTasksResponse(TypedDict):
    Results: list[_MoveTask]


class _StartMoveTaskResponse(TypedDict):
    TaskHandle: str | None


class RedriveSummary(TypedDict, total=False):
    action: str
    status: str
    messages_moved: int
    messages_to_move: int
    reason: str
    dlq_depth: int
    task_handle: str | None
    dlq_depth_at_start: int


class _SqsClient(Protocol):
    def get_queue_url(self, *, QueueName: str) -> _QueueUrlResponse: ...  # noqa: N803

    def get_queue_attributes(  # noqa: N802
        self, *, QueueUrl: str, AttributeNames: list[str]  # noqa: N803
    ) -> _QueueAttributesResponse: ...

    def list_message_move_tasks(self, *, SourceArn: str) -> _MoveTasksResponse: ...  # noqa: N803

    def start_message_move_task(self, **kwargs: str | int) -> _StartMoveTaskResponse: ...


class DlqRedriveError(RuntimeError):
    """Raised on any unexpected SQS condition. Fail-loud by default — a
    swallowed error here reproduces the exact invisible-failure class
    alpha-engine-config-I8111 closes."""


def _queue_url(sqs: _SqsClient, name: str) -> str:
    try:
        return sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except ClientError as exc:
        raise DlqRedriveError(f"get_queue_url({name}) failed: {exc}") from exc


def _queue_arn(sqs: _SqsClient, queue_url: str) -> str:
    try:
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        return attrs["Attributes"]["QueueArn"]
    except (ClientError, KeyError) as exc:
        raise DlqRedriveError(
            f"get_queue_attributes(QueueArn) for {queue_url} failed: {exc}"
        ) from exc


def _queue_depth(sqs: _SqsClient, queue_url: str) -> int:
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]
    except ClientError as exc:
        raise DlqRedriveError(f"get_queue_attributes(depth) for {queue_url} failed: {exc}") from exc
    return int(attrs.get("ApproximateNumberOfMessages", "0"))


def _active_move_task(sqs: _SqsClient, dlq_arn: str) -> _MoveTask | None:
    try:
        tasks = sqs.list_message_move_tasks(SourceArn=dlq_arn).get("Results", [])
    except ClientError as exc:
        raise DlqRedriveError(f"list_message_move_tasks({dlq_arn}) failed: {exc}") from exc
    for task in tasks:
        if task.get("Status") in _RUNNING_STATUSES:
            return task
    return None


def _start_redrive(
    sqs: _SqsClient, dlq_arn: str, dest_arn: str, max_per_second: int | None
) -> _StartMoveTaskResponse:
    kwargs: dict[str, str | int] = {"SourceArn": dlq_arn, "DestinationArn": dest_arn}
    if max_per_second:
        kwargs["MaxNumberOfMessagesPerSecond"] = max_per_second
    try:
        return sqs.start_message_move_task(**kwargs)
    except ClientError as exc:
        # A task another concurrent invocation started between our check and
        # this call races only against ourselves (idempotent by
        # construction, not by luck): SQS rejects the second start with a
        # message naming an existing move task, which is exactly the
        # "already redriving" state _active_move_task would otherwise have
        # reported. Not a bare except — matched by the specific message
        # text; everything else re-raises as a DlqRedriveError.
        if "already" in str(exc).lower() and "move task" in str(exc).lower():
            logger.info("redrive: a move task started concurrently — treating as running")
            return {"TaskHandle": None}
        raise DlqRedriveError(
            f"start_message_move_task({dlq_arn} -> {dest_arn}) failed: {exc}"
        ) from exc


def redrive(
    sqs: _SqsClient,
    dlq_name: str = DEFAULT_DLQ_NAME,
    dest_name: str = DEFAULT_DEST_NAME,
    max_per_second: int | None = None,
) -> RedriveSummary:
    """Deliverable 1 + 3. Returns a summary dict; never raises on the
    expected "nothing to do" / "already running" paths — only on a genuine
    SQS failure (DlqRedriveError)."""
    dlq_url = _queue_url(sqs, dlq_name)
    dlq_arn = _queue_arn(sqs, dlq_url)
    dest_url = _queue_url(sqs, dest_name)
    dest_arn = _queue_arn(sqs, dest_url)

    active = _active_move_task(sqs, dlq_arn)
    if active is not None:
        moved = active.get("ApproximateNumberOfMessagesMoved", 0)
        to_move = active.get("ApproximateNumberOfMessagesToMove", 0)
        logger.info(
            "redrive: move task already RUNNING for %s -> %s (moved=%s to_move=%s)",
            dlq_arn, dest_arn, moved, to_move,
        )
        return {
            "action": "poll",
            "status": "RUNNING",
            "messages_moved": moved,
            "messages_to_move": to_move,
        }

    depth = _queue_depth(sqs, dlq_url)
    if depth == 0:
        logger.info("redrive: %s empty — nothing to redrive", dlq_name)
        return {"action": "skip", "reason": "dlq-empty", "dlq_depth": 0}

    result = _start_redrive(sqs, dlq_arn, dest_arn, max_per_second)
    logger.info(
        "redrive: started move task %s (%s -> %s), dlq_depth_at_start=%d",
        result.get("TaskHandle"), dlq_arn, dest_arn, depth,
    )
    return {
        "action": "started",
        "task_handle": result.get("TaskHandle"),
        "dlq_depth_at_start": depth,
    }


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dlq-name", default=DEFAULT_DLQ_NAME)
    p.add_argument("--dest-name", default=DEFAULT_DEST_NAME)
    p.add_argument("--max-per-second", type=int, default=None)
    p.add_argument("--region", default="us-east-1")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _build_parser().parse_args(argv)
    sqs = cast(
        _SqsClient,
        boto3.client(  # pyright: ignore[reportUnknownMemberType] -- boto3 has no bundled types
            "sqs", region_name=args.region
        ),
    )
    result = redrive(sqs, args.dlq_name, args.dest_name, args.max_per_second)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
