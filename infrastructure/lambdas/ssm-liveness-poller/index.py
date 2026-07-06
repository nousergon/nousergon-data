"""alpha-engine-ssm-liveness-poller — liveness-aware SSM command poller.

One Lambda invocation = one poll iteration for a Step Functions SSM
polling loop. Replaces the bare ``ssm:getCommandInvocation`` poll states
in ``ne-preopen-trading-pipeline`` (previously copy-pasted 4x with
drifted semantics: only MorningEnrich ever got the bounded-attempt cap
from #970, and none of them checked instance liveness).

Why this exists (config#1811, 2026-07-06 incident): the SSM agent
enforces a command's ``executionTimeout`` FROM INSIDE the box being
watched. When the trading box became unresponsive under memory pressure
(config#1807), the stuck ``MorningArcticAppend`` command was not killed
until the agent happened to reconnect — 22 minutes PAST its 40-minute
timeout — while the SF poll loop read a frozen ``InProgress`` forever.
A watchdog that dies with its watchee is not a watchdog. This poller
combines, per iteration, in one place OUTSIDE the box:

  1. ``ssm:GetCommandInvocation``    — command status (as before);
  2. ``ssm:DescribeInstanceInformation`` — the agent's ``PingStatus``,
     the independent liveness signal (per the config#1724 principle:
     independent observation beats self-report);
  3. bounded-attempt + consecutive-ping-miss accounting (counters are
     carried through SF state — this Lambda is stateless).

Verdicts (the Choice states branch on exactly these):
  SUCCESS               — command reached Status=Success.
  IN_PROGRESS           — command still running (Pending/InProgress/
                          Delayed, or invocation not yet registered),
                          instance responsive, budgets not exhausted.
  COMMAND_FAILED        — command reached a terminal non-success status
                          (Failed / TimedOut / Cancelled / Cancelling).
  INSTANCE_UNRESPONSIVE — >= max_ping_misses consecutive polls saw
                          PingStatus != Online while the command was
                          nominally running. The 2026-07-06 shape:
                          detection in ~1 minute instead of 62.
  POLL_BUDGET_EXHAUSTED — attempts >= max_attempts without a terminal
                          status (the #970 stuck-InProgress shape).

Fail-loud: unexpected AWS errors RAISE (the SF Catch routes to
HandleFailure). The only swallowed error is InvocationDoesNotExist
during the registration window, which is expected SSM eventual
consistency and is still bounded by the attempt budget.

This Lambda is deliberately READ-ONLY (least privilege): remediation of
INSTANCE_UNRESPONSIVE (force-stopping the box) is a separate SF state
owned by the state machine's role, not this function.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ssm = boto3.client("ssm")

# Command statuses that mean "still running / not yet terminal".
_RUNNING_STATUSES = {"Pending", "InProgress", "Delayed"}
# Terminal, non-success.
_FAILED_STATUSES = {"Failed", "TimedOut", "Cancelled", "Cancelling"}

_STDERR_TAIL_CHARS = 1500


def _get_command_status(command_id: str, instance_id: str) -> dict[str, Any]:
    """Return the invocation's status fields, or a registering sentinel.

    InvocationDoesNotExist right after sendCommand is expected eventual
    consistency (the old ASL states carried a 10-attempt Retry for it);
    it is reported as still-registering rather than raised, and remains
    bounded by the caller's attempt budget.
    """
    try:
        inv = _ssm.get_command_invocation(
            CommandId=command_id, InstanceId=instance_id
        )
        return {
            "status": inv["Status"],
            "response_code": inv.get("ResponseCode", -1),
            "status_details": inv.get("StatusDetails", ""),
            "stderr_tail": (inv.get("StandardErrorContent") or "")[
                -_STDERR_TAIL_CHARS:
            ],
            "registered": True,
        }
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "InvocationDoesNotExist":
            return {
                "status": "Registering",
                "response_code": -1,
                "status_details": "InvocationDoesNotExist (registration window)",
                "stderr_tail": "",
                "registered": False,
            }
        raise


def _get_ping_status(instance_id: str) -> str:
    """The SSM agent's PingStatus — Online / ConnectionLost / Inactive.

    An empty InstanceInformationList (instance unknown to SSM) is
    reported as NotRegistered and counts as a ping miss: for a box that
    was reachable at sendCommand time, vanishing from SSM inventory is
    at least as alarming as ConnectionLost.
    """
    resp = _ssm.describe_instance_information(
        Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
    )
    info = resp.get("InstanceInformationList") or []
    if not info:
        return "NotRegistered"
    return info[0].get("PingStatus", "Unknown")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    instance_id = event["instance_id"]
    command_id = event["command_id"]
    attempts = int(event.get("attempts", 0)) + 1
    ping_misses = int(event.get("ping_misses", 0))
    max_attempts = int(event["max_attempts"])
    max_ping_misses = int(event.get("max_ping_misses", 3))
    step = event.get("step", "unknown")

    cmd = _get_command_status(command_id, instance_id)
    ping = _get_ping_status(instance_id)

    result: dict[str, Any] = {
        "attempts": attempts,
        "ping_misses": ping_misses,
        "status": cmd["status"],
        "response_code": cmd["response_code"],
        "status_details": cmd["status_details"],
        "stderr_tail": cmd["stderr_tail"],
        "ping_status": ping,
        "step": step,
        # Always present so the ASL ResultSelector's detail.$ path never
        # errors on a missing field (States.Runtime on absent paths).
        "detail": "",
    }

    if cmd["status"] == "Success":
        result["verdict"] = "SUCCESS"
    elif cmd["status"] in _FAILED_STATUSES:
        result["verdict"] = "COMMAND_FAILED"
        result["detail"] = (
            f"[{step}] SSM command {command_id} terminal status "
            f"{cmd['status']} (rc={cmd['response_code']}): "
            f"{cmd['status_details']}"
        )
    else:
        # Still running (or registering). Liveness + budget accounting.
        if ping != "Online":
            ping_misses += 1
        else:
            ping_misses = 0
        result["ping_misses"] = ping_misses

        if ping_misses >= max_ping_misses:
            result["verdict"] = "INSTANCE_UNRESPONSIVE"
            result["detail"] = (
                f"[{step}] instance {instance_id} PingStatus={ping} for "
                f"{ping_misses} consecutive polls while command "
                f"{command_id} is nominally {cmd['status']}. The box is "
                f"wedged (config#1807 shape) — the in-box executionTimeout "
                f"cannot fire. Recommended action: force-stop the instance."
            )
        elif attempts >= max_attempts:
            result["verdict"] = "POLL_BUDGET_EXHAUSTED"
            result["detail"] = (
                f"[{step}] command {command_id} did not reach a terminal "
                f"status within {max_attempts} poll iterations (last "
                f"status {cmd['status']}, PingStatus={ping}). Stuck "
                f"InProgress past the command's own executionTimeout "
                f"(#970 shape)."
            )
        else:
            result["verdict"] = "IN_PROGRESS"

    logger.info(json.dumps({k: v for k, v in result.items() if k != "stderr_tail"}))
    return result
