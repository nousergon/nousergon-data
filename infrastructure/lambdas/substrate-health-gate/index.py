"""alpha-engine-substrate-health-gate — fast pre-dispatch substrate check.

nousergon/alpha-engine-config#2249: the weekly SF's ``MorningEnrich`` is the
first Task state to actually dispatch work (via ``ssm:sendCommand``) to the
Saturday dispatch box. When that box is unhealthy — disk 100% full, or the
SSM agent wedged/unresponsive so a command never registers (SSM's
``InvocationDoesNotExist`` never resolves to a real invocation) —
``MorningEnrich`` used to burn its full "gold 4+2" retry ladder (config#2279,
~4 attempts x up to 300s backoff, worst case ~15 minutes) before finally
failing into the generic ``NormalizeFailureContext`` -> ``HandleFailure`` ->
``PipelineFailure`` path, with no named signal distinguishing "the box is
dead" from an ordinary transient SendCommand API error.

This Lambda runs as a NEW Task state immediately BEFORE ``MorningEnrich``
(this is a fast PRE-check, not a replacement for MorningEnrich's own retry
ladder — that ladder still exists for real transient issues once dispatch is
confirmed healthy). One invocation = one full gate: it issues its OWN tiny
SSM ``sendCommand`` (a `df` disk-headroom check, ``executionTimeout`` ~20s)
and polls it to a terminal status INSIDE this single Lambda invocation
(bounded by ``_POLL_BUDGET_SECONDS``, well under the Lambda's own configured
timeout), so the Step Function needs only one Task state for the whole gate
— no separate SF-level poll loop, unlike ssm-liveness-poller (that Lambda
polls a *pipeline* command already in flight; this one owns its own
short-lived probe command end to end).

Verdicts (the Step Function's Choice state branches on exactly these):
  HEALTHY           — the df check ran to Success on the box with headroom
                      under DISK_WARN_PERCENT, and the SSM round-trip proved
                      the agent is live and registering commands normally.
  SUBSTRATE_UNHEALTHY — one of three distinct reasons (``reason`` field):
    disk_full         — the df probe ran (agent is alive) but reported used%
                        at/above DISK_WARN_PERCENT.
    ssm_unresponsive  — the probe command was delivered/registered but never
                        reached a terminal status within the poll budget, OR
                        it reached a terminal non-Success status without
                        producing df output (the agent picked it up but the
                        box is thrashing/wedged).
    ssm_command_never_registered — SSM never confirms the invocation exists
                        for the whole poll budget (``InvocationDoesNotExist``
                        never resolves) — the agent itself is unreachable,
                        the fastest-detectable "box is gone" shape.

Fail-loud: an unexpected AWS error (e.g. AccessDenied, a malformed response)
RAISES rather than being swallowed into a verdict — the SF's own Catch routes
that to NormalizeFailureContext like any other infra failure. Only the three
named SUBSTRATE_UNHEALTHY reasons above are recognized "this box is bad"
outcomes; everything else is an ordinary Lambda/task failure.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

_ssm = boto3.client("ssm", region_name=REGION)

# Disk-usage warn threshold (percent used on the root volume). Chosen to fire
# before the box is fully wedged by disk pressure (the 2026-07-11 incident
# this issue cites was already at 100% by the time it was noticed manually).
DISK_WARN_PERCENT = 90

# The probe command's own in-box execution budget — a `df` invocation with a
# tiny bootstrap is a sub-second operation; 20s gives generous headroom
# without meaningfully eating into the "<2 min fail-fast" target.
_PROBE_EXECUTION_TIMEOUT_SECONDS = 20
# SSM delivery timeout — time for the agent to PICK UP the command, not to
# run it. Short: a healthy agent registers a command in low single-digit
# seconds; this is the primary signal for "SSM unresponsive."
_PROBE_DELIVERY_TIMEOUT_SECONDS = 15

# Total wall-clock budget (seconds) this Lambda spends polling its own probe
# command to a terminal status before giving up as ssm_unresponsive. Kept
# well under the Lambda's configured timeout (see deploy.sh) and under the
# "<2 min fail-fast" target this issue asks for.
_POLL_BUDGET_SECONDS = 45
_POLL_INTERVAL_SECONDS = 3

# `df -P / | tail -1` prints e.g. "/dev/xvda1 20961280 18642176 1264104 94% /"
# — field 5 (1-indexed) is the used-percent, trailing "%" stripped below.
_DF_LINE_RE = re.compile(r"(\d+)%\s+\S+\s*$")


def _send_probe(instance_id: str) -> str:
    resp = _ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment="substrate-health-gate disk probe — config#2249",
        Parameters={
            "commands": ["df -P / | tail -1"],
            "executionTimeout": [str(_PROBE_EXECUTION_TIMEOUT_SECONDS)],
        },
        TimeoutSeconds=_PROBE_DELIVERY_TIMEOUT_SECONDS,
    )
    return resp["Command"]["CommandId"]


def _poll_to_terminal(command_id: str, instance_id: str) -> dict[str, Any]:
    """Poll ``getCommandInvocation`` to a terminal status inside THIS Lambda
    invocation (bounded by _POLL_BUDGET_SECONDS). Returns a dict describing
    the outcome; never raises for the expected "still registering" /
    "still running" transients — those are folded into the returned dict so
    the caller can classify a poll-budget exhaustion as ssm_unresponsive.
    """
    deadline = time.monotonic() + _POLL_BUDGET_SECONDS
    ever_registered = False

    while time.monotonic() < deadline:
        try:
            inv = _ssm.get_command_invocation(
                CommandId=command_id, InstanceId=instance_id
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in (
                "InvocationDoesNotExist",
                "InvocationDoesNotExistException",
            ):
                time.sleep(_POLL_INTERVAL_SECONDS)
                continue
            raise

        ever_registered = True
        status = inv["Status"]
        if status in ("Pending", "InProgress", "Delayed"):
            time.sleep(_POLL_INTERVAL_SECONDS)
            continue

        return {
            "terminal": True,
            "ever_registered": ever_registered,
            "status": status,
            "response_code": inv.get("ResponseCode", -1),
            "status_details": inv.get("StatusDetails", ""),
            "stdout": inv.get("StandardOutputContent", ""),
        }

    return {
        "terminal": False,
        "ever_registered": ever_registered,
        "status": "PollBudgetExhausted",
        "response_code": -1,
        "status_details": "",
        "stdout": "",
    }


def _parse_disk_used_percent(stdout: str) -> int | None:
    match = _DF_LINE_RE.search(stdout.strip())
    if not match:
        return None
    return int(match.group(1))


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ARG001
    instance_id = event["instance_id"]

    command_id = _send_probe(instance_id)
    outcome = _poll_to_terminal(command_id, instance_id)

    result: dict[str, Any] = {
        "command_id": command_id,
        "instance_id": instance_id,
        "status": outcome["status"],
    }

    if not outcome["ever_registered"]:
        result["verdict"] = "SUBSTRATE_UNHEALTHY"
        result["reason"] = "ssm_command_never_registered"
        result["message"] = (
            f"SubstrateUnhealthy: SSM command never registered — the "
            f"disk-probe command {command_id} never reached a real "
            f"invocation on {instance_id} within {_POLL_BUDGET_SECONDS}s "
            f"(SSM kept returning InvocationDoesNotExist). The SSM agent is "
            f"unreachable — the box is likely down, out of capacity, or the "
            f"agent process is dead."
        )
        logger.warning(result["message"])
        return result

    if outcome["status"] != "Success":
        result["verdict"] = "SUBSTRATE_UNHEALTHY"
        result["reason"] = "ssm_unresponsive"
        result["message"] = (
            f"SubstrateUnhealthy: SSM unresponsive — the disk-probe command "
            f"{command_id} registered on {instance_id} but reached terminal "
            f"status {outcome['status']!r} (rc={outcome['response_code']}, "
            f"StatusDetails={outcome['status_details']!r}) instead of "
            f"Success within {_POLL_BUDGET_SECONDS}s. The agent registered "
            f"the command but the box did not complete it — likely wedged "
            f"or under severe resource pressure."
        )
        logger.warning(result["message"])
        return result

    used_percent = _parse_disk_used_percent(outcome["stdout"])
    result["disk_used_percent"] = used_percent

    if used_percent is None:
        # The command reported Success but its output didn't parse as a df
        # line — treat as unresponsive-class (something is off about the
        # box's shell environment) rather than silently declaring healthy.
        result["verdict"] = "SUBSTRATE_UNHEALTHY"
        result["reason"] = "ssm_unresponsive"
        result["message"] = (
            f"SubstrateUnhealthy: SSM unresponsive — disk-probe command "
            f"{command_id} on {instance_id} reported Success but produced "
            f"unparseable output ({outcome['stdout']!r}); treating as a "
            f"substrate signal failure rather than assuming healthy."
        )
        logger.warning(result["message"])
        return result

    if used_percent >= DISK_WARN_PERCENT:
        result["verdict"] = "SUBSTRATE_UNHEALTHY"
        result["reason"] = "disk_full"
        result["message"] = (
            f"SubstrateUnhealthy: disk {used_percent}% full on {instance_id} "
            f"(warn threshold {DISK_WARN_PERCENT}%) — dispatching "
            f"MorningEnrich onto this box would very likely fail or corrupt "
            f"partial output."
        )
        logger.warning(result["message"])
        return result

    result["verdict"] = "HEALTHY"
    result["message"] = (
        f"Substrate healthy: {instance_id} disk {used_percent}% used, SSM "
        f"agent responsive."
    )
    logger.info(result["message"])
    return result
