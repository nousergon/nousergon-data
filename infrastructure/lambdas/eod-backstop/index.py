"""alpha-engine-eod-backstop — same-day EOD-pipeline trigger of last resort.

The EOD Step Function (``ne-postclose-trading-pipeline``) is normally started by
the trading daemon's shutdown hook (``daemon.py`` finally block). That is the
SOLE trigger — a deliberate "no-backstop design". If the daemon dies before
its shutdown hook, the SSM ``RunDaemon`` step never reaches the finally block,
or the daemon never starts, the EOD SF never fires: no PostMarketData, no
CaptureSnapshot, and — the load-bearing failure — NO ``eod_pnl`` ROW for the
day. The next day's EOD reconcile then has no adjacent prior-day NAV baseline
and the headline daily return/alpha span multiple sessions (the 2026-06-24
gap → RGEN +14.92% class of bug; config#1229).

This Lambda is the missing backstop. Triggered by EventBridge ~22:30 UTC on
weekdays (well after the daemon's nominal ~20:15 UTC EOD), it starts the EOD
SF IFF both:

  1. the trading EC2 box is still RUNNING — the daemon never shut it down, so
     EOD never fired; and CaptureSnapshot needs a live IB session, which only
     exists while the box is up (this is a SAME-DAY-only recovery), AND
  2. no EOD execution has STARTED today — so we never double-run after a
     daemon-triggered EOD that already completed (or is mid-flight).

If the box is already stopped, EOD either ran (success or failure — both end
in stopping the box) or the box never booted (no trading → nothing to
reconcile): either way a no-op. The late-discovery case (box long gone, gap
found days later) is NOT this Lambda's job — that is the IBKR Flex Query
``eod_pnl`` backfill (config#1229).

The EOD SF's own DynamoDB mutex (``AcquireMutex``) is the concurrency
backstop: if a daemon-triggered EOD is mid-flight when this fires, our
StartExecution would only hit ``MutexConflict`` and fail cleanly — but the
``eod_ran_today`` guard means we don't even attempt it.

Fail-loud (``feedback_no_silent_fails``): any AWS call failure raises so the
EventBridge retry + Lambda-error CloudWatch alarm page the operator. We must
never silently skip the check on the one day it matters.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

from nousergon_lib.trading_calendar import is_trading_day, last_closed_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

EOD_SF_ARN = os.environ.get(
    "EOD_SF_ARN",
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline",
)
# The trading box (CaptureSnapshot / EODReconcile / StopTradingInstance target)
# and the dashboard box. ec2_instance_id (dashboard box) no longer targets an
# SSM InstanceIds param directly since DailySubstrateHealthCheck was spun out
# to a standalone dashboard-box systemd timer (alpha-engine-config-I2722,
# 2026-07-16) — it is still carried through the SF's top-level input because
# HealDispatchReplay passes it verbatim into its own replay execution's Input
# (schema fidelity for the closed self-heal loop, config-I2702). Mirror the
# daemon's _trigger_eod_pipeline input shape so the SF runs identically to a
# normal EOD.
TRADING_INSTANCE_ID = os.environ.get("TRADING_INSTANCE_ID", "i-018eb3307a21329bf")
DASHBOARD_INSTANCE_ID = os.environ.get("DASHBOARD_INSTANCE_ID", "i-09b539c844515d549")
SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN", f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"
)

# Count an EOD as "already fired today" regardless of terminal status — a
# started-then-failed EOD still ran HandleFailure → ForceStopInstance, so the
# box would be stopped and the box-running gate already covers it; this guard
# additionally prevents racing a mid-flight (RUNNING) EOD.
_STARTED_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")


def _trading_box_running(ec2_client: Optional[object] = None) -> bool:
    """True iff the trading EC2 instance is in the ``running`` state.

    A stopped/terminated/absent box means EOD already ran (and stopped it) or
    the box never booted — either way the backstop is a no-op. Raises on an
    EC2 API failure (fail-loud)."""
    if ec2_client is None:  # pragma: no cover — production path
        ec2_client = boto3.client("ec2", region_name=REGION)
    resp = ec2_client.describe_instances(InstanceIds=[TRADING_INSTANCE_ID])
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            state = (inst.get("State") or {}).get("Name")
            logger.info("Trading box %s state=%s", TRADING_INSTANCE_ID, state)
            return state == "running"
    logger.info("Trading box %s not found in describe_instances", TRADING_INSTANCE_ID)
    return False


def _eod_ran_today(now_utc: datetime, sf_client: Optional[object] = None) -> bool:
    """True iff at least one EOD SF execution STARTED since 00:00 UTC today.

    At the ~22:30 UTC firing time, today's expected EOD (~20:00–21:30 UTC) is
    within the since-midnight window, while the prior trading day's EOD is not
    — so this is trading-day-correct without a per-day marker. Raises on a
    ListExecutions failure (fail-loud)."""
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    for status_filter in _STARTED_STATUSES:
        next_token: Optional[str] = None
        while True:
            kwargs = {
                "stateMachineArn": EOD_SF_ARN,
                "statusFilter": status_filter,
                "maxResults": 100,
            }
            if next_token:
                kwargs["nextToken"] = next_token
            resp = sf_client.list_executions(**kwargs)
            for row in resp.get("executions") or []:
                start = row.get("startDate")
                if not hasattr(start, "astimezone"):
                    continue
                start_utc = (
                    start.astimezone(timezone.utc)
                    if start.tzinfo
                    else start.replace(tzinfo=timezone.utc)
                )
                if start_utc >= midnight:
                    logger.info(
                        "EOD execution %s already started today (%s, %s)",
                        row.get("name"), start_utc.isoformat(), status_filter,
                    )
                    return True
            next_token = resp.get("nextToken")
            if not next_token:
                break
    return False


def _start_eod(trading_day: str, sf_client: Optional[object] = None) -> str:
    """Start the EOD SF with the same input shape as the daemon, tagged
    ``triggered_by=backstop``. Returns the execution ARN."""
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    resp = sf_client.start_execution(
        stateMachineArn=EOD_SF_ARN,
        name=f"eod-backstop-{trading_day}-{int(time.time())}",
        input=json.dumps(
            {
                "trading_instance_id": [TRADING_INSTANCE_ID],
                "ec2_instance_id": [DASHBOARD_INSTANCE_ID],
                "sns_topic_arn": SNS_TOPIC_ARN,
                "run_date": trading_day,
                "triggered_by": "backstop",
                "pipeline_role": "eod",
            }
        ),
    )
    arn = resp.get("executionArn", "")
    logger.warning(
        "EOD-BACKSTOP fired: trading box was still running and no EOD ran today "
        "for trading_day=%s — started EOD SF %s",
        trading_day, arn,
    )
    return arn


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    now_utc = datetime.now(timezone.utc)

    # Only trading days have an expected EOD. The EventBridge rule is MON-FRI,
    # so this skips NYSE holidays that fall on weekdays.
    if not is_trading_day(now_utc.date()):
        logger.info("Not a NYSE trading day (%s) — no EOD expected; no-op.", now_utc.date())
        return {"action": "noop", "reason": "not_a_trading_day", "date": str(now_utc.date())}

    trading_day = last_closed_trading_day(now_utc).isoformat()

    if not _trading_box_running():
        logger.info(
            "Trading box not running — EOD already ran or box never booted; no-op."
        )
        return {"action": "noop", "reason": "trading_box_not_running", "trading_day": trading_day}

    if _eod_ran_today(now_utc):
        logger.info("An EOD execution already started today — no-op.")
        return {"action": "noop", "reason": "eod_already_ran_today", "trading_day": trading_day}

    execution_arn = _start_eod(trading_day)
    return {"action": "started_eod", "trading_day": trading_day, "execution_arn": execution_arn}
