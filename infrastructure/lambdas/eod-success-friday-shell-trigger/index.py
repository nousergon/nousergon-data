"""alpha-engine-eod-success-friday-shell-trigger — kick the weekly shell-run.

Subscribes to EventBridge ``Step Functions Execution Status Change`` events,
filtered to ``ne-postclose-trading-pipeline`` + ``SUCCEEDED``. On every EOD-SF
success the handler computes the trading_day this execution closed against
and, if that trading_day is a Friday, starts the Saturday Step Function in
shell-run mode (``shell_run: true``) so the spot instances boot for real
while the workload paths short-circuit.

Replaces the prior fixed-time EventBridge cron (``alpha-engine-friday-shell-run``,
cron(45 20 ? * FRI *) = 13:45 PT Friday), which fired unconditionally and
raced the EOD SF's ``StopTradingInstance`` state when EOD ran long. The
event-driven design has three guarantees the cron lacked:

  1. **No fire on Friday-EOD failure.** If Friday's EOD never reaches
     SUCCEEDED, this Lambda never invokes, so the shell-run does not chase
     a broken upstream.
  2. **Late re-runs work for free.** If Friday's EOD is fixed and re-run
     later that day (or Saturday morning processing Friday's data), the
     handler still sees a SUCCEEDED transition and trading_day is still
     Friday — the shell-run starts at the fix-time, not on a stale clock.
  3. **trading_day-bound, not wall-clock.** trading_day is derived via
     ``nousergon_lib.trading_calendar.last_closed_trading_day`` from
     ``detail.stopDate`` (epoch ms UTC), so a Friday EOD re-run that
     succeeds at 02:00 UTC Saturday (= Friday evening PT) correctly stamps
     trading_day=Fri and fires. Pure ``datetime.now()`` would have read
     Saturday and skipped.

Fail-loud semantics (per the ``feedback_wire_orphaned_producer_must_fail_loud``
discipline — this Lambda is a new producer of shell-run starts):

  * Missing ``detail.stopDate`` → raises. SUCCEEDED events without a stop
    timestamp are an upstream contract violation, not a silent skip.
  * trading_calendar lookup failure → raises (lib bug surfaces fast).
  * boto3 ``states:StartExecution`` failure → raises (do not silently
    fail to launch the shell run; the EventBridge → Lambda retry policy
    will retry, and CW alarms on Lambda errors will page).

Non-Friday trading_day is the intended skip path and returns ``{"fired": False}``
with a structured log line; this is NOT a swallow.

**Still keyed on ne-postclose-trading-pipeline after the 2026-09-30 split**
(alpha-engine-config-I11269 follow-up), deliberately. The split moved
EODReconcile, StopTradingInstance and the weekly-exercise chain into
``ne-postclose-reconcile-pipeline``; this trigger reads none of their outputs —
the shell run short-circuits every workload — and the race it was built to
avoid (``StopTradingInstance``) is no longer in the machine it watches, so the
~16:20 ET post-close SUCCEEDED is a strictly earlier and still-correct signal
that Friday closed. Its rule is PAUSED (automation_pause.json, I9751) and is
created only by ``--bootstrap``, so re-pointing it would be a source change no
merge applies and a standing EventBridge drift finding; if the ruling ever
un-pauses it, re-point it then, with the bootstrap that applies it.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3

from nousergon_lib.trading_calendar import last_closed_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

SATURDAY_SF_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline"
)
EOD_SF_NAME = "ne-postclose-trading-pipeline"

SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN",
    f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts",
)

FRIDAY_WEEKDAY = 4  # date.weekday(): Mon=0, Fri=4


def _derive_trading_day_utc_ms(stop_date_ms: int):
    """trading_day = NYSE last-closed session at the EventBridge stopDate moment.

    Accepts epoch milliseconds (UTC) from ``event.detail.stopDate`` and hands
    a tz-aware UTC datetime to the lib helper. The helper itself converts to
    NYSE local time before walking back to the most recent closed session,
    so callers do not have to reason about UTC ↔ ET rollover.
    """
    dt_utc = datetime.fromtimestamp(int(stop_date_ms) / 1000, tz=timezone.utc)
    return last_closed_trading_day(dt_utc)


def _build_shell_run_input() -> str:
    # NO ec2_instance_id (config#2248, root-cause SPOF fix): this used to
    # hardcode the always-on dashboard box id (despite the misleading
    # TRADING_EC2_INSTANCE_ID name — it was never the trading box) into the
    # SAME field all 14 of the weekly SF's downstream sendCommand/Lambda
    # states key off. The SF's own CheckSpotDispatchNeeded/
    # DispatchWeeklyFreshnessSpot states (step_function.json, right after
    # AcquireMutex) now populate $.ec2_instance_id from a fresh ephemeral
    # spot on every execution this Lambda starts — omitting the field here
    # lets that dispatch path engage for the Friday shell-run exactly like
    # it does for the real Saturday cron.
    return json.dumps(
        {
            "sns_topic_arn": SNS_TOPIC_ARN,
            "shell_run": True,
            # pipeline_role="shell-run" (Option-D taxonomy): tags this
            # execution so the dashboard page-25 / Slack / CLI role filters
            # distinguish the Friday-PM dry-pass from the canonical weekly
            # run. The retired cron rule (alpha-engine-friday-shell-run,
            # ROADMAP L4055) carried this tag; restored here so the
            # event-driven path is a faithful replacement, not a surface
            # regression. shell_run=true stays the load-bearing input.
            "pipeline_role": "shell-run",
            # skip_parity (2026-08-14, Brian ruling — alpha-engine-config-I7309
            # tracks re-enabling in phase 3). The Saturday CADENCE trigger has
            # carried this since 2026-08-13 (infrastructure/cloudformation/
            # alpha-engine-orchestration.yaml). This rehearsal did NOT, and that
            # divergence was itself the defect: a rehearsal whose input differs
            # from production's rehearses something production never runs.
            #
            # Measured on execution friday-shell-2026-08-14-validate-i7386: the
            # run cleared every stage Saturday actually executes — MorningEnrich,
            # DataPhase1, Scanner, PredictorTraining, RegimeSubstrate,
            # SignalsEnvelope — then died with States.DataLimitExceeded inside
            # ParityParallel, a state the Saturday run skips entirely. So the
            # rehearsal reported a failure production could not have, and cost a
            # diagnosis cycle establishing that.
            #
            # The escape hatch the CFN comment describes is unchanged: an
            # operator StartExecution carrying {"skip_parity": false} still runs
            # the full parity branch (InitializeInput's JsonMerge puts user input
            # ABOVE the defaults), which remains the working loop for I7309.
            "skip_parity": True,
        }
    )


def _start_saturday_shell_run(execution_name: str) -> str:
    client = boto3.client("stepfunctions", region_name=REGION)
    resp = client.start_execution(
        stateMachineArn=SATURDAY_SF_ARN,
        name=execution_name,
        input=_build_shell_run_input(),
    )
    return resp["executionArn"]


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    detail = event.get("detail") or {}

    sm_arn = detail.get("stateMachineArn", "")
    sm_name = sm_arn.rsplit(":", 1)[-1]
    status = detail.get("status", "")
    if sm_name != EOD_SF_NAME or status != "SUCCEEDED":
        # EventBridge rule is filtered, but defend against accidental
        # invocations (manual test fires, rule drift). Not a fail-loud case
        # — this is a "wrong audience" log, not a contract violation.
        logger.info(
            "ignored event: sm_name=%s status=%s (expected %s/SUCCEEDED)",
            sm_name,
            status,
            EOD_SF_NAME,
        )
        return {"fired": False, "reason": "wrong_event"}

    stop_date_ms = detail.get("stopDate")
    if stop_date_ms is None:
        raise RuntimeError(
            "EOD SUCCEEDED event missing detail.stopDate — upstream contract violation"
        )

    trading_day = _derive_trading_day_utc_ms(stop_date_ms)
    if trading_day.weekday() != FRIDAY_WEEKDAY:
        logger.info(
            "EOD SUCCEEDED trading_day=%s (weekday=%d) — not Friday, no shell run",
            trading_day.isoformat(),
            trading_day.weekday(),
        )
        return {
            "fired": False,
            "reason": "not_friday",
            "trading_day": trading_day.isoformat(),
        }

    eod_name = detail.get("name", "unknown")
    sat_name = f"friday-shell-{trading_day.isoformat()}-{eod_name}"[:80]
    sat_arn = _start_saturday_shell_run(sat_name)
    logger.info(
        "Friday EOD SUCCEEDED (trading_day=%s) → started saturday shell run: %s",
        trading_day.isoformat(),
        sat_arn,
    )
    return {
        "fired": True,
        "trading_day": trading_day.isoformat(),
        "saturday_execution_arn": sat_arn,
        "saturday_execution_name": sat_name,
    }
