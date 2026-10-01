"""alpha-engine-eod-backstop — starts the post-close pipelines when nothing else did.

SPLIT 2026-09-30 (alpha-engine-config-I11269 follow-up; Brian: "run all steps
in the post close sf that can run immediately after close and just set up the
part that relies on the collector as a separate sf"). The post-close work is now
TWO state machines, and this Lambda owns three entry points, selected by the
event it receives:

  * ``ne-postclose-trading-pipeline`` (``step_function_eod.json``) — gate,
    mutex, deploy-drift check, box start, executor refresh, CaptureSnapshot.
    Needs only the close. Still started by the daemon's shutdown hook at
    ~16:00 ET; the 22:30 UTC scheduled firing below (the original backstop)
    covers the daemon never firing it. Its dispatch predicate is now the
    SNAPSHOT (``trades/snapshots/{day}.json``), because the eod_pnl row is no
    longer this machine's output — keying it on the row would re-dispatch it
    every day before the reconcile half had run.
  * ``ne-postclose-reconcile-pipeline`` (``step_function_eod_reconcile.json``)
    — everything that depends on the standalone EOD collection
    (``ne-data-collection-eod``, 18:15 ET). Started HERE, event-driven: the
    ``alpha-engine-eod-reconcile-trigger`` rule forwards that collection's
    terminal Step Functions status change (SUCCEEDED / FAILED / TIMED_OUT;
    heal executions ``v1-eod-heal-*`` excluded in the pattern AND in code) and
    ``_handle_collection_terminal`` starts one reconcile per collection
    execution (the execution name is derived from the collection's ARN, so an
    EventBridge redelivery is an ``ExecutionAlreadyExists`` no-op). A FAILED or
    TIMED_OUT collection still starts it: the reconcile's precondition probe
    and self-heal loop are what act on a collection that did not land.
  * the RECONCILE BACKSTOP — ``alpha-engine-eod-reconcile-backstop-daily``
    (02:15 UTC TUE-SAT = 22:15 EDT / 21:15 EST, after the collection's cron plus
    the declared caps of every workload it runs) sends ``{"mode":
    "reconcile-backstop"}``. If the day's eod_pnl row is still missing, no
    reconcile and no EOD collection is RUNNING, and this backstop has not
    already fired for the day, it starts the reconcile pipeline. That covers
    the collection's trigger never firing (the reconcile's own heal loop then
    starts the collection) and this Lambda's event path failing. A second miss
    is a page, not another boot: ``alpha-engine-eod-snapshot-existence-check``
    (23:30 ET) pages on the missing eod_pnl row independently, and every raise
    here trips the Lambda-error alarm.

The original post-close backstop's history follows, unchanged in substance.

alpha-engine-eod-backstop — same-day EOD-pipeline trigger of last resort.

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
SF IFF:

  1. it is a NYSE trading day (an expected-EOD day at all), AND
  2. no EOD execution has STARTED today — so we never double-run after a
     daemon-triggered EOD that already completed (or is mid-flight).

WIDENED 2026-08-09 (alpha-engine-config-I6690): the original design also
required the trading box to still be RUNNING before dispatching, on the
theory that a stopped box meant "EOD already ran" or "box never booted, so
nothing to reconcile". That second premise was wrong: on 2026-08-05/06 the
PREOPEN pipeline itself failed pre-boot (config-I6615), the trading box never
started AT ALL, the daemon never ran, and this backstop's box-running gate
made it a silent no-op too — no EOD, no ``eod_pnl`` row, and (with
``alpha-engine-pipeline-watchdog-daily`` paused under I6617) no alert of any
kind. A box-never-started day is exactly the case this backstop most needs to
cover, so the box-running condition is dropped entirely: the EOD SF's own
first real state, ``StartTradingInstance`` (``step_function_eod.json:96``),
boots the box unconditionally and is idempotent (``ec2:startInstances`` on an
already-running box is a no-op) — so this Lambda needs no boot logic of its
own, whether the box was up, down, or never started. ``_trading_box_running``
is retained purely to TAG the dispatch (``triggered_by``) for observability,
never to gate it.

If the box is already stopped, EOD either ran (success or failure — both end
in stopping the box, and a completed EOD leaves its eod_pnl row, caught by
the ``_eod_did_its_job`` guard) or the box never
booted (caught by the widened dispatch above). The late-discovery case (box
long gone, gap found days later) is NOT this Lambda's job — that is the IBKR
Flex Query ``eod_pnl`` backfill (config#1229).

The EOD SF's own DynamoDB mutex (``AcquireMutex``) is the concurrency
backstop: if a daemon-triggered EOD is mid-flight when this fires, our
StartExecution would only hit ``MutexConflict`` and fail cleanly — but the
``_eod_did_its_job`` guard means we don't even attempt it.

CaptureSnapshot on a freshly-booted box (config-I6690 evidence, verified
against crucible-executor + step_function_eod.json, not assumed): IB Gateway
comes up via the ``ibgateway.service`` systemd unit, which is a hard
``Requires=``/``After=`` dependency of both ``alpha-engine-daemon.service``
and ``alpha-engine-morning.service`` (both ``WantedBy=multi-user.target``,
i.e. boot-enabled) — so any boot of the trading box, cold or warm, pulls
ibgateway up as a systemd dependency with no separate enablement needed.
Between ``StartTradingInstance`` and ``CaptureSnapshot`` the EOD SF spends
several minutes on ``WaitForInstanceReady``/the SSM-readiness poll (up to
~3 min), the conditional executor-checkout refresh, and — load-bearing —
``LaunchPostMarketDataSpot``'s post-market-data phase on a wholly separate
ephemeral spot box (``TimeoutSeconds: 420`` on the launch alone, plus its own
poll-to-completion), none of which touch or wait on the trading box's IB
session. That multi-minute buffer comfortably exceeds
``wait-for-ibgateway.sh``'s own 120s max-wait budget and the ~30s TOTP-auth
window ``alpha-engine-morning.service`` budgets for. ``executor/
snapshot_capturer.py``'s ``IBKRClient.connect`` additionally retries 3x with
exponential backoff (``executor/retry.py``) on its own, an extra ~60-90s of
tolerance. No explicit ``wait-for-ibgateway`` state exists inside
``CaptureSnapshot``'s own SSM command, so this is evidence of comfortable
timing margin, not a guarantee — if a cold-boot CaptureSnapshot failure is
ever observed in practice, the fix is to add an explicit gateway-readiness
poll to ``CaptureSnapshot``'s SSM command, not to skip the snapshot (the
I2700 skip shape assumes a snapshot ALREADY exists from an earlier attempt
in the same execution — that is never true on a box that never booted, so
skipping here would just hand ``EODReconcile`` no snapshot to read and it
would hard-fail with no fallback, by design).

Fail-loud (``feedback_no_silent_fails``): any AWS call failure raises so the
EventBridge retry + Lambda-error CloudWatch alarm page the operator. We must
never silently skip the check on the one day it matters.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from datetime import time as dtime
from typing import Optional
from zoneinfo import ZoneInfo

import boto3

from nousergon_lib.trading_calendar import is_trading_day, last_closed_trading_day

from eod_artifact_verification import verify_eod_artifacts

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
# The collector-dependent half of the post-close pipeline (split 2026-09-30).
RECONCILE_SF_ARN = os.environ.get(
    "RECONCILE_SF_ARN",
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-postclose-reconcile-pipeline",
)
# The standalone EOD collection whose terminal event starts the reconcile.
COLLECTION_SF_ARN = os.environ.get(
    "COLLECTION_SF_ARN",
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-data-collection-eod",
)
#: The reconcile pipeline's own heal loop starts the collection under this
#: prefix (step_function_eod_reconcile.json::HealStartCollection). Its terminal
#: must never start a second reconcile underneath the one that launched it.
HEAL_COLLECTION_PREFIX = "v1-eod-heal-"
#: Collection terminal status -> the reconcile's ``triggered_by``. ABORTED is
#: deliberately absent: an operator stopped the collection on purpose, and the
#: reconcile backstop still covers the day if that left the row missing.
COLLECTION_TRIGGERED_BY = {
    "SUCCEEDED": "collection-succeeded",
    "FAILED": "collection-failed",
    "TIMED_OUT": "collection-timed-out",
}
EVENT_EXECUTION_PREFIX = "eod-reconcile-"
RECONCILE_BACKSTOP_PREFIX = "eod-reconcile-backstop-"
#: The live-IB snapshot the post-close pipeline's CaptureSnapshot writes. Its
#: presence is that pipeline's "did its job" predicate since the split.
SNAPSHOT_BUCKET = "alpha-engine-research"
SNAPSHOT_KEY_TEMPLATE = "trades/snapshots/{run_date}.json"
ET = ZoneInfo("America/New_York")
#: NYSE close. A collection that STARTED before it is not this evening's.
MARKET_CLOSE_ET = dtime(16, 0)

TRADING_INSTANCE_ID = os.environ.get("TRADING_INSTANCE_ID", "i-018eb3307a21329bf")
DASHBOARD_INSTANCE_ID = os.environ.get("DASHBOARD_INSTANCE_ID", "i-09b539c844515d549")
SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN", f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"
)

# Count an EOD as "already fired today" regardless of terminal status — a
# started-then-failed EOD still ran HandleFailure → ForceStopInstance (box
# stopped again either way, config-I6690: no longer load-bearing here since
# dispatch isn't gated on box state); this guard's job is preventing a
# double-start, including racing a mid-flight (RUNNING) EOD.
_STARTED_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")


def _eod_did_its_job(trading_day: str, s3_client: Optional[object] = None) -> bool:
    """True iff ``trading_day``'s EOD produced its load-bearing ARTIFACT.

    alpha-engine-config-I7582. This replaced ``_eod_ran_today`` as the dispatch
    predicate. That one asked "did an execution START today", which covers *the
    EOD never fired* and not *the EOD fired and did not do its job* — and this
    module's whole reason for existing, in its own words above, is "the
    load-bearing failure — NO ``eod_pnl`` ROW for the day".

    Measured 2026-08-17: the EOD SF started at 20:00 UTC and ended at 21:55 UTC
    in ``DegradedRun`` (``reason: eod_reconcile_skipped_data_gap``) with no
    ArcticDB append, no ``EODReconcile`` and no ``eod_pnl`` row. This Lambda's
    22:30 UTC firing was a no-op, CORRECTLY per its own predicate, on exactly
    the day it exists for.

    Keyed on the ARTIFACT, not on the terminal's colour, and deliberately so: a
    ``DegradedRun`` whose degradation is unrelated to the row
    (``mutex_acquire_degraded``, ``weekly_exercise_launch_failed``) has already
    produced it, and re-dispatching would cost a second live-IB snapshot
    capture — the exact thing ``skip_capture_snapshot`` exists to avoid.

    The check itself is ``eod_artifact_verification``, the same module
    sf-telegram-notifier uses to decide whether a terminal message may read
    clean. Two consumers, one definition of "did the EOD do its job": a
    backstop that stands down on a day the notifier would call incomplete is
    the gap this closes, reopened.

    ``verify_eod_artifacts`` fails TOWARD absent on any non-404 S3 fault, which
    is the correct direction here too — an unverifiable day dispatches a
    backstop run rather than silently skipping one.

    Since the 2026-09-30 split this is the RECONCILE backstop's predicate
    (``_handle_reconcile_backstop``): the row is written by
    ``ne-postclose-reconcile-pipeline``'s EODReconcile, and re-running that
    machine never re-touches live IB. The post-close pipeline's own backstop
    keys on the snapshot instead (``_snapshot_present``).
    """
    if s3_client is None:  # pragma: no cover — production path
        s3_client = boto3.client("s3", region_name=REGION)
    status = verify_eod_artifacts(s3_client, trading_day)
    if status is None:
        logger.warning(
            "EOD artifact verification returned no status for %s — treating as "
            "NOT done so the backstop dispatches rather than silently skipping.",
            trading_day,
        )
        return False
    logger.info(
        "EOD artifacts for %s: completion_marker=%s pnl_row=%s",
        trading_day, status.completion_marker_present, status.pnl_row_present,
    )
    return status.pnl_row_present


def _eod_running(sf_client: Optional[object] = None) -> bool:
    """True iff an EOD execution is RUNNING right now.

    A degraded run still inside its self-heal loop must not be re-dispatched
    underneath itself. The SF's own DynamoDB ``AcquireMutex`` is the hard
    concurrency backstop; this is the cheap check that avoids relying on it.
    """
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    resp = sf_client.list_executions(
        stateMachineArn=EOD_SF_ARN, statusFilter="RUNNING", maxResults=1
    )
    running = bool(resp.get("executions"))
    if running:
        logger.info("An EOD execution is currently RUNNING — no-op.")
    return running


def _backstop_already_fired_today(
    now_utc: datetime, sf_client: Optional[object] = None
) -> bool:
    """True iff THIS Lambda already dispatched an EOD today.

    One retry per day. Without this the outcome predicate would re-dispatch on
    every firing for as long as the row stayed missing, which on a genuinely
    broken day is an unbounded loop of trading-box boots. A second miss is a
    page, not another attempt.
    """
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
                if not str(row.get("name") or "").startswith("eod-backstop-"):
                    continue
                start = row.get("startDate")
                if not hasattr(start, "astimezone"):
                    continue
                start_utc = (
                    start.astimezone(timezone.utc)
                    if start.tzinfo
                    else start.replace(tzinfo=timezone.utc)
                )
                if start_utc >= midnight:
                    return True
            next_token = resp.get("nextToken")
            if not next_token:
                break
    return False


def _trading_box_running(ec2_client: Optional[object] = None) -> bool:
    """True iff the trading EC2 instance is in the ``running`` state.

    OBSERVABILITY-ONLY (config-I6690): no longer gates dispatch — a stopped
    box is exactly the box-never-started case this backstop must still cover.
    Used solely to tag ``_start_eod``'s ``triggered_by`` value so a dashboard
    reader can tell "daemon was up but didn't fire EOD" apart from "box was
    never running at all" without re-deriving it from EC2 state. Raises on an
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


# _eod_ran_today was REMOVED (alpha-engine-config-I7582). It answered "did an
# EOD execution start since 00:00 UTC", which is not the question this Lambda
# exists to ask, and on 2026-08-17 it correctly returned True for an execution
# that produced no eod_pnl row — so the backstop stood down on exactly the day
# it was written for. Replaced by _eod_did_its_job (the artifact) + _eod_running
# (concurrency) + _backstop_already_fired_today (one retry per day). Deleted
# rather than left dormant: champion-challenger-policy.md §6.
def _start_eod(trading_day: str, triggered_by: str, sf_client: Optional[object] = None) -> str:
    """Start the EOD SF with the same input shape the daemon uses (config-I6690:
    identical regardless of whether the box was running — see the module
    docstring's CaptureSnapshot-on-a-cold-box evidence), tagged with the given
    ``triggered_by``. Returns the execution ARN."""
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
                "triggered_by": triggered_by,
                "pipeline_role": "eod",
            }
        ),
    )
    arn = resp.get("executionArn", "")
    logger.warning(
        "EOD-BACKSTOP fired (triggered_by=%s): no post-close snapshot for "
        "trading_day=%s — started post-close SF %s",
        triggered_by, trading_day, arn,
    )
    return arn


def _snapshot_present(trading_day: str, s3_client: Optional[object] = None) -> bool:
    """True iff the post-close pipeline's CaptureSnapshot wrote ``trading_day``'s
    snapshot (``trades/snapshots/{day}.json``).

    The post-close dispatch predicate since the 2026-09-30 split. The eod_pnl
    row it used to key on (alpha-engine-config-I7582) is the RECONCILE
    pipeline's output now, written hours after this machine finishes; keying
    the 22:30 UTC firing on it would re-dispatch the post-close pipeline — and
    a second live-IB capture — on every normal day.

    A clean 404 is "absent". Any other S3 error RAISES (``feedback_no_silent_fails``):
    this predicate gates a live-IB capture, so "could not check" must page via
    the Lambda-error alarm rather than silently dispatch or silently skip.
    """
    if s3_client is None:  # pragma: no cover — production path
        s3_client = boto3.client("s3", region_name=REGION)
    key = SNAPSHOT_KEY_TEMPLATE.format(run_date=trading_day)
    try:
        s3_client.head_object(Bucket=SNAPSHOT_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001 — classify, then re-raise non-404
        response = getattr(exc, "response", {}) or {}
        code = str(response.get("Error", {}).get("Code", ""))
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            logger.info("Snapshot s3://%s/%s absent.", SNAPSHOT_BUCKET, key)
            return False
        raise
    logger.info("Snapshot s3://%s/%s present.", SNAPSHOT_BUCKET, key)
    return True


def _running(state_machine_arn: str, sf_client: Optional[object] = None) -> bool:
    """True iff ``state_machine_arn`` has an execution RUNNING right now."""
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    resp = sf_client.list_executions(
        stateMachineArn=state_machine_arn, statusFilter="RUNNING", maxResults=1
    )
    running = bool(resp.get("executions"))
    if running:
        logger.info("%s has a RUNNING execution.", state_machine_arn.rsplit(":", 1)[-1])
    return running


def _reconcile_running(sf_client: Optional[object] = None) -> bool:
    """A reconcile still inside its readiness wait or self-heal loop must not
    have a second one started underneath it. Its own DynamoDB ``AcquireMutex``
    only rejects a duplicate started in the SAME UTC minute."""
    return _running(RECONCILE_SF_ARN, sf_client)


def _collection_running(sf_client: Optional[object] = None) -> bool:
    """The EOD collection is still working; its terminal event will start the
    reconcile, so the scheduled backstop must stand down rather than race it."""
    return _running(COLLECTION_SF_ARN, sf_client)


def _reconcile_backstop_already_fired(
    trading_day: str, sf_client: Optional[object] = None
) -> bool:
    """True iff THIS Lambda's scheduled reconcile backstop already started a
    reconcile for ``trading_day``. Keyed on the execution NAME, which carries
    the trading day, so a firing after 00:00 UTC still recognises the same
    evening's earlier attempt. One retry per day: a second miss is a page."""
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    prefix = f"{RECONCILE_BACKSTOP_PREFIX}{trading_day}-"
    for status_filter in _STARTED_STATUSES:
        next_token: Optional[str] = None
        while True:
            kwargs = {
                "stateMachineArn": RECONCILE_SF_ARN,
                "statusFilter": status_filter,
                "maxResults": 100,
            }
            if next_token:
                kwargs["nextToken"] = next_token
            resp = sf_client.list_executions(**kwargs)
            for row in resp.get("executions") or []:
                if str(row.get("name") or "").startswith(prefix):
                    return True
            next_token = resp.get("nextToken")
            if not next_token:
                break
    return False


def _start_reconcile(
    trading_day: str,
    triggered_by: str,
    execution_name: str,
    collection_execution_arn: Optional[str] = None,
    sf_client: Optional[object] = None,
) -> str:
    """Start ``ne-postclose-reconcile-pipeline`` with the same six-field input
    the post-close pipeline takes (infrastructure/sf_entry_contract.json).
    Returns the execution ARN."""
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    payload = {
        "trading_instance_id": [TRADING_INSTANCE_ID],
        "ec2_instance_id": [DASHBOARD_INSTANCE_ID],
        "sns_topic_arn": SNS_TOPIC_ARN,
        "run_date": trading_day,
        "triggered_by": triggered_by,
        "pipeline_role": "eod",
    }
    if collection_execution_arn:
        payload["collection_execution_arn"] = collection_execution_arn
    resp = sf_client.start_execution(
        stateMachineArn=RECONCILE_SF_ARN,
        name=execution_name,
        input=json.dumps(payload),
    )
    arn = resp.get("executionArn", "")
    logger.warning(
        "EOD-RECONCILE started (triggered_by=%s) for trading_day=%s — %s",
        triggered_by, trading_day, arn,
    )
    return arn


def _event_start_et(detail: dict, event: dict) -> Optional[datetime]:
    """The collection execution's start, in America/New_York. Step Functions
    status-change events carry ``detail.startDate`` as epoch milliseconds; the
    envelope ``time`` is the fallback."""
    start_ms = detail.get("startDate")
    if isinstance(start_ms, (int, float)) and start_ms > 0:
        return datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).astimezone(ET)
    raw = event.get("time")
    if isinstance(raw, str) and raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(ET)
    return None


def _execution_name_for(trading_day: str, collection_execution_arn: str) -> str:
    """Deterministic per collection execution, so an EventBridge redelivery (or
    the rule's own retry) is ExecutionAlreadyExists rather than a second run.
    Kept short: HealDispatchReplay names its replay
    ``eod-heal-replay-{run_date}-{this name}``, and SF names cap at 80."""
    digest = hashlib.sha1(collection_execution_arn.encode("utf-8")).hexdigest()[:8]
    return f"{EVENT_EXECUTION_PREFIX}{trading_day}-{digest}"


def _handle_collection_terminal(event: dict) -> dict:
    """``ne-data-collection-eod`` reached a terminal state: start the reconcile."""
    detail = event.get("detail") or {}
    collection_arn = str(detail.get("executionArn") or "")
    name = str(detail.get("name") or collection_arn.rsplit(":", 1)[-1])
    status = str(detail.get("status") or "")

    if detail.get("stateMachineArn") != COLLECTION_SF_ARN:
        logger.info("Event is not from %s — no-op.", COLLECTION_SF_ARN)
        return {"action": "noop", "reason": "not_the_eod_collection"}
    if name.startswith(HEAL_COLLECTION_PREFIX):
        # The rule's pattern already excludes these; asserting it again here
        # means a widened pattern cannot start a reconcile under a heal loop.
        logger.info("Collection %s is a reconcile heal execution — no-op.", name)
        return {"action": "noop", "reason": "heal_execution", "collection_execution": name}
    triggered_by = COLLECTION_TRIGGERED_BY.get(status)
    if triggered_by is None:
        logger.info("Collection status %s does not start a reconcile — no-op.", status)
        return {"action": "noop", "reason": "status_not_a_reconcile_trigger", "status": status}

    start_et = _event_start_et(detail, event)
    if start_et is None:
        # Fail loud: an event we cannot date is an event we cannot map to a
        # trading day, and guessing would reconcile the wrong session.
        raise ValueError(f"collection event carries no usable start time: {detail!r}")
    session_day: date = start_et.date()
    if not is_trading_day(session_day):
        logger.info("Collection %s started on %s, not a trading day — no-op.", name, session_day)
        return {"action": "noop", "reason": "not_a_trading_day", "date": str(session_day)}
    if start_et.time() < MARKET_CLOSE_ET:
        logger.info(
            "Collection %s started %s ET, before the close — not this evening's "
            "EOD collection; no-op.", name, start_et.isoformat(),
        )
        return {"action": "noop", "reason": "collection_started_before_the_close"}

    trading_day = session_day.isoformat()
    if _reconcile_running():
        return {"action": "noop", "reason": "reconcile_currently_running", "trading_day": trading_day}

    try:
        execution_arn = _start_reconcile(
            trading_day,
            triggered_by,
            _execution_name_for(trading_day, collection_arn or name),
            collection_execution_arn=collection_arn or None,
        )
    except Exception as exc:  # noqa: BLE001 — classify, re-raise everything else
        code = str((getattr(exc, "response", {}) or {}).get("Error", {}).get("Code", ""))
        if code != "ExecutionAlreadyExists":
            raise
        logger.info("Reconcile for collection %s already started — no-op.", name)
        return {"action": "noop", "reason": "already_started_for_this_collection", "trading_day": trading_day}
    return {
        "action": "started_reconcile",
        "trading_day": trading_day,
        "triggered_by": triggered_by,
        "execution_arn": execution_arn,
    }


def _handle_reconcile_backstop(now_utc: datetime) -> dict:
    """Scheduled 02:15 UTC TUE-SAT: start the reconcile if nothing produced the
    evening's eod_pnl row and nothing is still working on it."""
    session_day = now_utc.astimezone(ET).date()
    if not is_trading_day(session_day):
        logger.info("Not a NYSE trading day (%s ET) — no reconcile expected; no-op.", session_day)
        return {"action": "noop", "reason": "not_a_trading_day", "date": str(session_day)}
    trading_day = session_day.isoformat()

    # Cheapest and most decisive first; each no-op reason is distinct.
    if _reconcile_running():
        return {"action": "noop", "reason": "reconcile_currently_running", "trading_day": trading_day}
    if _collection_running():
        # Its terminal event starts the reconcile; racing it would reconcile
        # against a collection that is still writing.
        return {"action": "noop", "reason": "collection_still_running", "trading_day": trading_day}
    if _eod_did_its_job(trading_day):
        logger.info("eod_pnl row present for %s — no-op.", trading_day)
        return {"action": "noop", "reason": "eod_row_present", "trading_day": trading_day}
    if _reconcile_backstop_already_fired(trading_day):
        logger.error(
            "EOD-RECONCILE-BACKSTOP EXHAUSTED: already started a reconcile for "
            "trading_day=%s and the eod_pnl row is STILL missing. Not starting "
            "again — operator action required (config#1229 NAV continuity).",
            trading_day,
        )
        return {
            "action": "noop",
            "reason": "backstop_already_fired_and_row_still_missing",
            "trading_day": trading_day,
        }
    execution_arn = _start_reconcile(
        trading_day,
        "reconcile-backstop",
        f"{RECONCILE_BACKSTOP_PREFIX}{trading_day}-{int(time.time())}",
    )
    return {"action": "started_reconcile", "trading_day": trading_day, "execution_arn": execution_arn}


def _is_collection_status_event(event: dict) -> bool:
    return (
        event.get("source") == "aws.states"
        and event.get("detail-type") == "Step Functions Execution Status Change"
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    event = event if isinstance(event, dict) else {}
    if _is_collection_status_event(event):
        return _handle_collection_terminal(event)

    now_utc = datetime.now(timezone.utc)
    if event.get("mode") == "reconcile-backstop":
        return _handle_reconcile_backstop(now_utc)
    return _handle_postclose_backstop(now_utc)


def _handle_postclose_backstop(now_utc: datetime) -> dict:
    """The original 22:30 UTC firing, scoped since 2026-09-30 to the post-close
    pipeline (gate, box start, CaptureSnapshot)."""
    # Only trading days have an expected EOD. The EventBridge rule is MON-FRI,
    # so this skips NYSE holidays that fall on weekdays.
    if not is_trading_day(now_utc.date()):
        logger.info("Not a NYSE trading day (%s) — no EOD expected; no-op.", now_utc.date())
        return {"action": "noop", "reason": "not_a_trading_day", "date": str(now_utc.date())}

    trading_day = last_closed_trading_day(now_utc).isoformat()

    # alpha-engine-config-I7582: the dispatch predicate is the ARTIFACT, not the
    # fact that something started. Since the 2026-09-30 split the post-close
    # pipeline's artifact is the snapshot; the eod_pnl row is the reconcile
    # pipeline's, and its own backstop (_handle_reconcile_backstop) keys on it.
    if _eod_running():
        return {"action": "noop", "reason": "eod_currently_running", "trading_day": trading_day}

    if _snapshot_present(trading_day):
        logger.info("Post-close snapshot present for %s — no-op.", trading_day)
        return {"action": "noop", "reason": "snapshot_present", "trading_day": trading_day}

    if _backstop_already_fired_today(now_utc):
        # One retry per day. A second miss is a page, not another boot — see
        # _backstop_already_fired_today.
        logger.error(
            "EOD-BACKSTOP EXHAUSTED: this Lambda already dispatched the post-close "
            "pipeline today and trading_day=%s STILL has no snapshot. Not "
            "dispatching again — alpha-engine-eod-snapshot-existence-check pages "
            "at 23:30 ET; operator action required before NYSE-local midnight.",
            trading_day,
        )
        return {
            "action": "noop",
            "reason": "backstop_already_fired_and_snapshot_still_missing",
            "trading_day": trading_day,
        }

    # config-I6690: box state no longer gates dispatch — it is checked only
    # to tag the run (StartTradingInstance boots the box unconditionally and
    # idempotently either way; see module docstring).
    box_was_running = _trading_box_running()
    triggered_by = "backstop" if box_was_running else "backstop-box-stopped"
    execution_arn = _start_eod(trading_day, triggered_by)
    return {
        "action": "started_eod",
        "trading_day": trading_day,
        "execution_arn": execution_arn,
        "box_was_running": box_was_running,
    }
