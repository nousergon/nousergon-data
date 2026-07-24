"""alpha-engine-alert-drain-liveness-probe — mid-run spot-reclaim checker for
the Overseer alert-drain (config#3173, generalizing sf-watch-reclaim-sweep-handler's
config#2270 mechanism to the alert-drain family).

WHY: alert-drain runs on a twice-daily EventBridge schedule, so — unlike
ci-watch — a missed/dead run is NOT permanently silent: the next scheduled
run 12h later will still fire and drain whatever the queue's at-least-once
semantics left behind. But that self-heal is SLOW (up to ~24h if two
consecutive runs die) and, more importantly, SILENT — nothing tells anyone a
drain box died mid-run rather than finishing cleanly, which is exactly the
"no arc silently stalls" gap alpha-engine-config#3173 (child of the #3137
stall-watchdog charter) asks every dispatch family to close. Unlike ci-watch,
NO dispatch-record reconstruction is needed here: a relaunch just needs to
run the drain again (the SQS queue — not any per-run field — is the durable
state a fresh box picks up from), so this checker is simpler than its
ci-watch sibling.

MECHANISM (mirrors sf-watch-reclaim-sweep-handler's reclaim checker): EventBridge
target for `EC2 Spot Instance Interruption Warning` and `EC2 Instance
State-change Notification` (state=terminated) events fleet-wide (neither
event type is tag-scopable in the rule pattern — this handler filters by the
box's own Name tag). For an `alpha-engine-alert-drain-spot` box:
  1. DescribeTags for `alert-drain-run-id` (rides the RunInstances call
     atomically with launch, config#2292 — a box reaching this checker
     without it is a genuine anomaly).
  2. A `drill-`-prefixed run id (config#2223 pattern) is a canary drill, not a
     repair — isolated below exactly like sf-watch-reclaim-sweep-handler's
     `run_date.startswith("drill-")` carve-out: the missed
     `overseer/_canary/{date}.json` heartbeat is the correct alerting
     surface, not a page from this checker.
  3. HEAD the completion marker
     (`overseer/_control/completed/alert-drain-{run_id}.json`, the same key
     scripts/alert_drain_run.sh writes). Present = clean finish.
  4. Absent = died mid-run. Read the relaunch ledger
     (`overseer/_control/relaunch/alert-drain-{run_id}.json`): a record
     naming THIS dead instance is a duplicate notification (both EC2 event
     types fire for one death); a record naming a DIFFERENT instance is a
     second death for the same run_id — the exactly-one relaunch bound is
     spent, escalate LOUD instead of relaunching again.
  5. First death: record the relaunch decision FIRST (exactly-one bound),
     THEN invoke alpha-engine-alert-drain-dispatcher DIRECTLY (bypassing the
     Overseer router and the twice-daily schedule entirely, mirroring
     sf-watch-reclaim-sweep-handler's direct invoke of the spot dispatcher) with a
     fresh `{"is_drill": "false", "trigger": "reclaim-relaunch"}` — the drain
     needs no fields from the dead run, only to run again.

Fail-loud (CLAUDE.md no-silent-fails): DescribeTags and the ledger/marker
reads RAISE on any error OTHER than genuine absence. The Telegram send and
the dispatcher re-invoke are the only best-effort/secondary surfaces.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
_FLOW_NAME = "alert-drain-liveness-probe"
_DB_BASENAME = "flow_doctor_alert_drain_liveness_probe"
_OPS_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)

# The dispatcher this checker relaunches through directly — bypassing the
# Overseer router and the twice-daily schedule entirely.
ALERT_DRAIN_DISPATCHER_FUNCTION = os.environ.get(
    "ALERT_DRAIN_DISPATCHER_FUNCTION", "alpha-engine-alert-drain-dispatcher"
)

WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")

ALERT_DRAIN_SPOT_TAG_NAME = "alpha-engine-alert-drain-spot"
ALERT_DRAIN_RUN_ID_TAG_KEY = "alert-drain-run-id"
COMPLETION_MARKER_PREFIX = "overseer/_control/completed/"
RELAUNCH_LEDGER_PREFIX = "overseer/_control/relaunch/"

RECLAIM_INTERRUPTION_DETAIL_TYPE = "EC2 Spot Instance Interruption Warning"
RECLAIM_STATE_CHANGE_DETAIL_TYPE = "EC2 Instance State-change Notification"
RECLAIM_DETAIL_TYPES = frozenset(
    {RECLAIM_INTERRUPTION_DETAIL_TYPE, RECLAIM_STATE_CHANGE_DETAIL_TYPE}
)


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _ec2_client():
    return boto3.client("ec2", region_name=REGION)


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _lambda_client():
    return boto3.client("lambda", region_name=REGION)


def _is_reclaim_event(event: dict) -> bool:
    return (
        isinstance(event, dict)
        and event.get("source") == "aws.ec2"
        and event.get("detail-type") in RECLAIM_DETAIL_TYPES
    )


def _instance_tags(instance_id: str) -> dict[str, str]:
    """Raises on any API error (fail-loud) — see module docstring."""
    resp = _ec2_client().describe_tags(
        Filters=[{"Name": "resource-id", "Values": [instance_id]}]
    )
    return {t.get("Key", ""): t.get("Value", "") for t in resp.get("Tags", [])}


def _completion_key(run_id: str) -> str:
    return f"{COMPLETION_MARKER_PREFIX}alert-drain-{run_id}.json"


def _relaunch_key(run_id: str) -> str:
    return f"{RELAUNCH_LEDGER_PREFIX}alert-drain-{run_id}.json"


def _completion_marker_exists(s3, run_id: str) -> bool:
    """Only a true absence (404/NoSuchKey/NotFound) means 'no marker'; any
    OTHER error RAISES (config#2267 site-4 lesson)."""
    try:
        s3.head_object(Bucket=WATCH_BUCKET, Key=_completion_key(run_id))
        return True
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _read_json(s3, key: str) -> dict | None:
    """None on a true absence; RAISES on any other read error. A present-but-
    unparseable object also returns None (caller escalates rather than
    guessing)."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    try:
        return json.loads(obj["Body"].read())
    except (ValueError, TypeError) as exc:
        logger.warning("unparseable object at %s: %s", key, exc)
        return None


def _record_relaunch(s3, run_id: str, dead_instance_id: str) -> None:
    """PRIMARY deliverable of the relaunch path — RAISES on failure, runs
    BEFORE the dispatcher invoke (the exactly-one bound must never depend on
    the invoke succeeding)."""
    s3.put_object(
        Bucket=WATCH_BUCKET,
        Key=_relaunch_key(run_id),
        Body=json.dumps({
            "dead_instance_id": dead_instance_id,
            "relaunched_at": datetime.now(timezone.utc).isoformat(),
        }).encode("utf-8"),
        ContentType="application/json",
    )


def _escalate(text: str, dedup_key: str, context_info: dict) -> bool:
    try:
        return notify_via_flow_doctor(
            text,
            silent=False,
            severity="error",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context=context_info,
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still logged + returned
        logger.warning("alert-drain reclaim escalation Telegram send failed (non-fatal): %s", exc)
        return False


def _note(text: str, dedup_key: str, context_info: dict) -> bool:
    try:
        return notify_via_flow_doctor(
            text,
            silent=True,
            severity="info",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context=context_info,
            silent_topic=FleetTelegramTopic.OPS_HEALTH,
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; relaunch already fired + recorded
        logger.warning("alert-drain reclaim relaunch Telegram note failed (non-fatal): %s", exc)
        return False


def _handle_reclaim_event(event: dict) -> dict:
    detail = event.get("detail") or {}
    detail_type = str(event.get("detail-type") or "")
    instance_id = str(detail.get("instance-id") or "")
    if not instance_id:
        raise ValueError(f"EC2 reclaim event without instance-id (detail-type={detail_type!r})")
    base = {"reclaim_event": True, "detail_type": detail_type, "instance_id": instance_id}

    if detail_type == RECLAIM_STATE_CHANGE_DETAIL_TYPE and str(detail.get("state") or "") != "terminated":
        logger.info("reclaim check: ignoring non-terminated state-change for %s", instance_id)
        return {**base, "handled": False, "reason": "not_terminated"}

    tags = _instance_tags(instance_id)
    if tags.get("Name") != ALERT_DRAIN_SPOT_TAG_NAME:
        logger.info("reclaim check: %s is not an alert-drain box (Name=%r) — ignoring",
                    instance_id, tags.get("Name"))
        return {**base, "watch_box": False}

    run_id = tags.get(ALERT_DRAIN_RUN_ID_TAG_KEY, "")
    if not run_id:
        alerted = _escalate(
            "\U0001f6a8 *Alert-Drain reclaim checker — UNTAGGED watch box died*\n"
            f"Watch box `{instance_id}` terminated without its run-id "
            "discriminator tag — cannot verify completion or relaunch. "
            "Check the alert-drain-dispatcher tag-write path (config#2267 site 2).",
            dedup_key=f"{_FLOW_NAME}:untagged:{instance_id}",
            context_info={"instance_id": instance_id, "tags": tags},
        )
        return {**base, "watch_box": True, "handled": False,
                "reason": "missing_discriminator_tag", "escalated": alerted}

    key_ctx = {"instance_id": instance_id, "run_id": run_id}
    s3 = _s3_client()

    # Drill isolation (config#2223 pattern): a drill's run_id ALWAYS carries
    # the "drill-" prefix (alert-drain-dispatcher's _resolve_event_fields) —
    # never a repair to retry; its own missed canary heartbeat is the
    # alerting surface.
    if run_id.startswith("drill-"):
        completed = _completion_marker_exists(s3, run_id)
        logger.info(
            "reclaim check: drill box %s (run_id=%s) %s — no relaunch/"
            "escalation for drills; the missed _canary heartbeat is the "
            "alerting surface (config#2223)", instance_id, run_id,
            "finished cleanly" if completed else "died WITHOUT a completion marker",
        )
        return {**base, "watch_box": True, "drill": True, "completed": completed,
                "relaunched": False}

    if _completion_marker_exists(s3, run_id):
        logger.info("reclaim check: %s finished cleanly (run_id=%s)", instance_id, run_id)
        return {**base, "watch_box": True, "completed": True}

    # No completion marker: the box died mid-run.
    relaunch_record = _read_json(s3, _relaunch_key(run_id))
    if relaunch_record is not None:
        if relaunch_record.get("dead_instance_id") == instance_id:
            logger.info("reclaim check: death of %s already handled — duplicate notification",
                        instance_id)
            return {**base, "watch_box": True, "completed": False, "duplicate_notification": True}
        alerted = _escalate(
            "\U0001f6a8 *Alert-Drain reclaim checker — SECOND watch-box death*\n"
            f"run_id={run_id}: relaunched box `{instance_id}` ALSO died "
            "without a completion marker (prior relaunch: "
            f"`{relaunch_record.get('dead_instance_id', '?')}`). The bounded "
            "relaunch budget is spent — human needed (config#3173).",
            dedup_key=f"{_FLOW_NAME}:second_death:{run_id}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": False,
                "reason": "second_death", "escalated": alerted}

    # First mid-run death: record FIRST (exactly-one bound), then invoke —
    # no dispatch-record reconstruction needed (unlike ci-watch): a fresh
    # drain run carries no per-run fields, only "run again".
    _record_relaunch(s3, run_id, instance_id)
    payload = {"is_drill": "false", "trigger": "reclaim-relaunch"}
    try:
        _lambda_client().invoke(
            FunctionName=ALERT_DRAIN_DISPATCHER_FUNCTION,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        invoked = True
    except Exception as exc:  # noqa: BLE001 — relaunch decision already recorded; invoke is best-effort
        invoked = False
        logger.error(
            "alert-drain relaunch invoke failed for run_id=%s (relaunch "
            "already recorded — this will NOT retry itself; treat as an "
            "escalation): %s: %s", run_id, type(exc).__name__, exc,
        )
    logger.warning(
        "reclaim check: alert-drain box %s (run_id=%s) died mid-run — relaunch %s",
        instance_id, run_id, "dispatched" if invoked else "invoke FAILED",
    )
    if invoked:
        noted = _note(
            "\U0001f6f0️ *Alert-Drain reclaim checker — bounded relaunch*\n"
            f"Watch box `{instance_id}` (run_id={run_id}) was reclaimed "
            "mid-drain — a fresh drain was relaunched (attempt 1/1; a second "
            "death escalates loud, config#3173).",
            dedup_key=f"{_FLOW_NAME}:relaunch:{run_id}:{instance_id}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": True,
                "telegram_sent": noted}

    alerted = _escalate(
        "\U0001f6a8 *Alert-Drain reclaim checker — relaunch invoke FAILED*\n"
        f"run_id={run_id}: box `{instance_id}` died mid-run; the relaunch "
        "decision was recorded but invoking "
        f"`{ALERT_DRAIN_DISPATCHER_FUNCTION}` failed — no fresh box was "
        "actually launched. Relaunch manually (config#3173).",
        dedup_key=f"{_FLOW_NAME}:invoke_failed:{run_id}",
        context_info=key_ctx,
    )
    return {**base, "watch_box": True, "completed": False, "relaunched": False,
            "reason": "invoke_failed", "escalated": alerted}


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Entrypoint. The only path this Lambda serves is the EC2 reclaim/
    termination event — a non-reclaim invocation (e.g. a manual smoke test)
    is a documented no-op (no scheduled sweep needed: the twice-daily
    schedule itself already re-fires independent of any prior run's outcome
    — see module docstring)."""
    if _is_reclaim_event(event or {}):
        return _handle_reclaim_event(event)
    logger.info("alert-drain-liveness-probe: non-reclaim invocation — no-op (event=%r)", event)
    return {"reclaim_event": False, "noop": True}
