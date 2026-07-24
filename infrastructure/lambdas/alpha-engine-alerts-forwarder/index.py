"""SNS-to-EventBridge forwarder for the alpha-engine-alerts topic.

Subscribed to arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts.
For every SNS message, republishes it as a ``nousergon.alert.v1`` event on
the ``nousergon-alerts`` custom EventBridge bus so the Overseer intake queue
(consumed by the alert-drain agent) sees alerts that bypassed the krepis
chokepoints — native Step-Functions ``sns:publish`` ASL states and raw
boto3 ``sns.publish()`` call sites.

This is the "last-mile" bridge described in alpha-engine-config-I2908:
krepis-based emitters already reach the bus via ``krepis.alerts.publish``
→ ``nousergon.krepis`` source on the bus → intake queue. CloudWatch alarms
reach it via the default-bus CW tap rule. SNS-native publishes had no path
to the bus at all — this Lambda closes that gap.

Design (alpha-engine-config-I2908 deliverable):
- krepis-free: raw boto3 only (no transitive dep complexity).
- Fail-safe: exceptions are caught and logged; they MUST NOT propagate
  (SNS retries a failing subscriber, but blocking the downstream
  changelog-incident-mirror subscriber with Lambda startup-throttle
  would be worse).
- No dedup: dedup belongs to krepis markers / drain correlation. SNS→Lambda
  delivery is at-least-once; the drain already dedups by its own correlation
  keys. We synthesize a stable ``dedup_key`` from MessageId for lifecycle
  sanity.
- Source attribution: the original TopicArn, Subject, and MessageId are
  carried in the event detail so the drain can classify by source topic.
- Do NOT forward alpha-engine-alarm-backstop (epic I2821 invariant 3):
  the last-resort backstop must never route through the bus/queue machinery
  it watches.

Managed outside CloudFormation — see the sibling changelog-incident-mirror
directory for the reasoning. Deployment via sibling deploy.sh (identical
pattern).
"""

from __future__ import annotations

import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_EVENTS = boto3.client("events")
_BUS_NAME = os.environ.get("ALERTS_FORWARDER_BUS_NAME", "nousergon-alerts")
_REGION = os.environ.get("AWS_REGION", "us-east-1")
_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "")
if not _ACCOUNT_ID:
    try:
        _ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
    except Exception:
        _ACCOUNT_ID = "unknown"

# Event source for the forwarded events on the nousergon-alerts bus.
# Shared across all SNS-forwarded alerts so the drain's rule needs only
# one source match.
_SOURCE = "nousergon.sns-forwarder"

# Detail-type on the nousergon-alerts bus — matches the krepis event
# shape convention (nousergon.alert.v1).
_DETAIL_TYPE = "nousergon.alert.v1"


def _build_event_detail(sns_record: dict) -> dict:
    """Build the ``nousergon.alert.v1``-shaped detail from an SNS record.

    Carries the original SNS metadata so the drain can attribute the
    alert to its originating topic and classify by subject/message content.
    """
    message = sns_record.get("Message", "") or ""
    subject = sns_record.get("Subject", "") or ""
    topic_arn = sns_record.get("TopicArn", "") or ""
    message_id = sns_record.get("MessageId", "") or ""

    # Try to parse the message as JSON; fall back to raw text.
    try:
        parsed_message = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        parsed_message = {"text": message}

    return {
        "version": "1.0",
        "source": "sns-forwarder",
        "original_topic_arn": topic_arn,
        "original_subject": subject,
        "original_message_id": message_id,
        "message": parsed_message,
        "dedup_key": f"sns-{message_id}" if message_id else None,
    }


def handler(event: dict, context=None) -> dict:
    """Handle SNS notification event, forwarding each record to EventBridge.

    Returns a dict with counts of forwarded and failed records for
    observability in CloudWatch Logs.
    """
    forwarded = 0
    failed = 0
    errors: list[str] = []

    records = event.get("Records", [])
    if not records:
        logger.warning("No Records in event (not an SNS invocation?)")
        return {"forwarded": 0, "failed": 0, "errors": []}

    entries: list[dict] = []
    for record in records:
        sns = record.get("Sns", {})
        if not sns:
            logger.warning("Record missing Sns key: %s", json.dumps(record)[:200])
            failed += 1
            errors.append("missing Sns key")
            continue

        detail = _build_event_detail(sns)
        topic_arn = sns.get("TopicArn", "")
        topic_name = topic_arn.split(":")[-1] if topic_arn else "unknown"

        # Enforce epic I2821 invariant 3: never forward the backstop topic.
        if topic_name == "alpha-engine-alarm-backstop":
            logger.info(
                "Skipping backstop topic alpha-engine-alarm-backstop "
                "(epic I2821 invariant 3 — the backstop must not route "
                "through the bus it watches). MessageId=%s",
                sns.get("MessageId", ""),
            )
            continue

        entries.append(
            {
                "Source": _SOURCE,
                "DetailType": _DETAIL_TYPE,
                "Detail": json.dumps(detail),
                "EventBusName": _BUS_NAME,
                "Resources": [],
                # SNS Timestamp or current time
                "Time": sns.get("Timestamp") or None,
            }
        )

    if not entries:
        logger.info("No entries to forward (all records skipped or empty)")
        return {"forwarded": 0, "failed": failed, "errors": errors}

    # PutEvents supports up to 10 entries per call. Batch if needed.
    batch_size = 10
    for i in range(0, len(entries), batch_size):
        batch = entries[i : i + batch_size]
        try:
            resp = _EVENTS.put_events(Entries=batch)
            failed_count = resp.get("FailedEntryCount", 0)
            forwarded += len(batch) - failed_count
            if failed_count > 0:
                for entry_result in resp.get("Entries", []):
                    if "ErrorCode" in entry_result:
                        failed += 1
                        err_msg = (
                            f"PutEvents error for entry batch offset {i}: "
                            f"{entry_result['ErrorCode']} — {entry_result.get('ErrorMessage', '')}"
                        )
                        logger.error(err_msg)
                        errors.append(err_msg)
        except Exception as exc:
            failed += len(batch)
            err_msg = f"PutEvents exception for batch offset {i}: {exc}"
            logger.error(err_msg, exc_info=True)
            errors.append(err_msg)

    logger.info(
        "Forwarded=%d Failed=%d from %d SNS records",
        forwarded,
        failed,
        len(records),
    )
    return {"forwarded": forwarded, "failed": failed, "errors": errors}
