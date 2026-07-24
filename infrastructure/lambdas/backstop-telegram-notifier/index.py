"""alpha-engine-backstop-telegram-notifier — INDEPENDENT Telegram leg for the
backstop alarm topic (alpha-engine-alarm-backstop).

Subscribes DIRECTLY to the SNS topic (not via EventBridge), forwarding every
CloudWatch alarm message to Telegram via raw urllib — NO nousergon_lib, NO
krepis, NO flow-doctor, NO DynamoDB dedup, NO EventBridge bus involvement.
Shares zero code and zero non-IAM infrastructure with the smart forwarder paths
(sf-telegram-notifier, flow-doctor, etc.).

Designed per alpha-engine-config-I2899 invariant 3: the backstop must never
involve an agent, a queue, or anything that can fail non-obviously. The email
leg (the primary backstop channel) is unaffected — this is an ADDITIONAL
independent channel whose own errors are acceptable-silent.

Design:
  - urllib only — zero pip dependencies, zero 3rd-party packages
  - Bot token read from SSM at EVERY invoke (no cached credentials)
  - SNS → Lambda direct subscription (no EventBridge, no DLQ)
  - Errors are logged + swallowed — the email leg remains the canonical backstop
  - No dedup, no rate-limiting beyond SNS native batching
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
SSM_BOT_TOKEN_PATH = os.environ.get(
    "SSM_BOT_TOKEN_PATH", "/alpha-engine/TELEGRAM_BOT_TOKEN"
)
SSM_CHAT_ID_PATH = os.environ.get(
    "SSM_CHAT_ID_PATH", "/alpha-engine/TELEGRAM_CHAT_ID"
)
SSM_THREAD_ID_PATH = os.environ.get(
    "SSM_THREAD_ID_PATH",
    "/alpha-engine/FLOW_DOCTOR_TELEGRAM_THREAD_CRITICAL",
)

TELEGRAM_API_BASE = "https://api.telegram.org/bot"


def _ssm_parameter(path: str) -> str:
    """Read a plaintext SSM parameter. Raises on any error (fail-loud: a
    misconfigured SSM path means the forwarder cannot function at all — better
    to surface via CloudWatch logs + the Lambda Errors metric than silently
    drop every alarm)."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=path, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _send_telegram(
    bot_token: str,
    chat_id: str,
    text: str,
    thread_id: str | None = None,
) -> dict | None:
    """Send ``text`` to the given Telegram chat/thread via the Bot API.

    Returns the JSON response on success, or None on any non-fatal error (the
    email leg remains the canonical backstop; Telegram delivery failures are
    logged but never raised).

    Uses raw urllib — zero pip dependencies, mirrors the issue's requirement
    that the backstop shares zero code with the smart path.
    """
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if thread_id:
        payload["message_thread_id"] = int(thread_id)

    data = json.dumps(payload).encode("utf-8")
    url = f"{TELEGRAM_API_BASE}{bot_token}/sendMessage"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read()
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read()
            if isinstance(body, bytes):
                body = body.decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            body = str(exc)
        logger.warning("Telegram API HTTP %s: %s", exc.code, body)
        return None
    except urllib.error.URLError as exc:
        logger.warning("Telegram API connection failed: %s", exc.reason)
        return None
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("Telegram send unexpected error: %s", exc)
        return None


def _escape_markdown(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters in ``text``.

    MarkdownV2 requires escaping: _ * [ ] ( ) ~ ` > # + - = | { } . !
    """
    special = r"_*[]()~`>#+-=|{}.!"
    for ch in special:
        text = text.replace(ch, f"\\{ch}")
    return text


def _format_alarm_message(sns_message: dict) -> str:
    """Build a compact Telegram message from a CloudWatch alarm SNS payload.

    Extracts the key fields (alarm name, state, reason, metric, dimensions,
    link) and formats them for mobile-readable Telegram display.
    """
    alarm_name = sns_message.get("AlarmName", "unknown")
    old_state = sns_message.get("OldStateValue", "?")
    new_state = sns_message.get("NewStateValue", "?")
    reason = sns_message.get("NewStateReason", "")
    region = sns_message.get("Region", REGION)

    # Emoji based on severity
    state_emoji = {
        "ALARM": "🔴",
        "OK": "🟢",
        "INSUFFICIENT_DATA": "⚪",
    }
    emoji = state_emoji.get(new_state, "🔔")

    lines = [
        f"{emoji} *BACKSTOP: {alarm_name}*"
        if new_state == "ALARM"
        else f"{emoji} *BACKSTOP RESOLVED: {alarm_name}*",
        f"State: {old_state} → **{new_state}**",
    ]

    if reason:
        # Escape markdown and truncate — reasons can be very long
        escaped = _escape_markdown(reason)
        if len(escaped) > 600:
            escaped = escaped[:597] + "..."
        lines.append(f"Reason: {escaped}")

    # Extract metric info from trigger
    trigger = sns_message.get("Trigger", {})
    metric = trigger.get("MetricName", "")
    namespace = trigger.get("Namespace", "")
    if metric:
        dims = trigger.get("Dimensions", [])
        dim_str = " ".join(
            f"`{d['value']}`" for d in dims if isinstance(d, dict) and d.get("value")
        )
        lines.append(f"Metric: {_escape_markdown(namespace)} / {_escape_markdown(metric)} {dim_str}")

    # Console link
    encoded_name = alarm_name.replace(" ", "+")
    console_url = (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region}#alarmsV2:alarm/{encoded_name}"
    )
    lines.append(f"[AWS Console]({console_url})")

    return "\n".join(lines)


def handler(event: dict, context) -> dict:  # noqa: ARG001
    """SNS-triggered handler: forward every backstop alarm to Telegram.

    Accepts the SNS event shape (``Records[].Sns``) and forwards each unique
    SNS message to the configured Telegram chat.

    Returns a summary dict with per-record outcomes. Errors are logged and
    returned — never raised — so a transient Telegram failure does not cause
    SNS redrive (the email leg remains independent).
    """
    records = event.get("Records", [])
    if not records:
        # Direct invoke or test payload without Records array — try as a
        # single SNS message body or bare alarm payload.
        records = [{"Sns": {"Message": json.dumps(event)}}]

    # Read secrets at invoke time (no cached credentials).
    try:
        bot_token = _ssm_parameter(SSM_BOT_TOKEN_PATH)
        chat_id = _ssm_parameter(SSM_CHAT_ID_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to read SSM parameters: %s", exc)
        return {"status": "error", "reason": f"SSM read failed: {exc}", "sent": 0}

    # Thread ID is optional — read it best-effort
    thread_id: str | None = None
    try:
        thread_id = _ssm_parameter(SSM_THREAD_ID_PATH)
    except Exception:  # noqa: S110, BLE001
        pass  # no thread — send to the chat's general topic

    results: list[dict] = []
    sent_count = 0

    for record in records:
        sns_info = record.get("Sns", {})
        message_str = str(sns_info.get("Message", "{}"))
        message_id = str(sns_info.get("MessageId", "?"))

        try:
            alarm = json.loads(message_str)
        except (ValueError, TypeError) as exc:
            logger.warning("Non-JSON SNS message %s: %s", message_id, exc)
            results.append({"message_id": message_id, "status": "skipped", "reason": "non-json"})
            continue

        # Skip non-alarm messages (e.g. SNS subscription confirmation)
        if "AlarmName" not in alarm:
            logger.info("SNS message %s is not a CloudWatch alarm — skipping", message_id)
            results.append({"message_id": message_id, "status": "skipped", "reason": "not-an-alarm"})
            continue

        text = _format_alarm_message(alarm)
        resp = _send_telegram(bot_token, chat_id, text, thread_id)
        if resp and resp.get("ok"):
            sent_count += 1
            results.append({"message_id": message_id, "status": "sent", "ok": True})
        else:
            err_desc = (resp or {}).get("description", "send failed")
            logger.warning("Telegram send failed for %s: %s", message_id, err_desc)
            results.append({"message_id": message_id, "status": "failed", "error": err_desc})

    logger.info("backstop-telegram: %d/%d sent", sent_count, len(results))
    return {"status": "ok", "sent": sent_count, "total": len(results), "results": results}
