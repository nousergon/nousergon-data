"""CloudWatch Logs subscription filter → S3 mirror for the system-wide changelog.

Subscribed via per-Lambda CloudWatch Logs subscription filters that match
ERROR / CRITICAL / "Task timed out" patterns. For every matched log event,
writes one structured incident entry to:

  s3://alpha-engine-research/changelog/entries/{YYYY-MM-DD}/{event_id}.json

Schema 1.0.0 per alpha-engine-config/changelog/vocab.yaml. Closes the
"Lambda errors directly to S3" gap from the system-wide event-mining
coverage matrix (ROADMAP > Observability > Gap 2, line ~2121) — Lambda
crashes land in CloudWatch Logs but not (by default) in SNS, so the
SNS-mirror Lambda misses them.

Defaults applied to auto-emitted entries (operator can refine via a
follow-up `changelog-log --event-type investigation` referencing the
emitted event_id):

  severity            = "high"
  subsystem           = inferred from log group name (see _infer_subsystem)
  root_cause_category = "infrastructure_failure"
  auto_emitted        = true
  source              = "cloudwatch-mirror"

CloudWatch Logs subscription-filter event payloads arrive as base64-encoded
gzip blobs under `event.awslogs.data`. After decode/decompress, the JSON
shape is:

    {
      "messageType": "DATA_MESSAGE",
      "owner": "...",
      "logGroup": "/aws/lambda/<function-name>",
      "logStream": "...",
      "subscriptionFilters": ["alpha-engine-error-mirror"],
      "logEvents": [
        {"id": "...", "timestamp": <epoch_ms>, "message": "..."}
      ]
    }

One structured incident entry is written per matched logEvent.

Managed outside CloudFormation alongside the SNS-mirror Lambda — see
sibling deploy.sh + README.md.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
from datetime import datetime, timezone
from hashlib import sha1

import boto3

_S3 = boto3.client("s3")
_BUCKET = os.environ["CHANGELOG_BUCKET"]
_STRUCTURED_PREFIX = os.environ.get("CHANGELOG_STRUCTURED_PREFIX", "changelog/entries")

SCHEMA_VERSION = "1.0.0"

# Map log-group prefix → vocab.yaml subsystem. Order matters — first match
# wins. The default for unmatched groups is "infrastructure" (covers
# ec2-lifecycle, changelog-mirror, anything else operator hasn't classified).
_SUBSYSTEM_MAP: tuple[tuple[str, str], ...] = (
    ("/aws/lambda/alpha-engine-predictor", "predictor"),
    ("/aws/lambda/alpha-engine-research", "research"),
    ("/aws/lambda/alpha-engine-data", "data_pipeline"),
    ("/aws/lambda/alpha-engine-replay", "eval"),
)


def _infer_subsystem(log_group: str) -> str:
    for prefix, subsystem in _SUBSYSTEM_MAP:
        if log_group.startswith(prefix):
            return subsystem
    return "infrastructure"


def _put(key: str, body: dict) -> None:
    _S3.put_object(
        Bucket=_BUCKET,
        Key=key,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json",
    )


def _decode_payload(event: dict) -> dict:
    """Decode CloudWatch Logs subscription filter event → raw JSON dict."""
    blob = event["awslogs"]["data"]
    raw = gzip.decompress(base64.b64decode(blob))
    return json.loads(raw)


def handler(event, context):
    payload = _decode_payload(event)
    if payload.get("messageType") != "DATA_MESSAGE":
        # Subscription-filter control messages (e.g., CONTROL_MESSAGE on
        # initial wiring) carry no logEvents — silently no-op.
        return {"statusCode": 200, "wrote": 0, "skipped": payload.get("messageType")}

    log_group = payload.get("logGroup", "")
    log_stream = payload.get("logStream", "")
    function_name = log_group.split("/")[-1] if log_group else "unknown"
    subsystem = _infer_subsystem(log_group)

    wrote = 0
    for log_event in payload.get("logEvents", []):
        message = (log_event.get("message") or "").rstrip()
        if not message:
            continue
        ts_ms = log_event.get("timestamp") or 0
        ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms else datetime.now(timezone.utc)
        ts_utc = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        entry_date = ts.strftime("%Y-%m-%d")

        # First non-empty line is the summary; full message is the description.
        first_line = next((ln for ln in message.splitlines() if ln.strip()), message)
        summary = first_line[:240]

        # event_id format mirrors the SNS-mirror + composite-action scheme:
        # {ts}_{actor}_{7-hex}. Actor here is the source Lambda's function
        # name (sanitized).
        ts_id = ts_utc.replace(":", "-").rstrip("Z")
        actor_safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in function_name)
        # log_event.id is already unique per event in the source log stream;
        # hashing it (rather than ts+summary) prevents two errors-at-the-
        # same-second from colliding into one event_id.
        event_hash = sha1(
            f"{log_event.get('id', '')}|{message}".encode()
        ).hexdigest()[:7]
        event_id = f"{ts_id}_{actor_safe}_{event_hash}"

        structured_entry = {
            "schema_version": SCHEMA_VERSION,
            "event_id": event_id,
            "ts_utc": ts_utc,
            "event_type": "incident",
            "severity": "high",
            "subsystem": subsystem,
            "root_cause_category": "infrastructure_failure",
            "resolution_type": None,
            "started_at": None,
            "detected_at": ts_utc,
            "resolved_at": None,
            "verified_at": None,
            "summary": summary,
            "description": message,
            "resolution_notes": None,
            "actor": function_name,
            "machine": "lambda:changelog-cloudwatch-mirror",
            "source": "cloudwatch-mirror",
            "auto_emitted": True,
            "git_refs": [],
            "prompt_version": None,
            "run_id": None,
            "eval_run_ref": None,
            "cloudwatch": {
                "log_group": log_group,
                "log_stream": log_stream,
                "log_event_id": log_event.get("id", ""),
            },
        }
        structured_key = f"{_STRUCTURED_PREFIX}/{entry_date}/{event_id}.json"
        _put(structured_key, structured_entry)
        print(
            f"Wrote structured=s3://{_BUCKET}/{structured_key} "
            f"function={function_name} subsystem={subsystem} "
            f"summary={summary[:80]!r}"
        )
        wrote += 1

    return {"statusCode": 200, "wrote": wrote}
