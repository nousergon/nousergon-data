"""SNS-to-S3 mirror for the system-wide changelog.

Subscribed to arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts.
For every SNS message, writes one structured incident entry to:

  s3://alpha-engine-research/changelog/entries/{YYYY-MM-DD}/{event_id}.json

Schema 1.0.0 per alpha-engine-config/changelog/vocab.yaml. Carries the
controlled-vocab fields required by the schema-discipline arc.

Legacy dual-write to changelog/incidents/{YYYY}/{MM}/{DD}T... retired
2026-05-07 after the 1-week back-compat bake (per CLAUDE.md S3
contract). Historical changelog/incidents/ objects remain in S3 for
retroactive queries.

This is the "incident" half of the system-wide event-mining changelog
(deploys are written by the alpha-engine-docs append-changelog
composite action; manual + recovery entries by the changelog-log CLI).

Defaults applied to auto-emitted incident entries:
  severity            = "high"                  (alerts are high by default)
  subsystem           = "infrastructure"        (most SNS alerts are SF/Lambda failures)
  root_cause_category = "infrastructure_failure" (default; operator can override
                                                 with a follow-up changelog-log entry)
  auto_emitted        = true                    (so future aggregation can flag
                                                 entries needing human review)

Operator can refine these via a follow-up `changelog-log --event-type
investigation` entry whose `git_refs` reference the original event_id.

Managed outside CloudFormation — see ../../README.md and the sibling
deploy.sh in this directory. The decision to orphan from CF (rather
than keep it in the alpha-engine-orchestration stack) was made
2026-05-01 to avoid a perm cascade on the github-actions-lambda-deploy
OIDC role; trade-off + reconsideration triggers documented in the
alpha-engine-config/private-docs/ROADMAP.md "Observability" section.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from hashlib import sha1

import boto3

_S3 = boto3.client("s3")
_BUCKET = os.environ["CHANGELOG_BUCKET"]
_STRUCTURED_PREFIX = os.environ.get("CHANGELOG_STRUCTURED_PREFIX", "changelog/entries")

SCHEMA_VERSION = "1.0.0"


def _put(key: str, body: dict) -> None:
    _S3.put_object(
        Bucket=_BUCKET,
        Key=key,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json",
    )


def handler(event, context):
    wrote = 0
    for record in event.get("Records", []):
        sns = record.get("Sns", {})
        message = sns.get("Message", "") or ""
        subject = sns.get("Subject", "") or ""
        topic_arn = sns.get("TopicArn", "") or ""
        message_id = sns.get("MessageId", "") or ""
        ts_iso = sns.get("Timestamp") or datetime.now(timezone.utc).isoformat()

        ts_iso_clean = ts_iso.replace("Z", "+00:00")
        try:
            ts = datetime.fromisoformat(ts_iso_clean)
        except ValueError:
            ts = datetime.now(timezone.utc)

        ts_utc = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        entry_date = ts.strftime("%Y-%m-%d")

        topic_name = topic_arn.split(":")[-1] if topic_arn else "sns"
        summary_src = subject or (message.splitlines()[0] if message else "(empty)")
        summary = summary_src[:240]

        # Structured entry — schema 1.0.0 for downstream mining.
        # event_id matches the changelog-log CLI scheme.
        ts_id = ts_utc.replace(":", "-").rstrip("Z")
        digest_input = f"{ts_utc}|{topic_name}|{summary}".encode()
        event_hash = sha1(digest_input).hexdigest()[:7]
        actor_safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in topic_name)
        event_id = f"{ts_id}_{actor_safe}_{event_hash}"

        structured_entry = {
            "schema_version": SCHEMA_VERSION,
            "event_id": event_id,
            "ts_utc": ts_utc,
            "event_type": "incident",
            "severity": "high",
            "subsystem": "infrastructure",
            "root_cause_category": "infrastructure_failure",
            "resolution_type": None,
            "started_at": None,
            "detected_at": ts_utc,
            "resolved_at": None,
            "verified_at": None,
            "summary": summary,
            "description": message,
            "resolution_notes": None,
            "actor": topic_name,
            "machine": "lambda:changelog-incident-mirror",
            "source": "sns-mirror",
            "auto_emitted": True,
            "git_refs": [],
            "prompt_version": None,
            "run_id": None,
            "eval_run_ref": None,
            "sns": {
                "subject": subject,
                "topic_arn": topic_arn,
                "message_id": message_id,
            },
        }
        structured_key = f"{_STRUCTURED_PREFIX}/{entry_date}/{event_id}.json"
        _put(structured_key, structured_entry)

        print(
            f"Wrote structured=s3://{_BUCKET}/{structured_key} "
            f"subject={subject[:80]!r}"
        )
        wrote += 1

    return {"statusCode": 200, "wrote": wrote}
