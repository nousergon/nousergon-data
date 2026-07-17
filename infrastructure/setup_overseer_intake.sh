#!/usr/bin/env bash
# setup_overseer_intake.sh — Nousergon Overseer intake plane, phase 1
# (alpha-engine-config-I2822, epic alpha-engine-config-I2821).
#
# Why this exists: every operator alert the fleet sends (SNS email, Telegram
# push/silent) is human-facing and fire-and-forget — once delivered, nothing
# owns follow-through. Phase 1 gives alerts a machine-readable second life:
# the krepis chokepoints (krepis.alerts.publish, krepis.telegram.send_message,
# krepis>=0.15.0) emit structured `nousergon.alert.v1` events, and this script
# provisions where those events land:
#
#   1. EventBridge custom bus `nousergon-alerts` — the fleet alert event bus.
#   2. SQS `nousergon-overseer-intake` (+ DLQ) — the durable intake queue the
#      Overseer alert-drain (phase 3, alpha-engine-config-I2824) consumes.
#      14-day retention: the drain runs on a schedule, not a poller.
#   3. Rule on the custom bus: source=nousergon.krepis → intake queue.
#   4. Rule on the DEFAULT bus: CloudWatch alarm state-change → ALARM →
#      intake queue. This covers every CW alarm (deadman alarms, backstop
#      alarms, disk alarms) with ZERO code — CloudWatch emits these events
#      natively; no Lambda/SNS hop and no IAM on the emitting side.
#
# Deliberately NOT here (epic invariant 3, alpha-engine-config-I2821): the
# alpha-engine-alarm-backstop SNS topic and its CW alarms are untouched — the
# last-resort backstop must never route THROUGH the bus/queue machinery it
# watches. The default-bus rule above is an ADDITIVE tap on alarm state
# changes; SNS delivery of every alarm is unchanged.
#
# IAM for emitters is a separate concern — see
# attach_overseer_put_events_policy.sh (creates/attaches the
# nousergon-alerts-put-events managed policy to fleet Lambda/EC2 roles).
# Until a role has the grant, krepis falls back to an S3 drop-zone write
# (s3://alpha-engine-research/overseer/intake-fallback/) which the phase-3
# drain reads alongside the queue, so no event is lost during IAM rollout.
#
# Idempotent: create-event-bus/create-queue tolerate AlreadyExists; put-rule /
# put-targets / set-queue-attributes upsert. Safe to re-run.
#
# Usage:
#   ./infrastructure/setup_overseer_intake.sh [--dry-run]

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
BUS_NAME="nousergon-alerts"
QUEUE_NAME="nousergon-overseer-intake"
DLQ_NAME="nousergon-overseer-intake-dlq"
ALERT_RULE_NAME="overseer-intake-alert-events"
CW_ALARM_RULE_NAME="overseer-intake-cw-alarm-state"
RETENTION_SECONDS=1209600  # 14 days (SQS maximum)
MAX_RECEIVE_COUNT=5

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY-RUN: aws $*"
  else
    aws "$@"
  fi
}

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "== Overseer intake setup: account=${ACCOUNT_ID} region=${REGION} dry_run=${DRY_RUN}"

# ── 1. Custom event bus ──────────────────────────────────────────────────────
if aws events describe-event-bus --name "$BUS_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "event bus ${BUS_NAME}: exists"
else
  run events create-event-bus --name "$BUS_NAME" --region "$REGION" > /dev/null
  echo "event bus ${BUS_NAME}: created"
fi
BUS_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:event-bus/${BUS_NAME}"

# ── 2. DLQ + intake queue ────────────────────────────────────────────────────
ensure_queue() {
  local name="$1" attrs="$2"
  if aws sqs get-queue-url --queue-name "$name" --region "$REGION" >/dev/null 2>&1; then
    echo "queue ${name}: exists"
  else
    run sqs create-queue --queue-name "$name" --attributes "$attrs" --region "$REGION" > /dev/null
    echo "queue ${name}: created"
  fi
}

ensure_queue "$DLQ_NAME" "{\"MessageRetentionPeriod\":\"${RETENTION_SECONDS}\"}"
DLQ_URL=$(aws sqs get-queue-url --queue-name "$DLQ_NAME" --region "$REGION" --query QueueUrl --output text 2>/dev/null || echo "")
DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${DLQ_NAME}"

REDRIVE_POLICY="{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE_COUNT}\\\"}"
ensure_queue "$QUEUE_NAME" "{\"MessageRetentionPeriod\":\"${RETENTION_SECONDS}\",\"RedrivePolicy\":\"${REDRIVE_POLICY}\"}"
QUEUE_URL=$(aws sqs get-queue-url --queue-name "$QUEUE_NAME" --region "$REGION" --query QueueUrl --output text 2>/dev/null || echo "")
QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${QUEUE_NAME}"

# Upsert retention + redrive on pre-existing queues too (idempotent re-runs
# after parameter changes).
if [[ "$DRY_RUN" == "0" && -n "$QUEUE_URL" ]]; then
  aws sqs set-queue-attributes --queue-url "$QUEUE_URL" --region "$REGION" \
    --attributes "{\"MessageRetentionPeriod\":\"${RETENTION_SECONDS}\",\"RedrivePolicy\":\"${REDRIVE_POLICY}\"}"
fi

# ── 3. Rule on the custom bus: krepis alert events → queue ──────────────────
run events put-rule --region "$REGION" \
  --name "$ALERT_RULE_NAME" \
  --event-bus-name "$BUS_NAME" \
  --state ENABLED \
  --description "Overseer intake: structured nousergon.alert.v1 events from the krepis chokepoints (alpha-engine-config-I2822)" \
  --event-pattern '{"source":["nousergon.krepis"]}' > /dev/null
echo "rule ${ALERT_RULE_NAME}: upserted on ${BUS_NAME}"

run events put-targets --region "$REGION" \
  --event-bus-name "$BUS_NAME" \
  --rule "$ALERT_RULE_NAME" \
  --targets "Id=overseer-intake-queue,Arn=${QUEUE_ARN}" > /dev/null
echo "rule ${ALERT_RULE_NAME}: target ${QUEUE_NAME}"

# ── 4. Rule on the DEFAULT bus: CW alarm → ALARM state → queue ──────────────
run events put-rule --region "$REGION" \
  --name "$CW_ALARM_RULE_NAME" \
  --state ENABLED \
  --description "Overseer intake: every CloudWatch alarm transition to ALARM (additive tap; SNS alarm delivery unchanged) (alpha-engine-config-I2822)" \
  --event-pattern '{"source":["aws.cloudwatch"],"detail-type":["CloudWatch Alarm State Change"],"detail":{"state":{"value":["ALARM"]}}}' > /dev/null
echo "rule ${CW_ALARM_RULE_NAME}: upserted on default bus"

run events put-targets --region "$REGION" \
  --rule "$CW_ALARM_RULE_NAME" \
  --targets "Id=overseer-intake-queue,Arn=${QUEUE_ARN}" > /dev/null
echo "rule ${CW_ALARM_RULE_NAME}: target ${QUEUE_NAME}"

# ── 5. Queue policy: allow EventBridge (scoped to the two rules) ────────────
ALERT_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${BUS_NAME}/${ALERT_RULE_NAME}"
CW_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${CW_ALARM_RULE_NAME}"
POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowEventBridgeOverseerRules",
      "Effect": "Allow",
      "Principal": {"Service": "events.amazonaws.com"},
      "Action": "sqs:SendMessage",
      "Resource": "${QUEUE_ARN}",
      "Condition": {"ArnEquals": {"aws:SourceArn": ["${ALERT_RULE_ARN}", "${CW_RULE_ARN}"]}}
    }
  ]
}
JSON
)
if [[ "$DRY_RUN" == "0" ]]; then
  ESCAPED=$(python3 -c "import json,sys; print(json.dumps(sys.stdin.read()))" <<<"$POLICY")
  aws sqs set-queue-attributes --queue-url "$QUEUE_URL" --region "$REGION" \
    --attributes "{\"Policy\":${ESCAPED}}"
  echo "queue ${QUEUE_NAME}: policy upserted (EventBridge SendMessage, scoped to the 2 rules)"
else
  echo "DRY-RUN: would set queue policy on ${QUEUE_NAME}"
fi

echo "== Done. Bus=${BUS_ARN}"
echo "== Queue=${QUEUE_ARN} DLQ=${DLQ_ARN}"
echo "== Emitter IAM: run ./infrastructure/attach_overseer_put_events_policy.sh next."
