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
# Gap identified in alpha-engine-config-I2908: every CW alarm + every
# krepis-published alert now hits the bus, but an entire class of alerts
# never touches either krepis chokepoint — native Step-Functions sns:publish
# ASL states + raw boto3 sns.publish() call sites. Section 6 below bridges
# that gap via an SNS-to-EventBridge forwarder Lambda subscribed to the
# ``alpha-engine-alerts`` topic.
#
# Deliberately NOT here (epic invariant 3, alpha-engine-config-I2821): the
# alpha-engine-alarm-backstop SNS topic and its CW alarms are untouched — the
# last-resort backstop must never route THROUGH the bus/queue machinery it
# watches. The default-bus rule above is an ADDITIVE tap on alarm state
# changes; SNS delivery of every alarm is unchanged.
#
# Evaluation of other topics per alpha-engine-config-I2908:
# - alpha-engine-alarm-backstop: NOT forwarded — the backstop is independent
#   by design, and routing it through the bus it watches would violate epic
#   I2821 invariant 3. CW alarm state-changes already reach the bus via the
#   default-bus rule above. (Decision: leave UNforwarded.)
# - alpha-engine-watchdog-alerts: FORWARDED via the same alpha-engine-alerts
#   forwarder Lambda IF its messages arrive on the alpha-engine-alerts topic;
#   if it uses a separate SNS topic as its notification surface, it would
#   need its own subscription. Investigation (alpha-engine-config-I2908):
#   watchdog-alerts publishes via krepis (which already hits the bus) for
#   structured alerts and SNS for human-only paging (Telegram/SNS). The
#   structured path already reaches the intake queue via the krepis rule
#   above; the SNS-only path is human-facing fire-and-forget that belongs
#   outside the drain scope. Decision: no separate forwarder needed.
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
# VisibilityTimeout (alpha-engine-config-I2904): the SQS default is 30s.
# The charter's per-incident workflow (diagnose -> PR -> ledger -> THEN
# delete) takes minutes by design (2026-07-17 first live drain: real
# incidents ran multi-minute). At 30s, every real incident's message
# re-became visible mid-processing, got re-pulled under a NEW receipt handle
# (invalidating the one STEP 4 held), survived to MAX_RECEIVE_COUNT=5, and
# landed in the DLQ misreported as a "dropped alert" it never was.
# 1800s (30 min) is chosen as the p99 per-incident processing ceiling
# (diagnose + open PR + write ledger record for one incident, observed
# well under this in the 2026-07-17 drain) with headroom. It interacts with
# MAX_RECEIVE_COUNT=5 (redrive to the DLQ) as follows: with the deterministic
# ingest wrapper (scripts/alert_drain_ingest.py, alpha-engine-config)
# `ChangeMessageVisibility`-heartbeating an incident that runs long, the
# receive count only climbs on a GENUINE crash-loop / redelivery (the box
# dying mid-incident before a heartbeat or the ledger write), never on a
# false timeout expiry — so DLQ arrival keeps meaning "this incident
# actually failed repeatedly," not "the timeout was too short." Applied
# LIVE 2026-07-17 ~22:30 UTC (operator-authorized, ahead of this script
# update); this codifies that live change so a future re-run of this
# idempotent script doesn't drift back to the 30s default.
VISIBILITY_TIMEOUT_SECONDS=1800

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
ensure_queue "$QUEUE_NAME" "{\"MessageRetentionPeriod\":\"${RETENTION_SECONDS}\",\"RedrivePolicy\":\"${REDRIVE_POLICY}\",\"VisibilityTimeout\":\"${VISIBILITY_TIMEOUT_SECONDS}\"}"
QUEUE_URL=$(aws sqs get-queue-url --queue-name "$QUEUE_NAME" --region "$REGION" --query QueueUrl --output text 2>/dev/null || echo "")
QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${QUEUE_NAME}"

# Upsert retention + redrive + visibility-timeout on pre-existing queues too
# (idempotent re-runs after parameter changes — this is also what re-applies
# VISIBILITY_TIMEOUT_SECONDS if it's ever hand-changed live and needs
# reconciling back to the SSoT here).
if [[ "$DRY_RUN" == "0" && -n "$QUEUE_URL" ]]; then
  aws sqs set-queue-attributes --queue-url "$QUEUE_URL" --region "$REGION" \
    --attributes "{\"MessageRetentionPeriod\":\"${RETENTION_SECONDS}\",\"RedrivePolicy\":\"${REDRIVE_POLICY}\",\"VisibilityTimeout\":\"${VISIBILITY_TIMEOUT_SECONDS}\"}"
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

# ── 6. SNS forwarder: alpha-engine-alerts → nousergon-alerts bus ──────────
# (alpha-engine-config-I2908) — bridges the gap where native Step-Functions
# sns:publish ASL states and raw boto3 sns.publish() call sites bypass both
# krepis chokepoints and never reach the overseer bus. The forwarder Lambda
# subscribes to alpha-engine-alerts SNS and calls PutEvents on the
# nousergon-alerts custom bus with source attribution.
FORWARDER_FN="alpha-engine-alerts-forwarder"
FORWARDER_ROLE="alpha-engine-alerts-forwarder"
FORWARDER_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FORWARDER_FN}"
FORWARDER_PATH="infrastructure/lambdas/alpha-engine-alerts-forwarder"
ALERTS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"

# Create the Lambda function if it doesn't exist yet (bootstrap path for
# first-time setup, before deploy.sh is ever run).
if ! aws lambda get-function --function-name "$FORWARDER_FN" --region "$REGION" >/dev/null 2>&1; then
  echo "Lambda ${FORWARDER_FN}: not found — creating bootstrap function"

  # Create execution role if missing
  if ! aws iam get-role --role-name "$FORWARDER_ROLE" >/dev/null 2>&1; then
    echo "  Creating IAM role: ${FORWARDER_ROLE}"
    run aws iam create-role \
      --role-name "$FORWARDER_ROLE" \
      --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    run aws iam attach-role-policy \
      --role-name "$FORWARDER_ROLE" \
      --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    run aws iam put-role-policy \
      --role-name "$FORWARDER_ROLE" \
      --policy-name "alpha-engine-alerts-forwarder-events" \
      --policy-document "file://${FORWARDER_PATH}/iam-policy.json"
    echo "  Waiting 10s for IAM propagation..."
    sleep 10
  fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${FORWARDER_ROLE}"
  # Create a minimal placeholder function. Deploy the actual code via deploy.sh.
  if [[ "$DRY_RUN" == "0" ]]; then
    cd "$(dirname "$0")"
    TMP_DIR=$(mktemp -d)
    cat > "$TMP_DIR/placeholder.py" << 'PYEOF'
"""Placeholder — deploy the real handler via deploy.sh."""
def handler(event, context):
    return {"status": "placeholder"}
PYEOF
    (cd "$TMP_DIR" && zip -q /tmp/forwarder-placeholder.zip placeholder.py; mv /tmp/forwarder-placeholder.zip "$TMP_DIR/placeholder.zip")
    run aws lambda create-function \
      --function-name "$FORWARDER_FN" \
      --runtime python3.12 \
      --architectures arm64 \
      --handler placeholder.handler \
      --role "$ROLE_ARN" \
      --timeout 30 --memory-size 256 \
      --zip-file "fileb://${TMP_DIR}/placeholder.zip" \
      --region "$REGION" > /dev/null
    rm -rf "$TMP_DIR"
    echo "  Lambda ${FORWARDER_FN}: created (placeholder — deploy the real handler via deploy.sh)"
    rm -f /tmp/forwarder-placeholder.zip
  else
    echo "  (--dry-run) would create Lambda ${FORWARDER_FN}"
  fi
else
  echo "Lambda ${FORWARDER_FN}: exists"
fi

# Subscribe forwarder Lambda to alpha-engine-alerts SNS topic (idempotent).
EXISTING_SUB=$(aws sns list-subscriptions-by-topic \
  --topic-arn "$ALERTS_TOPIC_ARN" --region "$REGION" \
  --query "Subscriptions[?Endpoint=='${FORWARDER_ARN}'].SubscriptionArn | [0]" \
  --output text 2>/dev/null || echo "None")
if [[ "$EXISTING_SUB" == "None" ]] || [[ -z "$EXISTING_SUB" ]]; then
  run aws sns subscribe \
    --topic-arn "$ALERTS_TOPIC_ARN" \
    --protocol lambda \
    --notification-endpoint "$FORWARDER_ARN" \
    --region "$REGION" > /dev/null
  echo "SNS subscription: alpha-engine-alerts → ${FORWARDER_FN} (created)"
else
  echo "SNS subscription: alpha-engine-alerts → ${FORWARDER_FN} (exists)"
fi

# Allow SNS to invoke the forwarder Lambda (idempotent).
if ! aws lambda get-policy --function-name "$FORWARDER_FN" --region "$REGION" \
      --query "Policy" --output text 2>/dev/null \
      | grep -q "sns-alerts-forwarder-invoke"; then
  run aws lambda add-permission \
    --function-name "$FORWARDER_FN" \
    --statement-id sns-alerts-forwarder-invoke \
    --action lambda:InvokeFunction \
    --principal sns.amazonaws.com \
    --source-arn "$ALERTS_TOPIC_ARN" \
    --region "$REGION" > /dev/null || true  # may exist from earlier run despite idempotency check
  echo "Lambda permission: sns.amazonaws.com → ${FORWARDER_FN} (added)"
else
  echo "Lambda permission: sns.amazonaws.com → ${FORWARDER_FN} (exists)"
fi

echo "== Forwarder ${FORWARDER_FN} ready."
echo "== Deploy actual code: bash ${FORWARDER_PATH}/deploy.sh"
