#!/usr/bin/env bash
# setup_pipeline_deadman_alarms.sh — CloudWatch "dead man's switch" alarms for
# the three Alpha Engine orchestration Step Functions (config#856 Pipeline
# Reporting Revamp, infra scope item (b)).
#
# Why this exists: the existing per-state HandleFailure alerts (NotifyComplete
# / HandleFailure in step_function.json / step_function_daily.json /
# step_function_eod.json) only fire when a state machine's EXECUTION runs and
# fails. They cannot detect the strictly worse case — the state machine never
# starts at all (EventBridge rule silently disabled/deleted, a CFN stack
# rollback that drops the rule, an IAM permission regression on the
# EventBridge→SFN invoke role, etc.). AWS/States ExecutionsStarted is the
# metric that names that gap: alarm when it drops to zero over a window sized
# to the pipeline's cadence.
#
# INDEPENDENT alerting channel (the point of this script): every existing
# alarm in this repo (see setup_substrate_alarms.sh,
# setup_changelog_observability_alarms.sh, setup_research_runner_timeout_alarm.sh,
# and the CloudFormation-managed alarms in cloudformation/alpha-engine-orchestration.yaml)
# routes to the SAME alpha-engine-alerts SNS topic. That is correct for
# failure-of-a-running-pipeline alerts, but it creates a single point of
# failure for a "did the pipeline even start" watchdog: if alpha-engine-alerts
# itself goes dark (email subscription silently unconfirmed/removed, topic
# policy drift, accidental deletion, ...) then EVERY alarm routed through it
# goes dark at once, INCLUDING the one alarm whose entire job is to catch
# silent breakage. This script provisions + subscribes a SEPARATE SNS topic
# (alpha-engine-alarm-backstop) purely for these three deadman alarms, so a
# blackout of the primary channel cannot also silence the backstop.
#
# Metric semantics: AWS/States does not emit an explicit ExecutionsStarted=0
# datapoint during a quiet period — it emits NO datapoint at all when a state
# machine has zero executions. So "zero executions" surfaces as MISSING DATA,
# not as a real zero-valued point. --treat-missing-data breaching is required
# (not notBreaching) for this class of alarm to ever fire — mirrors the
# existing alpha-engine-research-runner-no-invocations /
# alpha-engine-backtester-no-heartbeat / alpha-engine-evaluator-no-heartbeat /
# alpha-engine-rag-ingestion-no-heartbeat alarms in
# cloudformation/alpha-engine-orchestration.yaml, all of which use
# TreatMissingData: breaching for the same "must run on this cadence" shape.
#
# Window: Period=604800 (7 days), EvaluationPeriods=1, Threshold=1,
# LessThanThreshold. A 7-day trailing window is used for ALL THREE state
# machines — including the two weekday-cadence pipelines (preopen-trading,
# postclose-trading) — rather than a 1-day window, because AWS/States (like
# AWS/Lambda) reports no datapoint on a quiet day, so a 1-day window cannot
# distinguish "expected weekend/holiday silence" from "pipeline silently
# stopped starting on a weekday" — both look identical (no data) to
# CloudWatch. A 7-day window sidesteps that ambiguity: any calendar week with
# zero executions across a state machine that is supposed to run 1x (weekly)
# or ~5x (weekday cadence) times is unambiguously broken.
#
# Idempotent: put-metric-alarm upserts by name; sns create-topic / subscribe
# dedupe by name/endpoint. Safe to re-run after threshold tweaks.
#
# Usage:
#   ./infrastructure/setup_pipeline_deadman_alarms.sh [--dry-run]
#   BACKSTOP_ALERT_EMAIL=you@example.com ./infrastructure/setup_pipeline_deadman_alarms.sh

set -euo pipefail

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

# Deliberately NOT alpha-engine-alerts — see header comment.
BACKSTOP_TOPIC_NAME="alpha-engine-alarm-backstop"
BACKSTOP_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${BACKSTOP_TOPIC_NAME}"
# Same default recipient as the CFN AlertEmail parameter is fine — the
# independence property this script provides is at the SNS-topic/subscription
# infrastructure layer (a break in one topic's plumbing cannot silence the
# other), not necessarily a different human inbox. Override via env var if a
# distinct on-call address is preferred.
BACKSTOP_ALERT_EMAIL="${BACKSTOP_ALERT_EMAIL:-cipher813@gmail.com}"

# The backstop Telegram forwarder Lambda — deploys together with the forwarder
# at infrastructure/lambdas/backstop-telegram-notifier/deploy.sh. MUST be
# bootstrapped before this script's Telegram subscription can succeed.
FORWARDER_FUNCTION_NAME="alpha-engine-backstop-telegram-notifier"
FORWARDER_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FORWARDER_FUNCTION_NAME}"

# The three canonical Alpha Engine orchestration state machines (per the
# pipeline-reporting-revamp scope / crucible-dashboard views/25_Pipeline_Status.py
# _SF_ORDER). alpha-engine-groom-pipeline is intentionally excluded — it is
# not part of the reporting-revamp's operator-facing pipeline set.
declare -A STATE_MACHINES=(
  ["weekly-freshness"]="ne-weekly-freshness-pipeline"
  ["preopen-trading"]="ne-preopen-trading-pipeline"
  ["postclose-trading"]="ne-postclose-trading-pipeline"
)

echo "Configuring pipeline deadman alarms"
echo "  Region:         $REGION"
echo "  Backstop topic: $BACKSTOP_TOPIC_ARN"
echo "  Backstop email: $BACKSTOP_ALERT_EMAIL"

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# --- 1. Provision the independent backstop SNS topic + subscription --------
# Deliberately create/subscribe here rather than assume-and-fail-fast like
# the other setup_*_alarms.sh scripts: this topic is NEW and has no other
# owning deploy script, so this script is its sole provisioner (mirrors how
# update_eod_pipeline_sf.sh is the EOD state machine's sole manager).

echo ""
echo "==> Ensuring backstop SNS topic exists..."
if $DRY_RUN; then
  echo "DRY: aws sns create-topic --name $BACKSTOP_TOPIC_NAME"
else
  aws sns create-topic --name "$BACKSTOP_TOPIC_NAME" --region "$REGION" --query "TopicArn" --output text >/dev/null
fi

EXISTING_SUBS=""
if ! $DRY_RUN; then
  EXISTING_SUBS=$(aws sns list-subscriptions-by-topic \
    --topic-arn "$BACKSTOP_TOPIC_ARN" \
    --query "Subscriptions[?Protocol=='email' && Endpoint=='${BACKSTOP_ALERT_EMAIL}'].Endpoint" \
    --output text --region "$REGION" 2>/dev/null || echo "")
fi
if [[ -z "$EXISTING_SUBS" ]]; then
  echo "  Subscribing $BACKSTOP_ALERT_EMAIL (requires manual email confirmation)..."
  run aws sns subscribe \
    --region "$REGION" \
    --topic-arn "$BACKSTOP_TOPIC_ARN" \
    --protocol email \
    --notification-endpoint "$BACKSTOP_ALERT_EMAIL" >/dev/null
else
  echo "  Subscription for $BACKSTOP_ALERT_EMAIL already present."
fi

# --- 1b. Backstop Telegram forwarder subscription (I2899) -------------------
# The email-only backstop was proven blind on 2026-07-17 (an ALARM fired at
# 20:52 UTC, the email arrived and sat unread; Brian only learned of the
# incident from sf-telegram-notifier's WARNING about the underlying SF failure
# — the watch plane was down for ~40 min with zero real-time page).
#
# This adds an INDEPENDENT Telegram-forwarder Lambda subscribed directly to the
# topic — raw urllib, token from SSM at invoke, no krepis/nousergon_lib
# imports, no DynamoDB dedup, no EventBridge bus involvement (I2899 invariant
# 3: the backstop must never involve an agent, a queue, or anything that can
# fail non-obviously). The email subscription above is kept as the primary
# backstop; this is a redundant fast channel.
#
# First-time bootstrap: run
#   bash infrastructure/lambdas/backstop-telegram-notifier/deploy.sh --bootstrap
# BEFORE this script so the Lambda exists to subscribe.
#
# Single-writer notice: the FORWARDER_FUNCTION_NAME and its SNS permission +
# subscription are managed HERE (the topic's sole owner), not by
# setup_watch_plane_alarms.sh or any other script.

if aws lambda get-function --function-name "${FORWARDER_FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  # Give SNS permission to invoke the Lambda (idempotent — 2>/dev/null||true
  # on existing statements).
  if ! $DRY_RUN; then
    aws lambda add-permission \
      --function-name "${FORWARDER_FUNCTION_NAME}" \
      --statement-id "sns-${BACKSTOP_TOPIC_NAME}" \
      --action lambda:InvokeFunction \
      --principal sns.amazonaws.com \
      --source-arn "${BACKSTOP_TOPIC_ARN}" \
      --region "${REGION}" 2>/dev/null || true
  fi

  EXISTING_LAMBDA_SUB=""
  if ! $DRY_RUN; then
    EXISTING_LAMBDA_SUB=$(aws sns list-subscriptions-by-topic \
      --topic-arn "${BACKSTOP_TOPIC_ARN}" \
      --query "Subscriptions[?Protocol=='lambda' && Endpoint=='${FORWARDER_ARN}'].SubscriptionArn" \
      --output text --region "${REGION}" 2>/dev/null || echo "")
  fi
  if [[ -z "$EXISTING_LAMBDA_SUB" || "$EXISTING_LAMBDA_SUB" == "None" ]]; then
    echo "  Subscribing ${FORWARDER_FUNCTION_NAME} to ${BACKSTOP_TOPIC_NAME}..."
    run aws sns subscribe \
      --region "${REGION}" \
      --topic-arn "${BACKSTOP_TOPIC_ARN}" \
      --protocol lambda \
      --notification-endpoint "${FORWARDER_ARN}" \
      --query 'SubscriptionArn' --output text
  else
    echo "  Telegram forwarder subscription already exists: ${EXISTING_LAMBDA_SUB}"
  fi
else
  echo "  WARNING: ${FORWARDER_FUNCTION_NAME} Lambda does not exist — skipping"
  echo "  Telegram subscription. Run the following to deploy it:"
  echo "    bash infrastructure/lambdas/backstop-telegram-notifier/deploy.sh --bootstrap"
  echo "  Then re-run this script to wire the subscription."
fi

# Fail fast if the topic isn't actually there before wiring alarms to it
# (mirrors the fail-fast-on-missing-topic convention in
# setup_substrate_alarms.sh / setup_changelog_observability_alarms.sh).
if ! $DRY_RUN && ! aws sns get-topic-attributes --topic-arn "$BACKSTOP_TOPIC_ARN" --region "$REGION" >/dev/null 2>&1; then
  echo "ERROR: backstop SNS topic $BACKSTOP_TOPIC_ARN not found after create-topic. Aborting before creating dangling alarms." >&2
  exit 1
fi

# --- 2. Per-state-machine ExecutionsStarted=0 deadman alarms ----------------

echo ""
echo "==> Creating per-state-machine deadman alarms..."
for label in "${!STATE_MACHINES[@]}"; do
  sf_name="${STATE_MACHINES[$label]}"
  sf_arn="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${sf_name}"
  alarm_name="alpha-engine-pipeline-deadman-${label}"

  echo "  -> $alarm_name (StateMachineArn=$sf_arn)"
  run aws cloudwatch put-metric-alarm \
    --region "$REGION" \
    --alarm-name "$alarm_name" \
    --alarm-description "Dead man's switch: fires when ${sf_name} has zero ExecutionsStarted in the trailing 7 days — the state machine did not even start (EventBridge rule disabled/deleted, IAM regression on the invoke role, CFN drift, ...), a failure class the per-execution HandleFailure SNS alert cannot see because it only runs when an execution exists. Routes to the INDEPENDENT ${BACKSTOP_TOPIC_NAME} topic (not alpha-engine-alerts) so a blackout of the primary alert channel cannot also silence this backstop (config#856 infra item b). TreatMissingData=breaching is required: AWS/States emits no datapoint at all for a quiet state machine, so 'zero executions' IS missing data, not a real zero — notBreaching would make this alarm impossible to fire." \
    --namespace "AWS/States" \
    --metric-name "ExecutionsStarted" \
    --dimensions "Name=StateMachineArn,Value=${sf_arn}" \
    --statistic "Sum" \
    --period 604800 \
    --evaluation-periods 1 \
    --datapoints-to-alarm 1 \
    --threshold 1 \
    --comparison-operator "LessThanThreshold" \
    --treat-missing-data "breaching" \
    --alarm-actions "$BACKSTOP_TOPIC_ARN" \
    --ok-actions "$BACKSTOP_TOPIC_ARN" >/dev/null
done

echo ""
echo "Done — ${#STATE_MACHINES[@]} deadman alarms upserted, routed to $BACKSTOP_TOPIC_ARN."
echo ""
echo "Validation:"
echo "  aws cloudwatch describe-alarms --region $REGION \\"
echo "    --alarm-name-prefix alpha-engine-pipeline-deadman- \\"
echo "    --query 'MetricAlarms[].[AlarmName,StateValue]' --output table"
echo ""
echo "NOTE: the email subscription above is pending until confirmed via the"
echo "confirmation link SNS sends to $BACKSTOP_ALERT_EMAIL — the backstop"
echo "channel is not actually live until that confirmation completes."
