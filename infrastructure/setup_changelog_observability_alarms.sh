#!/usr/bin/env bash
# setup_changelog_observability_alarms.sh — watch-the-watchers CloudWatch
# Errors alarms for the alert/monitoring infra Lambdas.
#
# Phase B of the flow-doctor SOTA arc (config#1273). Capture completeness
# (changelog-cloudwatch-mirror TARGET_FUNCTIONS) mirrors every Lambda's
# ERROR/CRITICAL/timeout LOG LINES into the changelog event-lake. But two
# classes need a direct metric alarm on top:
#
#   1. The two changelog MIRRORS — deliberately EXCLUDED from TARGET_FUNCTIONS
#      (recursion guard: a mirror's own ERROR must not feed back into itself),
#      so their failures are invisible to the log-capture path. A CloudWatch
#      Errors alarm is the ONLY direct signal that a mirror died.
#   2. The ALERTERS (freshness-monitor, pipeline-watchdog, sentinels,
#      sf-telegram-notifier, dispatchers) — the fleet's safety-net infra. They
#      ARE log-captured now, but a metric Errors alarm is a real-time,
#      belt-and-suspenders signal that the watcher itself failed (who watches
#      the watchers).
#
# The alarm fires on AWS/Lambda Errors >= 1 in a 5-minute window and routes to
# the existing alpha-engine-alerts SNS topic. treat-missing-data=notBreaching
# keeps schedule-driven Lambdas (weekly sentinels) quiet between invocations.
#
# Idempotent — put-metric-alarm upserts by name; safe to re-run.
#
# Usage: ./infrastructure/setup_changelog_observability_alarms.sh [--dry-run]

set -euo pipefail

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"

# The alert/monitoring infra: the 2 changelog mirrors + the 6 alerters.
# (Operational producers — eod-backstop, triggers, reaper — are covered by the
# log-capture path in TARGET_FUNCTIONS and don't need a separate metric alarm.)
TARGET_FUNCTIONS=(
  "alpha-engine-changelog-cloudwatch-mirror"
  "alpha-engine-changelog-incident-mirror"
  "alpha-engine-freshness-monitor"
  "alpha-engine-friday-shell-run-report"
  "alpha-engine-pipeline-watchdog"
  "alpha-engine-saturday-integrity-sentinel"
  "alpha-engine-saturday-sf-success-groom-dispatcher"
  "alpha-engine-saturday-sf-watch-dispatcher"
  "alpha-engine-sf-telegram-notifier"
)

echo "Configuring watch-the-watchers Lambda Errors alarms"
echo "  Region:    $REGION"
echo "  SNS topic: $SNS_TOPIC_ARN"
echo "  Targets:   ${#TARGET_FUNCTIONS[@]} Lambdas"

# Fail fast if the SNS topic is missing rather than create dangling alarms.
if ! aws sns get-topic-attributes --topic-arn "$SNS_TOPIC_ARN" --region "$REGION" >/dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run deploy_step_function.sh first." >&2
  exit 1
fi

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

for fn in "${TARGET_FUNCTIONS[@]}"; do
  alarm_name="alpha-engine-lambda-errors-${fn#alpha-engine-}"
  echo "  -> $alarm_name (FunctionName=$fn)"
  run aws cloudwatch put-metric-alarm \
    --region "$REGION" \
    --alarm-name "$alarm_name" \
    --alarm-description "Watch-the-watchers: fires when ${fn} (alert/monitoring infra) reports >=1 Lambda invocation error in a 5-minute window. The two changelog mirrors are excluded from changelog-cloudwatch-mirror's TARGET_FUNCTIONS (recursion guard), so this metric alarm is their only direct failure signal; for the alerters it is a real-time belt-and-suspenders on top of log capture. treat-missing-data=notBreaching keeps schedule-driven Lambdas quiet between invocations. config#1273 Phase B." \
    --namespace "AWS/Lambda" \
    --metric-name "Errors" \
    --dimensions "Name=FunctionName,Value=${fn}" \
    --statistic "Sum" \
    --period 300 \
    --evaluation-periods 1 \
    --datapoints-to-alarm 1 \
    --threshold 1 \
    --comparison-operator "GreaterThanOrEqualToThreshold" \
    --treat-missing-data "notBreaching" \
    --alarm-actions "$SNS_TOPIC_ARN" \
    --ok-actions "$SNS_TOPIC_ARN" >/dev/null
done

echo "Done — ${#TARGET_FUNCTIONS[@]} alarms upserted."
