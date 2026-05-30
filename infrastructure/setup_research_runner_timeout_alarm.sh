#!/usr/bin/env bash
# setup_research_runner_timeout_alarm.sh — One-shot CloudWatch alarm for the
# alpha-engine-research-runner Lambda approaching its 900s timeout (L4464).
#
# Why this exists: the 2026-05-30 Research run hit States.Timeout at the 900s
# Lambda hard ceiling and was SIGKILL'd before writing signals.json. A hard
# Lambda timeout runs NO in-process code, so it cannot self-alert; and it does
# NOT increment the Lambda Errors metric, so the existing
# alpha-engine-research-runner-errors alarm does not catch it. The operator
# only saw a generic SF PipelineFailure. This alarm names the timeout cause.
#
# Mechanism: Lambda emits a Duration datapoint (~900000 ms) even for a
# timed-out invocation (the billed duration). We alarm on Duration Maximum
# >= 870000 ms (30s below the ceiling) so it fires on a timeout AND on a
# near-miss overrun — an early warning that the run is creeping toward the
# budget even before it fails. The L1995 Phase 5 universe reduction
# (research #256) should keep real runs at ~10 min; this is the regression
# backstop, not the fix.
#
# Idempotent: safe to re-run. Notification target reuses alpha-engine-alerts
# (the pipeline-failure inbox), mirroring setup_eval_quality_alarm.sh.
#
# Usage: ./infrastructure/setup_research_runner_timeout_alarm.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
ALARM_NAME="alpha-engine-research-runner-timeout"
FUNCTION_NAME="alpha-engine-research-runner"
THRESHOLD="870000"   # ms — 30s below the 900s Lambda ceiling

echo "Configuring CloudWatch alarm: $ALARM_NAME"
echo "  Region:     $REGION"
echo "  SNS topic:  $SNS_TOPIC_ARN"
echo "  Threshold:  Duration Maximum >= ${THRESHOLD} ms (function $FUNCTION_NAME)"

# Verify the SNS topic exists — fail fast rather than create an alarm with a
# broken target.
if ! aws sns get-topic-attributes \
    --topic-arn "$SNS_TOPIC_ARN" \
    --region "$REGION" > /dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run deploy_step_function.sh first." >&2
  exit 1
fi

# Period 86400 (24h) Maximum with EvaluationPeriods=1: Research runs weekly
# (Saturday), so a 24h window contains at most one run; its Duration Maximum
# is evaluated directly. treat-missing-data=notBreaching keeps the alarm
# quiet on the ~6 days/week with no invocation.
aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "$ALARM_NAME" \
  --alarm-description "Fires when the alpha-engine-research-runner Lambda Duration approaches its 900s ceiling (>= ${THRESHOLD} ms) — a timeout or near-miss overrun. A hard Lambda timeout runs no in-process code and does NOT hit the Errors metric, so this is the only timeout-specific signal. Backstop for the L4464 / L1995-Phase-5 regression class (signals.json went stale 8 days when this fired silently). Names the cause; does not gate deploy." \
  --comparison-operator "GreaterThanOrEqualToThreshold" \
  --evaluation-periods 1 \
  --period 86400 \
  --statistic Maximum \
  --threshold "$THRESHOLD" \
  --treat-missing-data "notBreaching" \
  --namespace "AWS/Lambda" \
  --metric-name "Duration" \
  --dimensions "Name=FunctionName,Value=${FUNCTION_NAME}" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN"

echo ""
echo "Alarm $ALARM_NAME configured."
echo "Validation: aws cloudwatch describe-alarms --alarm-names $ALARM_NAME --region $REGION --query 'MetricAlarms[0].StateValue' --output text"
