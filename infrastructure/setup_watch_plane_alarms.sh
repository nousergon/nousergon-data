#!/usr/bin/env bash
# setup_watch_plane_alarms.sh — CloudWatch Errors/Throttles alarms for the four
# watch-plane Lambdas (config#2266).
#
# Why this exists: the watch-plane Lambdas (saturday-sf-watch-dispatcher,
# sf-watch-spot-dispatcher, ci-watch-dispatcher, sf-watch-liveness-probe) are
# the components whose JOB is to notice fleet failures — and their docstrings
# asserted that their own failures "surface via the Lambda error metric + CW
# alarm". Until this script, NO such alarm existed: an unhandled dispatcher
# exception incremented an AWS/Lambda Errors metric nobody watched, so the
# fail-loud raise in _write_watch_log (the PRIMARY watch record) failed
# silently in practice. The watch's own failure mode was exactly the
# unmonitored one. This script makes the docstring claim true.
#
# INDEPENDENT alerting channel: the watch plane IS a primary failure-detection
# channel, so its own failures must page via the INDEPENDENT backstop topic
# (alpha-engine-alarm-backstop), NOT alpha-engine-alerts — the same
# independence argument as setup_pipeline_deadman_alarms.sh (config#856 infra
# item b): a blackout of the primary alert channel must not also silence the
# alarms that watch the watchers.
#
# Topic ownership (single-writer convention): setup_pipeline_deadman_alarms.sh
# is the backstop topic's SOLE provisioner (create-topic + email subscription).
# This script only CONSUMES the topic and fails fast if it is missing —
# mirrors the fail-fast-on-missing-topic convention in
# setup_substrate_alarms.sh / setup_changelog_observability_alarms.sh.
#
# Metric semantics: unlike the deadman alarms (which alarm on ABSENCE of
# activity and therefore need TreatMissingData=breaching), these alarm on
# PRESENCE of errors. AWS/Lambda emits no Errors datapoint during quiet
# periods, so missing data is the healthy steady state here —
# TreatMissingData=notBreaching is correct, and breaching would page
# continuously on every idle 5-minute window.
#
# Window: Period=300 (5 min), EvaluationPeriods=1, Threshold=1,
# GreaterThanOrEqualToThreshold, Statistic=Sum — a single error or throttle in
# any 5-minute window pages immediately. These functions are low-volume and
# failure-driven; there is no acceptable nonzero error rate.
#
# Idempotent: put-metric-alarm upserts by name. Safe to re-run.
#
# Usage:
#   ./infrastructure/setup_watch_plane_alarms.sh [--dry-run]

set -euo pipefail

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

# Deliberately NOT alpha-engine-alerts — see header comment. Provisioned by
# setup_pipeline_deadman_alarms.sh (sole owner); consumed here.
BACKSTOP_TOPIC_NAME="alpha-engine-alarm-backstop"
BACKSTOP_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${BACKSTOP_TOPIC_NAME}"

# The four watch-plane Lambdas (deployed names verified against each
# infrastructure/lambdas/<dir>/deploy.sh FUNCTION_NAME).
# sf-watch-liveness-probe now carries ONLY its reclaim/sweep action paths
# config#2270/#2257; the wiring checks moved to the registry-driven
# overseer-liveness-probe per alpha-engine-config-I2831. Both stay under this
# dead-probe backstop. (Comment kept OUT of the array literal below so the
# tests/test_watch_plane_alarms_script.py block-parser isn't confused by a `)`.)
declare -A WATCH_PLANE_FUNCTIONS=(
  ["saturday-sf-watch-dispatcher"]="alpha-engine-saturday-sf-watch-dispatcher"
  ["sf-watch-spot-dispatcher"]="alpha-engine-sf-watch-spot-dispatcher"
  ["ci-watch-dispatcher"]="alpha-engine-ci-watch-dispatcher"
  ["sf-watch-liveness-probe"]="alpha-engine-sf-watch-liveness-probe"
  ["overseer-liveness-probe"]="alpha-engine-overseer-liveness-probe"
  ["overseer-dispatcher"]="alpha-engine-overseer-dispatcher"
)

echo "Configuring watch-plane Lambda alarms"
echo "  Region:         $REGION"
echo "  Backstop topic: $BACKSTOP_TOPIC_ARN"

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# --- 1. Fail fast if the backstop topic is missing ---------------------------
# setup_pipeline_deadman_alarms.sh owns topic creation + the email
# subscription; wiring alarms to a nonexistent topic would create silently
# dangling alarms — the exact failure class this script exists to close.

echo ""
echo "==> Verifying backstop SNS topic exists..."
if ! $DRY_RUN && ! aws sns get-topic-attributes --topic-arn "$BACKSTOP_TOPIC_ARN" --region "$REGION" >/dev/null 2>&1; then
  echo "ERROR: backstop SNS topic $BACKSTOP_TOPIC_ARN not found. Run" >&2
  echo "       infrastructure/setup_pipeline_deadman_alarms.sh first (it is the" >&2
  echo "       topic's sole provisioner). Aborting before creating dangling alarms." >&2
  exit 1
fi

# --- 2. Per-Lambda Errors + Throttles alarms ---------------------------------

echo ""
echo "==> Creating per-Lambda watch-plane alarms..."
for label in "${!WATCH_PLANE_FUNCTIONS[@]}"; do
  fn_name="${WATCH_PLANE_FUNCTIONS[$label]}"

  for metric in Errors Throttles; do
    metric_lc=$(echo "$metric" | tr '[:upper:]' '[:lower:]')
    alarm_name="alpha-engine-watch-plane-${label}-${metric_lc}"

    echo "  -> $alarm_name (FunctionName=$fn_name, metric=$metric)"
    run aws cloudwatch put-metric-alarm \
      --region "$REGION" \
      --alarm-name "$alarm_name" \
      --alarm-description "Watch-plane backstop: fires when ${fn_name} records any ${metric} in a 5-minute window. This Lambda is part of the fleet's failure-detection plane — its own unhandled failure (e.g. the fail-loud raise in _write_watch_log) is otherwise exactly the unmonitored failure mode (config#2266). Routes to the INDEPENDENT ${BACKSTOP_TOPIC_NAME} topic (not alpha-engine-alerts) so a blackout of the primary alert channel cannot also silence the alarm that watches the watchers. Provisioned by infrastructure/setup_watch_plane_alarms.sh." \
      --namespace "AWS/Lambda" \
      --metric-name "$metric" \
      --dimensions "Name=FunctionName,Value=${fn_name}" \
      --statistic "Sum" \
      --period 300 \
      --evaluation-periods 1 \
      --datapoints-to-alarm 1 \
      --threshold 1 \
      --comparison-operator "GreaterThanOrEqualToThreshold" \
      --treat-missing-data "notBreaching" \
      --alarm-actions "$BACKSTOP_TOPIC_ARN" \
      --ok-actions "$BACKSTOP_TOPIC_ARN" >/dev/null
  done
done

echo ""
echo "Done — $(( ${#WATCH_PLANE_FUNCTIONS[@]} * 2 )) watch-plane alarms upserted, routed to $BACKSTOP_TOPIC_ARN."
echo ""
echo "Validation:"
echo "  aws cloudwatch describe-alarms --region $REGION \\"
echo "    --alarm-name-prefix alpha-engine-watch-plane- \\"
echo "    --query 'MetricAlarms[].[AlarmName,StateValue]' --output table"

# --- 3. Overseer intake DLQ depth (alpha-engine-config-I2823) ----------------
# A message landing on the intake DLQ means EventBridge delivered an alert
# event 5x and the queue rejected it every time — structured alert events are
# being LOST. Same backstop-topic routing rationale as the Lambda alarms.

echo ""
echo "==> Creating overseer intake DLQ depth alarm..."
run aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "alpha-engine-watch-plane-overseer-intake-dlq-depth" \
  --alarm-description "Watch-plane backstop: any message on nousergon-overseer-intake-dlq means structured alert events (Overseer phase 1, alpha-engine-config-I2822) are being dropped after redrive exhaustion. Routes to the INDEPENDENT ${BACKSTOP_TOPIC_NAME} topic. Provisioned by infrastructure/setup_watch_plane_alarms.sh." \
  --namespace "AWS/SQS" \
  --metric-name "ApproximateNumberOfMessagesVisible" \
  --dimensions "Name=QueueName,Value=nousergon-overseer-intake-dlq" \
  --statistic "Maximum" \
  --period 300 \
  --evaluation-periods 1 \
  --datapoints-to-alarm 1 \
  --threshold 1 \
  --comparison-operator "GreaterThanOrEqualToThreshold" \
  --treat-missing-data "notBreaching" \
  --alarm-actions "$BACKSTOP_TOPIC_ARN" \
  --ok-actions "$BACKSTOP_TOPIC_ARN"

echo ""
echo "Done."
