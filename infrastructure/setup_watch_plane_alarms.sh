#!/usr/bin/env bash
# setup_watch_plane_alarms.sh — CloudWatch Errors/Throttles alarms for the
# watch/overseer-plane Lambdas (config#2266; roster extended config-I2900).
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
# ONBOARDING CHECKLIST (config-I2900): alpha-engine-substrate-health-gate went
# 3+ days (2026-07-14 -> 2026-07-17) and alpha-engine-alert-drain-dispatcher
# went undetected even longer with ZERO alarm coverage simply because nobody
# added their names to WATCH_PLANE_FUNCTIONS below when they were deployed —
# the same class of miss, twice in a row. EVERY new watch/overseer-plane
# Lambda (anything whose job is to detect, route, or drain fleet-failure
# signals — dispatchers, probes, sentinels, drains, gates) MUST be added to
# WATCH_PLANE_FUNCTIONS in the SAME PR that deploys it. This script has no way
# to auto-discover "is this Lambda part of the failure-detection plane" —
# that judgment call has to be made at review time.
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

# The watch/overseer-plane Lambdas (deployed names verified against each
# infrastructure/lambdas/<dir>/deploy.sh FUNCTION_NAME).
# sf-watch-liveness-probe now carries ONLY its reclaim/sweep action paths
# config#2270/#2257; the wiring checks moved to the registry-driven
# overseer-liveness-probe per alpha-engine-config-I2831. Both stay under this
# dead-probe backstop. (Comment kept OUT of the array literal below so the
# tests/test_watch_plane_alarms_script.py block-parser isn't confused by a `)`.)
# alert-drain-dispatcher (executes the twice-daily overseer-intake drain) and
# substrate-health-gate (weekly-pipeline SsmDiskProbe gate) added config-I2900
# — both were Active with zero alarm coverage; see the onboarding-checklist
# comment above the header of this file.
# pipeline-watchdog, canary-replay-liveness-probe, saturday-integrity-sentinel,
# freshness-monitor, sweep-artifact-monitor added config#3240 (found during
# the same I2900 onboarding sweep, scoped out of that issue to avoid silent
# scope creep). Three of the five (pipeline-watchdog, saturday-integrity-
# sentinel, freshness-monitor) already carry a separate Errors-only alarm via
# setup_changelog_observability_alarms.sh routed to the PRIMARY alpha-engine-
# alerts topic (Phase B "watch-the-watchers", config#1273) — that alarm stays;
# it does not satisfy the independent-backstop-topic argument this script
# exists for (see header), so both alarms are intentional, not duplicative.
declare -A WATCH_PLANE_FUNCTIONS=(
  ["saturday-sf-watch-dispatcher"]="alpha-engine-saturday-sf-watch-dispatcher"
  ["sf-watch-spot-dispatcher"]="alpha-engine-sf-watch-spot-dispatcher"
  ["ci-watch-dispatcher"]="alpha-engine-ci-watch-dispatcher"
  ["sf-watch-liveness-probe"]="alpha-engine-sf-watch-liveness-probe"
  ["overseer-liveness-probe"]="alpha-engine-overseer-liveness-probe"
  ["overseer-dispatcher"]="alpha-engine-overseer-dispatcher"
  ["alert-drain-dispatcher"]="alpha-engine-alert-drain-dispatcher"
  ["substrate-health-gate"]="alpha-engine-substrate-health-gate"
  ["pipeline-watchdog"]="alpha-engine-pipeline-watchdog"
  ["canary-replay-liveness-probe"]="alpha-engine-canary-replay-liveness-probe"
  ["saturday-integrity-sentinel"]="alpha-engine-saturday-integrity-sentinel"
  ["freshness-monitor"]="alpha-engine-freshness-monitor"
  ["sweep-artifact-monitor"]="alpha-engine-sweep-artifact-monitor"
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

# --- 4. Overseer intake queue age-of-oldest-message (alpha-engine-config-I2910)
# The DLQ-depth alarm above only fires once EventBridge has delivered an
# alert event, the queue received it, and processing failed 5x (redrive
# exhaustion). It says NOTHING about the case where the twice-daily drain
# (alert-drain-dispatcher, ~12h cadence) stops running entirely — schedule
# disabled, dispatcher broken, spot launch failing (all previously-observed
# failure modes): messages are never RECEIVED, so they never fail delivery
# and never reach the DLQ. They would sit silently on the intake queue for up
# to its 14-day MessageRetentionPeriod. ApproximateAgeOfOldestMessage closes
# that detection gap.
#
# Threshold: 72000s (20h). Rationale (I2910 asks for "~18-24h, comfortably
# above the 12h drain cadence"): under healthy operation the oldest message
# is at most ~12h old right before a drain runs; if ONE drain cycle is
# missed entirely, age climbs toward ~24h before the NEXT scheduled drain
# would also miss it. 20h pages with margin before that second miss, while
# staying safely above the ~12h healthy peak so ordinary cadence jitter
# (a drain running a bit late) does not false-page.
#
# Gotcha (I2910): ApproximateAgeOfOldestMessage is computed only over
# messages that are currently VISIBLE (never successfully received+deleted)
# — a message currently leased in-flight to a consumer (i.e. mid-processing,
# within its VisibilityTimeout window) does NOT count toward this metric.
# Confirmed against the queue's own VisibilityTimeout=1800s (30 min): a
# long-but-healthy drain run holding messages in-flight cannot itself trip
# this alarm — it only reflects messages nobody has successfully picked up.
#
# Missing-data semantics: like the DLQ alarm, AWS/SQS emits no
# ApproximateAgeOfOldestMessage datapoint when the queue is EMPTY (no
# messages -> no age to report), so missing data is the healthy steady
# state — notBreaching, not breaching.

echo ""
echo "==> Creating overseer intake queue age-of-oldest-message alarm..."
run aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "alpha-engine-watch-plane-overseer-intake-age" \
  --alarm-description "Watch-plane backstop: ApproximateAgeOfOldestMessage on nousergon-overseer-intake >= 72000s (20h) means the twice-daily alert-drain-dispatcher drain (~12h cadence) has missed at least one cycle and structured alert events (Overseer phase 1, alpha-engine-config-I2822) are sitting un-received, on track for the queue's 14-day retention ceiling. Complements the DLQ-depth alarm above, which only covers received-but-failing messages, not never-received ones (alpha-engine-config-I2910). Routes to the INDEPENDENT ${BACKSTOP_TOPIC_NAME} topic. Provisioned by infrastructure/setup_watch_plane_alarms.sh." \
  --namespace "AWS/SQS" \
  --metric-name "ApproximateAgeOfOldestMessage" \
  --dimensions "Name=QueueName,Value=nousergon-overseer-intake" \
  --statistic "Maximum" \
  --period 300 \
  --evaluation-periods 1 \
  --datapoints-to-alarm 1 \
  --threshold 72000 \
  --comparison-operator "GreaterThanOrEqualToThreshold" \
  --treat-missing-data "notBreaching" \
  --alarm-actions "$BACKSTOP_TOPIC_ARN" \
  --ok-actions "$BACKSTOP_TOPIC_ARN"

echo ""
echo "Done."
