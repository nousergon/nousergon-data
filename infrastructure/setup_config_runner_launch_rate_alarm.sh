#!/usr/bin/env bash
# setup_config_runner_launch_rate_alarm.sh — CloudWatch alarm on the
# config-runner-dispatcher's spot-launch RATE (alpha-engine-config#2697).
#
# Why this exists (2026-07-15 incident): with every runner box failing
# identically (a deprecated runner version), _reconcile() in
# infrastructure/lambdas/config-runner-dispatcher/index.py relaunched a fresh
# box per stuck queued job on every ~60s pass, FOREVER — ~150 t3.medium spot
# launches in 45 min, ~16 concurrent boxes = 32 vCPUs = 100% of the account's
# standard-spot quota (L-34B43A08, value 32). The post-close trading SF's
# data-spot launch then failed MaxSpotInstanceCountExceeded at 20:00 UTC. The
# runaway ran for ~3 HOURS with only a single quota-exhaustion page at the
# very end — nothing alerted on the RATE itself while it was building.
#
# index.py now also has a per-job attempt limit + a global fleet cap (same
# PR) that BOUND the damage a future runaway of this shape can do. This alarm
# is the complementary EARLY-WARNING signal: it fires on the launch rate
# itself, independent of whether the circuit breaker/fleet cap have already
# capped the damage — a human should still be told a runaway pattern is
# happening even though it can no longer exhaust the account's quota.
#
# Metric: AlphaEngine/Infra config_runner_launches (Count), emitted by
# index.py's _emit_launch_metric() on every successful spot launch (no
# dimensions — one Lambda, one workload). This repo has no log-metric-filter
# precedent; every existing alarm (setup_watch_plane_alarms.sh,
# spot-orphan-reaper's own spot_orphans_terminated metric) alarms on a
# Lambda-emitted custom/AWS metric instead — this follows the same
# convention rather than introducing a new mechanism for one alarm.
#
# Window: Period=900 (15 min), EvaluationPeriods=1, Threshold=10 launches,
# GreaterThanOrEqualToThreshold, Statistic=Sum. Sizing: normal steady-state
# dispatch is at most a handful of launches per 15 min (one webhook-triggered
# launch per queued CI job, occasional reconcile-backstop recoveries); the
# incident's runaway rate was ~50/15min. 10 is comfortably above legitimate
# burst traffic (e.g. a batch of PRs landing at once) and comfortably below
# the runaway rate, so it fires early without false-paging on ordinary CI
# bursts. TreatMissingData=notBreaching — AWS emits no datapoint when there
# are zero launches in a window, which is the healthy common case between
# CI bursts.
#
# Idempotent: put-metric-alarm upserts by name. Safe to re-run.
#
# Usage:
#   ./infrastructure/setup_config_runner_launch_rate_alarm.sh [--dry-run]

set -euo pipefail

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"

ALARM_NAME="alpha-engine-config-runner-launch-rate"
NAMESPACE="AlphaEngine/Infra"
METRIC_NAME="config_runner_launches"
THRESHOLD="10"
PERIOD_SECONDS="900"

echo "Configuring CloudWatch alarm: $ALARM_NAME"
echo "  Region:     $REGION"
echo "  SNS topic:  $SNS_TOPIC_ARN"
echo "  Metric:     ${NAMESPACE}/${METRIC_NAME}"
echo "  Threshold:  Sum >= ${THRESHOLD} launches per ${PERIOD_SECONDS}s"

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# Fail fast if the SNS topic is missing rather than create a dangling alarm
# (mirrors setup_research_runner_timeout_alarm.sh / setup_watch_plane_alarms.sh).
if ! $DRY_RUN && ! aws sns get-topic-attributes --topic-arn "$SNS_TOPIC_ARN" --region "$REGION" >/dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run deploy_step_function.sh first." >&2
  exit 1
fi

run aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "$ALARM_NAME" \
  --alarm-description "Fires when alpha-engine-config-runner-dispatcher launches >= ${THRESHOLD} config-runner spot boxes in a ${PERIOD_SECONDS}s window — the 2026-07-15 spot-quota-starvation runaway signature (~150 launches/45min, only a single quota page at the very end). Early-warning signal, independent of the per-job attempt limit + global fleet cap (same PR, alpha-engine-config#2697) that bound the actual damage. Provisioned by infrastructure/setup_config_runner_launch_rate_alarm.sh." \
  --namespace "$NAMESPACE" \
  --metric-name "$METRIC_NAME" \
  --statistic "Sum" \
  --period "$PERIOD_SECONDS" \
  --evaluation-periods 1 \
  --datapoints-to-alarm 1 \
  --threshold "$THRESHOLD" \
  --comparison-operator "GreaterThanOrEqualToThreshold" \
  --treat-missing-data "notBreaching" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN"

echo ""
echo "Alarm $ALARM_NAME configured."
echo "Validation: aws cloudwatch describe-alarms --alarm-names $ALARM_NAME --region $REGION --query 'MetricAlarms[0].StateValue' --output text"
