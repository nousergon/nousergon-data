#!/usr/bin/env bash
# setup_substrate_alarms.sh — One-shot CloudWatch alarm setup for the
# Phase 2 → 3 transparency-substrate health checker (alpha-engine-lib
# transparency.py + transparency_inventory.yaml).
#
# Idempotent: safe to re-run after threshold tweaks. Creates one alarm
# per inventory row plus one aggregate failure alarm. All point to the
# existing alpha-engine-alerts SNS topic.
#
# Per-row alarm:
#   alpha-engine-substrate-<row_id>
#   Fires when SubstrateRowOK metric for that row drops below 1
#   (the substrate checker emits 1 for ok/not_yet_effective, 0 for fail).
#
# Aggregate alarm:
#   alpha-engine-substrate-aggregate-failures
#   Fires when SubstrateChecksFailed > 0 in the trailing 24h window —
#   safety net in case a row alarm gets accidentally deleted.
#
# Period 3600 (1h) with EvaluationPeriods=24 + DatapointsToAlarm=1 keeps
# the trailing 24h evaluation window but bumps the refresh cadence to
# hourly. Pre-2026-05-08 the alarms used Period=86400 + EvalPeriods=1,
# which evaluated only at 24h boundaries — Sat SF emissions at 12:00 UTC
# Sat were not visible in alarm state until ~17:05 PT Sun (~37h lag).
# Hourly evaluation closes the lag to <1h. treat-missing-data=notBreaching
# keeps weekly-cadence rows quiet between emissions: a row that emits
# once per Sat SF has a datapoint in only one of the 24 hourly periods,
# the other 23 are treated as not-breaching, the alarm fires iff that one
# real datapoint is < 1. Cost: 24× more eval reads/day; bounded.
#
# Row enumeration is sourced from the lib's transparency_inventory.yaml
# so this script stays in sync with the inventory automatically — no
# hardcoded row list to drift.
#
# Usage:
#   pip install alpha-engine-lib==0.5.0  # (or activate a venv with it)
#   ./infrastructure/setup_substrate_alarms.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
NAMESPACE="AlphaEngine/Substrate"
PER_ROW_METRIC="SubstrateRowOK"
AGGREGATE_METRIC="SubstrateChecksFailed"

echo "Configuring CloudWatch alarms for transparency substrate"
echo "  Region:    $REGION"
echo "  SNS topic: $SNS_TOPIC_ARN"
echo "  Namespace: $NAMESPACE"

# Verify the SNS topic exists — fail fast rather than create alarms
# with broken targets.
if ! aws sns get-topic-attributes \
    --topic-arn "$SNS_TOPIC_ARN" \
    --region "$REGION" > /dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run deploy_step_function.sh first." >&2
  exit 1
fi

# Pull row IDs from the lib's inventory YAML — single source of truth.
# Adding a row to the YAML and re-running this script automatically
# adds the corresponding alarm. Removing a row leaves a stale alarm,
# which surfaces as INSUFFICIENT_DATA — safer than silently deleting.
ROW_IDS=$(python3 -c "
from alpha_engine_lib.transparency import load_inventory
print(' '.join(r['id'] for r in load_inventory()['inventory']))
")

if [[ -z "$ROW_IDS" ]]; then
  echo "ERROR: could not enumerate inventory rows. Is alpha-engine-lib installed?" >&2
  exit 1
fi

echo ""
echo "Creating per-row alarms for: $ROW_IDS"
echo ""

# --- Per-row alarms ---------------------------------------------------------

for row_id in $ROW_IDS; do
  alarm_name="alpha-engine-substrate-${row_id}"
  echo "==> $alarm_name"

  aws cloudwatch put-metric-alarm \
    --region "$REGION" \
    --alarm-name "$alarm_name" \
    --alarm-description "Fires when the transparency-substrate row '$row_id' fails to emit a passing measurement. The check is row-driven (alpha_engine_lib.transparency); the SF Sat pipeline runs --cadence weekly which sweeps weekly + daily rows. This alarm decrements the Phase 2 → 3 observation gate denominator for this row when it fires. Period=3600 + EvalPeriods=24 + DatapointsToAlarm=1 keeps the trailing 24h evaluation window but evaluates hourly so alarm state reflects the most recent SF emission within ~1h instead of ~24h. treat-missing-data=notBreaching keeps weekly-cadence rows quiet between emissions." \
    --comparison-operator "LessThanThreshold" \
    --evaluation-periods 24 \
    --datapoints-to-alarm 1 \
    --period 3600 \
    --statistic "Minimum" \
    --threshold 1 \
    --treat-missing-data "notBreaching" \
    --namespace "$NAMESPACE" \
    --metric-name "$PER_ROW_METRIC" \
    --dimensions "Name=RowID,Value=$row_id" \
    --alarm-actions "$SNS_TOPIC_ARN" \
    --ok-actions "$SNS_TOPIC_ARN" > /dev/null
done

# --- Aggregate failure alarm ------------------------------------------------

aggregate_name="alpha-engine-substrate-aggregate-failures"
echo "==> $aggregate_name"

aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "$aggregate_name" \
  --alarm-description "Aggregate safety-net alarm for the transparency substrate. Fires when SubstrateChecksFailed > 0 in any of the 24 trailing hourly periods. Catches the case where a per-row alarm has been accidentally deleted — the per-row alarms are authoritative for which row failed; this alarm only confirms the substrate is observing failures. Period=3600 + EvalPeriods=24 + DatapointsToAlarm=1 mirrors the per-row alarms. treat-missing-data=notBreaching means a substrate run with all rows passing emits zero failures and the alarm stays OK." \
  --comparison-operator "GreaterThanThreshold" \
  --evaluation-periods 24 \
  --datapoints-to-alarm 1 \
  --period 3600 \
  --statistic "Maximum" \
  --threshold 0 \
  --treat-missing-data "notBreaching" \
  --namespace "$NAMESPACE" \
  --metric-name "$AGGREGATE_METRIC" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN" > /dev/null

echo ""
echo "All substrate alarms configured."
echo ""
echo "Validation:"
echo "  aws cloudwatch describe-alarms --region $REGION \\"
echo "    --alarm-name-prefix alpha-engine-substrate- \\"
echo "    --query 'MetricAlarms[].[AlarmName,StateValue]' --output table"
echo ""
echo "First firing eligibility: per-row alarms remain INSUFFICIENT_DATA until the first weekly substrate run emits the metric (Sat SF) — they will not page during the gap. Rows with effective_date > today emit value=1 (counted as healthy) so they stay quiet until their grace period expires."
