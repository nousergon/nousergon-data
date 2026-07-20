#!/usr/bin/env bash
# setup_horizon_grading_alarms.sh — CloudWatch alarm setup for the
# horizon-grading freshness gate (config#2972).
#
# Background: a prior groom pass queried research.db directly, found
# predictor_outcomes.horizon_days / universe_returns.log_return_21d NULL for
# every date >= 2026-06-17, and mistook this for a silently-broken write
# path. Root-cause re-investigation found NO break — a 21-trading-day-
# forward metric is *expected* to lag "today" by up to 21 trading days
# before it can be populated at all (add_trading_days(2026-06-17, 21) ==
# 2026-07-20, which simply hadn't arrived yet). The apparent "cutoff" was
# the natural lag boundary, not a stall — but nothing distinguished the two
# cases, so a real investigation pass burned cycles on a false alarm.
#
# collectors/signal_returns.py::_emit_horizon_grading_lag_metric emits two
# gauges (AlphaEngine/Data namespace) every data-weekly run:
#   - universe_returns_horizon_grading_lag_trading_days
#   - predictor_outcomes_grading_lag_trading_days
# Both are 0 on a healthy pipeline (every date whose forward window has
# closed gets graded by the next run) and grow without bound on a genuine
# stall (the JOIN/backfill breaking). This script wires alarms on sustained
# non-zero lag — NOT on the raw NULL count, which is expected to be nonzero
# for the trailing `forward_days` window at all times.
#
# Idempotent: safe to re-run after threshold tweaks. Points at the existing
# alpha-engine-alerts SNS topic (same target as setup_substrate_alarms.sh).
#
# Cadence: universe_returns/predictor_outcomes are populated by the
# data-weekly collector (Saturday SF, ~weekly cadence — see
# collectors/universe_returns.py / collectors/signal_returns.py). One
# datapoint per week. Period=604800 (7d) + EvaluationPeriods=3 +
# DatapointsToAlarm=2 requires the lag to be sustained non-zero across at
# least 2 of the last 3 weekly runs before paging — a single week's lag > 0
# right after a new forward window closes (the expected transient) does not
# alarm; only a lag that FAILS TO RESET across consecutive runs does.
# treat-missing-data=notBreaching: a week where the collector didn't run for
# an unrelated reason (already covered by weekly_collector_manifest /
# research_db_backup freshness checks in alpha-engine-config's
# ARTIFACT_REGISTRY.yaml) doesn't independently page this alarm too.
#
# Usage:
#   pip install nousergon-lib  # (or activate a venv with it — not required
#                               # by this script itself, kept for parity with
#                               # setup_substrate_alarms.sh's doc convention)
#   ./infrastructure/setup_horizon_grading_alarms.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
NAMESPACE="AlphaEngine/Data"

echo "Configuring CloudWatch alarms for horizon-grading freshness (config#2972)"
echo "  Region:    $REGION"
echo "  SNS topic: $SNS_TOPIC_ARN"
echo "  Namespace: $NAMESPACE"

# Verify the SNS topic exists — fail fast rather than create alarms with
# broken targets.
if ! aws sns get-topic-attributes \
    --topic-arn "$SNS_TOPIC_ARN" \
    --region "$REGION" > /dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run infrastructure/deploy_step_function.sh first." >&2
  exit 1
fi

# --- universe_returns 21d-horizon grading lag --------------------------------

echo ""
echo "==> alpha-engine-universe-returns-horizon-lag"

aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "alpha-engine-universe-returns-horizon-lag" \
  --alarm-description "Fires when universe_returns_horizon_grading_lag_trading_days (AlphaEngine/Data, HorizonDays=21 dimension) stays > 0 across 2 of the last 3 weekly data-weekly runs. Lag=0 on a healthy pipeline: every eval_date whose 21-trading-day forward window has closed gets return_21d/log_return_21d populated by the next run. Sustained lag means the collectors/universe_returns.py backfill (_get_existing_dates / _trading_days_to_process) has genuinely stalled — NOT that recent dates are still waiting on their forward window to close (that's the expected, non-alarming transient). config#2972." \
  --comparison-operator "GreaterThanThreshold" \
  --evaluation-periods 3 \
  --datapoints-to-alarm 2 \
  --period 604800 \
  --statistic "Maximum" \
  --threshold 0 \
  --treat-missing-data "notBreaching" \
  --namespace "$NAMESPACE" \
  --metric-name "universe_returns_horizon_grading_lag_trading_days" \
  --dimensions "Name=HorizonDays,Value=21" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN" > /dev/null

# --- predictor_outcomes grading lag -------------------------------------------

echo "==> alpha-engine-predictor-outcomes-grading-lag"

aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "alpha-engine-predictor-outcomes-grading-lag" \
  --alarm-description "Fires when predictor_outcomes_grading_lag_trading_days (AlphaEngine/Data, HorizonDays=21 dimension) stays > 0 across 2 of the last 3 weekly data-weekly runs. Lag=0 on a healthy pipeline: every prediction_date whose forward_days window has closed gets horizon_days/correct/actual_log_alpha populated by collectors/signal_returns.py::_backfill_predictor_returns on the next run. Sustained lag means the universe_returns JOIN this backfill depends on has stopped finding matches for closed-window predictions — NOT that recent predictions are still waiting on grading (that's the expected, non-alarming transient). config#2972." \
  --comparison-operator "GreaterThanThreshold" \
  --evaluation-periods 3 \
  --datapoints-to-alarm 2 \
  --period 604800 \
  --statistic "Maximum" \
  --threshold 0 \
  --treat-missing-data "notBreaching" \
  --namespace "$NAMESPACE" \
  --metric-name "predictor_outcomes_grading_lag_trading_days" \
  --dimensions "Name=HorizonDays,Value=21" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN" > /dev/null

echo ""
echo "Horizon-grading alarms configured."
echo ""
echo "Validation:"
echo "  aws cloudwatch describe-alarms --region $REGION \\"
echo "    --alarm-names alpha-engine-universe-returns-horizon-lag alpha-engine-predictor-outcomes-grading-lag \\"
echo "    --query 'MetricAlarms[].[AlarmName,StateValue]' --output table"
echo ""
echo "First firing eligibility: both alarms remain INSUFFICIENT_DATA until 2 of the last 3 weekly data-weekly runs have emitted the metric (~2-3 weeks after this script + the signal_returns.py change deploy). treat-missing-data=notBreaching means a skipped run doesn't independently page — the existing weekly_collector_manifest / research_db_backup freshness checks (alpha-engine-config ARTIFACT_REGISTRY.yaml) already cover run-absence."
