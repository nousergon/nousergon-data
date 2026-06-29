#!/usr/bin/env bash
# deploy_step_function_daily.sh — Create/update the weekday pipeline.
#
# Orchestrates: Daily Data → Predictor Inference → EC2 Start (executor)
# Triggered Mon-Fri at 13:05 UTC (6:05 AM PT).
#
# Prerequisites:
#   - Saturday pipeline already deployed (IAM roles, SNS topic exist)
#   - Predictor Lambda (alpha-engine-predictor-inference) deployed
#   - SSM agent on micro instance
#
# Usage:
#   ./infrastructure/deploy_step_function_daily.sh
#   ./infrastructure/deploy_step_function_daily.sh --disable-old-rules

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STATE_MACHINE_NAME="ne-preopen-trading-pipeline"
ROLE_NAME="alpha-engine-step-functions-role"  # reuse from Saturday pipeline
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
EVENTBRIDGE_RULE="alpha-engine-weekday"

# EC2 instances
MICRO_INSTANCE="${AE_EC2_INSTANCE_ID:-i-09b539c844515d549}"
TRADING_INSTANCE="${AE_TRADING_INSTANCE_ID:-i-018eb3307a21329bf}"

echo "=== Alpha Engine Weekday Pipeline Deployment ==="
echo "  Region:          $REGION"
echo "  Account:         $ACCOUNT_ID"
echo "  Micro EC2:       $MICRO_INSTANCE"
echo "  Trading EC2:     $TRADING_INSTANCE"
echo ""

# ── Step Functions role IAM ─────────────────────────────────────────────────
#
# IAM for `alpha-engine-step-functions-role` is managed in the alpha-engine
# repo as the codified single source of truth:
#
#   alpha-engine/infrastructure/iam/alpha-engine-step-functions-role/alpha-engine-step-functions-role-policy.json
#
# Apply via:
#
#   cd ~/Development/alpha-engine && \
#     ./infrastructure/iam/apply.sh --role alpha-engine-step-functions-role
#
# This script no longer writes the role's inline policy. Two writers (here +
# apply.sh) drifted in shape (Sid presence, statement granularity, Lambda
# resource list) — drift detector flapped each time either ran. Single
# source of truth (codified IAM + apply.sh) eliminates the pattern.
#
# When this deploy script runs, it assumes the role policy is already current.
# CI's check-drift.py will fail loud if codified ↔ live drifts, so a missed
# apply.sh run gets caught before the next SF execution depends on the missing
# permission.

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# ── Create log group ────────────────────────────────────────────────────────

aws logs create-log-group \
  --log-group-name "/aws/stepfunctions/${STATE_MACHINE_NAME}" \
  --region "$REGION" 2>/dev/null || true

# ── State Machine ───────────────────────────────────────────────────────────

echo "Creating/updating state machine: $STATE_MACHINE_NAME..."

ASL_FILE="$SCRIPT_DIR/step_function_daily.json"
DEFINITION=$(cat "$ASL_FILE")

SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" --region "$REGION" &>/dev/null; then
  echo "  Updating existing state machine..."
  aws stepfunctions update-state-machine \
    --state-machine-arn "$SM_ARN" \
    --definition "$DEFINITION" \
    --role-arn "$ROLE_ARN" \
    --region "$REGION" > /dev/null
else
  echo "  Creating new state machine..."
  aws stepfunctions create-state-machine \
    --name "$STATE_MACHINE_NAME" \
    --definition "$DEFINITION" \
    --role-arn "$ROLE_ARN" \
    --type STANDARD \
    --logging-configuration '{
      "level": "ERROR",
      "includeExecutionData": true,
      "destinations": [
        {
          "cloudWatchLogsLogGroup": {
            "logGroupArn": "arn:aws:logs:'"$REGION"':'"$ACCOUNT_ID"':log-group:/aws/stepfunctions/'"$STATE_MACHINE_NAME"':*"
          }
        }
      ]
    }' \
    --region "$REGION" > /dev/null
fi
echo "  State machine ARN: $SM_ARN"

# ── EventBridge Rule ────────────────────────────────────────────────────────
#
# RULE + TARGETS ARE CFN-CANONICAL.
#
# This script no longer writes the EventBridge rule or its targets;
# both are codified in
# infrastructure/cloudformation/alpha-engine-orchestration.yaml as
# the single source of truth. See the matching comment block in
# infrastructure/deploy_step_function.sh for the 2026-05-26
# duplicate-target incident that drove this consolidation, and for
# the test chokepoints
# (``TestDeployScriptsHaveNoEventBridgeWrites``,
# ``TestCFNTargetUniqueness``) that fail CI on regression.
#
# Operators applying EventBridge changes run:
#
#   aws cloudformation deploy \
#     --template-file infrastructure/cloudformation/alpha-engine-orchestration.yaml \
#     --stack-name alpha-engine-orchestration \
#     --parameter-overrides ...

echo "  EventBridge rule: managed by CFN orchestration template (see comment above)"

# ── Disable old rules (optional) ───────────────────────────────────────────

if [ "${1:-}" = "--disable-old-rules" ]; then
  echo ""
  echo "Disabling old weekday rules..."
  aws events disable-rule --name "ae-predictor-run" --region "$REGION" 2>/dev/null && \
    echo "  Disabled: ae-predictor-run" || echo "  Not found: ae-predictor-run"
  echo "  Old rules disabled. Delete after 2 successful weeks."
fi

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "  State machine:  $SM_ARN"
echo "  EventBridge:    $EVENTBRIDGE_RULE (Mon-Fri 13:05 UTC / 6:05 AM PT)"
echo "  SNS topic:      $SNS_TOPIC_ARN"
echo ""
echo "To test manually:"
echo "  aws stepfunctions start-execution \\"
echo "    --state-machine-arn $SM_ARN \\"
echo "    --input '{\"trading_instance_id\": [\"$TRADING_INSTANCE\"], \"sns_topic_arn\": \"$SNS_TOPIC_ARN\"}' \\"
echo "    --region $REGION"
echo ""
