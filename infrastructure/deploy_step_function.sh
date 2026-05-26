#!/usr/bin/env bash
# deploy_step_function.sh — Create/update the Saturday pipeline Step Functions
# state machine, IAM role, SNS topic, and EventBridge trigger.
#
# Prerequisites:
#   1. AWS CLI configured with admin credentials
#   2. SSM agent installed on the always-on EC2 instance
#   3. Research Lambda (alpha-engine-research-runner) deployed
#   4. Data Phase 2 Lambda (alpha-engine-data-collector) deployed
#   5. Eval-judge Lambda (alpha-engine-research-eval-judge) deployed via
#      `infrastructure/deploy.sh eval_judge` from alpha-engine-research
#      (rolling-mean + rationale-clustering Lambdas auto-deployed by the
#      same workflow when the research repo's main branch updates)
#   6. Repos cloned on always-on EC2: alpha-engine-data, alpha-engine-predictor,
#      alpha-engine-backtester
#
# Usage:
#   ./infrastructure/deploy_step_function.sh
#   ./infrastructure/deploy_step_function.sh --disable-old-crons
#
# After deployment:
#   1. Run a test execution from the Step Functions console
#   2. Monitor first automated Saturday run (00:00 UTC)
#   3. After 2 successful weeks, run with --disable-old-crons

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STATE_MACHINE_NAME="alpha-engine-saturday-pipeline"
ROLE_NAME="alpha-engine-step-functions-role"
SNS_TOPIC_NAME="alpha-engine-alerts"
EVENTBRIDGE_RULE="alpha-engine-saturday"

# Always-on EC2 instance ID (micro instance that runs data collection + launches spot)
EC2_INSTANCE_ID="${AE_EC2_INSTANCE_ID:-}"
if [ -z "$EC2_INSTANCE_ID" ]; then
  echo "ERROR: Set AE_EC2_INSTANCE_ID env var to the always-on EC2 instance ID"
  echo "       (e.g., export AE_EC2_INSTANCE_ID=i-0abc123def456)"
  exit 1
fi

echo "=== Alpha Engine Step Functions Deployment ==="
echo "  Region:     $REGION"
echo "  Account:    $ACCOUNT_ID"
echo "  EC2:        $EC2_INSTANCE_ID"
echo ""

# ── 1. SNS Topic ────────────────────────────────────────────────────────────

echo "Creating SNS topic: $SNS_TOPIC_NAME..."
SNS_TOPIC_ARN=$(aws sns create-topic \
  --name "$SNS_TOPIC_NAME" \
  --query "TopicArn" --output text \
  --region "$REGION")
echo "  Topic ARN: $SNS_TOPIC_ARN"

# Check if email subscription exists
EXISTING_SUBS=$(aws sns list-subscriptions-by-topic \
  --topic-arn "$SNS_TOPIC_ARN" \
  --query "Subscriptions[?Protocol=='email'].Endpoint" --output text \
  --region "$REGION" 2>/dev/null || echo "")
if [ -z "$EXISTING_SUBS" ]; then
  echo "  WARNING: No email subscriptions on $SNS_TOPIC_NAME."
  echo "  Add one: aws sns subscribe --topic-arn $SNS_TOPIC_ARN --protocol email --notification-endpoint your@email.com"
fi

# ── 2. IAM Role for Step Functions ──────────────────────────────────────────
#
# Trust policy + role creation kept here (one-time bootstrap). The inline
# policy on this role is codified in this repo's infrastructure/iam/
# directory (alpha-engine-step-functions-role.json) and applied via
# apply.sh — NOT inline here.
#
# This script previously wrote its own inline put-role-policy against
# this role with a narrower policy than the codified version, which
# clobbered the codified state every saturday deploy and broke
# downstream pipelines (4 incidents in 2 months). Codified IAM is now
# the single writer.

echo "Ensuring IAM role exists: $ROLE_NAME..."

# Trust policy (one-time bootstrap; safe to re-run)
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "states.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}'

aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document "$TRUST_POLICY" \
  --region "$REGION" 2>/dev/null || echo "  Role already exists"

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo "  Role ARN: $ROLE_ARN"
echo "  Inline policy is codified in infrastructure/iam/${ROLE_NAME}.json"
echo "  Apply via: ./infrastructure/iam/apply.sh $ROLE_NAME"

# ── 3. State Machine ───────────────────────────────────────────────────────

echo "Creating/updating state machine: $STATE_MACHINE_NAME..."

ASL_FILE="$SCRIPT_DIR/step_function.json"
if [ ! -f "$ASL_FILE" ]; then
  echo "ERROR: $ASL_FILE not found"
  exit 1
fi

# Read the ASL definition
DEFINITION=$(cat "$ASL_FILE")

# Check if state machine exists
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
  SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
fi
echo "  State machine ARN: $SM_ARN"

# ── 4. EventBridge Rule + Targets — CFN-CANONICAL ──────────────────────────
#
# This script no longer writes the EventBridge rule or its targets;
# both are codified in
# infrastructure/cloudformation/alpha-engine-orchestration.yaml as the
# single source of truth.
#
# WHY (2026-05-26): PR #317 added a put-targets call here AND left the
# CFN ``Targets:`` block intact, so the alpha-engine-saturday rule
# (and its weekday sibling) carried TWO targets — Id="1" from this
# script + Id="saturday-pipeline" from CFN. EventBridge dispatched
# every weekday cron firing to BOTH targets, fanning the cron into two
# parallel SF executions on the same trading instance. Both ran
# MorningEnrich → both connected ArcticDB → 321 unique-symbol
# E_NON_INCREASING_INDEX_VERSION races → the 5%-threshold daily_append
# gate hard-failed both runs at 35.6% error rate (905 tickers,
# n_err=322). Trading didn't happen on 5/26.
#
# The substrate gate that prevents recurrence is
# ``tests/test_deploy_step_function_eventbridge_input.py``::
#   - ``TestDeployScriptsHaveNoEventBridgeWrites`` (this script must
#     not contain ``aws events put-rule`` or ``aws events put-targets``)
#   - ``TestCFNTargetUniqueness`` (each cron rule has exactly 1 target
#     in the CFN template)
#
# Operators applying EventBridge changes:
#
#   aws cloudformation deploy \
#     --template-file infrastructure/cloudformation/alpha-engine-orchestration.yaml \
#     --stack-name alpha-engine-orchestration \
#     --parameter-overrides ...
#
# This script's remaining responsibility is the SF state machine JSON
# (upload to S3 + update-state-machine), plus the bootstrap IAM role
# the CFN template's EventBridgeSfnRoleArn parameter references
# (kept here so a fresh region/account can still be bootstrapped via
# this script alone; idempotent ``|| true`` on re-runs).
#
# IAM bootstrap: trust policy + role creation only. The role's INLINE
# policy is codified in the alpha-engine repo's
# infrastructure/iam/ directory and applied via that repo's apply.sh —
# do NOT add inline-policy writes here (per the dual-writer incident
# 2026-04-21/05-04/05-06 documented above the IAM block historically).

EB_ROLE_NAME="alpha-engine-eventbridge-sfn-role"
EB_TRUST='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "events.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}'

aws iam create-role \
  --role-name "$EB_ROLE_NAME" \
  --assume-role-policy-document "$EB_TRUST" \
  --region "$REGION" 2>/dev/null || true

echo "  EventBridge rule + targets: managed by CFN orchestration template"
echo "  EventBridge IAM role bootstrap: $EB_ROLE_NAME (idempotent)"

# ── 5. Disable old crons (optional) ────────────────────────────────────────

if [ "${1:-}" = "--disable-old-crons" ]; then
  echo ""
  echo "Disabling old scheduling rules (keeping as fallback)..."

  # Research weekly EventBridge
  aws events disable-rule --name "alpha-research-weekly" --region "$REGION" 2>/dev/null && \
    echo "  Disabled: alpha-research-weekly" || echo "  Not found: alpha-research-weekly"

  # Backtester weekly EventBridge
  aws events disable-rule --name "alpha-engine-backtester-weekly" --region "$REGION" 2>/dev/null && \
    echo "  Disabled: alpha-engine-backtester-weekly" || echo "  Not found: alpha-engine-backtester-weekly"

  echo ""
  echo "  Old rules DISABLED (not deleted). Delete after 2 successful weeks:"
  echo "    aws events delete-rule --name alpha-research-weekly --region $REGION"
  echo "    aws events delete-rule --name alpha-engine-backtester-weekly --region $REGION"
  echo ""
  echo "  EC2 crons (predictor training, backtester, data collection) must be"
  echo "  disabled manually on the EC2 instance:"
  echo "    ae-trading 'crontab -l'   # review"
  echo "    ae-trading 'crontab -e'   # comment out old entries"
fi

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "  State machine:  $SM_ARN"
echo "  EventBridge:    $EVENTBRIDGE_RULE (Saturday 00:00 UTC)"
echo "  SNS topic:      $SNS_TOPIC_ARN"
echo ""
echo "To test manually:"
echo "  aws stepfunctions start-execution \\"
echo "    --state-machine-arn $SM_ARN \\"
echo "    --input '{\"ec2_instance_id\": [\"$EC2_INSTANCE_ID\"], \"sns_topic_arn\": \"$SNS_TOPIC_ARN\"}' \\"
echo "    --region $REGION"
echo ""
echo "To monitor:"
echo "  aws stepfunctions list-executions --state-machine-arn $SM_ARN --region $REGION"
echo ""
