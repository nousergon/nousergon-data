#!/usr/bin/env bash
# deploy_step_function.sh — Create/update the Saturday pipeline Step Functions
# state machine, IAM role, SNS topic, and EventBridge trigger.
#
# SINGLE-WRITER CONTRACT (config#2273): the repo file
# infrastructure/step_function.json is the sole source of truth for the
# ne-weekly-freshness-pipeline definition. Every deploy path (this script
# for manual/bootstrap use, deploy-infrastructure.sh on merge) uploads the
# stamped repo bytes to the CFN-referenced S3 key BEFORE calling
# update-state-machine from those same bytes, and passes
# --logging-configuration explicitly. CFN's DefinitionS3Location is only
# read at stack-create/replacement, so keeping the S3 object in lockstep
# means CFN can never restamp different bytes — see section 3 below.
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

STATE_MACHINE_NAME="ne-weekly-freshness-pipeline"
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

# ── 3. State Machine — SINGLE-WRITER CONTRACT (config#2273) ────────────────
#
# The repo file infrastructure/step_function.json is the SOLE source of
# truth for the ne-weekly-freshness-pipeline definition. Three copies of
# the definition exist — the repo file, the S3 object CFN's
# DefinitionS3Location references, and the live state machine — and the
# deploy path is the ONLY thing allowed to move any of them. Every deploy
# therefore does ALL of:
#
#   (a) stamp the repo file with the current git SHA (the same idempotent
#       [git:<sha>] Comment stamp as deploy-infrastructure.sh, so this
#       manual/bootstrap path and the on-merge CI path emit byte-identical
#       artifacts for the same commit);
#   (b) upload the stamped bytes to the CFN-referenced S3 key
#       (WEEKLY_SF_S3_BUCKET/WEEKLY_SF_S3_KEY below — MUST stay equal to
#       the SaturdayPipeline DefinitionS3Location in
#       cloudformation/alpha-engine-orchestration.yaml). CFN only reads
#       that object at stack-create / resource-replacement time; keeping
#       it in lockstep means a future CFN restamp re-deploys the SAME
#       bytes instead of silently rolling the live definition back to a
#       stale object — CFN stops being an independent writer in practice;
#   (c) update-state-machine from the SAME stamped bytes, passing
#       --logging-configuration EXPLICITLY — reconstructed from the shape
#       CFN declares on the SaturdayPipeline resource (level=ERROR,
#       includeExecutionData=true, the WeeklyFreshnessLogGroup log group)
#       — never relying on partial-update preservation (the
#       recreate-drops-logging bug class is config#1464; the ne-* rename
#       config#1381 hit it live).
#
# Drift backstop: infrastructure/step-functions/check-definition-drift.py
# compares repo file vs live definition vs S3 staged copy (normalized)
# and exits non-zero on any divergence. Shape-guard for THIS section:
# tests/test_deploy_step_function_single_writer.py.

echo "Creating/updating state machine: $STATE_MACHINE_NAME..."

ASL_FILE="$SCRIPT_DIR/step_function.json"
if [ ! -f "$ASL_FILE" ]; then
  echo "ERROR: $ASL_FILE not found"
  exit 1
fi

# CFN-referenced definition location (SaturdayPipeline DefinitionS3Location).
# tests/test_deploy_step_function_single_writer.py pins these to the CFN
# template so the script and CFN can never point at different objects.
WEEKLY_SF_S3_BUCKET="alpha-engine-research"
WEEKLY_SF_S3_KEY="infrastructure/step_function.json"

# Git SHA stamp — identical mechanics to deploy-infrastructure.sh so the
# deploy-drift preflight can compare the live Comment stamp against
# origin/main regardless of which deploy path last wrote the definition.
GIT_SHA="${GITHUB_SHA:-$(git -C "$SCRIPT_DIR/.." rev-parse HEAD 2>/dev/null || echo unknown)}"
echo "  Stamping definition with GIT_SHA=${GIT_SHA}"
STAMPED_ASL="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
trap 'rm -f "$STAMPED_ASL"' EXIT
python3 -c "
import json, sys
path_in, path_out, sha = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(path_in))
orig = d.get('Comment', '')
# Strip any existing [git:…] prefix so re-stamping is idempotent
if orig.startswith('[git:'):
    orig = orig.split(' ', 1)[1] if ' ' in orig else ''
d['Comment'] = f'[git:{sha}] {orig}'.rstrip()
json.dump(d, open(path_out, 'w'), indent=2)
" "$ASL_FILE" "$STAMPED_ASL" "$GIT_SHA"

# (b) S3 upload FIRST — the CFN read-source must never lag the live machine.
echo "  Uploading stamped definition to s3://${WEEKLY_SF_S3_BUCKET}/${WEEKLY_SF_S3_KEY}..."
aws s3 cp "$STAMPED_ASL" "s3://${WEEKLY_SF_S3_BUCKET}/${WEEKLY_SF_S3_KEY}" --quiet --region "$REGION"

# (c) Explicit LoggingConfiguration — shape mirrors the CFN SaturdayPipeline
# LoggingConfiguration declaration (CFN stays the declarative SoT for the
# SHAPE; asserting it on every update self-heals a recreate-dropped config
# instead of leaving the gap to the drift checker).
LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:/aws/stepfunctions/${STATE_MACHINE_NAME}:*"
LOGGING_CONFIG='{"level":"ERROR","includeExecutionData":true,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"'"$LOG_GROUP_ARN"'"}}]}'

# --definition via file:// — an inline "$(cat ...)" arg blows ARG_MAX once
# the ASL grows (2026-06-04 regression in deploy-infrastructure.sh; the
# weekly ASL is ~130 KB and growing).
SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" --region "$REGION" &>/dev/null; then
  echo "  Updating existing state machine..."
  aws stepfunctions update-state-machine \
    --state-machine-arn "$SM_ARN" \
    --definition "file://$STAMPED_ASL" \
    --role-arn "$ROLE_ARN" \
    --logging-configuration "$LOGGING_CONFIG" \
    --region "$REGION" > /dev/null
else
  echo "  Creating new state machine..."
  aws stepfunctions create-state-machine \
    --name "$STATE_MACHINE_NAME" \
    --definition "file://$STAMPED_ASL" \
    --role-arn "$ROLE_ARN" \
    --type STANDARD \
    --logging-configuration "$LOGGING_CONFIG" \
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
