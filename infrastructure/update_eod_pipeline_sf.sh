#!/usr/bin/env bash
# update_eod_pipeline_sf.sh — Apply the canonical EOD pipeline SF definition.
#
# NOTE (2026-06-23, config#1173): the EOD SF is now auto-deployed on every
# merge to main by deploy-infrastructure.sh (alongside the Saturday + weekday
# SFs), so this script is no longer required for normal merges. It is retained
# only as a manual fallback for out-of-band EOD-SF redeploys (e.g. re-applying
# the on-disk definition without a merge, or recovering from a failed
# deploy-infrastructure run). Note: unlike the auto-deploy path, this script
# applies the definition WITHOUT the [git:<sha>] Comment stamp.
#
# Reads the state-machine definition from
# infrastructure/step_function_eod.json (single source of truth, same
# pattern as deploy_step_function.sh for the Saturday SF) and applies
# it to ne-postclose-trading-pipeline. The JSON file is the authoritative
# definition — wiring tests pin its contents.
#
# Idempotent: re-running with the same definition is a no-op (AWS only
# bumps the revision when the definition actually changes).
#
# config#1416: also ensures the EOD execution-log group exists (idempotent
# create-log-group + put-retention-policy) and enables ERROR-level
# LoggingConfiguration on the state machine, mirroring the weekly/preopen
# pair (config#729/#537) so the same MutexConflict metric-filter + alarm
# pattern has a log group to read. Unlike those two, this log group is NOT
# a CloudFormation resource — see the CFN template comment for why.
#
# Usage:
#   ./infrastructure/update_eod_pipeline_sf.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFN_FILE="$SCRIPT_DIR/step_function_eod.json"

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline"

echo "=== Alpha Engine EOD Pipeline — SF Definition Update ==="
echo "  Region:        $REGION"
echo "  State machine: $SM_ARN"
echo "  Definition:    $DEFN_FILE"
echo ""

if [ ! -f "$DEFN_FILE" ]; then
    echo "ERROR: $DEFN_FILE not found" >&2
    exit 1
fi

# Validate JSON before sending it to AWS.
python3 -c "import json,sys; json.load(open(sys.argv[1])); print('  Definition: JSON valid')" "$DEFN_FILE"

LOG_GROUP_NAME="/aws/stepfunctions/ne-postclose-trading-pipeline"
LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${LOG_GROUP_NAME}:*"

echo "  Ensuring EOD log group exists (idempotent)..."
aws logs create-log-group --log-group-name "$LOG_GROUP_NAME" --region "$REGION" 2>/dev/null || true
aws logs put-retention-policy --log-group-name "$LOG_GROUP_NAME" --retention-in-days 30 --region "$REGION"

aws stepfunctions update-state-machine \
    --state-machine-arn "$SM_ARN" \
    --definition "file://$DEFN_FILE" \
    --logging-configuration '{"level":"ERROR","includeExecutionData":true,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"'"$LOG_GROUP_ARN"'"}}]}' \
    --region "$REGION" > /dev/null

echo "  State machine: definition updated (execution logging: ERROR level, enabled)"
echo ""
echo "=== EOD Pipeline SF Update Complete ==="
echo ""
echo "Verify:"
echo "  aws stepfunctions describe-state-machine --state-machine-arn $SM_ARN --query 'definition' --output text | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(\"States:\", list(d[\"States\"].keys()))'"
echo ""
echo "First run with new chain: next daemon-triggered firing"
echo "(daemon shutdown, weekday market close + IB delay grace)."
