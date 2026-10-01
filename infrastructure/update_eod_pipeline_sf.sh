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
# alpha-engine-config-I11269 follow-up (2026-09-30): the post-close pipeline is
# now TWO machines — ne-postclose-trading-pipeline (step_function_eod.json:
# gate, box start, executor refresh, CaptureSnapshot at the close) and
# ne-postclose-reconcile-pipeline (step_function_eod_reconcile.json: the
# collector-dependent reconcile half). This fallback applies BOTH, each with
# its own log group. It only UPDATES: the reconcile machine's first CREATE is
# deploy-infrastructure.sh's job (update_or_create), so an absent machine here
# is reported and the script fails rather than creating it without a stamp.
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

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

apply_sf() {
    local sm_name="$1" defn_file="$2"
    local sm_arn="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${sm_name}"
    local log_group_name="/aws/stepfunctions/${sm_name}"
    local log_group_arn="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${log_group_name}:*"

    echo "=== Alpha Engine post-close pipeline — SF Definition Update ==="
    echo "  Region:        $REGION"
    echo "  State machine: $sm_arn"
    echo "  Definition:    $defn_file"
    echo ""

    if [ ! -f "$defn_file" ]; then
        echo "ERROR: $defn_file not found" >&2
        exit 1
    fi

    # Validate JSON before sending it to AWS.
    python3 -c "import json,sys; json.load(open(sys.argv[1])); print('  Definition: JSON valid')" "$defn_file"

    if ! aws stepfunctions describe-state-machine --state-machine-arn "$sm_arn" --query name --output text --region "$REGION" >/dev/null 2>&1; then
        echo "ERROR: $sm_name does not exist. Its first create is deploy-infrastructure.sh's" >&2
        echo "       update_or_create (stamped, with logging) — run that, not this fallback." >&2
        exit 1
    fi

    echo "  Ensuring log group $log_group_name exists (idempotent)..."
    aws logs create-log-group --log-group-name "$log_group_name" --region "$REGION" 2>/dev/null || true
    aws logs put-retention-policy --log-group-name "$log_group_name" --retention-in-days 30 --region "$REGION"

    aws stepfunctions update-state-machine \
        --state-machine-arn "$sm_arn" \
        --definition "file://$defn_file" \
        --logging-configuration '{"level":"ERROR","includeExecutionData":true,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"'"$log_group_arn"'"}}]}' \
        --region "$REGION" > /dev/null

    echo "  State machine: definition updated (execution logging: ERROR level, enabled)"
    echo ""
    echo "Verify:"
    echo "  aws stepfunctions describe-state-machine --state-machine-arn $sm_arn --query 'definition' --output text | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(\"States:\", list(d[\"States\"].keys()))'"
    echo ""
}

apply_sf "ne-postclose-trading-pipeline"   "$SCRIPT_DIR/step_function_eod.json"
apply_sf "ne-postclose-reconcile-pipeline" "$SCRIPT_DIR/step_function_eod_reconcile.json"

echo "=== Post-close pipeline SF updates complete ==="
echo ""
echo "First run with new chain: next daemon-triggered post-close firing (~16:00 ET),"
echo "and the reconcile half on the next ne-data-collection-eod terminal event."
