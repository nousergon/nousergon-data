#!/usr/bin/env bash
# infrastructure/deploy-infrastructure.sh — Deploy Alpha Engine orchestration infrastructure.
#
# Uploads Step Function definitions to S3, then deploys/updates the CloudFormation
# stack. Also updates the state machines directly (CloudFormation can't update
# Step Function definitions from S3 on stack update — it only reads on create).
#
# Usage:
#   bash infrastructure/deploy-infrastructure.sh              # deploy/update
#   bash infrastructure/deploy-infrastructure.sh --dry-run    # validate only
#
# Prerequisites:
#   - AWS CLI configured with appropriate permissions
#   - Step Function JSON files in infrastructure/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUCKET="alpha-engine-research"
STACK_NAME="alpha-engine-orchestration"
TEMPLATE="$SCRIPT_DIR/cloudformation/alpha-engine-orchestration.yaml"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Git SHA stamp — baked into the SF Comment field and the CF stack tags so
# the deploy-drift preflight can detect when main has moved past the deployed
# artifact. CI supplies $GITHUB_SHA; local dev falls back to HEAD.
GIT_SHA="${GITHUB_SHA:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)}"
echo "  Stamping deploy with GIT_SHA=${GIT_SHA}"

DRY_RUN=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════"
echo "  Alpha Engine Infrastructure Deploy"
echo "═══════════════════════════════════════════════════════════════"
echo "  Stack:    $STACK_NAME"
echo "  Region:   $REGION"
echo "  Account:  $ACCOUNT_ID"
echo "  Dry run:  $DRY_RUN"
echo ""

# ── 1. Validate CloudFormation template ──────────────────────────────────────
echo "==> Validating CloudFormation template..."
aws cloudformation validate-template --template-body "file://$TEMPLATE" --query "Description" --output text
echo "  Template valid."

if $DRY_RUN; then
    echo ""
    echo "Dry run complete. No changes made."
    exit 0
fi

# ── 2. Stamp SF definitions with git SHA + upload to S3 ──────────────────────
# Prepend `[git:<sha>] ` to the top-level `Comment` field so the preflight
# drift check can extract + compare against origin/main. The stamped JSON is
# what gets uploaded to S3 AND fed to update-state-machine, so S3 copy and
# live definition stay in lockstep.
echo ""
echo "==> Stamping Step Function definitions with git SHA..."
SAT_STAMPED="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
DAILY_STAMPED="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
EOD_STAMPED="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
BTEVAL_STAMPED="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
trap "rm -f '$SAT_STAMPED' '$DAILY_STAMPED' '$EOD_STAMPED' '$BTEVAL_STAMPED'" EXIT
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
" "$SCRIPT_DIR/step_function.json" "$SAT_STAMPED" "$GIT_SHA"
python3 -c "
import json, sys
path_in, path_out, sha = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(path_in))
orig = d.get('Comment', '')
if orig.startswith('[git:'):
    orig = orig.split(' ', 1)[1] if ' ' in orig else ''
d['Comment'] = f'[git:{sha}] {orig}'.rstrip()
json.dump(d, open(path_out, 'w'), indent=2)
" "$SCRIPT_DIR/step_function_daily.json" "$DAILY_STAMPED" "$GIT_SHA"
python3 -c "
import json, sys
path_in, path_out, sha = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(path_in))
orig = d.get('Comment', '')
if orig.startswith('[git:'):
    orig = orig.split(' ', 1)[1] if ' ' in orig else ''
d['Comment'] = f'[git:{sha}] {orig}'.rstrip()
json.dump(d, open(path_out, 'w'), indent=2)
" "$SCRIPT_DIR/step_function_eod.json" "$EOD_STAMPED" "$GIT_SHA"
python3 -c "
import json, sys
path_in, path_out, sha = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(path_in))
orig = d.get('Comment', '')
if orig.startswith('[git:'):
    orig = orig.split(' ', 1)[1] if ' ' in orig else ''
d['Comment'] = f'[git:{sha}] {orig}'.rstrip()
json.dump(d, open(path_out, 'w'), indent=2)
" "$SCRIPT_DIR/step_function_backtest_eval.json" "$BTEVAL_STAMPED" "$GIT_SHA"

echo ""
echo "==> Uploading Step Function definitions to S3..."
aws s3 cp "$SAT_STAMPED" "s3://$BUCKET/infrastructure/step_function.json" --quiet
aws s3 cp "$DAILY_STAMPED" "s3://$BUCKET/infrastructure/step_function_daily.json" --quiet
aws s3 cp "$EOD_STAMPED" "s3://$BUCKET/infrastructure/step_function_eod.json" --quiet
aws s3 cp "$BTEVAL_STAMPED" "s3://$BUCKET/infrastructure/step_function_backtest_eval.json" --quiet
echo "  Uploaded to s3://$BUCKET/infrastructure/"

# ── 3. Update Step Functions directly ────────────────────────────────────────
echo ""
echo "==> Updating Step Function definitions..."

SAT_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-saturday-pipeline"
DAILY_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-weekday-pipeline"
EOD_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-eod-pipeline"
BTEVAL_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-backtest-eval-pipeline"

# Pass the definition via file:// — NOT inline "$(cat ...)". The Saturday ASL is
# ~131 KB and growing (one state per pipeline step); combined with the AWS
# session-token env on the CI runner, an inline arg blows past the effective
# ARG_MAX and the runner aborts with "aws: Argument list too long" (exit 126),
# which silently leaves the live SF stamp behind origin/main HEAD and trips the
# deploy-drift preflight on the next pipeline run. file:// reads from disk, so
# the definition size is bounded only by the SF service limit (1 MB), not ARG_MAX.
# Regression: 2026-06-04 — the Director SF state pushed the Saturday ASL over the
# line and broke this deploy.
aws stepfunctions update-state-machine --state-machine-arn "$SAT_ARN" --definition "file://$SAT_STAMPED" --query "updateDate" --output text
echo "  Saturday pipeline updated."

aws stepfunctions update-state-machine --state-machine-arn "$DAILY_ARN" --definition "file://$DAILY_STAMPED" --query "updateDate" --output text
echo "  Weekday pipeline updated."

# EOD SF (alpha-engine-eod-pipeline) — folded into auto-deploy 2026-06-23 (config#1173).
# Previously deployed only by the manual infrastructure/update_eod_pipeline_sf.sh, which
# nothing triggered on merge, so merged EOD SF changes silently never reached the live
# state machine (drift hit 2026-06-22 via #458's nousergon_lib migration). Same
# stamp + S3-copy + file:// update-state-machine pattern as Saturday/weekday above, so all
# three orchestration SFs now stay in lockstep with origin/main on every merge.
aws stepfunctions update-state-machine --state-machine-arn "$EOD_ARN" --definition "file://$EOD_STAMPED" --query "updateDate" --output text
echo "  EOD pipeline updated."

# Backtest+Eval SF (alpha-engine-backtest-eval-pipeline) — config#830 mid-week rerun SF.
# Created by the CloudFormation stack below (step 4) on first deploy, so on the very
# first deploy this state machine does not exist yet and update-state-machine would
# 404. Guard the update on existence: if the SF isn't there yet, the CFN create in
# step 4 reads the freshly-uploaded S3 definition (CFN reads DefinitionS3Location on
# create); every subsequent deploy updates it here in lockstep with origin/main, same
# stamp + S3-copy + file:// pattern as the three SFs above.
if aws stepfunctions describe-state-machine --state-machine-arn "$BTEVAL_ARN" --region "$REGION" &>/dev/null; then
    aws stepfunctions update-state-machine --state-machine-arn "$BTEVAL_ARN" --definition "file://$BTEVAL_STAMPED" --query "updateDate" --output text
    echo "  Backtest+Eval pipeline updated."
else
    echo "  Backtest+Eval pipeline not yet created — CloudFormation will create it from the uploaded S3 definition (step 4)."
fi

# ── 4. Deploy/update CloudFormation stack ────────────────────────────────────
echo ""
echo "==> Deploying CloudFormation stack..."

# Check if stack exists
STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "DOES_NOT_EXIST")

# Terminal / unusable states surface loudly instead of being silently
# skipped. 2026-04-20 incident: a stack sitting in ROLLBACK_COMPLETE
# was masked by the old `|| echo "no updates needed"` catch — the new
# UnscoredBuyCandidatesGap alarm from PR #72 was never actually created
# but the deploy script reported success. Never again.
case "$STACK_STATUS" in
    ROLLBACK_COMPLETE | ROLLBACK_FAILED | UPDATE_ROLLBACK_FAILED | CREATE_FAILED | DELETE_FAILED)
        echo "  ERROR: stack $STACK_NAME is in terminal state $STACK_STATUS — CloudFormation refuses to update."
        echo "         Remediation (one-time): run the import change-set flow to adopt pre-existing resources."
        echo "         See nous-ergon-ops/alpha-engine-data/infrastructure/cloudformation/resources-to-import.json (private ops repo) + the README section"
        echo "         'Recovering from ROLLBACK_COMPLETE' for the exact aws commands."
        exit 1
        ;;
    "" )
        echo "  ERROR: empty stack status from describe-stacks — aborting to avoid acting on partial state."
        exit 1
        ;;
esac

if [ "$STACK_STATUS" = "DOES_NOT_EXIST" ]; then
    echo "  Creating new stack..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://$TEMPLATE" \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags "Key=git-sha,Value=$GIT_SHA" \
        --query "StackId" --output text
    echo "  Waiting for stack creation..."
    aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
else
    echo "  Updating existing stack (current status: $STACK_STATUS)..."
    UPDATE_OUT="$(mktemp)"
    trap "rm -f '$UPDATE_OUT'" EXIT
    set +e
    aws cloudformation update-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://$TEMPLATE" \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags "Key=git-sha,Value=$GIT_SHA" \
        --query "StackId" --output text > "$UPDATE_OUT" 2>&1
    UPDATE_RC=$?
    set -e
    if [ $UPDATE_RC -eq 0 ]; then
        echo "  update-stack submitted — waiting for completion..."
        aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME"
    elif grep -q "No updates are to be performed" "$UPDATE_OUT"; then
        # Only one AWS response is an acceptable no-op: template + tags unchanged.
        # Every other error (including IAM denial, validation, rollback state)
        # still exits non-zero because it means the deploy DIDN'T happen.
        echo "  No updates needed (template + git-sha tag both current)."
    else
        echo "  ERROR: update-stack failed with rc=$UPDATE_RC:"
        cat "$UPDATE_OUT"
        exit 1
    fi
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Infrastructure deploy complete."
echo "═══════════════════════════════════════════════════════════════"
