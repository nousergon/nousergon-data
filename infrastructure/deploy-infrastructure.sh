#!/usr/bin/env bash
# infrastructure/deploy-infrastructure.sh — Deploy Alpha Engine orchestration infrastructure.
#
# Uploads the CFN template AND the Step Function definitions to S3, then
# deploys/updates the CloudFormation stack via --template-url (not inline
# --template-body — see alpha-engine-config-I7250). Also updates the state
# machines directly (CloudFormation can't update Step Function definitions
# from S3 on stack update — it only reads on create).
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
# alpha-engine-config-I7250: the template is uploaded to S3 and deployed via
# --template-url rather than passed inline. Inline --template-body is capped
# at CloudFormation's 51200-byte ceiling; an S3-sourced template raises that
# to 460800 bytes. Same bucket/prefix InfraDeployS3 already grants
# (arn:aws:s3:::alpha-engine-research/infrastructure/*) — no new IAM needed.
TEMPLATE_S3_KEY="infrastructure/alpha-engine-orchestration.yaml"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
TEMPLATE_URL="https://${BUCKET}.s3.${REGION}.amazonaws.com/${TEMPLATE_S3_KEY}"

# Git SHA stamp — baked into the SF Comment field and the CF stack tags so
# the deploy-drift preflight can detect when main has moved past the deployed
# artifact. CI supplies $GITHUB_SHA; local dev falls back to HEAD.
GIT_SHA="${GITHUB_SHA:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)}"
echo "  Stamping deploy with GIT_SHA=${GIT_SHA}"

DRY_RUN=false
# --validate-only: stamp every SF definition and run the validate-ALL preflight
# (step 2b), then exit BEFORE the first S3 write or CloudFormation call. This is
# the mode the PRE-MERGE check runs (alpha-engine-config-I7798), so the PR is
# graded by the SAME validator, on the SAME stamped artifacts, in the SAME
# all-or-nothing order the deploy uses — one implementation, not a second one
# that can drift from it. It needs exactly states:ValidateStateMachineDefinition
# (role ne-github-sf-validate); it deliberately performs no mutation, which is
# also why it skips step 1's template upload rather than reordering around it.
#
# NOTE --dry-run is a DIFFERENT and weaker mode despite its "validate only"
# header: it uploads the CFN template to S3 and validates THAT, then exits
# before the SF definitions are ever stamped or validated. Tracked separately.
VALIDATE_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --validate-only) VALIDATE_ONLY=true ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════"
echo "  Alpha Engine Infrastructure Deploy"
echo "═══════════════════════════════════════════════════════════════"
echo "  Stack:    $STACK_NAME"
echo "  Region:   $REGION"
echo "  Account:  $ACCOUNT_ID"
echo "  Dry run:  $DRY_RUN"
echo "  Validate only: $VALIDATE_ONLY"
echo ""

# ── 1. Upload the CFN template to S3, then validate via --template-url ───────
# Uploaded BEFORE validation (not just before create/update-stack) so
# validate-template exercises the exact artifact create-stack/update-stack
# will read below — an S3-sourced template that fails to upload fails here,
# not three steps later against a stale copy.
if $VALIDATE_ONLY; then
    echo "==> Skipping CloudFormation template upload/validate (--validate-only performs no mutation)."
else
    echo "==> Uploading CloudFormation template to S3..."
    aws s3 cp "$TEMPLATE" "s3://$BUCKET/$TEMPLATE_S3_KEY" --quiet
    echo "  Uploaded to s3://$BUCKET/$TEMPLATE_S3_KEY"

    echo "==> Validating CloudFormation template..."
    aws cloudformation validate-template --template-url "$TEMPLATE_URL" --query "Description" --output text
    echo "  Template valid."
fi

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
GROOM_STAMPED="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
trap "rm -f '$SAT_STAMPED' '$DAILY_STAMPED' '$EOD_STAMPED' '$GROOM_STAMPED'" EXIT
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
" "$SCRIPT_DIR/step_function_groom.json" "$GROOM_STAMPED" "$GIT_SHA"

# ── 2b. Validate-ALL preflight BEFORE any S3 upload or update (config#1897) ──
# All-or-nothing gate. `aws stepfunctions validate-state-machine-definition` is
# the SAME validation AWS runs at UpdateStateMachine time, so it catches the
# BROAD malformed-intrinsic class the in-repo unit guard (paren-balance only,
# TestIntrinsicsWellFormed, #677) cannot see: unknown intrinsic function names,
# wrong argument counts/types, invalid JSONPath in `.$` fields, bad
# `States.Format` placeholders. Running it here — for EVERY stamped definition
# the script would deploy, BEFORE the first update/create-state-machine call in
# step 3 — means a bad definition fails the deploy while NOTHING has been applied
# yet, so the fleet's SFs are never left stamped at mixed SHAs (the 2026-07-07
# incident, #676 → #677, where the weekly SF updated before the daily SF was
# rejected). We validate ALL FOUR and only then abort, so one run surfaces every
# bad definition rather than one-at-a-time. (alpha-engine-config-I2890
# 2026-07-17: the I2544/I2545 advisory + modelzoo child SFs were RETIRED —
# splits reversed; the weekly SF carries the full inline pattern again.)
#
# Per the AWS API contract we key the pass/fail decision ONLY on the `result`
# field (OK | FAIL) — AWS explicitly documents that diagnostic codes/wording may
# change, so we display diagnostics for the operator but never branch on them.
# The action is resource-less; the GHA deploy role grants it via the
# `InfraDeployValidateSFDefinition` statement in iam/github-actions-lambda-deploy.json.
echo ""
echo "==> Validating ALL Step Function definitions (all-or-nothing preflight)..."
validate_sf_definition() {
    local stamped="$1" label="$2" result rc status
    result="$(aws stepfunctions validate-state-machine-definition \
        --definition "file://$stamped" --type STANDARD --severity WARNING \
        --output json 2>&1)"
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "  ✗ $label — validate-state-machine-definition call failed (rc=$rc):"
        printf '       %s\n' "$result"
        return 1
    fi
    status="$(printf '%s' "$result" | python3 -c "import json,sys; print(json.load(sys.stdin).get('result',''))" 2>/dev/null || echo PARSE_ERROR)"
    # Surface every diagnostic (ERROR + WARNING) for operator visibility.
    printf '%s' "$result" | python3 -c "
import json, sys
for d in json.load(sys.stdin).get('diagnostics', []):
    loc = f\" @ {d['location']}\" if d.get('location') else ''
    print(f\"       [{d.get('severity','?')}] {d.get('code','?')}: {d.get('message','')}{loc}\")
" 2>/dev/null || true
    if [ "$status" != "OK" ]; then
        echo "  ✗ $label FAILED validation (result=$status)."
        return 1
    fi
    echo "  ✓ $label valid."
}

VALIDATION_FAILED=false
validate_sf_definition "$SAT_STAMPED"      "Weekly-freshness pipeline"      || VALIDATION_FAILED=true
validate_sf_definition "$DAILY_STAMPED"    "Pre-open trading pipeline"       || VALIDATION_FAILED=true
validate_sf_definition "$EOD_STAMPED"      "Post-close trading pipeline"     || VALIDATION_FAILED=true
validate_sf_definition "$GROOM_STAMPED"    "Backlog groom pipeline"          || VALIDATION_FAILED=true
if $VALIDATION_FAILED; then
    echo ""
    echo "  ERROR: one or more Step Function definitions failed validation (see above)."
    echo "         Aborting BEFORE any S3 upload or update-state-machine call, so the"
    echo "         fleet's state machines are NOT left stamped at mixed SHAs."
    exit 1
fi
echo "  All Step Function definitions valid."

if $VALIDATE_ONLY; then
    echo ""
    echo "Validate-only complete: every Step Function definition this repo would"
    echo "deploy is accepted by AWS. No artifact was uploaded and no state machine"
    echo "was touched."
    exit 0
fi

echo ""
echo "==> Uploading Step Function definitions to S3..."
aws s3 cp "$SAT_STAMPED" "s3://$BUCKET/infrastructure/step_function.json" --quiet
aws s3 cp "$DAILY_STAMPED" "s3://$BUCKET/infrastructure/step_function_daily.json" --quiet
aws s3 cp "$EOD_STAMPED" "s3://$BUCKET/infrastructure/step_function_eod.json" --quiet
aws s3 cp "$GROOM_STAMPED" "s3://$BUCKET/infrastructure/step_function_groom.json" --quiet
echo "  Uploaded to s3://$BUCKET/infrastructure/"

# ── 3. Update Step Function definitions (existence-aware) ────────────────────
# Push the stamped ASL to each live state machine. CFN owns CREATION of the two
# CFN-managed SFs (weekly-freshness, preopen-trading) via DefinitionS3Location —
# on a fresh stack OR a StateMachineName change (a CFN replacement) it reads the
# S3 copy uploaded in step 2. So here: UPDATE the SF if it exists, and for the
# CFN pair SKIP-if-absent (CFN creates it in step 4 from S3). The post-close
# (EOD) SF is NOT in CloudFormation — this script is its sole manager — so for it
# we CREATE-if-absent. This keeps the deploy idempotent ACROSS the 2026-06-29
# ne- rename cutover (config#1381), where the renamed SFs do not yet exist on the
# first post-merge deploy. Pre-rename names: alpha-engine-{saturday,weekday,eod}.
echo ""
echo "==> Updating Step Function definitions..."

SAT_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline"
DAILY_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:ne-preopen-trading-pipeline"
EOD_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline"
GROOM_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-groom-dispatch"
# alpha-engine-config-I2544/I2545: CFN-managed pair, same update-if-present/
# defer-to-CFN-if-absent pattern as SAT_ARN/DAILY_ARN (both are
# AWS::StepFunctions::StateMachine resources in the CFN template now).
# Shared SF execution role (the CFN StepFunctionsRoleArn default; all three
# orchestration SFs run under it). Used only for the EOD create-if-absent path.
SF_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/alpha-engine-step-functions-role"

sf_exists() {
    aws stepfunctions describe-state-machine --state-machine-arn "$1" --query "name" --output text >/dev/null 2>&1
}

# Pass the definition via file:// — NOT inline "$(cat ...)". The weekly-freshness
# ASL is ~131 KB and growing (one state per pipeline step); combined with the AWS
# session-token env on the CI runner, an inline arg blows past the effective
# ARG_MAX and the runner aborts with "aws: Argument list too long" (exit 126),
# which silently leaves the live SF stamp behind origin/main HEAD and trips the
# deploy-drift preflight on the next pipeline run. file:// reads from disk, so
# the definition size is bounded only by the SF service limit (1 MB), not ARG_MAX.
# Regression: 2026-06-04 — the Director SF state pushed the ASL over the line.

# CFN-managed SF: update if present, else defer creation to CloudFormation (step 4).
# The 4th arg is a JSON LoggingConfiguration string, passed EXPLICITLY on every
# update (config#2273 deliverable 3): never rely on partial-update semantics to
# preserve the CFN-set logging — a recreate-dropped config (config#1464 class,
# hit live by the ne-* rename config#1381) is self-healed by the next deploy
# instead of lingering until step-functions/check-drift.py pages. The SHAPE
# stays declared in cloudformation/alpha-engine-orchestration.yaml (SoT); the
# literals below must mirror it. Same mechanics as the EOD precedent
# (config#1416) in update_or_create below.
update_or_defer_to_cfn() {
    local arn="$1" stamped="$2" label="$3" logging="${4:-}"
    local logging_args=()
    if [ -n "$logging" ]; then
        logging_args=(--logging-configuration "$logging")
    fi
    if sf_exists "$arn"; then
        aws stepfunctions update-state-machine --state-machine-arn "$arn" --definition "file://$stamped" "${logging_args[@]}" --query "updateDate" --output text
        echo "  $label updated."
    else
        echo "  $label absent — CloudFormation will create it from S3 in step 4 (rename cutover)."
    fi
}

# Standalone SF (not in CFN — EOD, groom): update if present, else create.
# An optional 5th arg is a JSON LoggingConfiguration string — only EOD passes
# one (config#1416); groom's call omits it, preserving its current
# no-logging behavior exactly.
update_or_create() {
    local arn="$1" stamped="$2" name="$3" label="$4" logging="${5:-}"
    local logging_args=()
    if [ -n "$logging" ]; then
        logging_args=(--logging-configuration "$logging")
    fi
    if sf_exists "$arn"; then
        aws stepfunctions update-state-machine --state-machine-arn "$arn" --definition "file://$stamped" "${logging_args[@]}" --query "updateDate" --output text
        echo "  $label updated."
    else
        aws stepfunctions create-state-machine --name "$name" --definition "file://$stamped" --role-arn "$SF_ROLE_ARN" "${logging_args[@]}" --query "stateMachineArn" --output text
        echo "  $label created (was absent — rename cutover)."
    fi
}

# config#1416: EOD execution-log group + LoggingConfiguration, mirroring the
# weekly/preopen pair (config#729/#537). Deliberately NOT a CFN-owned log
# group — this script (and update_eod_pipeline_sf.sh) creates it idempotently
# BEFORE this step's update_or_create() call, since this step (3) runs before
# the CFN stack deploy (step 4) below; a CFN-owned log group would not exist
# yet on the very first deploy after this change merges. CFN owns only the
# metric-filter + alarm that read this log group by its literal name (see
# alpha-engine-orchestration.yaml).
EOD_LOG_GROUP_NAME="/aws/stepfunctions/ne-postclose-trading-pipeline"
EOD_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${EOD_LOG_GROUP_NAME}:*"
echo "  Ensuring EOD log group exists (idempotent)..."
aws logs create-log-group --log-group-name "$EOD_LOG_GROUP_NAME" --region "$REGION" 2>/dev/null || true
aws logs put-retention-policy --log-group-name "$EOD_LOG_GROUP_NAME" --retention-in-days 30 --region "$REGION"
EOD_LOGGING_CONFIG='{"level":"ERROR","includeExecutionData":true,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"'"$EOD_LOG_GROUP_ARN"'"}}]}'

# CFN-pair logging configs — mirror the LoggingConfiguration blocks CFN
# declares on SaturdayPipeline / WeekdayPipeline (level=ERROR,
# includeExecutionData=true, the CFN-owned per-SF log groups).
SAT_LOGGING_CONFIG='{"level":"ERROR","includeExecutionData":true,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"arn:aws:logs:'"$REGION"':'"$ACCOUNT_ID"':log-group:/aws/stepfunctions/ne-weekly-freshness-pipeline:*"}}]}'
DAILY_LOGGING_CONFIG='{"level":"ERROR","includeExecutionData":true,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"arn:aws:logs:'"$REGION"':'"$ACCOUNT_ID"':log-group:/aws/stepfunctions/ne-preopen-trading-pipeline:*"}}]}'

# config#2748-adjacent: groom-dispatch ERROR-level logging was enabled live
# 2026-07-16 (ad hoc, outside this script) to aid debugging active groom-driver
# incidents; codifying it here so deploys stop drifting from live and future
# incident debugging has execution data by default. Log group name matches
# what AWS auto-created under the vendedlogs convention when logging was first
# enabled via update-state-machine without an explicit destination — reusing
# it (not inventing a second /aws/stepfunctions/... group) avoids an orphaned
# duplicate.
GROOM_LOG_GROUP_NAME="/aws/vendedlogs/states/alpha-engine-groom-dispatch"
GROOM_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${GROOM_LOG_GROUP_NAME}:*"
echo "  Ensuring groom-dispatch log group exists (idempotent)..."
aws logs create-log-group --log-group-name "$GROOM_LOG_GROUP_NAME" --region "$REGION" 2>/dev/null || true
aws logs put-retention-policy --log-group-name "$GROOM_LOG_GROUP_NAME" --retention-in-days 30 --region "$REGION"
GROOM_LOGGING_CONFIG='{"level":"ERROR","includeExecutionData":false,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"'"$GROOM_LOG_GROUP_ARN"'"}}]}'

update_or_defer_to_cfn "$SAT_ARN"  "$SAT_STAMPED"  "Weekly-freshness pipeline" "$SAT_LOGGING_CONFIG"
update_or_defer_to_cfn "$DAILY_ARN" "$DAILY_STAMPED" "Pre-open trading pipeline" "$DAILY_LOGGING_CONFIG"
update_or_create "$EOD_ARN" "$EOD_STAMPED" "ne-postclose-trading-pipeline" "Post-close trading pipeline" "$EOD_LOGGING_CONFIG"
# CORRECTED 2026-07-12: was alpha-engine-groom-pipeline (the OLD name) — the EventBridge
# Scheduler targets alpha-engine-groom-dispatch (created by the scheduled-groom-dispatcher
# deploy.sh --bootstrap). Every deploy between config#2129 (2026-07-01) and this fix was
# updating the orphaned groom-pipeline name instead of the live groom-dispatch, which is
# why PRs #761 and #763's SF definition fixes had zero live effect on actual groom runs.
update_or_create "$GROOM_ARN" "$GROOM_STAMPED" "alpha-engine-groom-dispatch" "Backlog groom dispatch" "$GROOM_LOGGING_CONFIG"

# ── 3a. Sync the declared weekly exercise-cadence to SSM (config#6689) ───────
# infrastructure/weekly_cadence.json is the single declared source for whether
# infrastructure/step_function_eod.json's ReadExerciseCadence/CheckExerciseCadence
# states launch the weekly pipeline after every trading day's postclose
# (exercise_cadence="daily", alpha-engine-config#5489), gate it to the Saturday
# cron only ("weekly-only"), or treat no exercise launch as expected at all
# ("off"). Writing it here — in the SAME step that just updated the EOD SF
# definition above — keeps the manifest and the live parameter the SF task
# actually reads from drifting apart by more than one deploy cycle. Flipping
# the cadence is therefore a one-line diff to weekly_cadence.json's
# exercise_cadence field + merge; no SF-topology edit.
#
# NOT YET GRANTED (measured 2026-08-09, recorded in this change's PR body): the
# github-actions-lambda-deploy role (this script's CI identity) has no
# ssm:PutParameter on this resource, so this step fails loud on AccessDenied
# until a nous-ergon-ops IAM PR lands the grant. Failing loud here is
# deliberate — a swallowed AccessDenied would leave the manifest and SSM
# silently out of sync, which is exactly the two-surface bug config#6689 was
# filed to close. weekly_cadence_drift.py --check catches the same gap
# out-of-band once IAM exists.
echo ""
echo "==> Syncing declared weekly exercise-cadence to SSM (config#6689)..."
python3 "$SCRIPT_DIR/weekly_cadence_drift.py" --enforce

# ── 3b. Ensure EventBridge Scheduler trust on the SF-target role (config#2413) ─
# The CFN template's WeekdayPipelineSchedule (AWS::Scheduler::Schedule, migrated
# from AWS::Events::Rule in #816) is fired by EventBridge Scheduler, which — at
# CreateSchedule/UpdateSchedule time — validates that its RoleArn
# (EventBridgeSfnRoleArn → alpha-engine-eventbridge-sfn-role) is assumable by
# scheduler.amazonaws.com. Before #816 this role backed only AWS::Events::Rule
# targets, so its trust policy listed only events.amazonaws.com. The
# scheduler.amazonaws.com grant was added ONLY to the manual deploy_step_function.sh
# (no CI workflow runs it) — so the FIRST post-merge run of THIS workflow deploys
# the Scheduler resource against a role that does not yet trust it, and the CREATE
# rolls the stack back (UPDATE_ROLLBACK_COMPLETE). A `--no-execute-changeset` dry
# run does NOT exercise the target-role trust, which is why #816 validated clean
# but failed on live apply.
#
# Ensuring the trust HERE — idempotently, before the CFN deploy, mirroring the
# log-group prerequisites above and deploy_step_function.sh's own bootstrap —
# so both principals must be listed. The role is shared by BOTH the still-live
# SaturdayTrigger (events.amazonaws.com) and the now-Scheduler weekday trigger
# (scheduler.amazonaws.com).
#
# This step VERIFIES the trust; it no longer WRITES it (#1075 follow-up).
#
# It used to call update-assume-role-policy against
# infrastructure/iam/alpha-engine-eventbridge-sfn-role.trust.json. Commit
# 506be30 ("infra: remove the IAM tree — now owned by nous-ergon-ops", #1075)
# deleted that tree and this line was missed, so every run from 15:34 UTC on
# 2026-07-28 died here with "Unable to load paramfile … No such file or
# directory" (9 consecutive Deploy Infrastructure failures; see
# alpha-engine-config-I5271).
#
# Restoring the write is NOT the fix. Per nous-ergon-ops
# policies/infrastructure-ownership-policy.md §35, IAM roles/policies/trust
# documents were consolidated to nous-ergon-ops/infrastructure/iam/ on
# 2026-07-27; this repo no longer owns them. That repo's own
# iam-apply-on-merge.yml deliberately withholds iam:UpdateAssumeRolePolicy
# from its CI identity — "a CI identity that can rewrite trust policies is a
# privilege-escalation path" (alpha-engine-config#3150) — so trust is applied
# as a one-time bootstrap and thereafter guarded by check-drift.py's daily
# 09:35 UTC _check_trust. A deploy identity in THIS repo rewriting that
# document on every run is the same escalation path, one repo over.
#
# The #816 guarantee (EventBridge Scheduler validates RoleArn assumability at
# CreateSchedule/UpdateSchedule time, and a --no-execute-changeset dry run does
# not exercise it, so a missing principal surfaces only as an mid-apply
# UPDATE_ROLLBACK_COMPLETE) now lives with the OWNER of the document rather
# than here: nous-ergon-ops check-drift.py's _check_trust runs daily at 09:35
# UTC against the codified trust-policy.json.
#
# This deploy performs NO IAM call at all. PR #1093 first tried to keep a
# read-only `aws iam get-role` assertion here, which failed for a reason worth
# recording: the GHA deploy identity was granted `iam:UpdateAssumeRolePolicy`
# on this one role and nothing else, so it could WRITE the trust document but
# not READ it —
#
#   AccessDenied: User: .../github-actions-lambda-deploy/GitHubActions is not
#   authorized to perform: iam:GetRole on resource: role
#   alpha-engine-eventbridge-sfn-role
#
# Broadening the deploy identity to read IAM would move in exactly the wrong
# direction for a repo that no longer owns IAM, so the check is dropped
# instead. RESIDUAL RISK, stated rather than hidden: a trust document missing
# scheduler.amazonaws.com is no longer caught before the CFN apply and will
# present as an UPDATE_ROLLBACK_COMPLETE — the #816 symptom. The daily ops
# drift check is what closes that window now; if that proves too slow in
# practice, the fix is a scheduled ops-side assertion, not an IAM grant here.

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

# Stack tags: git-sha (deploy-drift stamp) and system (fleet cost-attribution
# key, alpha-engine-config-I10905). Stack-level tags propagate to every taggable
# resource, so the deploy role needs the matching *:TagResource / ListTags*
# actions per resource type; github-actions-cfn-sf-stack-deploy already holds
# them (git-sha changes on every deploy and exercises the same calls).
if [ "$STACK_STATUS" = "DOES_NOT_EXIST" ]; then
    echo "  Creating new stack..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-url "$TEMPLATE_URL" \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags "Key=git-sha,Value=$GIT_SHA" "Key=system,Value=alpha-engine-orchestration" \
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
        --template-url "$TEMPLATE_URL" \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags "Key=git-sha,Value=$GIT_SHA" "Key=system,Value=alpha-engine-orchestration" \
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
        echo "  No updates needed (template + git-sha + system tags both current)."
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
