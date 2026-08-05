#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-scheduled-groom-dispatcher Lambda,
# the alpha-engine-groom-dispatch Step Function that wraps it, and wire the
# EventBridge Scheduler rules to start SF executions (config#1322, #1432, #1472).
#
# Each Scheduler rule now starts an execution of the alpha-engine-groom-dispatch
# STEP FUNCTION (config#1472 — was: directly invoking the Lambda). The SF's sole
# job is orchestration/observability: invoke this Lambda (UNCHANGED — it still
# launches the spot box + fires the async SSM command exactly as before), then
# poll the SSM command to a real terminal status so the SF's own execution
# status (SUCCEEDED/FAILED) is truthful — which is what plugs the groom into the
# EXISTING Fleet-SF Watch resilience/observability agent (the same mechanism
# that already watches the Saturday/weekday/EOD pipelines), instead of the
# bespoke external liveness-probe Lambda (nousergon-data#556). The heavy
# grooming logic is completely unchanged — nothing about scripts/groom_run.sh
# or the spot box's own self-termination changes.
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole (the
# executor role) + ssm:SendCommand. The BOX reads all secrets itself from SSM via
# its instance profile (alpha-engine-executor-role → ssm:GetParameter on
# /alpha-engine/*), so the Lambda needs NO secret access. The SF's OWN execution
# role (sf-execution-iam-policy.json) needs only lambda:InvokeFunction (on this
# Lambda) + ssm:GetCommandInvocation (to poll) + sns:Publish (to alert on
# failure) — it never touches secrets or launches anything itself.
#
# alpha-engine-config-I5371 adds one further grant: states:ListExecutions,
# scoped to the alpha-engine-groom-dispatch state machine ONLY. The decide phase
# uses it to enforce a CYCLE SINGLETON — it lists RUNNING siblings of its own
# execution and skips when an earlier cycle is still alive. Read-only, and the
# narrowest resource scope the API allows.
#
# That grant is LOAD-BEARING FOR AVAILABILITY, not just correctness:
# _concurrent_cycle_blockers() fails CLOSED, so an AccessDenied is treated as
# "cannot establish singleton" and skips the cycle. A deploy that ships the code
# without the policy therefore stops ALL grooming, silently, until the grant
# lands. Run `deploy.sh --apply-iam` BEFORE (or with) any deploy that first
# introduces it — the GHA auto-deploy OIDC role deliberately lacks
# iam:PutRolePolicy (fleet single-writer rule), so a code-only deploy cannot
# apply it for you.
#
# Cadence (UTC). Three symmetric demand-all triggers per day + Sunday extra slot.
# Each trigger evaluates the FULL backlog and launches 0..3 tier boxes via
# decide_trigger / _primary_backend_for. All tiers route through DeepSeek primary.
# The end-of-SF sweep (DispatchEndOfSfSweep in step_function_groom.json) continues
# to run unconditionally after every trigger cycle — it covers the PR set
# regardless of how many groom boxes launch.
#  04:00 daily     cron(0 4 * * ? *)         FULL   demand-all  # 9pm PT
#  12:00 daily     cron(0 12 * * ? *)        FULL   demand-all  # 5am PT
#  20:00 daily     cron(0 20 * * ? *)        FULL   demand-all  # 1pm PT
#  Sun 09:00       cron(0 9 ? * SUN *)       FULL   demand-all  # weekly extra slot
#  Models: krepis.router selects per GROOM_ROUTER_CLASS — zero Anthropic
#
# SCHED_NAMES is the source of truth: any live scheduler rule under the
# alpha-engine-scheduled-groom- prefix that is NOT in SCHED_NAMES is PRUNED
# (deleted) on deploy, so removing a cadence here removes it live too.
#
# alpha-engine-config-I6461 (groom-sweep-policy §2.9/§4.8):
# ../../scheduler/schedule-manifest.json is the declarative record of every
# trigger below — name, stated reason, and (for the sweep rules) the
# attended-window flag — checked against these arrays by
# ../../scheduler/check-manifest-drift.py. GROOM_TARGET_DETECTION_LATENCY_MIN
# and the GROOM_ATTENDED_WINDOW_* env vars set on the Lambda below (steps 2b/
# 3) are bound to the SAME manifest by
# tests/test_attended_window_gate.py::test_env_defaults_match_manifest — edit
# the manifest first, then these values, never only one.
#
# Managed OUTSIDE CloudFormation — same rationale as the sibling dispatchers
# (keeps the github-actions-lambda-deploy OIDC role's blast radius narrow: it
# deliberately lacks iam:CreateRole/iam:PutRolePolicy, a fleet-wide policy
# after 4 IAM-clobber incidents in 2 months — see infrastructure/iam/README.md).
#
# CODE, SF DEFINITION and SCHEDULES all auto-deploy on merge to main via
# `.github/workflows/deploy-scheduled-groom-dispatcher.yml`, which runs this
# script twice: flagless (Lambda code + SF definition) and then
# `--reconcile-schedules` (EventBridge Scheduler upsert + prune), both under
# the github-actions-lambda-deploy OIDC role.
#
# alpha-engine-config-I5805 is why the schedule half is here at all. It used to
# sit inside --bootstrap next to the IAM-role creation, so the OIDC role — which
# deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet single-writer rule
# after 4 IAM-clobber incidents in 2 months) — could not run it, and a merged
# cadence change did nothing until an operator ran a command by hand. It was not
# run: nousergon-data#1179 added three independent sweep schedules on 2026-07-30
# and none of them was ever created. Splitting on the IAM boundary rather than
# on "first time vs not" is what makes the schedule half CI-runnable.
#
# What still needs an operator: --bootstrap-iam, i.e. creating the three IAM
# roles and their inline policies, plus first-ever creation of the Lambda and
# the Step Function. That is genuinely first-time-only.
#
# CUTOVER (config#1432, historical): after a manual --smoke spot run validates
# end-to-end, deploy this AND disable the GHA `schedule:` crons in
# backlog-groom.yml together (so there is no double-groom and no gap).
# NOTE: --smoke fires a REAL groom on a REAL spot box.
#
# Usage:
#   bash .../deploy.sh                        # code + SF definition (the CI path, step 1)
#   bash .../deploy.sh --reconcile-schedules  # + upsert/prune Scheduler rules (the CI path, step 2)
#   bash .../deploy.sh --bootstrap-iam        # operator-only: create IAM roles, Lambda, SF
#   bash .../deploy.sh --bootstrap            # both of the above (unchanged meaning)
#   bash .../deploy.sh --apply-iam            # re-apply iam-policy.json only (config#2825)
#   bash .../deploy.sh --dry-run              # show actions, do not apply
#   bash .../deploy.sh --smoke                # synthetic schedule event (⚠ fires a REAL groom)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-scheduled-groom-dispatcher"
ROLE_NAME="alpha-engine-scheduled-groom-dispatcher-role"
POLICY_NAME="alpha-engine-scheduled-groom-dispatcher-policy"
# The Step Function that wraps the Lambda (config#1472). "dispatch" (not
# "-pipeline") to distinguish it from the fleet's trading-orchestration SFs
# while still reading clearly as the dispatch mechanism.
SF_NAME="alpha-engine-groom-dispatch"
SF_ROLE_NAME="alpha-engine-groom-sf-role"
SF_POLICY_NAME="alpha-engine-groom-sf-policy"
SF_DEFINITION_FILE="${SCRIPT_DIR}/../../step_function_groom.json"
# EventBridge Scheduler execution role (assumed by scheduler.amazonaws.com to
# START THE SF). Single-target blast radius: states:StartExecution on this SF
# only (config#1472 — was lambda:InvokeFunction directly on the Lambda).
SCHED_ROLE_NAME="alpha-engine-scheduled-groom-dispatcher-scheduler-role"
SCHED_POLICY_NAME="start-execution-groom-dispatch"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SF_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SF_NAME}"
SF_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SF_ROLE_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

# Schedule definitions: name | cron expression (UTC) | JSON input (run_mode +
# schedule label, plus model/issue_filter per tier — config#1760 tier-split).
# deploy.sh prune drops orphaned rule names when cadence changes.
SCHED_NAMES=(
  "alpha-engine-scheduled-groom-0400-daily"
  "alpha-engine-scheduled-groom-1200-daily"
  "alpha-engine-scheduled-groom-2000-daily"
  "alpha-engine-scheduled-groom-sun0900-weekly"
)
SCHED_CRONS=(
  "cron(0 4 * * ? *)"
  "cron(0 12 * * ? *)"
  "cron(0 20 * * ? *)"
  "cron(0 9 ? * SUN *)"
)
SCHED_INPUTS=(
  '{"run_mode":"full","trigger":"demand-all","pr_budget":100,"schedule":"0 4 * * *"}'
  '{"run_mode":"full","trigger":"demand-all","pr_budget":100,"schedule":"0 12 * * *"}'
  '{"run_mode":"full","trigger":"demand-all","pr_budget":100,"schedule":"0 20 * * *"}'
  '{"run_mode":"full","trigger":"demand-all","pr_budget":100,"schedule":"0 9 * * 0"}'
)
# Prefix used to discover live rules for prune reconciliation (see step 2f).
SCHED_PREFIX="alpha-engine-scheduled-groom-"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops
# instead of silently running the real deploy path (alpha-engine-config-
# I2752 incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked
# here, matching other tools' convention, and triggered a real deploy).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
# alpha-engine-config-I5805 splits the old monolithic --bootstrap in two, along
# the line that actually matters: which grants the caller needs.
#
#   BOOTSTRAP_IAM       creates IAM roles and puts inline policies. Needs
#                       iam:CreateRole + iam:PutRolePolicy, which the GHA OIDC
#                       role deliberately does NOT have (fleet single-writer
#                       rule after 4 IAM-clobber incidents). Operator-only, and
#                       first-time-only in practice.
#   RECONCILE_SCHEDULES upserts + prunes the EventBridge Scheduler rules. Needs
#                       ONLY scheduler:{Get,List,Create,Update,Delete}Schedule
#                       and a scoped iam:PassRole — all of which
#                       github-actions-lambda-deploy already holds. Safe to run
#                       on every merge, and now does.
#
# --bootstrap keeps its old meaning (both) so every runbook, every comment and
# every operator habit that predates the split still does what it says.
BOOTSTRAP_IAM=false
RECONCILE_SCHEDULES=false
APPLY_IAM=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP_IAM=true; RECONCILE_SCHEDULES=true ;;
    --bootstrap-iam) BOOTSTRAP_IAM=true ;;
    --reconcile-schedules) RECONCILE_SCHEDULES=true ;;
    --apply-iam) APPLY_IAM=true ;;
    --smoke) SMOKE=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# `--reconcile-schedules` ALONE is a schedules-only run: it skips packaging
# (docker pip + handler tests), the code push and the SF definition push. The
# CI deploy therefore invokes this script twice without paying for the ~2min
# package build twice, and an operator repairing live schedule drift does not
# have to redeploy code to do it. Combined with --bootstrap or run flagless,
# everything happens as before.
CODE_DEPLOY=true
if $RECONCILE_SCHEDULES && ! $BOOTSTRAP_IAM && ! $APPLY_IAM && ! $SMOKE; then
  CODE_DEPLOY=false
fi

run() {
  if $DRY_RUN; then
    echo "DRY: $*"
  else
    "$@"
  fi
}

# ----- 0. Scratch dir + validate handler syntax -----------------------------
# PKG (the Lambda-zip staging dir) is created up front; the shared handler-
# test gate (0b) provisions its OWN scratch dir for pytest + deps (config#2381).

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

if $CODE_DEPLOY; then

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 0b. Preflight handler unit tests --------------------------------------
# Hermetic for AWS: boto3 + nousergon_lib.ec2_spot are stubbed in sys.modules
# before `import index` (see test_handler.py). nousergon_lib.flow_doctor_fleet
# is pure stdlib — install the REAL pinned enum from requirements.txt so the
# hand-maintained FleetTelegramTopic fake cannot drift (config#1772). It is
# passed to the shared gate, which lands it in its own scratch dir — NOT the
# caller's global site-packages, not bundled into the Lambda zip. (krepis was
# removed 2026-07-14 with the pace gate — usage pacing dismantled.)
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
NOUSERGON_LIB_REQ=$(grep -E '^nousergon-lib' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" "${NOUSERGON_LIB_REQ}"

# ----- 1. Package: pip install deps + zip handler ---------------------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/../flow_doctor_telegram.py" "${PKG}/flow_doctor_telegram.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

else
  echo "Schedules-only run (--reconcile-schedules): skipping handler tests + packaging."
fi

# ----- 2. IAM + resource bootstrap (operator-only, first-time only) ---------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied."
fi

if $BOOTSTRAP_IAM; then
  echo "Bootstrapping IAM + resources for ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline policy ---
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for IAM role propagation..."
    sleep 10
  fi

  # --- 2b. Lambda function ---
  # GROOM_MAX_DISPATCHES_DAILY (config#3173, generalizing config#2269's
  # sf-watch pattern): pinned here (not left to index.py's own default) so a
  # live env tweak never silently survives a redeploy — same rationale as
  # sf-watch's SF_WATCH_MAX_DISPATCHES_* (config#1818 lesson). Change via a
  # PR editing this value, not a console edit.
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 300 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,GROOM_DISPATCH_ENABLED=true,GROOM_MAX_DISPATCHES_DAILY=40,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1,GROOM_PRIMARY_DEEPSEEK_TIERS="low,mid,high",GROOM_ATTENDED_WINDOW_TZ="America/Los_Angeles",GROOM_ATTENDED_WINDOW_START_HOUR="8",GROOM_ATTENDED_WINDOW_END_HOUR="21",GROOM_TARGET_DETECTION_LATENCY_MIN="240"}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # --- 2c. The groom-dispatch Step Function (config#1472) ---
  SF_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SF_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating SF execution role: ${SF_ROLE_NAME}"
    run aws iam create-role \
      --role-name "${SF_ROLE_NAME}" \
      --assume-role-policy-document "${SF_TRUST}" \
      --description "Execution role for ${SF_NAME} - invokes the groom Lambda + polls SSM (config#1472)" \
      --query 'Role.RoleName' --output text
  else
    echo "  SF execution role exists: ${SF_ROLE_NAME}"
  fi
  echo "  Applying SF policy: ${SF_POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${SF_ROLE_NAME}" \
    --policy-name "${SF_POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/sf-execution-iam-policy.json"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for SF role propagation..."
    sleep 10
  fi

  # CREATE only. The UPDATE half moved to step 3b, which runs unconditionally
  # (alpha-engine-config-I5805) — see the comment there for why.
  if ! aws stepfunctions describe-state-machine --state-machine-arn "${SF_ARN}" \
      --query 'name' --output text >/dev/null 2>&1; then
    echo "  Creating Step Function: ${SF_NAME}"
    run aws stepfunctions create-state-machine \
      --name "${SF_NAME}" \
      --definition "file://${SF_DEFINITION_FILE}" \
      --role-arn "${SF_ROLE_ARN}" \
      --type STANDARD \
      --logging-configuration "level=ERROR,includeExecutionData=false,destinations=[{cloudWatchLogsLogGroup={logGroupArn=arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:/aws/vendedlogs/states/${SF_NAME}:*}}]" \
      --region "${REGION}" \
      --query 'stateMachineArn' --output text
  else
    echo "  Step Function exists — definition update happens in step 3b"
  fi

  # --- 2d. EventBridge Scheduler execution role (start THIS SF only) ---
  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role \
      --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: start ${SF_NAME} on the groom cadence" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["states:StartExecution"],"Resource":"${SF_ARN}"}]}
EOF
)
  echo "  Applying Scheduler start-execution policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "${SCHED_INVOKE_POLICY}"

  # The reconciler and the sweep rules both target the LAMBDA directly rather
  # than the SF, so the Scheduler role needs lambda:InvokeFunction on top of
  # states:StartExecution. This grant used to live inside block 2e-bis, next to
  # the rule it serves — which meant the whole schedule section required
  # iam:PutRolePolicy and could therefore never run in CI. Grants live here;
  # rules live below (alpha-engine-config-I5805).
  echo "  Applying Scheduler invoke-Lambda policy (reconciler + sweep rules)"
  run aws iam put-role-policy \
    --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}-recon-invoke" \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"${FN_ARN}\"}]}"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for Scheduler role propagation..."
    sleep 10
  fi
fi

# ----- 2bis. Schedule reconciliation (IAM-free — runs on every merge) --------
#
# alpha-engine-config-I5805. Everything below upserts or prunes EventBridge
# Scheduler rules and nothing below writes IAM, so github-actions-lambda-deploy
# can run it. Before the split these blocks sat inside --bootstrap alongside the
# role creation, so a merged cadence change was inert until an operator
# remembered a manual command. It was not remembered: nousergon-data#1179 gave
# the PR sweep its own three daily schedules on 2026-07-30 and they were never
# created — the sweep stayed coupled to groom health, which is the exact defect
# that PR was written to remove.
#
# Note this block does NOT gate on the rules already existing. It is a
# reconciler: source is the truth, live is made to match, every run.

if $RECONCILE_SCHEDULES; then
  echo "Reconciling EventBridge Scheduler rules against source..."

  # --- 2e. The EventBridge Scheduler rules (target = SF, not the Lambda) ---
  for i in "${!SCHED_NAMES[@]}"; do
    name="${SCHED_NAMES[$i]}"
    cron="${SCHED_CRONS[$i]}"
    input="${SCHED_INPUTS[$i]}"
    target=$(cat <<EOF
{"Arn":"${SF_ARN}","RoleArn":"${SCHED_ROLE_ARN}","Input":"$(printf '%s' "$input" | sed 's/"/\\"/g')"}
EOF
)
    if aws scheduler get-schedule --name "${name}" --region "${REGION}" \
        --query 'Name' --output text >/dev/null 2>&1; then
      echo "  Updating Scheduler rule: ${name} → ${cron}"
      run aws scheduler update-schedule \
        --name "${name}" \
        --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" \
        --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" \
        --region "${REGION}" \
        --query 'ScheduleArn' --output text
    else
      echo "  Creating Scheduler rule: ${name} → ${cron}"
      run aws scheduler create-schedule \
        --name "${name}" \
        --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" \
        --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" \
        --region "${REGION}" \
        --query 'ScheduleArn' --output text
    fi
    # Fail-loud: verify it landed.
    if ! $DRY_RUN; then
      aws scheduler get-schedule --name "${name}" --region "${REGION}" \
        --query 'Name' --output text >/dev/null \
        || { echo "ERROR: Scheduler rule ${name} not found after create/update" >&2; exit 1; }
    fi
  done

  # --- 2e-bis. The lane-death reconciler rule (alpha-engine-config-I5229).
  #
  # Deliberately NOT in SCHED_NAMES: every rule there targets the STEP FUNCTION
  # and starts a dispatch cycle. This one targets the LAMBDA directly with
  # {"mode":"reconcile"} and must never start a cycle. It also sits outside
  # SCHED_PREFIX so step 2f's prune reconciliation does not delete it.
  #
  # Why it must exist at all: groom-sweep-policy §2.7 makes the reconciler the
  # property the others depend on, and §2.2 holds that a recovery path which has
  # never executed is presumed broken. A reconciler with no trigger is exactly
  # that — the code merges, the tests pass, and nothing ever runs it. Every
  # 5 minutes is well inside the ~6h lane budget it is watching and costs one
  # short Lambda invocation.
  RECON_SCHED_NAME="alpha-engine-groom-lane-reconciler-5min"
  RECON_SCHED_CRON="rate(5 minutes)"
  # Built with python3, NOT a heredoc. In an unquoted heredoc bash unescapes
  # only \$, \`, \\ and \<newline> — a backslash before a double quote is
  # PRESERVED. So `\\\"` emitted `\\"` rather than `\"`, and every
  # create-schedule call died on `Invalid JSON: Expecting ',' delimiter`.
  # Counting backslashes through two layers of quoting is not a thing to get
  # right by inspection; json.dumps cannot get it wrong.
  # The lambda:InvokeFunction grant this rule depends on is applied in block 2d
  # (--bootstrap-iam), not here — see the note there.
  recon_target=$(python3 -c 'import json,sys; print(json.dumps({"Arn": sys.argv[1], "RoleArn": sys.argv[2], "Input": json.dumps({"mode": "reconcile"})}))' "${FN_ARN}" "${SCHED_ROLE_ARN}")

  if aws scheduler get-schedule --name "${RECON_SCHED_NAME}" --region "${REGION}" \
      --query 'Name' --output text >/dev/null 2>&1; then
    echo "  Updating reconciler rule: ${RECON_SCHED_NAME} → ${RECON_SCHED_CRON}"
    run aws scheduler update-schedule \
      --name "${RECON_SCHED_NAME}" \
      --schedule-expression "${RECON_SCHED_CRON}" \
      --flexible-time-window '{"Mode":"OFF"}' \
      --target "${recon_target}" \
      --region "${REGION}" \
      --query 'ScheduleArn' --output text
  else
    echo "  Creating reconciler rule: ${RECON_SCHED_NAME} → ${RECON_SCHED_CRON}"
    run aws scheduler create-schedule \
      --name "${RECON_SCHED_NAME}" \
      --schedule-expression "${RECON_SCHED_CRON}" \
      --flexible-time-window '{"Mode":"OFF"}' \
      --target "${recon_target}" \
      --region "${REGION}" \
      --query 'ScheduleArn' --output text
  fi
  if ! $DRY_RUN; then
    aws scheduler get-schedule --name "${RECON_SCHED_NAME}" --region "${REGION}" \
      --query 'Name' --output text >/dev/null \
      || { echo "ERROR: reconciler rule ${RECON_SCHED_NAME} not found after create/update" >&2; exit 1; }
  fi

  # --- 2e-ter. INDEPENDENT sweep schedules (Brian's ruling 2026-07-30).
  #
  # Until now the PR sweep ran only as DispatchEndOfSfSweep, the last state of
  # the groom cycle — so sweep frequency was hostage to groom health. That is
  # not theoretical: on 2026-07-30 two of three cycles ended in
  # `concurrent_cycle_skip`, and the sweep did not run on either. The sweep has
  # no dependency on groom OUTPUT (it classifies open PRs), so the coupling
  # bought nothing.
  #
  # These rules give it its own trigger, interleaved 4h after each groom:
  #
  #   groom  04:00 12:00 20:00 UTC   (9pm / 5am / 1pm PT — unchanged)
  #   sweep  00:00 08:00 16:00 UTC   (5pm / 1am / 9am PT)
  #
  # DispatchEndOfSfSweep is deliberately KEPT. groom-sweep-policy §2.5 makes it
  # the unconditional tail-catcher — "making it conditional on groom success
  # starves it exactly when it is most needed" — and removing it is an
  # orchestration change requiring a policy amendment, not a schedule tweak.
  # Keeping both yields a sweep roughly every 4h at one cheap box per run.
  #
  # Collisions are already handled: the dispatcher tags sweep boxes
  # `groom-issue-filter=sweep`, distinct from every groom tier tag, so the
  # config#1979 concurrent guard skips when a prior sweep box is still live.
  #
  # NAMED OUTSIDE SCHED_PREFIX on purpose. Step 2f prunes any live rule under
  # `alpha-engine-scheduled-groom-` that is absent from SCHED_NAMES, and
  # SCHED_NAMES entries target the STEP FUNCTION (a full groom cycle). These
  # target the LAMBDA with run_mode=sweep, so a shared prefix would have them
  # deleted on the next deploy.
  SWEEP_SCHED_NAMES=(
    "alpha-engine-groom-sweep-0000-daily"
    "alpha-engine-groom-sweep-0800-daily"
    "alpha-engine-groom-sweep-1600-daily"
  )
  SWEEP_SCHED_CRONS=(
    "cron(0 0 * * ? *)"
    "cron(0 8 * * ? *)"
    "cron(0 16 * * ? *)"
  )
  # Mirrors the DispatchEndOfSfSweep payload in step_function_groom.json:
  # a literal launch_decided sweep event. `issue_filter` is inert for
  # run_mode=sweep but the launch path validates it, so it is passed
  # explicitly rather than defaulted.
  for i in "${!SWEEP_SCHED_NAMES[@]}"; do
    sname="${SWEEP_SCHED_NAMES[$i]}"
    scron="${SWEEP_SCHED_CRONS[$i]}"
    sweep_target=$(python3 -c 'import json,sys; print(json.dumps({"Arn": sys.argv[1], "RoleArn": sys.argv[2], "Input": json.dumps({"run_mode": "sweep", "launch_decided": True, "model": "deepseek-v4-flash", "issue_filter": "mid-only", "schedule": sys.argv[3]})}))' "${FN_ARN}" "${SCHED_ROLE_ARN}" "${sname}")
    if aws scheduler get-schedule --name "${sname}" --region "${REGION}" \
        --query 'Name' --output text >/dev/null 2>&1; then
      echo "  Updating sweep rule: ${sname} → ${scron}"
      run aws scheduler update-schedule --name "${sname}" \
        --schedule-expression "${scron}" --schedule-expression-timezone "UTC" \
        --flexible-time-window '{"Mode":"OFF"}' --target "${sweep_target}" \
        --region "${REGION}" --query 'ScheduleArn' --output text
    else
      echo "  Creating sweep rule: ${sname} → ${scron}"
      run aws scheduler create-schedule --name "${sname}" \
        --schedule-expression "${scron}" --schedule-expression-timezone "UTC" \
        --flexible-time-window '{"Mode":"OFF"}' --target "${sweep_target}" \
        --region "${REGION}" --query 'ScheduleArn' --output text
    fi
    if ! $DRY_RUN; then
      aws scheduler get-schedule --name "${sname}" --region "${REGION}" \
        --query 'Name' --output text >/dev/null \
        || { echo "ERROR: sweep rule ${sname} not found after create/update" >&2; exit 1; }
    fi
  done

  # --- 2f. Prune reconciliation: delete any live rule under SCHED_PREFIX that is
  # no longer in SCHED_NAMES (so dropping a cadence above removes it live too,
  # rather than silently orphaning a still-firing schedule). Added 2026-06-29
  # alongside the 3->2/day reduction (the 1500-sunfri rule is the first prunee).
  echo "  Pruning orphaned Scheduler rules under prefix ${SCHED_PREFIX}..."
  LIVE_RULES=$(aws scheduler list-schedules --name-prefix "${SCHED_PREFIX}" \
    --region "${REGION}" --query 'Schedules[].Name' --output text 2>/dev/null || echo "")
  for live in ${LIVE_RULES}; do
    keep=false
    for want in "${SCHED_NAMES[@]}"; do
      [ "${live}" = "${want}" ] && { keep=true; break; }
    done
    if ! $keep; then
      echo "    Deleting orphaned Scheduler rule: ${live}"
      run aws scheduler delete-schedule --name "${live}" --region "${REGION}"
      if ! $DRY_RUN; then
        aws scheduler get-schedule --name "${live}" --region "${REGION}" \
          --query 'Name' --output text >/dev/null 2>&1 \
          && { echo "ERROR: Scheduler rule ${live} still present after delete" >&2; exit 1; }
      fi
    fi
  done
fi

# ----- 3. Update function code (always after bootstrap, idempotent) ---------

if $CODE_DEPLOY; then

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

echo "✓ Code deployed."

# ----- 3b. Update the Step Function definition (always, idempotent) ----------
#
# alpha-engine-config-I5805. This used to live in block 2c, behind --bootstrap,
# which made a definition-only change inert on merge. That is not theoretical
# and it is already documented as fixed when it was not: I5512 added
# `infrastructure/step_function_groom.json` to the deploy workflow's path
# filter, so the workflow FIRED on a definition change — and then ran the
# flagless, code-only path, which never touched the state machine. The trigger
# was correct and the deploy was a no-op, which is worse than not firing: the
# green check reads as "deployed".
#
# The definition is code. It deploys on every run, exactly like index.py above.
# github-actions-lambda-deploy already holds states:UpdateStateMachine on
# `alpha-engine-groom-dispatch` (InfraDeploySFDefinition) and a scoped
# iam:PassRole on `alpha-engine-*` (InfraDeployPassRole) — no new grant.
#
# Fail-loud if the state machine is absent: that means a first-time deploy that
# skipped --bootstrap-iam, and silently continuing would leave the schedules
# below pointing at an ARN that does not resolve.
echo "Updating Step Function definition: ${SF_NAME}"
if aws stepfunctions describe-state-machine --state-machine-arn "${SF_ARN}" \
    --query 'name' --output text >/dev/null 2>&1; then
  run aws stepfunctions update-state-machine \
    --state-machine-arn "${SF_ARN}" \
    --definition "file://${SF_DEFINITION_FILE}" \
    --role-arn "${SF_ROLE_ARN}" \
    --region "${REGION}" \
    --query 'updateDate' --output text
  echo "  ✓ Definition deployed."
elif $DRY_RUN; then
  echo "DRY: state machine ${SF_NAME} absent — would require --bootstrap-iam"
else
  echo "ERROR: state machine ${SF_NAME} does not exist. Run with --bootstrap-iam first." >&2
  exit 1
fi

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,GROOM_DISPATCH_ENABLED=true,GROOM_MAX_DISPATCHES_DAILY=40,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1,GROOM_PRIMARY_DEEPSEEK_TIERS="low,mid,high",GROOM_ATTENDED_WINDOW_TZ="America/Los_Angeles",GROOM_ATTENDED_WINDOW_START_HOUR="8",GROOM_ATTENDED_WINDOW_END_HOUR="21",GROOM_TARGET_DETECTION_LATENCY_MIN="240"}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

fi  # end CODE_DEPLOY (steps 3, 3b and the env update)

# ----- 4. Smoke (synthetic schedule event, via the SF — the real live path) --

if $SMOKE; then
  echo ""
  echo "Smoke-testing via SF start-execution (synthetic schedule event, run_mode=full)..."
  echo "⚠ this starts a REAL execution of ${SF_NAME}, which invokes the Lambda and fires a REAL groom."
  EXEC_ARN=$(aws stepfunctions start-execution \
    --state-machine-arn "${SF_ARN}" \
    --input '{"run_mode":"full","schedule":"smoke-test"}' \
    --region "${REGION}" \
    --query 'executionArn' --output text)
  echo "Started execution: ${EXEC_ARN}"
  echo "(async — poll with: aws stepfunctions describe-execution --execution-arn ${EXEC_ARN} --region ${REGION})"
  RESP=$(mktemp)
  echo "{\"executionArn\": \"${EXEC_ARN}\"}" > "${RESP}"
  cat "${RESP}"
  echo ""
  rm -f "${RESP}"
fi
