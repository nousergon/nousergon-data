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
# Cadence (UTC). Reduced 3->2/day on 2026-06-29 (the 15:00 UTC / 8am-PT run was
# dropped per usage pacing); a 3rd schedule was re-added 2026-07-01 (config#1495
# follow-up) at the SAME 15:00 UTC slot, now running a DIFFERENT tier — Opus,
# complexity:high ONLY — not a reinstatement of the old Sonnet drain-phase run.
# UNIFORM 3x/day, all 7 days, since 2026-07-02: the Sat-skip on the 07:00 slot
# (originally "avoid colliding with the 09:00 UTC Saturday pipeline") was never
# evidence-based — investigated and confirmed the groom and the weekly SF share
# NEITHER the Claude Max quota (groom = Max-plan OAuth token; weekly SF Research/
# Predictor = separate pay-as-you-go ANTHROPIC_API_KEY) NOR EC2 spot capacity
# (disjoint instance families: groom t3/t3a/t2.medium vs weekly-SF c5/m5/c6i/c5a/
# r5/r5a/r6i.large). No exceptions kept — the weekly SF can also now land on any
# day (e.g. Friday, per the holiday-aware weekly-schedule-adjuster, #578) without
# the groom cadence needing to track it.
# Off-peak tier-split cadence (2026-07-07): avoid Anthropic Max weekday peak
# (5–11am PT / 12:00–18:00 UTC PDT) and Brian's interactive hours where possible.
# config#2409 (2026-07-13): the 01:00 high-only slot moved off Opus onto Sonnet
# — dedicated queue/budget/off-peak schedule, no longer a distinct model tier.
#   01:00 daily     cron(0 1 * * ? *)         FULL   Sonnet, high-only      # 6pm PT, every day
#   07:00 daily     cron(0 7 * * ? *)         FULL   Sonnet, mid-only       # 12am PT, every day
#   19:00 daily     cron(0 19 * * ? *)        FULL   Haiku,  low-only       # 12pm PT, every day
#   Sun 09:00       cron(0 9 ? * SUN *)       FULL   Haiku,  gated-reverify # weekly stale-gate lane (config#1891)
#
# SCHED_NAMES is the source of truth: any live scheduler rule under the
# alpha-engine-scheduled-groom- prefix that is NOT in SCHED_NAMES is PRUNED
# (deleted) on deploy, so removing a cadence here removes it live too.
#
# Managed OUTSIDE CloudFormation — same rationale as the sibling dispatchers
# (keeps the github-actions-lambda-deploy OIDC role's blast radius narrow: it
# deliberately lacks iam:CreateRole/iam:PutRolePolicy, a fleet-wide policy
# after 4 IAM-clobber incidents in 2 months — see infrastructure/iam/README.md).
#
# CODE auto-deploys on merge to main via
# `.github/workflows/deploy-scheduled-groom-dispatcher.yml` (path-filtered to
# `infrastructure/lambdas/scheduled-groom-dispatcher/**`), which runs this
# script with NO flags (this script's default/flagless run is already
# code-only — --bootstrap is what ADDS IAM-role-creation + EventBridge
# Scheduler wiring on top, not the reverse) under the github-actions-lambda-
# deploy OIDC role (LambdaUpdate grant on `alpha-engine-*`, no IAM-role-create).
# A SCHED_NAMES/SCHED_CRONS/SCHED_INPUTS change (a schedule/cadence change,
# e.g. this file's own 2026-07-02 Sat-skip removal) still needs an operator to
# run `--bootstrap` by hand — merging alone has ZERO effect on the live
# EventBridge Scheduler rule. CUTOVER (config#1432, historical): after a
# manual --smoke spot run validates end-to-end, deploy this AND disable the
# GHA `schedule:` crons in backlog-groom.yml together (so there is no
# double-groom and no gap). NOTE: --smoke fires a REAL groom on a REAL spot box.
#
# Usage:
#   bash .../scheduled-groom-dispatcher/deploy.sh             # update code only (also the CI auto-deploy path)
#   bash .../scheduled-groom-dispatcher/deploy.sh --bootstrap # operator-only: create/update IAM roles + wire EventBridge Scheduler
#   bash .../scheduled-groom-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../scheduled-groom-dispatcher/deploy.sh --smoke     # invoke once with a synthetic schedule event (⚠ fires a REAL groom)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
  "alpha-engine-scheduled-groom-0100-daily-opus-high"
  "alpha-engine-scheduled-groom-0700-daily-mid"
  "alpha-engine-scheduled-groom-1900-daily-low"
  "alpha-engine-scheduled-groom-sun0900-weekly-gated-reverify"
)
SCHED_CRONS=(
  "cron(0 1 * * ? *)"
  "cron(0 7 * * ? *)"
  "cron(0 19 * * ? *)"
  "cron(0 9 ? * SUN *)"
)
SCHED_INPUTS=(
  '{"run_mode":"full","trigger":"demand-all","pr_budget":100,"schedule":"0 1 * * *"}'
  '{"run_mode":"full","trigger":"demand-all","pr_budget":100,"schedule":"0 7 * * *"}'
  '{"run_mode":"full","trigger":"demand-all","pr_budget":100,"schedule":"0 19 * * *"}'
  '{"run_mode":"full","model":"claude-haiku-4-5","issue_filter":"gated-reverify","schedule":"0 9 * * 0"}'
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
BOOTSTRAP=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --smoke) SMOKE=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

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

# ----- 2. Bootstrap (first-time only) ---------------------------------------

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

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
  #
  # GROOM_PRIMARY_DEEPSEEK_TIERS (alpha-engine-config-I3479, PRIMARY-mode
  # DeepSeek backend selection for scheduled low/mid groom launches):
  # DELIBERATELY ABSENT from the Variables map below — index.py's own
  # `os.environ.get("GROOM_PRIMARY_DEEPSEEK_TIERS", "")` default ("" =
  # feature OFF) governs, same as GROOM_DEMAND_GATE_ENABLED and GROOM_BACKEND
  # today. This is intentional, not an oversight: every deploy's
  # update-function-configuration call below REPLACES the ENTIRE Variables
  # map (see step 3), so a value set live via console/CLI would be silently
  # WIPED by the next code-only merge-triggered deploy — arming this MUST be
  # a reviewed PR that adds `GROOM_PRIMARY_DEEPSEEK_TIERS=low,mid` to BOTH
  # `--environment 'Variables={...}'` strings below (mirrors the
  # GROOM_MAX_DISPATCHES_DAILY precedent immediately above), never a
  # console-only edit. See index.py's module docstring / `_primary_backend_
  # for` for the full contract.
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
      --environment 'Variables={LOG_LEVEL=INFO,GROOM_DISPATCH_ENABLED=true,GROOM_MAX_DISPATCHES_DAILY=40,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
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
      --description "Execution role for ${SF_NAME} — invokes the groom Lambda + polls SSM (config#1472)" \
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
    echo "  Step Function exists, updating definition: ${SF_NAME}"
    run aws stepfunctions update-state-machine \
      --state-machine-arn "${SF_ARN}" \
      --definition "file://${SF_DEFINITION_FILE}" \
      --role-arn "${SF_ROLE_ARN}" \
      --region "${REGION}" \
      --query 'updateDate' --output text
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

  if ! $DRY_RUN; then
    echo "  Waiting 10s for Scheduler role propagation..."
    sleep 10
  fi

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

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,GROOM_DISPATCH_ENABLED=true,GROOM_MAX_DISPATCHES_DAILY=40,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

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
