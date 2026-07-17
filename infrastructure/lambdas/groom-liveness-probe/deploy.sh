#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-groom-liveness-probe Lambda and
# wire its EventBridge Scheduler rules.
#
# WHY: the EC2-spot backlog groom (config#1432) self-reports its terminal state
# (a `groom-digest` issue + Telegram ping), but only when the box lives long
# enough to run groom_run.sh's reporting trap. SILENT modes — spot reclaim
# mid-run, OOM/panic before the trap, a lost SSM command, a broken dispatcher
# Lambda, a disabled/misconfigured schedule (the 2026-06-29 dead-trigger class) —
# file NOTHING. This probe is the external watchdog: schedule-aware, it asserts
# every scheduled groom that has had time to finish filed a terminal digest, and
# LOUD-pings Telegram for any that didn't. (The "probe now" half; the
# Step-Function-wrap that would let the existing Fleet-SF Watch cover the groom
# natively is the tracked "SF later" follow-up.)
#
# IAM (iam-policy.json): logs + ssm:GetParameter (Telegram creds + the shared
# Fleet-Watch PAT) + s3 Get/Put on the dedup state key. No EC2/SSM-send — it only
# READS state, never launches anything.
#
# Cadence (UTC): two runs/day, each after a groom's worst-case completion
# (groom hard-ceiling 360 min + slack). Per-trigger windows + S3 dedup make the
# exact times non-load-bearing (generous LOOKBACK tolerates schedule drift):
#   06:30 daily   cron(30 6 * * ? *)   # after the 19:00 groom matures (~01:45)
#   14:30 daily   cron(30 14 * * ? *)  # after the 07:00 groom matures (~13:45)
#                                      #   (the 01:00 high-only run matures ~07:45 —
#                                      #   also covered by the 14:30 pass)
#
# Managed OUTSIDE CloudFormation — mirrors the sibling dispatchers (narrow OIDC
# blast radius: the CI role deliberately lacks iam:CreateRole/iam:PutRolePolicy,
# fleet-wide policy after 4 IAM-clobber incidents — infrastructure/iam/README.md).
#
# CODE auto-deploys on merge to main via
# `.github/workflows/deploy-groom-liveness-probe.yml` (path-filtered to this
# directory), which runs this script with NO flags (the default/flagless run
# is already code-only). A SCHED_CRONS change (this probe's OWN invocation
# cadence) still needs an operator to run `--bootstrap` by hand — merging
# alone has ZERO live effect on it.
#
# Usage:
#   bash .../groom-liveness-probe/deploy.sh             # update code only (same command CI runs)
#   bash .../groom-liveness-probe/deploy.sh --bootstrap # first-time create + wire schedules
#   bash .../groom-liveness-probe/deploy.sh --dry-run   # show actions, do not apply
#   bash .../groom-liveness-probe/deploy.sh --smoke     # invoke once (read-only check; pings only on a real miss)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-groom-liveness-probe"
ROLE_NAME="alpha-engine-groom-liveness-probe-role"
POLICY_NAME="alpha-engine-groom-liveness-probe-policy"
SCHED_ROLE_NAME="alpha-engine-groom-liveness-probe-scheduler-role"
SCHED_POLICY_NAME="invoke-groom-liveness-probe"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

SCHED_NAMES=(
  "alpha-engine-groom-liveness-0630-daily"
  "alpha-engine-groom-liveness-1430-daily"
)
SCHED_CRONS=(
  "cron(30 6 * * ? *)"
  "cron(30 14 * * ? *)"
)
SCHED_PREFIX="alpha-engine-groom-liveness-"

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
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

# ----- 0. Scratch dir + validate handler syntax -----------------------------
# PKG (the Lambda-zip staging dir) is created up front; the shared handler-
# test gate (0b) provisions its OWN scratch dir for pytest + deps (config#2381).

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"

# ----- 0b. Preflight handler unit tests --------------------------------------
# The git-only nousergon_lib submodules index.py imports at module scope
# (flow_doctor_fleet.FleetTelegramTopic; telegram.send_message via
# flow_doctor_telegram) are stubbed in sys.modules by test_handler.py BEFORE
# `import index` (see its header) — so this gate stays hermetic on bare python.
# `index.py`'s `import boto3` is REAL, and requirements.txt deliberately omits
# boto3 (provided by the Lambda runtime at deploy time, per its own comment),
# so pytest's `import index` needs it installed explicitly here — passed to the
# shared gate, which installs it into its own scratch dir, NOT the caller's
# global site-packages, not bundled into the Lambda zip. Two prior gaps
# here, same class (the gate's dep/stub set drifting from index.py's real
# module-level imports): 2026-07-02 "No module named pytest" (script had only
# run on operator laptops where pytest/boto3 were ambient); 2026-07-04 "No
# module named nousergon_lib" (config#1742 flow-doctor cutover moved the source
# onto nousergon_lib but the sys.modules stub was not migrated in lockstep).
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

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

  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 --role "${ROLE_ARN}" --handler index.handler \
      --zip-file "fileb://${ZIP}" --timeout 30 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} on the liveness cadence" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"${FN_ARN}\"}]}"
  echo "  Applying Scheduler invoke policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "${SCHED_INVOKE_POLICY}"

  if ! $DRY_RUN; then echo "  Waiting 10s for Scheduler role propagation..."; sleep 10; fi

  for i in "${!SCHED_NAMES[@]}"; do
    name="${SCHED_NAMES[$i]}"
    cron="${SCHED_CRONS[$i]}"
    target="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${SCHED_ROLE_ARN}\",\"Input\":\"{}\"}"
    if aws scheduler get-schedule --name "${name}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
      echo "  Updating Scheduler rule: ${name} → ${cron}"
      run aws scheduler update-schedule --name "${name}" --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" --region "${REGION}" --query 'ScheduleArn' --output text
    else
      echo "  Creating Scheduler rule: ${name} → ${cron}"
      run aws scheduler create-schedule --name "${name}" --schedule-expression "${cron}" \
        --schedule-expression-timezone "UTC" --flexible-time-window '{"Mode":"OFF"}' \
        --target "${target}" --region "${REGION}" --query 'ScheduleArn' --output text
    fi
    if ! $DRY_RUN; then
      aws scheduler get-schedule --name "${name}" --region "${REGION}" --query 'Name' --output text >/dev/null \
        || { echo "ERROR: Scheduler rule ${name} not found after create/update" >&2; exit 1; }
    fi
  done

  # Prune reconciliation: delete any live rule under SCHED_PREFIX not in SCHED_NAMES.
  echo "  Pruning orphaned Scheduler rules under prefix ${SCHED_PREFIX}..."
  LIVE_RULES=$(aws scheduler list-schedules --name-prefix "${SCHED_PREFIX}" --region "${REGION}" --query 'Schedules[].Name' --output text 2>/dev/null || echo "")
  for live in ${LIVE_RULES}; do
    keep=false
    for want in "${SCHED_NAMES[@]}"; do [ "${live}" = "${want}" ] && { keep=true; break; }; done
    if ! $keep; then
      echo "    Deleting orphaned Scheduler rule: ${live}"
      run aws scheduler delete-schedule --name "${live}" --region "${REGION}"
    fi
  done
fi

# ----- 3. Update function code (always, idempotent) -------------------------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "✓ Code deployed."

echo "Updating Lambda environment (flow-doctor SSM hydration)..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment 'Variables={LOG_LEVEL=INFO,FLOW_DOCTOR_ENABLED=1,ALPHA_ENGINE_DEPLOYED=1}' \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

# ----- 4. Smoke (synthetic invoke; read-only — only pings on a REAL miss) ----

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (read-only liveness check)..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out \
    --payload '{}' --region "${REGION}" "${RESP}" >/dev/null
  cat "${RESP}"; echo ""
  rm -f "${RESP}"
fi
