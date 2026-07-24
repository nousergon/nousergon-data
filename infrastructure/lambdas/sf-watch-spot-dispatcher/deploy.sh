#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-sf-watch-spot-dispatcher Lambda.
#
# WHY: finishes config#2001 (the saturday-sf-failure half; the ci-main-failure
# half shipped as ci-watch-dispatcher/deploy.sh under the same issue). Fleet-SF
# Watch's diagnose-fix-rerun agent still ran on a GHA-hosted `ubuntu-latest`
# runner for saturday-sf-failure, burning the org's metered Actions-minutes
# budget — exactly the exposure config#2001 was filed to eliminate. This
# Lambda moves it to EC2 spot, mirroring ci-watch-dispatcher's PROVEN pattern
# byte-for-byte in shape: no Step Function — invoked directly via a
# SYNCHRONOUS `lambda invoke` from a GHA job (built in alpha-engine-config's
# sf-watch.yml, `sf-watch-dispatch` job) once per real saturday-sf-failure
# event, not on a cron cadence. The ONE standing schedule is the weekly
# canary drill (config#2223, step 2c below), which fires a synthetic
# `is_drill` payload through the same pipe.
#
# IAM (iam-policy.json): the Lambda needs ec2:RunInstances + iam:PassRole
# (scoped to alpha-engine-sf-watch-executor-role — a NEW, dedicated role
# created in alpha-engine-config, deliberately NOT the shared trading
# alpha-engine-executor-role NOR the OIDC-only saturday-sf-watch-role) +
# ssm:SendCommand. The BOX reads its own run secrets (PAT) via ITS instance
# profile, so this Lambda needs no secret access of its own.
#
# DEFER-NOT-DROP (config#2226): the Lambda additionally needs
# scheduler:CreateSchedule/GetSchedule (scoped to schedule/default/
# sf-watch-defer-*), iam:PassRole on alpha-engine-sf-watch-defer-scheduler-
# role (the role EventBridge Scheduler assumes to re-invoke this Lambda —
# created below in --bootstrap), and states:ListExecutions on the three ne-*
# pipeline state machines for the deferred-invocation re-evaluation.
#
# Managed OUTSIDE CloudFormation (same rationale as the sibling dispatchers):
# keeps the github-actions-lambda-deploy OIDC role's blast radius narrow — it
# deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet-wide policy after
# 4 IAM-clobber incidents). This script's FLAGLESS run is already code-only
# (this is what the GHA auto-deploy workflow calls); --bootstrap is what ADDS
# IAM-role-creation + Lambda-function-creation on top, operator-run only,
# never in CI.
#
# Usage:
#   bash .../sf-watch-spot-dispatcher/deploy.sh             # update code only (also the CI auto-deploy path)
#   bash .../sf-watch-spot-dispatcher/deploy.sh --bootstrap # operator-only: create/update the IAM roles + Lambda function + weekly canary drill schedule (config#2223)
#   bash .../sf-watch-spot-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../sf-watch-spot-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#   bash .../sf-watch-spot-dispatcher/deploy.sh --smoke     # invoke once with a synthetic event (fires a REAL spot box)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-sf-watch-spot-dispatcher"
ROLE_NAME="alpha-engine-sf-watch-spot-dispatcher-role"
POLICY_NAME="alpha-engine-sf-watch-spot-dispatcher-policy"
# Role EventBridge Scheduler assumes to fire the one-shot defer-not-drop
# re-invokes of this Lambda (config#2226). Created in --bootstrap only.
DEFER_ROLE_NAME="alpha-engine-sf-watch-defer-scheduler-role"
DEFER_POLICY_NAME="alpha-engine-sf-watch-defer-scheduler-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
# The Wednesday canary drill now fires THROUGH the Overseer router
# (alpha-engine-config-I2832), so the full production dispatch path — the one
# every real dispatch takes since I2823 — is what the drill exercises, not
# just this executor's launch leg. Router ARN + the shared router-invoke
# scheduler role (created idempotently below).
OVERSEER_ROUTER_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:alpha-engine-overseer-dispatcher"
source "${SCRIPT_DIR}/../_shared/ensure_overseer_scheduler_role.sh"
DEFER_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${DEFER_ROLE_NAME}"
# Launch-config pins (config#2265): the DEPLOYED env is the observable source
# of truth the sf-watch-liveness-probe reads (it verifies these AMI/SG/subnet
# ids still exist twice daily and alerts loud if any key is MISSING from the
# live env). Values MUST equal index.py's in-code defaults — a lockstep test
# in test_handler.py (test_deploy_sh_launch_config_pins_match_index_defaults)
# enforces it. JSON form (not the Variables={...} shorthand) because the
# subnet list itself contains commas.
LAUNCH_AMI_ID="ami-0c421724a94bba6d6"
LAUNCH_SECURITY_GROUP="sg-03cd3c4bd91e610b0"
LAUNCH_SUBNETS="subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,subnet-c670118d,subnet-7cff7c43,subnet-e07166ec"
lambda_env_json() {
  # $1 = SF_WATCH_DISPATCH_ENABLED value (true|false)
  printf '{"Variables":{"LOG_LEVEL":"INFO","SF_WATCH_DISPATCH_ENABLED":"%s","SF_WATCH_DEFER_ROLE_ARN":"%s","SF_WATCH_AMI_ID":"%s","SF_WATCH_SECURITY_GROUP":"%s","SF_WATCH_SUBNETS":"%s"}}' \
    "$1" "${DEFER_ROLE_ARN}" "${LAUNCH_AMI_ID}" "${LAUNCH_SECURITY_GROUP}" "${LAUNCH_SUBNETS}"
}
# Bootstrap default (first-time deployment only) — sets SF_WATCH_DISPATCH_ENABLED=true
# as the safe default. The update path (step 3) will read the live value and preserve it.
LAMBDA_ENV_BOOTSTRAP="$(lambda_env_json true)"

# Shared operator-flag-preserve helper (config#1818/#2236/#2264 bug class).
source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"

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
APPLY_IAM=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
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
# before `import index` (see test_handler.py). The pinned nousergon-lib +
# krepis are installed for real by the shared gate into its own scratch dir — NOT the
# caller's global site-packages, not bundled into the Lambda zip.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
NOUSERGON_LIB_REQ=$(grep -E '^nousergon-lib' "${SCRIPT_DIR}/requirements.txt" | head -1)
KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" "${KREPIS_REQ}" "${NOUSERGON_LIB_REQ}"

# ----- 1. Package: pip install deps + zip handler ---------------------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied."
fi

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

  # --- 2a2. EventBridge Scheduler invoke role (defer-not-drop, config#2226) ---
  # Assumed by scheduler.amazonaws.com to fire the one-shot deferred
  # re-invokes; may ONLY invoke THIS function.
  DEFER_TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${DEFER_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${DEFER_ROLE_NAME}"
    run aws iam create-role \
      --role-name "${DEFER_ROLE_NAME}" \
      --assume-role-policy-document "${DEFER_TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${DEFER_ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${DEFER_POLICY_NAME}"
  # Both the unqualified function ARN and :* (version/alias-qualified) — the
  # schedule targets the unqualified ARN, but keep qualified invokes working
  # if an alias is ever introduced.
  DEFER_INVOKE_POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":["arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}","arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}:*"]}]}
EOF
)
  run aws iam put-role-policy \
    --role-name "${DEFER_ROLE_NAME}" \
    --policy-name "${DEFER_POLICY_NAME}" \
    --policy-document "${DEFER_INVOKE_POLICY}"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for IAM role propagation..."
    sleep 10
  fi

  # --- 2b. Lambda function ---
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
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # --- 2c-pre. Shared router-invoke scheduler role (I2832) ---
  OVERSEER_SCHED_ROLE_ARN=$(ensure_overseer_scheduler_role "${REGION}" "${ACCOUNT_ID}" run)
  if ! $DRY_RUN; then echo "  Waiting 10s for scheduler role propagation..."; sleep 10; fi

  # --- 2c. Weekly canary drill schedule (config#2223; routed via I2832) ---
  # Exercises the WHOLE production dispatch pipe (router -> registry lookup ->
  # executor Lambda IAM -> scoped RunInstances -> SSM -> bootstrap start)
  # without a real SF failure — the SAME path every real saturday-sf-failure
  # takes since I2823. The box short-circuits before the agent and writes the
  # _canary heartbeat the Fleet Status page escalates on when missed. Uses the
  # shared router-invoke scheduler role (2c-pre), NOT the defer-scheduler role
  # (which stays scoped to its real defer-not-drop self-reinvoke purpose).
  CANARY_SCHED_NAME="alpha-engine-sf-watch-canary-drill-weekly"
  CANARY_CRON="cron(0 15 ? * WED *)"
  # Static Input: run_date is deliberately ABSENT — the handler ALWAYS
  # synthesizes a drill-scoped run_date (drill-YYYY-MM-DD) so no payload can
  # carry a real run_date into a drill (see index.py DRILL_RUN_DATE_PREFIX).
  # The execution_arn is synthetic but ARN-shaped (allowlist-valid); the
  # drill never touches it.
  # Target is the ROUTER (I2832): the drill payload is wrapped in the
  # {playbook, payload} router envelope so the drill traverses router ->
  # registry lookup -> executor, exactly as a real saturday-sf-failure does.
  CANARY_TARGET=$(python3 - <<PY
import json
drill_payload = {
    "is_drill": "true",
    "pipeline_name": "ne-weekly-freshness-pipeline",
    "cadence_slug": "saturday",
    "execution_arn": "arn:aws:states:${REGION}:${ACCOUNT_ID}:execution:ne-weekly-freshness-pipeline:canary-drill",
    "cause": "synthetic weekly canary drill of the dispatch pipe (config#2223) - not a real failure",
}
print(json.dumps({
    "Arn": "${OVERSEER_ROUTER_ARN}",
    "RoleArn": "${OVERSEER_SCHED_ROLE_ARN}",
    "Input": json.dumps({"playbook": "sf-watch", "payload": drill_payload}),
    # config#2902: zero-retry — AWS Scheduler's 185-attempt default would
    # re-dispatch this drill for up to a day on any transient router error.
    "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 60},
}))
PY
)
  if aws scheduler get-schedule --name "${CANARY_SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
    echo "  Updating Scheduler rule: ${CANARY_SCHED_NAME} → ${CANARY_CRON}"
    run aws scheduler update-schedule --name "${CANARY_SCHED_NAME}" \
      --schedule-expression "${CANARY_CRON}" --schedule-expression-timezone "UTC" \
      --flexible-time-window '{"Mode":"OFF"}' --target "${CANARY_TARGET}" \
      --region "${REGION}" --query 'ScheduleArn' --output text
  else
    echo "  Creating Scheduler rule: ${CANARY_SCHED_NAME} → ${CANARY_CRON}"
    run aws scheduler create-schedule --name "${CANARY_SCHED_NAME}" \
      --schedule-expression "${CANARY_CRON}" --schedule-expression-timezone "UTC" \
      --flexible-time-window '{"Mode":"OFF"}' --target "${CANARY_TARGET}" \
      --region "${REGION}" --query 'ScheduleArn' --output text
  fi
  if ! $DRY_RUN; then
    aws scheduler get-schedule --name "${CANARY_SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null \
      || { echo "ERROR: Scheduler rule ${CANARY_SCHED_NAME} not found after create/update" >&2; exit 1; }
  fi
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

echo "Updating Lambda environment (preserving operator-owned SF_WATCH_DISPATCH_ENABLED)..."
# SF_WATCH_DISPATCH_ENABLED is an OPERATOR-OWNED runtime flag (defer-not-drop gate) —
# the update path must PRESERVE its live value, never reset it to bootstrap defaults.
# This mirrors the saturday-sf-watch-dispatcher fix (config#1818): a routine redeploy
# should not silently re-arm/disarm the operator's incident-response flag.
CURRENT_DISPATCH=$(preserve_env_flag "${FUNCTION_NAME}" "${REGION}" SF_WATCH_DISPATCH_ENABLED true)
# Launch-config pins ride every update so the liveness probe always finds them
# in the live env (see the lambda_env_json comment near the top, config#2265).
LAMBDA_ENV="$(lambda_env_json "${CURRENT_DISPATCH}")"
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${LAMBDA_ENV}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

# ----- 4. Smoke (synthetic event, direct invoke) -----------------------------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (synthetic saturday-sf-failure event)..."
  echo "⚠ this fires a REAL spot box + REAL sf_watch_spot_bootstrap.sh run."
  RESP=$(mktemp)
  trap "rm -f '${RESP}'" EXIT
  aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{"pipeline_name":"ne-weekly-freshness-pipeline","cadence_slug":"saturday","state_machine_arn":"arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline","execution_arn":"arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:smoke-test-0000000000000000","run_date":"1970-01-01","failed_state":"SmokeTest","cause":"synthetic smoke-test invocation, not a real failure","watch_log_key":"consolidated/saturday_sf_watch/1970-01-01.json","is_preflight":"false"}' \
    --cli-binary-format raw-in-base64-out \
    --region "${REGION}" \
    "${RESP}" >/dev/null
  cat "${RESP}"
  echo ""
fi
