#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-ssm-reachability-probe Lambda,
# wire its 5-minute EventBridge trigger, and create its two CloudWatch alarms.
#
# alpha-engine-config-I6198. SSM is the single transport by which every
# unattended workload in this fleet receives work, and it had no health check:
# on 2026-08-03 it was unreachable VPC-wide for 2h31m and emitted nothing.
#
# Zero third-party dependencies, so packaging is a plain `zip index.py` rather
# than the Docker lambda_pip_install.sh path the reaper needs for pydantic.
# That is deliberate: the detector for the fleet's transport must not be able to
# fail because of a package build.
#
# Usage:
#   bash infrastructure/lambdas/ssm-reachability-probe/deploy.sh             # update code only
#   bash infrastructure/lambdas/ssm-reachability-probe/deploy.sh --bootstrap # first-time create + wire EventBridge + alarms
#   bash infrastructure/lambdas/ssm-reachability-probe/deploy.sh --apply-iam # re-apply iam-policy.json only
#   bash infrastructure/lambdas/ssm-reachability-probe/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/ssm-reachability-probe/deploy.sh --smoke     # invoke once and print the scan result

set -euo pipefail

# alpha-engine-config-I6619: --state must come from the automation-pause
# manifest, not from the API default (ENABLED). See infrastructure/lambdas/_shared/pause.sh.
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-ssm-reachability-probe"
ROLE_NAME="alpha-engine-ssm-reachability-probe-role"
POLICY_NAME="alpha-engine-ssm-reachability-probe-policy"
RULE_NAME="alpha-engine-ssm-reachability-probe-5min"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Canonical function env — defined ONCE so create / update can never drift.
# GRACE_SECONDS sits above the groom dispatcher's own 180s SSM-online budget so
# a normally-booting box is never counted as unreachable.
PROD_ENV='Variables={GRACE_SECONDS=300,NAME_PREFIX=alpha-engine-}'

case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
APPLY_ALARMS=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    --apply-alarms) APPLY_ALARMS=true ;;
    --smoke) SMOKE=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
# These tests existed beside index.py and this deploy.sh never ran them, so
# the post-merge gate was absent for this lambda entirely (the config#2295
# shape: ci.yml's own glob runner keeps it green PRE-merge, which is exactly
# what makes the missing POST-merge gate invisible). No extra deps: this
# lambda's tests stub boto3 in sys.modules, and the helper's contract is that
# such a caller must NOT get boto3 installed alongside the stub.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
# `boto3`: index.py imports it at module scope. Measured —
# deploy-ssm-reachability-probe failed on main 2026-08-29T00:11 with
# ModuleNotFoundError: No module named 'boto3'.
run_handler_tests "${SCRIPT_DIR}" boto3

PKG="$(mktemp -d)"
trap 'rm -rf "${PKG}"' EXIT

package() {
  cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
  (cd "${PKG}" && zip -q "function.zip" index.py)
  echo "  Packaged ${PKG}/function.zip ($(wc -c < "${PKG}/function.zip") bytes)"
}

# Alarm creation is SEPARATE from --bootstrap on purpose (pull-request-policy
# §4.2 form 3). Bootstrap creates an IAM role, which the deploy OIDC role may
# not do, so it is operator-gated. Alarms are `alpha-engine-*`, which that role
# CAN write — so the merge itself lands them, and the -dead alarm sits in ALARM
# on missing heartbeat data from that moment until an operator actually runs
# --bootstrap. That is the detector which stays red until the command runs;
# folding alarm creation into --bootstrap would make it circular — the detector
# for "bootstrap has not run" would not exist until bootstrap ran.
# RETIRED as an alarm-creation path (alpha-engine-config-I7359, executing the
# I7359 ownership ruling). Both alarms this used to create
# (${FUNCTION_NAME}-unreachable, ${FUNCTION_NAME}-dead) are codified in the
# PRIVATE nous-ergon-ops repo:
#   infrastructure/cloudwatch/alarms/alpha-engine-ssm-reachability-probe-{unreachable,dead}.json
# Edit those files there; do not add put-metric-alarm back here —
# nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the build
# if it reappears. To apply immediately from nous-ergon-ops:
#   infrastructure/cloudwatch/apply.py --prefix alpha-engine-ssm-reachability-probe-
# Both files were DELETED 2026-10-09 (alpha-engine-config-I11792) while this
# probe is paused; restore them from automation_pause.json :: retired_alarms
# `restore_from` before un-pausing it.
apply_alarms() {
  echo "  (no-op: alarms for ${FUNCTION_NAME} are applied from nous-ergon-ops, alpha-engine-config-I7359)"
}

# ----- Apply IAM only -------------------------------------------------------

if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no alarms."
  exit 0
fi

# ----- Apply alarms only (what the deploy workflow runs on every merge) ------

if $APPLY_ALARMS; then
  echo "Applying CloudWatch alarms for ${FUNCTION_NAME}..."
  apply_alarms
  echo "  ✓ Alarms applied."
  exit 0
fi

# ----- 1. Bootstrap (operator-only: creates IAM, function, rule, alarms) -----

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

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

  package
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${PKG}/function.zip" \
      --timeout 60 \
      --memory-size 128 \
      --environment "${PROD_ENV}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
    verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${PKG}/function.zip"
  fi

  echo "  Creating EventBridge rule: ${RULE_NAME}"
  run aws events put-rule \
    --name "${RULE_NAME}" --state "$(pause_state "${RULE_NAME}")" \
    --schedule-expression "rate(5 minutes)" \
    --description "Probe whether every running alpha-engine instance is reachable via SSM (I6198)" \
    --region "${REGION}" \
    --query 'RuleArn' --output text

  FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
  run aws events put-targets \
    --rule "${RULE_NAME}" \
    --targets "Id=1,Arn=${FN_ARN}" \
    --region "${REGION}"

  RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
  run_tolerating "ResourceConflictException" \
    aws lambda add-permission \
    --function-name "${FUNCTION_NAME}" \
    --statement-id "eventbridge-${RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${RULE_ARN}" \
    --region "${REGION}"

  apply_alarms
fi

# ----- 2. Update function code (always, unless bootstrap already did) -------

if ! $BOOTSTRAP; then
  package
  echo "Updating Lambda function code: ${FUNCTION_NAME}"
  run aws lambda update-function-code \
    --function-name "${FUNCTION_NAME}" \
    --zip-file "fileb://${PKG}/function.zip" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text

  if ! $DRY_RUN; then
    aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
  fi

  verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${PKG}/function.zip"
fi

echo "✓ Code deployed."

# Converge env on EVERY deploy — update-function-code does not touch it, so an
# existing function would otherwise keep whatever env it had.
if ! $DRY_RUN; then
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --environment "${PROD_ENV}" \
    --region "${REGION}" \
    --query 'LastUpdateStatus' --output text > /dev/null
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
  echo "✓ Env converged: ${PROD_ENV}"
fi

# ----- 3. Smoke -------------------------------------------------------------

# shellcheck source=infrastructure/lambdas/_shared/smoke.sh
source "${SCRIPT_DIR}/../_shared/smoke.sh"
if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke..."
  RESP=$(mktemp)
  trap 'rm -f "${RESP}"; rm -rf "${PKG}"' EXIT
  INVOKE_STDOUT=$(aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}")
  cat "${RESP}"
  echo ""
  assert_no_function_error "${INVOKE_STDOUT}" "${RESP}"
fi
