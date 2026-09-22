#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-weekly-freshness-spot-dispatcher Lambda.
#
# This Lambda is the SF-invokable launcher for the Saturday weekly pipeline's
# LAUNCHER box (config#2248) — it replaces the always-on dashboard box
# (i-09b539c844515d549) that `ne-weekly-freshness-pipeline`'s
# `SaturdayTrigger` Input used to hardcode into `$.ec2_instance_id`. It is
# invoked DIRECTLY by the weekly Step Function's new `DispatchWeeklyFreshness
# Spot` state, so — like data-spot-dispatcher — there is NO wrapping Step
# Function and NO EventBridge Scheduler to wire here. This script manages
# only the Lambda and its execution role. The SF EXECUTION-role delta
# (sf-execution-iam-policy.json — `lambda:InvokeFunction` on this function)
# lives in `infrastructure/iam/alpha-engine-step-functions-role.json` and is
# applied SEPARATELY by `infrastructure/iam/apply.sh` (the human-gated IAM
# path), NOT here.
#
# Managed OUTSIDE CloudFormation — same rationale as every sibling dispatcher
# (data-spot-dispatcher, scheduled-groom-dispatcher, ci-watch-dispatcher,
# alert-drain-dispatcher): keep the github-actions-lambda-deploy OIDC role's
# blast radius narrow. That OIDC role deliberately LACKS iam:CreateRole /
# iam:PutRolePolicy, so the FIRST-TIME `--bootstrap` (which mints the
# execution role + creates the function) MUST be run by an operator with IAM
# rights. The flagless run is code-only (`update-function-code` + env
# converge) and is the CI auto-deploy path
# (.github/workflows/deploy-weekly-freshness-spot-dispatcher.yml,
# path-filtered).
#
# ORDERING WARNING (mirrors data-spot-dispatcher's README — see its 2026-07-08
# postmortem): the invoking SF definition (step_function.json) auto-deploys on
# merge. Merging a change that touches BOTH step_function.json and this
# Lambda goes live on the SF side immediately — this Lambda + its IAM MUST be
# bootstrapped BEFORE that merge lands, or the SF's DispatchWeeklyFreshnessSpot
# state 404s on the invoke the very next Saturday.
#
# Usage:
#   bash infrastructure/lambdas/weekly-freshness-spot-dispatcher/deploy.sh             # update code + env only (CI auto-deploy path)
#   bash infrastructure/lambdas/weekly-freshness-spot-dispatcher/deploy.sh --bootstrap # operator-only: create/update the execution role + create the Lambda
#   bash infrastructure/lambdas/weekly-freshness-spot-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash infrastructure/lambdas/weekly-freshness-spot-dispatcher/deploy.sh --dry-run   # show actions, do not apply
#
# alpha-engine-config-I9048: iam-policy.json gained WriteStageCoverageVerdicts
# (s3:PutObject on _stage_coverage/*) and PutStageCoverageMetric
# (cloudwatch:PutMetricData, namespace-conditioned) in the same PR as this
# comment. Deliberately NOT applied in-session before merge (the repo's usual
# preference) because the issue's own timing constraint forbids this grant
# landing on the live role before 2026-08-29 09:00 UTC (the Saturday weekly SF
# run this role is invoked by). Run `--apply-iam` (AWS_PROFILE=ne-admin) once
# that window has passed to make the grant live — the flagless CI auto-deploy
# path above is code-only and will NOT apply it.
#
# alpha-engine-config-I11227: iam-policy.json's `ec2:RunInstances` grant was
# `Resource: "*"` with NO condition, so this role could launch any instance
# type in the account — a p5.48xlarge is ~$98/h and the cost-control loop's
# first alert is ~1.5 days out. It now mirrors the shape four sibling
# dispatcher roles already carry: an instance-resource statement conditioned
# on `aws:RequestTag/Name` = `alpha-engine-weekly-freshness-spot` plus
# `ec2:InstanceType` in the set this dispatcher actually launches
# (c5/m5/c6i/c5a/m6i/m5a/c6a/m6a/r5/r6i .large), and a
# separate, unconditioned statement for the non-instance resources
# RunInstances also authorises (subnet, security group, network interface,
# volume, key pair, image, launch template) — a Condition applies to EVERY
# Resource in its statement, so they cannot share one.
#
# The allow-list is the code default of `WEEKLY_SPOT_INSTANCE_TYPES`, which
# is UNSET on the live function (measured 2026-09-20), so the default is what
# runs. Widening the pool means editing BOTH the default and this policy, then
# `--apply-iam`; the flagless CI auto-deploy path above is code-only and will
# NOT apply it.
#
# alpha-engine-config-I11427: that BOTH is no longer a comment. The two are
# held in lockstep by
# `tests/test_weekly_spot_pool_breadth.py::test_the_default_is_not_refused_by_the_roles_own_iam_allow_list`
# (and its converse, which refuses a grant no rotation entry uses). Note the
# ORDER the two edits must reach production in: `krepis.ec2_spot.launch`
# re-raises `UnauthorizedOperation` as a non-capacity `SpotLaunchError`
# WITHOUT rotating, so widen the policy and `--apply-iam` FIRST, then merge
# the default — the reverse order is an outage on the next capacity dip.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-weekly-freshness-spot-dispatcher"
ROLE_NAME="alpha-engine-weekly-freshness-spot-dispatcher-role"
POLICY_NAME="alpha-engine-weekly-freshness-spot-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Canonical function env — defined ONCE so the create / update paths can never
# drift out of lockstep. Every WEEKLY_SPOT_* knob besides the kill-switch uses
# the handler's in-code default, so it is intentionally NOT set here.
PROD_ENV='Variables={LOG_LEVEL=INFO,WEEKLY_SPOT_DISPATCH_ENABLED=true}'

# Timeout must cover the handler's worst case: launch a spot (RunInstances +
# state poll; longer on the on-demand fallback after capacity retries) PLUS
# the full SSM-online wait (WEEKLY_SPOT_SSM_ONLINE_BUDGET_SEC default 300s)
# before the async detached SSM send-command + return. 600s mirrors data-
# spot-dispatcher's identical launch+online composition (this Lambda does
# NOT wait for the bootstrap itself to finish — only fires it and returns;
# the SF's own poll loop waits for bootstrap completion).
FN_TIMEOUT=600
FN_MEMORY=256

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below (config-I2752 convention — see data-spot-dispatcher/deploy.sh).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Scratch dir + validate handler syntax ------------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- 1. Package: pip install deps (Lambda-safe) + zip handler --------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only, operator-run) --------------------------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no alarms."
  exit 0
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline policy ---
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    # --description must be ASCII-only: IAM constrains it to
    # [\u0009\u000A\u000D\u0020-\u007E\u00A1-\u00FF]. An em-dash (U+2014) is
    # outside that range and CreateRole fails with ValidationError. Caught on
    # the live 2026-07-27 bootstrap; use a plain hyphen, as siblings do.
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --description "Execution role for ${FUNCTION_NAME} - launch the weekly-freshness launcher spot + fire async SSM bootstrap (config#2248)" \
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
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout "${FN_TIMEOUT}" \
      --memory-size "${FN_MEMORY}" \
      --environment "${PROD_ENV}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 3. Update function code (always, idempotent) --------------------------
# On a not-yet-bootstrapped function this update-function-code FAILS LOUD with a
# 404 (set -e aborts) — deliberately: a flagless run cannot silently "succeed"
# when the function was never created. Run --bootstrap first.

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

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

echo "✓ Code deployed."

# ----- 4. Converge function env (always) -------------------------------------

echo "Converging Lambda environment..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${PROD_ENV}" \
  --timeout "${FN_TIMEOUT}" \
  --memory-size "${FN_MEMORY}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi
echo "✓ Env converged: ${PROD_ENV}"

echo ""
echo "Done. Next (operator):"
echo "  1. bash infrastructure/iam/apply.sh --role alpha-engine-step-functions-role"
echo "     (applies the SF execution-role invoke grant — see sf-execution-iam-policy.json)"
echo "  2. Re-deploy infrastructure/step_function.json (deploy-infrastructure.sh / CI)"
echo "  3. Validate end to end via a shell-run (bash infrastructure/run_weekly_offcycle.sh shell)"
echo "     BEFORE the next real Saturday cron fire."
