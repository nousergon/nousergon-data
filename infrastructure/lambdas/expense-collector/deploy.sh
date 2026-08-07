#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-expense-collector Lambda and
# wire its EventBridge Scheduler rule.
#
# WHY: Brian wants ONE console page tracking every external expense (AWS,
# Anthropic, OpenRouter, DeepSeek, Neon, GitHub Actions, future subscriptions)
# with month-to-date totals and over/under-budget pacing. This Lambda is the
# producer: it queries each provider's billing/usage API and writes the
# normalized rollup the console's Expenses page reads
# (s3://alpha-engine-research/expenses/latest.json + monthly/{YYYY-MM}.json).
#
# IAM (iam-policy.json): logs + ssm:GetParameter(s) for provider keys +
# ce:GetCostAndUsage/GetCostForecast + s3 Get/Put under expenses/* + s3 Get on
# config/expense_budgets.json and decision_artifacts/_cost_raw/* + Telegram
# SSM params and the flow-doctor DynamoDB dedup store (config#2843 over-budget
# alert).
#
# Cadence (UTC): twice daily — 00:15 (captures the month-start baseline within
# 15 min of rollover) and 12:15. Cost Explorer bills $0.01/request (2 CE calls
# per run ⇒ ~$1.2/mo, visible in the collector's own AWS row).
#   cron(15 0,12 * * ? *)
#
# Plus ONE monthly reconciliation run (alpha-engine-config#2849) — 03:00 UTC
# on the 2nd of each month, after every provider has finalized the prior
# month's numbers (AWS CE data lags ~24h; GitHub/Anthropic/Neon settle same-
# day-ish; the 2nd gives every provider a full day's slack past rollover).
# Same Lambda, same code — the ONLY difference from the twice-daily rule is
# the Scheduler target's `Input`, which flips `event["mode"]` to "reconcile"
# (see index.py::handler's dispatch). No separate function/deploy path.
#   cron(0 3 2 * ? *)
#
# Managed OUTSIDE CloudFormation — mirrors the sibling dispatchers (narrow OIDC
# blast radius). CODE, and (since alpha-engine-config-I5815) SCHEDULES, auto-deploy
# on merge to main via `.github/workflows/deploy-expense-collector.yml`, which runs
# this script twice: flagless (Lambda code) and then `--reconcile-schedules`
# (EventBridge Scheduler upsert + prune), both under the
# github-actions-lambda-deploy OIDC role.
#
# alpha-engine-config-I5815 is why the schedule half is here at all. It used to
# sit inside --bootstrap next to the IAM-role creation, so the OIDC role —
# which deliberately lacks iam:CreateRole/iam:PutRolePolicy (fleet single-writer
# rule after 4 IAM-clobber incidents in 2 months) — could not run it, and the
# monthly reconciliation schedule was never created. Splitting on the IAM
# boundary rather than on "first time vs not" is what makes the schedule half
# CI-runnable.
#
# What still needs an operator: --bootstrap-iam, i.e. creating the IAM roles
# and their inline policies, plus first-ever creation of the Lambda.
# That is genuinely first-time-only.
#
# Usage:
#   bash .../expense-collector/deploy.sh                        # code only (the CI path, step 1)
#   bash .../expense-collector/deploy.sh --reconcile-schedules  # + upsert/prune Scheduler rules (the CI path, step 2)
#   bash .../expense-collector/deploy.sh --bootstrap-iam        # operator-only: create IAM roles, Lambda
#   bash .../expense-collector/deploy.sh --bootstrap            # both of the above (unchanged meaning)
#   bash .../expense-collector/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash .../expense-collector/deploy.sh --dry-run    # show actions, do not apply
#   bash .../expense-collector/deploy.sh --smoke      # invoke once and print the rollup summary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-expense-collector"
ROLE_NAME="alpha-engine-expense-collector-role"
POLICY_NAME="alpha-engine-expense-collector-policy"
SCHED_ROLE_NAME="alpha-engine-expense-collector-scheduler-role"
SCHED_POLICY_NAME="invoke-expense-collector"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

SCHED_NAMES=(
  "alpha-engine-expense-collector-twicedaily"
  "alpha-engine-expense-collector-reconcile-monthly"
)
SCHED_CRONS=(
  "cron(15 0,12 * * ? *)"
  "cron(0 3 2 * ? *)"
)
SCHED_INPUTS=(
  "{}"
  "{\"mode\":\"reconcile\"}"
)
SCHED_PREFIX="alpha-engine-expense-collector-"

# alpha-engine-config-I5815 splits the old monolithic --bootstrap in two, along
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
# (docker pip + handler tests) and the code push. The CI deploy therefore
# invokes this script twice without paying for the ~2min package build twice,
# and an operator repairing live schedule drift does not have to redeploy code
# to do it. Combined with --bootstrap or run flagless, everything happens as
# before.
CODE_DEPLOY=true
if $RECONCILE_SCHEDULES && ! $BOOTSTRAP_IAM && ! $APPLY_IAM && ! $SMOKE; then
  CODE_DEPLOY=false
fi

run() {
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

if $CODE_DEPLOY; then
# ----- 0. Scratch dir + validate handler syntax ------------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"

# ----- 0b. Preflight handler unit tests --------------------------------------
# Shared provision-then-run mechanism (config#2381 — never hand-roll this
# step). boto3 passed explicitly: the tests do a real `import index` against
# real boto3 (the fakes are monkeypatched onto the module, not sys.modules).
# `-r requirements.txt` added alongside it (config#2843: index.py now imports
# flow_doctor_telegram / nousergon_lib.* at module scope for the over-budget
# alert) — matches the sibling flow-doctor consumers' deploy.sh gates (e.g.
# overseer-liveness-probe) even though test_handler.py's own sys.modules stubs
# for those two modules take precedence at import time; installing the real
# package here still keeps this gate's dep list matching requirements.txt.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3 -r "${SCRIPT_DIR}/requirements.txt"

# ----- 1. Package: pip install runtime deps + zip handler --------------------
# No longer stdlib-only (config#2843): nousergon-lib[flow-doctor] is now a
# real runtime import via flow_doctor_telegram.py (shared sibling module).

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing runtime deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/../flow_doctor_telegram.py" "${PKG}/flow_doctor_telegram.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"
fi

# ----- 2. Bootstrap IAM (operator-only — first-time only) -------------------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied."
fi

if $BOOTSTRAP_IAM; then
  echo "Bootstrapping IAM for ${FUNCTION_NAME}..."

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
      --zip-file "fileb://${ZIP}" --timeout 120 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO}' --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} on the twice-daily expense-collection cadence" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"${FN_ARN}\"}]}"
  echo "  Applying Scheduler invoke policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "${SCHED_INVOKE_POLICY}"

  if ! $DRY_RUN; then echo "  Waiting 10s for Scheduler role propagation..."; sleep 10; fi
fi

# ----- 2bis. Reconcile EventBridge Scheduler rules (CI-safe, every merge) ---

if $RECONCILE_SCHEDULES; then
  echo "Reconciling EventBridge Scheduler rules for ${FUNCTION_NAME}..."

  for i in "${!SCHED_NAMES[@]}"; do
    name="${SCHED_NAMES[$i]}"
    cron="${SCHED_CRONS[$i]}"
    input_json="${SCHED_INPUTS[$i]}"
    target=$(printf '{"Arn":"%s","RoleArn":"%s","Input":%s}' \
      "${FN_ARN}" "${SCHED_ROLE_ARN}" "$(printf '%s' "${input_json}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')")
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

if $CODE_DEPLOY; then
echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "✓ Code deployed."
fi

# ----- 4. Smoke (real invoke — writes the day's rollup, safe to repeat) ------

if $SMOKE; then
  echo ""
  echo "Smoke-testing via direct invoke (collects + writes expenses/latest.json)..."
  RESP=$(mktemp)
  aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out \
    --payload '{}' --region "${REGION}" "${RESP}" >/dev/null
  cat "${RESP}"; echo ""
  rm -f "${RESP}"
fi
