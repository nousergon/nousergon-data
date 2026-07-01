#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-friday-shell-run-report Lambda
# and wire its EventBridge trigger.
#
# Subscribes to `aws.states` / "Step Functions Execution Status Change" events
# for `ne-weekly-freshness-pipeline` terminal transitions (SUCCEEDED / FAILED
# / TIMED_OUT / ABORTED). The handler no-ops on real Saturday runs and, for
# shell-run executions (shell_run=true), reads the execution history and writes
# the consolidated report to s3://alpha-engine-research/friday-shell-run/{date}/
# report.json + a structured SNS summary (ROADMAP L658 design point 5).
#
# Managed outside CloudFormation — same rationale as the sibling
# eod-success-friday-shell-trigger (keeps the github-actions-lambda-deploy OIDC
# role's blast radius narrow; operator-deployed only).
#
# Usage:
#   bash infrastructure/lambdas/friday-shell-run-report/deploy.sh             # update code only
#   bash infrastructure/lambdas/friday-shell-run-report/deploy.sh --bootstrap # first-time create + wire EventBridge
#   bash infrastructure/lambdas/friday-shell-run-report/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-friday-shell-run-report"
ROLE_NAME="alpha-engine-friday-shell-run-report-role"
POLICY_NAME="alpha-engine-friday-shell-run-report-policy"
RULE_NAME="alpha-engine-friday-shell-run-report"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

DRY_RUN=false
BOOTSTRAP=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

run() {
  if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi
}

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "import ast; ast.parse(open('${SCRIPT_DIR}/index.py').read()); print('index.py syntax OK')"

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Running handler unit tests..."
  python3 -m pytest "${SCRIPT_DIR}/test_handler.py" -q
fi

# ----- 1. Package: pip install deps + zip handler ---------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (pip install -t)..."
python3 -m pip install --quiet --target "${PKG}" --upgrade -r "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
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
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then echo "  Waiting 10s for IAM role propagation..."; sleep 10; fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 --role "${ROLE_ARN}" --handler index.handler \
      --zip-file "fileb://${ZIP}" --timeout 60 --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,S3_BUCKET=alpha-engine-research}' \
      --region "${REGION}" --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi
fi

# ----- 2b. Reconcile EventBridge rule (ALWAYS — not bootstrap-gated) --------
#
# config#1453/config#1460: a rename that changes an SF ARN must be picked up
# by a PLAIN `deploy.sh` run, not just the first-time `--bootstrap` (this
# rule was one of the two EventBridge triggers the ne-* rename silently
# dropped). put-rule/put-targets/add-permission are idempotent create-or-
# update calls, so reconciling on every deploy converges the live rule to
# source with no bootstrap dependency. Requires the Lambda to already exist
# (i.e. `--bootstrap` has run at least once).
# The handler no-ops on non-shell-run executions, so no input-content filter is needed.
echo "Reconciling EventBridge rule: ${RULE_NAME}"
EVENT_PATTERN=$(cat <<EOF
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "stateMachineArn": ["arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline"],
    "status": ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
  }
}
EOF
)
run aws events put-rule --name "${RULE_NAME}" --event-pattern "${EVENT_PATTERN}" \
  --description "Consolidated Friday shell-run report on Saturday SF terminal (Lambda shell-run-guards)" \
  --region "${REGION}" --query 'RuleArn' --output text

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
run aws events put-targets --rule "${RULE_NAME}" --targets "Id=1,Arn=${FN_ARN}" --region "${REGION}"

RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
run aws lambda add-permission --function-name "${FUNCTION_NAME}" \
  --statement-id "eventbridge-${RULE_NAME}" --action lambda:InvokeFunction \
  --principal events.amazonaws.com --source-arn "${RULE_ARN}" --region "${REGION}" 2>/dev/null || true

# ----- 3. Update function code (always after bootstrap, idempotent) ---------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" --region "${REGION}" --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo "✓ Code deployed."
