#!/usr/bin/env bash
# deploy.sh — the fault-injection mock Lambda + its role, and the staging
# state machine's execution role (alpha-engine-config-I5718).
#
# This deploys NOTHING that production depends on. The mock exists only to
# stand in for the groom dispatcher inside the staging dispatch, and its role
# is deliberately the narrowest in the fleet: CloudWatch Logs, plus
# states:SendTask* so it can complete or fail injected lanes. No EC2, no SSM,
# no GitHub, no S3.
#
# The staging state machine itself is built by
# `scripts/fault_injection_run.py --deploy`, which DERIVES it from the
# production definition rather than carrying a copy. That script is the entry
# point; this file exists so the mock's IAM is codified rather than applied by
# hand (it was, once, on 2026-07-30 — this replaces that).
#
# Usage:
#   bash infrastructure/lambdas/groom-inject-mock/deploy.sh            # code only
#   bash infrastructure/lambdas/groom-inject-mock/deploy.sh --bootstrap  # + roles/topic
#   bash infrastructure/lambdas/groom-inject-mock/deploy.sh --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-groom-inject-mock"
ROLE_NAME="alpha-engine-groom-inject-mock-role"
POLICY_NAME="alpha-engine-groom-inject-mock-policy"
SF_ROLE_NAME="alpha-engine-groom-inject-sf-role"
SF_POLICY_NAME="alpha-engine-groom-inject-sf-policy"
STAGING_TOPIC="alpha-engine-groom-inject-alerts"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

BOOTSTRAP=false
DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --bootstrap) BOOTSTRAP=true ;;
    --dry-run) DRY_RUN=true ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

run() { if $DRY_RUN; then echo "[dry-run] $*"; else "$@"; fi; }

if $BOOTSTRAP; then
  echo "== mock execution role =="
  run aws iam create-role --role-name "${ROLE_NAME}" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    >/dev/null 2>&1 || echo "  (already exists)"
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  echo "== staging state-machine execution role =="
  # Deliberately NOT the production groom SF role: that role is scoped to the
  # production Lambda and the real alerts topic, so reusing it both fails (the
  # first injection run did) and would let a staging machine reach production
  # resources if the definition swap were ever incomplete. Isolation is
  # enforced by IAM here, not by the swap being correct.
  run aws iam create-role --role-name "${SF_ROLE_NAME}" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    >/dev/null 2>&1 || echo "  (already exists)"
  run aws iam put-role-policy --role-name "${SF_ROLE_NAME}" \
    --policy-name "${SF_POLICY_NAME}" \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"InvokeOnlyTheMock\",\"Effect\":\"Allow\",\"Action\":\"lambda:InvokeFunction\",\"Resource\":\"${FN_ARN}\"},{\"Sid\":\"PublishOnlyToStagingTopic\",\"Effect\":\"Allow\",\"Action\":\"sns:Publish\",\"Resource\":\"arn:aws:sns:${REGION}:${ACCOUNT_ID}:${STAGING_TOPIC}\"}]}"

  echo "== staging alerts topic =="
  run aws sns create-topic --name "${STAGING_TOPIC}" --region "${REGION}" \
    --query TopicArn --output text
fi

echo "== mock code =="
ZIP="$(mktemp -d)/mock.zip"
(cd "${SCRIPT_DIR}" && zip -q "${ZIP}" index.py)
if aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" \
    --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
  run aws lambda update-function-code --function-name "${FUNCTION_NAME}" \
    --zip-file "fileb://${ZIP}" --region "${REGION}" \
    --query LastUpdateStatus --output text
else
  run aws lambda create-function --function-name "${FUNCTION_NAME}" \
    --runtime python3.12 \
    --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
    --handler index.handler --zip-file "fileb://${ZIP}" \
    --timeout 60 --memory-size 256 --region "${REGION}" \
    --query FunctionArn --output text
fi

echo "Done. Build/refresh the staging state machine with:"
echo "  python3 scripts/fault_injection_run.py --deploy"
