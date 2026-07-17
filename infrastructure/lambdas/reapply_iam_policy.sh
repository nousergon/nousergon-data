#!/usr/bin/env bash
# reapply_iam_policy.sh — push one lambda's codified iam-policy.json to its
# live IAM exec role, WITHOUT touching the Lambda function/Step Function/
# Scheduler resources that `deploy.sh --bootstrap` would also (re-)create.
#
# config#2825: drift between infrastructure/lambdas/<name>/iam-policy.json and
# the live role regrows silently because deploy.sh only applies the policy on
# --bootstrap, and CI's auto-deploy-on-merge intentionally never passes that
# flag (the github-actions-lambda-deploy role lacks iam:PutRolePolicy by
# design — see infrastructure/iam/README.md "Single-writer rule", 4
# IAM-clobber incidents in 2 months). check_iam_drift.py (this directory)
# DETECTS the drift in CI; this script is the human's precise, minimal-blast-
# radius way to APPLY the fix once they've ruled the direction (codified vs
# live — do not blanket-apply, per alpha-engine-config#2825).
#
# Usage:
#   ./infrastructure/lambdas/reapply_iam_policy.sh <lambda-name> [--dry-run]
#
# Requires AWS creds with iam:PutRolePolicy on the target role (any admin
# profile locally; this script is NOT wired into CI).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${AWS_REGION:-us-east-1}"

LAMBDA_NAME="${1:-}"
DRY_RUN=0
for arg in "$@"; do
  [ "$arg" = "--dry-run" ] && DRY_RUN=1
done

if [ -z "$LAMBDA_NAME" ]; then
  echo "Usage: $0 <lambda-name> [--dry-run]" >&2
  echo "  e.g. $0 scheduled-groom-dispatcher" >&2
  exit 1
fi

LAMBDA_DIR="${SCRIPT_DIR}/${LAMBDA_NAME}"
DEPLOY_SH="${LAMBDA_DIR}/deploy.sh"
POLICY_FILE="${LAMBDA_DIR}/iam-policy.json"

[ -f "$DEPLOY_SH" ] || { echo "ERROR: $DEPLOY_SH not found" >&2; exit 1; }
[ -f "$POLICY_FILE" ] || { echo "ERROR: $POLICY_FILE not found" >&2; exit 1; }

ROLE_NAME=$(grep -m1 -oE '^ROLE_NAME="[^"]*"' "$DEPLOY_SH" | cut -d'"' -f2)
POLICY_NAME=$(grep -m1 -oE '^POLICY_NAME="[^"]*"' "$DEPLOY_SH" | cut -d'"' -f2)

if [ -z "$ROLE_NAME" ] || [ -z "$POLICY_NAME" ]; then
  echo "ERROR: could not parse ROLE_NAME/POLICY_NAME from $DEPLOY_SH" >&2
  exit 1
fi

python3 -c "import json; json.load(open('$POLICY_FILE'))"

echo "Reapplying ${POLICY_FILE} -> role=${ROLE_NAME} policy=${POLICY_NAME}"
if [ "$DRY_RUN" = 1 ]; then
  echo "  [dry-run] aws iam put-role-policy --role-name $ROLE_NAME --policy-name $POLICY_NAME --policy-document file://$POLICY_FILE --region $REGION"
  exit 0
fi

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "$POLICY_NAME" \
  --policy-document "file://${POLICY_FILE}" \
  --region "$REGION"
echo "  OK"
