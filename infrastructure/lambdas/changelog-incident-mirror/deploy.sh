#!/usr/bin/env bash
# deploy.sh — Update the changelog-incident-mirror Lambda's code from
# index.py in this directory.
#
# This Lambda is managed OUTSIDE the alpha-engine-orchestration CF stack
# (see this directory's README.md for why). The first-time creation +
# IAM role + SNS subscription were done via CloudFormation back when
# this lived in the orchestration stack; orphaning preserved the live
# resources via DeletionPolicy: Retain. As a result, this script only
# needs to update the function CODE — IAM, subscription, and permission
# are already wired up.
#
# Usage:
#   bash infrastructure/lambdas/changelog-incident-mirror/deploy.sh
#   bash infrastructure/lambdas/changelog-incident-mirror/deploy.sh --dry-run
#
# Auth: uses active AWS CLI creds. Personal IAM user (cipher813) has
# enough perms; the github-actions-lambda-deploy OIDC role does NOT —
# this script is intentionally NOT wired into CI to avoid expanding the
# OIDC role's blast radius for one small Lambda.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="alpha-engine-changelog-incident-mirror"
REGION="${AWS_REGION:-us-east-1}"

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# Validate index.py syntax + run handler smoke tests locally before shipping.
python3 -c "
import ast, sys
src = open('${SCRIPT_DIR}/index.py').read()
try:
    ast.parse(src)
except SyntaxError as e:
    print(f'index.py syntax error: {e}', file=sys.stderr)
    sys.exit(1)
print('index.py syntax OK')
"

if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
  echo "Running handler smoke tests..."
  python3 "${SCRIPT_DIR}/test_handler.py" >/dev/null
  echo "  ✓ Smoke tests pass"
fi

# Ensure the live Lambda env has the new structured-prefix var. Idempotent —
# overwrites in place, so re-running with no diff is harmless. Skipped on
# --dry-run.
ENV_PAYLOAD='Variables={CHANGELOG_BUCKET=alpha-engine-research,CHANGELOG_PREFIX=changelog/incidents,CHANGELOG_STRUCTURED_PREFIX=changelog/entries}'

# Package the handler into a zip in /tmp.
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -q "function.zip" index.py)
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

if $DRY_RUN; then
  echo "(--dry-run) would update Lambda code: ${FUNCTION_NAME}"
  exit 0
fi

# Update function code.
echo "Updating Lambda function code: ${FUNCTION_NAME}"
aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

# Wait for update to settle.
echo "Waiting for update to complete..."
aws lambda wait function-updated \
  --function-name "${FUNCTION_NAME}" \
  --region "${REGION}"

# Update env config to include CHANGELOG_STRUCTURED_PREFIX (added in PR 2 of
# the schema-discipline arc). Idempotent — overwriting in place is a no-op
# if all 3 vars already match. --query suppresses the JSON dump that would
# otherwise leak env values to stdout per CLAUDE.md "CLI Output Safety".
echo "Ensuring env config includes CHANGELOG_STRUCTURED_PREFIX..."
aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${ENV_PAYLOAD}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

aws lambda wait function-updated \
  --function-name "${FUNCTION_NAME}" \
  --region "${REGION}"

echo "✓ Deployed."

# Smoke test: publish a single SNS message and verify the entry lands.
SMOKE_ARG="${1:-}"
if [[ "${SMOKE_ARG}" == "--smoke" ]]; then
  echo "Smoke-testing via SNS publish..."
  TS=$(date -u +%s)
  aws sns publish \
    --topic-arn "arn:aws:sns:${REGION}:711398986525:alpha-engine-alerts" \
    --subject "deploy.sh smoke test ${TS}" \
    --message "Verifying changelog-incident-mirror after deploy ${TS}" \
    --query 'MessageId' --output text >/dev/null
  echo "  → Published. Entry should land in s3://alpha-engine-research/changelog/entries/ within ~3s."
  echo "  → Check with: aws s3 ls s3://alpha-engine-research/changelog/entries/$(date -u +%Y-%m-%d)/ --recursive | tail"
fi
