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
ENV_PAYLOAD='Variables={CHANGELOG_BUCKET=alpha-engine-research,CHANGELOG_PREFIX=changelog/incidents,CHANGELOG_STRUCTURED_PREFIX=changelog/entries,CHANGELOG_QUARANTINE_PREFIX=changelog/quarantine}'

# Package the handler + vendored vocab + classifier into a zip in /tmp.
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
cp "${SCRIPT_DIR}/../_shared/vocab.py" "${PKG}/vocab.py"
cp "${SCRIPT_DIR}/../_shared/classify.py" "${PKG}/classify.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -q "function.zip" index.py vocab.py classify.py)
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
# Migrated 2026-05-20 (ROADMAP L146) from raw ``aws sns publish`` to the
# canonical ``alpha_engine_lib.alerts`` primitive (v0.21.0, lib #52).
# Skips Telegram on purpose: this is a deliberate deploy smoke test, not
# a real failure event, and a per-deploy operator ping would be noise.
# SNS path stays identical (same default topic `alpha-engine-alerts`),
# so the changelog-incident-mirror Lambda still sees the message.
SMOKE_ARG="${1:-}"
if [[ "${SMOKE_ARG}" == "--smoke" ]]; then
  echo "Smoke-testing via alerts.publish (SNS-only, severity=info)..."
  TS=$(date -u +%s)
  # Resolve Python with alpha_engine_lib installed — prefer repo-local
  # .venv, fall back to system python3 (mirrors the alpha-engine
  # health_checker.sh pattern).
  _alert_python="python3"
  if [ -x "$(dirname "$0")/../../../.venv/bin/python" ]; then
    _alert_python="$(dirname "$0")/../../../.venv/bin/python"
  fi
  "$_alert_python" -m nousergon_lib.alerts publish \
    --severity info \
    --no-telegram \
    --source alpha-engine-data/changelog-incident-mirror/deploy.sh \
    --message "deploy.sh smoke test ${TS}: Verifying changelog-incident-mirror after deploy" \
    > /dev/null
  echo "  → Published. Entry should land in s3://alpha-engine-research/changelog/entries/ within ~3s."
  echo "  → Check with: aws s3 ls s3://alpha-engine-research/changelog/entries/$(date -u +%Y-%m-%d)/ --recursive | tail"
fi
