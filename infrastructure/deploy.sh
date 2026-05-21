#!/usr/bin/env bash
# deploy.sh — Build and deploy the Phase 2 alternative data Lambda.
#
# Container image deployment (ECR) — same pattern as research + predictor.
# Function: alpha-engine-data-collector
#   - Timeout: 600s (10 min, ~30 tickers of alternative data)
#   - Memory: 512 MB
#   - Triggered by Step Functions (Saturday pipeline)
#
# Prerequisites:
#   1. AWS CLI configured with appropriate credentials
#   2. IAM role created (alpha-engine-data-role)
#   3. ECR repository: alpha-engine-data-collector
#   4. Docker installed and running
#   5. .env file with FMP_API_KEY, EDGAR_IDENTITY
#
# Usage: ./infrastructure/deploy.sh

set -euo pipefail

FUNCTION_NAME="alpha-engine-data-collector"
REGION="${AWS_REGION:-us-east-1}"
BUCKET="alpha-engine-research"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION" 2>/dev/null || echo "ACCOUNT_ID")
ROLE_ARN="${LAMBDA_ROLE_ARN:-arn:aws:iam::${ACCOUNT_ID}:role/alpha-engine-data-role}"
ECR_REPO="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${FUNCTION_NAME}"

# ── Lambda env vars from master .env ──────────────────────────────────────
# This repo IS the master — .env here is the single source of truth.

LAMBDA_ENV_FILE=".env"

build_lambda_env_json() {
  if [ ! -f "$LAMBDA_ENV_FILE" ]; then
    echo "WARNING: $LAMBDA_ENV_FILE not found — Lambda will have no env vars." >&2
    echo ""
    return
  fi
  python3 -c "
import json
env = {}
with open('$LAMBDA_ENV_FILE') as f:
    for line in f:
        line = line.strip()
        if line == '# LAMBDA_SKIP':
            break
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        key, val = line.split('=', 1)
        key, val = key.strip(), val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('\"', \"'\"):
            val = val[1:-1]
        if key and val:
            env[key] = val
if env:
    print(json.dumps({'Variables': env}))
else:
    print('')
"
}

LAMBDA_ENV_JSON=$(build_lambda_env_json)

# ── Build and deploy ───────────────────────────────────────────────────────

echo "=== Building container image for $FUNCTION_NAME ==="

# alpha-engine-lib is installed inside the Dockerfile via pip from
# git+https://github.com/cipher813/alpha-engine-lib@v0.3.0 (public repo
# since 2026-05-03). No vendor staging needed.

docker build --platform linux/amd64 --provenance=false -t "$FUNCTION_NAME:latest" .

echo "Authenticating with ECR..."
aws ecr get-login-password --region "$REGION" | \
  docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

aws ecr describe-repositories --repository-names "$FUNCTION_NAME" --region "$REGION" &>/dev/null || \
  aws ecr create-repository --repository-name "$FUNCTION_NAME" --region "$REGION" > /dev/null

echo "Pushing image to ECR..."
docker tag "$FUNCTION_NAME:latest" "$ECR_REPO:latest"
docker push "$ECR_REPO:latest"
IMAGE_URI="$ECR_REPO:latest"

echo "Deploying $FUNCTION_NAME..."

ENV_ARGS=()
if [ -n "$LAMBDA_ENV_JSON" ]; then
  ENV_ARGS=(--environment "$LAMBDA_ENV_JSON")
  echo "  Env vars: $(echo "$LAMBDA_ENV_JSON" | python3 -c "import sys,json; print(', '.join(json.load(sys.stdin).get('Variables',{}).keys()))")"
fi

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" &>/dev/null; then
  EXISTING_PKG=$(aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" --region "$REGION" \
    --query "PackageType" --output text 2>/dev/null || echo "Zip")

  if [ "$EXISTING_PKG" = "Image" ]; then
    aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" \
      --image-uri "$IMAGE_URI" \
      --region "$REGION" > /dev/null
    if [ -n "$LAMBDA_ENV_JSON" ]; then
      echo "  Waiting for code update..."
      aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION" 2>/dev/null || sleep 5
      aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --environment "$LAMBDA_ENV_JSON" \
        --region "$REGION" > /dev/null
    fi
  else
    echo "  Migrating from zip to container image..."
    aws lambda delete-function --function-name "$FUNCTION_NAME" --region "$REGION"
    sleep 2
    aws lambda create-function \
      --function-name "$FUNCTION_NAME" \
      --package-type Image \
      --code "ImageUri=$IMAGE_URI" \
      --role "$ROLE_ARN" \
      --timeout 600 \
      --memory-size 512 \
      "${ENV_ARGS[@]}" \
      --region "$REGION" > /dev/null
  fi
else
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --package-type Image \
    --code "ImageUri=$IMAGE_URI" \
    --role "$ROLE_ARN" \
    --timeout 600 \
    --memory-size 512 \
    "${ENV_ARGS[@]}" \
    --region "$REGION" > /dev/null
fi
echo "  $FUNCTION_NAME deployed."

# Publish version and update 'live' alias
echo "  Publishing Lambda version..."
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION" 2>/dev/null || sleep 5
VERSION=$(aws lambda publish-version \
  --function-name "$FUNCTION_NAME" \
  --query "Version" --output text \
  --region "$REGION")
echo "  Published version: $VERSION"
aws lambda update-alias \
  --function-name "$FUNCTION_NAME" \
  --name live \
  --function-version "$VERSION" \
  --region "$REGION" 2>/dev/null || \
aws lambda create-alias \
  --function-name "$FUNCTION_NAME" \
  --name live \
  --function-version "$VERSION" \
  --region "$REGION"
echo "  Alias 'live' -> version $VERSION"

# Canary
echo "  Running canary (dry_run=true)..."
CANARY_OUT=$(mktemp)
aws lambda invoke \
  --function-name "${FUNCTION_NAME}:live" \
  --payload '{"phase": 2, "dry_run": true}' \
  --cli-binary-format raw-in-base64-out \
  --region "$REGION" \
  "$CANARY_OUT" > /dev/null

CANARY_STATUS=$(python3 -c "
import json, sys
d = json.load(open('$CANARY_OUT'))
s = d.get('status', '')
if s in ('OK', 'SKIPPED'):
    print(s)
elif d.get('statusCode') == 500:
    print('ENV_ERROR')
else:
    print(d.get('error', 'UNKNOWN'))
" 2>/dev/null || echo "PARSE_ERROR")
rm -f "$CANARY_OUT"

if [ "$CANARY_STATUS" != "OK" ] && [ "$CANARY_STATUS" != "SKIPPED" ]; then
  echo "  WARNING: Canary returned '$CANARY_STATUS'"
  echo "  Check CloudWatch logs for details."
  echo "  Rolling back..."
  # Roll back to previous version
  PREV_VERSION=$((VERSION - 1))
  if [ "$PREV_VERSION" -gt 0 ]; then
    aws lambda update-alias \
      --function-name "$FUNCTION_NAME" \
      --name live \
      --function-version "$PREV_VERSION" \
      --region "$REGION" 2>/dev/null || true
    echo "  Rolled back to version $PREV_VERSION"
  fi
  # Independent-channel surveillance per ROADMAP L221 — this exact
  # rollback chain fired silently 10 consecutive times across 2 days
  # (alpha-engine-data #274 retrospective) before Brian noticed the
  # GitHub Actions red-icon. Best-effort; trailing || true never
  # overrides the deploy's exit 1.
  python3 -m alpha_engine_lib.alerts publish \
    --severity error \
    --source "alpha-engine-data/infrastructure/deploy.sh" \
    --message "Canary rolled back: ${FUNCTION_NAME} canary returned status='${CANARY_STATUS}', live alias reverted v${VERSION}→v${PREV_VERSION}. See CloudWatch /aws/lambda/${FUNCTION_NAME} for payload." \
    || true
  exit 1
fi
echo "  Canary passed (status=$CANARY_STATUS)"

# NOTE: IAM role `alpha-engine-data-role` is a prerequisite (see header).
# It currently exists in AWS and is the execution role for the live
# alpha-engine-data-collector Lambda. A prior version of this script
# tried to `aws iam get-role` as a "create-if-missing" bootstrap and
# fell through to CreateRole when the GetRole call lacked permission —
# masking the permission error as "role not found" (silent fail) and
# then dying loudly on CreateRole. The github-actions-lambda-deploy
# role intentionally lacks iam:* permissions (principle of least
# privilege), so the bootstrap block had been dead code since day one
# of the auto-deploy path. Provisioning this role is a one-time
# operation — do it out of band with a privileged principal, ideally
# by extending infrastructure/iam/ the way #17 did for
# github-actions-lambda-deploy.

echo ""
echo "Deployment complete."
echo ""
