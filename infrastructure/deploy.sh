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
#   5. Secrets (FMP_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, …) present in SSM
#      Parameter Store under /alpha-engine/* — the Lambda reads them at runtime
#      via get_secret(); no .env is built into or sourced by this deploy
#      (config#890 .env→SSM arc).
#
# Usage: ./infrastructure/deploy.sh

set -euo pipefail

FUNCTION_NAME="alpha-engine-data-collector"
REGION="${AWS_REGION:-us-east-1}"
BUCKET="alpha-engine-research"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION" 2>/dev/null || echo "ACCOUNT_ID")
ROLE_ARN="${LAMBDA_ROLE_ARN:-arn:aws:iam::${ACCOUNT_ID}:role/alpha-engine-data-role}"
ECR_REPO="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${FUNCTION_NAME}"

# ── Build and deploy ───────────────────────────────────────────────────────

echo "=== Building container image for $FUNCTION_NAME ==="

# alpha-engine-lib is installed inside the Dockerfile via pip from
# git+https://github.com/nousergon/nousergon-lib@v0.3.0 (public repo
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

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" &>/dev/null; then
  EXISTING_PKG=$(aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" --region "$REGION" \
    --query "PackageType" --output text 2>/dev/null || echo "Zip")

  if [ "$EXISTING_PKG" = "Image" ]; then
    aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" \
      --image-uri "$IMAGE_URI" \
      --region "$REGION" > /dev/null
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

# Canary — POST-promotion (the 'live' alias already moved to $VERSION above).
#
# Invoke via the shared ``krepis.aws invoke-canary`` CLI (config#1494, published
# in krepis 0.7.0) instead of a bare ``aws lambda invoke``. The CLI retries ONLY
# on the throttle/concurrency signal (TooManyRequestsException /
# ReservedFunctionConcurrentInvocationLimitExceeded) with bounded backoff+jitter,
# writes the response payload to --out, prints the invoke metadata JSON to stdout,
# exits 0 on invoke-API success and 1 on a non-throttle boto error or throttle
# exhaustion. boto3 path — no ``--cli-binary-format``/base64.
echo "  Running canary (dry_run=true)..."
CANARY_OUT=$(mktemp)
if ! python3 -m krepis.aws invoke-canary \
    --function-name "${FUNCTION_NAME}:live" \
    --payload '{"phase": 2, "dry_run": true}' \
    --out "$CANARY_OUT" \
    --region "$REGION" \
    --max-attempts 6 \
    --label "${FUNCTION_NAME}-canary" > /dev/null; then
  # The invoke API never returned a payload — either a non-throttle error, or
  # the reserved-concurrency slot stayed busy past the bounded retry window.
  # The deploy itself SUCCEEDED (the live alias already moved to $VERSION); a
  # never-run smoke test is NOT a canary failure, so do NOT roll back —
  # reverting a healthy deploy because we couldn't get a test slot would be the
  # wrong action. Surface loud (fail the job) + alert so an operator confirms
  # the live version by hand. Distinct dedup-key from the bad-STATUS rollback
  # path below.
  rm -f "$CANARY_OUT"
  echo "  ERROR: canary could not be invoked (slot contention or invoke error) — deploy left LIVE on v${VERSION}, NOT rolled back."
  python3 -m krepis.alerts publish \
    --severity error \
    --source "alpha-engine-data/infrastructure/deploy.sh" \
    --dedup-key "canary-uninvokable-${FUNCTION_NAME}-v${VERSION}" \
    --message "Canary could NOT be invoked for ${FUNCTION_NAME} v${VERSION} (throttle/concurrency or invoke error, retries exhausted). Live alias LEFT on v${VERSION} — deploy succeeded, NOT rolled back. Verify the live version manually." \
    || true
  exit 1
fi

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
  # GitHub Actions red-icon. ``dedup_key`` collapses an image-wide
  # rebuild that breaks N Lambdas' canaries within the hour into one
  # alert per (Lambda, version) — lib v0.24.0 substrate (L221
  # retrofit 2026-05-22). Best-effort; trailing || true never
  # overrides the deploy's exit 1.
  python3 -m nousergon_lib.alerts publish \
    --severity error \
    --source "alpha-engine-data/infrastructure/deploy.sh" \
    --dedup-key "canary-fail-${FUNCTION_NAME}-v${VERSION}" \
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
