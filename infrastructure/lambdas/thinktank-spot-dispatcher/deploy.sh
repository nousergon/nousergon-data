#!/usr/bin/env bash
# deploy.sh — alpha-engine-thinktank-spot-dispatcher (config-I5208, §47).
#
# Managed OUTSIDE CloudFormation, same as every sibling dispatcher (keeps the
# github-actions-lambda-deploy OIDC role's blast radius narrow: it deliberately
# lacks iam:CreateRole/iam:PutRolePolicy after 4 IAM-clobber incidents in 2
# months — see infrastructure/iam/README.md).
#
# Flags are STAGED on purpose. A flagless run is code-only, so merging and
# auto-deploy can never repoint the live schedule:
#
#   (no flags)    update the Lambda's code only
#   --bootstrap   create the IAM role + Lambda (idempotent)
#   --smoke       fire ONE real run on a REAL spot box (the §47 validation gate)
#   --cutover     repoint alpha-research-thinktank-daily at this dispatcher
#
# See README.md for the full rollout order. crucible-research-PR544 must be
# merged to main BEFORE --smoke: the SSM prelude execs
# infrastructure/thinktank_spot_bootstrap.sh out of a shallow clone of main.

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
FUNCTION_NAME="alpha-engine-thinktank-spot-dispatcher"
ROLE_NAME="alpha-engine-thinktank-spot-dispatcher-role"
RULE_NAME="alpha-research-thinktank-daily"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

BOOTSTRAP=0; SMOKE=0; CUTOVER=0
while [ $# -gt 0 ]; do
    case "$1" in
        --bootstrap) BOOTSTRAP=1; shift ;;
        --smoke) SMOKE=1; shift ;;
        --cutover) CUTOVER=1; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

echo "==> building package"
cp "$SCRIPT_DIR/index.py" "$BUILD_DIR/"
python3.12 -m pip install --quiet --target "$BUILD_DIR" -r "$SCRIPT_DIR/requirements.txt"
(cd "$BUILD_DIR" && zip -qr /tmp/thinktank-spot-dispatcher.zip .)

if [ "$BOOTSTRAP" -eq 1 ]; then
    echo "==> creating IAM role $ROLE_NAME (idempotent)"
    aws iam create-role --role-name "$ROLE_NAME" \
        --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
        --region "$REGION" >/dev/null 2>&1 || echo "    (role exists)"
    aws iam put-role-policy --role-name "$ROLE_NAME" \
        --policy-name "${ROLE_NAME}-policy" \
        --policy-document "file://$SCRIPT_DIR/iam-policy.json" \
        --region "$REGION"
    echo "    waiting for IAM propagation..."
    sleep 10

    echo "==> creating Lambda $FUNCTION_NAME (idempotent)"
    aws lambda create-function --function-name "$FUNCTION_NAME" \
        --runtime python3.12 --handler index.handler \
        --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
        --zip-file fileb:///tmp/thinktank-spot-dispatcher.zip \
        --timeout 300 --memory-size 512 \
        --region "$REGION" >/dev/null 2>&1 || echo "    (function exists — updating code below)"
fi

echo "==> updating function code"
aws lambda update-function-code --function-name "$FUNCTION_NAME" \
    --zip-file fileb:///tmp/thinktank-spot-dispatcher.zip \
    --region "$REGION" --query 'LastModified' --output text
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"

if [ "$SMOKE" -eq 1 ]; then
    echo "==> SMOKE: firing ONE real Think Tank run on a REAL spot box"
    echo "    (this spends a spot instance and a real LLM pass — ~25 min expected)"
    aws lambda invoke --function-name "$FUNCTION_NAME" \
        --payload '{}' --cli-binary-format raw-in-base64-out \
        --region "$REGION" /tmp/thinktank-smoke-out.json --query 'StatusCode' --output text
    echo "    dispatcher returned:"
    cat /tmp/thinktank-smoke-out.json; echo
    echo "    Watch: aws logs tail /alpha-engine/thinktank-spot --follow"
    echo "    Gate:  thinktank/challenger_selection/, thinktank/ratings/ AND"
    echo "           thinktank/events/ written for the trading day, box self-terminated."
fi

if [ "$CUTOVER" -eq 1 ]; then
    echo "==> CUTOVER: repointing $RULE_NAME at $FUNCTION_NAME"
    TARGET_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
    aws lambda add-permission --function-name "$FUNCTION_NAME" \
        --statement-id "${RULE_NAME}-invoke" --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
        --region "$REGION" >/dev/null 2>&1 || echo "    (permission exists)"
    aws events put-targets --rule "$RULE_NAME" \
        --targets "[{\"Id\":\"1\",\"Arn\":\"${TARGET_ARN}\"}]" --region "$REGION"
    echo "    $RULE_NAME now targets $FUNCTION_NAME."
    echo "    NOTE: alpha-engine-thinktank-daily-run-failed still watches the OLD"
    echo "    Lambda's Errors metric and is now BLIND — move it before calling this done."
fi

echo "==> done"
