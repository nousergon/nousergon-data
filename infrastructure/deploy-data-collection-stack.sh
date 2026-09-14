#!/usr/bin/env bash
# infrastructure/deploy-data-collection-stack.sh — apply the
# nousergon-data-collection CloudFormation stack (alpha-engine-config-I10739).
#
# Run by .github/workflows/deploy-data-collection-stack.yml on every merge that
# touches the stack, under github-actions-data-collection-stack-deploy. It is a
# SEPARATE stack and a SEPARATE workflow from alpha-engine-orchestration /
# deploy-infrastructure.yml on purpose: data collection is component 1 of 4 and
# must not share a deploy path with the trading pipelines phase 4 tears down.
#
# Usage:
#   bash infrastructure/deploy-data-collection-stack.sh             # lint, stage, deploy, verify
#   bash infrastructure/deploy-data-collection-stack.sh --lint      # static checks only, no AWS
#   bash infrastructure/deploy-data-collection-stack.sh --check-live
#
# The stack contains no IAM. Its two runtime roles are codified in
# nous-ergon-ops infrastructure/iam/ and must exist before the first apply; the
# preflight below fails LOUD naming the exact command when they do not.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HELPER="$SCRIPT_DIR/data_collection_stack.py"
TEMPLATE="$SCRIPT_DIR/cloudformation/nousergon-data-collection.yaml"
DEFINITION="$SCRIPT_DIR/step-functions/data-collection.asl.json"
REGION="${AWS_REGION:-us-east-1}"
PYTHON="${PYTHON:-python3}"

MODE="deploy"
case "${1:-}" in
    "") ;;
    --lint) MODE="lint" ;;
    --check-live) MODE="check-live" ;;
    *) echo "ERROR: unknown argument: $1" >&2; exit 2 ;;
esac

field() { "$PYTHON" "$HELPER" get "$1"; }

echo "==> Lint (template, definition, schedules, pause manifest)"
"$PYTHON" "$HELPER" lint
if command -v cfn-lint >/dev/null 2>&1; then
    cfn-lint "$TEMPLATE"
elif [ "$MODE" = "lint" ]; then
    echo "ERROR: cfn-lint is not installed; the lint mode requires it" >&2
    exit 1
fi
[ "$MODE" = "lint" ] && exit 0

if [ "$MODE" = "check-live" ]; then
    exec "$PYTHON" "$HELPER" check-live
fi

STACK_NAME="$(field stack-name)"
BUCKET="$(field definition-bucket)"
KEY="$(field definition-key)"
GIT_SHA="${GITHUB_SHA:-$(git -C "$REPO_ROOT" rev-parse HEAD)}"

echo "==> Preflight: runtime roles exist (codified in nous-ergon-ops)"
for role in nousergon-data-collection-sfn-role alpha-engine-eventbridge-sfn-role; do
    if ! aws iam get-role --role-name "$role" --query Role.RoleName --output text >/dev/null 2>&1; then
        echo "ERROR: IAM role $role does not exist or is unreadable." >&2
        echo "  It is codified in nous-ergon-ops infrastructure/iam/$role/ and its FIRST apply is" >&2
        echo "  operator-gated (trust-policy.json). From an admin identity in nous-ergon-ops:" >&2
        echo "    bash infrastructure/iam/apply.sh --role $role" >&2
        echo "  then re-run this workflow (workflow_dispatch). Nothing was applied." >&2
        exit 1
    fi
done

echo "==> Preflight: stack is not stranded in ROLLBACK_COMPLETE"
# A FIRST create that fails leaves an empty stack in ROLLBACK_COMPLETE, which
# CloudFormation refuses to update; every later deploy would fail the same way
# with a message that names neither the cause nor the fix (2026-09-14). This
# role holds no cloudformation:DeleteStack on purpose, so name the fix and stop.
STATUS="$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK_NAME" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo ABSENT)"
if [ "$STATUS" = "ROLLBACK_COMPLETE" ]; then
    echo "ERROR: stack $STACK_NAME is ROLLBACK_COMPLETE (a failed first create; it holds no resources)." >&2
    echo "  Read the cause: aws cloudformation describe-stack-events --stack-name $STACK_NAME" >&2
    echo "  Fix the template, then from an admin identity:" >&2
    echo "    aws cloudformation delete-stack --stack-name $STACK_NAME && aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME" >&2
    echo "  and re-run this workflow (workflow_dispatch). Nothing was applied." >&2
    exit 1
fi

echo "==> Stage definition s3://$BUCKET/$KEY"
aws s3 cp "$DEFINITION" "s3://$BUCKET/$KEY" --region "$REGION" --only-show-errors

echo "==> Deploy stack $STACK_NAME"
aws cloudformation deploy --region "$REGION" --stack-name "$STACK_NAME" --template-file "$TEMPLATE" --no-fail-on-empty-changeset --parameter-overrides "DefinitionS3Key=$KEY" "CollectionState=$(field param-CollectionState)" "DailyHealState=$(field param-DailyHealState)" --tags "git-sha=$GIT_SHA" "template-sha256=$(field template-sha256)" "definition-sha256=$(field definition-sha256)" component=nousergon-data-collection

# "No changes to deploy" is also what a correct re-apply prints. Prove the
# effect instead of trusting the exit code: the live stack must carry THIS
# checkout's digests and every schedule its declared state.
echo "==> Verify live matches this checkout"
"$PYTHON" "$HELPER" check-live
echo "==> nousergon-data-collection deploy complete ($GIT_SHA)"
