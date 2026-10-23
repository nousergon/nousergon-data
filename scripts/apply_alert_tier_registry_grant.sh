#!/usr/bin/env bash
# Applies nousergon-data PR #2278: lets 7 alert-publishing Lambdas read
# s3://alpha-engine-research/overseer/alert_tier_registry.json, so their
# alerts stop paging by default. IAM only: no code, env or alarm change.
# Safe any time (no trading box involved). Run BEFORE merging PR #2278:
# the repo's iam-policy-change-guard passes once the live policy matches.
set -euo pipefail
export AWS_PROFILE=ne-admin AWS_REGION=us-east-1
REPO="$HOME/Development/nousergon-data"
OBJ="arn:aws:s3:::alpha-engine-research/overseer/alert_tier_registry.json"
LAMBDAS=(pipeline-watchdog canary-replay-liveness-probe data-spot-dispatcher
  eod-snapshot-existence-check overseer-dispatcher preopen-deploy-readiness-probe
  weekly-coverage-sweep)

cd "$REPO"
prev=$(git rev-parse --abbrev-ref HEAD)
git fetch -q origin claude/project-thread-yamia5
git checkout -q --detach FETCH_HEAD
trap 'git checkout -q "$prev"' EXIT
for d in "${LAMBDAS[@]}"; do
  grep -q '"AlertTierRegistryRead"' "infrastructure/lambdas/$d/iam-policy.json" \
    || { echo "STOP: $d policy has no AlertTierRegistryRead. Is this the PR #2278 branch?"; exit 1; }
done
echo "About to apply IAM for ${#LAMBDAS[@]} Lambda roles in account $(aws sts get-caller-identity --query Account --output text)."
read -r -p "Type yes to continue: " ans; [ "$ans" = "yes" ] || { echo "Stopped."; exit 1; }

fail=0
for d in "${LAMBDAS[@]}"; do
  echo "== $d"
  bash "infrastructure/lambdas/$d/deploy.sh" --apply-iam
  role=$(grep -m1 '^ROLE_NAME=' "infrastructure/lambdas/$d/deploy.sh" | cut -d'"' -f2)
  verdict=$(aws iam simulate-principal-policy \
    --policy-source-arn "arn:aws:iam::711398986525:role/$role" \
    --action-names s3:GetObject --resource-arns "$OBJ" \
    --query 'EvaluationResults[0].EvalDecision' --output text)
  echo "   $role -> s3:GetObject on registry: $verdict"
  [ "$verdict" = "allowed" ] || fail=1
done
[ $fail -eq 0 ] && echo "DONE: all 7 roles can read the registry. Re-run the PR checks, then merge #2278." || { echo "SOME ROLES NOT ALLOWED, paste this output back."; exit 1; }
