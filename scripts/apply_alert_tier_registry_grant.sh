#!/usr/bin/env bash
# Applies nousergon-data PR #2278 (aws iam put-role-policy per role): lets 7 alert-publishing Lambdas read
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
  # Same call deploy.sh --apply-iam makes for an existing role, without its
  # local handler-test preflight (which needs this Mac's venv to match CI).
  role=$(grep -m1 '^ROLE_NAME=' "infrastructure/lambdas/$d/deploy.sh" | cut -d'"' -f2)
  pol=$(grep -m1 '^POLICY_NAME=' "infrastructure/lambdas/$d/deploy.sh" | cut -d'"' -f2)
  [ -n "$role" ] && [ -n "$pol" ] || { echo "STOP: could not read ROLE_NAME/POLICY_NAME for $d"; exit 1; }
  aws iam get-role --role-name "$role" --query Role.RoleName --output text >/dev/null \
    || { echo "STOP: role $role does not exist (never bootstrapped)"; exit 1; }
  aws iam put-role-policy --role-name "$role" --policy-name "$pol" \
    --policy-document "file://infrastructure/lambdas/$d/iam-policy.json"
  echo "   applied $pol to $role"
  verdict=$(aws iam simulate-principal-policy \
    --policy-source-arn "arn:aws:iam::711398986525:role/$role" \
    --action-names s3:GetObject --resource-arns "$OBJ" \
    --query 'EvaluationResults[0].EvalDecision' --output text)
  echo "   $role -> s3:GetObject on registry: $verdict"
  [ "$verdict" = "allowed" ] || fail=1
done
[ $fail -eq 0 ] && echo "DONE: all 7 roles can read the registry. Re-run the PR checks, then merge #2278." || { echo "SOME ROLES NOT ALLOWED, paste this output back."; exit 1; }
