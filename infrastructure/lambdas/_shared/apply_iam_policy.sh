# apply_iam_policy.sh — shared idempotent IAM role/policy apply, sourced by
# every lambda's deploy.sh (alpha-engine-config#2825).
#
# WHY this exists: every lambda deploy.sh applied its iam-policy.json ONLY
# inside its `--bootstrap` block (first-time creation). Ordinary merges to
# main re-deploy CODE only (`deploy.sh` flagless, the CI auto-deploy path
# and the documented default) and never re-ran `put-role-policy` — so a
# post-bootstrap iam-policy.json edit silently drifted from live until an
# operator happened to know to re-run --bootstrap (undocumented tribal
# knowledge; confirmed as the root cause of 9 of 10 findings in
# nousergon-data-PR784's extended drift-check coverage). Each deploy.sh now
# also exposes a standalone `--apply-iam` flag (same pattern
# changelog-incident-mirror/deploy.sh already used for this, config#865)
# that calls this function directly, so re-applying a changed policy no
# longer requires the full (slower, more side-effectful) --bootstrap path.
#
# Deliberately requires an operator to run --apply-iam by hand rather than
# wiring this into CI: the github-actions-lambda-deploy OIDC role
# intentionally lacks iam:CreateRole/iam:PutRolePolicy fleet-wide, a
# boundary adopted after 4 IAM-clobber incidents in 2 months (see
# infrastructure/iam/README.md "Single-writer rule"). check-drift.py
# (config#2340 surface 3) is the automated half of this pair: it now covers
# every lambda exec role and runs on every PR + daily, so a future
# iam-policy.json edit that isn't re-applied still gets caught quickly
# instead of drifting silently for weeks.
#
# Expects the caller to already define `run()` (the $DRY_RUN-aware command
# wrapper) and `$DRY_RUN`, exactly as every deploy.sh already does.
#
# Usage: apply_iam_policy <role_name> <policy_name> <policy_file> <trust_policy_json>
apply_iam_policy() {
  local role_name="$1" policy_name="$2" policy_file="$3" trust_policy="$4"

  if ! aws iam get-role --role-name "${role_name}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${role_name}"
    run aws iam create-role \
      --role-name "${role_name}" \
      --assume-role-policy-document "${trust_policy}" \
      --query 'Role.RoleName' --output text
    if ! $DRY_RUN; then
      echo "  Waiting 10s for IAM role propagation..."
      sleep 10
    fi
  else
    echo "  IAM role exists: ${role_name}"
  fi

  echo "  Applying inline policy: ${policy_name}"
  run aws iam put-role-policy \
    --role-name "${role_name}" \
    --policy-name "${policy_name}" \
    --policy-document "file://${policy_file}"
}
