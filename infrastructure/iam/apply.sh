#!/usr/bin/env bash
#
# apply.sh — Apply all IAM policies in this directory to their matching roles.
#
# Each JSON file in this directory is treated as a role policy document. The
# filename (minus .json) is BOTH the target IAM role name AND the inline
# policy name. This keeps the mapping trivial: one file == one role == one
# policy. If you need multiple policies per role, put them in multiple files
# and accept the duplicate role name.
#
# This is intentionally low-ceremony — no CloudFormation, no Terraform. For
# a 5-module infra-light project, a flat JSON directory + idempotent apply
# script is the right amount of rigor. If the blast radius grows, migrate
# to CloudFormation/Terraform.
#
# Usage:
#   ./infrastructure/iam/apply.sh                  # apply every policy + trust snapshot
#   ./infrastructure/iam/apply.sh github-actions-lambda-deploy   # one role
#   ./infrastructure/iam/apply.sh --dry-run        # print planned commands
#
# Prerequisites:
#   - AWS CLI configured with iam:PutRolePolicy + iam:UpdateAssumeRolePolicy
#     on the target roles
#   - The target IAM roles already exist (this script does not create roles —
#     initial creation stays in the owning deploy script, since it also has
#     to handle the "role doesn't exist yet" bootstrap case)
#
# *.trust.json files are version-tracked snapshots of a role's ASSUME-ROLE
# (trust) policy, applied via `update-assume-role-policy` — distinct from the
# *.json inline permission documents applied via `put-role-policy` above.
# config#2826: these snapshots are the single source of truth a role's trust
# document is derived from; deploy scripts that need to (re-)assert a trust
# policy (e.g. deploy-infrastructure.sh's step 3b) read the same file rather
# than keeping their own inline copy, so the two can never drift apart.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${AWS_REGION:-us-east-1}"

DRY_RUN=0
TARGET_ROLE=""

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) TARGET_ROLE="$arg" ;;
  esac
done

apply_one() {
  local file="$1"
  local role
  role="$(basename "$file" .json)"
  local policy_name="${role}-policy"

  # Validate JSON locally before shipping it to IAM
  if ! python3 -c "import json; json.load(open('$file'))" 2>/dev/null; then
    echo "ERROR: $file is not valid JSON — skipping" >&2
    return 1
  fi

  echo "Applying $file -> role=$role policy=$policy_name"
  if [ "$DRY_RUN" = 1 ]; then
    echo "  [dry-run] aws iam put-role-policy --role-name $role --policy-name $policy_name --policy-document file://$file --region $REGION"
    return 0
  fi

  aws iam put-role-policy \
    --role-name "$role" \
    --policy-name "$policy_name" \
    --policy-document "file://$file" \
    --region "$REGION"
  echo "  OK"
}

apply_trust_one() {
  local file="$1"
  local role
  role="$(basename "$file" .trust.json)"

  if ! python3 -c "import json; json.load(open('$file'))" 2>/dev/null; then
    echo "ERROR: $file is not valid JSON — skipping" >&2
    return 1
  fi

  echo "Applying trust snapshot $file -> role=$role (update-assume-role-policy)"
  if [ "$DRY_RUN" = 1 ]; then
    echo "  [dry-run] aws iam update-assume-role-policy --role-name $role --policy-document file://$file --region $REGION"
    return 0
  fi

  aws iam update-assume-role-policy \
    --role-name "$role" \
    --policy-document "file://$file" \
    --region "$REGION"
  echo "  OK"
}

cd "$SCRIPT_DIR"

if [ -n "$TARGET_ROLE" ]; then
  found=0
  perm_file="${TARGET_ROLE}.json"
  trust_file="${TARGET_ROLE}.trust.json"
  if [ -f "$perm_file" ]; then
    apply_one "$perm_file"
    found=1
  fi
  if [ -f "$trust_file" ]; then
    apply_trust_one "$trust_file"
    found=1
  fi
  if [ "$found" = 0 ]; then
    echo "ERROR: neither $perm_file nor $trust_file found in $SCRIPT_DIR" >&2
    exit 1
  fi
else
  shopt -s nullglob
  trust_files=( *.trust.json )
  for file in "${trust_files[@]}"; do
    apply_trust_one "$file"
  done

  files=( *.json )
  if [ ${#files[@]} -eq 0 ]; then
    echo "No .json policy files found in $SCRIPT_DIR"
    exit 0
  fi
  for file in "${files[@]}"; do
    case "$file" in
      *.trust.json) continue ;;  # already applied above via apply_trust_one
    esac
    apply_one "$file"
  done
fi
