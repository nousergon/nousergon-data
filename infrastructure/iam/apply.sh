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
#   ./infrastructure/iam/apply.sh                  # apply every policy
#   ./infrastructure/iam/apply.sh github-actions-lambda-deploy   # one role
#   ./infrastructure/iam/apply.sh --dry-run        # print planned commands
#
# Prerequisites:
#   - AWS CLI configured with iam:PutRolePolicy on the target roles
#   - The target IAM roles already exist (this script only updates inline
#     policies; it does NOT create the roles themselves, because the trust
#     policies differ and are outside the scope of a simple flat-file
#     approach)

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

cd "$SCRIPT_DIR"

if [ -n "$TARGET_ROLE" ]; then
  file="${TARGET_ROLE}.json"
  if [ ! -f "$file" ]; then
    echo "ERROR: $file not found in $SCRIPT_DIR" >&2
    exit 1
  fi
  apply_one "$file"
else
  shopt -s nullglob
  files=( *.json )
  if [ ${#files[@]} -eq 0 ]; then
    echo "No .json policy files found in $SCRIPT_DIR"
    exit 0
  fi
  for file in "${files[@]}"; do
    # *.trust.json are version-tracked snapshots of assume-role (trust)
    # policies, NOT inline permission documents — they are applied with
    # `aws iam update-assume-role-policy`, never put-role-policy. Skip them
    # in the bulk pass (see README "Trust policies + role creation").
    case "$file" in
      *.trust.json) echo "Skipping trust snapshot $file (apply with update-assume-role-policy)"; continue ;;
    esac
    apply_one "$file"
  done
fi
