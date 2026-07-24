#!/usr/bin/env bash
# attach_overseer_put_events_policy.sh — grant fleet emitters PutEvents on the
# nousergon-alerts bus (Overseer phase 1, alpha-engine-config-I2822).
#
# The krepis chokepoints (krepis>=0.15.0) emit `nousergon.alert.v1` events via
# EventBridge PutEvents. Any role lacking the grant silently falls back to the
# S3 drop-zone write (see krepis.fleet_events), so this attach is a rollout
# optimization, not a correctness requirement — but the bus is the primary
# transport and roles should converge onto it.
#
# What it does:
#   1. Creates (idempotently) the customer-managed policy
#      `nousergon-alerts-put-events`: events:PutEvents scoped to the ONE bus
#      ARN — deliberately narrow, safe to attach broadly.
#   2. Enumerates every Lambda function named `alpha-engine-*` in the region,
#      collects the distinct execution roles, and attaches the policy.
#   3. Attaches to any extra role names passed as arguments (EC2 instance
#      roles for the trading/dashboard/spot boxes, GHA OIDC roles, ...).
#
# NEW ROLES: a newly created fleet Lambda/instance role does NOT get this
# automatically — re-run this script (it is a no-op for already-attached
# roles) or attach the managed policy in the role's own provisioning.
#
# alpha-engine-config-I2875/I2900 (2026-07-21): verified read-only that the
# four roles I2875 named as missing the grant — alert-drain-dispatcher-role,
# overseer-dispatcher-role, expense-collector-role, substrate-health-gate-role
# — are ALL Lambda execution roles for functions whose FunctionName already
# starts with "alpha-engine-", so step 2's existing wildcard enumeration
# (`starts_with(FunctionName, 'alpha-engine-')`) already targets all four —
# NO role-list change was needed here, only a re-run (see the apply manifest
# on alpha-engine-data-PR<n>). Do NOT "fix" a future missing-role report by
# hardcoding a static role list instead of re-running — that would silently
# reintroduce the exact drift this script exists to close (the wildcard IS
# the fix; a static list goes stale the moment the next Lambda is created).
# Note: alpha-engine-overseer-dispatcher-role's OWN inline policy
# (infrastructure/lambdas/overseer-dispatcher/iam-policy.json, Sid
# IntakeBusEvents) already grants an equivalent events:PutEvents statement —
# attaching the managed policy here is a harmless, redundant second path for
# that one role, not a functional gap; the other three roles have no such
# inline grant and functionally depend on this attach.
#
# Usage:
#   ./infrastructure/attach_overseer_put_events_policy.sh [--dry-run] [extra-role-name ...]

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
BUS_NAME="nousergon-alerts"
POLICY_NAME="nousergon-alerts-put-events"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  shift
fi
EXTRA_ROLES=("$@")

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUS_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:event-bus/${BUS_NAME}"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

echo "== PutEvents grant: account=${ACCOUNT_ID} bus=${BUS_NAME} dry_run=${DRY_RUN}"

# ── 1. Managed policy (idempotent) ──────────────────────────────────────────
if aws iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
  echo "policy ${POLICY_NAME}: exists"
else
  DOC=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PutOverseerAlertEvents",
      "Effect": "Allow",
      "Action": "events:PutEvents",
      "Resource": "${BUS_ARN}"
    }
  ]
}
JSON
)
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY-RUN: would create policy ${POLICY_NAME}"
  else
    aws iam create-policy --policy-name "$POLICY_NAME" --policy-document "$DOC" \
      --description "events:PutEvents on the nousergon-alerts bus only — Overseer intake emitters (alpha-engine-config-I2822)" > /dev/null
    echo "policy ${POLICY_NAME}: created"
  fi
fi

# ── 2. Enumerate fleet Lambda execution roles ───────────────────────────────
ROLE_NAMES=$(aws lambda list-functions --region "$REGION" \
  --query "Functions[?starts_with(FunctionName, 'alpha-engine-')].Role" --output text \
  | tr '\t' '\n' | awk -F'/' '{print $NF}' | sort -u)

attach() {
  local role="$1"
  if aws iam list-attached-role-policies --role-name "$role" \
      --query "AttachedPolicies[?PolicyArn=='${POLICY_ARN}']" --output text | grep -q .; then
    echo "role ${role}: already attached"
    return
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY-RUN: would attach to role ${role}"
  else
    aws iam attach-role-policy --role-name "$role" --policy-arn "$POLICY_ARN"
    echo "role ${role}: attached"
  fi
}

for role in $ROLE_NAMES; do
  attach "$role"
done

for role in "${EXTRA_ROLES[@]:-}"; do
  [[ -n "$role" ]] && attach "$role"
done

echo "== Done."
