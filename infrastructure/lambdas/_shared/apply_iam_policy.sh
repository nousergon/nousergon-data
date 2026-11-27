# shellcheck shell=bash
#
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
# probe_role_presence <role_name> -> prints "present" | "absent" | "unknown"
#
# WHY THIS EXISTS (the four-permanently-red-deploys defect, 2026-08-28).
#
# The old existence check was:
#
#   if ! aws iam get-role --role-name "$r" ... >/dev/null 2>&1; then  # CREATE
#
# `2>&1` to /dev/null makes AccessDenied and NoSuchEntity the same observation,
# and the `!` branch reads BOTH as "the role does not exist". The CI auto-deploy
# identity `github-actions-lambda-deploy` holds `iam:GetRole` on exactly two
# roles (alpha-engine-eventbridge-sfn-role, alpha-engine-step-functions-role)
# and on NO lambda execution role — by design, per the single-writer rule. So
# every code-only deploy that reached this helper probed a role it may not read,
# was denied, concluded "absent", printed `Creating IAM role: <name>` and called
# `aws iam create-role` — a grant it also does not hold and must not hold.
#
# A denied READ is not evidence of absence. Distinguishing the two is the whole
# job here, and it is why the stderr is captured instead of discarded.
probe_role_presence() {
  local role_name="${1:?probe_role_presence: role name required}"
  local err rc=0
  err="$(aws iam get-role --role-name "${role_name}" \
           --query 'Role.RoleName' --output text 2>&1 >/dev/null)" || rc=$?
  if [ "${rc}" -eq 0 ]; then
    echo present
    return 0
  fi
  # Only an explicit NoSuchEntity proves absence. Everything else — AccessDenied,
  # an expired token, a throttle, a network failure — is `unknown`.
  if printf '%s' "${err}" | grep -qiE 'NoSuchEntity|cannot be found|does not exist'; then
    echo absent
    return 0
  fi
  echo unknown
  return 0
}

# probe_role_policy_state <role_name> <policy_name> <policy_file>
#   -> prints "same" | "different" | "absent" | "unknown"
#
# WHY THIS EXISTS (the SECOND instance of the I9045 conflation, measured in
# nousergon-data run 33229043798 on 2026-08-29).
#
# PR1569 fixed `aws iam get-role`'s AccessDenied/NoSuchEntity conflation and
# left the IDENTICAL conflation one screen below it, on `get-role-policy`:
#
#   if live_doc="$(aws iam get-role-policy ... 2>/dev/null)"; then ...
#   else verdict="absent"   # <- a DENIED read, called "no policy yet"
#
# The CI identity cannot read a lambda execution role's inline policy either,
# so every code-only deploy printed
#
#   no inline policy alpha-engine-overseer-dispatcher-policy on the role yet
#   — first apply.
#
# about a policy that has existed since 2026-07-22. A read the identity is not
# allowed to make is not evidence about the thing being read. Same rule as
# probe_role_presence: only an explicit NoSuchEntity proves absence.
probe_role_policy_state() {
  local role_name="${1:?probe_role_policy_state: role name required}"
  local policy_name="${2:?probe_role_policy_state: policy name required}"
  local policy_file="${3:?probe_role_policy_state: policy file required}"
  local live_doc err err_file rc=0
  err_file="$(mktemp)"

  live_doc="$(aws iam get-role-policy \
      --role-name "${role_name}" \
      --policy-name "${policy_name}" \
      --query 'PolicyDocument' --output json 2>"${err_file}")" || rc=$?
  err="$(cat "${err_file}" 2>/dev/null || true)"
  rm -f "${err_file}"

  if [ "${rc}" -ne 0 ]; then
    if printf '%s' "${err}" | grep -qiE 'NoSuchEntity|cannot be found|does not exist'; then
      echo absent
    else
      echo unknown
    fi
    return 0
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    echo unknown
    return 0
  fi
  python3 -c '
import json, sys
try:
    live = json.loads(sys.argv[1])
    disk = json.load(open(sys.argv[2]))
except Exception:
    print("unknown"); raise SystemExit(0)
# Key-order- and whitespace-insensitive: only the semantic document matters.
print("same" if json.dumps(live, sort_keys=True) == json.dumps(disk, sort_keys=True)
      else "different")
' "${live_doc}" "${policy_file}" 2>/dev/null || echo unknown
}

# Usage: apply_iam_policy <role_name> <policy_name> <policy_file>
#                         <trust_policy_json> [managed_policy_arn] [may_create_role]
#
# `managed_policy_arn`, when supplied, is attached after a missing role is
# created. `may_create_role` (default "true") governs the ROLE-CREATION half
# only. The policy apply below runs either way. This is the OPERATOR path
# (--bootstrap / --apply-iam); the code-only deploy path uses
# check_iam_policy_on_deploy and
# never reaches this function. See probe_role_presence and
# the may_create_role rail, which the 45 scripts queued onto this helper by
# alpha-engine-config-I9207 will need as they migrate.
apply_iam_policy() {
  local role_name="$1" policy_name="$2" policy_file="$3" trust_policy="$4"
  local managed_policy_arn="${5:-}"
  local may_create_role="${6:-true}"

  local presence
  presence="$(probe_role_presence "${role_name}")"
  case "${presence}" in
    present)
      echo "  IAM role exists: ${role_name}"
      ;;
    absent)
      if [ "${may_create_role}" != "true" ]; then
        # alpha-engine-config-I9045 / the four-red-deploys defect. Creating a
        # role is a BOOTSTRAP act, and the single-writer rule (infrastructure/
        # iam/README.md) reserves it for an operator. The auto-apply-on-deploy
        # path must never attempt it: it holds no iam:CreateRole by design, so
        # the attempt can only ever fail, and failing here aborts a deploy
        # whose CODE has already shipped.
        echo "  IAM role ABSENT: ${role_name} — not creating it from this path." >&2
        echo "  This is the code-only auto-apply path, which is deliberately not a" >&2
        echo "  role creator (single-writer rule). Run this deploy.sh --bootstrap as" >&2
        echo "  an operator to create it. Skipping the policy apply too: there is no" >&2
        echo "  role to put it on." >&2
        APPLY_IAM_POLICY_VERDICT="role-absent"
        return 0
      fi
      echo "  Creating IAM role: ${role_name}"
      run aws iam create-role \
        --role-name "${role_name}" \
        --assume-role-policy-document "${trust_policy}" \
        --query 'Role.RoleName' --output text
      if [ -n "${managed_policy_arn}" ]; then
        run aws iam attach-role-policy \
          --role-name "${role_name}" \
          --policy-arn "${managed_policy_arn}"
      fi
      if ! $DRY_RUN; then
        echo "  Waiting 10s for IAM role propagation..."
        sleep 10
      fi
      ;;
    *)
      # `unknown` — the probe was DENIED or failed for a reason that is not
      # NoSuchEntity. It is NOT evidence of absence, and the old code treated
      # it as exactly that (see probe_role_presence). Fall through to the
      # policy apply, which is the call whose own success or AccessDenied is
      # the real answer.
      echo "  IAM role presence UNKNOWN for ${role_name} (the get-role probe was" >&2
      echo "  denied or unreadable). Not creating anything on an unknown; the" >&2
      echo "  policy apply below is the authoritative attempt." >&2
      ;;
  esac

  # Report whether this apply actually CHANGES anything
  # (alpha-engine-config-I7444).
  #
  # put-role-policy is idempotent, so re-applying an unchanged document
  # succeeds exactly like a real change and the caller's "IAM applied" line
  # prints either way. The command is therefore indistinguishable from a
  # no-op by reading its output — which is precisely how, on 2026-08-16, an
  # operator ran this from a checkout on `main` while the changed
  # iam-policy.json lived on an unmerged PR branch, saw the success line,
  # and reasonably believed the grant had landed. It had not: the file that
  # was applied was main's, identical to what was already live. The
  # iam-policy-change-guard stayed red and the cause was invisible.
  #
  # Comparing live against the file first costs one read and converts an
  # unfalsifiable "applied" into a statement with content. Best-effort by
  # design: if the read or the comparison cannot be done, say so and apply
  # anyway — a diagnostic must never be able to block the apply itself.
  local verdict
  verdict="$(probe_role_policy_state "${role_name}" "${policy_name}" "${policy_file}")"

  echo "  Applying inline policy: ${policy_name}"
  case "${verdict}" in
    same)
      echo "  NOTE: live policy already matches ${policy_file} — this apply is a NO-OP." >&2
      echo "  NOTE: If you expected a change, you are almost certainly running from the" >&2
      echo "  NOTE: wrong checkout: this reads iam-policy.json from the working tree, so" >&2
      echo "  NOTE: a policy edit that lives on an unmerged branch requires running from" >&2
      echo "  NOTE: THAT BRANCH (alpha-engine-config-I7444)." >&2
      ;;
    different) echo "  live policy DIFFERS from ${policy_file} — applying the change." ;;
    absent)    echo "  no inline policy ${policy_name} on the role yet (an explicit\n  NoSuchEntity, not a denied read) — first apply." ;;
    *)         echo "  (live policy could not be READ from this identity; the comparison is\n  unknown, not absent. Applying anyway.)" >&2 ;;
  esac

  run aws iam put-role-policy \
    --role-name "${role_name}" \
    --policy-name "${policy_name}" \
    --policy-document "file://${policy_file}"

  APPLY_IAM_POLICY_VERDICT="${verdict}"
}

# check_iam_policy_on_deploy — the code-only deploy path's IAM surface.
#
# READ-ONLY BY CONSTRUCTION. It reports whether live IAM matches
# iam-policy.json and names the operator command that fixes a mismatch. It
# issues no IAM write of any kind, and the guard in
# tests/test_iam_auto_apply_code_only_path.py executes it against a fake `aws`
# that fails loudly on every IAM-mutating verb.
#
# WHY IT REPLACED apply_iam_policy_on_deploy (alpha-engine-config-I9045,
# measured in run 33229043798 on 2026-08-29).
#
# The old function called `aws iam put-role-policy` on every merge-triggered
# deploy and then classified the resulting AccessDenied as expected. That is
# backwards. `github-actions-lambda-deploy` does not hold iam:PutRolePolicy and
# must not — the single-writer rule (infrastructure/iam/README.md), adopted
# after 4 IAM-clobber incidents in 2 months. Per identity-access-policy.md §4
# the answer to a denied write is never to grant the permission; here it was
# also not to keep making the call. So every merge emitted a CloudTrail
# AccessDenied on iam:PutRolePolicy, and the log line that "explained" it was
# indistinguishable from the same error raised for a different reason.
#
# It also mislabelled the state it reported: `get-role-policy` is denied to the
# same identity, and the old code read that denial as "no inline policy on the
# role yet — first apply", about a policy live since 2026-07-22. See
# probe_role_policy_state.
#
# THE SEPARATION THIS RESTORES. `deploy.sh` flagless is CODE ONLY — the claim
# every deploy-*.yml already makes in its own header. IAM mutation happens on
# exactly two paths, both requiring an operator to state the intent with a
# flag: `--bootstrap` (create) and `--apply-iam` (re-apply). A flag IS the
# declaration of intent; an identity check would not be.
#
# WHAT IS TRADED. #4472's auto-apply-on-merge is gone for a privileged operator
# running deploy.sh flagless: they now get a loud DRIFT line plus the exact
# command instead of a silent apply. Principle 3 (Automation) would prefer the
# apply; principle 5 (Human authority) and the single-writer rule outrank it,
# and check-drift.py — every PR and daily — remains the standing detector.
#
# Usage: check_iam_policy_on_deploy <role_name> <policy_name> <policy_file>
check_iam_policy_on_deploy() {
  local role_name="${1:?check_iam_policy_on_deploy: role name required}"
  local policy_name="${2:?check_iam_policy_on_deploy: policy name required}"
  local policy_file="${3:?check_iam_policy_on_deploy: policy file required}"

  # A missing iam-policy.json is a broken checkout, not a drift verdict. Fail
  # loud: the callers run under `set -euo pipefail`.
  if [ ! -r "${policy_file}" ]; then
    echo "ERROR: check_iam_policy_on_deploy: policy file not readable: ${policy_file}" >&2
    return 1
  fi

  local presence state
  presence="$(probe_role_presence "${role_name}")"
  if [ "${presence}" = "absent" ]; then
    # An explicit NoSuchEntity — the one observation that proves absence.
    echo "IAM DRIFT: role ${role_name} does not exist." >&2
    echo "IAM DRIFT: creating it is an operator act (single-writer rule). Run:" >&2
    echo "IAM DRIFT:   bash ${0} --bootstrap" >&2
    IAM_POLICY_CHECK_VERDICT="role-absent"
    return 0
  fi

  state="$(probe_role_policy_state "${role_name}" "${policy_name}" "${policy_file}")"
  case "${state}" in
    same)
      echo "  IAM: live ${policy_name} matches ${policy_file} — no drift."
      ;;
    different)
      echo "IAM DRIFT: live ${policy_name} DIFFERS from ${policy_file}." >&2
      echo "IAM DRIFT: this deploy shipped CODE ONLY and did not change IAM. Run:" >&2
      echo "IAM DRIFT:   bash ${0} --apply-iam" >&2
      ;;
    absent)
      echo "IAM DRIFT: role ${role_name} carries no inline policy ${policy_name}" >&2
      echo "IAM DRIFT: (an explicit NoSuchEntity, not a denied read). Run:" >&2
      echo "IAM DRIFT:   bash ${0} --apply-iam" >&2
      ;;
    *)
      # `unknown` — the read was denied. This is the CI identity's normal
      # state and is NOT a claim that the policy is absent or in drift.
      echo "  IAM: cannot verify ${policy_name} on ${role_name} from this identity" >&2
      echo "  (the get-role-policy read was denied). Expected for the CI auto-deploy" >&2
      echo "  OIDC role, which holds no IAM read or write on lambda execution roles" >&2
      echo "  by design. check-drift.py (every PR + daily) is the standing detector." >&2
      ;;
  esac

  IAM_POLICY_CHECK_VERDICT="${state}"
  return 0
}
