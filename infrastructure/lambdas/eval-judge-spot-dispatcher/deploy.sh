#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-research-eval-judge-spot-dispatcher Lambda.
#
# This Lambda is the SF-invokable launcher for the DEDICATED spot box the
# weekly pipeline's `EvalJudgeProcess` stage now runs on
# (alpha-engine-config-I9329, following -I9309). It is invoked DIRECTLY by the
# weekly Step Function's `DispatchEvalJudgeSpot` state, so — like
# data-spot-dispatcher and weekly-freshness-spot-dispatcher — there is NO
# wrapping Step Function and NO EventBridge rule to wire here. This script
# manages only the Lambda and its execution role.
#
# NO SF EXECUTION-ROLE DELTA IS NEEDED for the invoke, and that is deliberate,
# not an omission: `alpha-engine-step-functions-role`'s existing
# `lambda:InvokeFunction` grant already carries the resource wildcard
# `arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-research-eval-judge*`
# (verified live 2026-08-29), which this function name falls under. The one IAM
# change the cutover DOES need is `ssm:SendCommand` on the new box's tag, and
# that lives in nous-ergon-ops
# (infrastructure/iam/alpha-engine-step-functions-role/...-policy.json), where
# iam-apply-on-merge.yml applies it automatically on merge.
#
# Managed OUTSIDE CloudFormation — same rationale as every sibling dispatcher:
# keep the github-actions-lambda-deploy OIDC role's blast radius narrow. That
# OIDC role deliberately LACKS iam:CreateRole / iam:PutRolePolicy (fleet-wide
# after four IAM-clobber incidents in two months), so the FIRST-TIME
# `--bootstrap` — which mints the execution role and creates the function —
# MUST be run by an operator with IAM rights. That is a real privilege
# boundary, and TWO detectors stay red until it happens:
#
#   * this Lambda's own deploy workflow
#     (.github/workflows/deploy-eval-judge-spot-dispatcher.yml), whose
#     preflight below prints the exact command and exits non-zero;
#   * infrastructure/step-functions/check-lambda-existence.py, run by
#     sf-arn-drift-check.yml on every push to main and daily at 09:30 UTC,
#     which fails on any SF `lambda:invoke` naming a function that does not
#     exist live.
#
# ORDERING WARNING (mirrors data-spot-dispatcher's README and its 2026-07-08
# postmortem): the invoking SF definition (step_function.json) auto-deploys on
# merge via deploy-infrastructure.yml. Bootstrap this Lambda BEFORE merging the
# SF change, or `DispatchEvalJudgeSpot` 404s on the invoke the next Saturday —
# fail-soft into `MarkEvalJudgeDegraded`, so the run survives, but the week's
# eval coverage does not.
#
# Usage:
#   bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh             # update code + env only (CI auto-deploy path)
#   bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh --bootstrap # operator-only: create/update the execution role + create the Lambda
#   bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-research-eval-judge-spot-dispatcher"
ROLE_NAME="alpha-engine-research-eval-judge-spot-dispatcher-role"
POLICY_NAME="alpha-engine-research-eval-judge-spot-dispatcher-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

# Canonical function env — defined ONCE so the create / update paths can never
# drift out of lockstep. Every EVAL_JUDGE_SPOT_* knob besides the kill-switch
# uses the handler's in-code default, so it is intentionally NOT set here.
# In particular EVAL_JUDGE_SPOT_WATCHDOG_SECONDS is NOT set here: the handler
# refuses to launch when it does not exceed bootstrap + judge budget, and an
# env override is exactly how that inequality would be broken out of band.
PROD_ENV='Variables={LOG_LEVEL=INFO,EVAL_JUDGE_SPOT_DISPATCH_ENABLED=true,EVAL_JUDGE_SPOT_APPCONFIG_APPLICATION=yq405wh}'

# Timeout must cover the handler's worst case: launch a spot (RunInstances +
# state poll; longer on the on-demand fallback after capacity retries) PLUS
# the full SSM-online wait (EVAL_JUDGE_SPOT_SSM_ONLINE_BUDGET_SEC default 300s)
# before the async detached SSM send-command + return. 600s mirrors data-
# spot-dispatcher's identical launch+online composition (this Lambda does
# NOT wait for the bootstrap itself to finish — only fires it and returns;
# the SF's own poll loop waits for bootstrap completion).
FN_TIMEOUT=600
FN_MEMORY=256

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below (config-I2752 convention — see data-spot-dispatcher/deploy.sh).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Scratch dir + validate handler syntax ------------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# A syntax check is not a preflight: it proves index.py parses, not that the
# handler still behaves. Through the SHARED helper so this gate cannot re-drift
# into the naive no-install form (config#2381, after the config#2295 incident).
# shellcheck source=infrastructure/lambdas/_shared/run_handler_tests.sh
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
# krepis ONLY, and it is the MEASURED set, not a reasoned one (bare interpreter,
# 2026-08-29: 15/15 pass with krepis alone). test_handler.py stubs
# nousergon_lib + nousergon_lib.spot_dispatch in sys.modules, but imports
# krepis.spot_bootstrap FOR REAL because the rendered bootstrap is the thing
# under test. Passing NOUSERGON_LIB_REQ too would re-pull the heavy git-only
# dependency on every redeploy to satisfy a module the tests replace — the
# minimal-set half of the helper's declared deploy.sh-vs-ci.yml contract. boto3
# is deliberately absent: the helper never installs it implicitly, and index.py
# reaches AWS only through the stubbed spot_dispatch chokepoint.
KREPIS_REQ=$(requirement_pin "${SCRIPT_DIR}/requirements.txt" krepis)
run_handler_tests "${SCRIPT_DIR}" "${KREPIS_REQ}"

# ----- 1. Package: pip install deps (Lambda-safe) + zip handler --------------

LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Installing deps into ${PKG} (Lambda-safe pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only, operator-run) --------------------------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no alarms."
  exit 0
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline policy ---
  # Through the SHARED helper, never a hand-rolled
  # `if ! aws iam get-role ... >/dev/null 2>&1` probe. That probe makes
  # AccessDenied and NoSuchEntity the same observation, so a DENIED read is
  # acted on as proof of absence -- alpha-engine-config-I9045, four workflows
  # red on every run for a week. `apply_iam_policy` reaches create-role only
  # via `probe_role_presence`, which classifies a denial as `unknown` and
  # creates nothing. Enforced by tests/test_iam_role_creation_is_helper_gated.py.
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" \
    "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for IAM role propagation..."
    sleep 10
  fi

  # --- 2b. Lambda function ---
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  echo "  Creating Lambda: ${FUNCTION_NAME} (idempotent)"
  # `run_tolerating` rather than a get-function probe, for the same reason as
  # the role above: the already-exists conflict is the ONE benign failure, and
  # a probe would report that same reassuring line for AccessDenied.
  run_tolerating "ResourceConflictException" \
    aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout "${FN_TIMEOUT}" \
      --memory-size "${FN_MEMORY}" \
      --environment "${PROD_ENV}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
fi

# ----- 2c. Bootstrap preflight (the operator-gate detector) -------------------
# A flagless run against a function that was never created must not read as a
# transient AWS error. `pull-request-policy.md` §4.2 admits an operator-gated
# post-merge step only when the merge EMITS the exact command AND a detector
# stays red until it runs — this is the emitting half, and the non-zero exit is
# the red half. `2>/dev/null` is deliberately NOT used on stderr alone: a
# DENIED get-function and an ABSENT function must not be the same observation
# (alpha-engine-config-I9045), so the message names both possibilities.
if ! $BOOTSTRAP; then
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" \
        --region "${REGION}" --query 'Configuration.FunctionName' --output text >/dev/null; then
    echo "ERROR: ${FUNCTION_NAME} does not exist live (or this identity cannot read it)." >&2
    echo "" >&2
    echo "If it is ABSENT this is the first deploy, and creating the execution role" >&2
    echo "is a privilege this CI identity deliberately does not hold. Run, as an" >&2
    echo "operator with IAM rights, from a nousergon-data checkout on main:" >&2
    echo "" >&2
    echo "  AWS_PROFILE=ne-admin bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh --bootstrap" >&2
    echo "" >&2
    echo "Expected after it: the command prints 'Creating IAM role', 'Creating Lambda'," >&2
    echo "then 'Code deployed' and 'Env converged'. This workflow and SF-ARN Drift Check" >&2
    echo "both go green on their next run; until then both stay red BY DESIGN." >&2
    echo "" >&2
    echo "If it is a DENIED read instead, nothing is missing - fix the identity." >&2
    exit 1
  fi
fi

# ----- 3. Update function code (always, idempotent) --------------------------
# On a not-yet-bootstrapped function this update-function-code FAILS LOUD with a
# 404 (set -e aborts) — deliberately: a flagless run cannot silently "succeed"
# when the function was never created. Run --bootstrap first.

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

echo "✓ Code deployed."

# ----- 4. Converge function env (always) -------------------------------------

echo "Converging Lambda environment..."
run aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --environment "${PROD_ENV}" \
  --timeout "${FN_TIMEOUT}" \
  --memory-size "${FN_MEMORY}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi
echo "✓ Env converged: ${PROD_ENV}"

echo ""
echo "Done. Next (operator):"
echo "  1. Nothing for the SF invoke grant - alpha-engine-step-functions-role already"
echo "     wildcards alpha-engine-research-eval-judge* (verified live 2026-08-29)."
echo "  2. The ssm:SendCommand grant for the new box tag ships in nous-ergon-ops and"
echo "     applies itself on merge (iam-apply-on-merge.yml)."
echo "  3. Validate end to end via a shell-run (bash infrastructure/run_weekly_offcycle.sh shell)"
echo "     BEFORE the next real Saturday cron fire: that path sets research_dry=true and"
echo "     preflight_args=' --preflight-only', so the box boots, imports and probes S3"
echo "     without grading an artifact or spending a token."
