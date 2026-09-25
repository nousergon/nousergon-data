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
#   (no flags)    update the Lambda's code + converge its timeout (FN_TIMEOUT)
#   --bootstrap   create the IAM role + Lambda (idempotent)
#   --apply-iam   re-apply iam-policy.json only, no code/bootstrap side effects (config#2825)
#   --smoke       fire ONE real run on a REAL spot box (the §47 validation gate)
#   --cutover     repoint alpha-research-thinktank-daily at this dispatcher
#   --dry-run     print every mutating call instead of making it
#
# See README.md for the full rollout order. crucible-research-PR544 must be
# merged to main BEFORE --smoke: the SSM prelude execs
# infrastructure/thinktank_spot_bootstrap.sh out of a shallow clone of main.
#
# WHY THIS SCRIPT NO LONGER HAND-ROLLS ANYTHING (alpha-engine-config-I9114).
# Until 2026-08-28 this was the one deploy.sh in a private dialect: it made bare
# `aws` calls instead of `run`, packaged with a host-arch `pip install --target`
# instead of the shared Lambda-safe installer, never ran its own 25 handler
# tests, and hand-rolled its IAM with the exact
#
#     aws iam get-role ... >/dev/null 2>&1 || { aws iam create-role ...; }
#
# misclassification that made four sibling workflows red for a week
# (alpha-engine-config-I9045 / nousergon-data-PR1569 — `2>&1` to /dev/null makes
# AccessDenied and NoSuchEntity the same observation, so a DENIED read was read
# as "role absent"). Fixing `_shared/apply_iam_policy.sh` did not fix this copy,
# which is the whole reason a private dialect is a defect and not a style. Every
# mechanism below is now the shared one, so the next fix to it lands here too.
#
# alpha-engine-config-I11227: iam-policy.json's `ec2:RunInstances` grant was
# `Resource: "*"` with NO condition, so this role could launch any instance
# type in the account — a p5.48xlarge is ~$98/h and the cost-control loop's
# first alert is ~1.5 days out. It now mirrors the shape four sibling
# dispatcher roles already carry: an instance-resource statement conditioned
# on `aws:RequestTag/Name` = `alpha-engine-thinktank-spot` plus
# `ec2:InstanceType` in the set this dispatcher actually launches
# (c5.large, c5a.large, c6i.large, m5.large, m5a.large, m6i.large, r5.large,
# r5a.large, r6i.large — widened alpha-engine-config-I11340 item 5), and a
# separate, unconditioned statement for the non-instance resources
# RunInstances also authorises (subnet, security group, network interface,
# volume, key pair, image, launch template) — a Condition applies to EVERY
# Resource in its statement, so they cannot share one.
#
# The allow-list is the code default of `THINKTANK_SPOT_INSTANCE_TYPES`, which
# is UNSET on the live function (measured 2026-09-20), so the default is what
# runs. Widening the pool means editing BOTH the default and this policy, then
# `--apply-iam`; the flagless CI auto-deploy path above is code-only and will
# NOT apply it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REGION="${AWS_REGION:-us-east-1}"
FUNCTION_NAME="alpha-engine-thinktank-spot-dispatcher"
ROLE_NAME="alpha-engine-thinktank-spot-dispatcher-role"
# Authoritative inline-policy assignment — the consolidated drift checker
# (nous-ergon-ops/infrastructure/iam/check-drift.py --lambdas-root) maps each
# lambda's iam-policy.json to a live policy via these two top-level lines; a
# policy file without both is a coverage gap that fails the IAM drift sweep
# (alpha-engine-config#6061, config#2340 surface 3).
POLICY_NAME="alpha-engine-thinktank-spot-dispatcher-role-policy"
RULE_NAME="alpha-research-thinktank-daily"
# The function timeout — the ONE place it is declared (alpha-engine-config-I11532).
# It was 300s, EQUAL to index.py's SSM_ONLINE_BUDGET_SEC, and on 2026-09-23 a
# slow SSM registration let the wait eat the whole invocation: Lambda killed
# the handler after the launch and before the send, and the day's run was lost.
# It must exceed launch + the lib's 200s instance_running waiter +
# SSM_ONLINE_BUDGET_SEC + send. index.py mirrors it as LAMBDA_TIMEOUT_SECONDS;
# test_handler.py parses THIS line, fails if the two differ, and fails if the
# sum above stops fitting. Converged onto the live function on every deploy
# (step below), so raising it here is the whole change.
FN_TIMEOUT=900
# Same topic the alarms this cutover replaces already publish to, so the
# rotation does not silently change where a Think Tank page lands.
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts}"
TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run flag
# below, matching every sibling deploy.sh (alpha-engine-config-I2752: an
# operator assumed DRY_RUN=<env var> worked and triggered a real deploy). It
# must be DEFINED whatever happens: `apply_iam_policy` reads a bare `$DRY_RUN`,
# so leaving it unset would abort under this script's `set -u`.
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac

BOOTSTRAP=false; SMOKE=false; CUTOVER=false; APPLY_IAM=false
while [ $# -gt 0 ]; do
    case "$1" in
        --bootstrap) BOOTSTRAP=true; shift ;;
        --smoke) SMOKE=true; shift ;;
        --cutover) CUTOVER=true; shift ;;
        --apply-iam) APPLY_IAM=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# Alarm upserts RESET ActionsEnabled, so without this the next deploy re-arms any alarm the automation pause has silenced
# (alpha-engine-config-I7023).
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "${SCRIPT_DIR}/../_shared/pause.sh"
# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"
# shellcheck source=infrastructure/lambdas/_shared/apply_iam_policy.sh
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"

ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"

# ----- Apply IAM only (config#2825, no bootstrap/code side effects) --------
# Reached ONLY under --apply-iam, i.e. by an operator. apply_iam_policy's
# default may_create_role=true is therefore correct here — and its
# probe_role_presence means a DENIED get-role is reported as `unknown` and
# creates nothing, rather than being misread as absence.
if $APPLY_IAM; then
    echo "==> applying IAM (role=$ROLE_NAME, policy=$POLICY_NAME)"
    apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" \
      "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
    echo "  ✓ IAM applied. Nothing else was touched — no code, no alarms."
    echo "==> done"
    exit 0
fi

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
# 25 tests sat beside index.py that this deploy.sh never ran, so the post-merge
# gate was absent for this lambda entirely. No extra deps: they stub
# nousergon_lib and boto3 in sys.modules, and the helper's contract is that such
# a caller must NOT get boto3 installed alongside the stub.
# shellcheck source=infrastructure/lambdas/_shared/run_handler_tests.sh
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
# `-r requirements.txt` (the superset model ci.yml uses), not the empty dep
# list alpha-engine-config-I9114 predicted. Measured on a bare interpreter
# 2026-08-29: the tests do NOT stub nousergon_lib — 2 of 25 import it for
# real and ModuleNotFound without it. A dep list that was reasoned about
# rather than executed is how deploy-data-spot-dispatcher and
# deploy-ssm-reachability-probe went red on main the same evening.
run_handler_tests "${SCRIPT_DIR}" -r "${SCRIPT_DIR}/requirements.txt"

echo "==> building package"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT
# The shared Lambda-safe installer, not `pip install --target` (which this
# script used until I9114). A bare host install on macOS bundles darwin/arm64
# wheels that ImportError on the Lambda linux/amd64 runtime; the helper also
# chowns the tree back so a Linux CI runner's non-root cleanup does not EPERM.
bash "${SCRIPT_DIR}/../lambda_pip_install.sh" "$BUILD_DIR" "${SCRIPT_DIR}/requirements.txt"
cp "$SCRIPT_DIR/index.py" "$BUILD_DIR/"
# Inside BUILD_DIR, not a fixed /tmp path: the old /tmp/thinktank-spot-dispatcher.zip
# was shared mutable state between concurrent runs and survived the trap.
ZIP="$BUILD_DIR/function.zip"
(cd "$BUILD_DIR" && zip -qr function.zip . -x "function.zip")

if $BOOTSTRAP; then
    echo "==> creating IAM role $ROLE_NAME + applying policy (idempotent)"
    apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" \
      "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"

    echo "==> creating Lambda $FUNCTION_NAME (idempotent)"
    # `run_tolerating` rather than `|| echo "(function exists)"`: the old form
    # reported that same reassuring line for AccessDenied, an invalid role ARN
    # and a malformed zip, with the real message sent to /dev/null. The
    # already-exists conflict is the ONE benign failure.
    run_tolerating "ResourceConflictException" \
      aws lambda create-function --function-name "$FUNCTION_NAME" \
        --runtime python3.12 --handler index.handler \
        --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
        --zip-file "fileb://${ZIP}" \
        --timeout "$FN_TIMEOUT" --memory-size 512 \
        --region "$REGION" --query 'FunctionArn' --output text
fi

echo "==> updating function code"
run aws lambda update-function-code --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://${ZIP}" \
    --region "$REGION" --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
    aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
fi

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

# ----- Converge the timeout (alpha-engine-config-I11532) --------------------
# update-function-code never touches configuration, so before this step the
# timeout was set ONCE, by --bootstrap's create-function, and a change to it in
# this file was inert on the live function. Timeout ONLY, deliberately: the live
# function carries no environment (measured 2026-09-25) and `--environment`
# here would make this file the owner of every env override an operator sets.
# github-actions-lambda-deploy's LambdaUpdate grant already holds
# lambda:UpdateFunctionConfiguration on function:alpha-engine-*.
echo "==> converging function timeout to ${FN_TIMEOUT}s"
run aws lambda update-function-configuration --function-name "$FUNCTION_NAME" \
    --timeout "$FN_TIMEOUT" \
    --region "$REGION" --query 'LastUpdateStatus' --output text
if ! $DRY_RUN; then
    aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
fi

if $SMOKE; then
    echo "==> SMOKE: firing ONE real Think Tank run on a REAL spot box"
    echo "    (this spends a spot instance and a real LLM pass — ~25 min expected)"
    # shellcheck source=infrastructure/lambdas/_shared/smoke.sh
    source "${SCRIPT_DIR}/../_shared/smoke.sh"
    # Full invoke response JSON captured (not --query/--output text, which
    # discards FunctionError) so assert_no_function_error can see a crash.
    INVOKE_STDOUT=$(aws lambda invoke --function-name "$FUNCTION_NAME" \
        --payload '{}' --cli-binary-format raw-in-base64-out \
        --region "$REGION" /tmp/thinktank-smoke-out.json)
    echo "    dispatcher returned:"
    cat /tmp/thinktank-smoke-out.json; echo
    assert_no_function_error "${INVOKE_STDOUT}" /tmp/thinktank-smoke-out.json
    echo "    Watch: aws logs tail /alpha-engine/thinktank-spot --follow"
    echo "    Gate:  thinktank/challenger_selection/, thinktank/ratings/ AND"
    echo "           thinktank/events/ written for the trading day, box self-terminated."
fi

if $CUTOVER; then
    echo "==> CUTOVER: repointing $RULE_NAME at $FUNCTION_NAME"
    TARGET_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
    # `|| echo "(permission exists)"` reported that same reassuring line for
    # AccessDenied and a malformed source-arn too, with the real message sent
    # to /dev/null (alpha-engine-config-I8125). The conflict is the ONE benign
    # failure; everything else means the rule cannot invoke this function.
    run_tolerating "ResourceConflictException" \
      aws lambda add-permission --function-name "$FUNCTION_NAME" \
        --statement-id "${RULE_NAME}-invoke" --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
        --region "$REGION"
    run aws events put-targets --rule "$RULE_NAME" \
        --targets "[{\"Id\":\"1\",\"Arn\":\"${TARGET_ARN}\"}]" --region "$REGION"
    echo "    $RULE_NAME now targets $FUNCTION_NAME."

    # ── Alarm rotation — ATOMIC with the repoint, deliberately ───────────────
    # The instant the rule stops targeting alpha-engine-research-thinktank, that
    # function's Errors/Duration alarms stop seeing invocations. Both were
    # created with `--treat-missing-data notBreaching`, so zero invocations
    # evaluates to OK: they go GREEN because nothing ran. That is the exact
    # silence class this whole migration exists to close (config-I5208 — the
    # Think Tank ran dead for 12 days while every surface read healthy), so
    # leaving them behind "temporarily" would reintroduce it at the moment of
    # cutover. They are deleted here, in the same action that blinds them.
    OLD_FUNCTION="alpha-engine-research-thinktank"
    DISPATCH_ALARM="alpha-engine-thinktank-spot-dispatch-failed"

    # RETIRED as an alarm-creation path (alpha-engine-config-I7359, executing
    # the I7359 ownership ruling). $DISPATCH_ALARM is codified in the PRIVATE
    # nous-ergon-ops repo:
    #   infrastructure/cloudwatch/alarms/alpha-engine-thinktank-spot-dispatch-failed.json
    # Edit it there; do not add put-metric-alarm back here —
    # nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the
    # build if it reappears. This one-time CUTOVER path already ran; the
    # alarm this block used to arm on first cutover already exists live and
    # is now codified, so nothing here needs to (re-)create it.
    echo "==> $DISPATCH_ALARM is applied from nous-ergon-ops, not here (alpha-engine-config-I7359)"

    echo "==> deleting the now-blind $OLD_FUNCTION alarms"
    run aws cloudwatch delete-alarms --alarm-names \
        "alpha-engine-thinktank-daily-run-failed" \
        "alpha-engine-thinktank-daily-run-failed-timeout" \
        --region "$REGION"
    echo "    deleted. Coverage is now: $DISPATCH_ALARM (launch) +"
    echo "    ARTIFACT_REGISTRY thinktank_challenger_selection (end-to-end)."
fi

echo "==> done"
