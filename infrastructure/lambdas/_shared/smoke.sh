# shellcheck shell=bash
#
# smoke.sh — the ONE assertion every deploy.sh's --smoke path calls after
# invoking its Lambda: did the handler CRASH. Nothing else.
# (alpha-engine-config-I7535, following the I7379 fix on one script.)
#
# WHY. `aws lambda invoke` exits 0 when the handler RAISES — a handler
# exception is a successful *invocation* carrying a `FunctionError: Unhandled`
# header (present on the invoke command's OWN stdout, not the response
# payload file) and an `{"errorType": ..., "errorMessage": ...}` body in the
# response payload file. Measured 2026-08-17: all 31 `--smoke` paths in this
# repo sent that stdout to `/dev/null`, printed the response body, and fell
# through to `exit 0` regardless of its content. An operator running
# `deploy.sh --smoke` against a completely broken handler saw output and a
# zero exit, and read it as a pass.
#
# This is deliberately UNIVERSAL: "did the handler crash" needs no
# per-handler knowledge. Per-handler semantic success checks (e.g. `grep -q
# '"fired": *true'` in eod-success-friday-shell-trigger/deploy.sh) are a
# SEPARATE, additional assertion — keep them, and run this one first, since a
# crashed handler can also fail a semantic check for the wrong reason.
#
# Usage — call immediately after `aws lambda invoke`, capturing the invoke
# command's OWN stdout (not the response file) instead of discarding it:
#
#   RESP=$(mktemp)
#   INVOKE_STDOUT=$(aws lambda invoke \
#     --function-name "${FUNCTION_NAME}" \
#     --cli-binary-format raw-in-base64-out \
#     --payload '{}' \
#     --region "${REGION}" \
#     "${RESP}")
#   cat "${RESP}"
#   echo ""
#   assert_no_function_error "${INVOKE_STDOUT}" "${RESP}"
#   rm -f "${RESP}"
#
# Fails loud: prints the crash's errorType/errorMessage to stderr and exits
# non-zero. Never warns and continues — a swallowed crash here is exactly the
# defect this file exists to close.

assert_no_function_error() {
  local invoke_stdout="${1:?assert_no_function_error: invoke stdout required}"
  local resp_file="${2:?assert_no_function_error: response file required}"

  python3 - "${invoke_stdout}" "${resp_file}" <<'PY'
import json
import sys

invoke_stdout, resp_path = sys.argv[1], sys.argv[2]

# Signal 1: the invoke command's OWN stdout carries FunctionError when the
# handler raised. This is what every deploy.sh in this repo was piping to
# /dev/null.
function_error = None
try:
    invoke_doc = json.loads(invoke_stdout) if invoke_stdout.strip() else {}
    if isinstance(invoke_doc, dict):
        function_error = invoke_doc.get("FunctionError")
except json.JSONDecodeError:
    # Malformed invoke stdout is itself abnormal (the AWS CLI always emits
    # valid JSON here absent --query/--output). Fail loud rather than
    # silently treating "could not parse" as "did not crash".
    print(
        "SMOKE FAILED: could not parse `aws lambda invoke`'s own stdout as "
        f"JSON: {invoke_stdout!r}. Do not pass --query/--output to the "
        "invoke call that feeds assert_no_function_error.",
        file=sys.stderr,
    )
    sys.exit(1)

# Signal 2: the response payload body carries errorType/errorMessage on a
# crash (the standard Lambda Python-runtime unhandled-exception shape).
error_type = None
error_message = None
try:
    with open(resp_path, encoding="utf-8") as f:
        body = json.load(f)
    if isinstance(body, dict):
        error_type = body.get("errorType")
        error_message = body.get("errorMessage")
except (OSError, json.JSONDecodeError):
    # An unreadable/non-JSON response body is not this function's concern —
    # a per-handler assertion (if any) downstream of this call is what
    # judges response SHAPE. This function only judges "did it crash".
    pass

if function_error or error_type:
    print(
        "SMOKE FAILED: the handler CRASHED — FunctionError="
        f"{function_error!r} errorType={error_type!r}.",
        file=sys.stderr,
    )
    if error_message:
        print(f"  {error_message}", file=sys.stderr)
    sys.exit(1)

sys.exit(0)
PY
}

# assert_sf_lambda_task_not_failed — the same crash check for the ONE deploy.sh
# (scheduled-groom-dispatcher) whose --smoke path dispatches through a Step
# Function rather than a direct `aws lambda invoke`, so there is no invoke
# response to hand assert_no_function_error. The SF's first state IS "invoke
# the groom Lambda" (config#1472), so a handler crash surfaces as that
# execution transitioning to FAILED almost immediately — well before the
# multi-hour groom itself would finish. This polls briefly for that
# transition; it does NOT wait for the full run.
#
# Usage:
#   EXEC_ARN=$(aws stepfunctions start-execution ...)
#   assert_sf_lambda_task_not_failed "${EXEC_ARN}" "${REGION}"
#
# Fails loud on a FAILED execution within the poll window. A still-RUNNING or
# SUCCEEDED execution after the window is reported as "no crash observed" —
# honestly bounded, since this function cannot and must not wait out an
# hours-long groom to call that a pass.
assert_sf_lambda_task_not_failed() {
  local exec_arn="${1:?assert_sf_lambda_task_not_failed: execution ARN required}"
  local region="${2:?assert_sf_lambda_task_not_failed: region required}"
  local max_wait_s="${3:-90}"
  local interval_s=5
  local waited=0
  local status=""

  while [ "${waited}" -lt "${max_wait_s}" ]; do
    status=$(aws stepfunctions describe-execution \
      --execution-arn "${exec_arn}" \
      --region "${region}" \
      --query 'status' --output text)
    case "${status}" in
      FAILED)
        local error cause
        error=$(aws stepfunctions describe-execution \
          --execution-arn "${exec_arn}" --region "${region}" \
          --query 'error' --output text 2>/dev/null || echo "")
        cause=$(aws stepfunctions describe-execution \
          --execution-arn "${exec_arn}" --region "${region}" \
          --query 'cause' --output text 2>/dev/null || echo "")
        echo "SMOKE FAILED: the SF execution FAILED — the groom-dispatcher" >&2
        echo "  handler crashed on invoke. error=${error}" >&2
        echo "  cause=${cause}" >&2
        return 1
        ;;
      RUNNING)
        sleep "${interval_s}"
        waited=$((waited + interval_s))
        ;;
      *)
        # SUCCEEDED, ABORTED, TIMED_OUT (not a handler crash signal) — stop
        # polling, no crash observed.
        return 0
        ;;
    esac
  done

  echo "  (no FAILED status observed within ${max_wait_s}s — the Lambda task" >&2
  echo "   likely completed and the groom itself is still running; this is" >&2
  echo "   not a crash check on the multi-hour groom, only on the dispatch.)" >&2
  return 0
}

# Assert a handler-specific response contract after assert_no_function_error.
# EXPECTED_JSON is parsed as JSON, so callers can distinguish booleans and
# numbers from strings. Dotted field names address nested response objects.
assert_response_field_equals() {
  local resp_file="${1:?assert_response_field_equals: response file required}"
  local field="${2:?assert_response_field_equals: field required}"
  local expected_json="${3:?assert_response_field_equals: expected JSON required}"

  python3 - "${resp_file}" "${field}" "${expected_json}" <<'PY'
import json
import sys

resp_path, field, expected_json = sys.argv[1:]
try:
    with open(resp_path, encoding="utf-8") as response_file:
        value = json.load(response_file)
    for part in field.split("."):
        value = value[part]
    expected = json.loads(expected_json)
except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
    print(f"SMOKE FAILED: response does not satisfy field {field!r}: {exc}", file=sys.stderr)
    sys.exit(1)
if value != expected:
    print(
        f"SMOKE FAILED: expected response field {field!r} to equal "
        f"{expected!r}, got {value!r}.",
        file=sys.stderr,
    )
    sys.exit(1)
PY
}

assert_response_field_present() {
  local resp_file="${1:?assert_response_field_present: response file required}"
  local field="${2:?assert_response_field_present: field required}"

  python3 - "${resp_file}" "${field}" <<'PY'
import json
import sys

resp_path, field = sys.argv[1:]
try:
    with open(resp_path, encoding="utf-8") as response_file:
        value = json.load(response_file)
    for part in field.split("."):
        value = value[part]
except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
    print(f"SMOKE FAILED: response does not contain required field {field!r}: {exc}", file=sys.stderr)
    sys.exit(1)
PY
}
