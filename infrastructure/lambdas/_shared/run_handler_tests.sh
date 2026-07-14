#!/usr/bin/env bash
# run_handler_tests.sh — the SINGLE source of the "provision-then-run a lambda's
# handler unit tests" invariant, sourced by every infrastructure/lambdas/*/deploy.sh
# preflight gate AND by .github/workflows/ci.yml's glob step (config#2381).
#
# WHY (the drift class this kills — config#2295 incident, 2026-07-12): the
# "install this lambda's test deps, then run its test_handler.py" step used to
# be hand-written ~20 times, once per deploy.sh, with each copy re-implementing
# the pip-install list by hand. saturday-sf-watch-dispatcher's copy was written
# in the naive `python3 -m pytest test_handler.py` form with NO install; ci.yml
# stayed green (it uses its own correct glob runner), so the drift was invisible
# pre-merge and only bit POST-merge as a red deploy ("No module named pytest" on
# the bare deploy runner — nousergon-data#773). Extracting the mechanism into ONE
# helper means no deploy.sh can re-drift into the naive no-install form, and the
# pre-merge guard (ci.yml) and post-merge gate (deploy.sh) share one implementation.
#
# The helper owns the MECHANISM only (scratch dir + pip install pytest + caller's
# deps + PYTHONPATH + AWS_DEFAULT_REGION + pytest + cleanup). Each caller declares
# its own dep list as positional args — deliberately NOT derived purely from
# requirements.txt, because the two contexts legitimately differ:
#   * ci.yml (pre-merge) passes `-r <lambda>/requirements.txt` — the superset
#     source-of-truth model (config#1759), safe because sys.modules stubs in the
#     tests take precedence over anything installed;
#   * deploy.sh (post-merge / operator laptop) passes a MINIMAL explicit set so a
#     redeploy doesn't re-pull the heavy git-only nousergon-lib on lambdas whose
#     tests stub it. Both go through this one install-then-run mechanism, so
#     neither can re-drift into the naive form regardless of its dep list.
#
# Non-inferable gotchas baked in here:
#   * AWS_DEFAULT_REGION is exported (default us-east-1) — ssm-liveness-poller and
#     any future handler call boto3.client() at MODULE SCOPE with no explicit
#     region and hit botocore's NoRegionError on a bare runner otherwise.
#   * Tests that stub boto3 in sys.modules (ci-watch-dispatcher, the dispatchers)
#     MUST NOT get boto3 installed alongside — so boto3 is NEVER installed
#     implicitly here; a caller passes `boto3` only when its test does a real
#     `import index` against real boto3 (e.g. eod-backstop, ssm-liveness-poller).
#   * changelog-{incident,cloudwatch}-mirror are intentionally NOT wired through
#     this helper — they run `python3 test_handler.py` with zero deps and no
#     pytest; that carve-out is preserved in ci.yml and their deploy scripts.

# run_handler_tests SCRIPT_DIR [pip-install-args...]
#   SCRIPT_DIR         the lambda dir containing index.py + test_handler.py
#   pip-install-args   extra pip args installed alongside pytest into a scratch
#                      dir (explicit specs like `boto3` / "${NOUSERGON_LIB_REQ}",
#                      or `-r "${SCRIPT_DIR}/requirements.txt"`)
# Optional env:
#   HANDLER_TEST_PYTHONPATH  extra colon-path appended after the scratch deps dir
#                            (e.g. the lambdas dir so `import flow_doctor_telegram`
#                            resolves for tests that don't self-path)
#   HANDLER_TEST_TARGETS     extra pytest target paths run alongside test_handler.py
#                            (e.g. sf-telegram-notifier's test_execution_digest.py)
# Returns pytest's exit code (0 if the lambda has no test_handler.py). Cleans up
# its own scratch dir; safe under `set -euo pipefail`.
run_handler_tests() {
  local script_dir="$1"; shift
  local test_file="${script_dir}/test_handler.py"
  if [[ ! -f "${test_file}" ]]; then
    return 0
  fi

  local deps_dir
  deps_dir=$(mktemp -d)

  echo "Installing pytest${*:+ + $*} into ${deps_dir}..." >&2
  if ! python3 -m pip install --quiet --target "${deps_dir}" pytest "$@"; then
    echo "  ✗ test-dep install failed" >&2
    rm -rf "${deps_dir}"
    return 1
  fi

  local pypath="${deps_dir}${HANDLER_TEST_PYTHONPATH:+:${HANDLER_TEST_PYTHONPATH}}"
  echo "Running handler unit tests (${test_file##*/}${HANDLER_TEST_TARGETS:+ + ${HANDLER_TEST_TARGETS}})..." >&2

  local rc=0
  # ${HANDLER_TEST_TARGETS} intentionally unquoted — space-separated extra targets.
  PYTHONPATH="${pypath}" \
  AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
    python3 -m pytest "${test_file}" ${HANDLER_TEST_TARGETS:-} -q || rc=$?

  rm -rf "${deps_dir}"
  return "${rc}"
}
