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
#   HANDLER_TEST_TARGETS     extra pytest target paths run alongside test_handler.py.
#                            DEFAULTS to every sibling test_*.py in SCRIPT_DIR.
#                            Set it explicitly to narrow; set it to " " to run
#                            test_handler.py alone.
# Returns non-zero if ANY target failed (0 if the lambda has no test_handler.py).
# Cleans up its own scratch dir; safe under `set -euo pipefail`.
#
# Two invariants this function owns, both earned (alpha-engine-config-I7573):
#
#  1. AUTO-DISCOVERY. HANDLER_TEST_TARGETS was set in exactly one place —
#     sf-telegram-notifier/deploy.sh — and nowhere in ci.yml, whose glob step
#     passes only the directory. So that lambda's test_execution_digest.py and
#     test_eod_artifact_verification.py ran at DEPLOY time (post-merge) and
#     never pre-merge, and its test_flow_doctor_fleet_wiring.py ran in neither.
#     A test file that no gate runs is not coverage. Defaulting to the sibling
#     glob makes pre-merge and post-merge see the same set, which is the whole
#     reason this helper exists.
#
#  2. ONE PROCESS PER FILE. They used to share a single pytest invocation, so a
#     module-scope `sys.modules[...] = <stub>` in one file leaked into every
#     file collected after it. Live instance: sf-telegram-notifier's
#     test_handler.py installs a stub of `nousergon_lib.flow_doctor_fleet` that
#     omits `fleet_telegram_notifier_dicts`, and
#     test_flow_doctor_fleet_wiring.py — which exists precisely to check the
#     config against the REAL fleet spec — then fails on import. It passes
#     alone and fails in a full run, i.e. the result depended on collection
#     order. Separate processes make each file's answer its own.
# requirement_pin REQUIREMENTS_FILE PACKAGE
#   Prints the FIRST line of REQUIREMENTS_FILE matching ^PACKAGE, with any
#   inline `# ...` comment and trailing whitespace stripped, so it is safe to
#   hand to `pip install` as a command-line argument. Exits non-zero (and
#   prints a message to stderr) if no line matches.
#
# WHY (alpha-engine-config-I10337, eval-judge-spot-dispatcher run 34711758231
# and 34312894868): eight deploy.sh scripts each did
#   KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
# and passed the WHOLE matched line — including a legitimate lockstep-bump
# trailing comment like
#   krepis==0.59.54  # bumped 0.59.41 -> 0.59.54: ... alpha-engine-config-I10226
# — straight to `pip install` as an argument. A requirements FILE tolerates an
# inline comment; a pip command-line ARGUMENT does not:
#   ERROR: Invalid requirement: 'krepis==0.59.54  # bumped ... run 34309659890'
# eval-judge-spot-dispatcher's pin happened to carry a comment on the day this
# was found; the other seven sites carry the identical unguarded grep and
# break the next time a lockstep bump annotates THEIR pin. One helper closes
# the class instead of patching the one instance.
#
# Also fails loud on a missing pin: the old `grep | head -1` silently produced
# an EMPTY string when nothing matched, which `run_handler_tests` would then
# happily install as "pytest" with no extra requirement at all — a missing pin
# read as "no extra deps needed" instead of as the config error it is.
requirement_pin() {
  local req_file="$1" pkg="$2"
  local line
  line="$(grep -E "^${pkg}" "${req_file}" | head -1)"
  if [[ -z "${line}" ]]; then
    echo "requirement_pin: no '${pkg}' requirement found in ${req_file}" >&2
    return 1
  fi
  # Strip a trailing inline comment (the lockstep-bump annotation convention)
  # and any whitespace left dangling before it.
  line="${line%%#*}"
  line="${line%"${line##*[![:space:]]}"}"
  if [[ -z "${line}" ]]; then
    echo "requirement_pin: '${pkg}' line in ${req_file} is comment-only after stripping" >&2
    return 1
  fi
  echo "${line}"
}

run_handler_tests() {
  local script_dir="$1"; shift
  local test_file="${script_dir}/test_handler.py"
  if [[ ! -f "${test_file}" ]]; then
    return 0
  fi

  local targets
  if [[ -n "${HANDLER_TEST_TARGETS+x}" ]]; then
    # A shellcheck directive's trailing prose must start with its own `#`.
    # Written as `disable=SC2206 — intentional ...` this raised SC1125
    # (error severity) on every run, which is why widening CI's shellcheck
    # scope to _shared/*.sh could not land until it was fixed
    # (alpha-engine-config-I9117).
    # shellcheck disable=SC2206  # intentional word-split on the caller's list.
    targets=(${HANDLER_TEST_TARGETS})
  else
    targets=()
    local sibling
    for sibling in "${script_dir}"/test_*.py; do
      if [[ -f "${sibling}" && "${sibling}" != "${test_file}" ]]; then
        targets+=("${sibling}")
      fi
    done
  fi

  local deps_dir
  deps_dir=$(mktemp -d)

  echo "Installing pytest${*:+ + $*} into ${deps_dir}..." >&2
  if ! python3 -m pip install --quiet --target "${deps_dir}" pytest "$@"; then
    echo "  ✗ test-dep install failed" >&2
    rm -rf "${deps_dir}"
    return 1
  fi

  # The lambdas dir is ALWAYS on the path (alpha-engine-config-I7582), not only
  # when a caller remembers to set HANDLER_TEST_PYTHONPATH. It holds the modules
  # siblings import by bare name — flow_doctor_telegram.py, and now
  # eod_artifact_verification.py — and it was hand-set in exactly one deploy.sh
  # and in NO ci.yml step, so a lambda importing a shared sibling passed locally
  # and ModuleNotFound'd in CI. Same shape as the HANDLER_TEST_TARGETS gap this
  # helper already absorbed: a mechanism every caller needs, owned here instead
  # of re-declared per caller. An explicit HANDLER_TEST_PYTHONPATH still appends
  # after it, so callers that set one keep whatever extra roots they wanted.
  local lambdas_dir
  lambdas_dir="$(cd "${script_dir}/.." && pwd)"
  local pypath="${deps_dir}:${lambdas_dir}${HANDLER_TEST_PYTHONPATH:+:${HANDLER_TEST_PYTHONPATH}}"

  local rc=0 one file_rc
  # ${targets[@]+...} guard: bash 3.2 (the macOS system bash every deploy.sh
  # runs under) errors on "${arr[@]}" for an EMPTY array under `set -u`.
  for one in "${test_file}" ${targets[@]+"${targets[@]}"}; do
    echo "Running ${one##*/}..." >&2
    file_rc=0
    PYTHONPATH="${pypath}" \
    AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
      python3 -m pytest "${one}" -q || file_rc=$?
    if [[ "${file_rc}" -ne 0 ]]; then
      rc="${file_rc}"
    fi
  done

  rm -rf "${deps_dir}"
  return "${rc}"
}
