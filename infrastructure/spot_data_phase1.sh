#!/usr/bin/env bash
# infrastructure/spot_data_phase1.sh — DataPhase1 SF state runner.
# Full price-cache refresh (weekly_collector.py --phase 1 + prune) on a
# dedicated spot EC2.
#
# Sources infrastructure/_spot_common.sh for shared spot infrastructure.
#
# Supports:
#   --preflight-only  — boot + preflight, exit 0 (NO fetch/write)
#   --smoke-only      — boot + dry-run, exit 0
#   --instance-type   — override instance type
#
# Usage:
#   ./infrastructure/spot_data_phase1.sh                         # full run
#   ./infrastructure/spot_data_phase1.sh --preflight-only
#   ./infrastructure/spot_data_phase1.sh --smoke-only
#   ./infrastructure/spot_data_phase1.sh --instance-type c5.xlarge

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_spot_common.sh"

# ── Stage-specific defaults ──────────────────────────────────────────────────
_SPOT_NAME="${_SPOT_NAME:-data-phase1}"
_SSM_SLUG="${_SSM_SLUG:-spot-data-phase1}"
_PROCESS_NAME="${_PROCESS_NAME:-data-phase1}"
# alpha-engine-config-I10194 §3 — DECLARED, not defaulted (see the
# `_STAGE_WINDOW_TRACKS_CYCLE` block in _spot_common.sh for the full
# rationale and the measured 2026-09-04 evidence). `weekly_collector.py
# --phase 1` runs eight phases that auto-skip when this cycle's output is
# already on S3 (`constituents`, `macro`, `metron_valuation_medians`,
# `features`, `historical_constituents`, `short_interest`,
# `universe_classification`, `fundamentals` — every `_phase_collect` in
# `_run_phase1` that does not pass `supports_auto_skip=False`), so on a rerun
# this stage legitimately writes nothing new and its own valid output
# predates this execution. The bare assignment is deliberate: `${VAR:-1}`
# here would be the I6922 swallow.
_STAGE_WINDOW_TRACKS_CYCLE=1
# alpha-engine-config-I7176 / -I9201 (2026-08-28): 5400 -> 6600, DERIVED from
# the two most recent cold runs rather than estimated. Measured span from the
# DataPhase1 StateEntered event to the last CheckDataPhase1Status entry, one
# dispatch attempt each, no reissues:
#
#   2026-08-01  2388s (44%)   2026-08-15  4926s (91%)
#   2026-08-08  2418s (45%)   2026-08-22  5018s (93%)
#
# The step is NOT growth. Until 2026-08-13 a daily "exercise cadence" ran the
# whole weekly pipeline every weekday; its Friday pass wrote the same
# data/{date}/.phases/*.json markers PhaseRegistry auto-skips on, so Saturday
# skipped 7 of 10 phases (`PHASE_SKIP ... reason=auto_skip_marker_ok` in
# _ssm_logs/data-weekly/2026-08-08/). Retiring that cadence (4159239d, Brian
# ruling 2026-08-13) removed the incidental pre-warm, and 4926/5018s is
# DataPhase1's true cold cost — `fundamentals` (~1020s), `universe_classification`
# (~508s) and `short_interest` (~505s) are sequential per-ticker API loops over
# 903 tickers and account for ~1900s of it.
#
# 6600 = 5018 x 1.31, inside the 10800s max_budget_seconds already declared in
# infrastructure/sf_budgets.py. This buys margin against a known, stable cost;
# it is NOT a threshold widened until a stale value passes. The real fix is to
# batch those three loops against their providers' rate limits —
# alpha-engine-config-I9201 — and this ceiling comes back down when it lands.
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-6600}"

# ── Parse flags ──────────────────────────────────────────────────────────────
MODE="run"
PREFLIGHT_ONLY=0
ORIG_ARGS=("$@")

while [[ $# -gt 0 ]]; do
  case "$1" in
    --preflight-only) PREFLIGHT_ONLY=1; shift ;;
    --smoke-only) MODE="smoke-only"; shift ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    *) echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
  esac
done

# Default-init: SF-driven invocations pass zero flags, so the loop body
# above never runs and these would otherwise be unbound under `set -u`
# (config#2949, PR936/PR937 bug class). _spot_common.sh already sets safe
# defaults before this loop runs, but the guard in
# tests/test_shell_arg_parse_default_init.py is branch-path-sensitive per
# file and does not see across the `source` — restate the defaults here,
# matching the ID_ARTIFACT_KEY pattern in spot_data_weekly.sh.
BRANCH="${BRANCH:-main}"
INSTANCE_TYPE="${INSTANCE_TYPE:-}"

[ -n "$INSTANCE_TYPE" ] && INSTANCE_TYPES="$INSTANCE_TYPE"

_CONFIG_SRC="/home/ec2-user/alpha-engine-config/data/config.yaml"
[ ! -f "$_CONFIG_SRC" ] && _CONFIG_SRC="$HOME/Development/alpha-engine-config/data/config.yaml"
[ ! -f "$_CONFIG_SRC" ] && _CONFIG_SRC="$(cd "$SCRIPT_DIR/../.." && pwd)/config/config.yaml"

read -r -d '' _ENV_SOURCE <<'ENV_EOF' || true
export HOME=/home/ec2-user
export XDG_CACHE_HOME=/tmp
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
# Router addressing (alpha-engine-config-I7409) - undeclared before this, krepis defaulted to
# exec_context=laptop and RouterUnresolvable'd every flow-doctor diagnosis call on this box.
export KREPIS_EXEC_CONTEXT=ec2
export KREPIS_LITELLM_PROXY_URL=https://router.nousergon.ai:8443
export KREPIS_ROUTER_CREDENTIAL_SECRET=ROUTER_CONSUMER_DATA
export KREPIS_APPCONFIG_APPLICATION=alpha-engine
export KREPIS_APPCONFIG_CONFIG_PROFILE=llm-model-registry
export KREPIS_APPCONFIG_ENVIRONMENT=production
if ! command -v python3.12 >/dev/null 2>&1; then
    echo "ERROR: python3.12 not found on this spot — bootstrap_spot() installs and asserts it. Refusing to fall back to the AMI python3: requirements.txt is resolved against 3.12 and the wheels differ (alpha-engine-config-I7372)." >&2
    exit 1
fi
PYTHON_BIN=python3.12
export PYTHON_BIN
ENV_EOF

echo "═══════════════════════════════════════════════════════════════"
echo "  DataPhase1 — $(date +%Y-%m-%d)"
echo "═══════════════════════════════════════════════════════════════"
echo "  Instance types: $INSTANCE_TYPES | Branch: $BRANCH"
echo "  Preflight-only: $PREFLIGHT_ONLY | Attempt: $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS"
echo ""

# ── Launch + wait + config + SSM ─────────────────────────────────────────────
spot_launch
trap on_exit EXIT

aws ec2 wait instance-running --instance-ids "$_INSTANCE_ID" --region "$AWS_REGION"
stage_config "$_CONFIG_SRC" "config.yaml"
wait_ssm_agent
bootstrap_spot
install_deps
install_gitleaks_dlp

# ── Smoke-only ───────────────────────────────────────────────────────────────
if [ "$MODE" = "smoke-only" ]; then
  print_banner "SMOKE TEST"
  run_ssm "smoke" "$(cat <<SMOKE
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data
echo "==> Smoke: import weekly_collector"
\$PYTHON_BIN -c "import weekly_collector; print('import OK')"
echo "==> Smoke: import builders.prune_delisted_tickers"
\$PYTHON_BIN -c "from builders import prune_delisted_tickers; print('import OK')"
echo "==> Smoke: weekly_collector.py --phase 1 --dry-run"
\$PYTHON_BIN weekly_collector.py --phase 1 --dry-run 2>&1
SMOKE
)" 1800
  echo "==> Smoke complete."
  exit 0
fi

# ── Preflight-only (Friday shell-run dry path) ───────────────────────────────
if [ "$PREFLIGHT_ONLY" = "1" ]; then
  print_banner "PREFLIGHT-ONLY (NO fetch/write)"
  run_ssm "preflight" "$(cat <<PREFLIGHT
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data
echo "==> weekly_collector --phase 1 --preflight-only"
if ! \$PYTHON_BIN weekly_collector.py --phase 1 --preflight-only 2>&1; then
    echo "ERROR: phase1 preflight failed" >&2
    exit 1
fi
echo "Phase1 preflight OK at \$(date) — NO fetch, NO write."
PREFLIGHT
)" 900
  echo "==> Preflight complete."
  exit 0
fi

# ── DataPhase1 run (phase1 + prune) ──────────────────────────────────────────
print_banner "DATAPHASE1 (price refresh + prune)"
run_ssm "phase1" "$(cat <<WORKLOAD
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data

echo "==> Starting weekly_collector.py --phase 1 at \$(date)"
if ! \$PYTHON_BIN weekly_collector.py --phase 1 2>&1; then
    echo "ERROR: DataPhase1 failed" >&2
    exit 1
fi
echo "DataPhase1 complete at \$(date)"

echo "==> Starting builders.prune_delisted_tickers at \$(date)"
if ! \$PYTHON_BIN -m builders.prune_delisted_tickers --apply 2>&1; then
    echo "ERROR: prune_delisted_tickers failed" >&2
    exit 1
fi
echo "UniversePrune complete at \$(date)"
WORKLOAD
)" "${MAX_RUNTIME_SECONDS}"

emit_heartbeat

# ── Per-stage output assertion (config-I7214) ────────────────────────────────
# See spot_morning_enrich.sh for the full rationale. OBSERVE MODE — the CLI
# exits 0 for every verdict, and `|| echo ... >&2` rather than `|| true` keeps
# an unreachable assertion distinguishable from a covered stage.
# --run-date is explicit ($EXECUTION_RUN_DATE, not $RUN_DATE): this launcher
# never receives RUN_DATE at all, and even where RUN_DATE does exist elsewhere
# in the fleet it is reassigned to the trading day by crucible-backtester's
# infrastructure/_spot_common.sh — a carrier other code rewrites is exactly the
# defect alpha-engine-config-I8155 fixes. EXECUTION_RUN_DATE is exported by
# step_function.json from $.run_date and is never normalized by anything.
#
# --window-start is RESOLVED, not taken raw: this stage auto-skips phases an
# earlier attempt of the same cycle already completed, so on a rerun the raw
# "$_STAGE_WINDOW_START" starts AFTER this cycle's own output and reads it as
# a previous week's leftover (alpha-engine-config-I10194 §3). The resolver
# never raises and always prints a window.
_DATA_PHASE1_WINDOW="$(resolve_stage_window_start DataPhase1 "${EXECUTION_RUN_DATE:-}")"
"$LIB_PYTHON" -m krepis.stage_coverage assert --stage DataPhase1 --window-start "$_DATA_PHASE1_WINDOW" --run-date "$EXECUTION_RUN_DATE" || echo "WARNING: stage-coverage assertion did not run for DataPhase1 (rc=$?) — observe mode, stage NOT failed (config-I7214)" >&2

echo "==> DataPhase1 complete."
