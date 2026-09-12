#!/usr/bin/env bash
# infrastructure/spot_rag_ingestion.sh — RAGIngestion SF state runner.
# Multi-source news sweep (SEC filings, 8-Ks, earnings, thesis, change
# detection) on a dedicated spot EC2.
#
# Sources infrastructure/_spot_common.sh for shared spot infrastructure.
#
# RAG ingestion has higher resource requirements (6h timeout, 16 GiB RAM
# recommended) vs DataPhase1 (1.5h, 8 GiB). This script overrides
# MAX_RUNTIME_SECONDS to accommodate the ~3.15h Polygon news sweep.
# Default instance types favor larger instances than _spot_common.sh's
# defaults.
#
# Supports:
#   --preflight-only  — boot + RAG preflight (secret fetch + dry imports),
#                       exit 0 (NO ingest/write)
#   --rag-smoke-only  — SSM fetch + preflight + submodule imports + dry-run
#   --instance-type   — override instance type
#
# Usage:
#   ./infrastructure/spot_rag_ingestion.sh                           # full ingestion
#   ./infrastructure/spot_rag_ingestion.sh --preflight-only
#   ./infrastructure/spot_rag_ingestion.sh --rag-smoke-only
#   ./infrastructure/spot_rag_ingestion.sh --instance-type r5.large

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_spot_common.sh"

# ── Stage-specific defaults (RAG needs more resources) ───────────────────────
_SPOT_NAME="${_SPOT_NAME:-rag-ingestion}"
_SSM_SLUG="${_SSM_SLUG:-spot-rag-ingestion}"
_PROCESS_NAME="${_PROCESS_NAME:-rag-ingestion}"
# config#2938: RAGIngestion Step 5/9 Polygon news sweep needs ~3.15h;
# 6h total budget with the box's own shutdown watchdog as backstop.
# RAG needs 6h budget (config#2938); override the shared 5400s default
MAX_RUNTIME_SECONDS=21900
# RAG benefits from larger instances than the shared c5.large default
INSTANCE_TYPES="r5.large,m5.large,c5.2xlarge,c5.xlarge"

# ── Parse flags ──────────────────────────────────────────────────────────────
MODE="run"
PREFLIGHT_ONLY=0
ORIG_ARGS=("$@")

while [[ $# -gt 0 ]]; do
  case "$1" in
    --preflight-only) PREFLIGHT_ONLY=1; shift ;;
    --rag-smoke-only) MODE="rag-smoke-only"; shift ;;
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
if ! command -v python3.12 >/dev/null 2>&1; then
    echo "ERROR: python3.12 not found on this spot — bootstrap_spot() installs and asserts it. Refusing to fall back to the AMI python3: requirements.txt is resolved against 3.12 and the wheels differ (alpha-engine-config-I7372)." >&2
    exit 1
fi
PYTHON_BIN=python3.12
export PYTHON_BIN
ENV_EOF

echo "═══════════════════════════════════════════════════════════════"
echo "  RAGIngestion — $(date +%Y-%m-%d)"
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

# ── RAG smoke-only (dry-run submodules) ──────────────────────────────────────
if [ "$MODE" = "rag-smoke-only" ]; then
  print_banner "RAG SMOKE TEST"
  run_ssm "rag-smoke" "$(cat <<RAG_SMOKE
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data

echo "==> RAG smoke: fetching secrets from SSM"
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done
echo "RAG secrets fetched"

echo "==> RAG smoke: preflight env-var check"
\$PYTHON_BIN -m rag.preflight

echo "==> RAG smoke: import all 5 RAG submodules"
\$PYTHON_BIN -c "
import rag.pipelines.ingest_sec_filings
import rag.pipelines.ingest_8k_filings
import rag.pipelines.ingest_earnings_finnhub
import rag.pipelines.ingest_theses
import rag.pipelines.filing_change_detection
print('all 5 rag submodules imported OK')
"

echo "==> RAG smoke: run_weekly_ingestion.sh --dry-run"
bash rag/pipelines/run_weekly_ingestion.sh --dry-run 2>&1
RAG_SMOKE
)" 1800
  echo "==> RAG smoke complete."
  exit 0
fi

# ── Preflight-only (Friday shell-run dry path) ───────────────────────────────
if [ "$PREFLIGHT_ONLY" = "1" ]; then
  print_banner "RAG PREFLIGHT-ONLY (NO ingest/write)"
  run_ssm "rag-preflight" "$(cat <<RAG_PRE
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data

echo "==> Fetching RAG secrets from SSM"
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done

echo "==> Starting run_weekly_ingestion.sh --preflight-only at \$(date)"
if ! bash rag/pipelines/run_weekly_ingestion.sh --preflight-only 2>&1; then
    echo "ERROR: RAG preflight failed" >&2
    exit 1
fi
echo "RAG preflight OK at \$(date) — NO fetch, NO write."
RAG_PRE
)" 900
  echo "==> RAG preflight complete."
  exit 0
fi

# ── RAG ingestion run ────────────────────────────────────────────────────────
print_banner "RAG INGESTION RUN"

# RAG workload timeout is the full execution budget (not MAX_RUNTIME_SECONDS
# which includes boot + overhead). Use the configured workload timeout.
_RAG_WORKLOAD_TIMEOUT="${RAG_WORKLOAD_TIMEOUT:-21600}"
run_ssm "rag-ingestion" "$(cat <<WORKLOAD
set -eo pipefail
${_ENV_SOURCE}
cd /home/ec2-user/data

# Fetch RAG secrets from SSM
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done

echo "==> Starting run_weekly_ingestion.sh at \$(date)"
if ! bash rag/pipelines/run_weekly_ingestion.sh 2>&1; then
    echo "ERROR: RAG ingestion failed" >&2
    exit 1
fi
echo "RAG ingestion complete at \$(date)"
WORKLOAD
)" "${_RAG_WORKLOAD_TIMEOUT}"

emit_heartbeat

# ── Per-stage output assertion (config-I7214) ────────────────────────────────
# See spot_morning_enrich.sh for the full rationale. RAGIngestion's PRIMARY
# product is rows in the pgvector corpus, which is not an S3 key; the registry
# declares its two registerable artifacts (rag_corpus_freshness,
# rag_ingestion_progress) and this asserts exactly those. OBSERVE MODE.
#
# --run-date is explicit ($EXECUTION_RUN_DATE, not $RUN_DATE): this launcher
# never receives RUN_DATE at all, and even where RUN_DATE does exist elsewhere
# in the fleet it is reassigned to the trading day by crucible-backtester's
# infrastructure/_spot_common.sh — a carrier other code rewrites is exactly the
# defect alpha-engine-config-I8155 fixes. EXECUTION_RUN_DATE is exported by
# step_function.json from $.run_date and is never normalized by anything.
"$LIB_PYTHON" -m krepis.stage_coverage assert --stage RAGIngestion --window-start "$_STAGE_WINDOW_START" --run-date "$EXECUTION_RUN_DATE" || echo "WARNING: stage-coverage assertion did not run for RAGIngestion (rc=$?) — observe mode, stage NOT failed (config-I7214)" >&2

echo "==> RAG ingestion complete."
