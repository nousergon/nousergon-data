#!/usr/bin/env bash
# infrastructure/spot_data_weekly.sh — Run weekly data workloads on a spot EC2.
#
# Bundles DataPhase1 + RAGIngestion on a single spot: launches c5.large,
# clones alpha-engine-data, runs `python weekly_collector.py --phase 1`
# followed by `bash rag/pipelines/run_weekly_ingestion.sh`, emits a
# heartbeat on success, and self-terminates.
#
# Origin: moved off ae-dashboard (t3.micro, 1 GB RAM) after the 2026-04-16
# OOM incident (features/compute.py in the DAILY code path exhausted micro
# memory). Saturday's Phase 1 uses a different code path and hasn't OOM'd
# historically, but running heavy data-refresh workloads on 1 GB RAM is
# fragile-by-design. This spot pattern mirrors the Backtester +
# PredictorTraining launchers so all heavy weekly compute lives on
# fresh, self-terminating instances instead of the always-on micro.
#
# Bundling rationale: Phase 1 and RAG ingestion are sequential SF steps
# that share the same repo + venv. One spot per bundle saves ~7 min of
# bootstrap overhead and one spot request. Trade-off: any failure fails
# both — acceptable since partial Saturday failures typically require a
# full-pipeline rerun anyway.
#
# Usage:
#   ./infrastructure/spot_data_weekly.sh                   # phase1 + rag
#   ./infrastructure/spot_data_weekly.sh --smoke-only      # quick validation, then terminate
#   ./infrastructure/spot_data_weekly.sh --preflight-only  # boot + DataPhase1/MorningEnrich preflight, exit 0 (NO fetch/write)
#   ./infrastructure/spot_data_weekly.sh --rag-only --preflight-only  # boot + RAG-path preflight, exit 0 (NO fetch/write)
#   ./infrastructure/spot_data_weekly.sh --instance-type c5.xlarge   # override size
#   ./infrastructure/spot_data_weekly.sh --branch my-branch          # override branch
#
# --preflight-only (Friday shell-run dry path, ROADMAP "Friday shell-run —
# per-module dry-path activation" owed-item #1): boots the spot for real,
# installs deps, runs the EXISTING preflight (env/secret resolution via
# get_secret, AWS/SSM reachability, ArcticDB connect + libraries-present
# read, S3 HEAD), then exits 0 BEFORE any collector fetch or any
# S3/ArcticDB/config write. Hard invariant: ZERO external API data fetches
# (the preflight's polygon/FRED *reachability probes* are sub-second
# auth/HEAD-class calls that fetch no collector data) and ZERO
# S3/ArcticDB/config/email/SNS mutations under this flag. The point is to
# catch bootstrap-class breakage (lib-pin drift, sys.path collision, stale
# ArcticDB symbol, SSM timeout, Dockerfile/image gap) ~12h before the real
# Saturday run. Composes with --rag-only: `--rag-only --preflight-only`
# runs ONLY the RAG-path preflight (rag.preflight: env-vars + S3 HEAD);
# `--preflight-only` alone runs ONLY the DataPhase1/MorningEnrich preflight.
#
# Prerequisites on the launching host (ae-dashboard when invoked by the
# Saturday Step Function):
#   - AWS CLI with perms to RunInstances / TerminateInstances / DescribeInstances
#   - SSH key at ~/.ssh/alpha-engine-key.pem
#   - alpha-engine-data checked out at the script's parent dir
#
# Secrets resolve from SSM at Python startup via
# alpha_engine_lib.secrets.get_secret(); the spot's IAM profile
# (alpha-engine-executor-profile) grants ssm:GetParameter on /alpha-engine/*.
# No .env is sourced anywhere in this script post the 2026-05-14 .env-deprecation arc.

set -euo pipefail

# SSM RunCommand does not set HOME; default it for the SSH key lookup below.
export HOME="${HOME:-/home/ec2-user}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Spot configuration ──────────────────────────────────────────────────────
# Values mirror alpha-engine-backtester/infrastructure/spot_backtest.sh so
# new IAM/security-group/subnet resources aren't introduced. If any of these
# change in the backtester launcher, this file should change in lockstep.
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-alpha-engine-research}"
BRANCH="${BRANCH:-main}"
# Capacity-resilient instance-type fallback set (2026-05-22 incident:
# Evaluator's launch hit InsufficientInstanceCapacity for c5.large in
# us-east-1f). All 2 vCPU / 4-8 GB RAM — equivalent for our workloads.
# Order = preference; the lib CLI tries each in turn until one launches.
INSTANCE_TYPES="${INSTANCE_TYPES:-c5.large,m5.large,c6i.large,c5a.large}"
# Backward-compat: --instance-type X collapses the list to a single type.
INSTANCE_TYPE=""
AMI_ID="ami-0c421724a94bba6d6"      # Amazon Linux 2023 x86_64
# Spot-side watchdog budget: DataPhase1 historically runs 25-35 min;
# RAG ingestion adds another 20-45 min. 90 min with headroom covers both
# plus pip install + preflight. If the workload legitimately needs longer,
# bump this — don't silently rely on the orphan reaper.
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-5400}"
KEY_NAME="alpha-engine-key"
KEY_FILE="$HOME/.ssh/alpha-engine-key.pem"
SECURITY_GROUP="sg-03cd3c4bd91e610b0"
# All 6 default-VPC subnets across us-east-1{a,b,c,d,e,f}. The lib CLI
# (alpha_engine_lib.ec2_spot) rotates across this list on capacity
# error. Verified 2026-05-22 — all 6 are public-IP-on-launch, all in
# vpc-566f002e, all with ~4091 free IPs. If the VPC topology changes,
# update via `aws ec2 describe-subnets --filters Name=vpc-id,Values=vpc-566f002e`.
SUBNETS="${SUBNETS:-subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,subnet-c670118d,subnet-7cff7c43,subnet-e07166ec}"
IAM_PROFILE="alpha-engine-executor-profile"
# Lib CLI path: ae-dashboard is the SSM target instance ($MicroInstanceId)
# for all 8 Saturday-SF spot states; the dispatcher's .venv has
# alpha-engine-lib installed (see deploy-on-merge.sh in the dashboard
# repo). Bare `python3` resolves to system python which does NOT carry
# the lib — use the full venv path.
LIB_PYTHON="${LIB_PYTHON:-/home/ec2-user/alpha-engine-dashboard/.venv/bin/python}"

# ── Parse flags ──────────────────────────────────────────────────────────────
# RUN_MODE values:
#   full                — phase1 + rag (legacy bundled, manual/adhoc)
#   smoke-only          — imports + --phase 1 --dry-run, then terminate
#   rag-smoke-only      — RAG-via-SSM dry-run, then terminate
#   rag-only            — only RAG ingestion (DataPhase1 ran earlier)
#   data-only           — morning-enrich + phase1 + prune (legacy bundled,
#                          manual/adhoc backward-compat — RAG separate)
#   morning-enrich-only — ONLY weekly_collector.py --morning-enrich, then
#                          terminate (Saturday SF MorningEnrich state)
#   phase1-only         — ONLY weekly_collector.py --phase 1 + prune, then
#                          terminate (Saturday SF DataPhase1 state)
#
# The preflight-task-split (2026-05-16, plan
# alpha-engine-docs/private/preflight-task-split-260516.md) introduced
# morning-enrich-only / phase1-only so the Saturday SF runs each
# preflight-bearing action as its own SF task: a phase1 failure no longer
# re-pays the ~28-min morning-enrich. data-only stays for manual reruns.
RUN_MODE="full"
# PREFLIGHT_ONLY is a MODIFIER, orthogonal to RUN_MODE — it composes with
# the data path (default / --data-only / --phase1-only / --morning-enrich-only)
# AND with --rag-only. When set, every workload invocation is replaced by
# its existing preflight + an early `exit 0`; no collector fetch or
# S3/ArcticDB/config write code path is reachable. The preflight-task-split
# (2026-05-16) modes still select WHICH preflight (phase1 vs morning_enrich
# vs RAG) runs; --preflight-only only swaps "preflight + work" for
# "preflight + exit 0".
PREFLIGHT_ONLY=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke-only) RUN_MODE="smoke-only"; shift ;;
        --rag-smoke-only) RUN_MODE="rag-smoke-only"; shift ;;
        --rag-only) RUN_MODE="rag-only"; shift ;;
        --data-only) RUN_MODE="data-only"; shift ;;
        --morning-enrich-only) RUN_MODE="morning-enrich-only"; shift ;;
        --phase1-only) RUN_MODE="phase1-only"; shift ;;
        --preflight-only) PREFLIGHT_ONLY=1; shift ;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;  # legacy: collapses INSTANCE_TYPES to single value
        --branch) BRANCH="$2"; shift 2 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════"
echo "  Weekly Data Spot Run (Phase1 + RAG) — $(date +%Y-%m-%d)"
echo "═══════════════════════════════════════════════════════════════"
# --instance-type collapses the rotation list to a single value (legacy
# behavior). Otherwise the lib CLI rotates across INSTANCE_TYPES on
# capacity error.
if [ -n "$INSTANCE_TYPE" ]; then
    INSTANCE_TYPES="$INSTANCE_TYPE"
fi
echo "  Instance types: $INSTANCE_TYPES"
echo "  Subnets       : $SUBNETS"
echo "  AMI           : $AMI_ID"
echo "  Region        : $AWS_REGION"
echo "  Branch        : $BRANCH"
echo "  Run mode      : $RUN_MODE"
echo "  Preflight-only: $PREFLIGHT_ONLY  (1 = boot + preflight + exit 0, NO fetch/write)"
echo "  S3 bucket     : $S3_BUCKET"
echo ""

# ── Preflight ───────────────────────────────────────────────────────────────
if [ ! -f "$KEY_FILE" ]; then
    echo "ERROR: SSH key not found at $KEY_FILE"
    exit 1
fi
# Note: alpha-engine-lib was flipped public 2026-05-03; spot installs it
# directly from git+https with no auth required. Earlier versions of this
# script fetched a PAT from /alpha-engine/lib-token via SSM — no longer needed.

# ── Launch spot ──────────────────────────────────────────────────────────────
# Capacity-resilient launch via alpha_engine_lib.ec2_spot (lib v0.26.0+).
# The CLI iterates (instance_type × subnet) on InsufficientInstanceCapacity /
# InsufficientHostCapacity / Unsupported / InvalidAvailabilityZone /
# SpotMaxPriceTooLow, returning the InstanceId of the first successful
# launch. Non-capacity errors (auth, AMI not found, quota) raise
# immediately. Replaces the 2026-05-22 broken-by-design hardcoded
# single-subnet + single-instance-type pattern that failed Evaluator's
# launch when us-east-1f ran out of c5.large.
echo "==> Requesting spot instance (lib CLI rotation: types=[$INSTANCE_TYPES], subnets=[$SUBNETS])..."

INSTANCE_ID=$("$LIB_PYTHON" -m alpha_engine_lib.ec2_spot launch \
    --types "$INSTANCE_TYPES" \
    --subnets "$SUBNETS" \
    --image-id "$AMI_ID" \
    --key-name "$KEY_NAME" \
    --security-group "$SECURITY_GROUP" \
    --iam-profile "$IAM_PROFILE" \
    --name "alpha-engine-data-weekly-$(date +%Y%m%d)" \
    --region "$AWS_REGION")
ec2_spot_rc=$?
if [ "$ec2_spot_rc" -ne 0 ] || [ -z "$INSTANCE_ID" ]; then
    if [ "$ec2_spot_rc" -eq 64 ]; then
        echo "ERROR: capacity exhausted across all instance_type × subnet combinations. Wait + retry, or expand the lists." >&2
    fi
    exit "${ec2_spot_rc:-1}"
fi

echo "  Instance ID: $INSTANCE_ID"

# Always terminate, even on error.
cleanup() {
    echo ""
    echo "==> Terminating spot instance $INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" --output text > /dev/null 2>&1 || true
    echo "  Instance terminated."
}
trap cleanup EXIT

echo "==> Waiting for instance to enter running state..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"

PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text \
    --region "$AWS_REGION")

if [ "$PUBLIC_IP" = "None" ] || [ -z "$PUBLIC_IP" ]; then
    echo "ERROR: Instance has no public IP. Check subnet/VPC configuration."
    exit 1
fi

echo "  Public IP: $PUBLIC_IP"

# ── Wait for SSH ─────────────────────────────────────────────────────────────
echo "==> Waiting for SSH to become available..."
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"

for i in $(seq 1 30); do
    if ssh $SSH_OPTS -i "$KEY_FILE" ec2-user@"$PUBLIC_IP" "echo ok" 2>/dev/null; then
        echo "  SSH ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: SSH not available after 150s"
        exit 1
    fi
    sleep 5
done

run_remote() {
    ssh $SSH_OPTS -i "$KEY_FILE" ec2-user@"$PUBLIC_IP" "$@"
}

# ── Spot-side watchdog ──────────────────────────────────────────────────────
# Dispatcher-side `trap cleanup EXIT` (line ~140) only fires when THIS bash
# script exits cleanly. If the dispatcher SSM command is cancelled, the
# dispatcher EC2 is stopped mid-run, or the shell gets SIGKILLed, the trap
# never runs and the spot orphans until manually terminated — hit 3 times
# in April 2026 (~$20 orphan each). This installs a transient systemd
# timer on the spot that fires shutdown -h now after MAX_RUNTIME_SECONDS
# regardless of dispatcher state. AL2023's InstanceInitiatedShutdown for
# spots defaults to terminate, so shutdown = instance goes away.
echo "==> Installing spot-side watchdog (${MAX_RUNTIME_SECONDS}s = $((MAX_RUNTIME_SECONDS / 60)) min)..."
run_remote "sudo systemd-run --on-active=${MAX_RUNTIME_SECONDS} --unit=alpha-engine-watchdog --description='alpha-engine spot hard-timeout' /sbin/shutdown -h now"

# ── Bootstrap spot: python + git ─────────────────────────────────────────────
echo "==> Bootstrapping spot environment..."
run_remote bash -s <<'BOOTSTRAP'
set -euo pipefail
sudo dnf install -y -q python3.12 python3.12-pip python3.12-devel git gcc 2>/dev/null || \
    sudo dnf install -y -q python3 python3-pip python3-devel git gcc
if command -v python3.12 &>/dev/null; then
    echo "Using: $(python3.12 --version)"
else
    echo "Using: $(python3 --version)"
fi
mkdir -p ~/.ssh
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
BOOTSTRAP

# ── Clone alpha-engine-data on spot ──────────────────────────────────────────
echo "==> Cloning alpha-engine-data (branch: $BRANCH)..."
# HTTPS clone with PAT — matches the lib-install pattern below.
run_remote "git clone --depth 1 --branch $BRANCH https://github.com/cipher813/alpha-engine-data.git /home/ec2-user/alpha-engine-data"

# ── Upload alpha-engine-config/data/config.yaml ─────────────────────────────
# weekly_collector.py's load_config() searches /home/ec2-user/alpha-engine-config/data/config.yaml
# first. Private config repo — SCP from the dispatcher's clone (pulled daily by
# ae-dashboard's boot-pull) rather than cloning it on the spot (which would
# require broader git-auth setup; the spot only needs read access to public repos).
CONFIG_SRC="/home/ec2-user/alpha-engine-config/data/config.yaml"
if [ ! -f "$CONFIG_SRC" ]; then
    CONFIG_SRC="$HOME/Development/alpha-engine-config/data/config.yaml"
fi
if [ ! -f "$CONFIG_SRC" ]; then
    echo "ERROR: dispatcher config not found at /home/ec2-user/alpha-engine-config/data/config.yaml or $HOME/Development/alpha-engine-config/data/config.yaml — is alpha-engine-config cloned + pulled?"
    exit 1
fi
echo "==> Uploading alpha-engine-config/data/config.yaml to spot..."
run_remote "mkdir -p /home/ec2-user/alpha-engine-config/data"
scp $SSH_OPTS -i "$KEY_FILE" \
    "$CONFIG_SRC" \
    ec2-user@"$PUBLIC_IP":/home/ec2-user/alpha-engine-config/data/config.yaml

# ── Install python deps ──────────────────────────────────────────────────────
# The spot pulls its own alpha-engine-lib PAT from SSM (same pattern as
# ae-trading's boot-pull.sh). Dispatcher never handles the secret. The
# spot's IAM profile (alpha-engine-executor-profile) grants ssm:GetParameter
# on /alpha-engine/*. Token is scoped to a local shell var, never exported
# or logged.
echo "==> Installing Python dependencies..."
run_remote bash -s <<'DEPS'
set -euo pipefail
cd /home/ec2-user/alpha-engine-data

if command -v python3.12 &>/dev/null; then
    PIP="python3.12 -m pip"
else
    PIP="python3 -m pip"
fi

$PIP install --upgrade pip -q
$PIP install -q -r requirements.txt

# numpy<2 pin to match other spot workloads (pyarrow compiled against 1.x).
$PIP install -q 'numpy<2'

echo "Dependencies installed."
DEPS

REMOTE_PYTHON=$(run_remote "command -v python3.12 || command -v python3")
# Export PYTHON_BIN so downstream bash scripts (e.g.
# rag/pipelines/run_weekly_ingestion.sh) inherit the interpreter we
# bootstrapped. AL2023 spots install python3.12 but have no bare `python`
# symlink — the RAG script's `python -m ...` fails without this. Origin:
# 2026-04-17 Saturday Step Function failure in RAG step-0 preflight.
#
# AWS_REGION/AWS_DEFAULT_REGION: the spot shell no longer sources a .env
# (PR 9f / #241 removed `.env` sourcing in favor of runtime get_secret()
# SSM lookups), but AWS_REGION is a plain env var — not a secret — that
# alpha_engine_lib.preflight.check_env_vars hard-requires, and boto3 needs
# a default region with no .env present. Re-export it explicitly from the
# dispatcher-side $AWS_REGION (set above with us-east-1 fallback). Origin:
# 2026-05-16 Saturday SF DataPhase1 failure — weekly_collector --morning-enrich
# aborted at preflight with "required env vars missing: ['AWS_REGION']".
ENV_SOURCE="export XDG_CACHE_HOME=/tmp; export PYTHON_BIN=$REMOTE_PYTHON; export AWS_REGION=$AWS_REGION; export AWS_DEFAULT_REGION=$AWS_REGION;"

# ── Smoke-only: imports + --phase 1 --dry-run ────────────────────────────────
if [ "$RUN_MODE" = "smoke-only" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  SMOKE TEST"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<SMOKE
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "==> Smoke: python import weekly_collector"
$REMOTE_PYTHON -c "import weekly_collector; print('import OK')"

echo ""
echo "==> Smoke: python import builders.prune_delisted_tickers"
$REMOTE_PYTHON -c "from builders import prune_delisted_tickers; print('import OK')"

echo ""
echo "==> Smoke: weekly_collector.py --phase 1 --dry-run"
# Show full output (was tail -30 — truncated error tracebacks from early
# collectors so their failure mode was invisible during debugging).
$REMOTE_PYTHON weekly_collector.py --phase 1 --dry-run 2>&1
SMOKE

    echo "==> Smoke complete — instance will be terminated."
    exit 0
fi

# ── RAG-smoke-only: SSM fetch + preflight + submodule imports + dry-run ──────
# Exercises the RAG-via-SSM path end-to-end on a real AL2023 spot without
# hitting production external state (no SEC fetches, no Voyage embeddings,
# no Postgres writes — everything gated by --dry-run in the submodules).
# Validates:
#   1. IAM: spot can fetch the 4 RAG secrets from SSM
#   2. PYTHON_BIN resolution under python3.12 on AL2023
#   3. All 5 env vars pass rag/preflight.py::RAGPreflight.check_env_vars
#   4. All 5 RAG submodules import under python3.12
#   5. run_weekly_ingestion.sh --dry-run executes each pipeline's CLI path
# Does NOT validate: Postgres reachability (dry-run doesn't connect),
# external API quotas (dry-run doesn't hit them), runtime bugs that only
# trigger on production-shape data.
if [ "$RUN_MODE" = "rag-smoke-only" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  RAG SMOKE TEST"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<RAG_SMOKE
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

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
echo "RAG secrets fetched: VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, RAG_DATABASE_URL"

echo ""
echo "==> RAG smoke: preflight env-var check"
\$PYTHON_BIN -m rag.preflight

echo ""
echo "==> RAG smoke: import all 5 RAG submodules"
\$PYTHON_BIN -c "
import rag.pipelines.ingest_sec_filings
import rag.pipelines.ingest_8k_filings
import rag.pipelines.ingest_earnings_finnhub
import rag.pipelines.ingest_theses
import rag.pipelines.filing_change_detection
print('all 5 rag submodules imported OK')
"

echo ""
echo "==> RAG smoke: run_weekly_ingestion.sh --dry-run"
bash rag/pipelines/run_weekly_ingestion.sh --dry-run 2>&1
RAG_SMOKE

    echo "==> RAG smoke complete — instance will be terminated."
    exit 0
fi

# ── RAG-only: skip DataPhase1, run only RAG ingestion ───────────────────────
# Use when DataPhase1 succeeded earlier (e.g. last Saturday's SF cleared
# DataPhase1 but RAG failed downstream and needs a standalone re-run). Fetches
# secrets from SSM, runs the real (non-dry-run) RAG ingestion, emits only the
# rag-ingestion heartbeat so CloudWatch state accurately reflects what ran.
if [ "$RUN_MODE" = "rag-only" ]; then
    if [ "$PREFLIGHT_ONLY" = "1" ]; then
        echo ""
        echo "═══════════════════════════════════════════════════════════════"
        echo "  RAG-ONLY PREFLIGHT-ONLY (boot + RAG preflight, NO fetch/write)"
        echo "═══════════════════════════════════════════════════════════════"
        # Friday shell-run dry path. Fetch the 4 RAG secrets from SSM (so
        # rag.preflight's check_env_vars sees them) then run ONLY step 0
        # (rag.preflight: check_env_vars + check_s3_bucket HEAD — read-only,
        # no fetch, no write) and exit 0 BEFORE any ingest pipeline. The
        # run_weekly_ingestion.sh --preflight-only path exits 0 right
        # after `python -m rag.preflight` and before Step 1
        # (ingest_sec_filings) — proof that no ingest_*/embedding/Postgres
        # write code path is reachable. Heartbeat is deliberately NOT
        # emitted (a preflight is not a completed ingestion).
        run_remote bash -s <<RAG_ONLY_PREFLIGHT
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "──────────────────────────────────────────────────────────────"
echo "Fetching RAG secrets from SSM at \$(date)"
echo "──────────────────────────────────────────────────────────────"
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM — required for RAG preflight" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done
echo "RAG secrets fetched: VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, RAG_DATABASE_URL"

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Starting rag/pipelines/run_weekly_ingestion.sh --preflight-only at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! bash rag/pipelines/run_weekly_ingestion.sh --preflight-only 2>&1; then
    echo "ERROR: RAG preflight failed (bootstrap-class breakage caught ~12h before Saturday)." >&2
    exit 1
fi
echo "RAG preflight-only OK at \$(date)"
RAG_ONLY_PREFLIGHT

        echo ""
        echo "═══════════════════════════════════════════════════════════════"
        echo "  RAG preflight-only complete (NO fetch/write). Instance will be terminated."
        echo "═══════════════════════════════════════════════════════════════"
        exit 0
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  RAG-ONLY RUN (skipping DataPhase1)"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<RAG_ONLY
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "──────────────────────────────────────────────────────────────"
echo "Fetching RAG secrets from SSM at \$(date)"
echo "──────────────────────────────────────────────────────────────"
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM — required for RAG ingestion" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done
echo "RAG secrets fetched: VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, RAG_DATABASE_URL"

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Starting rag/pipelines/run_weekly_ingestion.sh at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! bash rag/pipelines/run_weekly_ingestion.sh 2>&1; then
    echo "ERROR: run_weekly_ingestion.sh failed." >&2
    exit 1
fi
echo "RAGIngestion complete at \$(date)"
RAG_ONLY

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  RAG-only run complete. Instance will be terminated."
    echo "═══════════════════════════════════════════════════════════════"

    aws cloudwatch put-metric-data \
        --namespace "AlphaEngine" \
        --metric-name "Heartbeat" \
        --dimensions "Process=rag-ingestion" \
        --value 1 --unit "Count" \
        --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
        && echo "Heartbeat emitted: rag-ingestion" \
        || echo "WARNING: Failed to emit heartbeat for rag-ingestion (non-fatal)"
    exit 0
fi

# ── Full / data-only / morning-enrich-only / phase1-only run ────────────────
# Each of morning-enrich and phase1+prune is independently gated via the
# DO_MORNING_ENRICH / DO_PHASE1 shell flags derived from RUN_MODE so that
# the Saturday SF can run each preflight-bearing action as its own SF task
# (preflight-task-split 2026-05-16):
#
#   full                — morning-enrich + phase1 + prune + RAG
#   data-only           — morning-enrich + phase1 + prune          (RAG separate)
#   morning-enrich-only — morning-enrich ONLY                      (RAG separate)
#   phase1-only         — phase1 + prune ONLY                      (RAG separate)
#
# MODE_LABEL feeds the spot-side S3 log key + the heartbeat dimension so a
# morning-enrich-only run is not mislabeled "data-phase1".
case "$RUN_MODE" in
    data-only)
        HEADER_LABEL="DATA-ONLY RUN: MorningEnrich + DataPhase1 (RAG runs separately)"
        DO_MORNING_ENRICH=1; DO_PHASE1=1; SKIP_RAG_BLOCK=1
        MODE_LABEL="data-phase1" ;;
    morning-enrich-only)
        HEADER_LABEL="MORNING-ENRICH-ONLY RUN (phase1 + RAG run separately)"
        DO_MORNING_ENRICH=1; DO_PHASE1=0; SKIP_RAG_BLOCK=1
        MODE_LABEL="morning-enrich" ;;
    phase1-only)
        HEADER_LABEL="PHASE1-ONLY RUN (morning-enrich + RAG run separately)"
        DO_MORNING_ENRICH=0; DO_PHASE1=1; SKIP_RAG_BLOCK=1
        MODE_LABEL="data-phase1" ;;
    *)
        HEADER_LABEL="FULL RUN: MorningEnrich + DataPhase1 + RAGIngestion"
        DO_MORNING_ENRICH=1; DO_PHASE1=1; SKIP_RAG_BLOCK=0
        MODE_LABEL="data-phase1" ;;
esac

# ── Data-path preflight-only (Friday shell-run dry path) ────────────────────
# Reuses the DO_MORNING_ENRICH / DO_PHASE1 gates above to decide WHICH
# weekly_collector preflight to run, then runs ONLY the preflight via the
# new `weekly_collector.py ... --preflight-only` flag. That flag executes
# DataPreflight(mode).run() (env/secret get_secret resolution, S3 HEAD,
# polygon/FRED auth-reachability probes, ArcticDB connect + libraries-present
# read) then sys.exit(0) BEFORE run_weekly() — run_weekly() is the sole
# function in weekly_collector that does ANY collector fetch or any
# S3/ArcticDB/parquet/config write, so it is statically unreachable here.
# No prune (builders.prune_delisted_tickers writes the prune-audit JSON),
# no RAG, no CloudWatch heartbeat, no S3 log upload — a preflight is not a
# completed workload. Zero external API DATA fetch and zero mutation.
#
# Note on universe-freshness tolerance (ROADMAP owed-item #5): the Friday
# shell-run uses the phase1 / morning_enrich preflight modes. Per
# preflight.py::DataPreflight.run, NEITHER mode runs check_arcticdb_fresh
# — they only do _check_arcticdb_libraries_present (a presence read, not a
# freshness gate). morning_enrich deliberately omits a freshness check
# (it is part of what *makes* ArcticDB fresh); phase1 *populates* ArcticDB.
# So a Friday run that predates Friday's settled polygon aggregate does
# NOT spuriously fail on a Thursday-last-bar: the only freshness gate
# (check_arcticdb_fresh, macro/SPY, 4d) lives in the "daily" mode, which
# the Saturday/Friday data path never selects. No --preflight-only-scoped
# tolerance code is required for the data path; documented here so a
# future mode-mapping change re-audits this invariant.
if [ "$PREFLIGHT_ONLY" = "1" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  PREFLIGHT-ONLY: $HEADER_LABEL"
    echo "  (boot + preflight + exit 0 — NO collector fetch, NO write)"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<PREFLIGHT_WORKLOADS
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

if [ "${DO_MORNING_ENRICH}" = "1" ]; then
    echo "──────────────────────────────────────────────────────────────"
    echo "weekly_collector.py --morning-enrich --preflight-only at \$(date)"
    echo "──────────────────────────────────────────────────────────────"
    if ! $REMOTE_PYTHON weekly_collector.py --morning-enrich --preflight-only 2>&1; then
        echo "ERROR: morning-enrich preflight failed (bootstrap-class breakage caught ~12h before Saturday)." >&2
        exit 1
    fi
fi

if [ "${DO_PHASE1}" = "1" ]; then
    echo ""
    echo "──────────────────────────────────────────────────────────────"
    echo "weekly_collector.py --phase 1 --preflight-only at \$(date)"
    echo "──────────────────────────────────────────────────────────────"
    if ! $REMOTE_PYTHON weekly_collector.py --phase 1 --preflight-only 2>&1; then
        echo "ERROR: phase1 preflight failed (bootstrap-class breakage caught ~12h before Saturday)." >&2
        exit 1
    fi
fi

echo ""
echo "Data-path preflight-only OK at \$(date) — NO fetch, NO write."
PREFLIGHT_WORKLOADS

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Preflight-only complete (NO fetch/write). Instance will be terminated."
    echo "═══════════════════════════════════════════════════════════════"
    exit 0
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  $HEADER_LABEL"
echo "═══════════════════════════════════════════════════════════════"

run_remote bash -s <<WORKLOADS
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

# ── Spot-side log capture ────────────────────────────────────────────
# SSM RunCommand truncates StandardOutputContent at 24KB and the spot
# terminates before the dispatcher can scp logs back, so post-mortem
# debugging requires the full log to land somewhere durable. Tee
# everything below into /tmp/<mode-label>.log + upload to S3 on ANY exit
# path (success, hard-fail, signal). Origin: 2026-05-03 SF failure where
# the postflight error message was past the SSM truncation cutoff and
# the spot was already gone by the time triage started. The S3 key uses
# the per-mode label (preflight-task-split 2026-05-16) so a
# morning-enrich-only run's log does not land under data_phase1_log/.
MODE_LABEL="${MODE_LABEL}"
LOG_FILE=/tmp/\${MODE_LABEL}.log
exec > >(tee -a "\$LOG_FILE") 2>&1

upload_log() {
    local exit_code=\$?
    local s3_key="health/\${MODE_LABEL//-/_}_log/\$(date +%Y-%m-%d)/\$(date +%Y%m%dT%H%M%SZ -u)-exit\${exit_code}.log"
    aws s3 cp "\$LOG_FILE" "s3://${S3_BUCKET}/\$s3_key" --region "\${AWS_REGION:-us-east-1}" 2>/dev/null \\
        && echo "[log-upload] s3://${S3_BUCKET}/\$s3_key" \\
        || echo "[log-upload] WARNING: failed to upload \$LOG_FILE to S3"
}
trap upload_log EXIT

# ── Morning enrich (Saturday-morning polygon-T+1 fill) ────────────────
# Polygon's grouped-daily aggregate for date T isn't fully settled
# until the next calendar day (T+1). The Friday weekday-SF run
# (Friday ~13:05 PT) collects daily_closes pre-settlement, so Friday's
# row in S3 + ArcticDB may carry stale / partial polygon data.
#
# By the time the Saturday SF kicks off (09:00 UTC = 02:00 AM PT Sat),
# polygon's Friday data IS settled. This step re-fetches Friday's
# daily_closes via polygon (same code path the weekday SF MorningEnrich
# Lambda uses) and re-appends to ArcticDB so all downstream Saturday
# work (Phase 1 prices, RAG, predictor training, backtester) reads
# polygon-authoritative Friday closes.
#
# Order matters: must run BEFORE Phase 1 + builders.prune_delisted_tickers
# so universe-state reflects the corrected Friday data.
#
# DO_MORNING_ENRICH / DO_PHASE1 (set on the dispatcher from RUN_MODE,
# interpolated below) gate each preflight-bearing action independently so
# a phase1 failure in its own SF task never re-runs a completed
# morning-enrich (preflight-task-split 2026-05-16).
if [ "${DO_MORNING_ENRICH}" = "1" ]; then
echo "──────────────────────────────────────────────────────────────"
echo "Starting weekly_collector.py --morning-enrich (Friday polygon-T+1 fill) at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! $REMOTE_PYTHON weekly_collector.py --morning-enrich 2>&1; then
    echo "ERROR: weekly_collector.py --morning-enrich failed — Friday's polygon-authoritative daily_closes not collected. Aborting so downstream consumers don't read stale data." >&2
    exit 1
fi
echo "MorningEnrich complete at \$(date)"
else
echo "──────────────────────────────────────────────────────────────"
echo "Skipping weekly_collector.py --morning-enrich (runs in separate SF state)"
echo "──────────────────────────────────────────────────────────────"
fi

if [ "${DO_PHASE1}" = "1" ]; then
echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Starting weekly_collector.py --phase 1 at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! $REMOTE_PYTHON weekly_collector.py --phase 1 2>&1; then
    echo "ERROR: weekly_collector.py --phase 1 failed." >&2
    exit 1
fi
echo "DataPhase1 complete at \$(date)"

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Starting builders.prune_delisted_tickers at \$(date)"
echo "──────────────────────────────────────────────────────────────"
# Prune delisted tickers from ArcticDB universe. Two-condition guard
# (constituents-absent AND last_date stale) prevents flapping; audit
# JSON is written to s3://alpha-engine-research/builders/prune_audit/.
# Composes with daily_append's missing-from-closes hard-fail (PR #101)
# — closes the loop on legit delistings so the threshold doesn't keep
# getting bumped or symbols manually deleted. Constituents.json was
# just refreshed by Phase 1 above, so this read is fresh.
if ! $REMOTE_PYTHON -m builders.prune_delisted_tickers --apply 2>&1; then
    echo "ERROR: prune_delisted_tickers failed." >&2
    exit 1
fi
echo "UniversePrune complete at \$(date)"
else
echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Skipping weekly_collector.py --phase 1 + prune (runs in separate SF state)"
echo "──────────────────────────────────────────────────────────────"
fi

if [ "${SKIP_RAG_BLOCK}" = "1" ]; then
    echo ""
    echo "──────────────────────────────────────────────────────────────"
    echo "data-only mode — skipping RAG ingestion (runs in separate SF state)"
    echo "──────────────────────────────────────────────────────────────"
    exit 0
fi

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Fetching RAG secrets from SSM at \$(date)"
echo "──────────────────────────────────────────────────────────────"
# Phase 2 SSM migration — RAG secrets come from SSM Parameter Store, NOT
# from the SCP'd .env. Origin: 2026-04-17 Saturday Step Function failure
# where RAG_DATABASE_URL silently truncated at an unquoted & in the .env
# (a Postgres DSN query-param). Bash source on AL2023 spots dropped the
# tail of the value after the shell metachar. SSM stores the value as an
# opaque string — no shell-parse fragility, no cross-instance sync via
# push-secrets.sh needed, and the spot's IAM profile already has
# ssm:GetParameter for parameters under /alpha-engine/*.
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM — required for RAG ingestion" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done
echo "RAG secrets fetched: VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, RAG_DATABASE_URL"

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Starting rag/pipelines/run_weekly_ingestion.sh at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! bash rag/pipelines/run_weekly_ingestion.sh 2>&1; then
    echo "ERROR: run_weekly_ingestion.sh failed." >&2
    exit 1
fi
echo "RAGIngestion complete at \$(date)"
WORKLOADS

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Weekly data bundle complete. Instance will be terminated."
echo "═══════════════════════════════════════════════════════════════"

# Heartbeat — one metric per sub-workload so CloudWatch alarms can
# distinguish between a missed MorningEnrich, a missed Phase 1, a missed
# prune, and a missed RAG. Per the preflight-task-split (2026-05-16) each
# mode emits only the heartbeats for the actions it actually ran so a
# morning-enrich-only run isn't credited with a data-phase1 heartbeat
# (and vice versa). In data-only / split modes the rag-ingestion
# heartbeat is emitted by the separate RAG-only spot run, so don't
# double-emit here.
case "$RUN_MODE" in
    morning-enrich-only) HEARTBEAT_PROCS=("morning-enrich") ;;
    phase1-only)         HEARTBEAT_PROCS=("data-phase1" "universe-prune") ;;
    data-only)           HEARTBEAT_PROCS=("morning-enrich" "data-phase1" "universe-prune") ;;
    *)                   HEARTBEAT_PROCS=("morning-enrich" "data-phase1" "universe-prune" "rag-ingestion") ;;
esac
for proc in "${HEARTBEAT_PROCS[@]}"; do
    aws cloudwatch put-metric-data \
        --namespace "AlphaEngine" \
        --metric-name "Heartbeat" \
        --dimensions "Process=$proc" \
        --value 1 --unit "Count" \
        --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
        && echo "Heartbeat emitted: $proc" \
        || echo "WARNING: Failed to emit heartbeat for $proc (non-fatal)"
done
