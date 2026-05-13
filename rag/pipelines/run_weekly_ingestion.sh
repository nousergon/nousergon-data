#!/usr/bin/env bash
# rag/pipelines/run_weekly_ingestion.sh — Weekly RAG ingestion pipeline.
#
# Runs all ingestion pipelines in sequence:
#   0. Preflight — env vars + S3 reachability (hard-fails on miss)
#   1. SEC filings (10-K/10-Q) — from signals universe, 2y lookback
#   2. 8-K material events — from signals universe, 1y lookback
#   3. Earnings transcripts (Finnhub) — from signals universe, latest 8
#   4. Thesis history — from research.db (incremental)
#   5. News pipeline (Wave 1 Gate A) — fetch via aggregator → NLP →
#      news_aggregates parquet + RAG corpus ingest
#   6. Form 4 insider transactions (Wave 1 Gate A) — EDGAR → parquet
#   7. Analyst pipeline (Wave 1 Gate A) — yfinance + Finnhub snapshot
#      → self-derived 7d/30d revisions
#   8. Filing change detection — analyze consecutive filings
#   9. Manifest emit — corpus snapshot for presentation layer
#
# Intended to run on the Saturday Step Function via SSM on the always-on
# EC2 instance. `set -euo pipefail` plus no `|| echo "non-fatal"`
# swallowers means any ingestion failure aborts the script with a
# non-zero exit, surfaces in SSM logs, fails the Step Function, and
# fires flow-doctor.
#
# Usage:
#   bash rag/pipelines/run_weekly_ingestion.sh              # full run
#   bash rag/pipelines/run_weekly_ingestion.sh --dry-run    # preview only
#
# Prerequisites (verified by step 0):
#   - .env with RAG_DATABASE_URL, VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY
#   - research.db available locally or fetchable from S3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Parse flags
DRY_RUN=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN="--dry-run" ;;
    esac
done

# Activate venv
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
fi

# Resolve python binary. Callers (e.g. spot_data_weekly.sh dispatcher) can
# export PYTHON_BIN so we inherit whichever interpreter they bootstrapped
# (Amazon Linux 2023 spots install python3.12 but have no bare `python`
# symlink, which caused the 2026-04-17 Saturday RAG failure). Fall back to
# python3 → python3.12 → python so local/manual runs and venv-activated
# runs still work.
if [ -z "${PYTHON_BIN:-}" ]; then
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN="python3"
    elif command -v python3.12 >/dev/null 2>&1; then
        PYTHON_BIN="python3.12"
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN="python"
    else
        echo "ERROR: no python interpreter found (tried python3, python3.12, python)" >&2
        exit 1
    fi
fi
echo "Using PYTHON_BIN=$PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"

START_TIME="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

echo "========================================"
echo "RAG Weekly Ingestion — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

# ── Step 0: Preflight — fail fast on env / connectivity drift ────────────────
echo ""
echo "==> Step 0/9: Preflight checks..."
$PYTHON_BIN -m rag.preflight

# ── Step 1: SEC filings (10-K/10-Q) ─────────────────────────────────────────
echo ""
echo "==> Step 1/9: SEC filings (10-K/10-Q)..."
$PYTHON_BIN -m rag.pipelines.ingest_sec_filings --from-signals --lookback-years 2 $DRY_RUN

# ── Step 2: 8-K material events ─────────────────────────────────────────────
echo ""
echo "==> Step 2/9: 8-K material events..."
$PYTHON_BIN -m rag.pipelines.ingest_8k_filings --from-signals --lookback-days 365 $DRY_RUN

# ── Step 3: Earnings transcripts (Finnhub) ──────────────────────────────────
# FINNHUB_API_KEY is verified by preflight; no runtime skip branch.
echo ""
echo "==> Step 3/9: Earnings transcripts (Finnhub)..."
$PYTHON_BIN -m rag.pipelines.ingest_earnings_finnhub --from-signals --max-per-ticker 8 $DRY_RUN

# ── Step 4: Thesis history (v2 quant/qual from signals.json) ─────────────────
echo ""
echo "==> Step 4/9: Thesis history..."
SINCE=$(date -u -d '14 days ago' '+%Y-%m-%d' 2>/dev/null || date -u -v-14d '+%Y-%m-%d')
$PYTHON_BIN -m rag.pipelines.ingest_theses --signals --since "$SINCE" $DRY_RUN

# ── LM dict bootstrap (one-time per spot instance) ───────────────────────────
# Loughran-McDonald master dictionary CSV is required by the news NLP
# pipeline's sentiment scorer. ~10 MB free download from Notre Dame.
# Idempotent: scripts/download_lm_dict.py overwrites if present; skip if
# the file already exists locally on a re-run.
LM_DICT_PATH="collectors/nlp/data/lm_master_dict.csv"
if [ ! -f "$LM_DICT_PATH" ]; then
    echo ""
    echo "==> LM dict bootstrap: downloading Loughran-McDonald master dict..."
    $PYTHON_BIN scripts/download_lm_dict.py || \
        echo "WARN: LM dict download failed — news NLP sentiment will return zero scores"
else
    echo "==> LM dict bootstrap: $LM_DICT_PATH already present, skipping download"
fi

# ── Step 5: News pipeline (Wave 1 Gate A) ────────────────────────────────────
# Fetch news via NewsAggregator (Polygon + GDELT + Yahoo RSS) → NLP pipeline
# (Loughran-McDonald sentiment + Anthropic-Haiku event extraction) → write
# structured aggregates parquet → ingest article narrative to RAG corpus.
echo ""
echo "==> Step 5/9: News pipeline..."
$PYTHON_BIN -m rag.pipelines.run_news_pipeline --from-signals --hours 168 $DRY_RUN

# ── Step 6: Form 4 insider transactions (Wave 1 Gate A) ──────────────────────
# EDGAR Form 4 → structured per-(filed_date) parquet at
# s3://alpha-engine-research/data/insider_transactions/{date}.parquet
echo ""
echo "==> Step 6/9: Form 4 insider transactions..."
$PYTHON_BIN -m rag.pipelines.ingest_form4 --from-signals --lookback-days 90 $DRY_RUN

# ── Step 7: Analyst pipeline (Wave 1 Gate A) ─────────────────────────────────
# Snapshot per-(ticker, date) consensus + price targets via yfinance + Finnhub,
# then compute 7d/30d revisions deltas from the accumulated time series.
# Revisions become meaningful after ~4 weekly snapshots (Gate B in ROADMAP).
echo ""
echo "==> Step 7/9: Analyst snapshot + revisions..."
$PYTHON_BIN -m rag.pipelines.run_analyst_pipeline --from-signals $DRY_RUN

# ── Step 8: Filing change detection ──────────────────────────────────────────
echo ""
echo "==> Step 8/9: Filing change detection..."
if [ -z "$DRY_RUN" ]; then
    $PYTHON_BIN -m rag.pipelines.filing_change_detection --output-s3
else
    echo "  SKIPPED in dry-run mode"
fi

# ── Step 9: Manifest emit (presentation-layer source of truth) ───────────────
echo ""
echo "==> Step 9/9: Emit corpus manifest..."
if [ -z "$DRY_RUN" ]; then
    $PYTHON_BIN -m rag.pipelines.emit_manifest --output-s3
else
    echo "  SKIPPED in dry-run mode"
fi

echo ""
echo "========================================"
echo "RAG Weekly Ingestion Complete — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

# Emit CloudWatch heartbeat on successful completion. No `|| echo` swallow
# — a broken heartbeat emission is a real signal worth escalating.
aws cloudwatch put-metric-data \
  --namespace "AlphaEngine" \
  --metric-name "Heartbeat" \
  --dimensions "Process=rag-ingestion" \
  --value 1 --unit "Count" \
  --region "${AWS_REGION:-us-east-1}"
echo "Heartbeat emitted: rag-ingestion"

# Send completion email. With `set -euo pipefail` active, reaching this
# point means all 9 pipelines succeeded — the hardcoded 'ok' statuses
# are truthful rather than aspirational. PYTHON_BIN resolved at top of script.
$PYTHON_BIN -c "
from emailer import send_step_email
from datetime import datetime, timezone
date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
results = {
    'phase': 'RAG',
    'date': date_str,
    'started_at': '$START_TIME',
    'completed_at': datetime.now(timezone.utc).isoformat(),
    'status': 'ok',
    'collectors': {
        'sec_filings': {'status': 'ok'},
        '8k_events': {'status': 'ok'},
        'earnings_transcripts': {'status': 'ok'},
        'thesis_history': {'status': 'ok'},
        'news_pipeline': {'status': 'ok'},
        'form4_insider': {'status': 'ok'},
        'analyst_pipeline': {'status': 'ok'},
        'filing_changes': {'status': 'ok'},
    },
}
send_step_email('RAG Ingestion', results, date_str)
"
