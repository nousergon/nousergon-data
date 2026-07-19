#!/usr/bin/env bash
# rag/pipelines/run_weekly_ingestion.sh — Saturday RAG ingestion: delta-only
# top-up (config#2943 / EPIC config#2967).
#
# STRUCTURAL CHANGE (config#2943): this step used to run a full-universe
# synchronous sweep (SEC filings + Polygon news over ALL ~944 signals
# tickers) taking 4-6+ hours inside the Saturday critical path. The weekday
# `run_daily_corpus_delta.sh` job (its own dedicated daily spot, see that
# script + infrastructure/lambdas/data-spot-dispatcher) now keeps the SAME
# pgvector corpus warm every weekday with a filings+news delta scoped to
# holdings ∪ active candidates ∪ top-60 signals board (config#2943 binding
# ruling, ≈100-150 tickers). Saturday's job is therefore now a SMALL
# delta-only TOP-UP: whatever the week's daily passes might have missed
# (a transient daily failure, a same-day scope change late in the week,
# weekend filings), NOT a full re-sweep. Target: O(10 min), well inside the
# config#2938/#2946 budget guard-rails this script keeps as a SAFETY NET
# (a delta-only run still needs a budget — just a much smaller one; see
# collectors/news_sources/fetch_budget.py).
#
# Steps:
#   0.  Preflight — env vars + S3 reachability (hard-fails on miss)
#   1.  Resolve corpus scope (holdings ∪ active candidates ∪ top-60 board) —
#       SAME resolver the daily delta uses (config#2943: one shared
#       resolver, not six copies re-deriving the universe independently).
#   2.  SEC filings (10-K/10-Q) top-up — short lookback, dedup-idempotent
#       against whatever the week's daily passes already ingested.
#   3.  8-K material events top-up — short lookback.
#   4.  Earnings transcripts (Finnhub) top-up.
#   5.  Thesis history top-up (scoped).
#   6.  News top-up — short lookback (not the full 168h/7-day sweep the
#       full-universe design used; the week's daily passes already covered
#       the 7 days incrementally).
#   7.  Form 4 insider transactions — UNCHANGED (--from-signals, out of
#       config#2943's RAG-corpus scope ruling; structured-only, not RAG —
#       see PR body for why this and 13F/analyst-pipeline stay as explicit
#       follow-up items).
#   8.  13F institutional ownership — scoped top-up.
#   9.  Analyst pipeline — UNCHANGED (--from-signals; not RAG-corpus, S3
#       JSON snapshots — same follow-up bucket as Form 4).
#   10. Filing change detection — scoped to the SAME corpus population.
#   11. Manifest emit — unchanged (whole-corpus snapshot; cheap, pgvector
#       aggregate query, not a fetch).
#
# Intended to run on the Saturday Step Function via SSM on a fresh spot
# EC2 (RAGIngestion state, infrastructure/step_function.json), same
# dispatch shape as before this change. `set -euo pipefail` plus no
# `|| echo "non-fatal"` swallowers means any ingestion failure aborts the
# script with a non-zero exit, surfaces in SSM logs, fails the Step
# Function, and fires flow-doctor.
#
# Usage:
#   bash rag/pipelines/run_weekly_ingestion.sh                 # full run
#   bash rag/pipelines/run_weekly_ingestion.sh --dry-run       # preview only
#   bash rag/pipelines/run_weekly_ingestion.sh --preflight-only # step 0 only, exit 0
#
# --preflight-only (Friday shell-run dry path, ROADMAP "Friday shell-run —
# per-module dry-path activation" #1): runs ONLY Step 0 (python -m
# rag.preflight: check_env_vars + check_s3_bucket HEAD — both read-only,
# zero external API fetch, zero write) then exits 0 BEFORE Step 1
# (resolving scope / ingest_sec_filings). No ingest_* pipeline, no Voyage
# embedding call, no Postgres/pgvector write, no manifest emit is reachable
# under it.
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
PREFLIGHT_ONLY=0
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN="--dry-run" ;;
        --preflight-only) PREFLIGHT_ONLY=1 ;;
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
echo "RAG Saturday Delta-Only Top-Up — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

# ── Step 0: Preflight — fail fast on env / connectivity drift ────────────────
echo ""
echo "==> Step 0/11: Preflight checks..."
$PYTHON_BIN -m rag.preflight

# Friday shell-run dry path: stop HERE, immediately after the existing
# rag.preflight passed and strictly BEFORE Step 1 (scope resolution + the
# first ingest pipeline). Every fetch (SEC/Finnhub/yfinance), every Voyage
# embedding call, and every Postgres/pgvector + parquet write lives in
# Steps 1-11 below — all statically unreachable once we exit here.
if [ "$PREFLIGHT_ONLY" = "1" ]; then
    echo ""
    echo "==> --preflight-only: rag.preflight passed — exiting 0 before Step 1"
    echo "    (NO ingest, NO embedding, NO Postgres/parquet write)."
    exit 0
fi

# ── Step 1: Resolve corpus scope + daily-coverage freshness ────────────────
# SAME shared resolver the daily delta uses (config#2943) — holdings ∪
# active candidates ∪ top-60 signals board, ≈100-150 tickers. Resolved ONCE
# and threaded through every step below via --tickers (not --scope) so a
# same-day scope change mid-run (e.g. Scanner's candidates.json landing
# between steps) can't split the run across two different populations.
#
# COLD-START / MISSED-WEEK GUARD: the short top-up windows below (14d
# filings, 48h news) are only correct if the week's daily deltas actually
# ran. Check rag_corpus/scope_state/latest.json's `as_of` (written ONLY by
# run_daily_corpus_delta.sh on a successful pass) — if it's missing, or
# older than 7 days (a full week with zero successful daily passes: first
# deploy of this PR, or a sustained daily-delta outage), widen every
# top-up step below back to full-coverage windows instead of silently
# running a thin delta with a clean exit code. WIDE_TOPUP=1 threads this
# through every step; the alternative (skip the whole run) would leave the
# corpus stale for another week, which is worse.
echo ""
echo "==> Step 1/11: Resolve corpus scope + daily-coverage freshness..."
SCOPE_FILE=/tmp/weekly_ingestion_scope.json
$PYTHON_BIN -c "
import json
from rag.pipelines._corpus_scope import resolve_corpus_scope
from rag.pipelines._corpus_scope_state import needs_wide_topup

scope = resolve_corpus_scope()
if not scope:
    raise SystemExit('[run_weekly_ingestion] resolve_corpus_scope() returned EMPTY — '
                      'all three sources (holdings/candidates/board) unavailable. Aborting.')
print(f'[run_weekly_ingestion] resolved scope: {len(scope)} tickers')

wide_topup = needs_wide_topup()
with open('$SCOPE_FILE', 'w') as f:
    json.dump({'scope': sorted(scope), 'wide_topup': wide_topup}, f)
"
SCOPE_TICKERS=$($PYTHON_BIN -c "import json; print(','.join(json.load(open('$SCOPE_FILE'))['scope']))")
SCOPE_COUNT=$($PYTHON_BIN -c "import json; print(len(json.load(open('$SCOPE_FILE'))['scope']))")
WIDE_TOPUP=$($PYTHON_BIN -c "import json; print('1' if json.load(open('$SCOPE_FILE'))['wide_topup'] else '0')")
if [ -z "$SCOPE_TICKERS" ]; then
    echo "ERROR: resolved scope is empty — aborting." >&2
    exit 1
fi
echo "  Scope: $SCOPE_COUNT tickers | wide top-up (cold-start/missed-week guard): $WIDE_TOPUP"

if [ "$WIDE_TOPUP" = "1" ]; then
    FILINGS_LOOKBACK_DAYS=730     # 2yr — matches the old full-coverage default
    EIGHTK_LOOKBACK_DAYS=365
    NEWS_HOURS=168                 # 7 days — matches the old full-coverage default
else
    FILINGS_LOOKBACK_DAYS=14
    EIGHTK_LOOKBACK_DAYS=14
    NEWS_HOURS=48
fi

# ── Step 2: SEC filings (10-K/10-Q) top-up ──────────────────────────────────
# Short lookback (14d) in the normal case — the week's daily passes already
# covered new filings incrementally; this catches anything a daily pass
# missed (transient failure) or filed since the last daily pass ran.
# Widened to the full 2yr window above if the cold-start/missed-week guard
# fired. Idempotent via document_exists regardless of overlap either way.
echo ""
echo "==> Step 2/11: SEC filings (10-K/10-Q) top-up (lookback=${FILINGS_LOOKBACK_DAYS}d)..."
$PYTHON_BIN -m rag.pipelines.ingest_sec_filings --tickers "$SCOPE_TICKERS" --lookback-days "$FILINGS_LOOKBACK_DAYS" $DRY_RUN

# ── Step 3: 8-K material events top-up ──────────────────────────────────────
echo ""
echo "==> Step 3/11: 8-K material events top-up (lookback=${EIGHTK_LOOKBACK_DAYS}d)..."
$PYTHON_BIN -m rag.pipelines.ingest_8k_filings --tickers "$SCOPE_TICKERS" --lookback-days "$EIGHTK_LOOKBACK_DAYS" $DRY_RUN

# ── Step 4: Earnings transcripts (Finnhub) top-up ───────────────────────────
echo ""
echo "==> Step 4/11: Earnings transcripts (Finnhub) top-up..."
$PYTHON_BIN -m rag.pipelines.ingest_earnings_finnhub --tickers "$SCOPE_TICKERS" --max-per-ticker 8 $DRY_RUN

# ── Step 5: Thesis history top-up (scoped, v2 signals.json) ────────────────
echo ""
echo "==> Step 5/11: Thesis history top-up..."
SINCE=$(date -u -d '14 days ago' '+%Y-%m-%d' 2>/dev/null || date -u -v-14d '+%Y-%m-%d')
$PYTHON_BIN -m rag.pipelines.ingest_theses --signals --scope holdings+candidates+board60 --since "$SINCE" $DRY_RUN

# ── LM dict bootstrap (one-time per spot instance) ───────────────────────────
LM_DICT_PATH="collectors/nlp/data/lm_master_dict.csv"
if [ ! -f "$LM_DICT_PATH" ]; then
    echo ""
    echo "==> LM dict bootstrap: downloading Loughran-McDonald master dict..."
    $PYTHON_BIN scripts/download_lm_dict.py || \
        echo "WARN: LM dict download failed — news NLP sentiment will return zero scores"
else
    echo "==> LM dict bootstrap: $LM_DICT_PATH already present, skipping download"
fi

# ── Step 6: News top-up ──────────────────────────────────────────────────────
# 48h lookback in the normal case (not the old 168h/7-day full sweep) — the
# week's daily passes already covered each day incrementally at 24h
# lookback; 48h gives a small overlap margin against a missed daily run.
# Widened to the full 168h/7-day window above if the cold-start/missed-week
# guard fired. Polygon budget derives from the SCOPE size (≈100-150
# tickers), not the full signals universe — see
# collectors/news_sources/fetch_budget.py::weekly_news_max_fetch_seconds,
# whose input is this scope's size, not len(signals.json universe).
echo ""
echo "==> Step 6/11: News top-up (hours=${NEWS_HOURS})..."
$PYTHON_BIN -m rag.pipelines.run_news_pipeline --tickers "$SCOPE_TICKERS" --hours "$NEWS_HOURS" $DRY_RUN

# ── Step 7: Form 4 insider transactions ─────────────────────────────────────
# UNCHANGED — out of config#2943's RAG-corpus scope ruling (structured-only
# S3 parquet, not RAG text corpus). Still --from-signals (full universe);
# flagged as an explicit follow-up in the PR body, not silently left as-is.
echo ""
echo "==> Step 7/11: Form 4 insider transactions..."
$PYTHON_BIN -m rag.pipelines.ingest_form4 --from-signals --lookback-days 90 $DRY_RUN

# ── Step 8: 13F institutional ownership (scoped top-up) ─────────────────────
echo ""
echo "==> Step 8/11: 13F institutional ownership top-up..."
$PYTHON_BIN -m rag.pipelines.ingest_13f --tickers "$SCOPE_TICKERS" $DRY_RUN

# ── Step 9: Analyst pipeline ─────────────────────────────────────────────────
# UNCHANGED — out of config#2943's RAG-corpus scope ruling (S3 JSON
# snapshots, not RAG text corpus). Still --from-signals; same follow-up
# bucket as Form 4 (see PR body).
echo ""
echo "==> Step 9/11: Analyst snapshot + revisions..."
$PYTHON_BIN -m rag.pipelines.run_analyst_pipeline --from-signals $DRY_RUN

# ── Step 10: Filing change detection (scoped) ────────────────────────────────
echo ""
echo "==> Step 10/11: Filing change detection..."
if [ -z "$DRY_RUN" ]; then
    $PYTHON_BIN -m rag.pipelines.filing_change_detection --tickers "$SCOPE_TICKERS" --output-s3
else
    echo "  SKIPPED in dry-run mode"
fi

# ── Step 11: Manifest emit (presentation-layer source of truth) ─────────────
# Unchanged: a whole-corpus pgvector aggregate query, not a fetch — cheap
# regardless of scope, and downstream consumers (presentation layer) expect
# a full-corpus snapshot including retained out-of-scope rows (config#2943
# ruling: out-of-scope tickers' existing rows are RETAINED, never deleted).
echo ""
echo "==> Step 11/11: Emit corpus manifest..."
if [ -z "$DRY_RUN" ]; then
    $PYTHON_BIN -m rag.pipelines.emit_manifest --output-s3
else
    echo "  SKIPPED in dry-run mode"
fi

echo ""
echo "========================================"
echo "RAG Saturday Delta-Only Top-Up Complete — $(date -u '+%Y-%m-%d %H:%M UTC')"
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
# point means all steps succeeded — the hardcoded 'ok' statuses are
# truthful rather than aspirational. PYTHON_BIN resolved at top of script.
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
        'inst_ownership_13f': {'status': 'ok'},
        'analyst_pipeline': {'status': 'ok'},
        'filing_changes': {'status': 'ok'},
    },
}
send_step_email('RAG Ingestion', results, date_str)
"
