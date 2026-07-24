#!/usr/bin/env bash
# rag/pipelines/run_daily_corpus_delta.sh — Weekday RAG corpus delta (config#2943).
#
# THE structural fix for the Saturday RAGIngestion critical-path problem:
# collects the FULL corpus incrementally on weekdays (filings delta + news
# delta) into the same pgvector corpus the Saturday step reads, scoped to
# holdings ∪ active candidates ∪ top-60 signals board (config#2943 binding
# ruling, ≈100-150 tickers — NOT the ~900-ticker signals.json universe).
# Saturday's RAGIngestion then becomes a small delta-only top-up instead of
# a full multi-hour sweep (see run_weekly_ingestion.sh's --delta-only mode).
#
# Steps:
#   0.  Preflight — env vars + S3 reachability (hard-fails on miss)
#   1.  Resolve corpus scope (holdings ∪ active candidates ∪ top-60 board)
#   2.  Diff against yesterday's persisted scope — tickers NEW to scope get
#       the full 2yr filings lookback folded in; existing tickers get a
#       short incremental lookback (new filings since the last pass —
#       idempotent either way via document_exists, so overlap is a no-op).
#   3.  SEC filings (10-K/10-Q) delta
#   4.  8-K material events delta
#   5.  Earnings transcripts (Finnhub) delta
#   6.  News delta (via run_news_pipeline.py — same RAG-ingest path the
#       Saturday sweep uses, just over the small scoped population and a
#       24h lookback instead of 168h)
#   7.  filing_change_detection over the same scoped set (config#2943 ruling
#       item 4 — the scoped set is the default population)
#   8.  Persist today's resolved scope as the new churn-detection pointer
#
# Intended to run on a dedicated small daily spot EC2 (own EventBridge
# schedule, own launch/terminate — see infrastructure/spot_data_weekly.sh
# --daily-corpus-delta and the design note in nousergon-data PR for
# config#2943), NOT inside the weekday trading SF and NOT bolted onto
# daily-news.service (see that PR's design note for why: daily-news runs
# a slim news-only venv with a hard-gated podcast dependency at 04:00 PT;
# this needs the FULL RAG stack — Voyage embeddings, pgvector — and must
# never risk that critical path). `set -euo pipefail` — the exit code
# surfaces in SSM logs, is caught by the launcher's heartbeat gate, and
# fires flow-doctor same as the weekly script.
#
# Usage:
#   bash rag/pipelines/run_daily_corpus_delta.sh                 # full delta run
#   bash rag/pipelines/run_daily_corpus_delta.sh --dry-run        # preview only
#   bash rag/pipelines/run_daily_corpus_delta.sh --preflight-only # step 0 only, exit 0
#
# Prerequisites (verified by step 0, same contract as run_weekly_ingestion.sh):
#   - .env with RAG_DATABASE_URL, VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY

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
echo "RAG Daily Corpus Delta — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

# ── Step 0: Preflight ─────────────────────────────────────────────────────
echo ""
echo "==> Step 0/8: Preflight checks..."
$PYTHON_BIN -m rag.preflight

if [ "$PREFLIGHT_ONLY" = "1" ]; then
    echo ""
    echo "==> --preflight-only: rag.preflight passed — exiting 0 before Step 1"
    echo "    (NO ingest, NO embedding, NO Postgres write)."
    exit 0
fi

# ── Step 1-2: Resolve scope + churn diff ────────────────────────────────────
# Writes /tmp/corpus_delta_scope.json (tickers + new_to_scope list) for the
# subsequent steps to consume via --tickers (explicit list, NOT --scope, so
# every step operates on the EXACT SAME resolved set — re-resolving per-step
# risks a same-day scope drift, e.g. Scanner's candidates.json landing mid-run).
echo ""
echo "==> Step 1-2/8: Resolve corpus scope + detect ticker churn..."
SCOPE_STATE_FILE=/tmp/corpus_delta_scope.json
$PYTHON_BIN -c "
import json
from rag.pipelines._corpus_scope import resolve_corpus_scope
from rag.pipelines._corpus_scope_state import load_prior_scope, diff_scope

current = resolve_corpus_scope()
if not current:
    raise SystemExit('[run_daily_corpus_delta] resolve_corpus_scope() returned EMPTY — '
                      'all three sources (holdings/candidates/board) unavailable. Aborting.')
prior = load_prior_scope()
new_to_scope, dropped = diff_scope(current, prior)
print(f'[run_daily_corpus_delta] scope={len(current)} new_to_scope={len(new_to_scope)} dropped={len(dropped)}')
if new_to_scope:
    print(f'[run_daily_corpus_delta] new-to-scope tickers (full 2yr backfill folded in): {sorted(new_to_scope)}')
with open('$SCOPE_STATE_FILE', 'w') as f:
    json.dump({
        'current': sorted(current),
        'new_to_scope': sorted(new_to_scope),
        'existing': sorted(current - new_to_scope),
    }, f)
"
CURRENT_TICKERS=$($PYTHON_BIN -c "import json; print(','.join(json.load(open('$SCOPE_STATE_FILE'))['current']))")
NEW_TICKERS=$($PYTHON_BIN -c "import json; d=json.load(open('$SCOPE_STATE_FILE')); print(','.join(d['new_to_scope']))")
EXISTING_TICKERS=$($PYTHON_BIN -c "import json; d=json.load(open('$SCOPE_STATE_FILE')); print(','.join(d['existing']))")

if [ -z "$CURRENT_TICKERS" ]; then
    echo "ERROR: resolved scope is empty — aborting delta run." >&2
    exit 1
fi

# ── Step 3: SEC filings delta ───────────────────────────────────────────────
# New-to-scope tickers get the full 2yr backfill; existing tickers get a
# short incremental lookback (idempotent overlap via document_exists — a
# ticker re-checked inside its already-covered window is a fast no-op per
# filing, not a re-download).
echo ""
echo "==> Step 3/8: SEC filings (10-K/10-Q) delta..."
if [ -n "$NEW_TICKERS" ]; then
    echo "  -- new-to-scope backfill (2yr): $NEW_TICKERS"
    $PYTHON_BIN -m rag.pipelines.ingest_sec_filings --tickers "$NEW_TICKERS" --lookback-years 2 $DRY_RUN
fi
if [ -n "$EXISTING_TICKERS" ]; then
    echo "  -- incremental (7d, dedup-idempotent): $(echo "$EXISTING_TICKERS" | tr ',' '\n' | wc -l) tickers"
    $PYTHON_BIN -m rag.pipelines.ingest_sec_filings --tickers "$EXISTING_TICKERS" --lookback-days 7 $DRY_RUN
fi

# ── Step 4: 8-K material events delta ───────────────────────────────────────
echo ""
echo "==> Step 4/8: 8-K material events delta..."
if [ -n "$NEW_TICKERS" ]; then
    $PYTHON_BIN -m rag.pipelines.ingest_8k_filings --tickers "$NEW_TICKERS" --lookback-days 365 $DRY_RUN
fi
if [ -n "$EXISTING_TICKERS" ]; then
    $PYTHON_BIN -m rag.pipelines.ingest_8k_filings --tickers "$EXISTING_TICKERS" --lookback-days 7 $DRY_RUN
fi

# ── Step 5: Earnings transcripts (Finnhub) delta ────────────────────────────
# Transcripts are inherently low-frequency (quarterly); run over the FULL
# current scope every day (max 8/ticker, dedup-idempotent) rather than
# splitting new/existing — the discovery call itself is cheap.
echo ""
echo "==> Step 5/8: Earnings transcripts (Finnhub) delta..."
$PYTHON_BIN -m rag.pipelines.ingest_earnings_finnhub --tickers "$CURRENT_TICKERS" --max-per-ticker 8 $DRY_RUN

# ── Step 6: News delta ──────────────────────────────────────────────────────
# Same RAG-ingest path (run_news_pipeline.py) the Saturday sweep uses — the
# corpus dedup key (config#2957) makes overlap with tomorrow's/Saturday's
# pass a no-op. 24h lookback (yesterday → today); the daily-news.service's
# own concurrent 24h pull is a SEPARATE artifact (data/news_aggregates_daily/)
# for the morning brief — this write targets the SAME data/news_aggregates/
# + RAG-corpus path the Saturday step reads, at daily cadence.
echo ""
echo "==> Step 6/8: News delta..."
$PYTHON_BIN -m rag.pipelines.run_news_pipeline --tickers "$CURRENT_TICKERS" --hours 24 --budget-profile daily $DRY_RUN

# ── Step 7: Filing change detection (scoped) ────────────────────────────────
echo ""
echo "==> Step 7/8: Filing change detection (scoped set)..."
if [ -z "$DRY_RUN" ]; then
    $PYTHON_BIN -m rag.pipelines.filing_change_detection --tickers "$CURRENT_TICKERS" --output-s3
else
    echo "  SKIPPED in dry-run mode"
fi

# ── Step 8: Persist today's scope as tomorrow's churn baseline ─────────────
echo ""
echo "==> Step 8/8: Persist scope state..."
if [ -z "$DRY_RUN" ]; then
    $PYTHON_BIN -c "
from rag.pipelines._corpus_scope_state import write_scope_state
import json
current = set(json.load(open('$SCOPE_STATE_FILE'))['current'])
write_scope_state(current)
"
else
    echo "  SKIPPED in dry-run mode (scope state pointer not advanced)"
fi

echo ""
echo "========================================"
echo "RAG Daily Corpus Delta Complete — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

aws cloudwatch put-metric-data \
  --namespace "AlphaEngine" \
  --metric-name "Heartbeat" \
  --dimensions "Process=rag-daily-corpus-delta" \
  --value 1 --unit "Count" \
  --region "${AWS_REGION:-us-east-1}"
echo "Heartbeat emitted: rag-daily-corpus-delta"
