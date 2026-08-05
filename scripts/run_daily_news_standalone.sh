#!/usr/bin/env bash
# Standalone daily-news collector runner for the always-on dashboard box.
#
# Mirrors the weekday SF's RunDailyNews command, but adapted to run OUTSIDE
# the trading Step Function on the shared dashboard EC2 box, using a slim,
# news-focused venv (requirements-daily-news.txt) so it doesn't need the full
# data stack (arcticdb/edgartools/etc.). Triggered by daily-news.timer at
# 04:00 PT — ahead of the 05:00 morning-signal run that consumes the
# data/news_*_daily/ artifact. Also still consumed by the dashboard
# "Daily News" console page.
#
# Best-effort refresh (git pull + slim pip) so a merged change is live on the
# next run, mirroring morning-signal's refresh-on-run; a transient git/pip blip
# must never block the pull (the collector is itself fail-soft per source).
set -uo pipefail

REPO=/home/ec2-user/alpha-engine-data
CONFIG_REPO=/home/ec2-user/alpha-engine-config
LOG=/home/ec2-user/daily-news.log   # user-writable (service runs as ec2-user)

export FLOW_DOCTOR_ENABLED=1
export ALPHA_ENGINE_DEPLOYED=1

cd "$REPO"

# Upload the run log to S3 on exit for observability (best-effort, never fatal).
trap 'aws s3 cp "$LOG" "s3://alpha-engine-research/_ssm_logs/daily-news-standalone/$(date -u +%Y-%m-%d)/$(hostname)-$(date -u +%H%M%SZ).log" --only-show-errors 2>/dev/null || true' EXIT

# ── Slim venv self-heal (alpha-engine-config#6063) ──────────────────────────
# The box's .venv was created with AL2023's default python3.9. The slim
# requirements pin boto3>=1.43.59 (Python 3.10+) and numpy>=2.4.6 (Python
# 3.11+), so `pip install` failed on EVERY run there and the venv could never
# advance — the RAG-ingest deps config-I5702 added therefore never landed and
# every standalone run silently skipped corpus warming. The only other .venv
# consumer on the box, systemd-unit-drift-check.service, runs a stdlib-only
# script (infrastructure/systemd/check-systemd-unit-drift.py) — 3.12-safe.
# Rebuild on python3.12 (the fleet standard; the interpreter
# install-metron-intraday.sh provisions on this same box) when missing or
# stale. Idempotent: a healthy >=3.11 venv is left untouched.
ensure_venv() {
    local venv_py="$REPO/.venv/bin/python"
    if [ ! -x "$venv_py" ] || ! "$venv_py" -c 'import sys; exit(0 if sys.version_info >= (3, 11) else 1)'; then
        echo "provisioning $REPO/.venv (python3.12)…"
        command -v python3.12 >/dev/null || {
            echo "FATAL: python3.12 not found. requirements-daily-news.txt pins" >&2
            echo "boto3>=1.43.59 (Python 3.10+) and numpy>=2.4.6 (Python 3.11+);" >&2
            echo "the box's 3.9 venv cannot be repaired by older wheels, it must" >&2
            echo "be rebuilt on python3.12. Install it before re-running." >&2
            exit 1
        }
        # .venv is gitignored and fully reproducible from the requirements
        # file below — a stale-interpreter rebuild is safe to discard.
        rm -rf "$REPO/.venv"
        python3.12 -m venv "$REPO/.venv"
    fi
}

{
  echo "=== daily-news standalone run $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  # Refresh code + slim deps (fail-loud on checkout divergence, pip install failure).
  # A diverged checkout makes ff-only fail forever with zero signal — must not
  # silently continue. Use reset --hard after fetch to align with origin/main.
  git fetch origin
  git reset --hard origin/main

  git -C "$CONFIG_REPO" fetch origin
  git -C "$CONFIG_REPO" reset --hard origin/main

  ensure_venv
  .venv/bin/pip install -q -r requirements-daily-news.txt

  # Verify HEAD matches origin/main (divergence guard)
  if ! git diff --quiet HEAD origin/main; then
    echo "ERROR: post-reset HEAD does not match origin/main — checkout divergence detected" >&2
    exit 1
  fi
} > "$LOG" 2>&1

# Load runtime env (Polygon key, AWS creds, etc.). bash `source` handles both
# `export K=V` and bare `K=V` lines.
set -a
# shellcheck disable=SC1091
source /home/ec2-user/.alpha-engine.env 2>/dev/null || true
set +a

# ── RAG corpus-ingest secrets (alpha-engine-config#6063) ────────────────────
# RAG_DATABASE_URL + VOYAGE_API_KEY come from SSM Parameter Store, NOT the
# SCP'd .env — the Phase-2 SSM migration spot_data_weekly.sh documents
# (2026-04-17 SF failure: RAG_DATABASE_URL truncated at an unquoted & in the
# .env; SSM stores the value as an opaque string, no shell-parse fragility).
# The dashboard role's tracked alpha-engine-ssm-read.json grants
# ssm:GetParameter on /alpha-engine/*. Fail-loud: the RAG ingest is a designed
# part of this run (config-I5702) — a missing secret is an environment error
# that fails EVERY run, and the silent-degradation variant is exactly the bug
# this fix closes (the ingest itself stays fail-soft in daily_news.py for
# service-level failures: pgvector down, Voyage throttled).
for name in RAG_DATABASE_URL VOYAGE_API_KEY; do
    val=$(aws ssm get-parameter --name "/alpha-engine/$name" --with-decryption --query 'Parameter.Value' --output text --region "${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/$name from SSM — required for RAG corpus ingest (alpha-engine-config#6063)" >> "$LOG"
        exit 1
    fi
    export "$name=$val"
    unset val
done
echo "RAG secrets fetched: RAG_DATABASE_URL, VOYAGE_API_KEY" >> "$LOG"

# ── Preflight: prove the venv can import the run's real closure BEFORE
# running (mirrors install-metron-intraday.sh — the 2026-07-29 incident class:
# a missing import degrades silently for days). The RAG chain's lazy imports
# sit inside daily_news.py's fail-soft try/except, so a missing dependency
# would otherwise be swallowed into rag_status=error with no page.
.venv/bin/python -c 'import collectors.daily_news; from rag.pipelines._watermarks import WatermarkStore; from rag.pipelines.ingest_news import ingest_articles; from rag.pipelines.run_news_pipeline import NEWS_DOC_TYPE, NEWS_SOURCE, _load_ticker_sector_map' >> "$LOG" 2>&1 || {
    echo "ERROR: preflight import failed — the slim venv is missing a dependency of the daily-news chain (see traceback above)" >> "$LOG"
    exit 1
}

# --require-digest: this box runner feeds the morning-signal podcast, whose
# consumer treats the digest as a hard prerequisite. Exit non-zero if the
# digest failed/empty so this service fails and morning-signal's Requires=
# blocks the pod, rather than letting a soft-failed digest feed a degraded
# episode. (The weekday SF invokes daily_news WITHOUT this flag — digest stays
# fail-soft there.) The aggregate + article artifacts the dashboard reads
# already wrote before the digest step, so they're unaffected by this exit.
.venv/bin/python -m collectors.daily_news --require-digest >> "$LOG" 2>&1
rc=$?
echo "=== daily-news standalone exit rc=$rc $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" >> "$LOG"
exit $rc
