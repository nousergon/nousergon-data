#!/usr/bin/env bash
# Standalone daily-news collector runner for the always-on dashboard box.
#
# Mirrors the weekday SF's RunDailyNews command, but adapted to run OUTSIDE
# the trading Step Function on the shared dashboard EC2 box, using a slim,
# news-only venv (requirements-daily-news.txt) so it doesn't need the full
# data stack (arcticdb/voyageai/etc.). Triggered by daily-news.timer at
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

{
  echo "=== daily-news standalone run $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  # Refresh code + slim deps (best-effort).
  git pull --ff-only origin main || true
  git -C "$CONFIG_REPO" pull --ff-only origin main || true
  .venv/bin/pip install -q -r requirements-daily-news.txt || true
} > "$LOG" 2>&1

# Load runtime env (Polygon key, AWS creds, etc.). bash `source` handles both
# `export K=V` and bare `K=V` lines.
set -a
# shellcheck disable=SC1091
source /home/ec2-user/.alpha-engine.env 2>/dev/null || true
set +a

.venv/bin/python -m collectors.daily_news >> "$LOG" 2>&1
rc=$?
echo "=== daily-news standalone exit rc=$rc $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" >> "$LOG"
exit $rc
