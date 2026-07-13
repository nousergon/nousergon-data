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
  # Refresh code + slim deps (fail-loud on checkout divergence, pip install failure).
  # A diverged checkout makes ff-only fail forever with zero signal — must not
  # silently continue. Use reset --hard after fetch to align with origin/main.
  git fetch origin
  git reset --hard origin/main

  git -C "$CONFIG_REPO" fetch origin
  git -C "$CONFIG_REPO" reset --hard origin/main

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
