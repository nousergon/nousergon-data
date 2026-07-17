#!/usr/bin/env bash
# Install/refresh the daily-news systemd units on the always-on dashboard box
# (config#2352). Mirrors install-metron-intraday.sh's shape exactly.
#
# One-time (and after unit-file edits) via SSM:
#   aws ssm send-command --instance-ids i-09b539c844515d549 \
#     --document-name AWS-RunShellScript \
#     --parameters 'commands=["sudo bash /home/ec2-user/alpha-engine-data/infrastructure/install-daily-news.sh"]'
# Idempotent: re-copies units, daemon-reloads, enables + starts the timer.
#
# daily-news.service itself does its own code refresh on every run (`git
# reset --hard` in scripts/run_daily_news_standalone.sh) — this script only
# handles the SEPARATE concern of the unit FILES landing in
# /etc/systemd/system/, which a plain code pull never touches.
set -euo pipefail

REPO_DIR="/home/ec2-user/alpha-engine-data"
UNIT_DIR="${REPO_DIR}/infrastructure/systemd"

cp "${UNIT_DIR}/daily-news.service" /etc/systemd/system/daily-news.service
cp "${UNIT_DIR}/daily-news.timer" /etc/systemd/system/daily-news.timer
systemctl daemon-reload
systemctl enable --now daily-news.timer
systemctl list-timers daily-news.timer --no-pager
echo "daily-news.timer installed and started"

# Installed-vs-repo drift probe (config#2352) — same install call also
# provisions the daily self-check so a future on-box unit edit (bypassing
# this script) pages within a day. Idempotent, same pattern as above.
cp "${UNIT_DIR}/systemd-unit-drift-check.service" /etc/systemd/system/systemd-unit-drift-check.service
cp "${UNIT_DIR}/systemd-unit-drift-check.timer" /etc/systemd/system/systemd-unit-drift-check.timer
systemctl daemon-reload
systemctl enable --now systemd-unit-drift-check.timer
echo "systemd-unit-drift-check.timer installed and started"
