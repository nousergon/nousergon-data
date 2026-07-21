#!/usr/bin/env bash
# Install/refresh the Metron intraday-quotes timer (config#1023).
#
# Host: ae-dashboard as of alpha-engine-config#1768 Phase 1 (2026-07-21) —
# moved off ae-trading (duplicated the intraday-price-alerts Lambda's work
# there; ae-dashboard is always-on, same box daily-news already runs on).
# This script is host-agnostic (no box-specific logic below), kept as the
# manual/emergency install path; the normal path is boot-pull.sh's own
# convergence on ae-dashboard (see that repo's infrastructure/boot-pull.sh).
# One-time (and after unit-file edits) via SSM:
#   aws ssm send-command --instance-ids <ae-dashboard-instance-id> --document-name AWS-RunShellScript \
#     --parameters 'commands=["sudo bash /home/ec2-user/alpha-engine-data/infrastructure/install-metron-intraday.sh"]'
# Idempotent: re-copies units, daemon-reloads, enables + starts the timer.
set -euo pipefail

REPO_DIR="/home/ec2-user/alpha-engine-data"
UNIT_DIR="${REPO_DIR}/infrastructure/systemd"

cp "${UNIT_DIR}/metron-intraday.service" /etc/systemd/system/metron-intraday.service
cp "${UNIT_DIR}/metron-intraday.timer" /etc/systemd/system/metron-intraday.timer
systemctl daemon-reload
systemctl enable --now metron-intraday.timer
systemctl list-timers metron-intraday.timer --no-pager
echo "metron-intraday.timer installed and started"

# Installed-vs-repo drift probe (config#2352) — same install call also
# provisions the daily self-check so a future on-box unit edit (bypassing
# this script) pages within a day. Idempotent, same pattern as above.
cp "${UNIT_DIR}/systemd-unit-drift-check.service" /etc/systemd/system/systemd-unit-drift-check.service
cp "${UNIT_DIR}/systemd-unit-drift-check.timer" /etc/systemd/system/systemd-unit-drift-check.timer
systemctl daemon-reload
systemctl enable --now systemd-unit-drift-check.timer
echo "systemd-unit-drift-check.timer installed and started"
