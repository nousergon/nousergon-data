#!/usr/bin/env bash
# Install/refresh the Metron intraday-quotes timer on the trading box (config#1023).
# One-time (and after unit-file edits) via SSM:
#   aws ssm send-command --instance-ids <trading-box> --document-name AWS-RunShellScript \
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
