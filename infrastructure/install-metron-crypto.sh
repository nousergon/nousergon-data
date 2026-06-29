#!/usr/bin/env bash
# Install/refresh the Metron crypto wallet-balance timer on the trading box (metron-ops#111).
# One-time (and after unit-file edits) via SSM:
#   aws ssm send-command --instance-ids <trading-box> --document-name AWS-RunShellScript \
#     --parameters 'commands=["sudo bash /home/ec2-user/alpha-engine-data/infrastructure/install-metron-crypto.sh"]'
# Idempotent: re-copies units, daemon-reloads, enables + starts the timer.
# NOTE: the instance role must allow s3:PutObject on crypto/* of the shared bucket (in
# addition to market_data/*) before this writes — see metron-ops#111.
set -euo pipefail

REPO_DIR="/home/ec2-user/alpha-engine-data"
UNIT_DIR="${REPO_DIR}/infrastructure/systemd"

cp "${UNIT_DIR}/metron-crypto.service" /etc/systemd/system/metron-crypto.service
cp "${UNIT_DIR}/metron-crypto.timer" /etc/systemd/system/metron-crypto.timer
systemctl daemon-reload
systemctl enable --now metron-crypto.timer
systemctl list-timers metron-crypto.timer --no-pager
echo "metron-crypto.timer installed and started"
