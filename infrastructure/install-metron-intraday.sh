#!/usr/bin/env bash
# Install/refresh the Metron intraday-quotes timer on the trading box (config#1023).
# One-time (and after unit-file edits) via SSM:
#   aws ssm send-command --instance-ids <trading-box> --document-name AWS-RunShellScript \
#     --parameters 'commands=["sudo bash /home/ec2-user/alpha-engine-data/infrastructure/install-metron-intraday.sh"]'
# Idempotent: re-copies units, daemon-reloads, enables + starts the timer.
set -euo pipefail

REPO_DIR="/home/ec2-user/alpha-engine-data"
UNIT_DIR="${REPO_DIR}/infrastructure/systemd"

# ── Provision the interpreter the unit names ────────────────────────────────
#
# The venv is DERIVED FROM THE UNIT FILE, never hardcoded here. Two files naming
# an interpreter independently is two places to be wrong, and the failure mode is
# silent: the installer would happily verify a venv the unit does not use. Parse
# ExecStart instead, so the thing tested is by construction the thing that runs.
PY=$(sed -n 's|^ExecStart=\([^ ]*\).*|\1|p' "${UNIT_DIR}/metron-intraday.service")
if [ -z "$PY" ]; then
    echo "FATAL: could not read an ExecStart interpreter from ${UNIT_DIR}/metron-intraday.service" >&2
    exit 1
fi
VENV_DIR="${PY%/bin/python}"

# `.venv-intraday`, not the shared `.venv`: the shared one is Python 3.9,
# provisioned for daily-news, and requirements.txt pins numpy>=2.4.6 (Python
# 3.11+). Rebuilding the shared venv would change the interpreter daily-news runs
# on; a dedicated one has no blast radius. See the unit file's header.
if [ ! -x "$PY" ]; then
    echo "provisioning ${VENV_DIR} (python3.12)…"
    command -v python3.12 >/dev/null || {
        echo "FATAL: python3.12 not found. requirements.txt pins numpy>=2.4.6," >&2
        echo "which needs Python 3.11+. Install it before running this." >&2
        exit 1
    }
    sudo -u ec2-user python3.12 -m venv "$VENV_DIR"
fi
sudo -u ec2-user "${VENV_DIR}/bin/pip" install -q --upgrade pip
sudo -u ec2-user "${VENV_DIR}/bin/pip" install -q -r "${REPO_DIR}/requirements.txt"

# ── PREFLIGHT: prove the interpreter can run the job BEFORE enabling it ──────
#
# This script used to copy units and `systemctl enable --now` without ever
# checking that the venv named in the unit's ExecStart could import what the
# collector needs. On 2026-07-29 that produced a timer that fired every five
# minutes for hours with zero successful runs: the box's venv had boto3 and
# feedparser (it was built for daily-news) but no yfinance and no pandas, so
# every fetch helper took its `except ImportError -> return {}` branch and the
# collector wrote an EMPTY artifact over a good one while reporting success.
#
# Enabling a unit is a claim that it works. An installer that makes that claim
# without testing it moves the failure from install time -- where a human is
# watching and the fix is obvious -- to run time, where it is a silent
# background job. Fail here instead, loudly, with the remedy in the message.
#
# It runs AFTER the provisioning above rather than instead of it: the install is
# what makes the claim true, this is what proves it.
if ! "$PY" - <<'PREFLIGHT'
import importlib.util, sys
# The collector's HARD dependencies. Absent, it degrades to empty results
# rather than crashing, which is why their absence has to be caught here.
required = ("boto3", "pandas", "yfinance")
missing = [m for m in required if importlib.util.find_spec(m) is None]
if missing:
    print("missing modules: " + ", ".join(missing), file=sys.stderr)
    sys.exit(1)
PREFLIGHT
then
    echo "FATAL: ${PY} cannot import the collector's required modules even after" >&2
    echo "installing requirements.txt into ${VENV_DIR}." >&2
    echo "" >&2
    echo "NOTE: requirements.txt pins numpy>=2.4.6, which needs Python 3.11+ --" >&2
    echo "a 3.9 venv cannot satisfy it and must be rebuilt on a newer" >&2
    echo "interpreter rather than patched with older wheels." >&2
    echo "" >&2
    echo "Refusing to enable a timer whose job provably cannot produce output." >&2
    exit 1
fi
echo "preflight ok: ${PY} ($("$PY" -V 2>&1)) can import boto3, pandas, yfinance"

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
