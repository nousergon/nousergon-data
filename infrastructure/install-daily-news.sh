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
# handles the SEPARATE concerns of the unit FILES landing in
# /etc/systemd/system/ and the venv the unit's runner invokes.
set -euo pipefail

REPO_DIR="/home/ec2-user/alpha-engine-data"
UNIT_DIR="${REPO_DIR}/infrastructure/systemd"
VENV_DIR="${REPO_DIR}/.venv"
VENV_PY="${VENV_DIR}/bin/python"

# ── Slim venv provisioning (alpha-engine-config#6063) ───────────────────────
# Mirrors install-metron-intraday.sh: the shared .venv was created with
# AL2023's default python3.9, but requirements-daily-news.txt pins
# boto3>=1.43.59 (Python 3.10+) and numpy>=2.4.6 (Python 3.11+), so pip
# install failed on every run and the venv could never advance — the
# RAG-ingest deps config-I5702 added never landed. Rebuild on python3.12
# (the interpreter metron-intraday provisions on this same box) when missing
# or stale. The unit's runner (scripts/run_daily_news_standalone.sh)
# self-heals the venv identically on every run, so this is install-time
# readiness, not the load-bearing path. The only other .venv consumer on the
# box, systemd-unit-drift-check.service, runs a stdlib-only script
# (infrastructure/systemd/check-systemd-unit-drift.py) — 3.12-safe.
if [ ! -x "$VENV_PY" ] || ! "$VENV_PY" -c 'import sys; exit(0 if sys.version_info >= (3, 11) else 1)'; then
    echo "provisioning ${VENV_DIR} (python3.12)…"
    command -v python3.12 >/dev/null || {
        echo "FATAL: python3.12 not found. requirements-daily-news.txt pins boto3>=1.43.59," >&2
        echo "which needs Python 3.10+, and numpy>=2.4.6 which needs Python 3.11+. The" >&2
        echo "3.9 venv cannot be repaired by older wheels — install python3.12 before" >&2
        echo "running this (alpha-engine-config#6063)." >&2
        exit 1
    }
    # .venv is gitignored and fully reproducible from requirements-daily-news.txt.
    rm -rf "$VENV_DIR"
    sudo -u ec2-user python3.12 -m venv "$VENV_DIR"
fi
sudo -u ec2-user "${VENV_DIR}/bin/pip" install -q --upgrade pip
sudo -u ec2-user "${VENV_DIR}/bin/pip" install -q -r "${REPO_DIR}/requirements-daily-news.txt"

# ── PREFLIGHT: prove the venv can import the run's real closure BEFORE
# enabling the timer (mirrors install-metron-intraday.sh — the 2026-07-29
# incident class: an enable-time claim made without testing moves the failure
# from install time, where a human is watching, to run time, where it is a
# silent background job). The RAG chain's lazy imports sit inside
# daily_news.py's fail-soft try/except, so a missing dependency would
# otherwise be swallowed into rag_status=error with no page.
if ! sudo -u ec2-user "$VENV_PY" -c 'import collectors.daily_news; from rag.pipelines._watermarks import WatermarkStore; from rag.pipelines.ingest_news import ingest_articles; from rag.pipelines.run_news_pipeline import NEWS_DOC_TYPE, NEWS_SOURCE, _load_ticker_sector_map'; then
    echo "FATAL: preflight import failed — the daily-news venv cannot import the run's dependency closure (alpha-engine-config#6063). See the traceback above." >&2
    exit 1
fi

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
