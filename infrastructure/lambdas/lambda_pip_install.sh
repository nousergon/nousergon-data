#!/usr/bin/env bash
# Install requirements.txt into TARGET for AWS Lambda python3.12 / linux/amd64.
#
# Operator-deployed Lambdas must not use bare `pip install -t` on macOS — binary
# wheels (pydantic_core, etc.) target the host arch and fail at import on Lambda
# with: No module named 'pydantic_core._pydantic_core'
#
# The container runs as root (needed for apt-get to install git), so every file
# pip writes into the bind-mounted TARGET lands root-owned on the host. On macOS
# Docker Desktop transparently maps that back to the invoking user, but on a
# Linux CI runner the bind mount preserves the container UID (0), leaving the
# non-root `runner` user unable to delete them — so the caller's `trap rm -rf`
# cleanup fails with "Permission denied" and reds the deploy step AFTER the live
# Lambda update already succeeded (introduced by the #621 Docker-pip cutover;
# surfaced by the groom-liveness-probe Deploy red, run 28719203767, 2026-07-04).
# Fix at this shared chokepoint: chown TARGET back to the host UID/GID inside the
# container (last step, still as root) so every caller's cleanup works on both
# host OSes.
#
# Usage:
#   bash infrastructure/lambdas/lambda_pip_install.sh /path/to/pkg /path/to/requirements.txt

set -euo pipefail

TARGET="${1:?target dir required}"
REQ_FILE="${2:?requirements.txt required}"

if [[ ! -f "${REQ_FILE}" ]]; then
  echo "ERROR: requirements file not found: ${REQ_FILE}" >&2
  exit 1
fi

mkdir -p "${TARGET}"

HOST_UID="$(id -u)"
HOST_GID="$(id -g)"

echo "Installing Lambda deps via Docker (python3.12 / linux/amd64) into ${TARGET}..."
docker run --rm --platform linux/amd64 \
  --entrypoint bash \
  -e HOST_UID="${HOST_UID}" -e HOST_GID="${HOST_GID}" \
  -v "${TARGET}:/out" \
  -v "${REQ_FILE}:/tmp/requirements.txt:ro" \
  python:3.12-slim \
  -c 'apt-get update -qq && apt-get install -qq -y git >/dev/null && pip install --quiet --target /out --upgrade -r /tmp/requirements.txt && chown -R "${HOST_UID}:${HOST_GID}" /out'
