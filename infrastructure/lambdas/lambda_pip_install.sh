#!/usr/bin/env bash
# Install requirements.txt into TARGET for AWS Lambda python3.12 / linux/amd64.
#
# Operator-deployed Lambdas must not use bare `pip install -t` on macOS — binary
# wheels (pydantic_core, etc.) target the host arch and fail at import on Lambda
# with: No module named 'pydantic_core._pydantic_core'
#
# The container runs as root (apt-get install git needs it), so pip writes
# root-owned files into the bind-mounted TARGET. On macOS Docker Desktop that is
# invisible (the VFS remaps bind mounts to the host user), but on a Linux CI
# runner the files stay uid 0 and the CALLER's non-root cleanup (`rm -rf` on the
# scratch dir) then fails with EPERM — a red deploy AFTER the Lambda already
# updated (2026-07-04, this bug's first CI exposure; it was masked on operator
# laptops for the same reason the pytest/nousergon_lib gate gaps were). So chown
# the tree back to the host uid/gid before the container exits.
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
  -c "apt-get update -qq && apt-get install -qq -y git >/dev/null && pip install --quiet --target /out --upgrade -r /tmp/requirements.txt && chown -R \"\${HOST_UID}:\${HOST_GID}\" /out"
