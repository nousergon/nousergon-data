#!/usr/bin/env bash
# Install requirements.txt into TARGET for AWS Lambda python3.12 / linux/amd64.
#
# Operator-deployed Lambdas must not use bare `pip install -t` on macOS — binary
# wheels (pydantic_core, etc.) target the host arch and fail at import on Lambda
# with: No module named 'pydantic_core._pydantic_core'
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

echo "Installing Lambda deps via Docker (python3.12 / linux/amd64) into ${TARGET}..."
docker run --rm --platform linux/amd64 \
  --entrypoint bash \
  -v "${TARGET}:/out" \
  -v "${REQ_FILE}:/tmp/requirements.txt:ro" \
  python:3.12-slim \
  -c "apt-get update -qq && apt-get install -qq -y git >/dev/null && pip install --quiet --target /out --upgrade -r /tmp/requirements.txt"
