#!/usr/bin/env bash
# Self-heal venv <-> requirements `alpha-engine-lib` pin drift before an
# entrypoint runs.
#
# Closes the code-vs-installed-lib deploy-skew bug class (the 2026-06-10
# weekday-SF MorningEnrich failure): an SSM entrypoint `git pull`s new code that
# imports a NEW lib symbol AND bumps the pin in the SAME commit (data #385 added
# `from alpha_engine_lib.logging import guard_entrypoint` + bumped the pin to
# v0.58.0), but the box venv is never reinstalled between the pull and the run --
# so the new symbol resolves against the STALE installed lib -> ImportError ->
# hard pipeline fail. The box auto-pulls code on a different cadence than it
# reinstalls the lib, and nothing closed that window.
#
# This is the reconcile step that belongs BETWEEN `git pull` and the entrypoint.
# It is deliberately a REPO script, NOT an `alpha_engine_lib` CLI like
# `ssm_log_capture`: the whole failure mode is "the installed lib is stale," so
# the heal mechanism must not live in the lib -- a lib-resident form would be
# absent on the very box it needs to fix (a venv old enough to lack the new
# symbol is also old enough to lack the heal module). A repo script arrives with
# the same `git pull` that brings the new pin, so it is never stale. (This is the
# documented deviation from the CLAUDE.md "lift cross-repo primitives into
# alpha-engine-lib" sub-rule: lib-residence has a bootstrapping hole here.)
#
# Idempotent: a cheap version compare on every run; pip only on the rare
# post-bump run. Fail-loud: exits non-zero if it cannot reconcile (better than
# silently running against a broken lib -- per the no-silent-fails rule).
#
# Usage:  ensure_lib_pin.sh <repo_dir> [requirements_file]
#   Call AFTER `source .venv/bin/activate` so `python`/`pip` resolve to the venv.
set -euo pipefail

repo_dir="${1:?ensure_lib_pin: <repo_dir> required}"
req_file="${2:-${repo_dir}/requirements.txt}"

if [ ! -f "$req_file" ]; then
  echo "ensure_lib_pin: $req_file not found -- skipping" >&2
  exit 0
fi

libspec="$(grep -E '^alpha-engine-lib' "$req_file" | head -1 || true)"
if [ -z "$libspec" ]; then
  echo "ensure_lib_pin: no alpha-engine-lib pin in $req_file -- skipping" >&2
  exit 0
fi

pinned="$(printf '%s' "$libspec" | grep -oE '@v[0-9]+\.[0-9]+\.[0-9]+' | head -1 | sed 's/^@v//')"
if [ -z "$pinned" ]; then
  # `@main` and other non-version refs are forbidden by TestLibVersionPin; if we
  # somehow cannot parse a vX.Y.Z pin, do not block the run -- just warn.
  echo "ensure_lib_pin: could not parse a vX.Y.Z pin from '$libspec' -- skipping" >&2
  exit 0
fi

installed="$(python -c 'import alpha_engine_lib as _l; print(_l.__version__)' 2>/dev/null || echo none)"

if [ "$installed" = "$pinned" ]; then
  echo "ensure_lib_pin: in sync (alpha-engine-lib v$installed)"
  exit 0
fi

echo "ensure_lib_pin: drift -- installed=$installed pinned=v$pinned -- reinstalling '$libspec'"
pip install --quiet "$libspec"

healed="$(python -c 'import alpha_engine_lib as _l; print(_l.__version__)' 2>/dev/null || echo none)"
if [ "$healed" != "$pinned" ]; then
  echo "ensure_lib_pin: FAILED to reconcile -- installed=$healed still != pinned=v$pinned" >&2
  exit 1
fi
echo "ensure_lib_pin: healed -> alpha-engine-lib v$healed"
