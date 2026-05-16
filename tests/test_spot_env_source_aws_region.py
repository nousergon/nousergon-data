"""Pins the spot dispatcher scripts to export AWS_REGION into the spot shell.

PR 9f (#241) removed the `.env` sourcing from the spot bootstrap in favor
of runtime get_secret() SSM lookups. That correctly handled *secrets*, but
the same `.env` was also the only thing exporting AWS_REGION — a plain env
var (not a secret) that:

  - alpha_engine_lib.preflight.check_env_vars() hard-requires, and
  - boto3 needs as a default region with no .env / ~/.aws/config present.

Result: 2026-05-16 Saturday SF DataPhase1 aborted at preflight with
"required env vars missing: ['AWS_REGION']". This test catches that
shim-deletion launch-mechanism regression class: any future edit to the
ENV_SOURCE injected into the remote heredocs must keep the region exports.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = [
    _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh",
    _REPO_ROOT / "infrastructure" / "spot_drift_detection.sh",
]


@pytest.mark.parametrize("script", _SCRIPTS, ids=lambda p: p.name)
def test_env_source_exports_region(script: Path):
    text = script.read_text()
    # The single ENV_SOURCE assignment that gets interpolated into every
    # remote `run_remote bash -s <<HEREDOC` workload.
    m = re.search(r'^ENV_SOURCE=.*$', text, re.MULTILINE)
    assert m, f"{script.name}: no ENV_SOURCE assignment found"
    env_source = m.group(0)
    assert "export AWS_REGION=" in env_source, (
        f"{script.name}: ENV_SOURCE must export AWS_REGION — without it the "
        "spot shell has no region (no .env post-#241) and lib preflight / "
        "boto3 fail. See 2026-05-16 DataPhase1 failure."
    )
    assert "export AWS_DEFAULT_REGION=" in env_source, (
        f"{script.name}: ENV_SOURCE must also export AWS_DEFAULT_REGION for "
        "boto3 clients that read the default-region var."
    )
