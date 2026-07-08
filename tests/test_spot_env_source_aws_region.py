"""Pins the spot dispatcher scripts to export AWS_REGION into the spot shell.

PR 9f (#241) removed the `.env` sourcing from the spot bootstrap in favor
of runtime get_secret() SSM lookups. That correctly handled *secrets*, but
the same `.env` was also the only thing exporting AWS_REGION — a plain env
var (not a secret) that:

  - nousergon_lib.preflight.check_env_vars() hard-requires, and
  - boto3 needs as a default region with no .env / ~/.aws/config present.

Result: 2026-05-16 Saturday SF DataPhase1 aborted at preflight with
"required env vars missing: ['AWS_REGION']". This test catches that
shim-deletion launch-mechanism regression class: any future edit to the
ENV_SOURCE injected into the remote heredocs must keep the region exports.

2026-05-27 SSH→SSM migration (ROADMAP L342 PR 2): the data-weekly script
moved from a single-line ``ENV_SOURCE="export ...; ..."`` shape to a
multi-line ``read -r -d '' ENV_SOURCE <<'ENV_EOF' ... ENV_EOF`` block.
This test now accepts either shape — the invariant is that the value
of ``ENV_SOURCE`` (however it gets assigned) exports both AWS_REGION
and AWS_DEFAULT_REGION when injected into the per-SSM-step shell.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = [
    _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh",
]


def _extract_env_source_body(text: str) -> str | None:
    """Return the body content of ENV_SOURCE regardless of assignment shape.

    Two supported shapes:
      1. Single-line: ``ENV_SOURCE="export X=...; export Y=...;"``
      2. Multi-line heredoc:
           ``read -r -d '' ENV_SOURCE <<'ENV_EOF'`` ...lines... ``ENV_EOF``

    The SSH→SSM migration (ROADMAP L342 PR 2) introduced the multi-line
    heredoc shape because the new ``run_ssm "<desc>" <timeout> <<HEREDOC``
    pattern reads from stdin, and a multi-line ``${ENV_SOURCE}`` expands
    cleanly into the body.
    """
    # Shape 2: ``read -r -d '' ENV_SOURCE <<'ENV_EOF' [|| true]\n...\nENV_EOF``
    # Note: shells idiomatically chain ``|| true`` after the read because
    # `read -d ''` returns nonzero when it hits EOF before the delimiter,
    # which is the expected path here.
    m = re.search(
        r"read\s+-r\s+-d\s+''\s+ENV_SOURCE\s*<<'?(\w+)'?[^\n]*\n(.*?)\n\1\b",
        text,
        re.DOTALL,
    )
    if m:
        return m.group(2)
    # Shape 1: ``ENV_SOURCE="..."`` (single-line)
    m = re.search(r'^ENV_SOURCE=.*$', text, re.MULTILINE)
    if m:
        return m.group(0)
    return None


@pytest.mark.parametrize("script", _SCRIPTS, ids=lambda p: p.name)
def test_env_source_exports_region(script: Path):
    text = script.read_text()
    env_source = _extract_env_source_body(text)
    assert env_source is not None, (
        f"{script.name}: no ENV_SOURCE assignment found (looked for both "
        f"single-line `ENV_SOURCE=\"...\"` and multi-line "
        f"`read -r -d '' ENV_SOURCE <<ENV_EOF ... ENV_EOF` shapes)"
    )
    assert "export AWS_REGION=" in env_source, (
        f"{script.name}: ENV_SOURCE must export AWS_REGION — without it the "
        "spot shell has no region (no .env post-#241) and lib preflight / "
        "boto3 fail. See 2026-05-16 DataPhase1 failure."
    )
    assert "export AWS_DEFAULT_REGION=" in env_source, (
        f"{script.name}: ENV_SOURCE must also export AWS_DEFAULT_REGION for "
        "boto3 clients that read the default-region var."
    )
