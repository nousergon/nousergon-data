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

2026-05-27 SSH→SSM migration (ROADMAP L342 PR 2): the data-weekly script
moved from a single-line ``ENV_SOURCE="export ...; ..."`` shape to a
multi-line ``read -r -d '' ENV_SOURCE <<'ENV_EOF' ... ENV_EOF`` block.
This test now accepts either shape — the invariant is that the value
of ``ENV_SOURCE`` (however it gets assigned) exports both AWS_REGION
and AWS_DEFAULT_REGION when injected into the per-SSM-step shell.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = [
    _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh",
]

# Step Function definitions whose per-step SSM command blocks formerly sourced
# /home/ec2-user/.alpha-engine.env. The .env-deprecation arc (#890) removed
# that source line; this is the same launch-mechanism regression class the
# spot scripts above guard against — the .env was also the only thing
# exporting AWS_REGION / AWS_DEFAULT_REGION, which lib preflight + boto3
# hard-require in the otherwise-minimal SSM shell.
_STEP_FUNCTIONS = [
    _REPO_ROOT / "infrastructure" / "step_function_daily.json",
    _REPO_ROOT / "infrastructure" / "step_function_eod.json",
]


def _split_states_array(expr: str) -> list[str]:
    """Split a ``States.Array('...', '...', States.Format(...))`` JSONPath
    intrinsic into its top-level single-quoted string arguments.

    The EOD SF assembles its SSM commands with ``"commands.$": "States.Array(
    ...)"`` instead of a plain ``"commands"`` list — a shape the original
    version of this guard silently skipped, which is exactly the liveness-
    proxy blind spot class: two of the three remaining .env sources lived in
    those blocks and the guard read as green. Nested intrinsics
    (``States.Format('...', $.x)``) contribute their literal text too, which
    is fine for a substring invariant.
    """
    return re.findall(r"'((?:[^'\\]|\\.)*)'", expr)


def _iter_command_blocks(node):
    """Yield every SSM command block in a SF definition, as a list of strings.

    Covers BOTH shapes: a plain ``"commands"`` list, and the JSONPath
    intrinsic ``"commands.$": "States.Array(...)"`` string.
    """
    if isinstance(node, dict):
        cmds = node.get("commands")
        if isinstance(cmds, list) and all(isinstance(c, str) for c in cmds):
            yield cmds
        dynamic = node.get("commands.$")
        if isinstance(dynamic, str):
            yield _split_states_array(dynamic)
        for v in node.values():
            yield from _iter_command_blocks(v)
    elif isinstance(node, list):
        for v in node:
            yield from _iter_command_blocks(v)


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


@pytest.mark.parametrize("sf", _STEP_FUNCTIONS, ids=lambda p: p.name)
def test_step_function_does_not_source_dotenv(sf: Path):
    """#890: no SSM command block may source the deprecated .env file."""
    definition = json.loads(sf.read_text())
    offenders = [
        (i, c)
        for block in _iter_command_blocks(definition)
        for i, c in enumerate(block)
        if "alpha-engine.env" in c or re.search(r"set -a\b.*\bsource\b", c)
    ]
    assert not offenders, (
        f"{sf.name}: SSM command blocks must not source .env after #890 — "
        "secrets come from get_secret()/SSM at runtime. Offending command(s):\n"
        + "\n".join(f"  {c!r}" for _, c in offenders)
    )


@pytest.mark.parametrize("sf", _STEP_FUNCTIONS, ids=lambda p: p.name)
def test_step_function_blocks_export_region(sf: Path):
    """#890: every SSM command block that runs the python pipeline must export
    AWS_REGION and AWS_DEFAULT_REGION itself, since the .env that used to
    provide them was removed. Guards the same 2026-05-16 DataPhase1 preflight
    regression class as the spot scripts (lib preflight check_env_vars +
    boto3 default region need them in the otherwise-minimal SSM shell).
    """
    definition = json.loads(sf.read_text())
    failures = []
    for block in _iter_command_blocks(definition):
        joined = "\n".join(block)
        # Scope: only the blocks #890 touched — those that formerly sourced
        # .env to run the data/executor pipeline (weekly_collector.py or
        # executor/*). Other pipeline blocks (e.g. the lib trading-calendar
        # check, the dashboard substrate-health check) never sourced .env and
        # resolve region from the dispatcher EC2's instance metadata, so they
        # are intentionally NOT covered by this invariant.
        touched = "source .venv/bin/activate" in joined and (
            "weekly_collector.py" in joined or "executor/" in joined
        )
        if not touched:
            continue
        has_region = re.search(r"\bAWS_REGION=us-east-1\b", joined) is not None
        has_default = (
            re.search(r"\bAWS_DEFAULT_REGION=us-east-1\b", joined) is not None
        )
        if not (has_region and has_default):
            failures.append(block)
    assert not failures, (
        f"{sf.name}: every pipeline-running SSM command block must export both "
        "AWS_REGION=us-east-1 and AWS_DEFAULT_REGION=us-east-1 (no .env post-"
        f"#890). {len(failures)} block(s) missing them."
    )
