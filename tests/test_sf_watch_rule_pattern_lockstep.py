"""alpha-engine-config-I3187 — the EventBridge rule pattern in
saturday-sf-watch-dispatcher/deploy.sh must stay in LOCKSTEP with
index.PIPELINES.

The failure-detection rule ``alpha-engine-saturday-sf-watch-failed`` is
(re)created from the EVENT_PATTERN heredoc in deploy.sh --bootstrap. The old
"keep the ARN list in lockstep with index.PIPELINES" comment was prose, not a
contract: the 2026-07-17 I2890 re-inline removed ne-weekly-advisory-pipeline /
ne-modelzoo-sunday-pipeline from PIPELINES (and every registry per
config#2937's lockstep test) but the heredoc silently kept both ARNs, so the
live rule carried two retired pipelines the overseer-liveness-probe then
flagged on every run. Drift now fails CI here.

PIPELINES is extracted STATICALLY (ast over the source text, same pattern as
tests/test_sf_watch_defer_prefix_lockstep.py) — importing index.py would drag
in boto3 and the Lambda's own pinned deps. The heredoc pipeline names are
extracted with a regex anchored to the stateMachine ARN template lines.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_DISPATCHER_DIR = (
    Path(__file__).parent.parent
    / "infrastructure"
    / "lambdas"
    / "saturday-sf-watch-dispatcher"
)
_INDEX = _DISPATCHER_DIR / "index.py"
_DEPLOY = _DISPATCHER_DIR / "deploy.sh"

_ARN_LINE = re.compile(
    r'"arn:aws:states:\$\{REGION\}:\$\{ACCOUNT_ID\}:stateMachine:([A-Za-z0-9_-]+)"'
)


def _registered_pipelines() -> set[str]:
    tree = ast.parse(_INDEX.read_text())
    for node in tree.body:
        targets = []
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target.id]
            value = node.value
        if "PIPELINES" in targets:
            assert isinstance(value, ast.Dict), "PIPELINES must be a dict literal"
            keys = set()
            for key in value.keys:
                assert isinstance(key, ast.Constant) and isinstance(key.value, str)
                keys.add(key.value)
            return keys
    raise AssertionError(f"module-level PIPELINES not found in {_INDEX}")


def _rule_pattern_pipelines() -> set[str]:
    names = set(_ARN_LINE.findall(_DEPLOY.read_text()))
    assert names, (
        f"no stateMachine ARN template lines found in {_DEPLOY} — "
        "EVENT_PATTERN heredoc moved or reshaped; update _ARN_LINE"
    )
    return names


def test_rule_pattern_matches_registered_pipelines() -> None:
    registered = _registered_pipelines()
    in_rule = _rule_pattern_pipelines()
    assert in_rule == registered, (
        "deploy.sh EVENT_PATTERN pipelines != index.PIPELINES — "
        f"only in rule: {sorted(in_rule - registered)}; "
        f"only in PIPELINES: {sorted(registered - in_rule)}. "
        "Update the EVENT_PATTERN heredoc in deploy.sh (and re-run "
        "`aws events put-rule` / deploy.sh --bootstrap live) whenever a "
        "pipeline is registered or retired."
    )
