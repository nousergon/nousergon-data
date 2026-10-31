"""The data-collection Lambdas carry the fleet cost-allocation tag on every deploy.

alpha-engine-config-I10788: every resource the data-collection component owns
carries ``system=data-collection`` (key ruled by alpha-engine-config-I10905).
The CloudFormation stack and the boxes the dispatcher launches already did; the
two Lambdas deployed outside CloudFormation did not (measured 2026-10-31 with
``aws lambda list-tags``: ``{}`` on both). Tagging only at ``create-function``
would leave the functions that already exist untagged forever, because CI runs
the code-only path, so each script must also converge the tag unconditionally
after the bootstrap block.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = [
    REPO_ROOT / "infrastructure/lambdas/data-spot-dispatcher/deploy.sh",
    REPO_ROOT / "infrastructure/lambdas/collection-readiness-probe/deploy.sh",
]
DISPATCHER_INDEX = REPO_ROOT / "infrastructure/lambdas/data-spot-dispatcher/index.py"


def _commands(text: str) -> list[str]:
    """Join backslash continuations so each shell command is one string."""
    return text.replace("\\\n", " ").splitlines()


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.parent.name)
def test_cost_tag_value_matches_the_launched_boxes(script: Path) -> None:
    index = DISPATCHER_INDEX.read_text()
    key = re.search(r'^COST_TAG_KEY = "([^"]+)"', index, re.M).group(1)
    value = re.search(r'^COST_TAG_VALUE = "([^"]+)"', index, re.M).group(1)
    assert f'COST_TAG="{key}={value}"' in script.read_text()


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.parent.name)
def test_create_function_tags_a_new_function(script: Path) -> None:
    creates = [c for c in _commands(script.read_text()) if "aws lambda create-function" in c]
    assert creates, "no create-function call found"
    for cmd in creates:
        assert '--tags "${COST_TAG}"' in cmd, cmd


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.parent.name)
def test_code_only_path_converges_the_tag(script: Path) -> None:
    text = script.read_text()
    lines = _commands(text)
    tag_calls = [i for i, c in enumerate(lines) if "aws lambda tag-resource" in c]
    assert len(tag_calls) == 1, "expected exactly one tag-resource call"
    cmd = lines[tag_calls[0]]
    assert '--tags "${COST_TAG}"' in cmd
    assert cmd.lstrip().startswith("run aws lambda tag-resource"), (
        "the converge must sit at top level (column 0), outside the --bootstrap block"
    )
    assert not cmd.startswith(" "), "tag-resource is indented: it is inside a conditional"
    # It must come after the code update, which is the CI auto-deploy path.
    update = max(i for i, c in enumerate(lines) if "aws lambda update-function-code" in c)
    assert tag_calls[0] > update
