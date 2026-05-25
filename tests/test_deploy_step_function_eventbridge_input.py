"""Pins the EventBridge Input contract in ``deploy_step_function.sh``.

The Saturday SF cron-fired execution gets its input from the
EventBridge rule's ``Input`` field, which is constructed in
``infrastructure/deploy_step_function.sh`` (see the ``INPUT_JSON``
heredoc + ``aws events put-targets`` invocation). The Saturday SF's
behavior on cron firing is therefore controlled by THIS file, not by
the SF JSON alone.

ROADMAP L1995 Phase 3 — `enable_standalone_scanner: true` must be in
the EventBridge Input or the new Scanner SF state (Phase 2) takes the
default-off path and parallel-observe mode does NOT run. This test
pins the flag's presence so a future deploy_step_function.sh edit
can't silently revert Phase 3 by dropping the flag.

If the operator deliberately wants to revert Phase 3 (e.g. divergence
audit failed on Sat 5/30 and the substrate needs a fix-and-rerun
cycle), this test should be updated in the same PR that flips the
flag back to false.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEPLOY_PATH = _REPO_ROOT / "infrastructure" / "deploy_step_function.sh"


@pytest.fixture(scope="module")
def script_text() -> str:
    return _DEPLOY_PATH.read_text()


@pytest.fixture(scope="module")
def input_json_block(script_text: str) -> str:
    """Extract the EventBridge target Input heredoc body."""
    # Match the INPUT_JSON=$(cat <<EOF ... EOF) heredoc.
    m = re.search(
        r"INPUT_JSON=\$\(cat <<EOF\n(.+?)\nEOF\n\)",
        script_text,
        re.DOTALL,
    )
    assert m is not None, "INPUT_JSON heredoc not found in deploy_step_function.sh"
    return m.group(1)


class TestEventBridgeInput:
    def test_ec2_instance_id_present(self, input_json_block):
        # Baseline — the rule was always supposed to thread the
        # MicroInstance ID through to the SF execution.
        assert "ec2_instance_id" in input_json_block

    def test_sns_topic_arn_present(self, input_json_block):
        # Baseline — same.
        assert "sns_topic_arn" in input_json_block

    def test_enable_standalone_scanner_flag_set_true(self, input_json_block):
        # L1995 Phase 3 — the new Scanner SF state (Phase 2) gates on
        # this flag. Without it the parallel-observe mode does NOT run
        # and Phase 3 soak does not happen. Revert deliberately by
        # flipping to false here in the same PR that updates this test.
        assert "enable_standalone_scanner" in input_json_block, (
            "deploy_step_function.sh::INPUT_JSON dropped the "
            "enable_standalone_scanner field; this silently reverts "
            "L1995 Phase 3 + freezes the arc. If the revert is "
            "intentional, update both this test and the SCRIPT in the "
            "same PR."
        )
        # Pin the value too — present-but-false also reverts Phase 3.
        assert re.search(
            r'"enable_standalone_scanner"\s*:\s*true',
            input_json_block,
        ), (
            "enable_standalone_scanner is present but not set to true. "
            "Phase 3 requires the flag value to be true."
        )
