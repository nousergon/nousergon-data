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

    def test_pipeline_role_set_to_weekly(self, input_json_block):
        """Option-D 2026-05-25: every cron-triggered execution carries a
        ``pipeline_role`` tag so page 25 / Slack / CLI consumers can filter
        out smoke / recovery / operator-replay executions from the
        canonical cadence run. Saturday cron = ``"weekly"``.

        Dropping this field silently reverts page 25 to the pre-Option-D
        "smoke runs displace the real weekly" behavior — operator opens
        page 25 expecting last weekly run, sees a smoke retry instead.
        """
        assert re.search(
            r'"pipeline_role"\s*:\s*"weekly"',
            input_json_block,
        ), (
            "deploy_step_function.sh::INPUT_JSON must set "
            "pipeline_role=\"weekly\" on the Saturday cron rule. If you "
            "are intentionally changing the role taxonomy, update both "
            "this test AND the SCRIPT in the same PR — and remember to "
            "update the dashboard's page-25 role_filter set to match."
        )


# ── Daily (Weekday) cron rule ─────────────────────────────────────────────


_DAILY_DEPLOY_PATH = _REPO_ROOT / "infrastructure" / "deploy_step_function_daily.sh"


@pytest.fixture(scope="module")
def daily_script_text() -> str:
    return _DAILY_DEPLOY_PATH.read_text()


@pytest.fixture(scope="module")
def daily_input_json_block(daily_script_text: str) -> str:
    m = re.search(
        r"INPUT_JSON=\$\(cat <<EOF\n(.+?)\nEOF\n\)",
        daily_script_text,
        re.DOTALL,
    )
    assert m is not None, (
        "INPUT_JSON heredoc not found in deploy_step_function_daily.sh"
    )
    return m.group(1)


class TestWeekdayEventBridgeInput:
    def test_pipeline_role_set_to_daily(self, daily_input_json_block):
        """Option-D 2026-05-25 — Weekday cron rule must tag executions
        with ``pipeline_role="daily"`` so page 25's Weekday section
        filters past operator-replay / smoke executions to land on the
        canonical 5:45 AM PT trading-cadence run."""
        assert re.search(
            r'"pipeline_role"\s*:\s*"daily"',
            daily_input_json_block,
        ), (
            "deploy_step_function_daily.sh::INPUT_JSON must set "
            "pipeline_role=\"daily\" on the Weekday cron rule."
        )


# ── CFN orchestration template chokepoint ─────────────────────────────────


_CFN_ORCHESTRATION_PATH = (
    _REPO_ROOT
    / "infrastructure"
    / "cloudformation"
    / "alpha-engine-orchestration.yaml"
)


@pytest.fixture(scope="module")
def orchestration_text() -> str:
    return _CFN_ORCHESTRATION_PATH.read_text()


class TestOrchestrationCFNPipelineRoles:
    """The CFN template is a parallel source-of-truth to the deploy
    scripts. When CFN is re-applied (or when a fresh region/account is
    bootstrapped) the cron rules' Input fields come from the YAML, not
    the .sh scripts. Both must carry pipeline_role to prevent drift
    between the two paths.
    """

    def _trigger_block(self, text: str, name: str) -> str:
        """Extract a single Rule block from the CFN text."""
        # Crude but stable: split on rule names that appear at the same
        # indent level. Each trigger block begins with the rule name +
        # ``:`` on a leading-whitespace line and ends before the next
        # such name. We pin against a known successor name per rule.
        markers = {
            "SaturdayTrigger": "FridayShellRunTrigger",
            "FridayShellRunTrigger": "WeekdayTrigger",
            "WeekdayTrigger": "ResearchAlerts",
        }
        head = text.split(f"{name}:", 1)
        assert len(head) == 2, f"{name} block not found in orchestration CFN"
        successor = markers[name]
        block = head[1].split(f"{successor}:", 1)[0]
        return block

    def test_saturday_trigger_has_weekly_role(self, orchestration_text):
        block = self._trigger_block(orchestration_text, "SaturdayTrigger")
        assert re.search(
            r'"pipeline_role"\s*:\s*"weekly"',
            block,
        ), (
            "SaturdayTrigger Input must carry pipeline_role=\"weekly\"."
        )

    def test_friday_shell_run_trigger_has_shell_run_role(self, orchestration_text):
        block = self._trigger_block(orchestration_text, "FridayShellRunTrigger")
        assert re.search(
            r'"pipeline_role"\s*:\s*"shell-run"',
            block,
        ), (
            "FridayShellRunTrigger Input must carry "
            "pipeline_role=\"shell-run\" — distinguishes the dry-pass "
            "from the canonical weekly run on page 25."
        )

    def test_weekday_trigger_has_daily_role(self, orchestration_text):
        block = self._trigger_block(orchestration_text, "WeekdayTrigger")
        assert re.search(
            r'"pipeline_role"\s*:\s*"daily"',
            block,
        ), "WeekdayTrigger Input must carry pipeline_role=\"daily\"."
