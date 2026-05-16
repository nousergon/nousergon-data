"""Pins the morning-enrich-only / phase1-only run-mode split in
infrastructure/spot_data_weekly.sh.

Origin: the preflight-task-split (2026-05-16, plan
alpha-engine-docs/private/preflight-task-split-260516.md). The Saturday
SF MorningEnrich state runs `--morning-enrich-only` and the DataPhase1
state runs `--phase1-only`; each must run ONLY its own action so a
phase1 failure never re-pays the completed ~28-min morning-enrich.
`--data-only` is preserved (runs both) for manual/adhoc backward-compat.

These are static greps (the script only runs end-to-end on a real spot)
mirroring tests/test_spot_env_source_aws_region.py — they catch the
regression class where the modes are removed, un-gated, or re-bundled.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"


@pytest.fixture(scope="module")
def text() -> str:
    return _SCRIPT.read_text()


class TestFlagParsing:
    """Both new flags must be parsed into their RUN_MODE values, and
    --data-only must be preserved."""

    @pytest.mark.parametrize(
        "flag,run_mode",
        [
            ("--morning-enrich-only", "morning-enrich-only"),
            ("--phase1-only", "phase1-only"),
            ("--data-only", "data-only"),
        ],
    )
    def test_flag_sets_run_mode(self, text, flag, run_mode):
        # case branch shape: `--flag) RUN_MODE="value"; shift ;;`
        pat = re.compile(
            re.escape(flag) + r'\)\s*RUN_MODE="' + re.escape(run_mode) + r'"'
        )
        assert pat.search(text), (
            f"{_SCRIPT.name}: flag {flag} must set RUN_MODE={run_mode!r} "
            f"in the arg-parse case block."
        )


def _dispatch_arm(text: str, mode: str) -> str:
    """Return the body of the `case "$RUN_MODE"` dispatch arm for `mode`.

    There are TWO `<mode>)` arms in the script: the flag-parse arm
    (`--<flag>) RUN_MODE="..."; shift ;;`) and the RUN_MODE dispatch arm
    (`<mode>) ...DO_MORNING_ENRICH=... ;;`). Anchor on a bare-word
    (non-`--`) arm head at line start so we only match the dispatch arm.
    """
    m = re.search(
        r"^\s*" + re.escape(mode) + r"\)(.*?);;",
        text,
        re.DOTALL | re.MULTILINE,
    )
    assert m, f"{_SCRIPT.name}: no RUN_MODE dispatch arm for {mode!r}"
    return m.group(1)


class TestModeDispatch:
    """RUN_MODE must derive independent DO_MORNING_ENRICH / DO_PHASE1
    gates so each action runs in isolation per the split."""

    @pytest.mark.parametrize(
        "mode,do_me,do_p1",
        [
            ("morning-enrich-only", "1", "0"),
            ("phase1-only", "0", "1"),
            ("data-only", "1", "1"),
        ],
    )
    def test_mode_sets_independent_gates(self, text, mode, do_me, do_p1):
        # The dispatch arm for the mode must set DO_MORNING_ENRICH=<x>
        # and DO_PHASE1=<y>.
        arm = _dispatch_arm(text, mode)
        assert f"DO_MORNING_ENRICH={do_me}" in arm, (
            f"{mode}: must set DO_MORNING_ENRICH={do_me} (got arm: {arm!r})"
        )
        assert f"DO_PHASE1={do_p1}" in arm, (
            f"{mode}: must set DO_PHASE1={do_p1} (got arm: {arm!r})"
        )

    def test_morning_enrich_block_independently_gated(self, text):
        """The --morning-enrich invocation must be wrapped in a
        DO_MORNING_ENRICH gate so phase1-only does NOT run it."""
        assert re.search(
            r'if \[ "\$\{DO_MORNING_ENRICH\}" = "1" \]; then',
            text,
        ), "morning-enrich must be gated by DO_MORNING_ENRICH"
        # The actual invocation lives inside the gated block.
        assert "weekly_collector.py --morning-enrich 2>&1" in text

    def test_phase1_block_independently_gated(self, text):
        """The --phase 1 + prune invocations must be wrapped in a
        DO_PHASE1 gate so morning-enrich-only does NOT run them."""
        assert re.search(
            r'if \[ "\$\{DO_PHASE1\}" = "1" \]; then',
            text,
        ), "phase1 + prune must be gated by DO_PHASE1"
        assert "weekly_collector.py --phase 1 2>&1" in text
        assert "builders.prune_delisted_tickers --apply" in text

    def test_split_modes_skip_rag_block(self, text):
        """morning-enrich-only / phase1-only / data-only all run RAG
        separately (SKIP_RAG_BLOCK=1) — RAG is its own SF state."""
        for mode in ("morning-enrich-only", "phase1-only", "data-only"):
            assert "SKIP_RAG_BLOCK=1" in _dispatch_arm(text, mode), (
                f"{mode}: must set SKIP_RAG_BLOCK=1 (RAG is a separate SF state)"
            )


class TestPerModeLabel:
    """The S3 log key + heartbeat dimension must reflect which action
    ran — a morning-enrich-only run must NOT be labeled data-phase1."""

    def test_mode_label_assigned_per_mode(self, text):
        assert 'MODE_LABEL="morning-enrich"' in _dispatch_arm(
            text, "morning-enrich-only"
        ), (
            "morning-enrich-only must set MODE_LABEL=morning-enrich so its "
            "S3 log key is not health/data_phase1_log/..."
        )

    def test_log_key_uses_mode_label(self, text):
        # s3_key built from ${MODE_LABEL...} not a hardcoded data_phase1.
        assert "MODE_LABEL" in text
        m = re.search(r"s3_key=.*?MODE_LABEL", text)
        assert m, (
            f"{_SCRIPT.name}: S3 log key must be derived from MODE_LABEL, "
            "not a hardcoded data-phase1 path."
        )

    def test_heartbeat_per_mode(self, text):
        """morning-enrich-only emits only the morning-enrich heartbeat;
        phase1-only emits data-phase1 + universe-prune; neither
        double-credits the other's action."""
        assert re.search(
            r'morning-enrich-only\)\s*HEARTBEAT_PROCS=\("morning-enrich"\)',
            text,
        )
        assert re.search(
            r'phase1-only\)\s*HEARTBEAT_PROCS=\("data-phase1" "universe-prune"\)',
            text,
        )
