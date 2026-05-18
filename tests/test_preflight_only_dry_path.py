"""Pins the --preflight-only Friday shell-run dry path.

Origin: ROADMAP "Friday shell-run — per-module dry-path activation"
owed-item #1. Under the Friday `shell_run`, the DataPhase1/MorningEnrich
+ RAGIngestion spot states must boot the spot for real, run their
EXISTING preflight, then exit 0 with ZERO external API data fetch and
ZERO S3/ArcticDB/config/email/SNS writes — catching bootstrap-class
breakage (lib-pin drift, sys.path collision, stale ArcticDB symbol, SSM
timeout, Dockerfile/image gap) ~12h before the real Saturday run.

These are static greps / AST-source assertions, matching the convention
of tests/test_spot_data_weekly_run_modes.py and
tests/test_weekly_collector_preflight_mode_mapping.py: the spot scripts
only run end-to-end on a real spot, and weekly_collector.main() reads
argv via _parse_args() with no DI. The assertions pin the load-bearing
invariant: --preflight-only is an early `exit 0` AFTER the existing
preflight and strictly BEFORE the sole fetch/write function so no
collector/embedding/write code path is reachable.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SPOT = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"
_RAG = _REPO_ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh"
_COLLECTOR = _REPO_ROOT / "weekly_collector.py"


@pytest.fixture(scope="module")
def spot_text() -> str:
    return _SPOT.read_text()


@pytest.fixture(scope="module")
def rag_text() -> str:
    return _RAG.read_text()


@pytest.fixture(scope="module")
def collector_text() -> str:
    return _COLLECTOR.read_text()


def _main_source() -> str:
    src = _COLLECTOR.read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return ast.get_source_segment(src, node)
    raise AssertionError("weekly_collector.main() not found")


# ── weekly_collector.py: flag exists + early-exit ordering ──────────────────


class TestCollectorPreflightOnly:
    def test_flag_parsed(self, collector_text):
        """--preflight-only must be a real argparse flag bound to
        dest=preflight_only."""
        assert '"--preflight-only"' in collector_text
        assert 'dest="preflight_only"' in collector_text

    def test_exit_zero_after_preflight_before_run_weekly(self):
        """The load-bearing invariant: main() must (1) run
        DataPreflight(...).run(), then (2) early-exit 0 on
        --preflight-only, then (3) only AFTER that call run_weekly().

        If the exit landed after run_weekly(), every collector fetch +
        every S3/ArcticDB/config write would already have happened —
        defeating the entire dry-path purpose.
        """
        src = _main_source()
        i_preflight = src.index("DataPreflight(")
        i_exit_guard = src.index('getattr(args, "preflight_only", False)')
        i_run_weekly = src.index("run_weekly(config, args)")

        assert i_preflight < i_exit_guard < i_run_weekly, (
            "main() ordering must be: DataPreflight().run() → "
            "--preflight-only guard → run_weekly(). Got positions "
            f"preflight={i_preflight}, guard={i_exit_guard}, "
            f"run_weekly={i_run_weekly}."
        )

    def test_guard_raises_systemexit_zero(self):
        """The guard must exit 0 (clean) so SSM/SF report Success — a
        passed preflight is a healthy outcome, not a failure."""
        src = _main_source()
        marker = 'getattr(args, "preflight_only", False)'
        body = src[src.index(marker):src.index("run_weekly(config, args)")]
        assert "raise SystemExit(0)" in body, (
            "The --preflight-only branch must `raise SystemExit(0)` before "
            f"run_weekly(). Branch body:\n{body!r}"
        )

    def test_run_weekly_only_called_once_after_guard(self):
        """run_weekly() (the sole fetch/write function) must be invoked
        exactly once and only after the guard — no pre-guard call."""
        src = _main_source()
        guard_pos = src.index('getattr(args, "preflight_only", False)')
        pre = src[:guard_pos]
        assert "run_weekly(config, args)" not in pre, (
            "run_weekly() must not be invoked before the --preflight-only "
            "guard — that would fetch/write before the early exit."
        )


# ── spot_data_weekly.sh: modifier flag composes with RUN_MODE ───────────────


class TestSpotScriptPreflightOnly:
    def test_flag_parsed_as_modifier(self, spot_text):
        """--preflight-only sets PREFLIGHT_ONLY=1 (a modifier orthogonal
        to RUN_MODE so it composes with the data path AND --rag-only)."""
        assert re.search(
            r'--preflight-only\)\s*PREFLIGHT_ONLY=1;\s*shift\s*;;',
            spot_text,
        ), "--preflight-only must set PREFLIGHT_ONLY=1 in the arg-parse loop"
        # PREFLIGHT_ONLY must be initialised (set -u safety) before parse.
        assert "PREFLIGHT_ONLY=0" in spot_text

    def test_data_path_swaps_work_for_preflight_only_invocation(self, spot_text):
        """Under PREFLIGHT_ONLY the data path must invoke
        weekly_collector.py with --preflight-only (not the real --phase 1
        / --morning-enrich work invocations) and never reach prune/RAG.

        Slice the data-path block: the LAST `if [ "$PREFLIGHT_ONLY" = "1"
        ]; then` (the rag-only nested one is earlier in the file) up to
        the real WORKLOADS heredoc.
        """
        i_block = spot_text.rindex('if [ "$PREFLIGHT_ONLY" = "1" ]; then')
        i_workloads = spot_text.index("run_remote bash -s <<WORKLOADS")
        block = spot_text[i_block:i_workloads]
        assert "weekly_collector.py --morning-enrich --preflight-only" in block
        assert "weekly_collector.py --phase 1 --preflight-only" in block
        # Hard invariant: no real work / write code path inside the block.
        assert "prune_delisted_tickers" not in block, (
            "prune writes a prune-audit JSON — must NOT run under --preflight-only"
        )
        assert "run_weekly_ingestion.sh" not in block, (
            "RAG ingestion must NOT run under the data-path --preflight-only block"
        )
        assert "put-metric-data" not in block, (
            "CloudWatch heartbeat must NOT be emitted for a preflight"
        )
        assert "aws s3 cp" not in block, (
            "no S3 log upload inside the preflight-only data block"
        )

    def test_preflight_only_data_block_exits_zero(self, spot_text):
        """The data-path preflight-only block must `exit 0` before the
        real WORKLOADS heredoc so the work path is unreachable."""
        i_block = spot_text.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then')
        i_workloads = spot_text.index("run_remote bash -s <<WORKLOADS")
        assert i_block < i_workloads, (
            "the PREFLIGHT_ONLY data block must precede the WORKLOADS heredoc"
        )
        between = spot_text[i_block:i_workloads]
        assert "exit 0" in between, (
            "the PREFLIGHT_ONLY data block must `exit 0` before WORKLOADS"
        )

    def test_rag_only_preflight_only_runs_rag_preflight_only(self, spot_text):
        """`--rag-only --preflight-only` must run the RAG-path preflight
        ONLY: run_weekly_ingestion.sh --preflight-only, no real RAG run,
        no rag-ingestion heartbeat."""
        m = re.search(
            r'if \[ "\$RUN_MODE" = "rag-only" \]; then\n'
            r'    if \[ "\$PREFLIGHT_ONLY" = "1" \]; then(.*?)\n        exit 0\n    fi',
            spot_text,
            re.DOTALL,
        )
        assert m, (
            "expected a nested `if PREFLIGHT_ONLY` block inside the rag-only arm"
        )
        block = m.group(1)
        assert "run_weekly_ingestion.sh --preflight-only" in block, (
            "rag-only + preflight-only must call the RAG script with "
            "--preflight-only (step-0-only, exit 0)"
        )
        assert "put-metric-data" not in block, (
            "no rag-ingestion heartbeat for a preflight (not a completed ingestion)"
        )


# ── run_weekly_ingestion.sh: step-0-only early exit ─────────────────────────


class TestRagScriptPreflightOnly:
    def test_flag_parsed(self, rag_text):
        assert "PREFLIGHT_ONLY=0" in rag_text
        assert re.search(
            r'--preflight-only\)\s*PREFLIGHT_ONLY=1\s*;;',
            rag_text,
        ), "--preflight-only must set PREFLIGHT_ONLY=1 in the RAG script arg loop"

    def test_exit_zero_after_step0_before_step1(self, rag_text):
        """The RAG dry path must exit 0 after `python -m rag.preflight`
        (step 0) and strictly BEFORE Step 1 (ingest_sec_filings) so no
        ingest / Voyage-embedding / Postgres-write path is reachable."""
        i_preflight = rag_text.index("$PYTHON_BIN -m rag.preflight")
        i_guard = rag_text.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then')
        # Anchor on the actual Step-1 invocation, not the header-comment
        # mention of ingest_sec_filings earlier in the file.
        i_step1 = rag_text.index("rag.pipelines.ingest_sec_filings")

        assert i_preflight < i_guard < i_step1, (
            "RAG ordering must be: rag.preflight → --preflight-only guard "
            f"→ Step 1. Got preflight={i_preflight}, guard={i_guard}, "
            f"step1={i_step1}."
        )
        guard_block = rag_text[i_guard:i_step1]
        assert "exit 0" in guard_block, (
            "the RAG --preflight-only guard must `exit 0` before Step 1"
        )
