"""Pins the RAGIngestion inner-step progress-telemetry wiring (config-I2966)
in ``rag/pipelines/run_weekly_ingestion.sh``.

The Fleet Status console strip (crucible-dashboard views/48_Fleet_Status.py)
reads ``health/rag_ingestion_progress/{run_date}.json`` and renders "step
N/10: <label>" for the RAGIngestion chip while the weekly SF is RUNNING.
That contract depends entirely on this shell script calling
``rag.pipelines.emit_progress`` at every step boundary with the RIGHT
(step, of, label) tuple, in order, BEFORE each step's actual pipeline
invocation (so the strip reflects "about to run step N", not "just
finished N-1" — the operator wants to know what's happening RIGHT NOW).

These are static-text assertions (no subprocess execution — the script
does live SEC/Finnhub/Voyage/Postgres work with no dry-run path for most
steps) mirroring the ``test_run_weekly_offcycle.py`` precedent of pinning
shell-script contracts by parsing the script body.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh"

# (step, of, label, following pipeline-module substring) — the label must
# match what views/48_Fleet_Status.py's RAGIngestion chip renders verbatim
# ("step 5/10: news"), and the module substring pins emit_progress firing
# BEFORE that step's actual work, not after.
_EXPECTED_STEPS = [
    (0, 10, "preflight", "rag.preflight"),
    (1, 10, "sec_filings", "rag.pipelines.ingest_sec_filings"),
    (2, 10, "8k_events", "rag.pipelines.ingest_8k_filings"),
    (3, 10, "earnings_transcripts", "rag.pipelines.ingest_earnings_finnhub"),
    (4, 10, "thesis_history", "rag.pipelines.ingest_theses"),
    (5, 10, "news", "rag.pipelines.run_news_pipeline"),
    (6, 10, "form4_insider", "rag.pipelines.ingest_form4"),
    (7, 10, "inst_ownership_13f", "rag.pipelines.ingest_13f"),
    (8, 10, "analyst_pipeline", "rag.pipelines.run_analyst_pipeline"),
    (9, 10, "filing_changes", "rag.pipelines.filing_change_detection"),
    (10, 10, "manifest_emit", "rag.pipelines.emit_manifest"),
]


@pytest.fixture(scope="module")
def script_text() -> str:
    return _SCRIPT.read_text()


def test_script_exists():
    assert _SCRIPT.exists()


def test_emit_progress_helper_defined(script_text: str):
    assert "emit_progress()" in script_text
    assert "rag.pipelines.emit_progress" in script_text
    assert "--run-date" in script_text and "RUN_DATE" in script_text


def test_run_date_is_utc_date_only(script_text: str):
    """RUN_DATE must be a YYYY-MM-DD date (matching the artifact key's
    {run_date} axis and the completion email's date_str), NOT the full
    ISO8601 START_TIME timestamp."""
    m = re.search(r'RUN_DATE="\$\(date -u \'([^\']+)\'\)"', script_text)
    assert m, "RUN_DATE assignment not found"
    assert m.group(1) == "+%Y-%m-%d"


@pytest.mark.parametrize("step,of,label,module", _EXPECTED_STEPS)
def test_step_emits_progress_before_its_pipeline_call(script_text, step, of, label, module):
    call = f'emit_progress {step} {of} "{label}"'
    assert call in script_text, f"missing progress call: {call}"
    i_emit = script_text.index(call)
    # Search for the module invocation ("$PYTHON_BIN -m <module>") AFTER the
    # emit_progress call site — the module name also appears earlier, in
    # this script's own top-of-file step-inventory docstring/comments, so a
    # plain .index() would find that mention instead of the real call.
    invocation = f"-m {module}"
    i_module = script_text.index(invocation, i_emit)
    assert i_emit < i_module, (
        f"emit_progress {step} {of} {label!r} must fire BEFORE its "
        f"pipeline invocation ({module}) so the strip reflects the step "
        f"about to run, not the one just finished"
    )


def test_progress_calls_are_fail_soft(script_text: str):
    """Every emit_progress() invocation inside the helper must swallow
    failure (WARN + continue) — this is the deliberate no-silent-fails
    deviation named in both the helper's own comment and the module
    docstring of rag.pipelines.emit_progress. Pinned here so a future edit
    can't accidentally drop the `|| echo WARN` guard and let a transient
    S3 hiccup abort a multi-hour ingestion run."""
    body = script_text[script_text.index("emit_progress() {"):]
    body = body[: body.index("\n}\n") + 3]
    assert "|| echo" in body and "WARN" in body
    assert "telemetry only" in body or "continuing" in body


def test_progress_calls_count_matches_step_total(script_text: str):
    calls = re.findall(r"emit_progress \d+ \d+ ", script_text)
    # One definition-site mention inside the helper itself is excluded
    # (the helper body references "$step" "$of" variables, not literals).
    assert len(calls) == len(_EXPECTED_STEPS), (
        f"expected {len(_EXPECTED_STEPS)} emit_progress call sites, found "
        f"{len(calls)}"
    )
