"""A weekly-SF rehearsal does not email the operator.

rehearsal-2026-10-16-2 (calendar_date 2026-10-15) ran with
``sns_topic_arn = alpha-engine-alerts-muted``, and its RAGIngestion stage still
put "Alpha Engine RAG Ingestion | 2026-10-15 | DEGRADED" in the operator's
inbox. The completion email is sent straight over SMTP/SES by ``emailer.py`` on
the spot box, so the muted topic never applied to it, and the box had no way
to know it was running a rehearsal: the SF execution name reached the launcher
as ``$RUN_TOKEN`` (krepis.ssm_log_capture) but was never forwarded into the
workload script the launcher sends over SSM.

Pinned here:
  * ``send_step_email`` sends nothing under a ``rehearsal-*`` RUN_TOKEN, logs
    the would-be email instead, and returns True (nothing failed);
  * production execution names, and no RUN_TOKEN at all, still send;
  * the RAG and DataPhase2 launchers forward RUN_TOKEN into the workload.
"""

from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path

import pytest

import emailer

ROOT = Path(__file__).resolve().parent.parent
INFRA = ROOT / "infrastructure"

_RESULTS = {
    "phase": "RAG",
    "date": "2026-10-15",
    "started_at": "2026-10-16T20:16:05+00:00",
    "completed_at": "2026-10-16T20:27:29+00:00",
    "status": "degraded",
    "collectors": {
        "sec_filings": {
            "status": "degraded",
            "error": "every one of the 2 new document(s) the source offered failed (no_sections=2)",
        },
        "8k_events": {"status": "ok"},
    },
}


@pytest.fixture()
def sent(monkeypatch):
    calls: list[tuple] = []

    def fake_send_email(subject, body, html=None):
        calls.append((subject, body, html))
        return True

    monkeypatch.setattr(emailer, "send_email", fake_send_email)
    return calls


# ── emailer ──────────────────────────────────────────────────────────────────


def test_rehearsal_run_token_suppresses_the_email(monkeypatch, sent, caplog):
    monkeypatch.setenv("RUN_TOKEN", "rehearsal-2026-10-16-2")
    with caplog.at_level(logging.WARNING, logger="emailer"):
        ok = emailer.send_step_email("RAG Ingestion", _RESULTS, "2026-10-15")
    assert ok is True, "a suppressed rehearsal email is not a send failure"
    assert sent == []
    log = caplog.text
    assert "rehearsal-2026-10-16-2" in log
    assert "Alpha Engine RAG Ingestion | 2026-10-15 | DEGRADED" in log
    assert "no_sections=2" in log, "the would-be body is kept in the run log"


@pytest.mark.parametrize(
    "token",
    [
        "ff6ac9fe-90a8-471f-9905-d73f00012ff5",  # scheduled Saturday run
        "weekly-rerun-2026-10-10-d16-2",
        "watch-rerun-2026-10-02-1",
        "spot-data-weekly-20261017",  # _spot_common.sh's fallback
        "my-rehearsal-2026-10-16-1",  # prefix, not substring
    ],
)
def test_production_run_tokens_still_send(monkeypatch, sent, token):
    monkeypatch.setenv("RUN_TOKEN", token)
    assert emailer.send_step_email("RAG Ingestion", _RESULTS, "2026-10-15") is True
    assert len(sent) == 1
    assert sent[0][0] == "Alpha Engine RAG Ingestion | 2026-10-15 | DEGRADED"


def test_no_run_token_still_sends(monkeypatch, sent):
    monkeypatch.delenv("RUN_TOKEN", raising=False)
    emailer.send_step_email("Data Phase 2", _RESULTS, "2026-10-15")
    assert len(sent) == 1


def test_rehearsal_execution_reads_the_prefix():
    assert emailer.rehearsal_execution({"RUN_TOKEN": "rehearsal-2026-10-16-2"}) == "rehearsal-2026-10-16-2"
    assert emailer.rehearsal_execution({"RUN_TOKEN": " rehearsal-x "}) == "rehearsal-x"
    assert emailer.rehearsal_execution({"RUN_TOKEN": ""}) is None
    assert emailer.rehearsal_execution({}) is None


# ── launcher → workload forwarding ───────────────────────────────────────────


def _run_token_export_block() -> str:
    text = (INFRA / "_spot_common.sh").read_text()
    m = re.search(r"^if \[ -n \"\$\{RUN_TOKEN:-\}\" \]; then\n.*?^fi\n", text, re.S | re.M)
    assert m, "_spot_common.sh no longer defines _RUN_TOKEN_EXPORT the way this test reads it"
    return m.group(0)


@pytest.mark.parametrize("token", ["rehearsal-2026-10-16-2", "weird token; $(touch /nonexistent)"])
def test_spot_common_export_line_round_trips_the_token(token):
    """The line _spot_common.sh builds, run on the worker, re-creates RUN_TOKEN exactly."""
    script = _run_token_export_block() + 'bash -c "${_RUN_TOKEN_EXPORT}"\'printf %s "$RUN_TOKEN"\''
    out = subprocess.run(
        ["bash", "-c", script], env={"RUN_TOKEN": token, "PATH": "/usr/bin:/bin"},
        capture_output=True, text=True, check=True,
    )
    assert out.stdout == token


def _heredoc(path: Path, opener: str, terminator: str) -> str:
    text = path.read_text()
    start = text.index(opener)
    end = text.index("\n" + terminator + "\n", start)
    return text[start:end]


def test_rag_ingestion_workload_carries_the_run_token():
    body = _heredoc(INFRA / "spot_rag_ingestion.sh", 'run_ssm "rag-ingestion"', "WORKLOAD")
    assert "${_RUN_TOKEN_EXPORT}" in body
    assert body.index("${_RUN_TOKEN_EXPORT}") < body.index("run_weekly_ingestion.sh"), (
        "RUN_TOKEN must be exported before the ingestion (and its completion email) runs"
    )


def test_data_phase2_workload_carries_the_run_token():
    body = _heredoc(INFRA / "spot_data_weekly.sh", 'run_ssm "phase2-only"', "PHASE2_ONLY")
    line = "export RUN_TOKEN=$(printf '%q' \"${RUN_TOKEN:-}\")"
    assert line in body
    assert body.index(line) < body.index("weekly_collector.py --phase 2")


def test_rag_completion_email_goes_through_send_step_email():
    """The RAG email this guards is the one run_weekly_ingestion.sh sends."""
    text = (ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh").read_text()
    assert "send_step_email('RAG Ingestion', results, date_str)" in text
