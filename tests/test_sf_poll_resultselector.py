"""Guards the ResultSelector that keeps SSM stdout out of SF state.

2026-06-06 — the Saturday SF FAILED with
``States.DataLimitExceeded``-class error: ``ResearchPredictorParallel
returned a result with a size exceeding the maximum number of bytes``
(256 KB). Root cause: the three ``WaitFor{DataPhase1,RAGIngestion,
MorningEnrich}`` getCommandInvocation tasks stored their *entire*
invocation result — including the ~24 KB ``StandardOutputContent`` SSM
run-log — at ``$.data_phase1_poll`` / ``$.rag_ingestion_poll`` /
``$.morning_enrich_poll``. Those ~80 KB of stdout rode in state through
the whole pipeline and were then tripled by ``ResearchPredictorParallel``
(``ResultPath: $.parallel_result`` keeps the original input AND appends
the 2-branch array, each branch passing the full input through) → ~240 KB
→ over the 256 KB limit → hard ExecutionFailed.

Fix: each WaitFor* task carries a ResultSelector that keeps only the
small fields the Choice/error path needs (Status, ResponseCode,
StatusDetails, StandardErrorContent) and drops StandardOutputContent.
Full SSM stdout is already shipped to S3 (_ssm_logs/), so nothing is
lost for diagnosis.

This test forbids a future edit from dropping the ResultSelector or
re-admitting StandardOutputContent into state.
"""

from __future__ import annotations

import json
from pathlib import Path

_SF = Path(__file__).resolve().parent.parent / "infrastructure" / "step_function.json"

_WAIT_STATES = {
    "WaitForDataPhase1": "$.data_phase1_poll",
    "WaitForRAGIngestion": "$.rag_ingestion_poll",
    "WaitForMorningEnrich": "$.morning_enrich_poll",
}

# The Choice state reads .Status; the Extract*Error path serializes the
# poll object. These must survive the ResultSelector.
_REQUIRED_SELECTED = {"Status.$"}


def _all_states() -> dict:
    d = json.loads(_SF.read_text())
    out = {}

    def walk(states):
        for name, st in states.items():
            out[name] = st
            if "States" in st:
                walk(st["States"])
            for b in st.get("Branches", []):
                walk(b["States"])

    walk(d["States"])
    return out


def test_wait_states_have_resultselector_dropping_stdout():
    states = _all_states()
    for name, expected_rp in _WAIT_STATES.items():
        st = states.get(name)
        assert st is not None, f"{name} missing from step_function.json"
        assert st.get("ResultPath") == expected_rp, (
            f"{name} ResultPath changed from {expected_rp!r}"
        )
        rs = st.get("ResultSelector")
        assert rs is not None, (
            f"{name} has no ResultSelector — its full getCommandInvocation "
            f"result (incl. ~24 KB StandardOutputContent) will ride in state "
            f"and re-trip the 256 KB ResearchPredictorParallel limit."
        )
        assert "StandardOutputContent.$" not in rs and "StandardOutputContent" not in rs, (
            f"{name} ResultSelector re-admits StandardOutputContent — that is "
            f"the 24 KB bloat that caused the 2026-06-06 SF failure."
        )
        for key in _REQUIRED_SELECTED:
            assert key in rs, (
                f"{name} ResultSelector dropped {key!r}, which Check*Status "
                f"reads as the poll Status."
            )


def test_choice_states_still_read_poll_status():
    """The Choice states must reference $.<poll>.Status, which the
    ResultSelector preserves."""
    text = _SF.read_text()
    for poll in _WAIT_STATES.values():
        assert f'"{poll}.Status"' in text, (
            f"Choice no longer reads {poll}.Status — wiring drift."
        )
