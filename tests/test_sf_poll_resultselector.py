"""Guards the ResultSelector that keeps SSM stdout out of Saturday-SF state.

2026-06-06 — the Saturday SF FAILED with ``States.DataLimitExceeded``:
``ResearchPredictorParallel returned a result with a size exceeding the
maximum number of bytes`` (256 KB). Root cause: the
``WaitFor{DataPhase1,RAGIngestion,MorningEnrich}`` getCommandInvocation tasks
stored their *entire* invocation result — including the ~24 KB
``StandardOutputContent`` SSM run-log — in state; ``ResearchPredictorParallel``
tripled it past 256 KB. Fix: those 3 got a ``ResultSelector`` dropping stdout.

2026-06-19 — **the same class recurred at ``WaitForEvaluator``**: the
2026-06-06 fix (and the prior version of THIS test) covered only the 3 states
in that incident's path, leaving the later Saturday poll states still dumping
full stdout. The evaluator's large stdout blew the 256 KB limit and killed the
run before ReportCard/Director/NotifyComplete.

This test now closes the class for the **Saturday SF**: it discovers EVERY
``aws-sdk:ssm:getCommandInvocation`` poll state and requires each to carry a
``ResultSelector`` that keeps ``Status`` and drops ``StandardOutputContent`` —
UNLESS a downstream state legitimately reads ``$.<poll>.StandardOutputContent``
(e.g. ``WaitResolveZoo``, whose stdout carries the resolved zoo spec list). Full
SSM stdout is shipped to S3 (``_ssm_logs/``), so nothing is lost for diagnosis.

Weekday + EOD SF poll states have different downstream field needs (EOD reads
``.CommandId``/``.InstanceId``; the trading-day check reads stdout) and are
tracked + fixed separately — see the follow-up issue referenced in the PR.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_SF = Path(__file__).resolve().parent.parent / "infrastructure" / "step_function.json"
_SF_TEXT = _SF.read_text()


def _walk_states(states: dict):
    for name, st in states.items():
        if not isinstance(st, dict):
            continue
        yield name, st
        for sub in ("ItemProcessor", "Iterator"):
            inner = st.get(sub)
            if isinstance(inner, dict) and isinstance(inner.get("States"), dict):
                yield from _walk_states(inner["States"])
        for b in st.get("Branches", []) or []:
            if isinstance(b, dict) and isinstance(b.get("States"), dict):
                yield from _walk_states(b["States"])


def _poll_states():
    sf = json.loads(_SF_TEXT)
    out = []
    for name, st in _walk_states(sf.get("States", {})):
        if "aws-sdk:ssm:getCommandInvocation" in str(st.get("Resource", "")):
            out.append((name, st))
    return out


def _stdout_read_downstream(result_path: str) -> bool:
    """Does any state read ``$.<poll>.StandardOutputContent``? Then stdout must
    be kept in that poll's ResultSelector (it's a real data dependency)."""
    if not result_path:
        return False
    # The read may be a bare field or wrapped in a States.StringToJson(...) /
    # States.Format(...) intrinsic, so match the path fragment, not a quoted key.
    return f"{result_path}.StandardOutputContent" in _SF_TEXT


def test_found_poll_states() -> None:
    # Vacuous-pass guard: the Saturday SF has many SSM poll states.
    assert len(_poll_states()) >= 10, "expected the Saturday SF SSM poll states"


@pytest.mark.parametrize(
    "name,st", _poll_states(), ids=[n for n, _ in _poll_states()]
)
def test_poll_state_trims_or_keeps_stdout_intentionally(name: str, st: dict) -> None:
    rs = st.get("ResultSelector")
    assert rs is not None, (
        f"{name} is an SSM getCommandInvocation poll with NO ResultSelector — its "
        f"full result (incl. ~24 KB StandardOutputContent) rides in state and can "
        f"re-trip the 256 KB limit (2026-06-06 ResearchPredictorParallel; "
        f"2026-06-19 WaitForEvaluator)."
    )
    assert any(k.startswith("Status") for k in rs), (
        f"{name} ResultSelector dropped Status, which Check*Status reads."
    )
    keeps_stdout = any("StandardOutputContent" in k for k in rs)
    needs_stdout = _stdout_read_downstream(st.get("ResultPath", ""))
    if needs_stdout:
        assert keeps_stdout, (
            f"{name}: a downstream state reads {st.get('ResultPath')}."
            f"StandardOutputContent, so the ResultSelector MUST keep it."
        )
    else:
        assert not keeps_stdout, (
            f"{name} keeps StandardOutputContent but nothing reads it — that is "
            f"the unbounded field behind both 256 KB failures. Drop it."
        )
