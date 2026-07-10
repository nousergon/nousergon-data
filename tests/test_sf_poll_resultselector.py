"""Guards the ResultSelector that keeps SSM stdout out of SF state across ALL
three Step Functions (Saturday / weekday / EOD).

2026-06-06 — the Saturday SF FAILED with ``States.DataLimitExceeded``:
``ResearchPredictorParallel returned a result with a size exceeding the
maximum number of bytes`` (256 KB). Root cause: the
``WaitFor{DataPhase1,RAGIngestion,MorningEnrich}`` getCommandInvocation tasks
stored their *entire* invocation result — including the ~24 KB
``StandardOutputContent`` SSM run-log — in state; ``ResearchPredictorParallel``
tripled it past 256 KB. Fix: those 3 got a ``ResultSelector`` dropping stdout.

2026-06-19 — **the same class recurred at ``WaitForEvaluator``**: the
2026-06-06 fix (and the prior version of THIS test) covered only the 3 states
in that incident's path. Expanded to close the class for the **Saturday SF**.

2026-06-22 (config#1163) — **closed the same latent class on the weekday + EOD
SFs.** Their poll states carried full stdout too; ``HandleFailure`` on both
serializes the entire ``$`` state (``States.JsonToString($)``), so accumulated
poll stdouts are exactly the bloat that re-trips 256 KB. The weekday/EOD polls
have *different* downstream field needs than Saturday — the weekday
trading-day check reads ``.StandardOutputContent`` (MARKET_CLOSED / TRADING
DAY marker), and every EOD ``*StatusError`` Pass state reads
``.Status``/``.StatusDetails``/``.ResponseCode``/``.CommandId``/``.InstanceId``
off its poll — so the guard below is generic: it discovers EVERY field a
downstream state reads off each poll's ResultPath and requires the
ResultSelector to keep exactly those (plus ``Status``), while dropping the
unbounded ``StandardOutputContent`` unless it is itself read. Full SSM stdout
is shipped to S3 (``_ssm_logs/``), so nothing is lost for diagnosis.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_SF_FILES = {
    "saturday": _INFRA / "step_function.json",
    "weekday": _INFRA / "step_function_daily.json",
    "eod": _INFRA / "step_function_eod.json",
}


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


def _is_liveness_poller(st: dict) -> bool:
    """config#1811: the weekday SF's poll loops migrated from bare
    aws-sdk getCommandInvocation to the ssm-liveness-poller Lambda. The
    256 KB invariant applies identically — the poller's result rides in
    SF state through the same Wait/Choice loops and HandleFailure still
    serializes all of $ — so those states are held to the same
    ResultSelector rules. (The Lambda caps stderr_tail at 1500 chars and
    never returns stdout, but the ResultSelector remains the SF-side
    guard.)"""
    if "lambda:invoke" not in str(st.get("Resource", "")).lower():
        return False
    return "alpha-engine-ssm-liveness-poller" in json.dumps(
        st.get("Parameters", {})
    )


def _poll_states():
    """Yield (sf, name, state, sf_text) for every SSM command poll —
    bare getCommandInvocation Tasks AND ssm-liveness-poller invocations —
    across all three SFs."""
    out = []
    for sf, path in _SF_FILES.items():
        text = path.read_text()
        sf_def = json.loads(text)
        for name, st in _walk_states(sf_def.get("States", {})):
            if "aws-sdk:ssm:getCommandInvocation" in str(
                st.get("Resource", "")
            ) or _is_liveness_poller(st):
                out.append((sf, name, st, text))
    return out


_POLLS = _poll_states()


def _fields_read_downstream(result_path: str, sf_text: str) -> set:
    """Every field read off ``$.<poll>.<Field>`` anywhere in the SF. Each such
    field is a real data dependency the ResultSelector MUST keep."""
    if not result_path:
        return set()
    esc = re.escape(result_path)
    # Field charset includes underscore: the config#1811 liveness-poller
    # fields are snake_case (ping_misses, attempts, verdict, detail) — the
    # pre-1811 charset silently truncated ping_misses to "ping".
    return set(re.findall(rf"{esc}\.([A-Za-z][A-Za-z0-9_]*)", sf_text))


def _rs_keeps(rs: dict) -> set:
    """Field names a ResultSelector keeps (strip the trailing ``.$``)."""
    return {k[:-2] if k.endswith(".$") else k for k in rs}


def test_found_poll_states() -> None:
    # Vacuous-pass guard: all three SFs together carry many SSM poll states.
    assert len(_POLLS) >= 15, "expected SSM poll states across the three SFs"
    assert {sf for sf, _, _, _ in _POLLS} == {"saturday", "weekday", "eod"}


@pytest.mark.parametrize(
    "sf,name,st,sf_text",
    _POLLS,
    ids=[f"{sf}:{name}" for sf, name, _, _ in _POLLS],
)
def test_poll_state_trims_or_keeps_stdout_intentionally(
    sf: str, name: str, st: dict, sf_text: str
) -> None:
    rs = st.get("ResultSelector")
    assert rs is not None, (
        f"[{sf}] {name} is an SSM getCommandInvocation poll with NO "
        f"ResultSelector — its full result (incl. ~24 KB StandardOutputContent) "
        f"rides in state and can re-trip the 256 KB limit (2026-06-06 "
        f"ResearchPredictorParallel; 2026-06-19 WaitForEvaluator; the weekday + "
        f"EOD HandleFailure both serialize all of $ via JsonToString)."
    )
    keeps = _rs_keeps(rs)
    assert any(k.startswith("Status") for k in keeps), (
        f"[{sf}] {name} ResultSelector dropped Status, which Check*Status reads."
    )

    needed = _fields_read_downstream(st.get("ResultPath", ""), sf_text)
    missing = needed - keeps
    assert not missing, (
        f"[{sf}] {name}: downstream states read {sorted(missing)} off "
        f"{st.get('ResultPath')} but the ResultSelector drops them — that "
        f"breaks the consumer (e.g. EOD *StatusError reads CommandId/InstanceId)."
    )

    if "StandardOutputContent" not in needed:
        assert "StandardOutputContent" not in keeps, (
            f"[{sf}] {name} keeps StandardOutputContent but nothing reads it — "
            f"that is the unbounded field behind every 256 KB failure. Drop it."
        )
