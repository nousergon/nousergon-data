"""config#2938 — the weekly RAGIngestion timeouts must hold the FULL-universe
news sweep, and all four surfaces of the 4h budget must stay in lockstep.

The 2026-07-18 weekly SF failure was a DRIFT bug: the news universe grew ~9x
while the RAGIngestion SSM ``executionTimeout`` stayed at a DataPhase1-sized
3600s, so the ~3.15h Polygon sweep SIGKILLed twice. The fix sizes the runtime
Polygon budget from the LIVE universe (``fetch_budget``) and raises the outer
step timeouts to the config#2938 4h hard cap. This guard pins that the three
static timeouts equal the single ``WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS``
constant they were derived from, so a future edit to any one of them fails CI
unless the others move with it:

  1. RAGIngestion ``executionTimeout`` in infrastructure/step_function.json,
  2. the inner ``run_ssm "rag-only"`` workload timeout in spot_data_weekly.sh,
  3. the rag-only spot-watchdog ``MAX_RUNTIME_SECONDS`` (cap + shutdown margin),

and that the runtime per-universe budget always leaves reserve for the rest of
the step inside that cap.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from collectors.news_sources.fetch_budget import (
    WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS,
    weekly_news_max_fetch_seconds,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF = _REPO_ROOT / "infrastructure" / "step_function.json"
_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"


def _find_state(node, name):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == name and isinstance(v, dict) and v.get("Type"):
                return v
            found = _find_state(v, name)
            if found is not None:
                return found
    elif isinstance(node, list):
        for x in node:
            found = _find_state(x, name)
            if found is not None:
                return found
    return None


def _rag_execution_timeout() -> int:
    sf = json.loads(_SF.read_text())
    state = _find_state(sf, "RAGIngestion")
    assert state is not None, "RAGIngestion state not found in step_function.json"
    et = state["Parameters"]["Parameters"]["executionTimeout"]
    assert isinstance(et, list) and len(et) == 1, f"unexpected executionTimeout shape: {et!r}"
    return int(et[0])


def test_rag_execution_timeout_matches_4h_cap():
    # The SIGKILL boundary that fired on 2026-07-18 (was 3600s).
    assert _rag_execution_timeout() == WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS


def test_rag_execution_timeout_holds_full_universe_sweep():
    # ruling 1: the outer cap must strictly exceed the runtime Polygon budget
    # for any universe, so the fetch + the rest of the step fit inside it.
    et = _rag_execution_timeout()
    for n in (944, 2000, 100_000):
        assert weekly_news_max_fetch_seconds(n) < et


def test_inner_run_ssm_rag_only_timeout_in_lockstep():
    text = _SCRIPT.read_text()
    m = re.search(r"RAG_ONLY_EXECUTION_TIMEOUT_SECONDS=(\d+)", text)
    assert m, "RAG_ONLY_EXECUTION_TIMEOUT_SECONDS not set in spot_data_weekly.sh"
    assert int(m.group(1)) == WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS
    # ...and the inner workload SSM call actually uses that variable, not a
    # re-introduced literal 3600.
    assert 'run_ssm "rag-only" "$RAG_ONLY_WORKLOAD_TIMEOUT_SECONDS"' in text
    assert 'run_ssm "rag-only" 3600' not in text


def test_rag_only_spot_watchdog_exceeds_outer_cap():
    # The box's shutdown watchdog must be a BACKSTOP: strictly greater than the
    # outer SF executionTimeout so cleanup (not a premature box shutdown) ends
    # the run.
    text = _SCRIPT.read_text()
    block = text[text.index('RUN_MODE" = "rag-only"'):]
    m = re.search(r"MAX_RUNTIME_SECONDS=(\d+)", block)
    assert m, "rag-only MAX_RUNTIME_SECONDS override not found"
    assert int(m.group(1)) > WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS


def test_other_modes_keep_dataphase1_watchdog_default():
    # Only rag-only gets the 4h watchdog; the shared default (DataPhase1 /
    # workloads) stays 5400s so those modes are not silently over-budgeted.
    text = _SCRIPT.read_text()
    assert 'MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-5400}"' in text


def test_max_runtime_explicit_default_initialized_before_use():
    # The script runs under `set -u`; the --max-runtime-seconds flag path is
    # the only assignment of MAX_RUNTIME_EXPLICIT=1. Without a default init
    # BEFORE the rag-only override check, every SF-driven rag-only dispatch
    # dies with "MAX_RUNTIME_EXPLICIT: unbound variable" (2026-07-18
    # watch-rerun-2026-07-18-1 failure — the exact incident this pins).
    text = _SCRIPT.read_text()
    default = 'MAX_RUNTIME_EXPLICIT="${MAX_RUNTIME_EXPLICIT:-0}"'
    assert default in text, "MAX_RUNTIME_EXPLICIT must be default-initialized"
    assert text.index(default) < text.index('"$MAX_RUNTIME_EXPLICIT" != "1"'), (
        "default init must precede the rag-only override check"
    )
