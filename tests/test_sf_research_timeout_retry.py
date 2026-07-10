"""Pin the weekly SF Research state's checkpoint-resume timeout retry
(config#1650 item 1, 2026-07-06).

The 2026-07-03 scheduled weekly failed the primary execution because the
Research state — a synchronous `lambda:invoke` with `TimeoutSeconds: 900`
(the Lambda hard maximum, walls identical on both sides) — hit
`States.Timeout`. The runner persists per-node state
(`archive/agent_runs/{date}/` + `archive/sector_team_runs/{date}/`) and
RESUMEs completed nodes at near-zero LLM cost: the 7/3 manual watch-rerun
re-entered Research and completed in ~5.9 min, re-running only
fetch_data + archive_writer + email. A single SF-level retry on the
timeout therefore IS "checkpoint+resume across invocations" — it turns
the manual watch-rerun into the automatic contract.

Pins:
  1. The Research state retries on BOTH `States.Timeout` (SF task-level
     timer) and `Lambda.Unknown` (Lambda-side function timeout surfaced
     in the invoke response) — with equal 900s walls either can fire
     first depending on which timer wins.
  2. `IntervalSeconds >= 60` — LOAD-BEARING: the retry must not fire
     until the first invocation is dead past its own 900s Lambda
     ceiling; the runner has reserved-concurrency=1 (verified live
     2026-07-06), so an overlapping invoke would throttle.
  3. `MaxAttempts == 1` — one resume pass; a second consecutive timeout
     means the corpus/runtime has genuinely outgrown the Lambda and the
     config#1687 spot-EC2 migration is due, not more retries.

Bridge until config#1687 (Brian-ratified 2026-07-03) moves Research off
the 900s Lambda ceiling entirely; remove this pin with that migration.
"""
from __future__ import annotations

import json
import pathlib

import pytest

SF_JSON = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def research_state() -> dict:
    definition = json.loads(SF_JSON.read_text())
    parallel = definition["States"]["ResearchPredictorParallel"]
    for branch in parallel["Branches"]:
        if "Research" in branch["States"]:
            return branch["States"]["Research"]
    raise AssertionError("Research state not found in ResearchPredictorParallel branches")


@pytest.fixture(scope="module")
def timeout_retry(research_state: dict) -> dict:
    matches = [
        r for r in research_state.get("Retry", [])
        if "States.Timeout" in r.get("ErrorEquals", [])
    ]
    assert len(matches) == 1, (
        "Research state must carry exactly one States.Timeout retry policy "
        "(checkpoint-resume contract, config#1650)"
    )
    return matches[0]


def test_research_retries_both_timeout_error_forms(timeout_retry: dict):
    assert set(timeout_retry["ErrorEquals"]) == {"States.Timeout", "Lambda.Unknown"}


def test_research_timeout_retry_is_single_attempt(timeout_retry: dict):
    assert timeout_retry["MaxAttempts"] == 1


def test_research_timeout_retry_waits_out_first_invocation(timeout_retry: dict):
    # 60s past the SF timeout guarantees the first invocation has died at
    # its own 900s Lambda ceiling (reserved-concurrency=1 would throttle
    # an overlapping invoke).
    assert timeout_retry["IntervalSeconds"] >= 60


def test_research_walls_still_equal_lambda_maximum(research_state: dict):
    # The retry design assumes SF and Lambda walls are both 900s (the
    # Lambda hard max). If someone raises TimeoutSeconds past 900, the
    # States.Timeout arm goes dead and Lambda.Unknown carries alone —
    # revisit the retry (and config#1687's priority) instead.
    assert research_state["TimeoutSeconds"] == 900
