"""config#2274 — the weekly SF must carry a top-level TimeoutSeconds.

Without it, a hung SSM ``WaitFor*`` poll loop (SSM control-plane degradation
— the 2026-07-11 disk-full incident class) runs toward the Step Functions
1-year default ceiling, invisible to EVERY layer: the deadman alarm only
watches ``ExecutionsStarted`` and sf-watch only fires on TERMINAL statuses,
which never arrive. The global ceiling converts a hang into ``TIMED_OUT`` —
a terminal status the sf-watch EventBridge rule already matches, so a hang
pages and dispatches repair automatically the moment it fires.

Bounds pinned here:
  * floor: the ceiling must clear the longest LEGITIMATE composition — the
    eval-judge batch poll's in-definition ``max_wait_seconds`` cap plus real
    stage headroom (recorded max full run: 3.04h over the 24-execution
    history sampled 2026-07-11);
  * cap: it must stay a same-day signal (<= 24h) — a multi-day ceiling would
    recreate the invisible-hang problem it exists to close.

Also pins the TIMED_OUT → sf-watch wiring this fix relies on: the dispatcher
deploy.sh's EVENT_PATTERN must keep matching TIMED_OUT for the weekly SF.
"""
from __future__ import annotations

import json
import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).parent.parent
_WEEKLY = _REPO_ROOT / "infrastructure" / "step_function.json"
_ADVISORY = _REPO_ROOT / "infrastructure" / "step_function_advisory.json"
_WATCH_DEPLOY = (
    _REPO_ROOT / "infrastructure" / "lambdas" / "saturday-sf-watch-dispatcher" / "deploy.sh"
)


def _definition() -> dict:
    return json.loads(_WEEKLY.read_text())


def _advisory_definition() -> dict:
    return json.loads(_ADVISORY.read_text())


def test_weekly_definition_has_global_timeout():
    definition = _definition()
    timeout = definition.get("TimeoutSeconds")
    assert isinstance(timeout, int), (
        "config#2274 regression: the weekly SF lost its top-level "
        "TimeoutSeconds — a hung poll loop would again run toward the 1-year "
        "ceiling invisibly"
    )
    assert timeout == 43200, (
        f"TimeoutSeconds changed ({timeout}) — re-derive from execution "
        "history and update this pin + the top-level Comment rationale together"
    )


def test_global_timeout_clears_longest_legitimate_composition():
    # alpha-engine-config-I2544: the eval-judge chain (and its
    # max_wait_seconds batch-poll cap) moved to the async
    # ne-weekly-advisory-pipeline child SF — this file no longer contains
    # that wait at all, so the weekly SF's own ceiling is now bounded by the
    # spot-stage/retry-ladder composition alone (see the top-level Comment).
    definition = _definition()
    timeout = definition["TimeoutSeconds"]
    text = _WEEKLY.read_text()
    max_waits = [int(m) for m in re.findall(r'"max_wait_seconds":\s*(\d+)', text)]
    assert not max_waits, (
        "a max_wait_seconds cap reappeared in step_function.json — the "
        "eval-judge chain that owned this pattern was moved to "
        "step_function_advisory.json by alpha-engine-config-I2544; if a new "
        "in-SF wait was legitimately added here, restore the "
        "clears-the-longest-wait assertion this test used to make"
    )
    assert timeout <= 24 * 3600, (
        "global ceiling above 24h stops being a same-day hang signal"
    )


def test_advisory_global_timeout_clears_its_own_eval_judge_poll_cap():
    """alpha-engine-config-I2544: the advisory child SF inherits the
    eval-judge chain's max_wait_seconds cap — its OWN top-level
    TimeoutSeconds must clear it with headroom, mirroring the parent SF's
    config#2274 discipline in miniature."""
    definition = _advisory_definition()
    timeout = definition["TimeoutSeconds"]
    assert isinstance(timeout, int)
    text = _ADVISORY.read_text()
    max_waits = [int(m) for m in re.findall(r'"max_wait_seconds":\s*(\d+)', text)]
    assert max_waits, "eval-judge max_wait_seconds cap not found in the advisory child SF"
    assert timeout >= max(max_waits) + 3600, (
        "advisory child SF's ceiling too tight: a legitimate slow Anthropic "
        "batch plus ReportCard/Director would TIMED_OUT a healthy run"
    )
    assert timeout <= 24 * 3600, (
        "advisory child SF's ceiling above 24h stops being a same-day hang signal"
    )


def test_timed_out_routes_into_sf_watch():
    """The ceiling is only load-bearing because TIMED_OUT lands in sf-watch:
    the dispatcher's EventBridge EVENT_PATTERN must match TIMED_OUT for the
    weekly pipeline."""
    deploy = _WATCH_DEPLOY.read_text()
    match = re.search(r'"status":\s*\[([^\]]*)\]', deploy)
    assert match, "EVENT_PATTERN status list not found in sf-watch deploy.sh"
    statuses = {s.strip().strip('"') for s in match.group(1).split(",")}
    assert "TIMED_OUT" in statuses
    assert "ne-weekly-freshness-pipeline" in deploy
    # alpha-engine-config-I2544/I2545: both new child SFs are watch-registered too.
    assert "ne-weekly-advisory-pipeline" in deploy
    assert "ne-modelzoo-sunday-pipeline" in deploy
