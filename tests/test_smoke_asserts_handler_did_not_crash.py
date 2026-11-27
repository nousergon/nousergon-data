"""Every `deploy.sh --smoke` path must be able to detect a handler CRASH
(alpha-engine-config-I7535).

## Why

`aws lambda invoke` exits 0 when the handler RAISES: a handler exception is
a successful *invocation* carrying a `FunctionError: Unhandled` header on
the invoke command's OWN stdout (not the response payload file) and an
`{"errorType": ..., "errorMessage": ...}` body in the response payload.
Measured 2026-08-17: all 31 `infrastructure/lambdas/*/deploy.sh` `--smoke`
paths sent that stdout to `/dev/null`, printed the response body, and fell
through to `exit 0` regardless of its content — 18 of them asserted nothing
at all. An operator running `--smoke` against a completely broken handler
saw output and a zero exit, and read it as a pass. `alpha-engine-config-
I7379` fixed exactly this shape on one script (`eod-success-friday-shell-
trigger`); this closed it on all 31.

The fix is `infrastructure/lambdas/_shared/smoke.sh`, sourced by every
`--smoke` path. It is deliberately UNIVERSAL — "did the handler crash" needs
no per-handler knowledge — and deliberately does NOT replace any existing
per-handler success assertion (e.g. the `grep -q '"fired": *true'` check in
`eod-success-friday-shell-trigger/deploy.sh`); it runs FIRST, ahead of any
such check, since a crashed handler can also fail a semantic check for the
wrong reason.

Two scripts' `--smoke` paths do not do a direct, synchronous `aws lambda
invoke` and so cannot call `assert_no_function_error` in the standard shape:

  * `scheduled-groom-dispatcher` dispatches via `aws stepfunctions
    start-execution` (the SF's first state invokes the groom Lambda,
    config#1472) — its crash check is `assert_sf_lambda_task_not_failed`,
    a bounded poll for that state failing, defined in the same shared file.
  * `changelog-incident-mirror`'s real-world smoke path publishes via SNS
    (async, no invoke response) — it keeps that real path AND adds one
    direct invoke (mirroring the synthetic-SNS-envelope technique already
    used by `backstop-telegram-notifier` / `overseer-backstop-responder`)
    purely so `assert_no_function_error` has a response to check.

Both still source `_shared/smoke.sh` and call one of its two assertions, so
this guard covers all 31 with one rule: source the helper, call an
assertion from it.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPTS = sorted(REPO.glob("infrastructure/lambdas/*/deploy.sh"))

# A deploy.sh "has a --smoke path" if its arg-parser recognizes --smoke as a
# flag: either a `case` branch (`--smoke)`) or a direct string comparison
# (`"--smoke"`, changelog-incident-mirror's older SMOKE_ARG style). Written
# against the ACT of recognizing the flag, not a comment mentioning it, so a
# --smoke mentioned only in a usage/help comment does not count.
_SMOKE_FLAG = re.compile(r'--smoke\)|"--smoke"')

_ASSERT_CALLS = ("assert_no_function_error", "assert_sf_lambda_task_not_failed")
_SEMANTIC_ASSERT_CALLS = (
    "assert_json_response_field",
    "assert_json_response_has_field",
)
_SEMANTIC_EXEMPTION_PREFIX = "SMOKE_SEMANTIC_EXEMPTION:"

# These handlers cannot have their success precondition manufactured safely by
# deploy.sh.  The exact explanation is pinned so an empty/stale exemption is
# visible in review, and the companion test below makes every entry retire as
# soon as its script gains a semantic assertion.
REASONED_SEMANTIC_EXEMPTIONS: dict[str, str] = {
    "alert-drain-liveness-probe": (
        "reclaim success requires a real EC2 reclaim EventBridge event; the empty "
        "smoke payload can only exercise the documented no-op path."
    ),
    "canary-replay-liveness-probe": (
        "a successful check requires the live scheduled check window and its run "
        "marker; the read-only smoke cannot create either on demand."
    ),
    "eod-backstop": (
        "started_eod requires a trading day with no current or completed EOD; "
        "smoke must not manufacture that live recovery condition."
    ),
}

# The measured residual from alpha-engine-config-I7535.  This named set may
# only shrink: scripts outside it already had established, sometimes inline,
# semantic checks whose spellings predate the shared JSON helpers.
I7554_RESIDUAL_SCRIPTS: frozenset[str] = frozenset(
    {
        "alert-drain-liveness-probe",
        "arctic-migration-dispatcher",
        "backstop-telegram-notifier",
        "canary-replay-dispatcher",
        "canary-replay-liveness-probe",
        "changelog-incident-mirror",
        "eod-backstop",
        "eod-precondition-probe",
        "overseer-backstop-responder",
        "pipeline-watchdog",
        "preflight-sweep-dispatcher",
        "saturday-integrity-sentinel",
        "saturday-sf-watch-dispatcher",
        "sf-telegram-notifier",
        "spot-orphan-reaper",
        "ssm-liveness-poller",
        "ssm-reachability-probe",
        "substrate-health-gate",
        "thinktank-spot-dispatcher",
    }
)

# Scripts whose --smoke path recognizes the flag but is exempt from calling
# the shared assertion. This set may ONLY SHRINK (mirrors
# GRANDFATHERED_INLINE_APPLY in test_apply_iam_is_iam_only.py) — an entry
# here is a tracked debt, not a permanent carve-out. Empty on landing: all
# 31 measured --smoke scripts were wired in the same PR that added this
# test (alpha-engine-config-I7535).
GRANDFATHERED_NO_CRASH_CHECK: frozenset[str] = frozenset()


def _smoke_scripts() -> "list[Path]":
    return [p for p in DEPLOY_SCRIPTS if _SMOKE_FLAG.search(p.read_text())]


def test_there_are_smoke_scripts_to_check() -> None:
    """A guard that silently matches nothing is not a guard."""
    assert len(_smoke_scripts()) >= 30


@pytest.mark.parametrize("script", _smoke_scripts(), ids=lambda p: p.parent.name)
def test_smoke_path_sources_the_shared_helper(script: Path) -> None:
    """One assertion, sourced — no script gets its own copy
    (`policy-shared-code`). The grandfather list may only shrink."""
    name = script.parent.name
    if name in GRANDFATHERED_NO_CRASH_CHECK:
        pytest.skip(f"{name} is grandfathered; the list may only shrink")
    body = script.read_text()
    assert "_shared/smoke.sh" in body, (
        f"{name}/deploy.sh recognizes --smoke but does not source "
        f"_shared/smoke.sh — it cannot detect a handler crash. See "
        f"infrastructure/lambdas/_shared/smoke.sh."
    )


@pytest.mark.parametrize("script", _smoke_scripts(), ids=lambda p: p.parent.name)
def test_smoke_path_calls_a_crash_assertion(script: Path) -> None:
    """Sourcing the helper is not enough — it must be CALLED. The grandfather
    list may only shrink."""
    name = script.parent.name
    if name in GRANDFATHERED_NO_CRASH_CHECK:
        pytest.skip(f"{name} is grandfathered; the list may only shrink")
    body = script.read_text()
    assert any(call in body for call in _ASSERT_CALLS), (
        f"{name}/deploy.sh sources _shared/smoke.sh but never calls "
        f"{' or '.join(_ASSERT_CALLS)} — sourcing alone asserts nothing."
    )


def test_grandfather_list_names_only_real_scripts() -> None:
    """A stale grandfather entry hides a script that no longer exists, and
    makes the list look smaller than the debt it represents."""
    names = {p.parent.name for p in _smoke_scripts()}
    stale = GRANDFATHERED_NO_CRASH_CHECK - names
    assert not stale, (
        f"GRANDFATHERED_NO_CRASH_CHECK names script(s) that no longer have "
        f"a --smoke path (or no longer exist): {sorted(stale)}. Remove them "
        f"— the list may only shrink toward zero, never carry dead weight."
    )


def test_grandfather_list_entries_still_actually_lack_the_check() -> None:
    """The self-retiring half: an entry that stops tripping the gap it names
    must be REMOVED, not left as a permanent exemption nobody re-examines.
    This is what makes the list shrink instead of just not grow — it turns
    CI red the moment the gap it names is fixed (mirrors
    crucible-predictor/tests/test_no_inline_spot_bootstrap.py's
    _KNOWN_LIVE_LAUNCHER_GAP shape)."""
    by_name = {p.parent.name: p for p in _smoke_scripts()}
    for name in GRANDFATHERED_NO_CRASH_CHECK:
        script = by_name[name]
        body = script.read_text()
        already_fixed = "_shared/smoke.sh" in body and any(
            call in body for call in _ASSERT_CALLS
        )
        assert not already_fixed, (
            f"{name}/deploy.sh now sources _shared/smoke.sh and calls a "
            f"crash assertion — remove it from GRANDFATHERED_NO_CRASH_CHECK, "
            f"the gap it was tracking is closed."
        )


def _semantic_assertions(body: str) -> list[str]:
    return [call for call in _SEMANTIC_ASSERT_CALLS if call in body]


def _semantic_exemption(body: str) -> str | None:
    for line in body.splitlines():
        if _SEMANTIC_EXEMPTION_PREFIX in line:
            return line.split(_SEMANTIC_EXEMPTION_PREFIX, 1)[1].strip()
    return None


@pytest.mark.parametrize(
    "script",
    [p for p in _smoke_scripts() if p.parent.name in I7554_RESIDUAL_SCRIPTS],
    ids=lambda p: p.parent.name,
)
def test_crash_only_smoke_paths_are_reasoned_exemptions(script: Path) -> None:
    """A crash check alone must never read as semantic smoke coverage."""
    name = script.parent.name
    body = script.read_text()
    semantic = _semantic_assertions(body)
    reason = _semantic_exemption(body)
    if semantic:
        assert name not in REASONED_SEMANTIC_EXEMPTIONS, (
            f"{name} gained {semantic}; remove its self-retiring exemption"
        )
        assert reason is None, f"{name} has both a semantic assertion and an exemption"
        return

    assert name in REASONED_SEMANTIC_EXEMPTIONS, (
        f"{name} has only a crash assertion and no named semantic exemption"
    )
    assert reason == REASONED_SEMANTIC_EXEMPTIONS[name], (
        f"{name}'s script must carry its exact, reviewable exemption reason"
    )


def test_semantic_exemption_set_names_only_current_smoke_scripts() -> None:
    names = {path.parent.name for path in _smoke_scripts()}
    assert not REASONED_SEMANTIC_EXEMPTIONS.keys() - names


def test_i7554_residual_set_names_only_current_smoke_scripts() -> None:
    names = {path.parent.name for path in _smoke_scripts()}
    assert not I7554_RESIDUAL_SCRIPTS - names


@pytest.mark.parametrize(
    ("body", "semantic", "reason"),
    [
        ("assert_json_response_field response ok true done", True, None),
        ("assert_no_function_error invoke response", False, None),
        (
            "# SMOKE_SEMANTIC_EXEMPTION: cannot create the subject on demand.",
            False,
            "cannot create the subject on demand.",
        ),
        (
            "# SMOKE_SEMANTIC_EXEMPTION: obsolete\n"
            "assert_json_response_has_field response result done",
            True,
            "obsolete",
        ),
    ],
    ids=("semantic", "crash-only", "valid-exemption", "self-retiring-exemption"),
)
def test_semantic_smoke_classification_shapes(
    body: str, semantic: bool, reason: str | None
) -> None:
    """Pin all four guard shapes without deriving expectations from the guard."""
    assert bool(_semantic_assertions(body)) is semantic
    assert _semantic_exemption(body) == reason


def test_json_response_assertion_fails_loud_for_broken_handler(tmp_path: Path) -> None:
    """Local rehearsal: the shared assertion rejects a quiet false success."""
    response = tmp_path / "response.json"
    response.write_text('{"launched": false, "reason": "broken copy"}')
    helper = REPO / "infrastructure/lambdas/_shared/smoke.sh"
    command = (
        f"source {helper}; assert_json_response_field {response} launched true "
        "'dispatcher must launch'"
    )
    completed = subprocess.run(
        ["bash", "-c", command], capture_output=True, text=True, check=False
    )
    assert completed.returncode != 0
    assert "SMOKE FAILED: dispatcher must launch" in completed.stderr
