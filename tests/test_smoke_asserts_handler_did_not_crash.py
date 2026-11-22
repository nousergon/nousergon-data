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
    "assert_json_response_field_equals",
    "assert_json_response_field_present",
)

# These smoke payloads deliberately cannot drive the handler's successful
# side-effect path on demand.  Each script must carry the same marker and a
# concrete explanation.  This named debt may only shrink; the tests below
# reject missing/stale reasons and self-retire an exemption when an assertion
# is added.
REASONED_NO_SEMANTIC_ASSERTION: frozenset[str] = frozenset(
    {
        "alert-drain-liveness-probe",
        "canary-replay-liveness-probe",
        "eod-backstop",
        "eod-precondition-probe",
        "preflight-sweep-dispatcher",
    }
)
_SEMANTIC_EXEMPTION = "SMOKE_SEMANTIC_EXEMPTION:"

# Paths that already had bespoke semantic checks before this residual was
# opened.  Keeping them named prevents a newly-added crash-only path from
# being silently treated as complete merely because it is not in the residual
# list; additions must use the shared helper or be deliberately classified.
ESTABLISHED_BESPOKE_SEMANTIC_ASSERTIONS: frozenset[str] = frozenset(
    {
        "changelog-cloudwatch-mirror",
        "ci-watch-dispatcher",
        "crypto-balances",
        "eod-snapshot-existence-check",
        "eod-success-friday-shell-trigger",
        "expense-collector",
        "freshness-monitor",
        "overseer-liveness-probe",
        "preopen-deploy-readiness-probe",
        "scheduled-groom-dispatcher",
        "sf-watch-spot-dispatcher",
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


def _assert_semantic_classified(name: str, body: str) -> None:
    if name in REASONED_NO_SEMANTIC_ASSERTION:
        marker_lines = [line for line in body.splitlines() if _SEMANTIC_EXEMPTION in line]
        assert len(marker_lines) == 1 and marker_lines[0].split(_SEMANTIC_EXEMPTION, 1)[1].strip(), (
            f"{name}/deploy.sh is exempt but lacks one non-empty "
            f"{_SEMANTIC_EXEMPTION} reason"
        )
        assert not any(call in body for call in _SEMANTIC_ASSERT_CALLS), (
            f"{name}/deploy.sh gained a semantic assertion; remove its exemption"
        )
    elif name not in ESTABLISHED_BESPOKE_SEMANTIC_ASSERTIONS:
        assert any(call in body for call in _SEMANTIC_ASSERT_CALLS), (
            f"{name}/deploy.sh is a crash-only smoke path: add a handler-specific "
            "shared semantic assertion or a reviewed reasoned exemption"
        )


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


@pytest.mark.parametrize("script", _smoke_scripts(), ids=lambda p: p.parent.name)
def test_smoke_path_has_semantic_assertion_or_reasoned_exemption(script: Path) -> None:
    name = script.parent.name
    _assert_semantic_classified(name, script.read_text())


def test_semantic_guard_accepts_shared_assertion() -> None:
    _assert_semantic_classified("new-handler", "assert_json_response_field_present response ok")


def test_semantic_guard_rejects_crash_only_path() -> None:
    with pytest.raises(AssertionError, match="crash-only"):
        _assert_semantic_classified("new-handler", "assert_no_function_error invoke response")


def test_semantic_guard_accepts_reasoned_exemption() -> None:
    name = next(iter(REASONED_NO_SEMANTIC_ASSERTION))
    _assert_semantic_classified(name, "# SMOKE_SEMANTIC_EXEMPTION: cannot force live state")


def test_semantic_guard_self_retires_exemption() -> None:
    name = next(iter(REASONED_NO_SEMANTIC_ASSERTION))
    body = "\n".join(
        (
            "# SMOKE_SEMANTIC_EXEMPTION: cannot force live state",
            "assert_json_response_field_present response ok",
        )
    )
    with pytest.raises(AssertionError, match="remove its exemption"):
        _assert_semantic_classified(name, body)


def test_semantic_exemptions_name_only_smoke_scripts() -> None:
    names = {path.parent.name for path in _smoke_scripts()}
    assert not REASONED_NO_SEMANTIC_ASSERTION - names
    assert not ESTABLISHED_BESPOKE_SEMANTIC_ASSERTIONS - names


def test_semantic_exemptions_self_retire_when_assertion_is_added() -> None:
    by_name = {path.parent.name: path for path in _smoke_scripts()}
    fixed = {
        name
        for name in REASONED_NO_SEMANTIC_ASSERTION
        if any(call in by_name[name].read_text() for call in _SEMANTIC_ASSERT_CALLS)
    }
    assert not fixed, f"remove fixed semantic exemption(s): {sorted(fixed)}"


_SMOKE_HELPER = REPO / "infrastructure" / "lambdas" / "_shared" / "smoke.sh"


def _run_semantic_helper(
    tmp_path: Path, response: str, *call: str
) -> subprocess.CompletedProcess[str]:
    """Source the shared helper and run one semantic assertion against a
    local response file, exactly as a deploy.sh --smoke path does. No AWS."""
    resp = tmp_path / "response.json"
    resp.write_text(response, encoding="utf-8")
    return subprocess.run(
        ["bash", "-c", 'source "$1"; shift; "$@"', "smoke-test", str(_SMOKE_HELPER),
         call[0], str(resp), *call[1:]],
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("response", "call", "ok"),
    [
        ('{"launched": true}', ("assert_json_response_field_equals", "launched", "true"), True),
        # The broken-handler shape: a quiet ``launched: false`` is not success.
        ('{"launched": false, "reason": "disabled"}', ("assert_json_response_field_equals", "launched", "true"), False),
        # JSON-typed comparison: the string "true" is not the boolean true.
        ('{"launched": "true"}', ("assert_json_response_field_equals", "launched", "true"), False),
        ('{"status": "Registering"}', ("assert_json_response_field_equals", "status", '"Registering"'), True),
        ('{"sent": 0}', ("assert_json_response_field_equals", "sent", "1"), False),
        ('null', ("assert_json_response_field_equals", "sent", "1"), False),
        ('not json', ("assert_json_response_field_equals", "sent", "1"), False),
        ('{"checked_at": "2026-11-21T00:00:00+00:00"}', ("assert_json_response_field_present", "checked_at"), True),
        ('{"checked_at": ""}', ("assert_json_response_field_present", "checked_at"), False),
        ('{"checked_at": null}', ("assert_json_response_field_present", "checked_at"), False),
        ('{}', ("assert_json_response_field_present", "checked_at"), False),
    ],
)
def test_semantic_helpers_execute_against_handler_responses(
    tmp_path: Path, response: str, call: tuple[str, ...], ok: bool
) -> None:
    """Executes the shared helpers (the acceptance's local broken-handler
    demonstration): a contract response passes, every broken one exits
    non-zero with a SMOKE FAILED diagnostic."""
    result = _run_semantic_helper(tmp_path, response, *call)
    if ok:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0
        assert "SMOKE FAILED" in result.stderr
