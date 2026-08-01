"""Binds every ``send-task-failure`` error name to LaunchGroomSpot's Catch.

Origin — alpha-engine-config-I5919, measured on the 2026-07-31 20:00 UTC cycle.

``LaunchGroomSpot``'s Catch was an ALLOWLIST of recoverable error names
(``States.Timeout``, ``LaneDeath``) with ``States.ALL`` routed to the terminal
``HandleFailure``. The default for any unenumerated name was therefore
TERMINATION. The on-box early spot-interruption watcher sends
``error="SpotInterrupted"`` — a name nobody had swept into the list — so every
spot reclamation it detected skipped the bounded-relaunch ladder entirely::

    TaskFailed(SpotInterrupted) -> HandleFailure -> RecordLaneFailure -> LaneCompleted

Both reclaimed lanes that cycle recorded zero relaunches and zero on-demand
escalations. That watcher fires on the IMDS interruption NOTICE (~2 min before
termination), so it always beats the lane reconciler's ``LaneDeath`` — the one
name that WAS wired was unreachable on the path that actually fires.

This is the same defect I5229/I5512 fixed for ``LaneDeath`` by adding a single
name to the allowlist. Adding one name does not survive the class: the next
producer to invent an error name reintroduces it silently, because nothing fails
when the producers and the catch list drift apart. They live in different repos.

Two guards, in order of what they protect:

  1. The catch DEFAULT must be recovery, not termination — ``States.ALL`` routes
     to the relaunch ladder and only an explicit terminal allowlist bypasses it.
  2. Every error name any producer actually sends must be accounted for: either
     on the terminal allowlist deliberately, or reaching the ladder.

Guard 1 is what makes the class safe going forward; guard 2 is what makes the
drift visible when someone re-inverts guard 1.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_groom.json"
_DISPATCHER = (
    _REPO_ROOT / "infrastructure" / "lambdas" / "scheduled-groom-dispatcher" / "index.py"
)
_INJECT_MOCK = (
    _REPO_ROOT / "infrastructure" / "lambdas" / "groom-inject-mock" / "index.py"
)

#: The state the bounded-relaunch ladder begins at. Reaching it means the lane
#: gets a completion-marker check and, if the work really was lost, a relaunch.
_LADDER_ENTRY = "CheckCompletionMarkerTaskToken"

#: The only error names that may legitimately bypass the ladder.
#:
#: ``GroomFailed`` — groom_spot_bootstrap.sh's finish() trap on a non-zero run
#: exit. The box ran; the RUN failed. A relaunch would repeat it on fresh
#: hardware and fail identically, burning the ladder against a deterministic
#: failure.
#:
#: ``InjectedCallbackRejected`` — the fault-injection harness deliberately
#: rejecting a callback. It must stay terminal or the injection loops.
_TERMINAL_ERRORS = {"GroomFailed", "InjectedCallbackRejected"}

#: Names the Step Functions service itself raises, which no producer file
#: contains as a literal.
_SERVICE_ERRORS = {"States.Timeout"}

#: `--error "Name"` (shell, bootstrap) and `error="Name"` / `error='Name'`
#: (boto3, Lambdas).
_ERROR_LITERAL = re.compile(
    r"""(?:--error\s+["']|(?<![A-Za-z_])error\s*=\s*["'])([A-Za-z][A-Za-z0-9_.]*)["']"""
)


@pytest.fixture(scope="module")
def lane_states() -> dict:
    return json.loads(_SF_PATH.read_text())["States"]["MapLaunches"]["ItemProcessor"]["States"]


@pytest.fixture(scope="module")
def catch(lane_states) -> list[dict]:
    return lane_states["LaunchGroomSpot"]["Catch"]


def _routes(catch: list[dict]) -> dict[str, str]:
    """Flatten the Catch into {error_name: destination_state}."""
    return {name: entry["Next"] for entry in catch for name in entry["ErrorEquals"]}


def test_catch_default_is_recovery_not_termination(catch):
    """I5919 — the inversion itself.

    ``States.ALL`` must reach the ladder. While it routed to HandleFailure,
    every error name nobody had swept was silently terminal.
    """
    routes = _routes(catch)
    assert "States.ALL" in routes, "LaunchGroomSpot must keep a States.ALL catch"
    assert routes["States.ALL"] == _LADDER_ENTRY, (
        "States.ALL must route to the relaunch ladder so an unenumerated error "
        "name defaults to RECOVERY. Routing it to a terminal state is the I5919 "
        "defect: SpotInterrupted was never enumerated and every reclaim it "
        "detected skipped the ladder."
    )


def test_states_all_catch_writes_the_timeout_error_field(catch):
    """groom-sweep-policy §2.2 — GroomRetriesExhausted reads $.timeoutError.

    Every catch reaching the ladder must write that SAME field. A differently
    named one leaves $.timeoutError absent on this path and raises an
    UNCATCHABLE States.Runtime downstream.
    """
    for entry in catch:
        if entry["Next"] == _LADDER_ENTRY:
            assert entry.get("ResultPath") == "$.timeoutError", (
                f"catch for {entry['ErrorEquals']} reaches the ladder but writes "
                f"{entry.get('ResultPath')!r}, not $.timeoutError"
            )


def test_terminal_allowlist_is_explicit_and_minimal(catch):
    """Only the deliberately-terminal names may bypass the ladder."""
    terminal = {
        name
        for entry in catch
        if entry["Next"] != _LADDER_ENTRY
        for name in entry["ErrorEquals"]
        if name != "States.ALL"
    }
    assert terminal == _TERMINAL_ERRORS, (
        f"terminal error names drifted: {terminal} != {_TERMINAL_ERRORS}. Adding "
        "a name here removes its relaunch coverage — justify it in the Catch "
        "Comment and update _TERMINAL_ERRORS in the same change."
    )


def test_terminal_names_are_ordered_before_the_states_all_catch(catch):
    """Step Functions evaluates Catch entries in order.

    A States.ALL entry placed first would swallow the terminal names and send a
    genuinely-failed run around the ladder forever.
    """
    names_in_order = [entry["ErrorEquals"] for entry in catch]
    all_index = next(i for i, names in enumerate(names_in_order) if "States.ALL" in names)
    for i, names in enumerate(names_in_order):
        if set(names) & _TERMINAL_ERRORS:
            assert i < all_index, (
                "terminal error names must be enumerated BEFORE the States.ALL "
                "catch or they never match"
            )


@pytest.mark.parametrize("path", [_DISPATCHER, _INJECT_MOCK])
def test_producer_source_is_readable(path: Path):
    """Fail loud rather than vacuously pass.

    A source-scanning contract test that silently passes when its input is
    missing is worse than no test — it reports coverage it does not have.
    """
    assert path.is_file(), f"producer source not found: {path}"
    assert path.read_text().strip(), f"producer source is empty: {path}"


def test_every_producer_error_name_is_accounted_for(catch):
    """Guard 2 — no producer name is unclassified.

    Scans the in-repo producers for ``send_task_failure`` error literals. The
    on-box bootstrap (``alpha-engine-config/infrastructure/groom_spot_bootstrap.sh``,
    the producer of ``SpotInterrupted`` and ``GroomFailed``) lives in another
    repo and is NOT readable from here — which is precisely why the inversion
    above is the primary guard rather than this scan. This asserts the names
    this repo can see; the inversion covers the ones it cannot.
    """
    found: set[str] = set()
    for path in (_DISPATCHER, _INJECT_MOCK):
        found |= set(_ERROR_LITERAL.findall(path.read_text()))
    # Names unrelated to the task-token contract (e.g. a boto3 kwarg elsewhere)
    # are still fine: everything not terminal reaches the ladder by default.
    unclassified = {
        n for n in found
        if n not in _TERMINAL_ERRORS and n not in _SERVICE_ERRORS
    }
    routes = _routes(catch)
    for name in unclassified:
        destination = routes.get(name, routes["States.ALL"])
        assert destination == _LADDER_ENTRY, (
            f"producer error name {name!r} routes to {destination!r}, which is "
            "not the relaunch ladder and is not on the terminal allowlist"
        )
