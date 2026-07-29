"""Exercise runs must not draw the Saturday sf-watch dispatch budget.

alpha-engine-config-I5502. `cadence_slug` is a FROZEN PER-PIPELINE label, so
every run of ne-weekly-freshness-pipeline resolves to "saturday" — including
the weekday EXERCISE runs that alpha-engine-config-I5489 chains off postclose.
The budget is keyed on (cadence, pipeline, run_date), so a new run_date every
trading day would REFILL the Saturday ceiling of 8 daily: up to 40 agent
dispatches a week where 8 was ruled, each a spot box plus a frontier-model run,
against a pipeline that currently fails ~4 runs in 5.

These pin the discriminator (the execution's own `pipeline_role`) and the
fail-safe direction (unknown/unreadable → conservative, never permissive).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_INDEX = (
    Path(__file__).resolve().parent.parent
    / "infrastructure"
    / "lambdas"
    / "saturday-sf-watch-dispatcher"
    / "index.py"
)


@pytest.fixture(scope="module")
def dispatcher():
    """Import index.py with its Lambda-only deps stubbed.

    The sibling lockstep tests parse this file with `ast` precisely to avoid
    this import. That is right for asserting on literals, but the functions
    here have BEHAVIOUR worth exercising — normalisation, fail-soft parsing,
    the ceiling decision — and an AST test of a branch is not a test of what
    the branch does. `flow_doctor_telegram` ships in the Lambda bundle, not
    in the test environment; it is only used for notification side effects,
    none of which these pure functions touch.
    """
    import types

    stub = types.ModuleType("flow_doctor_telegram")
    stub.notify_via_flow_doctor = lambda *a, **k: None  # never called by the functions under test
    stub.send_message = lambda *a, **k: None
    sys.modules["flow_doctor_telegram"] = stub

    spec = importlib.util.spec_from_file_location("_sf_watch_dispatcher_under_test", _INDEX)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _describe(payload: dict | None) -> dict:
    """An SF describe-execution response carrying `payload` as its input."""
    return {} if payload is None else {"input": json.dumps(payload)}


# ── the discriminator ───────────────────────────────────────────────────────


def test_exercise_run_gets_the_conservative_ceiling(dispatcher):
    """The whole point: a weekday exercise failure of the weekly pipeline
    must NOT be handed Saturday's 8."""
    assert dispatcher._max_dispatches("saturday", "exercise") == dispatcher._DEFAULT_MAX_DISPATCHES
    assert dispatcher._max_dispatches("saturday", "exercise") < dispatcher.SF_WATCH_MAX_DISPATCHES["saturday"]


def test_the_real_saturday_run_is_unchanged(dispatcher):
    """The fix must not quietly shrink the budget the week's actual belief
    refresh was ruled to have."""
    assert dispatcher._max_dispatches("saturday", "weekly") == dispatcher.SF_WATCH_MAX_DISPATCHES["saturday"]
    assert dispatcher._max_dispatches("saturday", "") == dispatcher.SF_WATCH_MAX_DISPATCHES["saturday"]


def test_manual_and_recovery_invocations_keep_their_budget(dispatcher):
    """Manual / recovery / watch-rerun executions carry other roles (or none)
    and are not exercise runs — they must not be caught by this."""
    for role in ("", "operator-replay", "recovery", "watch-rerun"):
        assert dispatcher._max_dispatches("saturday", role) == dispatcher.SF_WATCH_MAX_DISPATCHES["saturday"]


def test_other_pipelines_are_untouched(dispatcher):
    for slug in ("weekday", "eod"):
        assert dispatcher._max_dispatches(slug, "") == dispatcher.SF_WATCH_MAX_DISPATCHES[slug]
        assert dispatcher._max_dispatches(slug, "exercise") == dispatcher._DEFAULT_MAX_DISPATCHES


def test_unruled_cadence_still_gets_the_conservative_default(dispatcher):
    """Pre-existing behaviour, re-pinned: a future pipeline registered before
    its budget is ruled must never inherit Saturday's."""
    assert dispatcher._max_dispatches("some-future-cadence", "") == dispatcher._DEFAULT_MAX_DISPATCHES


# ── reading the role off the execution ──────────────────────────────────────


def test_role_is_read_from_the_execution_input(dispatcher):
    assert dispatcher._pipeline_role(_describe({"pipeline_role": "exercise"})) == "exercise"
    assert dispatcher._pipeline_role(_describe({"pipeline_role": "weekly"})) == "weekly"


def test_role_is_normalised(dispatcher):
    """The SF Input is hand-editable on a manual rerun; casing/whitespace must
    not silently hand an exercise run the Saturday budget."""
    assert dispatcher._pipeline_role(_describe({"pipeline_role": "  EXERCISE  "})) == "exercise"
    assert dispatcher._max_dispatches("saturday", dispatcher._pipeline_role(
        _describe({"pipeline_role": "Exercise"})
    )) == dispatcher._DEFAULT_MAX_DISPATCHES


@pytest.mark.parametrize(
    "resp",
    [
        None,
        {},
        {"input": ""},
        {"input": "not json"},
        {"input": json.dumps({})},
        {"input": json.dumps({"pipeline_role": None})},
        {"input": json.dumps({"pipeline_role": 7})},
    ],
)
def test_unreadable_input_never_raises_and_never_downgrades_saturday(dispatcher, resp):
    """Fail-soft in the SAFE direction. This runs inside the dispatcher's hot
    path; an exception here would take out the whole response plane for a
    pipeline that is already failing. A missing role means 'not known to be an
    exercise run', which leaves the Saturday budget intact rather than
    silently capping the real weekly run at 2.
    """
    role = dispatcher._pipeline_role(resp)
    assert role == ""
    assert dispatcher._max_dispatches("saturday", role) == dispatcher.SF_WATCH_MAX_DISPATCHES["saturday"]


def test_conservative_default_is_actually_smaller(dispatcher):
    """Guards the constants themselves — if someone raises _DEFAULT_MAX_DISPATCHES
    to 8, every assertion above still passes while the defect returns."""
    assert dispatcher._DEFAULT_MAX_DISPATCHES < dispatcher.SF_WATCH_MAX_DISPATCHES["saturday"]
