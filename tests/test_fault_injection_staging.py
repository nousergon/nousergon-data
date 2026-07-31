"""tests/test_fault_injection_staging.py — the staging definition must be
DERIVED from production, and the two dangerous swaps must be impossible to
forget (alpha-engine-config-I5718).

The harness's entire value rests on the staging topology being the production
topology. These tests guard the derivation, not the AWS calls.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MOCK_DIR = REPO_ROOT / "infrastructure" / "lambdas" / "groom-inject-mock"
sys.path.insert(0, str(MOCK_DIR))

from staging_definition import (  # noqa: E402
    MIN_TIMEOUT_SECONDS,
    MOCK_LAMBDA,
    PROD_LAMBDA,
    TIMEOUT_DIVISOR,
    build_staging_definition,
    load_prod_definition,
)

ACCOUNT, REGION = "711398986525", "us-east-1"


@pytest.fixture(scope="module")
def prod() -> dict:
    return load_prod_definition(REPO_ROOT)


@pytest.fixture(scope="module")
def staging(prod) -> dict:
    return build_staging_definition(prod, account_id=ACCOUNT, region=REGION)


def test_no_production_lambda_survives_the_swap(staging):
    """The single most dangerous omission: a staging run launching real boxes."""
    assert PROD_LAMBDA not in json.dumps(staging), (
        "staging definition still references the PRODUCTION dispatcher — an "
        "injection run would launch real spot boxes against the real backlog"
    )
    assert MOCK_LAMBDA in json.dumps(staging)


def test_no_production_sns_topic_survives_the_swap(staging):
    """The most obnoxious omission: every injection run paging Brian."""
    raw = json.dumps(staging)
    assert "alpha-engine-alerts" not in raw, (
        "staging definition still publishes to the real alerts topic — every "
        "scheduled injection run would page the operator"
    )
    assert "alpha-engine-groom-inject-alerts" in raw


def test_topology_is_preserved_exactly(prod, staging):
    """Same states, same order, same transitions — only the three swaps."""
    assert set(prod["States"]) == set(staging["States"])

    def transitions(defn):
        out = {}
        for name, state in defn["States"].items():
            out[name] = (state.get("Type"), state.get("Next"),
                         state.get("End"), json.dumps(state.get("Choices")))
        return out

    assert transitions(prod) == transitions(staging), (
        "a state's type, Next, End or Choices changed — the staging machine is "
        "no longer the production topology and the harness proves nothing"
    )


def test_timeouts_scale_but_preserve_ordering(prod, staging):
    """Recovery logic branches on which budget fires FIRST — order must hold."""
    def timeouts(node, acc=None):
        acc = acc if acc is not None else []
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "TimeoutSeconds" and isinstance(v, (int, float)):
                    acc.append(int(v))
                else:
                    timeouts(v, acc)
        elif isinstance(node, list):
            for item in node:
                timeouts(item, acc)
        return acc

    p, s = timeouts(prod), timeouts(staging)
    assert len(p) == len(s) and p, "timeout count changed"
    assert all(v >= MIN_TIMEOUT_SECONDS for v in s), (
        f"a scaled timeout fell below {MIN_TIMEOUT_SECONDS}s — the harness "
        "would go flaky on cold starts, which is how it gets switched off"
    )
    # The lane budget must still exceed the short task budgets, or the
    # timeout-then-relaunch path stops being reachable.
    assert max(s) > min(s)
    for prod_v, stg_v in zip(p, s):
        assert stg_v == max(MIN_TIMEOUT_SECONDS, prod_v // TIMEOUT_DIVISOR)


def test_lane_timeout_is_fast_enough_to_actually_run(staging):
    """A harness nobody can wait for is a harness that never runs."""
    lane = None
    for state in staging["States"].values():
        if state.get("Type") == "Map":
            body = state.get("ItemProcessor") or state.get("Iterator")
            lane = body["States"]["LaunchGroomSpot"]["TimeoutSeconds"]
    assert lane is not None, "LaunchGroomSpot not found in the Map body"
    assert lane <= 60, (
        f"staging lane timeout is {lane}s; three relaunch attempts would take "
        "over three minutes and the weekly programme would be skipped"
    )


def test_builder_refuses_a_definition_it_cannot_make_safe():
    """If either swap finds nothing, FAIL — never emit a half-swapped machine."""
    with pytest.raises(ValueError, match="names no"):
        build_staging_definition(
            {"States": {"X": {"Type": "Pass", "End": True}}},
            account_id=ACCOUNT, region=REGION)


def test_every_expectation_names_a_declared_scenario():
    """The driver may not assert on a scenario the mock cannot produce."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "fault_injection_run", REPO_ROOT / "scripts" / "fault_injection_run.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    mock_spec = importlib.util.spec_from_file_location(
        "inject_mock", MOCK_DIR / "index.py")
    mock = importlib.util.module_from_spec(mock_spec)
    mock_spec.loader.exec_module(mock)

    undeclared = set(mod.EXPECTATIONS) - set(mock.SCENARIOS)
    assert not undeclared, (
        f"driver expects scenarios the mock cannot produce: {sorted(undeclared)}"
    )
    # The reverse is a coverage hole, not an error — but it must be VISIBLE,
    # and the driver reports it as `unverified` rather than skipping it.
    uncovered = set(mock.SCENARIOS) - set(mod.EXPECTATIONS)
    assert uncovered == set(), (
        f"mock declares scenarios with no driver expectation: {sorted(uncovered)} "
        "— add an expectation or remove the scenario"
    )


def test_every_expectation_asserts_a_state_not_just_a_status():
    """`terminal` alone would pass for a machine that skipped every recovery."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "fault_injection_run", REPO_ROOT / "scripts" / "fault_injection_run.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    for name, exp in mod.EXPECTATIONS.items():
        assert exp.get("must_visit"), (
            f"{name} asserts only a terminal status — that would pass for an "
            "execution that never entered a single recovery state"
        )
        assert exp.get("why"), f"{name} has no stated rationale"


# ── I5718: the defect the harness's first successful run found ───────────────


def test_every_choice_guards_the_path_it_reads(prod):
    """ASL's `And` does not short-circuit path validity.

    `CheckMapLaneOutcomes` guarded `$.mapOutcome[N]` with IsPresent and then
    read `$.mapOutcome[N].laneOutcome.laneFailed` with BooleanEquals. But
    `laneOutcome` is written ONLY by `RecordLaneFailure`, so a lane that
    SUCCEEDED left that path absent and the comparison raised States.Runtime —
    uncatchable by any Catch (groom-sweep-policy §2.2). A cycle in which every
    lane succeeded could not complete.

    It survived because a fully-successful Map has never occurred under this
    definition: lost callbacks (alpha-engine-config-I4987) meant lanes timed
    out, and the Map's Catch bypasses this state entirely. Fixing the callback
    defect would have surfaced this immediately, in production.

    Generalised to every Choice so the next one cannot repeat it: any variable
    compared with a value comparator must ALSO be guarded by IsPresent on the
    SAME path, unless it is a top-level field the state is guaranteed to have.
    """
    value_comparators = {
        "BooleanEquals", "StringEquals", "NumericEquals", "NumericGreaterThan",
        "NumericLessThan", "StringMatches", "NumericGreaterThanEquals",
        "NumericLessThanEquals",
    }

    def choices_of(states):
        for name, state in states.items():
            if state.get("Type") == "Choice":
                yield name, state.get("Choices", [])
            if state.get("Type") == "Map":
                body = state.get("ItemProcessor") or state.get("Iterator")
                yield from choices_of(body["States"])

    offenders = []
    for state_name, choices in choices_of(prod["States"]):
        for choice in choices:
            conds = choice.get("And") or choice.get("Or") or [choice]
            guarded = {c["Variable"] for c in conds
                       if isinstance(c, dict) and c.get("IsPresent") is True}
            for cond in conds:
                if not isinstance(cond, dict) or "Variable" not in cond:
                    continue
                var = cond["Variable"]
                if not (value_comparators & set(cond)):
                    continue
                # Nested paths are the risk; a bare `$.field` on a state's own
                # guaranteed input is not.
                if var.count(".") >= 2 and var not in guarded:
                    offenders.append(f"{state_name}: {var}")

    assert not offenders, (
        "Choice condition(s) compare a nested path with no IsPresent guard on "
        f"that same path — States.Runtime, uncatchable: {offenders}"
    )
