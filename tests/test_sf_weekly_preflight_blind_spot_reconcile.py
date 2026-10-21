"""The weekly blind spot is retired only by the pass that closed it
(alpha-engine-config-I12481).

``WeeklyPreflight`` (the Lambda) skips the arctic / repo_modules / polygon /
checkout checks BY DESIGN and records them on ``$.weekly_preflight_blind_spot``.
``WeeklyPreflightOnSpot`` re-runs exactly those checks on the weekly box later
in the same execution. Until I12481 nothing joined the two, so the 2026-10-16
completion marker reported ``present: true`` with eight "unreachable" required
checks, and the on-spot artifact for that same execution showed all eight
``status: ok``.

These tests pin four things:

1. WIRING: the reconcile sits only on the on-spot OK arm and every exit
   rejoins CheckShellRun.
2. GUARDS: each dereference is IsPresent-guarded, so an older Lambda alias
   or an absent field falls to Default. The blind spot is never retired
   on a guess.
3. MATCH: the StringMatches pattern accepts the module's real OK verdict
   line and rejects BLIND_SPOT, ERROR and FAIL.
4. ONE PREDICATE: the set the Lambda calls "covered on spot" is the set the
   box actually runs.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import sf_preflight as sp
import sf_preflight_on_spot as spot

_DEF = Path(__file__).resolve().parent.parent / "infrastructure" / "step_function.json"
RECONCILE = "ReconcileWeeklyPreflightBlindSpot"
COVERED = "WeeklyPreflightBlindSpotCoveredOnSpot"

# The eight required checks the 2026-10-16 completion marker named as
# unreachable, which that execution's on-spot pass ran and found ok.
OBSERVED_2026_10_16 = {
    "arctic_connectivity", "constituents_fetch", "universe_drift",
    "universe_sample_freshness", "polygon_grouped_coverage",
    "predicted_missing_from_closes", "backfill_source_freshness",
    "tool_contracts",
}


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_DEF.read_text())["States"]


def _succ(state: dict) -> list[str]:
    out = [state[k] for k in ("Next", "Default") if k in state]
    out += [c["Next"] for c in state.get("Choices", [])]
    out += [c["Next"] for c in state.get("Catch", [])]
    return out


def _asl_string_matches(value: str, pattern: str) -> bool:
    """Step Functions ``StringMatches``: ``*`` is the only wildcard, ``\\*``
    is a literal asterisk, and the whole string must match."""
    parts = re.split(r"(\\\*|\*)", pattern)
    rx = "".join(
        ".*" if p == "*" else re.escape("*") if p == "\\*" else re.escape(p)
        for p in parts
    )
    return re.fullmatch(rx, value, flags=re.DOTALL) is not None


def _rule(states):
    choices = states[RECONCILE]["Choices"]
    assert len(choices) == 1, "exactly one retiring arm"
    return choices[0]


# ── 1. wiring ───────────────────────────────────────────────────────────────


def test_reconcile_is_entered_only_from_the_on_spot_ok_arm(states):
    entries = {n for n, st in states.items() if RECONCILE in _succ(st)}
    assert entries == {"RecordWeeklyPreflightOnSpot"}
    assert {n for n, st in states.items() if COVERED in _succ(st)} == {RECONCILE}


def test_every_exit_rejoins_check_shell_run(states):
    assert states[RECONCILE]["Type"] == "Choice"
    assert states[RECONCILE]["Default"] == "CheckShellRun"
    assert _rule(states)["Next"] == COVERED
    assert states[COVERED]["Type"] == "Pass"
    assert states[COVERED]["Next"] == "CheckShellRun"


def test_fail_and_unobserved_arms_never_retire_it(states):
    """An observed FAIL halts, and an unobserved pass closed nothing. Neither
    one may reach the covered arm."""
    for start in ("RecordWeeklyPreflightOnSpotFail", "WeeklyPreflightOnSpotUnobserved"):
        seen, todo = set(), [start]
        while todo:
            cur = todo.pop()
            if cur in seen or cur not in states or cur == "CheckShellRun":
                continue
            seen.add(cur)
            todo.extend(_succ(states[cur]))
        assert RECONCILE not in seen and COVERED not in seen, start


# ── 2. guards ───────────────────────────────────────────────────────────────


def test_every_dereference_is_is_present_guarded_first(states):
    leaves = _rule(states)["And"]
    guarded: set[str] = set()
    for leaf in leaves:
        var = leaf["Variable"]
        if leaf.get("IsPresent") is True:
            guarded.add(var)
        else:
            assert var in guarded, f"{var} is compared before its IsPresent guard"
    assert guarded == {
        "$.weekly_preflight_blind_spot_cause",
        "$.weekly_preflight_result.Payload.required_skips_on_spot_coverage",
        "$.weekly_preflight_on_spot.verdict_line",
    }


def test_all_three_conditions_are_required(states):
    leaves = _rule(states)["And"]
    by = {(l["Variable"], k): v for l in leaves for k, v in l.items() if k != "Variable"}
    assert by[("$.weekly_preflight_result.Payload.required_skips_on_spot_coverage", "StringEquals")] == "ALL"
    assert ("$.weekly_preflight_on_spot.verdict_line", "StringMatches") in by


def test_blind_spot_cause_is_written_only_by_the_blind_spot_arm(states):
    """Guard (1) proves the blind-spot arm ran only if nothing else, the
    InitializeInput floor included, writes that field."""
    writers = {n for n, st in states.items()
               if st.get("ResultPath") == "$.weekly_preflight_blind_spot_cause"}
    assert writers == {"WeeklyPreflightBlindSpotFromProbe"}
    merged = states["InitializeInput"]["Parameters"]["merged.$"]
    assert "weekly_preflight_blind_spot_cause" not in merged


def test_no_boolean_fact_is_introduced(states):
    """infrastructure/sf_reachability.py tracks BooleanEquals facts by
    top-level field. A fact on weekly_preflight_result would stay live from
    WeeklyPreflightGate all the way here and blow its context bound (measured:
    test_sf_field_reachability raised at 200,000 contexts)."""
    assert "BooleanEquals" not in json.dumps(states[RECONCILE]["Choices"])


def test_lambda_payload_field_name_matches_the_handler():
    handler = (
        Path(__file__).resolve().parent.parent
        / "infrastructure" / "lambdas" / "weekly-preflight" / "index.py"
    ).read_text()
    assert '"required_skips_on_spot_coverage":' in handler
    assert 'coverage = "ALL"' in handler


def test_covered_arm_reads_only_fields_every_entry_wrote(states):
    """A ``.$`` path that is absent fails the whole execution, because Pass
    states have no Catch. Every path the covered arm reads must be written
    by the state that recorded it: the declared blind spot, plus the on-spot
    record that reached this arm."""
    declared = states["WeeklyPreflightBlindSpotDeclared"]["Parameters"]
    record = states["RecordWeeklyPreflightOnSpot"]["Parameters"]
    for key, path in states[COVERED]["Parameters"].items():
        if not key.endswith(".$"):
            continue
        field = path.rsplit(".", 1)[1]
        if path.startswith("$.weekly_preflight_blind_spot."):
            assert field in declared or f"{field}.$" in declared, path
        elif path.startswith("$.weekly_preflight_on_spot."):
            assert field in record or f"{field}.$" in record, path
        else:
            pytest.fail(f"unexpected source {path}")


def test_covered_arm_shape(states):
    st = states[COVERED]
    p = st["Parameters"]
    assert st["ResultPath"] == "$.weekly_preflight_blind_spot"
    assert p["present"] is False
    assert p["required_skip_count"] == 0 and p["required_skip_names"] == []
    assert p["covered_by"] == "WeeklyPreflightOnSpot"
    # The Lambda's skip list survives, so "nothing skipped" and "skipped
    # there, run here" stay distinguishable on the completion marker.
    assert p["lambda_required_skip_names.$"] == "$.weekly_preflight_blind_spot.required_skip_names"
    assert p["lambda_required_skip_count.$"] == "$.weekly_preflight_blind_spot.required_skip_count"
    for f in ("ran_count", "skip_count", "warn_count"):
        assert f"{f}.$" in p, f


# ── 3. the verdict-line match ───────────────────────────────────────────────


def _line(verdict: str, **extra) -> str:
    record = {
        "mode": spot.MODE, "verdict": verdict, "ran_count": 10, "fail_count": 0,
        "required_skip_count": 0, "group_required_skip_count": 0,
        "group_required_skip_names": [], "fail_names": [], "blocked_names": [],
        "expected_skip_names": [], "elapsed_seconds": 6.7, **extra,
    }
    return spot.verdict_line(record, "s3://b/health/weekly_preflight_on_spot/d/e.json")


@pytest.mark.parametrize("trailer", ["", "\n"])
def test_pattern_accepts_the_real_ok_line(states, trailer):
    pattern = _rule(states)["And"][-1]["StringMatches"]
    assert _asl_string_matches(_line("OK") + trailer, pattern)


@pytest.mark.parametrize("verdict", ["BLIND_SPOT", "ERROR", "FAIL", "OKAY", "ok"])
def test_pattern_rejects_every_other_verdict(states, verdict):
    pattern = _rule(states)["And"][-1]["StringMatches"]
    assert not _asl_string_matches(_line(verdict), pattern)


def test_pattern_rejects_empty_and_garbage(states):
    pattern = _rule(states)["And"][-1]["StringMatches"]
    for value in ("", "Traceback (most recent call last):", '{"verdict":"OK"}'):
        # The last one has no trailing comma: verdict_line always writes more
        # keys after verdict, so a bare object is not this module's output.
        assert not _asl_string_matches(value, pattern), value


# ── 4. one predicate for both passes ────────────────────────────────────────


def test_on_spot_names_are_exactly_what_the_box_runs():
    assert sp.on_spot_check_names() == {
        fn.__name__[len("check_"):] for fn in spot._spot_checks()
    }


def test_no_lambda_runnable_check_is_claimed_on_spot():
    for fn in sp.CHECKS:
        name = fn.__name__[len("check_"):]
        lambda_runnable = sp.CHECK_CAPABILITIES[fn.__name__] <= sp.LAMBDA_CAPABILITIES
        assert (name in sp.on_spot_check_names()) is (not lambda_runnable), name


def test_the_2026_10_16_blind_spot_is_coverable():
    assert OBSERVED_2026_10_16 <= sp.on_spot_check_names()


def test_every_required_check_runs_in_exactly_one_pass():
    """Nothing came off the required list: every required check is still
    run, either by the Lambda or by the box."""
    for fn in sp.CHECKS:
        assert sp.CHECK_REQUIRED[fn.__name__] in (True, False)
    required = {fn.__name__[len("check_"):] for fn in sp.CHECKS if sp.CHECK_REQUIRED[fn.__name__]}
    lambda_side = {
        fn.__name__[len("check_"):] for fn in sp.CHECKS
        if sp.CHECK_CAPABILITIES[fn.__name__] <= sp.LAMBDA_CAPABILITIES
    }
    assert required <= lambda_side | sp.on_spot_check_names()
    assert OBSERVED_2026_10_16 <= required
