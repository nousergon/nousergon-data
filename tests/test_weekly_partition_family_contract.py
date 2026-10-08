"""Every weekly keying site names the date family it means.

``alpha-engine-config-I8809``.

## The defect this pins

One weekly cycle was written across TWO S3 date partitions.
``InitializeInput`` stamped ``run_date = date($$.Execution.StartTime)`` — the
CALENDAR date — and each consumer then decided for itself whether to normalise
it to the trading day. MEASURED (``AWS_PROFILE=ne-admin``, 2026-08-27) on the
2026-08-22 cycle::

    _stage_coverage/2026-08-21/  -> 28 verdicts
    _stage_coverage/2026-08-22/  -> 11 verdicts

Same cycle. ``lambdas/weekly-coverage-sweep`` reads ONE partition, so its first
production firing (2026-08-29 09:00 UTC) would have reported ~28 stages
``absent`` — the row state ``nousergon_lib.pipeline_status.coverage`` documents
as paging with no threshold.

## Why a comment is the enforceable artifact

There is no machine-readable place on an ASL state to record "this date means
the cycle" versus "this date means the firing". The two are indistinguishable
by inspection and the difference is exactly what produced the split. So the
Comment carries the declaration, and this test makes an UNDECLARED site fail —
including a site added later, which is the case that matters. The registry's
side of the same contract lives in ``alpha-engine-config``
(``ARTIFACT_REGISTRY.yaml``'s ``partition_family`` field and its own test).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_SF = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"
_MARK = "PARTITION FAMILY ="
_FAMILIES = ("trading_day", "calendar_date")

#: Sites that MUST read the execution's own wall-clock day, not the cycle's
#: trading day. Hardcoded, not derived: each is a considered decision, and a
#: state silently joining or leaving this set is the regression.
_CALENDAR_SITES = {
    # 'was YESTERDAY the week's last trading session' — pure calendar math,
    # and the trading day is the answer it computes, never its input.
    "WeeklyRunDayGate",
    # The legacy copies of the envelope marker. DELETED at the 2026-09-05
    # cutover — see nousergon_lib.pipeline_status.partition.CUTOVER_DATE.
    "WriteCompletionMarkerCalendar",
    "WriteCompletionMarkerDegradedCalendar",
    # An S3 LastModified is a wall-clock write time. Against the trading day
    # these comparisons become strictly WEAKER on every Saturday run.
    "CheckUniverseMembershipFresh",
    # CheckPredictorSkipWeightsFresh LEFT this set with alpha-engine-config-I12050:
    # it no longer compares a LastModified at all. It reads the arena verdict
    # keyed by the trading day (see test_predictor_skip_guard_is_trading_day_keyed).
    # The converter itself — calendar_date IN, trading_day OUT. The only site
    # in the graph that legitimately touches both.
    "NormalizeRunDates",
}


def _load() -> dict:
    return json.loads(_SF.read_text())


def _flatten(container: dict, out: dict) -> None:
    for name, state in (container.get("States") or {}).items():
        out[name] = state
        for branch in state.get("Branches") or []:
            _flatten(branch, out)
        for nested in ("ItemProcessor", "Iterator"):
            if nested in state:
                _flatten(state[nested], out)


def _states() -> dict:
    out: dict = {}
    _flatten(_load(), out)
    return out


def _touches_a_run_date(state: dict) -> bool:
    """Any reference to a run-date variable, excluding nested state machines."""

    def walk(node, top: bool = False) -> bool:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "Comment":
                    continue
                if key == "States" and not top:
                    continue
                if walk(value):
                    return True
        elif isinstance(node, list):
            return any(walk(v) for v in node)
        elif isinstance(node, str):
            return "$.run_date" in node or "$.calendar_date" in node
        return False

    return walk(state, top=True)


def test_every_state_touching_a_run_date_declares_its_family():
    undeclared = sorted(
        name
        for name, state in _states().items()
        if _touches_a_run_date(state) and _MARK not in state.get("Comment", "")
    )
    assert not undeclared, (
        "these states reference $.run_date or $.calendar_date without stating which "
        f"date family they mean: {undeclared}. Add "
        "'alpha-engine-config-I8809: PARTITION FAMILY = trading_day|calendar_date' to "
        "the state's Comment, naming WHY. trading_day = 'which cycle is this' (every "
        "artifact prefix, every stage-coverage verdict). calendar_date = 'when did "
        "this fire' (the run-day gate, the mutex slot, a LastModified comparison)."
    )


def test_no_state_declares_a_family_outside_the_closed_vocabulary():
    for name, state in _states().items():
        comment = state.get("Comment", "")
        idx = comment.find(_MARK)
        while idx != -1:
            tail = comment[idx + len(_MARK) :].strip()
            assert tail.startswith(_FAMILIES), (
                f"{name} declares an unknown partition family: {tail[:40]!r}. "
                f"The vocabulary is closed: {_FAMILIES}."
            )
            idx = comment.find(_MARK, idx + 1)


def test_the_calendar_family_set_is_exactly_the_ratified_one():
    """A new calendar-keyed site cannot appear without editing this test."""
    declared = {
        name
        for name, state in _states().items()
        if "PARTITION FAMILY = calendar_date" in state.get("Comment", "")
    }
    assert declared == _CALENDAR_SITES, (
        f"calendar-keyed sites drifted. Added: {sorted(declared - _CALENDAR_SITES)}. "
        f"Removed: {sorted(_CALENDAR_SITES - declared)}. Every entry is a considered "
        "decision — update _CALENDAR_SITES only with the reason in the state's Comment."
    )


# ── The normalization itself ────────────────────────────────────────────────


def test_initialize_input_stamps_all_three_date_fields():
    init = _states()["InitializeInput"]
    merged = init["Parameters"]["merged.$"]
    for field in ('"run_date"', '"calendar_date"', '"run_date_family":"calendar_date"'):
        assert field in merged, f"InitializeInput no longer seeds {field}"


def test_every_path_into_the_working_graph_passes_through_the_normalizer():
    """A path reaching CheckRunMode without normalizing keys the whole cycle on
    the calendar date — the exact defect, reintroduced by a routing edit."""
    states = _states()
    entries = {
        name
        for name, st in states.items()
        if name not in ("NormalizeRunDates", "ApplyNormalizedRunDate", "NormalizeRunDatesDegraded")
        and (
            st.get("Next") == "CheckRunMode"
            or st.get("Default") == "CheckRunMode"
            or any(c.get("Next") == "CheckRunMode" for c in st.get("Choices") or [])
        )
    }
    assert not entries, (
        f"these states route straight to CheckRunMode, bypassing NormalizeRunDates: "
        f"{sorted(entries)}. Route them through NormalizeRunDates instead "
        "(alpha-engine-config-I8809)."
    )


def test_the_normalizer_fails_open_and_says_so():
    n = _states()["NormalizeRunDates"]
    assert n["Parameters"]["Payload"]["action"] == "resolve_run_dates"
    assert n["Parameters"]["FunctionName"] == "alpha-engine-weekly-preflight"
    assert n["Parameters"]["Payload"]["calendar_date.$"] == "$.calendar_date"
    (catch,) = n["Catch"]
    assert catch["Next"] == "NormalizeRunDatesDegraded"
    assert catch["ErrorEquals"] == ["States.ALL"]
    # The degraded floor must NOT claim the trading-day family.
    degraded = _states()["NormalizeRunDatesDegraded"]
    assert degraded["Next"] == "CheckRunMode"


def test_now_dual_is_not_what_the_normalizer_uses():
    """``resolve_trading_day``, not ``now_dual()``.

    ``resolve_trading_day(d)`` is the most recent NYSE session on or before
    ``d``; ``now_dual().trading_day`` is the last session fully CLOSED at this
    instant. They disagree on a Friday-afternoon run, and every downstream
    normalizer in the fleet uses the first — so the second would open a THIRD
    partition on exactly the runs hardest to reason about.
    """
    src = (
        Path(__file__).resolve().parents[1]
        / "infrastructure"
        / "lambdas"
        / "weekly-preflight"
        / "index.py"
    ).read_text()
    body = src.split("def _resolve_run_dates")[1].split("\ndef ")[0]
    # Strip the docstring: it NAMES now_dual in order to rule it out.
    code = body.split('"""')[2] if body.count('"""') >= 2 else body
    assert "resolve_trading_day" in code
    assert "now_dual" not in code


# ── The dual-write, and its expiry ──────────────────────────────────────────


def test_the_completion_marker_is_dual_written_to_both_families():
    states = _states()
    for canonical, twin in (
        ("WriteCompletionMarker", "WriteCompletionMarkerCalendar"),
        ("WriteCompletionMarkerDegraded", "WriteCompletionMarkerDegradedCalendar"),
    ):
        assert states[canonical]["Next"] == twin
        assert "$.run_date" in states[canonical]["Parameters"]["Key.$"]
        assert "$.calendar_date" in states[twin]["Parameters"]["Key.$"]
        # The legacy copy must never fail a run whose canonical marker landed.
        assert states[twin]["Catch"], f"{twin} needs a fail-soft Catch"


def test_the_sweep_is_given_both_families():
    payload = _states()["WeeklyCoverageSweep"]["Parameters"]["Payload"]
    assert payload["run_date.$"] == "$.run_date"
    assert payload["calendar_date.$"] == "$.calendar_date"


def test_the_sweep_lambda_pins_a_lib_that_can_read_both_partitions():
    req = (
        Path(__file__).resolve().parents[1]
        / "infrastructure"
        / "lambdas"
        / "weekly-coverage-sweep"
        / "requirements.txt"
    ).read_text()
    import re

    match = re.search(r"nousergon-lib.*@(v0\.(\d+)\.(\d+))", req)
    assert match, "the sweep Lambda must pin nousergon-lib explicitly"
    minor, patch = int(match.group(2)), int(match.group(3))
    assert (minor, patch) >= (124, 94), (
        f"the sweep pins {match.group(1)}, which predates "
        "nousergon_lib.pipeline_status.partition. Without it read_coverage_sweep() "
        "rejects calendar_date and the handler returns outcome=unavailable every week "
        "— it pages honestly and detects nothing (alpha-engine-config-I8809)."
    )


def test_the_lambda_that_reads_the_sweep_actually_deploys_on_merge():
    """The sweep shipped with a deploy.sh and no workflow calling it.

    ``deploy-infrastructure.yml`` restamps the SF on every push to main but
    does not touch Lambda code, so every edit to this handler merged INERT.
    """
    wf = Path(__file__).resolve().parents[1] / ".github" / "workflows"
    path = wf / "deploy-weekly-coverage-sweep.yml"
    assert path.exists(), (
        "weekly-coverage-sweep has no deploy workflow — its code changes merge inert "
        "(alpha-engine-config-I8809)"
    )
    text = path.read_text()
    assert "infrastructure/lambdas/weekly-coverage-sweep/**" in text
    assert "infrastructure/lambdas/weekly-coverage-sweep/deploy.sh" in text


@pytest.mark.parametrize(
    "state,field",
    [
        ("CheckUniverseMembershipFresh", "pointer_last_modified_date"),
    ],
)
def test_last_modified_comparisons_use_the_calendar_date(state, field):
    """An S3 LastModified is a wall-clock write time.

    Comparing it against the trading day makes the check strictly WEAKER on
    every Saturday run: an artifact written on Friday would satisfy 'landed
    this cycle', which is the wrong pass these states exist to prevent.
    """
    choice = _states()[state]["Choices"][0]["And"]
    (cmp_,) = [c for c in choice if "StringGreaterThanEqualsPath" in c]
    assert cmp_["Variable"].endswith(field)
    assert cmp_["StringGreaterThanEqualsPath"] == "$.calendar_date"


def test_predictor_skip_guard_is_trading_day_keyed():
    """alpha-engine-config-I12050. The skip_predictor_training guard used to
    compare the live weights manifest's LastModified with calendar_date. That
    manifest moves only on promotion, so the guard now reads
    arena/model/{run_date}.verdict -- the cycle's own record, keyed like every
    arena prefix by the TRADING day -- and no wall-clock comparison remains.
    """
    states = _states()
    validate = states["ValidatePredictorSkipWeightsFresh"]
    assert validate["Parameters"]["Key.$"] == (
        "States.Format('arena/model/{}.verdict', $.run_date)"
    )
    assert "PARTITION FAMILY = trading_day" in validate["Comment"]
    check = states["CheckPredictorSkipWeightsFresh"]
    assert "PARTITION FAMILY = trading_day" in check["Comment"]
    assert "$.calendar_date" not in json.dumps(check["Choices"])
