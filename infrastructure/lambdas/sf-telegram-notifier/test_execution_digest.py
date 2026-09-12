"""Unit tests for execution_digest.py (config#1672)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from execution_digest import (
    STATE_DURATION_FLOORS_SEC,
    build_execution_digest,
    build_state_durations,
    fetch_execution_history,
    format_digest_lines,
    parse_run_date_from_execution_name,
    parse_task_state_durations,
    StateDuration,
)


def _ts(base: datetime, offset_sec: int) -> datetime:
    return base.replace(tzinfo=timezone.utc) + __import__("datetime").timedelta(seconds=offset_sec)


def test_parse_task_state_durations_computes_wall_clock():
    base = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "type": "TaskStateEntered",
            "timestamp": base,
            "stateEnteredEventDetails": {"name": "PredictorTraining"},
        },
        {
            "type": "TaskStateExited",
            "timestamp": _ts(base, 120),
            "stateExitedEventDetails": {"name": "PredictorTraining"},
        },
    ]
    assert parse_task_state_durations(events)["PredictorTraining"] == 120


def test_floor_breach_detected_when_under_minimum():
    # alpha-engine-config-I10574: "PredictorTraining" is the dispatch state
    # (never floored — see STATE_DURATION_FLOORS_SEC); the floor sits on its
    # companion poll state, "WaitForPredictorTraining".
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"WaitForPredictorTraining": 120},
        is_preflight=False,
        execution_start=start,
        run_date="2026-07-04",
        s3_client=None,
    )
    assert len(rows) == 1
    assert rows[0].floor_breach is True
    assert rows[0].anomaly is True


def test_preflight_suppresses_floor_breach():
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"WaitForPredictorTraining": 30},
        is_preflight=True,
        execution_start=start,
        run_date=None,
        s3_client=None,
    )
    assert rows[0].floor_breach is False


def test_poll_morning_enrich_spot_floor_is_the_recalibrated_value():
    # alpha-engine-config-I10164: the prior 8m floor was unmeasured and sat
    # above the median of every healthy run (median 273.7s across n=34
    # SUCCEEDED executions), firing a chronic false positive. Recalibrated to
    # 90s, ~15% below the measured minimum genuine duration (106.8s).
    assert STATE_DURATION_FLOORS_SEC["PollMorningEnrichSpot"] == 90


def test_poll_morning_enrich_spot_floor_clears_the_measured_minimum_genuine_run():
    # The slowest-to-clear genuine run measured (operator-recovery-2026-08-13,
    # 106.8s) must NOT breach the new floor — a floor still firing on the
    # fastest observed healthy run would be the same defect class recalibrated
    # to a different wrong number.
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PollMorningEnrichSpot": 107},
        is_preflight=False,
        execution_start=start,
        run_date="2026-07-04",
        s3_client=None,
    )
    assert rows[0].floor_breach is False


def test_poll_morning_enrich_spot_floor_still_catches_a_genuinely_hollow_run():
    # A run that returns before the constituents fetch even starts (no
    # measured genuine run, n=34, completes in under 90s) must still trip
    # HOLLOW-SUSPECT.
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PollMorningEnrichSpot": 45},
        is_preflight=False,
        execution_start=start,
        run_date="2026-07-04",
        s3_client=None,
    )
    assert rows[0].floor_breach is True
    assert rows[0].anomaly is True


def test_poll_morning_arctic_append_spot_floor_is_the_recalibrated_value():
    # alpha-engine-config-I10164 part 1: the prior 8m (480s) floor was also
    # unmeasured (config-I6857) and sat BELOW every genuine run (min 1474.9s
    # across n=29 Success-status executions), so it never false-positived —
    # but it also missed a confirmed-broken 929.7s run, the mirror-image
    # defect. Recalibrated to 1200s, ~19% below the measured genuine minimum.
    assert STATE_DURATION_FLOORS_SEC["PollMorningArcticAppendSpot"] == 1200


def test_poll_morning_arctic_append_spot_floor_clears_the_measured_minimum_genuine_run():
    # The fastest genuine (arctic_append_poll.Status == "Success") run
    # measured (500c7956-..., 1474.9s) must NOT breach the new floor.
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PollMorningArcticAppendSpot": 1475},
        is_preflight=False,
        execution_start=start,
        run_date="2026-07-04",
        s3_client=None,
    )
    assert rows[0].floor_breach is False


def test_poll_morning_arctic_append_spot_floor_still_catches_a_confirmed_broken_run():
    # 929.7s (b9d76d5c-..., 2026-07-14) is a CONFIRMED broken run — its
    # arctic_append_poll.Status read "Failed" (SSM StandardErrorContent:
    # a pip-cache-permission failure, "failed to run commands: exit status
    # 1") even though the overall SF execution completed SUCCEEDED. The old
    # 480s floor MISSED this one (929.7s > 480s); the recalibrated 1200s
    # floor must catch it.
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PollMorningArcticAppendSpot": 930},
        is_preflight=False,
        execution_start=start,
        run_date="2026-07-04",
        s3_client=None,
    )
    assert rows[0].floor_breach is True
    assert rows[0].anomaly is True


def test_weekly_wait_floors_recalibrated_off_the_poll_states_not_dispatch():
    # alpha-engine-config-I10545: the prior floors sat on "MorningEnrich" /
    # "DataPhase1" (the SSM dispatch Task states), which enter+exit in well
    # under a second every run (measured 0.22s / 0.24s on the 2026-09-12
    # execution) — a floor there breaches unconditionally. Floors now sit on
    # the poll states that actually span the workload, ~20% below the
    # measured minimum across the last four canonical weekly runs (min
    # WaitForMorningEnrich 31.7min=1902s, min WaitForDataPhase1 83.6min=5016s).
    assert "MorningEnrich" not in STATE_DURATION_FLOORS_SEC
    assert "DataPhase1" not in STATE_DURATION_FLOORS_SEC
    assert STATE_DURATION_FLOORS_SEC["WaitForMorningEnrich"] == 25 * 60
    assert STATE_DURATION_FLOORS_SEC["WaitForDataPhase1"] == 67 * 60


def test_weekly_wait_floors_clear_the_measured_minimum_genuine_runs():
    # The slowest-to-clear genuine minimums measured (31.7min / 83.6min) must
    # not breach the new floors.
    start = datetime(2026, 9, 12, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"WaitForMorningEnrich": int(31.7 * 60), "WaitForDataPhase1": int(83.6 * 60)},
        is_preflight=False,
        execution_start=start,
        run_date="2026-09-12",
        s3_client=None,
    )
    assert all(not r.floor_breach for r in rows)


def test_dispatch_state_duration_still_renders_with_no_floor():
    # The dispatch states are dropped from STATE_DURATION_FLOORS_SEC, not
    # from the digest entirely — no whitelist filter (alpha-engine-config-
    # I6857): a state absent from the floor mapping renders with no floor,
    # it is never dropped.
    start = datetime(2026, 9, 12, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"MorningEnrich": 0},
        is_preflight=False,
        execution_start=start,
        run_date="2026-09-12",
        s3_client=None,
    )
    assert rows[0].floor_sec is None
    assert rows[0].floor_breach is False


def test_format_digest_sorts_anomalies_visually():
    rows = [
        StateDuration("Backtester", 600, 600, False, False),
        StateDuration("PredictorTraining", 120, 1200, True, False),
    ]
    lines = format_digest_lines(rows)
    assert any("PredictorTraining" in line and "⚠️" in line for line in lines)
    assert any("Backtester" in line and "✓" in line for line in lines)


def test_build_execution_digest_hollow_on_fast_predictor():
    # alpha-engine-config-I10574: floored/attested on "WaitForPredictorTraining"
    # (the poll state), not "PredictorTraining" (the dispatch state).
    start_ms = 1_700_000_000_000
    sf = MagicMock()
    base = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    sf.get_execution_history.return_value = {
        "events": [
            {
                "type": "TaskStateEntered",
                "timestamp": base,
                "stateEnteredEventDetails": {"name": "WaitForPredictorTraining"},
            },
            {
                "type": "TaskStateExited",
                "timestamp": _ts(base, 90),
                "stateExitedEventDetails": {"name": "WaitForPredictorTraining"},
            },
        ],
    }
    lines, hollow, detailed_cause = build_execution_digest(
        execution_arn="arn:aws:states:us-east-1:123:execution:sm:exec",
        is_preflight=False,
        execution_start_ms=start_ms,
        run_date="2026-07-04",
        sf_client=sf,
        s3_client=None,
    )
    assert hollow is True
    assert detailed_cause is None
    assert any("WaitForPredictorTraining" in line for line in lines)
    assert STATE_DURATION_FLOORS_SEC["WaitForPredictorTraining"] == 20 * 60


def test_parse_run_date_from_execution_name_extracts_iso_date():
    # daemon.py's _trigger_eod_pipeline: name=f"eod-{run_date}-{epoch}"
    assert parse_run_date_from_execution_name("eod-2026-08-08-1754678901") == "2026-08-08"


def test_parse_run_date_from_execution_name_handles_backstop_prefix():
    # eod-backstop/index.py: name=f"eod-backstop-{trading_day}-{epoch}"
    assert (
        parse_run_date_from_execution_name("eod-backstop-2026-08-08-1700000000")
        == "2026-08-08"
    )


def test_parse_run_date_from_execution_name_returns_none_without_a_date():
    assert parse_run_date_from_execution_name("exec-001") is None


def test_parse_run_date_from_execution_name_returns_none_for_empty_input():
    assert parse_run_date_from_execution_name(None) is None
    assert parse_run_date_from_execution_name("") is None


# ── History pagination must not silently truncate a re-entered state's span
#   (alpha-engine-config-I10545) ───────────────────────────────────────────
#
# Live 2026-09-12: ne-weekly-freshness-pipeline's WaitForDataPhase1 (a Task
# state re-entered every ~15s across a 103.35min poll loop) produced 8916
# raw history events across 42 pages. The old max_pages=20 cap silently
# dropped everything past page 20 — parse_task_state_durations is correctly
# first-entry->last-exit, but fed a truncated event list it computes a real,
# short, WRONG span (34m instead of 103.35m), and the digest rendered it with
# a plain "✓". These tests guard both that a legitimately large history is no
# longer truncated at the old cap, and that when the (much higher, but still
# finite) new cap IS exhausted, the digest says so instead of asserting ✓.


def _paged_history_client(total_pages: int, events_per_page: int = 3):
    """A MagicMock sf_client whose get_execution_history hands back
    `total_pages` pages before nextToken goes empty."""
    sf = MagicMock()
    pages = []
    for i in range(total_pages):
        page_events = [
            {
                "type": "TaskStateEntered",
                "timestamp": _ts(datetime(2026, 9, 12, tzinfo=timezone.utc), i * events_per_page + j),
                "stateEnteredEventDetails": {"name": f"Filler{i}-{j}"},
            }
            for j in range(events_per_page)
        ]
        resp = {"events": page_events}
        if i < total_pages - 1:
            resp["nextToken"] = f"token-{i}"
        pages.append(resp)
    sf.get_execution_history.side_effect = pages
    return sf


def test_fetch_execution_history_paginates_past_the_old_twenty_page_cap():
    # 25 pages exceeds the OLD max_pages=20 cap but is well inside the new
    # default (_MAX_HISTORY_PAGES=150, headroom over the measured 42-page
    # need). Every event across every page must come back, untruncated.
    sf = _paged_history_client(total_pages=25, events_per_page=3)
    events, truncated = fetch_execution_history(sf, "arn:exec")
    assert len(events) == 25 * 3
    assert truncated is False
    assert sf.get_execution_history.call_count == 25


def test_fetch_execution_history_reports_truncation_when_pages_exhausted():
    # More pages exist (nextToken never runs out) than the caller's max_pages
    # budget — the exact shape of the real 2026-09-12 run under the old cap.
    sf = MagicMock()
    sf.get_execution_history.return_value = {
        "events": [{"type": "TaskStateEntered", "timestamp": datetime.now(timezone.utc),
                     "stateEnteredEventDetails": {"name": "Filler"}}],
        "nextToken": "always-more",
    }
    events, truncated = fetch_execution_history(sf, "arn:exec", max_pages=5)
    assert truncated is True
    assert sf.get_execution_history.call_count == 5


def test_build_execution_digest_announces_truncation_and_forces_hollow(monkeypatch):
    import execution_digest as ed

    monkeypatch.setattr(ed, "_MAX_HISTORY_PAGES", 2)
    base = datetime(2026, 9, 12, tzinfo=timezone.utc)
    sf = MagicMock()
    sf.get_execution_history.return_value = {
        "events": [
            {
                "type": "TaskStateEntered",
                "timestamp": base,
                "stateEnteredEventDetails": {"name": "Backtester"},
            },
            {
                "type": "TaskStateExited",
                "timestamp": _ts(base, 600),
                "stateExitedEventDetails": {"name": "Backtester"},
            },
        ],
        "nextToken": "always-more",
    }
    lines, hollow, _cause = build_execution_digest(
        execution_arn="arn:exec",
        is_preflight=False,
        execution_start_ms=int(base.timestamp() * 1000),
        run_date="2026-09-12",
        sf_client=sf,
        s3_client=None,
    )
    # A truncated history can only understate — never a silent ✓.
    assert hollow is True
    assert any("truncated" in line for line in lines)


def test_history_fetch_failure_surfaces_marker():
    sf = MagicMock()
    sf.get_execution_history.side_effect = RuntimeError("throttled")
    lines, hollow, detailed_cause = build_execution_digest(
        execution_arn="arn:exec",
        is_preflight=False,
        execution_start_ms=1_700_000_000_000,
        run_date=None,
        sf_client=sf,
        s3_client=None,
    )
    assert hollow is False
    assert detailed_cause is None
    assert any("digest unavailable" in line for line in lines)


# ── A failure notification must name the state that broke ───────────────────
#
# Live 2026-08-10: ne-weekly-freshness-pipeline failed twice at MorningEnrich
# (watch-rerun-2026-08-10-1 and -2), whose spot bootstrap hung and was SIGKILLed
# at its SSM budget. Both Telegram alerts rendered
# "States: -(no workload states in history)-" on a 60-state pipeline, because
# no workload state had EXITED so no duration existed. The state that broke was
# in the events the whole time, as a TaskStateEntered with no matching exit.


def _entered(base, name, offset=0):
    return {
        "type": "TaskStateEntered",
        "timestamp": _ts(base, offset),
        "stateEnteredEventDetails": {"name": name},
    }


def test_last_workload_state_entered_finds_the_unexited_state():
    from execution_digest import last_workload_state_entered

    base = datetime(2026, 8, 10, 23, 58, 0, tzinfo=timezone.utc)
    events = [
        _entered(base, "CheckSkipMorningEnrich", 0),   # untracked gate — ignored
        _entered(base, "MorningEnrich", 10),
    ]
    assert last_workload_state_entered(events) == "MorningEnrich"


def test_last_workload_state_entered_ignores_untracked_states():
    """Naming a gate or wait state would be true and useless.

    The exclusion is by EVENT TYPE, not by a name whitelist
    (alpha-engine-config-I6857). Choice and Wait states emit
    ChoiceStateEntered / WaitStateEntered, which _state_name_from_event does
    not read, so they cannot reach this function whatever they are called.

    This test previously fed CheckMorningEnrichStatus in as a
    TaskStateEntered and asserted it was dropped — which passed only because
    the name was missing from DIGEST_STATE_ORDER, and asserted a shape Step
    Functions never emits. Under the whitelist that also meant every genuine
    weekday Task state was dropped with it, which is the defect I6857 fixed.
    """
    from execution_digest import last_workload_state_entered

    base = datetime(2026, 8, 10, 23, 58, 0, tzinfo=timezone.utc)
    events = [
        {
            "type": "ChoiceStateEntered",
            "timestamp": _ts(base, 0),
            "stateEnteredEventDetails": {"name": "CheckMorningEnrichStatus"},
        },
        {
            "type": "WaitStateEntered",
            "timestamp": _ts(base, 5),
            "stateEnteredEventDetails": {"name": "MorningEnrichWait"},
        },
    ]
    assert last_workload_state_entered(events) is None


def test_last_workload_state_entered_names_a_weekday_task_state():
    """The counterpart: a Task state absent from DIGEST_STATE_ORDER is named.

    Under the old whitelist this returned None for every weekday pipeline
    state, so a preopen run dying before its first exit named nothing.
    """
    from execution_digest import last_workload_state_entered

    base = datetime(2026, 8, 11, 12, 15, 0, tzinfo=timezone.utc)
    assert last_workload_state_entered([_entered(base, "SomeBrandNewState", 0)]) == "SomeBrandNewState"


def test_digest_names_the_entered_state_instead_of_saying_nothing():
    lines = format_digest_lines([], last_entered="MorningEnrich")
    assert lines == ["MorningEnrich — entered, never completed ⚠️"]
    assert "no workload states in history" not in " ".join(lines)


def test_digest_keeps_the_empty_message_when_nothing_workload_ran():
    """A run that truly reached no workload state — the narrowed
    director-verify shape — still says so."""
    assert format_digest_lines([], last_entered=None) == [
        "_(no workload states in history)_"
    ]


def test_build_execution_digest_names_the_hung_state_end_to_end():
    """The 2026-08-10 shape, through the real entry point."""
    start_ms = 1_786_400_000_000
    base = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    sf = MagicMock()
    sf.get_execution_history.return_value = {
        "events": [
            _entered(base, "MorningEnrich", 30),  # entered, never exited
        ]
    }
    lines, hollow, detailed_cause = build_execution_digest(
        execution_arn="arn:aws:states:us-east-1:711398986525:execution:"
        "ne-weekly-freshness-pipeline:watch-rerun-2026-08-10-2",
        is_preflight=False,
        execution_start_ms=start_ms,
        run_date="2026-08-10",
        sf_client=sf,
        s3_client=None,
    )
    assert lines == ["MorningEnrich — entered, never completed ⚠️"]
    assert hollow is False
    assert detailed_cause is None


# ── Weekday pipelines render at all (alpha-engine-config-I6857) ───────────
#
# The 2026-08-11 preopen alert rendered "_(no workload states in history)_"
# for an execution with 1403 events across 18 distinct Task states. Not a
# one-off: DIGEST_STATE_ORDER and STATE_DURATION_FLOORS_SEC listed weekly
# state names only, and build_state_durations dropped everything in neither,
# so EVERY weekday alert rendered an empty list on EVERY run.
#
# nousergon-data-PR1295 had shipped that morning and did not prevent it — it
# covered a run dying before its first TaskStateExited. This run exited many.

import execution_digest as ed
import json as _json
from pathlib import Path as _Path

_FIXTURE = _Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "sf_history_preopen_2026-08-11.json"


def _preopen_history() -> list[dict]:
    """The real 2026-08-11 preopen execution, trimmed to Task state events.

    Execution 021b85f7-4814-477e-9fe0-05c77f4296d6 — the one whose alert
    named no states. Timestamps are parsed back to datetimes because that is
    what botocore hands the digest.
    """
    from datetime import datetime

    events = _json.loads(_FIXTURE.read_text())
    for e in events:
        e["timestamp"] = datetime.fromisoformat(e["timestamp"])
    return events


def test_the_real_2026_08_11_preopen_history_renders_states():
    """The regression, against the execution that produced it."""
    events = _preopen_history()
    durations = ed.parse_task_state_durations(events)
    rows = ed.build_state_durations(
        durations,
        is_preflight=False,
        execution_start=events[0]["timestamp"],
        run_date="2026-08-11",
        s3_client=None,
    )
    lines = ed.format_digest_lines(rows, last_entered=ed.last_workload_state_entered(events))

    assert lines != ["_(no workload states in history)_"]
    assert any("Scanner" in line for line in lines), (
        "the state that degraded the run must appear in the digest of that run"
    )


def test_the_preopen_poll_loop_reports_its_elapsed_not_zero():
    """PollMorningEnrichSpot spans ~15 min across ~60 entry/exit pairs.

    Max-per-pair reported 0s, which is worse than omitting it: it asserts the
    stage was instant.
    """
    durations = ed.parse_task_state_durations(_preopen_history())
    assert durations["PollMorningEnrichSpot"] > 10 * 60
    assert durations["PollMorningArcticAppendSpot"] > 10 * 60


def test_a_state_in_neither_collection_still_renders():
    """The filter is gone; the collections annotate and order only."""
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"TotallyUnknownState": 42},
        is_preflight=False,
        execution_start=start,
        run_date="2026-08-11",
        s3_client=None,
    )
    assert [r.name for r in rows] == ["TotallyUnknownState"]
    assert rows[0].floor_sec is None
    assert not rows[0].anomaly


def test_unknown_states_sort_after_known_ones_longest_first():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"ZebraState": 10, "AardvarkState": 900, "Scanner": 700},
        is_preflight=False,
        execution_start=start,
        run_date="2026-08-11",
        s3_client=None,
    )
    rows.sort(key=ed._sort_key)
    assert [r.name for r in rows] == ["Scanner", "AardvarkState", "ZebraState"]


def test_anomalous_states_lead_so_truncation_cannot_drop_them():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"Scanner": 5, "MorningEnrich": 20 * 60},  # Scanner breaches its 60s floor
        is_preflight=False,
        execution_start=start,
        run_date="2026-08-11",
        s3_client=None,
    )
    rows.sort(key=ed._sort_key)
    assert rows[0].name == "Scanner"
    assert rows[0].floor_breach


def test_truncation_is_announced_never_silent():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    many = {f"State{i:02d}": 100 - i for i in range(ed._MAX_DIGEST_ROWS + 3)}
    rows = ed.build_state_durations(
        many, is_preflight=False, execution_start=start, run_date=None, s3_client=None
    )
    rows.sort(key=ed._sort_key)
    lines = ed.format_digest_lines(rows)

    assert len(lines) == ed._MAX_DIGEST_ROWS + 1
    assert "+3 more states" in lines[-1]


def test_no_truncation_line_when_everything_fits():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"Scanner": 700}, is_preflight=False, execution_start=start, run_date=None, s3_client=None
    )
    lines = ed.format_digest_lines(rows)
    assert len(lines) == 1
    assert "elided" not in lines[0]


def test_every_weekday_state_name_in_the_order_list_exists_in_a_definition():
    """A misspelled state name is a silent no-op — it orders nothing.

    The old collections were not wrong about spelling, they were wrong about
    WHICH pipeline; this guard catches the other way of being wrong.
    """
    def _walk(states: dict) -> set[str]:
        """State names including those nested in Parallel branches / Map iterators.

        A flat scan of the top-level States map misses PredictorTraining,
        Parity and the rest, which live inside ResearchPredictorParallel's
        branches — and would then declare the digest's own weekly names
        phantom.
        """
        found = set(states)
        for body in states.values():
            for branch in body.get("Branches") or []:
                found |= _walk(branch.get("States") or {})
            iterator = body.get("Iterator") or body.get("ItemProcessor")
            if iterator:
                found |= _walk(iterator.get("States") or {})
        return found

    infra = _Path(__file__).resolve().parents[3] / "infrastructure"
    known: set[str] = set()
    for name in ("step_function.json", "step_function_daily.json", "step_function_eod.json"):
        known |= _walk(_json.loads((infra / name).read_text())["States"])

    unknown = sorted(s for s in ed.DIGEST_STATE_ORDER if s not in known)
    assert not unknown, f"DIGEST_STATE_ORDER names states no definition has: {unknown}"

    unknown_floors = sorted(s for s in ed.STATE_DURATION_FLOORS_SEC if s not in known)
    assert not unknown_floors, f"STATE_DURATION_FLOORS_SEC names states no definition has: {unknown_floors}"


# ── The wire format the API actually emits (alpha-engine-config-I9742) ──────
#
# `taskStateEnteredEventDetails` / `taskStateExitedEventDetails` are NOT
# fields the Step Functions API emits — every hand-written fixture above (and
# the committed `sf_history_preopen_2026-08-11.json`) used to fabricate them,
# so `_state_name_from_event` returned None unconditionally, and every test
# passed against a wire format that does not exist. Fixed by reading
# `stateEnteredEventDetails` / `stateExitedEventDetails` — the SAME generic
# state-transition detail Step Functions emits for every state type — and
# regression-guarded here two ways: (a) a schema guard that reddens on the
# next invented key, (b) a real, untouched, committed GetExecutionHistory
# response replayed end to end.

# The Step Functions HistoryEvent detail-field names the API actually emits
# (boto3 stepfunctions get_execution_history response schema). Anything
# ending in "EventDetails" that is NOT in this set is an invented key —
# exactly the shape of the alpha-engine-config-I9742 defect.
_REAL_HISTORY_EVENT_DETAIL_KEYS: frozenset[str] = frozenset(
    {
        "activityFailedEventDetails",
        "activityScheduleFailedEventDetails",
        "activityScheduledEventDetails",
        "activityStartedEventDetails",
        "activitySucceededEventDetails",
        "activityTimedOutEventDetails",
        "taskFailedEventDetails",
        "taskScheduledEventDetails",
        "taskStartFailedEventDetails",
        "taskStartedEventDetails",
        "taskSubmitFailedEventDetails",
        "taskSubmittedEventDetails",
        "taskSucceededEventDetails",
        "taskTimedOutEventDetails",
        "executionFailedEventDetails",
        "executionStartedEventDetails",
        "executionSucceededEventDetails",
        "executionAbortedEventDetails",
        "executionTimedOutEventDetails",
        "executionRedrivenEventDetails",
        "mapStateStartedEventDetails",
        "mapIterationStartedEventDetails",
        "mapIterationSucceededEventDetails",
        "mapIterationFailedEventDetails",
        "mapIterationAbortedEventDetails",
        "lambdaFunctionFailedEventDetails",
        "lambdaFunctionScheduleFailedEventDetails",
        "lambdaFunctionScheduledEventDetails",
        "lambdaFunctionStartFailedEventDetails",
        "lambdaFunctionStartedEventDetails",
        "lambdaFunctionSucceededEventDetails",
        "lambdaFunctionTimedOutEventDetails",
        "stateEnteredEventDetails",
        "stateExitedEventDetails",
        "mapRunStartedEventDetails",
        "mapRunFailedEventDetails",
        "mapRunRedrivenEventDetails",
        "stateMachineAliasUpdatedEventDetails",
        "stateMachineVersionUpdatedEventDetails",
    }
)


def test_module_reads_no_event_detail_key_outside_the_real_sf_schema():
    """Deliverable 3: the next invented key is red at merge, not silent.

    Scans execution_digest.py's own source for every string literal shaped
    like `<word>EventDetails` and asserts each one is a key the Step
    Functions API actually emits. This is what would have caught
    `taskStateEnteredEventDetails` / `taskStateExitedEventDetails` before
    they shipped — a fixture rebuild alone only proves today's fixture is
    honest, not that the module can't drift again.
    """
    source = (_Path(__file__).resolve().parent / "execution_digest.py").read_text()
    referenced = set(re.findall(r'"([A-Za-z]+EventDetails)"', source))
    assert referenced, "expected execution_digest.py to reference at least one *EventDetails key"
    invented = sorted(referenced - _REAL_HISTORY_EVENT_DETAIL_KEYS)
    assert not invented, (
        f"execution_digest.py reads *EventDetails key(s) the Step Functions API "
        f"does not emit: {invented}"
    )


def _load_real_history_fixture() -> list[dict]:
    """The real, untouched `GetExecutionHistory` response for
    `e888a3a4-05a7-4c42-b2a1-904f44e24bd5` (ne-preopen-trading-pipeline,
    FAILED 2026-09-01) — 122 events, one page, committed verbatim
    (alpha-engine-config-I9742). Timestamps are parsed back to datetimes,
    matching what botocore hands the digest (parse_task_state_durations
    silently drops any event whose timestamp is not one)."""
    fixture = (
        _Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "sf_history_preopen_2026-09-01_e888a3a4.json"
    )
    payload = _json.loads(fixture.read_text())
    events = payload["events"]
    for event in events:
        event["timestamp"] = datetime.fromisoformat(event["timestamp"])
    return events


def test_real_preopen_failure_history_names_the_state_that_broke():
    """Closes-when, part 1: replaying the real history through
    format_digest_lines produces a line naming WaitForCodeFreshness — the
    state whose SSM poll actually failed (the git-push 403), not
    HandleFailure, the state that merely sent the alert about it."""
    events = _load_real_history_fixture()
    durations = ed.parse_task_state_durations(events)
    assert durations == {
        "MarketHoursGate": 2,
        "AcquireMutex": 0,
        "DeployDriftCheck": 0,
        "TradingDayGate": 0,
        "StartExecutorEC2": 0,
        "DescribeInstanceInfo": 0,
        "CodeFreshnessGate": 0,
        "WaitForCodeFreshness": 62,
        "HandleFailure": 0,
    }
    rows = ed.build_state_durations(
        durations,
        is_preflight=False,
        execution_start=events[0]["timestamp"],
        run_date="2026-09-01",
        s3_client=None,
    )
    lines = ed.format_digest_lines(rows, last_entered=ed.last_workload_state_entered(events))
    assert any("WaitForCodeFreshness" in line for line in lines)


def test_last_workload_state_entered_excludes_handle_failure():
    """Deliverable 4: HandleFailure is the state that SENDS the alert, not
    the one that failed. Without the exclusion, the fallback on a run dying
    before its first exit would name the alerter instead of the work."""
    events = _load_real_history_fixture()
    assert ed.last_workload_state_entered(events) != "HandleFailure"
    assert ed.last_workload_state_entered(events) == "WaitForCodeFreshness"


def test_terminal_error_handling_states_covers_all_three_definitions():
    """HandleFailure is the shared Task name immediately preceding the
    terminal Fail state on the weekly, preopen AND postclose definitions —
    verified against the committed definitions, not assumed from one
    pipeline (alpha-engine-config-I9742 deliverable 4)."""
    infra = _Path(__file__).resolve().parents[3] / "infrastructure"
    for name in ("step_function.json", "step_function_daily.json", "step_function_eod.json"):
        states = _json.loads((infra / name).read_text())["States"]
        assert "HandleFailure" in states, f"{name} has no HandleFailure state"
        assert states["HandleFailure"]["Type"] == "Task"
    assert "HandleFailure" in ed.TERMINAL_ERROR_HANDLING_STATES


def test_every_task_preceding_a_fail_state_is_classified():
    """Exhaustive, or the next terminal state gets named as the failure.

    ``last_workload_state_entered`` returns the LAST Task entered, so any Task
    sitting immediately before a terminal ``Fail`` is a candidate to be named
    on exactly the runs where the digest is read. Two kinds sit there and they
    must be told apart by what the state DOES, because nothing structural
    separates them — both have ``Next`` pointing at a ``Fail``:

    - an ALERTER (``HandleFailure``, the market-hours refusals) — naming it
      reports the alerter as the failure, which is confidently wrong and worse
      than the honestly-empty placeholder it replaced;
    - a WORKER on the way out (``ForceStopInstance`` stops the trading box,
      ``WriteCompletionMarkerDegraded*`` writes the artifact every status-keyed
      consumer reads) — naming it is informative.

    This recomputes the candidate set from the three committed definitions and
    fails on anything in neither declared set, so a newly added terminal state
    is a red test rather than a wrong name inside a red alert
    (alpha-engine-config-I9742 deliverable 4).
    """
    infra = _Path(__file__).resolve().parents[3] / "infrastructure"
    classified = ed.TERMINAL_ERROR_HANDLING_STATES | ed.WORK_STATES_ON_TERMINAL_PATHS
    unclassified: dict[str, str] = {}
    for name in ("step_function.json", "step_function_daily.json", "step_function_eod.json"):
        states = _json.loads((infra / name).read_text())["States"]
        fail_states = {k for k, v in states.items() if v.get("Type") == "Fail"}
        for state_name, body in states.items():
            if body.get("Type") != "Task":
                continue
            if body.get("Next") in fail_states and state_name not in classified:
                unclassified[state_name] = name
    assert not unclassified, (
        "Task states sit immediately before a terminal Fail and are in neither "
        "TERMINAL_ERROR_HANDLING_STATES (alerters, excluded from the digest "
        "fallback) nor WORK_STATES_ON_TERMINAL_PATHS (real work, kept). Classify "
        f"each by what it DOES: {unclassified}"
    )
    # And the declared sets must not drift into naming states that no longer
    # exist — a stale exclusion silently stops excluding the moment a state is
    # renamed, which is the same failure one layer over.
    all_states: set[str] = set()
    for name in ("step_function.json", "step_function_daily.json", "step_function_eod.json"):
        all_states |= set(_json.loads((infra / name).read_text())["States"])
    assert classified <= all_states, (
        f"declared terminal states no longer exist in any definition: {classified - all_states}"
    )


def test_extract_detailed_failure_cause_surfaces_the_real_error():
    """Deliverable 5 / sf-pipeline-policy §2.3 corollary: the terminal
    failure message must carry the actual error. describe_execution's own
    error/cause on this execution is the constant
    "DailyPipelineFailure: One or more weekday pipeline steps failed." — the
    same string for every failure. The real diagnostic (the git push 403)
    lives in the terminal Fail state's own input, under the
    ssm-liveness-poller `code_freshness_poll` key."""
    events = _load_real_history_fixture()
    cause = ed.extract_detailed_failure_cause(events)
    assert cause is not None
    assert "403" in cause
    assert "alpha-engine-config.git" in cause
    assert "Write access to repository not granted" in cause


def test_extract_detailed_failure_cause_returns_none_without_a_poll_failure():
    """A failure the ssm-liveness-poller contract does not cover returns
    None rather than a guess — the caller falls back to describe_execution's
    error/cause."""
    events = [
        {
            "type": "FailStateEntered",
            "timestamp": datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc),
            "stateEnteredEventDetails": {
                "name": "FailExecution",
                "input": '{"some_gate": {"verdict": "PASS", "detail": "irrelevant"}}',
            },
        }
    ]
    assert ed.extract_detailed_failure_cause(events) is None
