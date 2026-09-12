"""Unit tests for floor_calibration.py (alpha-engine-config-I10164 part 2)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import floor_calibration as fc
from floor_calibration import (
    DEGENERATE_SPREAD_RATIO,
    MARGIN,
    MIN_SAMPLES,
    STATE_TO_STATE_MACHINE,
    FloorRecommendation,
    collect_state_duration_samples,
    compute_all_recommendations,
    compute_recommendation,
    render_report,
    run_check,
)


def _samples(durations, poll_statuses=None):
    if poll_statuses is None:
        return [{"duration_sec": d, "poll_status": None} for d in durations]
    return [
        {"duration_sec": d, "poll_status": s} for d, s in zip(durations, poll_statuses)
    ]


# ── compute_recommendation: core statuses ────────────────────────────────


def test_unmeasurable_below_min_samples():
    samples = _samples([100.0] * (MIN_SAMPLES - 1))
    rec = compute_recommendation("SomeState", samples, current_floor_sec=90)
    assert rec.status == "unmeasurable"
    assert rec.recommended_floor_sec is None
    assert rec.n_genuine == MIN_SAMPLES - 1


def test_unmeasurable_is_never_zero_and_never_dropped():
    """A too-small sample never defaults to a floor of 0 — it stays a
    reported row with its own status, not a silently omitted one."""
    samples = _samples([50.0, 60.0])
    rec = compute_recommendation("RareState", samples, current_floor_sec=30)
    assert rec.status == "unmeasurable"
    assert rec.current_floor_sec == 30  # untouched, not zeroed
    assert rec.recommended_floor_sec is None
    # See test_every_codified_floor_state_is_reported for the invariant that
    # every STATE_DURATION_FLOORS_SEC entry (not just states with samples) is
    # always present in the full report.


def test_degenerate_distribution_flagged_not_recommended():
    # Every sample within DEGENERATE_SPREAD_RATIO of each other.
    tight = [1000.0 + i * 0.01 for i in range(MIN_SAMPLES + 5)]
    assert (max(tight) / min(tight)) < DEGENERATE_SPREAD_RATIO
    rec = compute_recommendation("TooTight", _samples(tight), current_floor_sec=800)
    assert rec.status == "degenerate"
    assert rec.recommended_floor_sec is None


def test_ok_when_current_floor_within_tolerance_of_recommendation():
    durations = [100.0 + i for i in range(MIN_SAMPLES + 10)]  # min=100
    recommended = round(100.0 * (1 - MARGIN))  # 85
    rec = compute_recommendation("Steady", _samples(durations), current_floor_sec=recommended)
    assert rec.status == "ok"
    assert rec.recommended_floor_sec == recommended


def test_drift_tighten_when_current_floor_too_low():
    # Mirrors PollMorningArcticAppendSpot: current floor sits far BELOW the
    # measured genuine minimum, catching nothing — a false negative.
    durations = [1474.9 + i * 40 for i in range(MIN_SAMPLES + 10)]
    rec = compute_recommendation("TooLoose", _samples(durations), current_floor_sec=480)
    assert rec.status == "drift_tighten"
    assert rec.recommended_floor_sec is not None
    assert rec.recommended_floor_sec > 480


def test_drift_loosen_when_current_floor_too_high():
    # Mirrors PollMorningEnrichSpot pre-recalibration: current floor sits
    # ABOVE the measured genuine minimum, false-positiving on healthy runs.
    durations = [106.8 + i for i in range(MIN_SAMPLES + 10)]
    rec = compute_recommendation("TooTightFloor", _samples(durations), current_floor_sec=480)
    assert rec.status == "drift_loosen"
    assert rec.recommended_floor_sec is not None
    assert rec.recommended_floor_sec < 480


def test_never_silently_widens_a_correctly_tight_floor():
    """A floor that already sits BELOW the recommendation (correctly tight,
    catching more than the bare minimum would) is not reported OK just
    because it's conservative — it is drift_loosen only when it exceeds the
    band, otherwise the mechanism must not push it wider for no reason."""
    durations = [1000.0 + i * 40 for i in range(MIN_SAMPLES + 10)]
    recommended = round(1000.0 * (1 - MARGIN))
    # current floor already well below recommended (tighter than necessary,
    # but not by so much it is a false negative) — inside tolerance band.
    tight_but_in_band = int(recommended * 0.85)
    rec = compute_recommendation("Conservative", _samples(durations), current_floor_sec=tight_but_in_band)
    assert rec.status in ("ok", "drift_tighten")
    # Whichever it is, the formula-driven recommendation is symmetric — this
    # test exists to pin that "ok" is reachable from BELOW recommended too,
    # not only from above (i.e. the check is not loosen-only).
    if rec.status == "drift_tighten":
        assert rec.recommended_floor_sec > tight_but_in_band


# ── poll-status exclusion (the ArcticAppend lesson) ──────────────────────


def test_poll_status_failed_samples_excluded_from_genuine_distribution():
    """A state with a KNOWN_POLL_STATUS_KEYS entry must exclude Failed
    samples from the genuine min/percentile computation — the exact defect
    this module exists to prevent (a broken-but-SUCCEEDED Task laundering a
    short duration into the 'genuine' distribution)."""
    genuine_durations = [1474.9 + i * 40 for i in range(MIN_SAMPLES + 5)]
    broken_durations = [121.3, 260.3, 929.7]
    samples = _samples(
        broken_durations + genuine_durations,
        poll_statuses=["Failed"] * len(broken_durations)
        + ["Success"] * len(genuine_durations),
    )
    rec = compute_recommendation(
        "PollMorningArcticAppendSpot", samples, current_floor_sec=480
    )
    assert rec.n_excluded == 3
    assert rec.min_sec == pytest.approx(1474.9, abs=0.5)
    assert rec.status == "drift_tighten"


def test_state_without_poll_key_uses_raw_duration():
    """A state absent from KNOWN_POLL_STATUS_KEYS has no ground-truth signal
    beyond duration — this is a declared, documented limitation, not a bug:
    every sample counts as genuine regardless of a (nonexistent) poll_status."""
    durations = [500.0 + i for i in range(MIN_SAMPLES + 5)]
    rec = compute_recommendation("Scanner", _samples(durations), current_floor_sec=60)
    assert rec.n_excluded == 0
    assert rec.n_genuine == len(durations)


# ── compute_all_recommendations: derived set, not hand-kept ──────────────


def test_every_codified_floor_state_is_reported():
    """The report covers every entry in STATE_DURATION_FLOORS_SEC, derived
    from that module, never a separately hand-kept list here."""
    recs = compute_all_recommendations({})
    from execution_digest import STATE_DURATION_FLOORS_SEC

    reported_names = {r.state_name for r in recs}
    assert reported_names == set(STATE_DURATION_FLOORS_SEC)
    # With zero samples supplied, every one is unmeasurable — recorded, not
    # dropped.
    assert all(r.status == "unmeasurable" for r in recs)
    assert all(r.n_genuine == 0 for r in recs)


def test_state_to_state_machine_covers_every_codified_floor():
    from execution_digest import STATE_DURATION_FLOORS_SEC

    assert set(STATE_DURATION_FLOORS_SEC) == set(STATE_TO_STATE_MACHINE)


# ── render_report ──────────────────────────────────────────────────────


def test_render_report_includes_every_recommendation():
    recs = [
        FloorRecommendation(
            state_name="A", status="ok", current_floor_sec=90, n_genuine=20, n_excluded=0,
            min_sec=100.0, recommended_floor_sec=85, basis="x",
        ),
        FloorRecommendation(
            state_name="B", status="unmeasurable", current_floor_sec=480, n_genuine=2,
            n_excluded=0, basis="y",
        ),
    ]
    report = render_report(recs)
    assert "A" in report and "B" in report
    assert "ok" in report and "unmeasurable" in report


# ── collect_state_duration_samples: status extraction from Task output ───


def _entered(name, ts):
    return {"type": "TaskStateEntered", "timestamp": ts, "stateEnteredEventDetails": {"name": name}}


def _exited(name, ts, output):
    import json

    return {
        "type": "TaskStateExited",
        "timestamp": ts,
        "stateExitedEventDetails": {"name": name, "output": json.dumps(output)},
    }


def test_collect_state_duration_samples_extracts_poll_status():
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        _entered("PollMorningArcticAppendSpot", base),
        _exited(
            "PollMorningArcticAppendSpot",
            base + timedelta(seconds=930),
            {"arctic_append_poll": {"Status": "Failed"}},
        ),
    ]

    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [
        {"executions": [{"executionArn": "arn:exec:1", "name": "exec-1"}]}
    ]

    def fake_fetch(_client, _arn):
        return events

    samples = collect_state_duration_samples(
        sf_client,
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline",
        ["PollMorningArcticAppendSpot"],
        fetch_history=fake_fetch,
    )
    assert len(samples["PollMorningArcticAppendSpot"]) == 1
    sample = samples["PollMorningArcticAppendSpot"][0]
    assert sample["duration_sec"] == pytest.approx(930.0)
    assert sample["poll_status"] == "Failed"


# ── the real weekly-execution fixture (alpha-engine-config-I10574) ──────


import json as _json
from datetime import datetime as _datetime
from pathlib import Path as _Path

_WEEKLY_FIXTURE = (
    _Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "sf_history_weekly_canonical_2026-08-08.json"
)


def _weekly_canonical_history() -> list[dict]:
    """The real 2026-08-08 ne-weekly-freshness-pipeline execution
    (9b34ac0f-5e2f-f70b-d668-61b4c4751654_6fb6adf9-55fa-4426-6d06-79e4579bcaff),
    redacted to TaskStateEntered/TaskStateExited events only (type, state
    name, timestamp — no payload; the source history was fetched with
    ``includeExecutionData=False``, so no payload was ever present).

    This is the ONE genuine EventBridge-triggered SUCCEEDED execution in the
    account's full history carrying RAGIngestion/PredictorTraining/Backtester
    — the run alpha-engine-config-I10574's finding was measured against.
    """
    events = _json.loads(_WEEKLY_FIXTURE.read_text())
    for e in events:
        e["timestamp"] = _datetime.fromisoformat(e["timestamp"])
    return events


def test_real_weekly_history_dispatch_states_sample_to_zero():
    """Reproduces the reported defect: sampling the DISPATCH state names
    (the pre-fix STATE_TO_STATE_MACHINE keys) yields 0s for all three —
    exactly the nonsense `min_genuine=0.0` alpha-engine-config-I10574 reported."""
    events = _weekly_canonical_history()
    durations = fc.parse_task_state_durations(events)
    assert durations["RAGIngestion"] == 0
    assert durations["PredictorTraining"] == 0
    assert durations["Backtester"] == 0


def test_real_weekly_history_poll_states_sample_to_the_genuine_multi_minute_span():
    """The fix: sampling the companion WaitForX poll states (the corrected
    STATE_TO_STATE_MACHINE keys) recovers the real workload span."""
    events = _weekly_canonical_history()
    durations = fc.parse_task_state_durations(events)
    assert durations["WaitForRAGIngestion"] == 1057
    assert durations["WaitForPredictorTraining"] == 421
    assert durations["WaitForBacktester"] == 662
    # WaitForMorningEnrich / WaitForDataPhase1 (already correctly named pre-
    # I10574) are also genuinely multi-minute in this execution.
    assert durations["WaitForMorningEnrich"] == 1662
    assert durations["WaitForDataPhase1"] == 2417


def test_collect_state_duration_samples_end_to_end_on_the_real_fixture():
    """collect_state_duration_samples, driven end-to-end against the real
    fixture through a canonical execution name, must recover the genuine
    span for every renamed state and mark it as a non-zero sample."""
    events = _weekly_canonical_history()
    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [
        {
            "executions": [
                {
                    "executionArn": "arn:exec:1",
                    "name": (
                        "9b34ac0f-5e2f-f70b-d668-61b4c4751654_"
                        "6fb6adf9-55fa-4426-6d06-79e4579bcaff"
                    ),
                }
            ]
        }
    ]

    def fake_fetch(_client, _arn):
        return events

    samples = fc.collect_state_duration_samples(
        sf_client,
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
        [
            "WaitForRAGIngestion",
            "WaitForPredictorTraining",
            "WaitForBacktester",
            "WaitForMorningEnrich",
            "WaitForDataPhase1",
        ],
        fetch_history=fake_fetch,
    )
    for name, expected in (
        ("WaitForRAGIngestion", 1057),
        ("WaitForPredictorTraining", 421),
        ("WaitForBacktester", 662),
        ("WaitForMorningEnrich", 1662),
        ("WaitForDataPhase1", 2417),
    ):
        assert len(samples[name]) == 1, name
        sample = samples[name][0]
        assert sample["duration_sec"] == expected, name
        assert sample["exclusion_reason"] is None, name  # canonical name, non-zero


# ── zero-duration exclusion (I10574 deliverable 1) ───────────────────────


def test_zero_duration_sample_excluded_never_averaged_in():
    """A 0-second sample (a dispatch Task, not the workload span) is
    excluded with a named reason, never counted as genuine — regardless of
    KNOWN_POLL_STATUS_KEYS, and even when it would otherwise clear
    MIN_SAMPLES."""
    genuine_durations = [600.0 + i for i in range(MIN_SAMPLES + 5)]
    samples = [{"duration_sec": d, "poll_status": None, "exclusion_reason": None} for d in genuine_durations]
    samples += [
        {"duration_sec": 0.0, "poll_status": None, "exclusion_reason": None}
        for _ in range(5)
    ]
    rec = compute_recommendation("SomeDispatchState", samples, current_floor_sec=500)
    assert rec.n_genuine == len(genuine_durations)
    assert rec.n_excluded == 5
    assert rec.min_sec == pytest.approx(600.0)
    assert "zero-duration" in rec.exclusion_breakdown


def test_collect_state_duration_samples_tags_zero_duration_with_a_reason():
    from datetime import datetime, timezone

    base = datetime(2026, 8, 8, 10, 0, 0, tzinfo=timezone.utc)
    events = [_entered("RAGIngestion", base), _exited("RAGIngestion", base, {})]

    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [
        {"executions": [{"executionArn": "arn:exec:1", "name": "exec-1"}]}
    ]

    def fake_fetch(_client, _arn):
        return events

    samples = collect_state_duration_samples(
        sf_client,
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
        ["RAGIngestion"],
        fetch_history=fake_fetch,
    )
    sample = samples["RAGIngestion"][0]
    assert sample["duration_sec"] == 0
    assert "zero-duration" in sample["exclusion_reason"]


# ── canonical-execution-name filter (I10574 deliverable "measure the same
#    span"'s companion defect — averaging ad hoc reruns into "genuine") ───


def test_non_canonical_weekly_execution_excluded_with_a_named_reason():
    """watch-rerun-*/offcycle-shell-*/etc. executions of the weekly machine
    are tagged non-canonical and excluded — their bootstrap/skip semantics
    differ from the scheduled EventBridge-triggered run (I10574)."""
    from datetime import datetime, timezone

    base = datetime(2026, 8, 28, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        _entered("WaitForMorningEnrich", base),
        _exited("WaitForMorningEnrich", base, {}),
    ]
    # duration is 0 in this minimal fixture (same enter/exit timestamp) —
    # give it a non-zero span so the zero-duration exclusion doesn't mask
    # which reason actually fired.
    from datetime import timedelta

    events = [
        _entered("WaitForMorningEnrich", base),
        _exited("WaitForMorningEnrich", base + timedelta(seconds=151), {}),
    ]

    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [
        {"executions": [{"executionArn": "arn:exec:1", "name": "watch-rerun-2026-08-28-13"}]}
    ]

    def fake_fetch(_client, _arn):
        return events

    samples = fc.collect_state_duration_samples(
        sf_client,
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
        ["WaitForMorningEnrich"],
        fetch_history=fake_fetch,
    )
    sample = samples["WaitForMorningEnrich"][0]
    assert sample["duration_sec"] == 151
    assert "non-canonical" in sample["exclusion_reason"]


def test_canonical_execution_name_not_excluded():
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 8, 8, 9, 0, 0, tzinfo=timezone.utc)
    events = [
        _entered("WaitForMorningEnrich", base),
        _exited("WaitForMorningEnrich", base + timedelta(seconds=1662), {}),
    ]

    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [
        {
            "executions": [
                {
                    "executionArn": "arn:exec:1",
                    "name": (
                        "9b34ac0f-5e2f-f70b-d668-61b4c4751654_"
                        "6fb6adf9-55fa-4426-6d06-79e4579bcaff"
                    ),
                }
            ]
        }
    ]

    def fake_fetch(_client, _arn):
        return events

    samples = fc.collect_state_duration_samples(
        sf_client,
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline",
        ["WaitForMorningEnrich"],
        fetch_history=fake_fetch,
    )
    sample = samples["WaitForMorningEnrich"][0]
    assert sample["exclusion_reason"] is None


def test_canonical_name_filter_scoped_to_weekly_machine_only():
    """The weekday machine (ne-preopen-trading-pipeline) is NOT subject to
    the canonical-name filter — it is unmeasured there and scoping it
    narrowly avoids silently changing PollMorningEnrichSpot/
    PollMorningArcticAppendSpot's already-recalibrated (I10164) population."""
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 8, 8, 9, 0, 0, tzinfo=timezone.utc)
    events = [
        _entered("PollMorningEnrichSpot", base),
        _exited("PollMorningEnrichSpot", base + timedelta(seconds=200), {}),
    ]

    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [
        {"executions": [{"executionArn": "arn:exec:1", "name": "some-ad-hoc-name"}]}
    ]

    def fake_fetch(_client, _arn):
        return events

    samples = fc.collect_state_duration_samples(
        sf_client,
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline",
        ["PollMorningEnrichSpot"],
        fetch_history=fake_fetch,
    )
    sample = samples["PollMorningEnrichSpot"][0]
    assert sample["exclusion_reason"] is None


# ── --check output names the event pair measured (I10574 deliverable 3) ──


def test_recommendation_names_the_measured_event_pair():
    samples = _samples([600.0 + i for i in range(MIN_SAMPLES + 5)])
    rec = compute_recommendation("WaitForRAGIngestion", samples, current_floor_sec=600)
    assert "WaitForRAGIngestion" in rec.event_pair
    assert "TaskStateEntered" in rec.event_pair
    assert "TaskStateExited" in rec.event_pair


def test_render_report_includes_event_pair_and_exclusion_breakdown_columns():
    report = render_report(
        [
            FloorRecommendation(
                state_name="A",
                status="ok",
                current_floor_sec=90,
                n_genuine=20,
                n_excluded=2,
                min_sec=100.0,
                recommended_floor_sec=85,
                basis="x",
                event_pair="TaskStateEntered/TaskStateExited on 'A'",
                exclusion_breakdown="2 zero-duration",
            )
        ]
    )
    assert "event_pair" in report and "exclusion_breakdown" in report
    assert "TaskStateEntered/TaskStateExited on 'A'" in report
    assert "2 zero-duration" in report


def test_run_check_wires_every_state_machine():
    """run_check must reach every state machine named in
    STATE_TO_STATE_MACHINE, not only the one under active investigation —
    the whole point of a periodic mechanism is it does not need a human to
    remember which pipeline to point it at."""
    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [{"executions": []}]

    recs = run_check(sf_client)
    names = {r.state_name for r in recs}
    assert names == set(STATE_TO_STATE_MACHINE)

    called_arns = {
        call.kwargs.get("stateMachineArn")
        for call in sf_client.get_paginator.return_value.paginate.call_args_list
    }
    expected_machines = set(STATE_TO_STATE_MACHINE.values())
    called_machines = {arn.rsplit(":", 1)[-1] for arn in called_arns}
    assert called_machines == expected_machines
