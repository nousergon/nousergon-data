"""A weekly pipeline rerun reads ITS CYCLE's collection, not a fresh one.

alpha-engine-config-I12228. The weekly's readiness wait used to grade manifests
against the consumer's own `$$.Execution.StartTime` (lookback 0). On the
scheduled run that is right: it starts at 09:00Z, at or before the collection.
On a rerun it is hours late, so every manifest of the cycle - including a
data-only rerun that finished before the pipeline rerun started - reads
`manifest_missing`, and the only way through is a full data rerun timed to
finish after the pipeline rerun starts.

The wait now passes the cycle's anchor as `not_before` (InitializeInput floors
it to the execution's own start; a rerun carries the failed cycle's original
start) plus `consumer_started`, and the probe refuses an anchor more than
`COLLECTION_ANCHOR_MAX_AGE_SECONDS` older than the consumer, so no anchor can
reach back to the previous week's manifests.

Replays the real 2026-10-03 manifests (`tests/fixtures/
weekly_completion_2026-10-03/manifests.json`), the same fixture
`test_readiness_awaits_producer_retry.py` uses.
"""
from __future__ import annotations

import copy
import json
import pathlib
from datetime import datetime, timedelta, timezone

import pytest

from data_gate import run_manifest_predicate as predicate
from tests.test_readiness_awaits_producer_retry import ReplayS3

REPO = pathlib.Path(__file__).resolve().parent.parent
FIXTURE = REPO / "tests" / "fixtures" / "weekly_completion_2026-10-03" / "manifests.json"
#: The scheduled 10-03 weekly's start: its cycle anchor.
V1_STARTED = "2026-10-03T09:00:49Z"
#: A pipeline rerun started after every manifest of the cycle had landed.
RERUN_STARTED = "2026-10-03T15:30:00Z"


@pytest.fixture(autouse=True)
def _fresh_descriptors():
    predicate.reset_unit_cache()
    yield
    predicate.reset_unit_cache()


@pytest.fixture(scope="module")
def objects() -> dict:
    recorded = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return {r["key"]: copy.deepcopy(r["manifest"]) for r in recorded["manifests"]}


@pytest.fixture(scope="module")
def units() -> list[str]:
    definition = json.loads((REPO / "infrastructure" / "step_function.json").read_text())
    return list(
        definition["States"]["WaitForCollectionManifests"]["Parameters"]["Payload"]["units"]
    )


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _readiness(objects, units, not_before, **extra) -> dict:
    event = {"collection": "weekly", "units": units, "not_before": not_before,
             "lookback_seconds": 0, **extra}
    return predicate.readiness_check(event, s3_client=ReplayS3(objects))["readiness"]


def _last_finished(objects) -> str:
    return max(m["finished"] for m in objects.values())


def test_the_fixture_cycle_finished_before_the_rerun_started(objects):
    assert _last_finished(objects) < RERUN_STARTED


def test_a_rerun_graded_from_its_own_start_sees_nothing(objects, units):
    """The defect: the old payload (not_before = the rerun's own start)."""
    got = _readiness(objects, units, RERUN_STARTED)
    assert not got["ready"]
    assert got["missing"] == sorted(units)


def test_a_rerun_graded_from_the_cycle_anchor_reads_the_cycle(objects, units):
    """The fix: the rerun reaches the same verdict the scheduled run would
    have reached once the cycle's collection had finished."""
    scheduled = _readiness(objects, units, V1_STARTED)
    rerun = _readiness(objects, units, V1_STARTED, consumer_started=RERUN_STARTED)
    assert rerun == scheduled
    assert rerun["missing"] == []


def test_the_scheduled_run_is_unchanged(objects, units):
    """InitializeInput's floor makes not_before == consumer_started on the
    scheduled run; passing consumer_started then changes nothing."""
    assert _readiness(objects, units, V1_STARTED, consumer_started=V1_STARTED) == \
        _readiness(objects, units, V1_STARTED)


def test_an_anchor_old_enough_to_reach_the_previous_week_is_refused(objects, units):
    started = datetime.fromisoformat(V1_STARTED.replace("Z", "+00:00"))
    prior_week = _iso(started - timedelta(days=7))
    with pytest.raises(ValueError, match="alpha-engine-config-I12228"):
        _readiness(objects, units, prior_week, consumer_started=V1_STARTED)


def test_the_bound_is_inclusive_and_sits_inside_one_cycle(objects, units):
    started = datetime.fromisoformat(RERUN_STARTED.replace("Z", "+00:00"))
    edge = timedelta(seconds=predicate.COLLECTION_ANCHOR_MAX_AGE_SECONDS)
    assert edge < timedelta(days=7), "the bound must stay inside one weekly cycle"
    _readiness(objects, units, _iso(started - edge), consumer_started=RERUN_STARTED)
    with pytest.raises(ValueError):
        _readiness(objects, units, _iso(started - edge - timedelta(seconds=1)),
                   consumer_started=RERUN_STARTED)


def test_consumers_that_do_not_name_consumer_started_are_unbounded(objects, units):
    """The morning and EOD waits pass their own start as not_before and never
    set consumer_started; the bound must not reach them."""
    started = datetime.fromisoformat(V1_STARTED.replace("Z", "+00:00"))
    _readiness(objects, units, _iso(started - timedelta(days=30)))
