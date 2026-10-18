"""The monthly freshness-monitor threshold-tightening drill
(`data_gate/producers/freshness_commissioning.py`, alpha-engine-config-I10795).

The load-bearing property: a receipt the drill writes is graded by the REAL
reader (`data_gate.detector_readers.read_detector`), so the drill can never
write something the gate would read differently from what happened — a
commissioned row reads MET, a failed drill reads UNMET, and no case reads
UNMEASURABLE. Everything runs against an in-memory S3 double; no AWS.
"""

from __future__ import annotations

import ast
import dataclasses
import datetime as dt
import json
import pathlib
import sys

import pytest
import yaml

from data_gate import detector_readers
from data_gate.descriptors import load_units
from data_gate.producers import freshness_commissioning as drill
from tests.data_gate_support import EmptyStore

REPO = pathlib.Path(__file__).resolve().parents[1]
MONITOR = REPO / "infrastructure" / "lambdas" / "freshness-monitor" / "index.py"
WORKFLOW = REPO / ".github" / "workflows" / "phase-exit-metrics.yml"

#: A Monday that is the first NYSE trading day of November 2026, after the close.
DRILL_INSTANT = dt.datetime(2026, 11, 2, 22, 0, tzinfo=dt.timezone.utc)


def _monitor_frozenset(name: str) -> frozenset[str]:
    tree = ast.parse(MONITOR.read_text())
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [getattr(node, "target", None)]
        if any(isinstance(t, ast.Name) and t.id == name for t in targets):
            return frozenset(ast.literal_eval(node.value.args[0]))
    raise AssertionError(f"{name} not found in {MONITOR}")


def test_spec_fields_match_the_monitor():
    """A field the Lambda starts threading must be threaded by the drill too —
    otherwise the drill grades a different row than the one that pages."""
    assert drill.SPEC_FIELDS == _monitor_frozenset("_SPEC_FIELDS")


def test_the_fired_state_is_one_the_monitor_pages_on():
    assert drill._FIRED_STATE in _monitor_frozenset("_ALERTING_STATES")


class _Clock:
    def __init__(self, start: dt.datetime) -> None:
        self.now = start

    def __call__(self) -> dt.datetime:
        self.now += dt.timedelta(milliseconds=5)
        return self.now


class _FakeS3:
    """HEAD-only double: ``{key: LastModified}``; anything else is a 404."""

    def __init__(self, objects: dict[str, dt.datetime]) -> None:
        self.objects = objects
        self.calls: list[tuple[str, str]] = []

    def head_object(self, *, Bucket: str, Key: str) -> dict:  # noqa: N803 - boto3 shape
        self.calls.append(("head", Key))
        if Key not in self.objects:
            error = Exception("Not Found")
            error.response = {"Error": {"Code": "404"}}  # type: ignore[attr-defined]
            raise error
        return {"LastModified": self.objects[Key]}


ROW_KEY = "reference/price_cache/_freshness.json"


def _registry(**row_overrides) -> dict:
    row = {
        "artifact_id": "price_cache_freshness_sentinel",
        "s3_key_template": ROW_KEY,
        "cadence": "eod_sf",
        "sla_minutes_after_cron": 0,
        "owner_repo": "nousergon-data",
        "created_at": "2026-01-01",
    }
    row.update(row_overrides)
    return {
        "defaults": {"s3_bucket": "alpha-engine-research", "grace_period_cycles": 2, "severity": "warning"},
        "artifacts": [row],
    }


@pytest.fixture(scope="module")
def units():
    return {u.unit_id: u for u in load_units()}


def _detector(unit):
    return next(d for d in unit.raw["detectors"] if d["kind"] == drill.DETECTOR_KIND)


def _drill_d03(units, s3, registry=None):
    specs, broken = drill.build_specs(registry or _registry())
    assert not broken
    unit = units["D03"]
    return drill.drill_unit(unit, _detector(unit), specs, s3=s3, clock=_Clock(DRILL_INSTANT), run_ref="test-run")


def _read(units, unit_id: str, receipt: dict):
    key = f"commissioning/{unit_id}/freshness-monitor/latest.json"
    store = EmptyStore({key: json.dumps(receipt).encode()})
    return detector_readers.read_detector(store, units[unit_id])


def test_a_fresh_row_is_induced_fires_clears_and_reads_met(units):
    s3 = _FakeS3({ROW_KEY: DRILL_INSTANT - dt.timedelta(hours=1)})
    receipt, rows, why = _drill_d03(units, s3)
    assert why is None and receipt is not None
    assert receipt["outcome"] == "induced", receipt
    assert receipt["fired"]["subject"] == "price_cache_freshness_sentinel"
    assert rows[0]["states"] == {"baseline": "fresh", "induced": "stale", "cleared": "fresh"}
    # Three real probes, and nothing but reads.
    assert s3.calls == [("head", ROW_KEY)] * 3
    reading = _read(units, "D03", receipt)
    assert reading.met and not reading.unmeasurable, reading.detail


def test_the_threshold_is_restored_even_when_the_probe_raises():
    from nousergon_lib import artifact_freshness

    original = artifact_freshness._freshness_floor
    with pytest.raises(RuntimeError):
        with drill.tightened_threshold():
            assert artifact_freshness._freshness_floor is not original
            raise RuntimeError("probe blew up")
    assert artifact_freshness._freshness_floor is original


def test_a_row_already_stale_is_inconclusive_and_reads_unmet(units):
    s3 = _FakeS3({ROW_KEY: DRILL_INSTANT - dt.timedelta(days=30)})
    receipt, rows, _ = _drill_d03(units, s3)
    assert receipt["outcome"] == "inconclusive"
    assert "baseline reads 'stale'" in receipt["problem"]
    assert rows[0]["states"] == {"baseline": "stale"}
    reading = _read(units, "D03", receipt)
    assert not reading.met and not reading.unmeasurable, reading.detail


def test_a_row_that_does_not_fire_when_tightened_is_inconclusive(units, monkeypatch):
    """If the seam stops moving the threshold, the drill must not claim a fire."""
    monkeypatch.setattr(drill, "tightened_threshold", lambda: _noop())
    s3 = _FakeS3({ROW_KEY: DRILL_INSTANT - dt.timedelta(hours=1)})
    receipt, rows, _ = _drill_d03(units, s3)
    assert receipt["outcome"] == "inconclusive"
    assert "did not fire" in receipt["problem"]
    assert not _read(units, "D03", receipt).met


def _noop():
    import contextlib

    return contextlib.nullcontext()


def test_a_deadline_row_is_induced_without_its_regrade(units):
    """`deadline_local` can only re-admit an on-time row, so the induced probe
    drops it — and the restored probe keeps it."""
    # 15:30 ET, inside [midnight, 16:00 ET]: with the re-grade kept, the
    # tightened probe would be re-admitted as `fresh` and the drill could
    # never fire on a deadline row.
    written = dt.datetime(2026, 11, 2, 20, 30, tzinfo=dt.timezone.utc)
    s3 = _FakeS3({ROW_KEY: written})
    receipt, rows, _ = _drill_d03(units, s3, _registry(deadline_local="16:00 America/New_York"))
    assert receipt["outcome"] == "induced", receipt
    assert rows[0]["states"]["induced"] == "stale"


def test_a_subject_missing_from_the_registry_is_inconclusive(units):
    specs, _ = drill.build_specs(_registry(artifact_id="something_else"))
    unit = units["D03"]
    receipt, _, why = drill.drill_unit(
        unit, _detector(unit), specs, s3=_FakeS3({}), clock=_Clock(DRILL_INSTANT), run_ref="t"
    )
    assert why is None
    assert receipt["outcome"] == "inconclusive"
    assert "not in the published registry" in receipt["problem"]


def test_an_event_driven_only_unit_gets_no_receipt(units):
    """D39's only row is event_driven: nothing to tighten, and a receipt would
    say nothing the absent one does not."""
    specs, _ = drill.build_specs(
        _registry(artifact_id="thinktank_inst_ownership", cadence="event_driven", liveness_via="x")
    )
    unit = units["D39"]
    receipt, _, why = drill.drill_unit(
        unit, _detector(unit), specs, s3=_FakeS3({}), clock=_Clock(DRILL_INSTANT), run_ref="t"
    )
    assert receipt is None and "event_driven" in why


def test_a_unit_with_no_checkable_subject_gets_no_receipt(units):
    """A receipt for a unit whose descriptor names no subject would turn an
    UNMET row UNMEASURABLE (detector_readers), so none is written."""
    unit = units["D03"]
    bare = dataclasses.replace(unit, raw={**unit.raw, "registry_rows": []})
    receipt, _, why = drill.drill_unit(
        bare, _detector(bare), {}, s3=_FakeS3({}), clock=_Clock(DRILL_INSTANT), run_ref="t"
    )
    assert receipt is None and "no registry_rows subject" in why


def test_run_drill_covers_every_live_freshness_monitor_unit(units):
    expected = {
        u.unit_id
        for u in units.values()
        if not u.retired and any(d.get("kind") == drill.DETECTOR_KIND for d in u.raw.get("detectors") or [])
    }
    receipts, skipped = drill.run_drill(
        list(units.values()), {}, s3=_FakeS3({}), clock=_Clock(DRILL_INSTANT), run_ref="t"
    )
    graded = {r["unit_id"] for r in receipts.values()} | set(skipped)
    assert graded == expected
    for key, receipt in receipts.items():
        assert key == f"commissioning/{receipt['unit_id']}/freshness-monitor/latest.json"


@pytest.mark.parametrize(
    "day, due",
    [
        (dt.date(2026, 11, 2), True),  # Monday, first session of November
        (dt.date(2026, 11, 1), False),  # Sunday
        (dt.date(2026, 11, 3), False),  # second session
        (dt.date(2027, 1, 4), True),  # 1 Jan 2027 is a holiday (Friday)
        (dt.date(2027, 1, 1), False),
    ],
)
def test_drill_day_is_the_first_session_of_the_month(day, due):
    assert drill.is_drill_day(day) is due


def test_not_due_touches_no_aws(monkeypatch, capsys):
    monkeypatch.setattr(drill, "is_drill_day", lambda _day: False)
    monkeypatch.setitem(sys.modules, "boto3", None)  # any import of boto3 would raise
    assert drill.main(["--only-if-due"]) == 0
    assert "not due" in capsys.readouterr().out


def _utc(*args):
    return dt.datetime(*args, tzinfo=dt.timezone.utc)


def test_schedule_hour_matches_the_workflow_cron():
    cron = yaml.safe_load(WORKFLOW.read_text())[True]["schedule"][0]["cron"]
    minute, hour, *_rest = cron.split()
    assert (minute, int(hour)) == ("0", drill.SCHEDULE_UTC_HOUR)


@pytest.mark.parametrize(
    "started, session",
    [
        (_utc(2026, 11, 2, 22, 0), dt.date(2026, 11, 2)),  # on time
        (_utc(2026, 11, 2, 23, 59), dt.date(2026, 11, 2)),  # late, same UTC day
        (_utc(2026, 11, 3, 0, 7), dt.date(2026, 11, 2)),  # past 00:00Z (10-16 start)
        (_utc(2026, 11, 3, 2, 53), dt.date(2026, 11, 2)),  # 10-13 start
        (_utc(2026, 11, 3, 5, 0), dt.date(2026, 11, 2)),  # 5h late
        (_utc(2026, 11, 3, 21, 59), dt.date(2026, 11, 2)),  # ~24h late is still that fire
        (_utc(2026, 11, 3, 22, 0), dt.date(2026, 11, 3)),  # next fire
    ],
)
def test_late_start_keeps_its_scheduled_date(started, session):
    assert drill.scheduled_session_date(started) == session


def _run_only_if_due(monkeypatch, capsys, started):
    monkeypatch.setattr(drill, "_now", lambda: started)
    monkeypatch.setitem(sys.modules, "boto3", None)  # past the gate, boto3 import raises
    try:
        code = drill.main(["--only-if-due"])
    except ImportError:
        return "due", capsys.readouterr().out
    return ("skipped" if code == 0 else f"exit {code}"), capsys.readouterr().out


@pytest.mark.parametrize(
    "started, verdict",
    [
        (_utc(2026, 11, 2, 22, 0), "due"),
        (_utc(2026, 11, 3, 0, 7), "due"),  # the delayed-past-midnight case
        (_utc(2026, 11, 3, 4, 59), "due"),
        (_utc(2026, 11, 2, 0, 30), "skipped"),  # Nov 1 (Sunday) fire, started late
        (_utc(2026, 11, 3, 22, 5), "skipped"),  # second session's fire
        (_utc(2026, 11, 4, 1, 0), "skipped"),  # second session's fire, late
        (_utc(2026, 12, 2, 1, 0), "due"),  # Dec 1 (Tuesday) fire, started past midnight
    ],
)
def test_only_if_due_judges_the_scheduled_day_not_the_start_day(monkeypatch, capsys, started, verdict):
    assert _run_only_if_due(monkeypatch, capsys, started)[0] == verdict


def test_the_drill_rides_an_existing_scheduled_job():
    """No new workflow (fleet GHA-minutes cap): one step on the daily
    `trigger_observation` job, gated to the drill day by the script itself,
    and run even when the producer before it failed."""
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["trigger_observation"]["steps"]
    step = next(s for s in steps if "freshness_commissioning" in str(s.get("run", "")))
    assert "--only-if-due" in step["run"]
    assert step.get("if") == "${{ !cancelled() }}"
    assert steps.index(step) == len(steps) - 1


#: Units whose live, un-parked ARTIFACT_REGISTRY row was declared in
#: ``registry_rows`` alongside a ``freshness-monitor`` detector so the monthly
#: drill commissions them (alpha-engine-config-I10795). Each key is a fixed key
#: under ``market_data/``, which the drill role's
#: ``FreshnessDrillProbeTheDrilledRows`` grant already reads (nous-ergon-ops-PR1697).
NEWLY_DRILLED = {
    "D02": ("historical_constituents", "market_data/historical_constituents.json", "saturday_sf"),
    "D21": ("metron_close_history_consolidated", "market_data/close_history/consolidated.json", "eod_sf"),
    "D29": ("metron_analyst", "market_data/analyst/latest.json", "eod_sf"),
    "D30": ("metron_sentiment", "market_data/sentiment/latest.json", "eod_sf"),
}


@pytest.mark.parametrize("unit_id", sorted(NEWLY_DRILLED))
def test_a_declared_registry_row_is_drilled_and_reads_met(units, unit_id):
    artifact_id, key, cadence = NEWLY_DRILLED[unit_id]
    unit = units[unit_id]
    assert [r for r in unit.raw["registry_rows"]] == [artifact_id]
    assert "registry_rows_broken" not in unit.raw  # the rows were unparked (I10776)
    assert key in unit.raw["writes"]
    assert key.startswith("market_data/") and "{" not in key
    specs, broken = drill.build_specs(_registry(artifact_id=artifact_id, s3_key_template=key, cadence=cadence))
    assert not broken
    s3 = _FakeS3({key: DRILL_INSTANT - dt.timedelta(hours=1)})
    receipt, rows, why = drill.drill_unit(
        unit, _detector(unit), specs, s3=s3, clock=_Clock(DRILL_INSTANT), run_ref="t"
    )
    assert why is None and receipt is not None
    assert receipt["outcome"] == "induced", receipt
    assert receipt["fired"]["subject"] == artifact_id
    assert rows[0]["states"] == {"baseline": "fresh", "induced": "stale", "cleared": "fresh"}
    assert s3.calls == [("head", key)] * 3
    assert _read(units, unit_id, receipt).met


class _ListingS3(_FakeS3):
    """Adds the ``list_objects_v2`` paginator a date-templated row is probed with."""

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        outer = self

        class _Paginator:
            def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803 - boto3 shape
                outer.calls.append(("list", Prefix))
                yield {
                    "Contents": [
                        {"Key": k, "LastModified": lm} for k, lm in sorted(outer.objects.items()) if k.startswith(Prefix)
                    ]
                }

        return _Paginator()


def test_d19_drills_its_own_daily_closes_row_and_reads_met(units):
    """D19 writes the same ``staging/daily_closes/{date}.parquet`` as D17
    (``writes_shared_with``), so its freshness-monitor detector names the same
    row D17's does. The row is date-templated: the drill LISTs the prefix the
    drill role's ``FreshnessDrillListTheDrilledPrefixes`` grant already covers."""
    unit = units["D19"]
    assert unit.raw["registry_rows"] == ["daily_closes_parquet"]
    assert "staging/daily_closes/{date}.parquet" in unit.raw["writes"]
    assert units["D17"].raw["registry_rows"] == ["daily_closes_parquet"]
    template = "staging/daily_closes/{trading_day}.parquet"
    specs, broken = drill.build_specs(
        _registry(artifact_id="daily_closes_parquet", s3_key_template=template, cadence="weekday_sf")
    )
    assert not broken
    key = f"staging/daily_closes/{DRILL_INSTANT:%Y-%m-%d}.parquet"
    s3 = _ListingS3({key: DRILL_INSTANT - dt.timedelta(hours=1)})
    receipt, rows, why = drill.drill_unit(unit, _detector(unit), specs, s3=s3, clock=_Clock(DRILL_INSTANT), run_ref="t")
    assert why is None and receipt is not None
    assert receipt["outcome"] == "induced", receipt
    assert receipt["fired"]["subject"] == "daily_closes_parquet"
    assert rows[0]["states"] == {"baseline": "fresh", "induced": "stale", "cleared": "fresh"}
    assert {c[0] for c in s3.calls} == {"list"}
    assert all(prefix.startswith("staging/daily_closes/") for _, prefix in s3.calls)
    assert _read(units, "D19", receipt).met
