"""Declared ``trigger.schedule`` vs the LIVE trigger — `alpha-engine-config-I11189`
deliverable 2, with `-I11194`'s DISABLED-annotation check.

The closes-when this file carries: the reconciliation is GREEN when every
declaration matches what fires, and demonstrably RED when a descriptor's
declared schedule is edited away from its live trigger. Both directions run
against the REAL committed descriptors, with the live side synthesised from
them — so the only thing a test changes is the one fact it is about.

**No AWS.** The live side is an in-memory observation document; the producer's
AWS reads are exercised through fake clients.
"""

from __future__ import annotations

import copy
import datetime as dt
import json

import pytest

from data_gate.cadence import unit_cadence
from data_gate.clauses import generate
from data_gate.descriptors import Unit, load_units
from data_gate.producers import trigger_observation as producer
from data_gate.read import load_phases
from data_gate.trigger_reconcile import (
    FIRE_TOLERANCE,
    OBSERVATION_KEY,
    OBSERVATION_SCHEMA,
    declared_disabled,
    fires_between,
    owner_key,
    read_triggers_reconciled,
    reconcile_unit,
    units_in_scope,
)

from tests.data_gate_support import DeniedStore, EmptyStore

#: A Friday morning UTC, after a full week of fires.
NOW = dt.datetime(2026, 9, 25, 5, 0, tzinfo=dt.timezone.utc)
WINDOW_START = NOW - dt.timedelta(days=21)
READ_AT = NOW + dt.timedelta(hours=1)


def _iso(instant: dt.datetime) -> str:
    return (
        instant.astimezone(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _declared_text(unit: Unit) -> str:
    return str(unit.raw["trigger"]["schedule"]).partition(" — ")[0].strip()


def _live_matching(units: list[Unit]) -> dict:
    """The live side exactly as every descriptor declares it — execution starts
    40s after each declared fire, schedule objects carrying the declared
    expression and state."""
    owners: dict[str, dict] = {}
    in_scope, _ = units_in_scope(units)
    for unit in in_scope:
        key = owner_key(unit)
        if key is None:
            continue
        kind = key.split(":", 1)[0]
        if kind == "step-functions":
            entry = owners.setdefault(
                key, {"kind": kind, "status": "observed", "execution_starts": []}
            )
            fires = fires_between(unit_cadence(unit.raw), WINDOW_START, NOW)
            starts = set(entry["execution_starts"]) | {
                _iso(f + dt.timedelta(seconds=40)) for f in fires
            }
            entry["execution_starts"] = sorted(starts, reverse=True)
        else:
            owners[key] = {
                "kind": kind,
                "status": "observed",
                "state": "DISABLED" if declared_disabled(unit) else "ENABLED",
                "schedule_expression": _declared_text(unit),
                "timezone": "UTC",
            }
    return owners


def _document(owners: dict, *, as_of: dt.datetime = NOW) -> dict:
    return {
        "schema_version": OBSERVATION_SCHEMA,
        "as_of": _iso(as_of),
        "window_start": _iso(as_of - dt.timedelta(days=21)),
        "lookback_days": 21,
        "owners": owners,
    }


def _store(document: dict) -> EmptyStore:
    return EmptyStore({OBSERVATION_KEY: json.dumps(document).encode("utf-8")})


def _edit(units: list[Unit], unit_id: str, **trigger) -> list[Unit]:
    edited: list[Unit] = []
    for unit in units:
        if unit.unit_id == unit_id:
            raw = copy.deepcopy(unit.raw)
            raw["trigger"].update(trigger)
            unit = Unit(unit_id=unit.unit_id, path=unit.path, raw=raw)
        edited.append(unit)
    return edited


@pytest.fixture(scope="module")
def units() -> list[Unit]:
    return load_units()


@pytest.fixture(scope="module")
def live(units) -> dict:
    return _live_matching(units)


# ---------------------------------------------------------------------------
# Green when declared == live
# ---------------------------------------------------------------------------


def test_green_when_every_declaration_matches_its_live_trigger(units, live):
    reading = read_triggers_reconciled(_store(_document(live)), units, as_of=READ_AT)
    assert reading.met, reading.detail
    assert not reading.unmeasurable
    assert "0 divergent, 0 unreconcilable" in reading.detail


def test_the_scope_is_not_vacuous(units):
    """A reconciliation over no units would read green by construction."""
    in_scope, _ = units_in_scope(units)
    kinds = {str(u.raw["trigger"]["kind"]) for u in in_scope}
    assert len(in_scope) >= 30
    assert {
        "step-functions",
        "eventbridge-rule",
        "eventbridge-scheduler",
        "github-actions",
    } <= kinds
    assert {u.unit_id for u in in_scope if declared_disabled(u)} >= {"D33", "D38"}


# ---------------------------------------------------------------------------
# Red when a declaration is edited away from the live trigger (the I11189 shape)
# ---------------------------------------------------------------------------


def test_red_when_a_postclose_descriptor_is_edited_back_to_1645(units, live):
    """The exact I11189 defect: D19 declares 16:45 over a pipeline that starts 16:00."""
    edited = _edit(units, "D19", schedule="weekdays 16:45 America/New_York")
    reading = read_triggers_reconciled(_store(_document(live)), edited, as_of=READ_AT)
    assert not reading.met and not reading.unmeasurable, reading.detail
    assert "DIVERGENT D19" in reading.detail
    assert "16:00 America/New_York" in reading.detail, (
        "the finding must name where the live starts cluster"
    )
    assert "DIVERGENT D20" not in reading.detail


def test_red_when_a_rule_expression_is_edited(units, live):
    edited = _edit(
        units, "D33", schedule="cron(0 9 ? * MON-FRI *)"
    )  # annotation dropped
    reading = read_triggers_reconciled(_store(_document(live)), edited, as_of=READ_AT)
    assert "DIVERGENT D33" in reading.detail and not reading.met


def test_a_drift_inside_the_tolerance_is_not_a_finding(units, live):
    shifted = copy.deepcopy(live)
    key = "step-functions:ne-postclose-trading-pipeline"
    shifted[key]["execution_starts"] = [
        _iso(
            dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            + FIRE_TOLERANCE
            - dt.timedelta(minutes=1)
        )
        for s in shifted[key]["execution_starts"]
    ]
    reading = read_triggers_reconciled(_store(_document(shifted)), units, as_of=READ_AT)
    assert reading.met, reading.detail


# ---------------------------------------------------------------------------
# The DISABLED annotation, both directions (I11194's remaining deliverable)
# ---------------------------------------------------------------------------


def test_a_stale_disabled_annotation_over_an_enabled_trigger_is_a_finding(units, live):
    """The annotation grades run_record by NO gate, so a stale one hides a live trigger."""
    enabled = copy.deepcopy(live)
    enabled["eventbridge-rule:alpha-engine-daily-heal"]["state"] = "ENABLED"
    reading = read_triggers_reconciled(_store(_document(enabled)), units, as_of=READ_AT)
    assert not reading.met and not reading.unmeasurable
    assert "DIVERGENT D33" in reading.detail and "ENABLED" in reading.detail


def test_a_scheduler_entry_enabled_under_a_disabled_annotation_is_a_finding(
    units, live
):
    enabled = copy.deepcopy(live)
    enabled["eventbridge-scheduler:default/alpha-engine-crypto-balances-15min"][
        "state"
    ] = "ENABLED"
    reading = read_triggers_reconciled(_store(_document(enabled)), units, as_of=READ_AT)
    assert "DIVERGENT D38" in reading.detail and not reading.met


def test_a_disabled_live_trigger_without_the_annotation_is_a_finding(units, live):
    edited = _edit(units, "D38", schedule="rate(15 minutes)")
    reading = read_triggers_reconciled(_store(_document(live)), edited, as_of=READ_AT)
    assert "DIVERGENT D38" in reading.detail and "DISABLED" in reading.detail


# ---------------------------------------------------------------------------
# No data is not green
# ---------------------------------------------------------------------------


def test_absent_observation_is_unmeasurable(units):
    reading = read_triggers_reconciled(EmptyStore(), units, as_of=READ_AT)
    assert reading.unmeasurable and not reading.met
    assert OBSERVATION_KEY in reading.detail


def test_denied_observation_is_unmeasurable(units):
    reading = read_triggers_reconciled(DeniedStore(), units, as_of=READ_AT)
    assert reading.unmeasurable and not reading.met


def test_stale_observation_is_unmeasurable(units, live):
    reading = read_triggers_reconciled(
        _store(_document(live)), units, as_of=NOW + dt.timedelta(days=3)
    )
    assert reading.unmeasurable and "stale" in reading.detail


def test_wrong_schema_is_unmeasurable(units, live):
    document = _document(live)
    document["schema_version"] = "something.v0"
    assert read_triggers_reconciled(_store(document), units, as_of=READ_AT).unmeasurable


def test_an_owner_the_producer_could_not_read_is_unmeasurable_never_met(units, live):
    missing = copy.deepcopy(live)
    missing["eventbridge-scheduler:default/alpha-engine-crypto-balances-15min"] = {
        "kind": "eventbridge-scheduler",
        "status": "not_found",
        "error": "ResourceNotFoundException",
    }
    reading = read_triggers_reconciled(_store(_document(missing)), units, as_of=READ_AT)
    assert reading.unmeasurable and not reading.met
    assert "UNRECONCILABLE D38" in reading.detail


def test_an_owner_absent_from_the_document_is_unmeasurable(units, live):
    missing = {
        k: v
        for k, v in live.items()
        if k != "step-functions:ne-preopen-trading-pipeline"
    }
    reading = read_triggers_reconciled(_store(_document(missing)), units, as_of=READ_AT)
    assert reading.unmeasurable and "UNRECONCILABLE D17" in reading.detail


# ---------------------------------------------------------------------------
# Execution-history semantics
# ---------------------------------------------------------------------------


def _d19(units) -> Unit:
    return next(u for u in units if u.unit_id == "D19")


def test_manual_reruns_never_make_a_declared_fire_match(units):
    """Operator reruns scatter across the day; none sits at the declared instant."""
    reruns = [
        _iso(NOW - dt.timedelta(days=d, hours=3, minutes=7)) for d in range(1, 20)
    ]
    observation = {
        "step-functions:ne-postclose-trading-pipeline": {
            "status": "observed",
            "execution_starts": reruns,
        }
    }
    result = reconcile_unit(
        _d19(units), observation, window_start=WINDOW_START, window_end=NOW
    )
    assert result.outcome == "divergent", result.detail


def test_one_missed_fire_is_tolerated_and_named(units, live):
    starts = list(
        live["step-functions:ne-postclose-trading-pipeline"]["execution_starts"]
    )
    dropped = starts.pop(5)
    observation = {
        "step-functions:ne-postclose-trading-pipeline": {
            "status": "observed",
            "execution_starts": starts,
        }
    }
    result = reconcile_unit(
        _d19(units), observation, window_start=WINDOW_START, window_end=NOW
    )
    assert result.outcome == "reconciled", result.detail
    assert "with no start" in result.detail and dropped[:10] in result.detail


def test_a_schedule_moved_inside_the_window_is_caught_before_the_majority_shifts(
    units, live
):
    """The two most recent declared fires unmatched is a divergence even when
    older fires still match."""
    starts = sorted(
        live["step-functions:ne-postclose-trading-pipeline"]["execution_starts"]
    )
    moved = starts[:-2] + [
        _iso(
            dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            + dt.timedelta(minutes=45)
        )
        for s in starts[-2:]
    ]
    observation = {
        "step-functions:ne-postclose-trading-pipeline": {
            "status": "observed",
            "execution_starts": moved,
        }
    }
    result = reconcile_unit(
        _d19(units), observation, window_start=WINDOW_START, window_end=NOW
    )
    assert result.outcome == "divergent" and "two most recent" in result.detail


# ---------------------------------------------------------------------------
# github-actions: the workflow file on this checkout is the live schedule
# ---------------------------------------------------------------------------


def test_d39_declared_schedule_matches_its_workflow_file(units):
    """A CI-reachable live surface, graded directly (no producer needed)."""
    d39 = next(u for u in units if u.unit_id == "D39")
    result = reconcile_unit(d39, {}, window_start=WINDOW_START, window_end=NOW)
    assert result.outcome == "reconciled", result.detail


def test_a_workflow_cron_edited_away_from_the_declaration_is_a_finding(units, tmp_path):
    d39 = next(u for u in units if u.unit_id == "D39")
    workflow = str(d39.raw["trigger"]["detail"]).split(",")[0].strip()
    path = tmp_path / workflow
    path.parent.mkdir(parents=True)
    path.write_text("on:\n  schedule:\n    - cron: '40 12 * * 0'\n", encoding="utf-8")
    result = reconcile_unit(
        d39, {}, window_start=WINDOW_START, window_end=NOW, repo_root=tmp_path
    )
    assert result.outcome == "divergent", result.detail


# ---------------------------------------------------------------------------
# On the board
# ---------------------------------------------------------------------------


def test_the_clause_is_on_the_board_in_phase1_and_red_over_an_empty_store():
    board = generate(
        EmptyStore(), load_units(), load_phases(), trading_day=dt.date(2026, 9, 24)
    )
    clause = next(c for c in board if c.name == "data.phase1.triggers_reconciled")
    assert clause.phase == "data-phase1"
    assert clause.unmeasurable and not clause.met


# ---------------------------------------------------------------------------
# The producer
# ---------------------------------------------------------------------------


class _NotFound(Exception):
    response = {"Error": {"Code": "ResourceNotFoundException"}}


class _Denied(Exception):
    response = {"Error": {"Code": "AccessDeniedException"}}


class _Paginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **_):
        return iter(self.pages)


class _Sfn:
    def __init__(self, starts):
        self.starts = starts

    def get_paginator(self, name):
        assert name == "list_executions"
        return _Paginator([{"executions": [{"startDate": s} for s in self.starts]}])


class _Events:
    def describe_rule(self, Name):
        if Name == "alpha-engine-daily-heal":
            return {
                "State": "DISABLED",
                "ScheduleExpression": "cron(0 9 ? * MON-FRI *)",
            }
        raise _NotFound()


class _Scheduler:
    def get_schedule(self, GroupName, Name):
        if (GroupName, Name) == ("default", "alpha-engine-crypto-balances-15min"):
            return {
                "State": "DISABLED",
                "ScheduleExpression": "rate(15 minutes)",
                "ScheduleExpressionTimezone": "UTC",
            }
        raise _Denied()


def test_producer_owner_set_is_derived_from_the_descriptors(units):
    keys = producer.owner_keys(units)
    assert "step-functions:ne-postclose-trading-pipeline" in keys
    assert "eventbridge-rule:alpha-engine-daily-heal" in keys
    assert "eventbridge-scheduler:default/alpha-engine-crypto-balances-15min" in keys
    assert not any(k.startswith("github-actions") for k in keys)
    assert len(keys) == len(set(keys))


def test_producer_records_observations_and_failures_per_owner():
    recent = NOW - dt.timedelta(days=1)
    old = NOW - dt.timedelta(days=40)
    document = producer.build_document(
        [
            "step-functions:ne-postclose-trading-pipeline",
            "eventbridge-rule:alpha-engine-daily-heal",
            "eventbridge-rule:gone",
            "eventbridge-scheduler:default/alpha-engine-crypto-balances-15min",
            "eventbridge-scheduler:default/denied",
        ],
        sfn=_Sfn([recent, old]),
        events=_Events(),
        scheduler=_Scheduler(),
        now=NOW,
    )
    owners = document["owners"]
    assert document["schema_version"] == OBSERVATION_SCHEMA
    assert owners["step-functions:ne-postclose-trading-pipeline"][
        "execution_starts"
    ] == [_iso(recent)]
    assert owners["eventbridge-rule:alpha-engine-daily-heal"]["state"] == "DISABLED"
    assert owners["eventbridge-rule:gone"]["status"] == "not_found"
    assert (
        owners["eventbridge-scheduler:default/alpha-engine-crypto-balances-15min"][
            "timezone"
        ]
        == "UTC"
    )
    assert owners["eventbridge-scheduler:default/denied"] == {
        "kind": "eventbridge-scheduler",
        "status": "error",
        "error": "AccessDeniedException",
    }
