"""`data.<unit>.run_record` reads the run manifests, and reads them correctly.

`alpha-engine-config-I10810` deliverable 3, plus the ArcticDB-probe clause of
`alpha-engine-config-I10772`.

Four properties, each of which a plausible shortcut gets wrong:

1. **Absence is UNMET, a denied read is UNMEASURABLE.** "We looked and there was
   nothing" and "we could not look" have opposite owners; collapsing them makes
   a producer outage and an IAM regression the same row.
2. **A `failed` manifest SATISFIES this clause.** The requirement is a record on
   BOTH paths. Grading a failure record as UNMET would reward a unit for writing
   nothing on its bad days.
3. **Empty-but-fresh is counted from `guards[].verdict`, never `rows_out == 0`.**
   An auto-skipped phase legitimately records 0 rows with a `not_applicable`
   verdict; counting zeros would file every one of those as an empty write.
4. **An ArcticDB unit's evidence is the in-region probe, and a withheld probe is
   UNMEASURABLE — never MET.** The gate never opens ArcticDB.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest

from data_gate import descriptors, evidence
from data_gate.store import LocalStore

TRADING_DAY = dt.date(2026, 9, 14)


@pytest.fixture(scope="module")
def units() -> dict[str, descriptors.Unit]:
    return {u.unit_id: u for u in descriptors.load_units()}


def _manifest(**over) -> dict:
    base = {
        "schema_version": "data_run_manifest.v1",
        "run_id": "01JBX0000000000000000000AA",
        "unit_id": "D19",
        "trigger": "scheduled",
        "trading_day": TRADING_DAY.isoformat(),
        "calendar_date": TRADING_DAY.isoformat(),
        "status": "ok",
        "reason": "",
        "started": "2026-09-14T21:00:00Z",
        "finished": "2026-09-14T21:12:00Z",
        "code_sha": "a" * 40,
        "log_location": "cloudwatch:/alpha-engine/data-spot",
        "inputs": [],
        "outputs": [{"key": "market_data/eod_closes/latest.json", "rows_out": 896}],
        "rows_in": 900,
        "rows_out": 896,
        "rows_rejected": [],
        "cost_usd": 0.0,
        "compute": {"instance_type": "c6i.large", "spot": True},
    }
    base.update(over)
    return base


def _write(root: pathlib.Path, unit_id: str, run_id: str, manifest: dict) -> None:
    path = root / "runs" / unit_id / TRADING_DAY.isoformat() / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest))


class DeniedStore:
    """Every read and every listing is an access failure, not an absence."""

    uri = "s3://denied/denied"

    def get_bytes(self, key: str) -> bytes:
        raise PermissionError(f"AccessDenied: {key}")

    def list_keys(self, prefix: str = ""):
        raise PermissionError(f"AccessDenied: {prefix}")


# ── 1. Absence versus denial ────────────────────────────────────────────────


def test_no_manifest_for_the_day_is_unmet_not_unmeasurable(tmp_path, units):
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is False
    assert "no run manifest" in reading.detail


def test_a_denied_listing_is_unmeasurable_never_unmet(units):
    reading = evidence.read_run_record(DeniedStore(), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is True
    assert "could not list" in reading.detail


# ── 2. A record on BOTH paths ───────────────────────────────────────────────


def test_an_ok_run_is_met(tmp_path, units):
    _write(tmp_path, "D19", "01JBX0000000000000000000AA", _manifest())
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is True
    assert "1 run(s) recorded" in reading.detail


def test_a_failed_run_still_satisfies_the_clause(tmp_path, units):
    """The requirement is that the execution left a record, not that it
    succeeded. A failure record is the record the requirement is about."""
    _write(
        tmp_path, "D19", "01JBX0000000000000000000AA",
        _manifest(status="failed", reason="ClientError: AccessDenied", outputs=[], rows_out=0),
    )
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is True
    assert "'failed': 1" in reading.detail


def test_a_not_applicable_run_satisfies_the_clause_and_is_counted(tmp_path, units):
    _write(
        tmp_path, "D19", "01JBX0000000000000000000AA",
        _manifest(status="not_applicable", reason="not_a_trading_day", outputs=[], rows_out=0),
    )
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is True
    assert "'not_applicable': 1" in reading.detail


def test_a_third_ok_but_degraded_state_is_unmet_and_named(tmp_path, units):
    _write(tmp_path, "D19", "01JBX0000000000000000000AA", _manifest(status="degraded"))
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is False
    assert "degraded" in reading.detail
    assert "third ok-but-degraded state" in reading.detail


def test_a_foreign_schema_version_is_unmet(tmp_path, units):
    _write(
        tmp_path, "D19", "01JBX0000000000000000000AA",
        _manifest(schema_version="run_manifest.v2"),
    )
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is False
    assert "run_manifest.v2" in reading.detail


def test_an_unparseable_manifest_is_unmeasurable(tmp_path, units):
    path = tmp_path / "runs" / "D19" / TRADING_DAY.isoformat() / "x.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json")
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is True


def test_every_run_of_the_day_is_read_not_just_the_latest(tmp_path, units):
    _write(tmp_path, "D19", "01JBX0000000000000000000AA", _manifest())
    _write(tmp_path, "D19", "01JBX0000000000000000000BB", _manifest(status="failed"))
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert "2 run(s) recorded" in reading.detail
    assert "'failed': 1" in reading.detail and "'ok': 1" in reading.detail


# ── 3. Objective 6 counts guard verdicts, not zeros ─────────────────────────


def test_empty_fresh_is_counted_from_the_guard_verdict():
    empty = _manifest(
        rows_out=0,
        outputs=[{"key": "k", "rows_out": 0}],
        guards=[{"guard": "empty_fresh", "mode": "observe", "verdict": "empty_fresh", "detail": "0 rows"}],
    )
    assert evidence.empty_fresh_runs([empty]) == [empty["run_id"]]


def test_a_legitimate_zero_with_a_not_applicable_verdict_is_not_counted():
    """The case the naive `rows_out == 0` rule gets backwards: an auto-skipped
    phase publishes nothing and says so through its guard."""
    skipped = _manifest(
        rows_out=0,
        outputs=[],
        guards=[
            {
                "guard": "empty_fresh",
                "mode": "observe",
                "verdict": "not_applicable",
                "detail": "same-date auto-skip; there is no new write to grade",
            }
        ],
    )
    assert evidence.empty_fresh_runs([skipped]) == []


def test_a_run_with_rows_and_an_ok_verdict_is_not_counted():
    assert evidence.empty_fresh_runs([
        _manifest(guards=[{"guard": "empty_fresh", "mode": "observe", "verdict": "ok", "detail": "896 rows"}])
    ]) == []


def test_a_run_with_no_guards_at_all_is_not_counted_as_empty_fresh():
    """Silence is not evidence of an empty write either — a unit whose guard
    never ran is red on the guard-commissioning clause, not on this one."""
    assert evidence.empty_fresh_runs([_manifest(rows_out=0, outputs=[])]) == []


def test_the_reader_reports_the_empty_fresh_count(tmp_path, units):
    _write(
        tmp_path, "D19", "01JBX0000000000000000000AA",
        _manifest(
            rows_out=0,
            outputs=[{"key": "k", "rows_out": 0}],
            guards=[{"guard": "empty_fresh", "mode": "observe", "verdict": "empty_fresh", "detail": "0 rows"}],
        ),
    )
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert "empty-but-fresh runs (guards[].verdict == 'empty_fresh') 1" in reading.detail


# ── 4. ArcticDB units read the probe, and a withheld probe is UNMEASURABLE ──


ARCTIC_UNITS = ("D13", "D18", "D32")


def _probe(**libraries) -> dict:
    return {
        "schema_version": 1,
        "as_of": f"{TRADING_DAY.isoformat()}T21:30:00Z",
        "libraries": libraries,
    }


def _write_probe(root: pathlib.Path, document: dict) -> None:
    path = root / "probes" / "arctic" / f"{TRADING_DAY.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document))


@pytest.mark.parametrize("unit_id", ARCTIC_UNITS)
def test_an_arcticdb_unit_declares_probe_backed_evidence(units, unit_id):
    assert (units[unit_id].raw.get("arcticdb_evidence") or {}).get("via"), (
        f"{unit_id} writes ArcticDB, which the gate cannot read; its run evidence must be "
        "declared as the in-region probe (alpha-engine-config-I10772)"
    )


@pytest.mark.parametrize("unit_id", ARCTIC_UNITS)
def test_a_manifest_without_the_probe_is_unmeasurable_never_met(tmp_path, units, unit_id):
    """The closes-when of `alpha-engine-config-I10772`: an absent probe file
    leaves the clause UNMEASURABLE, never MET — even with a perfectly good run
    manifest sitting next to it."""
    _write(tmp_path, unit_id, "01JBX0000000000000000000AA", _manifest(unit_id=unit_id))
    reading = evidence.read_run_record(LocalStore(tmp_path), units[unit_id], trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is True
    assert "probe" in reading.detail


def test_a_withheld_library_reading_is_unmeasurable_never_met(tmp_path, units):
    _write(tmp_path, "D32", "01JBX0000000000000000000AA", _manifest(unit_id="D32"))
    _write_probe(
        tmp_path,
        _probe(
            universe={"read_ok": True, "row_count": 2_100_000, "symbol_count": 903,
                      "last_index_date": TRADING_DAY.isoformat()},
            macro={"read_ok": False, "row_count": None, "symbol_count": None, "last_index_date": None},
        ),
    )
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D32"], trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is True
    assert "macro" in reading.detail


def test_a_complete_probe_plus_a_manifest_is_met(tmp_path, units):
    _write(tmp_path, "D32", "01JBX0000000000000000000AA", _manifest(unit_id="D32"))
    _write_probe(
        tmp_path,
        _probe(
            universe={"read_ok": True, "row_count": 2_100_000, "symbol_count": 903,
                      "last_index_date": TRADING_DAY.isoformat()},
            macro={"read_ok": True, "row_count": 41_000, "symbol_count": 38,
                   "last_index_date": TRADING_DAY.isoformat()},
            delisted_history={"read_ok": True, "row_count": 190_000, "symbol_count": 120,
                              "last_index_date": TRADING_DAY.isoformat()},
        ),
    )
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D32"], trading_day=TRADING_DAY)
    assert reading.met is True
    assert "universe: 2100000 rows" in reading.detail
    assert "1 run(s) recorded" in reading.detail


def test_a_non_arcticdb_unit_needs_no_probe(tmp_path, units):
    """The probe requirement applies only where it is DECLARED — a unit that
    publishes an S3 key is graded on its manifest alone."""
    _write(tmp_path, "D19", "01JBX0000000000000000000AA", _manifest())
    reading = evidence.read_run_record(LocalStore(tmp_path), units["D19"], trading_day=TRADING_DAY)
    assert reading.met is True
    assert "probe" not in reading.detail
