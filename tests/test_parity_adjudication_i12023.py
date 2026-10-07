"""The frozen parity report's exceptions clear only through an independent, per-path adjudication.

`alpha-engine-config-I12023`, with the Crucible v2 review's conditions:

(a) attribution is per PATH, each path names its settling input(s) with the v1
    source key and version id, and a key clears only when every breaching path
    is attributed and none is unattributed;
(b) settling is a named lag from the vendor contract (`dates.bar_settlement`),
    every settling input is re-checked after settlement against an independent
    source, and a post-settlement mismatch stays RED;
plus the ruling on the eight half-cent closes: ``precision_limited`` is neither
a breach nor verified-equal.

The frozen report's own failed reading is never rewritten: the clause prints it
on every reading, adjudicated or not.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json

import pytest

from data_gate import evidence
from data_gate import parity_adjudication as adj
from data_gate.cutover import CUTOVER_UTC, cutover_trading_day

CUTOVER_DAY = cutover_trading_day()
AFTER = dt.date(2026, 10, 2)
REPORT_KEY = evidence.parity_store_key(CUTOVER_DAY)
RECORD_0 = f"parity_adjudication/{CUTOVER_DAY.isoformat()}/0001.json"
RECORD_1 = f"parity_adjudication/{CUTOVER_DAY.isoformat()}/0002.json"
ARTIFACT_KEY = f"parity_adjudication/{CUTOVER_DAY.isoformat()}/artifacts/arcticdb-universe.json"
ARTIFACT_BYTES = json.dumps({"comparator": "shadow.parity._compare_frames as_of cutover", "breach_paths": ["A.Close"]}).encode()

# v1 read the bar at 16:08 ET on the bar's own day: provisional by `dates.bar_settlement`.
V1_READ_PROVISIONAL = "2026-09-28T20:08:48Z"
# 18:30 ET on the bar's day: already settled, so a difference is not settling.
V1_READ_SETTLED = "2026-09-28T22:29:00Z"


class _Store:
    uri = "file://test"

    def __init__(self, documents: dict[str, dict]) -> None:
        self.raw = {k: v if isinstance(v, bytes) else json.dumps(v, sort_keys=True).encode("utf-8")
                    for k, v in documents.items()}

    def list_keys(self, prefix: str = ""):
        return [k for k in sorted(self.raw) if k.startswith(prefix)]

    def get_bytes(self, key: str) -> bytes:
        if key not in self.raw:
            raise FileNotFoundError(key)
        return self.raw[key]


def _report() -> dict:
    """The 09-28 shape in miniature: one mismatch, one in-region row, one unsettled D-1 pair."""
    return {
        "schema_version": "data_parity_report.v3",
        "trading_day": CUTOVER_DAY.isoformat(),
        "generated_at": "2026-09-28T23:43:46Z",
        "met": False,
        "summary": {"total": 4, "match": 2, "mismatch": 1, "in_region_only": 1, "settling_bar_keys": 1, "v1_cause": 0},
        "keys": [
            {"key": "market_data/technicals/rating_performance.json", "verdict": "mismatch", "values": {"breaches": 2}},
            {"key": "arcticdb/universe", "verdict": "in_region_only"},
            {"key": "a.json", "verdict": "match"},
            {"key": "b.json", "verdict": "match"},
        ],
        "prior_day_settled": {
            "unsettled": 1,
            "unsettled_examples": [{"key": "features/2026-09-25/technical.parquet", "breaches": 2}],
        },
    }


def _bar(ident: str, *, v1=175.19, shadow=175.21, settled=175.21, read=V1_READ_PROVISIONAL,
         row_date="2026-09-28", source="vendor", quantum=None) -> dict:
    item = {
        "id": ident,
        "kind": adj.INPUT_KIND_BAR,
        "source_key": f"reference/price_cache/{ident}.parquet",
        "source_version_id": "oOYVTh9Bo6",
        "row_date": row_date,
        "v1_read_utc": read,
        "v1_value": v1,
        "shadow_value": shadow,
    }
    if settled is not None:
        if source == "vendor":
            item["settled"] = {"source_kind": adj.SETTLED_SOURCE_VENDOR, "vendor": "polygon-grouped-daily",
                               "retrieved_at_utc": "2026-10-06T14:00:00Z", "value": settled}
        else:
            item["settled"] = {"source_kind": adj.SETTLED_SOURCE_V1_LATER, "source_key": item["source_key"],
                               "source_version_id": "later", "source_last_modified_utc": source, "value": settled,
                               "same_definition": True, "definition_note": "the same daily Close field"}
        if quantum is not None:
            item["settled"]["quantum"] = quantum
    return item


def _record(report_bytes: bytes, *, supersedes=None) -> dict:
    return {
        "schema_version": adj.ADJUDICATION_SCHEMA_VERSION,
        "supersedes": supersedes,
        "report": {"key": REPORT_KEY, "sha256": hashlib.sha256(report_bytes).hexdigest()},
        "exceptions": [
            {
                "key": "market_data/technicals/rating_performance.json",
                "kind": "mismatch",
                "measured": {"breaches": 2},
                "inputs": [_bar("SPY")],
                "attributions": [
                    {"path": "$.ic_series[742].ic", "inputs": ["SPY"]},
                    {"path": "$.segments.all.20.1.ic_mean", "inputs": ["SPY"]},
                ],
            },
            {
                "key": "arcticdb/universe",
                "kind": "in_region_only",
                "measured": {"artifact": {"key": ARTIFACT_KEY, "sha256": hashlib.sha256(ARTIFACT_BYTES).hexdigest()}},
                "inputs": [_bar("A")],
                "attributions": [{"path": "A.Close", "inputs": ["A"]}],
            },
            {
                "key": "features/2026-09-25/technical.parquet",
                "kind": "prior_day_unsettled",
                "measured": {"breaches": 2},
                "inputs": [_bar("AA", v1=42.87, shadow=42.85, settled=42.85, row_date="2026-09-25",
                                read="2026-09-25T20:07:56Z", source="2026-09-28T20:08:48Z")],
                "attributions": [
                    {"path": "AA.momentum_5d", "inputs": ["AA"]},
                    {"path": "AA.price_vs_ma50", "inputs": ["AA"]},
                ],
            },
        ],
    }


def _store(record_mutator=None, *, extra_records: list[dict] | None = None) -> _Store:
    report = _report()
    report_bytes = json.dumps(report, sort_keys=True).encode("utf-8")
    record = _record(report_bytes)
    if record_mutator:
        record_mutator(record)
    docs = {REPORT_KEY: report, RECORD_0: record, ARTIFACT_KEY: ARTIFACT_BYTES}
    for i, extra in enumerate(extra_records or []):
        docs[f"parity_adjudication/{CUTOVER_DAY.isoformat()}/{i + 2:04d}.json"] = extra
    return _Store(docs)


def _read(store):
    return evidence.read_parity(store, trading_day=AFTER)


def _grades(store):
    report_bytes = store.get_bytes(REPORT_KEY)
    keys = adj.adjudication_record_keys(store.list_keys("parity_adjudication/"), CUTOVER_DAY)
    return adj.grade_record(
        json.loads(store.get_bytes(keys[-1])), record_key=keys[-1], report_key=REPORT_KEY,
        report=json.loads(report_bytes), report_bytes=report_bytes,
        previous_record_key=keys[-2] if len(keys) > 1 else None, cutover_utc=CUTOVER_UTC,
        fetch_bytes=store.get_bytes,
    )


def test_without_a_record_the_frozen_reading_is_unchanged():
    store = _Store({REPORT_KEY: _report()})
    reading = _read(store)
    assert reading.met is False
    assert "adjudicat" not in reading.detail.lower()
    assert reading.evidence == (REPORT_KEY,)


def test_a_complete_independent_adjudication_reads_met_and_still_prints_the_failed_reading():
    reading = _read(_store())
    assert reading.met is True, reading.detail
    assert "2/4 published keys match" in reading.detail  # the original reading, verbatim
    assert "FROZEN" in reading.detail
    assert "ADJUDICATED" in reading.detail
    assert reading.evidence == (REPORT_KEY, RECORD_0)


def test_a_post_settlement_mismatch_stays_red():
    """Condition (b)'s regression test: settling was a reason to wait, never to drop."""
    def mutate(record):
        record["exceptions"][1]["inputs"][0]["settled"]["value"] = 175.30  # vendor's final bar != shadow
    reading = _read(_store(mutate))
    assert reading.met is False
    assert "breach" in reading.detail and "still differs from the shadow" in reading.detail


def test_an_input_settled_at_v1s_read_is_a_breach_not_a_settling_difference():
    def mutate(record):
        record["exceptions"][1]["inputs"][0]["v1_read_utc"] = V1_READ_SETTLED
    assert _read(_store(mutate)).met is False
    assert any(g.verdict == adj.BREACH for g in _grades(_store(mutate)).grades)


def test_one_unattributed_path_leaves_the_whole_key_red():
    def mutate(record):
        exc = record["exceptions"][0]
        exc["attributions"] = exc["attributions"][:1]
        exc["unattributed"] = ["$.segments.all.20.1.ic_mean"]
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["market_data/technicals/rating_performance.json"]
    assert grade.verdict == adj.UNATTRIBUTED
    assert _read(_store(mutate)).met is False


def test_listing_fewer_paths_than_the_report_measured_proves_nothing():
    def mutate(record):
        record["exceptions"][0]["attributions"].pop()
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["market_data/technicals/rating_performance.json"]
    assert grade.verdict == adj.INVALID and "every breaching path" in grade.detail


def test_the_breach_count_is_the_reports_not_the_records_claim():
    """The review's blocking change: a record cannot shrink the measurement to fit its own path list."""
    def mutate(record):
        record["exceptions"][0]["measured"]["breaches"] = 1
        record["exceptions"][0]["attributions"].pop()
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["market_data/technicals/rating_performance.json"]
    assert grade.verdict == adj.INVALID and "the measurement it must match has 2" in grade.detail


def test_a_report_row_without_a_breach_count_cannot_be_adjudicated():
    store = _store()
    report = json.loads(store.get_bytes(REPORT_KEY))
    del report["keys"][0]["values"]
    record = json.loads(store.get_bytes(RECORD_0))
    record["report"]["sha256"] = hashlib.sha256(json.dumps(report, sort_keys=True).encode()).hexdigest()
    store = _Store({REPORT_KEY: report, RECORD_0: record, ARTIFACT_KEY: ARTIFACT_BYTES})
    grade = {g.key: g for g in _grades(store).grades}["market_data/technicals/rating_performance.json"]
    assert grade.verdict == adj.INVALID and "no breach count" in grade.detail


def test_an_in_region_row_takes_its_paths_from_the_pinned_comparator_artifact():
    def tampered(record):
        record["exceptions"][1]["measured"]["artifact"]["sha256"] = "f" * 64
    grade = {g.key: g for g in _grades(_store(tampered)).grades}["arcticdb/universe"]
    assert grade.verdict == adj.INVALID and "not the pinned" in grade.detail

    def other_path(record):
        record["exceptions"][1]["attributions"] = [{"path": "B.Close", "inputs": ["A"]}]
    grade = {g.key: g for g in _grades(_store(other_path)).grades}["arcticdb/universe"]
    assert grade.verdict == adj.INVALID and "breach_paths" in grade.detail

    def no_artifact(record):
        record["exceptions"][1]["measured"] = {"breaches": 1}
    grade = {g.key: g for g in _grades(_store(no_artifact)).grades}["arcticdb/universe"]
    assert grade.verdict == adj.INVALID and "artifact" in grade.detail


def test_a_v1_later_reference_must_declare_the_same_definition():
    """v1's hundreds-rounded volume is a different definition and never a reference."""
    def mutate(record):
        del record["exceptions"][2]["inputs"][0]["settled"]["same_definition"]
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["features/2026-09-25/technical.parquet"]
    assert grade.verdict == adj.INVALID and "same_definition" in grade.detail

    def no_note(record):
        record["exceptions"][2]["inputs"][0]["settled"]["definition_note"] = " "
    grade = {g.key: g for g in _grades(_store(no_note)).grades}["features/2026-09-25/technical.parquet"]
    assert grade.verdict == adj.INVALID and "definition_note" in grade.detail


def test_a_pending_resettle_is_red_and_named():
    def mutate(record):
        del record["exceptions"][1]["inputs"][0]["settled"]
    reading = _read(_store(mutate))
    assert reading.met is False and "pending" in reading.detail


def test_the_collector_is_never_a_settled_source():
    def mutate(record):
        record["exceptions"][2]["inputs"][0]["settled"]["source_last_modified_utc"] = "2026-09-29T22:21:44Z"
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["features/2026-09-25/technical.parquet"]
    assert grade.verdict == adj.INVALID and "collector" in grade.detail

    def mutate_kind(record):
        record["exceptions"][1]["inputs"][0]["settled"]["source_kind"] = "collector"
    assert _read(_store(mutate_kind)).met is False


def test_a_derived_breach_needs_an_input_that_itself_differs():
    def mutate(record):
        record["exceptions"][1]["inputs"][0]["shadow_value"] = 175.19  # input agrees with v1
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["arcticdb/universe"]
    assert grade.verdict == adj.INVALID and "cannot explain a breach" in grade.detail


def test_a_half_cent_close_against_a_cents_reference_is_precision_limited_not_cleared():
    """The shape of the 09-25 half-cent closes: shadow 103.075, a cents-rounded reference 103.08."""
    def mutate(record):
        record["exceptions"][2]["inputs"][0].update(v1_value=103.05, shadow_value=103.075)
        record["exceptions"][2]["inputs"][0]["settled"].update(value=103.08, quantum=0.01)
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["features/2026-09-25/technical.parquet"]
    assert grade.verdict == adj.PRECISION_LIMITED
    assert _read(_store(mutate)).met is False


def test_precision_limited_needs_a_declared_quantum_and_a_true_rounding():
    def no_quantum(record):
        record["exceptions"][2]["inputs"][0].update(v1_value=103.07, shadow_value=103.075)
        record["exceptions"][2]["inputs"][0]["settled"].update(value=103.08)
    grade = {g.key: g for g in _grades(_store(no_quantum)).grades}["features/2026-09-25/technical.parquet"]
    assert grade.verdict == adj.BREACH

    def wrong_rounding(record):
        record["exceptions"][2]["inputs"][0].update(v1_value=103.07, shadow_value=103.074)
        record["exceptions"][2]["inputs"][0]["settled"].update(value=103.08, quantum=0.01)
    grade = {g.key: g for g in _grades(_store(wrong_rounding)).grades}["features/2026-09-25/technical.parquet"]
    assert grade.verdict == adj.BREACH


def test_a_missing_exception_is_red():
    def mutate(record):
        record["exceptions"].pop(1)
    grades = {g.key: g for g in _grades(_store(mutate)).grades}
    assert grades["arcticdb/universe"].verdict == adj.MISSING
    assert _read(_store(mutate)).met is False


def test_a_record_pinned_to_different_report_bytes_is_invalid():
    def mutate(record):
        record["report"]["sha256"] = "0" * 64
    reading = _read(_store(mutate))
    assert reading.met is False and "INVALID" in reading.detail and "changed after" in reading.detail


def test_the_newest_record_must_supersede_its_predecessor():
    base = _store()
    first = json.loads(base.get_bytes(RECORD_0))
    broken = copy.deepcopy(first)  # supersedes None, but a record already exists
    store = _store(extra_records=[broken])
    reading = _read(store)
    assert reading.met is False and "chain is broken" in reading.detail

    chained = copy.deepcopy(first)
    chained["supersedes"] = RECORD_0
    store = _store(extra_records=[chained])
    reading = _read(store)
    assert reading.met is True and reading.evidence == (REPORT_KEY, RECORD_1)


def test_a_release_without_the_vendors_release_time_is_invalid():
    def mutate(record):
        item = record["exceptions"][1]["inputs"][0]
        item["kind"] = adj.INPUT_KIND_RELEASE
    grade = {g.key: g for g in _grades(_store(mutate)).grades}["arcticdb/universe"]
    assert grade.verdict == adj.INVALID and "released_at_utc" in grade.detail

    def released(record):
        item = record["exceptions"][1]["inputs"][0]
        item["kind"] = adj.INPUT_KIND_RELEASE
        item["settled"]["released_at_utc"] = "2026-09-28T20:15:00Z"
    assert _read(_store(released)).met is True


def test_adjudication_records_are_never_listed_as_parity_reports():
    assert not adj.ADJUDICATION_KEY_PREFIX.startswith(evidence.PARITY_KEY_PREFIX)
    assert evidence._parity_report_day(RECORD_0) is None


@pytest.mark.parametrize("verdict", ["mismatch", "in_region_only", "shadow_missing", "unmeasurable"])
def test_every_non_passing_row_is_an_exception(verdict):
    report = {"keys": [{"key": "k", "verdict": verdict}, {"key": "m", "verdict": "match"},
                       {"key": "v", "verdict": "v1_cause"}]}
    assert set(adj.report_exceptions(report)) == {"k"}
