"""The console reachability receipt WRITER, graded by the reader it feeds.

`alpha-engine-config-I10795` (A3 part 2, the producer half). The payloads under
``tests/fixtures/console_doctor/`` were rendered by `nousergon-console`'s own
``GET /doctor/<id>`` JSON view (``console/render/json.py::payload`` over
``console/diagnose.py::doctor``), not hand-written:

* ``d03_unreported.json`` — a console serving only D03's real registry row
  (``registry.d/data-collector-d03-prices.yaml``): declared, nothing reporting,
  nothing linking to it. Recorded from a live ``console serve``.
* ``d03_reachable.json`` — the same id with an observation claim and an inbound
  edge: every link ok.

The load-bearing tests are the round trips: what this writer files, the reader
grades — MET only for the reachable payload, and UNMET naming the broken link
for the other. A writer whose output the reader cannot grade MET under ANY
console answer would leave the 40 clauses red forever while looking finished.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import pathlib

import pytest

from data_gate import console_entity_readers as cer
from data_gate.descriptors import load_units
from data_gate.producers import console_entity as producer
from tests.data_gate_support import EmptyStore

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "console_doctor"
NOW = dt.datetime(2026, 10, 7, 14, 0, tzinfo=dt.timezone.utc)
URL = "http://127.0.0.1:5180"

ALL_UNITS = load_units()
UNITS = {u.unit_id: u for u in ALL_UNITS}


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _as(payload: dict, unit) -> dict:
    """The recorded payload re-addressed to another unit's component id."""
    doc = copy.deepcopy(payload)
    doc["identifier"] = unit.component_id
    return doc


def _fetch_from(payloads: dict[str, dict], status: int = 200):
    calls: list[str] = []

    def fetch(url: str) -> tuple[int, bytes]:
        calls.append(url)
        component_id = url.rsplit("/doctor/", 1)[1]
        return status, json.dumps(payloads[component_id]).encode()

    fetch.calls = calls  # type: ignore[attr-defined]
    return fetch


def _store_with(collected) -> EmptyStore:
    return EmptyStore(
        {
            item.key.removeprefix(f"{producer.STORE_PREFIX}/"): json.dumps(item.receipt).encode()
            for item in collected
        }
    )


class _S3:
    def __init__(self) -> None:
        self.puts: dict[str, dict] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 - boto3's spelling
        assert ContentType == "application/json"
        self.puts[f"{Bucket}/{Key}"] = json.loads(Body)


# ---------------------------------------------------------------------------
# The contract with the reader: same schema, same key.
# ---------------------------------------------------------------------------


def test_the_reader_and_the_writer_share_one_schema_and_one_key():
    assert cer.RECEIPT_SCHEMA is producer.RECEIPT_SCHEMA
    assert cer.RECEIPT_KEY is producer.RECEIPT_KEY
    unit = UNITS["D03"]
    assert producer.receipt_object_key(unit) == f"data_collection/{cer.receipt_key(unit)}"
    assert producer.receipt_object_key(unit) == "data_collection/console_entity/D03/latest.json"


# ---------------------------------------------------------------------------
# Round trips: the writer's output, graded by the real reader.
# ---------------------------------------------------------------------------


def test_a_fully_reachable_console_answer_is_filed_as_a_receipt_the_reader_grades_met():
    unit = UNITS["D03"]
    collected = producer.collect_receipts(
        [unit], console_url=URL, fetch=_fetch_from({unit.component_id: _fixture("d03_reachable.json")}), now=NOW
    )
    reading = cer.read_console_entity(_store_with(collected), unit, now=NOW + dt.timedelta(hours=1))
    assert reading.met, reading.detail
    assert "renders HEALTHY on 1 reporting claim" in reading.detail


def test_the_recorded_unreported_answer_is_filed_and_reads_unmet_naming_the_broken_links():
    """The live recording: D03 declared, nothing reporting, nothing linking to
    it. Filed as it is — the writer does not decide the verdict — and the reader
    names both failed links."""
    unit = UNITS["D03"]
    collected = producer.collect_receipts(
        [unit], console_url=URL, fetch=_fetch_from({unit.component_id: _fixture("d03_unreported.json")}), now=NOW
    )
    receipt = collected[0].receipt
    assert receipt["entity"] == {"kind": "component", "state": "UNREPORTED", "reporting_claims": 0}
    assert receipt["doctor"]["broken"] == "adapter claim"
    reading = cer.read_console_entity(_store_with(collected), unit, now=NOW)
    assert not reading.met and not reading.unmeasurable
    assert "`adapter claim` failed" in reading.detail
    assert "`relation-reachable` failed" in reading.detail


def test_every_descriptor_gets_a_receipt_the_reader_can_grade():
    """The unit set is derived from the descriptors, and every receipt is
    addressed to its own unit: no unit's clause can be satisfied by another's."""
    reachable = _fixture("d03_reachable.json")
    fetch = _fetch_from({u.component_id: _as(reachable, u) for u in ALL_UNITS})
    collected = producer.collect_receipts(ALL_UNITS, console_url=URL, fetch=fetch, now=NOW)
    assert [c.unit.unit_id for c in collected] == [u.unit_id for u in ALL_UNITS]
    assert len({c.key for c in collected}) == len(ALL_UNITS)
    store = _store_with(collected)
    in_service = [u for u in ALL_UNITS if u.lifecycle == "in-service"]
    assert in_service
    for unit in in_service:
        reading = cer.read_console_entity(store, unit, now=NOW)
        assert reading.met or "partial_exclusion" in reading.detail or reading.detail.startswith("N/A"), (
            unit.unit_id,
            reading.detail,
        )


def test_a_console_that_predates_the_entity_facts_is_never_filed_as_passing():
    """An older console's doctor has no `entity_*` facts. The writer files
    nulls rather than guessing from prose, and the reader grades that UNMET."""
    unit = UNITS["D03"]
    old = _fixture("d03_reachable.json")
    for name in ("entity_kind", "entity_state", "reporting_claims"):
        del old["facts"][name]
    collected = producer.collect_receipts([unit], console_url=URL, fetch=_fetch_from({unit.component_id: old}), now=NOW)
    assert collected[0].receipt["entity"] == {"kind": None, "state": None, "reporting_claims": None}
    reading = cer.read_console_entity(_store_with(collected), unit, now=NOW)
    assert not reading.met
    assert "not a count" in reading.detail


# ---------------------------------------------------------------------------
# Refusals: nothing is filed from a console that could not answer.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate, needle",
    [
        (lambda d: d["index"].update(bootstrap=True), "bootstrap index"),
        (lambda d: d["index"].update(stale=True), "stale"),
        (lambda d: d["index"].pop("stale"), "stale"),
        (lambda d: d.pop("index"), "no `index` freshness block"),
        (lambda d: d.update(view="entity"), "expected 1 / 'doctor'"),
        (lambda d: d.update(schema_version=2), "expected 1 / 'doctor'"),
        (lambda d: d.update(identifier="data-collector-d01-constituents"), "diagnosed"),
    ],
)
def test_an_unusable_console_answer_files_nothing(mutate, needle):
    unit = UNITS["D03"]
    payload = _fixture("d03_reachable.json")
    mutate(payload)
    with pytest.raises(producer.ConsoleUnusable, match=needle):
        producer.collect_receipts([unit], console_url=URL, fetch=_fetch_from({unit.component_id: payload}), now=NOW)


def test_a_non_200_answer_files_nothing():
    unit = UNITS["D03"]
    fetch = _fetch_from({unit.component_id: _fixture("d03_reachable.json")}, status=503)
    with pytest.raises(producer.ConsoleUnusable, match="HTTP 503"):
        producer.collect_receipts([unit], console_url=URL, fetch=fetch, now=NOW)


def test_an_unreachable_console_files_nothing():
    """Nothing listens on port 1: the real urllib path raises ConsoleUnusable."""
    with pytest.raises(producer.ConsoleUnusable, match="unreachable"):
        producer.collect_receipts([UNITS["D03"]], console_url="http://127.0.0.1:1", now=NOW)


def test_an_empty_unit_set_is_refused():
    with pytest.raises(producer.ConsoleUnusable, match="empty set"):
        producer.collect_receipts([], console_url=URL, fetch=_fetch_from({}), now=NOW)


def test_the_component_id_is_url_quoted_into_the_doctor_route():
    assert producer.doctor_url(URL + "/", "a b/c") == f"{URL}/doctor/a%20b%2Fc"


# ---------------------------------------------------------------------------
# main(): writes every receipt plus a run record, or only an error record.
# ---------------------------------------------------------------------------


def test_main_writes_one_receipt_per_unit_and_an_ok_run_record():
    reachable = _fixture("d03_reachable.json")
    s3 = _S3()
    fetch = _fetch_from({u.component_id: _as(reachable, u) for u in ALL_UNITS})
    assert producer.main([], fetch=fetch, s3=s3) == 0
    receipts = {k: v for k, v in s3.puts.items() if "data_collection/console_entity/" in k}
    assert len(receipts) == len(ALL_UNITS)
    assert receipts["alpha-engine-research/data_collection/console_entity/D03/latest.json"]["unit_id"] == "D03"
    records = [v for k, v in s3.puts.items() if k.startswith("alpha-engine-research/data_collection/runs/console_entity/")]
    assert len(records) == 1 and records[0]["status"] == "ok"
    assert records[0]["detail"] == {"receipts": len(ALL_UNITS), "fully_reachable": len(ALL_UNITS)}


def test_main_on_an_unusable_console_writes_no_receipt_and_an_error_run_record():
    s3 = _S3()
    fetch = _fetch_from({u.component_id: _fixture("d03_reachable.json") for u in ALL_UNITS}, status=502)
    with pytest.raises(producer.ConsoleUnusable):
        producer.main([], fetch=fetch, s3=s3)
    assert not [k for k in s3.puts if "data_collection/console_entity/" in k]
    (record,) = s3.puts.values()
    assert record["status"] == "error" and "HTTP 502" in record["error"]


def test_main_dry_run_prints_and_writes_nothing(capsys):
    unit = UNITS["D03"]
    s3 = _S3()
    fetch = _fetch_from({unit.component_id: _fixture("d03_unreported.json")})
    assert producer.main(["--unit", "D03", "--no-write"], fetch=fetch, s3=s3) == 0
    assert s3.puts == {}
    out = capsys.readouterr().out
    assert "D03 data-collector-d03-prices: broken at 'adapter claim'; component UNREPORTED on 0 reporting claim(s)" in out
    assert "nothing written" in out


def test_main_refuses_an_unknown_unit():
    with pytest.raises(producer.ConsoleUnusable, match="no unit descriptor"):
        producer.main(["--unit", "D99", "--no-write"], fetch=_fetch_from({}))
