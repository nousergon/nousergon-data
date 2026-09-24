"""`schema_contract` grades each consumer pin on evidence, never on a non-empty list.

`alpha-engine-config-I11282`. The clause read MET for any non-empty
`contract.consumer_pins`, so a free-text string was indistinguishable from a
real pin: D10's first pin byte-copied the BODY schema into a consumer that only
ever reads the object KEY, and this clause would have graded it MET.

The four cases the issue names — a missing path, a stale copy, an unreachable
repository, a correct pin — plus the shape rules that make the comparison
honest and the descriptor validation that keeps free text out.
"""

from __future__ import annotations

import base64
import copy
import json
import pathlib

import pytest
import yaml

from data_gate import consumer_pins, evidence
from data_gate.descriptors import DescriptorError, Unit, load_units
from data_gate.sources import GitHubContents

from tests.data_gate_support import TRADING_DAY, EmptyStore

REPO = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def units():
    return load_units()


def _unit(units, unit_id: str, **contract_overrides) -> Unit:
    base = next(u for u in units if u.unit_id == unit_id)
    raw = copy.deepcopy(base.raw)
    raw["contract"].update(contract_overrides)
    return Unit(unit_id=base.unit_id, path=base.path, raw=raw)


def _producer(name: str) -> dict:
    return json.loads((REPO / "contracts" / name).read_text(encoding="utf-8"))


def _opener(visible: set[str], files: dict[tuple[str, str], object]):
    """A contents API: ``files`` maps (repo, path) to a JSON-able document,
    raw bytes, or the string ``"dir"``."""
    calls: list[str] = []

    def opener(url: str, headers: dict[str, str]):
        calls.append(url)
        tail = url.split("/repos/nousergon/", 1)[1]
        repo, _, rest = tail.partition("/")
        if repo not in visible:
            return 404, b'{"message": "Not Found"}'
        if not rest:
            return 200, b"{}"
        path = rest[len("contents/") :]
        if (repo, path) not in files:
            return 404, b'{"message": "Not Found"}'
        value = files[(repo, path)]
        if value == "dir":
            return 200, b"[]"
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        body = {"type": "file", "encoding": "base64", "content": base64.b64encode(raw).decode()}
        return 200, json.dumps(body).encode()

    return opener, calls


def _store(opener) -> EmptyStore:
    store = EmptyStore()
    store.github_contents = GitHubContents("t", opener=opener)
    return store


def _read(store, unit):
    return evidence.read_base(store, unit, "schema_contract", trading_day=TRADING_DAY)


D20_FILES = {
    ("metron", "tests/contracts/metron_closes.schema.json"): _producer("metron_closes.schema.json"),
    ("metron", "tests/contracts/metron_fx.schema.json"): _producer("metron_fx.schema.json"),
}


# ── the four cases the issue names ───────────────────────────────────────────


def test_a_correct_pin_reads_met(units):
    opener, _ = _opener({"metron"}, D20_FILES)
    reading = _read(_store(opener), _unit(units, "D20"))
    assert reading.met, reading.detail
    assert "pins held 2/2" in reading.detail


def test_a_pin_naming_a_missing_path_reads_unmet(units):
    files = dict(D20_FILES)
    files.pop(("metron", "tests/contracts/metron_fx.schema.json"))
    opener, _ = _opener({"metron"}, files)
    reading = _read(_store(opener), _unit(units, "D20"))
    assert not reading.met and not reading.unmeasurable
    assert "metron:tests/contracts/metron_fx.schema.json" in reading.detail
    assert "ABSENT" in reading.detail


def test_a_stale_copy_reads_unmet_naming_both_hashes(units):
    stale = _producer("metron_fx.schema.json")
    stale["required"] = [*stale.get("required", []), "a_field_the_producer_dropped"]
    files = {**D20_FILES, ("metron", "tests/contracts/metron_fx.schema.json"): stale}
    opener, _ = _opener({"metron"}, files)
    reading = _read(_store(opener), _unit(units, "D20"))
    assert not reading.met and not reading.unmeasurable
    assert "STALE" in reading.detail
    assert consumer_pins.shape_hash(stale) in reading.detail
    assert consumer_pins.shape_hash(_producer("metron_fx.schema.json")) in reading.detail


def test_an_unreachable_repository_reads_unmeasurable_never_met(units):
    opener, _ = _opener(set(), D20_FILES)  # metron invisible to the token
    reading = _read(_store(opener), _unit(units, "D20"))
    assert reading.unmeasurable and not reading.met
    assert "not visible" in reading.detail


# ── the shape rules ──────────────────────────────────────────────────────────


def test_no_github_reader_is_unmeasurable_never_met(units):
    reading = _read(EmptyStore(), _unit(units, "D20"))
    assert reading.unmeasurable and not reading.met
    assert "DATA_GATE_GITHUB_TOKEN" in reading.detail


def test_annotation_only_differences_are_not_drift():
    """A producer adding `x-*` gate annotations or rewording a description has
    not changed what a consumer validates against."""
    producer = _producer("metron_fx.schema.json")
    copy_ = json.loads(json.dumps(producer))
    copy_.pop("x-key-pattern", None)
    copy_["description"] = "a consumer's own wording"
    copy_["$comment"] = "pinned copy"
    assert consumer_pins.shape_hash(copy_) == consumer_pins.shape_hash(producer)


def test_a_property_named_like_an_annotation_is_still_compared():
    """`description` inside `properties` is a field name, not a keyword."""
    a = {"type": "object", "properties": {"description": {"type": "string"}}}
    b = {"type": "object", "properties": {"description": {"type": "integer"}}}
    c = {"type": "object", "properties": {}}
    assert consumer_pins.shape_hash(a) != consumer_pins.shape_hash(b)
    assert consumer_pins.shape_hash(a) != consumer_pins.shape_hash(c)


def test_a_directory_is_not_a_pinned_contract(units):
    files = {**D20_FILES, ("metron", "tests/contracts/metron_fx.schema.json"): "dir"}
    opener, _ = _opener({"metron"}, files)
    reading = _read(_store(opener), _unit(units, "D20"))
    assert not reading.met and "resolves to a dir" in reading.detail


def test_a_key_template_pin_is_compared_with_the_units_writes(units):
    """D10 — the case that motivated the issue: the consumer reads the KEY."""
    path = ("crucible-dashboard", "tests/contracts/fundamentals_snapshot_s3_key_template.json")
    good, _ = _opener({"crucible-dashboard"}, {path: {"object_key_template": "archive/fundamentals/{date}.json"}})
    assert _read(_store(good), _unit(units, "D10")).met
    bad, _ = _opener({"crucible-dashboard"}, {path: {"object_key_template": "archive/fundamentals/{date}.parquet"}})
    reading = _read(_store(bad), _unit(units, "D10"))
    assert not reading.met and "STALE" in reading.detail
    body, _ = _opener({"crucible-dashboard"}, {path: _producer("fundamentals_snapshot.schema.json")})
    reading = _read(_store(body), _unit(units, "D10"))
    assert not reading.met and "object_key_template" in reading.detail


def test_an_in_repo_reader_pin_is_graded_in_this_tree(units):
    """D15's consumer is in this repository; its pin is the test that drives
    the reader, and it must actually reference the reader it claims."""
    assert _read(EmptyStore(), _unit(units, "D15")).met
    pin = dict(_unit(units, "D15").raw["contract"]["consumer_pins"][0])
    pin["read_site"] = "features/compute.py::a_reader_the_test_never_calls"
    reading = _read(EmptyStore(), _unit(units, "D15", consumer_pins=[pin]))
    assert not reading.met and "never references" in reading.detail


def test_an_unpinned_consumer_is_a_finding_even_beside_a_good_pin(units):
    opener, _ = _opener({"metron"}, D20_FILES)
    unit = _unit(units, "D20", unpinned_consumers=["metron:api/services/fx.py"])
    reading = _read(_store(opener), unit)
    assert not reading.met and "no pinned contract file" in reading.detail


def test_one_request_per_pin_per_read_cached_across_units(units):
    opener, calls = _opener({"metron"}, D20_FILES)
    store = _store(opener)
    _read(store, _unit(units, "D20"))
    assert len(calls) == 1 + 2  # the repository once, then each pin once
    _read(store, _unit(units, "D20"))
    assert len(calls) == 3


# ── descriptor validation ────────────────────────────────────────────────────


def _write(tmp_path, units, **contract_overrides):
    raw = copy.deepcopy(next(u for u in units if u.unit_id == "D20").raw)
    raw["contract"].update(contract_overrides)
    (tmp_path / "D20-metron-eod-closes-fx.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")


def test_a_free_text_pin_is_refused(tmp_path, units):
    _write(tmp_path, units, consumer_pins=["metron:tests/contracts/metron_fx.schema.json"])
    with pytest.raises(DescriptorError, match="not a mapping"):
        load_units(tmp_path)


def test_a_pin_kind_outside_the_taxonomy_is_refused(tmp_path, units):
    pin = dict(next(u for u in units if u.unit_id == "D20").raw["contract"]["consumer_pins"][0])
    pin["pins"] = "vibes"
    _write(tmp_path, units, consumer_pins=[pin])
    with pytest.raises(DescriptorError, match="not one of"):
        load_units(tmp_path)


def test_a_body_schema_pin_must_name_one_of_the_units_schemas(tmp_path, units):
    pin = dict(next(u for u in units if u.unit_id == "D20").raw["contract"]["consumer_pins"][0])
    pin["producer"] = "contracts/metron_macro.schema.json"
    _write(tmp_path, units, consumer_pins=[pin])
    with pytest.raises(DescriptorError, match="producer"):
        load_units(tmp_path)


def test_every_pin_in_the_tree_is_structured(units):
    """The migration left no free text behind (load_units would have raised),
    and it is non-vacuous: the tree really does declare pins of each kind."""
    kinds = {
        pin["pins"]
        for unit in units
        for pin in (unit.raw.get("contract") or {}).get("consumer_pins") or []
    }
    assert kinds == {"body_schema", "key_template", "in_repo_reader"}
