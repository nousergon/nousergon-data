"""Consumer pin for D49 (NDX membership + weight) inside its one declared reader.

D49 (`collectors/nasdaq100.py`) publishes `market_data/index_constituents/NDX.json`
under `contracts/ndx_constituents.schema.json`. Its only declared consumer is D48's
weights reader, `collectors/index_contributions.py::_default_weights_source`. That
reader lives in THIS repository, so there is no cross-repo boundary to copy a schema
across. The pin is this test, declared in `registry.d/units/D49-nasdaq100-constituents.yaml`
as an `in_repo_reader` consumer pin, and the data gate's `schema_contract` clause
checks that this file exists and references the read site (alpha-engine-config-I11282).

The test runs the REAL producer (`nasdaq100.collect`, membership from the live-captured
2026-09-21 fixture the producer's own tests use), validates the body it writes against
the published contract, and hands that body to the REAL consumer. Nothing on either
side is a hand-written stand-in.

It caught a drift on its first run. The reader took the weights' date from `as_of`,
a field the contract does not have (`additionalProperties: false`; the producer writes
`date`), so D48 would have published `weights_as_of: null` on every NDX decomposition.
"""
from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from collectors import index_contributions as ic
from collectors import nasdaq100

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((REPO_ROOT / "contracts" / "ndx_constituents.schema.json").read_text())
FIXTURES = Path(__file__).parent / "fixtures"
NDX_KEY = "market_data/index_constituents/NDX.json"
RUN_DATE = "2026-09-21"


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _ProducerS3:
    """What `nasdaq100.collect` touches: one fundamentals archive, and the PUTs."""

    def __init__(self) -> None:
        self.puts: dict[str, dict] = {}

    def list_objects_v2(self, Bucket, Prefix):  # noqa: N803 - boto3's keyword names
        return {"Contents": [{"Key": "archive/fundamentals/2026-09-20.json"}]}

    def get_object(self, Bucket, Key):  # noqa: N803
        caps = {"AAPL": {"market_cap_raw": 3000.0}, "MSFT": {"market_cap_raw": 2000.0}}
        return {"Body": _Body(json.dumps(caps).encode())}

    def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
        self.puts[Key] = json.loads(Body)


class _ConsumerS3:
    """What `_default_weights_source` reads for NDX: exactly one key."""

    def __init__(self, objects: dict[str, dict]) -> None:
        self._objects = objects

    def get_object(self, Bucket, Key):  # noqa: N803
        return {"Body": _Body(json.dumps(self._objects[Key]).encode())}


@pytest.fixture
def produced_ndx(tmp_path, monkeypatch) -> dict:
    membership = json.loads((FIXTURES / "nasdaq100_rows_full.json").read_text())
    producer_s3 = _ProducerS3()
    monkeypatch.setattr(nasdaq100.boto3, "client", lambda *a, **k: producer_s3)
    monkeypatch.setattr(nasdaq100, "_try_invesco_holdings", lambda: None)
    monkeypatch.setattr(
        nasdaq100,
        "_fetch_nasdaq100_membership",
        lambda: nasdaq100.parse_nasdaq100_response(membership),
    )
    monkeypatch.setattr(nasdaq100, "_CACHE_PATH", tmp_path / "nasdaq100_cache.csv")
    result = nasdaq100.collect(bucket="fake-bucket", run_date=RUN_DATE)
    assert result["status"] == "ok"
    return producer_s3.puts[NDX_KEY]


def test_the_produced_body_satisfies_the_published_contract(produced_ndx) -> None:
    jsonschema.Draft202012Validator.check_schema(SCHEMA)
    jsonschema.validate(instance=produced_ndx, schema=SCHEMA)


def test_default_weights_source_reads_the_contracted_fields(produced_ndx, monkeypatch) -> None:
    monkeypatch.setattr(ic.boto3, "client", lambda *a, **k: _ConsumerS3({NDX_KEY: produced_ndx}))
    weights = ic._default_weights_source("fake-bucket")("NDX")

    assert weights.weight_map == produced_ndx["weight_map"]
    assert weights.weight_map["AAPL"] + weights.weight_map["MSFT"] == pytest.approx(1.0)
    assert weights.method == "modified_cap_approx"
    # The contract's date field is `date`. Reading `as_of` returned None here.
    assert weights.as_of == RUN_DATE

