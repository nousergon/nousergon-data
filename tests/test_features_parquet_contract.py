"""Producer contract test for contracts/features_parquet.schema.json (D31/D50).

alpha-engine-config-I10793, Brian's ruling 2026-10-14 (option (a)): the per-column
contract for ``features/{date}/{group}.parquet`` is a JSON Schema GENERATED from
``features/registry.py::CATALOG`` (``features/gen_parquet_contract.py``), which
crucible-research ``scoring/factor_scoring.py`` and crucible-dashboard
``views/13_Feature_Store.py`` pin a copy of.

Covers:
  1. the committed file is exactly a fresh render of CATALOG — a catalog change
     that does not regenerate the contract fails here, not in a consumer repo;
  2. every CATALOG column is declared in its own group with its JSON type, and
     ``ticker``/``date`` are required the way the writer writes them;
  3. the REAL serialiser (``features/writer.py::snapshot_group_frames``, the
     bytes D31 and D50 both publish) over a full-catalog frame validates;
  4. the contract REJECTS what a pin exists to catch: an undeclared column, a
     missing identity column, an unknown group;
  5. the split keeps parity grading unchanged: every parquet key resolves to this
     contract with the same ``key_date`` settling bar, and
     ``features/registry.json`` resolves to its own contract.
"""

from __future__ import annotations

import io
import json

import numpy as np
import pandas as pd
import pytest

jsonschema = pytest.importorskip("jsonschema")

from contracts import validate_features_parquet
from features import gen_parquet_contract
from features.registry import CATALOG, GROUPS
from features.writer import snapshot_group_frames
from shadow import parity

DATE = "2026-10-14"


def _schema() -> dict:
    return json.loads(gen_parquet_contract.CONTRACT_PATH.read_text(encoding="utf-8"))


def _full_catalog_frame(n_tickers: int = 4) -> pd.DataFrame:
    rng = np.random.default_rng(10793)
    frame = pd.DataFrame({"ticker": [f"T{i}" for i in range(n_tickers)]})
    for entry in CATALOG:
        frame[entry.name] = rng.normal(size=n_tickers).astype("float64")
    # One NaN cell per group: a ticker whose indicator could not be computed.
    for names in GROUPS.values():
        frame.loc[0, names[0]] = np.nan
    return frame


def _snapshot_document(frame: pd.DataFrame) -> dict:
    """What a consumer sees: each published parquet read back, as records."""
    doc = {}
    for group, (_df, body) in snapshot_group_frames(DATE, frame).items():
        read_back = pd.read_parquet(io.BytesIO(body))
        doc[group] = json.loads(read_back.to_json(orient="records"))
    return doc


class TestGenerated:
    def test_committed_contract_is_a_fresh_render_of_catalog(self):
        committed = gen_parquet_contract.CONTRACT_PATH.read_text(encoding="utf-8")
        assert committed == gen_parquet_contract.render_text(), (
            "contracts/features_parquet.schema.json is stale against CATALOG. Run "
            "`python3 features/gen_parquet_contract.py --write`, then re-pin the copies in "
            "crucible-research and crucible-dashboard (tests/contracts/features_parquet.schema.json)."
        )

    def test_check_mode_agrees(self):
        assert gen_parquet_contract.main(["--check"]) == 0

    def test_schema_is_valid_draft_2020_12(self):
        jsonschema.Draft202012Validator.check_schema(_schema())


class TestColumnsMirrorCatalog:
    def test_every_group_is_a_property_and_a_def(self):
        schema = _schema()
        assert set(schema["properties"]) == set(GROUPS)
        assert set(schema["$defs"]) == set(GROUPS)
        assert schema["additionalProperties"] is False

    @pytest.mark.parametrize("group", sorted(GROUPS))
    def test_group_declares_exactly_its_catalog_columns(self, group):
        row = _schema()["$defs"][group]
        declared = [c for c in row["properties"] if c not in ("ticker", "date")]
        assert declared == GROUPS[group]
        assert row["additionalProperties"] is False
        for name in declared:
            assert row["properties"][name]["type"] == ["number", "null"]

    @pytest.mark.parametrize("group", sorted(GROUPS))
    def test_identity_columns_are_required_as_written(self, group):
        row = _schema()["$defs"][group]
        per_ticker = next(e.per_ticker for e in CATALOG if e.group == group)
        if per_ticker:
            assert row["required"] == ["ticker", "date"]
        else:
            assert row["required"] == ["date"]
            assert "ticker" not in row["properties"]

    def test_an_unmapped_dtype_refuses_to_render(self, monkeypatch):
        entry = CATALOG[0]
        odd = type(entry)(**{**entry.__dict__, "dtype": "complex128"})
        monkeypatch.setattr(gen_parquet_contract, "CATALOG", [odd])
        with pytest.raises(ValueError, match="no JSON type"):
            gen_parquet_contract.render_group(entry.group)


class TestRealSerialiserValidates:
    def test_full_catalog_snapshot_validates(self):
        doc = _snapshot_document(_full_catalog_frame())
        assert set(doc) == set(GROUPS)
        assert validate_features_parquet(doc) == []

    def test_a_partial_snapshot_validates(self):
        """The writer skips a column (and a group) it did not compute this run."""
        frame = _full_catalog_frame().drop(columns=GROUPS["alternative"] + GROUPS["technical"][:3])
        doc = _snapshot_document(frame)
        assert "alternative" not in doc
        assert validate_features_parquet(doc) == []


class TestRejects:
    def _doc(self) -> dict:
        return _snapshot_document(_full_catalog_frame(2))

    def test_an_undeclared_column_is_rejected(self):
        doc = self._doc()
        doc["technical"][0]["not_a_catalog_column_pct"] = 0.1
        assert validate_features_parquet(doc) != []

    def test_a_missing_ticker_is_rejected(self):
        doc = self._doc()
        del doc["fundamental"][0]["ticker"]
        assert validate_features_parquet(doc) != []

    def test_a_missing_date_is_rejected(self):
        doc = self._doc()
        del doc["macro"][0]["date"]
        assert validate_features_parquet(doc) != []

    def test_a_string_feature_value_is_rejected(self):
        doc = self._doc()
        doc["technical"][0][GROUPS["technical"][0]] = "high"
        assert validate_features_parquet(doc) != []

    def test_an_unknown_group_is_rejected(self):
        doc = self._doc()
        doc["sentiment"] = [{"ticker": "T0", "date": DATE}]
        assert validate_features_parquet(doc) != []

    def test_an_empty_snapshot_is_rejected(self):
        assert validate_features_parquet({}) != []


class TestParityResolutionUnchanged:
    @pytest.mark.parametrize("group", ["technical", "fundamental", "interaction", "macro", "alternative"])
    def test_group_keys_resolve_here_with_the_key_date_settling_bar(self, group):
        key = f"features/{DATE}/{group}.parquet"
        contract = parity.resolve_contract(key)
        assert contract is not None and contract.path.name == "features_parquet.schema.json"
        assert contract.settling_basis == "key_date"
        assert contract.provenance_fields == frozenset()
        assert parity.settling_basis(key, contract, pd.Timestamp(DATE).date()) == "key_date"

    def test_registry_json_resolves_to_its_own_contract_without_a_settling_bar(self):
        contract = parity.resolve_contract("features/registry.json")
        assert contract is not None and contract.path.name == "feature_registry.schema.json"
        assert contract.settling_basis == ""
