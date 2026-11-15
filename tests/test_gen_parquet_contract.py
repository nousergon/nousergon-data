"""Behaviour tests for features/gen_parquet_contract.py (JSON Schema render from CATALOG)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from features import gen_parquet_contract as gpc
from features.registry import FeatureEntry


def _catalog() -> list[FeatureEntry]:
    return [
        FeatureEntry(name="rsi_14_ratio", group="technical", description="RSI.", dtype="float32"),
        FeatureEntry(name="n_obs_count", group="technical", description="Count.", dtype="int64"),
        FeatureEntry(name="is_flag_bool", group="technical", description="Flag.", dtype="bool"),
        FeatureEntry(name="label_str", group="technical", description="Label.", dtype="string"),
        FeatureEntry(
            name="vix_level", group="macro", description="VIX.", dtype="float64", per_ticker=False
        ),
    ]


@pytest.fixture
def small_catalog(monkeypatch: pytest.MonkeyPatch) -> list[FeatureEntry]:
    cat = _catalog()
    monkeypatch.setattr(gpc, "CATALOG", cat)
    monkeypatch.setattr(gpc, "GROUPS", {"technical": [], "macro": []})
    return cat


def test_per_ticker_group_requires_ticker_and_date(small_catalog: list[FeatureEntry]) -> None:
    schema = gpc.render_group("technical")
    assert schema["required"] == ["ticker", "date"]
    assert schema["additionalProperties"] is False
    assert list(schema["properties"]) == [
        "ticker",
        "date",
        "rsi_14_ratio",
        "n_obs_count",
        "is_flag_bool",
        "label_str",
    ]
    assert schema["properties"]["ticker"]["minLength"] == 1
    assert schema["properties"]["date"]["pattern"] == r"^\d{4}-\d{2}-\d{2}$"
    assert schema["description"] == "One row of features/{date}/technical.parquet."


def test_non_per_ticker_group_has_no_ticker(small_catalog: list[FeatureEntry]) -> None:
    # Mutation guarded: always emitting ticker, which the macro writer never writes.
    schema = gpc.render_group("macro")
    assert schema["required"] == ["date"]
    assert "ticker" not in schema["properties"]


def test_dtype_mapping_is_nullable_json_type(small_catalog: list[FeatureEntry]) -> None:
    props = gpc.render_group("technical")["properties"]
    assert props["rsi_14_ratio"]["type"] == ["number", "null"]
    assert props["n_obs_count"]["type"] == ["integer", "null"]
    assert props["is_flag_bool"]["type"] == ["boolean", "null"]
    assert props["label_str"]["type"] == ["string", "null"]
    assert props["rsi_14_ratio"]["description"] == "RSI."


def test_feature_columns_not_required(small_catalog: list[FeatureEntry]) -> None:
    assert "rsi_14_ratio" not in gpc.render_group("technical")["required"]


def test_unknown_dtype_refuses_to_render(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gpc, "CATALOG", [FeatureEntry(name="x_ratio", group="g", description="d", dtype="complex128")]
    )
    with pytest.raises(ValueError, match="complex128"):
        gpc.render_group("g")


def test_mixed_per_ticker_group_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gpc,
        "CATALOG",
        [
            FeatureEntry(name="a_ratio", group="g", description="d"),
            FeatureEntry(name="b_ratio", group="g", description="d", per_ticker=False),
        ],
    )
    with pytest.raises(ValueError, match="mixes per_ticker"):
        gpc.render_group("g")


def test_identity_column_collision_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gpc, "CATALOG", [FeatureEntry(name="date", group="g", description="d", per_ticker=False)]
    )
    with pytest.raises(ValueError, match="collides"):
        gpc.render_group("g")


def test_render_document_shape(small_catalog: list[FeatureEntry]) -> None:
    doc = gpc.render()
    assert doc["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert doc["$id"] == "nousergon-data/contracts/features_parquet.schema.json"
    assert list(doc["properties"]) == ["technical", "macro"]
    assert doc["properties"]["macro"] == {
        "type": "array",
        "items": {"$ref": "#/$defs/macro"},
    }
    assert set(doc["$defs"]) == {"technical", "macro"}
    assert doc["x-generated-from"] == "features/registry.py::CATALOG"
    assert doc["x-settling-bar"]["basis"] == "key_date"
    assert doc["x-key-pattern"][0] == "features/{date}/technical.parquet"
    assert doc["x-key-pattern"][-1] == "features/{date}/*.parquet"


def test_render_returns_independent_copies(small_catalog: list[FeatureEntry]) -> None:
    doc = gpc.render()
    doc["x-settling-bar"]["basis"] = "mutated"
    doc["x-key-pattern"].append("extra")
    assert gpc.SETTLING_BAR["basis"] == "key_date"
    assert "extra" not in gpc.KEY_PATTERNS


def test_render_text_is_indented_json_with_newline(small_catalog: list[FeatureEntry]) -> None:
    text = gpc.render_text()
    assert text.endswith("}\n")
    assert text.startswith('{\n  "$schema"')
    assert json.loads(text) == gpc.render()


def test_main_default_prints_text(
    small_catalog: list[FeatureEntry], capsys: pytest.CaptureFixture[str]
) -> None:
    assert gpc.main([]) == 0
    assert capsys.readouterr().out == gpc.render_text()


def test_main_write_then_check(
    small_catalog: list[FeatureEntry],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = tmp_path / "contracts" / "x.schema.json"
    target.parent.mkdir()
    monkeypatch.setattr(gpc, "CONTRACT_PATH", target)
    monkeypatch.setattr(gpc, "REPO_ROOT", tmp_path)
    assert gpc.main(["--write"]) == 0
    assert target.read_text(encoding="utf-8") == gpc.render_text()
    assert "wrote contracts/x.schema.json" in capsys.readouterr().out
    assert gpc.main(["--check"]) == 0


def test_main_check_stale_and_missing_fail(
    small_catalog: list[FeatureEntry],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Mutation guarded: --check passing on a stale or absent committed file.
    target = tmp_path / "x.schema.json"
    monkeypatch.setattr(gpc, "CONTRACT_PATH", target)
    monkeypatch.setattr(gpc, "REPO_ROOT", tmp_path)
    assert gpc.main(["--check"]) == 1
    assert "stale against CATALOG" in capsys.readouterr().err
    target.write_text("{}\n", encoding="utf-8")
    assert gpc.main(["--check"]) == 1
