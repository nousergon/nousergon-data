"""Hermetic tests for the schema stamp and producer pre-write guard."""

from dataclasses import dataclass
from datetime import datetime
import re

import pandas as pd
import pytest

from store.schema_version import (
    BASELINE_SCHEMA_VERSION,
    SCHEMA_VERSION_SYMBOL,
    SchemaVersionMismatch,
    assert_schema_version,
    read_schema_version,
    write_schema_version,
)


@dataclass
class _Item:
    metadata: dict[str, object] | None
    data: pd.DataFrame


@dataclass
class _Write:
    symbol: str
    frame: pd.DataFrame
    metadata: dict[str, object]
    prune_previous_versions: bool


class _Library:
    def __init__(self, item: _Item | None = None) -> None:
        self.items: dict[str, _Item] = (
            {} if item is None else {SCHEMA_VERSION_SYMBOL: item}
        )
        self.writes: list[_Write] = []

    def has_symbol(self, name: str) -> bool:
        return name in self.items

    def read(self, name: str) -> _Item:
        return self.items[name]

    def write(
        self,
        symbol: str,
        frame: pd.DataFrame,
        *,
        metadata: dict[str, object],
        prune_previous_versions: bool,
    ) -> None:
        self.writes.append(_Write(symbol, frame, metadata, prune_previous_versions))
        self.items[symbol] = _Item(metadata, frame)


def _stamped(version: int) -> _Library:
    return _Library(
        _Item({"schema_version": version}, pd.DataFrame({"schema_version": [version]}))
    )


def test_constants_and_exception() -> None:
    assert SCHEMA_VERSION_SYMBOL == "schema_version"
    assert BASELINE_SCHEMA_VERSION == 0
    assert issubclass(SchemaVersionMismatch, RuntimeError)


def test_read_unstamped() -> None:
    assert read_schema_version(_Library()) is None


def test_read_metadata_is_authoritative_and_converted_to_int() -> None:
    library = _Library(
        _Item({"schema_version": "3"}, pd.DataFrame({"schema_version": [1, 2]}))
    )
    version = read_schema_version(library)
    assert version == 3
    assert type(version) is int


@pytest.mark.parametrize("metadata", [None, {}, {"migration_number": 9}])
def test_read_falls_back_to_last_data_row(metadata: dict[str, object] | None) -> None:
    library = _Library(_Item(metadata, pd.DataFrame({"schema_version": [1, 2]})))
    assert read_schema_version(library) == 2


def test_write_stamp_and_read_it_back() -> None:
    library = _Library()
    write_schema_version(library, 2, migration_number=2, columns_after=("a", "b"))

    assert len(library.writes) == 1
    write = library.writes[0]
    assert write.symbol == "schema_version"
    assert write.prune_previous_versions is True
    assert set(write.metadata) == {
        "schema_version", "migration_number", "applied_utc", "columns", "n_columns"
    }
    assert write.metadata["schema_version"] == 2
    assert write.metadata["migration_number"] == 2
    assert write.metadata["columns"] == ["a", "b"]
    assert isinstance(write.metadata["columns"], list)
    assert write.metadata["n_columns"] == 2
    applied = write.metadata["applied_utc"]
    assert isinstance(applied, str)
    timestamp = datetime.fromisoformat(applied)
    assert timestamp.tzinfo is not None
    assert timestamp.utcoffset() is not None
    assert write.frame.shape == (1, 2)
    assert list(write.frame.columns) == ["schema_version", "migration_number"]
    assert write.frame.index.name == "applied_utc"
    assert write.frame.to_dict(orient="list") == {
        "schema_version": [2], "migration_number": [2]
    }
    assert read_schema_version(library) == 2


def test_assert_accepts_unstamped_baseline() -> None:
    assert assert_schema_version(_Library(), 0) == 0


def test_assert_accepts_matching_stamp() -> None:
    assert assert_schema_version(_stamped(2), 2) == 2


@pytest.mark.parametrize(
    ("stamp", "expected", "pending", "message"),
    [
        (None, 1, None, "['0001']"),
        (1, 3, None, "['0002', '0003']"),
        (1, 3, [7], "['0007']"),
        (1, 3, [], "['0002', '0003']"),
    ],
)
def test_assert_names_pending_migrations(
    stamp: int | None, expected: int, pending: list[int] | None, message: str
) -> None:
    library = _Library() if stamp is None else _stamped(stamp)
    with pytest.raises(SchemaVersionMismatch, match=re.escape(message)):
        assert_schema_version(library, expected, pending_migrations=pending)


def test_assert_rejects_stale_producer() -> None:
    with pytest.raises(SchemaVersionMismatch, match="STALE"):
        assert_schema_version(_stamped(3), 2)
