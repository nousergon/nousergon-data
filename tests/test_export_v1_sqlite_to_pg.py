from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Never, cast

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "export_v1_sqlite_to_pg.py"
SPEC = importlib.util.spec_from_file_location("export_v1", SCRIPT)
assert SPEC and SPEC.loader
export = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = export
SPEC.loader.exec_module(export)
export = cast(ModuleType, export)


def make_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE fills (id INTEGER PRIMARY KEY, price REAL, note TEXT, raw BLOB, amount NUMERIC)")
        connection.execute("INSERT INTO fills VALUES (?, ?, ?, ?, ?)", (1, 12.5, "ok", b"\x00\xff", 7))


class Paginator:
    def __init__(self, contents: list[dict[str, object]]) -> None:
        self.contents = contents

    def paginate(self, **_kwargs: object) -> list[dict[str, object]]:
        return [{"Contents": self.contents}]


class FakeS3:
    def __init__(self, trades: Path, research: Path) -> None:
        self.files = {"trades/final/trades.db": trades, "backups/final/research.db": research}
        now = datetime(2026, 11, 1, tzinfo=UTC)
        self.contents = [{"Key": key, "LastModified": now} for key in self.files]
        self.puts: list[dict[str, object]] = []

    def get_paginator(self, _name: str) -> Paginator:
        return Paginator(self.contents)

    def download_file(self, _bucket: str, key: str, filename: str) -> None:
        Path(filename).write_bytes(self.files[key].read_bytes())

    def head_object(self, **_kwargs: object) -> dict[str, object]:
        return {"ETag": '"abc"'}

    def put_object(self, **kwargs: object) -> None:
        self.puts.append(kwargs)


def test_dry_run_is_default_and_does_not_connect_or_upload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    trades = tmp_path / "trades.db"
    research = tmp_path / "research.db"
    make_database(trades)
    make_database(research)
    s3 = FakeS3(trades, research)

    def unexpected_connect(_dsn: str) -> Never:
        raise AssertionError("dry-run connected to Postgres")

    assert export.run(["--read-role", "archive_reader"], s3=s3, connect=unexpected_connect) == 0
    output = capsys.readouterr().out
    assert "s3://alpha-engine-research/trades/final/trades.db" in output
    assert '"price" double precision' in output
    assert "rows=1" in output
    assert "dry-run: no Postgres writes and no manifest upload" in output
    assert s3.puts == []


def test_inspection_maps_types_and_preserves_composite_primary_key(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE sample (a TEXT, b INTEGER, value BLOB, PRIMARY KEY (b, a))")
    table = export.inspect_tables(database, "trades")[0]
    assert table.target_table == "v1_trades_sample"
    assert export.create_sql(table) == (
        'CREATE TABLE "v1_archive"."v1_trades_sample" '
        '("a" text, "b" bigint, "value" bytea, PRIMARY KEY ("b", "a"))'
    )


def test_refuses_existing_target_with_different_count() -> None:
    table = export.Table("trades", "fills", "v1_trades_fills", (), 2)

    class Cursor:
        def __init__(self) -> None:
            self.results = iter([("v1_archive.v1_trades_fills",), (1,)])
        def __enter__(self) -> Cursor:
            return self
        def __exit__(self, *_args: object) -> None:
            return None
        def execute(self, _query: str, _params: object = ()) -> Cursor:
            return self
        def fetchone(self) -> tuple[object, ...]:
            return next(self.results)

    class Connection:
        def __init__(self) -> None:
            self.rolled_back = False
        def cursor(self) -> Cursor:
            return Cursor()
        def commit(self) -> None:
            raise AssertionError("committed mismatched table")
        def rollback(self) -> None:
            self.rolled_back = True

    connection = Connection()
    with pytest.raises(RuntimeError, match="target has 1 rows, source has 2"):
        export.apply_table(connection, Path("unused"), table, "reader")
    assert connection.rolled_back


def test_unsupported_declared_type_is_refused(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE bad (created DATETIME)")
    with pytest.raises(ValueError, match="unsupported SQLite type"):
        export.inspect_tables(database, "research")
