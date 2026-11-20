from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "export_v1_sqlite_to_pg.py"
spec = importlib.util.spec_from_file_location("export_v1_sqlite_to_pg", SCRIPT)
assert spec and spec.loader
export = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = export
spec.loader.exec_module(export)


def _sqlite(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE fills (account TEXT, id INTEGER, price REAL, payload BLOB, amount NUMERIC, "
            "PRIMARY KEY (account, id))"
        )
        connection.execute("INSERT INTO fills VALUES (?, ?, ?, ?, ?)", ("A", 7, 1.25, b"x\x00", "2.5"))


class FakeS3:
    def __init__(self, source: Path) -> None:
        self.source = source

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        del bucket, key
        Path(filename).write_bytes(self.source.read_bytes())


def test_download_uses_online_backup_and_inspection_maps_schema(tmp_path: Path) -> None:
    original = tmp_path / "original.db"
    _sqlite(original)
    backup, digest = export.download_backup(FakeS3(original), "trades", "trades/final.db", tmp_path)
    tables = export.inspect_tables("trades", backup)

    assert ".backup(destination)" in SCRIPT.read_text()
    assert len(digest) == 64
    assert len(tables) == 1
    assert [column.pg_type for column in tables[0].columns] == [
        "text", "bigint", "double precision", "bytea", "numeric",
    ]
    assert 'PRIMARY KEY ("account", "id")' in export.create_sql(tables[0])
    assert tables[0].rows == (("A", 7, 1.25, b"x\x00", 2.5),)


def test_latest_source_obeys_cutoff_and_db_suffix() -> None:
    class Paginator:
        def paginate(self, **kwargs: object) -> list[dict[str, object]]:
            assert kwargs == {"Bucket": export.BUCKET, "Prefix": "trades/"}
            return [{"Contents": [
                {"Key": "trades/old.db", "LastModified": datetime(2026, 10, 29, tzinfo=UTC), "ETag": '"old"'},
                {"Key": "trades/new.txt", "LastModified": datetime(2026, 11, 2, tzinfo=UTC), "ETag": '"txt"'},
                {"Key": "trades/final.db", "LastModified": datetime(2026, 11, 1, tzinfo=UTC), "ETag": '"etag"'},
            ]}]

    client = SimpleNamespace(get_paginator=lambda operation: Paginator())
    assert export.find_latest_source(client, "trades/") == ("trades/final.db", "etag")


class Cursor:
    def __init__(self, answers: list[tuple[object, ...] | None]) -> None:
        self.answers = answers
        self.sql: list[str] = []
        self.inserted: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> None:
        del params
        self.sql.append(query)

    def executemany(self, query: str, params: object) -> None:
        self.sql.append(query)
        self.inserted = list(params)

    def fetchone(self) -> tuple[object, ...] | None:
        return self.answers.pop(0)

    def close(self) -> None:
        pass


class Connection:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> Cursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _table() -> object:
    return export.Table("trades", "fills", "v1_trades_fills", (export.Column("id", "bigint", 1),), ((1,),))


def test_apply_loads_new_table_and_sets_select_only() -> None:
    cursor = Cursor([(None,)])
    connection = Connection(cursor)
    assert export.apply_table(connection, _table(), "archive_reader") == 1
    assert connection.commits == 1
    assert cursor.inserted == [(1,)]
    assert any("REVOKE ALL PRIVILEGES" in sql for sql in cursor.sql)
    assert any("GRANT SELECT" in sql for sql in cursor.sql)


def test_matching_table_is_noop_but_privileges_are_reasserted() -> None:
    cursor = Cursor([("v1_archive.v1_trades_fills",), (1,)])
    connection = Connection(cursor)
    export.apply_table(connection, _table(), "archive_reader")
    assert not cursor.inserted
    assert not any(sql.startswith("CREATE TABLE") for sql in cursor.sql)
    assert any("GRANT SELECT" in sql for sql in cursor.sql)


def test_different_existing_count_is_refused_and_rolled_back() -> None:
    cursor = Cursor([("v1_archive.v1_trades_fills",), (2,)])
    connection = Connection(cursor)
    with pytest.raises(RuntimeError, match="refusing"):
        export.apply_table(connection, _table(), "archive_reader")
    assert connection.rollbacks == 1
    assert connection.commits == 0


def test_cli_defaults_to_dry_run() -> None:
    args = export.parse_args(["--dsn", "postgres://example", "--read-role", "reader"])
    assert args.apply is False
