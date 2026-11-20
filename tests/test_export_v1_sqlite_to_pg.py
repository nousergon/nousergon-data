from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import cast

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "export_v1_sqlite_to_pg.py"
SPEC = importlib.util.spec_from_file_location("export_v1_sqlite_to_pg", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)
export = cast(ModuleType, module)


def _database(path: Path, ddl: str, rows: list[tuple[object, ...]]) -> bytes:
    with sqlite3.connect(path) as connection:
        connection.execute(ddl)
        table = ddl.split()[2].split("(")[0]
        placeholders = ",".join("?" for _ in rows[0])
        connection.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
    return path.read_bytes()


class FakePaginator:
    def __init__(self, objects: list[dict[str, object]]) -> None:
        self.objects = objects

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, object]]:  # noqa: N803
        assert Bucket == export.BUCKET
        return [{"Contents": [item for item in self.objects if str(item["Key"]).startswith(Prefix)]}]


class FakeS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.data = objects
        self.puts: list[dict[str, object]] = []
        self.listing = [
            {"Key": key, "ETag": f'"etag-{index}"', "LastModified": datetime(2026, 11, index + 1, tzinfo=UTC)}
            for index, key in enumerate(objects)
        ]

    def get_paginator(self, operation_name: str) -> FakePaginator:
        assert operation_name == "list_objects_v2"
        return FakePaginator(self.listing)

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        assert bucket == export.BUCKET
        Path(filename).write_bytes(self.data[key])

    def put_object(self, **kwargs: object) -> object:
        self.puts.append(kwargs)
        return {}


class FakeCursor:
    def __init__(self, connection: FakePg) -> None:
        self.connection = connection
        self.result: tuple[object, ...] | None = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> object:
        self.connection.queries.append((query, params))
        if query.startswith("SELECT to_regclass"):
            name = str(params[0]) if params else ""
            target = name.removeprefix(f"{export.SCHEMA}.")
            self.result = (name if target in self.connection.counts else None,)
        elif query.startswith("SELECT COUNT(*) FROM"):
            target = query.split('"."')[1].rstrip('"')
            self.result = (self.connection.counts[target],)
        elif query.startswith("CREATE TABLE"):
            target = query.split('"."')[1].split('"')[0]
            self.connection.counts[target] = 0
        elif query.startswith("INSERT INTO"):
            target = query.split('"."')[1].split('"')[0]
            self.connection.counts[target] += 1
        return None

    def fetchone(self) -> tuple[object, ...] | None:
        return self.result


class FakePg:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[object, ...] | None]] = []
        self.counts: dict[str, int] = {}
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def sources(tmp_path: Path) -> FakeS3:
    trades = _database(
        tmp_path / "trades.db",
        "CREATE TABLE fills(id INTEGER PRIMARY KEY, price REAL, note TEXT, raw BLOB, amount NUMERIC)",
        [(1, 10.5, "ok", b"bytes", 12)],
    )
    research = _database(
        tmp_path / "research.db",
        "CREATE TABLE scores(symbol TEXT PRIMARY KEY, score REAL)",
        [("ABC", 0.75)],
    )
    return FakeS3({"trades/2026-11-01/trades.db": trades, "backups/2026-11-02/research.db": research})


def test_default_is_dry_run_and_prints_complete_plan(sources: FakeS3, capsys: pytest.CaptureFixture[str]) -> None:
    assert export.run([], s3=sources) == 0

    output = capsys.readouterr().out
    assert "trades/2026-11-01/trades.db etag=etag-0 sha256=" in output
    assert "v1_trades_fills" in output
    assert '"id" bigint' in output
    assert '"price" double precision' in output
    assert '"note" text' in output
    assert '"raw" bytea' in output
    assert '"amount" numeric' in output
    assert "source_rows=1" in output
    assert "dry-run: no Postgres or S3 writes performed" in output
    assert sources.puts == []


def test_apply_loads_each_table_and_writes_manifest(sources: FakeS3) -> None:
    pg = FakePg()

    assert export.run(["--apply"], s3=sources, pg=pg) == 0

    assert pg.counts == {"v1_trades_fills": 1, "v1_research_scores": 1}
    assert pg.commits == 3  # schema plus one transaction per table
    assert any(query.startswith("REVOKE ALL PRIVILEGES") for query, _ in pg.queries)
    assert any(query.startswith("GRANT SELECT") for query, _ in pg.queries)
    assert pg.closed
    assert len(sources.puts) == 1
    manifest = json.loads(cast(bytes, sources.puts[0]["Body"]))
    assert manifest["sources"][0]["key"] == "trades/2026-11-01/trades.db"
    assert manifest["sources"][0]["etag"] == "etag-0"
    assert len(manifest["sources"][0]["sha256"]) == 64
    assert manifest["tables"] == [
        {"source_rows": 1, "table": "v1_trades_fills", "target_rows": 1},
        {"source_rows": 1, "table": "v1_research_scores", "target_rows": 1},
    ]
    assert manifest["generated_utc"].endswith("+00:00")


def test_existing_matching_table_is_not_loaded_but_privileges_are_set(sources: FakeS3) -> None:
    pg = FakePg()
    pg.counts = {"v1_trades_fills": 1, "v1_research_scores": 1}

    export.run(["--apply"], s3=sources, pg=pg)

    assert not any(query.startswith(("CREATE TABLE", "INSERT INTO")) for query, _ in pg.queries)
    assert sum(query.startswith("GRANT SELECT") for query, _ in pg.queries) == 2


def test_existing_mismatched_table_is_refused_and_rolled_back(sources: FakeS3) -> None:
    pg = FakePg()
    pg.counts["v1_trades_fills"] = 99

    with pytest.raises(RuntimeError, match="source has 1 rows, target has 99"):
        export.run(["--apply"], s3=sources, pg=pg)

    assert pg.rollbacks == 1
    assert sources.puts == []
