from __future__ import annotations

import io
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts import export_v1_sqlite_to_pg as export


def _database_bytes(path: Path) -> bytes:
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE fills (id INTEGER PRIMARY KEY, price REAL, note TEXT, raw BLOB, amount NUMERIC)")
        db.execute("INSERT INTO fills VALUES (1, 2.5, 'ok', ?, 3.75)", (b"bytes",))
    return path.read_bytes()


class FakeS3:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        prefix = str(kwargs["Prefix"])
        return {"Contents": [
            {"Key": prefix + "old.db", "LastModified": datetime(2026, 10, 29, tzinfo=UTC), "ETag": '"old"'},
            {"Key": prefix + "final.db", "LastModified": datetime(2026, 10, 31, tzinfo=UTC), "ETag": '"etag"'},
        ]}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        return {"Body": io.BytesIO(self.payload)}

    def put_object(self, **kwargs: object) -> object:
        return {}


def test_download_snapshot_uses_final_object_and_builds_exact_schema(tmp_path: Path) -> None:
    payload = _database_bytes(tmp_path / "input.db")
    source = export.download_snapshot(FakeS3(payload), "trades", "trades/", tmp_path)
    plan, manifest = export.build_plan([source])

    assert source.key == "trades/final.db"
    assert source.etag == "etag"
    assert len(source.sha256) == 64
    assert manifest["sources"] == [{"name": "trades", "s3_key": "trades/final.db",
                                    "etag": "etag", "sha256": source.sha256}]
    assert plan[0]["source_row_count"] == 1
    assert plan[0]["create_sql"] == (
        'CREATE TABLE "v1_archive"."v1_trades_fills" ("id" bigint, "price" double precision, '
        '"note" text, "raw" bytea, "amount" numeric, PRIMARY KEY ("id"))'
    )


def test_unknown_sqlite_declared_type_is_refused() -> None:
    with pytest.raises(ValueError, match="unsupported SQLite type"):
        export.sqlite_type_to_pg("VARCHAR(20)")


def test_no_post_cutoff_database_is_refused() -> None:
    class OldS3(FakeS3):
        def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
            return {"Contents": [{"Key": "trades/old.db",
                                  "LastModified": datetime(2026, 10, 29, tzinfo=UTC)}]}

    with pytest.raises(RuntimeError, match="dated on or after 2026-10-30"):
        export.download_snapshot(OldS3(b""), "trades", "trades/", Path("/tmp"))
