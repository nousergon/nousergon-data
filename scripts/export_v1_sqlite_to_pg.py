#!/usr/bin/env python3
"""One-shot export of retired v1 SQLite history into Postgres."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

BUCKET = "alpha-engine-research"
MANIFEST_KEY = "ops/v1_archive_export/manifest.json"
CUTOFF = "2026-10-30"


class S3Client(Protocol):
    def list_objects_v2(self, **kwargs: object) -> dict[str, object]: ...
    def get_object(self, **kwargs: object) -> dict[str, object]: ...
    def put_object(self, **kwargs: object) -> object: ...


class ReadableBody(Protocol):
    def read(self, size: int = -1) -> bytes: ...


class PgCursor(Protocol):
    def __enter__(self) -> PgCursor: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...
    def execute(self, query: str, parameters: object = ...) -> object: ...
    def executemany(self, query: str, parameters: Iterable[Sequence[object]]) -> object: ...
    def fetchone(self) -> tuple[object, ...] | None: ...


class PgConnection(Protocol):
    def __enter__(self) -> PgConnection: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...
    def cursor(self) -> PgCursor: ...


@dataclass(frozen=True)
class Source:
    name: str
    prefix: str
    key: str
    etag: str
    sha256: str
    path: Path


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str
    primary_key_order: int


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def sqlite_type_to_pg(declared: str) -> str:
    """Map the five settled SQLite declared types to Postgres types."""
    normalized = declared.strip().upper()
    mapping = {
        "INTEGER": "bigint",
        "REAL": "double precision",
        "TEXT": "text",
        "BLOB": "bytea",
        "NUMERIC": "numeric",
    }
    try:
        return mapping[normalized]
    except KeyError as exc:
        raise ValueError(f"unsupported SQLite type {declared!r}") from exc


def table_columns(db: sqlite3.Connection, table: str) -> list[Column]:
    rows = db.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
    return [Column(str(row[1]), sqlite_type_to_pg(str(row[2])), int(row[5])) for row in rows]


def create_table_sql(target: str, columns: Sequence[Column]) -> str:
    definitions = [f"{_quote(column.name)} {column.pg_type}" for column in columns]
    primary = sorted((column for column in columns if column.primary_key_order), key=lambda c: c.primary_key_order)
    if primary:
        definitions.append("PRIMARY KEY (" + ", ".join(_quote(column.name) for column in primary) + ")")
    return f"CREATE TABLE {_quote('v1_archive')}.{_quote(target)} (" + ", ".join(definitions) + ")"


def _latest_key(s3: S3Client, prefix: str) -> tuple[str, str]:
    token: str | None = None
    candidates: list[tuple[datetime, str, str]] = []
    while True:
        request: dict[str, object] = {"Bucket": BUCKET, "Prefix": prefix}
        if token:
            request["ContinuationToken"] = token
        response = s3.list_objects_v2(**request)
        for raw in cast(list[dict[str, object]], response.get("Contents", [])):
            key = str(raw["Key"])
            modified = cast(datetime, raw["LastModified"])
            if modified.astimezone(UTC).date().isoformat() >= CUTOFF and key.endswith(".db"):
                candidates.append((modified, key, str(raw.get("ETag", "")).strip('"')))
        if not response.get("IsTruncated"):
            break
        token = str(response["NextContinuationToken"])
    if not candidates:
        raise RuntimeError(f"no .db object under {prefix!r} dated on or after {CUTOFF}")
    _modified, key, etag = max(candidates)
    return key, etag


def download_snapshot(s3: S3Client, name: str, prefix: str, directory: Path) -> Source:
    """Download the selected object and snapshot it through SQLite's backup API."""
    key, etag = _latest_key(s3, prefix)
    response = s3.get_object(Bucket=BUCKET, Key=key)
    body = cast(ReadableBody, response["Body"])
    downloaded = directory / f"{name}.download.db"
    digest = hashlib.sha256()
    with downloaded.open("wb") as output:
        while chunk := body.read(1024 * 1024):
            digest.update(chunk)
            output.write(chunk)
    snapshot = directory / f"{name}.db"
    with sqlite3.connect(downloaded) as source, sqlite3.connect(snapshot) as destination:
        source.backup(destination)
    downloaded.unlink()
    return Source(name, prefix, key, etag, digest.hexdigest(), snapshot)


def _tables(db: sqlite3.Connection) -> list[str]:
    return [str(row[0]) for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )]


def build_plan(sources: Iterable[Source]) -> tuple[list[dict[str, object]], dict[str, object]]:
    plan: list[dict[str, object]] = []
    source_manifest: list[dict[str, str]] = []
    for source in sources:
        source_manifest.append({"name": source.name, "s3_key": source.key, "etag": source.etag, "sha256": source.sha256})
        with sqlite3.connect(source.path) as db:
            for table in _tables(db):
                target = f"v1_{source.name}_{table}"
                columns = table_columns(db, table)
                count = int(db.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0])
                plan.append({"source": source, "source_table": table, "target_table": target,
                             "columns": columns, "create_sql": create_table_sql(target, columns),
                             "source_row_count": count})
    manifest: dict[str, object] = {"sources": source_manifest, "tables": [],
                                   "generated_utc": datetime.now(UTC).isoformat()}
    return plan, manifest


def _first_value(cursor: PgCursor) -> object:
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Postgres returned no row")
    return row[0]


def apply_plan(connection: PgConnection, plan: list[dict[str, object]], read_role: str) -> list[dict[str, object]]:
    """Load each table in a separate transaction and return manifest table entries."""
    results: list[dict[str, object]] = []
    for item in plan:
        target = str(item["target_table"])
        source_table = str(item["source_table"])
        columns = cast(list[Column], item["columns"])
        source_count_raw = item["source_row_count"]
        if not isinstance(source_count_raw, int):
            raise TypeError("source_row_count must be an integer")
        source_count = source_count_raw
        # psycopg2's connection context commits on success and rolls back on
        # failure; entering it once per loop iteration gives each table its own
        # transaction without closing the connection.
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass(%s)", (f"v1_archive.{target}",))
                exists = _first_value(cursor) is not None
                if exists:
                    cursor.execute(f"SELECT COUNT(*) FROM {_quote('v1_archive')}.{_quote(target)}")
                    count_value = _first_value(cursor)
                    if not isinstance(count_value, int):
                        raise TypeError("Postgres COUNT(*) did not return an integer")
                    target_count = count_value
                    if target_count != source_count:
                        raise RuntimeError(f"{target}: existing row count {target_count} != source {source_count}")
                else:
                    cursor.execute(str(item["create_sql"]))
                    source = cast(Source, item["source"])
                    with sqlite3.connect(source.path) as db:
                        rows: Iterator[Sequence[object]] = iter(db.execute(f"SELECT * FROM {_quote(source_table)}"))
                        placeholders = ", ".join(["%s"] * len(columns))
                        insert = (f"INSERT INTO {_quote('v1_archive')}.{_quote(target)} ("
                                  + ", ".join(_quote(column.name) for column in columns)
                                  + f") VALUES ({placeholders})")
                        cursor.executemany(insert, rows)
                    target_count = source_count
                qualified = f"{_quote('v1_archive')}.{_quote(target)}"
                cursor.execute(f"REVOKE ALL PRIVILEGES ON TABLE {qualified} FROM PUBLIC")
                cursor.execute(f"REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE {qualified} FROM {_quote(read_role)}")
                cursor.execute(f"GRANT SELECT ON TABLE {qualified} TO {_quote(read_role)}")
        results.append({"source_table": source_table, "target_table": target,
                        "source_row_count": source_count, "target_row_count": target_count})
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--postgres-dsn", help="required with --apply")
    parser.add_argument("--read-role", default="alpha_engine_read")
    args = parser.parse_args(argv)
    if args.apply and not args.postgres_dsn:
        parser.error("--postgres-dsn is required with --apply")

    import boto3  # pyright: ignore[reportMissingTypeStubs]

    s3 = cast(S3Client, boto3.client("s3"))  # pyright: ignore[reportUnknownMemberType]
    with tempfile.TemporaryDirectory() as raw_directory:
        directory = Path(raw_directory)
        sources = [download_snapshot(s3, "trades", "trades/", directory),
                   download_snapshot(s3, "research", "backups/", directory)]
        plan, manifest = build_plan(sources)
        printable = {"mode": "apply" if args.apply else "dry-run",
                     "sources": manifest["sources"],
                     "tables": [{key: value for key, value in item.items() if key not in {"source", "columns"}}
                                for item in plan]}
        print(json.dumps(printable, indent=2))
        if not args.apply:
            return 0
        import psycopg2

        with psycopg2.connect(args.postgres_dsn) as connection:
            manifest["tables"] = apply_plan(cast(PgConnection, connection), plan, args.read_role)
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                      Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
