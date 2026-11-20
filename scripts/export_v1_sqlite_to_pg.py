#!/usr/bin/env python3
"""One-shot exporter for the retired v1 SQLite history (aec#12515)."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

BUCKET = "alpha-engine-research"
MANIFEST_KEY = "ops/v1_archive_export/manifest.json"
CUTOFF = datetime(2026, 10, 30, tzinfo=UTC)
SOURCES = {"trades": "trades/", "research": "backups/"}
TYPE_MAP = {
    "INTEGER": "bigint",
    "REAL": "double precision",
    "TEXT": "text",
    "BLOB": "bytea",
    "NUMERIC": "numeric",
}


class S3Client(Protocol):
    def get_paginator(self, operation_name: str) -> Any: ...  # pyright: ignore[reportExplicitAny] -- boto3 is untyped
    def download_file(self, bucket: str, key: str, filename: str) -> None: ...
    def put_object(self, **kwargs: object) -> object: ...


class PgCursor(Protocol):
    def execute(self, query: str, params: Sequence[object] | None = None) -> object: ...
    def executemany(self, query: str, params: Iterable[Sequence[object]]) -> object: ...
    def fetchone(self) -> Sequence[object] | None: ...
    def close(self) -> None: ...


class PgConnection(Protocol):
    def cursor(self) -> PgCursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class Source:
    name: str
    key: str
    etag: str
    sha256: str


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str
    pk_position: int


@dataclass(frozen=True)
class Table:
    source: str
    sqlite_name: str
    target_name: str
    columns: tuple[Column, ...]
    rows: tuple[tuple[object, ...], ...]


def quote_identifier(value: str) -> str:
    """Quote a SQLite/Postgres identifier without treating it as SQL."""
    return '"' + value.replace('"', '""') + '"'


def map_type(declared_type: str) -> str:
    """Map the five settled SQLite declared types to Postgres types."""
    normalized = declared_type.strip().upper()
    if normalized not in TYPE_MAP:
        raise ValueError(f"unsupported SQLite declared type {declared_type!r}")
    return TYPE_MAP[normalized]


def create_sql(table: Table) -> str:
    """Return CREATE TABLE SQL containing only columns and the source PK."""
    definitions = [f"{quote_identifier(c.name)} {c.pg_type}" for c in table.columns]
    primary_key = sorted((c.pk_position, c.name) for c in table.columns if c.pk_position)
    if primary_key:
        names = ", ".join(quote_identifier(name) for _, name in primary_key)
        definitions.append(f"PRIMARY KEY ({names})")
    return (
        f'CREATE TABLE "v1_archive".{quote_identifier(table.target_name)} '
        f"({', '.join(definitions)})"
    )


def find_latest_source(s3: S3Client, prefix: str) -> tuple[str, str]:
    """Find the latest eligible final SQLite object and its ETag."""
    paginator = s3.get_paginator("list_objects_v2")
    eligible: list[tuple[datetime, str, str]] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            modified = item["LastModified"]
            key = item["Key"]
            if modified >= CUTOFF and key.endswith(".db"):
                eligible.append((modified, key, str(item["ETag"]).strip('"')))
    if not eligible:
        raise RuntimeError(f"no .db object dated on or after 2026-10-30 under s3://{BUCKET}/{prefix}")
    _, key, etag = max(eligible)
    return key, etag


def download_backup(s3: S3Client, name: str, key: str, directory: Path) -> tuple[Path, str]:
    """Download a source then use SQLite online backup for the readable copy."""
    downloaded = directory / f"{name}.download.db"
    backup = directory / f"{name}.db"
    s3.download_file(BUCKET, key, str(downloaded))
    digest = hashlib.sha256(downloaded.read_bytes()).hexdigest()
    with sqlite3.connect(downloaded) as source, sqlite3.connect(backup) as destination:
        source.backup(destination)
    return backup, digest


def inspect_tables(source_name: str, path: Path) -> list[Table]:
    """Read all user tables, schemas, primary keys and rows from a backup."""
    result: list[Table] = []
    with sqlite3.connect(path) as connection:
        names = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for (raw_name,) in names:
            name = str(raw_name)
            info = connection.execute(f"PRAGMA table_info({quote_identifier(name)})").fetchall()
            columns = tuple(Column(str(row[1]), map_type(str(row[2])), int(row[5])) for row in info)
            rows = tuple(tuple(row) for row in connection.execute(f"SELECT * FROM {quote_identifier(name)}"))
            result.append(Table(source_name, name, f"v1_{source_name}_{name}", columns, rows))
    return result


def target_count(cursor: PgCursor, table_name: str) -> int | None:
    """Return a target count, or None when the target does not exist."""
    cursor.execute("SELECT to_regclass(%s)", (f"v1_archive.{table_name}",))
    found = cursor.fetchone()
    if not found or found[0] is None:
        return None
    cursor.execute(f'SELECT count(*) FROM "v1_archive".{quote_identifier(table_name)}')
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"count query returned no row for {table_name}")
    return int(cast(int, row[0]))


def secure_read_only(cursor: PgCursor, table_name: str, read_role: str) -> None:
    """Leave the archive table with SELECT-only access for the read role."""
    qualified = f'"v1_archive".{quote_identifier(table_name)}'
    role = quote_identifier(read_role)
    cursor.execute(f"REVOKE ALL PRIVILEGES ON TABLE {qualified} FROM {role}")
    cursor.execute(f"GRANT SELECT ON TABLE {qualified} TO {role}")


def apply_table(connection: PgConnection, table: Table, read_role: str) -> int:
    """Load or confirm one table in one transaction and return target count."""
    cursor = connection.cursor()
    try:
        existing = target_count(cursor, table.target_name)
        if existing is not None and existing != len(table.rows):
            raise RuntimeError(
                f"refusing {table.target_name}: source has {len(table.rows)} rows, target has {existing}"
            )
        if existing is None:
            cursor.execute(create_sql(table))
            if table.rows:
                columns = ", ".join(quote_identifier(column.name) for column in table.columns)
                placeholders = ", ".join(["%s"] * len(table.columns))
                cursor.executemany(
                    f'INSERT INTO "v1_archive".{quote_identifier(table.target_name)} ({columns}) VALUES ({placeholders})',
                    table.rows,
                )
        secure_read_only(cursor, table.target_name, read_role)
        connection.commit()
        return len(table.rows)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def print_plan(
    sources: Sequence[Source], tables: Sequence[Table], target_counts: dict[str, int | None]
) -> None:
    """Print source keys, target schemas and source row counts."""
    for source in sources:
        print(f"source {source.name}: s3://{BUCKET}/{source.key} ETag={source.etag} sha256={source.sha256}")
    for table in tables:
        print(
            f"table {table.target_name}: {create_sql(table)}; "
            f"source_rows={len(table.rows)} target_rows={target_counts[table.target_name]}"
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the deliberately dry-run-by-default command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="write tables and manifest")
    mode.add_argument("--dry-run", action="store_true", help="print plan only (default)")
    parser.add_argument("--dsn", required=True, help="Neon Postgres DSN")
    parser.add_argument("--read-role", required=True, help="role that receives SELECT only")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the one-shot archive export."""
    args = parse_args(argv)
    import boto3  # pyright: ignore[reportMissingTypeStubs] -- boto3 ships no inline types
    import psycopg2

    s3 = cast(S3Client, boto3.client("s3"))  # pyright: ignore[reportUnknownMemberType] -- narrowed to protocol
    sources: list[Source] = []
    tables: list[Table] = []
    with tempfile.TemporaryDirectory(prefix="v1-archive-") as temp:
        directory = Path(temp)
        for name, prefix in SOURCES.items():
            key, etag = find_latest_source(s3, prefix)
            backup, digest = download_backup(s3, name, key, directory)
            sources.append(Source(name, key, etag, digest))
            tables.extend(inspect_tables(name, backup))
    connection = cast(PgConnection, psycopg2.connect(args.dsn))
    cursor = connection.cursor()
    try:
        target_counts = {table.target_name: target_count(cursor, table.target_name) for table in tables}
    finally:
        cursor.close()
    print_plan(sources, tables, target_counts)
    if not args.apply:
        connection.close()
        print("dry-run: no Postgres or S3 writes performed")
        return 0

    counts: list[dict[str, object]] = []
    try:
        cursor = connection.cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "v1_archive"')
        connection.commit()
        cursor.close()
        for table in tables:
            target = apply_table(connection, table, args.read_role)
            counts.append({"table": table.target_name, "source_rows": len(table.rows), "target_rows": target})
    finally:
        connection.close()
    manifest = {
        "sources": [asdict(source) for source in sources],
        "tables": counts,
        "generated_utc": datetime.now(UTC).isoformat(),
    }
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(manifest, sort_keys=True).encode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
