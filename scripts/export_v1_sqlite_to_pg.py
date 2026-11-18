#!/usr/bin/env python3
"""One-time, restart-safe export of the retired v1 SQLite databases."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import tempfile
from collections.abc import Callable, Generator, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol, cast

import boto3  # pyright: ignore[reportMissingTypeStubs] -- boto3-stubs is not a runtime dependency

BUCKET = "alpha-engine-research"
MANIFEST_KEY = "ops/v1_archive_export/manifest.json"
CUTOFF = date(2026, 10, 30)
SCHEMA = "v1_archive"
TYPE_MAP = {
    "INTEGER": "bigint",
    "REAL": "double precision",
    "TEXT": "text",
    "BLOB": "bytea",
    "NUMERIC": "numeric",
}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Cursor(Protocol):
    def execute(self, query: str, params: Sequence[object] = ()) -> Cursor: ...
    def fetchone(self) -> Sequence[object] | None: ...
    def __enter__(self) -> Cursor: ...
    def __exit__(self, *args: object) -> None: ...


class PgConnection(Protocol):
    def cursor(self) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


class PsycopgModule(Protocol):
    def connect(self, dsn: str) -> object: ...


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
    primary_key_position: int


@dataclass(frozen=True)
class Table:
    source_name: str
    source_table: str
    target_table: str
    columns: tuple[Column, ...]
    source_rows: int


def quote_identifier(value: str) -> str:
    """Quote a previously validated SQL identifier."""
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def sqlite_type(declared: str) -> str:
    """Map the five supported SQLite declared types to Postgres."""
    normalized = declared.strip().upper()
    if normalized not in TYPE_MAP:
        raise ValueError(f"unsupported SQLite type {declared!r}")
    return TYPE_MAP[normalized]


def latest_object(s3: object, prefix: str, filename: str) -> dict[str, object]:
    """Return the newest eligible source object under a prefix."""
    client = cast("S3Client", s3)
    paginator = client.get_paginator("list_objects_v2")
    candidates: list[dict[str, object]] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for item in cast(list[dict[str, object]], page.get("Contents", [])):
            modified = cast(datetime, item["LastModified"])
            if str(item["Key"]).endswith(filename) and modified.date() >= CUTOFF:
                candidates.append(item)
    if not candidates:
        raise RuntimeError(f"no {filename} object dated on or after {CUTOFF}")
    return max(candidates, key=lambda item: cast(datetime, item["LastModified"]))


class S3Paginator(Protocol):
    def paginate(self, **kwargs: object) -> Iterator[dict[str, object]]: ...


class S3Client(Protocol):
    def get_paginator(self, name: str) -> S3Paginator: ...
    def download_file(self, bucket: str, key: str, filename: str) -> None: ...
    def head_object(self, **kwargs: object) -> dict[str, object]: ...
    def put_object(self, **kwargs: object) -> object: ...


def snapshot_source(s3: object, name: str, key: str, destination: Path) -> Source:
    """Download a source and make the SQLite snapshot via its online backup API."""
    client = cast(S3Client, s3)
    raw = destination.with_suffix(".download")
    client.download_file(BUCKET, key, str(raw))
    digest = hashlib.sha256(raw.read_bytes()).hexdigest()
    with sqlite3.connect(raw) as source, sqlite3.connect(destination) as snapshot:
        source.backup(snapshot)
    metadata = client.head_object(Bucket=BUCKET, Key=key)
    return Source(name, key, str(metadata["ETag"]).strip('"'), digest)


def inspect_tables(path: Path, source_name: str) -> list[Table]:
    """Inspect user tables, rejecting types or names we cannot reproduce safely."""
    result: list[Table] = []
    with sqlite3.connect(path) as connection:
        names = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for (raw_name,) in names:
            table_name = str(raw_name)
            quote_identifier(table_name)
            info = connection.execute(
                f"PRAGMA table_info({quote_identifier(table_name)})"
            ).fetchall()
            columns = tuple(
                Column(str(row[1]), sqlite_type(str(row[2])), int(row[5])) for row in info
            )
            for column in columns:
                quote_identifier(column.name)
            count = int(
                connection.execute(
                    f"SELECT count(*) FROM {quote_identifier(table_name)}"
                ).fetchone()[0]
            )
            target = f"v1_{source_name}_{table_name}"
            quote_identifier(target)
            result.append(Table(source_name, table_name, target, columns, count))
    return result


def create_sql(table: Table) -> str:
    """Build the target DDL with only the source primary-key constraint."""
    definitions = [f"{quote_identifier(c.name)} {c.pg_type}" for c in table.columns]
    keys = sorted((c for c in table.columns if c.primary_key_position), key=lambda c: c.primary_key_position)
    if keys:
        definitions.append("PRIMARY KEY (" + ", ".join(quote_identifier(c.name) for c in keys) + ")")
    return f"CREATE TABLE {quote_identifier(SCHEMA)}.{quote_identifier(table.target_table)} (" + ", ".join(definitions) + ")"


def rows(path: Path, table: Table) -> Iterator[tuple[object, ...]]:
    """Stream source rows without coercing SQLite values."""
    connection = sqlite3.connect(path)
    try:
        cursor = connection.execute(f"SELECT * FROM {quote_identifier(table.source_table)}")
        yield from cursor
    finally:
        connection.close()


def target_count(cursor: Cursor, table: Table) -> int | None:
    """Return the target count, or None when the table does not exist."""
    cursor.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.{table.target_table}",))
    found = cursor.fetchone()
    if found is None or found[0] is None:
        return None
    cursor.execute(f"SELECT count(*) FROM {quote_identifier(SCHEMA)}.{quote_identifier(table.target_table)}")
    count = cursor.fetchone()
    if count is None:
        raise RuntimeError("count query returned no row")
    return int(cast(int | str, count[0]))


def apply_table(connection: PgConnection, path: Path, table: Table, read_role: str) -> int:
    """Load or verify one table in its own transaction and enforce read-only grants."""
    try:
        with connection.cursor() as cursor:
            existing = target_count(cursor, table)
            if existing is not None and existing != table.source_rows:
                raise RuntimeError(
                    f"{table.target_table}: target has {existing} rows, source has {table.source_rows}"
                )
            if existing is None:
                cursor.execute(create_sql(table))
                placeholders = ", ".join(["%s"] * len(table.columns))
                insert = (
                    f"INSERT INTO {quote_identifier(SCHEMA)}.{quote_identifier(table.target_table)} "
                    f"VALUES ({placeholders})"
                )
                for row in rows(path, table):
                    cursor.execute(insert, row)
            qualified = f"{quote_identifier(SCHEMA)}.{quote_identifier(table.target_table)}"
            role = quote_identifier(read_role)
            cursor.execute(f"REVOKE ALL PRIVILEGES ON TABLE {qualified} FROM {role}")
            cursor.execute(f"GRANT SELECT ON TABLE {qualified} TO {role}")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return table.source_rows


def print_plan(sources: Sequence[Source], tables: Sequence[Table]) -> None:
    """Print a deterministic human-readable dry-run plan."""
    for source in sources:
        print(f"source {source.name}: s3://{BUCKET}/{source.key} etag={source.etag} sha256={source.sha256}")
    for table in tables:
        print(f"table {SCHEMA}.{table.target_table}: {create_sql(table)}; rows={table.source_rows}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments; dry-run is deliberately the default."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pg-dsn", help="required with --apply")
    parser.add_argument("--read-role", required=True)
    return parser.parse_args(argv)


@contextmanager
def postgres(dsn: str) -> Generator[PgConnection]:
    """Open psycopg only on the apply path so dry-run needs no PG driver."""
    import psycopg  # pyright: ignore[reportMissingImports] -- deployment dependency

    driver = cast(PsycopgModule, psycopg)
    connection = cast(PgConnection, driver.connect(dsn))
    try:
        yield connection
    finally:
        connection.close()


def run(
    argv: Sequence[str] | None = None,
    *,
    s3: object | None = None,
    connect: Callable[[str], AbstractContextManager[PgConnection]] = postgres,
) -> int:
    """Execute the export plan."""
    args = parse_args(argv)
    if args.apply and not args.pg_dsn:
        raise SystemExit("--pg-dsn is required with --apply")
    client = (
        cast(S3Client, s3)
        if s3 is not None
        else cast(S3Client, boto3.client("s3"))  # pyright: ignore[reportUnknownMemberType]
    )
    objects = [
        ("trades", latest_object(client, "trades/", "trades.db")),
        ("research", latest_object(client, "backups/", "research.db")),
    ]
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        sources: list[Source] = []
        paths: dict[str, Path] = {}
        tables: list[Table] = []
        for name, item in objects:
            path = root / f"{name}.db"
            source = snapshot_source(client, name, str(item["Key"]), path)
            sources.append(source)
            paths[name] = path
            tables.extend(inspect_tables(path, name))
        print_plan(sources, tables)
        if not args.apply:
            print("dry-run: no Postgres writes and no manifest upload")
            return 0
        counts: list[dict[str, object]] = []
        with connect(str(args.pg_dsn)) as pg:
            for table in tables:
                target = apply_table(pg, paths[table.source_name], table, str(args.read_role))
                counts.append({"table": table.target_table, "source_rows": table.source_rows, "target_rows": target})
        manifest = {
            "sources": [asdict(source) for source in sources],
            "tables": counts,
            "generated_utc": datetime.now(UTC).isoformat(),
        }
        client.put_object(
            Bucket=BUCKET,
            Key=MANIFEST_KEY,
            Body=json.dumps(manifest, sort_keys=True).encode(),
            ContentType="application/json",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
