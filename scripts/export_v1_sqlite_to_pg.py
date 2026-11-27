#!/usr/bin/env python3
"""One-shot export of retired v1 SQLite history into Postgres.

Dry-run is the default.  Use ``--apply`` only after reviewing the printed plan.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol, cast

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


class S3Client(Protocol):
    def get_paginator(self, operation_name: str) -> S3Paginator: ...

    def download_file(self, bucket: str, key: str, filename: str) -> None: ...

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> object: ...  # noqa: N803


class S3Paginator(Protocol):
    def paginate(self, *, Bucket: str, Prefix: str) -> Iterable[Mapping[str, object]]: ...  # noqa: N803


class Cursor(Protocol):
    def execute(self, query: str, params: Sequence[object] | None = None) -> object: ...

    def fetchone(self) -> Sequence[object] | None: ...

    def __enter__(self) -> Cursor: ...

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...


class PgConnection(Protocol):
    def cursor(self) -> Cursor: ...

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
    source_name: str
    target_name: str
    columns: tuple[Column, ...]
    source_rows: int


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _object_date(item: Mapping[str, object]) -> date | None:
    modified = item.get("LastModified")
    if isinstance(modified, datetime):
        return modified.date()
    key = str(item.get("Key", ""))
    match = re.search(r"20\d{2}[-_/]?\d{2}[-_/]?\d{2}", key)
    if match:
        return datetime.strptime(re.sub(r"[-_/]", "", match.group()), "%Y%m%d").date()
    return None


def find_source(s3: S3Client, prefix: str) -> tuple[str, str]:
    candidates: list[tuple[datetime, str, str]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        contents_value = page.get("Contents", [])
        if not isinstance(contents_value, list):
            continue
        contents = cast(list[object], contents_value)
        for value in contents:
            if not isinstance(value, Mapping):
                continue
            raw = cast(Mapping[str, object], value)
            dated = _object_date(raw)
            if dated is None or dated < CUTOFF:
                continue
            key = str(raw.get("Key", ""))
            if not key.endswith(".db"):
                continue
            modified = raw.get("LastModified")
            stamp = modified if isinstance(modified, datetime) else datetime.combine(dated, datetime.min.time(), UTC)
            candidates.append((stamp, key, str(raw.get("ETag", "")).strip('"')))
    if not candidates:
        raise RuntimeError(f"no .db object dated on or after {CUTOFF.isoformat()} under s3://{BUCKET}/{prefix}")
    _, key, etag = max(candidates)
    return key, etag


def download_backup(s3: S3Client, key: str, directory: Path) -> tuple[Path, str]:
    downloaded = directory / (Path(key).name + ".download")
    backup = directory / Path(key).name
    s3.download_file(BUCKET, key, str(downloaded))
    digest = hashlib.sha256(downloaded.read_bytes()).hexdigest()
    with sqlite3.connect(f"file:{downloaded}?mode=ro", uri=True) as source, sqlite3.connect(backup) as target:
        source.backup(target)
    return backup, digest


def inspect_tables(database: Path, source_name: str) -> list[Table]:
    result: list[Table] = []
    with sqlite3.connect(database) as connection:
        names = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for (raw_name,) in names:
            name = str(raw_name)
            columns: list[Column] = []
            for row in connection.execute(f"PRAGMA table_info({_quote(name)})"):
                declared = str(row[2]).upper().strip()
                base_type = re.split(r"[ (]", declared, maxsplit=1)[0]
                if base_type not in TYPE_MAP:
                    raise RuntimeError(f"unsupported SQLite type {declared!r} for {name}.{row[1]}")
                columns.append(Column(str(row[1]), TYPE_MAP[base_type], int(row[5])))
            if not columns:
                raise RuntimeError(f"source table {name!r} has no columns")
            count = int(connection.execute(f"SELECT COUNT(*) FROM {_quote(name)}").fetchone()[0])
            result.append(Table(name, f"v1_{source_name}_{name}", tuple(columns), count))
    return result


def create_sql(table: Table) -> str:
    definitions = [f"{_quote(column.name)} {column.pg_type}" for column in table.columns]
    primary = sorted((column for column in table.columns if column.pk_position), key=lambda column: column.pk_position)
    if primary:
        definitions.append("PRIMARY KEY (" + ", ".join(_quote(column.name) for column in primary) + ")")
    return f"CREATE TABLE {_quote(SCHEMA)}.{_quote(table.target_name)} (" + ", ".join(definitions) + ")"


def _target_count(cursor: Cursor, table: Table) -> int | None:
    cursor.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.{table.target_name}",))
    found = cursor.fetchone()
    if found is None or found[0] is None:
        return None
    cursor.execute(f"SELECT COUNT(*) FROM {_quote(SCHEMA)}.{_quote(table.target_name)}")
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"could not count {table.target_name}")
    value = row[0]
    if not isinstance(value, (int, str)):
        raise RuntimeError(f"invalid row count for {table.target_name}: {value!r}")
    return int(value)


def load_table(pg: PgConnection, sqlite_path: Path, table: Table, read_role: str) -> int:
    try:
        with pg.cursor() as cursor:
            target_count = _target_count(cursor, table)
            if target_count is not None and target_count != table.source_rows:
                raise RuntimeError(
                    f"refusing {table.target_name}: source has {table.source_rows} rows, target has {target_count}"
                )
            if target_count is None:
                cursor.execute(create_sql(table))
                placeholders = ", ".join(["%s"] * len(table.columns))
                insert = (
                    f"INSERT INTO {_quote(SCHEMA)}.{_quote(table.target_name)} ("
                    + ", ".join(_quote(column.name) for column in table.columns)
                    + f") VALUES ({placeholders})"
                )
                with sqlite3.connect(sqlite_path) as source:
                    for row in source.execute(f"SELECT * FROM {_quote(table.source_name)}"):
                        cursor.execute(insert, tuple(row))
                target_count = table.source_rows
            qualified = f"{_quote(SCHEMA)}.{_quote(table.target_name)}"
            cursor.execute(f"REVOKE ALL PRIVILEGES ON TABLE {qualified} FROM PUBLIC")
            cursor.execute(f"REVOKE ALL PRIVILEGES ON TABLE {qualified} FROM {_quote(read_role)}")
            cursor.execute(f"GRANT SELECT ON TABLE {qualified} TO {_quote(read_role)}")
        pg.commit()
        return target_count
    except Exception:
        pg.rollback()
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write tables and the manifest (default: dry-run)")
    parser.add_argument("--dsn", help="Postgres DSN; required with --apply")
    parser.add_argument("--read-role", default="v1_archive_read", help="role receiving SELECT")
    parser.add_argument("--region", default="us-east-1")
    return parser


def run(argv: Sequence[str] | None = None, *, s3: S3Client | None = None, pg: PgConnection | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.apply and not args.dsn and pg is None:
        _parser().error("--dsn is required with --apply")
    if s3 is None:
        import boto3  # pyright: ignore[reportMissingTypeStubs]

        s3 = cast(S3Client, boto3.client("s3", region_name=args.region))  # pyright: ignore[reportUnknownMemberType]
    manifest_sources: list[dict[str, str]] = []
    manifest_tables: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="v1-archive-") as temporary:
        directory = Path(temporary)
        databases: list[tuple[Path, list[Table]]] = []
        for name, prefix in (("trades", "trades/"), ("research", "backups/")):
            key, etag = find_source(s3, prefix)
            path, sha256 = download_backup(s3, key, directory)
            source = Source(name, key, etag, sha256)
            tables = inspect_tables(path, name)
            databases.append((path, tables))
            manifest_sources.append(asdict(source))
            print(f"source s3://{BUCKET}/{key} etag={etag} sha256={sha256}")
            for table in tables:
                print(f"table {SCHEMA}.{table.target_name}: {create_sql(table)}; source_rows={table.source_rows}")
        if args.apply:
            if pg is None:
                import psycopg2

                pg = cast(PgConnection, psycopg2.connect(args.dsn))
            try:
                with pg.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(SCHEMA)}")
                pg.commit()
                for path, tables in databases:
                    for table in tables:
                        target_rows = load_table(pg, path, table, str(args.read_role))
                        manifest_tables.append(
                            {"table": table.target_name, "source_rows": table.source_rows, "target_rows": target_rows}
                        )
            finally:
                pg.close()
            manifest = {
                "generated_utc": datetime.now(UTC).isoformat(),
                "sources": manifest_sources,
                "tables": manifest_tables,
            }
            s3.put_object(
                Bucket=BUCKET,
                Key=MANIFEST_KEY,
                Body=(json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode(),
                ContentType="application/json",
            )
            print(f"wrote s3://{BUCKET}/{MANIFEST_KEY}")
        else:
            print("dry-run: no Postgres or S3 writes performed")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
