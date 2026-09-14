"""Two store backends behind the engine's two-method `GateStore` surface.

`s3://bucket/prefix` for the real read, a local directory for tests and for a
developer who will not otherwise run the thing at all.

**Read-only by construction on the read path.** `put_bytes` exists because the
CLI publishes the reading, and it is a DIFFERENT call from the ones the clauses
take: a gate that could write through the handle it grades with could satisfy
its own clause. `--dry-run` refuses the write outright rather than pointing it
somewhere harmless, because a dry run that writes to a scratch key is still a
run that exercised a code path the real one does not.
"""

from __future__ import annotations

import pathlib
from collections.abc import Iterator

__all__ = ["DryRunWriteRefusedError", "LocalStore", "S3Store", "open_store", "parse_store_uri"]


class DryRunWriteRefusedError(RuntimeError):
    """A write was attempted under ``--dry-run``."""


class LocalStore:
    """Keys as paths under a directory."""

    def __init__(self, root: pathlib.Path | str, *, dry_run: bool = False) -> None:
        self.root = pathlib.Path(root)
        self.dry_run = dry_run

    @property
    def uri(self) -> str:
        return f"file://{self.root}"

    def get_bytes(self, key: str) -> bytes:
        path = self.root / key
        if not path.is_file():
            raise FileNotFoundError(key)
        return path.read_bytes()

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        base = self.root
        if not base.is_dir():
            return
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            key = path.relative_to(base).as_posix()
            if key.startswith(prefix):
                yield key

    def put_bytes(self, key: str, payload: bytes) -> None:
        if self.dry_run:
            raise DryRunWriteRefusedError(key)
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


class S3Store:
    """Keys under ``s3://bucket/prefix``."""

    def __init__(
        self, bucket: str, prefix: str = "", *, client=None, iam_client=None, dry_run: bool = False
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = client
        self._iam_client = iam_client
        self.dry_run = dry_run

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}" if self.prefix else f"s3://{self.bucket}"

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        return self._client

    @property
    def iam_client(self):
        """The IAM client `data_gate.evidence.read_roles_bootstrapped` reads
        the standalone stack's roles through — lazy like `.client` above, and
        present ONLY on this backend: `LocalStore`/`EmptyStore` (tests, a
        developer running nothing) carry no such attribute at all, so a
        roles-bootstrapped read against them is UNMEASURABLE by construction
        rather than by ever reaching for `boto3` (`alpha-engine-config-
        I10777`).
        """
        if self._iam_client is None:
            import boto3

            self._iam_client = boto3.client("iam")
        return self._iam_client

    def _s3_key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._s3_key(key))
        except Exception as exc:  # noqa: BLE001 - classified, then re-raised in kind
            code = ""
            response_meta = getattr(exc, "response", None)
            if isinstance(response_meta, dict):
                code = str((response_meta.get("Error") or {}).get("Code") or "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                # Absence is an ANSWER, and the engine's reader needs it typed
                # as one. Everything else keeps its own type and reaches the
                # reader as an access problem -> UNMEASURABLE.
                raise FileNotFoundError(key) from exc
            raise
        return response["Body"].read()

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        full = self._s3_key(prefix)
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full):
            for item in page.get("Contents", []):
                key = item["Key"]
                yield key[len(self.prefix) + 1 :] if self.prefix else key

    def put_bytes(self, key: str, payload: bytes) -> None:
        if self.dry_run:
            raise DryRunWriteRefusedError(key)
        self.client.put_object(
            Bucket=self.bucket,
            Key=self._s3_key(key),
            Body=payload,
            ContentType="application/json",
        )


def parse_store_uri(uri: str) -> tuple[str, str]:
    """``(scheme, location)`` for ``s3://…`` or a local path."""
    if uri.startswith("s3://"):
        return "s3", uri[len("s3://") :]
    if uri.startswith("file://"):
        return "file", uri[len("file://") :]
    return "file", uri


def open_store(uri: str, *, dry_run: bool = False):
    """The store named by ``uri``."""
    scheme, location = parse_store_uri(uri)
    if scheme == "s3":
        bucket, _, prefix = location.partition("/")
        if not bucket:
            raise ValueError(f"store uri {uri!r} names no bucket")
        return S3Store(bucket, prefix, dry_run=dry_run)
    return LocalStore(location, dry_run=dry_run)
