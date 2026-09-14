"""Shared fixtures for the data-gate tests.

**No AWS.** Every store here is in-memory or a tmp directory. A gate test that
reaches a real bucket grades whatever that bucket happens to contain today, and
the first thing this board must be able to prove is that it does not silently
read state nobody controls.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

TRADING_DAY = dt.date(2026, 9, 14)


class EmptyStore:
    """A store with nothing in it. Absence is an ANSWER, so reads succeed."""

    uri = "memory://empty"

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.written: dict[str, bytes] = {}

    def get_bytes(self, key: str) -> bytes:
        try:
            return self.objects[key]
        except KeyError:
            raise FileNotFoundError(key) from None

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        for key in sorted(self.objects):
            if key.startswith(prefix):
                yield key

    def put_bytes(self, key: str, payload: bytes) -> None:
        self.objects[key] = payload
        self.written[key] = payload


class DeniedStore(EmptyStore):
    """A store that refuses every read — the AccessDenied case.

    Not the same as empty, and the whole board hangs on the difference: an empty
    store means the producers have written nothing (a finding about them), a
    denied store means we could not look (a finding about us).
    """

    uri = "memory://denied"

    def get_bytes(self, key: str) -> bytes:
        raise PermissionError(f"AccessDenied: {key}")

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        raise PermissionError(f"AccessDenied: {prefix}")
