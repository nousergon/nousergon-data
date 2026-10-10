"""`alpha-engine-config-I11792` — one gate reading LISTs and GETs each thing once.

Measured 2026-10-09 in the `alpha-engine-research` access logs: the
`data-gate.yml` reads issued 55k LISTs and 71k GETs a day over ~1,050 distinct
`data_collection/runs/<unit>/<date>/` prefixes and ~1,250 distinct keys, the
same prefix listed up to 120 times in one run, because every clause re-derives
its evidence through the store. `data_gate.store.S3Store` now memoises both per
instance; these tests pin that each is fetched once, that absence is memoised
as an answer, that any other failure is not, and that a write drops what the
memo held.
"""

from __future__ import annotations

import pytest

from data_gate.store import S3Store


class _ClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _Paginator:
    def __init__(self, s3: "_CountingS3") -> None:
        self._s3 = s3

    def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803 - boto3's shape
        self._s3.lists += 1
        if self._s3.fail_lists:
            self._s3.fail_lists -= 1
            yield {"Contents": [{"Key": sorted(self._s3.objects)[0]}]}
            raise _ClientError("SlowDown")
        keys = sorted(k for k in self._s3.objects if k.startswith(Prefix))
        for i in range(0, len(keys), 2):  # two keys a page, so pagination is real
            yield {"Contents": [{"Key": k} for k in keys[i : i + 2]]}


class _CountingS3:
    """Counts the LIST and GET requests a store makes."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.lists = 0
        self.gets = 0
        self.denied: set[str] = set()
        self.fail_lists = 0

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator(self)

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N803 - boto3's shape
        self.gets += 1
        if Key in self.denied:
            raise _ClientError("AccessDenied")
        if Key not in self.objects:
            raise _ClientError("NoSuchKey")
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str):  # noqa: N803
        self.objects[Key] = Body
        return {}


RUNS = "data_collection/runs/D16/2026-10-01/"


@pytest.fixture
def s3() -> _CountingS3:
    fake = _CountingS3()
    for i in range(5):
        fake.objects[f"{RUNS}run-{i}.json"] = f'{{"run": {i}}}'.encode()
    fake.objects["data_collection/runs/D16/2026-10-02/run-0.json"] = b"{}"
    return fake


def test_a_prefix_is_listed_once_however_many_clauses_ask(s3) -> None:
    store = S3Store("bucket", client=s3)
    first = list(store.list_keys(RUNS))
    for _ in range(40):
        assert list(store.list_keys(RUNS)) == first
    assert len(first) == 5
    assert s3.lists == 1


def test_distinct_prefixes_are_listed_separately(s3) -> None:
    store = S3Store("bucket", client=s3)
    assert len(list(store.list_keys(RUNS))) == 5
    assert len(list(store.list_keys("data_collection/runs/D16/"))) == 6
    assert s3.lists == 2


def test_a_key_is_got_once_however_many_clauses_read_it(s3) -> None:
    store = S3Store("bucket", client=s3)
    for _ in range(46):
        assert store.get_bytes(f"{RUNS}run-3.json") == b'{"run": 3}'
    assert s3.gets == 1


def test_absence_is_memoised_as_an_answer(s3) -> None:
    store = S3Store("bucket", client=s3)
    for _ in range(3):
        with pytest.raises(FileNotFoundError):
            store.get_bytes(f"{RUNS}missing.json")
    assert s3.gets == 1


def test_an_access_problem_is_never_memoised(s3) -> None:
    key = f"{RUNS}run-1.json"
    s3.denied.add(key)
    store = S3Store("bucket", client=s3)
    for _ in range(2):
        with pytest.raises(_ClientError):
            store.get_bytes(key)
    assert s3.gets == 2
    s3.denied.clear()
    assert store.get_bytes(key) == b'{"run": 1}'


def test_a_listing_that_fails_part_way_is_not_memoised(s3) -> None:
    s3.fail_lists = 1
    store = S3Store("bucket", client=s3)
    with pytest.raises(_ClientError):
        list(store.list_keys(RUNS))
    assert len(list(store.list_keys(RUNS))) == 5
    assert s3.lists == 2


def test_a_write_drops_the_memo_for_its_key_and_every_prefix_over_it(s3) -> None:
    store = S3Store("bucket", client=s3)
    assert len(list(store.list_keys(RUNS))) == 5
    with pytest.raises(FileNotFoundError):
        store.get_bytes(f"{RUNS}run-9.json")
    store.put_bytes(f"{RUNS}run-9.json", b'{"run": 9}')
    assert store.get_bytes(f"{RUNS}run-9.json") == b'{"run": 9}'
    assert len(list(store.list_keys(RUNS))) == 6


def test_each_store_instance_reads_fresh(s3) -> None:
    """The memo is per process (per instance), never across gate invocations."""
    S3Store("bucket", client=s3).get_bytes(f"{RUNS}run-0.json")
    S3Store("bucket", client=s3).get_bytes(f"{RUNS}run-0.json")
    assert s3.gets == 2
