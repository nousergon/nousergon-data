"""Tests for ``load_universe_from_membership`` (alpha-engine-config-I10529).

The scheduled entry point resolves the producer's universe from the
Scanner's ``universe_membership/latest.json`` pointer rather than a
hand-maintained ticker file. This exercises the read + parse contract in
isolation, mirroring ``test_inst_ownership_reader.py``'s in-memory S3 stub.
"""

from __future__ import annotations

import json

import pytest

from data.derived.inst_ownership import (
    MEMBERSHIP_LATEST_KEY,
    UniverseUnavailable,
    load_universe_from_membership,
)


class _InMemoryS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self._store = objects

    def get_object(self, *, Bucket, Key):
        if Key not in self._store:
            raise Exception(f"NoSuchKey: {Key}")
        return {"Body": _Body(self._store[Key])}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self):
        return self._data


def _membership_payload(ranks: dict) -> bytes:
    return json.dumps({
        "schema_version": 1,
        "generated_at": "2026-09-13T00:00:00Z",
        "ranks": ranks,
    }).encode("utf-8")


def test_resolves_full_ranks_universe_sorted_uppercased():
    ranks = {
        "msft": {"attractiveness_rank": 2, "attractiveness_score": 0.5},
        "AAPL": {"attractiveness_rank": 1, "attractiveness_score": 0.9},
    }
    s3 = _InMemoryS3({MEMBERSHIP_LATEST_KEY: _membership_payload(ranks)})

    tickers = load_universe_from_membership(s3_client=s3, bucket="test-bucket")

    assert tickers == ["AAPL", "MSFT"]


def test_missing_pointer_raises_universe_unavailable():
    s3 = _InMemoryS3({})

    with pytest.raises(UniverseUnavailable, match="missing or unparseable"):
        load_universe_from_membership(s3_client=s3, bucket="test-bucket")


def test_empty_ranks_raises_universe_unavailable():
    s3 = _InMemoryS3({MEMBERSHIP_LATEST_KEY: _membership_payload({})})

    with pytest.raises(UniverseUnavailable, match="no non-empty 'ranks'"):
        load_universe_from_membership(s3_client=s3, bucket="test-bucket")


def test_unparseable_json_raises_universe_unavailable():
    s3 = _InMemoryS3({MEMBERSHIP_LATEST_KEY: b"not json"})

    with pytest.raises(UniverseUnavailable, match="missing or unparseable"):
        load_universe_from_membership(s3_client=s3, bucket="test-bucket")
