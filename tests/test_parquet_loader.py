"""Behaviour tests for store/parquet_loader.py::load_parquet_from_s3 (no S3: fake client)."""

from __future__ import annotations

import io
from typing import Any

import pandas as pd  # pyright: ignore[reportMissingTypeStubs] - pandas has no complete stubs
import pytest

from store.parquet_loader import load_parquet_from_s3


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.calls: list[tuple[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> Any:
        self.calls.append((Bucket, Key))
        return {"Body": _Body(self._data)}


def _parquet_bytes(df: Any) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow")
    return buf.getvalue()


def test_requests_given_bucket_and_key() -> None:
    s3 = _FakeS3(_parquet_bytes(pd.DataFrame({"x": [1]}, index=pd.to_datetime(["2026-01-02"]))))
    load_parquet_from_s3(s3, "example-bucket", "some/key.parquet")
    assert s3.calls == [("example-bucket", "some/key.parquet")]


def test_datetime_index_kept_and_sorted() -> None:
    idx = pd.to_datetime(["2026-01-03", "2026-01-01", "2026-01-02"])
    s3 = _FakeS3(_parquet_bytes(pd.DataFrame({"x": [3, 1, 2]}, index=idx)))
    out = load_parquet_from_s3(s3, "b", "k")
    assert [d.strftime("%Y-%m-%d") for d in out.index] == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert list(out["x"]) == [1, 2, 3]


def test_capital_date_column_becomes_index() -> None:
    df = pd.DataFrame({"Date": ["2026-01-02", "2026-01-01"], "x": [2, 1]})
    out = load_parquet_from_s3(_FakeS3(_parquet_bytes(df)), "b", "k")
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index.name == "Date"
    assert "Date" not in out.columns
    assert list(out["x"]) == [1, 2]


def test_lowercase_date_column_becomes_index() -> None:
    df = pd.DataFrame({"date": ["2026-01-02", "2026-01-01"], "x": [2, 1]})
    out = load_parquet_from_s3(_FakeS3(_parquet_bytes(df)), "b", "k")
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index.name == "date"
    assert "date" not in out.columns
    assert list(out["x"]) == [1, 2]


def test_string_index_without_date_column_is_parsed() -> None:
    df = pd.DataFrame({"x": [1, 2]}, index=["2026-01-02", "2026-01-01"])
    out = load_parquet_from_s3(_FakeS3(_parquet_bytes(df)), "b", "k")
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index[0] == pd.Timestamp("2026-01-01")
    assert list(out["x"]) == [2, 1]


def test_tz_aware_index_converted_to_naive_utc() -> None:
    # Mutation guarded: dropping tz_convert("UTC") would keep the local wall time (05:00 not 10:00).
    idx = pd.DatetimeIndex(["2026-01-01 05:00"], tz="America/New_York")
    out = load_parquet_from_s3(_FakeS3(_parquet_bytes(pd.DataFrame({"x": [1]}, index=idx))), "b", "k")
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index.tz is None
    assert out.index[0] == pd.Timestamp("2026-01-01 10:00")


def test_non_date_non_parseable_index_raises() -> None:
    df = pd.DataFrame({"x": [1]}, index=["not-a-date"])
    with pytest.raises(Exception):  # noqa: B017 - pandas raises its own parse error type
        load_parquet_from_s3(_FakeS3(_parquet_bytes(df)), "b", "k")


def test_corrupt_bytes_raise() -> None:
    with pytest.raises(Exception):  # noqa: B017 - pyarrow raises its own error type
        load_parquet_from_s3(_FakeS3(b"not parquet"), "b", "k")
