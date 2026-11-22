"""
store/parquet_loader.py — Shared single-parquet S3 loader helper.

Extracted from features/compute.py so that non-feature callers can reuse
the same normalized DataFrame shape without importing private helpers out
of features.*. The bulk slim-cache loader (load_slim_cache) was removed in
the Wave-4 predictor/price_cache_slim deletion — all price reads now go
through the ArcticDB universe/macro libs (nousergon_lib.arcticdb).
"""

from __future__ import annotations

import io
import logging
from typing import Protocol, TypedDict

import pandas as pd  # pyright: ignore[reportMissingTypeStubs] - pandas has no complete stubs

log = logging.getLogger(__name__)


class _ReadableBody(Protocol):
    def read(self) -> bytes: ...


class _GetObjectOutput(TypedDict):
    Body: _ReadableBody


class _S3Client(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> _GetObjectOutput: ...


def load_parquet_from_s3(s3: _S3Client, bucket: str, key: str) -> pd.DataFrame:
    """Download a single parquet from S3 and return a normalized DataFrame.

    Normalizes the index to a tz-naive UTC DatetimeIndex (sorted ascending),
    matching the convention used by the predictor and feature store so that
    downstream reindex / join operations never raise on mixed tz.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buf, engine="pyarrow")  # pyright: ignore[reportUnknownMemberType] - pandas stubs expose partially unknown optional parameters
    if not isinstance(df.index, pd.DatetimeIndex):
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType] - pandas stubs leave Series element types unknown
            df = df.set_index("Date")  # pyright: ignore[reportUnknownMemberType] - pandas stubs leave the keys parameter unknown
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType] - pandas stubs leave Series element types unknown
            df = df.set_index("date")  # pyright: ignore[reportUnknownMemberType] - pandas stubs leave the keys parameter unknown
        else:
            df.index = pd.to_datetime(df.index)  # pyright: ignore[reportUnknownMemberType] - pandas stubs expose partially unknown overloads
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)  # pyright: ignore[reportUnknownMemberType] - pandas stubs leave timezone parameters unknown
    if isinstance(df.index, pd.DatetimeIndex) and not df.index.is_monotonic_increasing:
        df = df.sort_index()
    return df
