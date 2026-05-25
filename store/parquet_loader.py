"""
store/parquet_loader.py — Shared single-parquet S3 loader helper.

Extracted from features/compute.py so that non-feature callers can reuse
the same normalized DataFrame shape without importing private helpers out
of features.*. The bulk slim-cache loader (load_slim_cache) was removed in
the Wave-4 predictor/price_cache_slim deletion — all price reads now go
through the ArcticDB universe/macro libs (alpha_engine_lib.arcticdb).
"""

from __future__ import annotations

import io
import logging

import pandas as pd

log = logging.getLogger(__name__)


def load_parquet_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    """Download a single parquet from S3 and return a normalized DataFrame.

    Normalizes the index to a tz-naive UTC DatetimeIndex (sorted ascending),
    matching the convention used by the predictor and feature store so that
    downstream reindex / join operations never raise on mixed tz.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buf, engine="pyarrow")
    if not isinstance(df.index, pd.DatetimeIndex):
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        else:
            df.index = pd.to_datetime(df.index)
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)
    if isinstance(df.index, pd.DatetimeIndex) and not df.index.is_monotonic_increasing:
        df = df.sort_index()
    return df
