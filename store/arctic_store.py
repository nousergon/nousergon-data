"""
store/arctic_store.py — ArcticDB connection manager.

Thin wrapper over ArcticDB that provides library access for all modules.
Uses S3 backend — no additional infrastructure beyond the existing bucket.

Usage:
    from store.arctic_store import get_universe_lib, get_macro_lib

    universe = get_universe_lib()
    df = universe.read("AAPL").data

Libraries:
    universe — per-ticker time series (OHLCV + 53 computed features)
    macro    — market-wide time series (VIX, yields, commodities, macro features)
"""

from __future__ import annotations

import logging
import os

import arcticdb as adb
import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
ARCTIC_PREFIX = "arcticdb"

_arctic_instance: adb.Arctic | None = None


def _get_arctic(bucket: str | None = None) -> adb.Arctic:
    """Get or create the ArcticDB connection singleton."""
    global _arctic_instance
    if _arctic_instance is not None:
        return _arctic_instance

    bucket = bucket or os.environ.get("ARCTIC_BUCKET", DEFAULT_BUCKET)
    region = os.environ.get("AWS_REGION", "us-east-1")
    uri = f"s3s://s3.{region}.amazonaws.com:{bucket}?path_prefix={ARCTIC_PREFIX}&aws_auth=true"

    log.info("Connecting to ArcticDB: s3://%s/%s (region=%s)", bucket, ARCTIC_PREFIX, region)
    _arctic_instance = adb.Arctic(uri)
    return _arctic_instance


def get_universe_lib(bucket: str | None = None) -> adb.library.Library:
    """Get the universe library (per-ticker OHLCV + features)."""
    arctic = _get_arctic(bucket)
    return arctic.get_library("universe", create_if_missing=True)


def get_macro_lib(bucket: str | None = None) -> adb.library.Library:
    """Get the macro library (market-wide time series)."""
    arctic = _get_arctic(bucket)
    return arctic.get_library("macro", create_if_missing=True)


def reset_connection():
    """Reset the singleton (useful for testing or credential rotation)."""
    global _arctic_instance
    _arctic_instance = None


def to_arctic_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a DataFrame to dtypes ArcticDB accepts on write/update.

    ArcticDB's ``_handle_categorical_columns`` raises
    ``ArcticDbNotYetImplemented`` on any ``CategoricalDtype`` column during
    ``update_batch`` / ``write_batch`` (verified 2026-05-12 EOD incident on
    BRK-B). Callers may keep Categorical in-memory for memory savings
    (PR #211: ~108MB reduction across the universe pass in
    ``_apply_daily_delta``) — this helper is the *single* boundary between
    that in-memory representation and the ArcticDB storage contract.

    Call exactly once, immediately before every ``update_batch`` /
    ``write_batch`` / ``write`` invocation. Empty frames and frames with no
    categorical columns return unchanged (no copy); frames with categoricals
    are copied + cast to object dtype (matches PR #196's pre-#211 storage
    representation, which round-trips cleanly through ArcticDB).
    """
    if df.empty:
        return df
    cat_cols = [c for c in df.columns if isinstance(df[c].dtype, pd.CategoricalDtype)]
    if not cat_cols:
        return df
    df = df.copy()
    for col in cat_cols:
        df[col] = df[col].astype(object)
    return df
