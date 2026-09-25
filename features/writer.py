"""
features/writer.py — Write feature snapshots to S3 as dated Parquet files.

Each snapshot is split by feature group (technical, macro, interaction,
alternative, fundamental) so consumers can read only what they need.

Self-contained copy from alpha-engine-predictor/feature_store/writer.py.
"""

from __future__ import annotations

import io
import logging
from typing import Optional

import pandas as pd

from features.registry import GROUPS, validate_units_suffix

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = "features/"


def write_feature_snapshot(
    date_str: str,
    features_df: pd.DataFrame,
    bucket: str,
    prefix: str = DEFAULT_PREFIX,
    s3_client=None,
) -> dict[str, int]:
    """
    Write a feature DataFrame to S3, split by group.

    Parameters
    ----------
    date_str : YYYY-MM-DD date for this snapshot.
    features_df : DataFrame with a 'ticker' column plus feature columns.
                  One row per ticker. Missing feature columns are skipped.
    bucket : S3 bucket name.
    prefix : S3 key prefix (default "features/").
    s3_client : Optional boto3 S3 client (for testing / reuse).

    Returns
    -------
    dict mapping group name -> number of rows written.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    written = {}

    for group, (group_df, body) in snapshot_group_frames(date_str, features_df).items():
        key = f"{prefix}{date_str}/{group}.parquet"
        s3_client.put_object(Bucket=bucket, Key=key, Body=body)

        written[group] = len(group_df)
        logger.debug("Wrote %s: %d rows, %d features", key, len(group_df), group_df.shape[1])

    total_groups = len(written)
    total_rows = sum(written.values())
    logger.info(
        "Feature snapshot written for %s: %d groups, %d total rows",
        date_str, total_groups, total_rows,
    )
    return written


def snapshot_group_frames(
    date_str: str, features_df: pd.DataFrame,
) -> dict[str, tuple[pd.DataFrame, bytes]]:
    """Each feature group's frame and the exact Parquet bytes the snapshot publishes for it.

    The ONE place a group's published bytes are produced. `write_feature_snapshot`
    PUTs exactly these bytes, and the D31 recompute lineage
    (`shadow.recompute_lineage`, alpha-engine-config-I11203) compares them with
    a published file — so the two can never serialise a group differently.

    Resolves which registered columns are present per group and enforces the
    write-time units-suffix contract across ALL groups before returning
    anything, so a mis-suffixed column fails the whole snapshot rather than
    some groups.
    """
    # Resolve which registered columns are actually present, per group,
    # BEFORE any S3 write.
    group_available: dict[str, list[str]] = {}
    for group, feature_names in GROUPS.items():
        available = [f for f in feature_names if f in features_df.columns]
        if available:
            group_available[group] = available
        else:
            logger.debug("Skipping group %s — no columns present in DataFrame", group)

    # Write-time units-suffix contract (alpha-engine-config#10781): every
    # column about to be written must carry a units suffix (`_raw`,
    # `_ratio`, `_pct`, `_zscore`, `_log_return`) or be grandfathered
    # (`features.registry.GRANDFATHERED_BARE_FIELDS`) — enforced HERE, not
    # only later in CI (`tests/test_schema_contract.py`), closing the
    # avg_volume_20d root cause: it was emitted as a normalized ratio and
    # consumed as raw shares, 901/903 tickers silently failing the scanner
    # liquidity gate for months. Validated across ALL groups before any
    # `put_object`, so a mis-suffixed column fails the whole snapshot rather
    # than writing some groups and rejecting the rest partway through.
    for available in group_available.values():
        for name in available:
            validate_units_suffix(name)

    out: dict[str, tuple[pd.DataFrame, bytes]] = {}
    for group, available in group_available.items():
        # Build the group DataFrame
        if group == "macro":
            # Macro features are identical across tickers — write one row per date
            row = features_df[available].iloc[0:1].copy()
            row.insert(0, "date", date_str)
            group_df = row
        else:
            # Per-ticker features
            id_cols = []
            if "ticker" in features_df.columns:
                id_cols.append("ticker")
            group_df = features_df[id_cols + available].copy()
            group_df.insert(len(id_cols), "date", date_str)

        buf = io.BytesIO()
        group_df.to_parquet(buf, index=False, engine="pyarrow")
        out[group] = (group_df, buf.getvalue())
    return out
