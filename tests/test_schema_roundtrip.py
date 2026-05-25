"""Feature-store parquet round-trip schema test.

Pins the FULL pipeline ``compute_features → writer → reader``: both
``avg_volume_20d`` (normalized predictor input) and ``avg_volume_20d_raw``
(scanner liquidity gate) survive the parquet round-trip with their
respective unit scales intact.

Catches the "real-S3 schema drift" gap that the in-memory schema
contract test misses. A writer-layer regression (column rename, dtype
coercion stripping a column, group-mapping bug) would not fail
``test_schema_contract.py`` but would silently degrade scanner output
in production.

Test strategy: use a MagicMock S3 client that captures put_object
payloads, then feeds them back through read_feature_snapshot's
get_object path. No real S3 dependency — pure in-process parquet
round-trip exercising the actual writer + reader.
"""

from __future__ import annotations

import io
import os
import sys
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.feature_engineer import FEATURES, compute_features
from features.writer import write_feature_snapshot
from features.reader import read_feature_snapshot


# ── Fixture: realistic multi-ticker feature DataFrame ────────────────────────


def _synthetic_ohlcv(n: int = 400, seed: int = 0) -> pd.DataFrame:
    """OHLCV frame long enough to exercise compute_features' 252d warmup."""
    rng = np.random.default_rng(seed)
    daily_returns = rng.normal(0.0005, 0.012, n)
    close = 100.0 * np.exp(np.cumsum(daily_returns))
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=idx,
    )


def _build_features_df(n_tickers: int = 5) -> pd.DataFrame:
    """Build a multi-ticker features DataFrame matching the production shape
    that ``compute.py`` feeds to ``write_feature_snapshot``: one row per
    ticker (most-recent date's features) with a ``ticker`` column."""
    rows = []
    for i in range(n_tickers):
        df = _synthetic_ohlcv(seed=i)
        out = compute_features(df)
        # Take the final row (mirrors compute.py's per-ticker snapshot).
        latest = out.iloc[-1]
        row = {"ticker": f"T{i:03d}"}
        for col in FEATURES:
            if col in latest.index:
                row[col] = float(latest[col]) if pd.notna(latest[col]) else 0.0
        rows.append(row)
    return pd.DataFrame(rows)


# ── Mock S3 client capturing writes + serving reads ─────────────────────────


class _CapturingS3:
    """Minimal in-process mock — captures put_object payloads and serves
    them back on get_object. Mirrors the boto3 S3 client interface that
    writer.py and reader.py call."""

    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        # reader.py catches NoSuchKey by name — expose a matching class.
        class _Exceptions:
            class NoSuchKey(Exception):
                pass
        self.exceptions = _Exceptions()

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = bytes(Body)
        return {}

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self.exceptions.NoSuchKey(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


# ── Round-trip tests ─────────────────────────────────────────────────────────


def test_avg_volume_raw_and_normalized_survive_roundtrip():
    """Both columns must round-trip through writer → parquet → reader
    with their unit scales preserved.

    The in-memory schema-contract test only proves compute_features
    EMITS both columns. This proves the persistence layer doesn't
    drop or coerce them.
    """
    features_df = _build_features_df(n_tickers=5)
    assert "avg_volume_20d" in features_df.columns
    assert "avg_volume_20d_raw" in features_df.columns

    s3 = _CapturingS3()
    written = write_feature_snapshot(
        date_str="2026-05-25",
        features_df=features_df,
        bucket="test-bucket",
        s3_client=s3,
    )
    # Technical group must have been written (raw + normalized both live there).
    assert "technical" in written
    assert written["technical"] == 5

    # Read the technical group back and verify both columns are present
    # with the correct unit scales.
    tech_df = read_feature_snapshot(
        date_str="2026-05-25",
        group="technical",
        bucket="test-bucket",
        s3_client=s3,
    )
    assert tech_df is not None
    assert "avg_volume_20d" in tech_df.columns, (
        "Normalized avg_volume_20d column lost during writer → reader "
        "round-trip. Predictor inference will break."
    )
    assert "avg_volume_20d_raw" in tech_df.columns, (
        "Raw avg_volume_20d_raw column lost during writer → reader "
        "round-trip. Scanner liquidity gate will silently regress to "
        "zero-output."
    )

    # Unit-scale invariants — same checks the in-memory contract test pins,
    # now verified post-parquet-serialization.
    raw_median = float(tech_df["avg_volume_20d_raw"].median())
    norm_median = float(tech_df["avg_volume_20d"].median())

    assert raw_median >= 1_000_000, (
        f"Post-roundtrip avg_volume_20d_raw median = {raw_median:,.0f}; "
        "expected raw shares (>= 1M). Writer/reader may be coercing units."
    )
    assert 0.5 <= norm_median <= 2.0, (
        f"Post-roundtrip avg_volume_20d median = {norm_median:.4f}; "
        "expected normalized ratio ~1.0."
    )
    assert raw_median / max(norm_median, 1e-12) > 1e5, (
        "Post-roundtrip raw and normalized columns are on similar scales — "
        "round-trip somehow conflated them."
    )


def test_roundtrip_preserves_ticker_identity():
    """Round-trip must keep per-ticker rows aligned — a writer bug that
    re-orders or de-dupes rows would conflate per-ticker values.
    """
    features_df = _build_features_df(n_tickers=5)
    s3 = _CapturingS3()
    write_feature_snapshot(
        "2026-05-25", features_df, bucket="test-bucket", s3_client=s3,
    )
    tech_df = read_feature_snapshot(
        "2026-05-25", "technical", bucket="test-bucket", s3_client=s3,
    )
    assert tech_df is not None
    assert set(tech_df["ticker"]) == set(features_df["ticker"])
    assert len(tech_df) == len(features_df)


def test_roundtrip_loud_failure_when_raw_column_dropped_before_write():
    """If a future refactor strips ``avg_volume_20d_raw`` before write
    (e.g., a "cleanup" pass deleting columns), the round-trip must NOT
    silently round-trip a partial schema.

    Scenario: caller drops the raw column. After the round-trip the
    column should be absent — this is the loud-fail signal the consumer
    contract test in alpha-engine-research reacts to (loud-fail at
    consumer side, not silent degrade).
    """
    features_df = _build_features_df(n_tickers=5)
    features_df = features_df.drop(columns=["avg_volume_20d_raw"])

    s3 = _CapturingS3()
    write_feature_snapshot(
        "2026-05-25", features_df, bucket="test-bucket", s3_client=s3,
    )
    tech_df = read_feature_snapshot(
        "2026-05-25", "technical", bucket="test-bucket", s3_client=s3,
    )
    assert tech_df is not None
    assert "avg_volume_20d_raw" not in tech_df.columns
    # Predictor's normalized column still survives — fault is isolated.
    assert "avg_volume_20d" in tech_df.columns


def test_roundtrip_covers_all_groups_in_catalog():
    """Smoke: every group in CATALOG that has ≥1 column in the features
    DataFrame round-trips successfully. Catches a writer regression that
    silently skips a group.
    """
    from features.registry import CATALOG

    features_df = _build_features_df(n_tickers=5)
    expected_groups: set[str] = set()
    for entry in CATALOG:
        if entry.name in features_df.columns and entry.per_ticker:
            expected_groups.add(entry.group)

    s3 = _CapturingS3()
    written = write_feature_snapshot(
        "2026-05-25", features_df, bucket="test-bucket", s3_client=s3,
    )

    # Every group with at least one per-ticker column should appear in
    # the written manifest. Macro (per_ticker=False) is handled by a
    # separate single-row writer path and is intentionally not asserted
    # here — the fixture is per-ticker.
    for group in expected_groups:
        assert group in written, (
            f"Group {group!r} has columns in features_df but was NOT "
            "written by writer.py. Possible writer regression."
        )
        # Verify each can be read back.
        df = read_feature_snapshot(
            "2026-05-25", group, bucket="test-bucket", s3_client=s3,
        )
        assert df is not None, f"Failed to read back group {group!r}"
        assert len(df) > 0
