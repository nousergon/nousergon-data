"""Tests for builders/migrate_universe_feature_order.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from builders.migrate_universe_feature_order import (
    _canonical_column_order,
    _is_canonical,
    migrate_universe_feature_order,
)
from builders.daily_append import OHLCV_COLS, PROVENANCE_COL
from features.feature_engineer import FEATURES


def _stock_frame(cols: list[str], rows: int = 5) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=rows, freq="B")
    return pd.DataFrame(
        {c: np.linspace(1.0, 2.0, rows) for c in cols},
        index=idx,
    )


# ── _canonical_column_order / _is_canonical ──────────────────────────────────


def test_canonical_order_places_ohlcv_source_then_features():
    """Canonical layout = OHLCV + source + FEATURES (matches daily_append today_row)."""
    existing = list(OHLCV_COLS) + [PROVENANCE_COL] + list(FEATURES)
    canonical = _canonical_column_order(existing)
    assert canonical[: len(OHLCV_COLS)] == list(OHLCV_COLS)
    assert canonical[len(OHLCV_COLS)] == PROVENANCE_COL
    assert canonical[len(OHLCV_COLS) + 1:] == list(FEATURES)


def test_canonical_order_relocates_pillar_fields_from_end():
    """The 2026-05-21 regression layout: pillars appended at end → re-inserted mid-FEATURES."""
    pillar_fields = [
        "revenue_growth_3y",
        "eps_growth_3y",
        "payout_ratio",
        "dividend_yield",
        "capex_growth_5y",
    ]
    # Build the scrambled layout: every FEATURES col EXCEPT the pillars,
    # then the pillars appended at the end (the actual EOD-failure shape).
    non_pillar_features = [f for f in FEATURES if f not in pillar_fields]
    existing = (
        list(OHLCV_COLS) + [PROVENANCE_COL] + non_pillar_features + pillar_fields
    )
    canonical = _canonical_column_order(existing)
    # Canonical = OHLCV + source + FEATURES (pillars back in their middle slot).
    assert canonical == list(OHLCV_COLS) + [PROVENANCE_COL] + list(FEATURES)
    # And the scrambled layout is not canonical to begin with.
    assert not _is_canonical(existing)


def test_canonical_order_preserves_unknown_tail_columns():
    """Cols that aren't in OHLCV/source/FEATURES survive at the end of the row."""
    deprecated = "experimental_factor_x"
    assert deprecated not in FEATURES  # guard
    existing = list(OHLCV_COLS) + [PROVENANCE_COL] + list(FEATURES) + [deprecated]
    canonical = _canonical_column_order(existing)
    assert canonical[-1] == deprecated
    assert len(canonical) == len(existing)


def test_is_canonical_recognizes_correct_layout():
    cols = list(OHLCV_COLS) + [PROVENANCE_COL] + list(FEATURES)
    assert _is_canonical(cols)


def test_is_canonical_rejects_pillar_at_end_layout():
    pillar_fields = [
        "revenue_growth_3y",
        "eps_growth_3y",
        "payout_ratio",
        "dividend_yield",
        "capex_growth_5y",
    ]
    non_pillar = [f for f in FEATURES if f not in pillar_fields]
    scrambled = (
        list(OHLCV_COLS) + [PROVENANCE_COL] + non_pillar + pillar_fields
    )
    assert not _is_canonical(scrambled)


# ── migrate_universe_feature_order (functional) ──────────────────────────────


def _patch_libs(monkeypatch, tickers_to_frames: dict[str, pd.DataFrame]):
    """Stub out the universe lib + s3 client so the migration runs in-memory."""
    from builders import migrate_universe_feature_order as _m

    universe_lib = MagicMock()
    universe_lib.list_symbols.return_value = list(tickers_to_frames.keys())

    state = {t: df.copy() for t, df in tickers_to_frames.items()}

    def _read(ticker):
        result = MagicMock()
        result.data = state[ticker].copy()
        return result

    def _write(ticker, df, prune_previous_versions=False):
        state[ticker] = df.copy()
        return None

    universe_lib.read.side_effect = _read
    universe_lib.write.side_effect = _write

    monkeypatch.setattr(_m, "get_universe_lib", lambda *a, **k: universe_lib)
    monkeypatch.setattr(_m, "boto3", MagicMock())
    monkeypatch.setattr(_m, "_write_audit", MagicMock())

    return universe_lib, state


def _scrambled_pillar_frame(rows: int = 5) -> pd.DataFrame:
    """Build the actual EOD-failure column shape: pillars appended at end."""
    pillar_fields = [
        "revenue_growth_3y",
        "eps_growth_3y",
        "payout_ratio",
        "dividend_yield",
        "capex_growth_5y",
    ]
    non_pillar = [f for f in FEATURES if f not in pillar_fields]
    cols = list(OHLCV_COLS) + [PROVENANCE_COL] + non_pillar + pillar_fields
    return _stock_frame(cols, rows)


def test_migration_dry_run_makes_no_writes(monkeypatch):
    frames = {"MMM": _scrambled_pillar_frame()}
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_feature_order(apply=False)
    assert result["migrated_count"] == 1
    assert result["errors_count"] == 0
    assert universe_lib.write.call_count == 0
    # In-memory state untouched.
    assert list(state["MMM"].columns)[-1] == "capex_growth_5y"


def test_migration_apply_restores_canonical_pillar_layout(monkeypatch):
    frames = {"MMM": _scrambled_pillar_frame()}
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_feature_order(apply=True)
    assert result["migrated_count"] == 1
    assert result["errors_count"] == 0
    assert universe_lib.write.call_count == 1
    expected = list(OHLCV_COLS) + [PROVENANCE_COL] + list(FEATURES)
    assert list(state["MMM"].columns) == expected


def test_migration_skips_already_canonical_symbols(monkeypatch):
    cols = list(OHLCV_COLS) + [PROVENANCE_COL] + list(FEATURES)
    frames = {"AAPL": _stock_frame(cols)}
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_feature_order(apply=True)
    assert result["already_canonical_count"] == 1
    assert result["migrated_count"] == 0
    assert universe_lib.write.call_count == 0


def test_migration_apply_is_idempotent(monkeypatch):
    frames = {"MMM": _scrambled_pillar_frame()}
    universe_lib, state = _patch_libs(monkeypatch, frames)
    migrate_universe_feature_order(apply=True)
    second = migrate_universe_feature_order(apply=True)
    assert second["already_canonical_count"] == 1
    assert second["migrated_count"] == 0


def test_migration_preserves_row_values_across_reorder(monkeypatch):
    """Reorder must not corrupt cell values — just move columns around."""
    frames = {"MMM": _scrambled_pillar_frame(rows=10)}
    before = frames["MMM"].copy()
    universe_lib, state = _patch_libs(monkeypatch, frames)
    migrate_universe_feature_order(apply=True)
    after = state["MMM"]
    # Same row index, same per-column values — just different column order.
    for col in before.columns:
        np.testing.assert_array_equal(before[col].values, after[col].values)
