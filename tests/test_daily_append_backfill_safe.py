"""Tests for _write_row_backfill_safe — the helper that handles both
append (target_date > all existing dates, fast lib.update() path) and
backfill (target_date in middle of series, lib.write() full rewrite).

Background: the 2026-04-24 historical VWAP repair (after the polygon
4/17→4/23 outage) surfaced that ArcticDB's update() raises
"index must be monotonic increasing or decreasing" when asked to insert
behind the latest stored date. daily_append was originally designed for
"append today's row at the head" only; this helper makes it usable for
arbitrary historical backfills too.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from builders.daily_append import _write_row_backfill_safe


def _series(dates: list[str], close_values: list[float] | None = None) -> pd.DataFrame:
    """Build a minimal stored-series DataFrame for the mock lib.read()."""
    closes = close_values if close_values is not None else [100.0 + i for i in range(len(dates))]
    return pd.DataFrame(
        {"Close": closes, "Open": closes, "High": closes, "Low": closes,
         "Volume": [1_000_000] * len(dates), "VWAP": [None] * len(dates)},
        index=pd.DatetimeIndex(pd.to_datetime(dates)),
    )


def _new_row(date: str, close: float = 999.0) -> pd.DataFrame:
    return pd.DataFrame(
        {"Close": [close], "Open": [close], "High": [close], "Low": [close],
         "Volume": [1_000_000], "VWAP": [close * 0.99]},
        index=pd.DatetimeIndex(pd.to_datetime([date])),
    )


# ── append path (target > latest, fast) ────────────────────────────────────


def test_append_uses_lib_update_when_target_after_latest():
    lib = MagicMock()
    existing = _series(["2026-04-20", "2026-04-21", "2026-04-22"])
    new_row = _new_row("2026-04-23")

    mode = _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=existing)

    assert mode == "append"
    lib.update.assert_called_once_with("AAPL", new_row)
    lib.write.assert_not_called()


def test_append_when_existing_series_is_empty():
    """First write to a previously-empty symbol takes the append path."""
    lib = MagicMock()
    empty = pd.DataFrame(
        columns=["Close", "Open", "High", "Low", "Volume", "VWAP"],
        index=pd.DatetimeIndex([]),
    )
    new_row = _new_row("2026-04-23")

    mode = _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=empty)

    assert mode == "append"
    lib.update.assert_called_once()
    lib.write.assert_not_called()


def test_first_write_to_nonexistent_symbol():
    """If lib.read raises, the symbol doesn't exist — first write must use write()."""
    lib = MagicMock()
    lib.read.side_effect = Exception("symbol not found")

    new_row = _new_row("2026-04-23")
    mode = _write_row_backfill_safe(lib, "NEW_TICKER", new_row)

    assert mode == "append"
    lib.write.assert_called_once()
    lib.update.assert_not_called()


# ── backfill path (target ≤ latest, full rewrite) ──────────────────────────


def test_backfill_uses_lib_write_when_target_before_latest():
    """Inserting a row behind the latest must NOT call update() (would raise
    'index must be monotonic increasing or decreasing'). Must use write()
    with the spliced full series."""
    lib = MagicMock()
    existing = _series(["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23"])
    # Target = 2026-04-17, BEFORE all existing dates
    new_row = _new_row("2026-04-17", close=200.0)

    mode = _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=existing)

    assert mode == "backfill"
    lib.update.assert_not_called()
    lib.write.assert_called_once()
    # Verify the written frame includes both the new row + all existing rows,
    # in monotonic-sorted order, with no duplicates.
    written = lib.write.call_args.args[1]
    assert pd.Timestamp("2026-04-17") in written.index
    assert pd.Timestamp("2026-04-23") in written.index
    assert written.index.is_monotonic_increasing
    assert not written.index.has_duplicates


def test_backfill_replaces_existing_same_date_row():
    """If the target date already has a row in the existing series, the new
    row must REPLACE it (matches update()'s same-date semantics)."""
    lib = MagicMock()
    existing = _series(
        ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23"],
        close_values=[100.0, 101.0, 102.0, 103.0],
    )
    new_row = _new_row("2026-04-21", close=999.0)

    mode = _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=existing)

    assert mode == "backfill"
    written = lib.write.call_args.args[1]
    # 2026-04-21 should now be 999.0 (the new row), not 101.0 (the old row).
    assert written.loc[pd.Timestamp("2026-04-21"), "Close"] == 999.0
    # Other rows unchanged.
    assert written.loc[pd.Timestamp("2026-04-20"), "Close"] == 100.0
    assert written.loc[pd.Timestamp("2026-04-23"), "Close"] == 103.0


def test_backfill_target_in_middle_of_series():
    """Target = date in middle of existing series. Backfill mode + write."""
    lib = MagicMock()
    existing = _series(["2026-04-15", "2026-04-16", "2026-04-22", "2026-04-23"])
    new_row = _new_row("2026-04-17", close=500.0)

    mode = _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=existing)

    assert mode == "backfill"
    written = lib.write.call_args.args[1]
    assert pd.Timestamp("2026-04-17") in written.index
    assert written.loc[pd.Timestamp("2026-04-17"), "Close"] == 500.0
    assert len(written) == 5  # 4 existing + 1 new
    assert written.index.is_monotonic_increasing


# ── boundary cases ────────────────────────────────────────────────────────


def test_target_equal_to_latest_takes_append_path():
    """target_ts == latest_ts is the steady-state daily case (re-running
    today). Should use update() (which is idempotent for same-date)."""
    lib = MagicMock()
    existing = _series(["2026-04-21", "2026-04-22", "2026-04-23"])
    new_row = _new_row("2026-04-23", close=200.0)  # same date as latest

    mode = _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=existing)

    # target_ts >= latest is True (they're equal), so the append path runs
    # with update() — which is idempotent for same-date rows (replaces in
    # place rather than appending duplicates). Previously the >-check
    # (changed to >= in config-I2812) forced same-date overwrites into the
    # backfill path — full series read+write per ticker — burning 40+ min
    # of the preopen window on every MorningEnrich run.
    assert mode == "append"
    lib.update.assert_called_once()


def test_lib_write_called_with_prune_previous_versions():
    """write() with prune_previous_versions=True keeps storage small —
    backfill is rare enough that we don't need version history bloat."""
    lib = MagicMock()
    existing = _series(["2026-04-20", "2026-04-21"])
    new_row = _new_row("2026-04-15")

    _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=existing)

    call_kwargs = lib.write.call_args.kwargs
    assert call_kwargs.get("prune_previous_versions") is True


def test_passing_existing_series_avoids_extra_lib_read():
    """When the caller passes existing_series (the per-ticker loop in
    daily_append already reads `hist` for warmup), the helper must NOT
    re-read — saves a round-trip per ticker on the steady-state daily run.
    """
    lib = MagicMock()
    existing = _series(["2026-04-21", "2026-04-22"])
    new_row = _new_row("2026-04-23")

    _write_row_backfill_safe(lib, "AAPL", new_row, existing_series=existing)

    lib.read.assert_not_called()
