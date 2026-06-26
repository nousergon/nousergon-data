"""Tests for point-in-time S&P 500 membership reconstruction (G12, #657).

Pure parse + replay tests — no network. A small synthetic changes table and
current roster exercise the survivorship-free reconstruction.
"""

from __future__ import annotations

import pandas as pd
import pytest

from collectors.historical_constituents import (
    ADDED,
    REMOVED,
    ConstituentChange,
    build_pit_membership,
    parse_changes_table,
    select_changes_table,
)


def _changes_df() -> pd.DataFrame:
    # Mirrors the Wikipedia "Selected changes to the list" shape: a Date
    # column plus Added/Removed ticker columns (one side may be blank).
    return pd.DataFrame(
        {
            "Date": ["March 20, 2025", "January 2, 2024", "January 2, 2024"],
            "Added Ticker": ["NEWCO", "ADDED1", ""],
            "Removed Ticker": ["OLDCO", "", "REMOVED1"],
        }
    )


# ── Parsing ────────────────────────────────────────────────────────────────


def test_parse_extracts_added_and_removed_events():
    changes = parse_changes_table(_changes_df())
    by = {(c.ticker, c.action) for c in changes}
    assert ("NEWCO", ADDED) in by
    assert ("OLDCO", REMOVED) in by
    assert ("ADDED1", ADDED) in by
    assert ("REMOVED1", REMOVED) in by
    # The blank cells must NOT produce phantom events.
    assert all(c.ticker for c in changes)


def test_parse_normalizes_dates_to_iso_and_sorts_oldest_first():
    changes = parse_changes_table(_changes_df())
    dates = [c.date for c in changes]
    assert dates == sorted(dates)
    assert changes[0].date == "2024-01-02"
    assert changes[-1].date == "2025-03-20"


def test_parse_strips_footnote_markers():
    df = pd.DataFrame(
        {
            "Date": ["March 20, 2025"],
            "Added Ticker": ["NEWCO[1]"],
            "Removed Ticker": ["OLD.CO[2]"],
        }
    )
    changes = parse_changes_table(df)
    tickers = {c.ticker for c in changes}
    assert "NEWCO" in tickers and "OLD.CO" in tickers


def test_select_changes_table_picks_by_columns():
    banner = pd.DataFrame({0: ["disambiguation"], 1: ["note"]})
    roster = pd.DataFrame({"Symbol": ["AAPL"], "GICS Sector": ["Tech"]})
    picked = select_changes_table([banner, roster, _changes_df()])
    cols = [c.lower() for c in picked.columns]
    assert any("added" in c for c in cols) and any("removed" in c for c in cols)


def test_select_changes_table_raises_when_absent():
    with pytest.raises(RuntimeError):
        select_changes_table([pd.DataFrame({"Symbol": ["AAPL"]})])


# ── Point-in-time replay ───────────────────────────────────────────────────


def test_build_pit_undoes_changes_backward():
    # Current roster reflects all changes applied. NEWCO/ADDED1 are present
    # because they were added; OLDCO/REMOVED1 are absent because removed.
    current = ["AAPL", "MSFT", "NEWCO", "ADDED1"]
    changes = [
        ConstituentChange("2025-03-20", "NEWCO", ADDED),
        ConstituentChange("2025-03-20", "OLDCO", REMOVED),
        ConstituentChange("2024-01-02", "ADDED1", ADDED),
        ConstituentChange("2024-01-02", "REMOVED1", REMOVED),
    ]
    pit = build_pit_membership(current, changes)

    # Just before the 2025-03-20 change: NEWCO not yet a member, OLDCO still in.
    before_2025 = set(pit["2025-03-20"])
    assert "NEWCO" not in before_2025
    assert "OLDCO" in before_2025
    assert {"AAPL", "MSFT", "ADDED1"} <= before_2025

    # Just before 2024-01-02: undo that day too — ADDED1 not yet in,
    # REMOVED1 still in, and (walking further back) OLDCO still in.
    before_2024 = set(pit["2024-01-02"])
    assert "ADDED1" not in before_2024
    assert "REMOVED1" in before_2024
    assert "OLDCO" in before_2024
    assert "NEWCO" not in before_2024


def test_pit_snapshots_are_sorted_lists():
    current = ["MSFT", "AAPL", "NEWCO"]
    changes = [ConstituentChange("2025-03-20", "NEWCO", ADDED)]
    pit = build_pit_membership(current, changes)
    snap = pit["2025-03-20"]
    assert snap == sorted(snap)
    assert "NEWCO" not in snap  # added on that date -> absent just before


def test_delisted_ticker_reappears_in_historical_universe():
    """The core survivorship fix: a name removed from the index is ABSENT
    from today's roster but PRESENT in the as-of-date universe."""
    current = ["AAPL"]  # survivor only
    changes = [ConstituentChange("2024-06-01", "DELISTED", REMOVED)]
    pit = build_pit_membership(current, changes)
    assert "DELISTED" in pit["2024-06-01"]
