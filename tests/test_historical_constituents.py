"""Tests for point-in-time S&P 500 membership reconstruction (G12, #657).

Pure parse + replay tests — no network. A small synthetic changes table and
current roster exercise the survivorship-free reconstruction.
"""

from __future__ import annotations

import pandas as pd
import pytest

import collectors.historical_constituents as hc
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


# ── Source failover (config-I6944) ─────────────────────────────────────────
#
# The changes table moved off "List of S&P 500 companies" into its own
# article on 2026-08-11 and hard-failed DataPhase1 of the weekly SF three
# hours later. These pin the failover: a candidate that 404s, or that serves
# a page with no changes table, must fall through to the next one.


class _FakeResponse:
    def __init__(self, text: str, ok: bool = True):
        self.text = text
        self._ok = ok

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError("HTTP 404")


def _install_fake_fetch(monkeypatch, pages: dict[str, object]):
    """Map url -> _FakeResponse; read_html returns the tables keyed by text."""
    tables_by_text: dict[str, list[pd.DataFrame]] = {}

    def fake_get(url, headers=None, timeout=None):
        entry = pages[url]
        if isinstance(entry, _FakeResponse):
            return entry
        text = f"page:{url}"
        tables_by_text[text] = entry
        return _FakeResponse(text)

    monkeypatch.setattr(hc.requests, "get", fake_get)
    monkeypatch.setattr(hc.pd, "read_html", lambda buf: tables_by_text[buf.getvalue()])


def test_fetch_falls_through_to_the_next_url_when_a_page_lacks_the_table(monkeypatch):
    roster_only = [pd.DataFrame({"Symbol": ["AAPL"], "GICS Sector": ["Tech"]})]
    _install_fake_fetch(
        monkeypatch,
        {"https://a.example/moved": roster_only, "https://b.example/old": [_changes_df()]},
    )
    df, url = hc._fetch_changes_table(
        ("https://a.example/moved", "https://b.example/old")
    )
    assert url == "https://b.example/old"
    assert any("added" in str(c).lower() for c in df.columns)


def test_fetch_falls_through_when_a_page_errors(monkeypatch):
    _install_fake_fetch(
        monkeypatch,
        {
            "https://a.example/gone": _FakeResponse("", ok=False),
            "https://b.example/old": [_changes_df()],
        },
    )
    _, url = hc._fetch_changes_table(
        ("https://a.example/gone", "https://b.example/old")
    )
    assert url == "https://b.example/old"


def test_fetch_raises_naming_every_candidate_when_all_fail(monkeypatch):
    roster_only = [pd.DataFrame({"Symbol": ["AAPL"]})]
    _install_fake_fetch(
        monkeypatch,
        {"https://a.example/one": roster_only, "https://b.example/two": roster_only},
    )
    with pytest.raises(RuntimeError) as exc:
        hc._fetch_changes_table(("https://a.example/one", "https://b.example/two"))
    # A failure that names only the last URL tried sends the reader to the
    # wrong page; both candidates must appear.
    assert "a.example/one" in str(exc.value) and "b.example/two" in str(exc.value)


def test_the_dedicated_article_is_tried_before_the_roster_page():
    assert hc._SP500_CHANGES_URLS[0].endswith("Historical_components_of_the_S%26P_500")
    assert "List_of_S%26P_500_companies" in hc._SP500_CHANGES_URLS[1]


# ── Self-sourced membership (config-I6946) ─────────────────────────────────
#
# Wikipedia went from the PRODUCER of index changes to their auditor. The
# frozen artifact covers settled pre-cutover history; everything after comes
# from diffing our own dated roster snapshots.


def _snaps() -> dict[str, list[str]]:
    return {
        "2026-04-04": ["AAA", "BBB", "CCC"],
        "2026-04-11": ["AAA", "BBB", "DDD"],   # CCC out, DDD in
        "2026-04-18": ["AAA", "BBB", "DDD", "EEE"],  # EEE in, nothing out
    }


def test_snapshot_diff_emits_the_observed_adds_and_removes():
    changes, _ = hc.changes_from_snapshots(_snaps())
    assert set((c.ticker, c.action) for c in changes) == {
        ("CCC", REMOVED), ("DDD", ADDED), ("EEE", ADDED),
    }


def test_the_earliest_snapshot_is_a_baseline_not_an_event():
    changes, _ = hc.changes_from_snapshots(_snaps())
    # AAA/BBB/CCC exist on the first snapshot; none of them may be reported
    # as ADDED, or every backtest would think the index was created that day.
    assert not [c for c in changes if c.date == "2026-04-04"]


def test_a_change_is_dated_to_the_snapshot_that_observed_it():
    changes, _ = hc.changes_from_snapshots(_snaps())
    assert {c.date for c in changes} == {"2026-04-11", "2026-04-18"}


def test_a_known_reticker_is_not_a_membership_change():
    """The failure this pins: BK -> BNY and SATS -> ECHO showed up in the
    holdings diff as a company leaving the index and a different one joining.
    Both are corporate rebrands. Polygon reports neither as a ticker_change
    (verified live 2026-08-12), so nothing downstream would have caught it."""
    snaps = {
        "2026-05-15": ["AAA", "BK"],
        "2026-05-22": ["AAA", "BNY"],
    }
    changes, unresolved = hc.changes_from_snapshots(snaps, {"BK": "BNY"})
    assert changes == []
    assert unresolved == []


def test_an_unexplained_swap_is_still_emitted_and_reported():
    # A real index change is a swap most weeks. Suppressing it would be
    # strictly worse than mis-dating a rename, so it is emitted — and named.
    snaps = {"2026-05-15": ["AAA", "OLD"], "2026-05-22": ["AAA", "NEW"]}
    changes, unresolved = hc.changes_from_snapshots(snaps)
    assert set((c.ticker, c.action) for c in changes) == {
        ("OLD", REMOVED), ("NEW", ADDED),
    }
    assert len(unresolved) == 1 and "OLD" in unresolved[0] and "NEW" in unresolved[0]


def test_same_date_swaps_only_flags_dates_with_both_directions():
    swaps = hc.same_date_swaps(_snaps())
    assert "2026-04-11" in swaps          # CCC out / DDD in
    assert "2026-04-18" not in swaps      # EEE in, nothing out — not a swap


# ── The frozen pre-cutover history ─────────────────────────────────────────


def test_frozen_history_loads_and_its_hash_verifies():
    changes = hc.load_frozen_changes()
    assert len(changes) > 700
    assert all(c.date < hc.SNAPSHOT_CUTOVER for c in changes), (
        "the frozen artifact must not overlap the observed window, or every "
        "post-cutover change would be counted twice"
    )


def test_a_tampered_frozen_history_raises_rather_than_building_a_universe(tmp_path):
    import json as _json
    body = _json.loads(hc._FROZEN_CHANGES_PATH.read_text())
    body["changes"].append({"date": "1999-01-01", "ticker": "FAKE", "action": ADDED})
    path = tmp_path / "frozen.json"
    path.write_text(_json.dumps(body))
    with pytest.raises(RuntimeError, match="content hash mismatch"):
        hc.load_frozen_changes(path)


# ── The attestation ────────────────────────────────────────────────────────


def test_divergences_ignores_dates_and_compares_membership():
    # The snapshot derivation dates a change to the snapshot that observed
    # it, which is later than the effective date. Comparing dates would
    # report a disagreement on every change and mean nothing.
    observed = [ConstituentChange("2026-05-22", "AAA", ADDED)]
    reference = [ConstituentChange("2026-05-19", "AAA", ADDED)]
    assert hc.divergences(observed, reference, since=hc.SNAPSHOT_CUTOVER) == []


def test_divergences_reports_both_directions():
    observed = [ConstituentChange("2026-05-22", "AAA", ADDED)]
    reference = [ConstituentChange("2026-05-22", "BBB", ADDED)]
    found = hc.divergences(observed, reference, since=hc.SNAPSHOT_CUTOVER)
    assert any("AAA" in f and "observed" in f for f in found)
    assert any("BBB" in f and "reference" in f for f in found)


def test_divergences_ignores_everything_before_the_cutover():
    # Pre-cutover changes come from the frozen artifact, which was built FROM
    # the reference — comparing them would assert nothing and hide the window
    # that matters.
    observed = []
    reference = [ConstituentChange("2020-01-01", "OLD", ADDED)]
    assert hc.divergences(observed, reference, since=hc.SNAPSHOT_CUTOVER) == []


# ── The S&P 500 slice of a roster snapshot ─────────────────────────────────


def test_explicit_per_index_list_is_preferred_over_the_prefix():
    snap = {"tickers": ["A", "B", "C"], "sp500_count": 1, "sp400_count": 2,
            "sp500_tickers": ["A", "B"]}
    assert hc._sp500_roster(snap) == ["A", "B"]


def test_the_prefix_is_used_when_the_counts_account_for_the_whole_list():
    snap = {"tickers": ["A", "B", "C"], "sp500_count": 2, "sp400_count": 1}
    assert hc._sp500_roster(snap) == ["A", "B"]


def test_a_snapshot_whose_counts_do_not_add_up_is_refused():
    """Slicing anyway would drop names off the roster, which the diff would
    then emit as a burst of index removals that never happened — worse than
    a gap, because it looks like real churn. A list LONGER than the counts,
    or an overlap far larger than any rebalance, is that case.

    (A SMALL shortfall is a rebalance-day overlap and is recoverable —
    alpha-engine-config-I11470; see test_historical_constituents_quality_i11470.)"""
    longer = {"tickers": ["A", "B", "C", "D"], "sp500_count": 2, "sp400_count": 1}
    assert hc._sp500_roster(longer) is None
    too_much_overlap = {
        "tickers": ["A", "B", "C"], "sp500_count": 2,
        "sp400_count": 2 + hc.MAX_RECOVERABLE_INDEX_OVERLAP,
    }
    assert hc._sp500_roster(too_much_overlap) is None
