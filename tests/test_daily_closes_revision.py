"""Tests for the ``revision`` per-cell write counter
(alpha-engine-config-I10783, sources/contract.py::PriceBar).

``revision`` is a queryable audit trail of how many times a
staging/daily_closes cell has actually been overwritten, so "no silent
overwrite between vendors" is measurable rather than inferred from logs.
"""

from __future__ import annotations

from collectors.daily_closes import _coalesce_by_source_priority
from sources.contract import PriceBar, RECORD_KEYS


def _row(ticker, close, source, revision=None):
    r = {
        "ticker": ticker, "date": "2026-09-14", "Open": close, "High": close,
        "Low": close, "Close": close, "Adj_Close": close, "Volume": 0,
        "VWAP": None, "source": source,
    }
    if revision is not None:
        r["revision"] = revision
    return r


def test_pricebar_defaults_revision_to_one():
    bar = PriceBar(
        ticker="AAPL", date="2026-09-14", open=1, high=1, low=1, close=1,
        adj_close=1, volume=100, source="polygon",
    )
    assert bar.revision == 1
    assert bar.to_record()["revision"] == 1
    assert "revision" in RECORD_KEYS


def test_new_only_cell_starts_at_revision_one():
    merged, stats = _coalesce_by_source_priority(
        [_row("AAPL", 100.0, "polygon")], [], "2026-09-14",
    )
    by_ticker = {r["ticker"]: r for r in merged}
    assert by_ticker["AAPL"]["revision"] == 1
    assert stats["new_only"] == 1


def test_overwrite_increments_revision_off_the_prior_rows():
    existing = [_row("AAPL", 100.0, "yfinance", revision=1)]
    fresh = [_row("AAPL", 100.5, "polygon")]
    merged, stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    by_ticker = {r["ticker"]: r for r in merged}
    assert by_ticker["AAPL"]["revision"] == 2
    assert stats["overwritten"] == 1


def test_a_legacy_prior_row_with_no_revision_column_reads_as_one():
    """A parquet written before I10783 has no `revision` column at all."""
    existing = [_row("AAPL", 100.0, "yfinance")]  # no revision key
    fresh = [_row("AAPL", 100.5, "polygon")]
    merged, _stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    by_ticker = {r["ticker"]: r for r in merged}
    assert by_ticker["AAPL"]["revision"] == 2  # legacy read as 1, then + 1


def test_retained_cell_keeps_its_prior_revision_unchanged():
    existing = [_row("TNX", 4.5, "fred", revision=7)]
    fresh = [_row("AAPL", 200.0, "polygon")]  # TNX absent this run
    merged, stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    by_ticker = {r["ticker"]: r for r in merged}
    assert by_ticker["TNX"]["revision"] == 7
    assert stats["retained"] == 1


def test_downgrade_blocked_cell_keeps_its_prior_revision_unchanged():
    existing = [_row("AAPL", 100.0, "polygon", revision=3)]
    fresh = [_row("AAPL", 101.0, "yfinance")]  # strictly lower priority
    merged, stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    by_ticker = {r["ticker"]: r for r in merged}
    assert by_ticker["AAPL"]["revision"] == 3
    assert stats["downgrade_blocked"] == 1


def test_repeated_overwrites_accumulate_revision():
    """day 1: new. day 2: polygon restates. day 3: polygon restates again."""
    merged1, _ = _coalesce_by_source_priority(
        [_row("AAPL", 100.0, "polygon")], [], "2026-09-14",
    )
    merged2, _ = _coalesce_by_source_priority(
        [_row("AAPL", 100.1, "polygon")], merged1, "2026-09-14",
    )
    merged3, _ = _coalesce_by_source_priority(
        [_row("AAPL", 100.2, "polygon")], merged2, "2026-09-14",
    )
    by_ticker = {r["ticker"]: r for r in merged3}
    assert by_ticker["AAPL"]["revision"] == 3
