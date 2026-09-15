"""Producer contract test for staging_daily_closes.schema.json
(alpha-engine-config-I10783, data-collector plan P-16).

Mirrors the existing contract-test pattern (contracts/__init__.py — see
test_technical_rating_contracts.py): every row this producer writes must
validate cleanly against its own versioned JSON Schema, checked at PR time.

Covers:
  - A row produced by the REAL coalesce logic (``_coalesce_by_source_priority``
    -> ``sources.contract.PriceBar.from_record``), for both the "new" and the
    "overwritten" (revision-bumped) cases.
  - A hand-built minimal fixture, independent of the producer code, so a
    producer bug that silently matches its own (wrong) output can't also pass
    the contract by construction.
  - A legacy row (no `revision` column) still validates once normalized
    through `PriceBar.from_record` — the additive migration path.
"""

from __future__ import annotations

import pytest

from collectors.daily_closes import _coalesce_by_source_priority
from contracts import validate_staging_daily_closes_row
from sources.contract import PriceBar

pytest.importorskip("jsonschema")


def _row(ticker, close, source, revision=None):
    r = {
        "ticker": ticker, "date": "2026-09-14", "Open": close, "High": close,
        "Low": close, "Close": close, "Adj_Close": close, "Volume": 100,
        "VWAP": None, "source": source,
    }
    if revision is not None:
        r["revision"] = revision
    return r


def test_new_only_row_validates():
    merged, _stats = _coalesce_by_source_priority(
        [_row("AAPL", 200.0, "polygon")], [], "2026-09-14",
    )
    assert validate_staging_daily_closes_row(merged[0]) == []


def test_overwritten_row_with_bumped_revision_validates():
    existing = [_row("AAPL", 199.0, "yfinance", revision=1)]
    fresh = [_row("AAPL", 200.0, "polygon")]
    merged, _stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    row = merged[0]
    assert row["revision"] == 2
    assert validate_staging_daily_closes_row(row) == []


def test_pricebar_round_trip_row_validates():
    bar = PriceBar(
        ticker="MSFT", date="2026-09-14", open=1, high=2, low=0.5, close=1.5,
        adj_close=1.5, volume=1000, source="yfinance",
    )
    assert validate_staging_daily_closes_row(bar.to_record()) == []


def test_hand_built_minimal_fixture_validates():
    """Independent of the producer code — pins the contract, not the code."""
    row = {
        "ticker": "SPY", "date": "2026-09-14", "Open": 500.0, "High": 505.0,
        "Low": 499.0, "Close": 503.0, "Adj_Close": 503.0, "Volume": 1_000_000,
        "VWAP": 502.5, "source": "polygon", "revision": 1,
    }
    assert validate_staging_daily_closes_row(row) == []


def test_missing_required_field_fails_validation():
    row = {
        "ticker": "SPY", "date": "2026-09-14", "Open": 500.0, "High": 505.0,
        "Low": 499.0, "Close": 503.0, "Adj_Close": 503.0, "Volume": 1_000_000,
        "VWAP": 502.5, "source": "polygon",
        # "revision" deliberately omitted
    }
    assert validate_staging_daily_closes_row(row) != []


def test_unknown_source_fails_validation():
    row = {
        "ticker": "SPY", "date": "2026-09-14", "Open": 500.0, "High": 505.0,
        "Low": 499.0, "Close": 503.0, "Adj_Close": 503.0, "Volume": 1_000_000,
        "VWAP": 502.5, "source": "databento", "revision": 1,
    }
    assert validate_staging_daily_closes_row(row) != []


def test_legacy_row_with_no_revision_column_normalizes_and_validates():
    """A pre-I10783 persisted row, read back through PriceBar.from_record,
    gets the default revision=1 and validates — the additive migration path."""
    legacy = _row("VIX", 17.0, "fred")  # no revision key
    normalized = PriceBar.from_record(legacy).to_record()
    assert validate_staging_daily_closes_row(normalized) == []
