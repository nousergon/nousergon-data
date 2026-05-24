"""Regression tests for L1308 edgartools API drift fix.

Pre-fix: `{h.cusip: h.value for h in thirteen_f.holdings}` iterated over
a DataFrame (edgartools 5.x return shape), yielding column-name strings
that lack `.cusip` / `.value` attrs → AttributeError → 0/N institutional
data populated → DataPhase2 status=error → health/data_phase2.json stale
29 days (2026-04-25 → 2026-05-24 audit).

Post-fix: `_holdings_to_value_dict` helper handles both API shapes
(DataFrame from edgartools 5.x, list-of-objects from 4.x) and tolerates
column-case variations.

Pins:
  1. DataFrame input (current edgartools 5.x) → correct cusip→value map.
  2. List-of-objects input (legacy edgartools 4.x) → same shape.
  3. None / empty → empty dict (no crash).
  4. Missing column → empty dict + WARNING (not a crash).
  5. Column-case variations (Cusip / cusip / CUSIP) tolerated.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from collectors.alternative import _holdings_to_value_dict


def test_dataframe_input_pascalcase_columns():
    """edgartools 5.x current API: PascalCase Cusip + Value columns."""
    df = pd.DataFrame({
        "Cusip": ["68389X105", "037833100", "594918104"],
        "Value": [50_000_000, 75_000_000, 100_000_000],
        "Issuer": ["ORCL", "AAPL", "MSFT"],
    })
    out = _holdings_to_value_dict(df)
    assert out == {
        "68389X105": 50_000_000,
        "037833100": 75_000_000,
        "594918104": 100_000_000,
    }


def test_dataframe_input_lowercase_columns():
    """Tolerate lowercase variants (some edgartools versions diverge)."""
    df = pd.DataFrame({
        "cusip": ["AAA", "BBB"],
        "value": [10, 20],
    })
    out = _holdings_to_value_dict(df)
    assert out == {"AAA": 10, "BBB": 20}


def test_dataframe_input_uppercase_columns():
    """Tolerate ALL-CAPS variants (some edgartools versions)."""
    df = pd.DataFrame({
        "CUSIP": ["AAA"],
        "VALUE": [10],
    })
    out = _holdings_to_value_dict(df)
    assert out == {"AAA": 10}


def test_dataframe_missing_columns_returns_empty():
    """Defensive: a DataFrame without Cusip/Value columns must return
    empty dict + WARN log (NOT a crash)."""
    df = pd.DataFrame({"foo": [1, 2], "bar": [3, 4]})
    out = _holdings_to_value_dict(df)
    assert out == {}


def test_empty_dataframe():
    df = pd.DataFrame(columns=["Cusip", "Value"])
    assert _holdings_to_value_dict(df) == {}


def test_none_input():
    assert _holdings_to_value_dict(None) == {}


def test_legacy_list_of_objects_fallback():
    """edgartools 4.x legacy: iterating holdings yielded objects with
    .cusip + .value attrs. The fallback path must still work for any
    consumer that pins the old version (or a future drift back)."""
    class FakeHolding:
        def __init__(self, cusip, value):
            self.cusip = cusip
            self.value = value
    holdings = [
        FakeHolding("AAA", 100),
        FakeHolding("BBB", 200),
    ]
    out = _holdings_to_value_dict(holdings)
    assert out == {"AAA": 100, "BBB": 200}


def test_legacy_iteration_crash_returns_empty():
    """If neither path works, return empty dict — never raise."""
    # Iterating a non-iterable that's not DataFrame triggers TypeError.
    out = _holdings_to_value_dict(42)
    assert out == {}
