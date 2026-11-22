"""Unit tests for the split-restatement math helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from corporate_actions._split_math import (
    cumulative_factor,
    restate_series_for_splits,
    split_events,
)


class _SplitClient:
    def __init__(self, events: list[dict[str, Any]]) -> None:
        self.events = events
        self.tickers: list[str] = []

    def get_splits(self, ticker: str) -> list[dict[str, Any]]:
        self.tickers.append(ticker)
        return self.events


class _FailingSplitClient:
    def get_splits(self, ticker: str) -> list[dict[str, Any]]:
        raise RuntimeError(f"split lookup failed for {ticker}")


def test_split_events_returns_client_results() -> None:
    events: list[dict[str, Any]] = [
        {"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}
    ]
    client = _SplitClient(events)

    assert split_events("EXAMPLE", client) == [
        {"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}
    ]
    assert client.tickers == ["EXAMPLE"]


def test_split_events_accepts_empty_results() -> None:
    assert split_events("EXAMPLE", _SplitClient([])) == []


def test_split_events_propagates_client_failure() -> None:
    with pytest.raises(RuntimeError, match="split lookup failed for EXAMPLE"):
        split_events("EXAMPLE", _FailingSplitClient())


def test_cumulative_factor_compounds_splits_through_reference_date() -> None:
    events = [
        {"execution_date": "2026-01-10", "split_from": 1, "split_to": 2},
        {"execution_date": "2026-06-24", "split_from": 3, "split_to": 1},
        {"execution_date": "2026-09-01", "split_from": 1, "split_to": 4},
    ]

    assert cumulative_factor(
        events, "2026-01-01", reference_date="2026-06-24"
    ) == pytest.approx(1.5)


def test_cumulative_factor_excludes_split_on_price_date() -> None:
    events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]

    # Changing the strict comparison from e > d to e >= d makes this fail.
    assert cumulative_factor(events, "2026-06-24") == 1.0


def test_cumulative_factor_rejects_invalid_date() -> None:
    with pytest.raises(ValueError):
        cumulative_factor([], "not-a-date")


def test_restate_series_adjusts_prices_and_inverse_volume_without_mutation() -> None:
    frame = pd.DataFrame(
        {"Close": [10.0, 30.0], "Volume": [300.0, 100.0], "Other": [7, 8]},
        index=pd.to_datetime(["2026-06-23", "2026-06-24"]),
    )
    events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]

    result = restate_series_for_splits(frame, events)

    assert result["Close"].tolist() == [30.0, 30.0]
    assert result["Volume"].tolist() == [100.0, 100.0]
    assert result["Other"].tolist() == [7, 8]
    assert frame["Close"].tolist() == [10.0, 30.0]


def test_restate_series_empty_frame_is_same_object() -> None:
    frame = pd.DataFrame(columns=["Close"])

    assert restate_series_for_splits(frame, []) is frame


def test_restate_series_rejects_unparseable_index() -> None:
    frame = pd.DataFrame({"Close": [10.0]}, index=["not-a-date"])
    events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]

    with pytest.raises(ValueError):
        restate_series_for_splits(frame, events)
