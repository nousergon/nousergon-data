"""alpha-engine-config-I13143: ``days_since_earnings`` was never produced.

``features/compute.py::_alt_entry_from_payload`` read
``eps_revision.days_since_earnings`` with a 0.0 default, and no producer wrote
it, so the feature was 0.0 for every ticker. These tests pin the producer
(``collectors/alternative._fetch_days_since_earnings``) and the consumer's
absent state.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from collectors import alternative
from features.compute import _alt_entry_from_payload


def _yf_with_dates(rows: list[tuple[str, float | None]]) -> MagicMock:
    idx = pd.DatetimeIndex(
        [pd.Timestamp(d, tz="America/New_York") for d, _ in rows], name="Earnings Date"
    )
    df = pd.DataFrame(
        {"EPS Estimate": [1.0] * len(rows), "Reported EPS": [r for _, r in rows]},
        index=idx,
    )
    mod = MagicMock()
    mod.Ticker.return_value.get_earnings_dates.return_value = df
    return mod


def _days(mod, run_date="2026-10-30"):
    with patch.dict("sys.modules", {"yfinance": mod}):
        return alternative._fetch_days_since_earnings("AAPL", run_date)


def test_days_from_last_reported_date_on_or_before_run_date():
    mod = _yf_with_dates([
        ("2027-01-29 16:30", np.nan),   # upcoming: not reported
        ("2026-10-29 16:30", 1.85),     # last reported
        ("2026-07-30 16:30", 1.91),
    ])
    assert _days(mod) == 1


def test_a_date_after_run_date_never_counts_even_if_reported():
    # Replaying 10-28 must not see the 10-29 report (point-in-time).
    mod = _yf_with_dates([("2026-10-29 16:30", 1.85), ("2026-07-30 16:30", 1.91)])
    assert _days(mod, run_date="2026-10-28") == 90


def test_no_reported_date_is_none():
    assert _days(_yf_with_dates([("2027-01-29 16:30", np.nan)])) is None


def test_yfinance_failure_is_none_not_raise():
    mod = MagicMock()
    mod.Ticker.return_value.get_earnings_dates.side_effect = RuntimeError("429")
    assert _days(mod) is None


def test_consumer_absent_is_none_not_zero():
    entry = _alt_entry_from_payload({"eps_revision": {"revision_4w": 1.0, "streak": 2}})
    assert entry["earnings"]["days_since_earnings"] is None


def test_consumer_passes_produced_value():
    entry = _alt_entry_from_payload({"eps_revision": {"days_since_earnings": 12}})
    assert entry["earnings"]["days_since_earnings"] == 12
