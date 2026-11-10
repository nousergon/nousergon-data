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
import pytest

from collectors import alternative
from collectors.yahoo_session import YahooAuthError
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


def test_yahoo_auth_failure_propagates():
    mod = _yf_with_dates([])
    with patch("collectors.yahoo_session.call_yahoo", side_effect=YahooAuthError("session rejected")):
        with pytest.raises(YahooAuthError, match="session rejected"):
            _days(mod)


def test_empty_earnings_frame_is_none():
    assert _days(_yf_with_dates([])) is None


def test_yahoo_auth_failure_propagates_through_ticker_worker():
    s3 = MagicMock()
    with (
        patch.object(alternative, "_fetch_analyst", return_value={}),
        patch.object(alternative, "_fetch_revisions", return_value={}),
        patch("collectors.yahoo_session.call_yahoo", side_effect=YahooAuthError("session rejected")),
        patch.dict("sys.modules", {"yfinance": _yf_with_dates([])}),
    ):
        with pytest.raises(YahooAuthError, match="session rejected"):
            alternative._process_one_ticker("AAPL", "2026-10-30", "bucket", [], s3, "")
    s3.put_object.assert_not_called()


def test_consumer_absent_is_none_not_zero():
    entry = _alt_entry_from_payload({"eps_revision": {"revision_4w": 1.0, "streak": 2}})
    assert entry["earnings"]["days_since_earnings"] is None


def test_consumer_passes_produced_value():
    entry = _alt_entry_from_payload({"eps_revision": {"days_since_earnings": 12}})
    assert entry["earnings"]["days_since_earnings"] == 12


def test_replay_of_an_old_date_widens_the_window_back_to_the_anchor():
    """alpha-engine-config-I13240 (d1cdb30f): the yfinance window counts back
    from today, so a fixed 8 rows cannot reach a replay anchor ~2 years old."""
    from datetime import datetime, timedelta, timezone

    mod = _yf_with_dates([("2024-10-29 16:30", 1.85)])
    old = (datetime.now(timezone.utc).date() - timedelta(days=730)).isoformat()
    _days(mod, run_date=old)
    limit = mod.Ticker.return_value.get_earnings_dates.call_args.kwargs["limit"]
    assert limit >= 8 + 730 // 91


def test_a_run_for_today_keeps_the_base_window():
    from datetime import datetime, timezone

    mod = _yf_with_dates([("2024-10-29 16:30", 1.85)])
    _days(mod, run_date=datetime.now(timezone.utc).date().isoformat())
    assert mod.Ticker.return_value.get_earnings_dates.call_args.kwargs["limit"] == 8
