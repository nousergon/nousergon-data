"""Regression tests for the FRED-fetcher per-date semantic.

Pins the fix for the 2026-05-12 FlowDoctor `polygon_only OVERWRITE VIX`
alerts: the pre-fix ``_fetch_fred_closes`` queried FRED with
``sort_order=desc, limit=5`` and no upper-bound — it always returned the
single most-recent non-missing observation across all of FRED history.
Combined with the windowed-reconciliation arc (PRs #199/#200/#201 +
alpha-engine-config cutover to ``daily_closes: { window_days: 14,
skip_if_canonical: true }`` 2026-05-10) that meant every historical date
in the rolling 14-BDay window got today's latest FRED VIX/VIX3M/TNX/IRX/
TWO/HYOAS/BAA10Y stamped on it, clobbering correct historical values.

Lock the corrected behavior:

  - per-date calls send ``observation_end=date_str``
  - the returned observation date must be ≤ ``date_str``
  - the FRED value, not today's "latest", lands in the per-date record
  - same-day call (FRED's T-1 publishing lag) still yields the prior
    business day's value — preserves the legacy "today's parquet carries
    yesterday's FRED close" semantic
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors import daily_closes


def _fred_response(observations: list[dict]) -> MagicMock:
    """Build a mock ``requests.get`` response with the given observations."""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"observations": observations}
    return resp


@pytest.fixture
def fred_api_key(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "test-key-xyz")


# ── per-date semantic ────────────────────────────────────────────────────────


def test_fred_request_pins_observation_end_to_run_date(fred_api_key):
    """``observation_end`` must equal the per-date ``date_str`` — bounds the
    lookup so a historical date in the windowed-reconciliation loop returns
    that date's FRED observation, not today's latest."""
    captured_params: list[dict] = []

    def fake_get(url, params=None, timeout=None):
        captured_params.append(params)
        return _fred_response([
            {"date": "2026-04-22", "value": "18.36"},
        ])

    records: list[dict] = []
    with patch("collectors.daily_closes.requests.get", side_effect=fake_get):
        daily_closes._fetch_fred_closes(
            tickers=["VIX"],
            date_str="2026-04-22",
            records=records,
        )

    assert len(captured_params) == 1
    assert captured_params[0]["observation_end"] == "2026-04-22"
    assert captured_params[0]["series_id"] == "VIXCLS"
    assert len(records) == 1
    assert records[0]["ticker"] == "VIX"
    assert records[0]["date"] == "2026-04-22"
    assert records[0]["Close"] == 18.36


def test_fred_writes_per_date_observation_not_latest(fred_api_key):
    """For a historical date, the per-date FRED value lands in the record,
    not the most-recent-ever observation. This is the direct regression
    pin: the bug stamped 17.19 (today's value) onto 2026-04-22's parquet."""
    historical_obs = {"date": "2026-04-22", "value": "18.36"}

    def fake_get(url, params=None, timeout=None):
        # FRED with ``observation_end=2026-04-22, sort_order=desc, limit=5``
        # returns observations on or before 2026-04-22 in desc order.
        return _fred_response([historical_obs])

    records: list[dict] = []
    with patch("collectors.daily_closes.requests.get", side_effect=fake_get):
        daily_closes._fetch_fred_closes(
            tickers=["VIX"],
            date_str="2026-04-22",
            records=records,
        )

    assert records[0]["Close"] == 18.36  # not 17.19 (today's latest)


def test_fred_per_date_calls_return_distinct_values(fred_api_key):
    """Iterating the windowed reconciliation over multiple dates must
    yield distinct per-date FRED values — not the same "latest" pasted
    onto every date. The bug signature was two different trading days
    with identical Close to the cent in the alert payload."""
    per_date_values = {
        "2026-04-22": "18.36",
        "2026-04-28": "19.50",
        "2026-05-11": "17.19",
    }

    def fake_get(url, params=None, timeout=None):
        end = params["observation_end"]
        return _fred_response([{"date": end, "value": per_date_values[end]}])

    records: list[dict] = []
    with patch("collectors.daily_closes.requests.get", side_effect=fake_get):
        for d in per_date_values:
            daily_closes._fetch_fred_closes(["VIX"], d, records)

    closes = {r["date"]: r["Close"] for r in records}
    assert closes == {"2026-04-22": 18.36, "2026-04-28": 19.50, "2026-05-11": 17.19}


# ── same-day legacy semantic preserved ───────────────────────────────────────


def test_fred_same_day_returns_prior_business_day_when_no_today_obs(fred_api_key):
    """FRED publishes T-1 — when MorningEnrich runs at 6 AM PT for today's
    date, FRED has no observation for today yet. The function should
    fall back to the most recent on-or-before value (typically T-1),
    preserving the legacy "today's parquet carries yesterday's FRED
    value" semantic."""
    def fake_get(url, params=None, timeout=None):
        # observation_end=2026-05-12 returns 2026-05-11's value because
        # today's hasn't been published yet.
        return _fred_response([{"date": "2026-05-11", "value": "17.19"}])

    records: list[dict] = []
    with patch("collectors.daily_closes.requests.get", side_effect=fake_get):
        daily_closes._fetch_fred_closes(
            tickers=["VIX"],
            date_str="2026-05-12",
            records=records,
        )

    assert len(records) == 1
    assert records[0]["Close"] == 17.19
    # Record carries the parquet-key date (today), not the FRED-observation
    # date. Matches the existing single-date contract.
    assert records[0]["date"] == "2026-05-12"


# ── defensive guard ──────────────────────────────────────────────────────────


def test_fred_refuses_future_dated_observation(fred_api_key, caplog):
    """If FRED somehow returns an observation date AFTER date_str (API
    behavior change, server bug, etc.), refuse to write rather than
    silently stamp a future value. Belt-and-suspenders for the original
    bug class."""
    def fake_get(url, params=None, timeout=None):
        # date > date_str — should be filtered out.
        return _fred_response([{"date": "2026-05-11", "value": "17.19"}])

    records: list[dict] = []
    import logging
    with caplog.at_level(logging.ERROR, logger="collectors.daily_closes"):
        with patch("collectors.daily_closes.requests.get", side_effect=fake_get):
            count = daily_closes._fetch_fred_closes(
                tickers=["VIX"],
                date_str="2026-04-22",
                records=records,
            )

    assert count == 0
    assert records == []
    assert any("> requested 2026-04-22" in rec.message for rec in caplog.records)


# ── missing-data path ────────────────────────────────────────────────────────


def test_fred_skips_when_no_observation_on_or_before_date(fred_api_key):
    """If FRED has no non-missing observation on or before date_str,
    log a warning and skip — don't fabricate a value, and don't fall
    forward to a later observation (that would re-introduce the bug)."""
    def fake_get(url, params=None, timeout=None):
        # Empty observations payload — FRED has nothing in window.
        return _fred_response([])

    records: list[dict] = []
    with patch("collectors.daily_closes.requests.get", side_effect=fake_get):
        count = daily_closes._fetch_fred_closes(
            tickers=["VIX"],
            date_str="1990-01-02",
            records=records,
        )

    assert count == 0
    assert records == []


def test_fred_skips_missing_value_marker(fred_api_key):
    """FRED publishes ``"."`` for missing observations — those must be
    skipped rather than parsed as numeric."""
    def fake_get(url, params=None, timeout=None):
        return _fred_response([
            {"date": "2026-04-22", "value": "."},
            {"date": "2026-04-21", "value": "18.30"},
        ])

    records: list[dict] = []
    with patch("collectors.daily_closes.requests.get", side_effect=fake_get):
        daily_closes._fetch_fred_closes(["VIX"], "2026-04-22", records)

    # Skipped the missing-marker obs, took the next-most-recent.
    assert len(records) == 1
    assert records[0]["Close"] == 18.30
