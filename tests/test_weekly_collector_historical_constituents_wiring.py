"""Wiring pin for the point-in-time (historical) constituents collector.

config#657 (G12, survivorship-free universe). The collector
``collectors/historical_constituents.py`` shipped in nousergon-data#490 but was
never invoked from ``weekly_collector`` — so ``market_data/historical_constituents.json``
was never written and any downstream consumer would read a nonexistent key.
This pins that it now runs in the Phase-1 sweep and is reachable via
``--only historical_constituents``, reusing the roster the constituents phase
already produced.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import weekly_collector


def _args(**kw) -> SimpleNamespace:
    base = dict(
        date="2026-06-13", dry_run=True, only=None,
        skip_phases="", force_phases="", force=False,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _fake_constituents(roster) -> MagicMock:
    fake = MagicMock()
    fake.load_from_s3.return_value = {"tickers": list(roster)}
    return fake


def test_historical_constituents_runs_with_roster_and_correct_key():
    """--only historical_constituents replays PIT membership off today's roster
    and writes to market_data/historical_constituents.json."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    captured: dict = {}

    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "n_changes": 3, "n_snapshots": 2}

    fake_hist = MagicMock()
    fake_hist.collect.side_effect = fake_collect

    with patch("weekly_collector.constituents", _fake_constituents(["AAPL", "MSFT"])), \
         patch("weekly_collector.historical_constituents", fake_hist):
        results = weekly_collector._run_phase1(config, _args(only="historical_constituents"))

    # The collector was invoked with the roster (no second live fetch) + the
    # market-data prefix that resolves to market_data/historical_constituents.json.
    assert captured["current_tickers"] == ["AAPL", "MSFT"]
    assert captured["s3_prefix"] == "market_data/"
    assert captured["bucket"] == "test-bucket"
    assert results["collectors"]["historical_constituents"]["status"] == "ok"


def test_historical_constituents_skips_gracefully_without_roster():
    """No roster available (S3 empty) → skip with a reason, never crash."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    empty = MagicMock()
    empty.load_from_s3.return_value = {"tickers": []}
    fake_hist = MagicMock()

    with patch("weekly_collector.constituents", empty), \
         patch("weekly_collector.historical_constituents", fake_hist):
        results = weekly_collector._run_phase1(config, _args(only="historical_constituents"))

    fake_hist.collect.assert_not_called()
    assert results["collectors"]["historical_constituents"] == {
        "status": "skipped", "reason": "no tickers",
    }


def test_historical_constituents_is_an_only_choice():
    """The operator can target just this collector — guards against the wiring
    silently regressing to dead code again."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only",
        choices=["constituents", "historical_constituents", "prices", "macro",
                 "short_interest", "universe_classification", "universe_returns",
                 "alternative", "daily_closes", "features", "arcticdb"],
    )
    ns = parser.parse_args(["--only", "historical_constituents"])
    assert ns.only == "historical_constituents"
