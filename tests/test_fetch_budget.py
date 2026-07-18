"""Tests for the shared news-sweep budget derivation (config#2938).

The derivation is the single source of truth for the Polygon-bottlenecked
news fetch budgets. These pins guard the two properties the 2026-07-18 double
SIGKILL incident turned into hard requirements:

  * WEEKLY budgets scale with the LIVE universe (so universe growth can never
    silently outrun them, ruling 2) and are sized to COMPLETE the sweep at the
    current universe (ruling 1), while staying inside the 4h SSM cap; and
  * the DAILY systemd ``TimeoutStartSec`` stays in lockstep with the derived
    value (no hand-maintained constant drifting from the code).
"""

from __future__ import annotations

import re
from pathlib import Path

from collectors.news_sources.fetch_budget import (
    DAILY_NEWS_MAX_FETCH_SECONDS,
    POLYGON_SECONDS_PER_TICKER,
    WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS,
    daily_news_timeout_start_seconds,
    weekly_news_max_fetch_seconds,
)

# The universe that took down both consumers on 2026-07-18 (79 holdings ∪ 903
# AE-signals). Used as the "current universe" reference for the completion pin.
_INCIDENT_UNIVERSE = 944


class TestWeeklyBudget:
    def test_scales_with_universe(self):
        assert weekly_news_max_fetch_seconds(1000) > weekly_news_max_fetch_seconds(100)

    def test_monotonic_non_decreasing(self):
        prev = -1
        for n in (0, 1, 50, 200, 500, 944, 1200, 5000):
            cur = weekly_news_max_fetch_seconds(n)
            assert cur >= prev
            prev = cur

    def test_small_universe_hits_floor(self):
        # A single-ticker universe still gets a sane (floored) budget.
        assert weekly_news_max_fetch_seconds(1) == weekly_news_max_fetch_seconds(0)
        assert weekly_news_max_fetch_seconds(1) >= 1_800

    def test_completes_current_universe_sweep(self):
        # ruling 1: the budget must exceed the nominal full-universe Polygon
        # crawl time (universe × 12s) so a clean run COMPLETES, not bails.
        nominal_crawl = _INCIDENT_UNIVERSE * 12
        assert weekly_news_max_fetch_seconds(_INCIDENT_UNIVERSE) >= nominal_crawl

    def test_never_exceeds_step_cap(self):
        # ruling 2: always leaves reserve for the rest of the RAGIngestion
        # step inside the 4h SSM executionTimeout — even for an absurd universe.
        for n in (944, 2000, 100_000):
            assert (
                weekly_news_max_fetch_seconds(n)
                < WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS
            )

    def test_huge_universe_caps_not_unbounded(self):
        # Beyond the cap the sweep fails soft (partial), it does not demand an
        # unbounded budget — the cap is the same for 100k and 1M tickers.
        assert weekly_news_max_fetch_seconds(100_000) == weekly_news_max_fetch_seconds(
            1_000_000
        )

    def test_weekly_cap_matches_4h_policy(self):
        # The SSM executionTimeout the nousergon-data guard pins against is the
        # config#2938 4h ruling. If this changes, the three nousergon-data
        # timeouts (executionTimeout / run_ssm / MAX_RUNTIME) move in lockstep.
        assert WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS == 14_400


class TestDailyBudget:
    def test_daily_bail_budget_is_tight(self):
        assert DAILY_NEWS_MAX_FETCH_SECONDS == 1_200

    def test_timeout_start_covers_source_plus_reserve(self):
        assert daily_news_timeout_start_seconds() > DAILY_NEWS_MAX_FETCH_SECONDS

    def test_daily_news_service_timeout_in_lockstep(self):
        # The deployed systemd TimeoutStartSec MUST equal the derivation — no
        # hand-maintained constant silently drifting from the code (config#2938).
        svc = (
            Path(__file__).resolve().parent.parent
            / "infrastructure"
            / "systemd"
            / "daily-news.service"
        )
        m = re.search(r"^TimeoutStartSec=(\d+)", svc.read_text(), re.MULTILINE)
        assert m, "daily-news.service has no TimeoutStartSec"
        assert int(m.group(1)) == daily_news_timeout_start_seconds()


def test_per_ticker_rate_reflects_polygon_free_tier():
    # 5 req/min = 12s/ticker; the derivation must not silently assume a faster
    # (paid-tier) rate that would under-size every budget.
    assert POLYGON_SECONDS_PER_TICKER >= 12.0
