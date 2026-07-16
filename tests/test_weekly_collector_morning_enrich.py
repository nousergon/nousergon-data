"""Tests for the --morning-enrich path in weekly_collector.

Covers:
  * _previous_trading_day finds the most recent trading day before today,
    walking back over weekends + holidays correctly.
  * _run_morning_enrich invokes daily_closes with source='polygon_only'
    (no yfinance fallback masking polygon failures) and follows up with
    daily_append on the same date.
  * Hard-fail propagation when polygon raises PolygonForbiddenError.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

import weekly_collector
from polygon_client import PolygonForbiddenError

_PT = ZoneInfo("America/Los_Angeles")


# ── _previous_trading_day ───────────────────────────────────────────────────


def test_previous_trading_day_walks_back_over_weekend():
    """Monday morning should resolve to Friday's date, not Sunday's."""
    # 2026-04-27 is a Monday. Previous trading day = 2026-04-24 (Friday).
    monday = datetime(2026, 4, 27, 13, 0, 0, tzinfo=timezone.utc)
    result = weekly_collector._previous_trading_day(reference=monday)
    assert result == "2026-04-24"


def test_previous_trading_day_skips_holiday():
    """Day after a market holiday should resolve to the trading day before it."""
    # 2026-12-25 is Christmas (NYSE closed). 2026-12-28 (Mon) → 2026-12-24 (Thu).
    day_after = datetime(2026, 12, 28, 13, 0, 0, tzinfo=timezone.utc)
    result = weekly_collector._previous_trading_day(reference=day_after)
    assert result == "2026-12-24"


def test_previous_trading_day_strict_inequality():
    """Always returns a date STRICTLY before the reference, never the same day."""
    # Even if today is a trading day, --morning-enrich is for prior session enrichment.
    # 2026-04-23 is a Thursday (trading day). Result should be 2026-04-22 (Wednesday).
    thursday = datetime(2026, 4, 23, 13, 0, 0, tzinfo=timezone.utc)
    result = weekly_collector._previous_trading_day(reference=thursday)
    assert result == "2026-04-22"


def test_previous_trading_day_raises_on_runaway():
    """Defensive: if is_trading_day returns False for 10 days straight, raise."""
    with patch("nousergon_lib.trading_calendar.is_trading_day", return_value=False):
        with pytest.raises(RuntimeError, match="trading_calendar.is_trading_day appears broken"):
            weekly_collector._previous_trading_day(
                reference=datetime(2026, 4, 23, 13, 0, 0, tzinfo=timezone.utc)
            )


# ── _run_morning_enrich orchestration ───────────────────────────────────────


@pytest.fixture
def enrich_args():
    return SimpleNamespace(date=None, dry_run=True, morning_enrich=True)


@pytest.fixture
def enrich_args_with_date():
    return SimpleNamespace(date="2026-04-22", dry_run=True, morning_enrich=True)


def test_morning_enrich_uses_polygon_only_source(enrich_args_with_date):
    """The morning enrichment must call daily_closes with source='polygon_only'."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}

    captured = {}
    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok_dry_run", "polygon": 100, "fred": 4, "yfinance": 0,
                "tickers_captured": 104, "source": kwargs["source"]}

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL", "MSFT", "NVDA"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, enrich_args_with_date)

    assert captured["source"] == "polygon_only"
    assert captured["run_date"] == "2026-04-22"
    assert result["status"] == "ok"
    assert result["mode"] == "morning_enrich"
    assert result["date"] == "2026-04-22"


def test_morning_enrich_hard_fails_on_polygon_forbidden(enrich_args_with_date):
    """If polygon raises PolygonForbiddenError, the enrich step must report failed."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch(
             "weekly_collector.daily_closes.collect",
             side_effect=PolygonForbiddenError("403 simulation"),
         ):
        result = weekly_collector._run_morning_enrich(config, enrich_args_with_date)

    assert result["status"] == "failed"
    assert result["collectors"]["daily_closes"]["status"] == "error"
    assert "403" in result["collectors"]["daily_closes"]["error"]
    # daily_append should NOT have run after polygon failed
    assert "arcticdb" not in result["collectors"]


def test_morning_enrich_calls_daily_append_after_polygon_succeeds(enrich_args_with_date):
    """daily_append must run after polygon-only daily_closes lands the parquet."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    daily_append_calls = []
    def fake_daily_append(**kwargs):
        daily_append_calls.append(kwargs)
        return {"status": "ok", "tickers_appended": 1}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0, "yfinance": 0,
                             "tickers_captured": 1, "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append", side_effect=fake_daily_append):
        result = weekly_collector._run_morning_enrich(config, enrich_args_with_date)

    assert result["status"] == "ok"
    assert len(daily_append_calls) == 1
    assert daily_append_calls[0]["date_str"] == "2026-04-22"


def test_morning_enrich_default_date_uses_previous_trading_day():
    """When --date is not specified, _previous_trading_day fills in."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date=None, dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    captured = {}
    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok_dry_run", "polygon": 1, "fred": 0, "yfinance": 0,
                "tickers_captured": 1, "source": "polygon_only"}

    # Pin the skip guard to OFF so this test exercises only the
    # _previous_trading_day fill-in. Without this the test would depend on
    # ArcticDB live state (the staleness check reads SPY's last_date).
    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._arctic_spy_last_date", return_value=None), \
         patch("weekly_collector._should_skip_morning_enrich",
               return_value=(False, None)), \
         patch("weekly_collector._previous_trading_day", return_value="2026-04-23"):
        result = weekly_collector._run_morning_enrich(config, args)

    assert captured["run_date"] == "2026-04-23"
    assert result["date"] == "2026-04-23"


# ── daily_data health stamp refresh ─────────────────────────────────────────


def test_morning_enrich_refreshes_daily_data_stamp_on_success():
    """On success, _run_morning_enrich must call _write_module_health to refresh
    the `daily_data` stamp. Without this the executor's 26h staleness gate trips
    on Monday mornings (post-close stamp from Friday afternoon → ~65h on Monday
    open). Regression: 2026-04-27 weekday SF aborted on this exact gap."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    # Pre-MorningEnrich preflight: refresh constituents in-process.
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL"], "date": "2026-04-24",
    }

    health_calls = []
    def fake_write_health(bucket, module_name, run_date, status, **kwargs):
        health_calls.append({
            "bucket": bucket, "module_name": module_name,
            "run_date": run_date, "status": status, **kwargs,
        })

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok", "polygon": 913, "fred": 4,
                             "yfinance": 0, "tickers_captured": 917,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health", side_effect=fake_write_health):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "ok"
    assert len(health_calls) == 1, "expected exactly one daily_data stamp write"
    stamp = health_calls[0]
    assert stamp["module_name"] == "daily_data"
    assert stamp["run_date"] == "2026-04-24"
    assert stamp["status"] == "ok"
    assert stamp["summary"]["morning_enrich"] is True
    assert stamp["summary"]["polygon"] == 913


def test_morning_enrich_does_not_stamp_on_polygon_failure():
    """If polygon fails the prior stamp must be left in place — executor's
    staleness gate then fires correctly. Writing a fresh "ok" stamp on failure
    would mask the outage."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL"], "date": "2026-04-24",
    }

    health_calls = []
    def fake_write_health(*args_, **kwargs):
        health_calls.append(kwargs)

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
         patch("weekly_collector.daily_closes.collect",
               side_effect=PolygonForbiddenError("403 simulation")), \
         patch("weekly_collector._write_module_health", side_effect=fake_write_health):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "failed"
    assert health_calls == [], (
        "morning_enrich must NOT refresh daily_data stamp on failure — would "
        "mask outages from the executor's staleness gate"
    )


def test_morning_enrich_does_not_stamp_in_dry_run():
    """Dry runs (CLI --dry-run, backfill rehearsals) must not touch S3 stamps."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    health_calls = []
    def fake_write_health(*args_, **kwargs):
        health_calls.append(kwargs)

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health", side_effect=fake_write_health):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "ok"
    assert health_calls == []


# ── --daily routes through yfinance_only ────────────────────────────────────


def test_daily_mode_calls_collect_with_yfinance_only_source():
    """--daily must invoke daily_closes with source='yfinance_only' (no polygon attempt)."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(
        date="2026-04-23", dry_run=True, morning_enrich=False,
        daily=True, only=None,
    )

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    captured = {}
    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok_dry_run", "polygon": 0, "fred": 4, "yfinance": 1,
                "tickers_captured": 5, "source": kwargs["source"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("features.compute.compute_and_write",
               return_value={"status": "ok"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}):
        weekly_collector._run_daily(config, args)

    assert captured["source"] == "yfinance_only", (
        "--daily must use source='yfinance_only' to skip polygon entirely "
        "(per the 2026-04-23 split-by-source design — polygon free-tier 403's "
        "same-day, morning enrichment fills VWAP overnight)."
    )


# ── Preflight: refresh-constituents + prune-stragglers ─────────────────────────


def test_morning_enrich_refreshes_constituents_before_collect():
    """Pre-flight architectural fix (2026-05-02 incident): MorningEnrich must
    call constituents.collect() in-process BEFORE the daily_closes call so
    polygon is asked about the freshest S&P membership, not last week's. The
    bandage scoping in PR #132/#133 then becomes a quiet no-op rather than
    the load-bearing path."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.return_value = {
        "status": "ok",
        "tickers": ["AAPL", "MSFT", "NVDA"],  # fresh, post-churn list
        "date": "2026-04-24",
    }

    captured_dc = {}
    def fake_dc_collect(**kwargs):
        captured_dc.update(kwargs)
        return {"status": "ok", "polygon": 3, "fred": 0, "yfinance": 0,
                "tickers_captured": 3, "source": "polygon_only"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_dc_collect), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health"):
        weekly_collector._run_morning_enrich(config, args)

    fake_constituents.collect.assert_called_once()
    # daily_closes must use the fresh tickers + macro additions, NOT
    # load_from_s3's stale snapshot.
    sent_tickers = captured_dc["tickers"]
    assert "AAPL" in sent_tickers and "MSFT" in sent_tickers and "NVDA" in sent_tickers
    fake_constituents.load_from_s3.assert_not_called()


def test_morning_enrich_prunes_stragglers_before_daily_append():
    """Prune must run BEFORE daily_closes/daily_append so the missing-from-
    closes + freshness checks see a coherent universe. Use the in-process
    constituents_override (not the public latest_weekly.json pointer) so
    cross-module readers don't see a half-updated pointer mid-SF."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL", "MSFT"], "date": "2026-04-24",
    }

    prune_calls = []
    def fake_prune(**kwargs):
        prune_calls.append(kwargs)
        return {"status": "ok", "pruned_count": 0, "skipped_recent_count": 0}

    da_calls = []
    def fake_daily_append(**kwargs):
        # By the time daily_append fires, prune must already have run.
        assert prune_calls, "prune must run before daily_append"
        da_calls.append(kwargs)
        return {"status": "ok"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               side_effect=fake_prune), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok", "polygon": 2, "fred": 0,
                             "yfinance": 0, "tickers_captured": 2,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append", side_effect=fake_daily_append), \
         patch("weekly_collector._write_module_health"):
        weekly_collector._run_morning_enrich(config, args)

    assert len(prune_calls) == 1
    assert prune_calls[0]["apply"] is True
    assert prune_calls[0]["absent_days"] == 5  # tighter than the 14d default
    assert prune_calls[0]["constituents_override"] == {"AAPL", "MSFT"}
    assert len(da_calls) == 1


def test_morning_enrich_aborts_if_constituents_refresh_fails():
    """Constituents refresh is the source of truth for prune + daily_closes
    request list. If it fails, we cannot proceed safely — Wikipedia outages,
    schema drift, or sector-mapping completeness failures all warrant a
    hard-fail per feedback_no_silent_fails."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.side_effect = RuntimeError("Wikipedia 503")

    dc_calls = []
    def fake_dc(**kwargs):
        dc_calls.append(kwargs)
        return {"status": "ok"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers"), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_dc), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health"):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "failed"
    assert result["collectors"]["constituents_preflight"]["status"] == "error"
    assert "Wikipedia 503" in result["collectors"]["constituents_preflight"]["error"]
    assert dc_calls == [], "daily_closes must NOT run if constituents refresh failed"


def test_morning_enrich_continues_if_prune_fails():
    """Prune is best-effort here — daily_append's expected_tickers scoping
    (PR #132/#133) still tolerates stragglers as a fallback. A prune failure
    must surface loudly (ERROR log + result entry) but must NOT block the
    rest of the enrich pipeline tonight."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL"], "date": "2026-04-24",
    }

    da_called = []

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               side_effect=RuntimeError("ArcticDB transient")), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: (da_called.append(k), {"status": "ok"})[1]), \
         patch("weekly_collector._write_module_health"):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["prune_preflight_warning"]["status"] == "error"
    assert "ArcticDB transient" in result["prune_preflight_warning"]["error"]
    assert "prune_preflight" not in result["collectors"], (
        "prune failure must NOT land in results['collectors'] — that key feeds "
        "the status aggregator and would make the whole MorningEnrich fail"
    )
    assert len(da_called) == 1, "daily_append must still run when prune fails"
    assert result["status"] == "ok"


# ── _should_skip_morning_enrich (data-staleness skip guard) ────────────────


def test_skip_guard_fires_when_polygon_target_older_than_arctic():
    """Wed-PM rerun: polygon target=Tue, ArcticDB last=Wed (yfinance EOD).

    Polygon's T+1 settled day is older than what's already in ArcticDB.
    Running polygon would overwrite Wed's authoritative yfinance EOD row
    with a Tue write — wrong row anyway, but more importantly it would not
    refresh today's data. Skip.
    """
    from datetime import date
    skip, reason = weekly_collector._should_skip_morning_enrich(
        target_date="2026-04-21",  # Tue
        arctic_last_date=date(2026, 4, 22),  # Wed
    )
    assert skip is True
    assert reason is not None
    assert "stale_overwrite" in reason
    assert "2026-04-21" in reason
    assert "2026-04-22" in reason


def test_skip_guard_does_not_fire_when_target_equals_arctic_last():
    """Saturday cron: polygon target=Fri, ArcticDB last=Fri (yfinance EOD).

    This is the canonical case the morning enrich exists to handle:
    overwrite the yfinance EOD row with polygon's authoritative VWAP/OHLCV.
    Equal dates → run polygon.
    """
    from datetime import date
    skip, reason = weekly_collector._should_skip_morning_enrich(
        target_date="2026-04-24",  # Fri
        arctic_last_date=date(2026, 4, 24),  # Fri
    )
    assert skip is False
    assert reason is None


def test_skip_guard_does_not_fire_when_target_newer_than_arctic():
    """ArcticDB lags target (e.g., a fresh universe with no recent rows).
    polygon should run to extend history."""
    from datetime import date
    skip, reason = weekly_collector._should_skip_morning_enrich(
        target_date="2026-04-24",
        arctic_last_date=date(2026, 4, 17),
    )
    assert skip is False
    assert reason is None


def test_skip_guard_falls_through_when_arctic_read_unavailable():
    """If we couldn't read ArcticDB SPY last_date, fall through to running
    polygon. Polygon will surface its own 403/availability failures loudly.
    Better to fail loudly downstream than silently skip without evidence."""
    skip, reason = weekly_collector._should_skip_morning_enrich(
        target_date="2026-04-24",
        arctic_last_date=None,
    )
    assert skip is False
    assert reason is None


# ── _run_morning_enrich integration with the skip guard ────────────────────


def test_morning_enrich_short_circuits_when_skip_guard_fires():
    """When the guard says skip, _run_morning_enrich must return
    status='skipped' WITHOUT calling polygon, daily_append, or any
    side-effecting collector. The yfinance row already in ArcticDB stays
    authoritative for this run."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date=None, dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    polygon_calls = []
    daily_append_calls = []
    health_calls = []

    with patch(
        "weekly_collector._should_skip_morning_enrich",
        return_value=(True, "stale_overwrite (test)"),
    ), patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector._arctic_spy_last_date", return_value=None), \
         patch("weekly_collector.daily_closes.collect",
               side_effect=lambda **k: polygon_calls.append(k) or {"status": "ok"}), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: daily_append_calls.append(k) or {"status": "ok"}), \
         patch("weekly_collector._write_module_health",
               side_effect=lambda *a, **k: health_calls.append(k)):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "skipped"
    assert "stale_overwrite" in result["skip_reason"]
    assert result["would_have_targeted"]  # _previous_trading_day() filled it in
    assert polygon_calls == [], "polygon must not be called when guard skips"
    assert daily_append_calls == [], "daily_append must not be called when guard skips"
    assert health_calls == [], "health stamp must not refresh when guard skips"
    fake_constituents.collect.assert_not_called()


def test_morning_enrich_explicit_date_overrides_skip_guard():
    """Explicit --date is operator-driven backfill — must run polygon even
    when the staleness guard would otherwise fire. Operator knows what they're
    doing (e.g., backfilling a date polygon T+1 has long since settled)."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-22", dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    polygon_calls = []
    def fake_collect(**kwargs):
        polygon_calls.append(kwargs)
        return {"status": "ok_dry_run", "polygon": 1, "fred": 0, "yfinance": 0,
                "tickers_captured": 1, "source": "polygon_only"}

    # Force the guard to be willing to fire — verifies that --date bypasses it.
    with patch(
        "weekly_collector._should_skip_morning_enrich",
        return_value=(True, "would-have-skipped"),
    ), patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append", return_value={"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "ok"
    assert result["date"] == "2026-04-22"
    assert len(polygon_calls) == 1, (
        "explicit --date must bypass the skip guard and call polygon"
    )


def test_morning_enrich_skips_when_arctic_already_has_newer_row():
    """End-to-end: Wed-PM manual rerun. _previous_trading_day(PT-aware) yields
    Tue, ArcticDB SPY already has Wed (from Wed EOD yfinance). Skip fires with
    stale_overwrite reason; no polygon, no daily_append, no health stamp."""
    from datetime import date
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date=None, dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    polygon_calls = []
    daily_append_calls = []

    with patch("weekly_collector._previous_trading_day", return_value="2026-04-21"), \
         patch("weekly_collector._arctic_spy_last_date",
               return_value=date(2026, 4, 22)), \
         patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               side_effect=lambda **k: polygon_calls.append(k) or {"status": "ok"}), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: daily_append_calls.append(k) or {"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "skipped"
    assert "stale_overwrite" in result["skip_reason"]
    assert "2026-04-21" in result["skip_reason"]
    assert "2026-04-22" in result["skip_reason"]
    assert result["would_have_targeted"] == "2026-04-21"
    assert polygon_calls == []
    assert daily_append_calls == []
    fake_constituents.collect.assert_not_called()


def test_morning_enrich_runs_when_arctic_matches_target_date():
    """End-to-end: Saturday cron path. target=Fri, arctic=Fri (yfinance EOD).
    Equal dates → run polygon to overwrite with authoritative VWAP/OHLCV."""
    from datetime import date
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date=None, dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    polygon_calls = []
    def fake_collect(**kwargs):
        polygon_calls.append(kwargs)
        return {"status": "ok_dry_run", "polygon": 1, "fred": 0, "yfinance": 0,
                "tickers_captured": 1, "source": "polygon_only"}

    with patch("weekly_collector._previous_trading_day", return_value="2026-04-24"), \
         patch("weekly_collector._arctic_spy_last_date",
               return_value=date(2026, 4, 24)), \
         patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append", return_value={"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "ok"
    assert result["date"] == "2026-04-24"
    assert len(polygon_calls) == 1
    assert polygon_calls[0]["run_date"] == "2026-04-24"


def test_morning_enrich_dry_run_skips_preflight_writes():
    """Dry runs must not refresh constituents.json or prune ArcticDB —
    side-effect-free is the contract."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    # collect MUST NOT be called in dry-run.

    prune_called = []

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               side_effect=lambda **k: (prune_called.append(k), {})[1]), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._arctic_spy_last_date", return_value=None):
        weekly_collector._run_morning_enrich(config, args)

    fake_constituents.collect.assert_not_called()
    assert prune_called == []


# ── chronic-gap heal split (2026-06-11) ─────────────────────────────────────


def test_chronic_gap_heal_routes_via_run_weekly():
    """run_weekly must dispatch --chronic-gap-heal to _run_chronic_gap_heal."""
    args = SimpleNamespace(
        morning_enrich=False, chronic_gap_heal=True, daily=False,
    )
    with patch("weekly_collector._run_chronic_gap_heal",
               return_value={"status": "ok", "mode": "chronic_gap_heal"}) as heal:
        out = weekly_collector.run_weekly({"bucket": "b"}, args)
    heal.assert_called_once()
    assert out["mode"] == "chronic_gap_heal"


def test_morning_enrich_skip_chronic_heal_does_not_run_inline_heal():
    """--skip-chronic-heal (the weekday SF path) suppresses the inline heal —
    the separate ChronicGapSelfHeal SF state runs it instead."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(
        date="2026-04-22", dry_run=True, morning_enrich=True,
        skip_chronic_heal=True,
    )
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append", return_value={"status": "ok"}), \
         patch("weekly_collector._run_chronic_gap_heal") as heal:
        result = weekly_collector._run_morning_enrich(config, args)

    heal.assert_not_called()
    assert result["status"] == "ok"


def test_morning_enrich_runs_inline_heal_without_skip_flag():
    """Without --skip-chronic-heal (the Saturday SF path), the inline heal
    still runs before DataPhase1's postflight."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(
        date="2026-04-22", dry_run=True, morning_enrich=True,
        skip_chronic_heal=False,
    )
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append", return_value={"status": "ok"}), \
         patch("weekly_collector._run_chronic_gap_heal",
               return_value={"status": "ok", "collectors": {}}) as heal:
        weekly_collector._run_morning_enrich(config, args)

    heal.assert_called_once()


def test_run_chronic_gap_heal_never_raises_on_load_failure():
    """The standalone heal must return status=ok (best-effort) even when an
    inner call raises — it must exit 0 so the SF state is non-fatal."""
    config = {"bucket": "test-bucket"}
    args = SimpleNamespace(date="2026-04-22", dry_run=True)
    with patch("weekly_collector._load_chronic_polygon_gaps",
               side_effect=RuntimeError("boom")):
        out = weekly_collector._run_chronic_gap_heal(config, args)
    assert out["status"] == "ok"
    assert out["mode"] == "chronic_gap_heal"
    assert out["collectors"]["chronic_gap_heal_wrapper"]["status"] == "error"


# ── arctic-append split (L4608) ─────────────────────────────────────────────


def test_arctic_append_routes_via_run_weekly():
    """run_weekly must dispatch --morning-arctic-append to _run_morning_arctic_append."""
    args = SimpleNamespace(
        morning_enrich=False, morning_arctic_append=True, chronic_gap_heal=False, daily=False,
    )
    with patch("weekly_collector._run_morning_arctic_append",
               return_value={"status": "ok", "mode": "morning_arctic_append"}) as ap:
        out = weekly_collector.run_weekly({"bucket": "b"}, args)
    ap.assert_called_once()
    assert out["mode"] == "morning_arctic_append"


def test_morning_enrich_skip_arctic_append_does_not_append():
    """--skip-arctic-append (weekday SF) suppresses the inline daily_append —
    the separate MorningArcticAppend SF state runs it instead."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(
        date="2026-04-22", dry_run=True, morning_enrich=True,
        skip_chronic_heal=True, skip_arctic_append=True,
    )
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    append_calls = []
    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1, "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: append_calls.append(k) or {"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, args)
    assert append_calls == [], "daily_append must not run when --skip-arctic-append"
    assert "arcticdb" not in result["collectors"]


def test_arctic_append_runs_daily_append_and_is_load_bearing():
    """_run_morning_arctic_append runs daily_append for the target date and
    returns failed (load-bearing) when the append errors."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-22", dry_run=False)

    def _fake_loader(s3, bucket, run_date=None):
        return ({"AAPL", "MSFT"}, run_date or "2026-04-22")

    # happy path. The prior-gap self-heal is a separate collaborator with its
    # own test file (test_universe_gap_self_heal.py); no-op it here so this
    # test isolates the same-day append behavior it asserts on.
    _noop_heal = {"missing_days": [], "healed_days": [], "deferred_days": [], "errors": []}
    calls = []
    with patch("weekly_collector.boto3.client", return_value=MagicMock()), \
         patch("weekly_collector._self_heal_missing_universe_days", return_value=_noop_heal), \
         patch("builders._constituents_loader.load_constituents_for_run_date",
               side_effect=_fake_loader), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: calls.append(k) or {"status": "ok"}):
        ok = weekly_collector._run_morning_arctic_append(config, args)
    assert ok["status"] == "ok" and ok["mode"] == "morning_arctic_append"
    assert calls and calls[0]["date_str"] == "2026-04-22"

    # load-bearing: append raises → failed
    with patch("weekly_collector.boto3.client", return_value=MagicMock()), \
         patch("weekly_collector._self_heal_missing_universe_days", return_value=_noop_heal), \
         patch("builders._constituents_loader.load_constituents_for_run_date",
               side_effect=_fake_loader), \
         patch("builders.daily_append.daily_append", side_effect=RuntimeError("boom")):
        bad = weekly_collector._run_morning_arctic_append(config, args)
    assert bad["status"] == "failed"


def test_arctic_append_reads_constituents_by_run_date_not_pointer():
    """REGRESSION (2026-06-25 weekday-SF halt): _run_morning_arctic_append must
    load constituents via the run_date-DIRECT read, NOT the latest_weekly.json
    pointer.

    The pointer only advances on the weekly Saturday _write_manifest; daily
    MorningEnrich writes the fresh dated weekly/{run_date}/constituents.json
    but leaves the pointer stale. On an S&P-reconstitution week the pointer's
    prior universe still lists the churn-out tickers — which are absent from
    today's daily_closes — so daily_append's missing-from-closes guard counts
    them as a data gap and halts the SF. Reading by run_date passes the FRESH
    universe to expected_tickers, where the straggler-exclusion correctly
    drops the churn-out tickers.
    """
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    # No --date → run_date resolves to the trading-day-axis default (config#1014:
    # the date MorningEnrich wrote constituents under, mirroring
    # ``run_date = args.date or default_run_date()`` in _run_morning_enrich), and
    # target_date to the prior trading day. The two differ; the append must read
    # by the RUN date.
    args = SimpleNamespace(date=None, dry_run=False)

    seen = {}

    def _fake_loader(s3, bucket, run_date=None):
        seen["run_date"] = run_date
        # Fresh universe — churn-out tickers already removed.
        return ({"AAPL", "MSFT", "NVDA"}, run_date)

    captured = []
    # No-op the prior-gap self-heal (own test file) so the loader/append calls
    # asserted below are exactly today's append, not the heal's per-day reads.
    _noop_heal = {"missing_days": [], "healed_days": [], "deferred_days": [], "errors": []}
    with patch("weekly_collector.boto3.client", return_value=MagicMock()), \
         patch("weekly_collector._self_heal_missing_universe_days", return_value=_noop_heal), \
         patch("builders._constituents_loader.load_constituents_for_run_date",
               side_effect=_fake_loader), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: captured.append(k) or {"status": "ok"}):
        out = weekly_collector._run_morning_arctic_append(config, args)

    assert out["status"] == "ok"
    # The loader was called with an explicit run_date (direct read), NOT None
    # (which would be the stale-pointer fallback path).
    assert seen.get("run_date") is not None, (
        "must read constituents by run_date direct, not the latest_weekly pointer"
    )
    # Run date is the trading-day-axis default (config#1014) — the same
    # chokepoint expression _run_morning_arctic_append uses — distinct from the
    # prior-trading-day target_date the append row is keyed on. Assert against
    # the chokepoint directly so the test is axis-correct and not flaky across
    # the NYSE close (a raw calendar-now string would diverge from the migrated
    # trading_day default on weekends/pre-open).
    import dates

    assert seen["run_date"] == dates.default_run_date()
    # The fresh universe is what flows into expected_tickers.
    assert captured and set(captured[0]["expected_tickers"]) == {"AAPL", "MSFT", "NVDA"}


# ── EOD daily_append split: PostMarketData + PostMarketArcticAppend (2026-06-16) ──


def test_daily_arctic_append_routes_via_run_weekly():
    """run_weekly must dispatch --daily-arctic-append to _run_daily_arctic_append."""
    args = SimpleNamespace(
        morning_enrich=False, morning_arctic_append=False,
        daily_arctic_append=True, chronic_gap_heal=False, daily=False,
    )
    with patch("weekly_collector._run_daily_arctic_append",
               return_value={"status": "ok", "mode": "daily_arctic_append"}) as ap:
        out = weekly_collector.run_weekly({"bucket": "b"}, args)
    ap.assert_called_once()
    assert out["mode"] == "daily_arctic_append"


def test_daily_skip_arctic_append_does_not_append():
    """--daily --skip-arctic-append (EOD PostMarketData) suppresses the inline
    daily_append — the separate PostMarketArcticAppend SF state runs it instead."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(
        date="2026-06-16", dry_run=True, morning_enrich=False,
        daily=True, only=None, skip_arctic_append=True,
    )
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    append_calls = []
    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 0, "fred": 4,
                             "yfinance": 1, "tickers_captured": 5, "source": "yfinance_only"}), \
         patch("features.compute.compute_and_write", return_value={"status": "ok"}), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: append_calls.append(k) or {"status": "ok"}):
        result = weekly_collector._run_daily(config, args)
    assert append_calls == [], "daily_append must not run inline when --skip-arctic-append"
    assert "arcticdb" not in result["collectors"]


def test_daily_arctic_append_runs_daily_append_with_skip_if_exists_and_is_load_bearing():
    """_run_daily_arctic_append runs daily_append for today's date with
    skip_if_exists=True (cheap reruns) and returns failed (load-bearing) on error."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-06-16", dry_run=False)
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL", "MSFT"]}

    calls = []
    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: calls.append(k) or {"status": "ok"}):
        ok = weekly_collector._run_daily_arctic_append(config, args)
    assert ok["status"] == "ok" and ok["mode"] == "daily_arctic_append"
    assert calls and calls[0]["date_str"] == "2026-06-16"
    assert calls[0]["skip_if_exists"] is True, (
        "EOD append must keep skip_if_exists=True so an operator rerun "
        "short-circuits tickers whose row already landed (matches the inline "
        "block it was split out of)."
    )
    # macro daily tickers must be in the expected_tickers scope (same as _run_daily)
    assert "SPY" in calls[0]["expected_tickers"]

    # load-bearing: append raises → failed
    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.daily_append.daily_append", side_effect=RuntimeError("boom")):
        bad = weekly_collector._run_daily_arctic_append(config, args)
    assert bad["status"] == "failed"


# ── _MACRO_DAILY_TICKERS hard-pin (config-I2703, 2026-07-15 P0) ─────────────
#
# SPY (the alpha-math + eod_reconcile benchmark) was found excluded from
# daily_append's internal expected_tickers scoping — a SEPARATE bug fixed
# in builders/daily_append.py (see tests/test_spy_universe_member.py
# section D). While investigating, a second, independent drift risk turned
# up here: _run_morning_enrich carried its OWN inline copy of the
# macro/benchmark ticker list (local var, no leading underscore) instead of
# sharing the module-level _MACRO_DAILY_TICKERS constant that
# _load_daily_universe_tickers (the EOD PostMarketArcticAppend twin of this
# weekday state) uses. Two independently-editable literals of "the daily
# macro tickers" is a duplicate-pin-drift risk: SPY could be removed from
# one copy (e.g. a churn-scoping refactor) and silently survive in the
# other, masking exactly the kind of gap this incident was about. These
# tests pin (a) SPY is a permanent member of the single canonical constant,
# and (b) both call sites derive from it — no second inline copy.


def test_macro_daily_tickers_constant_pins_spy():
    """The canonical macro/benchmark ticker list must always include SPY —
    this is the hard pin the freshness/missing-from-closes scoping in
    daily_append.py relies on to never treat SPY as a churn-eligible
    S&P straggler."""
    assert "SPY" in weekly_collector._MACRO_DAILY_TICKERS


def test_morning_enrich_and_daily_arctic_append_share_one_macro_ticker_list():
    """Guard against config-I2703 regressing: _run_morning_enrich must NOT
    reintroduce a separate inline copy of the macro-tickers list — both the
    weekday (MorningEnrich) and EOD (_load_daily_universe_tickers) paths
    must derive expected_tickers from the SAME module-level constant."""
    import inspect

    morning_enrich_src = inspect.getsource(weekly_collector._run_morning_enrich)
    assert "_MACRO_DAILY_TICKERS" in morning_enrich_src, (
        "_run_morning_enrich no longer references the shared "
        "_MACRO_DAILY_TICKERS constant — check it hasn't reintroduced a "
        "separate inline copy of the macro/benchmark ticker list "
        "(the config-I2703 duplicate-pin-drift regression)."
    )
    assert "MACRO_DAILY_TICKERS = [" not in morning_enrich_src, (
        "_run_morning_enrich has its own inline macro-tickers list literal "
        "again — this must be a single shared constant "
        "(weekly_collector._MACRO_DAILY_TICKERS), not two independently-"
        "editable copies."
    )


# ── chronic-gap heal hard timeout (L4605) ───────────────────────────────────


def test_chronic_gap_heal_hard_timeout_is_best_effort_skip(monkeypatch):
    """A hung self-heal hits the in-process hard timeout and is recorded as a
    best-effort skip (overall status stays ok) — it must never propagate or
    fail the pipeline (the Saturday-inline SIGKILL fix, L4605)."""
    import time as _time

    config = {"bucket": "b", "chronic_polygon_gaps": {"tickers": {"BRK-B": "x"}}}
    args = SimpleNamespace(date="2026-04-22", dry_run=True)

    monkeypatch.setattr(weekly_collector, "_CHRONIC_HEAL_HARD_TIMEOUT_S", 1)
    monkeypatch.setattr(weekly_collector, "_load_chronic_polygon_gaps", lambda c: ["BRK-B"])
    monkeypatch.setattr(weekly_collector, "_detect_chronic_gap_polygon_recovery",
                        lambda **k: {"status": "ok"})
    monkeypatch.setattr(weekly_collector, "_detect_chronic_gap_constituents_drift",
                        lambda **k: {"still_constituents": ["BRK-B"]})

    def _hang(**k):
        _time.sleep(5)  # interrupted by SIGALRM at 1s
        return {"healed": [], "skipped_already_fresh": [], "errors": []}
    monkeypatch.setattr(weekly_collector, "_self_heal_chronic_polygon_gaps", _hang)

    out = weekly_collector._run_chronic_gap_heal(config, args)

    assert out["status"] == "ok", "heal is best-effort — a timeout must not fail it"
    assert out["collectors"]["chronic_gap_self_heal"]["status"] == "skipped"
    assert "hard timeout" in out["collectors"]["chronic_gap_self_heal"]["error"]


def test_hard_timeout_clean_exit_does_not_fire():
    """The watchdog is a no-op when the block completes under budget."""
    ran = []
    with weekly_collector._hard_timeout(5, "x"):
        ran.append(True)
    assert ran == [True]
