"""Tests for the prior-universe-gap self-heal (config#1228).

When a weekday/EOD Step Function is skipped (e.g. the 2026-06-24 halt), no
daily_append runs for that session and the ArcticDB universe series develops
an interior hole. The next EOD reconcile then reads a non-adjacent prior close
and mislabels a multi-session move as one day (RGEN +14.92% on 06-25). The
heal — run at the head of the 40-min MorningArcticAppend state — detects such
holes (via the fixed-key macro/SPY index) and backfills them, gaplessly and
fail-soft, so subsequent days measure against the true previous trading day.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from alpha_engine_lib.trading_calendar import previous_trading_day

from weekly_collector import (
    _detect_missing_universe_days,
    _self_heal_missing_universe_days,
)

TARGET = "2026-06-25"


def _recent_tds(target: str, n: int) -> list[date]:
    """The n trading sessions strictly before target, newest first."""
    out: list[date] = []
    d = previous_trading_day(date.fromisoformat(target))
    for _ in range(n):
        out.append(d)
        d = previous_trading_day(d)
    return out


def _macro_lib_with_present(present: list[date]) -> MagicMock:
    df = pd.DataFrame(
        {"Close": [1.0] * len(present)},
        index=pd.DatetimeIndex(sorted(present)),
    )
    tail_obj = MagicMock()
    tail_obj.data = df
    lib = MagicMock()
    lib.tail.return_value = tail_obj
    return lib


# ── Detection ────────────────────────────────────────────────────────────────


class TestDetectMissingUniverseDays:
    def test_interior_gap_detected(self):
        tds = _recent_tds(TARGET, 5)
        # Drop one interior day (present on both sides) — the RGEN case.
        gap = tds[1]
        present = [d for d in tds if d != gap]
        with patch("store.arctic_store.get_macro_lib", return_value=_macro_lib_with_present(present)):
            missing = _detect_missing_universe_days("bkt", TARGET, lookback_trading_days=5)
        assert missing == [gap.strftime("%Y-%m-%d")]

    def test_no_gap_returns_empty(self):
        tds = _recent_tds(TARGET, 5)
        with patch("store.arctic_store.get_macro_lib", return_value=_macro_lib_with_present(tds)):
            assert _detect_missing_universe_days("bkt", TARGET, lookback_trading_days=5) == []

    def test_multi_day_end_gap_newest_first(self):
        tds = _recent_tds(TARGET, 5)
        # Two most-recent sessions missing.
        present = tds[2:]
        with patch("store.arctic_store.get_macro_lib", return_value=_macro_lib_with_present(present)):
            missing = _detect_missing_universe_days("bkt", TARGET, lookback_trading_days=5)
        assert missing == [tds[0].strftime("%Y-%m-%d"), tds[1].strftime("%Y-%m-%d")]

    def test_target_date_itself_never_flagged(self):
        # target_date is owned by the same run's append, so even if absent it
        # is outside the strictly-before window.
        tds = _recent_tds(TARGET, 5)
        with patch("store.arctic_store.get_macro_lib", return_value=_macro_lib_with_present(tds)):
            missing = _detect_missing_universe_days("bkt", TARGET, lookback_trading_days=5)
        assert TARGET not in missing

    def test_reference_read_failure_is_noop(self):
        with patch("store.arctic_store.get_macro_lib", side_effect=RuntimeError("arctic down")):
            assert _detect_missing_universe_days("bkt", TARGET, lookback_trading_days=5) == []


# ── Heal orchestration ───────────────────────────────────────────────────────


class TestSelfHealMissingUniverseDays:
    def _patches(self, missing, append_status="ok"):
        collect = MagicMock(return_value={"status": "ok"})
        append = MagicMock(return_value={"status": append_status})
        loader = MagicMock(return_value=({"AAA", "BBB"}, "2026-06-19"))
        return (
            patch("weekly_collector._detect_missing_universe_days", return_value=missing),
            patch("collectors.daily_closes.collect", collect),
            patch("builders.daily_append.daily_append", append),
            patch("builders._constituents_loader.load_constituents_for_run_date", loader),
            patch("weekly_collector.boto3"),
            collect,
            append,
        )

    def test_backfills_missing_day_via_collect_then_append(self):
        p_det, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(["2026-06-24"])
        with p_det, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, dry_run=False, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert summary["errors"] == []
        # collect staged the day, append spliced the same day.
        assert collect.call_args.kwargs["run_date"] == "2026-06-24"
        assert append.call_args.kwargs["date_str"] == "2026-06-24"
        # macro keys folded into the expected-ticker scope.
        assert "SPY" in append.call_args.kwargs["expected_tickers"]

    def test_no_missing_days_is_clean_noop(self):
        p_det, p_col, p_app, p_ldr, p_b3, collect, append = self._patches([])
        with p_det, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={})
        assert summary["healed_days"] == [] and summary["missing_days"] == []
        append.assert_not_called()

    def test_caps_heal_at_max_per_run_defers_the_rest(self):
        p_det, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            ["2026-06-24", "2026-06-23", "2026-06-22"]
        )
        with p_det, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert summary["deferred_days"] == ["2026-06-23", "2026-06-22"]
        assert append.call_count == 1

    def test_append_failure_recorded_not_raised(self):
        p_det, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            ["2026-06-24"], append_status="failed"
        )
        with p_det, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={}, max_heal_days=1)
        assert summary["healed_days"] == []
        assert summary["errors"] and "status=failed" in summary["errors"][0]["reason"]
