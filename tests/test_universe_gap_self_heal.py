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

from builders.daily_append import UniverseFreshnessViolation
from nousergon_lib.trading_calendar import previous_trading_day

from weekly_collector import (
    _detect_fallback_quality_universe_days,
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


def _macro_lib_with_sources(sources_by_date: dict[date, str]) -> MagicMock:
    """Build a macro/SPY tail mock carrying a ``source`` column, keyed by date."""
    dates = sorted(sources_by_date)
    df = pd.DataFrame(
        {
            "Close": [1.0] * len(dates),
            "source": [sources_by_date[d] for d in dates],
        },
        index=pd.DatetimeIndex(dates),
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


class TestDetectFallbackQualityUniverseDays:
    """config#2664: a day PRESENT in ArcticDB but stuck on EOD's yfinance
    fallback because the next-morning Polygon overwrite never landed.

    Reads SPY's ``source`` column from the UNIVERSE library (not macro) —
    verified live 2026-07-15 that ``macro_lib.tail("SPY").data`` carries
    only a bare ``Close`` column with no ``source``/OHLCV, so an earlier
    version of this detector that read macro_lib silently always returned
    ``[]``. Only ``store.arctic_store.get_universe_lib`` is patched here.
    """

    def test_yfinance_sourced_day_flagged(self):
        tds = _recent_tds(TARGET, 5)
        stale = tds[0]
        sources = {d: ("yfinance" if d == stale else "polygon") for d in tds}
        with patch("store.arctic_store.get_universe_lib", return_value=_macro_lib_with_sources(sources)):
            flagged = _detect_fallback_quality_universe_days("bkt", TARGET, lookback_trading_days=5)
        assert flagged == [stale.strftime("%Y-%m-%d")]

    def test_all_polygon_returns_empty(self):
        tds = _recent_tds(TARGET, 5)
        sources = {d: "polygon" for d in tds}
        with patch("store.arctic_store.get_universe_lib", return_value=_macro_lib_with_sources(sources)):
            assert _detect_fallback_quality_universe_days("bkt", TARGET, lookback_trading_days=5) == []

    def test_absent_day_not_flagged_here(self):
        # A day missing entirely is _detect_missing_universe_days's job, not
        # this detector's — it can only flag days present in the index.
        tds = _recent_tds(TARGET, 5)
        present = tds[1:]  # drop the newest
        sources = {d: "polygon" for d in present}
        with patch("store.arctic_store.get_universe_lib", return_value=_macro_lib_with_sources(sources)):
            flagged = _detect_fallback_quality_universe_days("bkt", TARGET, lookback_trading_days=5)
        assert tds[0].strftime("%Y-%m-%d") not in flagged

    def test_missing_source_column_is_noop(self):
        tds = _recent_tds(TARGET, 5)
        with patch("store.arctic_store.get_universe_lib", return_value=_macro_lib_with_present(tds)):
            assert _detect_fallback_quality_universe_days("bkt", TARGET, lookback_trading_days=5) == []

    def test_reference_read_failure_is_noop(self):
        with patch("store.arctic_store.get_universe_lib", side_effect=RuntimeError("arctic down")):
            assert _detect_fallback_quality_universe_days("bkt", TARGET, lookback_trading_days=5) == []

    def test_multiple_stale_days_newest_first(self):
        tds = _recent_tds(TARGET, 5)
        sources = {d: "polygon" for d in tds}
        sources[tds[2]] = "yfinance"
        sources[tds[0]] = "fred"
        with patch("store.arctic_store.get_universe_lib", return_value=_macro_lib_with_sources(sources)):
            flagged = _detect_fallback_quality_universe_days("bkt", TARGET, lookback_trading_days=5)
        assert flagged == [tds[0].strftime("%Y-%m-%d"), tds[2].strftime("%Y-%m-%d")]

    def test_does_not_touch_macro_lib(self):
        """Regression guard for the exact bug caught 2026-07-15 live-testing:
        this detector must never read macro_lib (Close-only, no source)."""
        tds = _recent_tds(TARGET, 5)
        sources = {d: "polygon" for d in tds}
        sources[tds[0]] = "yfinance"
        with (
            patch("store.arctic_store.get_universe_lib", return_value=_macro_lib_with_sources(sources)) as p_uni,
            patch("store.arctic_store.get_macro_lib") as p_macro,
        ):
            flagged = _detect_fallback_quality_universe_days("bkt", TARGET, lookback_trading_days=5)
        assert flagged == [tds[0].strftime("%Y-%m-%d")]
        p_uni.assert_called_once()
        p_macro.assert_not_called()


# ── Heal orchestration ───────────────────────────────────────────────────────


class TestSelfHealMissingUniverseDays:
    def _patches(
        self, missing, append_status="ok", fallback_quality=None,
        freshness_violation_symbols=None,
    ):
        collect = MagicMock(return_value={"status": "ok"})
        if freshness_violation_symbols is not None:
            # config#2685: the target-date write itself succeeded, but the
            # post-write whole-universe scan found an UNRELATED stale
            # symbol — daily_append raises UniverseFreshnessViolation
            # rather than returning, same as the live incident (JHG/BLD).
            stale_symbols = [
                {"symbol": s, "last_date": "2026-07-10", "age_trading_days": 5}
                for s in freshness_violation_symbols
            ]
            append = MagicMock(
                side_effect=UniverseFreshnessViolation(
                    "Universe-freshness scan: unrelated stale symbol(s)",
                    stale_symbols=stale_symbols,
                )
            )
        else:
            append = MagicMock(return_value={"status": append_status})
        loader = MagicMock(return_value=({"AAA", "BBB"}, "2026-06-19"))
        return (
            patch("weekly_collector._detect_missing_universe_days", return_value=missing),
            patch(
                "weekly_collector._detect_fallback_quality_universe_days",
                return_value=fallback_quality or [],
            ),
            patch("collectors.daily_closes.collect", collect),
            patch("builders.daily_append.daily_append", append),
            patch("builders._constituents_loader.load_constituents_for_run_date", loader),
            patch("weekly_collector.boto3"),
            collect,
            append,
        )

    def test_backfills_missing_day_via_collect_then_append(self):
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(["2026-06-24"])
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
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
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches([])
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={})
        assert summary["healed_days"] == [] and summary["missing_days"] == []
        append.assert_not_called()

    def test_caps_heal_at_max_per_run_defers_the_rest(self):
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            ["2026-06-24", "2026-06-23", "2026-06-22"]
        )
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert summary["deferred_days"] == ["2026-06-23", "2026-06-22"]
        assert append.call_count == 1

    def test_append_failure_recorded_not_raised(self):
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            ["2026-06-24"], append_status="failed"
        )
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={}, max_heal_days=1)
        assert summary["healed_days"] == []
        assert summary["errors"] and "status=failed" in summary["errors"][0]["reason"]

    def test_unrelated_stale_ticker_does_not_misreport_a_successful_heal(self):
        """config#2685: daily_append()'s post-write whole-universe freshness
        scan can find a symbol unrelated to the just-healed day's own write
        stale (2026-07-15 live incident: JHG/BLD). That must not misreport
        `day`'s successful write as a heal error — the day still lands in
        `healed_days`, and no entry is added to `errors`."""
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            ["2026-06-24"], freshness_violation_symbols=["JHG", "BLD"]
        )
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert summary["errors"] == []

    # ── config#2664: fallback-quality days folded into the same heal ────────

    def test_fallback_quality_day_healed_via_same_path(self):
        """A day with no absence gap, only a stale (yfinance) source, still
        gets collect+append re-run — the exact overwrite MorningEnrich itself
        relies on (skip_if_exists defaults False)."""
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            [], fallback_quality=["2026-07-14"]
        )
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-07-14"]
        assert [h["kind"] for h in summary["healed_days"]] == ["fallback_quality"]
        assert summary["fallback_quality_days"] == ["2026-07-14"]
        assert collect.call_args.kwargs["run_date"] == "2026-07-14"
        assert append.call_args.kwargs["date_str"] == "2026-07-14"

    def test_missing_days_healed_before_fallback_quality_days(self):
        """Missing (absent) days are the more severe gap — they consume the
        per-run budget before fallback-quality days get a turn."""
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            ["2026-06-24"], fallback_quality=["2026-07-14"]
        )
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert [h["kind"] for h in summary["healed_days"]] == ["missing"]
        assert summary["deferred_days"] == ["2026-07-14"]

    def test_budget_covers_both_kinds_in_same_run(self):
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches(
            ["2026-06-24"], fallback_quality=["2026-07-14"]
        )
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=2
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24", "2026-07-14"]
        assert [h["kind"] for h in summary["healed_days"]] == ["missing", "fallback_quality"]
        assert summary["deferred_days"] == []

    def test_neither_missing_nor_fallback_quality_is_clean_noop(self):
        p_det, p_fq, p_col, p_app, p_ldr, p_b3, collect, append = self._patches([])
        with p_det, p_fq, p_col, p_app, p_ldr, p_b3:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={})
        assert summary["fallback_quality_days"] == []
        append.assert_not_called()
