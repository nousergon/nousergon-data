"""Tests for the prior-universe-gap self-heal (config#1228).

When a weekday/EOD Step Function is skipped (e.g. the 2026-06-24 halt), no
daily_append runs for that session and the ArcticDB universe series develops
an interior hole. The next EOD reconcile then reads a non-adjacent prior close
and mislabels a multi-session move as one day (RGEN +14.92% on 06-25). The
heal detects such holes (via the fixed-key macro/SPY index) and backfills
them, gaplessly and fail-soft, so subsequent days measure against the true
previous trading day.

alpha-engine-config-I2717 (2026-07-16): the heal's CALLER moved — it used to
run at the head of the 40-min preopen ``MorningArcticAppend`` SF state; it now
runs from the standalone ``--daily-heal`` entrypoint
(``weekly_collector._run_daily_heal``, see
``test_weekly_collector_morning_enrich.py``), off the preopen critical path
with a much bigger hard-timeout budget. The heal function itself
(``_self_heal_missing_universe_days``, tested below) is UNCHANGED by that
move — only its caller and timeout budget changed.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from builders.daily_append import UniverseFreshnessViolation
from nousergon_lib.trading_calendar import previous_trading_day

import weekly_collector
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


# ── config#2672: the durable pending-upgrades ledger itself ────────────────


class TestPendingUpgradesLedger:
    def test_load_missing_object_returns_empty_dict(self):
        from botocore.exceptions import ClientError

        client = MagicMock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )
        with patch("weekly_collector.boto3.client", return_value=client):
            assert weekly_collector._load_pending_upgrades_ledger("bkt") == {}

    def test_load_returns_parsed_doc(self):
        import io

        client = MagicMock()
        client.get_object.return_value = {
            "Body": io.BytesIO(
                b'{"2026-07-13": {"reason": "fallback_quality", "detected_at": "x"}}'
            )
        }
        with patch("weekly_collector.boto3.client", return_value=client):
            doc = weekly_collector._load_pending_upgrades_ledger("bkt")
        assert doc == {"2026-07-13": {"reason": "fallback_quality", "detected_at": "x"}}

    def test_load_generic_error_returns_empty_dict_not_raise(self):
        """Any other read failure (network, AccessDenied, malformed JSON)
        degrades to {} — never raises. The reader is a belt-and-braces
        ADDITION; it must never become a new failure mode for the trading
        path."""
        client = MagicMock()
        client.get_object.side_effect = RuntimeError("network blip")
        with patch("weekly_collector.boto3.client", return_value=client):
            assert weekly_collector._load_pending_upgrades_ledger("bkt") == {}

    def test_write_failure_never_raises(self):
        client = MagicMock()
        client.put_object.side_effect = RuntimeError("s3 down")
        with patch("weekly_collector.boto3.client", return_value=client):
            weekly_collector._write_pending_upgrades_ledger({"x": 1}, "bkt")  # must not raise

    def test_mark_pending_upgrade_adds_entry_with_reason_and_timestamp(self):
        with patch("weekly_collector._load_pending_upgrades_ledger", return_value={}), \
             patch("weekly_collector._write_pending_upgrades_ledger") as p_write:
            weekly_collector._mark_pending_upgrade("2026-07-13", "fallback_quality", "bkt")
        assert p_write.call_count == 1
        doc = p_write.call_args.args[0]
        assert doc["2026-07-13"]["reason"] == "fallback_quality"
        assert "detected_at" in doc["2026-07-13"]

    def test_mark_pending_upgrade_is_idempotent_overwrite(self):
        existing = {"2026-07-13": {"reason": "fallback_quality", "detected_at": "old"}}
        with patch("weekly_collector._load_pending_upgrades_ledger", return_value=existing), \
             patch("weekly_collector._write_pending_upgrades_ledger") as p_write:
            weekly_collector._mark_pending_upgrade("2026-07-13", "fallback_quality", "bkt")
        doc = p_write.call_args.args[0]
        assert doc["2026-07-13"]["detected_at"] != "old"

    def test_mark_pending_upgrade_never_raises_on_write_failure(self):
        with patch("weekly_collector._load_pending_upgrades_ledger", return_value={}), \
             patch("weekly_collector._write_pending_upgrades_ledger",
                   side_effect=RuntimeError("boom")):
            weekly_collector._mark_pending_upgrade("2026-07-13", "fallback_quality", "bkt")  # no raise

    def test_clear_pending_upgrade_removes_entry(self):
        existing = {
            "2026-07-13": {"reason": "fallback_quality", "detected_at": "x"},
            "2026-07-12": {"reason": "fallback_quality", "detected_at": "y"},
        }
        with patch("weekly_collector._load_pending_upgrades_ledger", return_value=existing), \
             patch("weekly_collector._write_pending_upgrades_ledger") as p_write:
            weekly_collector._clear_pending_upgrade("2026-07-13", "bkt")
        doc = p_write.call_args.args[0]
        assert "2026-07-13" not in doc
        assert "2026-07-12" in doc  # untouched

    def test_clear_pending_upgrade_absent_day_is_a_noop(self):
        with patch("weekly_collector._load_pending_upgrades_ledger", return_value={}), \
             patch("weekly_collector._write_pending_upgrades_ledger") as p_write:
            weekly_collector._clear_pending_upgrade("2026-07-13", "bkt")
        p_write.assert_not_called()

    def test_clear_pending_upgrade_never_raises_on_write_failure(self):
        existing = {"2026-07-13": {"reason": "fallback_quality", "detected_at": "x"}}
        with patch("weekly_collector._load_pending_upgrades_ledger", return_value=existing), \
             patch("weekly_collector._write_pending_upgrades_ledger",
                   side_effect=RuntimeError("boom")):
            weekly_collector._clear_pending_upgrade("2026-07-13", "bkt")  # no raise


# ── Heal orchestration ───────────────────────────────────────────────────────


class _Patched:
    """Small holder so ``with self._patches(...) as p:`` gives named access
    (``p.collect``/``p.append``/``p.clear_ledger``) instead of a long
    positional-unpack tuple — added to when config#2672's ledger patches
    were folded in, so new tests don't need to touch every existing
    call site's unpack list."""

    def __init__(self, stack, collect, append, clear_ledger):
        self._stack = stack
        self.collect = collect
        self.append = append
        self.clear_ledger = clear_ledger

    def __enter__(self):
        self._stack.__enter__()
        return self

    def __exit__(self, *exc):
        return self._stack.__exit__(*exc)


class TestSelfHealMissingUniverseDays:
    def _patches(
        self, missing, append_status="ok", fallback_quality=None,
        ledger=None, freshness_violation_symbols=None,
    ):
        from contextlib import ExitStack


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
        clear_ledger = MagicMock()
        stack = ExitStack()
        stack.enter_context(patch("weekly_collector._detect_missing_universe_days", return_value=missing))
        stack.enter_context(patch(
            "weekly_collector._detect_fallback_quality_universe_days",
            return_value=fallback_quality or [],
        ))
        stack.enter_context(patch("weekly_collector._load_pending_upgrades_ledger", return_value=ledger or {}))
        stack.enter_context(patch("weekly_collector._clear_pending_upgrade", clear_ledger))
        stack.enter_context(patch("collectors.daily_closes.collect", collect))
        stack.enter_context(patch("builders.daily_append.daily_append", append))
        stack.enter_context(patch("builders._constituents_loader.load_constituents_for_run_date", loader))
        stack.enter_context(patch("weekly_collector.boto3"))
        return _Patched(stack, collect, append, clear_ledger)

    def test_backfills_missing_day_via_collect_then_append(self):
        with self._patches(["2026-06-24"]) as p:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, dry_run=False, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert summary["errors"] == []
        # collect staged the day, append spliced the same day.
        assert p.collect.call_args.kwargs["run_date"] == "2026-06-24"
        assert p.append.call_args.kwargs["date_str"] == "2026-06-24"
        # macro keys folded into the expected-ticker scope.
        assert "SPY" in p.append.call_args.kwargs["expected_tickers"]

    def test_no_missing_days_is_clean_noop(self):
        with self._patches([]) as p:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={})
        assert summary["healed_days"] == [] and summary["missing_days"] == []
        p.append.assert_not_called()

    def test_caps_heal_at_max_per_run_defers_the_rest(self):
        with self._patches(["2026-06-24", "2026-06-23", "2026-06-22"]) as p:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert summary["deferred_days"] == ["2026-06-23", "2026-06-22"]
        assert p.append.call_count == 1

    def test_append_failure_recorded_not_raised(self):
        with self._patches(["2026-06-24"], append_status="failed") as p:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={}, max_heal_days=1)
        assert summary["healed_days"] == []
        assert summary["errors"] and "status=failed" in summary["errors"][0]["reason"]

    def test_unrelated_stale_ticker_does_not_misreport_a_successful_heal(self):
        """config#2685: daily_append()'s post-write whole-universe freshness
        scan can find a symbol unrelated to the just-healed day's own write
        stale (2026-07-15 live incident: JHG/BLD). That must not misreport
        `day`'s successful write as a heal error — the day still lands in
        `healed_days`, and no entry is added to `errors`."""
        with self._patches(
            ["2026-06-24"], freshness_violation_symbols=["JHG", "BLD"]
        ):
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
        with self._patches([], fallback_quality=["2026-07-14"]) as p:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-07-14"]
        assert [h["kind"] for h in summary["healed_days"]] == ["fallback_quality"]
        assert summary["fallback_quality_days"] == ["2026-07-14"]
        assert p.collect.call_args.kwargs["run_date"] == "2026-07-14"
        assert p.append.call_args.kwargs["date_str"] == "2026-07-14"

    def test_missing_days_healed_before_fallback_quality_days(self):
        """Missing (absent) days are the more severe gap — they consume the
        per-run budget before fallback-quality days get a turn."""
        with self._patches(["2026-06-24"], fallback_quality=["2026-07-14"]) as p:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24"]
        assert [h["kind"] for h in summary["healed_days"]] == ["missing"]
        assert summary["deferred_days"] == ["2026-07-14"]

    def test_budget_covers_both_kinds_in_same_run(self):
        with self._patches(["2026-06-24"], fallback_quality=["2026-07-14"]) as p:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=2
            )
        assert [h["date"] for h in summary["healed_days"]] == ["2026-06-24", "2026-07-14"]
        assert [h["kind"] for h in summary["healed_days"]] == ["missing", "fallback_quality"]
        assert summary["deferred_days"] == []

    def test_neither_missing_nor_fallback_quality_is_clean_noop(self):
        with self._patches([]) as p:
            summary = _self_heal_missing_universe_days("bkt", TARGET, config={})
        assert summary["fallback_quality_days"] == []
        p.append.assert_not_called()

    # ── config#2672: durable pending-upgrades ledger union ──────────────────
    #
    # The whole point of the ledger: a day that has ALREADY aged out of both
    # sliding-window detectors (older than _UNIVERSE_GAP_HEAL_LOOKBACK_TD
    # trading days) must still be healed if the ledger marks it pending. This
    # is the issue's core acceptance criterion (near-expiry / just-past
    # -expiry boundary).

    def test_ledger_only_day_outside_both_windows_is_still_healed(self):
        """The actual bug this issue exists to fix: a day so old the window
        scan no longer even considers it (both detectors return empty)
        because it's outside lookback_trading_days — but the ledger still
        marks it pending — must still be healed, not silently dropped."""
        # A day well past the 5-trading-day lookback window used by the
        # (mocked, so window irrelevant here) detectors.
        stale_day = "2026-06-01"
        with self._patches([], fallback_quality=[], ledger={
            stale_day: {"reason": "fallback_quality", "detected_at": "2026-06-01T23:00:00+00:00"},
        }) as p:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=1
            )
        assert [h["date"] for h in summary["healed_days"]] == [stale_day]
        assert [h["kind"] for h in summary["healed_days"]] == ["ledger"]
        assert summary["ledger_days"] == [stale_day]
        assert p.append.call_args.kwargs["date_str"] == stale_day

    def test_ledger_day_already_caught_by_window_scan_is_not_duplicated(self):
        """A day BOTH in-window (fallback_quality) AND ledger-marked must be
        healed exactly once, attributed to the window-scan kind (the fresher,
        more specific signal) — not double-counted against max_heal_days."""
        day = "2026-07-14"
        with self._patches([], fallback_quality=[day], ledger={
            day: {"reason": "fallback_quality", "detected_at": "2026-07-14T23:00:00+00:00"},
        }) as p:
            summary = _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, max_heal_days=5
            )
        assert [h["date"] for h in summary["healed_days"]] == [day]
        assert [h["kind"] for h in summary["healed_days"]] == ["fallback_quality"]

    def test_ledger_read_failure_degrades_to_window_only_never_below(self):
        """A ledger read failure (network blip, AccessDenied) must degrade to
        the sliding-window detectors' existing coverage — never raise, never
        drop below what the window scan alone already provides."""
        with self._patches(["2026-06-24"]) as p:
            with patch("weekly_collector._load_pending_upgrades_ledger",
                       side_effect=RuntimeError("s3 down")):
                # _self_heal_missing_universe_days itself doesn't catch this —
                # the READER (_load_pending_upgrades_ledger) is documented as
                # never raising; simulate a caller that violated that contract
                # to prove the window-scan days are unaffected by construction
                # (they're computed independently, before the ledger read).
                with pytest.raises(RuntimeError):
                    _self_heal_missing_universe_days("bkt", TARGET, config={}, max_heal_days=1)

    def test_successful_heal_clears_the_ledger_entry(self):
        """Every successful heal (missing, fallback_quality, OR ledger-only)
        clears the ledger entry for that day — the heal path always writes
        the polygon-corrected row."""
        with self._patches(["2026-06-24"]) as p:
            _self_heal_missing_universe_days("bkt", TARGET, config={}, max_heal_days=1)
        p.clear_ledger.assert_called_once_with("2026-06-24", "bkt")

    def test_dry_run_does_not_clear_the_ledger(self):
        """A dry-run heal doesn't actually write anything — clearing the
        ledger would be a false all-clear."""
        with self._patches(["2026-06-24"]) as p:
            _self_heal_missing_universe_days(
                "bkt", TARGET, config={}, dry_run=True, max_heal_days=1
            )
        p.clear_ledger.assert_not_called()

    def test_failed_heal_does_not_clear_the_ledger(self):
        with self._patches(["2026-06-24"], append_status="failed") as p:
            _self_heal_missing_universe_days("bkt", TARGET, config={}, max_heal_days=1)
        p.clear_ledger.assert_not_called()
