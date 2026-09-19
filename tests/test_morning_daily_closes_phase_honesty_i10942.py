"""``morning_daily_closes`` must not read ``ok`` when its own trading day's
parquet was never written, and its ``artifact_keys`` must say what it wrote.

`alpha-engine-config-I10942`. The 2026-09-16 shadow run's D17 manifest read
`status: failed` (correctly — `morning_enrich` itself hard-fails on a
non-`ok` `daily_closes` result), but the SEPARATE `.phases/
morning_daily_closes.json` phase marker read `status: ok` with
`artifact_keys: []`, even though `daily_closes.collect` in window mode wrote
nine parquet files and never wrote the run's own trading day.

Root cause traced in `weekly_collector.py::_run_morning_enrich`:
`daily_closes.collect` in window mode (`_collect_window`) does NOT raise on
a target-date failure — "Fatality is TARGET-driven" in that function is
enforced by returning `status="error"` as a VALUE, precisely so a
best-effort *backfill* date miss never aborts the run. But the call site
wrapped the collect() call in `_maybe_phase(reg, "morning_daily_closes")`,
which is marker-only (see its docstring: "no ctx.skipped / no
record_artifact") and records `status: ok` on its marker unless the `with`
block itself raises. Nothing checked `dc_result["status"]` before exiting
the block, so a target-date failure silently became a `ok` phase marker
with an empty `artifact_keys` — exactly the reported shape.

NOTE on I10942 deliverable 1 (history_window/clip_to_trading_day boundary):
established by reading `collectors/daily_closes.py` end to end —
`daily_closes.collect`/`_collect_window` do not call
`dates.history_window` or `dates.clip_to_trading_day` at all (those serve
`collectors/prices.py`, `collectors/macro.py`, `collectors/alternative.py`,
`collectors/metron_market_data.py`, `features/metron_supplemental.py`
instead). The daily-closes window boundary is
`collectors/daily_closes.py::_previous_business_days`, which already
INCLUDES the anchor `run_date` as its first (newest) entry whenever that
date is itself an NYSE trading day — read end to end, it is not exclusive.
This test file covers the confirmed defect (the phase marker's honesty);
it does not re-litigate the boundary question, which needs a live
re-dispatch to settle definitively (left as follow-up — see the PR body).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import weekly_collector


@pytest.fixture
def enrich_args_live():
    # dry_run=False so `_build_registry` returns a real (best-effort,
    # S3-write-optional) `RunStatePhaseRegistry` and `_maybe_phase` yields a
    # real `ctx` rather than `nullcontext()` — the exact code path this
    # defect lived in.
    return SimpleNamespace(date="2026-09-16", dry_run=False, morning_enrich=True)


def _base_mocks():
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL"], "date": "2026-09-16",
    }
    return fake_constituents


def test_phase_reads_failed_when_target_date_missing_from_window_result(enrich_args_live):
    """Induces the exact reported shape: window-mode `daily_closes.collect`
    returns `status="error"` as a VALUE (never raises) because the run's own
    trading day (the target date) is absent from what it wrote — nine
    backfill dates present, the target date is not. The mode must read
    `failed`, and `daily_closes`'s own collector result must carry
    `status="error"`."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    fake_constituents = _base_mocks()

    backfill_dates = [
        "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09",
        "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15",
    ]
    dc_result = {
        "status": "error",
        "source": "polygon_only",
        "window_days": 10,
        "target_date": "2026-09-16",
        "error": "target date 2026-09-16 failed: vendor returned no rows",
        "per_date": {
            **{d: {"status": "ok"} for d in backfill_dates},
            "2026-09-16": {"status": "error", "error": "vendor returned no rows"},
        },
        "backfill_failed_dates": [],
    }

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
         patch("weekly_collector.daily_closes.collect", return_value=dc_result), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, enrich_args_live)

    assert result["status"] == "failed", (
        "morning_enrich must read failed when its own trading day's parquet "
        "was never written, not just when the aggregate happens to raise"
    )
    assert result["collectors"]["daily_closes"]["status"] == "error"
    # The failing collector must be diagnosable from the reason alone
    # (I10941's bounded-reason contract applies here too).
    assert "2026-09-16" in result["collectors"]["daily_closes"]["error"]


def test_phase_reads_ok_and_still_succeeds_when_target_date_present(enrich_args_live):
    """Control: a window result whose target date IS present and ok must
    still read `ok` — this fix must not turn every window run red."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    fake_constituents = _base_mocks()

    dc_result = {
        "status": "ok",
        "source": "polygon_only",
        "window_days": 2,
        "target_date": "2026-09-16",
        "per_date": {
            "2026-09-15": {"status": "ok"},
            "2026-09-16": {"status": "ok"},
        },
        "backfill_failed_dates": [],
    }

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
         patch("weekly_collector.daily_closes.collect", return_value=dc_result), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, enrich_args_live)

    assert result["status"] == "ok"
    assert result["collectors"]["daily_closes"]["status"] == "ok"


# ── _daily_closes_written_keys ──────────────────────────────────────────────


def test_written_keys_window_mode_only_counts_ok_dates():
    dc_result = {
        "per_date": {
            "2026-09-15": {"status": "ok"},
            "2026-09-16": {"status": "error", "error": "boom"},
            "2026-09-14": {"status": "ok_dry_run"},
        },
    }
    keys = weekly_collector._daily_closes_written_keys(
        dc_result, "staging/daily_closes/", "2026-09-16"
    )
    assert keys == [
        "staging/daily_closes/2026-09-14.parquet",
        "staging/daily_closes/2026-09-15.parquet",
    ]
    assert "staging/daily_closes/2026-09-16.parquet" not in keys


def test_written_keys_single_date_mode_uses_target_date():
    assert weekly_collector._daily_closes_written_keys(
        {"status": "ok"}, "staging/daily_closes/", "2026-09-16"
    ) == ["staging/daily_closes/2026-09-16.parquet"]
    assert weekly_collector._daily_closes_written_keys(
        {"status": "error"}, "staging/daily_closes/", "2026-09-16"
    ) == []


def test_written_keys_handles_prefix_with_or_without_trailing_slash():
    assert weekly_collector._daily_closes_written_keys(
        {"status": "ok"}, "staging/daily_closes", "2026-09-16"
    ) == ["staging/daily_closes/2026-09-16.parquet"]
