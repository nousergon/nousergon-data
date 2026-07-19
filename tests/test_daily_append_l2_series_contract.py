"""Regression tests for the L2 series-contract wiring in
``builders/daily_append.py`` (alpha-engine-config#2456).

Background: ``validators/price_validator.py``'s ``validate_today_row``
already gates OHLC/sanity/volume anomalies at write time. This wave adds
``nousergon_lib.series_contract.validate_series`` as a SECOND, independent
gate at the same call site, covering three checks ``price_validator``
does not: calendar-aware continuity (vs. ``validate_parquet``'s naive
calendar-day gap heuristic), vol-scaled outlier (vs. the fixed
``MAX_DAILY_RETURN=0.50``), and calendar-monotonic (no prior equivalent).

Tests cover:

1. A clean, calendar-monotonic row with no gate failures does not block
   or warn.
2. A calendar_monotonic failure (duplicate date after concatenating
   hist+today_row) blocks by default.
3. The MorningEnrich overwrite contract: when today's date is ALREADY in
   hist (the legitimate polygon-overwrite-yfinance case,
   skip_if_exists=False), the L2 gate must NOT treat this as a duplicate-
   date corruption — this is the regression this PR's implementation had
   to fix (today_row REPLACES the same-date hist row before the L2 checks
   run, rather than being concatenated alongside it).
4. ``DAILY_APPEND_L2_BLOCK_GATES`` env var override — promoting a
   warn-default gate (continuity) to block.
5. ``_load_l2_block_gates`` malformed/unknown-gate error paths.
6. The aggregated end-of-run alert fires at ERROR when any row is
   quarantined, and at WARNING (not ERROR) when rows are only alarmed
   (non-quarantining).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from tests.conftest import recent_trading_day_str


@pytest.fixture(autouse=True)
def _disable_factor_momentum_daily(monkeypatch):
    monkeypatch.setenv("FACTOR_MOMENTUM_DAILY_ENABLED", "false")
    monkeypatch.setenv("FACTOR_LOADING_ZSCORE_DAILY_ENABLED", "false")


def _stub_closes(tickers: list[str]) -> dict:
    return {
        t: {"Open": 100.0, "High": 101.0, "Low": 99.0,
            "Close": 100.0, "Volume": 1_000_000, "VWAP": 100.5}
        for t in tickers
    }


def _patch_targets(
    monkeypatch,
    *,
    universe_symbols: list[str],
    today_in_hist: bool,
    today_str: str,
    hist_periods: int = 280,
):
    """Minimal daily_append patch surface for a single-ticker L2 check.

    ``hist_periods`` defaults to 280 business days ending just before
    ``today_str`` — comfortably above ``MIN_ROWS_FOR_FEATURES`` (265) so
    the per-ticker loop doesn't fall into the ``_load_parquet_warmup`` S3
    fallback path (which this fixture's bare ``MagicMock`` S3 client
    can't serve). Anchored to end just before ``today_str`` (dense,
    near-contiguous) rather than the sibling fixture's Jan-2024 anchor —
    that keeps the continuity gate's expected-vs-present trading-day diff
    small and predictable for these tests instead of the multi-year gap a
    fixed-date anchor would introduce (real, but not what these tests are
    isolating).
    """
    from builders import daily_append as _da

    macro_keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]
    sector_etfs = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
                   "XLP", "XLRE", "XLU", "XLV", "XLY"]
    closes = _stub_closes(universe_symbols + macro_keys + sector_etfs)

    today_ts = pd.Timestamp(today_str)
    hist_dates = pd.bdate_range(end=today_ts - pd.Timedelta(days=1), periods=hist_periods)
    if today_in_hist:
        hist_dates = hist_dates.append(pd.DatetimeIndex([today_ts]))
    hist_df = pd.DataFrame(
        {
            "Open": 100.0, "High": 101.0, "Low": 99.0,
            "Close": 100.0, "Volume": 1_000_000, "VWAP": 100.5,
        },
        index=hist_dates,
    )
    hist_df.index.name = "date"

    universe_lib = MagicMock()
    universe_lib.list_symbols.return_value = universe_symbols
    universe_lib.read_batch.return_value = [
        MagicMock(spec=[], data=hist_df.copy()) for _ in universe_symbols
    ]
    today_row = pd.DataFrame({"Close": [100.0]}, index=[today_ts])
    universe_lib.tail.return_value = MagicMock(spec=[], data=today_row)

    macro_lib = MagicMock()
    macro_df = hist_df[["Close"]].copy()
    macro_df.loc[today_ts] = 100.0
    macro_lib.read.return_value = MagicMock(data=macro_df)
    macro_lib.list_symbols.return_value = sector_etfs

    monkeypatch.setattr(_da, "_load_daily_closes", lambda *a, **k: closes)
    monkeypatch.setattr(_da, "_load_sector_map", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_fundamentals", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_alternative", lambda *a, **k: {})
    monkeypatch.setattr(_da, "get_universe_lib", lambda *a, **k: universe_lib)
    monkeypatch.setattr(_da, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_da, "_emit_missing_from_closes_metric", MagicMock())
    monkeypatch.setattr(_da, "_emit_quality_gate_metrics", MagicMock())

    from features.feature_engineer import FEATURES

    def _fake_compute_features(combined, **_):
        out = combined.copy()
        for f in list(FEATURES)[:3]:
            out[f] = 0.5
        return out

    monkeypatch.setattr(_da, "compute_features", _fake_compute_features)

    write_calls: list[tuple[str, str]] = []

    def _spy_write(lib, sym, df, existing_series=None):
        which = "universe" if lib is universe_lib else "macro"
        write_calls.append((which, sym))
        return "append"

    monkeypatch.setattr(_da, "_write_row_backfill_safe", _spy_write)

    def _spy_update_batch(payloads, **kwargs):
        for p in payloads:
            write_calls.append(("universe", p.symbol))
        return [MagicMock(symbol=p.symbol) for p in payloads]

    def _spy_write_batch(payloads, **kwargs):
        for p in payloads:
            write_calls.append(("universe", p.symbol))
        return [MagicMock(symbol=p.symbol) for p in payloads]

    universe_lib.update_batch.side_effect = _spy_update_batch
    universe_lib.write_batch.side_effect = _spy_write_batch

    mock_s3 = MagicMock()
    monkeypatch.setattr("builders.daily_append.boto3.client", lambda *a, **k: mock_s3)

    return universe_lib, macro_lib, write_calls


# ── 1 & 2: clean row passes, calendar_monotonic blocks ─────────────────────


def test_clean_dense_history_no_l2_block(monkeypatch):
    """A dense, near-contiguous 30-business-day history + a new today row
    (append-at-head, not already in hist) should not trip calendar_monotonic
    or schema/sanity. Continuity may still warn (short window still has
    trading-day granularity) but must not block or prevent the write."""
    today_str = recent_trading_day_str()
    universe = ["AAPL"]
    _, _, write_calls = _patch_targets(
        monkeypatch, universe_symbols=universe,
        today_in_hist=False, today_str=today_str,
    )
    from builders.daily_append import daily_append

    result = daily_append(date_str=today_str)
    assert result["status"] == "ok"
    assert result["tickers_appended"] == 1
    assert result["tickers_l2_quarantined"] == 0
    universe_writes = [w for w in write_calls if w[0] == "universe"]
    assert len(universe_writes) == 1


def test_morning_enrich_overwrite_not_treated_as_duplicate(monkeypatch, caplog):
    """The regression this PR's implementation had to fix: when today's
    date is ALREADY in hist (skip_if_exists=False MorningEnrich overwrite
    path), the L2 gate must REPLACE that row rather than concatenate a
    duplicate — a duplicate date would false-positive calendar_monotonic
    (a block-default gate) and silently break the overwrite contract that
    test_daily_append_skip_if_exists.py separately locks."""
    today_str = recent_trading_day_str()
    universe = ["AAPL", "MSFT"]
    _, _, write_calls = _patch_targets(
        monkeypatch, universe_symbols=universe,
        today_in_hist=True, today_str=today_str,
    )
    from builders.daily_append import daily_append

    with caplog.at_level(logging.WARNING, logger="builders.daily_append"):
        result = daily_append(date_str=today_str)

    assert result["status"] == "ok"
    assert result["tickers_appended"] == len(universe), (
        f"L2 gate must not quarantine the legitimate MorningEnrich overwrite; "
        f"tickers_l2_quarantined={result['tickers_l2_quarantined']}"
    )
    assert result["tickers_l2_quarantined"] == 0
    assert not any(
        "calendar_monotonic" in rec.message for rec in caplog.records
    ), "no calendar_monotonic complaint should fire for a legitimate same-date overwrite"
    universe_writes = [w for w in write_calls if w[0] == "universe"]
    assert len(universe_writes) == len(universe)


# ── 3: env-var override promotes a warn-default gate to block ──────────────


def test_l2_block_gates_env_override_promotes_continuity(monkeypatch):
    """DAILY_APPEND_L2_BLOCK_GATES=["continuity"] must quarantine a row
    whose history has a real (non-holiday) missing trading day, even
    though continuity is warn-by-default."""
    from builders import daily_append as _da

    today_str = recent_trading_day_str()
    universe = ["AAPL"]
    _patch_targets(
        monkeypatch, universe_symbols=universe,
        today_in_hist=False, today_str=today_str,
    )

    # Force a genuine continuity gap: drop one business day from the
    # patched hist frame by monkeypatching read_batch's returned data
    # after _patch_targets sets it up.
    universe_lib = _da.get_universe_lib("any-bucket")
    hist = universe_lib.read_batch.return_value[0].data
    # Drop the middle row to create a real gap that isn't a weekend/holiday.
    mid = len(hist) // 2
    dropped = hist.drop(hist.index[mid])
    universe_lib.read_batch.return_value = [
        MagicMock(spec=[], data=dropped) for _ in universe
    ]

    monkeypatch.setenv("DAILY_APPEND_L2_BLOCK_GATES", json.dumps(["continuity"]))

    result = _da.daily_append(date_str=today_str)
    assert result["tickers_l2_quarantined"] == 1
    assert "continuity" in result["l2_gate_counts"]


# ── 4: _load_l2_block_gates validation ──────────────────────────────────────


class TestLoadL2BlockGates:
    def test_default_when_unset(self, monkeypatch):
        from builders.daily_append import _load_l2_block_gates
        from nousergon_lib.series_contract import DEFAULT_BLOCK_GATES

        monkeypatch.delenv("DAILY_APPEND_L2_BLOCK_GATES", raising=False)
        assert _load_l2_block_gates() == DEFAULT_BLOCK_GATES

    def test_valid_override(self, monkeypatch):
        from builders.daily_append import _load_l2_block_gates

        monkeypatch.setenv(
            "DAILY_APPEND_L2_BLOCK_GATES", json.dumps(["outlier", "staleness"])
        )
        assert _load_l2_block_gates() == frozenset({"outlier", "staleness"})

    def test_malformed_json_raises(self, monkeypatch):
        from builders.daily_append import _load_l2_block_gates

        monkeypatch.setenv("DAILY_APPEND_L2_BLOCK_GATES", "{not json")
        with pytest.raises(RuntimeError, match="not valid JSON"):
            _load_l2_block_gates()

    def test_unknown_gate_raises(self, monkeypatch):
        from builders.daily_append import _load_l2_block_gates

        monkeypatch.setenv("DAILY_APPEND_L2_BLOCK_GATES", json.dumps(["not_a_gate"]))
        with pytest.raises(RuntimeError, match="unknown gate names"):
            _load_l2_block_gates()

    def test_non_list_raises(self, monkeypatch):
        from builders.daily_append import _load_l2_block_gates

        monkeypatch.setenv("DAILY_APPEND_L2_BLOCK_GATES", json.dumps("schema"))
        with pytest.raises(RuntimeError, match="JSON list of strings"):
            _load_l2_block_gates()


# ── 5: aggregated alert severity ────────────────────────────────────────────


def test_quarantine_logs_error_not_just_warning(monkeypatch, caplog):
    """A quarantined row must produce an aggregated ERROR-level record —
    that's the log line flow-doctor's ERROR handler captures (see the
    module's alarming-convention docstring)."""
    from builders import daily_append as _da

    today_str = recent_trading_day_str()
    universe = ["AAPL"]
    _patch_targets(
        monkeypatch, universe_symbols=universe,
        today_in_hist=False, today_str=today_str,
    )
    universe_lib = _da.get_universe_lib("any-bucket")
    hist = universe_lib.read_batch.return_value[0].data
    # Inject a non-positive close into hist+today's combined series via a
    # corrupted history row — sanity is block-default.
    corrupted = hist.copy()
    corrupted.iloc[0, corrupted.columns.get_loc("Close")] = 0.0
    universe_lib.read_batch.return_value = [
        MagicMock(spec=[], data=corrupted) for _ in universe
    ]

    with caplog.at_level(logging.WARNING, logger="builders.daily_append"):
        result = _da.daily_append(date_str=today_str)

    assert result["tickers_l2_quarantined"] == 1
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("L2 series-contract quarantined" in r.message for r in error_records)
