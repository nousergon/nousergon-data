"""Regression tests for the ``skip_if_exists`` flag on
``builders.daily_append.daily_append``.

Background (incident 2026-05-01):

The 4/27 VWAP migration was undone by a Saturday SF backfill on 4/30
that wrote every ticker without a VWAP column (yfinance source). The
following morning's MorningEnrich, then the post-market daily_append,
hit a column-position mismatch and the EOD SF failed.

After the immediate recovery (re-running migrate_universe_vwap +
daily_append manually), today's 5/1 row was already in ArcticDB. The
EOD SF rerun then timed out at the SSM 1200s ceiling because every
ticker hit the ``_write_row_backfill_safe`` "backfill" branch
(target_ts == existing.index.max()) → ``lib.write(combined,
prune_previous_versions=True)`` per ticker → 904 × ~1.5s ≈ 22 min.

The fix: a source-aware ``skip_if_exists`` flag. EOD post-market
(yfinance, immutable once written) passes True so re-runs are
microsecond no-ops. MorningEnrich (polygon must overwrite to apply
true VWAP) leaves it False (default).

These tests lock the contract:
1. ``skip_if_exists=True`` + today's row in hist → skip (no write call).
2. ``skip_if_exists=True`` + today's row missing → write proceeds.
3. ``skip_if_exists=False`` + today's row in hist → write proceeds (overwrite).
4. EOD path in ``weekly_collector._run_daily`` passes ``skip_if_exists=True``.
5. MorningEnrich path in ``weekly_collector._run_morning_enrich`` does NOT
   pass ``skip_if_exists=True`` (default False preserves polygon overwrite).
6. CLI ``--skip-if-exists`` flag wires through to the function.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Shared "today" anchor (config#1630): promoted out of this module into
# tests/conftest.py so every date-driven daily_append test — not just the
# two originally cross-importing this file — has one discoverable
# chokepoint. See conftest.recent_trading_day_str's docstring for the
# incident history (2026-05-04, 2026-06-22, 2026-07-03).
from tests.conftest import recent_trading_day_str


_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"
_WEEKLY_COLLECTOR = Path(__file__).parent.parent / "weekly_collector.py"


@pytest.fixture(autouse=True)
def _disable_factor_momentum_daily(monkeypatch):
    # L4484: isolate these tests from the daily factor-momentum second pass —
    # its extra read_batch/update_batch calls would perturb the skip/call-count
    # assertions here. The pass has its own tests (test_factor_momentum.py +
    # test_daily_append_factor_momentum.py).
    monkeypatch.setenv("FACTOR_MOMENTUM_DAILY_ENABLED", "false")
    monkeypatch.setenv("FACTOR_LOADING_ZSCORE_DAILY_ENABLED", "false")


def _stub_closes(tickers: list[str]) -> dict:
    """Minimal daily_closes shape: per-ticker {Open,High,Low,Close,Volume,VWAP}."""
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
):
    """Patch surface mirroring test_daily_append_missing_from_closes.py.

    ``today_in_hist`` controls whether the per-ticker hist frames include
    a row at ``today_str`` — that's the trigger for the skip path.
    """
    from builders import daily_append as _da

    macro_keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]
    sector_etfs = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
                   "XLP", "XLRE", "XLU", "XLV", "XLY"]
    closes = _stub_closes(universe_symbols + macro_keys + sector_etfs)

    hist_dates = pd.date_range("2024-01-01", periods=300, freq="B").tolist()
    if today_in_hist:
        hist_dates.append(pd.Timestamp(today_str))
    hist_index = pd.DatetimeIndex(hist_dates)
    hist_df = pd.DataFrame(
        {
            "Open": 100.0, "High": 101.0, "Low": 99.0,
            "Close": 100.0, "Volume": 1_000_000, "VWAP": 100.5,
        },
        index=hist_index,
    )
    hist_df.index.name = "date"

    universe_lib = MagicMock()
    universe_lib.list_symbols.return_value = universe_symbols
    universe_lib.read_batch.return_value = [
        MagicMock(spec=[], data=hist_df.copy()) for _ in universe_symbols
    ]
    today_row = pd.DataFrame(
        {"Close": [100.0]}, index=[pd.Timestamp(today_str)],
    )
    universe_lib.tail.return_value = MagicMock(spec=[], data=today_row)

    macro_lib = MagicMock()
    macro_df = hist_df[["Close"]].copy()
    macro_df.loc[pd.Timestamp(today_str)] = 100.0
    macro_lib.read.return_value = MagicMock(data=macro_df)
    macro_lib.list_symbols.return_value = sector_etfs

    monkeypatch.setattr(_da, "_load_daily_closes", lambda *a, **k: closes)
    monkeypatch.setattr(_da, "_load_sector_map", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_fundamentals", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_alternative", lambda *a, **k: {})
    monkeypatch.setattr(_da, "get_universe_lib", lambda *a, **k: universe_lib)
    monkeypatch.setattr(_da, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_da, "_emit_missing_from_closes_metric", MagicMock())

    from features.feature_engineer import FEATURES

    def _fake_compute_features(combined, **_):
        out = combined.copy()
        for f in list(FEATURES)[:3]:
            out[f] = 0.5
        return out

    monkeypatch.setattr(_da, "compute_features", _fake_compute_features)

    write_calls: list[tuple[str, str]] = []

    def _spy_write(lib, sym, df, existing_series=None):
        # Macro path still uses _write_row_backfill_safe per-symbol — universe
        # path was refactored to update_batch in PR #153 (2026-05-05).
        which = "universe" if lib is universe_lib else "macro"
        write_calls.append((which, sym))
        return "append"

    monkeypatch.setattr(_da, "_write_row_backfill_safe", _spy_write)

    # Universe path now uses update_batch / write_batch — capture
    # payload symbols + return one MagicMock VersionedItem per payload
    # so the aggregation loop's `for payload, result in zip(...)` walks
    # them and increments n_ok / n_partial correctly.
    def _spy_update_batch(payloads, **kwargs):
        for p in payloads:
            write_calls.append(("universe", p.symbol))
        return [MagicMock(symbol=p.symbol) for p in payloads]

    def _spy_write_batch(payloads, **kwargs):
        # Same "universe" label as update_batch — the test's invariant
        # is "ticker hit the write path", regardless of update vs backfill.
        for p in payloads:
            write_calls.append(("universe", p.symbol))
        return [MagicMock(symbol=p.symbol) for p in payloads]

    universe_lib.update_batch.side_effect = _spy_update_batch
    universe_lib.write_batch.side_effect = _spy_write_batch

    mock_s3 = MagicMock()
    monkeypatch.setattr("builders.daily_append.boto3.client", lambda *a, **k: mock_s3)

    return universe_lib, macro_lib, write_calls


# ── 1. Functional behavior of skip_if_exists ──────────────────────────────────


def test_skip_if_exists_true_skips_when_today_in_hist(monkeypatch):
    """The whole point: re-runs short-circuit when today's row already
    lives in ArcticDB. No per-ticker write call, no compute_features
    spend, just a microsecond ``today_ts in hist.index`` check."""
    from builders.daily_append import daily_append

    # Anchor to the most recent NYSE *trading* day (see
    # conftest.recent_trading_day_str): raw "now" rots when hardcoded (2026-05-04)
    # and detonates the config#1572 phantom-session gate on weekends and
    # market holidays (2026-07-03). The trading-day anchor dodges both.
    today_str = recent_trading_day_str()
    universe = ["AAPL", "MSFT", "GOOGL"]
    _, _, write_calls = _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=True,
        today_str=today_str,
    )

    result = daily_append(date_str=today_str, skip_if_exists=True)

    assert result["status"] == "ok"
    assert result["tickers_skipped"] == len(universe), (
        f"All {len(universe)} tickers should skip when their hist already "
        f"contains today_ts; got tickers_skipped={result['tickers_skipped']}, "
        f"tickers_appended={result['tickers_appended']}."
    )
    assert result["tickers_appended"] == 0
    universe_writes = [w for w in write_calls if w[0] == "universe"]
    assert universe_writes == [], (
        f"No per-ticker universe write should fire on skip path; "
        f"got: {universe_writes}"
    )


def test_skip_if_exists_true_writes_when_today_missing(monkeypatch):
    """Inverse case: hist has yesterday only. skip_if_exists=True must
    NOT block writes — that would be a regression to the 2026-04-18
    silent-skip bug for tickers with genuinely-missing today rows."""
    from builders.daily_append import daily_append

    # Anchor to the most recent NYSE *trading* day (see
    # conftest.recent_trading_day_str): raw "now" rots when hardcoded (2026-05-04)
    # and detonates the config#1572 phantom-session gate on weekends and
    # market holidays (2026-07-03). The trading-day anchor dodges both.
    today_str = recent_trading_day_str()
    universe = ["AAPL", "MSFT"]
    _, _, write_calls = _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=False,
        today_str=today_str,
    )

    result = daily_append(date_str=today_str, skip_if_exists=True)

    assert result["status"] == "ok"
    assert result["tickers_appended"] == len(universe), (
        f"All {len(universe)} tickers should write when hist lacks today_ts "
        f"even with skip_if_exists=True; got: {result}"
    )
    universe_writes = [w for w in write_calls if w[0] == "universe"]
    assert len(universe_writes) == len(universe)


def test_skip_if_exists_false_writes_even_when_today_in_hist(monkeypatch):
    """MorningEnrich contract: polygon refresh MUST overwrite yfinance's
    NaN-VWAP row. Default (False) preserves the always-overwrite path
    that the 2026-04-18 commit introduced for the polygon-label incident."""
    from builders.daily_append import daily_append

    # Anchor to the most recent NYSE *trading* day (see
    # conftest.recent_trading_day_str): raw "now" rots when hardcoded (2026-05-04)
    # and detonates the config#1572 phantom-session gate on weekends and
    # market holidays (2026-07-03). The trading-day anchor dodges both.
    today_str = recent_trading_day_str()
    universe = ["AAPL", "MSFT"]
    _, _, write_calls = _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=True,
        today_str=today_str,
    )

    # Default skip_if_exists=False — MorningEnrich path.
    result = daily_append(date_str=today_str)

    assert result["status"] == "ok"
    assert result["tickers_appended"] == len(universe), (
        f"skip_if_exists=False (default) must overwrite even when today_ts "
        f"is in hist; got tickers_skipped={result['tickers_skipped']}."
    )
    universe_writes = [w for w in write_calls if w[0] == "universe"]
    assert len(universe_writes) == len(universe), (
        "Every ticker must hit the write path when skip_if_exists=False."
    )


def test_skip_if_exists_default_is_false():
    """Default must preserve the 2026-04-18 always-overwrite contract.
    Flipping the default would silently make MorningEnrich skip tickers
    with stale yfinance VWAP — masking polygon refresh failures."""
    import inspect
    from builders.daily_append import daily_append

    sig = inspect.signature(daily_append)
    assert sig.parameters["skip_if_exists"].default is False, (
        "Default must be False — MorningEnrich (polygon overwrite) is "
        "the load-bearing path. EOD callers explicitly opt in."
    )


# ── 2. Call-site wiring ───────────────────────────────────────────────────────


def test_eod_path_passes_skip_if_exists_true():
    """``weekly_collector._run_daily`` must pass ``skip_if_exists=True``
    for the EOD post-market re-run path. Yfinance closes are immutable
    once written; re-runs hit the slow backfill path otherwise."""
    src = _WEEKLY_COLLECTOR.read_text()
    # Find the daily_append() call inside _run_daily — the second call
    # site in the file (the first is _run_morning_enrich).
    daily_section = src.split("def _run_daily(")[1] if "_run_daily" in src else ""
    assert "skip_if_exists=True" in daily_section, (
        "EOD weekly_collector._run_daily must pass skip_if_exists=True "
        "to daily_append — without it the EOD SF re-run will time out at "
        "the SSM 1200s ceiling on the lib.write backfill path "
        "(2026-05-01 incident)."
    )


def test_morning_enrich_path_does_not_pass_skip_if_exists_true():
    """``weekly_collector._run_morning_enrich`` must NOT pass
    ``skip_if_exists=True``. Polygon's morning refresh exists precisely
    to overwrite yfinance's NaN VWAP — passing the skip flag would
    silently regress to the pre-polygon era."""
    src = _WEEKLY_COLLECTOR.read_text()
    # Isolate the _run_morning_enrich function body.
    if "_run_morning_enrich" in src:
        morning_section = src.split("def _run_morning_enrich(")[1]
        # Stop at the next top-level def.
        next_def = morning_section.find("\ndef ")
        if next_def != -1:
            morning_section = morning_section[:next_def]
    else:
        morning_section = ""
    assert "skip_if_exists=True" not in morning_section, (
        "MorningEnrich must leave skip_if_exists at the default (False) — "
        "polygon refresh's purpose is to overwrite the EOD yfinance row. "
        "Passing True here would silently regress the Phase 7 VWAP "
        "centralization contract."
    )


# ── 3. CLI wiring ─────────────────────────────────────────────────────────────


def test_cli_skip_if_exists_flag_present():
    """The CLI must expose ``--skip-if-exists`` so the SSM script in the
    EOD SF can opt in without a code change to the wrapper."""
    src = _DAILY_APPEND.read_text()
    assert '"--skip-if-exists"' in src, (
        "argparse must register --skip-if-exists; the EOD SSM step calls "
        "this CLI directly (`python -m builders.daily_append ...`)."
    )
    assert "skip_if_exists=args.skip_if_exists" in src, (
        "main() must wire args.skip_if_exists into the daily_append() call."
    )


def test_eod_ssm_script_has_no_redundant_daily_append():
    """The deployed SSM script for the EOD SF must not invoke
    ``python -m builders.daily_append`` after ``weekly_collector --daily``.

    History: the EOD SSM had two python invocations back-to-back —
    ``weekly_collector --daily --only daily_closes`` followed by a bare
    ``python -m builders.daily_append``. The second one was redundant
    (``_run_daily`` runs ``daily_closes`` + ``feature_store`` +
    ``daily_append`` regardless of ``--only``, so the second invocation
    just ran daily_append again — without ``skip_if_exists`` exposed via
    a flag, so on re-runs it took the slow backfill path on every ticker
    and timed out the SSM 1200s ceiling on the 2026-05-01 EOD recovery).

    Locks the simplification: a single ``weekly_collector --daily``
    invocation is canonical; the redundant second call must not return.
    """
    sf_eod = Path(__file__).parent.parent / "infrastructure" / "step_function_eod.json"
    deploy_sh = Path(__file__).parent.parent / "infrastructure" / "update_eod_pipeline_sf.sh"
    for src_path in (sf_eod, deploy_sh):
        if not src_path.exists():
            continue
        src = src_path.read_text()
        # The bare CLI invocation appearing as an SSM command line is the
        # redundant pattern. Allow the substring in comments / other
        # contexts but not as an executable command (`tee` marks the SSM
        # runner pattern).
        executable_lines = [
            line for line in src.splitlines()
            if "python -m builders.daily_append" in line
            and "tee" in line
        ]
        assert executable_lines == [], (
            f"{src_path.name} still contains redundant invocation(s): "
            f"{executable_lines}. weekly_collector._run_daily already "
            f"runs daily_append; the second SSM call is dead weight and "
            f"reintroduces the 2026-05-01 timeout regression."
        )
