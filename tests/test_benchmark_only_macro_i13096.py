"""Benchmark-only macro series (alpha-engine-config-I13096).

QQQ is collected daily the way SPY is (EOD macro Close, overwritten by the
settled polygon close at MorningEnrich) so the EOD reconcile can persist a
settled ``qqq_close`` beside ``spy_close`` and the perf tracker's ``vs_qqq``
leg stops carrying ``qqq_not_polygon_settled``. No feature reads it.

Four lists have to agree, and each omission fails differently:

* not requested daily       -> no bar in staging/daily_closes, nothing written;
* not skip-protected        -> computed and written to ``universe`` as a stock;
* not in the price cache    -> the weekly backfill has nothing to write;
* not a raw macro series    -> one ``macro`` row per weekday, no history
  (the alpha-engine-config-I9289 defect).
"""

from __future__ import annotations

from pathlib import Path

from builders.backfill import _RAW_MACRO_SERIES
from builders.daily_append import BENCHMARK_ONLY_MACRO_SYMBOLS
from collectors.prices import _ALWAYS_DOWNLOAD
from features.compute import (
    UNIVERSE_BENCHMARK_PROXIES,
    _SKIP_TICKERS,
    admits_universe_write,
)
from weekly_collector import _MACRO_DAILY_TICKERS

_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def test_qqq_is_declared_benchmark_only():
    assert "QQQ" in BENCHMARK_ONLY_MACRO_SYMBOLS


def test_benchmark_only_symbols_are_requested_daily():
    missing = sorted(set(BENCHMARK_ONLY_MACRO_SYMBOLS) - set(_MACRO_DAILY_TICKERS))
    assert not missing, f"never requested in _MACRO_DAILY_TICKERS: {missing}"


def test_benchmark_only_symbols_are_never_stocks():
    for sym in BENCHMARK_ONLY_MACRO_SYMBOLS:
        assert sym in _SKIP_TICKERS, f"{sym} would be computed as a stock"
        assert not admits_universe_write(sym), f"{sym} would be written to universe"
        assert sym not in UNIVERSE_BENCHMARK_PROXIES


def test_benchmark_only_symbols_keep_their_history():
    for sym in BENCHMARK_ONLY_MACRO_SYMBOLS:
        assert sym in _ALWAYS_DOWNLOAD, f"{sym} has no price-cache parquet"
        assert sym in _RAW_MACRO_SERIES, f"{sym} history never reaches ArcticDB macro"


def test_benchmark_only_write_is_best_effort():
    """A missing QQQ bar must never fail the daily run: the loop warns and
    continues, and never feeds ``macro_missing_from_closes`` (the hard-fail)."""
    src = _DAILY_APPEND.read_text()
    start = src.find("for sym in BENCHMARK_ONLY_MACRO_SYMBOLS:")
    assert start != -1
    end = src.find("# FRED-only macro series", start)
    body = src[start:end]
    assert "macro_missing_from_closes" not in body
    assert "raise" not in body
    assert "macro_updated.append(sym)" in body
