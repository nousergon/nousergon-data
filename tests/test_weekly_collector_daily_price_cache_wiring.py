"""Daily price_cache refresh wiring (config#2756).

``reference/price_cache/*.parquet`` previously only refreshed on the Saturday
``--phase 1`` run, so metron_market_data.collect_history's 1-trading-day
price_cache staleness gate (data#693) only cleared on Monday — Tue-Fri the
whole universe fell back to an independent yfinance fetch (~20% of the
intended dedup, not the ~95% assumed). These pin that ``_run_daily`` (the
Mon-Fri EOD path) now also runs ``prices.collect``, trading-day-exact
(see tests/test_prices_trading_day_stale.py), and that both call sites
thread ``run_date`` through as ``reference_date`` so staleness is evaluated
against the pipeline's own date, not a live ``datetime.now()`` call.
"""

from __future__ import annotations

from pathlib import Path

_WEEKLY_COLLECTOR = Path(__file__).parent.parent / "weekly_collector.py"


def _section(src: str, def_line: str) -> str:
    body = src.split(def_line)[1]
    next_def = body.find("\ndef ")
    return body if next_def == -1 else body[:next_def]


def _balanced_call(section: str, call_prefix: str) -> str:
    """Extract a full call's argument text, respecting nested parens —
    a naive ``split(")")`` truncates at the first nested ``price_cfg.get(...)``
    close-paren instead of the call's own."""
    start = section.index(call_prefix) + len(call_prefix)
    depth = 1
    i = start
    while depth > 0:
        if section[i] == "(":
            depth += 1
        elif section[i] == ")":
            depth -= 1
        i += 1
    return section[start:i - 1]


def test_run_daily_invokes_prices_collect():
    src = _WEEKLY_COLLECTOR.read_text()
    daily_section = _section(src, "def _run_daily(")
    assert "prices.collect(" in daily_section, (
        "weekly_collector._run_daily must refresh reference/price_cache/ "
        "on the Mon-Fri EOD path (config#2756) — without this, metron's "
        "1-trading-day price_cache staleness gate (data#693) only clears "
        "on Monday."
    )


def test_run_daily_prices_call_threads_reference_date():
    src = _WEEKLY_COLLECTOR.read_text()
    daily_section = _section(src, "def _run_daily(")
    prices_call = _balanced_call(daily_section, "prices.collect(")
    assert "reference_date=run_date" in prices_call, (
        "The daily prices.collect() call must pass reference_date=run_date "
        "so staleness is trading-day-exact against the pipeline's own date, "
        "not datetime.now() at call time."
    )


def test_run_daily_prices_call_uses_daily_staleness_threshold_config_key():
    src = _WEEKLY_COLLECTOR.read_text()
    daily_section = _section(src, "def _run_daily(")
    prices_call = _balanced_call(daily_section, "prices.collect(")
    assert "daily_staleness_threshold_days" in prices_call, (
        "The daily call must read its own config key (default 1 trading "
        "day) rather than reusing the weekly staleness_threshold_days "
        "default of 3 — the whole point of config#2756 is a tighter daily "
        "staleness bound matching metron's PRICE_CACHE_MAX_STALE_TRADING_DAYS."
    )


def test_run_phase1_prices_call_threads_reference_date():
    src = _WEEKLY_COLLECTOR.read_text()
    phase1_section = _section(src, "def _run_phase1(")
    prices_call = _balanced_call(phase1_section, "prices.collect(")
    assert "reference_date=run_date" in prices_call, (
        "The weekly (Saturday) prices.collect() call must also thread "
        "reference_date=run_date — the trading-day-exact staleness check "
        "(config#2756) needs an explicit reference to stay deterministic "
        "across re-runs."
    )
