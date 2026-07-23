"""Regression tests for config#2898: morning-arctic-append vs.
post-market-arctic-append built ``expected_tickers`` differently, and only
the evening path reliably included SPY.

Root cause: ``_run_morning_arctic_append`` never unioned its loaded
constituents with ``_MACRO_DAILY_TICKERS`` before calling ``daily_append``,
while the evening twin (``_run_daily_arctic_append``, via
``_load_daily_universe_tickers``) always did. PR849 / config-I2703's
``_UNIVERSE_EXTRA`` carve-out (see ``test_spy_universe_member.py``) only
PROTECTS a ticker that's already present in ``expected_tickers`` from being
filtered out — it can't rescue a ticker the morning caller never inserted
in the first place.

The fix: a single shared ``_augment_with_macro_daily_tickers`` helper that
every ``expected_tickers`` constructor routes through, so SPY (and the rest
of ``_MACRO_DAILY_TICKERS``) can't drift out of one caller while staying in
another. These tests pin (A) the constant contract, (B) that the two
call sites route through the shared helper (source-text-pin style, mirroring
``test_spy_universe_member.py``), and (C) the behavioral guarantee that
``_run_morning_arctic_append`` passes SPY into ``daily_append``'s
``expected_tickers`` even when the constituents loader returns a set
without it.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import weekly_collector
from weekly_collector import _MACRO_DAILY_TICKERS, _augment_with_macro_daily_tickers

_REPO_ROOT = Path(__file__).resolve().parent.parent


# ── A. Constant-contract invariant ──────────────────────────────────────────


def test_spy_in_macro_daily_tickers():
    assert "SPY" in _MACRO_DAILY_TICKERS


def test_augment_helper_unions_and_dedupes():
    # De-dupes: SPY passed in once, present once, order preserved from input
    # then macro list appended (dict.fromkeys semantics).
    result = _augment_with_macro_daily_tickers(["AAPL", "SPY"])
    assert result.count("SPY") == 1
    assert result[0] == "AAPL"
    assert "GLD" in result


def test_augment_helper_inserts_spy_when_absent():
    result = _augment_with_macro_daily_tickers(["AAPL", "MSFT"])
    assert "SPY" in result


# ── B. Source-text-pin: both call sites route through the shared helper ────
# (mirrors the section-C/D convention in test_spy_universe_member.py)


def test_load_daily_universe_tickers_routes_through_helper():
    src = (_REPO_ROOT / "weekly_collector.py").read_text()
    # Isolate the function body so this doesn't just match some unrelated call.
    start = src.index("def _load_daily_universe_tickers(")
    end = src.index("\ndef _run_daily(", start)
    body = src[start:end]
    assert "_augment_with_macro_daily_tickers(tickers)" in body, (
        "_load_daily_universe_tickers no longer routes through "
        "_augment_with_macro_daily_tickers — the single-chokepoint union "
        "invariant (config#2898) regressed."
    )
    assert "_MACRO_DAILY_TICKERS)" not in body.replace(
        "_augment_with_macro_daily_tickers(tickers)", ""
    ), "found a re-inlined _MACRO_DAILY_TICKERS union outside the helper"


def test_morning_arctic_append_routes_through_helper():
    src = (_REPO_ROOT / "weekly_collector.py").read_text()
    start = src.index("def _run_morning_arctic_append(")
    end = src.index("\ndef _run_chronic_gap_heal(", start)
    body = src[start:end]
    assert "_augment_with_macro_daily_tickers(tickers)" in body, (
        "_run_morning_arctic_append no longer unions its loaded tickers "
        "with _MACRO_DAILY_TICKERS via the shared helper — this is the "
        "exact config#2898 regression: SPY silently dropped from the "
        "morning caller's expected_tickers."
    )
    # The union must land BEFORE the daily_append(...) call, not after.
    augment_idx = body.index("_augment_with_macro_daily_tickers(tickers)")
    daily_append_idx = body.index("daily_append(\n")
    assert augment_idx < daily_append_idx, (
        "the macro-ticker union must happen before daily_append() is called, "
        "otherwise expected_tickers won't reflect it"
    )


# ── C. Behavioral guard: SPY reaches daily_append's expected_tickers ───────


def _stub_s3_constituents_loader(*, tickers_without_spy, weekly_date="2026-07-16"):
    """Patches builders._constituents_loader.load_constituents_for_run_date
    (imported inline inside _run_morning_arctic_append) to return a
    constituents set WITHOUT SPY — mirroring the live incident: SPY is never
    in constituents.json (see test_spy_universe_member.py's docstring)."""
    return MagicMock(return_value=(set(tickers_without_spy), weekly_date))


def test_morning_arctic_append_includes_spy_in_expected_tickers():
    """Functional guard: even when the constituents loader returns a set
    with no SPY (the real-world shape — SPY never appears in
    constituents.json), _run_morning_arctic_append must still pass SPY in
    the expected_tickers kwarg it hands to daily_append. This is the exact
    scenario from the 2026-07-17 incident log in config#2898: 'daily_append:
    2 ArcticDB stock symbols absent from expected_tickers ... [SATS, SPY]'."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-07-17", dry_run=True)

    import builders.daily_append  # noqa: F401  ensure importable before patching

    fake_loader = _stub_s3_constituents_loader(
        tickers_without_spy=["AAPL", "MSFT", "NVDA"]
    )
    captured = {}

    def fake_daily_append(**kwargs):
        captured.update(kwargs)
        return {"status": "ok_dry_run"}

    with patch(
        "builders._constituents_loader.load_constituents_for_run_date",
        fake_loader,
    ), patch("boto3.client", return_value=MagicMock()), patch(
        "builders.daily_append.daily_append", side_effect=fake_daily_append
    ):
        result = weekly_collector._run_morning_arctic_append(config, args)

    assert "expected_tickers" in captured, "daily_append was never called with expected_tickers"
    assert "SPY" in captured["expected_tickers"], (
        "SPY missing from expected_tickers passed to daily_append — the "
        "config#2898 regression: the morning path never unioned in "
        "_MACRO_DAILY_TICKERS the way the evening path always did"
    )
    assert result["status"] == "ok"


def test_morning_arctic_append_expected_tickers_matches_evening_macro_scope():
    """Cross-entrypoint parity: whatever _MACRO_DAILY_TICKERS members the
    evening path (_load_daily_universe_tickers) guarantees must ALSO be
    guaranteed by the morning path — the deliverable config#2898 explicitly
    asks for ('assert both entrypoints' constructed expected_tickers
    supersets are identical for _UNIVERSE_EXTRA/macro membership')."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-07-17", dry_run=True)

    import builders.daily_append  # noqa: F401  ensure importable before patching

    fake_loader = _stub_s3_constituents_loader(
        tickers_without_spy=["AAPL", "MSFT", "NVDA"]
    )
    morning_captured = {}

    def fake_daily_append(**kwargs):
        morning_captured.update(kwargs)
        return {"status": "ok_dry_run"}

    with patch(
        "builders._constituents_loader.load_constituents_for_run_date",
        fake_loader,
    ), patch("boto3.client", return_value=MagicMock()), patch(
        "builders.daily_append.daily_append", side_effect=fake_daily_append
    ):
        weekly_collector._run_morning_arctic_append(config, args)

    morning_scope = set(morning_captured["expected_tickers"])
    evening_macro_scope = set(_MACRO_DAILY_TICKERS)
    missing = evening_macro_scope - morning_scope
    assert not missing, (
        f"morning-arctic-append's expected_tickers is missing macro tickers "
        f"the evening path guarantees: {sorted(missing)}"
    )
