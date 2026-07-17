"""SPY-as-full-universe-member contract (additive producer change).

SPY became a held core position with the 2026-05-13 portfolio-optimizer
cutover. Every held-position code path (eod_reconcile #181, morning-planner
ATR #185) independently rediscovered "SPY isn't in `universe`" as a separate
production incident. The durable fix promotes SPY to a full `universe`
member (full OHLCV + engineered features) while keeping its Close-only
`macro` write during the transition.

The one-wrong-move risk (audit §E #6/#7): SPY must NOT be removed from
`_SKIP_TICKERS` to get it written to `universe` — `_SKIP_TICKERS` is also
the prune-protection / coverage-diff set, and SPY is never in
`constituents.json`, so dropping it there would make SPY a
`prune_delisted_tickers` candidate. The correct shape is a SEPARATE
additive predicate `_UNIVERSE_EXTRA` that ONLY widens the universe-write
candidate set. These tests pin that invariant so a future refactor can't
silently re-open the gap.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from builders import prune_delisted_tickers as _prune_mod
from features.compute import _SKIP_TICKERS, _UNIVERSE_EXTRA, _is_sector_etf

_REPO_ROOT = Path(__file__).resolve().parent.parent


# ── A. Constant-contract invariants ────────────────────────────────────────


def test_spy_is_a_universe_extra():
    assert "SPY" in _UNIVERSE_EXTRA


def test_universe_extra_members_stay_skip_protected():
    """Every _UNIVERSE_EXTRA member MUST also be in _SKIP_TICKERS.

    This is the load-bearing invariant. If a member is universe-extra but
    NOT skip-protected, prune_delisted_tickers (it's ∉ constituents.json)
    and the daily_append coverage-diff accounting break. Generalised so a
    future addition to _UNIVERSE_EXTRA can't violate it.
    """
    assert _UNIVERSE_EXTRA <= _SKIP_TICKERS, (
        f"_UNIVERSE_EXTRA members not skip-protected: "
        f"{sorted(_UNIVERSE_EXTRA - _SKIP_TICKERS)} — these would become "
        f"prune_delisted_tickers candidates (∉ constituents.json)"
    )


def test_spy_specifically_still_skip_protected():
    assert "SPY" in _SKIP_TICKERS


# ── B. Prune-protection behavioural guard (audit §E risk #6) ────────────────


def _stub_s3(*, constituents_tickers, weekly_date="2026-04-25"):
    s3 = MagicMock()
    pointer = json.dumps(
        {"date": weekly_date, "s3_prefix": f"market_data/weekly/{weekly_date}/"}
    ).encode()
    cons = json.dumps(
        {
            "date": weekly_date,
            "tickers": list(constituents_tickers),
            "sector_map": {t: "Industrials" for t in constituents_tickers},
        }
    ).encode()

    def fake_get_object(**kwargs):
        key = kwargs["Key"]
        if key == "market_data/latest_weekly.json":
            return {"Body": MagicMock(read=lambda: pointer)}
        if key.endswith("/constituents.json"):
            return {"Body": MagicMock(read=lambda: cons)}
        raise KeyError(key)

    s3.get_object.side_effect = fake_get_object
    return s3


def _stub_universe_lib(*, symbols, last_dates):
    lib = MagicMock()
    lib.list_symbols.return_value = symbols

    def _frame_for(symbol):
        idx = pd.DatetimeIndex([last_dates[symbol]])
        return pd.DataFrame({"Close": [100.0]}, index=idx)

    def fake_tail(symbol, n):
        if symbol not in last_dates:
            return MagicMock(data=pd.DataFrame())
        return MagicMock(data=_frame_for(symbol))

    def fake_read(symbol, **kwargs):
        if symbol not in last_dates:
            return MagicMock(data=pd.DataFrame())
        return MagicMock(data=_frame_for(symbol))

    lib.tail.side_effect = fake_tail
    lib.read.side_effect = fake_read
    return lib


def test_spy_in_universe_is_never_pruned_even_when_absent_and_stale(monkeypatch):
    """SPY ∉ constituents AND stale — the two-condition prune trigger — yet
    SPY must survive because it's _SKIP_TICKERS-protected. A genuinely
    delisted ticker in the same run IS pruned, proving the guard is
    specific to macro/universe-extra symbols, not a blanket no-prune.
    """
    s3 = _stub_s3(constituents_tickers=["AAPL", "MSFT"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "MSFT", "SPY", "HOLX"],
        last_dates={
            "AAPL": "2026-04-25",
            "MSFT": "2026-04-25",
            "SPY": "2026-04-06",  # stale AND absent from constituents
            "HOLX": "2026-04-06",  # genuinely delisted: stale + absent
        },
    )
    monkeypatch.setattr(
        _prune_mod, "boto3", MagicMock(client=lambda *a, **k: s3)
    )
    monkeypatch.setattr(
        _prune_mod, "get_universe_lib", lambda *a, **k: lib
    )
    monkeypatch.setattr(
        _prune_mod, "get_delisted_history_lib", lambda *a, **k: MagicMock()
    )
    # PR6: prune runs rename detection before deletion — neutralize the polygon
    # seam (HOLX is a genuine delist, no ticker_change) so this guard tests the
    # SKIP-protection path, not rename detection.
    no_rename_poly = MagicMock()
    no_rename_poly.get_ticker_events.return_value = []
    monkeypatch.setattr(_prune_mod, "polygon_client", lambda *a, **k: no_rename_poly)

    summary = _prune_mod.prune_delisted_tickers(
        absent_days=14, apply=True, today=pd.Timestamp("2026-04-28")
    )

    pruned = {p["ticker"] for p in summary["pruned"]}
    assert "SPY" not in pruned, "SPY must never be a prune candidate"
    assert "HOLX" in pruned, "genuinely delisted ticker should still prune"
    lib.delete.assert_called_once_with("HOLX")


# ── C. Universe-write predicate contract (audit §A5) ────────────────────────
#
# The two universe-write filters are inline in backfill()/daily_append().
# Replicate them here as the pinned contract AND assert the production
# source still references _UNIVERSE_EXTRA, so the replicated predicate
# can't silently drift from production (repo `assert <expr> in src`
# convention, cf. test_sf_ssm_pipefail_wiring.py).


def _backfill_admits(t, *, price_data, constituents_set):
    return (
        (t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA)
        and not _is_sector_etf(t)
        and price_data[t] is not None
        and (t in constituents_set or t in _UNIVERSE_EXTRA)
    )


def _daily_append_admits(t):
    return (
        t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA
    ) and not _is_sector_etf(t)


def test_backfill_universe_filter_admits_spy_despite_absent_constituents():
    price_data = {"AAPL": object(), "SPY": object(), "DELISTED": object()}
    constituents = {"AAPL"}  # SPY never appears in constituents.json
    assert _backfill_admits("AAPL", price_data=price_data, constituents_set=constituents)
    assert _backfill_admits("SPY", price_data=price_data, constituents_set=constituents)
    assert not _backfill_admits(
        "DELISTED", price_data=price_data, constituents_set=constituents
    )
    assert not _backfill_admits("VIX", price_data={"VIX": object()}, constituents_set=set())


def test_daily_append_stock_filter_admits_spy_not_other_macros():
    assert _daily_append_admits("AAPL")
    assert _daily_append_admits("SPY")
    assert not _daily_append_admits("VIX")
    assert not _daily_append_admits("XLK")  # sector ETF


@pytest.mark.parametrize(
    "rel_path,needle",
    [
        ("builders/backfill.py", "t in _UNIVERSE_EXTRA"),
        ("builders/daily_append.py", "t in _UNIVERSE_EXTRA"),
    ],
)
def test_production_filters_reference_universe_extra(rel_path, needle):
    """Guard against the replicated predicates above drifting from the
    real inline filters."""
    src = (_REPO_ROOT / rel_path).read_text()
    assert needle in src, (
        f"{rel_path} no longer references {needle!r} — the universe-write "
        f"filter changed; update _backfill_admits/_daily_append_admits and "
        f"re-verify the SPY-admission + prune-protection contract"
    )


# ── D. expected_tickers-scoping predicate contract (config-I2703) ───────────
#
# 2026-07-15 P0: the WRITE-path predicate above (_daily_append_admits) has
# always carved out _UNIVERSE_EXTRA correctly, but the freshness-scan and
# missing-from-closes checks in daily_append.py — which ALSO derive an
# "expected" scoped set from expected_tickers, to exclude genuine S&P
# churn-out stragglers — filtered on bare `_SKIP_TICKERS` with no
# `_UNIVERSE_EXTRA` carve-out. Net effect: SPY (in `_SKIP_TICKERS` by
# design) was silently excluded from expected_tickers' "must be fresh /
# must be in today's closes" scope on every run, logged identically to a
# genuine churn-out straggler ("S&P churn-out straggler, awaiting prune").
# A future SPY write outage would have gone undetected by the freshness
# gate. Fixed by aligning all THREE call sites (write / missing-from-closes
# / freshness-scan) on the identical carve-out predicate. These tests pin
# that invariant the same way section C pins the write-path predicate.


def _expected_set_admits(t: str) -> bool:
    """Mirrors the `expected_set` / `expected_stocks` comprehension in both
    `_scan_universe_and_emit_freshness_receipt` and the missing-from-closes
    check in builders/daily_append.py (identical predicate, kept in
    lockstep by the source-text guard below)."""
    stripped = t.lstrip("^")
    return (
        stripped not in _SKIP_TICKERS or stripped in _UNIVERSE_EXTRA
    ) and not _is_sector_etf(stripped)


def _closes_stock_keys_admits(t: str) -> bool:
    """Mirrors the `closes_stock_keys` comprehension (missing-from-closes
    check) in builders/daily_append.py — same predicate as
    `_daily_append_admits` above (write-path), kept in lockstep by the
    source-text guard below."""
    return (t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA) and not _is_sector_etf(t)


def test_expected_set_admits_spy_despite_skip_ticker_membership():
    """SPY must be admitted into the expected_tickers scoping set — it is
    a hard-pinned benchmark, never a churn-eligible S&P straggler, even
    though it lives in `_SKIP_TICKERS` (which ALSO serves the unrelated
    prune-protection role — see section A/B)."""
    assert _expected_set_admits("SPY")
    assert _expected_set_admits("^SPY")  # caret-stripped callers
    assert not _expected_set_admits("VIX"), "VIX is macro-only, never a universe member"
    assert not _expected_set_admits("XLK"), "sector ETFs never enter the universe scope"


def test_closes_stock_keys_admits_spy():
    """SPY present in today's daily_closes parquet must be recognized as
    a stock key — otherwise it always computes as 'missing from closes'
    even on days it's genuinely present (the inverse failure mode: a false
    hard-fail instead of a masked real one)."""
    assert _closes_stock_keys_admits("SPY")
    assert not _closes_stock_keys_admits("VIX")
    assert not _closes_stock_keys_admits("XLK")


@pytest.mark.parametrize(
    "needle",
    [
        # freshness scan's expected_set
        'if (t.lstrip("^") not in _SKIP_TICKERS or t.lstrip("^") in _UNIVERSE_EXTRA)',
        # missing-from-closes's closes_stock_keys
        "if (t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA)",
    ],
)
def test_daily_append_scoping_sites_reference_universe_extra(needle):
    """Guard against config-I2703 regressing: both expected_tickers-scoping
    call sites in daily_append.py (freshness scan + missing-from-closes)
    must carry the same _UNIVERSE_EXTRA carve-out the write-path predicate
    already had. Source-text pin, same convention as section C."""
    src = (_REPO_ROOT / "builders" / "daily_append.py").read_text()
    assert needle in src, (
        f"builders/daily_append.py no longer contains {needle!r} — an "
        f"expected_tickers-scoping site lost its _UNIVERSE_EXTRA carve-out. "
        f"This is the exact config-I2703 regression: SPY silently excluded "
        f"from the freshness/missing-from-closes safety net."
    )


# ── E. backfill.py regression-preflight sample predicate (config-I2704) ─────
#
# 2026-07-16: the third, lower-severity sibling of config-I2703. The
# universe-side sample-candidate predicate in backfill.py's
# `_assert_no_arctic_regression` (the preflight that blocks backfill from
# writing regressed data — see its docstring for the 2026-05-02 incident it
# guards against) filtered on bare `_SKIP_TICKERS` with no `_UNIVERSE_EXTRA`
# carve-out, so SPY could never be drawn into the sampled regression-check
# pool. Unlike I2703, this was never an active masked incident — the
# macro-side loop in the same function already checks SPY's `macro.SPY`
# Close-only row on every run, unconditionally — but a regression isolated
# to SPY's full-OHLCV+features `universe` row (with `macro.SPY` intact)
# would have silently skipped this preflight. Fixed by aligning this site on
# the identical carve-out predicate the other three sites in section C/D use.


def _regression_preflight_candidate_admits(t: str, *, arctic_syms: set[str]) -> bool:
    """Mirrors the `candidates` comprehension in backfill.py's
    `_assert_no_arctic_regression` (identical predicate, kept in lockstep by
    the source-text guard below)."""
    return (
        t in arctic_syms
        and (t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA)
        and not _is_sector_etf(t)
    )


def test_regression_preflight_candidates_admit_spy():
    """SPY must be eligible for backfill's regression-preflight sample pool
    — it is a hard-pinned benchmark, never a churn-eligible S&P straggler,
    even though it lives in `_SKIP_TICKERS`. This is eligibility only: the
    check is sampled, so SPY isn't guaranteed to be drawn every run."""
    arctic_syms = {"AAPL", "SPY", "VIX", "XLK"}
    assert _regression_preflight_candidate_admits("SPY", arctic_syms=arctic_syms)
    assert _regression_preflight_candidate_admits("AAPL", arctic_syms=arctic_syms)
    assert not _regression_preflight_candidate_admits("VIX", arctic_syms=arctic_syms), (
        "VIX is macro-only, never a universe member"
    )
    assert not _regression_preflight_candidate_admits("XLK", arctic_syms=arctic_syms), (
        "sector ETFs never enter the universe scope"
    )
    assert not _regression_preflight_candidate_admits("SPY", arctic_syms={"AAPL"}), (
        "not yet present in ArcticDB universe lib — can't be a regression candidate"
    )


def test_regression_preflight_site_references_universe_extra():
    """Guard against config-I2704 regressing: the regression-preflight
    sample-candidate predicate in backfill.py's `_assert_no_arctic_regression`
    must carry the same `_UNIVERSE_EXTRA` carve-out the other three sites
    (section C/D) already have. Source-text pin, same convention as those
    sections."""
    src = (_REPO_ROOT / "builders" / "backfill.py").read_text()
    needle = "and (t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA)"
    assert needle in src, (
        f"builders/backfill.py no longer contains {needle!r} — the "
        f"regression-preflight sample-candidate predicate lost its "
        f"_UNIVERSE_EXTRA carve-out. This is the exact config-I2704 "
        f"regression: SPY silently excluded from the backfill "
        f"regression-preflight sample pool."
    )
