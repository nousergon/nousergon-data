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

    def fake_tail(symbol, n):
        if symbol not in last_dates:
            return MagicMock(data=pd.DataFrame())
        idx = pd.DatetimeIndex([last_dates[symbol]])
        return MagicMock(data=pd.DataFrame({"Close": [100.0]}, index=idx))

    lib.tail.side_effect = fake_tail
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
