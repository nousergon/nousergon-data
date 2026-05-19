"""
Wave 4 (predictor/price_cache_slim deletion) — parity harness + consumer lock.

PR0b of the arc. **No production path is switched here.** This file does two
things:

1. **Parity harness wiring + teeth** — proves the lib v0.19.0 substrate
   (``alpha_engine_lib.arcticdb.load_universe_ohlcv`` +
   ``alpha_engine_lib.reconcile.reconcile_frame_dicts``) is importable in the
   data repo and that the cutover gate correctly PASSES when the ArcticDB
   read matches the slim-cache read and FAILS on any value divergence. The
   *live* S3-vs-ArcticDB observation is the PR4 Saturday-SF gate; this is the
   offline proof that the gate machinery is sound.

2. **Consumer-set lock (anti-drift guard)** — pins the exact set of
   ``predictor/price_cache_slim/`` touch-points so a future change that adds
   a new slim consumer fails this test until the Wave-4 inventory below is
   updated. Mirrors the orphaned-producer / prefix-invariant guard pattern.

AUDIT CORRECTION (2026-05-19): the ROADMAP entry claimed "3 active
production callers" (data ``collectors/macro.py``, backtester
``analysis/exit_timing.py``, dashboard ``health_checker.py``). The data-repo
audit for this PR found a **fourth, ROADMAP-missed** data-read consumer —
``features/compute.py::_load_prices_and_macro`` — which is the price+macro
base for the entire feature-compute pipeline (slim -> _apply_daily_delta ->
_extract_macro), a heavier consumer than macro-breadth. The canonical
inventory below is the corrected source of truth for PR1-PR4.
"""

from __future__ import annotations

import subprocess
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from alpha_engine_lib.reconcile import reconcile_frame_dicts

_REPO = Path(__file__).resolve().parent.parent


# ── Canonical Wave-4 consumer-set lock ───────────────────────────────────────
#
# Every production touch-point of the predictor/price_cache_slim/ tier.
# Cross-repo entries are documented (not testable from this repo); the
# data-repo entries are enforced by test_consumer_set_has_not_drifted below.

WAVE4_INVENTORY = {
    # producer/writer — DELETED in PR4
    "writer": [
        "collectors/slim_cache.py",            # builds & uploads the 2y slices
        "weekly_collector.py",                 # invokes slim_cache.collect()
    ],
    # loader API — DELETED in PR4 (after all readers migrated)
    "loader_api": [
        "store/parquet_loader.py",             # load_slim_cache + SLIM_CACHE_PREFIX
    ],
    # data-read consumers — MIGRATE to ArcticDB (lib load_universe_ohlcv) in PR1
    "data_read_consumers": [
        "collectors/macro.py",                 # :84 _compute_market_breadth (ROADMAP-known)
        "features/compute.py",                 # :360 _load_prices_and_macro (ROADMAP-MISSED)
    ],
    # doc/comment-only — cosmetic cleanup in PR4, no behaviour
    "doc_only": [
        "builders/backfill.py",
        "validators/price_validator.py",
    ],
    # cross-repo — handled by their own PRs, NOT testable from data repo:
    #   backtester analysis/exit_timing.py:201  -> migrate (has price_cache fallback)  [PR2]
    #   dashboard  health_checker.py:166        -> RETIRE freshness check, not migrate [PR3]
}

# Files allowed to mention slim in the data tree = every category above plus
# this guard test itself. Drift outside this set must fail loudly.
_ALLOWED_SLIM_FILES = {
    *WAVE4_INVENTORY["writer"],
    *WAVE4_INVENTORY["loader_api"],
    *WAVE4_INVENTORY["data_read_consumers"],
    *WAVE4_INVENTORY["doc_only"],
    "features/compute.py",  # also carries a docstring mention (line 17)
}


# ── 1. Parity harness wiring + teeth ─────────────────────────────────────────


def _slim_shaped(frames: dict) -> dict:
    """A load_slim_cache()-shaped dict: ticker -> tz-naive DatetimeIndex df."""
    return {k: v.copy() for k, v in frames.items()}


def _install_arctic_stub(monkeypatch, frames: dict):
    """Stub arcticdb so lib.load_universe_ohlcv reads `frames`."""

    class _Res:
        def __init__(self, data):
            self.data = data

    class _Lib:
        def list_symbols(self):
            return list(frames)

        def read(self, sym, date_range=None, columns=None):
            df = frames[sym]
            if date_range is not None:
                lo, hi = date_range
                df = df[(df.index >= lo) & (df.index <= hi)]
            if columns is not None:
                df = df[list(columns)]
            return _Res(df.copy())

    class _Arctic:
        def get_library(self, name):
            return _Lib()

    mod = types.ModuleType("arcticdb")
    mod.Arctic = lambda uri: _Arctic()
    monkeypatch.setitem(sys.modules, "arcticdb", mod)


def _build_universe(n=120):
    idx = pd.bdate_range(end=pd.Timestamp("2026-05-15"), periods=n)
    return {
        "AAA": pd.DataFrame(
            {"Close": [float(v) for v in range(100, 100 + n)], "Volume": [1] * n},
            index=idx,
        ),
        "BBB": pd.DataFrame(
            {"Close": [float(v) for v in range(200, 200 + n)], "Volume": [2] * n},
            index=idx,
        ),
    }


def test_parity_gate_passes_when_arctic_matches_slim(monkeypatch):
    """The real lib reader vs a slim-shaped dict of the same data -> PASS."""
    from alpha_engine_lib import arcticdb as ae_arctic

    universe = _build_universe()
    slim = _slim_shaped(universe)
    _install_arctic_stub(monkeypatch, universe)

    arctic = ae_arctic.load_universe_ohlcv(
        "alpha-engine-research", lookback_days=3650, end="2026-05-15"
    )

    report = reconcile_frame_dicts(slim, arctic, value_cols=("Close",))
    assert report.passed, report.summary()
    assert report.ticker_sets_match
    assert report.max_abs_value_delta == 0.0
    # JSON-able so PR4's Saturday-SF gate can emit it to the metrics surface.
    assert report.as_metrics()["passed"] is True


def test_parity_gate_has_teeth_on_value_divergence(monkeypatch):
    """A single perturbed cell must fail the gate and be located."""
    from alpha_engine_lib import arcticdb as ae_arctic

    universe = _build_universe()
    slim = _slim_shaped(universe)
    universe["BBB"] = universe["BBB"].copy()
    universe["BBB"].iloc[-1, universe["BBB"].columns.get_loc("Close")] += 0.01
    _install_arctic_stub(monkeypatch, universe)

    arctic = ae_arctic.load_universe_ohlcv(
        "alpha-engine-research", lookback_days=3650, end="2026-05-15"
    )

    report = reconcile_frame_dicts(slim, arctic, value_cols=("Close",), epsilon=1e-6)
    assert not report.passed
    assert report.n_cells_over_epsilon == 1
    assert report.worst_cell[0] == "BBB"


def test_boundary_rowcount_delta_is_reported_but_not_fatal(monkeypatch):
    """Slim 2y tail vs ArcticDB date_range read differ at the edge while
    agreeing on the overlap — the institutional nuance, exercised."""
    from alpha_engine_lib import arcticdb as ae_arctic

    universe = _build_universe()
    # slim only keeps the last 60 rows; arctic read returns all 120
    slim = {k: v.iloc[-60:].copy() for k, v in universe.items()}
    _install_arctic_stub(monkeypatch, universe)

    arctic = ae_arctic.load_universe_ohlcv(
        "alpha-engine-research", lookback_days=3650, end="2026-05-15"
    )

    report = reconcile_frame_dicts(slim, arctic, value_cols=("Close",))
    assert not report.rowcounts_match           # delta reported
    assert report.passed                        # overlap agrees -> gate PASS
    strict = reconcile_frame_dicts(
        slim, arctic, value_cols=("Close",), require_rowcount_match=True
    )
    assert not strict.passed                    # strict mode available


# ── 2. Consumer-set lock (anti-drift guard) ──────────────────────────────────


def test_consumer_set_has_not_drifted():
    """Fail if a data-repo file references the slim tier and is not in the
    locked Wave-4 inventory. Forces WAVE4_INVENTORY to stay the single
    source of truth for the migration arc."""
    out = subprocess.run(
        ["git", "-C", str(_REPO), "grep", "-lE",
         r"price_cache_slim|load_slim_cache|build_slim_cache", "--", "*.py"],
        capture_output=True, text=True,
    ).stdout

    found = set()
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("tests/") or "/.claude/" in line:
            continue
        found.add(line)

    unexpected = found - _ALLOWED_SLIM_FILES
    assert not unexpected, (
        f"New slim-cache touch-point(s) not in the Wave-4 inventory: "
        f"{sorted(unexpected)}. Add to WAVE4_INVENTORY (and decide: "
        f"migrate-to-ArcticDB consumer, or doc-only?) before merging."
    )

    # Inventory entries must still exist (catch a rename that silently
    # drops a consumer from the migration plan).
    for rel in sorted(_ALLOWED_SLIM_FILES):
        assert (_REPO / rel).exists(), f"Inventory file vanished: {rel}"
