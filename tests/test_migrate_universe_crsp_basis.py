"""Tests for builders/migrate_universe_crsp_basis.py + the additive CRSP schema.

Corporate-actions PR7-7a (epic config#1433 / config#1434). The migration is the
OFFLINE, build-the-evidence step: it reconstructs the universe on the CRSP basis
(Close = split-adjusted LEVEL, NEW total_return_close = split-adjusted +
dividend-back-adjusted) into a SCRATCH ArcticDB library and emits a per-ticker
reconciliation report that FAILS LOUD on any unexplained residual.

These tests pin:
  * schema: total_return_close lands immediately AFTER Close in the canonical
    order; to_arctic_canonical accepts it; absent → live layout is unchanged.
  * get_scratch_universe_lib refuses the live library names (never writes live).
  * reconstruct_basis: split-adjusted Close LEVEL + total_return_close on a
    ticker with a KNOWN split AND KNOWN dividend (TR == hand-computed
    yfinance-auto_adjust-equivalent within tol).
  * compute_features(close_col=...) shim feeds the chosen close column.
  * reconcile: a clean ticker is within_tol; an injected missing-dividend /
    wrong-split residual is OUT-OF-TOL + unexplained.
  * orchestration: writes go to the SCRATCH lib (never universe); a clean run
    succeeds + is idempotent; an unexplained residual RAISES (fail-loud).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import corporate_actions as ca
from store.arctic_store import (
    OHLCV_COLS,
    PROVENANCE_COL,
    TOTAL_RETURN_COL,
    get_scratch_universe_lib,
    to_arctic_canonical,
)
from builders import migrate_universe_crsp_basis as m
from builders.migrate_universe_crsp_basis import (
    ReconcileRecord,
    reconcile_total_return,
    reconstruct_basis,
)


# ── schema: total_return_close canonical placement ───────────────────────────


def test_total_return_close_placed_immediately_after_close():
    feats = ["rsi_14", "momentum_20d"]
    cols = list(OHLCV_COLS) + [TOTAL_RETURN_COL, PROVENANCE_COL] + feats
    idx = pd.date_range("2024-01-01", periods=4, freq="B")
    df = pd.DataFrame({c: np.linspace(1.0, 2.0, 4) for c in cols}, index=idx)
    df[PROVENANCE_COL] = "yfinance"

    out = to_arctic_canonical(df, features=feats)
    expected = [
        "Open", "High", "Low", "Close", TOTAL_RETURN_COL, "Volume", "VWAP",
        PROVENANCE_COL, *feats,
    ]
    assert list(out.columns) == expected
    # total_return_close is adjacent to Close.
    assert out.columns[out.columns.get_loc("Close") + 1] == TOTAL_RETURN_COL


def test_canonical_unchanged_when_total_return_close_absent():
    """A live-universe-shaped frame (no total_return_close) is laid out exactly
    as before — the schema change is additive."""
    feats = ["rsi_14", "momentum_20d"]
    cols = list(OHLCV_COLS) + [PROVENANCE_COL] + feats
    idx = pd.date_range("2024-01-01", periods=4, freq="B")
    df = pd.DataFrame({c: np.linspace(1.0, 2.0, 4) for c in cols}, index=idx)
    df[PROVENANCE_COL] = "yfinance"

    out = to_arctic_canonical(df, features=feats)
    assert list(out.columns) == list(OHLCV_COLS) + [PROVENANCE_COL] + feats
    assert TOTAL_RETURN_COL not in out.columns


# ── get_scratch_universe_lib refuses live names ──────────────────────────────


def test_get_scratch_universe_lib_refuses_live_universe():
    with pytest.raises(ValueError, match="universe"):
        get_scratch_universe_lib("universe")


def test_get_scratch_universe_lib_refuses_macro():
    with pytest.raises(ValueError, match="macro"):
        get_scratch_universe_lib("macro")


# ── reconstruct_basis: split LEVEL + total_return_close derivation ────────────


def _raw_split_div_frame():
    """Raw (unadjusted) frame: a 2-for-1 forward split at index 4 (price halves
    100→50) and a $0.50 dividend ex at index 2."""
    idx = pd.bdate_range("2026-06-01", periods=6)
    raw = pd.DataFrame(
        {"Close": [100.0, 100.0, 100.0, 100.0, 50.0, 50.0],
         "Volume": [1e6] * 6},
        index=idx,
    )
    split = ca.CorporateAction.from_split("X", idx[4].strftime("%Y-%m-%d"), 1, 2)
    div = ca.CorporateAction.from_dividend("X", idx[2].strftime("%Y-%m-%d"), 0.5, "CD")
    return raw, [split], [div]


def test_reconstruct_basis_close_is_split_adjusted_level():
    raw, splits, divs = _raw_split_div_frame()
    out, applied = reconstruct_basis("X", raw, splits, divs)
    # 2-for-1: pre rows (0-3) ×0.5 → flat split-adjusted level 50.
    assert list(out["Close"].to_numpy()) == pytest.approx([50.0] * 6)
    assert any(r["status"] == "applied" for r in applied)


def test_reconstruct_basis_total_return_close_back_adjusts_dividend():
    raw, splits, divs = _raw_split_div_frame()
    out, _ = reconstruct_basis("X", raw, splits, divs)
    # total_return_close = split-adjusted close, rows<ex_div (0,1) ×0.99
    # (close_prev = split-adj 50, $0.50 div → 1 - 0.5/50 = 0.99).
    assert TOTAL_RETURN_COL in out.columns
    assert list(out[TOTAL_RETURN_COL].to_numpy()) == pytest.approx(
        [49.5, 49.5, 50.0, 50.0, 50.0, 50.0]
    )
    # Close (the LEVEL) is NOT mutated by the dividend back-adjust.
    assert list(out["Close"].to_numpy()) == pytest.approx([50.0] * 6)


def test_reconstruct_basis_matches_yfinance_autoadjust_within_tol():
    """The derived total_return_close equals the yfinance auto_adjust-equivalent
    total-return close (split + dividend adjusted) up to tol — the whole premise
    of the migration (both are total-return; the new one is polygon-authoritative)."""
    raw, splits, divs = _raw_split_div_frame()
    out, _ = reconstruct_basis("X", raw, splits, divs)
    # Hand-computed yfinance auto_adjust close for the same actions.
    yf_autoadjust = pd.Series(
        [49.5, 49.5, 50.0, 50.0, 50.0, 50.0], index=out.index,
    )
    rec = reconcile_total_return(
        "X", out[TOTAL_RETURN_COL], yf_autoadjust,
        split_actions=splits, dividend_actions=divs, rel_tol=0.02,
    )
    assert rec.status == "within_tol"
    assert rec.explained is True


# ── compute_features close_col shim ──────────────────────────────────────────


def test_compute_features_uses_close_col():
    from features.feature_engineer import compute_features

    idx = pd.bdate_range("2024-01-01", periods=60)
    close = pd.Series(np.linspace(100.0, 130.0, 60), index=idx)
    # A NON-proportional basis (like a compounding dividend back-adjust) — a
    # constant scale factor would cancel in ratio features, so vary it.
    tr = close * np.linspace(0.8, 1.0, 60)
    df = pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99,
         "Close": close, "Volume": 1e6, TOTAL_RETURN_COL: tr},
        index=idx,
    )
    on_close = compute_features(df, close_col="Close")
    on_tr = compute_features(df, close_col=TOTAL_RETURN_COL)
    # price_vs_ma50 is a close-derived feature; the two bases must differ.
    a = on_close["price_vs_ma50"].dropna().to_numpy()
    b = on_tr["price_vs_ma50"].dropna().to_numpy()
    assert a.size and b.size
    assert not np.allclose(a, b)


def test_compute_features_missing_close_col_raises():
    from features.feature_engineer import compute_features

    idx = pd.bdate_range("2024-01-01", periods=5)
    df = pd.DataFrame({"Close": np.linspace(1, 2, 5), "Volume": 1e6}, index=idx)
    with pytest.raises(KeyError, match="total_return_close"):
        compute_features(df, close_col=TOTAL_RETURN_COL)


# ── reconcile classification ─────────────────────────────────────────────────


def test_reconcile_clean_ticker_within_tol():
    idx = pd.bdate_range("2026-06-01", periods=6)
    new = pd.Series([49.5, 49.5, 50.0, 50.0, 50.0, 50.0], index=idx)
    old = new * 1.001  # 0.1% feed rounding
    rec = reconcile_total_return("X", new, old, rel_tol=0.02)
    assert rec.status == "within_tol"
    assert rec.explained is True


def test_reconcile_missing_dividend_is_out_of_tol_unexplained():
    """yfinance applied an extra/larger dividend our registry lacks → the old
    total-return Close sits below ours pre-ex by more than tol."""
    idx = pd.bdate_range("2026-06-01", periods=6)
    new = pd.Series([49.5, 49.5, 50.0, 50.0, 50.0, 50.0], index=idx)
    old = pd.Series([47.0, 47.0, 50.0, 50.0, 50.0, 50.0], index=idx)  # ~5% off pre-ex
    rec = reconcile_total_return("X", new, old, rel_tol=0.02)
    assert rec.status == "out_of_tol"
    assert rec.explained is False
    assert rec.max_rel_dev > 0.02


def test_reconcile_wrong_split_is_out_of_tol_unexplained():
    """A doubled/mis-ratio'd split leaves a large boundary divergence."""
    idx = pd.bdate_range("2026-06-01", periods=6)
    new = pd.Series([50.0] * 6, index=idx)
    old = pd.Series([100.0, 100.0, 100.0, 100.0, 50.0, 50.0], index=idx)  # split not applied
    rec = reconcile_total_return("X", new, old, rel_tol=0.02)
    assert rec.status == "out_of_tol"
    assert rec.explained is False


def test_reconcile_known_divergence_is_explained():
    idx = pd.bdate_range("2026-06-01", periods=6)
    new = pd.Series([49.5, 49.5, 50.0, 50.0, 50.0, 50.0], index=idx)
    old = pd.Series([47.0, 47.0, 50.0, 50.0, 50.0, 50.0], index=idx)
    rec = reconcile_total_return("X", new, old, rel_tol=0.02, known_divergence=True)
    assert rec.status == "out_of_tol"
    assert rec.explained is True


def test_reconcile_no_overlap_is_unexplained():
    new = pd.Series([50.0] * 3, index=pd.bdate_range("2026-06-01", periods=3))
    old = pd.Series([50.0] * 3, index=pd.bdate_range("2020-06-01", periods=3))
    rec = reconcile_total_return("X", new, old, rel_tol=0.02)
    assert rec.status == "no_overlap"
    assert rec.explained is False


# ── orchestration: scratch-only writes, idempotency, fail-loud ───────────────


class _FakePolygon:
    """Per-ticker polygon double: get_splits / get_dividends return the raw
    polygon event-dict shapes corporate_actions.get_splits/get_dividends parse."""

    def __init__(self, splits: dict, dividends: dict):
        self._splits = splits
        self._dividends = dividends

    def get_splits(self, ticker):
        return self._splits.get(ticker, [])

    def get_dividends(self, ticker):
        return self._dividends.get(ticker, [])


def _patch_orchestration(monkeypatch, *, old_closes: dict, scratch_lib=None):
    """Wire the migration's live (read-only) universe lib, scratch lib, and s3
    audit so the orchestration runs fully in-memory."""
    live_lib = MagicMock()
    live_lib.list_symbols.return_value = list(old_closes.keys())

    def _read(ticker):
        result = MagicMock()
        result.data = pd.DataFrame({"Close": old_closes[ticker]})
        return result

    live_lib.read.side_effect = _read

    scratch = scratch_lib if scratch_lib is not None else MagicMock()

    monkeypatch.setattr(m, "get_universe_lib", lambda *a, **k: live_lib)
    monkeypatch.setattr(m, "get_scratch_universe_lib", lambda *a, **k: scratch)
    monkeypatch.setattr(m, "boto3", MagicMock())
    return live_lib, scratch


def _clean_setup():
    """A single clean ticker whose reconstruction matches its old yfinance Close."""
    raw, splits, divs = _raw_split_div_frame()
    raw_frames = {"X": raw}
    idx = raw.index
    old_closes = {"X": pd.Series([49.5, 49.5, 50.0, 50.0, 50.0, 50.0], index=idx)}
    client = _FakePolygon(
        splits={"X": [{"execution_date": idx[4].strftime("%Y-%m-%d"),
                       "split_from": 1, "split_to": 2}]},
        dividends={"X": [{"ex_dividend_date": idx[2].strftime("%Y-%m-%d"),
                          "cash_amount": 0.5, "dividend_type": "CD"}]},
    )
    return raw_frames, old_closes, client


def test_orchestration_dry_run_clean_succeeds_no_writes(monkeypatch):
    raw_frames, old_closes, client = _clean_setup()
    live_lib, scratch = _patch_orchestration(monkeypatch, old_closes=old_closes)

    result = m.migrate_universe_crsp_basis(
        apply=False,
        raw_fetch=lambda t: raw_frames[t],
        client=client,
        rel_tol=0.02,
        workers=1,
    )
    assert result["status"] == "ok"
    assert result["within_tol_count"] == 1
    assert result["unexplained_count"] == 0
    assert result["written_count"] == 0
    scratch.write.assert_not_called()
    # The live universe lib was only ever READ, never written.
    live_lib.write.assert_not_called()


def test_orchestration_fail_loud_on_unexplained_residual(monkeypatch):
    raw_frames, old_closes, client = _clean_setup()
    # Corrupt the old Close so the reconstruction diverges beyond tol with no
    # acknowledged divergence → must FAIL LOUD.
    old_closes["X"] = pd.Series([40.0, 40.0, 50.0, 50.0, 50.0, 50.0],
                                index=raw_frames["X"].index)
    live_lib, scratch = _patch_orchestration(monkeypatch, old_closes=old_closes)

    with pytest.raises(RuntimeError, match="FAILED LOUD"):
        m.migrate_universe_crsp_basis(
            apply=False,
            raw_fetch=lambda t: raw_frames[t],
            client=client,
            rel_tol=0.02,
            workers=1,
        )
    # Live lib never written even on the failing path.
    live_lib.write.assert_not_called()


def test_orchestration_known_divergence_does_not_fail(monkeypatch):
    raw_frames, old_closes, client = _clean_setup()
    old_closes["X"] = pd.Series([40.0, 40.0, 50.0, 50.0, 50.0, 50.0],
                                index=raw_frames["X"].index)
    _patch_orchestration(monkeypatch, old_closes=old_closes)

    result = m.migrate_universe_crsp_basis(
        apply=False,
        raw_fetch=lambda t: raw_frames[t],
        client=client,
        rel_tol=0.02,
        known_divergence_tickers=frozenset({"X"}),
        workers=1,
    )
    assert result["status"] == "ok"
    assert result["out_of_tol_count"] == 1
    assert result["unexplained_count"] == 0


def test_orchestration_apply_writes_scratch_lib_only(monkeypatch, tmp_path):
    """Apply path: the reconstructed series is written to a REAL (LMDB) scratch
    library, and the live universe lib is never written."""
    adb = pytest.importorskip("arcticdb")
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    scratch = ac.get_library("universe_crsp", create_if_missing=True)

    raw_frames, old_closes, client = _clean_setup()
    live_lib, _ = _patch_orchestration(
        monkeypatch, old_closes=old_closes, scratch_lib=scratch,
    )

    result = m.migrate_universe_crsp_basis(
        apply=True,
        raw_fetch=lambda t: raw_frames[t],
        client=client,
        rel_tol=0.02,
        workers=1,
        macro={}, sector_map={}, fundamentals={}, alt_data={},
    )
    assert result["status"] == "ok"
    assert result["written_count"] == 1
    # Written to the SCRATCH lib...
    assert "X" in scratch.list_symbols()
    stored = scratch.read("X").data
    assert TOTAL_RETURN_COL in stored.columns
    # Close is the split-adjusted LEVEL; total_return_close is dividend-adjusted.
    assert list(stored["Close"].to_numpy()) == pytest.approx([50.0] * 6)
    assert list(stored[TOTAL_RETURN_COL].to_numpy()) == pytest.approx(
        [49.5, 49.5, 50.0, 50.0, 50.0, 50.0]
    )
    # ...and NEVER the live universe lib.
    live_lib.write.assert_not_called()


def test_orchestration_apply_is_idempotent(monkeypatch, tmp_path):
    adb = pytest.importorskip("arcticdb")
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    scratch = ac.get_library("universe_crsp", create_if_missing=True)

    raw_frames, old_closes, client = _clean_setup()
    _patch_orchestration(monkeypatch, old_closes=old_closes, scratch_lib=scratch)

    kwargs = dict(
        apply=True, raw_fetch=lambda t: raw_frames[t], client=client,
        rel_tol=0.02, workers=1,
        macro={}, sector_map={}, fundamentals={}, alt_data={},
    )
    r1 = m.migrate_universe_crsp_basis(**kwargs)
    r2 = m.migrate_universe_crsp_basis(**kwargs)
    assert r1["written_count"] == 1
    assert r2["written_count"] == 1
    # Re-run overwrites in place — one symbol, not duplicated.
    assert scratch.list_symbols() == ["X"]


def test_record_round_trips_to_dict():
    rec = ReconcileRecord(
        ticker="X", status="within_tol", n_common_dates=6, max_rel_dev=0.001,
        max_dev_date="2026-06-01", explained=True, explanation="ok",
    )
    d = rec.to_dict()
    assert d["ticker"] == "X" and d["status"] == "within_tol"
